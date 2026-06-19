// SPDX-License-Identifier: Apache-2.0
//! S3 output plugin — uploads events to an S3 bucket with file rotation.
//!
//! Buffering, size/time rotation logic, and S3-key generation are unchanged; the
//! upload itself uses `aws-sdk-s3` `PutObject`. Optional gzip encoding is applied
//! to the serialized payload before upload.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use ferro_stash_codec::{create_codec_from_settings, resolve_codec, Codec};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use flate2::write::GzEncoder;
use flate2::Compression;
use tokio::sync::OnceCell;
use tracing::{error, info, warn};

/// S3 output configuration — mirrors the Logstash S3 output settings.
///
/// `Debug` is implemented manually so the `secret_access_key` secret is never
/// rendered in logs/diagnostics (`{:?}` prints `Some("***")` / `None`, not the
/// plaintext key).
#[derive(Clone)]
pub struct S3OutputConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub time_file: u64,
    pub codec: String,
    pub encoding: String,
    pub size_file: usize,
    pub rotation_strategy: RotationStrategy,
    /// Optional custom endpoint for S3-compatible stores (MinIO, LocalStack, …).
    pub endpoint: Option<String>,
    /// Use path-style addressing (`endpoint/bucket/key`) — required by most
    /// S3-compatible stores.
    pub force_path_style: bool,
}

impl std::fmt::Debug for S3OutputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the secret access key so neither this struct nor any wrapper
        // (e.g. `S3Output`'s derived Debug) can leak the secret via `{:?}`.
        // `access_key_id` is an identifier, not a secret, so it stays visible.
        let secret_access_key = self.secret_access_key.as_ref().map(|_| "***");
        f.debug_struct("S3OutputConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &secret_access_key)
            .field("time_file", &self.time_file)
            .field("codec", &self.codec)
            .field("encoding", &self.encoding)
            .field("size_file", &self.size_file)
            .field("rotation_strategy", &self.rotation_strategy)
            .field("endpoint", &self.endpoint)
            .field("force_path_style", &self.force_path_style)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationStrategy {
    Size,
    Time,
    SizeAndTime,
}

/// Hard cap on buffered lines so an idle/never-rotating stream cannot grow the
/// in-RAM buffer without bound between `output()` calls (see `should_rotate`).
const S3_MAX_BUFFERED_LINES: usize = 1_000_000;

#[derive(Debug)]
pub struct S3Output {
    config: S3OutputConfig,
    condition: Option<Condition>,
    /// Codec used to serialize each event before buffering.
    codec: Box<dyn Codec>,
    /// In-memory buffer for events pending upload.
    ///
    /// Each entry is the raw codec-encoded bytes for one event. Lines are stored
    /// as `Vec<u8>` (not `String`) so binary codecs (msgpack/avro/protobuf) round-
    /// trip byte-for-byte into the uploaded object — round-tripping through
    /// `String`/`from_utf8_lossy` would replace invalid bytes and corrupt the
    /// object. The default `plain`/`json`/`line` codecs produce UTF-8, so those
    /// are unaffected.
    buffer: Mutex<Vec<Vec<u8>>>,
    /// When the current (post-rotation) buffer first received a line; drives
    /// time-based rotation. `None` while the buffer is empty.
    buffer_started: Mutex<Option<Instant>>,
    /// Bytes written to the current file.
    current_bytes: AtomicU64,
    /// Sequence number for file naming.
    sequence: AtomicU64,
    /// Lazily-built S3 client (construction is async; deferred out of `from_config`).
    client: OnceCell<aws_sdk_s3::Client>,
}

impl S3Output {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let bucket = settings.get_string("bucket").ok_or_else(|| {
            ferro_stash_core::error::FerroStashError::Output {
                plugin: "s3".to_string(),
                message: "bucket is required".to_string(),
            }
        })?;

        let prefix = settings
            .get_string("prefix")
            .unwrap_or_else(|| "logstash/".to_string());
        let region = settings
            .get_string("region")
            .unwrap_or_else(|| "us-east-1".to_string());
        let access_key_id = settings.get_string("access_key_id");
        let secret_access_key = settings.get_string("secret_access_key");
        let time_file = settings.get_u64("time_file").unwrap_or(900); // 15 min default
                                                                      // Resolve the codec name from both DSL forms so the recorded name matches
                                                                      // the codec that is actually built below.
        let (codec, _) = resolve_codec(settings, "plain");
        let encoding = settings
            .get_string("encoding")
            .unwrap_or_else(|| "none".to_string());
        let size_file = settings.get_u64("size_file").unwrap_or(5_242_880) as usize; // 5MB default
        let rotation_strategy = match settings
            .get("rotation_strategy")
            .and_then(|v| v.as_str())
            .unwrap_or("size_and_time")
        {
            "size" => RotationStrategy::Size,
            "time" => RotationStrategy::Time,
            _ => RotationStrategy::SizeAndTime,
        };
        let endpoint = settings.get_string("endpoint");
        let force_path_style = settings.get_bool("force_path_style").unwrap_or(false);

        // Build the codec used to serialize event payloads (config error => fail
        // loud), mirroring the kafka/redis outputs. `create_codec_from_settings`
        // honors both the string form (`codec => json`) and the descriptor form
        // (`codec => json { ... }`); `get_string("codec")` cannot see the
        // descriptor form, so without this it would silently fall back to the
        // default codec and drop its sub-settings.
        let codec_impl = create_codec_from_settings(settings, "plain")?;

        Ok(Self {
            config: S3OutputConfig {
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                time_file,
                codec,
                encoding,
                size_file,
                rotation_strategy,
                endpoint,
                force_path_style,
            },
            condition,
            codec: codec_impl,
            buffer: Mutex::new(Vec::new()),
            buffer_started: Mutex::new(None),
            current_bytes: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            client: OnceCell::new(),
        })
    }

    /// Returns the S3 client, building it on first use.
    ///
    /// When both `access_key_id` and `secret_access_key` are set, static
    /// credentials are used; otherwise the default AWS credential chain
    /// (env, profile, IMDS, …) is consulted.
    async fn client(&self) -> Result<&aws_sdk_s3::Client> {
        self.client
            .get_or_try_init(|| async {
                let region = aws_config::Region::new(self.config.region.clone());
                let mut loader =
                    aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

                if let (Some(access_key), Some(secret_key)) = (
                    self.config.access_key_id.as_ref(),
                    self.config.secret_access_key.as_ref(),
                ) {
                    let creds = aws_sdk_s3::config::Credentials::new(
                        access_key.clone(),
                        secret_key.clone(),
                        None,
                        None,
                        "ferro-stash-static",
                    );
                    loader = loader.credentials_provider(creds);
                }

                let sdk_config = loader.load().await;

                let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
                if let Some(endpoint) = self.config.endpoint.as_ref() {
                    s3_config = s3_config.endpoint_url(endpoint);
                }
                if self.config.force_path_style {
                    s3_config = s3_config.force_path_style(true);
                }

                Ok(aws_sdk_s3::Client::from_conf(s3_config.build()))
            })
            .await
    }

    /// Join the buffered (raw codec-encoded) lines with a single `\n` separator
    /// and apply gzip if `encoding == "gzip"`.
    ///
    /// The buffer holds raw codec bytes, so the join and the upload operate on
    /// bytes end-to-end — binary codecs are never lossily decoded.
    fn encode_payload(&self, lines: &[Vec<u8>]) -> Result<Vec<u8>> {
        // Join with a single `\n` separator without an intermediate `String`,
        // preserving arbitrary (non-UTF-8) codec bytes verbatim.
        let total: usize =
            lines.iter().map(Vec::len).sum::<usize>() + lines.len().saturating_sub(1);
        let mut joined = Vec::with_capacity(total);
        for (i, line) in lines.iter().enumerate() {
            if i > 0 {
                joined.push(b'\n');
            }
            joined.extend_from_slice(line);
        }
        if self.config.encoding == "gzip" {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(&joined)
                .map_err(|e| FerroStashError::Output {
                    plugin: "s3".to_string(),
                    message: format!("gzip encode error: {e}"),
                })?;
            encoder.finish().map_err(|e| FerroStashError::Output {
                plugin: "s3".to_string(),
                message: format!("gzip finish error: {e}"),
            })
        } else {
            Ok(joined)
        }
    }

    /// Upload one rotated file's worth of buffered lines to S3.
    async fn upload(&self, key: &str, lines: &[Vec<u8>]) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }

        // Gzip-encoded objects get a `.gz` suffix and content metadata.
        let (object_key, content_encoding, content_type) = if self.config.encoding == "gzip" {
            (format!("{key}.gz"), Some("gzip"), "application/gzip")
        } else {
            (key.to_string(), None, "text/plain")
        };

        let body = self.encode_payload(lines)?;
        let body_len = body.len();
        let client = self.client().await?;

        let mut request = client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&object_key)
            .body(ByteStream::from(body))
            .content_type(content_type);
        if let Some(enc) = content_encoding {
            request = request.content_encoding(enc);
        }

        request.send().await.map_err(|e| FerroStashError::Output {
            plugin: "s3".to_string(),
            // `DisplayErrorContext` surfaces the underlying service/error detail.
            message: format!(
                "PutObject s3://{}/{} failed: {}",
                self.config.bucket,
                object_key,
                aws_sdk_s3::error::DisplayErrorContext(&e)
            ),
        })?;

        info!(
            bucket = %self.config.bucket,
            key = %object_key,
            lines = lines.len(),
            bytes = body_len,
            "S3 output: uploaded object"
        );

        Ok(())
    }

    /// Serialize an event to a buffer line via the configured codec.
    ///
    /// Returns the raw codec bytes verbatim. S3 lines are buffered as `Vec<u8>`,
    /// so binary codecs (msgpack/avro/protobuf) are preserved byte-for-byte —
    /// there is no `String`/`from_utf8_lossy` round-trip that could corrupt them.
    fn encode_line(&self, event: &Event) -> Result<Vec<u8>> {
        self.codec
            .encode(event)
            .map_err(|e| FerroStashError::Output {
                plugin: "s3".to_string(),
                message: format!("codec encode error: {e}"),
            })
    }

    /// Generate an S3 key for the current file.
    fn generate_key(&self) -> String {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S");
        format!("{}{}-{:04}.log", self.config.prefix, ts, seq)
    }

    /// Whether the current buffer has aged past `time_file` seconds.
    ///
    /// `started` is the instant the current (post-rotation) buffer first received
    /// a line; `None` means the buffer is empty, so nothing to rotate on time.
    fn time_elapsed(&self, started: Option<Instant>) -> bool {
        match started {
            Some(start) => start.elapsed().as_secs() >= self.config.time_file,
            None => false,
        }
    }

    /// Check if the buffer should be rotated (flushed).
    ///
    /// Size-based rotation fires for `Size`/`SizeAndTime` when the byte counter
    /// reaches `size_file`. Time-based rotation fires for `Time`/`SizeAndTime`
    /// when the buffer has aged past `time_file` seconds.
    ///
    /// NOTE (idle-timer residual): rotation is only evaluated when `output()`
    /// runs (i.e. when events arrive). A buffer that goes idle after a partial
    /// fill will not rotate on a wall-clock timer — it rotates on the *next*
    /// `output()` call after `time_file` has elapsed, or on `flush()`/`close()`.
    /// A fully-correct background idle-flush timer is out of scope here.
    fn should_rotate(&self, started: Option<Instant>, buffered_lines: usize) -> bool {
        let size_hit = matches!(
            self.config.rotation_strategy,
            RotationStrategy::Size | RotationStrategy::SizeAndTime
        ) && self.current_bytes.load(Ordering::Relaxed) as usize
            >= self.config.size_file;

        let time_hit = matches!(
            self.config.rotation_strategy,
            RotationStrategy::Time | RotationStrategy::SizeAndTime
        ) && self.time_elapsed(started);

        // Safety net against unbounded RAM growth for a stream that never trips
        // size/time rotation (e.g. `Time` strategy with a very long interval and
        // a steady trickle of events): force a rotation once the buffer is huge.
        let cap_hit = buffered_lines >= S3_MAX_BUFFERED_LINES;

        size_hit || time_hit || cap_hit
    }

    /// Lock the line buffer, mapping a poisoned lock to a plugin error.
    fn lock_buffer(&self) -> Result<std::sync::MutexGuard<'_, Vec<Vec<u8>>>> {
        self.buffer.lock().map_err(|e| FerroStashError::Output {
            plugin: "s3".to_string(),
            message: format!("buffer lock poisoned: {e}"),
        })
    }

    /// Lock the buffer-start timestamp, mapping a poisoned lock to a plugin error.
    fn lock_started(&self) -> Result<std::sync::MutexGuard<'_, Option<Instant>>> {
        self.buffer_started
            .lock()
            .map_err(|e| FerroStashError::Output {
                plugin: "s3".to_string(),
                message: format!("buffer-start lock poisoned: {e}"),
            })
    }

    /// Upload a detached payload; on failure, restore it (and its byte count) to
    /// the front of the buffer so events are never lost on a transient error.
    ///
    /// Returns `Ok(())` on a successful upload, or the upload error (with the
    /// payload already restored to the buffer) on failure. Callers decide whether
    /// the failure is reportable: the rotation path (`output()`) swallows it
    /// (events stay buffered for the next rotation/flush), while the terminal
    /// `flush()`/`close()` path surfaces it.
    ///
    /// Any events buffered concurrently during the failed upload are preserved by
    /// re-prepending the detached lines ahead of them.
    async fn upload_or_restore(
        &self,
        key: &str,
        payload: Vec<Vec<u8>>,
        detached_bytes: u64,
    ) -> Result<()> {
        match self.upload(key, &payload).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Restore the payload ahead of anything buffered while we awaited.
                let mut buf = self.lock_buffer()?;
                let mut started = self.lock_started()?;
                if buf.is_empty() {
                    *buf = payload;
                } else {
                    let mut restored = payload;
                    restored.append(&mut buf);
                    *buf = restored;
                }
                self.current_bytes
                    .fetch_add(detached_bytes, Ordering::Relaxed);
                // Re-arm the rotation timer if it was cleared by the take. Residual:
                // this resets the clock to "now" rather than the original first-write
                // time, so a failed time-rotation effectively restarts the interval.
                if started.is_none() && !buf.is_empty() {
                    *started = Some(Instant::now());
                }
                Err(e)
            }
        }
    }
}

#[async_trait]
impl OutputPlugin for S3Output {
    fn name(&self) -> &'static str {
        "s3"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        // Encode events via the configured codec *before* taking the lock so a
        // codec error fails without mutating buffer state.
        let mut new_lines: Vec<(Vec<u8>, u64)> = Vec::with_capacity(events.len());
        for event in &events {
            let line = self.encode_line(event)?;
            let line_bytes = line.len() as u64;
            new_lines.push((line, line_bytes));
        }

        // Buffer the events and, if rotation triggers, detach the payload to upload.
        // The lock is released before the (async) upload so we never hold a
        // std::sync::Mutex across an await point. We record the detached byte count
        // so it can be restored if the upload fails.
        let rotated: Option<(String, Vec<Vec<u8>>, u64)> = {
            let mut buf = self.lock_buffer()?;
            let mut started = self.lock_started()?;

            for (line, line_bytes) in new_lines {
                // Stamp the buffer's first-write time so time-rotation can fire.
                if started.is_none() {
                    *started = Some(Instant::now());
                }
                buf.push(line);
                self.current_bytes.fetch_add(line_bytes, Ordering::Relaxed);
            }

            // Check for size-/time-/cap-based rotation.
            if self.should_rotate(*started, buf.len()) {
                let key = self.generate_key();
                let payload = std::mem::take(&mut *buf);
                let detached_bytes = self.current_bytes.swap(0, Ordering::Relaxed);
                *started = None;
                Some((key, payload, detached_bytes))
            } else {
                None
            }
        };

        if let Some((key, payload, detached_bytes)) = rotated {
            // Ownership model: for a *buffering* output, a rotation upload failure
            // must NOT propagate as an `output()` Err. `upload_or_restore` has
            // already put the detached payload back into the buffer, so the events
            // are safely retained and re-attempted on the next rotation or on
            // `flush()`/`close()`. Propagating `Err` here would make the core
            // pipeline DLQ this batch (pipeline.rs ~580-590) even though the events
            // are retained — and the failed payload spans events from MANY prior
            // already-`Ok`'d `output()` calls, which the per-call DLQ can't own
            // correctly anyway. Returning `Err` therefore double-handles the events
            // (they upload on the next rotation AND land in the DLQ → duplicates +
            // spurious failure accounting). We log and return `Ok(())` instead; the
            // buffering output owns the retry.
            if let Err(e) = self.upload_or_restore(&key, payload, detached_bytes).await {
                warn!(
                    bucket = %self.config.bucket,
                    key = %key,
                    error = %e,
                    "S3 output: rotation upload failed; events retained in buffer for retry on next rotation/flush"
                );
            }
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let rotated: Option<(String, Vec<Vec<u8>>, u64)> = {
            let mut buf = self.lock_buffer()?;
            let mut started = self.lock_started()?;

            if buf.is_empty() {
                None
            } else {
                let key = self.generate_key();
                let payload = std::mem::take(&mut *buf);
                let detached_bytes = self.current_bytes.swap(0, Ordering::Relaxed);
                *started = None;
                Some((key, payload, detached_bytes))
            }
        };

        if let Some((key, payload, detached_bytes)) = rotated {
            // Terminal attempt: `flush()`/`close()` is the *last* upload with no
            // further retry, so unlike the rotation path we surface the failure as
            // an `Err`. `upload_or_restore` still restores the payload to the buffer
            // (no data loss within the process lifetime), but at shutdown there is
            // no later flush to drain it.
            //
            // Known residual of the buffering model: a *persistent* S3 outage that
            // lasts through `close()` loses the still-buffered events when the
            // process exits — they only ever lived in the in-RAM buffer. This loss
            // is bounded by the existing `S3_MAX_BUFFERED_LINES` cap (the buffer can
            // never grow without limit). A durable on-disk spill at shutdown is out
            // of scope for this in-memory buffering output.
            if let Err(e) = self.upload_or_restore(&key, payload, detached_bytes).await {
                error!(
                    bucket = %self.config.bucket,
                    key = %key,
                    error = %e,
                    "S3 output: final flush upload failed; unflushed buffered events are unrecoverable at shutdown"
                );
                return Err(e);
            }
        }

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.flush().await
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Bytes,
        extract::Path,
        http::{HeaderMap, StatusCode},
        routing::put,
        Router,
    };
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    #[test]
    fn test_s3_output_config_defaults() {
        let settings = serde_json::json!({ "bucket": "my-bucket" });
        let output = S3Output::from_config(&settings, None).expect("config");
        assert_eq!(output.config.bucket, "my-bucket");
        assert_eq!(output.config.prefix, "logstash/");
        assert_eq!(output.config.region, "us-east-1");
        assert_eq!(output.config.time_file, 900);
        assert_eq!(output.config.size_file, 5_242_880);
        assert_eq!(output.name(), "s3");
    }

    #[test]
    fn test_s3_output_config_full() {
        let settings = serde_json::json!({
            "bucket": "prod",
            "prefix": "app/logs/",
            "region": "ap-northeast-1",
            "access_key_id": "AKIA...",
            "secret_access_key": "sec",
            "time_file": 300,
            "codec": "json",
            "encoding": "gzip",
            "size_file": 10485760,
            "rotation_strategy": "size"
        });
        let output = S3Output::from_config(&settings, None).expect("config");
        assert_eq!(output.config.prefix, "app/logs/");
        assert_eq!(output.config.time_file, 300);
        assert_eq!(output.config.encoding, "gzip");
        assert_eq!(output.config.rotation_strategy, RotationStrategy::Size);
    }

    #[test]
    fn test_s3_config_debug_redacts_secret_access_key() {
        // The secret_access_key must never appear in Debug output.
        let settings = serde_json::json!({
            "bucket": "prod",
            "access_key_id": "AKIAEXAMPLE",
            "secret_access_key": "super-secret-sak",
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        let config_dbg = format!("{:?}", output.config);
        assert!(
            !config_dbg.contains("super-secret-sak"),
            "config Debug leaked the secret_access_key: {config_dbg}"
        );
        assert!(
            config_dbg.contains("***"),
            "config Debug must mark redaction"
        );
        // Non-secret fields stay visible for diagnostics.
        assert!(config_dbg.contains("prod"), "bucket should remain visible");

        // The wrapper's Debug (which prints the config) must also not leak it.
        let output_dbg = format!("{output:?}");
        assert!(
            !output_dbg.contains("super-secret-sak"),
            "output Debug leaked the secret_access_key: {output_dbg}"
        );
    }

    #[test]
    fn test_s3_output_missing_bucket() {
        let settings = serde_json::json!({});
        assert!(S3Output::from_config(&settings, None).is_err());
    }

    #[tokio::test]
    async fn test_s3_output_buffering() {
        let settings = serde_json::json!({
            "bucket": "test",
            "size_file": 999999
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        let events = vec![Event::new("event1"), Event::new("event2")];
        let result = output.output(events).await;
        assert!(result.is_ok());

        let buf = output.buffer.lock().expect("lock");
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_s3_output_key_generation() {
        let settings = serde_json::json!({ "bucket": "b", "prefix": "p/" });
        let output = S3Output::from_config(&settings, None).expect("config");
        let key = output.generate_key();
        assert!(key.starts_with("p/"));
        assert!(key.ends_with(".log"));
    }

    #[test]
    fn test_s3_encode_payload_plain() {
        let settings = serde_json::json!({ "bucket": "b" });
        let output = S3Output::from_config(&settings, None).expect("config");
        let lines = vec![b"a".to_vec(), b"b".to_vec()];
        let bytes = output.encode_payload(&lines).expect("encode");
        assert_eq!(bytes, b"a\nb");
    }

    #[test]
    fn test_s3_encode_payload_gzip() {
        let settings = serde_json::json!({ "bucket": "b", "encoding": "gzip" });
        let output = S3Output::from_config(&settings, None).expect("config");
        let lines = vec![b"hello".to_vec(), b"world".to_vec()];
        let compressed = output.encode_payload(&lines).expect("encode");
        // gzip magic header.
        assert_eq!(&compressed[..2], &[0x1f, 0x8b]);
        // Round-trips back to the original payload.
        use std::io::Read;
        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut out = String::new();
        decoder.read_to_string(&mut out).expect("decode");
        assert_eq!(out, "hello\nworld");
    }

    /// Spawns a mock S3-compatible endpoint that accepts PUT object requests and
    /// records (path, content-encoding, body). Returns the base URL.
    #[allow(clippy::type_complexity)]
    async fn spawn_mock_s3() -> (
        String,
        Arc<std::sync::Mutex<Vec<(String, Option<String>, Vec<u8>)>>>,
    ) {
        let captured: Arc<std::sync::Mutex<Vec<(String, Option<String>, Vec<u8>)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let handle = Arc::clone(&captured);
        // Path-style: PUT /{bucket}/{key...} (axum 0.7 route syntax).
        let app = Router::new().route(
            "/:bucket/*key",
            put(
                move |Path((bucket, key)): Path<(String, String)>,
                      headers: HeaderMap,
                      body: Bytes| {
                    let handle = Arc::clone(&handle);
                    async move {
                        let enc = headers
                            .get("content-encoding")
                            .and_then(|v| v.to_str().ok())
                            .map(String::from);
                        if let Ok(mut guard) = handle.lock() {
                            guard.push((format!("{bucket}/{key}"), enc, body.to_vec()));
                        }
                        (StatusCode::OK, "")
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        (format!("http://{addr}"), captured)
    }

    #[tokio::test]
    async fn test_s3_output_flush_uploads() {
        let (endpoint, captured) = spawn_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "test-bucket",
            "prefix": "logs/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        output
            .output(vec![Event::new("flush-me")])
            .await
            .expect("output");
        output.flush().await.expect("flush");

        // Buffer drained.
        assert!(output.buffer.lock().expect("lock").is_empty());

        // One object uploaded to the right bucket/prefix with the line body.
        let objects = captured.lock().expect("lock");
        assert_eq!(objects.len(), 1, "expected one PutObject");
        let (path, enc, body) = &objects[0];
        assert!(path.starts_with("test-bucket/logs/"), "path was {path}");
        assert!(
            enc.is_none(),
            "plain encoding should set no content-encoding"
        );
        assert!(String::from_utf8_lossy(body).contains("flush-me"));
    }

    #[tokio::test]
    async fn test_s3_output_rotation_uploads_gzip() {
        let (endpoint, captured) = spawn_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            "encoding": "gzip",
            "rotation_strategy": "size",
            // Tiny size so the first batch rotates immediately.
            "size_file": 1,
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        output
            .output(vec![Event::new("rotate-now")])
            .await
            .expect("output");

        // Rotation drained the buffer and uploaded a gzip object with `.gz` suffix.
        assert!(output.buffer.lock().expect("lock").is_empty());
        let objects = captured.lock().expect("lock");
        assert_eq!(objects.len(), 1);
        let (path, enc, body) = &objects[0];
        assert!(
            path.ends_with(".gz"),
            "gzip key should end with .gz: {path}"
        );
        assert_eq!(enc.as_deref(), Some("gzip"));
        assert_eq!(&body[..2], &[0x1f, 0x8b], "body should be gzip-compressed");
    }

    /// Spawns a mock S3-compatible endpoint that always fails PUT object requests
    /// with HTTP 500 (records the attempt count). Returns the base URL.
    async fn spawn_failing_mock_s3() -> (String, Arc<AtomicUsize>) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let handle = Arc::clone(&attempts);
        let app = Router::new().route(
            "/:bucket/*key",
            put(move || {
                let handle = Arc::clone(&handle);
                async move {
                    handle.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::INTERNAL_SERVER_ERROR, "boom")
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        (format!("http://{addr}"), attempts)
    }

    #[tokio::test]
    async fn test_s3_output_upload_failure_preserves_buffer() {
        // Regression for finding #1 (data-loss) + round-13 finding (dup/DLQ): a
        // transient rotation PutObject error must NOT lose the buffered events —
        // they stay in the buffer (and byte count) for retry on the next
        // rotation/flush. AND `output()` must return `Ok(())` rather than `Err`:
        // this is a *buffering* output that owns its own retry, so propagating an
        // error would make the core pipeline DLQ a batch that is actually retained
        // (→ later re-uploads = duplicates + spurious failure accounting).
        let (endpoint, attempts) = spawn_failing_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            "rotation_strategy": "size",
            // Force rotation on the first event.
            "size_file": 1,
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        // Rotation path: output() triggers an upload that fails. The buffering
        // output swallows the failure (retains events) and returns Ok — so the
        // pipeline does NOT DLQ this batch.
        let result = output.output(vec![Event::new("keep-me")]).await;
        assert!(
            result.is_ok(),
            "rotation upload failure must be swallowed (events retained for retry), not surfaced as Err"
        );
        assert!(
            attempts.load(Ordering::SeqCst) >= 1,
            "PutObject was attempted"
        );

        // The event is preserved in the buffer (not lost) and bytes are restored.
        let buf = output.buffer.lock().expect("lock");
        assert_eq!(buf.len(), 1, "buffered event must be retained on failure");
        assert!(String::from_utf8_lossy(&buf[0]).contains("keep-me"));
        assert!(
            output.current_bytes.load(Ordering::Relaxed) > 0,
            "byte counter must be restored on failure"
        );
    }

    #[tokio::test]
    async fn test_s3_output_transient_rotation_failure_then_flush_uploads_once() {
        // Regression for round-13 finding: a transient rotation-upload failure
        // followed by a successful flush must upload the events EXACTLY ONCE (no
        // duplicate) and the failed `output()` must NOT have returned Err (so the
        // pipeline would not also DLQ the same events). This is the end-to-end
        // proof that the buffering output owns retry rather than double-handing the
        // events to the pipeline DLQ.
        //
        // The mock fails every PutObject while `fail` is set (so the rotation
        // upload genuinely fails even after the AWS SDK's internal retries) and
        // succeeds once the test clears `fail` before calling flush(). This lets us
        // observe the single surviving object from the flush retry.
        let captured: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let cap_handle = Arc::clone(&captured);
        let att_handle = Arc::clone(&attempts);
        let fail_handle = Arc::clone(&fail);
        let app = Router::new().route(
            "/:bucket/*key",
            put(move |_p: Path<(String, String)>, body: Bytes| {
                let cap_handle = Arc::clone(&cap_handle);
                let att_handle = Arc::clone(&att_handle);
                let fail_handle = Arc::clone(&fail_handle);
                async move {
                    att_handle.fetch_add(1, Ordering::SeqCst);
                    if fail_handle.load(Ordering::SeqCst) {
                        (StatusCode::INTERNAL_SERVER_ERROR, "boom")
                    } else {
                        if let Ok(mut g) = cap_handle.lock() {
                            g.push(body.to_vec());
                        }
                        (StatusCode::OK, "ok")
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        let endpoint = format!("http://{addr}");

        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            "rotation_strategy": "size",
            // Force rotation on the first event so output() drives the upload.
            "size_file": 1,
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        // Rotation upload fails — but the buffering output retains the event and
        // returns Ok (no DLQ double-handling).
        let result = output.output(vec![Event::new("once-only")]).await;
        assert!(
            result.is_ok(),
            "transient rotation failure must not surface as Err (would cause DLQ dup)"
        );
        // Event retained for retry.
        assert_eq!(
            output.buffer.lock().expect("lock").len(),
            1,
            "event must remain buffered after the transient rotation failure"
        );

        // Clear the transient fault so the next upload (flush) succeeds, then flush
        // uploads the retained event exactly once.
        fail.store(false, Ordering::SeqCst);
        output.flush().await.expect("flush should succeed");
        assert!(
            output.buffer.lock().expect("lock").is_empty(),
            "buffer must drain after the successful flush"
        );

        let objects = captured.lock().expect("lock");
        assert_eq!(
            objects.len(),
            1,
            "the event must be uploaded EXACTLY ONCE (no duplicate from retry + DLQ)"
        );
        assert!(
            String::from_utf8_lossy(&objects[0]).contains("once-only"),
            "the single uploaded object must carry the retained event"
        );
        // Two PutObject attempts total: the failed rotation + the successful flush.
        assert!(
            attempts.load(Ordering::SeqCst) >= 2,
            "expected a failed rotation attempt followed by a successful flush attempt"
        );
    }

    #[tokio::test]
    async fn test_s3_output_flush_failure_preserves_buffer() {
        // Same protection on the flush() path.
        let (endpoint, _attempts) = spawn_failing_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            // Large size so output() only buffers; flush() does the failing upload.
            "size_file": 9_999_999,
            "rotation_strategy": "size",
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        output
            .output(vec![Event::new("flush-keep")])
            .await
            .expect("buffer only");
        let result = output.flush().await;
        assert!(
            result.is_err(),
            "flush upload failure must surface an error"
        );

        let buf = output.buffer.lock().expect("lock");
        assert_eq!(buf.len(), 1, "flush failure must retain the event");
        assert!(String::from_utf8_lossy(&buf[0]).contains("flush-keep"));
    }

    #[tokio::test]
    async fn test_s3_output_time_rotation_fires() {
        // Regression for finding #2: with a 0s time_file, the buffered events must
        // rotate (upload) on the next output() call for both Time and SizeAndTime.
        for strategy in ["time", "size_and_time"] {
            let (endpoint, captured) = spawn_mock_s3().await;
            let settings = serde_json::json!({
                "bucket": "b",
                "prefix": "p/",
                "endpoint": endpoint,
                "force_path_style": true,
                "access_key_id": "test",
                "secret_access_key": "test",
                "rotation_strategy": strategy,
                // Never rotate on size; rotate purely on elapsed time.
                "size_file": 9_999_999,
                "time_file": 0,
            });
            let output = S3Output::from_config(&settings, None).expect("config");

            // First call buffers and stamps the start time; time_file=0 means the
            // elapsed check is already satisfied, so it rotates immediately.
            output
                .output(vec![Event::new("tick")])
                .await
                .expect("output");

            assert!(
                output.buffer.lock().expect("lock").is_empty(),
                "time rotation ({strategy}) must drain the buffer",
            );
            let objects = captured.lock().expect("lock");
            assert_eq!(
                objects.len(),
                1,
                "time rotation ({strategy}) must upload one object",
            );
        }
    }

    #[tokio::test]
    async fn test_s3_output_honors_plain_codec() {
        // Regression for finding #3: the configured codec is used to serialize
        // events. The `plain` codec emits the raw message (+newline), not JSON.
        let (endpoint, captured) = spawn_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            "codec": "plain",
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        output
            .output(vec![Event::new("plain-line")])
            .await
            .expect("output");
        output.flush().await.expect("flush");

        let objects = captured.lock().expect("lock");
        assert_eq!(objects.len(), 1);
        let body = String::from_utf8_lossy(&objects[0].2);
        // Plain codec => raw message, no JSON braces/quotes around it.
        assert!(body.contains("plain-line"), "body was {body}");
        assert!(
            !body.contains("\"message\""),
            "plain codec must not emit JSON keys: {body}"
        );
    }

    #[tokio::test]
    async fn test_s3_output_honors_json_codec() {
        // The json codec produces a JSON object per event.
        let (endpoint, captured) = spawn_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            "codec": "json",
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        output
            .output(vec![Event::new("json-line")])
            .await
            .expect("output");
        output.flush().await.expect("flush");

        let objects = captured.lock().expect("lock");
        assert_eq!(objects.len(), 1);
        let body = String::from_utf8_lossy(&objects[0].2);
        assert!(body.contains("\"message\""), "json codec body: {body}");
        assert!(body.contains("json-line"), "json codec body: {body}");
    }

    #[test]
    fn test_s3_output_unknown_codec_rejected() {
        // An unknown codec must fail loudly at config time (like kafka/redis).
        let settings = serde_json::json!({ "bucket": "b", "codec": "no-such-codec" });
        assert!(S3Output::from_config(&settings, None).is_err());
    }

    #[tokio::test]
    async fn test_s3_descriptor_form_codec_honored() {
        // The DSL descriptor form `codec => json { ... }` (object with `_plugin`)
        // must build the NAMED codec, not the default `plain`. `get_string("codec")`
        // returns None for it, which used to silently fall back to the default.
        let (endpoint, captured) = spawn_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            "codec": { "_plugin": "json" },
        });
        let output = S3Output::from_config(&settings, None).expect("config");
        assert_eq!(
            output.config.codec, "json",
            "descriptor codec name resolved"
        );

        output
            .output(vec![Event::new("desc-line")])
            .await
            .expect("output");
        output.flush().await.expect("flush");

        let objects = captured.lock().expect("lock");
        assert_eq!(objects.len(), 1);
        let body = String::from_utf8_lossy(&objects[0].2);
        // json codec => JSON keys present (descriptor honored, not the plain default).
        assert!(body.contains("\"message\""), "json codec body: {body}");
        assert!(body.contains("desc-line"), "json codec body: {body}");
    }

    #[test]
    fn test_s3_descriptor_form_unknown_codec_rejected() {
        // An unknown codec inside the descriptor form still fails loudly.
        let settings = serde_json::json!({
            "bucket": "b",
            "codec": { "_plugin": "no-such-codec" },
        });
        assert!(S3Output::from_config(&settings, None).is_err());
    }

    #[tokio::test]
    async fn test_s3_output_binary_codec_roundtrips_byte_for_byte() {
        // Regression for round-3 finding #1: a binary codec (msgpack) must round-
        // trip byte-for-byte into the uploaded object. Previously the buffer stored
        // `String` and decoded codec bytes via `from_utf8_lossy`, which REPLACES
        // invalid bytes (U+FFFD) and corrupts non-UTF-8 payloads.
        let (endpoint, captured) = spawn_mock_s3().await;
        let settings = serde_json::json!({
            "bucket": "b",
            "prefix": "p/",
            "endpoint": endpoint,
            "force_path_style": true,
            "access_key_id": "test",
            "secret_access_key": "test",
            "codec": "msgpack",
        });
        let output = S3Output::from_config(&settings, None).expect("config");

        // Two events so we also exercise the `\n` join over binary lines.
        let event1 = Event::new("msgpack-bytes-1");
        let event2 = Event::new("msgpack-bytes-2");

        // Compute the exact bytes we expect: each event's raw codec output joined
        // by a single `\n`, using the SAME codec the output is configured with.
        let codec = create_codec_from_settings(&settings, "plain").expect("codec");
        let enc1 = codec.encode(&event1).expect("encode1");
        let enc2 = codec.encode(&event2).expect("encode2");
        // Sanity: msgpack output for these events is genuinely non-UTF-8 (so a
        // lossy String round-trip would have corrupted it).
        assert!(
            std::str::from_utf8(&enc1).is_err(),
            "msgpack payload should be non-UTF-8 for this test to be meaningful",
        );
        let mut expected = Vec::new();
        expected.extend_from_slice(&enc1);
        expected.push(b'\n');
        expected.extend_from_slice(&enc2);

        output.output(vec![event1, event2]).await.expect("output");
        output.flush().await.expect("flush");

        let objects = captured.lock().expect("lock");
        assert_eq!(objects.len(), 1, "expected one PutObject");
        let body = &objects[0].2;
        // Byte-for-byte equality: no replacement characters, no corruption.
        assert_eq!(
            body, &expected,
            "uploaded body must equal the raw encoded bytes joined by \\n",
        );
        // Defensively assert the body is not valid UTF-8 (it carries raw msgpack),
        // confirming we did not silently coerce through a UTF-8 path.
        assert!(
            std::str::from_utf8(body).is_err(),
            "uploaded binary payload must remain non-UTF-8",
        );
    }

    /// Live smoke test against real S3 (or an S3-compatible store).
    /// Gated behind `S3_BUCKET`; honors `AWS_REGION`, `S3_ENDPOINT`,
    /// `S3_FORCE_PATH_STYLE`, and the standard AWS credential env vars.
    /// Run with `cargo test -p ferro-stash-output -- --ignored s3_live`.
    #[tokio::test]
    #[ignore = "requires S3 access (S3_BUCKET env var + AWS credentials)"]
    async fn s3_live_smoke() {
        let bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET");
        let mut settings = serde_json::json!({
            "bucket": bucket,
            "prefix": "ferro-stash-live/",
            "region": std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        });
        if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
            settings["endpoint"] = serde_json::Value::String(endpoint);
        }
        if std::env::var("S3_FORCE_PATH_STYLE").is_ok() {
            settings["force_path_style"] = serde_json::Value::Bool(true);
        }
        if let (Ok(ak), Ok(sk)) = (
            std::env::var("AWS_ACCESS_KEY_ID"),
            std::env::var("AWS_SECRET_ACCESS_KEY"),
        ) {
            settings["access_key_id"] = serde_json::Value::String(ak);
            settings["secret_access_key"] = serde_json::Value::String(sk);
        }
        let output = S3Output::from_config(&settings, None).expect("config");
        output
            .output(vec![Event::new("s3 live smoke")])
            .await
            .expect("output");
        output.flush().await.expect("live PutObject should succeed");
    }
}
