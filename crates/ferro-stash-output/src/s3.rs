// SPDX-License-Identifier: Apache-2.0
//! S3 output plugin — uploads events to an S3 bucket with file rotation.
//!
//! Buffering, size/time rotation logic, and S3-key generation are unchanged; the
//! upload itself uses `aws-sdk-s3` `PutObject`. Optional gzip encoding is applied
//! to the serialized payload before upload.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use flate2::write::GzEncoder;
use flate2::Compression;
use tokio::sync::OnceCell;
use tracing::info;

/// S3 output configuration — mirrors the Logstash S3 output settings.
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationStrategy {
    Size,
    Time,
    SizeAndTime,
}

#[derive(Debug)]
pub struct S3Output {
    config: S3OutputConfig,
    condition: Option<Condition>,
    /// In-memory buffer for events pending upload.
    buffer: Mutex<Vec<String>>,
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
        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "plain".to_string());
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
            buffer: Mutex::new(Vec::new()),
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

    /// Serialize the buffered lines and apply gzip if `encoding == "gzip"`.
    fn encode_payload(&self, lines: &[String]) -> Result<Vec<u8>> {
        let joined = lines.join("\n");
        if self.config.encoding == "gzip" {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(joined.as_bytes())
                .map_err(|e| FerroStashError::Output {
                    plugin: "s3".to_string(),
                    message: format!("gzip encode error: {e}"),
                })?;
            encoder.finish().map_err(|e| FerroStashError::Output {
                plugin: "s3".to_string(),
                message: format!("gzip finish error: {e}"),
            })
        } else {
            Ok(joined.into_bytes())
        }
    }

    /// Upload one rotated file's worth of buffered lines to S3.
    async fn upload(&self, key: &str, lines: &[String]) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }

        // Gzip-encoded objects get a `.gz` suffix and content metadata.
        let (object_key, content_encoding, content_type) = if self.config.encoding == "gzip" {
            (
                format!("{key}.gz"),
                Some("gzip"),
                "application/gzip",
            )
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

    /// Generate an S3 key for the current file.
    fn generate_key(&self) -> String {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S");
        format!("{}{}-{:04}.log", self.config.prefix, ts, seq)
    }

    /// Check if the buffer should be rotated (flushed) based on size.
    fn should_rotate(&self) -> bool {
        match self.config.rotation_strategy {
            RotationStrategy::Size | RotationStrategy::SizeAndTime => {
                self.current_bytes.load(Ordering::Relaxed) as usize >= self.config.size_file
            }
            RotationStrategy::Time => false, // Time-based rotation handled externally
        }
    }
}

#[async_trait]
impl OutputPlugin for S3Output {
    fn name(&self) -> &'static str {
        "s3"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        // Buffer the events and, if rotation triggers, detach the payload to upload.
        // The lock is released before the (async) upload so we never hold a
        // std::sync::Mutex across an await point.
        let rotated: Option<(String, Vec<String>)> = {
            let mut buf =
                self.buffer
                    .lock()
                    .map_err(|e| FerroStashError::Output {
                        plugin: "s3".to_string(),
                        message: format!("buffer lock poisoned: {e}"),
                    })?;

            for event in &events {
                let line = event.to_json_string();
                let line_bytes = line.len() as u64;
                buf.push(line);
                self.current_bytes.fetch_add(line_bytes, Ordering::Relaxed);
            }

            // Check for size-based rotation.
            if self.should_rotate() {
                let key = self.generate_key();
                let payload = std::mem::take(&mut *buf);
                self.current_bytes.store(0, Ordering::Relaxed);
                Some((key, payload))
            } else {
                None
            }
        };

        if let Some((key, payload)) = rotated {
            self.upload(&key, &payload).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let rotated: Option<(String, Vec<String>)> = {
            let mut buf =
                self.buffer
                    .lock()
                    .map_err(|e| FerroStashError::Output {
                        plugin: "s3".to_string(),
                        message: format!("buffer lock poisoned: {e}"),
                    })?;

            if buf.is_empty() {
                None
            } else {
                let key = self.generate_key();
                let payload = std::mem::take(&mut *buf);
                self.current_bytes.store(0, Ordering::Relaxed);
                Some((key, payload))
            }
        };

        if let Some((key, payload)) = rotated {
            self.upload(&key, &payload).await?;
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
        let lines = vec!["a".to_string(), "b".to_string()];
        let bytes = output.encode_payload(&lines).expect("encode");
        assert_eq!(bytes, b"a\nb");
    }

    #[test]
    fn test_s3_encode_payload_gzip() {
        let settings = serde_json::json!({ "bucket": "b", "encoding": "gzip" });
        let output = S3Output::from_config(&settings, None).expect("config");
        let lines = vec!["hello".to_string(), "world".to_string()];
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
        assert!(enc.is_none(), "plain encoding should set no content-encoding");
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
        assert!(path.ends_with(".gz"), "gzip key should end with .gz: {path}");
        assert_eq!(enc.as_deref(), Some("gzip"));
        assert_eq!(&body[..2], &[0x1f, 0x8b], "body should be gzip-compressed");
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
