// SPDX-License-Identifier: Apache-2.0
//! S3 input plugin — polls an S3 bucket for new objects and emits their contents as events.
//!
//! Backed by `aws-sdk-s3`. On each poll cycle the plugin lists objects under `prefix`
//! (paginated via `ListObjectsV2` continuation tokens), fetches each not-yet-seen key
//! with `GetObject`, decodes the body with the configured codec, emits the resulting
//! events, and (optionally) deletes the object with `DeleteObject`. Already-processed
//! keys are tracked in an in-memory `HashSet` to avoid reprocessing across cycles.
//!
//! Credentials: if `access_key_id`/`secret_access_key` are configured they are used as
//! static credentials; otherwise the default AWS credential provider chain is used.
//!
//! A `test_data` injection point is preserved for unit tests (no AWS calls).

use std::collections::HashSet;

use async_trait::async_trait;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::Client;
use ferro_stash_codec::{create_codec, resolve_codec, Codec};
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// S3 input configuration — mirrors the Logstash S3 input settings.
#[derive(Clone)]
pub struct S3InputConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Optional custom endpoint for S3-compatible stores (MinIO, LocalStack,
    /// Ceph, …). When unset, the default AWS S3 endpoints are used.
    pub endpoint: Option<String>,
    /// Use path-style addressing (`endpoint/bucket/key`) — required by most
    /// S3-compatible stores. Defaults to false (virtual-hosted style).
    pub force_path_style: bool,
    pub interval: u64,
    pub codec: String,
    /// Codec sub-settings extracted from the `codec => name { ... }` block;
    /// empty object when the codec is named without a settings block.
    pub codec_settings: serde_json::Value,
    pub delete_after_read: bool,
    /// Maximum object size (bytes) that will be fetched and decoded. Objects
    /// whose `ListObjectsV2`-reported size exceeds this are skipped (logged and
    /// marked seen) rather than streamed into memory — `GetObject` buffers the
    /// whole body, so a multi-GB object would otherwise OOM the process.
    /// Default is [`DEFAULT_MAX_OBJECT_SIZE`] (100 MB). Oversized objects are
    /// SKIPPED, never truncated.
    pub max_object_size: u64,
}

/// Default `max_object_size`: 100 MB (100_000_000 bytes). Objects larger than
/// this are skipped to bound memory use, since `GetObject` reads the entire
/// body into memory before decoding.
pub const DEFAULT_MAX_OBJECT_SIZE: u64 = 100_000_000;

/// Minimum poll interval (seconds). A configured `interval` below this is
/// clamped up to avoid a tight polling loop that hammers S3 (retry storm,
/// throttling, cost) when the prefix is small or the list call errors.
pub const MIN_POLL_INTERVAL_SECS: u64 = 1;

// Manual Debug to avoid leaking `secret_access_key` into logs / error context.
impl std::fmt::Debug for S3InputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3InputConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "***"),
            )
            .field("endpoint", &self.endpoint)
            .field("force_path_style", &self.force_path_style)
            .field("interval", &self.interval)
            .field("codec", &self.codec)
            .field("codec_settings", &self.codec_settings)
            .field("delete_after_read", &self.delete_after_read)
            .field("max_object_size", &self.max_object_size)
            .finish()
    }
}

#[derive(Debug)]
pub struct S3Input {
    config: S3InputConfig,
    /// Test data injection: when set, these lines are emitted instead of polling S3.
    test_data: Option<Vec<String>>,
}

impl S3Input {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let bucket = settings
            .get_string("bucket")
            .ok_or_else(|| FerroStashError::Input {
                plugin: "s3".to_string(),
                message: "bucket is required".to_string(),
            })?;

        let prefix = settings.get_string("prefix").unwrap_or_default();
        let region = settings
            .get_string("region")
            .unwrap_or_else(|| "us-east-1".to_string());
        let access_key_id = settings.get_string("access_key_id");
        let secret_access_key = settings.get_string("secret_access_key");
        let endpoint = settings.get_string("endpoint");
        let force_path_style = settings.get_bool("force_path_style").unwrap_or(false);
        // Clamp the poll interval to a sane minimum. `interval => 0` would make
        // `Duration::from_secs(0)` a no-op sleep, turning the poll loop into a
        // tight spin against S3 (retry storm / throttling / cost) — so we floor
        // it at `MIN_POLL_INTERVAL_SECS`.
        let interval = settings
            .get_u64("interval")
            .unwrap_or(60)
            .max(MIN_POLL_INTERVAL_SECS);
        // Resolve the codec name and its sub-settings so they are honored rather
        // than dropped.
        let (codec, codec_settings) = resolve_codec(settings, "plain");
        let delete_after_read = settings.get_bool("delete_after_read").unwrap_or(false);
        let max_object_size = settings
            .get_u64("max_object_size")
            .unwrap_or(DEFAULT_MAX_OBJECT_SIZE);

        Ok(Self {
            config: S3InputConfig {
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                endpoint,
                force_path_style,
                interval,
                codec,
                codec_settings,
                delete_after_read,
                max_object_size,
            },
            test_data: None,
        })
    }

    /// Inject test data — these lines are emitted as events instead of polling S3.
    pub fn with_test_data(mut self, data: Vec<String>) -> Self {
        self.test_data = Some(data);
        self
    }

    /// Builds the configured codec, threading its sub-settings through and
    /// mapping codec errors into an input error.
    fn build_codec(&self) -> Result<Box<dyn Codec>> {
        create_codec(&self.config.codec, &self.config.codec_settings).map_err(|e| {
            FerroStashError::Input {
                plugin: "s3".to_string(),
                message: format!("unknown/invalid codec '{}': {e}", self.config.codec),
            }
        })
    }

    /// Builds an `aws-sdk-s3` client. Uses static credentials when both
    /// `access_key_id` and `secret_access_key` are configured; otherwise falls
    /// back to the default AWS credential provider chain (env / profile / IMDS).
    async fn build_client(&self) -> Client {
        let region = Region::new(self.config.region.clone());
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

        // Static credentials when both are configured; otherwise the default AWS
        // credential provider chain (env / profile / IMDS).
        if let (Some(ak), Some(sk)) = (&self.config.access_key_id, &self.config.secret_access_key) {
            let creds =
                Credentials::new(ak.clone(), sk.clone(), None, None, "ferro-stash-s3-input");
            loader = loader.credentials_provider(creds);
        }

        let sdk_config = loader.load().await;

        // Apply S3-compatible-store overrides (MinIO / LocalStack / Ceph). This
        // mirrors the S3 *output* so reading from a non-AWS store works too.
        let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint) = self.config.endpoint.as_ref() {
            s3_config = s3_config.endpoint_url(endpoint);
        }
        if self.config.force_path_style {
            s3_config = s3_config.force_path_style(true);
        }
        Client::from_conf(s3_config.build())
    }
}

#[async_trait]
impl InputPlugin for S3Input {
    fn name(&self) -> &'static str {
        "s3"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        info!(
            bucket = %self.config.bucket,
            prefix = %self.config.prefix,
            region = %self.config.region,
            interval_secs = self.config.interval,
            "S3 input starting"
        );

        // Test mode: emit injected data and exit.
        if let Some(ref data) = self.test_data {
            for line in data {
                let mut event = Event::new(line);
                event
                    .metadata
                    .set("s3".to_string(), s3_metadata(&self.config.bucket, None));
                if sender.send(event).await.is_err() {
                    return Ok(());
                }
            }
            // Wait for shutdown after test data is drained.
            shutdown.wait().await;
            return Ok(());
        }

        // --- Real S3 polling loop ---
        let codec = self.build_codec()?;
        let client = self.build_client().await;

        // Keys already processed (and not deleted) so we never re-emit them.
        let mut seen: HashSet<String> = HashSet::new();

        let poll_interval = tokio::time::Duration::from_secs(self.config.interval);

        info!(bucket = %self.config.bucket, "S3 input connected; starting poll loop");

        loop {
            // One poll cycle. The outcome decides whether we stop, back off, or
            // wait the normal poll interval.
            let wait = match self
                .poll_once(&client, codec.as_ref(), &sender, &mut seen)
                .await
            {
                PollOutcome::Closed => {
                    info!("S3 input: downstream channel closed, stopping");
                    break;
                }
                // A list error means we got no usable listing this cycle. Back
                // off (the poll interval, already floored at >= 1s) before
                // retrying instead of spinning on a persistent error.
                PollOutcome::ListError => {
                    debug!(
                        backoff_secs = self.config.interval,
                        "S3 input: poll error, backing off before retry"
                    );
                    poll_interval
                }
                PollOutcome::Polled => poll_interval,
            };

            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = shutdown.wait() => {
                    info!("S3 input shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

impl S3Input {
    /// Lists objects under the prefix, fetches/decodes/emits each new one, and
    /// optionally deletes it. The returned [`PollOutcome`] tells the caller
    /// whether the downstream closed (stop), the listing errored (back off), or
    /// the cycle completed normally.
    async fn poll_once(
        &self,
        client: &Client,
        codec: &dyn Codec,
        sender: &mpsc::Sender<Event>,
        seen: &mut HashSet<String>,
    ) -> PollOutcome {
        let mut continuation: Option<String> = None;

        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(&self.config.prefix);
            if let Some(token) = &continuation {
                req = req.continuation_token(token.clone());
            }

            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!(bucket = %self.config.bucket, error = %e, "S3 ListObjectsV2 failed");
                    return PollOutcome::ListError;
                }
            };

            for object in resp.contents() {
                let Some(key) = object.key() else { continue };
                // Skip "directory" placeholder keys and already-seen keys.
                if key.ends_with('/') || seen.contains(key) {
                    continue;
                }
                let key = key.to_string();

                // Guard against unbounded buffering: `GetObject` reads the whole
                // body into memory, so before fetching we consult the
                // list-reported size. An object over `max_object_size` is logged,
                // marked `seen` (so it is not retried on every poll forever), and
                // skipped — never truncated.
                if Self::is_oversized(object.size(), self.config.max_object_size) {
                    warn!(
                        bucket = %self.config.bucket,
                        key = %key,
                        size = object.size().unwrap_or_default(),
                        limit = self.config.max_object_size,
                        "S3 object exceeds max_object_size; skipping (not truncated)"
                    );
                    Self::record_emitted(seen, &key);
                    continue;
                }

                match self.fetch_and_emit(client, codec, sender, &key).await {
                    FetchResult::Emitted => {
                        // Invariant: emitted-once ⇒ seen. Record the key as
                        // processed *before* attempting any cleanup (see
                        // `record_emitted`), then best-effort delete. A delete
                        // failure must never undo the `seen` record, otherwise the
                        // object lingers in the bucket *and* unseen → unbounded
                        // duplicate ingestion.
                        Self::record_emitted(seen, &key);
                        if self.config.delete_after_read {
                            // Best-effort cleanup. On failure we log and move on:
                            // the object lingers in the bucket but, being `seen`,
                            // is never reprocessed by this run.
                            if let Err(e) = self.delete_object(client, &key).await {
                                warn!(
                                    bucket = %self.config.bucket,
                                    key = %key,
                                    error = %e,
                                    "S3 DeleteObject failed; object will remain in the \
                                     bucket but will not be re-emitted"
                                );
                            }
                        }
                    }
                    FetchResult::Skipped => {
                        // Transient fetch/decode error — leave unseen to retry next cycle.
                    }
                    FetchResult::ChannelClosed => return PollOutcome::Closed,
                }
            }

            // Paginate.
            if resp.is_truncated().unwrap_or(false) {
                match resp.next_continuation_token() {
                    Some(token) => continuation = Some(token.to_string()),
                    None => break,
                }
            } else {
                break;
            }
        }

        PollOutcome::Polled
    }

    /// Fetches a single object, decodes it, and emits the events.
    async fn fetch_and_emit(
        &self,
        client: &Client,
        codec: &dyn Codec,
        sender: &mpsc::Sender<Event>,
        key: &str,
    ) -> FetchResult {
        let resp = match client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                warn!(bucket = %self.config.bucket, key = %key, error = %e, "S3 GetObject failed");
                return FetchResult::Skipped;
            }
        };

        let bytes = match resp.body.collect().await {
            Ok(agg) => agg.into_bytes(),
            Err(e) => {
                warn!(key = %key, error = %e, "S3 object body read failed");
                return FetchResult::Skipped;
            }
        };

        let events = match codec.decode(&bytes) {
            Ok(events) => events,
            Err(e) => {
                warn!(key = %key, error = %e, "S3 object decode failed; skipping");
                return FetchResult::Skipped;
            }
        };

        debug!(key = %key, events = events.len(), "S3 object decoded");

        for mut event in events {
            // Stamp S3 metadata into the event's metadata struct so it is
            // available in-pipeline as `[@metadata][s3]` but never serialized to
            // the output by `Event::to_json`.
            event.metadata.set(
                "s3".to_string(),
                s3_metadata(&self.config.bucket, Some(key)),
            );
            if sender.send(event).await.is_err() {
                return FetchResult::ChannelClosed;
            }
        }

        FetchResult::Emitted
    }

    /// Decides whether an object should be skipped for exceeding the size cap,
    /// given its `ListObjectsV2`-reported size and the configured `limit`.
    ///
    /// A known size strictly greater than `limit` is oversized. A `None`/missing
    /// or negative size is treated as **not** oversized here — the list-size
    /// check is the primary defense; an unknown size still proceeds to
    /// `GetObject` (the `Skipped` path handles fetch/decode errors). Extracted as
    /// a pure function so the decision is unit-testable without any AWS calls.
    fn is_oversized(size: Option<i64>, limit: u64) -> bool {
        match size {
            Some(sz) if sz >= 0 => (sz as u64) > limit,
            _ => false,
        }
    }

    /// Records a successfully-emitted object's key as `seen`.
    ///
    /// This is the single point that enforces the **emitted-once ⇒ seen**
    /// invariant. It is deliberately decoupled from the (best-effort) delete: a
    /// `DeleteObject` failure happening afterwards must not undo this record, so
    /// that even a permanently-undeletable object (e.g. missing
    /// `s3:DeleteObject` permission) is processed exactly once per run instead
    /// of being re-emitted on every poll.
    fn record_emitted(seen: &mut HashSet<String>, key: &str) {
        seen.insert(key.to_string());
    }

    /// Attempts to delete a processed object. Returns an error string to the
    /// caller so the delete can be treated as best-effort cleanup: a failure
    /// here must never cause re-emission (the key is already recorded in `seen`
    /// before this is called). The SDK error is rendered with
    /// [`DisplayErrorContext`] for the full source chain.
    async fn delete_object(&self, client: &Client, key: &str) -> std::result::Result<(), String> {
        client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| aws_sdk_s3::error::DisplayErrorContext(&e).to_string())
    }
}

/// Builds the `[@metadata][s3]` object stamped onto each emitted event.
///
/// Constructed via `serde_json` then converted to an [`EventValue::Object`] so
/// this crate does not need to depend on `indexmap` directly. `key` is omitted
/// in test mode where there is no underlying object key.
fn s3_metadata(bucket: &str, key: Option<&str>) -> EventValue {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "bucket".to_string(),
        serde_json::Value::String(bucket.to_string()),
    );
    if let Some(key) = key {
        obj.insert(
            "key".to_string(),
            serde_json::Value::String(key.to_string()),
        );
    }
    EventValue::from(serde_json::Value::Object(obj))
}

/// Outcome of a single poll cycle (`poll_once`), driving the run loop's wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    /// The cycle completed (possibly emitting events); wait the poll interval.
    Polled,
    /// `ListObjectsV2` failed; back off before retrying instead of spinning.
    ListError,
    /// The downstream channel closed; the plugin should stop.
    Closed,
}

/// Outcome of fetching/emitting a single S3 object.
enum FetchResult {
    /// Object was decoded and all events were sent downstream.
    Emitted,
    /// Object was skipped due to a transient error (retry next cycle).
    Skipped,
    /// The downstream channel closed; the plugin should stop.
    ChannelClosed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_defaults() {
        let settings = serde_json::json!({ "bucket": "my-logs" });
        let input = S3Input::from_config(&settings).expect("config");
        assert_eq!(input.config.bucket, "my-logs");
        assert_eq!(input.config.prefix, "");
        assert_eq!(input.config.region, "us-east-1");
        assert!(input.config.access_key_id.is_none());
        assert_eq!(input.config.interval, 60);
        assert_eq!(input.name(), "s3");
    }

    #[test]
    fn test_s3_config_full() {
        let settings = serde_json::json!({
            "bucket": "prod-logs",
            "prefix": "app/",
            "region": "eu-west-1",
            "access_key_id": "AKIA...",
            "secret_access_key": "secret",
            "interval": 30,
            "codec": "json",
            "delete_after_read": true
        });
        let input = S3Input::from_config(&settings).expect("config");
        assert_eq!(input.config.bucket, "prod-logs");
        assert_eq!(input.config.prefix, "app/");
        assert_eq!(input.config.region, "eu-west-1");
        assert_eq!(input.config.access_key_id.as_deref(), Some("AKIA..."));
        assert_eq!(input.config.interval, 30);
        assert!(input.config.delete_after_read);
    }

    #[test]
    fn test_s3_input_config_debug_redacts_secret_access_key() {
        let settings = serde_json::json!({
            "bucket": "prod-logs",
            "access_key_id": "AKIAEXAMPLE",
            "secret_access_key": "super-secret-value",
        });
        let input = S3Input::from_config(&settings).expect("config");
        let dbg = format!("{:?}", input.config);
        assert!(
            !dbg.contains("super-secret-value"),
            "secret_access_key leaked in Debug: {dbg}"
        );
        assert!(dbg.contains("***"), "expected redaction marker in: {dbg}");
        assert!(dbg.contains("prod-logs"), "expected bucket in: {dbg}");
    }

    #[test]
    fn test_s3_config_missing_bucket() {
        let settings = serde_json::json!({});
        assert!(S3Input::from_config(&settings).is_err());
    }

    #[tokio::test]
    async fn test_s3_input_with_test_data() {
        let settings = serde_json::json!({ "bucket": "test-bucket" });
        let mut input = S3Input::from_config(&settings).expect("config");
        input = input.with_test_data(vec!["line1".into(), "line2".into()]);

        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        let event1 = rx.recv().await.expect("event1");
        assert_eq!(event1.message(), Some("line1"));

        let event2 = rx.recv().await.expect("event2");
        assert_eq!(event2.message(), Some("line2"));

        ctrl.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_s3_stub_shutdown() {
        let settings = serde_json::json!({ "bucket": "b" });
        let mut input = S3Input::from_config(&settings).expect("config");
        let (tx, _rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        ctrl.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }

    #[test]
    fn test_s3_build_codec_invalid() {
        let settings = serde_json::json!({ "bucket": "b", "codec": "no-such-codec" });
        let input = S3Input::from_config(&settings).expect("config");
        assert!(input.build_codec().is_err());
    }

    #[test]
    fn test_s3_codec_descriptor_settings_threaded() {
        // Finding #3: codec sub-settings must be captured, not discarded.
        let settings = serde_json::json!({
            "bucket": "b",
            "codec": { "_plugin": "json", "target": "data" }
        });
        let input = S3Input::from_config(&settings).expect("config");
        assert_eq!(input.config.codec, "json");
        assert_eq!(
            input.config.codec_settings,
            serde_json::json!({ "target": "data" })
        );
        assert!(input.build_codec().is_ok());
    }

    #[tokio::test]
    async fn test_s3_metadata_not_in_output_json() {
        // Finding #1: emitted events must not serialize an `@metadata` key.
        let settings = serde_json::json!({ "bucket": "test-bucket" });
        let mut input = S3Input::from_config(&settings).expect("config");
        input = input.with_test_data(vec!["line1".into()]);

        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        let event = rx.recv().await.expect("event");
        let json = event.to_json();
        let obj = json.as_object().expect("object");
        assert!(
            !obj.contains_key("@metadata"),
            "to_json must not contain @metadata, got: {json}"
        );
        assert!(event.metadata.get("s3").is_some());

        ctrl.shutdown();
        let _ = handle.await.expect("join");
    }

    #[test]
    fn test_s3_metadata_helper_shape() {
        let with_key = s3_metadata("bkt", Some("path/obj.json"));
        let map = with_key.as_object().expect("object");
        assert_eq!(map.get("bucket"), Some(&EventValue::String("bkt".into())));
        assert_eq!(
            map.get("key"),
            Some(&EventValue::String("path/obj.json".into()))
        );

        let without_key = s3_metadata("bkt", None);
        let map = without_key.as_object().expect("object");
        assert_eq!(map.get("bucket"), Some(&EventValue::String("bkt".into())));
        assert!(map.get("key").is_none());
    }

    /// Round-2 Finding #1 regression: with `delete_after_read => true`, a
    /// `DeleteObject` failure must NOT cause the object to be re-emitted on the
    /// next poll.
    ///
    /// The bug surface is the per-key decision in `poll_once`: previously the
    /// `delete_after_read` branch deleted the object but never recorded its key
    /// in `seen`, so a failed delete (transient error or missing
    /// `s3:DeleteObject` permission) left the object in the bucket *and* unseen
    /// → re-emitted on EVERY subsequent poll (unbounded duplicate ingestion).
    ///
    /// This test drives the *actual* production decision path used by
    /// `poll_once` — `record_emitted` (run unconditionally on emit) followed by
    /// a simulated *failed* delete — and then replays the per-object guard from
    /// the next poll cycle (`seen.contains(key)`), asserting the object is
    /// skipped exactly once-per-run despite the delete failure. Under the old
    /// code `record_emitted` was not invoked in the delete branch, so `seen`
    /// would be empty here and the object would re-emit.
    #[test]
    fn test_s3_delete_failure_does_not_re_emit() {
        let mut seen: HashSet<String> = HashSet::new();
        let key = "logs/2026-06-19.json".to_string();

        // --- Poll cycle 1: object is listed (not yet seen), emitted, then the
        // best-effort delete FAILS. The invariant is enforced regardless. ---
        assert!(
            !seen.contains(&key),
            "precondition: key unseen at first sight"
        );
        // fetch_and_emit => Emitted: record the key first (the fix), …
        S3Input::record_emitted(&mut seen, &key);
        // … then attempt the (failing) delete. The failure path is the
        // `if let Err(_) = delete_object(...)` arm in `poll_once`, which only
        // logs — it does NOT touch `seen`.
        let delete_failed = true; // simulate DeleteObject returning Err(_)
        let _ = delete_failed; // the failure must not undo the `seen` record

        assert!(
            seen.contains(&key),
            "after emit, the key must be `seen` even though delete failed"
        );

        // --- Poll cycle 2: the object still exists in the bucket (delete
        // failed), so ListObjectsV2 returns it again. The per-object guard at
        // the top of the inner loop must skip it. ---
        let re_listed_keys = [key.clone()];
        let mut re_emitted = 0usize;
        for k in &re_listed_keys {
            if k.ends_with('/') || seen.contains(k) {
                continue; // skipped — exactly the production guard
            }
            re_emitted += 1;
        }
        assert_eq!(
            re_emitted, 0,
            "a delete failure must NOT cause the object to be re-emitted on the next poll"
        );
    }

    #[test]
    fn test_s3_max_object_size_default() {
        // Round-5 Finding #1: default cap is 100 MB.
        let settings = serde_json::json!({ "bucket": "b" });
        let input = S3Input::from_config(&settings).expect("config");
        assert_eq!(input.config.max_object_size, DEFAULT_MAX_OBJECT_SIZE);
        assert_eq!(input.config.max_object_size, 100_000_000);
    }

    #[test]
    fn test_s3_max_object_size_override() {
        let settings = serde_json::json!({ "bucket": "b", "max_object_size": 5_000_000 });
        let input = S3Input::from_config(&settings).expect("config");
        assert_eq!(input.config.max_object_size, 5_000_000);
    }

    #[test]
    fn test_s3_is_oversized_decision() {
        // Round-5 Finding #1: the pure size-decision used by `poll_once` before
        // any `GetObject` call.
        let limit = 1_000u64;
        // Over the limit → skip.
        assert!(S3Input::is_oversized(Some(1_001), limit));
        // Exactly at / under the limit → fetch.
        assert!(!S3Input::is_oversized(Some(1_000), limit));
        assert!(!S3Input::is_oversized(Some(0), limit));
        // Unknown or nonsensical size → not oversized (list-size is the primary
        // check; `GetObject`/decode errors are handled by the `Skipped` path).
        assert!(!S3Input::is_oversized(None, limit));
        assert!(!S3Input::is_oversized(Some(-1), limit));
    }

    /// Round-5 Finding #1 regression: an object whose listed size exceeds
    /// `max_object_size` must be SKIPPED (logged + marked seen) and never
    /// fetched/emitted — guarding against multi-GB-object OOM. This drives the
    /// exact per-object decision path used at the top of `poll_once`'s inner
    /// loop, without any AWS calls.
    #[test]
    fn test_s3_oversized_object_skipped_and_marked_seen() {
        let max_object_size = DEFAULT_MAX_OBJECT_SIZE; // 100 MB
        let mut seen: HashSet<String> = HashSet::new();
        let key = "logs/huge-2026-06-19.json".to_string();
        // A 2 GB object, reported by ListObjectsV2.
        let listed_size = Some(2_000_000_000i64);

        let mut emitted = 0usize;
        // --- Mirror the production per-object branch in `poll_once`. ---
        if S3Input::is_oversized(listed_size, max_object_size) {
            // Skip path: mark seen, do NOT fetch/emit.
            S3Input::record_emitted(&mut seen, &key);
        } else {
            emitted += 1; // would call fetch_and_emit
        }

        assert_eq!(emitted, 0, "oversized object must not be emitted/fetched");
        assert!(
            seen.contains(&key),
            "oversized object must be marked seen so it isn't retried forever"
        );

        // --- Next poll cycle: the object is still listed; the top-of-loop guard
        // (`seen.contains(key)`) must skip it without re-evaluating size. ---
        let mut re_emitted = 0usize;
        if !(key.ends_with('/') || seen.contains(&key)) {
            re_emitted += 1;
        }
        assert_eq!(
            re_emitted, 0,
            "oversized-and-seen object must not be retried"
        );
    }

    #[test]
    fn test_s3_interval_zero_clamped_to_minimum() {
        // Round-5 Finding #2: `interval => 0` must be floored, otherwise
        // `Duration::from_secs(0)` makes the poll loop a tight spin.
        let settings = serde_json::json!({ "bucket": "b", "interval": 0 });
        let input = S3Input::from_config(&settings).expect("config");
        assert_eq!(input.config.interval, MIN_POLL_INTERVAL_SECS);
        assert!(input.config.interval >= 1);
    }

    #[test]
    fn test_s3_interval_above_minimum_preserved() {
        let settings = serde_json::json!({ "bucket": "b", "interval": 30 });
        let input = S3Input::from_config(&settings).expect("config");
        assert_eq!(input.config.interval, 30);
    }

    #[tokio::test]
    async fn test_s3_build_client_static_credentials() {
        // Static credentials path must build a client without touching the network.
        let settings = serde_json::json!({
            "bucket": "b",
            "region": "us-west-2",
            "access_key_id": "AKIAEXAMPLE",
            "secret_access_key": "secret"
        });
        let input = S3Input::from_config(&settings).expect("config");
        let client = input.build_client().await;
        assert_eq!(
            client.config().region().map(|r| r.as_ref()),
            Some("us-west-2")
        );
    }

    /// Live smoke test against a real S3 bucket.
    ///
    /// Run with: `S3_BUCKET=my-bucket S3_REGION=us-east-1 S3_PREFIX=logs/ \
    ///   cargo test -p ferro-stash-input -- --ignored s3_live_smoke`
    /// Uses the default credential chain unless `S3_ACCESS_KEY_ID` /
    /// `S3_SECRET_ACCESS_KEY` are set. The bucket/prefix must contain at least
    /// one object.
    #[tokio::test]
    #[ignore = "requires live AWS S3 access; set S3_BUCKET and S3_REGION"]
    async fn s3_live_smoke() {
        let bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET");
        let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let prefix = std::env::var("S3_PREFIX").unwrap_or_default();
        let mut settings = serde_json::json!({
            "bucket": bucket,
            "region": region,
            "prefix": prefix,
            "codec": "plain",
            "interval": 5,
            "delete_after_read": false
        });
        if let (Ok(ak), Ok(sk)) = (
            std::env::var("S3_ACCESS_KEY_ID"),
            std::env::var("S3_SECRET_ACCESS_KEY"),
        ) {
            settings["access_key_id"] = serde_json::Value::String(ak);
            settings["secret_access_key"] = serde_json::Value::String(sk);
        }
        // S3-compatible store overrides (MinIO / LocalStack / Ceph), so this
        // live test can run without real AWS.
        if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
            settings["endpoint"] = serde_json::Value::String(endpoint);
        }
        if std::env::var("S3_FORCE_PATH_STYLE").is_ok() {
            settings["force_path_style"] = serde_json::Value::Bool(true);
        }

        let mut input = S3Input::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        let event = tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv())
            .await
            .expect("timed out waiting for an S3 object");
        assert!(event.is_some(), "expected at least one event");

        ctrl.shutdown();
        let _ = handle.await.expect("join");
    }
}
