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
use ferro_stash_codec::{create_codec, Codec};
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// S3 input configuration — mirrors the Logstash S3 input settings.
#[derive(Debug, Clone)]
pub struct S3InputConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub interval: u64,
    pub codec: String,
    /// Codec sub-settings extracted from the `codec => name { ... }` block;
    /// empty object when the codec is named without a settings block.
    pub codec_settings: serde_json::Value,
    pub delete_after_read: bool,
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
        let interval = settings.get_u64("interval").unwrap_or(60);
        // Resolve the codec name and its sub-settings so they are honored rather
        // than dropped.
        let (codec, codec_settings) = crate::codec_config::resolve_codec(settings, "plain");
        let delete_after_read = settings.get_bool("delete_after_read").unwrap_or(false);

        Ok(Self {
            config: S3InputConfig {
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                interval,
                codec,
                codec_settings,
                delete_after_read,
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

        match (&self.config.access_key_id, &self.config.secret_access_key) {
            (Some(ak), Some(sk)) => {
                let creds = Credentials::new(
                    ak.clone(),
                    sk.clone(),
                    None,
                    None,
                    "ferro-stash-s3-input",
                );
                let conf = aws_sdk_s3::config::Builder::new()
                    .region(region)
                    .credentials_provider(creds)
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build();
                Client::from_conf(conf)
            }
            _ => {
                let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(region)
                    .load()
                    .await;
                Client::new(&sdk_config)
            }
        }
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
            // One poll cycle. On a downstream-closed signal, stop entirely.
            if self
                .poll_once(&client, codec.as_ref(), &sender, &mut seen)
                .await
            {
                info!("S3 input: downstream channel closed, stopping");
                break;
            }

            tokio::select! {
                () = tokio::time::sleep(poll_interval) => {}
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
    /// optionally deletes it. Returns `true` if the downstream channel closed
    /// (caller should stop).
    async fn poll_once(
        &self,
        client: &Client,
        codec: &dyn Codec,
        sender: &mpsc::Sender<Event>,
        seen: &mut HashSet<String>,
    ) -> bool {
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
                    return false;
                }
            };

            for object in resp.contents() {
                let Some(key) = object.key() else { continue };
                // Skip "directory" placeholder keys and already-seen keys.
                if key.ends_with('/') || seen.contains(key) {
                    continue;
                }
                let key = key.to_string();

                match self.fetch_and_emit(client, codec, sender, &key).await {
                    FetchResult::Emitted => {
                        if self.config.delete_after_read {
                            self.delete_object(client, &key).await;
                            // Deleted objects won't reappear, so no need to track.
                        } else {
                            seen.insert(key);
                        }
                    }
                    FetchResult::Skipped => {
                        // Transient fetch/decode error — leave unseen to retry next cycle.
                    }
                    FetchResult::ChannelClosed => return true,
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

        false
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
            event
                .metadata
                .set("s3".to_string(), s3_metadata(&self.config.bucket, Some(key)));
            if sender.send(event).await.is_err() {
                return FetchResult::ChannelClosed;
            }
        }

        FetchResult::Emitted
    }

    /// Deletes a processed object, logging (but not failing) on error.
    async fn delete_object(&self, client: &Client, key: &str) {
        if let Err(e) = client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            warn!(bucket = %self.config.bucket, key = %key, error = %e, "S3 DeleteObject failed");
        }
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
        obj.insert("key".to_string(), serde_json::Value::String(key.to_string()));
    }
    EventValue::from(serde_json::Value::Object(obj))
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
