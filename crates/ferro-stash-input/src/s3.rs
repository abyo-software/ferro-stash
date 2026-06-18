// SPDX-License-Identifier: Apache-2.0
//! S3 input plugin — polls an S3 bucket for new objects and emits their contents as events.
//!
//! Production-shaped stub: config parsing, polling loop skeleton, and the `InputPlugin` trait
//! are fully wired. The actual S3 API calls are stubbed (no AWS SDK dependency). Replace
//! `list_and_fetch_stub` with real S3 SDK calls to make this production-ready.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tracing::{info, warn};

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
        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "plain".to_string());
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
                event.set(
                    "[@metadata][s3][bucket]",
                    EventValue::String(self.config.bucket.clone()),
                );
                if sender.send(event).await.is_err() {
                    return Ok(());
                }
            }
            // Wait for shutdown after test data is drained.
            shutdown.wait().await;
            return Ok(());
        }

        // --- Stub: real S3 polling loop ---
        warn!("S3 input plugin: using stub implementation — configure real S3 connection for production");

        let poll_interval = tokio::time::Duration::from_secs(self.config.interval);
        loop {
            tokio::select! {
                () = tokio::time::sleep(poll_interval) => {
                    // Stub: in production, list objects with prefix, fetch new ones,
                    // decode via codec, emit events, optionally delete processed objects.
                }
                () = shutdown.wait() => {
                    info!("S3 input shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
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
}
