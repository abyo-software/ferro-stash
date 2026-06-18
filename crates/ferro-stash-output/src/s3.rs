// SPDX-License-Identifier: Apache-2.0
//! S3 output plugin — uploads events to an S3 bucket with file rotation.
//!
//! Production-shaped stub: config parsing, buffering, file rotation logic, and the
//! `OutputPlugin` trait are fully wired. The actual S3 upload is stubbed. Replace
//! `upload_stub` with real S3 SDK calls for production.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tracing::{info, warn};

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
            },
            condition,
            buffer: Mutex::new(Vec::new()),
            current_bytes: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
        })
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
        warn!("S3 output plugin: using stub implementation — configure real S3 connection for production");

        let mut buf =
            self.buffer
                .lock()
                .map_err(|e| ferro_stash_core::error::FerroStashError::Output {
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

            info!(
                bucket = %self.config.bucket,
                key = %key,
                lines = payload.len(),
                "S3 output: would upload {} lines to s3://{}/{}",
                payload.len(),
                self.config.bucket,
                key,
            );

            // Production: upload `payload.join("\n")` to S3 with configured encoding
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut buf =
            self.buffer
                .lock()
                .map_err(|e| ferro_stash_core::error::FerroStashError::Output {
                    plugin: "s3".to_string(),
                    message: format!("buffer lock poisoned: {e}"),
                })?;

        if buf.is_empty() {
            return Ok(());
        }

        let key = self.generate_key();
        let payload = std::mem::take(&mut *buf);
        self.current_bytes.store(0, Ordering::Relaxed);

        info!(
            bucket = %self.config.bucket,
            key = %key,
            lines = payload.len(),
            "S3 output flush: would upload {} lines",
            payload.len(),
        );

        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn test_s3_output_flush() {
        let settings = serde_json::json!({ "bucket": "test" });
        let output = S3Output::from_config(&settings, None).expect("config");

        let events = vec![Event::new("flush-me")];
        output.output(events).await.expect("output");
        output.flush().await.expect("flush");

        let buf = output.buffer.lock().expect("lock");
        assert!(buf.is_empty());
    }

    #[test]
    fn test_s3_output_key_generation() {
        let settings = serde_json::json!({ "bucket": "b", "prefix": "p/" });
        let output = S3Output::from_config(&settings, None).expect("config");
        let key = output.generate_key();
        assert!(key.starts_with("p/"));
        assert!(key.ends_with(".log"));
    }
}
