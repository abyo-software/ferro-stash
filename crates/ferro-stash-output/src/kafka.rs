// SPDX-License-Identifier: Apache-2.0
//! Kafka output plugin — produces messages to Apache Kafka topics.
//!
//! Production-shaped stub: config parsing, batching, and the `OutputPlugin` trait are
//! fully wired. The actual Kafka producer is stubbed. Replace with `rdkafka` producer
//! for production use.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tracing::{info, warn};

/// Kafka compression type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    fn from_str_config(s: &str) -> Self {
        match s {
            "gzip" => Self::Gzip,
            "snappy" => Self::Snappy,
            "lz4" => Self::Lz4,
            "zstd" => Self::Zstd,
            _ => Self::None,
        }
    }
}

/// Kafka output configuration — mirrors the Logstash kafka output settings.
#[derive(Debug, Clone)]
pub struct KafkaOutputConfig {
    pub bootstrap_servers: Vec<String>,
    pub topic: String,
    pub key: Option<String>,
    pub codec: String,
    pub compression_type: CompressionType,
    pub batch_size: usize,
    pub client_id: String,
    pub acks: String,
    pub retries: usize,
}

#[derive(Debug)]
pub struct KafkaOutput {
    config: KafkaOutputConfig,
    condition: Option<Condition>,
}

impl KafkaOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let bootstrap_servers = settings
            .get("bootstrap_servers")
            .and_then(|v| v.as_str())
            .unwrap_or("localhost:9092")
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let topic = settings
            .get_string("topic")
            .ok_or_else(|| FerroStashError::Output {
                plugin: "kafka".to_string(),
                message: "topic is required".to_string(),
            })?;

        let key = settings.get_string("key");
        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "plain".to_string());
        let compression_type = settings
            .get("compression_type")
            .and_then(|v| v.as_str())
            .map_or(CompressionType::None, CompressionType::from_str_config);
        let batch_size = settings.get_u64("batch_size").unwrap_or(16384) as usize;
        let client_id = settings
            .get_string("client_id")
            .unwrap_or_else(|| "ferro-stash-kafka-output".to_string());
        let acks = settings
            .get_string("acks")
            .unwrap_or_else(|| "1".to_string());
        let retries = settings.get_u64("retries").unwrap_or(3) as usize;

        Ok(Self {
            config: KafkaOutputConfig {
                bootstrap_servers,
                topic,
                key,
                codec,
                compression_type,
                batch_size,
                client_id,
                acks,
                retries,
            },
            condition,
        })
    }
}

#[async_trait]
impl OutputPlugin for KafkaOutput {
    fn name(&self) -> &'static str {
        "kafka"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        warn!("Kafka output plugin: using stub implementation — configure real Kafka connection for production");

        info!(
            topic = %self.config.topic,
            event_count = events.len(),
            "Kafka output: would produce {} events to topic '{}'",
            events.len(),
            self.config.topic,
        );

        // Production implementation would:
        // 1. Serialize events using the configured codec
        // 2. Optionally extract the key from each event via self.config.key field reference
        // 3. Batch produce to self.config.topic
        // 4. Apply compression_type, acks, retries settings
        // 5. Handle producer errors with retry logic

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
    fn test_kafka_output_config_defaults() {
        let settings = serde_json::json!({ "topic": "output-topic" });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.topic, "output-topic");
        assert_eq!(output.config.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(output.config.compression_type, CompressionType::None);
        assert_eq!(output.config.batch_size, 16384);
        assert_eq!(output.name(), "kafka");
    }

    #[test]
    fn test_kafka_output_config_full() {
        let settings = serde_json::json!({
            "bootstrap_servers": "b1:9092,b2:9092",
            "topic": "events",
            "key": "%{[type]}",
            "codec": "json",
            "compression_type": "snappy",
            "batch_size": 32768,
            "acks": "all",
            "retries": 5
        });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.bootstrap_servers, vec!["b1:9092", "b2:9092"]);
        assert_eq!(output.config.key.as_deref(), Some("%{[type]}"));
        assert_eq!(output.config.compression_type, CompressionType::Snappy);
        assert_eq!(output.config.acks, "all");
        assert_eq!(output.config.retries, 5);
    }

    #[test]
    fn test_kafka_output_missing_topic() {
        let settings = serde_json::json!({});
        assert!(KafkaOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_kafka_output_compression_types() {
        for (s, expected) in [
            ("gzip", CompressionType::Gzip),
            ("snappy", CompressionType::Snappy),
            ("lz4", CompressionType::Lz4),
            ("zstd", CompressionType::Zstd),
            ("none", CompressionType::None),
        ] {
            let settings = serde_json::json!({ "topic": "t", "compression_type": s });
            let output = KafkaOutput::from_config(&settings, None).expect("config");
            assert_eq!(output.config.compression_type, expected);
        }
    }

    #[tokio::test]
    async fn test_kafka_output_stub_succeeds() {
        let settings = serde_json::json!({ "topic": "test" });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        let events = vec![Event::new("test event")];
        let result = output.output(events).await;
        assert!(result.is_ok());
    }
}
