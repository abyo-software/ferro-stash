// SPDX-License-Identifier: Apache-2.0
//! Kafka output plugin — produces messages to Apache Kafka topics.
//!
//! Uses the `rdkafka` `FutureProducer`. Events are serialized via the configured
//! codec, optionally keyed via a Logstash field reference, and produced to the
//! configured topic with compression/acks/retries applied at the librdkafka level.

use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_codec::{create_codec, Codec};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use tokio::sync::OnceCell;
use tracing::{debug, info};

/// How long `send()` blocks if the librdkafka queue is full before erroring.
const KAFKA_QUEUE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for outstanding deliveries on flush/close.
const KAFKA_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

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

    /// The librdkafka `compression.type` config value.
    fn rdkafka_value(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
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

pub struct KafkaOutput {
    config: KafkaOutputConfig,
    condition: Option<Condition>,
    /// Codec used to serialize each event payload.
    codec: Box<dyn Codec>,
    /// Lazily-built producer (creation can fail; deferred out of `from_config`).
    producer: OnceCell<FutureProducer>,
}

impl std::fmt::Debug for KafkaOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaOutput")
            .field("config", &self.config)
            .field("condition", &self.condition)
            .field("codec", &self.codec)
            .field("producer_built", &self.producer.get().is_some())
            .finish()
    }
}

impl KafkaOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        // `bootstrap_servers` accepts both the array form
        // (`["b1:9092", "b2:9092"]`) and a comma-separated string
        // (`"b1:9092,b2:9092"`). An empty/whitespace-only value is a config error
        // rather than a silent fall-back to localhost.
        let bootstrap_servers: Vec<String> = match settings.get("bootstrap_servers") {
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            Some(serde_json::Value::String(s)) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            // Not configured at all => default broker (preserves prior behavior).
            None => vec!["localhost:9092".to_string()],
            // Any other JSON type is a misconfiguration.
            Some(_) => {
                return Err(FerroStashError::Output {
                    plugin: "kafka".to_string(),
                    message: "bootstrap_servers must be a string or array of strings".to_string(),
                })
            }
        };
        if bootstrap_servers.is_empty() {
            return Err(FerroStashError::Output {
                plugin: "kafka".to_string(),
                message: "bootstrap_servers is empty".to_string(),
            });
        }

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

        // Build the codec used to serialize event payloads (config error => fail loud).
        let codec_impl = create_codec(&codec, settings)?;

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
            codec: codec_impl,
            producer: OnceCell::new(),
        })
    }

    /// Returns the producer, building it on first use.
    async fn producer(&self) -> Result<&FutureProducer> {
        self.producer
            .get_or_try_init(|| async {
                let mut cfg = ClientConfig::new();
                cfg.set("bootstrap.servers", self.config.bootstrap_servers.join(","))
                    .set("client.id", &self.config.client_id)
                    .set("acks", &self.config.acks)
                    .set(
                        "compression.type",
                        self.config.compression_type.rdkafka_value(),
                    )
                    .set(
                        "message.send.max.retries",
                        self.config.retries.to_string(),
                    )
                    // `batch_size` mirrors librdkafka's batch.size (bytes).
                    .set("batch.size", self.config.batch_size.to_string());

                cfg.create::<FutureProducer>()
                    .map_err(|e| FerroStashError::Output {
                        plugin: "kafka".to_string(),
                        message: format!("failed to create Kafka producer: {e}"),
                    })
            })
            .await
    }

    /// Serialize an event to bytes via the configured codec.
    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        self.codec.encode(event).map_err(|e| FerroStashError::Output {
            plugin: "kafka".to_string(),
            message: format!("codec encode error: {e}"),
        })
    }

    /// Resolve the partition key for an event from the configured field reference.
    /// Returns `None` when no key is configured or the reference resolves empty.
    fn resolve_key(&self, event: &Event) -> Option<String> {
        let template = self.config.key.as_ref()?;
        let resolved = event.sprintf(template);
        // An unresolved `%{field}` template or empty result yields no key.
        if resolved.is_empty() || resolved == *template {
            None
        } else {
            Some(resolved)
        }
    }
}

#[async_trait]
impl OutputPlugin for KafkaOutput {
    fn name(&self) -> &'static str {
        "kafka"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let producer = self.producer().await?;

        // Serialize payloads + keys up front (codec errors fail before producing).
        let mut records: Vec<(Vec<u8>, Option<String>)> = Vec::with_capacity(events.len());
        for event in &events {
            let payload = self.encode(event)?;
            let key = self.resolve_key(event);
            records.push((payload, key));
        }

        // Produce each record and await its delivery acknowledgement. librdkafka
        // batches/compresses internally and a background thread drives delivery,
        // so awaiting in order does not serialize the network round-trips.
        for (payload, key) in &records {
            let mut record: FutureRecord<'_, str, [u8]> =
                FutureRecord::to(&self.config.topic).payload(payload.as_slice());
            if let Some(k) = key {
                record = record.key(k.as_str());
            }
            producer
                .send(record, Timeout::After(KAFKA_QUEUE_TIMEOUT))
                .await
                .map_err(|(kafka_err, _msg)| FerroStashError::Output {
                    plugin: "kafka".to_string(),
                    message: format!("Kafka delivery failed: {kafka_err}"),
                })?;
        }

        debug!(
            topic = %self.config.topic,
            event_count = records.len(),
            "Kafka output: delivered events"
        );
        info!(
            topic = %self.config.topic,
            event_count = records.len(),
            "Kafka output: produced events"
        );

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        if let Some(producer) = self.producer.get() {
            producer
                .flush(Timeout::After(KAFKA_FLUSH_TIMEOUT))
                .map_err(|e| FerroStashError::Output {
                    plugin: "kafka".to_string(),
                    message: format!("Kafka flush failed: {e}"),
                })?;
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // Ensure all buffered messages are delivered before dropping the producer.
        self.flush().await
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
    fn test_kafka_bootstrap_servers_array_form() {
        // The array form must be parsed (previously silently fell back to localhost).
        let settings = serde_json::json!({
            "topic": "t",
            "bootstrap_servers": ["b1:9092", "b2:9092", " b3:9092 "],
        });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        assert_eq!(
            output.config.bootstrap_servers,
            vec!["b1:9092", "b2:9092", "b3:9092"]
        );
    }

    #[test]
    fn test_kafka_bootstrap_servers_empty_rejected() {
        // An empty string or empty array must error, not silently use localhost.
        let empty_str = serde_json::json!({ "topic": "t", "bootstrap_servers": "" });
        assert!(KafkaOutput::from_config(&empty_str, None).is_err());

        let empty_arr = serde_json::json!({ "topic": "t", "bootstrap_servers": [] });
        assert!(KafkaOutput::from_config(&empty_arr, None).is_err());

        let blanks = serde_json::json!({ "topic": "t", "bootstrap_servers": "  , ," });
        assert!(KafkaOutput::from_config(&blanks, None).is_err());
    }

    #[test]
    fn test_kafka_bootstrap_servers_default() {
        // Unconfigured => default localhost broker (prior behavior preserved).
        let settings = serde_json::json!({ "topic": "t" });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.bootstrap_servers, vec!["localhost:9092"]);
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

    #[test]
    fn test_kafka_output_codec_built() {
        // Unknown codec must fail loudly at config time.
        let settings = serde_json::json!({ "topic": "t", "codec": "no-such-codec" });
        assert!(KafkaOutput::from_config(&settings, None).is_err());
    }

    #[tokio::test]
    async fn test_kafka_output_empty_is_ok() {
        // Empty batches must not build a producer or connect.
        let settings = serde_json::json!({ "topic": "test" });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![]).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_resolve_key() {
        let settings = serde_json::json!({ "topic": "t", "key": "%{[user]}" });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("hi");
        event.set(
            "user",
            ferro_stash_core::event::EventValue::String("alice".into()),
        );
        assert_eq!(output.resolve_key(&event).as_deref(), Some("alice"));

        // Unresolved template yields no key.
        let no_field = Event::new("hi");
        assert!(output.resolve_key(&no_field).is_none());

        // No key configured at all.
        let settings2 = serde_json::json!({ "topic": "t" });
        let output2 = KafkaOutput::from_config(&settings2, None).expect("config");
        assert!(output2.resolve_key(&Event::new("x")).is_none());
    }

    #[test]
    fn test_kafka_compression_rdkafka_value() {
        assert_eq!(CompressionType::None.rdkafka_value(), "none");
        assert_eq!(CompressionType::Gzip.rdkafka_value(), "gzip");
        assert_eq!(CompressionType::Snappy.rdkafka_value(), "snappy");
        assert_eq!(CompressionType::Lz4.rdkafka_value(), "lz4");
        assert_eq!(CompressionType::Zstd.rdkafka_value(), "zstd");
    }

    /// Live smoke test against a real Kafka broker.
    /// Gated behind `KAFKA_BROKERS` (e.g. `localhost:9092`) and `KAFKA_TOPIC`
    /// (default `ferro-stash-live-test`). The topic must exist or auto-create
    /// must be enabled. Run with
    /// `cargo test -p ferro-stash-output -- --ignored kafka_live`.
    #[tokio::test]
    #[ignore = "requires a running Kafka broker (KAFKA_BROKERS env var)"]
    async fn kafka_live_smoke() {
        let brokers = std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS");
        let topic =
            std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "ferro-stash-live-test".to_string());
        let settings = serde_json::json!({
            "bootstrap_servers": brokers,
            "topic": topic,
            "codec": "json",
            "key": "%{[user]}",
        });
        let output = KafkaOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("kafka live smoke");
        event.set(
            "user",
            ferro_stash_core::event::EventValue::String("smoke".into()),
        );
        output.output(vec![event]).await.expect("live produce");
        output.flush().await.expect("flush");
    }
}
