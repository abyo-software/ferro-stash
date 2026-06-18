// SPDX-License-Identifier: Apache-2.0
//! Kafka input plugin — consumes messages from Apache Kafka topics.
//!
//! This is a production-shaped stub: config parsing, trait implementation, and the
//! consumer-loop skeleton are fully wired. The actual Kafka connection is stubbed
//! (no external kafka crate) and can be swapped for `rdkafka` or a pure-Rust client
//! by replacing the inner `consume_stub` method.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Kafka consumer configuration — mirrors the Logstash kafka input settings.
#[derive(Debug, Clone)]
pub struct KafkaInputConfig {
    pub bootstrap_servers: Vec<String>,
    pub topics: Vec<String>,
    pub group_id: String,
    pub auto_offset_reset: AutoOffsetReset,
    pub consumer_threads: usize,
    pub codec: String,
    pub client_id: String,
    pub max_poll_records: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
}

impl std::fmt::Display for AutoOffsetReset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Earliest => write!(f, "earliest"),
            Self::Latest => write!(f, "latest"),
        }
    }
}

#[derive(Debug)]
pub struct KafkaInput {
    config: KafkaInputConfig,
    /// Channel-based test data injection point — production code replaces this with
    /// a real Kafka consumer.
    test_receiver: Option<mpsc::Receiver<String>>,
}

impl KafkaInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let bootstrap_servers = settings
            .get("bootstrap_servers")
            .and_then(|v| v.as_str())
            .unwrap_or("localhost:9092")
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let topics: Vec<String> = match settings.get("topics") {
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
            Some(serde_json::Value::String(s)) => {
                s.split(',').map(|t| t.trim().to_string()).collect()
            }
            _ => {
                return Err(FerroStashError::Input {
                    plugin: "kafka".to_string(),
                    message: "topics is required (array or comma-separated string)".to_string(),
                });
            }
        };

        if topics.is_empty() {
            return Err(FerroStashError::Input {
                plugin: "kafka".to_string(),
                message: "at least one topic is required".to_string(),
            });
        }

        let group_id = settings
            .get_string("group_id")
            .unwrap_or_else(|| "ferro-stash".to_string());

        let auto_offset_reset = match settings
            .get("auto_offset_reset")
            .and_then(|v| v.as_str())
            .unwrap_or("latest")
        {
            "earliest" | "beginning" | "smallest" => AutoOffsetReset::Earliest,
            _ => AutoOffsetReset::Latest,
        };

        let consumer_threads = settings.get_u64("consumer_threads").unwrap_or(1) as usize;
        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "plain".to_string());
        let client_id = settings
            .get_string("client_id")
            .unwrap_or_else(|| "ferro-stash-kafka-input".to_string());
        let max_poll_records = settings.get_u64("max_poll_records").unwrap_or(500) as usize;

        Ok(Self {
            config: KafkaInputConfig {
                bootstrap_servers,
                topics,
                group_id,
                auto_offset_reset,
                consumer_threads,
                codec,
                client_id,
                max_poll_records,
            },
            test_receiver: None,
        })
    }

    /// Inject a channel receiver for testing — messages on this channel are emitted as events.
    pub fn with_test_receiver(mut self, rx: mpsc::Receiver<String>) -> Self {
        self.test_receiver = Some(rx);
        self
    }
}

#[async_trait]
impl InputPlugin for KafkaInput {
    fn name(&self) -> &'static str {
        "kafka"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        info!(
            topics = ?self.config.topics,
            group_id = %self.config.group_id,
            bootstrap_servers = ?self.config.bootstrap_servers,
            "Kafka input starting"
        );

        // If we have a test receiver, drain it (for testing / demo).
        // In production, replace this branch with rdkafka consumer poll loop.
        if let Some(ref mut rx) = self.test_receiver {
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(payload) => {
                                let mut event = Event::new(&payload);
                                event.set("[@metadata][kafka][topic]",
                                    EventValue::String(self.config.topics.first()
                                        .cloned().unwrap_or_default()));
                                if sender.send(event).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    () = shutdown.wait() => {
                        info!("Kafka input shutting down (test mode)");
                        break;
                    }
                }
            }
            return Ok(());
        }

        // --- Stub: real Kafka consumer would go here ---
        warn!("Kafka input plugin: using stub implementation — configure real Kafka connection for production");

        // Wait for shutdown (stub does not produce events).
        shutdown.wait().await;
        info!("Kafka input shutting down");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config_defaults() {
        let settings = serde_json::json!({ "topics": ["test-topic"] });
        let input = KafkaInput::from_config(&settings).expect("config");
        assert_eq!(input.config.topics, vec!["test-topic"]);
        assert_eq!(input.config.group_id, "ferro-stash");
        assert_eq!(input.config.auto_offset_reset, AutoOffsetReset::Latest);
        assert_eq!(input.config.consumer_threads, 1);
        assert_eq!(input.config.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(input.name(), "kafka");
    }

    #[test]
    fn test_kafka_config_full() {
        let settings = serde_json::json!({
            "bootstrap_servers": "broker1:9092,broker2:9092",
            "topics": ["topic-a", "topic-b"],
            "group_id": "my-group",
            "auto_offset_reset": "earliest",
            "consumer_threads": 4,
            "codec": "json"
        });
        let input = KafkaInput::from_config(&settings).expect("config");
        assert_eq!(
            input.config.bootstrap_servers,
            vec!["broker1:9092", "broker2:9092"]
        );
        assert_eq!(input.config.topics, vec!["topic-a", "topic-b"]);
        assert_eq!(input.config.group_id, "my-group");
        assert_eq!(input.config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert_eq!(input.config.consumer_threads, 4);
    }

    #[test]
    fn test_kafka_config_missing_topics() {
        let settings = serde_json::json!({});
        assert!(KafkaInput::from_config(&settings).is_err());
    }

    #[test]
    fn test_kafka_config_comma_separated_topics() {
        let settings = serde_json::json!({ "topics": "a,b,c" });
        let input = KafkaInput::from_config(&settings).expect("config");
        assert_eq!(input.config.topics, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn test_kafka_input_with_test_channel() {
        let settings = serde_json::json!({ "topics": ["test"] });
        let mut input = KafkaInput::from_config(&settings).expect("config");

        let (test_tx, test_rx) = mpsc::channel(10);
        input = input.with_test_receiver(test_rx);

        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        test_tx.send("hello kafka".to_string()).await.expect("send");
        test_tx.send("second msg".to_string()).await.expect("send");
        drop(test_tx);

        let event1 = rx.recv().await.expect("event1");
        assert_eq!(event1.message(), Some("hello kafka"));

        let event2 = rx.recv().await.expect("event2");
        assert_eq!(event2.message(), Some("second msg"));

        ctrl.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_kafka_stub_shutdown() {
        let settings = serde_json::json!({ "topics": ["test"] });
        let mut input = KafkaInput::from_config(&settings).expect("config");
        let (tx, _rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        // Immediately shut down — the stub should exit cleanly
        ctrl.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }
}
