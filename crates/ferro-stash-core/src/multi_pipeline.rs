// SPDX-License-Identifier: Apache-2.0
//! Multiple pipelines — run multiple independent pipelines simultaneously.
//!
//! Logstash 9.x compatible: pipelines.yml defines multiple pipeline configs.
//! Also supports pipeline-to-pipeline communication via virtual addresses.

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::error::{FerroStashError, Result};
use crate::event::Event;

/// Configuration for multiple pipelines (pipelines.yml).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PipelinesConfig {
    pub pipelines: Vec<PipelineEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PipelineEntry {
    #[serde(rename = "pipeline.id")]
    pub id: String,
    #[serde(rename = "path.config", default)]
    pub config_path: Option<String>,
    #[serde(rename = "config.string", default)]
    pub config_string: Option<String>,
    #[serde(rename = "pipeline.workers", default)]
    pub workers: Option<usize>,
    #[serde(rename = "pipeline.batch.size", default)]
    pub batch_size: Option<usize>,
    #[serde(rename = "queue.type", default)]
    pub queue_type: Option<String>,
}

/// Virtual address for pipeline-to-pipeline communication.
#[derive(Debug, Clone)]
pub struct PipelineBus {
    senders: HashMap<String, mpsc::Sender<Event>>,
}

impl PipelineBus {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    /// Register a pipeline's input channel.
    pub fn register(&mut self, address: String, sender: mpsc::Sender<Event>) {
        self.senders.insert(address, sender);
    }

    /// Send an event to a pipeline address.
    pub async fn send(&self, address: &str, event: Event) -> Result<()> {
        if let Some(tx) = self.senders.get(address) {
            tx.send(event).await.map_err(|_| {
                FerroStashError::Pipeline(format!("pipeline {address} channel closed"))
            })?;
        } else {
            return Err(FerroStashError::Pipeline(format!(
                "unknown pipeline address: {address}"
            )));
        }
        Ok(())
    }

    /// List registered addresses.
    pub fn addresses(&self) -> Vec<&String> {
        self.senders.keys().collect()
    }
}

impl Default for PipelineBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a pipelines.yml file.
pub fn parse_pipelines_config(content: &str) -> Result<PipelinesConfig> {
    // pipelines.yml is a YAML array of pipeline entries
    let entries: Vec<PipelineEntry> = serde_yaml::from_str(content)
        .map_err(|e| FerroStashError::Config(format!("pipelines.yml parse error: {e}")))?;
    Ok(PipelinesConfig { pipelines: entries })
}

/// Parse from file.
pub fn parse_pipelines_file(path: &str) -> Result<PipelinesConfig> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| FerroStashError::Config(format!("cannot read {path}: {e}")))?;
    parse_pipelines_config(&content)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pipelines_yml() {
        let yaml = r"
- pipeline.id: main
  path.config: /etc/ferrostash/main.conf
  pipeline.workers: 4
- pipeline.id: dlq
  path.config: /etc/ferrostash/dlq.conf
  pipeline.workers: 1
";
        let config = parse_pipelines_config(yaml).expect("parse");
        assert_eq!(config.pipelines.len(), 2);
        assert_eq!(config.pipelines[0].id, "main");
        assert_eq!(config.pipelines[1].id, "dlq");
    }

    #[test]
    fn test_parse_pipelines_config_string() {
        let yaml = r#"
- pipeline.id: inline
  config.string: "input { generator { count => 1 } } output { null { } }"
  pipeline.workers: 1
"#;
        let config = parse_pipelines_config(yaml).expect("parse");
        assert_eq!(config.pipelines.len(), 1);
        assert_eq!(config.pipelines[0].id, "inline");
        assert!(config.pipelines[0].config_string.is_some());
        assert!(config.pipelines[0].config_path.is_none());
    }

    #[test]
    fn test_pipeline_bus() {
        let mut bus = PipelineBus::new();
        let (tx, _rx) = mpsc::channel(10);
        bus.register("output-to-dlq".to_string(), tx);
        assert_eq!(bus.addresses().len(), 1);
    }

    #[tokio::test]
    async fn test_pipeline_bus_send() {
        let mut bus = PipelineBus::new();
        let (tx, mut rx) = mpsc::channel(10);
        bus.register("test-pipe".to_string(), tx);
        let event = Event::new("hello");
        bus.send("test-pipe", event).await.expect("send");
        let received = rx.recv().await.expect("recv");
        assert_eq!(received.message(), Some("hello"));
    }

    #[tokio::test]
    async fn test_pipeline_bus_send_unknown() {
        let bus = PipelineBus::new();
        let event = Event::new("test");
        let result = bus.send("nonexistent", event).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_bus_default() {
        let bus = PipelineBus::default();
        assert!(bus.addresses().is_empty());
    }

    #[test]
    fn test_parse_pipelines_config_with_options() {
        let yaml = r"
- pipeline.id: main
  path.config: /etc/ferrostash/main.conf
  pipeline.workers: 8
  pipeline.batch.size: 1000
  queue.type: persisted
";
        let config = parse_pipelines_config(yaml).expect("parse");
        assert_eq!(config.pipelines.len(), 1);
        assert_eq!(
            config.pipelines[0].config_path,
            Some("/etc/ferrostash/main.conf".to_string())
        );
        assert_eq!(config.pipelines[0].workers, Some(8));
        assert_eq!(config.pipelines[0].batch_size, Some(1000));
        assert_eq!(
            config.pipelines[0].queue_type,
            Some("persisted".to_string())
        );
    }

    #[test]
    fn test_parse_pipelines_config_invalid() {
        let yaml = "not: valid: yaml: [[[";
        assert!(parse_pipelines_config(yaml).is_err());
    }

    #[test]
    fn test_pipeline_bus_multiple_addresses() {
        let mut bus = PipelineBus::new();
        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        bus.register("pipe-a".to_string(), tx1);
        bus.register("pipe-b".to_string(), tx2);
        assert_eq!(bus.addresses().len(), 2);
    }
}
