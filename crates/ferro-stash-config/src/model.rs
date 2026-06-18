// SPDX-License-Identifier: Apache-2.0
//! Configuration model — shared data structures.

use ferro_stash_core::condition::Condition;
use serde::{Deserialize, Serialize};

/// Top-level configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    pub pipeline: PipelineSettings,
    pub inputs: Vec<InputConfig>,
    pub filters: Vec<FilterConfig>,
    pub outputs: Vec<OutputConfig>,
    /// Persistent queue configuration.
    #[serde(default)]
    pub queue: QueueConfig,
    /// Dead letter queue configuration.
    #[serde(default)]
    pub dead_letter_queue: DlqConfigSettings,
}

/// Persistent queue configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Queue type: `"memory"` (default) or `"persisted"`.
    #[serde(rename = "type", default = "default_queue_type")]
    pub queue_type: String,
    /// Path for persisted queue data.
    #[serde(default = "default_queue_path")]
    pub path: String,
    /// Maximum size in bytes.
    #[serde(default = "default_queue_max_bytes")]
    pub max_bytes: u64,
}

fn default_queue_type() -> String {
    "memory".to_string()
}
fn default_queue_path() -> String {
    "data/queue".to_string()
}
fn default_queue_max_bytes() -> u64 {
    1_073_741_824 // 1GB
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            queue_type: default_queue_type(),
            path: default_queue_path(),
            max_bytes: default_queue_max_bytes(),
        }
    }
}

/// Dead letter queue configuration settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqConfigSettings {
    /// Enable the dead letter queue.
    #[serde(default)]
    pub enable: bool,
    /// Path for DLQ data.
    #[serde(default = "default_dlq_path")]
    pub path: String,
    /// Maximum size in bytes.
    #[serde(default = "default_dlq_max_bytes")]
    pub max_bytes: u64,
}

fn default_dlq_path() -> String {
    "data/dead_letter_queue".to_string()
}
fn default_dlq_max_bytes() -> u64 {
    104_857_600 // 100MB
}

impl Default for DlqConfigSettings {
    fn default() -> Self {
        Self {
            enable: false,
            path: default_dlq_path(),
            max_bytes: default_dlq_max_bytes(),
        }
    }
}

/// Pipeline-level settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSettings {
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_delay")]
    pub batch_delay_ms: u64,
    #[serde(default)]
    pub id: String,
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
}

impl Default for PipelineSettings {
    fn default() -> Self {
        Self {
            workers: default_workers(),
            batch_size: default_batch_size(),
            batch_delay_ms: default_batch_delay(),
            id: "main".to_string(),
            buffer_size: default_buffer_size(),
        }
    }
}

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
}

fn default_batch_size() -> usize {
    500
}

fn default_batch_delay() -> u64 {
    50 // Logstash default: 50ms
}

fn default_buffer_size() -> usize {
    10_000
}

/// Input plugin configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub settings: serde_json::Value,
    #[serde(default)]
    pub codec: Option<String>,
    #[serde(default)]
    pub codec_settings: serde_json::Value,
    /// Tags to add to events from this input.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Type field to set on events from this input.
    #[serde(rename = "event_type", default)]
    pub event_type: Option<String>,
}

/// Filter plugin configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub settings: serde_json::Value,
    #[serde(default)]
    pub condition: Option<Condition>,
}

/// Output plugin configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub settings: serde_json::Value,
    #[serde(default)]
    pub codec: Option<String>,
    #[serde(default)]
    pub codec_settings: serde_json::Value,
    #[serde(default)]
    pub condition: Option<Condition>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_settings_default() {
        let s = PipelineSettings::default();
        assert!(s.workers > 0);
        assert_eq!(s.batch_size, 500);
        assert_eq!(s.batch_delay_ms, 50);
    }

    #[test]
    fn test_config_default() {
        let c = Config::default();
        assert!(c.inputs.is_empty());
        assert!(c.filters.is_empty());
        assert!(c.outputs.is_empty());
        assert_eq!(c.queue.queue_type, "memory");
        assert!(!c.dead_letter_queue.enable);
    }

    #[test]
    fn test_queue_config_default() {
        let q = QueueConfig::default();
        assert_eq!(q.queue_type, "memory");
        assert_eq!(q.path, "data/queue");
        assert_eq!(q.max_bytes, 1_073_741_824);
    }

    #[test]
    fn test_dlq_config_default() {
        let d = DlqConfigSettings::default();
        assert!(!d.enable);
        assert_eq!(d.path, "data/dead_letter_queue");
        assert_eq!(d.max_bytes, 104_857_600);
    }

    #[test]
    fn test_queue_config_serde() {
        let json = serde_json::json!({
            "type": "persisted",
            "path": "/tmp/pq",
            "max_bytes": 500
        });
        let q: QueueConfig = serde_json::from_value(json).expect("parse");
        assert_eq!(q.queue_type, "persisted");
        assert_eq!(q.path, "/tmp/pq");
        assert_eq!(q.max_bytes, 500);
    }

    #[test]
    fn test_dlq_config_serde() {
        let json = serde_json::json!({
            "enable": true,
            "path": "/tmp/dlq",
            "max_bytes": 1000
        });
        let d: DlqConfigSettings = serde_json::from_value(json).expect("parse");
        assert!(d.enable);
        assert_eq!(d.path, "/tmp/dlq");
        assert_eq!(d.max_bytes, 1000);
    }
}
