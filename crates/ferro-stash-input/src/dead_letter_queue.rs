// SPDX-License-Identifier: Apache-2.0
//! Dead Letter Queue input plugin — reads failed events from DLQ for reprocessing.
//!
//! Usage in Logstash config:
//! ```text
//! input {
//!   dead_letter_queue {
//!     path => "/path/to/data/dead_letter_queue"
//!     pipeline_id => "main"
//!     commit_offsets => true
//!   }
//! }
//! ```

use async_trait::async_trait;
use ferro_stash_core::dead_letter_queue::{DeadLetterEntry, DeadLetterQueue, DlqConfig};
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug)]
pub struct DlqInput {
    path: String,
    pipeline_id: String,
    #[allow(dead_code)]
    commit_offsets: bool,
}

impl DlqInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let path = settings
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("data/dead_letter_queue")
            .to_string();

        let pipeline_id = settings
            .get("pipeline_id")
            .and_then(|v| v.as_str())
            .unwrap_or("main")
            .to_string();

        let commit_offsets = settings
            .get("commit_offsets")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        Ok(Self {
            path,
            pipeline_id,
            commit_offsets,
        })
    }

    fn entry_to_event(entry: &DeadLetterEntry) -> Event {
        // Reconstruct event from the DLQ entry's stored event JSON
        if let Some(msg) = entry.event.get("message").and_then(|v| v.as_str()) {
            Event::new(msg)
        } else {
            // Use the full JSON as message
            Event::new(entry.event.to_string())
        }
    }
}

#[async_trait]
impl InputPlugin for DlqInput {
    fn name(&self) -> &'static str {
        "dead_letter_queue"
    }

    async fn run(&mut self, sender: mpsc::Sender<Event>, shutdown: ShutdownSignal) -> Result<()> {
        // Build the DLQ path for the target pipeline
        let dlq_path = format!("{}/{}", self.path, self.pipeline_id);

        // Determine which path to read from
        let read_path = if std::path::Path::new(&dlq_path).exists() {
            dlq_path
        } else if std::path::Path::new(&self.path).exists() {
            self.path.clone()
        } else {
            info!(path = %self.path, "DLQ directory does not exist, waiting...");
            loop {
                if shutdown.is_shutdown() {
                    return Ok(());
                }
                if std::path::Path::new(&dlq_path).exists()
                    || std::path::Path::new(&self.path).exists()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            if std::path::Path::new(&dlq_path).exists() {
                dlq_path
            } else {
                self.path.clone()
            }
        };

        let config = DlqConfig {
            path: read_path,
            ..Default::default()
        };

        let dlq = DeadLetterQueue::open(config)?;
        let entries = dlq.read_all()?;

        info!(count = entries.len(), "DLQ input: reading entries");

        for entry in &entries {
            if shutdown.is_shutdown() {
                break;
            }

            let event = Self::entry_to_event(entry);
            if sender.send(event).await.is_err() {
                warn!("DLQ input: output channel closed");
                break;
            }
        }

        info!("DLQ input: all entries sent");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_input_from_config() {
        let settings = serde_json::json!({
            "path": "/tmp/dlq",
            "pipeline_id": "main",
            "commit_offsets": true,
        });
        let input = DlqInput::from_config(&settings).expect("ok");
        assert_eq!(input.name(), "dead_letter_queue");
        assert_eq!(input.path, "/tmp/dlq");
        assert_eq!(input.pipeline_id, "main");
        assert!(input.commit_offsets);
    }

    #[test]
    fn test_dlq_input_defaults() {
        let settings = serde_json::json!({});
        let input = DlqInput::from_config(&settings).expect("ok");
        assert_eq!(input.path, "data/dead_letter_queue");
        assert_eq!(input.pipeline_id, "main");
    }

    #[test]
    fn test_entry_to_event() {
        let entry = DeadLetterEntry {
            timestamp: "2026-04-11T00:00:00Z".to_string(),
            plugin_type: "output".to_string(),
            plugin_name: "elasticsearch".to_string(),
            reason: "mapping error".to_string(),
            event: serde_json::json!({"message": "test event", "field1": "value1"}),
            entry_id: "test-entry-id".to_string(),
        };
        let event = DlqInput::entry_to_event(&entry);
        assert_eq!(event.message(), Some("test event"));
    }
}
