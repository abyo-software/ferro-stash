// SPDX-License-Identifier: Apache-2.0
//! Pipeline output plugin — sends events to another pipeline via the pipeline bus.
//!
//! Usage in Logstash config:
//! ```text
//! output {
//!   pipeline { send_to => ["my-address"] }
//! }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::multi_pipeline::PipelineBus;
use ferro_stash_core::plugin::OutputPlugin;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct PipelineOutput {
    send_to: Vec<String>,
    bus: Arc<RwLock<PipelineBus>>,
    condition: Option<Condition>,
}

impl PipelineOutput {
    pub fn new(
        send_to: Vec<String>,
        bus: Arc<RwLock<PipelineBus>>,
        condition: Option<Condition>,
    ) -> Self {
        Self {
            send_to,
            bus,
            condition,
        }
    }

    pub fn from_config(
        settings: &serde_json::Value,
        bus: Arc<RwLock<PipelineBus>>,
        condition: Option<Condition>,
    ) -> Result<Self> {
        let send_to = if let Some(arr) = settings.get("send_to").and_then(|v| v.as_array()) {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else if let Some(s) = settings.get("send_to").and_then(|v| v.as_str()) {
            vec![s.to_string()]
        } else {
            vec!["default".to_string()]
        };
        Ok(Self::new(send_to, bus, condition))
    }
}

#[async_trait]
impl OutputPlugin for PipelineOutput {
    fn name(&self) -> &'static str {
        "pipeline"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        for event in events {
            for address in &self.send_to {
                // Wait for the target pipeline's address to become available
                // (the consuming pipeline may still be starting up).
                let mut retries = 30u32;
                loop {
                    let bus = self.bus.read().await;
                    let has_address = bus.addresses().iter().any(|a| a.as_str() == address);
                    if has_address {
                        let send_result = bus.send(address, event.clone()).await;
                        drop(bus);
                        send_result?;
                        break;
                    }
                    drop(bus);
                    if retries == 0 {
                        return Err(ferro_stash_core::error::FerroStashError::Pipeline(format!(
                            "pipeline address '{address}' never became available"
                        )));
                    }
                    retries -= 1;
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
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
    fn test_pipeline_output_from_config() {
        let bus = Arc::new(RwLock::new(PipelineBus::new()));
        let settings = serde_json::json!({"send_to": ["pipe-a", "pipe-b"]});
        let output = PipelineOutput::from_config(&settings, bus, None);
        assert!(output.is_ok());
        let output = output.expect("ok");
        assert_eq!(output.name(), "pipeline");
        assert_eq!(output.send_to, vec!["pipe-a", "pipe-b"]);
    }

    #[test]
    fn test_pipeline_output_single_string() {
        let bus = Arc::new(RwLock::new(PipelineBus::new()));
        let settings = serde_json::json!({"send_to": "my-pipe"});
        let output = PipelineOutput::from_config(&settings, bus, None).expect("ok");
        assert_eq!(output.send_to, vec!["my-pipe"]);
    }

    #[test]
    fn test_pipeline_output_default() {
        let bus = Arc::new(RwLock::new(PipelineBus::new()));
        let settings = serde_json::json!({});
        let output = PipelineOutput::from_config(&settings, bus, None).expect("ok");
        assert_eq!(output.send_to, vec!["default"]);
    }
}
