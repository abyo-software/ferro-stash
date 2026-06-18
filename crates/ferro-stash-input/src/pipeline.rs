// SPDX-License-Identifier: Apache-2.0
//! Pipeline input plugin — receives events from another pipeline via the pipeline bus.
//!
//! Usage in Logstash config:
//! ```text
//! input {
//!   pipeline { address => "my-address" }
//! }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::multi_pipeline::PipelineBus;
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug)]
pub struct PipelineInput {
    address: String,
    bus: Arc<RwLock<PipelineBus>>,
}

impl PipelineInput {
    pub fn new(address: String, bus: Arc<RwLock<PipelineBus>>) -> Self {
        Self { address, bus }
    }

    pub fn from_config(
        settings: &serde_json::Value,
        bus: Arc<RwLock<PipelineBus>>,
    ) -> Result<Self> {
        let address = settings
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        Ok(Self::new(address, bus))
    }
}

#[async_trait]
impl InputPlugin for PipelineInput {
    fn name(&self) -> &'static str {
        "pipeline"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        // Register our address on the bus
        let (bus_tx, mut bus_rx) = mpsc::channel::<Event>(1000);
        {
            let mut bus = self.bus.write().await;
            bus.register(self.address.clone(), bus_tx);
        }

        // Forward events from bus to pipeline
        loop {
            tokio::select! {
                event = bus_rx.recv() => {
                    match event {
                        Some(ev) => {
                            if sender.send(ev).await.is_err() {
                                break;
                            }
                        }
                        None => break, // bus sender dropped
                    }
                }
                () = shutdown.wait() => {
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
    fn test_pipeline_input_from_config() {
        let bus = Arc::new(RwLock::new(PipelineBus::new()));
        let settings = serde_json::json!({"address": "my-pipe"});
        let input = PipelineInput::from_config(&settings, bus);
        assert!(input.is_ok());
        let input = input.expect("ok");
        assert_eq!(input.name(), "pipeline");
        assert_eq!(input.address, "my-pipe");
    }

    #[test]
    fn test_pipeline_input_default_address() {
        let bus = Arc::new(RwLock::new(PipelineBus::new()));
        let settings = serde_json::json!({});
        let input = PipelineInput::from_config(&settings, bus).expect("ok");
        assert_eq!(input.address, "default");
    }
}
