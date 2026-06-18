// SPDX-License-Identifier: Apache-2.0
//! Plugin trait definitions — the extension points for inputs, filters, and outputs.

use std::fmt::Debug;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::condition::Condition;
use crate::error::Result;
use crate::event::Event;
use crate::shutdown::ShutdownSignal;

// Re-export async_trait for plugin implementors
pub use async_trait::async_trait as plugin_async_trait;

/// An input plugin reads events from an external source.
#[async_trait]
pub trait InputPlugin: Send + Sync + Debug {
    /// Returns the plugin name (e.g., "file", "stdin", "kafka").
    fn name(&self) -> &str;

    /// Starts reading events and sending them to the channel.
    /// This method should run until shutdown is signaled.
    async fn run(&mut self, sender: mpsc::Sender<Event>, shutdown: ShutdownSignal) -> Result<()>;
}

/// A filter plugin transforms events.
#[async_trait]
pub trait FilterPlugin: Send + Sync + Debug {
    /// Returns the plugin name (e.g., "grok", "mutate", "json").
    fn name(&self) -> &str;

    /// Processes a single event.
    /// Returns the event (potentially modified), or None to drop it.
    /// May return multiple events (e.g., clone filter).
    async fn filter(&self, event: Event) -> Result<Vec<Event>>;

    /// Returns the condition for this filter, if any.
    fn condition(&self) -> Option<&Condition> {
        None
    }
}

/// An output plugin sends events to an external destination.
#[async_trait]
pub trait OutputPlugin: Send + Sync + Debug {
    /// Returns the plugin name (e.g., "elasticsearch", "stdout", "file").
    fn name(&self) -> &str;

    /// Sends a batch of events to the output.
    async fn output(&self, events: Vec<Event>) -> Result<()>;

    /// Sends a single event.
    async fn output_one(&self, event: Event) -> Result<()> {
        self.output(vec![event]).await
    }

    /// Returns the condition for this output, if any.
    fn condition(&self) -> Option<&Condition> {
        None
    }

    /// Flushes any pending output.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Closes the output and releases resources.
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Configuration for a plugin instance.
#[derive(Debug, Clone)]
pub struct PluginConfig {
    pub plugin_type: String,
    pub settings: serde_json::Value,
    pub condition: Option<Condition>,
}

impl PluginConfig {
    pub fn new(plugin_type: impl Into<String>, settings: serde_json::Value) -> Self {
        Self {
            plugin_type: plugin_type.into(),
            settings,
            condition: None,
        }
    }

    pub fn with_condition(mut self, condition: Condition) -> Self {
        self.condition = Some(condition);
        self
    }
}
