// SPDX-License-Identifier: Apache-2.0
//! Null output plugin — discards events (useful for testing/benchmarking).

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;

#[derive(Debug)]
pub struct NullOutput {
    condition: Option<Condition>,
}

impl NullOutput {
    pub fn from_config(
        _settings: &serde_json::Value,
        condition: Option<Condition>,
    ) -> Result<Self> {
        Ok(Self { condition })
    }
}

#[async_trait]
impl OutputPlugin for NullOutput {
    fn name(&self) -> &'static str {
        "null"
    }

    async fn output(&self, _events: Vec<Event>) -> Result<()> {
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
    fn test_null_config() {
        let settings = serde_json::json!({});
        let output = NullOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.name(), "null");
    }

    #[tokio::test]
    async fn test_null_output() {
        let settings = serde_json::json!({});
        let output = NullOutput::from_config(&settings, None).expect("config");
        let events = vec![Event::new("test1"), Event::new("test2")];
        let result = output.output(events).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_null_output_empty() {
        let settings = serde_json::json!({});
        let output = NullOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![]).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_null_no_condition() {
        let settings = serde_json::json!({});
        let output = NullOutput::from_config(&settings, None).expect("config");
        assert!(output.condition().is_none());
    }
}
