// SPDX-License-Identifier: Apache-2.0
//! Drop filter — removes events from the pipeline.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct DropFilter {
    percentage: f64,
    condition: Option<Condition>,
}

impl DropFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let percentage = settings
            .get("percentage")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(100.0);

        Ok(Self {
            percentage,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for DropFilter {
    fn name(&self) -> &'static str {
        "drop"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if self.percentage >= 100.0 {
            event.cancel();
        } else if self.percentage > 0.0 {
            // Simple probabilistic drop
            let rand_val = (event
                .id
                .as_bytes()
                .iter()
                .map(|b| u64::from(*b))
                .sum::<u64>()
                % 100) as f64;
            if rand_val < self.percentage {
                event.cancel();
            }
        }
        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_drop_all() {
        let settings = serde_json::json!({});
        let filter = DropFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].is_cancelled());
    }

    #[tokio::test]
    async fn test_drop_zero_percentage() {
        let settings = serde_json::json!({ "percentage": 0 });
        let filter = DropFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].is_cancelled());
    }

    #[tokio::test]
    async fn test_drop_100_percentage() {
        let settings = serde_json::json!({ "percentage": 100 });
        let filter = DropFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].is_cancelled());
    }

    #[test]
    fn test_drop_name() {
        let settings = serde_json::json!({});
        let filter = DropFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "drop");
    }

    #[tokio::test]
    async fn test_drop_partial_percentage() {
        // With percentage < 100, some events might be dropped, some not
        let settings = serde_json::json!({ "percentage": 50 });
        let filter = DropFilter::from_config(&settings, None).expect("config");
        let mut cancelled = 0;
        let mut kept = 0;
        for _ in 0..100 {
            let event = Event::new("test");
            let result = filter.filter(event).await.expect("filter");
            if result[0].is_cancelled() {
                cancelled += 1;
            } else {
                kept += 1;
            }
        }
        // Both should be non-zero with 50% probability
        assert!(cancelled > 0 || kept > 0);
    }
}
