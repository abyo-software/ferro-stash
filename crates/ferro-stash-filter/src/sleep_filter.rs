// SPDX-License-Identifier: Apache-2.0
//! Sleep filter — adds a sleep delay to events. Useful for rate limiting and testing.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::FilterPlugin;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::Duration;

#[derive(Debug)]
pub struct SleepFilter {
    /// Delay in seconds (can be fractional).
    time_seconds: f64,
    /// Sleep every N events (default: 1, every event).
    every: u64,
    /// Replay mode (not fully implemented, accepted for compatibility).
    #[allow(dead_code)]
    replay: bool,
    condition: Option<Condition>,
    counter: AtomicU64,
}

impl SleepFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let time_seconds = settings
            .get("time")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_u64().map(|u| u as f64))
                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .unwrap_or(1.0);

        let every = settings
            .get("every")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
            })
            .unwrap_or(1)
            .max(1);

        let replay = settings
            .get("replay")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self {
            time_seconds,
            every,
            replay,
            condition,
            counter: AtomicU64::new(0),
        })
    }
}

#[async_trait]
impl FilterPlugin for SleepFilter {
    fn name(&self) -> &'static str {
        "sleep"
    }

    async fn filter(&self, event: Event) -> Result<Vec<Event>> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        if n % self.every == 0 {
            let ms = (self.time_seconds * 1000.0).max(0.0) as u64;
            if ms > 0 {
                tokio::time::sleep(Duration::from_millis(ms)).await;
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
    async fn test_sleep_no_delay() {
        let settings = serde_json::json!({ "time": 0 });
        let filter = SleepFilter::from_config(&settings, None).expect("config");
        let event = Event::new("x");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_sleep_every_10() {
        let settings = serde_json::json!({ "time": 0, "every": 10 });
        let filter = SleepFilter::from_config(&settings, None).expect("config");
        // Should process without error
        for _ in 0..20 {
            let event = Event::new("x");
            let result = filter.filter(event).await.expect("filter");
            assert_eq!(result.len(), 1);
        }
    }

    #[test]
    fn test_sleep_config_string_time() {
        let settings = serde_json::json!({ "time": "1.5" });
        let filter = SleepFilter::from_config(&settings, None).expect("config");
        assert!((filter.time_seconds - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sleep_name() {
        let settings = serde_json::json!({});
        let filter = SleepFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "sleep");
    }
}
