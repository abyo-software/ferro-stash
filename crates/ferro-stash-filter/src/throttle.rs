// SPDX-License-Identifier: Apache-2.0
//! Throttle filter — rate-limit events by a key.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::FilterPlugin;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct ThrottleFilter {
    key: String,
    before_count: i64,
    after_count: i64,
    period: Duration,
    max_age: Duration,
    condition: Option<Condition>,
    /// Tracks: key -> (count, window_start)
    state: Arc<Mutex<HashMap<String, (i64, Instant)>>>,
}

impl ThrottleFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let key = settings
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or("%{message}")
            .to_string();

        let before_count = settings
            .get("before_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);

        let after_count = settings
            .get("after_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);

        let period_secs = settings
            .get("period")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        let max_age_secs = settings
            .get("max_age")
            .and_then(|v| v.as_u64())
            .unwrap_or(86400);

        Ok(Self {
            key,
            before_count,
            after_count,
            period: Duration::from_secs(period_secs),
            max_age: Duration::from_secs(max_age_secs),
            condition,
            state: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl FilterPlugin for ThrottleFilter {
    fn name(&self) -> &'static str {
        "throttle"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let resolved_key = event.sprintf(&self.key);
        let now = Instant::now();
        let mut state = self.state.lock().await;

        // Clean expired entries
        state.retain(|_, (_, start)| now.duration_since(*start) < self.max_age);

        let entry = state.entry(resolved_key).or_insert_with(|| (0, now));

        // Reset window if period elapsed
        if now.duration_since(entry.1) >= self.period {
            entry.0 = 0;
            entry.1 = now;
        }

        entry.0 += 1;
        let count = entry.0;

        let throttled = if self.before_count >= 0 && count <= self.before_count {
            true
        } else {
            self.after_count >= 0 && count > self.after_count
        };

        if throttled {
            event.add_tag("_throttle");
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
    async fn test_throttle_after_count() {
        let settings = serde_json::json!({
            "key": "%{host}",
            "after_count": 2,
            "period": 3600
        });
        let filter = ThrottleFilter::from_config(&settings, None).expect("config");

        let mut e1 = Event::new("msg1");
        e1.set("host", "server1".into());
        let r1 = filter.filter(e1).await.expect("filter");
        assert!(!r1[0].has_tag("_throttle"), "first event should pass");

        let mut e2 = Event::new("msg2");
        e2.set("host", "server1".into());
        let r2 = filter.filter(e2).await.expect("filter");
        assert!(!r2[0].has_tag("_throttle"), "second event should pass");

        let mut e3 = Event::new("msg3");
        e3.set("host", "server1".into());
        let r3 = filter.filter(e3).await.expect("filter");
        assert!(
            r3[0].has_tag("_throttle"),
            "third event should be throttled"
        );
    }

    #[tokio::test]
    async fn test_throttle_before_count() {
        let settings = serde_json::json!({
            "key": "%{host}",
            "before_count": 1,
            "period": 3600
        });
        let filter = ThrottleFilter::from_config(&settings, None).expect("config");

        let mut e1 = Event::new("msg1");
        e1.set("host", "server1".into());
        let r1 = filter.filter(e1).await.expect("filter");
        assert!(
            r1[0].has_tag("_throttle"),
            "first event within before_count should be throttled"
        );

        let mut e2 = Event::new("msg2");
        e2.set("host", "server1".into());
        let r2 = filter.filter(e2).await.expect("filter");
        assert!(
            !r2[0].has_tag("_throttle"),
            "second event exceeds before_count, should pass"
        );
    }

    #[tokio::test]
    async fn test_throttle_different_keys() {
        let settings = serde_json::json!({
            "key": "%{host}",
            "after_count": 1,
            "period": 3600
        });
        let filter = ThrottleFilter::from_config(&settings, None).expect("config");

        let mut e1 = Event::new("msg");
        e1.set("host", "server1".into());
        let r1 = filter.filter(e1).await.expect("filter");
        assert!(!r1[0].has_tag("_throttle"));

        let mut e2 = Event::new("msg");
        e2.set("host", "server2".into());
        let r2 = filter.filter(e2).await.expect("filter");
        assert!(
            !r2[0].has_tag("_throttle"),
            "different key should have separate count"
        );
    }

    #[test]
    fn test_throttle_name() {
        let settings = serde_json::json!({});
        let filter = ThrottleFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "throttle");
    }
}
