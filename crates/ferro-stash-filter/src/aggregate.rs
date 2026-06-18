// SPDX-License-Identifier: Apache-2.0
//! Aggregate filter — accumulate events by a key, emit when timeout or end condition is met.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct AggregateFilter {
    task_id: String,
    timeout: Duration,
    push_map_as_event_on_timeout: bool,
    end_of_task: bool,
    map_action: MapAction,
    condition: Option<Condition>,
    /// Shared state: task_id_value -> (accumulated map, last update time)
    #[allow(clippy::type_complexity)]
    state: Arc<Mutex<HashMap<String, (IndexMap<String, EventValue>, Instant)>>>,
}

#[derive(Debug, Clone)]
enum MapAction {
    Create,
    Update,
}

impl AggregateFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let task_id = settings
            .get("task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("%{task_id}")
            .to_string();

        let timeout_secs = settings
            .get("timeout")
            .and_then(|v| v.as_u64())
            .unwrap_or(1800);

        let push_map_as_event_on_timeout = settings
            .get("push_map_as_event_on_timeout")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let end_of_task = settings
            .get("end_of_task")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let map_action = match settings
            .get("map_action")
            .and_then(|v| v.as_str())
            .unwrap_or("create")
        {
            "update" => MapAction::Update,
            _ => MapAction::Create,
        };

        Ok(Self {
            task_id,
            timeout: Duration::from_secs(timeout_secs),
            push_map_as_event_on_timeout,
            end_of_task,
            map_action,
            condition,
            state: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn resolve_task_id(&self, event: &Event) -> String {
        event.sprintf(&self.task_id)
    }
}

#[async_trait]
impl FilterPlugin for AggregateFilter {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    async fn filter(&self, event: Event) -> Result<Vec<Event>> {
        let task_key = self.resolve_task_id(&event);
        let mut state = self.state.lock().await;
        let now = Instant::now();

        // Check for timed-out entries and emit them
        let mut results = Vec::new();
        let mut expired_keys = Vec::new();
        for (key, (map, last_update)) in state.iter() {
            if now.duration_since(*last_update) > self.timeout && self.push_map_as_event_on_timeout
            {
                let mut timeout_event = Event::empty();
                for (k, v) in map {
                    timeout_event.set(k.clone(), v.clone());
                }
                timeout_event.add_tag("_aggregatetimeout");
                results.push(timeout_event);
                expired_keys.push(key.clone());
            }
        }
        for key in &expired_keys {
            state.remove(key);
        }

        match self.map_action {
            MapAction::Create => {
                if !state.contains_key(&task_key) {
                    let mut map = IndexMap::new();
                    // Copy all event fields into the aggregate map
                    for (k, v) in event.fields() {
                        map.insert(k.clone(), v.clone());
                    }
                    state.insert(task_key.clone(), (map, now));
                }
            }
            MapAction::Update => {
                let entry = state
                    .entry(task_key.clone())
                    .or_insert_with(|| (IndexMap::new(), now));
                for (k, v) in event.fields() {
                    entry.0.insert(k.clone(), v.clone());
                }
                entry.1 = now;
            }
        }

        if self.end_of_task {
            if let Some((map, _)) = state.remove(&task_key) {
                let mut final_event = Event::empty();
                for (k, v) in &map {
                    final_event.set(k.clone(), v.clone());
                }
                results.push(final_event);
            }
        }

        // The original event passes through
        results.push(event);
        Ok(results)
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aggregate_create() {
        let settings = serde_json::json!({
            "task_id": "%{session_id}",
            "map_action": "create"
        });
        let filter = AggregateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("first");
        event.set("session_id", EventValue::String("s1".into()));
        let result = filter.filter(event).await.expect("filter");
        // Original event passes through
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_aggregate_update_accumulates() {
        let settings = serde_json::json!({
            "task_id": "%{session_id}",
            "map_action": "update"
        });
        let filter = AggregateFilter::from_config(&settings, None).expect("config");

        let mut e1 = Event::new("first");
        e1.set("session_id", EventValue::String("s1".into()));
        e1.set("count", EventValue::Integer(1));
        filter.filter(e1).await.expect("filter");

        let mut e2 = Event::new("second");
        e2.set("session_id", EventValue::String("s1".into()));
        e2.set("count", EventValue::Integer(2));
        filter.filter(e2).await.expect("filter");

        // The internal state should have the updated count
        let state = filter.state.lock().await;
        let (map, _) = state.get("s1").expect("should have s1");
        assert_eq!(map.get("count"), Some(&EventValue::Integer(2)));
    }

    #[tokio::test]
    async fn test_aggregate_end_of_task() {
        let settings = serde_json::json!({
            "task_id": "%{session_id}",
            "map_action": "update",
            "end_of_task": true
        });
        let filter = AggregateFilter::from_config(&settings, None).expect("config");

        let mut e1 = Event::new("done");
        e1.set("session_id", EventValue::String("s1".into()));
        e1.set("total", EventValue::Integer(42));
        let result = filter.filter(e1).await.expect("filter");
        // Should emit the aggregated event + the original
        assert!(result.len() >= 2);
        // Aggregated event should have the total
        let agg = &result[0];
        assert_eq!(agg.get("total"), Some(&EventValue::Integer(42)));
    }

    #[test]
    fn test_aggregate_name() {
        let settings = serde_json::json!({});
        let filter = AggregateFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "aggregate");
    }

    #[tokio::test]
    async fn test_aggregate_default_config() {
        let settings = serde_json::json!({});
        let filter = AggregateFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.timeout, Duration::from_secs(1800));
        assert!(!filter.push_map_as_event_on_timeout);
        assert!(!filter.end_of_task);
    }
}
