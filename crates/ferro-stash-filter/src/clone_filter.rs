// SPDX-License-Identifier: Apache-2.0
//! Clone filter — duplicates events.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct CloneFilter {
    clones: usize,
    add_tags: Vec<String>,
    condition: Option<Condition>,
}

impl CloneFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        // Logstash clone filter: `clones => ["sun", "moon"]` — array of clone names,
        // each element creates one clone. Also accept integer for backwards-compat.
        let clones = settings.get("clones").map_or(1, |v| {
            if let Some(arr) = v.as_array() {
                arr.len()
            } else {
                v.as_u64().unwrap_or(1) as usize
            }
        });
        let add_tags = settings
            .get("add_tag")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            clones,
            add_tags,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for CloneFilter {
    fn name(&self) -> &'static str {
        "clone"
    }

    async fn filter(&self, event: Event) -> Result<Vec<Event>> {
        let mut events = vec![event.clone()];
        for _ in 0..self.clones {
            let mut cloned = event.clone();
            cloned.id = uuid::Uuid::new_v4().to_string();
            for tag in &self.add_tags {
                cloned.add_tag(tag);
            }
            events.push(cloned);
        }
        Ok(events)
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_clone() {
        let settings = serde_json::json!({ "clones": 2 });
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 3); // original + 2 clones
    }

    #[tokio::test]
    async fn test_clone_with_tags() {
        let settings = serde_json::json!({
            "clones": 1,
            "add_tag": ["cloned"]
        });
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 2);
        assert!(!result[0].has_tag("cloned")); // original
        assert!(result[1].has_tag("cloned")); // clone
    }

    #[tokio::test]
    async fn test_clone_default_one() {
        let settings = serde_json::json!({});
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 2); // original + 1 default clone
    }

    #[tokio::test]
    async fn test_clone_preserves_message() {
        let settings = serde_json::json!({ "clones": 1 });
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hello world"));
        assert_eq!(result[1].message(), Some("hello world"));
    }

    #[tokio::test]
    async fn test_clone_unique_ids() {
        let settings = serde_json::json!({ "clones": 2 });
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        // All events should have different IDs
        let ids: std::collections::HashSet<&str> = result.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids.len(), 3);
    }

    use ferro_stash_core::event::EventValue;

    #[tokio::test]
    async fn test_clone_preserves_fields() {
        let settings = serde_json::json!({ "clones": 1 });
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("host", EventValue::String("server01".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[1].get("host"),
            Some(&EventValue::String("server01".into()))
        );
    }

    #[test]
    fn test_clone_name() {
        let settings = serde_json::json!({});
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "clone");
    }
}
