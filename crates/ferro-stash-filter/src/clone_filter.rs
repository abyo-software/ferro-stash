// SPDX-License-Identifier: Apache-2.0
//! Clone filter — duplicates events.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct CloneFilter {
    /// One entry per clone to emit. The string is the clone's name, which real
    /// Logstash adds as a tag on that clone; an empty name (integer-count form)
    /// produces an untagged clone.
    clone_names: Vec<String>,
    add_tags: Vec<String>,
    condition: Option<Condition>,
}

impl CloneFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        // Logstash clone filter: `clones => ["sun", "moon"]` — array of clone names,
        // each element creates one clone tagged with its name. Also accept an
        // integer for backwards-compat (N untagged clones). Default: 1 clone.
        let clone_names: Vec<String> = settings.get("clones").map_or_else(
            || vec![String::new()],
            |v| {
                if let Some(arr) = v.as_array() {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                } else if let Some(n) = v.as_u64() {
                    vec![String::new(); n as usize]
                } else {
                    vec![String::new()]
                }
            },
        );
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
            clone_names,
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
        // The original passes through untouched; each clone is tagged with its
        // name (matching Logstash) plus any common `add_tag` values.
        let mut events = Vec::with_capacity(self.clone_names.len() + 1);
        events.push(event.clone());
        for name in &self.clone_names {
            let mut cloned = event.clone();
            cloned.id = uuid::Uuid::new_v4().to_string();
            if !name.is_empty() {
                cloned.add_tag(name);
            }
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

    #[tokio::test]
    async fn test_clone_named_clones_get_name_tag() {
        // Logstash parity: `clones => ["audit", "metrics"]` tags each clone with
        // its name; the original is emitted untagged.
        let settings = serde_json::json!({ "clones": ["audit", "metrics"] });
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 3); // original + 2 named clones
        assert!(!result[0].has_tag("audit") && !result[0].has_tag("metrics")); // original untouched
        assert!(result[1].has_tag("audit") && !result[1].has_tag("metrics"));
        assert!(result[2].has_tag("metrics") && !result[2].has_tag("audit"));
    }

    #[test]
    fn test_clone_name() {
        let settings = serde_json::json!({});
        let filter = CloneFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "clone");
    }
}
