// SPDX-License-Identifier: Apache-2.0
//! JSON filter — parses a field as JSON and merges into the event.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct JsonFilter {
    source: String,
    target: Option<String>,
    tag_on_failure: Vec<String>,
    skip_on_invalid_json: bool,
    condition: Option<Condition>,
}

impl JsonFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_string();
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["_jsonparsefailure".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );
        let skip_on_invalid_json = settings
            .get("skip_on_invalid_json")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        Ok(Self {
            source,
            target,
            tag_on_failure,
            skip_on_invalid_json,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for JsonFilter {
    fn name(&self) -> &'static str {
        "json"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let raw = if let Some(val) = event.get(&self.source) {
            val.to_string_lossy()
        } else {
            if !self.skip_on_invalid_json {
                for tag in &self.tag_on_failure {
                    event.add_tag(tag);
                }
            }
            return Ok(vec![event]);
        };

        match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(parsed) => {
                if let Some(ref target) = self.target {
                    event.set(target.clone(), EventValue::from(parsed));
                } else if let serde_json::Value::Object(map) = parsed {
                    for (k, v) in map {
                        event.set(k, EventValue::from(v));
                    }
                }
            }
            Err(_) => {
                if !self.skip_on_invalid_json {
                    for tag in &self.tag_on_failure {
                        event.add_tag(tag);
                    }
                }
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
    async fn test_json_filter_parse() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#"{"name": "Alice", "age": 30}"#);
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("name"),
            Some(&EventValue::String("Alice".into()))
        );
        assert_eq!(result[0].get("age"), Some(&EventValue::Integer(30)));
    }

    #[tokio::test]
    async fn test_json_filter_with_target() {
        let settings = serde_json::json!({ "source": "message", "target": "data" });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#"{"key": "value"}"#);
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("data"));
    }

    #[tokio::test]
    async fn test_json_filter_invalid() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new("not json");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_jsonparsefailure"));
    }

    #[tokio::test]
    async fn test_json_filter_skip_on_invalid() {
        let settings = serde_json::json!({
            "source": "message",
            "skip_on_invalid_json": true
        });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new("not json");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_jsonparsefailure"));
    }

    #[tokio::test]
    async fn test_json_filter_missing_source() {
        let settings = serde_json::json!({ "source": "nonexistent" });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_jsonparsefailure"));
    }

    #[tokio::test]
    async fn test_json_filter_missing_source_skip() {
        let settings = serde_json::json!({
            "source": "nonexistent",
            "skip_on_invalid_json": true
        });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_jsonparsefailure"));
    }

    #[tokio::test]
    async fn test_json_filter_nested_objects() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#"{"user": {"name": "Alice", "age": 30}}"#);
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("user"));
    }

    #[tokio::test]
    async fn test_json_filter_array_value() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#"{"tags": ["a", "b", "c"]}"#);
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("tags"));
    }

    #[tokio::test]
    async fn test_json_filter_custom_failure_tag() {
        let settings = serde_json::json!({
            "source": "message",
            "tag_on_failure": ["json_error"]
        });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new("bad");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("json_error"));
    }

    #[tokio::test]
    async fn test_json_filter_default_source() {
        let settings = serde_json::json!({});
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#"{"key": "val"}"#);
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("key"),
            Some(&EventValue::String("val".into()))
        );
    }

    #[test]
    fn test_json_filter_name() {
        let settings = serde_json::json!({});
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "json");
    }

    #[tokio::test]
    async fn test_json_filter_boolean_values() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = JsonFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#"{"active": true, "deleted": false}"#);
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("active"), Some(&EventValue::Boolean(true)));
        assert_eq!(result[0].get("deleted"), Some(&EventValue::Boolean(false)));
    }
}
