// SPDX-License-Identifier: Apache-2.0
//! De_dot filter — replace dots in field names with a configurable separator.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;

#[derive(Debug)]
pub struct DeDotFilter {
    /// Separator to replace dots with (default: "_").
    separator: String,
    /// Whether to process nested object field names recursively.
    nested: bool,
    condition: Option<Condition>,
}

impl DeDotFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let separator = settings
            .get("separator")
            .and_then(|v| v.as_str())
            .unwrap_or("_")
            .to_string();

        let nested = settings
            .get("nested")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);

        Ok(Self {
            separator,
            nested,
            condition,
        })
    }

    /// Replace dots in a field name with the separator.
    fn de_dot_name(&self, name: &str) -> String {
        name.replace('.', &self.separator)
    }

    /// Recursively process an EventValue, replacing dots in Object keys.
    fn de_dot_value(&self, value: EventValue) -> EventValue {
        match value {
            EventValue::Object(map) => {
                let mut new_map = IndexMap::new();
                for (k, v) in map {
                    let new_key = self.de_dot_name(&k);
                    let new_val = if self.nested { self.de_dot_value(v) } else { v };
                    new_map.insert(new_key, new_val);
                }
                EventValue::Object(new_map)
            }
            EventValue::Array(arr) => {
                if self.nested {
                    EventValue::Array(arr.into_iter().map(|v| self.de_dot_value(v)).collect())
                } else {
                    EventValue::Array(arr)
                }
            }
            other => other,
        }
    }
}

#[async_trait]
impl FilterPlugin for DeDotFilter {
    fn name(&self) -> &'static str {
        "de_dot"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        // Collect all fields, transform their names, and replace.
        // We work with fields_mut() directly to avoid FieldRef interpreting dots.
        let fields: Vec<(String, EventValue)> = event
            .fields()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Clear all fields and re-insert with transformed names
        event.fields_mut().clear();

        for (key, value) in fields {
            let new_key = self.de_dot_name(&key);
            let new_value = if self.nested {
                self.de_dot_value(value)
            } else {
                value
            };
            event.fields_mut().insert(new_key, new_value);
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
    async fn test_de_dot_basic() {
        let settings = serde_json::json!({});
        let filter = DeDotFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        // Insert with literal dots in key names (bypass FieldRef)
        event.fields_mut().insert(
            "host.name".to_string(),
            EventValue::String("server01".into()),
        );
        event
            .fields_mut()
            .insert("os.type".to_string(), EventValue::String("linux".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].fields().contains_key("host_name"));
        assert!(result[0].fields().contains_key("os_type"));
        assert_eq!(
            result[0].fields().get("host_name"),
            Some(&EventValue::String("server01".into()))
        );
    }

    #[tokio::test]
    async fn test_de_dot_custom_separator() {
        let settings = serde_json::json!({ "separator": "-" });
        let filter = DeDotFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event
            .fields_mut()
            .insert("a.b.c".to_string(), EventValue::String("value".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].fields().contains_key("a-b-c"));
    }

    #[tokio::test]
    async fn test_de_dot_nested_objects() {
        let settings = serde_json::json!({ "nested": true });
        let filter = DeDotFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        let mut inner = IndexMap::new();
        inner.insert("inner.field".to_string(), EventValue::String("val".into()));
        event.set("outer", EventValue::Object(inner));
        let result = filter.filter(event).await.expect("filter");
        let outer = result[0].get("outer").expect("outer");
        let obj = outer.as_object().expect("should be object");
        assert!(obj.contains_key("inner_field"));
        assert!(!obj.contains_key("inner.field"));
    }

    #[tokio::test]
    async fn test_de_dot_no_dots() {
        let settings = serde_json::json!({});
        let filter = DeDotFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("clean_field", EventValue::String("value".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("clean_field"),
            Some(&EventValue::String("value".into()))
        );
    }

    #[tokio::test]
    async fn test_de_dot_not_nested() {
        let settings = serde_json::json!({ "nested": false });
        let filter = DeDotFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        let mut inner = IndexMap::new();
        inner.insert("inner.field".to_string(), EventValue::String("val".into()));
        event
            .fields_mut()
            .insert("outer.key".to_string(), EventValue::Object(inner));
        let result = filter.filter(event).await.expect("filter");
        // Top-level key should be de-dotted
        assert!(result[0].fields().contains_key("outer_key"));
        // But inner key should NOT be de-dotted when nested=false
        let outer = result[0].fields().get("outer_key").expect("outer_key");
        let obj = outer.as_object().expect("should be object");
        assert!(obj.contains_key("inner.field"));
    }

    #[test]
    fn test_de_dot_name() {
        let settings = serde_json::json!({});
        let filter = DeDotFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "de_dot");
    }
}
