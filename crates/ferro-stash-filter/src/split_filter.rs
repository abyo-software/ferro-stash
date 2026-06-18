// SPDX-License-Identifier: Apache-2.0
//! Split filter — split a single event into multiple events based on a field value.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct SplitFilter {
    field: String,
    separator: String,
    target: Option<String>,
    condition: Option<Condition>,
}

impl SplitFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let field = settings
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_string();

        let separator = settings
            .get("separator")
            .and_then(|v| v.as_str())
            .unwrap_or("\n")
            .to_string();

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);

        Ok(Self {
            field,
            separator,
            target,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for SplitFilter {
    fn name(&self) -> &'static str {
        "split"
    }

    async fn filter(&self, event: Event) -> Result<Vec<Event>> {
        let field_value = match event.get(&self.field) {
            Some(v) => v.clone(),
            None => return Ok(vec![event]),
        };

        // If the field is an array, split into multiple events directly
        let parts: Vec<EventValue> = match field_value {
            EventValue::Array(arr) => arr,
            EventValue::String(ref s) => s
                .split(&self.separator)
                .map(|part| EventValue::String(part.to_string()))
                .collect(),
            _ => return Ok(vec![event]),
        };

        if parts.is_empty() {
            return Ok(vec![event]);
        }

        let dest_field = self.target.as_deref().unwrap_or(&self.field);
        let mut results = Vec::with_capacity(parts.len());

        for part in parts {
            let mut new_event = event.clone();
            new_event.set(dest_field.to_string(), part);
            results.push(new_event);
        }

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
    async fn test_split_by_newline() {
        let settings = serde_json::json!({
            "field": "message",
            "separator": "\n"
        });
        let filter = SplitFilter::from_config(&settings, None).expect("config");
        let event = Event::new("line1\nline2\nline3");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].message(), Some("line1"));
        assert_eq!(result[1].message(), Some("line2"));
        assert_eq!(result[2].message(), Some("line3"));
    }

    #[tokio::test]
    async fn test_split_by_comma() {
        let settings = serde_json::json!({
            "field": "items",
            "separator": ","
        });
        let filter = SplitFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("items", EventValue::String("a,b,c".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0].get("items"),
            Some(&EventValue::String("a".into()))
        );
    }

    #[tokio::test]
    async fn test_split_array_field() {
        let settings = serde_json::json!({
            "field": "items"
        });
        let filter = SplitFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "items",
            EventValue::Array(vec![
                EventValue::String("x".into()),
                EventValue::String("y".into()),
            ]),
        );
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_split_with_target() {
        let settings = serde_json::json!({
            "field": "data",
            "separator": ",",
            "target": "item"
        });
        let filter = SplitFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("data", EventValue::String("a,b".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get("item"), Some(&EventValue::String("a".into())));
    }

    #[tokio::test]
    async fn test_split_missing_field() {
        let settings = serde_json::json!({ "field": "nonexistent" });
        let filter = SplitFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_split_name() {
        let settings = serde_json::json!({});
        let filter = SplitFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "split");
    }
}
