// SPDX-License-Identifier: Apache-2.0
//! JSON encode filter — encode a field or the entire event to a JSON string.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct JsonEncodeFilter {
    /// Source field to encode. If empty, encode the entire event.
    source: Option<String>,
    /// Target field to store the JSON string.
    target: String,
    condition: Option<Condition>,
}

impl JsonEncodeFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .map(String::from);

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("json_encoded")
            .to_string();

        Ok(Self {
            source,
            target,
            condition,
        })
    }

    /// Convert an EventValue to a serde_json::Value for serialization.
    fn event_value_to_json(val: &EventValue) -> serde_json::Value {
        serde_json::Value::from(val.clone())
    }
}

#[async_trait]
impl FilterPlugin for JsonEncodeFilter {
    fn name(&self) -> &'static str {
        "json_encode"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let json_string = if let Some(ref source) = self.source {
            // Encode specific field
            match event.get(source) {
                Some(val) => {
                    let json_val = Self::event_value_to_json(val);
                    serde_json::to_string(&json_val).unwrap_or_default()
                }
                None => return Ok(vec![event]),
            }
        } else {
            // Encode entire event
            let json_val = event.to_json();
            serde_json::to_string(&json_val).unwrap_or_default()
        };

        event.set(self.target.clone(), EventValue::String(json_string));

        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    #[tokio::test]
    async fn test_json_encode_field() {
        let settings = serde_json::json!({
            "source": "data",
            "target": "data_json"
        });
        let filter = JsonEncodeFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        let mut obj = IndexMap::new();
        obj.insert("key".to_string(), EventValue::String("value".into()));
        obj.insert("num".to_string(), EventValue::Integer(42));
        event.set("data", EventValue::Object(obj));
        let result = filter.filter(event).await.expect("filter");
        let encoded = result[0]
            .get("data_json")
            .expect("target")
            .as_str()
            .expect("string");
        assert!(encoded.contains("key"));
        assert!(encoded.contains("value"));
        assert!(encoded.contains("42"));
        // Verify it's valid JSON
        let _parsed: serde_json::Value = serde_json::from_str(encoded).expect("valid JSON");
    }

    #[tokio::test]
    async fn test_json_encode_entire_event() {
        let settings = serde_json::json!({
            "target": "event_json"
        });
        let filter = JsonEncodeFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        let encoded = result[0]
            .get("event_json")
            .expect("target")
            .as_str()
            .expect("string");
        assert!(encoded.contains("hello world"));
        assert!(encoded.contains("@timestamp"));
    }

    #[tokio::test]
    async fn test_json_encode_missing_source() {
        let settings = serde_json::json!({
            "source": "nonexistent",
            "target": "out"
        });
        let filter = JsonEncodeFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        // Should not create target field if source doesn't exist
        assert!(!result[0].has_field("out"));
    }

    #[tokio::test]
    async fn test_json_encode_string_value() {
        let settings = serde_json::json!({
            "source": "message",
            "target": "msg_json"
        });
        let filter = JsonEncodeFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello");
        let result = filter.filter(event).await.expect("filter");
        let encoded = result[0]
            .get("msg_json")
            .expect("target")
            .as_str()
            .expect("string");
        assert_eq!(encoded, "\"hello\"");
    }

    #[tokio::test]
    async fn test_json_encode_array() {
        let settings = serde_json::json!({
            "source": "items",
            "target": "items_json"
        });
        let filter = JsonEncodeFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "items",
            EventValue::Array(vec![EventValue::String("a".into()), EventValue::Integer(1)]),
        );
        let result = filter.filter(event).await.expect("filter");
        let encoded = result[0]
            .get("items_json")
            .expect("target")
            .as_str()
            .expect("string");
        assert!(encoded.contains("\"a\""));
        assert!(encoded.contains('1'));
    }

    #[test]
    fn test_json_encode_name() {
        let settings = serde_json::json!({});
        let filter = JsonEncodeFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "json_encode");
    }
}
