// SPDX-License-Identifier: Apache-2.0
//! URL-decode filter — decode percent-encoded field values.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use percent_encoding::percent_decode_str;

#[derive(Debug)]
pub struct UrldecodeFilter {
    field: Option<String>,
    all_fields: bool,
    condition: Option<Condition>,
}

impl UrldecodeFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let field = settings
            .get("field")
            .and_then(|v| v.as_str())
            .map(String::from);

        let all_fields = settings
            .get("all_fields")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self {
            field,
            all_fields,
            condition,
        })
    }
}

fn decode_value(val: &EventValue) -> EventValue {
    if let Some(s) = val.as_str() {
        let decoded = percent_decode_str(s).decode_utf8_lossy().to_string();
        EventValue::String(decoded)
    } else {
        val.clone()
    }
}

#[async_trait]
impl FilterPlugin for UrldecodeFilter {
    fn name(&self) -> &'static str {
        "urldecode"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if self.all_fields {
            let field_names: Vec<String> = event.field_names().cloned().collect();
            for name in field_names {
                if let Some(val) = event.get(&name).cloned() {
                    let decoded = decode_value(&val);
                    event.set(name, decoded);
                }
            }
        } else if let Some(ref field) = self.field {
            if let Some(val) = event.get(field).cloned() {
                let decoded = decode_value(&val);
                event.set(field.clone(), decoded);
            }
        } else {
            // Default: decode "message"
            if let Some(val) = event.get("message").cloned() {
                let decoded = decode_value(&val);
                event.set("message", decoded);
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
    async fn test_urldecode_field() {
        let settings = serde_json::json!({
            "field": "url"
        });
        let filter = UrldecodeFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "url",
            EventValue::String("hello%20world%26foo%3Dbar".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("url"),
            Some(&EventValue::String("hello world&foo=bar".into()))
        );
    }

    #[tokio::test]
    async fn test_urldecode_all_fields() {
        let settings = serde_json::json!({
            "all_fields": true
        });
        let filter = UrldecodeFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello%20world");
        event.set("path", EventValue::String("/a%2Fb".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hello world"));
        assert_eq!(
            result[0].get("path"),
            Some(&EventValue::String("/a/b".into()))
        );
    }

    #[tokio::test]
    async fn test_urldecode_default_message() {
        let settings = serde_json::json!({});
        let filter = UrldecodeFilter::from_config(&settings, None).expect("config");
        let event = Event::new("foo%3Dbar");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("foo=bar"));
    }

    #[tokio::test]
    async fn test_urldecode_already_decoded() {
        let settings = serde_json::json!({ "field": "msg" });
        let filter = UrldecodeFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("msg", EventValue::String("no encoding here".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("msg"),
            Some(&EventValue::String("no encoding here".into()))
        );
    }

    #[test]
    fn test_urldecode_name() {
        let settings = serde_json::json!({});
        let filter = UrldecodeFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "urldecode");
    }
}
