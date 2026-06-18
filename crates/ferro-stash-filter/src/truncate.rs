// SPDX-License-Identifier: Apache-2.0
//! Truncate filter — truncate field values to a maximum byte length.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct TruncateFilter {
    fields: Vec<String>,
    length_bytes: usize,
    condition: Option<Condition>,
}

impl TruncateFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let fields = if let Some(arr) = settings.get("fields").and_then(|v| v.as_array()) {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else {
            // Default: truncate all string fields
            Vec::new()
        };

        let length_bytes = settings
            .get("length_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(400) as usize;

        Ok(Self {
            fields,
            length_bytes,
            condition,
        })
    }
}

/// Truncate a string to at most `max_bytes` bytes, ensuring we don't split a
/// multi-byte UTF-8 character.
fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    // Find the last char boundary at or before max_bytes
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[async_trait]
impl FilterPlugin for TruncateFilter {
    fn name(&self) -> &'static str {
        "truncate"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if self.fields.is_empty() {
            // Truncate all string fields
            let field_names: Vec<String> = event.field_names().cloned().collect();
            for name in field_names {
                if let Some(val) = event.get(&name) {
                    if let Some(s) = val.as_str() {
                        if s.len() > self.length_bytes {
                            let truncated = truncate_utf8(s, self.length_bytes).to_string();
                            event.set(name, EventValue::String(truncated));
                        }
                    }
                }
            }
        } else {
            for field in &self.fields {
                if let Some(val) = event.get(field) {
                    if let Some(s) = val.as_str() {
                        if s.len() > self.length_bytes {
                            let truncated = truncate_utf8(s, self.length_bytes).to_string();
                            event.set(field.clone(), EventValue::String(truncated));
                        }
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
    async fn test_truncate_basic() {
        let settings = serde_json::json!({
            "fields": ["message"],
            "length_bytes": 5
        });
        let filter = TruncateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hello"));
    }

    #[tokio::test]
    async fn test_truncate_no_op_when_short() {
        let settings = serde_json::json!({
            "fields": ["message"],
            "length_bytes": 100
        });
        let filter = TruncateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("short");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("short"));
    }

    #[tokio::test]
    async fn test_truncate_utf8_safe() {
        let settings = serde_json::json!({
            "fields": ["message"],
            "length_bytes": 4
        });
        let filter = TruncateFilter::from_config(&settings, None).expect("config");
        // Japanese chars are 3 bytes each in UTF-8
        let event = Event::new("\u{3042}\u{3044}\u{3046}"); // "あいう" = 9 bytes
        let result = filter.filter(event).await.expect("filter");
        let msg = result[0].message().expect("message");
        // Should truncate to 3 bytes (one char), not split a character
        assert_eq!(msg, "\u{3042}"); // "あ"
        assert!(msg.len() <= 4);
    }

    #[tokio::test]
    async fn test_truncate_all_fields() {
        let settings = serde_json::json!({
            "length_bytes": 3
        });
        let filter = TruncateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello");
        event.set("host", EventValue::String("server01".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hel"));
        assert_eq!(
            result[0].get("host"),
            Some(&EventValue::String("ser".into()))
        );
    }

    #[test]
    fn test_truncate_name() {
        let settings = serde_json::json!({});
        let filter = TruncateFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "truncate");
    }
}
