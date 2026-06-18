// SPDX-License-Identifier: Apache-2.0
//! KV (Key-Value) filter — extracts key=value pairs from text.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[allow(dead_code)]
#[derive(Debug)]
pub struct KvFilter {
    source: String,
    target: Option<String>,
    field_split: String,
    value_split: String,
    trim_key: Option<String>,
    trim_value: Option<String>,
    include_keys: Vec<String>,
    exclude_keys: Vec<String>,
    prefix: Option<String>,
    tag_on_failure: Vec<String>,
    condition: Option<Condition>,
}

impl KvFilter {
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
        let field_split = settings
            .get("field_split")
            .and_then(|v| v.as_str())
            .unwrap_or(" ")
            .to_string();
        let value_split = settings
            .get("value_split")
            .and_then(|v| v.as_str())
            .unwrap_or("=")
            .to_string();
        let trim_key = settings
            .get("trim_key")
            .and_then(|v| v.as_str())
            .map(String::from);
        let trim_value = settings
            .get("trim_value")
            .and_then(|v| v.as_str())
            .map(String::from);
        let include_keys = settings
            .get("include_keys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let exclude_keys = settings
            .get("exclude_keys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let prefix = settings
            .get("prefix")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["_kv_filter_error".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );

        Ok(Self {
            source,
            target,
            field_split,
            value_split,
            trim_key,
            trim_value,
            include_keys,
            exclude_keys,
            prefix,
            tag_on_failure,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for KvFilter {
    fn name(&self) -> &'static str {
        "kv"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let text = match event.get(&self.source) {
            Some(val) => val.to_string_lossy(),
            None => return Ok(vec![event]),
        };

        let pairs: Vec<&str> = text.split(&self.field_split).collect();

        for pair in pairs {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }

            if let Some((key, value)) = pair.split_once(&self.value_split) {
                let mut key = key.to_string();
                let mut value = value.to_string();

                // Trim
                if let Some(ref chars) = self.trim_key {
                    key = key.trim_matches(|c: char| chars.contains(c)).to_string();
                }
                if let Some(ref chars) = self.trim_value {
                    value = value.trim_matches(|c: char| chars.contains(c)).to_string();
                }

                // Strip quotes from value
                if (value.starts_with('"') && value.ends_with('"'))
                    || (value.starts_with('\'') && value.ends_with('\''))
                {
                    value = value[1..value.len() - 1].to_string();
                }

                // Include/exclude
                if !self.include_keys.is_empty() && !self.include_keys.contains(&key) {
                    continue;
                }
                if self.exclude_keys.contains(&key) {
                    continue;
                }

                // Prefix
                let field_name = if let Some(ref prefix) = self.prefix {
                    format!("{prefix}{key}")
                } else {
                    key
                };

                if let Some(ref target) = self.target {
                    event.set(format!("{target}.{field_name}"), EventValue::String(value));
                } else {
                    event.set(field_name, EventValue::String(value));
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
    async fn test_kv_basic() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("host=server01 port=8080 status=200");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("host"),
            Some(&EventValue::String("server01".into()))
        );
        assert_eq!(
            result[0].get("port"),
            Some(&EventValue::String("8080".into()))
        );
    }

    #[tokio::test]
    async fn test_kv_with_prefix() {
        let settings = serde_json::json!({
            "source": "message",
            "prefix": "kv_"
        });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("key1=val1 key2=val2");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("kv_key1"),
            Some(&EventValue::String("val1".into()))
        );
    }

    #[tokio::test]
    async fn test_kv_exclude_keys() {
        let settings = serde_json::json!({
            "source": "message",
            "exclude_keys": ["password"]
        });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("user=admin password=secret");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("user"));
        assert!(!result[0].has_field("password"));
    }

    #[tokio::test]
    async fn test_kv_custom_separators() {
        let settings = serde_json::json!({
            "source": "message",
            "field_split": "&",
            "value_split": "="
        });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("foo=bar&baz=qux");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("foo"),
            Some(&EventValue::String("bar".into()))
        );
        assert_eq!(
            result[0].get("baz"),
            Some(&EventValue::String("qux".into()))
        );
    }

    #[tokio::test]
    async fn test_kv_include_keys() {
        let settings = serde_json::json!({
            "source": "message",
            "include_keys": ["user"]
        });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("user=admin role=editor password=secret");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("user"));
        assert!(!result[0].has_field("role"));
        assert!(!result[0].has_field("password"));
    }

    #[tokio::test]
    async fn test_kv_with_target() {
        let settings = serde_json::json!({
            "source": "message",
            "target": "params"
        });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("key1=val1 key2=val2");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("params"));
    }

    #[tokio::test]
    async fn test_kv_quoted_values() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        // KV splits on spaces first, so use a value without spaces
        let event = Event::new(r#"name="Alice" age="30""#);
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("name"),
            Some(&EventValue::String("Alice".into()))
        );
        assert_eq!(result[0].get("age"), Some(&EventValue::String("30".into())));
    }

    #[tokio::test]
    async fn test_kv_missing_source() {
        let settings = serde_json::json!({ "source": "nonexistent" });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        // Should not fail, just pass through
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_kv_empty_value() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("key=");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("key"),
            Some(&EventValue::String(String::new()))
        );
    }

    #[tokio::test]
    async fn test_kv_trim_key() {
        let settings = serde_json::json!({
            "source": "message",
            "field_split": "&",
            "trim_key": "_"
        });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("_key_=value&_name_=alice");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("key"));
        assert!(result[0].has_field("name"));
    }

    #[test]
    fn test_kv_name() {
        let settings = serde_json::json!({});
        let filter = KvFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "kv");
    }

    #[tokio::test]
    async fn test_kv_no_pairs() {
        let settings = serde_json::json!({ "source": "message" });
        let filter = KvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("no key value pairs here");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 1);
    }
}
