// SPDX-License-Identifier: Apache-2.0
//! Script filter — Painless-compatible native scripting.
//!
//! Provides a high-performance alternative to the Ruby filter. Scripts are
//! written in a subset of Elasticsearch's Painless language and executed
//! natively in Rust (no interpreter overhead).
//!
//! ```logstash
//! filter {
//!   script {
//!     code => "ctx._source.upper = ctx._source.message.toUpperCase()"
//!   }
//! }
//! ```

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use tracing::warn;

#[derive(Debug)]
pub struct ScriptFilter {
    code: String,
    condition: Option<Condition>,
}

impl ScriptFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let code = settings
            .get("code")
            .or_else(|| settings.get("source"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        Ok(Self { code, condition })
    }
}

#[async_trait]
impl FilterPlugin for ScriptFilter {
    fn name(&self) -> &'static str {
        "script"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if self.code.is_empty() {
            return Ok(vec![event]);
        }

        // Build script context from event
        let mut ctx = ferro_script::ScriptContext::new();

        // Map event fields to ctx._source
        let mut source = serde_json::Map::new();
        for (k, v) in event.fields() {
            source.insert(k.clone(), serde_json::Value::from(v.clone()));
        }
        if let Some(msg) = event.message() {
            source.insert(
                "message".to_string(),
                serde_json::Value::String(msg.to_string()),
            );
        }
        ctx.source = serde_json::Value::Object(source);

        // Also populate doc for doc['field'].value access
        for (k, v) in event.fields() {
            ctx.doc
                .insert(k.clone(), serde_json::Value::from(v.clone()));
        }

        // Execute script
        match ferro_script::evaluate(&self.code, &mut ctx) {
            Ok(_result) => {
                // Apply changes from ctx._source back to event
                if let serde_json::Value::Object(src) = &ctx.source {
                    for (k, v) in src {
                        if k == "message" {
                            continue; // message is handled separately
                        }
                        event.set(k.clone(), EventValue::from(v.clone()));
                    }
                    // Update message if changed
                    if let Some(serde_json::Value::String(msg)) = src.get("message") {
                        event.set_message(msg.clone());
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "script filter error");
                event.add_tag("_scripterror");
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
    async fn test_script_uppercase() {
        let settings = serde_json::json!({
            "code": "ctx._source.upper = ctx._source.message.toUpperCase()"
        });
        let filter = ScriptFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("upper"),
            Some(&EventValue::String("HELLO WORLD".into()))
        );
    }

    #[tokio::test]
    async fn test_script_arithmetic() {
        let settings = serde_json::json!({
            "code": "ctx._source.doubled = ctx._source.count * 2"
        });
        let filter = ScriptFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("count", EventValue::Integer(21));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("doubled"), Some(&EventValue::Integer(42)));
    }

    #[tokio::test]
    async fn test_script_conditional() {
        let settings = serde_json::json!({
            "code": r#"if (ctx._source.message.contains("ERROR")) { ctx._source.level = "error" } else { ctx._source.level = "info" }"#
        });
        let filter = ScriptFilter::from_config(&settings, None).expect("config");
        let event = Event::new("2026-04-16 ERROR disk full");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("level"),
            Some(&EventValue::String("error".into()))
        );
    }

    #[tokio::test]
    async fn test_script_empty_code() {
        let settings = serde_json::json!({ "code": "" });
        let filter = ScriptFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("test"));
    }

    #[test]
    fn test_script_name() {
        let settings = serde_json::json!({ "code": "" });
        let filter = ScriptFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "script");
    }
}
