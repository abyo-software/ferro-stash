// SPDX-License-Identifier: Apache-2.0
//! Script filter — Painless-compatible native scripting.
//!
//! A native-Rust alternative to the Ruby filter, written in a subset of
//! Elasticsearch's Painless language. The script is parsed once at config time
//! and the cached AST is evaluated per event by `ferro-script`'s tree-walking
//! interpreter (no JVM, no per-event parse).
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
    /// The script parsed **once** at config time and reused for every event, so
    /// the hot path never re-parses the source (it previously parsed per event).
    /// `None` when the configured `code` is empty.
    program: Option<Vec<ferro_script::Stmt>>,
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

        // Parse once up front: a malformed script now fails fast at config load
        // instead of tagging every event with `_scripterror` at runtime.
        let program = if code.trim().is_empty() {
            None
        } else {
            Some(ferro_script::parse(&code).map_err(|e| {
                ferro_stash_core::error::FerroStashError::Config(format!(
                    "script filter: invalid Painless source: {e}"
                ))
            })?)
        };

        Ok(Self { program, condition })
    }
}

#[async_trait]
impl FilterPlugin for ScriptFilter {
    fn name(&self) -> &'static str {
        "script"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let Some(program) = self.program.as_ref() else {
            return Ok(vec![event]);
        };

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

        // Execute the pre-parsed script (no per-event re-parse). The evaluator
        // is a dynamic tree-walker over attacker-controlled event data; even
        // with the known panics fixed, contain any future panic with
        // `catch_unwind` so one crafted event tags-and-continues instead of
        // unwinding past this `filter` and aborting the whole filter worker
        // (which would permanently wedge the pipeline's filter stage).
        let eval = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ferro_script::evaluate_parsed(program, &mut ctx)
        }));
        match eval {
            Ok(Ok(_result)) => {
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
            Ok(Err(e)) => {
                warn!(error = %e, "script filter error");
                event.add_tag("_scripterror");
            }
            Err(_panic) => {
                warn!("script filter panicked on event data; tagging and continuing");
                event.add_tag("_scripterror");
                event.add_tag("_scriptpanic");
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

    #[tokio::test]
    async fn hostile_event_data_does_not_wedge_worker() {
        // A script that errors on crafted event data must tag-and-continue, not
        // unwind past filter() and abort the filter worker. The known panics are
        // fixed at the evaluator level (they now return Err → _scripterror); this
        // also exercises the catch_unwind containment path for any future panic.
        let settings = serde_json::json!({
            "code": "ctx._source.out = ctx._source.message.substring(0, 2)"
        });
        let filter = ScriptFilter::from_config(&settings, None).expect("config");
        // "€abc" — byte index 2 is inside the 3-byte '€'.
        let event = Event::new("€abc");
        let result = filter
            .filter(event)
            .await
            .expect("filter must not error out");
        assert_eq!(result.len(), 1, "event must survive (not be dropped)");
        assert!(
            result[0].has_tag("_scripterror"),
            "expected _scripterror tag on surviving event"
        );
    }
}
