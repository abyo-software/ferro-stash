// SPDX-License-Identifier: Apache-2.0
//! Ruby filter — full Ruby execution via Artichoke interpreter.
//!
//! Provides complete Logstash Ruby filter compatibility including:
//! - Full Ruby language support (conditionals, loops, regex, closures, etc.)
//! - `LogStash::Event` compatible API (`get`, `set`, `remove`, `cancel`, `tag`, etc.)
//! - Nested field access via `[field][subfield]` syntax
//! - `init` block for one-time setup
//! - `path` option for external script files
//! - `new_event_block` for creating additional events
//! - Ruby stdlib (JSON, Time, Regexp, etc.)
//!
//! ## Residuals (honest limitations)
//!
//! - **Per-thread runtime cache is not evicted on reload.** Each OS worker thread
//!   lazily builds and caches its own Artichoke interpreter in the thread-local
//!   `RUBY_RUNTIMES` map, keyed by filter id (Artichoke is not `Send`/`Sync`, so
//!   one interpreter cannot be shared across threads). That cache is **not**
//!   evicted on `config.reload.automatic`: each reload mints a new filter id, so a
//!   long-lived process that reloads repeatedly slowly leaks one interpreter per
//!   reload × worker thread. This is acceptable for the common, non-reloading
//!   deployment; a proper fix needs pipeline-teardown hooks (out of scope here).
//! - **Interpreter state is per-OS-worker-thread, not global.** Because each
//!   worker thread has its own interpreter, instance/global variables (`@x`,
//!   `$x`) and other interpreter state accumulated by the `init`/`code` blocks are
//!   **per-thread**, not shared by one interpreter. Stateful Ruby accumulators
//!   (e.g. a running counter in `$x`) are therefore **non-deterministic** across
//!   threads — each thread keeps its own copy. Use the event itself (or an
//!   external store) for cross-event state instead of interpreter globals.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::FilterPlugin;
use ferro_stash_ruby::RubyRuntime;
use tracing::warn;

/// Unique id per `RubyFilter` instance, used to key the per-thread runtime cache.
static RUBY_FILTER_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Per-worker-thread Artichoke interpreters, keyed by filter id. Artichoke
    /// is not `Send`/`Sync`, so each worker thread builds and reuses its own
    /// interpreter — letting the ruby filter run in parallel across the
    /// pipeline's filter workers. The previous design shared a single
    /// `Mutex<RubyRuntime>`, which serialized every event through one
    /// interpreter, so adding workers gave no speedup at all.
    static RUBY_RUNTIMES: RefCell<HashMap<u64, RubyRuntime>> = RefCell::new(HashMap::new());
}

#[derive(Debug)]
pub struct RubyFilter {
    id: u64,
    /// Init code (event-bridge setup + user `init` + script `load`) used to
    /// lazily build one interpreter per worker thread.
    init_code: Option<String>,
    code: Option<String>,
    script_path: Option<String>,
    tag_on_exception: Vec<String>,
    tag_with_exception_message: bool,
    condition: Option<Condition>,
}

impl RubyFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let code = settings
            .get("code")
            .and_then(|v| v.as_str())
            .map(String::from);
        let init = settings
            .get("init")
            .and_then(|v| v.as_str())
            .map(String::from);
        let script_path = settings
            .get("path")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tag_on_exception = settings
            .get("tag_on_exception")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["_rubyexception".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );
        let tag_with_exception_message = settings
            .get("tag_with_exception_message")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Build init code: combine user init + script file loading
        let mut full_init = String::new();
        if let Some(ref init_code) = init {
            full_init.push_str(init_code);
            full_init.push('\n');
        }
        if let Some(ref path) = script_path {
            full_init.push_str(&format!("load '{path}'\n"));
        }

        let init_code = if full_init.is_empty() {
            None
        } else {
            Some(full_init)
        };

        // Validate the runtime builds (fail fast on bad init/script at config
        // load). The validated runtime is dropped; each worker thread builds its
        // own lazily in `filter`.
        RubyRuntime::new(init_code.clone()).map_err(|e| {
            ferro_stash_core::error::FerroStashError::Config(format!(
                "failed to initialize Ruby runtime: {e}"
            ))
        })?;

        Ok(Self {
            id: RUBY_FILTER_ID.fetch_add(1, Ordering::Relaxed),
            init_code,
            code,
            script_path,
            tag_on_exception,
            tag_with_exception_message,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for RubyFilter {
    fn name(&self) -> &'static str {
        "ruby"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if self.code.is_none() && self.script_path.is_none() {
            return Ok(vec![event]); // no code or script — pass through
        }

        // Execute on this worker thread's own interpreter (built once per thread,
        // keyed by filter id), so filter workers run Ruby in parallel.
        let result = RUBY_RUNTIMES.with(|cell| {
            let mut map = cell.borrow_mut();
            let runtime = match map.entry(self.id) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(RubyRuntime::new(self.init_code.clone())?)
                }
            };
            if let Some(ref code) = self.code {
                runtime.execute(code, &mut event)
            } else {
                runtime.execute_script(&mut event)
            }
        });

        match result {
            Ok(events) => Ok(events),
            Err(e) => {
                warn!(error = %e, "ruby filter error");
                for tag in &self.tag_on_exception {
                    event.add_tag(tag);
                }
                if self.tag_with_exception_message {
                    event.add_tag(format!("_rubyexception:{e}"));
                }
                Ok(vec![event])
            }
        }
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_stash_core::event::EventValue;

    #[tokio::test]
    async fn test_ruby_set() {
        let settings = serde_json::json!({
            "code": r#"event.set("status", 200)"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("status"), Some(&EventValue::Integer(200)));
    }

    #[tokio::test]
    async fn test_ruby_cancel() {
        let settings = serde_json::json!({ "code": "event.cancel" });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].is_cancelled());
    }

    #[tokio::test]
    async fn test_ruby_tag() {
        let settings = serde_json::json!({ "code": r#"event.tag("processed")"# });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("processed"));
    }

    #[tokio::test]
    async fn test_ruby_multiple_statements() {
        let settings = serde_json::json!({
            "code": r#"
                event.set("a", 1)
                event.set("b", "hello")
                event.tag("done")
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("a"), Some(&EventValue::Integer(1)));
        assert_eq!(
            result[0].get("b"),
            Some(&EventValue::String("hello".into()))
        );
        assert!(result[0].has_tag("done"));
    }

    #[tokio::test]
    async fn test_ruby_set_float() {
        let settings = serde_json::json!({
            "code": r#"event.set("pi", 2.5)"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("pi"), Some(&EventValue::Float(2.5)));
    }

    #[tokio::test]
    async fn test_ruby_set_boolean() {
        let settings = serde_json::json!({
            "code": r#"event.set("flag", true)"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("flag"), Some(&EventValue::Boolean(true)));
    }

    #[tokio::test]
    async fn test_ruby_set_null() {
        let settings = serde_json::json!({
            "code": r#"event.set("val", nil)"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("val"), Some(&EventValue::Null));
    }

    #[tokio::test]
    async fn test_ruby_remove_field() {
        let settings = serde_json::json!({
            "code": r#"event.remove("message")"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_field("message"));
    }

    #[tokio::test]
    async fn test_ruby_event_get_copy() {
        let settings = serde_json::json!({
            "code": r#"event.set("copy", event.get("message"))"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("copy"),
            Some(&EventValue::String("hello".into()))
        );
    }

    #[tokio::test]
    async fn test_ruby_empty_code() {
        let settings = serde_json::json!({ "code": "" });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("test"));
    }

    #[test]
    fn test_ruby_name() {
        let settings = serde_json::json!({ "code": "" });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "ruby");
    }

    #[tokio::test]
    async fn test_ruby_custom_exception_tag() {
        let settings = serde_json::json!({
            "code": "",
            "tag_on_exception": ["my_error"]
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        // Empty code should not produce exception
        assert!(!result[0].has_tag("my_error"));
    }

    // === New tests that leverage full Ruby capabilities ===

    #[tokio::test]
    async fn test_ruby_string_upcase() {
        let settings = serde_json::json!({
            "code": r#"event.set("upper", event.get("message").upcase)"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("upper"),
            Some(&EventValue::String("HELLO WORLD".into()))
        );
    }

    #[tokio::test]
    async fn test_ruby_if_else() {
        let settings = serde_json::json!({
            "code": r#"
                code = event.get("status_code")
                if code == 200
                  event.set("level", "info")
                elsif code == 404
                  event.set("level", "warn")
                else
                  event.set("level", "error")
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");

        let mut event = Event::new("test");
        event.set("status_code", EventValue::Integer(404));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("level"),
            Some(&EventValue::String("warn".into()))
        );
    }

    #[tokio::test]
    async fn test_ruby_regex_match() {
        let settings = serde_json::json!({
            "code": r#"
                msg = event.get("message")
                if msg =~ /ERROR/
                  event.set("is_error", true)
                else
                  event.set("is_error", false)
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("2026-04-15 ERROR disk full");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("is_error"), Some(&EventValue::Boolean(true)));
    }

    #[tokio::test]
    async fn test_ruby_gsub() {
        let settings = serde_json::json!({
            "code": r#"event.set("cleaned", event.get("message").gsub(/\d+/, "X"))"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("order 123 item 456");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("cleaned"),
            Some(&EventValue::String("order X item X".into()))
        );
    }

    #[tokio::test]
    async fn test_ruby_split_array() {
        let settings = serde_json::json!({
            "code": r#"
                parts = event.get("message").split(",")
                event.set("first", parts[0])
                event.set("count", parts.length)
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("a,b,c,d");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("first"),
            Some(&EventValue::String("a".into()))
        );
        assert_eq!(result[0].get("count"), Some(&EventValue::Integer(4)));
    }

    #[tokio::test]
    async fn test_ruby_init_block_with_helper() {
        let settings = serde_json::json!({
            "init": r#"
                def severity(code)
                  case code
                  when 0..299 then "info"
                  when 300..499 then "warn"
                  else "error"
                  end
                end
            "#,
            "code": r#"event.set("severity", severity(event.get("status_code")))"#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("status_code", EventValue::Integer(503));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("severity"),
            Some(&EventValue::String("error".into()))
        );
    }

    #[tokio::test]
    async fn test_ruby_nested_field_access() {
        let settings = serde_json::json!({
            "code": r#"
                event.set("[http][status]", 200)
                event.set("[http][method]", "GET")
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        let http = result[0].get("http");
        assert!(http.is_some());
    }

    #[tokio::test]
    async fn test_ruby_new_event_block() {
        let settings = serde_json::json!({
            "code": r#"
                cloned = event.clone
                cloned.set("message", "split")
                new_event_block.call(cloned)
                event.set("source", "original")
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].get("source"),
            Some(&EventValue::String("original".into()))
        );
        assert_eq!(result[1].message(), Some("split"));
    }

    #[tokio::test]
    async fn test_ruby_metadata() {
        let settings = serde_json::json!({
            "code": r#"
                event.set("[@metadata][index]", "my-logs")
                event.set("[@metadata][type]", "syslog")
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].metadata.get("index"),
            Some(&EventValue::String("my-logs".into()))
        );
        assert_eq!(
            result[0].metadata.get("type"),
            Some(&EventValue::String("syslog".into()))
        );
    }

    #[tokio::test]
    async fn test_ruby_exception_with_rescue() {
        let settings = serde_json::json!({
            "code": r#"raise "test error""#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        // The exception is caught by our wrapper, event gets tagged
        assert!(result[0].has_tag("_rubyexception"));
    }

    #[tokio::test]
    async fn test_ruby_loop_iteration() {
        let settings = serde_json::json!({
            "code": r#"
                sum = 0
                [1, 2, 3, 4, 5].each { |n| sum += n }
                event.set("sum", sum)
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("sum"), Some(&EventValue::Integer(15)));
    }

    // === Integration tests: real-world Logstash Ruby filter patterns ===

    #[tokio::test]
    async fn test_real_world_access_log_parsing() {
        // Nginx-style access log parsing with regex captures
        let settings = serde_json::json!({
            "code": r#"
                msg = event.get("message")
                captures = msg.scan(/^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+(\S+)\s+\S+"\s+(\d+)\s+(\d+)/)
                if captures.length > 0
                  fields = captures[0]
                  event.set("client_ip", fields[0])
                  event.set("timestamp_raw", fields[1])
                  event.set("method", fields[2])
                  event.set("path", fields[3])
                  event.set("status", fields[4].to_i)
                  event.set("bytes", fields[5].to_i)
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new(
            r#"192.168.1.1 - alice [15/Apr/2026:10:30:00 +0000] "GET /api/v1/users HTTP/1.1" 200 1234"#,
        );
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("client_ip"),
            Some(&EventValue::String("192.168.1.1".into()))
        );
        assert_eq!(
            result[0].get("method"),
            Some(&EventValue::String("GET".into()))
        );
        assert_eq!(
            result[0].get("path"),
            Some(&EventValue::String("/api/v1/users".into()))
        );
        assert_eq!(result[0].get("status"), Some(&EventValue::Integer(200)));
        assert_eq!(result[0].get("bytes"), Some(&EventValue::Integer(1234)));
    }

    #[tokio::test]
    async fn test_real_world_kv_extraction() {
        // Key=value parsing, common for syslog/structured logs
        let settings = serde_json::json!({
            "code": r#"
                event.get("message").scan(/(\w+)=([^\s]+)/).each do |k, v|
                  event.set(k, v)
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("user=alice action=login result=success ip=10.0.0.1");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("user"),
            Some(&EventValue::String("alice".into()))
        );
        assert_eq!(
            result[0].get("action"),
            Some(&EventValue::String("login".into()))
        );
        assert_eq!(
            result[0].get("result"),
            Some(&EventValue::String("success".into()))
        );
    }

    #[tokio::test]
    async fn test_real_world_conditional_enrichment() {
        // Enrichment based on field values, tag-based routing
        let settings = serde_json::json!({
            "init": r#"
                def classify_status(code)
                  case code
                  when 100..199 then "informational"
                  when 200..299 then "success"
                  when 300..399 then "redirect"
                  when 400..499 then "client_error"
                  when 500..599 then "server_error"
                  else "unknown"
                  end
                end
            "#,
            "code": r#"
                code = event.get("status_code")
                if code
                  category = classify_status(code)
                  event.set("status_category", category)
                  event.tag("error") if category.end_with?("_error")
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");

        let mut event1 = Event::new("test");
        event1.set("status_code", EventValue::Integer(503));
        let r1 = filter.filter(event1).await.expect("filter");
        assert_eq!(
            r1[0].get("status_category"),
            Some(&EventValue::String("server_error".into()))
        );
        assert!(r1[0].has_tag("error"));

        let mut event2 = Event::new("test");
        event2.set("status_code", EventValue::Integer(200));
        let r2 = filter.filter(event2).await.expect("filter");
        assert_eq!(
            r2[0].get("status_category"),
            Some(&EventValue::String("success".into()))
        );
        assert!(!r2[0].has_tag("error"));
    }

    #[tokio::test]
    async fn test_real_world_drop_noisy_events() {
        // Sample: drop debug-level events
        let settings = serde_json::json!({
            "code": r#"
                if event.get("level") == "DEBUG"
                  event.cancel
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");

        let mut event_debug = Event::new("test");
        event_debug.set("level", EventValue::String("DEBUG".into()));
        let r1 = filter.filter(event_debug).await.expect("filter");
        assert!(r1[0].is_cancelled());

        let mut event_info = Event::new("test");
        event_info.set("level", EventValue::String("INFO".into()));
        let r2 = filter.filter(event_info).await.expect("filter");
        assert!(!r2[0].is_cancelled());
    }

    #[tokio::test]
    async fn test_real_world_multi_line_split() {
        // Split multi-line stack traces into separate events
        let settings = serde_json::json!({
            "code": r#"
                lines = event.get("message").split("\n")
                event.set("message", lines[0])
                event.set("line_number", 0)
                lines.each_with_index do |line, idx|
                  next if idx == 0
                  e = event.clone
                  e.set("message", line)
                  e.set("line_number", idx)
                  new_event_block.call(e)
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("Exception occurred\n  at Foo.bar\n  at Bar.baz");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].message(), Some("Exception occurred"));
        assert_eq!(result[1].message(), Some("  at Foo.bar"));
        assert_eq!(result[2].message(), Some("  at Bar.baz"));
    }

    #[tokio::test]
    async fn test_real_world_array_max_min() {
        // Aggregation over a field that holds an array of numeric samples.
        let settings = serde_json::json!({
            "code": r#"
                samples = event.get("samples")
                event.set("max", samples.max)
                event.set("min", samples.min)
                event.set("range", samples.max - samples.min)
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "samples",
            EventValue::Array(vec![
                EventValue::Integer(42),
                EventValue::Integer(7),
                EventValue::Integer(99),
                EventValue::Integer(3),
                EventValue::Integer(50),
            ]),
        );
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("max"), Some(&EventValue::Integer(99)));
        assert_eq!(result[0].get("min"), Some(&EventValue::Integer(3)));
        assert_eq!(result[0].get("range"), Some(&EventValue::Integer(96)));
    }

    #[tokio::test]
    async fn test_real_world_array_zip() {
        // Pair parallel arrays (e.g. column names + values extracted from a log line).
        let settings = serde_json::json!({
            "code": r#"
                keys = ["user", "action", "result"]
                values = event.get("message").split(",")
                keys.zip(values).each { |k, v| event.set(k, v) }
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("alice,login,success");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("user"),
            Some(&EventValue::String("alice".into()))
        );
        assert_eq!(
            result[0].get("action"),
            Some(&EventValue::String("login".into()))
        );
        assert_eq!(
            result[0].get("result"),
            Some(&EventValue::String("success".into()))
        );
    }

    #[tokio::test]
    async fn test_real_world_filename_tail() {
        // File path → basename via String#rpartition on the last `/`.
        let settings = serde_json::json!({
            "code": r#"
                path = event.get("path")
                _, _, filename = path.rpartition("/")
                event.set("filename", filename)
                _, _, ext = filename.rpartition(".")
                event.set("ext", ext)
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "path",
            EventValue::String("/var/log/app/service.log".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("filename"),
            Some(&EventValue::String("service.log".into()))
        );
        assert_eq!(
            result[0].get("ext"),
            Some(&EventValue::String("log".into()))
        );
    }

    #[tokio::test]
    async fn test_real_world_character_quality_check() {
        // Counts alphanumerics, digits, and non-alphanumerics via String#count
        // — used for heuristic "is this field plausible" checks.
        let settings = serde_json::json!({
            "code": r#"
                msg = event.get("message")
                event.set("digits", msg.count("0-9"))
                event.set("alpha", msg.count("A-Za-z"))
                event.set("non_alnum", msg.count("^A-Za-z0-9"))
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("abc123 def456!");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("digits"), Some(&EventValue::Integer(6)));
        assert_eq!(result[0].get("alpha"), Some(&EventValue::Integer(6)));
        // non_alnum = space + `!` = 2
        assert_eq!(result[0].get("non_alnum"), Some(&EventValue::Integer(2)));
    }

    #[tokio::test]
    async fn test_real_world_sampling() {
        // Random sampling — used for "keep every N-th event" / reservoir
        // sampling Logstash patterns. The exact selection is random, but
        // we can assert on the count and membership constraints.
        let settings = serde_json::json!({
            "code": r#"
                all_candidates = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                event.set("picked", all_candidates.sample(3))
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        let picked = result[0]
            .get("picked")
            .and_then(|v| v.as_array())
            .expect("picked array");
        assert_eq!(picked.len(), 3);
        // Every picked value should be one of the candidates.
        for v in picked {
            let n = v.as_i64().expect("integer");
            assert!((1..=10).contains(&n), "{n} not in candidates");
        }
        // All picks should be distinct.
        let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for v in picked {
            seen.insert(v.as_i64().expect("integer"));
        }
        assert_eq!(seen.len(), 3, "sample(3) should return distinct elements");
    }

    #[tokio::test]
    async fn test_real_world_date_parse() {
        // Date.parse — the most common require 'date' use case in
        // Logstash Ruby filters for normalising timestamp strings.
        let settings = serde_json::json!({
            "code": r#"
                require 'date'
                d = Date.parse(event.get("date_str"))
                event.set("year", d.year)
                event.set("month", d.month)
                event.set("day", d.day)
                event.set("iso", d.to_s)
                event.set("day_name", d.strftime("%A"))
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("date_str", EventValue::String("2026-04-16".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("year"), Some(&EventValue::Integer(2026)));
        assert_eq!(result[0].get("month"), Some(&EventValue::Integer(4)));
        assert_eq!(result[0].get("day"), Some(&EventValue::Integer(16)));
        assert_eq!(
            result[0].get("iso"),
            Some(&EventValue::String("2026-04-16".into()))
        );
    }

    #[tokio::test]
    async fn test_real_world_date_arithmetic() {
        // Date arithmetic — computing retention windows, SLA deadlines.
        let settings = serde_json::json!({
            "code": r#"
                require 'date'
                today = Date.parse(event.get("today"))
                created = Date.parse(event.get("created"))
                event.set("age_days", today - created)
                event.set("expires", (today + 30).to_s)
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("today", EventValue::String("2026-04-16".into()));
        event.set("created", EventValue::String("2026-04-06".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("age_days"), Some(&EventValue::Integer(10)));
        assert_eq!(
            result[0].get("expires"),
            Some(&EventValue::String("2026-05-16".into()))
        );
    }

    #[tokio::test]
    async fn test_real_world_timestamp_construction() {
        // Build a Time from year/month/day fields — often needed to
        // normalise dates from grok-parsed log lines.
        let settings = serde_json::json!({
            "code": r#"
                y = event.get("year").to_i
                m = event.get("month").to_i
                d = event.get("day").to_i
                t = Time.new(y, m, d, 12, 0, 0)
                event.set("normalized_year", t.year)
                event.set("normalized_month", t.month)
                event.set("normalized_day", t.day)
                event.set("normalized_hour", t.hour)
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("year", EventValue::String("2026".into()));
        event.set("month", EventValue::String("4".into()));
        event.set("day", EventValue::String("15".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("normalized_year"),
            Some(&EventValue::Integer(2026))
        );
        assert_eq!(
            result[0].get("normalized_month"),
            Some(&EventValue::Integer(4))
        );
        assert_eq!(
            result[0].get("normalized_day"),
            Some(&EventValue::Integer(15))
        );
        assert_eq!(
            result[0].get("normalized_hour"),
            Some(&EventValue::Integer(12))
        );
    }

    #[tokio::test]
    async fn test_real_world_timebucket_hash_key() {
        // Dedup / rate-limit filters commonly use `Time` as a Hash key
        // bucket. Exercises the new `Time#hash` implementation — without
        // it, this pattern would explode with NotImplementedError.
        let settings = serde_json::json!({
            "init": r#"
                $seen = {}
            "#,
            "code": r#"
                bucket = Time.utc(2026, 4, 16, event.get("hour").to_i)
                if $seen[bucket]
                  event.set("is_duplicate", true)
                else
                  $seen[bucket] = 1
                  event.set("is_duplicate", false)
                end
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");

        let mut first = Event::new("test");
        first.set("hour", EventValue::String("12".into()));
        let r1 = filter.filter(first).await.expect("filter");
        assert_eq!(r1[0].get("is_duplicate"), Some(&EventValue::Boolean(false)));

        let mut second = Event::new("test");
        second.set("hour", EventValue::String("12".into()));
        let r2 = filter.filter(second).await.expect("filter");
        assert_eq!(r2[0].get("is_duplicate"), Some(&EventValue::Boolean(true)));

        let mut third = Event::new("test");
        third.set("hour", EventValue::String("13".into()));
        let r3 = filter.filter(third).await.expect("filter");
        assert_eq!(r3[0].get("is_duplicate"), Some(&EventValue::Boolean(false)));
    }

    #[tokio::test]
    async fn test_real_world_top_n_latencies() {
        // Top-N / bottom-N via the new `max(n)` / `min(n)` overloads.
        let settings = serde_json::json!({
            "code": r#"
                latencies = event.get("latencies")
                event.set("top3", latencies.max(3))
                event.set("bottom3", latencies.min(3))
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "latencies",
            EventValue::Array(vec![
                EventValue::Integer(120),
                EventValue::Integer(45),
                EventValue::Integer(999),
                EventValue::Integer(8),
                EventValue::Integer(300),
                EventValue::Integer(50),
            ]),
        );
        let result = filter.filter(event).await.expect("filter");
        let top3 = result[0]
            .get("top3")
            .and_then(|v| v.as_array())
            .expect("top3");
        assert_eq!(top3.len(), 3);
        assert_eq!(top3[0], EventValue::Integer(999));
        assert_eq!(top3[1], EventValue::Integer(300));
        assert_eq!(top3[2], EventValue::Integer(120));
        let bottom3 = result[0]
            .get("bottom3")
            .and_then(|v| v.as_array())
            .expect("bottom3");
        assert_eq!(bottom3[0], EventValue::Integer(8));
        assert_eq!(bottom3[1], EventValue::Integer(45));
        assert_eq!(bottom3[2], EventValue::Integer(50));
    }

    #[tokio::test]
    async fn test_real_world_sequential_id_generation() {
        // `String#succ` / `#next` for identifier sequences — common in
        // stateful Logstash filters that mint bucket IDs.
        let settings = serde_json::json!({
            "init": r#"
                $next_id = 'AA00'
            "#,
            "code": r#"
                event.set("id", $next_id)
                $next_id = $next_id.succ
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");

        let r1 = filter.filter(Event::new("test")).await.expect("filter");
        let r2 = filter.filter(Event::new("test")).await.expect("filter");
        let r3 = filter.filter(Event::new("test")).await.expect("filter");
        assert_eq!(r1[0].get("id"), Some(&EventValue::String("AA00".into())));
        assert_eq!(r2[0].get("id"), Some(&EventValue::String("AA01".into())));
        assert_eq!(r3[0].get("id"), Some(&EventValue::String("AA02".into())));
    }

    #[tokio::test]
    async fn test_real_world_squeeze_log_cleanup() {
        // Squeeze excessive whitespace in log messages — a common
        // normalisation step before indexing.
        let settings = serde_json::json!({
            "code": r#"
                msg = event.get("message")
                event.set("normalized", msg.squeeze(" "))
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("ERROR:   multiple   spaces    here");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("normalized"),
            Some(&EventValue::String("ERROR: multiple spaces here".into()))
        );
    }

    #[tokio::test]
    async fn test_real_world_split_with_block() {
        // Split-with-block to process each token in-place without
        // building an intermediate array (CRuby 3.x feature).
        let settings = serde_json::json!({
            "code": r#"
                count = 0
                event.get("message").split(",") { |token| count += 1 }
                event.set("token_count", count)
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("a,b,c,d,e");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("token_count"), Some(&EventValue::Integer(5)));
    }

    #[tokio::test]
    async fn test_real_world_type_coercion() {
        // Common: coerce string fields from grok/dissect into proper types
        let settings = serde_json::json!({
            "code": r#"
                event.set("status", event.get("status_str").to_i) if event.get("status_str")
                event.set("duration_ms", (event.get("duration_sec").to_f * 1000).to_i) if event.get("duration_sec")
                event.set("size_kb", event.get("size_bytes").to_f / 1024.0) if event.get("size_bytes")
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("status_str", EventValue::String("200".into()));
        event.set("duration_sec", EventValue::String("0.456".into()));
        event.set("size_bytes", EventValue::String("2048".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("status"), Some(&EventValue::Integer(200)));
        assert_eq!(
            result[0].get("duration_ms"),
            Some(&EventValue::Integer(456))
        );
        // size_kb is a Float
        let kb = result[0]
            .get("size_kb")
            .and_then(|v| v.as_f64())
            .expect("size_kb");
        assert!((kb - 2.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_ruby_map_select() {
        let settings = serde_json::json!({
            "code": r#"
                nums = [1, 2, 3, 4, 5, 6]
                evens = nums.select { |n| n % 2 == 0 }
                event.set("evens_count", evens.length)
                doubled = nums.map { |n| n * 2 }
                event.set("first_doubled", doubled[0])
            "#
        });
        let filter = RubyFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("evens_count"), Some(&EventValue::Integer(3)));
        assert_eq!(
            result[0].get("first_doubled"),
            Some(&EventValue::Integer(2))
        );
    }
}
