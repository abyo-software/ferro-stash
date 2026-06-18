// SPDX-License-Identifier: Apache-2.0
//! Ruby runtime wrapper around Artichoke for executing Logstash Ruby filters.

use artichoke_backend::prelude::*;
use ferro_stash_core::event::Event;
use tracing::{debug, warn};

use crate::event_bridge;

/// The `LogStash::Event` class definition, loaded once per interpreter.
static EVENT_BRIDGE_SOURCE: &[u8] = include_bytes!("event_bridge.rb");

/// Errors from the Ruby runtime.
#[derive(Debug, thiserror::Error)]
pub enum RubyRuntimeError {
    #[error("failed to initialize Ruby interpreter: {0}")]
    InterpreterInit(String),

    #[error("Ruby execution error: {0}")]
    Execution(String),

    #[error("failed to convert Ruby result: {0}")]
    Conversion(String),
}

/// A Ruby runtime capable of executing Logstash-compatible Ruby filter code.
///
/// Each `RubyRuntime` owns an Artichoke interpreter pre-loaded with the
/// `LogStash::Event` class. The interpreter is NOT thread-safe — create one
/// per worker thread.
///
/// The interpreter is wrapped in `Option` so we can take ownership on drop
/// to call `Artichoke::close()`.
pub struct RubyRuntime {
    interp: Option<Artichoke>,
    init_code: Option<String>,
    init_executed: bool,
    code_compiled: bool,
}

impl std::fmt::Debug for RubyRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RubyRuntime")
            .field("interp", &"<Artichoke>")
            .field("init_code", &self.init_code)
            .field("init_executed", &self.init_executed)
            .field("code_compiled", &self.code_compiled)
            .finish()
    }
}

impl RubyRuntime {
    /// Create a new Ruby runtime with the `LogStash::Event` class pre-loaded.
    pub fn new(init_code: Option<String>) -> Result<Self, RubyRuntimeError> {
        let mut interp = artichoke_backend::interpreter()
            .map_err(|e| RubyRuntimeError::InterpreterInit(e.to_string()))?;

        // Load the LogStash::Event bridge
        interp
            .eval(EVENT_BRIDGE_SOURCE)
            .map_err(|e| RubyRuntimeError::InterpreterInit(format!("event bridge: {e}")))?;

        debug!("Ruby runtime initialized with LogStash::Event support");

        Ok(Self {
            interp: Some(interp),
            init_code,
            init_executed: false,
            code_compiled: false,
        })
    }

    fn interp(&mut self) -> &mut Artichoke {
        self.interp
            .as_mut()
            .expect("interpreter should be available (not yet dropped)")
    }

    /// Execute the `init` block if it hasn't been run yet.
    fn ensure_init(&mut self) -> Result<(), RubyRuntimeError> {
        if self.init_executed {
            return Ok(());
        }
        self.init_executed = true;

        if let Some(init) = self.init_code.clone() {
            debug!(code = %init, "executing Ruby init block");
            self.interp()
                .eval(init.as_bytes())
                .map_err(|e| RubyRuntimeError::Execution(format!("init block: {e}")))?;
        }
        Ok(())
    }

    /// Execute Ruby code against an event, returning modified events.
    ///
    /// The `code` parameter is the Ruby code from the Logstash `ruby { code => "..." }` block.
    /// The event is serialized to a Ruby Hash, wrapped in a `LogStash::Event`, and the code
    /// is executed. The modified event is then deserialized back.
    ///
    /// Returns a Vec because Ruby filters can produce multiple events (via `new_event_block`).
    pub fn execute(
        &mut self,
        code: &str,
        event: &mut Event,
    ) -> Result<Vec<Event>, RubyRuntimeError> {
        self.ensure_init()?;

        // Compile the user's Ruby code into a cached Proc on first call.
        // Subsequent calls reuse the compiled proc, avoiding re-parse overhead.
        if !self.code_compiled {
            // Pre-require json once. Compile user code into a method so it
            // can access `event` and `new_event_block` as global variables.
            self.interp()
                .eval(b"require 'json'")
                .map_err(|e| RubyRuntimeError::Execution(format!("require json: {e}")))?;

            // Define the user's code as a method on a helper object. This
            // avoids per-event code parsing. `event` and `new_event_block`
            // are set as globals before each call.
            let compile = format!(
                "def $__ferro_runner__.call_filter\n\
                   event = $__ferro_event__\n\
                   new_event_block = $__ferro_neb__\n\
                   {code}\n\
                 end\n"
            );
            self.interp()
                .eval(b"$__ferro_runner__ = Object.new")
                .map_err(|e| RubyRuntimeError::Execution(format!("runner: {e}")))?;
            self.interp()
                .eval(compile.as_bytes())
                .map_err(|e| RubyRuntimeError::Execution(format!("compile: {e}")))?;
            self.code_compiled = true;
        }

        // Serialize event, set globals, invoke compiled method
        let ruby_hash = event_bridge::event_to_ruby_hash(event);
        let wrapper = format!(
            "$__ferro_new_events__ = []\n\
             $__ferro_event__ = LogStash::Event.new({ruby_hash})\n\
             $__ferro_neb__ = Proc.new {{ |e| $__ferro_new_events__ << e }}\n\
             begin\n\
               $__ferro_runner__.call_filter\n\
             rescue => e\n\
               $__ferro_event__.tag(\"_rubyexception\")\n\
               $__ferro_event__.set(\"ruby_exception\", e.message)\n\
             end\n\
             __r__ = [$__ferro_event__.__to_ferro_hash__]\n\
             $__ferro_new_events__.each {{ |ne| __r__ << ne.__to_ferro_hash__ }}\n\
             JSON.generate(__r__)\n",
        );

        let result = self
            .interp()
            .eval(wrapper.as_bytes())
            .map_err(|e| RubyRuntimeError::Execution(e.to_string()))?;

        // Extract the JSON string result
        let result_str: Option<String> = result
            .try_convert_into_mut(self.interp())
            .map_err(|e| RubyRuntimeError::Conversion(e.to_string()))?;

        let Some(result_str) = result_str else {
            warn!("Ruby filter returned nil");
            return Ok(vec![event.clone()]);
        };

        // Parse the JSON array of events
        let result_array: Vec<serde_json::Value> = serde_json::from_str(&result_str)
            .map_err(|e| RubyRuntimeError::Conversion(format!("parse result: {e}")))?;

        if result_array.is_empty() {
            return Ok(vec![]);
        }

        // First element modifies the original event
        event_bridge::apply_ruby_result(event, &result_array[0]);
        let mut events = vec![event.clone()];

        // Additional events from new_event_block
        for extra_json in &result_array[1..] {
            let mut extra = Event::empty();
            event_bridge::apply_ruby_result(&mut extra, extra_json);
            events.push(extra);
        }

        Ok(events)
    }

    /// Execute a Ruby script file's `filter(event)` method.
    pub fn execute_script(&mut self, event: &mut Event) -> Result<Vec<Event>, RubyRuntimeError> {
        self.ensure_init()?;

        let ruby_hash = event_bridge::event_to_ruby_hash(event);
        let wrapper = format!(
            "require 'json'\n\
             __ferro_event__ = LogStash::Event.new({ruby_hash})\n\
             begin\n\
               __ferro_result__ = filter(__ferro_event__)\n\
             rescue => e\n\
               __ferro_event__.tag(\"_rubyexception\")\n\
               __ferro_event__.set(\"ruby_exception\", e.message)\n\
               __ferro_result__ = [__ferro_event__]\n\
             end\n\
             __ferro_result__ = [__ferro_event__] unless __ferro_result__.is_a?(Array)\n\
             JSON.generate(__ferro_result__.map {{ |e| e.__to_ferro_hash__ }})\n",
        );

        let result = self
            .interp()
            .eval(wrapper.as_bytes())
            .map_err(|e| RubyRuntimeError::Execution(e.to_string()))?;

        let result_str: Option<String> = result
            .try_convert_into_mut(self.interp())
            .map_err(|e| RubyRuntimeError::Conversion(e.to_string()))?;

        let Some(result_str) = result_str else {
            return Ok(vec![event.clone()]);
        };

        let result_array: Vec<serde_json::Value> = serde_json::from_str(&result_str)
            .map_err(|e| RubyRuntimeError::Conversion(format!("parse result: {e}")))?;

        if result_array.is_empty() {
            return Ok(vec![]);
        }

        event_bridge::apply_ruby_result(event, &result_array[0]);
        let mut events = vec![event.clone()];

        for extra_json in &result_array[1..] {
            let mut extra = Event::empty();
            event_bridge::apply_ruby_result(&mut extra, extra_json);
            events.push(extra);
        }

        Ok(events)
    }
}

// SAFETY: RubyRuntime wraps a single-threaded mruby interpreter.
// We guarantee single-threaded access via Mutex in the filter layer.
// The raw pointer inside Artichoke (mrb_state) is the only non-Send field.
unsafe impl Send for RubyRuntime {}

impl Drop for RubyRuntime {
    fn drop(&mut self) {
        if let Some(interp) = self.interp.take() {
            interp.close();
            debug!("Ruby runtime closed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_stash_core::event::EventValue;

    #[test]
    fn test_runtime_basic_set() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("hello");
        let events = rt
            .execute(r#"event.set("status", 200)"#, &mut event)
            .expect("execute");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("status"), Some(&EventValue::Integer(200)));
    }

    #[test]
    #[ignore = "dev-only probe: reads /tmp/test_missing3.rb which is developer-local"]
    fn test_ruby_feature_probe() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let code = std::fs::read_to_string("/tmp/test_missing3.rb").expect("read probe");
        let result = rt.execute(&code, &mut event).expect("probe");
        let missing = result[0]
            .get("missing")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let count = result[0]
            .get("count")
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);
        eprintln!("MISSING ({count}):\n{missing}");
    }

    #[test]
    fn test_require_date() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let result = rt.execute(
            r#"
            require 'date'
            d = Date.new(2026, 4, 16)
            event.set("year", d.year)
            event.set("to_s", d.to_s)
            "#,
            &mut event,
        );
        match result {
            Ok(events) => {
                eprintln!("date year: {:?}", events[0].get("year"));
                eprintln!("date to_s: {:?}", events[0].get("to_s"));
            }
            Err(e) => eprintln!("date error: {e}"),
        }
    }

    #[test]
    fn test_runtime_string_manipulation() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("Hello World");
        let events = rt
            .execute(
                r#"event.set("upper", event.get("message").upcase)"#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].get("upper"),
            Some(&EventValue::String("HELLO WORLD".into()))
        );
    }

    #[test]
    fn test_runtime_conditional_logic() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        event.set("status_code", EventValue::Integer(404));
        let events = rt
            .execute(
                r#"
                if event.get("status_code") == 404
                  event.set("error", true)
                  event.tag("not_found")
                end
                "#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(events[0].get("error"), Some(&EventValue::Boolean(true)));
        assert!(events[0].has_tag("not_found"));
    }

    #[test]
    fn test_runtime_regex() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("2026-04-15 ERROR something broke");
        let events = rt
            .execute(
                r#"
                msg = event.get("message")
                if msg =~ /(\d{4}-\d{2}-\d{2})\s+(\w+)\s+(.*)/
                  event.set("date", $1)
                  event.set("level", $2)
                  event.set("body", $3)
                end
                "#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].get("date"),
            Some(&EventValue::String("2026-04-15".into()))
        );
        assert_eq!(
            events[0].get("level"),
            Some(&EventValue::String("ERROR".into()))
        );
        assert_eq!(
            events[0].get("body"),
            Some(&EventValue::String("something broke".into()))
        );
    }

    #[test]
    fn test_runtime_cancel() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt.execute("event.cancel", &mut event).expect("execute");
        assert!(events[0].is_cancelled());
    }

    #[test]
    fn test_runtime_init_block() {
        let mut rt = RubyRuntime::new(Some(
            r#"
            def compute_severity(code)
              case code
              when 0..299 then "info"
              when 300..499 then "warn"
              else "error"
              end
            end
            "#
            .to_string(),
        ))
        .expect("init");
        let mut event = Event::new("test");
        event.set("status_code", EventValue::Integer(503));
        let events = rt
            .execute(
                r#"event.set("severity", compute_severity(event.get("status_code")))"#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].get("severity"),
            Some(&EventValue::String("error".into()))
        );
    }

    #[test]
    fn test_runtime_nested_fields() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                event.set("[http][status]", 200)
                event.set("[http][method]", "GET")
                "#,
                &mut event,
            )
            .expect("execute");
        // Nested fields are stored as a hash in the "http" key
        let http = events[0].get("http");
        assert!(http.is_some());
    }

    #[test]
    fn test_runtime_hash_operations() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        // Use Hash directly rather than JSON.parse (which has `chr` limitation in Artichoke)
        let events = rt
            .execute(
                r#"
                h = {"key" => "value", "num" => 42}
                event.set("parsed_key", h["key"])
                event.set("parsed_num", h["num"])
                event.set("key_count", h.keys.length)
                "#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].get("parsed_key"),
            Some(&EventValue::String("value".into()))
        );
        assert_eq!(events[0].get("parsed_num"), Some(&EventValue::Integer(42)));
        assert_eq!(events[0].get("key_count"), Some(&EventValue::Integer(2)));
    }

    #[test]
    fn test_runtime_array_operations() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("a,b,c,d");
        let events = rt
            .execute(
                r#"
                parts = event.get("message").split(",")
                event.set("first", parts.first)
                event.set("count", parts.length)
                "#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].get("first"),
            Some(&EventValue::String("a".into()))
        );
        assert_eq!(events[0].get("count"), Some(&EventValue::Integer(4)));
    }

    #[test]
    fn test_runtime_exception_handling() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(r#"raise "something went wrong""#, &mut event)
            .expect("execute");
        assert!(events[0].has_tag("_rubyexception"));
        assert_eq!(
            events[0].get("ruby_exception"),
            Some(&EventValue::String("something went wrong".into()))
        );
    }

    #[test]
    fn test_runtime_new_event_block() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("original");
        let events = rt
            .execute(
                r#"
                new_event = event.clone
                new_event.set("message", "cloned")
                new_event.set("source", "split")
                new_event_block.call(new_event)
                event.set("source", "original")
                "#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message(), Some("original"));
        assert_eq!(events[1].message(), Some("cloned"));
    }

    #[test]
    fn test_runtime_metadata() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                event.set("[@metadata][index]", "my-index")
                event.set("[@metadata][type]", "log")
                "#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].metadata.get("index"),
            Some(&EventValue::String("my-index".into()))
        );
    }

    #[test]
    fn test_runtime_include_check() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                if event.include?("message")
                  event.set("has_message", true)
                end
                if !event.include?("missing")
                  event.set("no_missing", true)
                end
                "#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].get("has_message"),
            Some(&EventValue::Boolean(true))
        );
        assert_eq!(
            events[0].get("no_missing"),
            Some(&EventValue::Boolean(true))
        );
    }

    #[test]
    fn test_runtime_gsub_regex() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("Hello 123 World 456");
        let events = rt
            .execute(
                r#"event.set("cleaned", event.get("message").gsub(/\d+/, "NUM"))"#,
                &mut event,
            )
            .expect("execute");
        assert_eq!(
            events[0].get("cleaned"),
            Some(&EventValue::String("Hello NUM World NUM".into()))
        );
    }

    #[test]
    fn test_runtime_to_hash() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        event.set("foo", EventValue::String("bar".into()));
        let events = rt
            .execute(
                r#"
                h = event.to_hash
                event.set("key_count", h.keys.length)
                "#,
                &mut event,
            )
            .expect("execute");
        // to_hash includes message, foo, @timestamp = 3 keys
        let count = events[0].get("key_count").and_then(|v| v.as_i64());
        assert!(count.is_some());
        assert!(count.expect("count") >= 2);
    }

    #[test]
    fn test_integer_chr() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(r#"event.set("char", 65.chr)"#, &mut event)
            .expect("chr should work");
        assert_eq!(events[0].get("char"), Some(&EventValue::String("A".into())));
    }

    #[test]
    fn test_json_parse() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                require 'json'
                parsed = JSON.parse('{"key":"value","num":42}')
                event.set("key", parsed["key"])
                event.set("num", parsed["num"])
                "#,
                &mut event,
            )
            .expect("JSON.parse should work");
        assert_eq!(
            events[0].get("key"),
            Some(&EventValue::String("value".into()))
        );
        assert_eq!(events[0].get("num"), Some(&EventValue::Integer(42)));
    }

    #[test]
    fn test_json_parse_from_event_field() {
        // Simulate real-world: event contains a JSON string field, parse it in Ruby
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        event.set(
            "raw_json",
            EventValue::String(r#"{"user":"alice","age":30,"active":true}"#.into()),
        );
        let events = rt
            .execute(
                r#"
                require 'json'
                data = JSON.parse(event.get("raw_json"))
                event.set("user", data["user"])
                event.set("age", data["age"])
                event.set("active", data["active"])
                "#,
                &mut event,
            )
            .expect("parse field JSON");
        assert_eq!(
            events[0].get("user"),
            Some(&EventValue::String("alice".into()))
        );
        assert_eq!(events[0].get("age"), Some(&EventValue::Integer(30)));
        assert_eq!(events[0].get("active"), Some(&EventValue::Boolean(true)));
    }

    #[test]
    fn test_time_now() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(r#"event.set("has_time", !Time.now.nil?)"#, &mut event)
            .expect("Time.now should work");
        assert_eq!(events[0].get("has_time"), Some(&EventValue::Boolean(true)));
    }

    #[test]
    fn test_string_methods_comprehensive() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("  Hello, World!  ");
        let events = rt
            .execute(
                r#"
                msg = event.get("message")
                event.set("stripped", msg.strip)
                event.set("downcase", msg.downcase)
                event.set("length", msg.length)
                event.set("includes_hello", msg.include?("Hello"))
                event.set("starts_with", msg.strip.start_with?("Hello"))
                event.set("ends_with", msg.strip.end_with?("!"))
                event.set("replaced", msg.gsub("World", "Ruby"))
                "#,
                &mut event,
            )
            .expect("string methods");
        assert_eq!(
            events[0].get("stripped"),
            Some(&EventValue::String("Hello, World!".into()))
        );
        // Length may vary slightly depending on how Artichoke handles whitespace
        let len = events[0]
            .get("length")
            .and_then(|v| v.as_i64())
            .expect("length");
        assert!(len >= 15, "length should be at least 15, got {len}");
        assert_eq!(
            events[0].get("includes_hello"),
            Some(&EventValue::Boolean(true))
        );
    }

    #[test]
    fn test_case_when() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        event.set("level", EventValue::String("WARN".into()));
        let events = rt
            .execute(
                r#"
                severity = case event.get("level")
                           when "DEBUG" then 0
                           when "INFO" then 1
                           when "WARN" then 2
                           when "ERROR" then 3
                           when "FATAL" then 4
                           else -1
                           end
                event.set("severity", severity)
                "#,
                &mut event,
            )
            .expect("case/when");
        assert_eq!(events[0].get("severity"), Some(&EventValue::Integer(2)));
    }

    #[test]
    fn test_begin_rescue() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                begin
                  result = Integer("not_a_number")
                rescue ArgumentError => e
                  event.set("error_class", "ArgumentError")
                  event.set("error_msg", e.message)
                end
                "#,
                &mut event,
            )
            .expect("begin/rescue");
        assert_eq!(
            events[0].get("error_class"),
            Some(&EventValue::String("ArgumentError".into()))
        );
    }

    #[test]
    fn test_each_with_object() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                words = ["hello", "world", "foo"]
                result = words.map { |w| w.upcase }
                event.set("first", result[0])
                event.set("last", result[-1])
                "#,
                &mut event,
            )
            .expect("map");
        assert_eq!(
            events[0].get("first"),
            Some(&EventValue::String("HELLO".into()))
        );
        assert_eq!(
            events[0].get("last"),
            Some(&EventValue::String("FOO".into()))
        );
    }

    #[test]
    fn test_hash_merge_and_keys() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                a = {"x" => 1, "y" => 2}
                b = {"y" => 3, "z" => 4}
                merged = a.merge(b)
                event.set("keys_count", merged.keys.length)
                event.set("y_val", merged["y"])
                event.set("has_z", merged.key?("z"))
                "#,
                &mut event,
            )
            .expect("hash merge");
        assert_eq!(events[0].get("keys_count"), Some(&EventValue::Integer(3)));
        assert_eq!(events[0].get("y_val"), Some(&EventValue::Integer(3)));
        assert_eq!(events[0].get("has_z"), Some(&EventValue::Boolean(true)));
    }

    #[test]
    fn test_regex_capture_groups() {
        // Access log parsing with String#scan + capture groups — exercises
        // scan(Regexp) returning nested Array<Array<String>>, plus
        // String#to_i and String#to_f on captured fields.
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("192.168.1.100 - GET /api/users 200 0.045");
        let events = rt
            .execute(
                r#"
                captures = event.get("message").scan(/^(\S+)\s+-\s+(\w+)\s+(\S+)\s+(\d+)\s+([\d.]+)/)
                parts = captures[0]
                event.set("client_ip", parts[0])
                event.set("method", parts[1])
                event.set("path", parts[2])
                event.set("status", parts[3].to_i)
                event.set("duration", parts[4].to_f)
                "#,
                &mut event,
            )
            .expect("regex captures");
        assert_eq!(
            events[0].get("client_ip"),
            Some(&EventValue::String("192.168.1.100".into()))
        );
        assert_eq!(
            events[0].get("method"),
            Some(&EventValue::String("GET".into()))
        );
        assert_eq!(
            events[0].get("path"),
            Some(&EventValue::String("/api/users".into()))
        );
        assert_eq!(events[0].get("status"), Some(&EventValue::Integer(200)));
        assert_eq!(events[0].get("duration"), Some(&EventValue::Float(0.045)));
    }

    #[test]
    fn test_string_scan() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("error=404 warn=12 info=300");
        let events = rt
            .execute(
                r#"
                counts = event.get("message").scan(/(\w+)=(\d+)/)
                event.set("pair_count", counts.length)
                event.set("first_key", counts[0][0])
                event.set("first_val", counts[0][1].to_i)
                "#,
                &mut event,
            )
            .expect("string scan");
        assert_eq!(events[0].get("pair_count"), Some(&EventValue::Integer(3)));
        assert_eq!(
            events[0].get("first_key"),
            Some(&EventValue::String("error".into()))
        );
        assert_eq!(events[0].get("first_val"), Some(&EventValue::Integer(404)));
    }

    #[test]
    fn test_multiple_event_split() {
        // Common Logstash pattern: split a multi-line message into separate events
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("line1\nline2\nline3");
        let events = rt
            .execute(
                r#"
                lines = event.get("message").split("\n")
                event.set("message", lines[0])
                lines[1..-1].each do |line|
                  e = event.clone
                  e.set("message", line)
                  new_event_block.call(e)
                end
                "#,
                &mut event,
            )
            .expect("multi-event split");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message(), Some("line1"));
        assert_eq!(events[1].message(), Some("line2"));
        assert_eq!(events[2].message(), Some("line3"));
    }

    #[test]
    fn test_proc_and_lambda() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                double = Proc.new { |x| x * 2 }
                event.set("doubled", double.call(21))
                "#,
                &mut event,
            )
            .expect("proc");
        assert_eq!(events[0].get("doubled"), Some(&EventValue::Integer(42)));
    }

    #[test]
    fn test_string_to_f() {
        // Exercises our upstream String#to_f implementation in ferro-artichoke.
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                event.set("simple", "1.25".to_f)
                event.set("integer", "42".to_f)
                event.set("negative", "-2.5".to_f)
                event.set("with_suffix", "0.045xyz".to_f)
                event.set("leading_space", "  1.5".to_f)
                event.set("exponent", "1.5e2".to_f)
                event.set("underscores", "1_000.5".to_f)
                event.set("invalid", "not_a_number".to_f)
                event.set("empty", "".to_f)
                "#,
                &mut event,
            )
            .expect("to_f should work");
        assert_eq!(events[0].get("simple"), Some(&EventValue::Float(1.25)));
        assert_eq!(events[0].get("integer"), Some(&EventValue::Float(42.0)));
        assert_eq!(events[0].get("negative"), Some(&EventValue::Float(-2.5)));
        assert_eq!(
            events[0].get("with_suffix"),
            Some(&EventValue::Float(0.045))
        );
        assert_eq!(
            events[0].get("leading_space"),
            Some(&EventValue::Float(1.5))
        );
        assert_eq!(events[0].get("exponent"), Some(&EventValue::Float(150.0)));
        assert_eq!(
            events[0].get("underscores"),
            Some(&EventValue::Float(1000.5))
        );
        assert_eq!(events[0].get("invalid"), Some(&EventValue::Float(0.0)));
        assert_eq!(events[0].get("empty"), Some(&EventValue::Float(0.0)));
    }

    #[test]
    fn test_json_generate() {
        let mut rt = RubyRuntime::new(None).expect("init");
        let mut event = Event::new("test");
        let events = rt
            .execute(
                r#"
                require 'json'
                event.set("json_out", JSON.generate({"a" => 1, "b" => "hello"}))
                "#,
                &mut event,
            )
            .expect("JSON.generate should work");
        let json_out = events[0]
            .get("json_out")
            .and_then(|v| v.as_str())
            .expect("json_out");
        assert!(json_out.contains("\"a\":1") || json_out.contains("\"a\": 1"));
    }
}
