// SPDX-License-Identifier: Apache-2.0
//! Script codec — user-defined encode/decode via a safe expression DSL.
//!
//! This is the Rust-native alternative to Logstash's Ruby codec.
//! Uses the same expression DSL as the ruby filter:
//!   - `event.set("field", value)`
//!   - `event.get("field")`
//!   - `event.remove("field")`
//!   - `event.tag("name")`
//!   - `event.cancel()`
//!
//! Usage in Logstash config:
//! ```text
//! input {
//!   stdin {
//!     codec => script {
//!       decode => "event.set('parsed', event.get('message'))"
//!       encode => "event.set('output', event.get('message'))"
//!     }
//!   }
//! }
//! ```

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use tracing::warn;

use crate::Codec;

/// Script codec configuration.
#[derive(Debug, Clone)]
pub struct ScriptCodec {
    /// Script to run on decode (input → event transformation).
    pub decode_script: String,
    /// Script to run on encode (event → output transformation).
    pub encode_script: String,
    /// Optional init script (run once, currently informational only).
    pub init: Option<String>,
}

impl ScriptCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let decode_script = settings
            .get("decode")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let encode_script = settings
            .get("encode")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let init = settings
            .get("init")
            .and_then(|v| v.as_str())
            .map(String::from);

        Ok(Self {
            decode_script,
            encode_script,
            init,
        })
    }

    /// Evaluate a script DSL against an event (same engine as ruby filter).
    fn evaluate(script: &str, event: &mut Event) -> std::result::Result<(), String> {
        for stmt in script.split(';') {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            Self::eval_statement(event, stmt)?;
        }
        Ok(())
    }

    fn eval_statement(event: &mut Event, stmt: &str) -> std::result::Result<(), String> {
        // event.cancel()
        if stmt == "event.cancel()" {
            event.cancel();
            return Ok(());
        }

        // event.tag("name")
        if let Some(rest) = stmt.strip_prefix("event.tag(") {
            if let Some(tag) = rest.strip_suffix(')') {
                let tag = tag.trim().trim_matches('"').trim_matches('\'');
                event.add_tag(tag);
                return Ok(());
            }
        }

        // event.set("field", value)
        if let Some(rest) = stmt.strip_prefix("event.set(") {
            if let Some(args) = rest.strip_suffix(')') {
                let parts: Vec<&str> = args.splitn(2, ',').collect();
                if parts.len() == 2 {
                    let field = parts[0].trim().trim_matches('"').trim_matches('\'');
                    let value_str = parts[1].trim();
                    let value = Self::parse_value(value_str, event);
                    event.set(field.to_string(), value);
                    return Ok(());
                }
            }
        }

        // event.remove("field")
        if let Some(rest) = stmt.strip_prefix("event.remove(") {
            if let Some(field) = rest.strip_suffix(')') {
                let field = field.trim().trim_matches('"').trim_matches('\'');
                event.remove(field);
                return Ok(());
            }
        }

        // String concatenation: event.set("field", event.get("a") + " " + event.get("b"))
        // For now, warn on unsupported
        warn!(statement = stmt, "unsupported script expression");
        Ok(())
    }

    fn parse_value(value_str: &str, event: &Event) -> EventValue {
        if value_str.starts_with('"') || value_str.starts_with('\'') {
            EventValue::String(value_str.trim_matches('"').trim_matches('\'').to_string())
        } else if let Ok(n) = value_str.parse::<i64>() {
            EventValue::Integer(n)
        } else if let Ok(f) = value_str.parse::<f64>() {
            EventValue::Float(f)
        } else if value_str == "true" {
            EventValue::Boolean(true)
        } else if value_str == "false" {
            EventValue::Boolean(false)
        } else if value_str == "nil" || value_str == "null" {
            EventValue::Null
        } else if let Some(get_field) = value_str
            .strip_prefix("event.get(")
            .and_then(|s| s.strip_suffix(')'))
        {
            let get_field = get_field.trim().trim_matches('"').trim_matches('\'');
            event.get(get_field).cloned().unwrap_or(EventValue::Null)
        } else {
            EventValue::String(value_str.to_string())
        }
    }
}

impl Codec for ScriptCodec {
    fn name(&self) -> &'static str {
        "script"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let trimmed = text.trim_end_matches('\n').trim_end_matches('\r');
        let mut event = Event::new(trimmed);

        if !self.decode_script.is_empty() {
            if let Err(e) = Self::evaluate(&self.decode_script, &mut event) {
                warn!(error = %e, "script codec decode error");
                event.add_tag("_scriptcodecexception");
            }
        }

        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let mut event = event.clone();

        if !self.encode_script.is_empty() {
            if let Err(e) = Self::evaluate(&self.encode_script, &mut event) {
                warn!(error = %e, "script codec encode error");
                return Err(FerroStashError::Codec(format!(
                    "script codec encode error: {e}"
                )));
            }
        }

        // After encoding script, output the event as JSON
        let json = event.to_json_string();
        Ok(format!("{json}\n").into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_codec_decode_basic() {
        let codec = ScriptCodec::from_config(&serde_json::json!({})).expect("config");
        let event = codec
            .decode(b"hello world")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello world"));
    }

    #[test]
    fn test_script_codec_decode_with_script() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "decode": r#"event.set("decoded", true); event.tag("script_processed")"#
        }))
        .expect("config");
        let event = codec
            .decode(b"test data")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("test data"));
        assert_eq!(event.get("decoded"), Some(&EventValue::Boolean(true)));
        assert!(event.has_tag("script_processed"));
    }

    #[test]
    fn test_script_codec_decode_transform_field() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "decode": r#"event.set("original", event.get("message"))"#
        }))
        .expect("config");
        let event = codec
            .decode(b"hello")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("original"),
            Some(&EventValue::String("hello".into()))
        );
    }

    #[test]
    fn test_script_codec_encode_basic() {
        let codec = ScriptCodec::from_config(&serde_json::json!({})).expect("config");
        let event = Event::new("test");
        let bytes = codec.encode(&event).expect("encode");
        let output = String::from_utf8(bytes).expect("utf8");
        assert!(output.contains("test"));
    }

    #[test]
    fn test_script_codec_encode_with_script() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "encode": r#"event.set("encoded", true)"#
        }))
        .expect("config");
        let event = Event::new("test");
        let bytes = codec.encode(&event).expect("encode");
        let output = String::from_utf8(bytes).expect("utf8");
        assert!(output.contains("encoded"));
        assert!(output.contains("true"));
    }

    #[test]
    fn test_script_codec_name() {
        let codec = ScriptCodec::from_config(&serde_json::json!({})).expect("config");
        assert_eq!(codec.name(), "script");
    }

    #[test]
    fn test_script_codec_with_init() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "init": "counter = 0",
            "decode": "",
            "encode": ""
        }))
        .expect("config");
        assert_eq!(codec.init, Some("counter = 0".to_string()));
    }

    #[test]
    fn test_script_codec_set_integer() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "decode": r#"event.set("count", 42)"#
        }))
        .expect("config");
        let event = codec
            .decode(b"data")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("count"), Some(&EventValue::Integer(42)));
    }

    #[test]
    fn test_script_codec_multiple_statements() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "decode": r#"event.set("a", 1); event.set("b", "two"); event.tag("multi")"#
        }))
        .expect("config");
        let event = codec
            .decode(b"x")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("a"), Some(&EventValue::Integer(1)));
        assert_eq!(event.get("b"), Some(&EventValue::String("two".into())));
        assert!(event.has_tag("multi"));
    }

    #[test]
    fn test_script_codec_remove_field() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "decode": r#"event.remove("message")"#
        }))
        .expect("config");
        let event = codec
            .decode(b"hello")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(!event.has_field("message"));
    }

    #[test]
    fn test_script_codec_cancel() {
        let codec = ScriptCodec::from_config(&serde_json::json!({
            "decode": "event.cancel()"
        }))
        .expect("config");
        let event = codec
            .decode(b"hello")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.is_cancelled());
    }
}
