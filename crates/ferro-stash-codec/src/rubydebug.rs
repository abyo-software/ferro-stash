// SPDX-License-Identifier: Apache-2.0
//! Rubydebug codec — Ruby pp-style pretty-printed output (Logstash default debug format).

use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Rubydebug codec — encodes events in a Ruby `pp`-style format.
///
/// This is Logstash's default stdout codec. Output looks like:
/// ```text
/// {
///        "message" => "hello world",
///     "@timestamp" => 2026-04-12T10:00:00.000Z,
///           "host" => "server01"
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct RubydebugCodec;

impl RubydebugCodec {
    pub fn from_config(_settings: &serde_json::Value) -> Result<Self> {
        Ok(Self)
    }

    /// Format an `EventValue` in Ruby inspect style.
    fn format_value(val: &EventValue) -> String {
        match val {
            EventValue::String(s) => format!("\"{s}\""),
            EventValue::Integer(n) => n.to_string(),
            EventValue::Float(f) => f.to_string(),
            EventValue::Boolean(b) => b.to_string(),
            EventValue::Null => "nil".to_string(),
            EventValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(Self::format_value).collect();
                format!("[{}]", items.join(", "))
            }
            EventValue::Object(obj) => {
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("\"{}\" => {}", k, Self::format_value(v)))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
}

impl Codec for RubydebugCodec {
    fn name(&self) -> &'static str {
        "rubydebug"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        // Rubydebug is primarily an output codec; decode falls back to plain text.
        let text = String::from_utf8_lossy(data);
        Ok(vec![Event::new(text.trim_end())])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let mut lines = Vec::new();

        // Collect all key-value pairs for alignment
        let mut pairs: Vec<(String, String)> = Vec::new();
        pairs.push((
            "@timestamp".to_string(),
            event
                .timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        ));
        for (k, v) in event.fields() {
            pairs.push((k.clone(), Self::format_value(v)));
        }
        if !event.tags.is_empty() {
            let tags: Vec<String> = event.tags.iter().map(|t| format!("\"{t}\"")).collect();
            pairs.push(("tags".to_string(), format!("[{}]", tags.join(", "))));
        }

        // Find max key length for right-alignment
        let max_key_len = pairs.iter().map(|(k, _)| k.len()).max().unwrap_or(0);

        lines.push("{".to_string());
        for (i, (key, value)) in pairs.iter().enumerate() {
            let comma = if i < pairs.len() - 1 { "," } else { "" };
            lines.push(format!(
                "    {:>width$} => {value}{comma}",
                format!("\"{key}\""),
                width = max_key_len + 2
            ));
        }
        lines.push("}".to_string());

        let output = lines.join("\n");
        Ok(format!("{output}\n").into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rubydebug_encode() {
        let codec = RubydebugCodec;
        let mut event = Event::new("hello world");
        event.set("host", EventValue::String("server01".into()));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("\"message\" => \"hello world\""));
        assert!(text.contains("\"host\" => \"server01\""));
        assert!(text.contains("@timestamp"));
        assert!(text.starts_with('{'));
        assert!(text.trim_end().ends_with('}'));
    }

    #[test]
    fn test_rubydebug_decode_fallback() {
        let codec = RubydebugCodec;
        let event = codec
            .decode(b"some text\n")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("some text"));
    }

    #[test]
    fn test_rubydebug_encode_with_tags() {
        let codec = RubydebugCodec;
        let mut event = Event::new("test");
        event.add_tag("tag1");
        event.add_tag("tag2");
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("\"tags\" => [\"tag1\", \"tag2\"]"));
    }

    #[test]
    fn test_rubydebug_format_value_nil() {
        assert_eq!(RubydebugCodec::format_value(&EventValue::Null), "nil");
    }

    #[test]
    fn test_rubydebug_format_value_array() {
        let arr = EventValue::Array(vec![EventValue::Integer(1), EventValue::Integer(2)]);
        assert_eq!(RubydebugCodec::format_value(&arr), "[1, 2]");
    }

    #[test]
    fn test_rubydebug_name() {
        assert_eq!(RubydebugCodec.name(), "rubydebug");
    }
}
