// SPDX-License-Identifier: Apache-2.0
//! Bridge between ferro-stash Event and Ruby `LogStash::Event`.
//!
//! Converts Event ↔ Ruby Hash for passing across the Artichoke boundary.

use ferro_stash_core::event::{Event, EventValue};
use indexmap::IndexMap;
use serde_json::Value as JsonValue;

/// Convert a ferro-stash Event into a Ruby Hash literal string.
///
/// This avoids going through Artichoke's JSON parser (which has `StringScanner`
/// limitations) by generating a Ruby Hash literal that can be eval'd directly.
pub fn event_to_ruby_hash(event: &Event) -> String {
    let mut parts = Vec::new();

    // Timestamp
    parts.push(format!(
        "\"@timestamp\" => \"{}\"",
        event.timestamp.to_rfc3339()
    ));

    // Fields
    for (k, v) in event.fields() {
        parts.push(format!(
            "\"{}\" => {}",
            escape_ruby_string(k),
            event_value_to_ruby(v)
        ));
    }

    // Tags
    if !event.tags.is_empty() {
        let tags: Vec<String> = event
            .tags
            .iter()
            .map(|t| format!("\"{}\"", escape_ruby_string(t)))
            .collect();
        parts.push(format!("\"tags\" => [{}]", tags.join(", ")));
    }

    // Metadata
    let mut meta_parts = Vec::new();
    for (k, v) in event.metadata.iter() {
        meta_parts.push(format!(
            "\"{}\" => {}",
            escape_ruby_string(k),
            event_value_to_ruby(v)
        ));
    }
    if !meta_parts.is_empty() {
        parts.push(format!("\"@metadata\" => {{{}}}", meta_parts.join(", ")));
    }

    format!("{{{}}}", parts.join(", "))
}

/// Convert an `EventValue` to a Ruby literal string.
fn event_value_to_ruby(v: &EventValue) -> String {
    match v {
        EventValue::String(s) => format!("\"{}\"", escape_ruby_string(s)),
        EventValue::Integer(n) => n.to_string(),
        EventValue::Float(f) => {
            if f.is_infinite() || f.is_nan() {
                "nil".to_string()
            } else {
                f.to_string()
            }
        }
        EventValue::Boolean(b) => b.to_string(),
        EventValue::Null => "nil".to_string(),
        EventValue::Array(a) => {
            let items: Vec<String> = a.iter().map(event_value_to_ruby).collect();
            format!("[{}]", items.join(", "))
        }
        EventValue::Object(o) => {
            let items: Vec<String> = o
                .iter()
                .map(|(k, v)| {
                    format!(
                        "\"{}\" => {}",
                        escape_ruby_string(k),
                        event_value_to_ruby(v)
                    )
                })
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

/// Escape a string for embedding in a Ruby double-quoted string.
fn escape_ruby_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + s.len() / 8);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '#' => out.push_str("\\#"),
            '\0' => out.push_str("\\0"),
            _ => out.push(c),
        }
    }
    out
}

/// Convert a ferro-stash Event into a JSON value suitable for Ruby.
///
/// Currently unused at runtime (we use `event_to_ruby_hash` instead to avoid
/// Artichoke's JSON parser), but kept as a utility for tests and future use.
#[allow(dead_code)]
pub fn event_to_json(event: &Event) -> JsonValue {
    let mut map = serde_json::Map::new();

    // Timestamp
    map.insert(
        "@timestamp".to_string(),
        JsonValue::String(event.timestamp.to_rfc3339()),
    );

    // Fields
    for (k, v) in event.fields() {
        map.insert(k.clone(), event_value_to_json(v));
    }

    // Tags
    if !event.tags.is_empty() {
        map.insert(
            "tags".to_string(),
            JsonValue::Array(
                event
                    .tags
                    .iter()
                    .map(|t| JsonValue::String(t.clone()))
                    .collect(),
            ),
        );
    }

    // Metadata
    let mut meta_map = serde_json::Map::new();
    for (k, v) in event.metadata.iter() {
        meta_map.insert(k.clone(), event_value_to_json(v));
    }
    if !meta_map.is_empty() {
        map.insert("@metadata".to_string(), JsonValue::Object(meta_map));
    }

    JsonValue::Object(map)
}

/// Apply changes from Ruby execution back to a ferro-stash Event.
pub fn apply_ruby_result(event: &mut Event, result: &JsonValue) {
    let Some(obj) = result.as_object() else {
        return;
    };

    // Check cancellation
    if let Some(cancelled) = obj.get("__cancelled__")
        && cancelled.as_bool() == Some(true)
    {
        event.cancel();
    }

    // Timestamp
    if let Some(ts) = obj.get("@timestamp").and_then(|v| v.as_str())
        && let Ok(parsed) = ts.parse()
    {
        event.timestamp = parsed;
    }

    // Tags
    if let Some(tags) = obj.get("tags").and_then(|v| v.as_array()) {
        event.tags = tags
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    } else {
        event.tags.clear();
    }

    // Metadata
    if let Some(meta) = obj.get("@metadata").and_then(|v| v.as_object()) {
        for (k, v) in meta {
            event.metadata.set(k.clone(), json_to_event_value(v));
        }
    }

    // Fields — replace all fields with what Ruby returned
    let fields = event.fields_mut();
    fields.clear();
    for (k, v) in obj {
        if matches!(
            k.as_str(),
            "@timestamp" | "tags" | "@metadata" | "__cancelled__"
        ) {
            continue;
        }
        fields.insert(k.clone(), json_to_event_value(v));
    }
}

#[allow(dead_code)]
fn event_value_to_json(v: &EventValue) -> JsonValue {
    match v {
        EventValue::String(s) => JsonValue::String(s.clone()),
        EventValue::Integer(n) => JsonValue::Number((*n).into()),
        EventValue::Float(f) => {
            serde_json::Number::from_f64(*f).map_or(JsonValue::Null, JsonValue::Number)
        }
        EventValue::Boolean(b) => JsonValue::Bool(*b),
        EventValue::Null => JsonValue::Null,
        EventValue::Array(a) => JsonValue::Array(a.iter().map(event_value_to_json).collect()),
        EventValue::Object(o) => {
            let map: serde_json::Map<String, JsonValue> = o
                .iter()
                .map(|(k, v)| (k.clone(), event_value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
    }
}

fn json_to_event_value(v: &JsonValue) -> EventValue {
    match v {
        JsonValue::Null => EventValue::Null,
        JsonValue::Bool(b) => EventValue::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                EventValue::Integer(i)
            } else {
                EventValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        JsonValue::String(s) => EventValue::String(s.clone()),
        JsonValue::Array(a) => EventValue::Array(a.iter().map(json_to_event_value).collect()),
        JsonValue::Object(o) => {
            let map: IndexMap<String, EventValue> = o
                .iter()
                .map(|(k, v)| (k.clone(), json_to_event_value(v)))
                .collect();
            EventValue::Object(map)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_roundtrip() {
        let mut event = Event::new("hello world");
        event.set("host", EventValue::String("server01".into()));
        event.set("port", EventValue::Integer(8080));
        event.add_tag("test");
        event
            .metadata
            .set("index".to_string(), EventValue::String("logs".into()));

        let json = event_to_json(&event);
        let obj = json.as_object().expect("should be object");
        assert_eq!(obj["message"], "hello world");
        assert_eq!(obj["host"], "server01");
        assert_eq!(obj["port"], 8080);
        assert!(obj["@timestamp"].is_string());
        assert!(obj["tags"].is_array());
        assert!(obj["@metadata"].is_object());
    }

    #[test]
    fn test_apply_result_basic() {
        let mut event = Event::new("original");
        let result = serde_json::json!({
            "message": "modified",
            "new_field": 42,
            "@timestamp": "2026-01-01T00:00:00+00:00",
            "tags": ["processed"],
            "@metadata": {"index": "test"},
            "__cancelled__": false
        });
        apply_ruby_result(&mut event, &result);
        assert_eq!(event.message(), Some("modified"));
        assert_eq!(event.get("new_field"), Some(&EventValue::Integer(42)));
        assert!(event.has_tag("processed"));
        assert!(!event.is_cancelled());
    }

    #[test]
    fn test_apply_result_cancelled() {
        let mut event = Event::new("test");
        let result = serde_json::json!({
            "message": "test",
            "__cancelled__": true
        });
        apply_ruby_result(&mut event, &result);
        assert!(event.is_cancelled());
    }
}
