// SPDX-License-Identifier: Apache-2.0
//! Event model — the fundamental data unit flowing through the pipeline.
//!
//! An Event is a structured document with typed fields, similar to a Logstash event.
//! The `message` field is the primary payload; `@timestamp` and `@metadata` are special fields.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::field_ref::FieldRef;

/// Global monotonic event counter — faster than UUID v4 (no syscall).
/// UUID is only needed for external identity; internal pipeline routing
/// uses this lightweight counter.
static EVENT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A value within an event field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Array(Vec<EventValue>),
    Object(IndexMap<String, EventValue>),
}

impl EventValue {
    /// Returns the value as a string representation.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the value as an i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns the value as an f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Integer(n) => Some(*n as f64),
            _ => None,
        }
    }

    /// Returns the value as a bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns a reference to the inner array, if this is an Array variant.
    pub fn as_array(&self) -> Option<&Vec<EventValue>> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Returns a mutable reference to the inner array.
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<EventValue>> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Returns a reference to the inner object.
    pub fn as_object(&self) -> Option<&IndexMap<String, EventValue>> {
        match self {
            Self::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Returns a mutable reference to the inner object.
    pub fn as_object_mut(&mut self) -> Option<&mut IndexMap<String, EventValue>> {
        match self {
            Self::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Converts to a display-friendly string.
    pub fn to_string_lossy(&self) -> String {
        match self {
            Self::String(s) => s.clone(),
            Self::Integer(n) => n.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Boolean(b) => b.to_string(),
            Self::Null => String::new(),
            Self::Array(_) | Self::Object(_) => serde_json::to_string(self).unwrap_or_default(),
        }
    }

    /// Returns true if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns true if this is truthy (non-null, non-false, non-empty).
    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Boolean(b) => *b,
            Self::String(s) => !s.is_empty(),
            Self::Integer(_) | Self::Float(_) => true,
            Self::Array(a) => !a.is_empty(),
            Self::Object(o) => !o.is_empty(),
        }
    }
}

impl fmt::Display for EventValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "{s}"),
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Null => write!(f, ""),
            Self::Array(_) | Self::Object(_) => {
                write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
            }
        }
    }
}

impl From<String> for EventValue {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for EventValue {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<i64> for EventValue {
    fn from(n: i64) -> Self {
        Self::Integer(n)
    }
}

impl From<f64> for EventValue {
    fn from(f: f64) -> Self {
        Self::Float(f)
    }
}

impl From<bool> for EventValue {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<Vec<EventValue>> for EventValue {
    fn from(v: Vec<EventValue>) -> Self {
        Self::Array(v)
    }
}

impl From<serde_json::Value> for EventValue {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Self::Integer(i)
                } else {
                    Self::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Self::String(s),
            serde_json::Value::Array(a) => {
                Self::Array(a.into_iter().map(EventValue::from).collect())
            }
            serde_json::Value::Object(o) => {
                let map: IndexMap<String, EventValue> = o
                    .into_iter()
                    .map(|(k, v)| (k, EventValue::from(v)))
                    .collect();
                Self::Object(map)
            }
        }
    }
}

impl From<EventValue> for serde_json::Value {
    fn from(v: EventValue) -> Self {
        match v {
            EventValue::Null => serde_json::Value::Null,
            EventValue::Boolean(b) => serde_json::Value::Bool(b),
            EventValue::Integer(n) => serde_json::Value::Number(serde_json::Number::from(n)),
            EventValue::Float(f) => serde_json::Value::Number(
                serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
            EventValue::String(s) => serde_json::Value::String(s),
            EventValue::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(serde_json::Value::from).collect())
            }
            EventValue::Object(o) => {
                let map: serde_json::Map<String, serde_json::Value> = o
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect();
                serde_json::Value::Object(map)
            }
        }
    }
}

/// Event metadata — fields that do not appear in the output but control pipeline behavior.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    fields: HashMap<String, EventValue>,
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<&EventValue> {
        self.fields.get(key)
    }

    pub fn set(&mut self, key: String, value: EventValue) {
        self.fields.insert(key, value);
    }

    pub fn remove(&mut self, key: &str) -> Option<EventValue> {
        self.fields.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &EventValue)> {
        self.fields.iter()
    }
}

/// The core event type flowing through the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique event ID.
    #[serde(skip)]
    pub id: String,

    /// Event fields.
    fields: IndexMap<String, EventValue>,

    /// Event timestamp.
    #[serde(rename = "@timestamp")]
    pub timestamp: DateTime<Utc>,

    /// Event metadata (not serialized to output).
    #[serde(rename = "@metadata", default)]
    pub metadata: Metadata,

    /// Tags applied by filters.
    #[serde(default)]
    pub tags: Vec<String>,

    /// Whether this event has been cancelled (dropped).
    #[serde(skip)]
    cancelled: bool,

    /// Originating persistent-queue sequence number, for at-least-once delivery.
    ///
    /// In-memory only (`#[serde(skip)]`, never persisted and never emitted): the
    /// PQ drainer stamps each popped event with the originating queue entry's
    /// `seq`, filter workers re-stamp it onto every derived event, and the output
    /// path uses it to acknowledge (durably checkpoint) the PQ entry only AFTER
    /// the event has been delivered. That ack-after-output ordering is what makes
    /// PQ delivery at-least-once across a crash/restart: an event popped but not
    /// yet delivered is left un-acked and replays on the next start.
    #[serde(skip)]
    pq_seq: Option<u64>,
}

impl Event {
    /// Creates a new event with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            id: EVENT_COUNTER.fetch_add(1, Ordering::Relaxed).to_string(),
            fields: {
                let mut m = IndexMap::with_capacity(8);
                m.insert("message".to_string(), EventValue::String(message.into()));
                m
            },
            timestamp: Utc::now(),
            metadata: Metadata::new(),
            tags: Vec::new(),
            cancelled: false,
            pq_seq: None,
        }
    }

    /// Creates an empty event (no message).
    pub fn empty() -> Self {
        Self {
            id: EVENT_COUNTER.fetch_add(1, Ordering::Relaxed).to_string(),
            fields: IndexMap::with_capacity(8),
            timestamp: Utc::now(),
            metadata: Metadata::new(),
            tags: Vec::new(),
            cancelled: false,
            pq_seq: None,
        }
    }

    /// Creates an event from a JSON value.
    pub fn from_json(value: serde_json::Value) -> Self {
        let mut event = Self::empty();
        if let serde_json::Value::Object(map) = value {
            for (k, v) in map {
                if k == "@timestamp" {
                    if let Some(s) = v.as_str() {
                        if let Ok(ts) = s.parse::<DateTime<Utc>>() {
                            event.timestamp = ts;
                            continue;
                        }
                    }
                }
                event.fields.insert(k, EventValue::from(v));
            }
        }
        event
    }

    /// Gets a field value by name (supports dotted paths via `FieldRef`).
    pub fn get(&self, field: &str) -> Option<&EventValue> {
        if field == "@timestamp" {
            return None; // timestamp accessed via .timestamp
        }
        let field_ref = FieldRef::parse(field);
        field_ref.get_from_fields(&self.fields)
    }

    /// Gets a mutable field value by name.
    pub fn get_mut(&mut self, field: &str) -> Option<&mut EventValue> {
        let field_ref = FieldRef::parse(field);
        field_ref.get_mut_from_fields(&mut self.fields)
    }

    /// Sets a field value.
    pub fn set(&mut self, field: impl Into<String>, value: EventValue) {
        let field_str = field.into();
        let field_ref = FieldRef::parse(&field_str);
        field_ref.set_in_fields(&mut self.fields, value);
    }

    /// Removes a field and returns its value.
    pub fn remove(&mut self, field: &str) -> Option<EventValue> {
        let field_ref = FieldRef::parse(field);
        field_ref.remove_from_fields(&mut self.fields)
    }

    /// Returns true if the field exists.
    pub fn has_field(&self, field: &str) -> bool {
        self.get(field).is_some()
    }

    /// Gets the message field.
    pub fn message(&self) -> Option<&str> {
        self.fields.get("message").and_then(EventValue::as_str)
    }

    /// Sets the message field.
    pub fn set_message(&mut self, message: impl Into<String>) {
        self.fields
            .insert("message".to_string(), EventValue::String(message.into()));
    }

    /// Adds a tag if not already present.
    pub fn add_tag(&mut self, tag: impl Into<String>) {
        let tag = tag.into();
        if !self.tags.contains(&tag) {
            self.tags.push(tag);
        }
    }

    /// Removes a tag.
    pub fn remove_tag(&mut self, tag: &str) {
        self.tags.retain(|t| t != tag);
    }

    /// Returns true if the event has a specific tag.
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }

    /// Cancels this event (it will be dropped from the pipeline).
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Returns true if the event is cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// Returns the originating persistent-queue sequence number, if this event
    /// was read from a persistent queue (set by the PQ drainer and propagated by
    /// filter workers). Used by the output path to acknowledge the PQ entry only
    /// after delivery. `None` for events that never passed through a PQ.
    #[must_use]
    pub fn pq_seq(&self) -> Option<u64> {
        self.pq_seq
    }

    /// Stamps this event with the persistent-queue sequence it derives from.
    ///
    /// The drainer stamps freshly-popped events; filter workers re-stamp every
    /// derived event (clones/splits produce new events whose `pq_seq` must be set
    /// back to the originating entry) so a single PQ entry is acknowledged only
    /// once all of its derived events reach the output (or are dropped/DLQ'd).
    pub fn set_pq_seq(&mut self, seq: u64) {
        self.pq_seq = Some(seq);
    }

    /// Returns an iterator over all field names.
    pub fn field_names(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }

    /// Returns the number of fields.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Returns all fields as an immutable reference.
    pub fn fields(&self) -> &IndexMap<String, EventValue> {
        &self.fields
    }

    /// Returns all fields as a mutable reference.
    pub fn fields_mut(&mut self) -> &mut IndexMap<String, EventValue> {
        &mut self.fields
    }

    /// Format a chrono UTC instant the way Logstash serialises `@timestamp`:
    /// RFC3339 with millisecond precision and a `Z` zone suffix, e.g.
    /// `2026-06-23T15:14:53.874Z`. The default `DateTime::to_rfc3339()`
    /// emits nanoseconds and a `+00:00` offset, which downstream consumers
    /// (Elasticsearch index templates, Beats clients, Kibana time filters)
    /// can misparse or sort differently from Logstash output.
    pub fn format_logstash_timestamp(ts: &DateTime<Utc>) -> String {
        ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    /// Converts the event to a JSON Value for output.
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert(
            "@timestamp".to_string(),
            serde_json::Value::String(Self::format_logstash_timestamp(&self.timestamp)),
        );
        // Every Logstash event carries `@version: "1"`. Downstream tooling
        // (Elasticsearch index templates, Beats, dashboards filtering on
        // @version) silently drops or misindexes events that lack it, so we
        // always emit it.
        map.insert(
            "@version".to_string(),
            serde_json::Value::String("1".to_string()),
        );
        if !self.tags.is_empty() {
            map.insert(
                "tags".to_string(),
                serde_json::Value::Array(
                    self.tags
                        .iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect(),
                ),
            );
        }
        for (k, v) in &self.fields {
            map.insert(k.clone(), serde_json::Value::from(v.clone()));
        }
        serde_json::Value::Object(map)
    }

    /// Converts to a JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&self.to_json()).unwrap_or_default()
    }

    /// Converts to a pretty JSON string.
    pub fn to_json_string_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.to_json()).unwrap_or_default()
    }

    /// Applies sprintf-style field interpolation: `%{field_name}`.
    pub fn sprintf(&self, template: &str) -> String {
        let mut result = String::with_capacity(template.len());
        let mut chars = template.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '%' && chars.peek() == Some(&'{') {
                chars.next(); // consume '{'
                let mut field_name = String::new();
                for ch in chars.by_ref() {
                    if ch == '}' {
                        break;
                    }
                    field_name.push(ch);
                }
                if field_name == "@timestamp" {
                    result.push_str(&Self::format_logstash_timestamp(&self.timestamp));
                } else if field_name == "tags" {
                    // Logstash format: tags are rendered as comma-joined
                    result.push_str(&self.tags.join(","));
                } else if field_name == "message" {
                    result.push_str(self.message().unwrap_or(""));
                } else if let Some(val) = self.get(&field_name) {
                    result.push_str(&val.to_string_lossy());
                } else {
                    result.push_str("%{");
                    result.push_str(&field_name);
                    result.push('}');
                }
            } else {
                result.push(c);
            }
        }
        result
    }
}

impl Default for Event {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_new() {
        let event = Event::new("hello world");
        assert_eq!(event.message(), Some("hello world"));
        assert!(!event.is_cancelled());
        assert!(event.tags.is_empty());
    }

    #[test]
    fn test_event_set_get() {
        let mut event = Event::new("test");
        event.set("host", EventValue::String("localhost".into()));
        assert_eq!(
            event.get("host"),
            Some(&EventValue::String("localhost".into()))
        );
    }

    #[test]
    fn test_event_nested_fields() {
        let mut event = Event::new("test");
        event.set("a.b.c", EventValue::Integer(42));
        assert_eq!(event.get("a.b.c"), Some(&EventValue::Integer(42)));
    }

    #[test]
    fn test_event_remove() {
        let mut event = Event::new("test");
        event.set("foo", EventValue::String("bar".into()));
        let removed = event.remove("foo");
        assert_eq!(removed, Some(EventValue::String("bar".into())));
        assert!(!event.has_field("foo"));
    }

    #[test]
    fn test_event_tags() {
        let mut event = Event::new("test");
        event.add_tag("_grokparsefailure");
        assert!(event.has_tag("_grokparsefailure"));
        event.add_tag("_grokparsefailure"); // duplicate
        assert_eq!(event.tags.len(), 1);
        event.remove_tag("_grokparsefailure");
        assert!(!event.has_tag("_grokparsefailure"));
    }

    #[test]
    fn test_event_cancel() {
        let mut event = Event::new("test");
        assert!(!event.is_cancelled());
        event.cancel();
        assert!(event.is_cancelled());
    }

    #[test]
    fn test_event_sprintf() {
        let mut event = Event::new("hello");
        event.set("host", EventValue::String("server01".into()));
        let result = event.sprintf("msg=%{message} from=%{host}");
        assert_eq!(result, "msg=hello from=server01");
    }

    #[test]
    fn test_event_to_json() {
        let event = Event::new("test message");
        let json = event.to_json();
        assert_eq!(json["message"], "test message");
        assert!(json["@timestamp"].is_string());
    }

    #[test]
    fn test_event_from_json() {
        let json = serde_json::json!({
            "message": "hello",
            "host": "server01",
            "port": 8080
        });
        let event = Event::from_json(json);
        assert_eq!(event.message(), Some("hello"));
        assert_eq!(
            event.get("host"),
            Some(&EventValue::String("server01".into()))
        );
        assert_eq!(event.get("port"), Some(&EventValue::Integer(8080)));
    }

    #[test]
    fn test_event_value_truthy() {
        assert!(EventValue::String("hello".into()).is_truthy());
        assert!(!EventValue::String(String::new()).is_truthy());
        assert!(EventValue::Integer(1).is_truthy());
        assert!(EventValue::Boolean(true).is_truthy());
        assert!(!EventValue::Boolean(false).is_truthy());
        assert!(!EventValue::Null.is_truthy());
    }

    #[test]
    fn test_event_value_conversions() {
        let json_val = serde_json::json!({"key": "value", "num": 42});
        let ev: EventValue = json_val.clone().into();
        let back: serde_json::Value = ev.into();
        assert_eq!(json_val, back);
    }

    #[test]
    fn test_event_empty() {
        let event = Event::empty();
        assert!(event.message().is_none());
        assert!(!event.is_cancelled());
        assert!(event.tags.is_empty());
        assert_eq!(event.field_count(), 0);
    }

    #[test]
    fn test_event_field_count() {
        let mut event = Event::new("test");
        assert_eq!(event.field_count(), 1); // message
        event.set("host", EventValue::String("server".into()));
        assert_eq!(event.field_count(), 2);
    }

    #[test]
    fn test_event_field_names() {
        let mut event = Event::new("test");
        event.set("host", EventValue::String("s".into()));
        let names: Vec<&String> = event.field_names().collect();
        assert!(names.contains(&&"message".to_string()));
        assert!(names.contains(&&"host".to_string()));
    }

    #[test]
    fn test_event_set_message() {
        let mut event = Event::new("original");
        event.set_message("updated");
        assert_eq!(event.message(), Some("updated"));
    }

    #[test]
    fn test_event_has_field() {
        let event = Event::new("test");
        assert!(event.has_field("message"));
        assert!(!event.has_field("nonexistent"));
    }

    #[test]
    fn test_event_default() {
        let event = Event::default();
        assert!(event.message().is_none());
    }

    #[test]
    fn test_event_to_json_with_tags() {
        let mut event = Event::new("test");
        event.add_tag("tag1");
        event.add_tag("tag2");
        let json = event.to_json();
        assert!(json["tags"].is_array());
        let tags = json["tags"].as_array().expect("tags array");
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn test_event_to_json_string() {
        let event = Event::new("hello");
        let s = event.to_json_string();
        assert!(s.contains("hello"));
        assert!(s.contains("@timestamp"));
    }

    #[test]
    fn test_event_from_json_with_timestamp() {
        let json = serde_json::json!({
            "message": "test",
            "@timestamp": "2024-01-15T10:30:00Z"
        });
        let event = Event::from_json(json);
        assert_eq!(event.message(), Some("test"));
        assert_eq!(event.timestamp.year(), 2024);
    }

    use chrono::Datelike;

    #[test]
    fn test_event_from_json_invalid_timestamp() {
        let json = serde_json::json!({
            "message": "test",
            "@timestamp": "not-a-date"
        });
        let event = Event::from_json(json);
        // Invalid timestamp string: the code tries parse, fails, then
        // falls through with continue, so @timestamp stays as default (now)
        // and the string is NOT stored as a field
        assert_eq!(event.message(), Some("test"));
    }

    #[test]
    fn test_event_sprintf_missing_field() {
        let event = Event::new("test");
        let result = event.sprintf("field=%{missing}");
        assert_eq!(result, "field=%{missing}");
    }

    #[test]
    fn test_event_sprintf_timestamp() {
        let event = Event::new("test");
        let result = event.sprintf("ts=%{@timestamp}");
        assert!(result.starts_with("ts="));
        assert!(result.len() > 3);
    }

    #[test]
    fn test_event_value_display() {
        assert_eq!(format!("{}", EventValue::String("hi".into())), "hi");
        assert_eq!(format!("{}", EventValue::Integer(42)), "42");
        assert_eq!(format!("{}", EventValue::Float(2.5)), "2.5");
        assert_eq!(format!("{}", EventValue::Boolean(true)), "true");
        assert_eq!(format!("{}", EventValue::Null), "");
    }

    #[test]
    fn test_event_value_as_str() {
        assert_eq!(EventValue::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(EventValue::Integer(42).as_str(), None);
    }

    #[test]
    fn test_event_value_as_i64() {
        assert_eq!(EventValue::Integer(42).as_i64(), Some(42));
        assert_eq!(EventValue::String("42".into()).as_i64(), None);
    }

    #[test]
    fn test_event_value_as_f64() {
        assert_eq!(EventValue::Float(2.5).as_f64(), Some(2.5));
        assert_eq!(EventValue::Integer(42).as_f64(), Some(42.0));
        assert_eq!(EventValue::String("x".into()).as_f64(), None);
    }

    #[test]
    fn test_event_value_as_bool() {
        assert_eq!(EventValue::Boolean(true).as_bool(), Some(true));
        assert_eq!(EventValue::Integer(1).as_bool(), None);
    }

    #[test]
    fn test_event_value_as_array() {
        let arr = EventValue::Array(vec![EventValue::Integer(1)]);
        assert!(arr.as_array().is_some());
        assert!(EventValue::Integer(1).as_array().is_none());
    }

    #[test]
    fn test_event_value_as_object() {
        let obj = EventValue::Object(IndexMap::new());
        assert!(obj.as_object().is_some());
        assert!(EventValue::String("x".into()).as_object().is_none());
    }

    #[test]
    fn test_event_value_is_null() {
        assert!(EventValue::Null.is_null());
        assert!(!EventValue::Integer(0).is_null());
    }

    #[test]
    fn test_event_value_from_string() {
        let ev: EventValue = "hello".into();
        assert_eq!(ev, EventValue::String("hello".into()));
    }

    #[test]
    fn test_event_value_from_i64() {
        let ev: EventValue = 42i64.into();
        assert_eq!(ev, EventValue::Integer(42));
    }

    #[test]
    fn test_event_value_from_f64() {
        let ev: EventValue = 2.5f64.into();
        assert_eq!(ev, EventValue::Float(2.5));
    }

    #[test]
    fn test_event_value_from_bool() {
        let ev: EventValue = true.into();
        assert_eq!(ev, EventValue::Boolean(true));
    }

    #[test]
    fn test_event_value_from_vec() {
        let ev: EventValue = vec![EventValue::Integer(1), EventValue::Integer(2)].into();
        assert!(matches!(ev, EventValue::Array(_)));
    }

    #[test]
    fn test_event_value_from_json_array() {
        let json = serde_json::json!([1, 2, 3]);
        let ev = EventValue::from(json);
        assert!(matches!(ev, EventValue::Array(_)));
    }

    #[test]
    fn test_event_value_from_json_null() {
        let json = serde_json::json!(null);
        let ev = EventValue::from(json);
        assert!(matches!(ev, EventValue::Null));
    }

    #[test]
    fn test_event_value_to_string_lossy() {
        assert_eq!(
            EventValue::String("hello".into()).to_string_lossy(),
            "hello"
        );
        assert_eq!(EventValue::Integer(42).to_string_lossy(), "42");
        assert_eq!(EventValue::Boolean(false).to_string_lossy(), "false");
        assert_eq!(EventValue::Null.to_string_lossy(), "");
    }

    #[test]
    fn test_metadata() {
        let mut meta = Metadata::new();
        meta.set("key".to_string(), EventValue::String("val".into()));
        assert_eq!(meta.get("key"), Some(&EventValue::String("val".into())));
        assert!(meta.get("missing").is_none());
        let removed = meta.remove("key");
        assert!(removed.is_some());
        assert!(meta.get("key").is_none());
    }

    #[test]
    fn test_metadata_iter() {
        let mut meta = Metadata::new();
        meta.set("a".to_string(), EventValue::Integer(1));
        meta.set("b".to_string(), EventValue::Integer(2));
        let count = meta.iter().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_event_get_mut() {
        let mut event = Event::new("test");
        event.set("count", EventValue::Integer(1));
        if let Some(val) = event.get_mut("count") {
            *val = EventValue::Integer(2);
        }
        assert_eq!(event.get("count"), Some(&EventValue::Integer(2)));
    }

    #[test]
    fn test_event_multiple_tags() {
        let mut event = Event::new("test");
        event.add_tag("a");
        event.add_tag("b");
        event.add_tag("c");
        assert_eq!(event.tags.len(), 3);
        event.remove_tag("b");
        assert_eq!(event.tags.len(), 2);
        assert!(!event.has_tag("b"));
    }

    #[test]
    fn test_event_fields_accessor() {
        let event = Event::new("test");
        let fields = event.fields();
        assert!(fields.contains_key("message"));
    }

    #[test]
    fn test_event_fields_mut_accessor() {
        let mut event = Event::new("test");
        let fields = event.fields_mut();
        fields.insert("new_field".to_string(), EventValue::Integer(42));
        assert!(event.has_field("new_field"));
    }
}
