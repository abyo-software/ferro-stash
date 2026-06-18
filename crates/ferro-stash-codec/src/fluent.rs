// SPDX-License-Identifier: Apache-2.0
//! Fluent codec — Fluentd/Fluent Bit Forward Protocol codec.
//!
//! The Fluent Forward Protocol uses MessagePack with the format:
//! `[tag, timestamp, record]` (Message mode)
//! or `[tag, [[timestamp, record], ...]]` (Forward mode)
//!
//! This codec handles the Message mode for single events.

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Fluent Forward Protocol codec.
#[derive(Debug, Clone)]
pub struct FluentCodec {
    /// Default tag for encoding.
    pub tag: String,
}

impl Default for FluentCodec {
    fn default() -> Self {
        Self {
            tag: "ferrostash".to_string(),
        }
    }
}

impl FluentCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let tag = settings
            .get("tag")
            .and_then(|v| v.as_str())
            .unwrap_or("ferrostash")
            .to_string();
        Ok(Self { tag })
    }

    fn set_timestamp(event: &mut Event, ts_value: &serde_json::Value) {
        if let Some(ts) = ts_value.as_i64() {
            if let Some(dt) = chrono::DateTime::from_timestamp(ts, 0) {
                event.timestamp = dt;
            }
        } else if let Some(ts) = ts_value.as_f64() {
            #[allow(clippy::cast_possible_truncation)]
            let secs = ts as i64;
            let nsecs = ((ts - secs as f64) * 1_000_000_000.0) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                event.timestamp = dt;
            }
        }
    }
}

impl Codec for FluentCodec {
    fn name(&self) -> &'static str {
        "fluent"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        // Fluent Forward Protocol supports:
        // - Message mode: [tag, timestamp, record]
        // - Forward mode: [tag, [[timestamp, record], [timestamp, record], ...]]
        let value: serde_json::Value = rmp_serde::from_slice(data)
            .map_err(|e| FerroStashError::Codec(format!("fluent decode error: {e}")))?;

        let arr = value
            .as_array()
            .ok_or_else(|| FerroStashError::Codec("fluent message must be an array".to_string()))?;

        if arr.len() < 2 {
            return Err(FerroStashError::Codec(format!(
                "fluent message needs at least 2 elements, got {}",
                arr.len()
            )));
        }

        let tag = arr[0].as_str().unwrap_or("unknown");

        // Forward mode: [tag, [[timestamp, record], ...]]
        if arr.len() == 2 {
            if let Some(entries) = arr[1].as_array() {
                let mut events = Vec::with_capacity(entries.len());
                for entry in entries {
                    if let Some(pair) = entry.as_array() {
                        if pair.len() >= 2 {
                            let mut event = Event::from_json(pair[1].clone());
                            event.set("fluent_tag", EventValue::String(tag.to_string()));
                            Self::set_timestamp(&mut event, &pair[0]);
                            events.push(event);
                        }
                    }
                }
                if !events.is_empty() {
                    return Ok(events);
                }
            }
        }

        // Message mode: [tag, timestamp, record]
        if arr.len() < 3 {
            return Err(FerroStashError::Codec(format!(
                "fluent message needs at least 3 elements, got {}",
                arr.len()
            )));
        }

        let record = &arr[2];
        let mut event = Event::from_json(record.clone());
        event.set("fluent_tag", EventValue::String(tag.to_string()));
        Self::set_timestamp(&mut event, &arr[1]);

        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let tag = event
            .get("fluent_tag")
            .and_then(EventValue::as_str)
            .unwrap_or(&self.tag);

        let timestamp = event.timestamp.timestamp();
        let record = event.to_json();

        let message = serde_json::json!([tag, timestamp, record]);

        rmp_serde::to_vec(&message)
            .map_err(|e| FerroStashError::Codec(format!("fluent encode error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fluent_roundtrip() {
        let codec = FluentCodec::default();
        let mut event = Event::new("hello fluent");
        event.set("host", EventValue::String("web01".into()));
        let encoded = codec.encode(&event).expect("encode");
        let decoded = codec
            .decode(&encoded)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(decoded.message(), Some("hello fluent"));
        assert_eq!(
            decoded.get("fluent_tag"),
            Some(&EventValue::String("ferrostash".into()))
        );
    }

    #[test]
    fn test_fluent_custom_tag() {
        let codec = FluentCodec {
            tag: "app.logs".to_string(),
        };
        let event = Event::new("test");
        let encoded = codec.encode(&event).expect("encode");
        let decoded = codec
            .decode(&encoded)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            decoded.get("fluent_tag"),
            Some(&EventValue::String("app.logs".into()))
        );
    }

    #[test]
    fn test_fluent_invalid_data() {
        let codec = FluentCodec::default();
        let result = codec.decode(b"not msgpack");
        assert!(result.is_err());
    }

    #[test]
    fn test_fluent_name() {
        assert_eq!(FluentCodec::default().name(), "fluent");
    }

    #[test]
    fn test_fluent_from_config() {
        let settings = serde_json::json!({ "tag": "my.app" });
        let codec = FluentCodec::from_config(&settings).expect("config");
        assert_eq!(codec.tag, "my.app");
    }

    #[test]
    fn test_fluent_forward_mode() {
        let codec = FluentCodec::default();
        // Forward mode: [tag, [[timestamp, record], [timestamp, record]]]
        let batch = serde_json::json!([
            "app.logs",
            [
                [1712880000, {"message": "first"}],
                [1712880001, {"message": "second"}],
                [1712880002, {"message": "third"}]
            ]
        ]);
        let encoded = rmp_serde::to_vec(&batch).expect("encode");
        let events = codec.decode(&encoded).expect("decode");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message(), Some("first"));
        assert_eq!(events[1].message(), Some("second"));
        assert_eq!(events[2].message(), Some("third"));
        assert_eq!(
            events[0].get("fluent_tag"),
            Some(&EventValue::String("app.logs".into()))
        );
    }
}
