// SPDX-License-Identifier: Apache-2.0
//! Elasticsearch Bulk codec — decode/encode events in Elasticsearch Bulk API NDJSON format.
//!
//! The Bulk format consists of alternating action/metadata lines and optional source lines:
//! ```text
//! {"index":{"_index":"logs","_id":"1"}}
//! {"message":"hello","@timestamp":"2026-04-12T00:00:00Z"}
//! {"delete":{"_index":"logs","_id":"2"}}
//! ```

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Elasticsearch Bulk codec.
#[derive(Debug, Clone, Default)]
pub struct EsBulkCodec {
    /// Default index name to use when encoding.
    pub index: Option<String>,
    /// Default action (index, create, update, delete).
    pub action: String,
}

impl EsBulkCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let index = settings
            .get("index")
            .and_then(|v| v.as_str())
            .map(String::from);
        let action = settings
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("index")
            .to_string();
        Ok(Self { index, action })
    }
}

impl Codec for EsBulkCodec {
    fn name(&self) -> &'static str {
        "es_bulk"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let text = text.trim();
        if text.is_empty() {
            return Err(FerroStashError::Codec("empty bulk data".to_string()));
        }

        let mut lines = text.lines().peekable();
        let mut events = Vec::new();

        while lines.peek().is_some() {
            // Parse action line
            let action_line = match lines.next() {
                Some(l) if !l.trim().is_empty() => l,
                _ => break,
            };
            let action_json: serde_json::Value = serde_json::from_str(action_line)
                .map_err(|e| FerroStashError::Codec(format!("invalid bulk action JSON: {e}")))?;

            let (action_type, meta) = action_json
                .as_object()
                .and_then(|obj| obj.iter().next().map(|(k, v)| (k.clone(), v.clone())))
                .ok_or_else(|| FerroStashError::Codec("invalid bulk action format".to_string()))?;

            let mut event = if action_type == "delete" {
                Event::empty()
            } else if let Some(source_line) = lines.next() {
                if source_line.trim().is_empty() {
                    Event::empty()
                } else {
                    let source: serde_json::Value =
                        serde_json::from_str(source_line).map_err(|e| {
                            FerroStashError::Codec(format!("invalid bulk source JSON: {e}"))
                        })?;
                    Event::from_json(source)
                }
            } else {
                Event::empty()
            };

            event.set("[@metadata][action]", EventValue::String(action_type));
            if let Some(index) = meta.get("_index").and_then(|v| v.as_str()) {
                event.set("[@metadata][_index]", EventValue::String(index.to_string()));
            }
            if let Some(id) = meta.get("_id").and_then(|v| v.as_str()) {
                event.set("[@metadata][_id]", EventValue::String(id.to_string()));
            }
            if let Some(routing) = meta.get("routing").and_then(|v| v.as_str()) {
                event.set(
                    "[@metadata][routing]",
                    EventValue::String(routing.to_string()),
                );
            }

            events.push(event);
        }

        if events.is_empty() {
            return Err(FerroStashError::Codec("no bulk actions found".to_string()));
        }

        Ok(events)
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let action = event
            .metadata
            .get("action")
            .and_then(EventValue::as_str)
            .unwrap_or(&self.action)
            .to_string();

        let index = event
            .metadata
            .get("_index")
            .and_then(EventValue::as_str)
            .map(String::from)
            .or_else(|| self.index.clone());

        let id = event
            .metadata
            .get("_id")
            .and_then(EventValue::as_str)
            .map(String::from);

        // Build action metadata
        let mut meta = serde_json::Map::new();
        if let Some(idx) = &index {
            meta.insert("_index".to_string(), serde_json::Value::String(idx.clone()));
        }
        if let Some(id) = &id {
            meta.insert("_id".to_string(), serde_json::Value::String(id.clone()));
        }

        let action_line = serde_json::json!({ &action: meta });
        let action_bytes = serde_json::to_vec(&action_line)
            .map_err(|e| FerroStashError::Codec(format!("bulk encode error: {e}")))?;

        let mut output = action_bytes;
        output.push(b'\n');

        if action != "delete" {
            let source = event.to_json();
            let source_bytes = serde_json::to_vec(&source)
                .map_err(|e| FerroStashError::Codec(format!("bulk encode error: {e}")))?;
            output.extend_from_slice(&source_bytes);
            output.push(b'\n');
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_es_bulk_decode_index() {
        let codec = EsBulkCodec::default();
        let data = b"{\"index\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n{\"message\":\"hello\"}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello"));
    }

    #[test]
    fn test_es_bulk_decode_delete() {
        let codec = EsBulkCodec::default();
        let data = b"{\"delete\":{\"_index\":\"logs\",\"_id\":\"1\"}}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.message().is_none());
    }

    #[test]
    fn test_es_bulk_encode() {
        let codec = EsBulkCodec {
            index: Some("test-index".to_string()),
            action: "index".to_string(),
        };
        let event = Event::new("hello");
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("\"index\""));
        assert!(text.contains("test-index"));
        assert!(text.contains("hello"));
    }

    #[test]
    fn test_es_bulk_decode_empty() {
        let codec = EsBulkCodec::default();
        let result = codec.decode(b"");
        assert!(result.is_err());
    }

    #[test]
    fn test_es_bulk_decode_create() {
        let codec = EsBulkCodec::default();
        let data = b"{\"create\":{\"_index\":\"logs\"}}\n{\"message\":\"new doc\"}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("new doc"));
    }

    #[test]
    fn test_es_bulk_name() {
        assert_eq!(EsBulkCodec::default().name(), "es_bulk");
    }

    #[test]
    fn test_es_bulk_roundtrip() {
        let codec = EsBulkCodec {
            index: Some("logs".to_string()),
            action: "index".to_string(),
        };
        let event = Event::new("roundtrip");
        let encoded = codec.encode(&event).expect("encode");
        let decoded = codec
            .decode(&encoded)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(decoded.message(), Some("roundtrip"));
    }

    #[test]
    fn test_es_bulk_decode_multiple_actions() {
        let codec = EsBulkCodec::default();
        let data = b"{\"index\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n{\"message\":\"first\"}\n{\"index\":{\"_index\":\"logs\",\"_id\":\"2\"}}\n{\"message\":\"second\"}\n{\"delete\":{\"_index\":\"logs\",\"_id\":\"3\"}}";
        let events = codec.decode(data).expect("decode");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message(), Some("first"));
        assert_eq!(events[1].message(), Some("second"));
        assert!(events[2].message().is_none()); // delete has no source
    }
}
