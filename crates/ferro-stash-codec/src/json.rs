// SPDX-License-Identifier: Apache-2.0
//! JSON codec — decodes JSON objects into events and encodes events as JSON.

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;

use crate::Codec;

/// JSON codec configuration.
#[derive(Debug, Clone, Default)]
pub struct JsonCodec {
    /// Target field to place the parsed JSON (None = merge into root).
    pub target: Option<String>,
    /// Whether to pretty-print output.
    pub pretty: bool,
}

impl JsonCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);
        let pretty = settings
            .get("pretty")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        Ok(Self { target, pretty })
    }
}

impl Codec for JsonCodec {
    fn name(&self) -> &'static str {
        "json"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let value: serde_json::Value = serde_json::from_slice(data)
            .map_err(|e| FerroStashError::Codec(format!("JSON decode error: {e}")))?;

        if let Some(ref target) = self.target {
            let mut event = Event::empty();
            event.set(target.clone(), value.into());
            Ok(vec![event])
        } else {
            Ok(vec![Event::from_json(value)])
        }
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let json = event.to_json();
        let bytes = if self.pretty {
            serde_json::to_vec_pretty(&json)
        } else {
            serde_json::to_vec(&json)
        }
        .map_err(|e| FerroStashError::Codec(format!("JSON encode error: {e}")))?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_decode() {
        let codec = JsonCodec::default();
        let data = br#"{"message": "hello", "host": "server01"}"#;
        let event = codec
            .decode(data)
            .expect("decode failed")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello"));
        assert!(event.has_field("host"));
    }

    #[test]
    fn test_json_decode_with_target() {
        let codec = JsonCodec {
            target: Some("data".to_string()),
            pretty: false,
        };
        let data = br#"{"key": "value"}"#;
        let event = codec
            .decode(data)
            .expect("decode failed")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_field("data"));
    }

    #[test]
    fn test_json_encode() {
        let codec = JsonCodec::default();
        let event = Event::new("hello world");
        let bytes = codec.encode(&event).expect("encode failed");
        let text = String::from_utf8(bytes).expect("utf8 failed");
        assert!(text.contains("hello world"));
        assert!(text.contains("@timestamp"));
    }

    #[test]
    fn test_json_decode_invalid() {
        let codec = JsonCodec::default();
        let result = codec.decode(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_roundtrip() {
        let codec = JsonCodec::default();
        let event = Event::new("roundtrip test");
        let encoded = codec.encode(&event).expect("encode");
        let decoded = codec
            .decode(&encoded)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(decoded.message(), Some("roundtrip test"));
    }
}
