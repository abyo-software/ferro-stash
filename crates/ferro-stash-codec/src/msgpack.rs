// SPDX-License-Identifier: Apache-2.0
//! MessagePack codec — binary serialization using the MessagePack format.
//!
//! Encodes/decodes events using MessagePack, which is more compact than JSON.
//! Common in high-throughput scenarios and inter-service communication.

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;

use crate::Codec;

/// MessagePack codec configuration.
#[derive(Debug, Clone, Default)]
pub struct MsgpackCodec {
    /// Target field for decoded data (None = merge into root).
    pub target: Option<String>,
}

impl MsgpackCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self { target })
    }
}

impl Codec for MsgpackCodec {
    fn name(&self) -> &'static str {
        "msgpack"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let value: serde_json::Value = rmp_serde::from_slice(data)
            .map_err(|e| FerroStashError::Codec(format!("msgpack decode error: {e}")))?;

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
        rmp_serde::to_vec(&json)
            .map_err(|e| FerroStashError::Codec(format!("msgpack encode error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_stash_core::event::EventValue;

    #[test]
    fn test_msgpack_roundtrip() {
        let codec = MsgpackCodec::default();
        let event = Event::new("hello msgpack");
        let encoded = codec.encode(&event).expect("encode");
        let decoded = codec
            .decode(&encoded)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(decoded.message(), Some("hello msgpack"));
    }

    #[test]
    fn test_msgpack_with_fields() {
        let codec = MsgpackCodec::default();
        let mut event = Event::new("test");
        event.set("host", EventValue::String("server01".into()));
        event.set("port", EventValue::Integer(8080));
        let encoded = codec.encode(&event).expect("encode");
        let decoded = codec
            .decode(&encoded)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(decoded.message(), Some("test"));
        assert_eq!(
            decoded.get("host"),
            Some(&EventValue::String("server01".into()))
        );
        assert_eq!(decoded.get("port"), Some(&EventValue::Integer(8080)));
    }

    #[test]
    fn test_msgpack_with_target() {
        let codec = MsgpackCodec {
            target: Some("data".to_string()),
        };
        let original = Event::new("test");
        let encoded = codec.encode(&original).expect("encode");
        let decoded = codec
            .decode(&encoded)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(decoded.has_field("data"));
    }

    #[test]
    fn test_msgpack_invalid_data() {
        let codec = MsgpackCodec::default();
        // Use truly invalid msgpack (truncated map with missing values)
        let result = codec.decode(b"\x82\xa1a");
        assert!(result.is_err());
    }

    #[test]
    fn test_msgpack_name() {
        assert_eq!(MsgpackCodec::default().name(), "msgpack");
    }

    #[test]
    fn test_msgpack_compact() {
        let codec = MsgpackCodec::default();
        let json_codec = crate::json::JsonCodec::default();
        let event = Event::new("compactness test");
        let msgpack_bytes = codec.encode(&event).expect("msgpack");
        let json_bytes = json_codec.encode(&event).expect("json");
        // MessagePack should be more compact than JSON
        assert!(msgpack_bytes.len() < json_bytes.len());
    }
}
