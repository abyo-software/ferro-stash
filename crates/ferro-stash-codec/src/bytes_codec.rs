// SPDX-License-Identifier: Apache-2.0
//! Bytes codec — raw binary passthrough without any parsing or transformation.

use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Bytes codec — passes raw bytes as the `message` field without line splitting.
///
/// Unlike the `plain` codec which trims newlines, `bytes` preserves the exact byte content.
/// Useful for binary protocols or when line boundaries are handled upstream.
#[derive(Debug, Clone, Default)]
pub struct BytesCodec;

impl BytesCodec {
    pub fn from_config(_settings: &serde_json::Value) -> Result<Self> {
        Ok(Self)
    }
}

impl Codec for BytesCodec {
    fn name(&self) -> &'static str {
        "bytes"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let mut event = Event::new(text.as_ref());
        event.set("content_length", EventValue::Integer(data.len() as i64));
        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let msg = event.message().unwrap_or("");
        Ok(msg.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_decode() {
        let codec = BytesCodec;
        let event = codec
            .decode(b"hello\nworld\n")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello\nworld\n"));
        assert_eq!(event.get("content_length"), Some(&EventValue::Integer(12)));
    }

    #[test]
    fn test_bytes_encode() {
        let codec = BytesCodec;
        let event = Event::new("raw data");
        let bytes = codec.encode(&event).expect("encode");
        assert_eq!(bytes, b"raw data");
    }

    #[test]
    fn test_bytes_binary_data() {
        let codec = BytesCodec;
        let data = b"\x00\x01\x02\xff";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("content_length"), Some(&EventValue::Integer(4)));
    }

    #[test]
    fn test_bytes_name() {
        assert_eq!(BytesCodec.name(), "bytes");
    }

    #[test]
    fn test_bytes_empty() {
        let codec = BytesCodec;
        let event = codec
            .decode(b"")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some(""));
        assert_eq!(event.get("content_length"), Some(&EventValue::Integer(0)));
    }
}
