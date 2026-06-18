// SPDX-License-Identifier: Apache-2.0
//! Dots codec — outputs a dot (`.`) for each event, useful for throughput monitoring.

use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;

use crate::Codec;

/// Dots codec configuration.
#[derive(Debug, Clone, Default)]
pub struct DotsCodec;

impl DotsCodec {
    pub fn from_config(_settings: &serde_json::Value) -> Result<Self> {
        Ok(Self)
    }
}

impl Codec for DotsCodec {
    fn name(&self) -> &'static str {
        "dots"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        Ok(vec![Event::new(text.trim_end())])
    }

    fn encode(&self, _event: &Event) -> Result<Vec<u8>> {
        Ok(b".".to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dots_encode() {
        let codec = DotsCodec;
        let event = Event::new("test");
        let bytes = codec.encode(&event).expect("encode");
        assert_eq!(bytes, b".");
    }

    #[test]
    fn test_dots_decode() {
        let codec = DotsCodec;
        let event = codec
            .decode(b"hello")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello"));
    }

    #[test]
    fn test_dots_name() {
        assert_eq!(DotsCodec.name(), "dots");
    }

    #[test]
    fn test_dots_encode_multiple() {
        let codec = DotsCodec;
        let mut output = Vec::new();
        for _ in 0..5 {
            output.extend_from_slice(&codec.encode(&Event::new("x")).expect("encode"));
        }
        assert_eq!(output, b".....");
    }
}
