// SPDX-License-Identifier: Apache-2.0
//! Plain text codec — each line becomes the `message` field of an event.

use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;

use crate::Codec;

/// Plain text codec configuration.
#[derive(Debug, Clone)]
pub struct PlainCodec {
    /// Character encoding (default: "UTF-8").
    pub charset: String,
    /// Output format string.
    pub format: Option<String>,
}

impl Default for PlainCodec {
    fn default() -> Self {
        Self {
            charset: "UTF-8".to_string(),
            format: None,
        }
    }
}

impl PlainCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let charset = settings
            .get("charset")
            .and_then(|v| v.as_str())
            .unwrap_or("UTF-8")
            .to_string();
        let format = settings
            .get("format")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self { charset, format })
    }
}

impl Codec for PlainCodec {
    fn name(&self) -> &'static str {
        "plain"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let trimmed = text.trim_end_matches('\n').trim_end_matches('\r');
        Ok(vec![Event::new(trimmed)])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        if let Some(ref fmt) = self.format {
            let formatted = event.sprintf(fmt);
            Ok(formatted.into_bytes())
        } else {
            let msg = event.message().unwrap_or("");
            Ok(format!("{msg}\n").into_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_decode() {
        let codec = PlainCodec::default();
        let event = codec
            .decode(b"hello world\n")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello world"));
    }

    #[test]
    fn test_plain_encode() {
        let codec = PlainCodec::default();
        let event = Event::new("test message");
        let bytes = codec.encode(&event).expect("encode");
        assert_eq!(bytes, b"test message\n");
    }

    #[test]
    fn test_plain_encode_with_format() {
        let codec = PlainCodec {
            charset: "UTF-8".to_string(),
            format: Some("%{message} from %{host}".to_string()),
        };
        let mut event = Event::new("hello");
        event.set(
            "host",
            ferro_stash_core::event::EventValue::String("srv01".into()),
        );
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert_eq!(text, "hello from srv01");
    }
}
