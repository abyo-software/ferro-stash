// SPDX-License-Identifier: Apache-2.0
//! Protobuf codec — Protocol Buffers wire format decoder/encoder.
//!
//! Decodes protobuf wire format into events with field numbers as keys.
//! Without a `.proto` schema, fields are decoded generically by wire type.
//!
//! Wire types:
//! - 0: Varint (int32, int64, uint32, uint64, sint32, sint64, bool, enum)
//! - 1: 64-bit (fixed64, sfixed64, double)
//! - 2: Length-delimited (string, bytes, embedded messages, packed repeated fields)
//! - 5: 32-bit (fixed32, sfixed32, float)

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Protobuf codec configuration.
#[derive(Debug, Clone, Default)]
pub struct ProtobufCodec {
    /// Optional class/message name hint for metadata.
    pub class_name: Option<String>,
    /// Whether to include raw bytes in the output.
    pub include_raw: bool,
}

impl ProtobufCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let class_name = settings
            .get("class_name")
            .and_then(|v| v.as_str())
            .map(String::from);
        let include_raw = settings
            .get("include_raw")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        Ok(Self {
            class_name,
            include_raw,
        })
    }

    /// Read a varint from the buffer.
    fn read_varint(data: &[u8], offset: &mut usize) -> Option<u64> {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            if *offset >= data.len() {
                return None;
            }
            let byte = data[*offset];
            *offset += 1;
            result |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
        Some(result)
    }

    /// Decode protobuf wire format fields.
    fn decode_fields(data: &[u8]) -> Vec<(u32, EventValue)> {
        let mut fields = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            let tag = match Self::read_varint(data, &mut offset) {
                Some(t) => t,
                None => break,
            };

            let field_number = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;

            let value = match wire_type {
                0 => {
                    // Varint
                    match Self::read_varint(data, &mut offset) {
                        Some(v) => EventValue::Integer(v as i64),
                        None => break,
                    }
                }
                1 => {
                    // 64-bit
                    let Some(end) = offset.checked_add(8) else {
                        break;
                    };
                    if end > data.len() {
                        break;
                    }
                    let bytes = &data[offset..end];
                    offset = end;
                    let v = f64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    if v.fract() == 0.0 && v.abs() < i64::MAX as f64 {
                        EventValue::Integer(v as i64)
                    } else {
                        EventValue::Float(v)
                    }
                }
                2 => {
                    // Length-delimited.
                    //
                    // The varint here is attacker-controlled; values
                    // near `u64::MAX` cast to a near-`usize::MAX`
                    // length, so `offset + len` must use checked
                    // arithmetic to avoid an `attempt to add with
                    // overflow` panic in dev / abort in release with
                    // `overflow-checks = true`. Discovered by 60s
                    // smoke fuzz on `codec_decode` 2026-05-02; see
                    // `tests::test_protobuf_decode_huge_varint_length_no_overflow`.
                    let len = match Self::read_varint(data, &mut offset) {
                        Some(l) => l as usize,
                        None => break,
                    };
                    let Some(end) = offset.checked_add(len) else {
                        break;
                    };
                    if end > data.len() {
                        break;
                    }
                    let bytes = &data[offset..end];
                    offset = end;

                    // Try to decode as UTF-8 string first
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if s.chars()
                            .all(|c| !c.is_control() || c == '\n' || c == '\r' || c == '\t')
                        {
                            EventValue::String(s.to_string())
                        } else {
                            // Try as nested message
                            let nested = Self::decode_fields(bytes);
                            if nested.is_empty() {
                                EventValue::String(hex_encode(bytes))
                            } else {
                                let mut obj = indexmap::IndexMap::new();
                                for (num, val) in nested {
                                    obj.insert(format!("field_{num}"), val);
                                }
                                EventValue::Object(obj)
                            }
                        }
                    } else {
                        // Try as nested message
                        let nested = Self::decode_fields(bytes);
                        if nested.is_empty() {
                            EventValue::String(hex_encode(bytes))
                        } else {
                            let mut obj = indexmap::IndexMap::new();
                            for (num, val) in nested {
                                obj.insert(format!("field_{num}"), val);
                            }
                            EventValue::Object(obj)
                        }
                    }
                }
                5 => {
                    // 32-bit
                    let Some(end) = offset.checked_add(4) else {
                        break;
                    };
                    if end > data.len() {
                        break;
                    }
                    let bytes = &data[offset..end];
                    offset = end;
                    let v = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    EventValue::Float(f64::from(v))
                }
                _ => {
                    // Unknown wire type — skip
                    break;
                }
            };

            fields.push((field_number, value));
        }

        fields
    }
}

fn hex_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    data.iter()
        .fold(String::with_capacity(data.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

impl Codec for ProtobufCodec {
    fn name(&self) -> &'static str {
        "protobuf"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        if data.is_empty() {
            return Err(FerroStashError::Codec("empty protobuf data".to_string()));
        }

        let fields = Self::decode_fields(data);
        if fields.is_empty() {
            return Err(FerroStashError::Codec(
                "no valid protobuf fields decoded".to_string(),
            ));
        }

        let mut event = Event::empty();

        for (field_number, value) in fields {
            event.set(format!("field_{field_number}"), value);
        }

        if let Some(ref class_name) = self.class_name {
            event.set("protobuf_class", EventValue::String(class_name.clone()));
        }

        if self.include_raw {
            event.set("raw_bytes", EventValue::String(hex_encode(data)));
        }

        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        // Encode as JSON since we don't have a schema for proper protobuf encoding
        let json = event.to_json();
        serde_json::to_vec(&json)
            .map_err(|e| FerroStashError::Codec(format!("protobuf encode error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protobuf_decode_varint() {
        let codec = ProtobufCodec::default();
        // field 1, wire type 0 (varint), value 150
        // tag = (1 << 3) | 0 = 0x08
        // value 150 = 0x96 0x01
        let data = vec![0x08, 0x96, 0x01];
        let event = codec
            .decode(&data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("field_1"), Some(&EventValue::Integer(150)));
    }

    #[test]
    fn test_protobuf_decode_string() {
        let codec = ProtobufCodec::default();
        // field 2, wire type 2 (length-delimited), value "testing"
        // tag = (2 << 3) | 2 = 0x12
        // length = 7
        let mut data = vec![0x12, 0x07];
        data.extend_from_slice(b"testing");
        let event = codec
            .decode(&data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("field_2"),
            Some(&EventValue::String("testing".into()))
        );
    }

    #[test]
    fn test_protobuf_decode_multiple_fields() {
        let codec = ProtobufCodec::default();
        // field 1 varint 42, field 2 string "hi"
        let data = vec![0x08, 0x2A, 0x12, 0x02, b'h', b'i'];
        let event = codec
            .decode(&data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("field_1"), Some(&EventValue::Integer(42)));
        assert_eq!(event.get("field_2"), Some(&EventValue::String("hi".into())));
    }

    #[test]
    fn test_protobuf_decode_empty() {
        let codec = ProtobufCodec::default();
        assert!(codec.decode(b"").is_err());
    }

    #[test]
    fn test_protobuf_with_class_name() {
        let codec = ProtobufCodec {
            class_name: Some("MyMessage".to_string()),
            include_raw: false,
        };
        let data = vec![0x08, 0x01];
        let event = codec
            .decode(&data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("protobuf_class"),
            Some(&EventValue::String("MyMessage".into()))
        );
    }

    #[test]
    fn test_protobuf_include_raw() {
        let codec = ProtobufCodec {
            class_name: None,
            include_raw: true,
        };
        let data = vec![0x08, 0x01];
        let event = codec
            .decode(&data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_field("raw_bytes"));
    }

    #[test]
    fn test_protobuf_encode() {
        let codec = ProtobufCodec::default();
        let event = Event::new("test");
        let bytes = codec.encode(&event).expect("encode");
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_protobuf_name() {
        assert_eq!(ProtobufCodec::default().name(), "protobuf");
    }

    #[test]
    fn test_protobuf_from_config() {
        let settings = serde_json::json!({
            "class_name": "MyProto",
            "include_raw": true
        });
        let codec = ProtobufCodec::from_config(&settings).expect("config");
        assert_eq!(codec.class_name, Some("MyProto".to_string()));
        assert!(codec.include_raw);
    }

    /// Regression: a length-delimited field whose varint length is near
    /// `u64::MAX` must not cause `attempt to add with overflow` when
    /// `offset + len` is computed. Discovered by 60s smoke fuzz on
    /// `codec_decode` (2026-05-02). Corpus byte-for-byte matches
    /// `fuzz/corpus/codec_decode/regression-protobuf-len-overflow-2026-05-02`.
    ///
    /// Body layout for the multiplexed fuzz target was
    /// `[selector=0x6e, ...]` where selector%21 == 5 (protobuf). The
    /// trailing `0x6e` got dispatched as protobuf input, producing tag
    /// `0x0a` (field 1, wire type 2, length-delimited) followed by a
    /// 10-byte varint that decodes to a value with the high bit set —
    /// `as usize` casts that to a near-u64::MAX size, and the
    /// subsequent `offset + len` panicked on overflow.
    ///
    /// Direct reproducer (already-stripped selector byte):
    #[test]
    fn test_protobuf_decode_huge_varint_length_no_overflow() {
        let codec = ProtobufCodec::default();
        // Tag 0x0a = field 1, wire type 2 (length-delimited), then a
        // 10-byte varint with all top bits set decodes to ~u64::MAX,
        // which used to overflow `offset + len`.
        let data = [
            0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff,
            0x5b, 0x6e,
        ];
        // Must not panic. Either Ok with no usable fields, or Err — both
        // are fine; abort-on-overflow is what we are guarding against.
        let _ = codec.decode(&data);
    }

    /// Regression: identical shape but starting from the original
    /// 17-byte fuzz seed (selector byte `0x6e` first, body 16 bytes).
    /// Decoded directly through `ProtobufCodec::decode` to avoid
    /// pulling in the multiplexer logic.
    #[test]
    fn test_protobuf_decode_corpus_regression_2026_05_02() {
        let codec = ProtobufCodec::default();
        let corpus: [u8; 16] = [
            0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff,
            0x5b, 0x6e,
        ];
        // No assertion on Ok/Err — only that we don't panic / abort.
        let _ = codec.decode(&corpus);
    }
}
