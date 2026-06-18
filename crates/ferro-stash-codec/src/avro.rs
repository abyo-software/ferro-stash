// SPDX-License-Identifier: Apache-2.0
//! Avro codec — Apache Avro container format and single-object encoding.
//!
//! Supports decoding Avro Object Container Files (OCF) and encoding events
//! as Avro JSON (schema-less mode) or raw JSON.
//!
//! Avro OCF structure:
//! - 4-byte magic: `Obj\x01`
//! - File header with schema (JSON) and sync marker
//! - Data blocks: count + size + compressed data + sync marker

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use indexmap::IndexMap;

use crate::Codec;

const AVRO_MAGIC: &[u8; 4] = b"Obj\x01";

/// Avro codec configuration.
#[derive(Debug, Clone, Default)]
pub struct AvroCodec {
    /// Schema JSON string (used for encoding).
    pub schema_json: Option<String>,
    /// Target field for decoded data.
    pub target: Option<String>,
}

impl AvroCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let schema_json = settings
            .get("schema")
            .and_then(|v| v.as_str())
            .map(String::from);
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self {
            schema_json,
            target,
        })
    }

    /// Read a variable-length integer (Avro uses zig-zag encoding).
    fn read_varint(data: &[u8], offset: &mut usize) -> Option<i64> {
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
        // Zig-zag decode
        Some(((result >> 1) as i64) ^ -((result & 1) as i64))
    }

    /// Read a length-prefixed string.
    fn read_string(data: &[u8], offset: &mut usize) -> Option<String> {
        let len = Self::read_varint(data, offset)? as usize;
        // checked_add: a varint near u64::MAX would overflow `*offset + len`
        // and panic in overflow-checks builds. See sibling fix in
        // `protobuf.rs` (regression-protobuf-len-overflow-2026-05-02).
        let end = offset.checked_add(len)?;
        if end > data.len() {
            return None;
        }
        let s = String::from_utf8_lossy(&data[*offset..end]).to_string();
        *offset = end;
        Some(s)
    }

    /// Read a length-prefixed byte array.
    fn read_bytes(data: &[u8], offset: &mut usize) -> Option<Vec<u8>> {
        let len = Self::read_varint(data, offset)? as usize;
        let end = offset.checked_add(len)?;
        if end > data.len() {
            return None;
        }
        let bytes = data[*offset..end].to_vec();
        *offset = end;
        Some(bytes)
    }

    /// Decode the Avro file header metadata as a map.
    fn read_header_meta(data: &[u8], offset: &mut usize) -> Option<IndexMap<String, String>> {
        let count = Self::read_varint(data, offset)?;
        let mut meta = IndexMap::new();

        if count > 0 {
            for _ in 0..count {
                let key = Self::read_string(data, offset)?;
                let value_bytes = Self::read_bytes(data, offset)?;
                let value = String::from_utf8_lossy(&value_bytes).to_string();
                meta.insert(key, value);
            }
            // Map is terminated by a zero block count
            let _end = Self::read_varint(data, offset)?;
        }

        Some(meta)
    }
}

impl Codec for AvroCodec {
    fn name(&self) -> &'static str {
        "avro"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        // Check if this is an Avro OCF (Object Container File)
        if data.len() >= 4 && &data[0..4] == AVRO_MAGIC {
            return self.decode_ocf(data).map(|e| vec![e]);
        }

        // Try to decode as JSON (Avro JSON encoding)
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) {
            if let Some(ref target) = self.target {
                let mut event = Event::empty();
                event.set(target.clone(), value.into());
                return Ok(vec![event]);
            }
            return Ok(vec![Event::from_json(value)]);
        }

        Err(FerroStashError::Codec(
            "unrecognized Avro format".to_string(),
        ))
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        // Encode as Avro JSON representation
        let json = event.to_json();
        serde_json::to_vec(&json)
            .map_err(|e| FerroStashError::Codec(format!("avro encode error: {e}")))
    }
}

impl AvroCodec {
    fn decode_ocf(&self, data: &[u8]) -> Result<Event> {
        let mut offset = 4; // skip magic

        // Read file header metadata
        let meta = Self::read_header_meta(data, &mut offset).ok_or_else(|| {
            FerroStashError::Codec("failed to read Avro header metadata".to_string())
        })?;

        // Read 16-byte sync marker
        if offset + 16 > data.len() {
            return Err(FerroStashError::Codec(
                "Avro OCF too short for sync marker".to_string(),
            ));
        }
        offset += 16; // skip sync marker

        let schema_str = meta.get("avro.schema").cloned().unwrap_or_default();
        let codec_name = meta.get("avro.codec").cloned().unwrap_or_default();

        let mut event = Event::empty();
        event.set("avro_schema", EventValue::String(schema_str));
        event.set("avro_codec", EventValue::String(codec_name));
        event.set("avro_format", EventValue::String("ocf".to_string()));

        // Read first data block
        if offset + 4 <= data.len() {
            let block_count = Self::read_varint(data, &mut offset);
            if let Some(count) = block_count {
                event.set("block_record_count", EventValue::Integer(count));
            }

            let block_size = Self::read_varint(data, &mut offset);
            if let Some(size) = block_size {
                event.set("block_size", EventValue::Integer(size));

                // Extract block data.
                //
                // `size` is a signed zig-zag varint and therefore
                // attacker-controlled — both negative values and values
                // near `i64::MAX` reach this point. The previous
                // `(offset as i64 + size).min(data.len() as i64) as usize`
                // chain wrapped negative results to a near-`u64::MAX`
                // index (162-byte input → "range end index
                // 18446744062033484611 out of range for slice of length
                // 161" panic), and would also overflow on huge positive
                // values. Sibling fix to the `read_string` /
                // `read_bytes` `checked_add` cleanup landed in
                // `d1103a1`; the OCF block-data path was missed there.
                // Persisted as
                // `fuzz/corpus/codec_decode/regression-avro-offset-underflow-line188-2026-05-03`.
                let Ok(size_usize) = usize::try_from(size) else {
                    return Ok(event);
                };
                let Some(end_unbounded) = offset.checked_add(size_usize) else {
                    return Ok(event);
                };
                let end = end_unbounded.min(data.len());
                if end > offset {
                    let block_data = &data[offset..end];
                    // Try to interpret as JSON
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(block_data) {
                        if let Some(ref target) = self.target {
                            event.set(target.clone(), value.into());
                        } else {
                            // Merge JSON fields into event
                            if let serde_json::Value::Object(map) = value {
                                for (k, v) in map {
                                    event.set(k, EventValue::from(v));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avro_json_decode() {
        let codec = AvroCodec::default();
        let data = br#"{"message":"hello avro","host":"server01"}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello avro"));
    }

    #[test]
    fn test_avro_json_decode_with_target() {
        let codec = AvroCodec {
            schema_json: None,
            target: Some("data".to_string()),
        };
        let data = br#"{"key":"value"}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_field("data"));
    }

    #[test]
    fn test_avro_ocf_magic_detection() {
        let codec = AvroCodec::default();
        // Minimal OCF — magic + empty metadata (should fail gracefully)
        let mut data = Vec::new();
        data.extend_from_slice(AVRO_MAGIC);
        // Empty map: count=0
        data.push(0);
        // Sync marker (16 bytes)
        data.extend_from_slice(&[0u8; 16]);
        let event = codec
            .decode(&data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("avro_format"),
            Some(&EventValue::String("ocf".into()))
        );
    }

    #[test]
    fn test_avro_encode() {
        let codec = AvroCodec::default();
        let event = Event::new("test avro");
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("test avro"));
    }

    #[test]
    fn test_avro_invalid() {
        let codec = AvroCodec::default();
        assert!(codec.decode(b"\xff\xfe\xfd").is_err());
    }

    #[test]
    fn test_avro_name() {
        assert_eq!(AvroCodec::default().name(), "avro");
    }

    #[test]
    fn test_avro_varint() {
        let data = [0x04]; // zig-zag encoded 2
        let mut offset = 0;
        assert_eq!(AvroCodec::read_varint(&data, &mut offset), Some(2));
    }

    #[test]
    fn test_avro_varint_negative() {
        let data = [0x03]; // zig-zag encoded -2
        let mut offset = 0;
        assert_eq!(AvroCodec::read_varint(&data, &mut offset), Some(-2));
    }

    #[test]
    fn test_avro_from_config() {
        let settings = serde_json::json!({
            "schema": "{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}",
            "target": "data"
        });
        let codec = AvroCodec::from_config(&settings).expect("config");
        assert!(codec.schema_json.is_some());
        assert_eq!(codec.target, Some("data".to_string()));
    }

    /// Regression: an Avro OCF whose header `read_bytes`/`read_string`
    /// length varint decodes to ~`u64::MAX` must not cause `attempt to
    /// add with overflow` when `*offset + len` is computed. Discovered
    /// by 60s smoke fuzz on `codec_decode` (2026-05-02), corpus byte-
    /// for-byte matches
    /// `fuzz/corpus/codec_decode/regression-avro-len-overflow-2026-05-02`.
    /// Same anti-pattern as the protobuf fix landed in the same wave.
    #[test]
    fn test_avro_decode_huge_varint_length_no_overflow() {
        let codec = AvroCodec::default();
        // 36-byte fuzz seed: 'O','b','j',1 magic + minimal header that
        // hits the read_bytes/read_string length-varint branch.
        let data = [
            0x4f, 0x62, 0x6a, 0x01, 0x30, 0x2e, 0x34, 0x30, 0x36, 0x36, 0x36, 0x36, 0x36, 0x30,
            0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x31, 0x31, 0x38, 0x38, 0x30,
            0x34, 0x31, 0x35, 0x36, 0x88, 0xd8, 0x24, 0x86,
        ];
        // Must not panic. Either Ok or Err is fine — guarding against
        // abort-on-overflow.
        let _ = codec.decode(&data);
    }
}
