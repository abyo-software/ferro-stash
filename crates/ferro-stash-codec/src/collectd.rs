// SPDX-License-Identifier: Apache-2.0
//! Collectd codec — decodes collectd binary protocol and JSON format.
//!
//! Collectd sends metrics in either binary protocol (UDP port 25826)
//! or JSON format (write_http plugin). This codec handles both.
//!
//! Binary protocol uses type-length-value (TLV) parts:
//! - Type (2 bytes) + Length (2 bytes) + Value (variable)

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

// Collectd binary protocol part types
const PART_HOST: u16 = 0x0000;
const PART_TIME: u16 = 0x0001;
const PART_TIME_HR: u16 = 0x0008;
const PART_PLUGIN: u16 = 0x0002;
const PART_PLUGIN_INSTANCE: u16 = 0x0003;
const PART_TYPE: u16 = 0x0004;
const PART_TYPE_INSTANCE: u16 = 0x0005;
const PART_VALUES: u16 = 0x0006;
const PART_INTERVAL: u16 = 0x0007;
const PART_INTERVAL_HR: u16 = 0x0009;

/// Collectd codec configuration.
#[derive(Debug, Clone, Default)]
pub struct CollectdCodec {
    /// Whether to expect JSON format instead of binary (write_http plugin).
    pub json_mode: bool,
}

impl CollectdCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let json_mode = settings
            .get("json_mode")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        Ok(Self { json_mode })
    }

    fn decode_json(&self, data: &[u8]) -> Result<Vec<Event>> {
        let value: serde_json::Value = serde_json::from_slice(data)
            .map_err(|e| FerroStashError::Codec(format!("collectd JSON error: {e}")))?;

        // Collectd JSON format can be an array of records — emit one event per record
        if let Some(arr) = value.as_array() {
            let mut events = Vec::with_capacity(arr.len());
            for record in arr {
                let mut event = Event::from_json(record.clone());
                event.set("collectd_type", EventValue::String("json".to_string()));
                events.push(event);
            }
            if events.is_empty() {
                let mut event = Event::empty();
                event.set("collectd_type", EventValue::String("json".to_string()));
                return Ok(vec![event]);
            }
            return Ok(events);
        }

        let mut event = Event::from_json(value);
        event.set("collectd_type", EventValue::String("json".to_string()));
        Ok(vec![event])
    }

    fn decode_binary(&self, data: &[u8]) -> Result<Vec<Event>> {
        let mut event = Event::empty();
        event.set("collectd_type", EventValue::String("binary".to_string()));

        let mut offset = 0;
        while offset + 4 <= data.len() {
            let part_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
            let part_length = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;

            if part_length < 4 || offset + part_length > data.len() {
                break;
            }

            let payload = &data[offset + 4..offset + part_length];

            match part_type {
                PART_HOST => {
                    let s = String::from_utf8_lossy(payload);
                    event.set(
                        "host",
                        EventValue::String(s.trim_end_matches('\0').to_string()),
                    );
                }
                PART_PLUGIN => {
                    let s = String::from_utf8_lossy(payload);
                    event.set(
                        "plugin",
                        EventValue::String(s.trim_end_matches('\0').to_string()),
                    );
                }
                PART_PLUGIN_INSTANCE => {
                    let s = String::from_utf8_lossy(payload);
                    event.set(
                        "plugin_instance",
                        EventValue::String(s.trim_end_matches('\0').to_string()),
                    );
                }
                PART_TYPE => {
                    let s = String::from_utf8_lossy(payload);
                    event.set(
                        "type",
                        EventValue::String(s.trim_end_matches('\0').to_string()),
                    );
                }
                PART_TYPE_INSTANCE => {
                    let s = String::from_utf8_lossy(payload);
                    event.set(
                        "type_instance",
                        EventValue::String(s.trim_end_matches('\0').to_string()),
                    );
                }
                PART_TIME if payload.len() >= 8 => {
                    let ts = u64::from_be_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ]);
                    event.set("time", EventValue::Integer(ts as i64));
                    if let Some(dt) = chrono::DateTime::from_timestamp(ts as i64, 0) {
                        event.timestamp = dt;
                    }
                }
                PART_TIME_HR if payload.len() >= 8 => {
                    let ts_hr = u64::from_be_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ]);
                    // High-resolution time: top 32 bits = seconds, bottom 32 bits = fraction
                    let secs = (ts_hr >> 30) as i64;
                    let nsecs = ((ts_hr & 0x3FFF_FFFF) * 1_000_000_000 / (1u64 << 30)) as u32;
                    event.set("time_hr", EventValue::Integer(ts_hr as i64));
                    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                        event.timestamp = dt;
                    }
                }
                PART_INTERVAL if payload.len() >= 8 => {
                    let interval = u64::from_be_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ]);
                    event.set("interval", EventValue::Integer(interval as i64));
                }
                PART_INTERVAL_HR if payload.len() >= 8 => {
                    let interval_hr = u64::from_be_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ]);
                    let secs = interval_hr >> 30;
                    event.set("interval", EventValue::Integer(secs as i64));
                }
                PART_VALUES if payload.len() >= 2 => {
                    let num_values = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                    let types_offset = 2;
                    let values_offset = types_offset + num_values;

                    let mut values = Vec::new();
                    for i in 0..num_values {
                        if types_offset + i >= payload.len() {
                            break;
                        }
                        let value_type = payload[types_offset + i];
                        let val_off = values_offset + i * 8;
                        if val_off + 8 > payload.len() {
                            break;
                        }
                        let val_bytes = &payload[val_off..val_off + 8];

                        let value = match value_type {
                            0 => {
                                // Counter (uint64)
                                let v = u64::from_be_bytes([
                                    val_bytes[0],
                                    val_bytes[1],
                                    val_bytes[2],
                                    val_bytes[3],
                                    val_bytes[4],
                                    val_bytes[5],
                                    val_bytes[6],
                                    val_bytes[7],
                                ]);
                                EventValue::Integer(v as i64)
                            }
                            1 => {
                                // Gauge (double, host byte order — little endian on x86)
                                let v = f64::from_le_bytes([
                                    val_bytes[0],
                                    val_bytes[1],
                                    val_bytes[2],
                                    val_bytes[3],
                                    val_bytes[4],
                                    val_bytes[5],
                                    val_bytes[6],
                                    val_bytes[7],
                                ]);
                                EventValue::Float(v)
                            }
                            2 => {
                                // Derive (int64)
                                let v = i64::from_be_bytes([
                                    val_bytes[0],
                                    val_bytes[1],
                                    val_bytes[2],
                                    val_bytes[3],
                                    val_bytes[4],
                                    val_bytes[5],
                                    val_bytes[6],
                                    val_bytes[7],
                                ]);
                                EventValue::Integer(v)
                            }
                            3 => {
                                // Absolute (uint64)
                                let v = u64::from_be_bytes([
                                    val_bytes[0],
                                    val_bytes[1],
                                    val_bytes[2],
                                    val_bytes[3],
                                    val_bytes[4],
                                    val_bytes[5],
                                    val_bytes[6],
                                    val_bytes[7],
                                ]);
                                EventValue::Integer(v as i64)
                            }
                            _ => EventValue::Null,
                        };
                        values.push(value);
                    }

                    if values.len() == 1 {
                        if let Some(v) = values.into_iter().next() {
                            event.set("value", v);
                        }
                    } else {
                        event.set("values", EventValue::Array(values));
                    }
                }
                _ => {
                    // Unknown part type — skip
                }
            }

            offset += part_length;
        }

        Ok(vec![event])
    }
}

impl Codec for CollectdCodec {
    fn name(&self) -> &'static str {
        "collectd"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        if self.json_mode {
            return self.decode_json(data);
        }

        // Auto-detect: if it starts with '[' or '{', treat as JSON
        let first_byte = data.first().copied().unwrap_or(0);
        if first_byte == b'[' || first_byte == b'{' {
            self.decode_json(data)
        } else {
            self.decode_binary(data)
        }
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        // Encode as JSON (collectd write_http compatible)
        let json = event.to_json();
        let mut output = serde_json::to_vec(&json)
            .map_err(|e| FerroStashError::Codec(format!("collectd encode error: {e}")))?;
        output.push(b'\n');
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_binary_packet() -> Vec<u8> {
        let mut pkt = Vec::new();

        // Host part: "web01"
        let host = b"web01\0";
        pkt.extend_from_slice(&PART_HOST.to_be_bytes());
        pkt.extend_from_slice(&((4 + host.len()) as u16).to_be_bytes());
        pkt.extend_from_slice(host);

        // Plugin part: "cpu"
        let plugin = b"cpu\0";
        pkt.extend_from_slice(&PART_PLUGIN.to_be_bytes());
        pkt.extend_from_slice(&((4 + plugin.len()) as u16).to_be_bytes());
        pkt.extend_from_slice(plugin);

        // Type part: "gauge"
        let type_name = b"gauge\0";
        pkt.extend_from_slice(&PART_TYPE.to_be_bytes());
        pkt.extend_from_slice(&((4 + type_name.len()) as u16).to_be_bytes());
        pkt.extend_from_slice(type_name);

        // Time part
        let ts: u64 = 1_712_880_000;
        pkt.extend_from_slice(&PART_TIME.to_be_bytes());
        pkt.extend_from_slice(&12u16.to_be_bytes());
        pkt.extend_from_slice(&ts.to_be_bytes());

        pkt
    }

    #[test]
    fn test_collectd_binary_decode() {
        let codec = CollectdCodec::default();
        let pkt = make_binary_packet();
        let event = codec
            .decode(&pkt)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("host"), Some(&EventValue::String("web01".into())));
        assert_eq!(event.get("plugin"), Some(&EventValue::String("cpu".into())));
        assert_eq!(event.get("type"), Some(&EventValue::String("gauge".into())));
    }

    #[test]
    fn test_collectd_json_decode() {
        let codec = CollectdCodec { json_mode: true };
        let data = br#"[{"host":"web01","plugin":"cpu","time":1712880000,"values":[0.75]}]"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("host"), Some(&EventValue::String("web01".into())));
    }

    #[test]
    fn test_collectd_auto_detect_json() {
        let codec = CollectdCodec::default();
        let data = br#"{"host":"web01","plugin":"memory"}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("host"), Some(&EventValue::String("web01".into())));
    }

    #[test]
    fn test_collectd_encode() {
        let codec = CollectdCodec::default();
        let mut event = Event::empty();
        event.set("host", EventValue::String("web01".into()));
        event.set("plugin", EventValue::String("cpu".into()));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("web01"));
        assert!(text.contains("cpu"));
    }

    #[test]
    fn test_collectd_name() {
        assert_eq!(CollectdCodec::default().name(), "collectd");
    }

    #[test]
    fn test_collectd_from_config() {
        let settings = serde_json::json!({ "json_mode": true });
        let codec = CollectdCodec::from_config(&settings).expect("config");
        assert!(codec.json_mode);
    }

    #[test]
    fn test_collectd_json_multiple_records() {
        let codec = CollectdCodec { json_mode: true };
        let data = br#"[
            {"host":"web01","plugin":"cpu","values":[0.5]},
            {"host":"web01","plugin":"memory","values":[1024]},
            {"host":"web02","plugin":"cpu","values":[0.8]}
        ]"#;
        let events = codec.decode(data).expect("decode");
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0].get("plugin"),
            Some(&EventValue::String("cpu".into()))
        );
        assert_eq!(
            events[1].get("plugin"),
            Some(&EventValue::String("memory".into()))
        );
        assert_eq!(
            events[2].get("host"),
            Some(&EventValue::String("web02".into()))
        );
    }
}
