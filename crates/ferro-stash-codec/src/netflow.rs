// SPDX-License-Identifier: Apache-2.0
//! NetFlow codec — decodes NetFlow v5, v9, and IPFIX (v10) binary flow records.
//!
//! NetFlow v5: Fixed 48-byte records with a 24-byte header.
//! NetFlow v9: Template-based flexible records.
//! IPFIX (v10): IETF standard extension of NetFlow v9.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Maximum number of distinct templates retained in the persistent cache.
///
/// Each template id maps to a `Vec<TemplateField>`. Without a bound an
/// attacker can stream small packets that each declare a fresh template id and
/// make the shared cache accumulate unbounded memory (remote OOM). Real
/// exporters use a handful of templates; 4096 is far beyond any legitimate
/// configuration while still capping total retained memory.
const MAX_TEMPLATES: usize = 4096;

/// Maximum number of fields a single template may declare.
///
/// A crafted v9/IPFIX template flowset can claim `field_count = 65535`
/// (≈ 256 KiB per template once stored). Real templates have at most a few
/// dozen fields; 1024 is a generous sanity cap that still rejects the abusive
/// extreme. Each field definition is 4 bytes on the wire (two `u16`s).
const MAX_TEMPLATE_FIELDS: usize = 1024;

/// Bytes per template field definition on the wire (field_type u16 + length u16).
const TEMPLATE_FIELD_DEF_LEN: usize = 4;

/// NetFlow codec supporting v5, v9, and IPFIX.
#[derive(Debug, Clone)]
pub struct NetflowCodec {
    /// Template cache for v9/IPFIX (shared across decodes).
    templates: Arc<Mutex<HashMap<u16, Vec<TemplateField>>>>,
    /// Which version to target for encoding (default: 5).
    pub target_version: u16,
}

#[derive(Debug, Clone)]
struct TemplateField {
    field_type: u16,
    length: u16,
}

impl Default for NetflowCodec {
    fn default() -> Self {
        Self {
            templates: Arc::new(Mutex::new(HashMap::new())),
            target_version: 5,
        }
    }
}

impl NetflowCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let target_version = settings
            .get("target_version")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(5) as u16;
        Ok(Self {
            templates: Arc::new(Mutex::new(HashMap::new())),
            target_version,
        })
    }

    /// Get the field name for a NetFlow v9/IPFIX field type ID.
    fn field_name(field_type: u16) -> &'static str {
        match field_type {
            1 => "in_bytes",
            2 => "in_pkts",
            3 => "flows",
            4 => "protocol",
            // IANA NetFlow v9/IPFIX Information Elements (RFC 5102 + extensions)
            5 => "src_tos",
            6 => "tcp_flags",
            7 => "l4_src_port",
            8 => "ipv4_src_addr",
            9 => "src_mask",
            10 => "input_snmp",
            11 => "l4_dst_port",
            12 => "ipv4_dst_addr",
            13 => "dst_mask",
            14 => "output_snmp",
            15 => "ipv4_next_hop",
            16 => "src_as",
            17 => "dst_as",
            18 => "bgp_ipv4_next_hop",
            19 => "mul_dst_pkts",
            20 => "mul_dst_bytes",
            21 => "last_switched",
            22 => "first_switched",
            23 => "out_bytes",
            24 => "out_pkts",
            25 => "min_pkt_length",
            26 => "max_pkt_length",
            27 => "ipv6_src_addr",
            28 => "ipv6_dst_addr",
            29 => "ipv6_src_mask",
            30 => "ipv6_dst_mask",
            31 => "ipv6_flow_label",
            32 => "icmp_type",
            33 => "mul_igmp_type",
            34 => "sampling_interval",
            35 => "sampling_algorithm",
            36 => "flow_active_timeout",
            37 => "flow_inactive_timeout",
            38 => "engine_type",
            39 => "engine_id",
            40 => "total_bytes_exp",
            41 => "total_pkts_exp",
            42 => "total_flows_exp",
            44 => "ipv4_src_prefix",
            45 => "ipv4_dst_prefix",
            46 => "mpls_top_label_type",
            47 => "mpls_top_label_ip_addr",
            48 => "flow_sampler_id",
            49 => "flow_sampler_mode",
            50 => "flow_sampler_random_interval",
            52 => "min_ttl",
            53 => "max_ttl",
            54 => "ipv4_ident",
            55 => "dst_tos",
            56 => "in_src_mac",
            57 => "out_dst_mac",
            58 => "src_vlan",
            59 => "dst_vlan",
            60 => "ip_protocol_version",
            61 => "direction",
            62 => "ipv6_next_hop",
            63 => "bgp_ipv6_next_hop",
            64 => "ipv6_option_headers",
            70 => "mpls_label_1",
            71 => "mpls_label_2",
            72 => "mpls_label_3",
            73 => "mpls_label_4",
            74 => "mpls_label_5",
            75 => "mpls_label_6",
            76 => "mpls_label_7",
            77 => "mpls_label_8",
            78 => "mpls_label_9",
            79 => "mpls_label_10",
            80 => "in_dst_mac",
            81 => "out_src_mac",
            82 => "interface_name",
            83 => "interface_description",
            85 => "octet_total_count",
            86 => "packet_total_count",
            88 => "fragment_offset",
            89 => "forwarding_status",
            90 => "mpls_vpn_rd",
            128 => "bgp_next_adjacent_as",
            129 => "bgp_prev_adjacent_as",
            130 => "exporter_ipv4_address",
            131 => "exporter_ipv6_address",
            136 => "flow_sampler_id",
            139 => "icmp_type_ipv4",
            140 => "icmp_code_ipv4",
            141 => "icmp_type_ipv6",
            142 => "icmp_code_ipv6",
            148 => "connection_id",
            150 => "flow_start_milliseconds",
            151 => "flow_end_milliseconds",
            152 => "flow_start_microseconds",
            153 => "flow_end_microseconds",
            154 => "flow_start_nanoseconds",
            155 => "flow_end_nanoseconds",
            156 => "flow_start_delta_microseconds",
            157 => "flow_end_delta_microseconds",
            158 => "system_init_time_milliseconds",
            159 => "flow_duration_milliseconds",
            160 => "flow_duration_microseconds",
            161 => "flow_end_reason",
            176 => "icmp_type_code_ipv4",
            177 => "icmp_type_code_ipv6",
            225 => "post_nat_source_ipv4_address",
            226 => "post_nat_destination_ipv4_address",
            227 => "post_napt_source_transport_port",
            228 => "post_napt_destination_transport_port",
            230 => "nat_originating_address_realm",
            231 => "nat_event",
            233 => "firewall_event",
            234 => "ingress_vrfid",
            235 => "egress_vrfid",
            _ => "unknown",
        }
    }

    /// Parse a NetFlow v5 packet — one event per flow record.
    fn decode_v5(&self, data: &[u8]) -> Result<Vec<Event>> {
        if data.len() < 24 {
            return Err(FerroStashError::Codec(
                "netflow v5: packet too short for header".to_string(),
            ));
        }

        let count = u16::from_be_bytes([data[2], data[3]]) as usize;
        let sys_uptime = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let unix_secs = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        let unix_nsecs = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
        let flow_sequence = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
        let engine_type = data[20];
        let engine_id = data[21];

        let ts = chrono::DateTime::from_timestamp(i64::from(unix_secs), unix_nsecs);

        // Clamp the pre-allocation to what the body can actually hold. The
        // header `count` is attacker-controlled (up to 65535 → ~12 MB transient
        // over-allocation) but each v5 record is 48 bytes after the 24-byte
        // header. The record loop below is bounds-checked, so this only removes
        // the speculative over-allocation; it never drops valid records.
        let max_records = data.len().saturating_sub(24) / 48;
        let cap = count.min(max_records).max(1);
        let mut events = Vec::with_capacity(cap);

        // Parse ALL flow records (each 48 bytes, starting at offset 24)
        for i in 0..count {
            let rec_offset = 24 + i * 48;
            if rec_offset + 48 > data.len() {
                break;
            }
            let rec = &data[rec_offset..rec_offset + 48];

            let mut event = Event::empty();
            event.set("netflow_version", EventValue::Integer(5));
            event.set("flow_count", EventValue::Integer(count as i64));
            event.set("sys_uptime_ms", EventValue::Integer(i64::from(sys_uptime)));
            event.set("unix_secs", EventValue::Integer(i64::from(unix_secs)));
            event.set("unix_nsecs", EventValue::Integer(i64::from(unix_nsecs)));
            event.set(
                "flow_sequence",
                EventValue::Integer(i64::from(flow_sequence)),
            );
            event.set("engine_type", EventValue::Integer(i64::from(engine_type)));
            event.set("engine_id", EventValue::Integer(i64::from(engine_id)));

            if let Some(dt) = ts {
                event.timestamp = dt;
            }

            let src_addr = format!("{}.{}.{}.{}", rec[0], rec[1], rec[2], rec[3]);
            let dst_addr = format!("{}.{}.{}.{}", rec[4], rec[5], rec[6], rec[7]);
            let next_hop = format!("{}.{}.{}.{}", rec[8], rec[9], rec[10], rec[11]);
            let input = u16::from_be_bytes([rec[12], rec[13]]);
            let output = u16::from_be_bytes([rec[14], rec[15]]);
            let d_pkts = u32::from_be_bytes([rec[16], rec[17], rec[18], rec[19]]);
            let d_octets = u32::from_be_bytes([rec[20], rec[21], rec[22], rec[23]]);
            let src_port = u16::from_be_bytes([rec[32], rec[33]]);
            let dst_port = u16::from_be_bytes([rec[34], rec[35]]);
            let tcp_flags = rec[37];
            let protocol = rec[38];

            event.set("ipv4_src_addr", EventValue::String(src_addr));
            event.set("ipv4_dst_addr", EventValue::String(dst_addr));
            event.set("ipv4_next_hop", EventValue::String(next_hop));
            event.set("input_snmp", EventValue::Integer(i64::from(input)));
            event.set("output_snmp", EventValue::Integer(i64::from(output)));
            event.set("in_pkts", EventValue::Integer(i64::from(d_pkts)));
            event.set("in_bytes", EventValue::Integer(i64::from(d_octets)));
            event.set("l4_src_port", EventValue::Integer(i64::from(src_port)));
            event.set("l4_dst_port", EventValue::Integer(i64::from(dst_port)));
            event.set("protocol", EventValue::Integer(i64::from(protocol)));
            event.set("tcp_flags", EventValue::Integer(i64::from(tcp_flags)));

            events.push(event);
        }

        // If no records parsed (count=0), return header-only event
        if events.is_empty() {
            let mut event = Event::empty();
            event.set("netflow_version", EventValue::Integer(5));
            event.set("flow_count", EventValue::Integer(0));
            if let Some(dt) = ts {
                event.timestamp = dt;
            }
            events.push(event);
        }

        Ok(events)
    }

    /// Parse a NetFlow v9 packet.
    fn decode_v9(&self, data: &[u8]) -> Result<Vec<Event>> {
        if data.len() < 20 {
            return Err(FerroStashError::Codec(
                "netflow v9: packet too short for header".to_string(),
            ));
        }

        let count = u16::from_be_bytes([data[2], data[3]]);
        let sys_uptime = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let unix_secs = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        let sequence = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
        let source_id = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);

        let mut event = Event::empty();
        event.set("netflow_version", EventValue::Integer(9));
        event.set("flow_count", EventValue::Integer(i64::from(count)));
        event.set("sys_uptime_ms", EventValue::Integer(i64::from(sys_uptime)));
        event.set("unix_secs", EventValue::Integer(i64::from(unix_secs)));
        event.set("flow_sequence", EventValue::Integer(i64::from(sequence)));
        event.set("source_id", EventValue::Integer(i64::from(source_id)));

        if let Some(dt) = chrono::DateTime::from_timestamp(i64::from(unix_secs), 0) {
            event.timestamp = dt;
        }

        // Parse flowsets
        let mut offset = 20;
        while offset + 4 <= data.len() {
            let flowset_id = u16::from_be_bytes([data[offset], data[offset + 1]]);
            let flowset_length = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;

            if flowset_length < 4 || offset + flowset_length > data.len() {
                break;
            }

            if flowset_id == 0 {
                // Template flowset
                self.parse_v9_template(&data[offset + 4..offset + flowset_length]);
            } else if flowset_id > 255 {
                // Data flowset — parse using cached template
                self.parse_v9_data(
                    flowset_id,
                    &data[offset + 4..offset + flowset_length],
                    &mut event,
                );
            }

            offset += flowset_length;
        }

        Ok(vec![event])
    }

    fn parse_v9_template(&self, data: &[u8]) {
        let mut offset = 0;
        while offset + 4 <= data.len() {
            let template_id = u16::from_be_bytes([data[offset], data[offset + 1]]);
            let field_count = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
            offset += 4;

            // Sanity-cap the declared field count. A crafted template can claim
            // up to 65535 fields (≈ 256 KiB once retained); reject anything
            // beyond what a real exporter would ever emit so the persistent
            // cache can never hold an oversized template.
            if field_count > MAX_TEMPLATE_FIELDS {
                tracing::warn!(
                    template_id,
                    field_count,
                    max = MAX_TEMPLATE_FIELDS,
                    "netflow v9: template field_count exceeds cap, skipping"
                );
                // We cannot trust the (huge) declared count to advance past
                // this template's field defs, so abandon the rest of the
                // flowset rather than risk mis-parsing subsequent templates.
                return;
            }

            // Validate the field definitions are actually present in the packet
            // BEFORE allocating. Each field def is TEMPLATE_FIELD_DEF_LEN bytes.
            // This prevents `Vec::with_capacity(field_count)` from reserving
            // memory for bytes that do not exist (remote OOM via small packet).
            let needed = match field_count.checked_mul(TEMPLATE_FIELD_DEF_LEN) {
                Some(n) => n,
                None => return,
            };
            if offset.checked_add(needed).is_none_or(|end| end > data.len()) {
                // Field defs run past the end of the buffer — the declared
                // count is untrustworthy. Do not allocate; stop parsing.
                tracing::warn!(
                    template_id,
                    field_count,
                    "netflow v9: template field defs truncated, skipping"
                );
                return;
            }

            // Clamp the pre-allocation to what the buffer can actually hold.
            let max_fields_present = data.len().saturating_sub(offset) / TEMPLATE_FIELD_DEF_LEN;
            let mut fields = Vec::with_capacity(field_count.min(max_fields_present));
            for _ in 0..field_count {
                if offset + TEMPLATE_FIELD_DEF_LEN > data.len() {
                    break;
                }
                let field_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
                let length = u16::from_be_bytes([data[offset + 2], data[offset + 3]]);
                fields.push(TemplateField { field_type, length });
                offset += TEMPLATE_FIELD_DEF_LEN;
            }

            if let Ok(mut templates) = self.templates.lock() {
                // Bound the persistent cache. Re-learning an existing template
                // id (the common case — exporters periodically resend) is
                // always allowed; only refuse to grow the map with NEW ids once
                // it is full, so a flood of crafted template ids cannot drive
                // unbounded memory growth.
                if templates.len() >= MAX_TEMPLATES && !templates.contains_key(&template_id) {
                    tracing::warn!(
                        template_id,
                        cache_len = templates.len(),
                        max = MAX_TEMPLATES,
                        "netflow v9: template cache full, rejecting new template id"
                    );
                } else {
                    templates.insert(template_id, fields);
                }
            }
        }
    }

    fn parse_v9_data(&self, template_id: u16, data: &[u8], event: &mut Event) {
        let templates = match self.templates.lock() {
            Ok(t) => t,
            Err(_) => return,
        };

        let template = match templates.get(&template_id) {
            Some(t) => t.clone(),
            None => return,
        };
        drop(templates);

        let mut offset = 0;
        for field in &template {
            let len = field.length as usize;
            if offset + len > data.len() {
                break;
            }

            let field_data = &data[offset..offset + len];
            let name = Self::field_name(field.field_type);

            let value = match len {
                1 => EventValue::Integer(i64::from(field_data[0])),
                2 => EventValue::Integer(i64::from(u16::from_be_bytes([
                    field_data[0],
                    field_data[1],
                ]))),
                4 => {
                    if matches!(field.field_type, 8 | 12 | 15) {
                        // IPv4 address
                        EventValue::String(format!(
                            "{}.{}.{}.{}",
                            field_data[0], field_data[1], field_data[2], field_data[3]
                        ))
                    } else {
                        EventValue::Integer(i64::from(u32::from_be_bytes([
                            field_data[0],
                            field_data[1],
                            field_data[2],
                            field_data[3],
                        ])))
                    }
                }
                8 => {
                    let val = u64::from_be_bytes([
                        field_data[0],
                        field_data[1],
                        field_data[2],
                        field_data[3],
                        field_data[4],
                        field_data[5],
                        field_data[6],
                        field_data[7],
                    ]);
                    EventValue::Integer(val as i64)
                }
                16 => {
                    // IPv6 address
                    let mut segments = Vec::new();
                    for i in (0..16).step_by(2) {
                        segments.push(format!(
                            "{:x}",
                            u16::from_be_bytes([field_data[i], field_data[i + 1]])
                        ));
                    }
                    EventValue::String(segments.join(":"))
                }
                _ => EventValue::String(hex::encode(field_data)),
            };

            if name != "unknown" {
                event.set(name.to_string(), value);
            } else {
                event.set(format!("field_{}", field.field_type), value);
            }

            offset += len;
        }
    }

    /// Decode IPFIX (v10) — similar to NetFlow v9 but with minor header differences.
    fn decode_ipfix(&self, data: &[u8]) -> Result<Vec<Event>> {
        if data.len() < 16 {
            return Err(FerroStashError::Codec(
                "IPFIX: packet too short for header".to_string(),
            ));
        }

        let _length = u16::from_be_bytes([data[2], data[3]]);
        let export_time = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let sequence = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        let observation_domain = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);

        let mut event = Event::empty();
        event.set("netflow_version", EventValue::Integer(10));
        event.set("export_time", EventValue::Integer(i64::from(export_time)));
        event.set("flow_sequence", EventValue::Integer(i64::from(sequence)));
        event.set(
            "observation_domain",
            EventValue::Integer(i64::from(observation_domain)),
        );

        if let Some(dt) = chrono::DateTime::from_timestamp(i64::from(export_time), 0) {
            event.timestamp = dt;
        }

        // Parse sets (same structure as v9 flowsets but with set_id 2 for templates, 3 for option templates)
        let mut offset = 16;
        while offset + 4 <= data.len() {
            let set_id = u16::from_be_bytes([data[offset], data[offset + 1]]);
            let set_length = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;

            if set_length < 4 || offset + set_length > data.len() {
                break;
            }

            if set_id == 2 {
                // Template set
                self.parse_v9_template(&data[offset + 4..offset + set_length]);
            } else if set_id > 255 {
                self.parse_v9_data(set_id, &data[offset + 4..offset + set_length], &mut event);
            }

            offset += set_length;
        }

        Ok(vec![event])
    }
}

impl Codec for NetflowCodec {
    fn name(&self) -> &'static str {
        "netflow"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        if data.len() < 4 {
            return Err(FerroStashError::Codec(
                "netflow: packet too short".to_string(),
            ));
        }

        let version = u16::from_be_bytes([data[0], data[1]]);
        match version {
            5 => self.decode_v5(data),
            9 => self.decode_v9(data),
            10 => self.decode_ipfix(data),
            _ => Err(FerroStashError::Codec(format!(
                "unsupported netflow version: {version}"
            ))),
        }
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        // Encode as JSON representation of the netflow data for output
        let json = event.to_json();
        serde_json::to_vec(&json)
            .map_err(|e| FerroStashError::Codec(format!("netflow encode error: {e}")))
    }
}

/// Hex encoding helper (no external dependency needed).
mod hex {
    use std::fmt::Write;

    pub fn encode(data: &[u8]) -> String {
        data.iter()
            .fold(String::with_capacity(data.len() * 2), |mut s, b| {
                let _ = write!(s, "{b:02x}");
                s
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_v5_packet() -> Vec<u8> {
        let mut pkt = vec![0u8; 72]; // header (24) + 1 record (48)
                                     // Version = 5
        pkt[0] = 0;
        pkt[1] = 5;
        // Count = 1
        pkt[2] = 0;
        pkt[3] = 1;
        // sys_uptime = 1000
        pkt[7] = 0xe8;
        pkt[6] = 0x03;
        // unix_secs = 1712880000
        let ts = 1_712_880_000u32.to_be_bytes();
        pkt[8..12].copy_from_slice(&ts);
        // First record: src=10.0.0.1, dst=192.168.1.1
        pkt[24] = 10;
        pkt[25] = 0;
        pkt[26] = 0;
        pkt[27] = 1;
        pkt[28] = 192;
        pkt[29] = 168;
        pkt[30] = 1;
        pkt[31] = 1;
        // src_port = 12345 at offset 24+32=56
        let sp = 12345u16.to_be_bytes();
        pkt[56] = sp[0];
        pkt[57] = sp[1];
        // dst_port = 80
        pkt[58] = 0;
        pkt[59] = 80;
        // protocol = 6 (TCP) at offset 24+38=62
        pkt[62] = 6;
        pkt
    }

    #[test]
    fn test_netflow_v5_decode() {
        let codec = NetflowCodec::default();
        let pkt = make_v5_packet();
        let event = codec
            .decode(&pkt)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("netflow_version"), Some(&EventValue::Integer(5)));
        assert_eq!(
            event.get("ipv4_src_addr"),
            Some(&EventValue::String("10.0.0.1".into()))
        );
        assert_eq!(
            event.get("ipv4_dst_addr"),
            Some(&EventValue::String("192.168.1.1".into()))
        );
        assert_eq!(event.get("l4_src_port"), Some(&EventValue::Integer(12345)));
        assert_eq!(event.get("l4_dst_port"), Some(&EventValue::Integer(80)));
        assert_eq!(event.get("protocol"), Some(&EventValue::Integer(6)));
    }

    #[test]
    fn test_netflow_v5_too_short() {
        let codec = NetflowCodec::default();
        assert!(codec.decode(&[0, 5, 0]).is_err());
    }

    #[test]
    fn test_netflow_unknown_version() {
        let codec = NetflowCodec::default();
        assert!(codec.decode(&[0, 99, 0, 0]).is_err());
    }

    #[test]
    fn test_netflow_v9_header() {
        let codec = NetflowCodec::default();
        let mut pkt = vec![0u8; 20];
        pkt[0] = 0;
        pkt[1] = 9;
        pkt[2] = 0;
        pkt[3] = 0; // count=0
        let event = codec
            .decode(&pkt)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("netflow_version"), Some(&EventValue::Integer(9)));
    }

    #[test]
    fn test_netflow_ipfix_header() {
        let codec = NetflowCodec::default();
        let mut pkt = vec![0u8; 16];
        pkt[0] = 0;
        pkt[1] = 10;
        let event = codec
            .decode(&pkt)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("netflow_version"), Some(&EventValue::Integer(10)));
    }

    #[test]
    fn test_netflow_name() {
        assert_eq!(NetflowCodec::default().name(), "netflow");
    }

    #[test]
    fn test_netflow_encode() {
        let codec = NetflowCodec::default();
        let event = Event::new("flow data");
        let bytes = codec.encode(&event).expect("encode");
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_netflow_v5_multiple_records() {
        let codec = NetflowCodec::default();
        // Build a v5 packet with 2 flow records (header=24, each record=48)
        let mut pkt = vec![0u8; 24 + 48 * 2];
        pkt[0] = 0;
        pkt[1] = 5;
        pkt[2] = 0;
        pkt[3] = 2; // count=2
                    // Record 1: src=10.0.0.1
        pkt[24] = 10;
        pkt[25] = 0;
        pkt[26] = 0;
        pkt[27] = 1;
        // Record 2: src=10.0.0.2
        pkt[72] = 10;
        pkt[73] = 0;
        pkt[74] = 0;
        pkt[75] = 2;

        let events = codec.decode(&pkt).expect("decode");
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].get("ipv4_src_addr"),
            Some(&EventValue::String("10.0.0.1".into()))
        );
        assert_eq!(
            events[1].get("ipv4_src_addr"),
            Some(&EventValue::String("10.0.0.2".into()))
        );
    }

    /// Build a v9 packet carrying a single template flowset with the given
    /// template id, declared `field_count`, and only `present_fields` actual
    /// 4-byte field definitions in the buffer.
    fn make_v9_template_packet(
        template_id: u16,
        declared_field_count: u16,
        present_fields: usize,
    ) -> Vec<u8> {
        // v9 header (20 bytes)
        let mut pkt = vec![0u8; 20];
        pkt[1] = 9; // version
                    // count, sys_uptime, etc. left at 0 (header parsing tolerates this)

        // Template flowset: flowset_id = 0, then length, then template records.
        // Each template record: template_id(2) + field_count(2) + fields(4*N).
        let body_len = 4 + 4 * present_fields; // template header + present field defs
        let flowset_len = 4 + body_len; // flowset id+length prefix
        let mut flowset = Vec::with_capacity(flowset_len);
        flowset.extend_from_slice(&0u16.to_be_bytes()); // flowset_id = 0 (template)
        flowset.extend_from_slice(&(flowset_len as u16).to_be_bytes());
        flowset.extend_from_slice(&template_id.to_be_bytes());
        flowset.extend_from_slice(&declared_field_count.to_be_bytes());
        for i in 0..present_fields {
            // field_type = (1 + i), length = 4
            flowset.extend_from_slice(&((i as u16) + 1).to_be_bytes());
            flowset.extend_from_slice(&4u16.to_be_bytes());
        }
        pkt.extend_from_slice(&flowset);
        pkt
    }

    /// FINDING 1 (a): a v9 template declaring field_count=65535 with only a few
    /// bytes of field defs must NOT allocate 256 KiB and must be rejected (not
    /// cached) rather than parsing the huge count.
    #[test]
    fn test_netflow_v9_template_field_count_overflow_rejected() {
        let codec = NetflowCodec::default();
        // Declare 65535 fields but provide only 2 actual field defs.
        let pkt = make_v9_template_packet(256, 65535, 2);
        // Decode must not panic / hang / OOM.
        let events = codec.decode(&pkt).expect("decode");
        assert_eq!(events.len(), 1);
        // The template must NOT have been cached (field_count > MAX_TEMPLATE_FIELDS).
        let templates = codec.templates.lock().expect("lock");
        assert!(
            !templates.contains_key(&256),
            "oversized template must not be cached"
        );
    }

    /// FINDING 1 (a) variant: field_count within the field cap but the declared
    /// defs run past the end of the buffer — must be rejected without allocating
    /// for the missing bytes.
    #[test]
    fn test_netflow_v9_template_truncated_field_defs_rejected() {
        let codec = NetflowCodec::default();
        // Declare 100 fields (<= MAX_TEMPLATE_FIELDS) but provide only 1.
        let pkt = make_v9_template_packet(300, 100, 1);
        let events = codec.decode(&pkt).expect("decode");
        assert_eq!(events.len(), 1);
        let templates = codec.templates.lock().expect("lock");
        assert!(
            !templates.contains_key(&300),
            "truncated template must not be cached"
        );
    }

    /// A well-formed template within the caps IS cached normally.
    #[test]
    fn test_netflow_v9_template_valid_cached() {
        let codec = NetflowCodec::default();
        let pkt = make_v9_template_packet(400, 3, 3);
        codec.decode(&pkt).expect("decode");
        let templates = codec.templates.lock().expect("lock");
        let fields = templates.get(&400).expect("template should be cached");
        assert_eq!(fields.len(), 3);
    }

    /// FINDING 1 (a): a template exactly at the field cap is allowed; one over
    /// the cap is rejected. Uses present_fields = declared so the buffer is
    /// well-formed and only the cap governs the decision.
    #[test]
    fn test_netflow_v9_template_field_cap_boundary() {
        let codec = NetflowCodec::default();
        // Exactly at cap: allowed.
        let at_cap = make_v9_template_packet(401, MAX_TEMPLATE_FIELDS as u16, MAX_TEMPLATE_FIELDS);
        codec.decode(&at_cap).expect("decode");
        {
            let templates = codec.templates.lock().expect("lock");
            assert!(templates.contains_key(&401), "at-cap template allowed");
        }
        // One over cap: rejected. (declared count is u16, MAX_TEMPLATE_FIELDS+1.)
        let over_cap = make_v9_template_packet(
            402,
            (MAX_TEMPLATE_FIELDS + 1) as u16,
            MAX_TEMPLATE_FIELDS + 1,
        );
        codec.decode(&over_cap).expect("decode");
        let templates = codec.templates.lock().expect("lock");
        assert!(
            !templates.contains_key(&402),
            "over-cap template rejected"
        );
    }

    /// FINDING 1 (b): the persistent template cache must not grow past
    /// MAX_TEMPLATES no matter how many crafted (distinct) template ids arrive.
    #[test]
    fn test_netflow_v9_template_cache_bounded() {
        let codec = NetflowCodec::default();
        // Send MAX_TEMPLATES + 5000 distinct, well-formed template ids.
        // Template ids must be > 255 to be valid data flowset ids, but for
        // template learning the id space is the full u16; we just need many
        // distinct ids. We iterate well past the cap.
        let total: u32 = MAX_TEMPLATES as u32 + 5000;
        for id in 0..total {
            let pkt = make_v9_template_packet(id as u16, 2, 2);
            codec.decode(&pkt).expect("decode");
        }
        let templates = codec.templates.lock().expect("lock");
        assert!(
            templates.len() <= MAX_TEMPLATES,
            "cache len {} must not exceed MAX_TEMPLATES {}",
            templates.len(),
            MAX_TEMPLATES
        );
    }

    /// Re-learning an already-cached template id must always be allowed even
    /// when the cache is full (exporters periodically resend templates).
    #[test]
    fn test_netflow_v9_template_relearn_when_full() {
        let codec = NetflowCodec::default();
        // Fill the cache.
        for id in 0..(MAX_TEMPLATES as u32) {
            let pkt = make_v9_template_packet(id as u16, 2, 2);
            codec.decode(&pkt).expect("decode");
        }
        // Re-learn an existing id with a different field shape; must update.
        let pkt = make_v9_template_packet(0, 3, 3);
        codec.decode(&pkt).expect("decode");
        let templates = codec.templates.lock().expect("lock");
        let fields = templates.get(&0).expect("re-learned template present");
        assert_eq!(fields.len(), 3, "existing template id must be updatable");
    }

    /// FINDING 2: a v5 header claiming count=65535 but with a short body must
    /// decode the records actually present without speculatively allocating
    /// ~12 MB.
    #[test]
    fn test_netflow_v5_count_overflow_bounded_alloc() {
        let codec = NetflowCodec::default();
        // header (24) + 2 real records (96) = 120 bytes, but count claims 65535.
        let mut pkt = vec![0u8; 24 + 48 * 2];
        pkt[1] = 5; // version
        pkt[2] = 0xff;
        pkt[3] = 0xff; // count = 65535
                       // Record 1 src = 10.0.0.1
        pkt[24] = 10;
        pkt[27] = 1;
        // Record 2 src = 10.0.0.2
        pkt[72] = 10;
        pkt[75] = 2;

        let events = codec.decode(&pkt).expect("decode");
        // Only the 2 records that fit are returned.
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].get("ipv4_src_addr"),
            Some(&EventValue::String("10.0.0.1".into()))
        );
        assert_eq!(
            events[1].get("ipv4_src_addr"),
            Some(&EventValue::String("10.0.0.2".into()))
        );
        // Each event still reports the header's claimed count.
        assert_eq!(events[0].get("flow_count"), Some(&EventValue::Integer(65535)));
    }
}
