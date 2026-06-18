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

        let mut events = Vec::with_capacity(count.max(1));

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

            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                if offset + 4 > data.len() {
                    break;
                }
                let field_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
                let length = u16::from_be_bytes([data[offset + 2], data[offset + 3]]);
                fields.push(TemplateField { field_type, length });
                offset += 4;
            }

            if let Ok(mut templates) = self.templates.lock() {
                templates.insert(template_id, fields);
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
}
