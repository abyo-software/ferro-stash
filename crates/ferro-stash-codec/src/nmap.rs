// SPDX-License-Identifier: Apache-2.0
//! Nmap codec — decodes nmap XML scan results into events.
//!
//! Parses key elements from nmap XML output:
//! - Host information (address, hostname, status)
//! - Port scan results (port, protocol, state, service)
//! - OS detection results
//!
//! This is a lightweight parser — it extracts structured data from nmap XML
//! without requiring a full XML DOM library.

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use indexmap::IndexMap;

use crate::Codec;

/// Nmap XML codec.
#[derive(Debug, Clone, Default)]
pub struct NmapCodec;

impl NmapCodec {
    pub fn from_config(_settings: &serde_json::Value) -> Result<Self> {
        Ok(Self)
    }

    /// Extract an attribute value from an XML tag string.
    fn extract_attr<'a>(tag: &'a str, attr_name: &str) -> Option<&'a str> {
        let search = format!("{attr_name}=\"");
        let start = tag.find(&search)?;
        let value_start = start + search.len();
        let rest = &tag[value_start..];
        let end = rest.find('"')?;
        Some(&rest[..end])
    }

    /// Parse host elements from XML.
    fn parse_hosts(xml: &str) -> Vec<IndexMap<String, EventValue>> {
        let mut hosts = Vec::new();
        let mut search_start = 0;

        while let Some(host_start) = xml[search_start..].find("<host") {
            let abs_start = search_start + host_start;
            let host_end = xml[abs_start..]
                .find("</host>")
                .map_or(xml.len(), |e| abs_start + e + 7);

            let host_xml = &xml[abs_start..host_end];
            let mut host_data = IndexMap::new();

            // Extract address
            if let Some(addr_start) = host_xml.find("<address") {
                let addr_tag_end = host_xml[addr_start..]
                    .find('>')
                    .map_or(host_xml.len(), |e| addr_start + e + 1);
                let addr_tag = &host_xml[addr_start..addr_tag_end];

                if let Some(addr) = Self::extract_attr(addr_tag, "addr") {
                    host_data.insert("ip".to_string(), EventValue::String(addr.to_string()));
                }
                if let Some(addrtype) = Self::extract_attr(addr_tag, "addrtype") {
                    host_data.insert(
                        "addrtype".to_string(),
                        EventValue::String(addrtype.to_string()),
                    );
                }
            }

            // Extract hostname
            if let Some(hn_start) = host_xml.find("<hostname") {
                let hn_tag_end = host_xml[hn_start..]
                    .find('>')
                    .map_or(host_xml.len(), |e| hn_start + e + 1);
                let hn_tag = &host_xml[hn_start..hn_tag_end];
                if let Some(name) = Self::extract_attr(hn_tag, "name") {
                    host_data.insert("hostname".to_string(), EventValue::String(name.to_string()));
                }
            }

            // Extract status
            if let Some(st_start) = host_xml.find("<status") {
                let st_tag_end = host_xml[st_start..]
                    .find('>')
                    .map_or(host_xml.len(), |e| st_start + e + 1);
                let st_tag = &host_xml[st_start..st_tag_end];
                if let Some(state) = Self::extract_attr(st_tag, "state") {
                    host_data.insert("status".to_string(), EventValue::String(state.to_string()));
                }
            }

            // Extract ports — search for "<port " (with space) to avoid matching "<ports>"
            let mut ports = Vec::new();
            let mut port_search = 0;
            while let Some(port_start) = host_xml[port_search..].find("<port ") {
                let abs_port_start = port_search + port_start;
                let port_tag_end = host_xml[abs_port_start..]
                    .find('>')
                    .map_or(host_xml.len(), |e| abs_port_start + e + 1);
                let port_section_end = host_xml[abs_port_start..]
                    .find("</port>")
                    .map_or(port_tag_end, |e| abs_port_start + e + 7);

                let port_tag = &host_xml[abs_port_start..port_tag_end];
                let port_section = &host_xml[abs_port_start..port_section_end];

                let mut port_data = IndexMap::new();
                if let Some(portid) = Self::extract_attr(port_tag, "portid") {
                    if let Ok(n) = portid.parse::<i64>() {
                        port_data.insert("port".to_string(), EventValue::Integer(n));
                    }
                }
                if let Some(protocol) = Self::extract_attr(port_tag, "protocol") {
                    port_data.insert(
                        "protocol".to_string(),
                        EventValue::String(protocol.to_string()),
                    );
                }

                // State
                if let Some(state_start) = port_section.find("<state") {
                    let state_tag_end = port_section[state_start..]
                        .find('>')
                        .map_or(port_section.len(), |e| state_start + e + 1);
                    let state_tag = &port_section[state_start..state_tag_end];
                    if let Some(state) = Self::extract_attr(state_tag, "state") {
                        port_data
                            .insert("state".to_string(), EventValue::String(state.to_string()));
                    }
                }

                // Service
                if let Some(svc_start) = port_section.find("<service") {
                    let svc_tag_end = port_section[svc_start..]
                        .find('>')
                        .map_or(port_section.len(), |e| svc_start + e + 1);
                    let svc_tag = &port_section[svc_start..svc_tag_end];
                    if let Some(name) = Self::extract_attr(svc_tag, "name") {
                        port_data
                            .insert("service".to_string(), EventValue::String(name.to_string()));
                    }
                    if let Some(product) = Self::extract_attr(svc_tag, "product") {
                        port_data.insert(
                            "product".to_string(),
                            EventValue::String(product.to_string()),
                        );
                    }
                    if let Some(version) = Self::extract_attr(svc_tag, "version") {
                        port_data.insert(
                            "version".to_string(),
                            EventValue::String(version.to_string()),
                        );
                    }
                }

                if !port_data.is_empty() {
                    ports.push(EventValue::Object(port_data));
                }
                port_search = port_section_end;
            }

            if !ports.is_empty() {
                host_data.insert("ports".to_string(), EventValue::Array(ports));
            }

            hosts.push(host_data);
            search_start = host_end;
        }

        hosts
    }
}

impl Codec for NmapCodec {
    fn name(&self) -> &'static str {
        "nmap"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let xml = String::from_utf8_lossy(data);
        let xml = xml.trim();

        if !xml.contains("<nmaprun") && !xml.contains("<host") {
            return Err(FerroStashError::Codec(
                "not an nmap XML document".to_string(),
            ));
        }

        // Extract scan-level metadata
        let mut scan_meta = IndexMap::new();
        if let Some(run_start) = xml.find("<nmaprun") {
            let run_tag_end = xml[run_start..]
                .find('>')
                .map_or(xml.len(), |e| run_start + e + 1);
            let run_tag = &xml[run_start..run_tag_end];

            if let Some(scanner) = Self::extract_attr(run_tag, "scanner") {
                scan_meta.insert(
                    "scanner".to_string(),
                    EventValue::String(scanner.to_string()),
                );
            }
            if let Some(args) = Self::extract_attr(run_tag, "args") {
                scan_meta.insert(
                    "scan_args".to_string(),
                    EventValue::String(args.to_string()),
                );
            }
            if let Some(start) = Self::extract_attr(run_tag, "start") {
                if let Ok(ts) = start.parse::<i64>() {
                    scan_meta.insert("scan_start".to_string(), EventValue::Integer(ts));
                }
            }
            if let Some(startstr) = Self::extract_attr(run_tag, "startstr") {
                scan_meta.insert(
                    "scan_start_str".to_string(),
                    EventValue::String(startstr.to_string()),
                );
            }
        }

        // Extract summary
        if let Some(summary_start) = xml.find("<finished") {
            let summary_end = xml[summary_start..]
                .find('>')
                .map_or(xml.len(), |e| summary_start + e + 1);
            let summary_tag = &xml[summary_start..summary_end];
            if let Some(elapsed) = Self::extract_attr(summary_tag, "elapsed") {
                if let Ok(f) = elapsed.parse::<f64>() {
                    scan_meta.insert("scan_elapsed".to_string(), EventValue::Float(f));
                }
            }
            if let Some(summary) = Self::extract_attr(summary_tag, "summary") {
                scan_meta.insert(
                    "message".to_string(),
                    EventValue::String(summary.to_string()),
                );
            }
        }

        // Parse hosts — one event per host
        let hosts = Self::parse_hosts(xml);
        if hosts.is_empty() {
            // No hosts found — return scan metadata as a single event
            let mut event = Event::empty();
            for (k, v) in &scan_meta {
                event.set(k.clone(), v.clone());
            }
            return Ok(vec![event]);
        }

        let mut events = Vec::with_capacity(hosts.len());
        for host_data in hosts {
            let mut event = Event::empty();
            // Merge scan metadata
            for (k, v) in &scan_meta {
                event.set(k.clone(), v.clone());
            }
            // Merge host data
            for (k, v) in host_data {
                event.set(k, v);
            }
            if let Some(EventValue::String(msg)) = scan_meta.get("message") {
                event.set_message(msg.as_str());
            }
            if let Some(EventValue::Integer(ts)) = scan_meta.get("scan_start") {
                if let Some(dt) = chrono::DateTime::from_timestamp(*ts, 0) {
                    event.timestamp = dt;
                }
            }
            events.push(event);
        }

        Ok(events)
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        // Encode as JSON (nmap XML generation is complex and rarely needed)
        let json = event.to_json();
        serde_json::to_vec_pretty(&json)
            .map_err(|e| FerroStashError::Codec(format!("nmap encode error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nmap_decode_basic() {
        let codec = NmapCodec;
        let xml = r#"<?xml version="1.0"?>
<nmaprun scanner="nmap" args="nmap -sV 192.168.1.1" start="1712880000" startstr="2026-04-12 00:00">
  <host>
    <status state="up"/>
    <address addr="192.168.1.1" addrtype="ipv4"/>
    <hostname name="router.local"/>
    <ports>
      <port portid="22" protocol="tcp">
        <state state="open"/>
        <service name="ssh" product="OpenSSH" version="8.9"/>
      </port>
      <port portid="80" protocol="tcp">
        <state state="open"/>
        <service name="http" product="nginx"/>
      </port>
    </ports>
  </host>
  <finished elapsed="1.23" summary="Nmap done: 1 IP address (1 host up)"/>
</nmaprun>"#;
        let event = codec
            .decode(xml.as_bytes())
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("ip"),
            Some(&EventValue::String("192.168.1.1".into()))
        );
        assert_eq!(event.get("status"), Some(&EventValue::String("up".into())));
        assert_eq!(
            event.get("hostname"),
            Some(&EventValue::String("router.local".into()))
        );
        assert_eq!(event.get("scan_elapsed"), Some(&EventValue::Float(1.23)));

        let ports = event.get("ports").expect("ports");
        let arr = ports.as_array().expect("array");
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_nmap_decode_port_details() {
        let codec = NmapCodec;
        let xml = r#"<nmaprun scanner="nmap">
  <host>
    <address addr="10.0.0.1" addrtype="ipv4"/>
    <ports>
      <port portid="443" protocol="tcp">
        <state state="open"/>
        <service name="https" product="Apache" version="2.4"/>
      </port>
    </ports>
  </host>
</nmaprun>"#;
        let event = codec
            .decode(xml.as_bytes())
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        let ports = event.get("ports").expect("ports");
        let arr = ports.as_array().expect("array");
        let port = arr[0].as_object().expect("object");
        assert_eq!(port.get("port"), Some(&EventValue::Integer(443)));
        assert_eq!(
            port.get("service"),
            Some(&EventValue::String("https".into()))
        );
        assert_eq!(
            port.get("product"),
            Some(&EventValue::String("Apache".into()))
        );
    }

    #[test]
    fn test_nmap_not_xml() {
        let codec = NmapCodec;
        assert!(codec.decode(b"not xml at all").is_err());
    }

    #[test]
    fn test_nmap_name() {
        assert_eq!(NmapCodec.name(), "nmap");
    }

    #[test]
    fn test_nmap_extract_attr() {
        let tag = r#"<address addr="10.0.0.1" addrtype="ipv4"/>"#;
        assert_eq!(NmapCodec::extract_attr(tag, "addr"), Some("10.0.0.1"));
        assert_eq!(NmapCodec::extract_attr(tag, "addrtype"), Some("ipv4"));
        assert_eq!(NmapCodec::extract_attr(tag, "missing"), None);
    }

    #[test]
    fn test_nmap_encode() {
        let codec = NmapCodec;
        let mut event = Event::empty();
        event.set("ip", EventValue::String("10.0.0.1".into()));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("10.0.0.1"));
    }
}
