// SPDX-License-Identifier: Apache-2.0
//! CIDR filter — checks whether IP address(es) fall within given CIDR
//! network(s); on a match, applies the common `add_field` / `add_tag`.
//!
//! ```logstash
//! filter {
//!   cidr {
//!     address => [ "%{clientip}" ]
//!     network => [ "10.0.0.0/8", "192.168.0.0/16" ]
//!     add_tag => [ "internal" ]
//!     add_field => { "network_zone" => "private" }
//!   }
//! }
//! ```
//!
//! A match occurs when ANY resolved address falls within ANY configured
//! network. Matching standard Logstash behaviour, the event is only mutated
//! (tags / fields added) when an address matches; non-matching events pass
//! through untouched. Both IPv4 and IPv6 CIDR notation are supported.

use std::net::IpAddr;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct CidrFilter {
    /// Address templates; each may use `%{field}` interpolation against the
    /// event before being parsed as an IP address.
    addresses: Vec<String>,
    /// Parsed networks as `(network address, prefix length in bits)`.
    networks: Vec<(IpAddr, u8)>,
    /// `add_field` entries (field name → value template) applied on match.
    add_fields: Vec<(String, String)>,
    /// `add_tag` entries (tag templates) applied on match.
    add_tags: Vec<String>,
    condition: Option<Condition>,
}

impl CidrFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let err = |m: String| FerroStashError::Filter {
            plugin: "cidr".to_string(),
            message: m,
        };

        let addresses = string_array(settings, "address");
        let network_strs = string_array(settings, "network");
        if network_strs.is_empty() {
            return Err(err("cidr filter requires at least one `network`".to_string()));
        }
        let mut networks = Vec::with_capacity(network_strs.len());
        for n in &network_strs {
            networks.push(parse_network(n).map_err(err)?);
        }

        let add_fields = settings
            .get("add_field")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let add_tags = string_array(settings, "add_tag");

        Ok(Self {
            addresses,
            networks,
            add_fields,
            add_tags,
            condition,
        })
    }

    /// Returns true if any resolved address falls within any configured network.
    fn any_match(&self, event: &Event) -> bool {
        for template in &self.addresses {
            let rendered = event.sprintf(template);
            let Ok(addr) = rendered.trim().parse::<IpAddr>() else {
                continue;
            };
            for (net, prefix) in &self.networks {
                if in_network(*net, *prefix, addr) {
                    return true;
                }
            }
        }
        false
    }
}

/// Reads a config key as an array of strings, accepting both a JSON array and a
/// single scalar string (Logstash allows either form).
fn string_array(settings: &serde_json::Value, key: &str) -> Vec<String> {
    match settings.get(key) {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        Some(serde_json::Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// Parses a CIDR string (e.g. `"10.0.0.0/8"`) into `(network, prefix)`. A bare
/// IP with no `/prefix` is treated as a single host (`/32` or `/128`).
fn parse_network(s: &str) -> std::result::Result<(IpAddr, u8), String> {
    let s = s.trim();
    match s.split_once('/') {
        Some((ip_str, p)) => {
            let ip: IpAddr = ip_str
                .trim()
                .parse()
                .map_err(|_| format!("invalid IP address in CIDR '{s}'"))?;
            let prefix: u8 = p
                .trim()
                .parse()
                .map_err(|_| format!("invalid prefix length in CIDR '{s}'"))?;
            let max = if ip.is_ipv4() { 32 } else { 128 };
            if prefix > max {
                return Err(format!("CIDR prefix /{prefix} exceeds /{max} in '{s}'"));
            }
            Ok((ip, prefix))
        }
        None => {
            let ip: IpAddr = s
                .parse()
                .map_err(|_| format!("invalid IP / CIDR network '{s}'"))?;
            let max = if ip.is_ipv4() { 32 } else { 128 };
            Ok((ip, max))
        }
    }
}

/// Returns true if `addr` falls within `net/prefix`. IPv4 and IPv6 only match
/// against networks of the same family.
fn in_network(net: IpAddr, prefix: u8, addr: IpAddr) -> bool {
    match (net, addr) {
        (IpAddr::V4(net), IpAddr::V4(addr)) => {
            if prefix > 32 {
                return false;
            }
            let net = u32::from(net);
            let addr = u32::from(addr);
            let mask = if prefix == 0 { 0 } else { u32::MAX << (32 - prefix) };
            (net & mask) == (addr & mask)
        }
        (IpAddr::V6(net), IpAddr::V6(addr)) => {
            if prefix > 128 {
                return false;
            }
            let net = u128::from(net);
            let addr = u128::from(addr);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            (net & mask) == (addr & mask)
        }
        // Mixed families never match.
        _ => false,
    }
}

#[async_trait]
impl FilterPlugin for CidrFilter {
    fn name(&self) -> &'static str {
        "cidr"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if self.any_match(&event) {
            for (field, template) in &self.add_fields {
                let value = event.sprintf(template);
                event.set(field.clone(), EventValue::String(value));
            }
            for tag in &self.add_tags {
                let rendered = event.sprintf(tag);
                event.add_tag(rendered);
            }
        }
        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(settings: serde_json::Value) -> CidrFilter {
        CidrFilter::from_config(&settings, None).expect("config")
    }

    #[test]
    fn test_cidr_name() {
        let f = mk(serde_json::json!({ "network": ["10.0.0.0/8"] }));
        assert_eq!(f.name(), "cidr");
    }

    #[test]
    fn test_cidr_requires_network() {
        let err = CidrFilter::from_config(&serde_json::json!({ "address": ["1.2.3.4"] }), None);
        assert!(err.is_err(), "missing network must be a config error");
    }

    #[test]
    fn test_cidr_invalid_network_rejected() {
        let err = CidrFilter::from_config(
            &serde_json::json!({ "network": ["10.0.0.0/40"] }),
            None,
        );
        assert!(err.is_err(), "out-of-range prefix must fail config");
        let err = CidrFilter::from_config(&serde_json::json!({ "network": ["nope"] }), None);
        assert!(err.is_err(), "non-IP network must fail config");
    }

    #[test]
    fn test_in_network_v4() {
        assert!(in_network(
            "10.0.0.0".parse().expect("ip"),
            8,
            "10.5.6.7".parse().expect("ip")
        ));
        assert!(!in_network(
            "10.0.0.0".parse().expect("ip"),
            8,
            "11.5.6.7".parse().expect("ip")
        ));
        // /0 matches everything.
        assert!(in_network(
            "0.0.0.0".parse().expect("ip"),
            0,
            "203.0.113.9".parse().expect("ip")
        ));
        // /32 is an exact host match.
        assert!(in_network(
            "192.168.1.1".parse().expect("ip"),
            32,
            "192.168.1.1".parse().expect("ip")
        ));
        assert!(!in_network(
            "192.168.1.1".parse().expect("ip"),
            32,
            "192.168.1.2".parse().expect("ip")
        ));
    }

    #[test]
    fn test_in_network_v6_and_mixed() {
        assert!(in_network(
            "2001:db8::".parse().expect("ip"),
            32,
            "2001:db8:1234::1".parse().expect("ip")
        ));
        assert!(!in_network(
            "2001:db8::".parse().expect("ip"),
            32,
            "2001:dead::1".parse().expect("ip")
        ));
        // Mixed families never match.
        assert!(!in_network(
            "10.0.0.0".parse().expect("ip"),
            8,
            "::1".parse().expect("ip")
        ));
    }

    #[tokio::test]
    async fn test_cidr_match_adds_field_and_tag() {
        let f = mk(serde_json::json!({
            "address": ["%{clientip}"],
            "network": ["10.0.0.0/8", "192.168.0.0/16"],
            "add_tag": ["internal"],
            "add_field": { "zone": "private" }
        }));
        let mut event = Event::new("hit");
        event.set("clientip", EventValue::String("192.168.5.5".into()));
        let out = f.filter(event).await.expect("filter");
        assert!(out[0].has_tag("internal"));
        assert_eq!(out[0].get("zone"), Some(&EventValue::String("private".into())));
    }

    #[tokio::test]
    async fn test_cidr_no_match_leaves_event_untouched() {
        let f = mk(serde_json::json!({
            "address": ["%{clientip}"],
            "network": ["10.0.0.0/8"],
            "add_tag": ["internal"]
        }));
        let mut event = Event::new("miss");
        event.set("clientip", EventValue::String("8.8.8.8".into()));
        let out = f.filter(event).await.expect("filter");
        assert!(!out[0].has_tag("internal"));
    }

    #[tokio::test]
    async fn test_cidr_literal_address() {
        // A non-template literal address still works.
        let f = mk(serde_json::json!({
            "address": ["172.16.4.4"],
            "network": ["172.16.0.0/12"],
            "add_tag": ["match"]
        }));
        let out = f.filter(Event::new("x")).await.expect("filter");
        assert!(out[0].has_tag("match"));
    }

    #[tokio::test]
    async fn test_cidr_unparseable_address_is_no_match() {
        let f = mk(serde_json::json!({
            "address": ["%{clientip}"],
            "network": ["10.0.0.0/8"],
            "add_tag": ["internal"]
        }));
        let mut event = Event::new("x");
        event.set("clientip", EventValue::String("not-an-ip".into()));
        let out = f.filter(event).await.expect("filter");
        assert!(!out[0].has_tag("internal"));
    }
}
