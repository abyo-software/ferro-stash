// SPDX-License-Identifier: Apache-2.0
//! `GeoIP` filter — adds geographic information based on IP addresses.
//!
//! This is a stub implementation that provides the interface.
//! Full implementation requires a `GeoIP` database (`MaxMind` `GeoLite2`).
//! For now, it enriches with RFC 5737 / private IP classification.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

use indexmap::IndexMap;

#[derive(Debug)]
pub struct GeoipFilter {
    source: String,
    target: String,
    tag_on_failure: Vec<String>,
    condition: Option<Condition>,
}

impl GeoipFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("client_ip")
            .to_string();
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("geoip")
            .to_string();
        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["_geoip_lookup_failure".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );

        Ok(Self {
            source,
            target,
            tag_on_failure,
            condition,
        })
    }
}

fn classify_ip(ip: &str) -> Option<IndexMap<String, EventValue>> {
    let mut result = IndexMap::new();
    result.insert("ip".to_string(), EventValue::String(ip.to_string()));

    // Classify known IP ranges
    if ip.starts_with("10.")
        || ip.starts_with("172.16.")
        || ip.starts_with("172.17.")
        || ip.starts_with("172.18.")
        || ip.starts_with("172.19.")
        || ip.starts_with("172.2")
        || ip.starts_with("172.30.")
        || ip.starts_with("172.31.")
        || ip.starts_with("192.168.")
    {
        result.insert(
            "network_type".to_string(),
            EventValue::String("private".to_string()),
        );
    } else if ip.starts_with("127.") {
        result.insert(
            "network_type".to_string(),
            EventValue::String("loopback".to_string()),
        );
    } else if ip == "::1" {
        result.insert(
            "network_type".to_string(),
            EventValue::String("loopback".to_string()),
        );
    } else {
        result.insert(
            "network_type".to_string(),
            EventValue::String("public".to_string()),
        );
    }

    Some(result)
}

#[async_trait]
impl FilterPlugin for GeoipFilter {
    fn name(&self) -> &'static str {
        "geoip"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let ip = if let Some(val) = event.get(&self.source) {
            val.to_string_lossy()
        } else {
            for tag in &self.tag_on_failure {
                event.add_tag(tag);
            }
            return Ok(vec![event]);
        };

        if let Some(geo_data) = classify_ip(&ip) {
            event.set(self.target.clone(), EventValue::Object(geo_data));
        } else {
            for tag in &self.tag_on_failure {
                event.add_tag(tag);
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

    #[tokio::test]
    async fn test_geoip_private() {
        let settings = serde_json::json!({ "source": "client_ip" });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("client_ip", EventValue::String("192.168.1.1".into()));
        let result = filter.filter(event).await.expect("filter");
        let geoip = result[0].get("geoip");
        assert!(geoip.is_some());
    }

    #[tokio::test]
    async fn test_geoip_loopback() {
        let settings = serde_json::json!({ "source": "ip" });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("127.0.0.1".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_geoip_lookup_failure"));
    }

    #[tokio::test]
    async fn test_geoip_public_ip() {
        let settings = serde_json::json!({ "source": "ip" });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("8.8.8.8".into()));
        let result = filter.filter(event).await.expect("filter");
        let geoip = result[0].get("geoip");
        assert!(geoip.is_some());
        if let Some(EventValue::Object(obj)) = geoip {
            assert_eq!(
                obj.get("network_type"),
                Some(&EventValue::String("public".into()))
            );
        }
    }

    #[tokio::test]
    async fn test_geoip_private_network_type() {
        let settings = serde_json::json!({ "source": "ip" });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("10.0.0.1".into()));
        let result = filter.filter(event).await.expect("filter");
        if let Some(EventValue::Object(obj)) = result[0].get("geoip") {
            assert_eq!(
                obj.get("network_type"),
                Some(&EventValue::String("private".into()))
            );
        }
    }

    #[tokio::test]
    async fn test_geoip_ipv6_loopback() {
        let settings = serde_json::json!({ "source": "ip" });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("::1".into()));
        let result = filter.filter(event).await.expect("filter");
        if let Some(EventValue::Object(obj)) = result[0].get("geoip") {
            assert_eq!(
                obj.get("network_type"),
                Some(&EventValue::String("loopback".into()))
            );
        }
    }

    #[tokio::test]
    async fn test_geoip_missing_source() {
        let settings = serde_json::json!({ "source": "nonexistent" });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_geoip_lookup_failure"));
    }

    #[tokio::test]
    async fn test_geoip_custom_target() {
        let settings = serde_json::json!({
            "source": "ip",
            "target": "geo_info"
        });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("192.168.1.1".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("geo_info"));
        assert!(!result[0].has_field("geoip"));
    }

    #[tokio::test]
    async fn test_geoip_custom_failure_tag() {
        let settings = serde_json::json!({
            "source": "nonexistent",
            "tag_on_failure": ["geo_fail"]
        });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("geo_fail"));
    }

    #[test]
    fn test_geoip_name() {
        let settings = serde_json::json!({});
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "geoip");
    }

    #[test]
    fn test_classify_ip_172_private() {
        let result = classify_ip("172.16.0.1").expect("classify");
        assert_eq!(
            result.get("network_type"),
            Some(&EventValue::String("private".into()))
        );
    }
}
