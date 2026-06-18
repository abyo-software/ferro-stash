// SPDX-License-Identifier: Apache-2.0
//! DNS filter — perform DNS lookups and reverse lookups on event fields.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

/// Action to take when a lookup succeeds.
#[derive(Debug, Clone, Copy, PartialEq)]
enum DnsAction {
    /// Replace the field value with the lookup result.
    Replace,
    /// Append the lookup result to the field as an array.
    Append,
}

#[derive(Debug)]
pub struct DnsFilter {
    /// Fields to perform forward DNS lookup on (hostname -> IP).
    resolve: Vec<String>,
    /// Fields to perform reverse DNS lookup on (IP -> hostname).
    reverse: Vec<String>,
    /// Action to take with the result.
    action: DnsAction,
    /// Nameserver to use (stub: not actually used).
    #[allow(dead_code)]
    nameserver: Option<String>,
    /// Whether to add a tag on failure.
    tag_on_failure: String,
    condition: Option<Condition>,
}

impl DnsFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let resolve = settings
            .get("resolve")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let reverse = settings
            .get("reverse")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let action = match settings.get("action").and_then(|v| v.as_str()) {
            Some("append") => DnsAction::Append,
            _ => DnsAction::Replace,
        };

        let nameserver = settings
            .get("nameserver")
            .and_then(|v| v.as_str())
            .map(String::from);

        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_str())
            .unwrap_or("_dnsfailure")
            .to_string();

        Ok(Self {
            resolve,
            reverse,
            action,
            nameserver,
            tag_on_failure,
            condition,
        })
    }

    /// Stub forward lookup: in real implementation, this would resolve hostname to IP.
    /// Returns the input value as-is and adds a tag indicating stub behavior.
    fn resolve_forward(&self, _hostname: &str) -> Option<String> {
        // Stub: return None to indicate DNS resolution is not available.
        // In a real implementation, this would use std::net::ToSocketAddrs or a DNS library.
        None
    }

    /// Stub reverse lookup: in real implementation, this would resolve IP to hostname.
    /// Returns the input value as-is and adds a tag indicating stub behavior.
    fn resolve_reverse(&self, _ip: &str) -> Option<String> {
        // Stub: return None to indicate DNS resolution is not available.
        // In a real implementation, this would use a DNS library for PTR lookups.
        None
    }
}

#[async_trait]
impl FilterPlugin for DnsFilter {
    fn name(&self) -> &'static str {
        "dns"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let mut any_failure = false;

        // Forward lookups
        for field in &self.resolve {
            if let Some(val) = event.get(field).cloned() {
                let hostname = val.to_string_lossy();
                match self.resolve_forward(&hostname) {
                    Some(resolved) => match self.action {
                        DnsAction::Replace => {
                            event.set(field.clone(), EventValue::String(resolved));
                        }
                        DnsAction::Append => {
                            let existing = event.get(field).cloned();
                            match existing {
                                Some(EventValue::Array(mut arr)) => {
                                    arr.push(EventValue::String(resolved));
                                    event.set(field.clone(), EventValue::Array(arr));
                                }
                                Some(other) => {
                                    event.set(
                                        field.clone(),
                                        EventValue::Array(vec![
                                            other,
                                            EventValue::String(resolved),
                                        ]),
                                    );
                                }
                                None => {
                                    event.set(field.clone(), EventValue::String(resolved));
                                }
                            }
                        }
                    },
                    None => {
                        any_failure = true;
                    }
                }
            }
        }

        // Reverse lookups
        for field in &self.reverse {
            if let Some(val) = event.get(field).cloned() {
                let ip = val.to_string_lossy();
                match self.resolve_reverse(&ip) {
                    Some(resolved) => match self.action {
                        DnsAction::Replace => {
                            event.set(field.clone(), EventValue::String(resolved));
                        }
                        DnsAction::Append => {
                            let existing = event.get(field).cloned();
                            match existing {
                                Some(EventValue::Array(mut arr)) => {
                                    arr.push(EventValue::String(resolved));
                                    event.set(field.clone(), EventValue::Array(arr));
                                }
                                Some(other) => {
                                    event.set(
                                        field.clone(),
                                        EventValue::Array(vec![
                                            other,
                                            EventValue::String(resolved),
                                        ]),
                                    );
                                }
                                None => {
                                    event.set(field.clone(), EventValue::String(resolved));
                                }
                            }
                        }
                    },
                    None => {
                        any_failure = true;
                    }
                }
            }
        }

        if any_failure {
            event.add_tag(&self.tag_on_failure);
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
    async fn test_dns_resolve_stub_adds_failure_tag() {
        let settings = serde_json::json!({
            "resolve": ["host"]
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("host", EventValue::String("example.com".into()));
        let result = filter.filter(event).await.expect("filter");
        // Stub DNS returns None, so failure tag should be added
        assert!(result[0].has_tag("_dnsfailure"));
        // Original value should remain
        assert_eq!(
            result[0].get("host"),
            Some(&EventValue::String("example.com".into()))
        );
    }

    #[tokio::test]
    async fn test_dns_reverse_stub_adds_failure_tag() {
        let settings = serde_json::json!({
            "reverse": ["client_ip"]
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("client_ip", EventValue::String("192.168.1.1".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_dnsfailure"));
    }

    #[tokio::test]
    async fn test_dns_no_fields_no_failure() {
        let settings = serde_json::json!({
            "resolve": ["host"]
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        // Event doesn't have "host" field, so no lookup attempted
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_dnsfailure"));
    }

    #[tokio::test]
    async fn test_dns_custom_failure_tag() {
        let settings = serde_json::json!({
            "resolve": ["host"],
            "tag_on_failure": "_dns_lookup_failed"
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("host", EventValue::String("example.com".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_dns_lookup_failed"));
        assert!(!result[0].has_tag("_dnsfailure"));
    }

    #[tokio::test]
    async fn test_dns_action_config() {
        let settings = serde_json::json!({
            "resolve": ["host"],
            "action": "append",
            "nameserver": "8.8.8.8"
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.action, DnsAction::Append);
        assert_eq!(filter.nameserver, Some("8.8.8.8".to_string()));
    }

    #[test]
    fn test_dns_name() {
        let settings = serde_json::json!({});
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "dns");
    }
}
