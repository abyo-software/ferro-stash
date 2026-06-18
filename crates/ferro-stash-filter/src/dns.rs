// SPDX-License-Identifier: Apache-2.0
//! DNS filter — perform DNS lookups and reverse lookups on event fields.
//!
//! Forward lookups resolve a hostname field to its A/AAAA address(es); reverse
//! lookups resolve an IP field to its PTR hostname(s). Resolution uses the
//! `hickory-resolver` (0.25) async resolver over a Tokio connection provider.
//!
//! The resolver is built lazily once on first use (so config parsing never
//! requires a runtime or network) and reused for the filter's lifetime. By
//! default the system resolver configuration (`/etc/resolv.conf`) is used; if
//! the `nameserver` config option is set the filter resolves against that
//! server (UDP/53) instead.
//!
//! Runtime resolution errors (NXDOMAIN, timeouts, network down, empty answers)
//! are never fatal: the configured `tag_on_failure` tag is added and the event
//! flows on.

use std::net::IpAddr;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig};
use hickory_resolver::name_server::TokioConnectionProvider;
use tokio::sync::OnceCell;
use tracing::warn;

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
    /// Custom nameserver to resolve against (e.g. `8.8.8.8`). When `None` the
    /// system resolver configuration is used.
    nameserver: Option<String>,
    /// Whether to add a tag on failure.
    tag_on_failure: String,
    /// Lazily-built, reused resolver. `None` inside the cell means the resolver
    /// could not be built (e.g. unreadable `/etc/resolv.conf`); lookups then
    /// fail gracefully (tagged).
    resolver: OnceCell<Option<TokioResolver>>,
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
            resolver: OnceCell::new(),
            condition,
        })
    }

    /// Build the resolver. Honors a custom `nameserver` (UDP/53) if configured,
    /// otherwise reads the system configuration. Returns `None` on failure so
    /// callers degrade gracefully.
    fn build_resolver(&self) -> Option<TokioResolver> {
        let provider = TokioConnectionProvider::default();
        if let Some(ns) = &self.nameserver {
            match ns.parse::<IpAddr>() {
                Ok(ip) => {
                    let group = NameServerConfigGroup::from_ips_clear(&[ip], 53, true);
                    let config = ResolverConfig::from_parts(None, Vec::new(), group);
                    Some(TokioResolver::builder_with_config(config, provider).build())
                }
                Err(e) => {
                    warn!(
                        nameserver = %ns,
                        error = %e,
                        "dns: invalid nameserver address; falling back to system resolver"
                    );
                    self.build_system_resolver(provider)
                }
            }
        } else {
            self.build_system_resolver(provider)
        }
    }

    fn build_system_resolver(&self, provider: TokioConnectionProvider) -> Option<TokioResolver> {
        match TokioResolver::builder(provider) {
            Ok(builder) => Some(builder.build()),
            Err(e) => {
                warn!(error = %e, "dns: failed to read system resolver configuration");
                None
            }
        }
    }

    /// Lazily obtain the shared resolver (built at most once).
    async fn resolver(&self) -> Option<&TokioResolver> {
        self.resolver
            .get_or_init(|| async { self.build_resolver() })
            .await
            .as_ref()
    }

    /// Forward lookup: hostname -> first resolved address (as a string).
    /// Returns `None` on any error or empty answer.
    async fn resolve_forward(&self, hostname: &str) -> Option<String> {
        let resolver = self.resolver().await?;
        match resolver.lookup_ip(hostname).await {
            Ok(lookup) => lookup.iter().next().map(|ip| ip.to_string()),
            Err(e) => {
                warn!(hostname = %hostname, error = %e, "dns: forward lookup failed");
                None
            }
        }
    }

    /// Reverse lookup: IP -> first PTR hostname (trailing dot stripped).
    /// Returns `None` on any error or empty answer.
    async fn resolve_reverse(&self, ip: &str) -> Option<String> {
        let addr: IpAddr = match ip.parse() {
            Ok(a) => a,
            Err(e) => {
                warn!(ip = %ip, error = %e, "dns: reverse lookup value is not an IP");
                return None;
            }
        };
        let resolver = self.resolver().await?;
        match resolver.reverse_lookup(addr).await {
            Ok(lookup) => lookup.iter().next().map(|ptr| {
                // PTR derefs to a Name whose Display includes a trailing dot;
                // strip it for Logstash-style output.
                ptr.to_string().trim_end_matches('.').to_string()
            }),
            Err(e) => {
                warn!(ip = %ip, error = %e, "dns: reverse lookup failed");
                None
            }
        }
    }
}

/// Apply a resolved value to a field per the configured action.
fn apply_result(event: &mut Event, field: &str, resolved: String, action: DnsAction) {
    match action {
        DnsAction::Replace => {
            event.set(field.to_string(), EventValue::String(resolved));
        }
        DnsAction::Append => match event.get(field).cloned() {
            Some(EventValue::Array(mut arr)) => {
                arr.push(EventValue::String(resolved));
                event.set(field.to_string(), EventValue::Array(arr));
            }
            Some(other) => {
                event.set(
                    field.to_string(),
                    EventValue::Array(vec![other, EventValue::String(resolved)]),
                );
            }
            None => {
                event.set(field.to_string(), EventValue::String(resolved));
            }
        },
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
                match self.resolve_forward(&hostname).await {
                    Some(resolved) => apply_result(&mut event, field, resolved, self.action),
                    None => any_failure = true,
                }
            }
        }

        // Reverse lookups
        for field in &self.reverse {
            if let Some(val) = event.get(field).cloned() {
                let ip = val.to_string_lossy();
                match self.resolve_reverse(&ip).await {
                    Some(resolved) => apply_result(&mut event, field, resolved, self.action),
                    None => any_failure = true,
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
    async fn test_dns_no_fields_no_failure() {
        let settings = serde_json::json!({
            "resolve": ["host"]
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        // Event doesn't have "host" field, so no lookup attempted and the
        // resolver is never built — purely offline.
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_dnsfailure"));
    }

    #[tokio::test]
    async fn test_dns_reverse_invalid_ip_tags_failure() {
        // A non-IP value in a reverse field fails fast (no network) and tags.
        let settings = serde_json::json!({
            "reverse": ["client_ip"],
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("client_ip", EventValue::String("not-an-ip".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_dnsfailure"));
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

    #[tokio::test]
    async fn test_dns_custom_failure_tag_config() {
        let settings = serde_json::json!({
            "resolve": ["host"],
            "tag_on_failure": "_dns_lookup_failed"
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.tag_on_failure, "_dns_lookup_failed");
    }

    #[test]
    fn test_apply_result_replace() {
        let mut event = Event::new("test");
        event.set("host", EventValue::String("example.com".into()));
        apply_result(&mut event, "host", "1.2.3.4".to_string(), DnsAction::Replace);
        assert_eq!(
            event.get("host"),
            Some(&EventValue::String("1.2.3.4".into()))
        );
    }

    #[test]
    fn test_apply_result_append_scalar() {
        let mut event = Event::new("test");
        event.set("host", EventValue::String("example.com".into()));
        apply_result(&mut event, "host", "1.2.3.4".to_string(), DnsAction::Append);
        match event.get("host") {
            Some(EventValue::Array(arr)) => assert_eq!(arr.len(), 2),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_result_append_array() {
        let mut event = Event::new("test");
        event.set(
            "host",
            EventValue::Array(vec![EventValue::String("a".into())]),
        );
        apply_result(&mut event, "host", "1.2.3.4".to_string(), DnsAction::Append);
        match event.get("host") {
            Some(EventValue::Array(arr)) => assert_eq!(arr.len(), 2),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_dns_name() {
        let settings = serde_json::json!({});
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "dns");
    }

    // ----- Live-smoke tests (require network) -----
    //
    // Gated behind the `DNS_LIVE` env var to avoid flakiness in offline CI:
    //   DNS_LIVE=1 cargo test -p ferro-stash-filter dns_live -- --ignored

    #[tokio::test]
    #[ignore = "requires network DNS; set DNS_LIVE=1 to enable"]
    async fn test_dns_live_forward_resolve() {
        let settings = serde_json::json!({
            "resolve": ["host"],
            "nameserver": "8.8.8.8"
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        // dns.google has stable A records (8.8.8.8 / 8.8.4.4).
        event.set("host", EventValue::String("dns.google".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(
            !result[0].has_tag("_dnsfailure"),
            "forward resolve should succeed: {:?}",
            result[0].get("host")
        );
        // host field should now hold an IP string.
        let resolved = result[0].get("host").expect("host present").to_string_lossy();
        assert!(
            resolved.parse::<IpAddr>().is_ok(),
            "resolved value should be an IP: {resolved}"
        );
    }

    #[tokio::test]
    #[ignore = "requires network DNS; set DNS_LIVE=1 to enable"]
    async fn test_dns_live_reverse_resolve() {
        let settings = serde_json::json!({
            "reverse": ["ip"],
            "nameserver": "8.8.8.8"
        });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("8.8.8.8".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(
            !result[0].has_tag("_dnsfailure"),
            "reverse resolve should succeed: {:?}",
            result[0].get("ip")
        );
        let resolved = result[0].get("ip").expect("ip present").to_string_lossy();
        assert!(
            resolved.contains("dns.google"),
            "reverse of 8.8.8.8 should be dns.google: {resolved}"
        );
    }
}
