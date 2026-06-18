// SPDX-License-Identifier: Apache-2.0
//! DNS filter — perform DNS lookups and reverse lookups on event fields.
//!
//! Forward lookups resolve a hostname field to its A/AAAA address(es); reverse
//! lookups resolve an IP field to its PTR hostname(s). Resolution uses the
//! `hickory-resolver` (0.25) async resolver over a Tokio connection provider.
//!
//! The resolver is built lazily on first use (so config parsing never requires
//! a runtime or network) and, once built successfully, reused for the filter's
//! lifetime. A *failed* build (e.g. `/etc/resolv.conf` momentarily unreadable
//! during a DHCP/netplan reconfigure or before networking is up) is **not**
//! cached: the next event retries the build, so a transient fault does not
//! permanently disable resolution. By default the system resolver configuration
//! (`/etc/resolv.conf`) is used; if the `nameserver` config option is set the
//! filter resolves against that server (UDP/53) instead.
//!
//! An explicitly-configured `nameserver` is **validated at config time**
//! ([`DnsFilter::from_config`]): an unparseable address is a hard config error
//! ([`FerroStashError::Filter`]) that fails the pipeline loudly at startup.
//! This is deliberate — silently falling back to the system resolver when an
//! operator asked for a specific (often *internal*) nameserver would leak the
//! hostnames they intended to keep on the internal resolver to whatever
//! `/etc/resolv.conf` points at, and resolve them differently. The
//! system-resolver path is therefore taken **only** when no `nameserver` is
//! configured at all.
//!
//! Each lookup is also bounded in latency (see [`LOOKUP_TIMEOUT`]): the resolver
//! is built with a tight per-attempt timeout / single attempt, and every lookup
//! is additionally wrapped in a hard `tokio::time::timeout`. This prevents a
//! single unreachable nameserver from stalling the (serial, per-worker) pipeline
//! for ~10s while hickory exhausts its default 5s × 2-attempt budget.
//!
//! Runtime resolution errors (NXDOMAIN, timeouts, network down, empty answers)
//! are never fatal: the configured `tag_on_failure` tag is added and the event
//! flows on.

use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig};
use hickory_resolver::name_server::TokioConnectionProvider;
use tokio::sync::OnceCell;
use tracing::warn;

/// Hard upper bound on the wall-clock latency of a single DNS lookup.
///
/// hickory's default `ResolverOpts` allows `timeout` (5s) × `attempts` (2) ≈
/// 10s per lookup before failing, which would serialize and stall the per-worker
/// pipeline. We bound it two ways: the resolver is built with these tighter
/// options (single attempt, ~2s timeout), and each lookup is wrapped in a
/// `tokio::time::timeout(LOOKUP_TIMEOUT)` as a belt-and-braces ceiling so a slow
/// nameserver tags failure quickly instead of stalling the batch. 2s is a sane
/// default for a non-blocking enrichment filter.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

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
    /// Custom nameserver to resolve against (e.g. `8.8.8.8`). The address is
    /// validated/parsed at config time ([`Self::from_config`]), so `Some` means
    /// an operator explicitly chose this server and it is known-parseable.
    /// `None` means no nameserver was configured → the system resolver is used.
    /// This is the only signal that drives the resolver build; there is no
    /// silent fall-through from a configured-but-bad nameserver to the system
    /// resolver.
    nameserver: Option<IpAddr>,
    /// Whether to add a tag on failure.
    tag_on_failure: String,
    /// Lazily-built, reused resolver. Only a *successful* build is memoized
    /// here; a failed build (e.g. transiently unreadable `/etc/resolv.conf`) is
    /// not cached, so the build is retried on the next lookup and recovers once
    /// the system configuration becomes readable again.
    resolver: OnceCell<TokioResolver>,
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

        // Validate an explicitly-configured nameserver at config time. An
        // operator who names a specific resolver (commonly an *internal* one)
        // must NOT silently fall through to the system resolver if the value is
        // unusable — that would leak the very hostnames they intended to keep on
        // the internal resolver and resolve them differently. So an unparseable
        // nameserver is a hard, loud startup error rather than a warn-and-ignore.
        let nameserver = match settings.get("nameserver").and_then(|v| v.as_str()) {
            Some(ns) => Some(ns.parse::<IpAddr>().map_err(|e| FerroStashError::Filter {
                plugin: "dns".to_string(),
                message: format!(
                    "invalid `nameserver` {ns:?}: {e}. Provide a valid IP address \
                     (e.g. \"8.8.8.8\" or \"2001:4860:4860::8888\"), or omit \
                     `nameserver` to use the system resolver (/etc/resolv.conf)."
                ),
            })?),
            None => None,
        };

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

    /// Build the resolver. Honors the explicitly-configured `nameserver`
    /// (UDP/53) if one was set, otherwise reads the system configuration.
    /// Returns `Err` only when the build genuinely fails (e.g. unreadable
    /// `/etc/resolv.conf` on the system path); the error is surfaced to the
    /// caller so a *transient* failure is not memoized.
    ///
    /// Note there is **no** silent fall-through from a configured nameserver to
    /// the system resolver: the address was already validated in
    /// [`Self::from_config`], so [`Self::nameserver`] being `Some` means an
    /// operator-chosen server, and being `None` means none was configured.
    ///
    /// The resulting resolver always uses the bounded options from
    /// [`Self::apply_bounded_options`] so no single lookup can exhaust hickory's
    /// default ~10s budget.
    fn build_resolver(&self) -> std::result::Result<TokioResolver, String> {
        let provider = TokioConnectionProvider::default();
        match self.nameserver {
            Some(ip) => {
                let group = NameServerConfigGroup::from_ips_clear(&[ip], 53, true);
                let config = ResolverConfig::from_parts(None, Vec::new(), group);
                let mut builder = TokioResolver::builder_with_config(config, provider);
                self.apply_bounded_options(builder.options_mut());
                Ok(builder.build())
            }
            None => self.build_system_resolver(provider),
        }
    }

    fn build_system_resolver(
        &self,
        provider: TokioConnectionProvider,
    ) -> std::result::Result<TokioResolver, String> {
        match TokioResolver::builder(provider) {
            Ok(mut builder) => {
                // Preserve the system-derived options (search domains, ndots,
                // strategy, …) but clamp the latency-relevant ones.
                self.apply_bounded_options(builder.options_mut());
                Ok(builder.build())
            }
            Err(e) => {
                warn!(error = %e, "dns: failed to read system resolver configuration");
                Err(e.to_string())
            }
        }
    }

    /// Clamp the latency-relevant `ResolverOpts` in place: one attempt and a
    /// tight per-attempt timeout so a single unreachable nameserver fails fast
    /// (~[`LOOKUP_TIMEOUT`]) rather than burning hickory's default 5s × 2.
    fn apply_bounded_options(&self, opts: &mut hickory_resolver::config::ResolverOpts) {
        opts.timeout = LOOKUP_TIMEOUT;
        opts.attempts = 1;
    }

    /// Lazily obtain the shared resolver. A successful build is memoized and
    /// reused; a failed build is *not* cached, so subsequent calls retry and
    /// recover from a transient configuration fault.
    async fn resolver(&self) -> Option<&TokioResolver> {
        self.resolver
            .get_or_try_init(|| async { self.build_resolver() })
            .await
            .ok()
    }

    /// Forward lookup: hostname -> first resolved address (as a string).
    /// Returns `None` on any error or empty answer. The lookup is bounded by
    /// [`LOOKUP_TIMEOUT`] so a slow nameserver cannot stall the worker.
    async fn resolve_forward(&self, hostname: &str) -> Option<String> {
        let resolver = self.resolver().await?;
        match tokio::time::timeout(LOOKUP_TIMEOUT, resolver.lookup_ip(hostname)).await {
            Ok(Ok(lookup)) => lookup.iter().next().map(|ip| ip.to_string()),
            Ok(Err(e)) => {
                warn!(hostname = %hostname, error = %e, "dns: forward lookup failed");
                None
            }
            Err(_) => {
                warn!(hostname = %hostname, "dns: forward lookup timed out");
                None
            }
        }
    }

    /// Reverse lookup: IP -> first PTR hostname (trailing dot stripped).
    /// Returns `None` on any error or empty answer. The lookup is bounded by
    /// [`LOOKUP_TIMEOUT`] so a slow nameserver cannot stall the worker.
    async fn resolve_reverse(&self, ip: &str) -> Option<String> {
        let addr: IpAddr = match ip.parse() {
            Ok(a) => a,
            Err(e) => {
                warn!(ip = %ip, error = %e, "dns: reverse lookup value is not an IP");
                return None;
            }
        };
        let resolver = self.resolver().await?;
        match tokio::time::timeout(LOOKUP_TIMEOUT, resolver.reverse_lookup(addr)).await {
            Ok(Ok(lookup)) => lookup.iter().next().map(|ptr| {
                // PTR derefs to a Name whose Display includes a trailing dot;
                // strip it for Logstash-style output.
                ptr.to_string().trim_end_matches('.').to_string()
            }),
            Ok(Err(e)) => {
                warn!(ip = %ip, error = %e, "dns: reverse lookup failed");
                None
            }
            Err(_) => {
                warn!(ip = %ip, "dns: reverse lookup timed out");
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
        assert_eq!(
            filter.nameserver,
            Some("8.8.8.8".parse::<IpAddr>().expect("valid v4"))
        );
    }

    #[test]
    fn test_dns_invalid_nameserver_rejected_at_config_time() {
        // Finding (DD R5): an explicitly-configured but unparseable `nameserver`
        // must NOT silently fall back to the system resolver (an internal-name
        // leak / wrong-answer risk). It is a hard config error at startup.
        let settings = serde_json::json!({
            "resolve": ["host"],
            "nameserver": "not-an-ip"
        });
        let err = DnsFilter::from_config(&settings, None)
            .expect_err("invalid nameserver must be rejected at config time");
        match err {
            FerroStashError::Filter { plugin, message } => {
                assert_eq!(plugin, "dns");
                assert!(
                    message.contains("nameserver"),
                    "error should explain the bad nameserver: {message}"
                );
            }
            other => panic!("expected FerroStashError::Filter, got {other:?}"),
        }
    }

    #[test]
    fn test_dns_valid_nameserver_parsed_at_config_time() {
        // A valid nameserver (v4 or v6) parses to the IP that drives the build —
        // and it never routes through the system resolver.
        let v4 = DnsFilter::from_config(&serde_json::json!({ "nameserver": "8.8.8.8" }), None)
            .expect("valid v4 nameserver");
        assert_eq!(
            v4.nameserver,
            Some("8.8.8.8".parse::<IpAddr>().expect("v4"))
        );

        let v6 = DnsFilter::from_config(
            &serde_json::json!({ "nameserver": "2001:4860:4860::8888" }),
            None,
        )
        .expect("valid v6 nameserver");
        assert_eq!(
            v6.nameserver,
            Some("2001:4860:4860::8888".parse::<IpAddr>().expect("v6"))
        );
    }

    #[test]
    fn test_dns_no_nameserver_uses_system_resolver() {
        // No nameserver configured => system resolver path (nameserver None).
        // This is the ONLY case in which the system resolver is used.
        let filter = DnsFilter::from_config(&serde_json::json!({ "resolve": ["host"] }), None)
            .expect("config without nameserver");
        assert_eq!(filter.nameserver, None);
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

    #[test]
    fn test_lookup_timeout_is_bounded() {
        // Finding 2: a single lookup must not be able to consume hickory's
        // default ~10s budget. The chosen bound is short and well under that.
        assert!(
            LOOKUP_TIMEOUT <= std::time::Duration::from_secs(3),
            "per-lookup bound should be tight: {LOOKUP_TIMEOUT:?}"
        );
    }

    #[test]
    fn test_bounded_options_applied() {
        // Finding 2: the resolver options are clamped to a single attempt and
        // the tight per-attempt timeout, so attempts × timeout cannot blow up.
        let settings = serde_json::json!({ "nameserver": "8.8.8.8" });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        let mut opts = hickory_resolver::config::ResolverOpts::default();
        // Defaults are the dangerous ones we are guarding against.
        assert_eq!(opts.attempts, 2);
        filter.apply_bounded_options(&mut opts);
        assert_eq!(opts.attempts, 1, "attempts must be clamped to 1");
        assert_eq!(opts.timeout, LOOKUP_TIMEOUT, "timeout must be clamped");
    }

    #[tokio::test]
    async fn test_custom_nameserver_build_succeeds() {
        // Finding 1 (positive side): a custom-nameserver build never fails, so
        // it is memoized — the custom path keeps working.
        let settings = serde_json::json!({ "nameserver": "8.8.8.8" });
        let filter = DnsFilter::from_config(&settings, None).expect("config");
        assert!(
            filter.build_resolver().is_ok(),
            "custom nameserver build should succeed offline"
        );
        // First obtain memoizes; second returns the same cached instance.
        let first = filter.resolver().await.map(std::ptr::from_ref);
        let second = filter.resolver().await.map(std::ptr::from_ref);
        assert!(first.is_some(), "resolver should build");
        assert_eq!(first, second, "successful build must be memoized, not rebuilt");
    }

    #[tokio::test]
    async fn test_build_failure_not_permanently_latched() {
        // Finding 1: a *failed* build must not poison the OnceCell. We exercise
        // the retry semantics directly: a fresh OnceCell that fails its init
        // closure stays empty and can succeed on a later attempt. This is the
        // exact `get_or_try_init` contract `resolver()` relies on, so a
        // transient `/etc/resolv.conf` read failure recovers once readable.
        let cell: OnceCell<u32> = OnceCell::new();
        let first: std::result::Result<&u32, &'static str> = cell
            .get_or_try_init(|| async { Err("transient failure") })
            .await;
        assert!(first.is_err(), "failing init should return the error");
        assert!(cell.get().is_none(), "a failed init must NOT be cached");
        // A subsequent attempt succeeds and is then memoized.
        let second = cell.get_or_try_init(|| async { Ok::<u32, &'static str>(42) }).await;
        assert_eq!(second, Ok(&42), "retry after failure should succeed");
        assert_eq!(cell.get(), Some(&42), "successful init is memoized");
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
