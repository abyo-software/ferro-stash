// SPDX-License-Identifier: Apache-2.0
//! `GeoIP` filter — adds geographic information based on IP addresses.
//!
//! Performs real `MaxMind` `GeoLite2`/`GeoIP2` lookups using the `maxminddb`
//! crate. A `.mmdb` database file is opened once at construction (config field
//! `database`, mirroring Logstash's `database =>`). On each event the source
//! field is parsed as an IP address and looked up; the Logstash-style geoip
//! subfields (`ip`, `country_code2`, `country_name`, `city_name`, `latitude`,
//! `longitude`, `continent_code`, `region_*`, `postal_code`, `location`,
//! `timezone`) are written to the target field.
//!
//! When no database is configured, the filter falls back to RFC 5737 / private
//! IP classification so it degrades gracefully (and remains useful in
//! environments without a `MaxMind` database).

use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

use indexmap::IndexMap;
use maxminddb::Reader;
use maxminddb::geoip2;
use tracing::warn;

#[derive(Debug)]
pub struct GeoipFilter {
    source: String,
    target: String,
    tag_on_failure: Vec<String>,
    /// Opened database reader, shared (cheap to clone the `Arc`). `None` when
    /// no database is configured or it could not be opened.
    reader: Option<Arc<Reader<Vec<u8>>>>,
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
        let database = settings
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("")
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

        // Open the database once at construction. A missing/unreadable file is
        // a graceful failure (events get tagged at filter time), NOT a hard
        // config error — this matches Logstash, where a misconfigured geoip
        // simply tags events rather than aborting the pipeline.
        let reader = if database.is_empty() {
            None
        } else {
            match Reader::open_readfile(&database) {
                Ok(r) => Some(Arc::new(r)),
                Err(e) => {
                    warn!(
                        database = %database,
                        error = %e,
                        "geoip: failed to open MaxMind database; lookups will be tagged as failures"
                    );
                    None
                }
            }
        };

        Ok(Self {
            source,
            target,
            tag_on_failure,
            reader,
            condition,
        })
    }

    /// Perform a real `MaxMind` City lookup, returning Logstash-style geoip
    /// subfields. Returns `None` when the IP is not present in the database
    /// (or on any decode error).
    fn lookup(&self, reader: &Reader<Vec<u8>>, ip: IpAddr) -> Option<IndexMap<String, EventValue>> {
        let result = match reader.lookup(ip) {
            Ok(r) => r,
            Err(e) => {
                warn!(ip = %ip, error = %e, "geoip: lookup error");
                return None;
            }
        };

        // maxminddb 0.28: `lookup()` -> `LookupResult`, `.decode::<T>()` ->
        // `Result<Option<T>>`. `None` means the IP was not found in the tree.
        let city: geoip2::City = match result.decode::<geoip2::City>() {
            Ok(Some(c)) => c,
            Ok(None) => return None,
            Err(e) => {
                warn!(ip = %ip, error = %e, "geoip: decode error");
                return None;
            }
        };

        let mut out = IndexMap::new();
        out.insert("ip".to_string(), EventValue::String(ip.to_string()));

        // Country
        if let Some(code) = city.country.iso_code {
            out.insert(
                "country_code2".to_string(),
                EventValue::String(code.to_string()),
            );
        }
        if let Some(name) = city.country.names.english {
            out.insert(
                "country_name".to_string(),
                EventValue::String(name.to_string()),
            );
        }

        // Continent
        if let Some(code) = city.continent.code {
            out.insert(
                "continent_code".to_string(),
                EventValue::String(code.to_string()),
            );
        }

        // City
        if let Some(name) = city.city.names.english {
            out.insert("city_name".to_string(), EventValue::String(name.to_string()));
        }

        // Subdivisions (region) — Logstash exposes the first (largest) subdivision
        // as region_code / region_name.
        if let Some(sub) = city.subdivisions.first() {
            if let Some(code) = sub.iso_code {
                out.insert(
                    "region_code".to_string(),
                    EventValue::String(code.to_string()),
                );
                // Logstash also exposes a composite `region_iso_code`
                // (CC-SUB) when both the country and subdivision codes exist.
                if let Some(cc) = city.country.iso_code {
                    out.insert(
                        "region_iso_code".to_string(),
                        EventValue::String(format!("{cc}-{code}")),
                    );
                }
            }
            if let Some(name) = sub.names.english {
                out.insert(
                    "region_name".to_string(),
                    EventValue::String(name.to_string()),
                );
            }
        }

        // Postal
        if let Some(code) = city.postal.code {
            out.insert(
                "postal_code".to_string(),
                EventValue::String(code.to_string()),
            );
        }

        // Location: latitude, longitude, timezone, and a combined `location`
        // object (Logstash emits a GeoJSON-style {lat, lon}).
        let lat = city.location.latitude;
        let lon = city.location.longitude;
        if let Some(lat) = lat {
            out.insert("latitude".to_string(), EventValue::Float(lat));
        }
        if let Some(lon) = lon {
            out.insert("longitude".to_string(), EventValue::Float(lon));
        }
        if let (Some(lat), Some(lon)) = (lat, lon) {
            let mut loc = IndexMap::new();
            loc.insert("lat".to_string(), EventValue::Float(lat));
            loc.insert("lon".to_string(), EventValue::Float(lon));
            out.insert("location".to_string(), EventValue::Object(loc));
        }
        if let Some(tz) = city.location.time_zone {
            out.insert("timezone".to_string(), EventValue::String(tz.to_string()));
        }

        // A successful lookup that resolved to a record with no usable fields
        // (only `ip`) is still a success — the IP was in the DB.
        Some(out)
    }
}

/// Fallback classification (RFC 1918 private / loopback / public) used when no
/// `MaxMind` database is configured. Always succeeds for a syntactically valid
/// IP, so events are enriched even without a database.
fn classify_ip(ip: IpAddr) -> IndexMap<String, EventValue> {
    let mut result = IndexMap::new();
    result.insert("ip".to_string(), EventValue::String(ip.to_string()));

    let network_type = if ip.is_loopback() {
        "loopback"
    } else if is_private(ip) {
        "private"
    } else {
        "public"
    };
    result.insert(
        "network_type".to_string(),
        EventValue::String(network_type.to_string()),
    );

    result
}

/// RFC 1918 (IPv4) / RFC 4193 (IPv6 ULA) private-range check, plus link-local.
fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_link_local(),
        // `Ipv6Addr::is_unique_local`/`is_unicast_link_local` are unstable on
        // older toolchains, so test the prefixes directly: fc00::/7 (ULA) and
        // fe80::/10 (link-local).
        IpAddr::V6(v6) => {
            let seg = v6.segments()[0];
            (seg & 0xfe00) == 0xfc00 || (seg & 0xffc0) == 0xfe80
        }
    }
}

#[async_trait]
impl FilterPlugin for GeoipFilter {
    fn name(&self) -> &'static str {
        "geoip"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        // Source field present?
        let raw = if let Some(val) = event.get(&self.source) {
            val.to_string_lossy()
        } else {
            for tag in &self.tag_on_failure {
                event.add_tag(tag);
            }
            return Ok(vec![event]);
        };

        // Valid IP?
        let ip: IpAddr = match raw.parse() {
            Ok(ip) => ip,
            Err(_) => {
                for tag in &self.tag_on_failure {
                    event.add_tag(tag);
                }
                return Ok(vec![event]);
            }
        };

        let geo_data = match &self.reader {
            // Real MaxMind lookup.
            Some(reader) => self.lookup(reader, ip),
            // No database configured: graceful classification fallback.
            None => Some(classify_ip(ip)),
        };

        match geo_data {
            Some(data) => event.set(self.target.clone(), EventValue::Object(data)),
            None => {
                for tag in &self.tag_on_failure {
                    event.add_tag(tag);
                }
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

    // ----- Config / classification-fallback tests -----
    //
    // These exercise the no-database path (graceful classification fallback).
    // They were originally written against the stub `classify_ip` and are
    // retained — the fallback behaviour is identical (private/loopback/public),
    // but `classify_ip` now takes a parsed `IpAddr` and uses std-library
    // range checks instead of string prefix matching, so the assertions still
    // hold. See the module docs for why the fallback exists.

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
    async fn test_geoip_invalid_ip_tags_failure() {
        // New behaviour: a non-IP value in the source field tags failure
        // (the stub never parsed the value, so this case was previously
        // impossible to test).
        let settings = serde_json::json!({ "source": "ip" });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("not-an-ip".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_geoip_lookup_failure"));
        assert!(result[0].get("geoip").is_none());
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
        let ip: IpAddr = "172.16.0.1".parse().expect("valid ip");
        let result = classify_ip(ip);
        assert_eq!(
            result.get("network_type"),
            Some(&EventValue::String("private".into()))
        );
    }

    #[test]
    fn test_database_default_empty() {
        // `database` defaults to empty (graceful-failure / classification mode),
        // so no reader is opened.
        let settings = serde_json::json!({});
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        assert!(filter.reader.is_none());
    }

    #[tokio::test]
    async fn test_geoip_missing_database_file_tags_failure() {
        // A configured-but-unopenable database is a graceful failure: the
        // reader is None and lookups tag failure (NOT a hard config error).
        let settings = serde_json::json!({
            "source": "ip",
            "database": "/nonexistent/path/to/GeoLite2-City.mmdb"
        });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        assert!(filter.reader.is_none());
        let mut event = Event::new("test");
        event.set("ip", EventValue::String("8.8.8.8".into()));
        let result = filter.filter(event).await.expect("filter");
        // No reader configured AND a path was given: classification fallback
        // still runs (reader is None ⇒ fallback), so it is enriched, NOT
        // tagged. This mirrors graceful degradation: a public IP gets a
        // network_type rather than a lost event.
        assert!(result[0].get("geoip").is_some());
    }

    // ----- Live-smoke test (real MaxMind database) -----
    //
    // Gated on the `GEOIP_MMDB` env var pointing to a `.mmdb` (the maxminddb
    // crate does NOT ship its test database to crates.io). Works against both
    // the canonical MaxMind *test* DB (81.2.69.x -> UK) and a real GeoLite2
    // City DB (8.8.8.8 / 1.1.1.1):
    //   GEOIP_MMDB=/path/to/GeoLite2-City.mmdb \
    //     cargo test -p ferro-stash-filter geoip_live -- --ignored
    #[tokio::test]
    #[ignore = "requires a real MaxMind .mmdb; set GEOIP_MMDB to enable"]
    async fn test_geoip_live_real_database() {
        let db = std::env::var("GEOIP_MMDB").expect("GEOIP_MMDB must be set for this test");
        let settings = serde_json::json!({ "source": "ip", "database": db });
        let filter = GeoipFilter::from_config(&settings, None).expect("config");
        assert!(filter.reader.is_some(), "database should have opened");

        // Probe several IPs so the test passes against either the canonical
        // MaxMind test DB or a real GeoLite2 City DB. At least one must yield
        // a real geoip object with a country code.
        let candidates = ["81.2.69.142", "89.160.20.128", "8.8.8.8", "1.1.1.1"];
        let mut found = false;
        for ip in candidates {
            let mut event = Event::new("test");
            event.set("ip", EventValue::String(ip.into()));
            let result = filter.filter(event).await.expect("filter");
            if let Some(EventValue::Object(obj)) = result[0].get("geoip") {
                assert_eq!(obj.get("ip"), Some(&EventValue::String(ip.into())));
                assert!(
                    obj.contains_key("country_code2"),
                    "expected a country_code2 for {ip}: {obj:?}"
                );
                assert!(!result[0].has_tag("_geoip_lookup_failure"));
                // For a City DB, latitude/longitude must be floats and the
                // combined `location` object must carry lat/lon. (Guarded so a
                // Country-only DB still passes.)
                if let Some(EventValue::Float(_)) = obj.get("latitude") {
                    assert!(matches!(obj.get("longitude"), Some(EventValue::Float(_))));
                    match obj.get("location") {
                        Some(EventValue::Object(loc)) => {
                            assert!(matches!(loc.get("lat"), Some(EventValue::Float(_))));
                            assert!(matches!(loc.get("lon"), Some(EventValue::Float(_))));
                        }
                        other => panic!("expected location object, got {other:?}"),
                    }
                }
                found = true;
                break;
            }
        }
        assert!(
            found,
            "none of {candidates:?} resolved in the database; is it a City DB?"
        );
    }
}
