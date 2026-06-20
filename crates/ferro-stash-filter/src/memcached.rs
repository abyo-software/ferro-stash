// SPDX-License-Identifier: Apache-2.0
//! Memcached filter — `get`/`set` event fields against memcached. Mirrors
//! Logstash's `memcached` filter.
//!
//! ```logstash
//! filter {
//!   memcached {
//!     hosts     => ["localhost:11211"]
//!     namespace => "app:"
//!     ttl       => 60
//!     # fetch each memcached key, store the value into the event field:
//!     get => { "user:%{user_id}" => "[user_name]" }
//!     # store each event field's value at the memcached key:
//!     set => { "[user_name]" => "user:%{user_id}" }
//!   }
//! }
//! ```
//!
//! Backed by the synchronous `memcache` crate (multi-host consistent hashing)
//! driven from `tokio::task::spawn_blocking`, since its `Client` exposes `&self`
//! get/set (a clean fit for the `&self` filter signature) and natively supports
//! the `hosts` array. The `tls`/OpenSSL feature is disabled to match the repo's
//! rustls-only TLS stance (plaintext memcached only). The client is established
//! lazily on first use and reused.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tokio::sync::OnceCell;

fn filter_err(message: String) -> FerroStashError {
    FerroStashError::Filter {
        plugin: "memcached".to_string(),
        message,
    }
}

/// Normalize a `host:port` (or full `memcache://…`) entry into a connection URL,
/// adding a per-operation timeout so a dead server fails fast.
fn host_to_url(h: &str) -> String {
    let base = if h.contains("://") {
        h.to_string()
    } else {
        format!("memcache://{h}")
    };
    if base.contains('?') {
        base
    } else {
        format!("{base}?timeout=5")
    }
}

fn parse_map(v: Option<&serde_json::Value>) -> Vec<(String, String)> {
    v.and_then(|v| v.as_object())
        .map(|o| {
            o.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

pub struct MemcachedFilter {
    hosts: Vec<String>,
    namespace: Option<String>,
    ttl: u32,
    /// `(memcache_key_template, event_field)` — fetch the key, set the field.
    get: Vec<(String, String)>,
    /// `(event_field, memcache_key_template)` — store the field value at the key.
    set: Vec<(String, String)>,
    condition: Option<Condition>,
    /// Lazily-established, shared sync client (serialized via the Mutex).
    client: OnceCell<Arc<Mutex<memcache::Client>>>,
}

impl std::fmt::Debug for MemcachedFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hosts can be memcache:// URLs with SASL userinfo — redact each.
        let hosts: Vec<String> = self
            .hosts
            .iter()
            .map(|h| ferro_stash_core::redact_url(h.as_str()))
            .collect();
        f.debug_struct("MemcachedFilter")
            .field("hosts", &hosts)
            .field("namespace", &self.namespace)
            .field("ttl", &self.ttl)
            .field("get", &self.get)
            .field("set", &self.set)
            .field("condition", &self.condition)
            .field("connected", &self.client.get().is_some())
            .finish()
    }
}

impl MemcachedFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let hosts: Vec<String> = match settings.get("hosts") {
            Some(serde_json::Value::Array(a)) => a
                .iter()
                .filter_map(|v| v.as_str().map(host_to_url))
                .collect(),
            Some(serde_json::Value::String(s)) => vec![host_to_url(s)],
            _ => vec![],
        };
        let hosts = if hosts.is_empty() {
            vec![host_to_url("localhost:11211")]
        } else {
            hosts
        };

        let ttl = settings
            .get_u32("ttl", 0)
            .map_err(|message| FerroStashError::Filter {
                plugin: "memcached".to_string(),
                message,
            })?;

        Ok(Self {
            hosts,
            namespace: settings.get_string("namespace"),
            ttl,
            get: parse_map(settings.get("get")),
            set: parse_map(settings.get("set")),
            condition,
            client: OnceCell::new(),
        })
    }

    fn namespaced(&self, key: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}{key}"),
            None => key.to_string(),
        }
    }

    /// Lazily connect (on a blocking thread) and return the shared client handle.
    async fn client(&self) -> Result<Arc<Mutex<memcache::Client>>> {
        self.client
            .get_or_try_init(|| async {
                let urls = self.hosts.clone();
                let client = tokio::task::spawn_blocking(move || memcache::connect(urls))
                    .await
                    .map_err(|e| filter_err(format!("connect task join error: {e}")))?
                    .map_err(|e| filter_err(format!("memcached connect failed: {e}")))?;
                Ok(Arc::new(Mutex::new(client)))
            })
            .await
            .map(Arc::clone)
    }
}

#[async_trait]
impl FilterPlugin for MemcachedFilter {
    fn name(&self) -> &str {
        "memcached"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if self.get.is_empty() && self.set.is_empty() {
            return Ok(vec![event]);
        }

        // Resolve all keys/values up-front (templates are `%{field}`-aware).
        let get_ops: Vec<(String, String)> = self
            .get
            .iter()
            .map(|(mc_key, field)| (field.clone(), self.namespaced(&event.sprintf(mc_key))))
            .collect();
        let set_ops: Vec<(String, String)> = self
            .set
            .iter()
            .filter_map(|(field, mc_key)| {
                event
                    .get(field)
                    .map(|v| (self.namespaced(&event.sprintf(mc_key)), v.to_string_lossy()))
            })
            .collect();

        let client = self.client().await?;
        let ttl = self.ttl;

        // All blocking memcached I/O runs on a blocking thread. Gets run first
        // (enrich), then sets (store).
        let results: Vec<(String, Option<Vec<u8>>)> =
            tokio::task::spawn_blocking(move || -> Result<Vec<(String, Option<Vec<u8>>)>> {
                // Recover from a poisoned mutex (a prior panic while holding the
                // lock) instead of cascading the panic into this worker.
                let guard = client.lock().unwrap_or_else(|e| e.into_inner());
                let mut out = Vec::with_capacity(get_ops.len());
                for (field, key) in get_ops {
                    let val = guard
                        .get::<Vec<u8>>(&key)
                        .map_err(|e| filter_err(format!("memcached get '{key}' failed: {e}")))?;
                    out.push((field, val));
                }
                for (key, value) in set_ops {
                    guard
                        .set(&key, value.as_bytes(), ttl)
                        .map_err(|e| filter_err(format!("memcached set '{key}' failed: {e}")))?;
                }
                Ok(out)
            })
            .await
            .map_err(|e| filter_err(format!("memcached task join error: {e}")))??;

        for (field, val) in results {
            if let Some(bytes) = val {
                event.set(
                    field,
                    EventValue::String(String::from_utf8_lossy(&bytes).into_owned()),
                );
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

    #[test]
    fn defaults() {
        let f = MemcachedFilter::from_config(&serde_json::json!({}), None).expect("config");
        assert_eq!(f.hosts, vec!["memcache://localhost:11211?timeout=5"]);
        assert_eq!(f.ttl, 0);
        assert!(f.namespace.is_none());
        assert!(f.get.is_empty() && f.set.is_empty());
        assert_eq!(f.name(), "memcached");
    }

    #[test]
    fn parses_hosts_get_set_namespace_ttl() {
        let f = MemcachedFilter::from_config(
            &serde_json::json!({
                "hosts": ["mc1:11211", "mc2:11211"],
                "namespace": "app:",
                "ttl": 60,
                "get": { "user:%{uid}": "[user_name]" },
                "set": { "[user_name]": "user:%{uid}" },
            }),
            None,
        )
        .expect("config");
        assert_eq!(f.hosts.len(), 2);
        assert!(f.hosts[0].starts_with("memcache://mc1:11211"));
        assert_eq!(f.namespace.as_deref(), Some("app:"));
        assert_eq!(f.ttl, 60);
        assert_eq!(
            f.get,
            vec![("user:%{uid}".to_string(), "[user_name]".to_string())]
        );
        assert_eq!(
            f.set,
            vec![("[user_name]".to_string(), "user:%{uid}".to_string())]
        );
    }

    #[test]
    fn host_string_form_accepted() {
        let f = MemcachedFilter::from_config(&serde_json::json!({ "hosts": "h:11211" }), None)
            .expect("config");
        assert_eq!(f.hosts, vec!["memcache://h:11211?timeout=5"]);
    }

    #[test]
    fn ttl_overflow_rejected() {
        // ttl > u32::MAX must fail loudly instead of truncating via `as u32`.
        assert!(MemcachedFilter::from_config(
            &serde_json::json!({ "ttl": 4_294_967_296u64 }),
            None
        )
        .is_err());
    }

    #[test]
    fn namespaced_prefixes_key() {
        let f = MemcachedFilter::from_config(&serde_json::json!({ "namespace": "ns:" }), None)
            .expect("config");
        assert_eq!(f.namespaced("k"), "ns:k");
        let f2 = MemcachedFilter::from_config(&serde_json::json!({}), None).expect("config");
        assert_eq!(f2.namespaced("k"), "k");
    }

    #[tokio::test]
    async fn no_ops_passthrough_without_connection() {
        // With no get/set, the filter must not attempt a connection.
        let f =
            MemcachedFilter::from_config(&serde_json::json!({ "hosts": ["127.0.0.1:1"] }), None)
                .expect("config");
        let out = f.filter(Event::new("hi")).await.expect("passthrough");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].message(), Some("hi"));
    }

    /// Live smoke (real memcached): set `MEMCACHED_HOST` (e.g. `localhost:11211`).
    /// Sets a key then gets it back into a field. Run with memcached up:
    ///   MEMCACHED_HOST=localhost:11211 \
    ///     cargo test -p ferro-stash-filter -- --ignored memcached_live
    #[tokio::test]
    #[ignore = "live: set MEMCACHED_HOST (running memcached)"]
    async fn memcached_live_round_trip() {
        let Ok(host) = std::env::var("MEMCACHED_HOST") else {
            eprintln!("SKIPPED: set MEMCACHED_HOST");
            return;
        };
        let unique = format!("ferro-stash-live-{}", std::process::id());

        // Store the event's `payload` field at the key.
        let setter = MemcachedFilter::from_config(
            &serde_json::json!({
                "hosts": [host.clone()],
                "set": { "[payload]": unique.clone() },
            }),
            None,
        )
        .expect("config");
        let mut ev = Event::new("x");
        ev.set("payload", EventValue::String("cached-value".into()));
        setter.filter(ev).await.expect("set");

        // Fetch the key back into `restored`.
        let getter = MemcachedFilter::from_config(
            &serde_json::json!({
                "hosts": [host],
                "get": { unique: "[restored]" },
            }),
            None,
        )
        .expect("config");
        let out = getter.filter(Event::new("y")).await.expect("get");
        assert_eq!(
            out[0].get("restored"),
            Some(&EventValue::String("cached-value".into()))
        );
    }
}
