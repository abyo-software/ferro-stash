// SPDX-License-Identifier: Apache-2.0
//! HTTP poller input — periodically requests configured HTTP endpoints and emits
//! each response (decoded by the codec) as events. Mirrors Logstash's
//! `http_poller` input for the common case.
//!
//! ```logstash
//! input {
//!   http_poller {
//!     urls => {
//!       health  => "http://localhost:8080/health"
//!       metrics => { url => "http://localhost:8080/m"  method => "get"
//!                    headers => { "Authorization" => "Bearer x" } }
//!     }
//!     schedule        => { every => "10s" }   # or: interval => 10
//!     request_timeout => 30
//!     codec           => "json"
//!   }
//! }
//! ```
//!
//! Each emitted event carries the request name in a top-level `http_poller_name`
//! field so downstream filters can route by source. Per-request failures are
//! logged and skipped (no synthetic failure event yet — see README "Honest
//! limitations").

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_codec::{create_codec, Codec};
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Clone, Debug)]
struct UrlSpec {
    name: String,
    url: String,
    method: String,
    headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct HttpPollerInput {
    urls: Vec<UrlSpec>,
    interval: u64,
    request_timeout: u64,
    codec: String,
    codec_settings: serde_json::Value,
}

/// Parse a Logstash-style duration (`"10s"`, `"5m"`, `"1h"`, or a bare number of
/// seconds) into seconds. Returns `None` for unrecognised input.
fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let (num, unit) = s.split_at(s.find(|c: char| c.is_alphabetic())?);
    let n: u64 = num.trim().parse().ok()?;
    match unit.trim() {
        "s" | "sec" | "secs" | "second" | "seconds" => Some(n),
        "m" | "min" | "mins" | "minute" | "minutes" => Some(n * 60),
        "h" | "hour" | "hours" => Some(n * 3600),
        "d" | "day" | "days" => Some(n * 86400),
        _ => None,
    }
}

impl HttpPollerInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let err = |m: String| FerroStashError::Input {
            plugin: "http_poller".to_string(),
            message: m,
        };

        let urls_obj = settings
            .get("urls")
            .and_then(|v| v.as_object())
            .ok_or_else(|| err("http_poller requires a `urls` map".to_string()))?;

        let mut urls = Vec::new();
        for (name, val) in urls_obj {
            let spec = if let Some(url) = val.as_str() {
                UrlSpec {
                    name: name.clone(),
                    url: url.to_string(),
                    method: "get".to_string(),
                    headers: BTreeMap::new(),
                }
            } else if let Some(obj) = val.as_object() {
                let url = obj
                    .get("url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| err(format!("url `{name}` is missing a `url`")))?
                    .to_string();
                let method = obj
                    .get("method")
                    .and_then(|v| v.as_str())
                    .unwrap_or("get")
                    .to_string();
                let headers = obj
                    .get("headers")
                    .and_then(|v| v.as_object())
                    .map(|h| {
                        h.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default();
                UrlSpec {
                    name: name.clone(),
                    url,
                    method,
                    headers,
                }
            } else {
                return Err(err(format!(
                    "url `{name}` must be a string or an object with a `url`"
                )));
            };
            urls.push(spec);
        }
        if urls.is_empty() {
            return Err(err("http_poller `urls` map is empty".to_string()));
        }

        // Schedule: `interval => N` (seconds) or `schedule => { every => "10s" }`.
        let interval = settings
            .get_u64("interval")
            .or_else(|| {
                settings
                    .get("schedule")
                    .and_then(|s| s.get("every"))
                    .and_then(|v| v.as_str())
                    .and_then(parse_duration_secs)
            })
            .unwrap_or(60)
            .max(1);

        let request_timeout = settings.get_u64("request_timeout").unwrap_or(60).max(1);
        let codec = settings.get_string("codec").unwrap_or_else(|| "json".to_string());
        let codec_settings = settings
            .get("codec_settings")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        Ok(Self {
            urls,
            interval,
            request_timeout,
            codec,
            codec_settings,
        })
    }

    fn build_codec(&self) -> Result<Box<dyn Codec>> {
        create_codec(&self.codec, &self.codec_settings).map_err(|e| FerroStashError::Input {
            plugin: "http_poller".to_string(),
            message: format!("unknown/invalid codec '{}': {e}", self.codec),
        })
    }
}

#[async_trait]
impl InputPlugin for HttpPollerInput {
    fn name(&self) -> &str {
        "http_poller"
    }

    async fn run(&mut self, sender: mpsc::Sender<Event>, mut shutdown: ShutdownSignal) -> Result<()> {
        let codec = self.build_codec()?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.request_timeout))
            .build()
            .map_err(|e| FerroStashError::Input {
                plugin: "http_poller".to_string(),
                message: format!("failed to build HTTP client: {e}"),
            })?;
        let poll_interval = Duration::from_secs(self.interval);
        info!(urls = self.urls.len(), interval_secs = self.interval, "http_poller starting");

        loop {
            for spec in &self.urls {
                let method = reqwest::Method::from_bytes(spec.method.to_uppercase().as_bytes())
                    .unwrap_or(reqwest::Method::GET);
                let mut req = client.request(method, &spec.url);
                for (k, v) in &spec.headers {
                    req = req.header(k, v);
                }
                match req.send().await {
                    Ok(resp) => {
                        let status = resp.status();
                        match resp.bytes().await {
                            Ok(body) => match codec.decode(&body) {
                                Ok(events) => {
                                    for mut event in events {
                                        event.set(
                                            "http_poller_name",
                                            EventValue::String(spec.name.clone()),
                                        );
                                        if sender.send(event).await.is_err() {
                                            info!("http_poller: downstream closed, stopping");
                                            return Ok(());
                                        }
                                    }
                                }
                                Err(e) => warn!(name = %spec.name, error = %e, "http_poller decode error"),
                            },
                            Err(e) => {
                                warn!(name = %spec.name, error = %e, "http_poller body read error")
                            }
                        }
                        debug!(name = %spec.name, %status, "http_poller polled");
                    }
                    Err(e) => warn!(name = %spec.name, url = %spec.url, error = %e, "http_poller request failed"),
                }
            }

            tokio::select! {
                () = tokio::time::sleep(poll_interval) => {}
                () = shutdown.wait() => {
                    info!("http_poller shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_forms() {
        assert_eq!(parse_duration_secs("10"), Some(10));
        assert_eq!(parse_duration_secs("10s"), Some(10));
        assert_eq!(parse_duration_secs("5m"), Some(300));
        assert_eq!(parse_duration_secs("1h"), Some(3600));
        assert_eq!(parse_duration_secs("bogus"), None);
    }

    #[test]
    fn from_config_string_url() {
        let s = serde_json::json!({ "urls": { "a": "http://x/health" }, "interval": 5 });
        let i = HttpPollerInput::from_config(&s).expect("config");
        assert_eq!(i.urls.len(), 1);
        assert_eq!(i.urls[0].url, "http://x/health");
        assert_eq!(i.urls[0].method, "get");
        assert_eq!(i.interval, 5);
    }

    #[test]
    fn from_config_object_url_and_schedule() {
        let s = serde_json::json!({
            "urls": { "m": { "url": "http://x/m", "method": "post",
                             "headers": { "Authorization": "Bearer t" } } },
            "schedule": { "every": "2m" }
        });
        let i = HttpPollerInput::from_config(&s).expect("config");
        assert_eq!(i.urls[0].method, "post");
        assert_eq!(i.urls[0].headers.get("Authorization").map(String::as_str), Some("Bearer t"));
        assert_eq!(i.interval, 120);
    }

    #[test]
    fn from_config_requires_urls() {
        assert!(HttpPollerInput::from_config(&serde_json::json!({})).is_err());
        assert!(HttpPollerInput::from_config(&serde_json::json!({ "urls": {} })).is_err());
    }

    #[test]
    fn interval_floored_and_defaulted() {
        let i = HttpPollerInput::from_config(&serde_json::json!({ "urls": { "a": "http://x" } }))
            .expect("config");
        assert_eq!(i.interval, 60); // default
    }

    /// Live smoke: polls a real JSON endpoint once and asserts an event is
    /// emitted carrying the request name. Run manually with the service up:
    ///   HTTP_POLLER_TEST_URL=http://localhost:8080/health \
    ///     cargo test -p ferro-stash-input -- --ignored live_poll_emits_events
    #[tokio::test]
    #[ignore = "live: set HTTP_POLLER_TEST_URL to a JSON endpoint"]
    async fn live_poll_emits_events() {
        use ferro_stash_core::shutdown::ShutdownController;
        let Ok(url) = std::env::var("HTTP_POLLER_TEST_URL") else {
            eprintln!("SKIPPED: set HTTP_POLLER_TEST_URL");
            return;
        };
        let cfg = serde_json::json!({ "urls": { "t": url }, "interval": 1, "codec": "json" });
        let mut input = HttpPollerInput::from_config(&cfg).expect("config");
        let (tx, mut rx) = mpsc::channel(64);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });
        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("channel closed without an event");
        assert_eq!(ev.get("http_poller_name").and_then(EventValue::as_str), Some("t"));
        controller.shutdown();
        let _ = handle.await;
    }
}
