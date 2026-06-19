// SPDX-License-Identifier: Apache-2.0
//! Datadog output plugin — sends events to the Datadog Log Intake API.
//!
//! Sends events to the Datadog Log Intake HTTP API (`/api/v2/logs`) using a
//! shared `reqwest` client, batching, and retry/backoff on transient failures.

use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use reqwest::Client;
use tracing::{debug, info, warn};

/// Number of send attempts (1 initial + retries) for transient failures.
const DATADOG_MAX_ATTEMPTS: usize = 4;
/// Base backoff between retries (doubled per attempt, capped).
const DATADOG_BACKOFF_BASE_MS: u64 = 250;

/// Datadog output configuration — mirrors the Logstash datadog output settings.
///
/// `Debug` is implemented manually so the `api_key` secret is never rendered in
/// logs/diagnostics (`{:?}` prints `"***"`, not the plaintext key).
#[derive(Clone)]
pub struct DatadogOutputConfig {
    pub api_key: String,
    pub host: String,
    pub codec: String,
    pub batch_size: usize,
    pub use_ssl: bool,
    pub source: String,
    pub service: String,
    pub tags: Vec<String>,
}

impl std::fmt::Debug for DatadogOutputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the API key so neither this struct nor any wrapper (e.g.
        // `DatadogOutput`'s derived Debug) can leak the secret via `{:?}`.
        f.debug_struct("DatadogOutputConfig")
            .field("api_key", &"***")
            .field("host", &self.host)
            .field("codec", &self.codec)
            .field("batch_size", &self.batch_size)
            .field("use_ssl", &self.use_ssl)
            .field("source", &self.source)
            .field("service", &self.service)
            .field("tags", &self.tags)
            .finish()
    }
}

#[derive(Debug)]
pub struct DatadogOutput {
    config: DatadogOutputConfig,
    condition: Option<Condition>,
    /// Pre-built request URL (`{scheme}://{host}/api/v2/logs`).
    url: String,
    /// Shared HTTP client (connection pooling + timeout).
    client: Client,
}

impl DatadogOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let api_key = settings
            .get_string("api_key")
            .ok_or_else(|| FerroStashError::Output {
                plugin: "datadog".to_string(),
                message: "api_key is required".to_string(),
            })?;

        let host = settings
            .get_string("host")
            .unwrap_or_else(|| "http-intake.logs.datadoghq.com".to_string());

        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "json".to_string());
        // The parser accepts `batch_size => 0`, but `chunks(0)` panics. A batch of
        // zero events is meaningless, so clamp to a minimum of 1.
        let batch_size = (settings.get_u64("batch_size").unwrap_or(50) as usize).max(1);
        let use_ssl = settings.get_bool("use_ssl").unwrap_or(true);
        let source = settings
            .get_string("source")
            .unwrap_or_else(|| "ferro-stash".to_string());
        let service = settings.get_string("service").unwrap_or_default();

        let tags: Vec<String> = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let scheme = if use_ssl { "https" } else { "http" };
        let url = format!("{scheme}://{host}/api/v2/logs");

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| FerroStashError::Output {
                plugin: "datadog".to_string(),
                message: format!("HTTP client error: {e}"),
            })?;

        Ok(Self {
            config: DatadogOutputConfig {
                api_key,
                host,
                codec,
                batch_size,
                use_ssl,
                source,
                service,
                tags,
            },
            condition,
            url,
            client,
        })
    }

    /// POST a single payload to the Datadog Log Intake API with retry/backoff.
    async fn post_payload(&self, payload: &str, event_count: usize) -> Result<()> {
        let mut last_error: Option<String> = None;

        for attempt in 0..DATADOG_MAX_ATTEMPTS {
            if attempt > 0 {
                let backoff = DATADOG_BACKOFF_BASE_MS * (1u64 << (attempt - 1).min(4));
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                debug!(attempt, "retrying Datadog log intake request");
            }

            let response = self
                .client
                .post(&self.url)
                .header("DD-API-KEY", &self.config.api_key)
                .header("Content-Type", "application/json")
                .body(payload.to_string())
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        debug!(event_count, status = %status, "Datadog logs accepted");
                        return Ok(());
                    }

                    let retriable = status.is_server_error() || status.as_u16() == 429;
                    let body = ferro_stash_core::read_bounded_body_stream(
                        Box::pin(resp.bytes_stream()),
                        crate::ERROR_BODY_SNIPPET_LIMIT,
                    )
                    .await;
                    warn!(status = %status, attempt, body = %body, "Datadog log intake error");
                    last_error = Some(format!("HTTP {status}: {body}"));
                    if !retriable {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, attempt, "Datadog request failed");
                    last_error = Some(format!("request failed: {e}"));
                }
            }
        }

        Err(FerroStashError::Output {
            plugin: "datadog".to_string(),
            message: last_error.unwrap_or_else(|| "request failed".to_string()),
        })
    }

    /// Format events into the Datadog Log Intake JSON format.
    ///
    /// Returns a JSON array of log entries, each with: message, ddsource, service,
    /// ddtags, hostname, and any additional fields.
    fn format_datadog_payload(&self, events: &[Event]) -> String {
        let entries: Vec<serde_json::Value> = events
            .iter()
            .map(|event| {
                let message = event.message().unwrap_or("").to_string();
                let hostname = event
                    .get("host")
                    .map(|v| v.to_string_lossy())
                    .unwrap_or_default();

                let mut entry = serde_json::Map::new();

                // Include extra fields from the event first, preserving their JSON
                // types so numbers/booleans/objects stay native (string-coercion
                // breaks facets). The reserved Datadog attributes are written
                // *after* this merge so an event field literally named `timestamp`,
                // `ddsource`, `ddtags`, `hostname`, `service`, or `message` (very
                // common in parsed logs) can never clobber the event-derived
                // reserved values — otherwise Datadog would fall back to ingest
                // time (re-opening the round-1 timestamp fix).
                for (key, value) in event.fields() {
                    if key != "message" && key != "host" && key != "@timestamp" && key != "@version"
                    {
                        entry.insert(key.clone(), serde_json::Value::from(value.clone()));
                    }
                }

                // Reserved attributes always win (written last).
                entry.insert("message".to_string(), serde_json::Value::String(message));
                entry.insert(
                    "ddsource".to_string(),
                    serde_json::Value::String(self.config.source.clone()),
                );
                entry.insert("hostname".to_string(), serde_json::Value::String(hostname));
                // Datadog reads the event time from `timestamp`/`date`; without it
                // the intake stamps ingest time instead of the actual event time.
                entry.insert(
                    "timestamp".to_string(),
                    serde_json::Value::String(event.timestamp.to_rfc3339()),
                );

                if !self.config.service.is_empty() {
                    entry.insert(
                        "service".to_string(),
                        serde_json::Value::String(self.config.service.clone()),
                    );
                }

                if !self.config.tags.is_empty() {
                    entry.insert(
                        "ddtags".to_string(),
                        serde_json::Value::String(self.config.tags.join(",")),
                    );
                }

                serde_json::Value::Object(entry)
            })
            .collect();

        serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
    }
}

#[async_trait]
impl OutputPlugin for DatadogOutput {
    fn name(&self) -> &'static str {
        "datadog"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Process in batches; each batch is a single POST to the log intake API.
        for chunk in events.chunks(self.config.batch_size) {
            let payload = self.format_datadog_payload(chunk);
            info!(
                url = %self.url,
                event_count = chunk.len(),
                payload_bytes = payload.len(),
                "Datadog output: POSTing {} events",
                chunk.len(),
            );
            self.post_payload(&payload, chunk.len()).await?;
        }

        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::HeaderMap, http::StatusCode, routing::post, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::net::TcpListener;

    #[test]
    fn test_datadog_config_defaults() {
        let settings = serde_json::json!({ "api_key": "abc123" });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.api_key, "abc123");
        assert_eq!(output.config.host, "http-intake.logs.datadoghq.com");
        assert_eq!(output.config.batch_size, 50);
        assert!(output.config.use_ssl);
        assert_eq!(output.config.source, "ferro-stash");
        assert_eq!(output.name(), "datadog");
    }

    #[test]
    fn test_datadog_config_full() {
        let settings = serde_json::json!({
            "api_key": "mykey",
            "host": "http-intake.logs.datadoghq.eu",
            "codec": "json",
            "batch_size": 100,
            "use_ssl": false,
            "source": "myapp",
            "service": "backend",
            "tags": ["env:prod", "team:platform"]
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.host, "http-intake.logs.datadoghq.eu");
        assert_eq!(output.config.batch_size, 100);
        assert!(!output.config.use_ssl);
        assert_eq!(output.config.service, "backend");
        assert_eq!(output.config.tags, vec!["env:prod", "team:platform"]);
    }

    #[test]
    fn test_datadog_missing_api_key() {
        let settings = serde_json::json!({});
        assert!(DatadogOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_datadog_payload_format() {
        let settings = serde_json::json!({
            "api_key": "key",
            "source": "myapp",
            "service": "web",
            "tags": ["env:test"]
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");

        let events = vec![Event::new("hello world")];
        let payload = output.format_datadog_payload(&events);
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&payload).expect("valid JSON array");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["message"], "hello world");
        assert_eq!(parsed[0]["ddsource"], "myapp");
        assert_eq!(parsed[0]["service"], "web");
        assert_eq!(parsed[0]["ddtags"], "env:test");
    }

    #[test]
    fn test_datadog_batch_size_zero_rejected() {
        // `batch_size => 0` would panic in `chunks(0)`; it must be clamped to 1.
        let settings = serde_json::json!({ "api_key": "key", "batch_size": 0 });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.batch_size, 1, "batch_size 0 must clamp to 1");
    }

    #[tokio::test]
    async fn test_datadog_batch_size_zero_does_not_panic() {
        // End-to-end guard: a 0 batch_size config must produce a working chunk
        // iterator (1 POST for 1 event) rather than panicking.
        let (host, requests, _key) = spawn_mock_intake(StatusCode::ACCEPTED).await;
        let settings = serde_json::json!({
            "api_key": "key",
            "host": host,
            "use_ssl": false,
            "batch_size": 0,
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("ok")]).await;
        assert!(result.is_ok());
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_datadog_reserved_attrs_win_over_user_fields() {
        // Regression: an event field literally named `timestamp` / `service` /
        // `ddsource` / `ddtags` / `hostname` / `message` must NOT clobber the
        // event-derived reserved attributes (which would re-open the round-1
        // timestamp fix and let user data override Datadog reserved attributes).
        let settings = serde_json::json!({
            "api_key": "key",
            "source": "myapp",
            "service": "web",
            "tags": ["env:test"],
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");

        let mut event = Event::new("real message");
        // User fields colliding with reserved attributes.
        event.set(
            "timestamp",
            ferro_stash_core::event::EventValue::String("1999-01-01T00:00:00Z".into()),
        );
        event.set(
            "service",
            ferro_stash_core::event::EventValue::String("user-service".into()),
        );
        event.set(
            "ddsource",
            ferro_stash_core::event::EventValue::String("user-source".into()),
        );
        event.set(
            "ddtags",
            ferro_stash_core::event::EventValue::String("user:tag".into()),
        );
        event.set(
            "hostname",
            ferro_stash_core::event::EventValue::String("user-host".into()),
        );

        let payload = output.format_datadog_payload(&[event.clone()]);
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&payload).expect("valid JSON array");

        // The event-derived timestamp must win, not the user's 1999 string.
        assert_eq!(
            parsed[0]["timestamp"],
            event.timestamp.to_rfc3339(),
            "event timestamp must win over a user `timestamp` field"
        );
        assert_ne!(parsed[0]["timestamp"], "1999-01-01T00:00:00Z");

        // Config-derived reserved attributes win over the user fields.
        assert_eq!(parsed[0]["service"], "web");
        assert_eq!(parsed[0]["ddsource"], "myapp");
        assert_eq!(parsed[0]["ddtags"], "env:test");
        assert_eq!(parsed[0]["message"], "real message");
        // `hostname` is event-derived (from `host`, here absent) and must not be
        // overridden by a user `hostname` field.
        assert_ne!(parsed[0]["hostname"], "user-host");
    }

    #[test]
    fn test_datadog_config_debug_redacts_api_key() {
        // The api_key secret must never appear in Debug output.
        let settings = serde_json::json!({
            "api_key": "super-secret-key",
            "host": "intake.example.com",
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");

        let config_dbg = format!("{:?}", output.config);
        assert!(
            !config_dbg.contains("super-secret-key"),
            "config Debug leaked the api_key: {config_dbg}"
        );
        assert!(
            config_dbg.contains("***"),
            "config Debug must mark redaction"
        );
        // Non-secret fields stay visible for diagnostics.
        assert!(
            config_dbg.contains("intake.example.com"),
            "host should remain visible"
        );

        // The wrapper's Debug (which prints the config) must also not leak it.
        let output_dbg = format!("{output:?}");
        assert!(
            !output_dbg.contains("super-secret-key"),
            "output Debug leaked the api_key: {output_dbg}"
        );
    }

    #[test]
    fn test_datadog_payload_preserves_types_and_timestamp() {
        let settings = serde_json::json!({ "api_key": "key" });
        let output = DatadogOutput::from_config(&settings, None).expect("config");

        let mut event = Event::new("typed");
        event.set("count", ferro_stash_core::event::EventValue::Integer(42));
        event.set("ok", ferro_stash_core::event::EventValue::Boolean(true));
        let payload = output.format_datadog_payload(&[event]);
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&payload).expect("valid JSON array");

        // Numeric field stays numeric; boolean stays boolean (not string-coerced).
        assert!(parsed[0]["count"].is_number(), "count must stay numeric");
        assert_eq!(parsed[0]["count"], 42);
        assert!(parsed[0]["ok"].is_boolean(), "ok must stay boolean");
        assert_eq!(parsed[0]["ok"].as_bool(), Some(true));

        // A timestamp attribute is emitted (so Datadog uses event time, not ingest).
        assert!(
            parsed[0]["timestamp"].is_string(),
            "timestamp attribute must be present"
        );
        assert!(!parsed[0]["timestamp"].as_str().unwrap_or("").is_empty());
    }

    #[tokio::test]
    async fn test_datadog_output_empty_is_ok() {
        let settings = serde_json::json!({ "api_key": "key" });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![]).await;
        assert!(result.is_ok());
    }

    /// Spawns a mock Datadog intake server, returns (host:port, request-counter,
    /// captured-api-key).
    async fn spawn_mock_intake(
        status: StatusCode,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<std::sync::Mutex<Option<String>>>,
    ) {
        let requests = Arc::new(AtomicUsize::new(0));
        let api_key = Arc::new(std::sync::Mutex::new(None));
        let req_handle = Arc::clone(&requests);
        let key_handle = Arc::clone(&api_key);
        let app = Router::new().route(
            "/api/v2/logs",
            post(move |headers: HeaderMap, _body: String| {
                let req_handle = Arc::clone(&req_handle);
                let key_handle = Arc::clone(&key_handle);
                async move {
                    req_handle.fetch_add(1, Ordering::SeqCst);
                    if let Some(v) = headers.get("DD-API-KEY").and_then(|v| v.to_str().ok()) {
                        if let Ok(mut guard) = key_handle.lock() {
                            *guard = Some(v.to_string());
                        }
                    }
                    (status, "")
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        (addr.to_string(), requests, api_key)
    }

    #[tokio::test]
    async fn test_datadog_output_posts_logs() {
        let (host, requests, api_key) = spawn_mock_intake(StatusCode::ACCEPTED).await;
        let settings = serde_json::json!({
            "api_key": "secret-key",
            "host": host,
            "use_ssl": false,
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("hello")]).await;
        assert!(result.is_ok(), "expected ok, got {result:?}");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            api_key.lock().expect("lock").as_deref(),
            Some("secret-key"),
            "DD-API-KEY header must be forwarded"
        );
    }

    #[tokio::test]
    async fn test_datadog_output_batching_multiple_posts() {
        let (host, requests, _key) = spawn_mock_intake(StatusCode::OK).await;
        let settings = serde_json::json!({
            "api_key": "key",
            "host": host,
            "use_ssl": false,
            "batch_size": 2,
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let events = vec![Event::new("e1"), Event::new("e2"), Event::new("e3")];
        // 3 events / batch_size 2 => 2 POSTs.
        let result = output.output(events).await;
        assert!(result.is_ok());
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_datadog_output_non_2xx_errors() {
        // 400 is non-retriable => single attempt, returns Err.
        let (host, requests, _key) = spawn_mock_intake(StatusCode::BAD_REQUEST).await;
        let settings = serde_json::json!({
            "api_key": "key",
            "host": host,
            "use_ssl": false,
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("bad")]).await;
        assert!(result.is_err(), "non-2xx must return an error");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_datadog_output_retries_transient() {
        // Server returns 503 on first attempt, then 202.
        let requests = Arc::new(AtomicUsize::new(0));
        let req_handle = Arc::clone(&requests);
        let app = Router::new().route(
            "/api/v2/logs",
            post(move || {
                let req_handle = Arc::clone(&req_handle);
                async move {
                    let n = req_handle.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        (StatusCode::SERVICE_UNAVAILABLE, "retry")
                    } else {
                        (StatusCode::ACCEPTED, "ok")
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        let settings = serde_json::json!({
            "api_key": "key",
            "host": addr.to_string(),
            "use_ssl": false,
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("hi")]).await;
        assert!(result.is_ok(), "should succeed after retry: {result:?}");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    /// Live smoke test against the real Datadog Log Intake API.
    /// Gated behind `DATADOG_API_KEY` (and optional `DATADOG_HOST`); run with
    /// `cargo test -p ferro-stash-output -- --ignored datadog_live`.
    #[tokio::test]
    #[ignore = "requires DATADOG_API_KEY env var and network access"]
    async fn datadog_live_smoke() {
        let api_key = std::env::var("DATADOG_API_KEY").expect("DATADOG_API_KEY");
        let mut settings = serde_json::json!({ "api_key": api_key });
        if let Ok(host) = std::env::var("DATADOG_HOST") {
            settings["host"] = serde_json::Value::String(host);
        }
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let event = Event::new("ferro-stash datadog live smoke test");
        output
            .output(vec![event])
            .await
            .expect("live Datadog POST should succeed");
    }
}
