// SPDX-License-Identifier: Apache-2.0
//! Elasticsearch output plugin — sends events via the Bulk API.
//!
//! Compatible with:
//! - Elasticsearch 7.x/8.x/9.x
//! - `FerroSearch` (all editions)
//! - `OpenSearch` 1.x/2.x

use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use reqwest::Client;
use tracing::{debug, error, warn};

/// Elasticsearch output plugin.
///
/// `Debug` is implemented manually so the `password` and `api_key` secrets are
/// never rendered in logs/diagnostics (`{:?}` prints `Some("***")` / `None`,
/// not the plaintext credentials).
#[allow(dead_code)]
pub struct ElasticsearchOutput {
    hosts: Vec<String>,
    index: String,
    client: Client,
    username: Option<String>,
    password: Option<String>,
    api_key: Option<String>,
    pipeline: Option<String>,
    document_id: Option<String>,
    routing: Option<String>,
    action: BulkAction,
    retry_count: usize,
    retry_delay_ms: u64,
    timeout_secs: u64,
    condition: Option<Condition>,
    host_index: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for ElasticsearchOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the password and api_key secrets so they can never leak via
        // `{:?}`. `username` is an identifier, not a secret, so it stays visible.
        let password = self.password.as_ref().map(|_| "***");
        let api_key = self.api_key.as_ref().map(|_| "***");
        f.debug_struct("ElasticsearchOutput")
            .field("hosts", &self.hosts)
            .field("index", &self.index)
            .field("client", &self.client)
            .field("username", &self.username)
            .field("password", &password)
            .field("api_key", &api_key)
            .field("pipeline", &self.pipeline)
            .field("document_id", &self.document_id)
            .field("routing", &self.routing)
            .field("action", &self.action)
            .field("retry_count", &self.retry_count)
            .field("retry_delay_ms", &self.retry_delay_ms)
            .field("timeout_secs", &self.timeout_secs)
            .field("condition", &self.condition)
            .field("host_index", &self.host_index)
            .finish()
    }
}

#[derive(Debug, Clone)]
enum BulkAction {
    Index,
    Create,
    Update,
    Delete,
}

impl ElasticsearchOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let hosts = match settings.get("hosts") {
            Some(serde_json::Value::Array(a)) => a
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
            Some(serde_json::Value::String(s)) => vec![s.clone()],
            _ => vec!["http://localhost:9200".to_string()],
        };

        let index = settings
            .get("index")
            .and_then(|v| v.as_str())
            .unwrap_or("ferrostash-%{+%Y.%m.%d}")
            .to_string();

        let username = settings
            .get("user")
            .or_else(|| settings.get("username"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let password = settings
            .get("password")
            .and_then(|v| v.as_str())
            .map(String::from);
        let api_key = settings
            .get("api_key")
            .and_then(|v| v.as_str())
            .map(String::from);
        let pipeline = settings
            .get("pipeline")
            .and_then(|v| v.as_str())
            .map(String::from);
        let document_id = settings
            .get("document_id")
            .and_then(|v| v.as_str())
            .map(String::from);
        let routing = settings
            .get("routing")
            .and_then(|v| v.as_str())
            .map(String::from);
        let action = match settings
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("index")
        {
            "create" => BulkAction::Create,
            "update" => BulkAction::Update,
            "delete" => BulkAction::Delete,
            _ => BulkAction::Index,
        };
        let retry_count = settings
            .get("retry_max_interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(3) as usize;
        let retry_delay_ms = settings
            .get("retry_initial_interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(2000);
        let timeout_secs = settings
            .get("timeout")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(60);

        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .danger_accept_invalid_certs(
                settings
                    .get("ssl_certificate_verification")
                    .and_then(ferro_stash_core::settings_helpers::as_bool_flexible)
                    .is_some_and(|v| !v),
            )
            .gzip(true)
            .build()
            .map_err(|e| FerroStashError::Output {
                plugin: "elasticsearch".to_string(),
                message: format!("HTTP client error: {e}"),
            })?;

        Ok(Self {
            hosts,
            index,
            client,
            username,
            password,
            api_key,
            pipeline,
            document_id,
            routing,
            action,
            retry_count,
            retry_delay_ms,
            timeout_secs,
            condition,
            host_index: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn next_host(&self) -> &str {
        let idx = self
            .host_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.hosts[idx % self.hosts.len()]
    }

    fn build_bulk_body(&self, events: &[Event]) -> String {
        let mut body = String::with_capacity(events.len() * 512);

        for event in events {
            let index_name = self.resolve_index(event);
            let action_name = match self.action {
                BulkAction::Index => "index",
                BulkAction::Create => "create",
                BulkAction::Update => "update",
                BulkAction::Delete => "delete",
            };

            // Action line
            let mut action_meta = serde_json::Map::new();
            action_meta.insert("_index".to_string(), serde_json::Value::String(index_name));
            if let Some(ref id_template) = self.document_id {
                let id = event.sprintf(id_template);
                action_meta.insert("_id".to_string(), serde_json::Value::String(id));
            }
            if let Some(ref routing_template) = self.routing {
                let routing = event.sprintf(routing_template);
                action_meta.insert("routing".to_string(), serde_json::Value::String(routing));
            }

            let action_obj = serde_json::json!({ action_name: action_meta });
            body.push_str(&serde_json::to_string(&action_obj).unwrap_or_default());
            body.push('\n');

            // Document line
            if !matches!(self.action, BulkAction::Delete) {
                body.push_str(&event.to_json_string());
                body.push('\n');
            }
        }

        body
    }

    fn resolve_index(&self, event: &Event) -> String {
        // Handle date math: %{+YYYY.MM.dd} → actual date
        let index = event.sprintf(&self.index);
        // Replace %{+format} with timestamp, converting Logstash date tokens to strftime
        if index.contains("%{+") {
            let mut result = index.clone();
            while let Some(start) = result.find("%{+") {
                if let Some(end) = result[start..].find('}') {
                    let logstash_fmt = &result[start + 3..start + end];
                    let strftime_fmt = logstash_date_to_strftime(logstash_fmt);
                    let formatted = event.timestamp.format(&strftime_fmt).to_string();
                    result = format!(
                        "{}{}{}",
                        &result[..start],
                        formatted,
                        &result[start + end + 1..]
                    );
                } else {
                    break;
                }
            }
            result
        } else {
            index
        }
    }
}

#[async_trait]
impl OutputPlugin for ElasticsearchOutput {
    fn name(&self) -> &'static str {
        "elasticsearch"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let body = self.build_bulk_body(&events);
        let host = self.next_host();
        let url = format!(
            "{}/_bulk{}",
            host.trim_end_matches('/'),
            self.pipeline
                .as_ref()
                .map(|p| format!("?pipeline={p}"))
                .unwrap_or_default()
        );

        let mut last_err = None;

        for attempt in 0..=self.retry_count {
            if attempt > 0 {
                let delay = self.retry_delay_ms * (1 << (attempt - 1).min(5));
                tokio::time::sleep(Duration::from_millis(delay)).await;
                debug!(attempt, "retrying bulk request");
            }

            let mut request = self
                .client
                .post(&url)
                .header("Content-Type", "application/x-ndjson")
                .body(body.clone());

            if let Some(ref api_key) = self.api_key {
                request = request.header("Authorization", format!("ApiKey {api_key}"));
            } else if let (Some(ref user), Some(ref pass)) = (&self.username, &self.password) {
                request = request.basic_auth(user, Some(pass));
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let body_text = response.text().await.unwrap_or_default();
                        // Check for item-level errors
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body_text) {
                            if json["errors"].as_bool() == Some(true) {
                                let failed = json["items"].as_array().map_or(0, |items| {
                                    items
                                        .iter()
                                        .filter(|item| {
                                            item.as_object()
                                                .and_then(|object| object.values().next())
                                                .is_some_and(|value| value.get("error").is_some())
                                        })
                                        .count()
                                });
                                warn!(
                                    events = events.len(),
                                    failed, "bulk request completed with errors"
                                );
                                return Err(FerroStashError::Output {
                                    plugin: "elasticsearch".to_string(),
                                    message: format!("bulk item-level errors: {failed} failed"),
                                });
                            }
                        }
                        debug!(events = events.len(), status = %status, "bulk request successful");
                        return Ok(());
                    } else if status.as_u16() == 429 {
                        // Too Many Requests — retry
                        warn!(status = %status, "rate limited, will retry");
                        last_err = Some(format!("HTTP {status}"));
                        continue;
                    } else if status.is_server_error() {
                        warn!(status = %status, "server error, will retry");
                        last_err = Some(format!("HTTP {status}"));
                        continue;
                    } else {
                        let body_text = response.text().await.unwrap_or_default();
                        error!(status = %status, body = %body_text, "bulk request failed");
                        return Err(FerroStashError::Output {
                            plugin: "elasticsearch".to_string(),
                            message: format!("HTTP {status}: {body_text}"),
                        });
                    }
                }
                Err(e) => {
                    warn!(error = %e, attempt, "bulk request error");
                    last_err = Some(e.to_string());
                    continue;
                }
            }
        }

        Err(FerroStashError::Output {
            plugin: "elasticsearch".to_string(),
            message: format!(
                "failed after {} retries: {}",
                self.retry_count,
                last_err.unwrap_or_default()
            ),
        })
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

/// Converts Logstash date format tokens to chrono strftime equivalents.
///
/// Logstash uses Joda-Time tokens: YYYY, MM, dd, HH, mm, ss, etc.
/// Chrono uses strftime: %Y, %m, %d, %H, %M, %S, etc.
fn logstash_date_to_strftime(logstash_fmt: &str) -> String {
    let mut result = logstash_fmt.to_string();
    // Order matters: longer tokens first to avoid partial replacements
    let replacements = [
        ("YYYY", "%Y"),
        ("yyyy", "%Y"),
        ("YY", "%y"),
        ("yy", "%y"),
        ("MM", "%m"),
        ("dd", "%d"),
        ("HH", "%H"),
        ("hh", "%I"),
        ("mm", "%M"),
        ("ss", "%S"),
        ("SSS", "%3f"),
        ("SS", "%f"),
        ("EEE", "%a"),
        ("EEEE", "%A"),
        ("MMM", "%b"),
        ("MMMM", "%B"),
        ("ZZ", "%z"),
        ("Z", "%Z"),
        ("ww", "%W"),
        ("ee", "%u"),
    ];
    for (from, to) in &replacements {
        result = result.replace(from, to);
    }
    // If the format already looks like strftime (contains %), pass through
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use tokio::net::TcpListener;

    #[test]
    fn test_build_bulk_body() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test-index"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let events = vec![Event::new("hello"), Event::new("world")];
        let body = output.build_bulk_body(&events);
        assert!(body.contains(r#""_index":"test-index""#));
        assert!(body.contains("hello"));
        assert!(body.contains("world"));
        // Each event has 2 lines (action + doc), so 4 lines total
        assert_eq!(body.lines().count(), 4);
    }

    #[test]
    fn test_resolve_index_static() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "my-index"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        assert_eq!(output.resolve_index(&event), "my-index");
    }

    #[test]
    fn test_config_defaults() {
        let settings = serde_json::json!({});
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.hosts, vec!["http://localhost:9200"]);
    }

    #[test]
    fn test_resolve_index_logstash_date() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "logs-%{+YYYY.MM.dd}"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let resolved = output.resolve_index(&event);
        // Should be "logs-2026.04.05" style, NOT "logs-YYYY.MM.dd"
        assert!(
            !resolved.contains("YYYY"),
            "Logstash date tokens should be converted: {resolved}"
        );
        assert!(
            !resolved.contains("MM}"),
            "Logstash date tokens should be converted: {resolved}"
        );
        // Should contain actual year
        assert!(
            resolved.contains("202"),
            "Should contain actual year: {resolved}"
        );
    }

    #[test]
    fn test_logstash_date_to_strftime() {
        assert_eq!(logstash_date_to_strftime("YYYY.MM.dd"), "%Y.%m.%d");
        assert_eq!(logstash_date_to_strftime("YYYY-MM-dd"), "%Y-%m-%d");
        assert_eq!(logstash_date_to_strftime("YYYY.MM.dd-HH"), "%Y.%m.%d-%H");
    }

    #[test]
    fn test_resolve_index_strftime_passthrough() {
        // If user already uses strftime format, it should still work
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "logs-%{+%Y.%m.%d}"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let resolved = output.resolve_index(&event);
        assert!(
            resolved.contains("202"),
            "Should contain actual year: {resolved}"
        );
    }

    use ferro_stash_core::event::EventValue;

    #[test]
    fn test_config_with_auth() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "user": "elastic",
            "password": "changeme"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.username, Some("elastic".to_string()));
        assert_eq!(output.password, Some("changeme".to_string()));
    }

    #[test]
    fn test_config_with_api_key() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "api_key": "abc123"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.api_key, Some("abc123".to_string()));
    }

    #[test]
    fn test_elasticsearch_debug_redacts_secrets() {
        // Neither the password nor the api_key must appear in Debug output.
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "user": "elastic",
            "password": "super-secret-pw",
            "api_key": "super-secret-key",
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");

        let output_dbg = format!("{output:?}");
        assert!(
            !output_dbg.contains("super-secret-pw"),
            "Debug leaked the password: {output_dbg}"
        );
        assert!(
            !output_dbg.contains("super-secret-key"),
            "Debug leaked the api_key: {output_dbg}"
        );
        assert!(output_dbg.contains("***"), "Debug must mark redaction");
        // Non-secret fields stay visible for diagnostics.
        assert!(
            output_dbg.contains("elastic"),
            "username should remain visible"
        );
    }

    #[test]
    fn test_config_create_action() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "action": "create"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.action, BulkAction::Create));
    }

    #[test]
    fn test_config_delete_action() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "action": "delete"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.action, BulkAction::Delete));
    }

    #[test]
    fn test_config_pipeline() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "pipeline": "my_pipeline"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.pipeline, Some("my_pipeline".to_string()));
    }

    #[test]
    fn test_config_document_id() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "document_id": "%{id}"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.document_id, Some("%{id}".to_string()));
    }

    #[test]
    fn test_build_bulk_body_delete() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test",
            "action": "delete",
            "document_id": "%{id}"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("id", EventValue::String("doc1".into()));
        let body = output.build_bulk_body(&[event]);
        assert!(body.contains("delete"));
        // Delete action has no document line
        assert_eq!(body.lines().count(), 1);
    }

    #[test]
    fn test_build_bulk_body_with_routing() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test",
            "routing": "%{tenant}"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("tenant", EventValue::String("t1".into()));
        let body = output.build_bulk_body(&[event]);
        assert!(body.contains("routing"));
        assert!(body.contains("t1"));
    }

    #[test]
    fn test_next_host_round_robin() {
        let settings = serde_json::json!({
            "hosts": ["http://host1:9200", "http://host2:9200"]
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let h1 = output.next_host().to_string();
        let h2 = output.next_host().to_string();
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_resolve_index_with_field_interpolation() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "logs-%{env}"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("env", EventValue::String("prod".into()));
        let resolved = output.resolve_index(&event);
        assert_eq!(resolved, "logs-prod");
    }

    #[tokio::test]
    async fn test_output_empty_events() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"]
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_output_fails_on_bulk_item_errors() {
        let app = Router::new().route(
            "/_bulk",
            post(|| async {
                Json(serde_json::json!({
                    "errors": true,
                    "items": [{
                        "index": {
                            "status": 400,
                            "error": {
                                "type": "mapper_parsing_exception",
                                "reason": "bad field"
                            }
                        }
                    }]
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let settings = serde_json::json!({
            "hosts": [format!("http://{addr}")],
            "index": "test"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("hello")]).await;

        assert!(result.is_err(), "bulk item-level errors must fail output");
    }

    #[test]
    fn test_config_single_host_string() {
        let settings = serde_json::json!({
            "hosts": "http://single:9200"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.hosts, vec!["http://single:9200"]);
    }
}
