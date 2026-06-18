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

/// Generous cap on the bulk *success* (2xx) response body that we buffer before
/// scanning it for item-level errors. `Response::text()` buffers the entire body
/// unconditionally, so a misconfigured/compromised host could return a `200`
/// with a multi-GB body and OOM the process. Bulk responses are normally bounded
/// by the batch size, so this never trips legitimately — it only stops a
/// pathological response. Matches the ES filter/input 256 MB cap for consistency.
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

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
    doc_as_upsert: bool,
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
            .field("doc_as_upsert", &self.doc_as_upsert)
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
        let hosts: Vec<String> = match settings.get("hosts") {
            Some(serde_json::Value::Array(a)) => a
                .iter()
                // Trim each entry and drop empty/whitespace-only hosts so a blank
                // host (e.g. `hosts => ["", "  "]`) can never reach `next_host()`.
                .filter_map(|v| v.as_str().map(str::trim).filter(|s| !s.is_empty()))
                .map(String::from)
                .collect(),
            Some(serde_json::Value::String(s)) => {
                // A single host string is also trimmed; a blank/whitespace-only
                // string collects to an empty vec and is rejected below.
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    Vec::new()
                } else {
                    vec![trimmed.to_string()]
                }
            }
            // Not configured at all => default host (preserves prior behavior).
            _ => vec!["http://localhost:9200".to_string()],
        };
        // A configured-but-empty `hosts` (e.g. `hosts => []`, an array of
        // non-strings like `hosts => [9200]`, or one with only blank/whitespace
        // entries like `hosts => [""]` / `hosts => ""`) collects to an empty vec.
        // Reject it at config time — otherwise the first `output()` call would
        // panic in `next_host()` via `idx % self.hosts.len()` (divide-by-zero)
        // and the out-of-bounds index. Mirrors the kafka output's
        // `bootstrap_servers` contract: fail loudly at config time rather than at
        // runtime.
        if hosts.is_empty() {
            return Err(FerroStashError::Output {
                plugin: "elasticsearch".to_string(),
                message: "hosts is empty (configure at least one host URL)".to_string(),
            });
        }

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
        // A bulk UPDATE addresses an existing document by `_id`, so it cannot
        // work without one. Reject an `action => "update"` config that has no
        // `document_id` source at all — otherwise every batch would produce a
        // per-item "document missing" / routing error and nothing would ever be
        // updated. A `document_id` driven by `%{field}` is fine (the id is
        // resolved per event); we only reject the no-id case. `delete` likewise
        // needs an `_id`, but its prior contract is left unchanged here.
        if matches!(action, BulkAction::Update) && document_id.is_none() {
            return Err(FerroStashError::Output {
                plugin: "elasticsearch".to_string(),
                message: "action => \"update\" requires a document_id (set `document_id`, \
                          e.g. document_id => \"%{id}\"): a bulk update addresses an \
                          existing document by _id and cannot work without one"
                    .to_string(),
            });
        }
        // Opt-in `doc_as_upsert => true`: emit `"doc_as_upsert": true` alongside
        // the partial `doc` so an update to a missing document inserts it
        // instead of failing. Only meaningful for the `update` action; ignored
        // for index/create/delete. Defaults to false to preserve prior behavior.
        let doc_as_upsert = settings
            .get("doc_as_upsert")
            .and_then(ferro_stash_core::settings_helpers::as_bool_flexible)
            .unwrap_or(false);
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
            doc_as_upsert,
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

            // Source line.
            //
            // `delete` has no source line. `index`/`create` send the raw
            // document. `update` is different: the bulk UPDATE source line must
            // be a set of partial-update instructions, NOT the raw document. ES
            // expects `{"doc": {...}}` (or `{"script": ...}`); sending the raw
            // event there yields a per-item "Validation Failed: script or doc
            // is missing" error for EVERY document, so a normal
            // `action => "update"` config would fail/retry/DLQ every batch and
            // nothing would ever be updated. Wrap the event body in `doc`
            // (optionally with `doc_as_upsert`) so the partial update is valid.
            match self.action {
                BulkAction::Delete => {}
                BulkAction::Update => {
                    let mut update_body = serde_json::Map::new();
                    update_body.insert("doc".to_string(), event.to_json());
                    if self.doc_as_upsert {
                        update_body.insert(
                            "doc_as_upsert".to_string(),
                            serde_json::Value::Bool(true),
                        );
                    }
                    let update_obj = serde_json::Value::Object(update_body);
                    body.push_str(&serde_json::to_string(&update_obj).unwrap_or_default());
                    body.push('\n');
                }
                BulkAction::Index | BulkAction::Create => {
                    body.push_str(&event.to_json_string());
                    body.push('\n');
                }
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
        self.output_capped(events, MAX_RESPONSE_BYTES).await
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

impl ElasticsearchOutput {
    /// Like [`OutputPlugin::output`], but with an explicit cap on the number of
    /// bytes buffered from a successful (2xx) bulk response. `output()` always
    /// passes the production [`MAX_RESPONSE_BYTES`]; tests pass a small cap to
    /// drive the over-limit path without producing a 256 MB body.
    async fn output_capped(&self, events: Vec<Event>, max_response_bytes: usize) -> Result<()> {
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
                        // Bound the success-path body read. `Response::text()`
                        // buffers the whole body, so a misconfigured/compromised
                        // host returning a huge 200 could OOM the process. Stream
                        // it with a generous cap; an over-limit/unreadable body is
                        // treated as a bulk *failure* (retry, like a non-2xx),
                        // never an unbounded buffer.
                        //
                        // `bytes_stream()` consumes `response`, so the status
                        // borrow above must (and does) happen first.
                        let body_bytes = match ferro_stash_core::read_capped_body(
                            Box::pin(response.bytes_stream()),
                            max_response_bytes,
                        )
                        .await
                        {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                warn!(
                                    status = %status,
                                    error = %e,
                                    "bulk 2xx response body too large or unreadable, will retry"
                                );
                                last_err = Some(format!("response body unreadable: {e}"));
                                continue;
                            }
                        };
                        // A 2xx status alone does NOT mean Elasticsearch
                        // acknowledged the batch: a proxy / auth gateway / WAF /
                        // misrouted host can return HTTP 200 with HTML or a
                        // non-bulk body like `{"ok":true}`. Treating such a
                        // response as delivered is silent data loss — the events
                        // are recorded as written even though no item was ever
                        // acknowledged (same class as the round-14 form bug).
                        //
                        // Only a well-formed bulk response counts as delivered:
                        // it MUST parse as JSON, carry a boolean `errors` field
                        // and an array `items` field, and `items.len()` MUST
                        // equal the number of events we sent (the bulk response
                        // acknowledges exactly the actions submitted). Anything
                        // else is treated as a FAILURE so the pipeline can
                        // retry/DLQ — the same path the retriable 5xx/429 branch
                        // uses. The 256 MB read cap above still applies.
                        // Bounded snippet of the (possibly unexpected) body for
                        // diagnostics; never logs/embeds the body unbounded.
                        let snippet = ferro_stash_core::bounded_snippet(
                            &String::from_utf8_lossy(&body_bytes),
                            crate::ERROR_BODY_SNIPPET_LIMIT,
                        );
                        let json = match serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                            Ok(json) => json,
                            Err(e) => {
                                warn!(
                                    status = %status,
                                    error = %e,
                                    body = %snippet,
                                    "bulk 2xx response is not valid JSON (proxy/WAF/misrouted host?), \
                                     treating as failure"
                                );
                                last_err = Some(format!(
                                    "2xx response is not a valid bulk response (not JSON): {snippet}"
                                ));
                                continue;
                            }
                        };
                        let errors_field = json.get("errors").and_then(serde_json::Value::as_bool);
                        let items = json.get("items").and_then(serde_json::Value::as_array);
                        let (errors_present, items) = match (errors_field, items) {
                            (Some(errors), Some(items)) => (errors, items),
                            _ => {
                                warn!(
                                    status = %status,
                                    body = %snippet,
                                    "bulk 2xx response is missing the boolean `errors` and/or \
                                     array `items` fields (not a bulk response), treating as failure"
                                );
                                last_err = Some(format!(
                                    "2xx response is not a valid bulk response \
                                     (missing errors/items): {snippet}"
                                ));
                                continue;
                            }
                        };
                        if items.len() != events.len() {
                            warn!(
                                status = %status,
                                expected = events.len(),
                                got = items.len(),
                                body = %snippet,
                                "bulk 2xx response acknowledged a different number of items than \
                                 events sent, treating as failure"
                            );
                            last_err = Some(format!(
                                "2xx bulk response acknowledged {} items but {} events were sent",
                                items.len(),
                                events.len()
                            ));
                            continue;
                        }
                        // Well-formed bulk response: apply item-level-error detection.
                        if errors_present {
                            let failed = items
                                .iter()
                                .filter(|item| {
                                    item.as_object()
                                        .and_then(|object| object.values().next())
                                        .is_some_and(|value| value.get("error").is_some())
                                })
                                .count();
                            warn!(
                                events = events.len(),
                                failed, "bulk request completed with errors"
                            );
                            return Err(FerroStashError::Output {
                                plugin: "elasticsearch".to_string(),
                                message: format!("bulk item-level errors: {failed} failed"),
                            });
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
                        let body_text = ferro_stash_core::read_bounded_body_stream(
                            Box::pin(response.bytes_stream()),
                            crate::ERROR_BODY_SNIPPET_LIMIT,
                        )
                        .await;
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
    async fn test_bulk_error_body_is_bounded() {
        // Regression: a non-2xx bulk response with a huge error body must not be
        // read/logged/returned unbounded. The diagnostic body is read via
        // `read_bounded_body_stream`, which reads at most `limit + 1` bytes (the
        // READ is bounded, not just the snippet — a multi-GB error body cannot
        // OOM the process), then truncates to a capped snippet that carries the
        // `bytes total` truncation marker.
        let huge_len = crate::ERROR_BODY_SNIPPET_LIMIT * 8;
        let huge_body = "E".repeat(huge_len);
        let huge_for_handler = huge_body.clone();
        let app = Router::new().route(
            "/_bulk",
            post(move || {
                let huge = huge_for_handler.clone();
                async move { (axum::http::StatusCode::BAD_REQUEST, huge) }
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

        let err = result.expect_err("non-2xx bulk response must fail output");
        let message = err.to_string();
        // The full body would be 8x the limit; the message must be far smaller.
        assert!(
            message.len() < huge_len,
            "error message must not embed the unbounded body (len {})",
            message.len()
        );
        // The READ is bounded to `limit + 1` bytes, so the truncation marker
        // reports the bounded read length — not the full (unread) body length.
        let read_len = crate::ERROR_BODY_SNIPPET_LIMIT + 1;
        assert!(
            message.contains(&format!("{read_len} bytes total")),
            "error message must carry the truncation marker: {message}"
        );
        // The snippet stays within the limit plus a small marker overhead;
        // it never approaches the full body size.
        assert!(
            message.len() < crate::ERROR_BODY_SNIPPET_LIMIT + 128,
            "snippet must stay bounded (len {})",
            message.len()
        );
        // No unbounded run: the verbatim body must not appear in full.
        assert!(
            !message.contains(&huge_body),
            "error message must not contain the full body verbatim"
        );
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

    #[test]
    fn test_config_empty_hosts_array_rejected() {
        // A configured-but-empty `hosts` array must be rejected at config time
        // rather than panicking at runtime in `next_host()` (`% 0` /
        // out-of-bounds). A non-string array (which also collects to empty) must
        // be rejected the same way.
        let empty = serde_json::json!({ "hosts": [], "index": "test" });
        assert!(
            ElasticsearchOutput::from_config(&empty, None).is_err(),
            "empty hosts array must be rejected at config time"
        );

        let non_strings = serde_json::json!({ "hosts": [9200], "index": "test" });
        assert!(
            ElasticsearchOutput::from_config(&non_strings, None).is_err(),
            "array of non-strings collects to empty and must be rejected"
        );
    }

    #[test]
    fn test_config_blank_hosts_rejected() {
        // `hosts => [""]` / `hosts => ""` / whitespace-only entries collect to an
        // empty vec after trimming and must be rejected at config time, exactly
        // like the empty-array case — otherwise a blank host could reach
        // `next_host()`.
        let empty_string = serde_json::json!({ "hosts": "", "index": "test" });
        assert!(
            ElasticsearchOutput::from_config(&empty_string, None).is_err(),
            "blank single host string must be rejected at config time"
        );

        let whitespace_string = serde_json::json!({ "hosts": "   ", "index": "test" });
        assert!(
            ElasticsearchOutput::from_config(&whitespace_string, None).is_err(),
            "whitespace-only single host string must be rejected at config time"
        );

        let blank_in_array = serde_json::json!({ "hosts": [""], "index": "test" });
        assert!(
            ElasticsearchOutput::from_config(&blank_in_array, None).is_err(),
            "array of only blank hosts must be rejected at config time"
        );

        let whitespace_in_array = serde_json::json!({ "hosts": ["  ", "\t"], "index": "test" });
        assert!(
            ElasticsearchOutput::from_config(&whitespace_in_array, None).is_err(),
            "array of only whitespace hosts must be rejected at config time"
        );
    }

    #[test]
    fn test_config_blank_hosts_dropped_keeping_valid() {
        // A mix of blank and valid hosts keeps only the valid (trimmed) ones.
        let settings = serde_json::json!({
            "hosts": ["", "  http://valid:9200  ", "\t"],
            "index": "test"
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.hosts, vec!["http://valid:9200"]);
    }

    #[tokio::test]
    async fn test_oversized_success_body_is_treated_as_failure() {
        // Regression: a *successful* (200) bulk response with a body larger than
        // the cap must be treated as a failure (retry/error), never buffered
        // unbounded (OOM). The production cap is 256 MB; we drive the over-limit
        // path with a small cap via `output_capped` so the test stays cheap.
        //
        // Use a 64 KiB *valid-JSON* 200 body (`{"errors":false,...}`) — well over
        // the 4 KiB test cap. The body is valid JSON, so the failure must come
        // from the size cap, not from a parse error.
        let filler = "F".repeat(64 * 1024);
        let huge_json = format!(r#"{{"errors":false,"filler":"{filler}"}}"#);
        let app = Router::new().route(
            "/_bulk",
            post(move || {
                let body = huge_json.clone();
                async move { (axum::http::StatusCode::OK, body) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let settings = serde_json::json!({
            "hosts": [format!("http://{addr}")],
            "index": "test",
            // No retries so the test resolves quickly: the single attempt's
            // over-limit 200 body must yield an error, not a parsed success.
            "retry_max_interval": 0,
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");

        // 4 KiB cap: the 64 KiB 200 body exceeds it, so `read_capped_body` errs
        // and the bulk is treated as failed rather than buffered/parsed.
        let result = output
            .output_capped(vec![Event::new("hello")], 4 * 1024)
            .await;

        assert!(
            result.is_err(),
            "an over-limit 200 bulk body must be treated as a failure, not parsed"
        );
    }

    #[tokio::test]
    async fn test_200_non_bulk_json_body_is_treated_as_failure() {
        // Round-15 HIGH regression: a 2xx response that is NOT a valid bulk
        // response (e.g. a proxy/auth-gateway/WAF returns 200 with `{"ok":true}`)
        // must be treated as a FAILURE, not silently recorded as delivered. The
        // body parses as JSON but lacks the `errors`/`items` fields.
        let app = Router::new().route(
            "/_bulk",
            post(|| async { Json(serde_json::json!({ "ok": true })) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let settings = serde_json::json!({
            "hosts": [format!("http://{addr}")],
            "index": "test",
            // No retries so the single attempt's non-bulk 200 resolves to Err fast.
            "retry_max_interval": 0,
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("hello")]).await;

        assert!(
            result.is_err(),
            "a 2xx response missing errors/items must be treated as a failure, not delivered"
        );
    }

    #[tokio::test]
    async fn test_200_html_body_is_treated_as_failure() {
        // A misrouted host / WAF returning 200 with an HTML body (not JSON at
        // all) must also be treated as a failure rather than delivered.
        let app = Router::new().route(
            "/_bulk",
            post(|| async {
                (
                    axum::http::StatusCode::OK,
                    "<html><body>Forbidden by gateway</body></html>",
                )
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let settings = serde_json::json!({
            "hosts": [format!("http://{addr}")],
            "index": "test",
            "retry_max_interval": 0,
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("hello")]).await;

        assert!(
            result.is_err(),
            "a 2xx response with an HTML (non-JSON) body must be treated as a failure"
        );
    }

    #[tokio::test]
    async fn test_200_bulk_item_count_mismatch_is_treated_as_failure() {
        // A well-formed-looking bulk 200 whose `items` count does NOT match the
        // number of events sent must be treated as a failure: the bulk response
        // must acknowledge exactly the events submitted.
        let app = Router::new().route(
            "/_bulk",
            post(|| async {
                // Two events will be sent, but only one item is acknowledged.
                Json(serde_json::json!({
                    "errors": false,
                    "items": [{ "index": { "status": 201 } }]
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
            "index": "test",
            "retry_max_interval": 0,
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let result = output
            .output(vec![Event::new("one"), Event::new("two")])
            .await;

        assert!(
            result.is_err(),
            "a bulk 200 whose items.len() != events.len() must be treated as a failure"
        );
    }

    #[tokio::test]
    async fn test_200_well_formed_bulk_matching_items_succeeds() {
        // A well-formed bulk 200 with `errors:false` and `items.len()` equal to
        // the number of events sent must still succeed — the strict validation
        // only rejects non-bulk / mismatched responses, never the happy path.
        let app = Router::new().route(
            "/_bulk",
            post(|| async {
                Json(serde_json::json!({
                    "errors": false,
                    "items": [
                        { "index": { "status": 201 } },
                        { "index": { "status": 201 } }
                    ]
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
            "index": "test",
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let result = output
            .output(vec![Event::new("one"), Event::new("two")])
            .await;

        assert!(
            result.is_ok(),
            "a well-formed bulk 200 with matching item count must succeed: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_within_cap_success_body_still_parsed() {
        // Sanity companion to the over-limit test: a *within-cap* 200 body with
        // no item-level errors still succeeds through the same capped path, so
        // the cap only rejects oversize bodies and never breaks the happy path.
        // One event is sent below, so the bulk response must acknowledge
        // exactly one item (the strict round-15 validation requires
        // `items.len() == events.len()`).
        let app = Router::new().route(
            "/_bulk",
            post(|| async {
                Json(serde_json::json!({
                    "errors": false,
                    "items": [{ "index": { "status": 201 } }]
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

        let result = output
            .output_capped(vec![Event::new("hello")], 4 * 1024)
            .await;

        assert!(
            result.is_ok(),
            "a within-cap, error-free 200 body must still succeed: {result:?}"
        );
    }

    #[test]
    fn test_build_bulk_body_update_wraps_source_in_doc() {
        // Round-16 regression: `action => "update"` must emit a partial-update
        // source line `{"doc": {...}}`, NOT the raw event JSON. Sending the raw
        // document is rejected per-item by ES ("script or doc is missing"), so
        // every batch would fail/retry/DLQ. The update body must carry the event
        // under `doc` (and must NOT be the bare event object).
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test-index",
            "action": "update",
            "document_id": "%{id}",
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello");
        event.set("id", EventValue::String("doc1".into()));
        let body = output.build_bulk_body(&[event]);

        // Two lines: action metadata + source. The source line is the 2nd.
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "update emits an action + a source line: {body}");
        assert!(lines[0].contains(r#""update""#), "action line must be update: {body}");

        let source: serde_json::Value =
            serde_json::from_str(lines[1]).expect("source line must be valid JSON");
        // The source MUST be `{"doc": {...}}` — i.e. the top-level object has a
        // `doc` key wrapping the event, not the raw event fields at top level.
        let doc = source
            .get("doc")
            .and_then(serde_json::Value::as_object)
            .expect("update source line must wrap the event under `doc`");
        // The wrapped doc carries the actual event payload (the `message`/`id`),
        // proving it's the event body and not an empty/sentinel object.
        assert!(
            doc.contains_key("message") || doc.contains_key("id"),
            "the wrapped doc must contain the event fields: {body}"
        );
        // The raw event must NOT appear at the top level (would be the bug).
        assert!(
            source.get("message").is_none(),
            "the raw event must not be at the top level (only under `doc`): {body}"
        );
        // doc_as_upsert defaults off, so it must be absent unless opted in.
        assert!(
            source.get("doc_as_upsert").is_none(),
            "doc_as_upsert must be absent by default: {body}"
        );
    }

    #[test]
    fn test_build_bulk_body_update_doc_as_upsert_opt_in() {
        // With `doc_as_upsert => true`, the update source line carries both the
        // partial `doc` and `"doc_as_upsert": true` so a missing document is
        // inserted instead of failing.
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test-index",
            "action": "update",
            "document_id": "%{id}",
            "doc_as_upsert": true,
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello");
        event.set("id", EventValue::String("doc1".into()));
        let body = output.build_bulk_body(&[event]);

        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "update emits an action + a source line: {body}");
        let source: serde_json::Value =
            serde_json::from_str(lines[1]).expect("source line must be valid JSON");
        assert!(
            source.get("doc").and_then(serde_json::Value::as_object).is_some(),
            "update source must still wrap the event under `doc`: {body}"
        );
        assert_eq!(
            source.get("doc_as_upsert").and_then(serde_json::Value::as_bool),
            Some(true),
            "doc_as_upsert => true must emit `\"doc_as_upsert\": true`: {body}"
        );
    }

    #[test]
    fn test_config_update_without_document_id_rejected() {
        // An `action => "update"` config with NO `document_id` source can never
        // work (a bulk update addresses a document by `_id`), so it must be
        // rejected at config time rather than failing every batch at runtime.
        let no_id = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test",
            "action": "update",
        });
        assert!(
            ElasticsearchOutput::from_config(&no_id, None).is_err(),
            "update action without document_id must be rejected at config time"
        );

        // A dynamic `%{field}` document_id is fine — resolved per event.
        let with_id = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test",
            "action": "update",
            "document_id": "%{id}",
        });
        assert!(
            ElasticsearchOutput::from_config(&with_id, None).is_ok(),
            "update action with a document_id must be accepted"
        );
    }

    #[test]
    fn test_build_bulk_body_index_still_sends_raw_doc() {
        // Guard: index/create must be unchanged — the source line is the raw
        // event document, NOT wrapped under `doc`.
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "test-index",
            "action": "index",
        });
        let output = ElasticsearchOutput::from_config(&settings, None).expect("config");
        let body = output.build_bulk_body(&[Event::new("hello")]);
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "index emits an action + a source line: {body}");
        let source: serde_json::Value =
            serde_json::from_str(lines[1]).expect("source line must be valid JSON");
        // The raw event (message) is at the top level; there is no `doc` wrapper.
        assert!(
            source.get("message").is_some(),
            "index source must be the raw event document: {body}"
        );
        assert!(
            source.get("doc").is_none(),
            "index source must NOT wrap the event under `doc`: {body}"
        );
    }
}
