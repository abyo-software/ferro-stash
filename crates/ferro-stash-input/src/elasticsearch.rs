// SPDX-License-Identifier: Apache-2.0
//! Elasticsearch input plugin — reads events from Elasticsearch using `search_after` + PIT.
//!
//! Logstash 9.3 compatible features:
//! - `search_after` pagination with Point-in-Time (PIT)
//! - Field value tracking (persisted to disk for resume)
//! - ES|QL query support (tech preview)
//! - Scheduled polling

use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_core::bounded_snippet;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use reqwest::Client;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[allow(dead_code)]
pub struct ElasticsearchInput {
    hosts: Vec<String>,
    index: String,
    query: serde_json::Value,
    esql_query: Option<String>,
    username: Option<String>,
    password: Option<String>,
    api_key: Option<String>,
    schedule: Option<String>,
    scroll_size: usize,
    tracking_field: Option<String>,
    last_run_metadata_path: Option<String>,
    client: Client,
    tags: Vec<String>,
}

// Manual Debug to avoid leaking `password` / `api_key` into logs / error context.
impl std::fmt::Debug for ElasticsearchInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticsearchInput")
            .field("hosts", &self.hosts)
            .field("index", &self.index)
            .field("query", &self.query)
            .field("esql_query", &self.esql_query)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("api_key", &self.api_key.as_ref().map(|_| "***"))
            .field("schedule", &self.schedule)
            .field("scroll_size", &self.scroll_size)
            .field("tracking_field", &self.tracking_field)
            .field("last_run_metadata_path", &self.last_run_metadata_path)
            .field("client", &self.client)
            .field("tags", &self.tags)
            .finish()
    }
}

impl ElasticsearchInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let hosts: Vec<String> = settings
            .get("hosts")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["http://localhost:9200".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );
        // A configured-but-empty `hosts` array (or an array of non-strings) must
        // be rejected here. `next_host()` indexes `self.hosts[0]`, so an empty
        // vector would panic on the first poll. The localhost default only
        // applies when the key is ABSENT (handled by `map_or_else` above); once
        // the key is present we honor it strictly and fail loudly rather than
        // silently substituting a default. Mirrors the kafka input's
        // `bootstrap_servers must contain at least one broker` check.
        if hosts.is_empty() {
            return Err(FerroStashError::Input {
                plugin: "elasticsearch".to_string(),
                message: "hosts must contain at least one URL string".to_string(),
            });
        }
        let index = settings
            .get("index")
            .and_then(|v| v.as_str())
            .unwrap_or("*")
            .to_string();
        let query = settings
            .get("query")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"match_all": {}}));
        let esql_query = settings
            .get("esql")
            .or_else(|| {
                settings
                    .get("query_type")
                    .filter(|v| v.as_str() == Some("esql"))
                    .and(settings.get("query"))
            })
            .and_then(|v| v.as_str())
            .map(String::from);
        let username = settings
            .get("user")
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
        let schedule = settings
            .get("schedule")
            .and_then(|v| v.as_str())
            .map(String::from);
        // Validate the schedule at config time so an obviously-degenerate value
        // (e.g. "0" or "*/0 * * * *", which parse to a zero interval) fails
        // loudly at startup rather than panicking `tokio::time::interval` on the
        // first scheduled tick. `run()` additionally clamps as a belt-and-braces
        // safeguard. (DD round-6 Finding #2.)
        if let Some(ref sched) = schedule {
            validate_schedule(sched)?;
        }
        let scroll_size = settings
            .get("size")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(1000) as usize;
        let tracking_field = settings
            .get("tracking_field")
            .and_then(|v| v.as_str())
            .map(String::from);
        let last_run_metadata_path = settings
            .get("last_run_metadata_path")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let timeout = settings
            .get("timeout")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(60);
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .map_err(|e| FerroStashError::Input {
                plugin: "elasticsearch".to_string(),
                message: format!("HTTP client error: {e}"),
            })?;

        Ok(Self {
            hosts,
            index,
            query,
            esql_query,
            username,
            password,
            api_key,
            schedule,
            scroll_size,
            tracking_field,
            last_run_metadata_path,
            client,
            tags,
        })
    }

    fn next_host(&self) -> &str {
        &self.hosts[0]
    }

    fn load_last_value(&self) -> Option<String> {
        self.last_run_metadata_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn save_last_value(&self, value: &str) {
        if let Some(ref path) = self.last_run_metadata_path {
            if let Some(parent) = std::path::Path::new(path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(path, value) {
                warn!(error = %e, "failed to save last_run_metadata");
            }
        }
    }

    async fn build_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = request;
        if let Some(ref api_key) = self.api_key {
            req = req.header("Authorization", format!("ApiKey {api_key}"));
        } else if let (Some(ref user), Some(ref pass)) = (&self.username, &self.password) {
            req = req.basic_auth(user, Some(pass));
        }
        req
    }

    async fn run_search(&self, sender: &mpsc::Sender<Event>) -> Result<()> {
        let host = self.next_host();

        // If ES|QL query is provided, use that
        if let Some(ref esql) = self.esql_query {
            return self.run_esql(host, esql, sender).await;
        }

        // Standard DSL search with search_after + PIT
        let last_value = self.load_last_value();

        // Build query with tracking field filter
        let mut query = self.query.clone();
        if let (Some(ref field), Some(ref value)) = (&self.tracking_field, &last_value) {
            query = serde_json::json!({
                "bool": {
                    "must": [self.query],
                    "filter": [{"range": {field: {"gt": value}}}]
                }
            });
        }

        // Open Point-in-Time (PIT) for consistent pagination
        let pit_url = format!(
            "{}/{}/_pit?keep_alive=5m",
            host.trim_end_matches('/'),
            self.index
        );
        let pit_req = self.build_request(self.client.post(&pit_url)).await;
        let mut pit_id = match pit_req.send().await {
            Ok(resp) => resp
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|v| v["id"].as_str().map(String::from)),
            Err(e) => {
                warn!(error = %e, "failed to open PIT, falling back to plain search");
                None
            }
        };

        let sort = if let Some(ref field) = self.tracking_field {
            serde_json::json!([{field: "asc"}, {"_id": "asc"}])
        } else {
            serde_json::json!([{"_id": "asc"}])
        };

        // Use /_search (with PIT, no index in URL)
        let url = if pit_id.is_some() {
            format!("{}/_search", host.trim_end_matches('/'))
        } else {
            format!("{}/{}/_search", host.trim_end_matches('/'), self.index)
        };
        let mut search_after: Option<serde_json::Value> = None;
        let mut last_tracking_value: Option<String> = None;
        let mut total_docs = 0usize;

        loop {
            let mut body = serde_json::json!({
                "query": query,
                "size": self.scroll_size,
                "sort": sort,
            });

            // Include PIT if available
            if let Some(ref pit) = pit_id {
                body["pit"] = serde_json::json!({
                    "id": pit,
                    "keep_alive": "5m"
                });
            }

            if let Some(ref sa) = search_after {
                body["search_after"] = sa.clone();
            }

            let req = self.client.post(&url).json(&body);
            let req = self.build_request(req).await;

            let response = req.send().await.map_err(|e| FerroStashError::Input {
                plugin: "elasticsearch".to_string(),
                message: format!("search error: {e}"),
            })?;

            // An ES error response (401/403/404/429/5xx) is still valid JSON but
            // has no `hits.hits`. Without this check the loop would parse it,
            // find an empty hit list, break, and return Ok(())  silently
            // ingesting nothing while masking an auth/index/server failure
            // (in scheduled mode the failure is never even logged). Mirror the
            // `if !status.is_success()` check used by the ES OUTPUT/FILTER
            // plugins and fail loudly. (DD round-6 Finding #1.)
            let status = response.status();
            if !status.is_success() {
                return Err(es_status_error("search", status, response).await);
            }

            let result: serde_json::Value =
                response.json().await.map_err(|e| FerroStashError::Input {
                    plugin: "elasticsearch".to_string(),
                    message: format!("response parse error: {e}"),
                })?;

            // Update PIT id if returned (ES can rotate it between pages)
            if let Some(new_pit) = result["pit_id"].as_str() {
                pit_id = Some(new_pit.to_string());
            }

            let hits = result["hits"]["hits"]
                .as_array()
                .cloned()
                .unwrap_or_default();

            if hits.is_empty() {
                break;
            }

            for hit in &hits {
                let source = hit
                    .get("_source")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let mut event = Event::from_json(source);

                // Set metadata
                if let Some(index) = hit.get("_index").and_then(|v| v.as_str()) {
                    event.set("_index", EventValue::String(index.to_string()));
                }
                if let Some(id) = hit.get("_id").and_then(|v| v.as_str()) {
                    event.set("_id", EventValue::String(id.to_string()));
                }
                for tag in &self.tags {
                    event.add_tag(tag);
                }

                // Track last value
                if let Some(ref field) = self.tracking_field {
                    if let Some(val) = hit.get("_source").and_then(|s| s.get(field)) {
                        last_tracking_value = Some(val.to_string().trim_matches('"').to_string());
                    }
                }

                if sender.send(event).await.is_err() {
                    return Ok(());
                }
                total_docs += 1;
            }

            // Get search_after from last hit's sort
            if let Some(last_hit) = hits.last() {
                search_after = last_hit.get("sort").cloned();
            }

            if hits.len() < self.scroll_size {
                break; // no more pages
            }
        }

        // Close PIT
        if let Some(ref pit) = pit_id {
            let close_url = format!("{}/_pit", host.trim_end_matches('/'));
            let close_req = self
                .build_request(
                    self.client
                        .delete(&close_url)
                        .json(&serde_json::json!({"id": pit})),
                )
                .await;
            if let Err(e) = close_req.send().await {
                debug!(error = %e, "failed to close PIT (non-fatal)");
            }
        }

        // Save tracking value
        if let Some(ref value) = last_tracking_value {
            self.save_last_value(value);
        }

        debug!(docs = total_docs, "elasticsearch search completed");
        Ok(())
    }

    async fn run_esql(
        &self,
        host: &str,
        esql_query: &str,
        sender: &mpsc::Sender<Event>,
    ) -> Result<()> {
        let url = format!("{}/_query", host.trim_end_matches('/'));
        let body = serde_json::json!({
            "query": esql_query,
        });

        let req = self.client.post(&url).json(&body);
        let req = self.build_request(req).await;

        let response = req.send().await.map_err(|e| FerroStashError::Input {
            plugin: "elasticsearch".to_string(),
            message: format!("ES|QL error: {e}"),
        })?;

        // Same hazard as `run_search`: an ES error response is valid JSON with
        // no `values`/`columns`, so without this check the query would yield
        // zero rows and return Ok(()), masking the failure. (DD round-6
        // Finding #1.)
        let status = response.status();
        if !status.is_success() {
            return Err(es_status_error("ES|QL", status, response).await);
        }

        let result: serde_json::Value =
            response.json().await.map_err(|e| FerroStashError::Input {
                plugin: "elasticsearch".to_string(),
                message: format!("ES|QL response parse error: {e}"),
            })?;

        // ES|QL returns { "columns": [...], "values": [[...], ...] }
        let columns: Vec<String> = result["columns"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|c| c["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let rows = result["values"].as_array().cloned().unwrap_or_default();
        let mut count = 0usize;

        for row in rows {
            let values = row.as_array().cloned().unwrap_or_default();
            let mut event = Event::empty();
            for (i, col) in columns.iter().enumerate() {
                if let Some(val) = values.get(i) {
                    event.set(col.clone(), EventValue::from(val.clone()));
                }
            }
            for tag in &self.tags {
                event.add_tag(tag);
            }
            if sender.send(event).await.is_err() {
                return Ok(());
            }
            count += 1;
        }

        debug!(rows = count, "ES|QL query completed");
        Ok(())
    }
}

/// Maximum number of bytes of a non-success ES response body included in the
/// error message. Bounds the snippet so a large/hostile error body cannot
/// produce an unbounded log line, while still aiding debugging.
const ERROR_BODY_SNIPPET_LIMIT: usize = 512;

/// Build a loud `Input` error from a non-success Elasticsearch HTTP response.
///
/// Includes the status code and a bounded (`ERROR_BODY_SNIPPET_LIMIT`-byte)
/// snippet of the response body. Mirrors the `if !status.is_success()` handling
/// in the ES OUTPUT/FILTER plugins so an auth/index/server failure surfaces as
/// an error instead of being silently treated as an empty successful search.
async fn es_status_error(
    op: &str,
    status: reqwest::StatusCode,
    response: reqwest::Response,
) -> FerroStashError {
    // `text()` consumes the body; on a transport error mid-read fall back to an
    // empty snippet rather than masking the (more important) status code.
    let body = response.text().await.unwrap_or_default();
    let snippet = bounded_snippet(&body, ERROR_BODY_SNIPPET_LIMIT);
    FerroStashError::Input {
        plugin: "elasticsearch".to_string(),
        message: format!("{op} request failed with HTTP status {status}: {snippet}"),
    }
}

/// Parse a schedule string into a polling interval in seconds, clamped to a
/// minimum of 1.
///
/// Accepts cron `*/N * * * *` (every N minutes) or a plain seconds count.
/// Anything unparseable falls back to the 300s (5 minute) default.
///
/// The `.max(1)` clamp is load-bearing: `tokio::time::interval` PANICS on a
/// zero `Duration`, and a zero interval is reachable via `schedule => "0"`
/// (parses to 0) or `schedule => "*/0 * * * *"` (`*/0` → 0). Clamping here
/// (and validating in `from_config`) keeps a degenerate schedule from
/// panicking the input task on its first scheduled tick. (DD round-6
/// Finding #2.)
fn schedule_interval_secs(schedule: &str) -> u64 {
    let s = schedule.trim();
    let parsed = if let Some(rest) = s.strip_prefix("*/") {
        // Cron format: */N * * * * → every N minutes
        let num_str = rest.split_whitespace().next().unwrap_or(rest);
        num_str.parse::<u64>().ok().map(|n| n.saturating_mul(60))
    } else {
        // Try plain seconds
        s.parse::<u64>().ok()
    };
    parsed.unwrap_or(300).max(1)
}

/// Reject an obviously-degenerate schedule at config time.
///
/// A schedule that *parses* to a zero interval (`"0"`, `"*/0 * * * *"`) is
/// always a configuration error: it would request "poll every 0 seconds",
/// which is meaningless and (absent the `run()` clamp) panics
/// `tokio::time::interval`. We fail loudly here so the operator learns at
/// startup. Unparseable / non-`*/` strings are NOT rejected — they fall back
/// to the 300s default in `schedule_interval_secs`, preserving prior behavior.
fn validate_schedule(schedule: &str) -> Result<()> {
    let s = schedule.trim();
    let parsed_zero = if let Some(rest) = s.strip_prefix("*/") {
        let num_str = rest.split_whitespace().next().unwrap_or(rest);
        num_str.parse::<u64>().ok() == Some(0)
    } else {
        s.parse::<u64>().ok() == Some(0)
    };
    if parsed_zero {
        return Err(FerroStashError::Input {
            plugin: "elasticsearch".to_string(),
            message: format!(
                "schedule {schedule:?} resolves to a zero polling interval; \
                 use a positive number of seconds or a cron `*/N * * * *` with N >= 1"
            ),
        });
    }
    Ok(())
}

#[async_trait]
impl InputPlugin for ElasticsearchInput {
    fn name(&self) -> &'static str {
        "elasticsearch"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        info!(hosts = ?self.hosts, index = %self.index, "elasticsearch input starting");

        // If no schedule, run once
        if self.schedule.is_none() {
            self.run_search(&sender).await?;
            return Ok(());
        }

        // Scheduled polling: cron `*/N * * * *` means every N minutes.
        // `schedule_interval_secs` clamps the result to a minimum of 1 so a
        // degenerate schedule (e.g. "0" or "*/0 * * * *") cannot pass a zero
        // `Duration` to `tokio::time::interval`, which would PANIC the task on
        // its first tick. (DD round-6 Finding #2.)
        let interval_secs = self
            .schedule
            .as_deref()
            .map_or(300, schedule_interval_secs);

        let mut timer = tokio::time::interval(Duration::from_secs(interval_secs));

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if let Err(e) = self.run_search(&sender).await {
                        warn!(error = %e, "elasticsearch input search error");
                    }
                }
                () = shutdown.wait() => break,
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_es_input_config_defaults() {
        let settings = serde_json::json!({});
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.hosts, vec!["http://localhost:9200"]);
        assert_eq!(input.index, "*");
        assert_eq!(input.scroll_size, 1000);
    }

    #[test]
    fn test_es_input_config_custom() {
        let settings = serde_json::json!({
            "hosts": ["http://es1:9200", "http://es2:9200"],
            "index": "my-index",
            "user": "admin",
            "password": "secret",
            "size": 500,
            "tags": ["es_input"]
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.hosts.len(), 2);
        assert_eq!(input.index, "my-index");
        assert_eq!(input.username, Some("admin".to_string()));
        assert_eq!(input.scroll_size, 500);
    }

    #[test]
    fn test_es_input_config_api_key() {
        let settings = serde_json::json!({ "api_key": "my_api_key" });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.api_key, Some("my_api_key".to_string()));
    }

    #[test]
    fn test_es_input_debug_redacts_password_and_api_key() {
        let settings = serde_json::json!({
            "index": "my-index",
            "user": "admin",
            "password": "hunter2-secret",
            "api_key": "topsecret-api-key",
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        let dbg = format!("{input:?}");
        assert!(
            !dbg.contains("hunter2-secret"),
            "password leaked in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("topsecret-api-key"),
            "api_key leaked in Debug: {dbg}"
        );
        assert!(dbg.contains("***"), "expected redaction marker in: {dbg}");
        assert!(dbg.contains("my-index"), "expected index in: {dbg}");
    }

    #[test]
    fn test_es_input_config_schedule() {
        let settings = serde_json::json!({ "schedule": "*/5 * * * *" });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.schedule, Some("*/5 * * * *".to_string()));
    }

    #[test]
    fn test_es_input_config_tracking() {
        let settings = serde_json::json!({
            "tracking_field": "@timestamp",
            "last_run_metadata_path": "/tmp/es_last_run"
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.tracking_field, Some("@timestamp".to_string()));
    }

    #[test]
    fn test_es_input_name() {
        let settings = serde_json::json!({});
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "elasticsearch");
    }

    #[test]
    fn test_es_input_next_host() {
        let settings = serde_json::json!({
            "hosts": ["http://host1:9200"]
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.next_host(), "http://host1:9200");
    }

    #[test]
    fn test_es_input_empty_hosts_array_rejected() {
        // Round-5 Finding #3: a configured-but-empty `hosts` array must be
        // rejected at config time. Previously this produced an empty
        // `self.hosts`, and `next_host()` (`&self.hosts[0]`) panicked on the
        // first poll. The localhost default applies only when the key is absent.
        let settings = serde_json::json!({ "hosts": [] });
        let err = ElasticsearchInput::from_config(&settings)
            .expect_err("empty hosts array must be rejected");
        assert!(
            matches!(err, FerroStashError::Input { ref plugin, .. } if plugin == "elasticsearch"),
            "expected elasticsearch Input error, got: {err:?}"
        );
    }

    #[test]
    fn test_es_input_hosts_array_of_non_strings_rejected() {
        // An array containing only non-string values collects to an empty
        // `hosts` vector and must likewise be rejected (not silently
        // defaulted), since the key is present.
        let settings = serde_json::json!({ "hosts": [123, true, null] });
        assert!(
            ElasticsearchInput::from_config(&settings).is_err(),
            "hosts array with no usable string entries must be rejected"
        );
    }

    #[test]
    fn test_es_input_esql_config() {
        let settings = serde_json::json!({
            "esql": "FROM logs | LIMIT 10"
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.esql_query, Some("FROM logs | LIMIT 10".to_string()));
    }

    // ---- DD round-6 Finding #2: zero-interval schedule must not panic ----

    #[test]
    fn test_schedule_interval_secs_clamps_zero_to_one() {
        // "0" → 0 seconds, "*/0 * * * *" → 0 minutes; both must clamp to >= 1
        // so `Duration::from_secs` / `tokio::time::interval` never see 0.
        assert_eq!(schedule_interval_secs("0"), 1);
        assert_eq!(schedule_interval_secs("*/0 * * * *"), 1);
        assert_eq!(schedule_interval_secs("*/0"), 1);
        // Whitespace variants exercise trim().
        assert_eq!(schedule_interval_secs("  0  "), 1);
    }

    #[test]
    fn test_schedule_interval_secs_normal_values() {
        assert_eq!(schedule_interval_secs("*/5 * * * *"), 300); // 5 minutes
        assert_eq!(schedule_interval_secs("30"), 30); // 30 seconds
        assert_eq!(schedule_interval_secs("*/1 * * * *"), 60); // 1 minute
        // Unparseable falls back to the 5-minute default.
        assert_eq!(schedule_interval_secs("not-a-schedule"), 300);
    }

    #[test]
    fn test_schedule_interval_secs_no_panic_on_construction() {
        // Reproduces the panic path directly: build a Duration from the clamped
        // value for a zero schedule. `tokio::time::interval` panics on a zero
        // period, so a non-zero Duration here proves the clamp protects it.
        let secs = schedule_interval_secs("*/0 * * * *");
        assert!(secs >= 1);
        let _dur = Duration::from_secs(secs); // would feed tokio::time::interval
    }

    #[test]
    fn test_es_input_zero_schedule_rejected_at_config() {
        // Loud-at-startup validation: a schedule that resolves to a zero
        // interval is a config error, not a first-tick panic.
        for sched in ["0", "*/0 * * * *", "*/0"] {
            let settings = serde_json::json!({ "schedule": sched });
            let err = ElasticsearchInput::from_config(&settings)
                .expect_err(&format!("schedule {sched:?} must be rejected at config time"));
            assert!(
                matches!(err, FerroStashError::Input { ref plugin, .. } if plugin == "elasticsearch"),
                "expected elasticsearch Input error for {sched:?}, got: {err:?}"
            );
        }
    }

    #[test]
    fn test_es_input_valid_schedule_accepted_at_config() {
        for sched in ["*/5 * * * *", "30", "*/1 * * * *"] {
            let settings = serde_json::json!({ "schedule": sched });
            assert!(
                ElasticsearchInput::from_config(&settings).is_ok(),
                "schedule {sched:?} should be accepted"
            );
        }
    }

    #[test]
    fn test_validate_schedule_rejects_zero() {
        assert!(validate_schedule("0").is_err());
        assert!(validate_schedule("*/0 * * * *").is_err());
        assert!(validate_schedule("*/0").is_err());
        assert!(validate_schedule("  0 ").is_err());
    }

    #[test]
    fn test_validate_schedule_accepts_nonzero_and_unparseable() {
        assert!(validate_schedule("*/5 * * * *").is_ok());
        assert!(validate_schedule("30").is_ok());
        // Unparseable strings fall back to the default and are not rejected.
        assert!(validate_schedule("garbage").is_ok());
    }

    // ---- DD round-6 Finding #1: non-2xx HTTP responses must error ----

    /// Spawn a one-shot mock HTTP server that replies with the given status
    /// line + JSON body to the first connection, then returns its address.
    async fn spawn_mock_es(status_line: &'static str, body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            // Accept a few connections (PIT open + search both hit the server).
            for _ in 0..4u8 {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                // Drain the request (best-effort; we don't parse it).
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf).await;
                let response = format!(
                    "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn test_run_search_errors_on_non_2xx() {
        // A 403 with a valid JSON error body must produce Err, NOT Ok with zero
        // events. Without the status check this would parse the body, find no
        // `hits.hits`, break, and return Ok(()) — silently ingesting nothing.
        let host = spawn_mock_es(
            "HTTP/1.1 403 Forbidden",
            r#"{"error":{"type":"security_exception","reason":"action denied"},"status":403}"#,
        )
        .await;
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "logs",
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(16);
        let result = input.run_search(&tx).await;
        assert!(
            result.is_err(),
            "non-2xx search must return Err, got Ok: {result:?}"
        );
        if let Err(FerroStashError::Input { plugin, message }) = result {
            assert_eq!(plugin, "elasticsearch");
            assert!(message.contains("403"), "status code missing: {message}");
            assert!(
                message.contains("security_exception"),
                "body snippet missing: {message}"
            );
        } else {
            panic!("expected Input error variant");
        }
        // No events were emitted.
        assert!(rx.try_recv().is_err(), "no events should be sent on error");
    }

    #[tokio::test]
    async fn test_run_esql_errors_on_non_2xx() {
        let host = spawn_mock_es(
            "HTTP/1.1 500 Internal Server Error",
            r#"{"error":{"type":"server_error","reason":"boom"},"status":500}"#,
        )
        .await;
        let settings = serde_json::json!({
            "hosts": [host],
            "esql": "FROM logs | LIMIT 10",
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(16);
        // run_esql is reached via run_search when esql_query is set.
        let result = input.run_search(&tx).await;
        assert!(
            result.is_err(),
            "non-2xx ES|QL must return Err, got Ok: {result:?}"
        );
        if let Err(FerroStashError::Input { plugin, message }) = result {
            assert_eq!(plugin, "elasticsearch");
            assert!(message.contains("500"), "status code missing: {message}");
            assert!(
                message.contains("server_error"),
                "body snippet missing: {message}"
            );
        } else {
            panic!("expected Input error variant");
        }
        assert!(rx.try_recv().is_err(), "no events should be sent on error");
    }
}
