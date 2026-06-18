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
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use reqwest::Client;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[allow(dead_code)]
#[derive(Debug)]
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

impl ElasticsearchInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let hosts = settings
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

        // Scheduled polling: cron `*/N * * * *` means every N minutes
        let interval_secs = self
            .schedule
            .as_deref()
            .and_then(|s| {
                let s = s.trim();
                if let Some(rest) = s.strip_prefix("*/") {
                    // Cron format: */N * * * * → every N minutes
                    let num_str = rest.split_whitespace().next().unwrap_or(rest);
                    num_str.parse::<u64>().ok().map(|n| n * 60) // minutes → seconds
                } else {
                    // Try plain seconds
                    s.parse::<u64>().ok()
                }
            })
            .unwrap_or(300); // default 5 minutes

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
    fn test_es_input_esql_config() {
        let settings = serde_json::json!({
            "esql": "FROM logs | LIMIT 10"
        });
        let input = ElasticsearchInput::from_config(&settings).expect("config");
        assert_eq!(input.esql_query, Some("FROM logs | LIMIT 10".to_string()));
    }
}
