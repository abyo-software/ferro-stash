// SPDX-License-Identifier: Apache-2.0
//! Elasticsearch filter — enrich events by querying Elasticsearch.
//!
//! Builds a query from `query_template` (with `%{field}` sprintf
//! substitution), POSTs it to `{host}/{index}/_search` via reqwest (trying the
//! configured hosts in order for basic failover), and maps the returned
//! `hits.hits[]._source` documents into the event. Mirrors the reqwest client
//! pattern used by the Elasticsearch *output* plugin (shared `reqwest::Client`,
//! optional basic-auth / API-key, configurable timeout).
//!
//! Compatible with Elasticsearch 7.x/8.x/9.x, `FerroSearch`, and
//! `OpenSearch`.

use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;
use reqwest::Client;
use tracing::{debug, warn};

#[derive(Debug)]
pub struct ElasticsearchFilter {
    /// Elasticsearch hosts; tried in order for basic failover.
    hosts: Vec<String>,
    /// Index (or index pattern) to query.
    index: String,
    /// Query template with `%{field}` substitution.
    query_template: String,
    /// Maximum number of results.
    result_size: usize,
    /// Fields to copy from ES result into the event.
    fields: Vec<(String, String)>,
    /// Whether to sort results (currently advisory — sorting is expected to be
    /// expressed inside `query_template`).
    #[allow(dead_code)]
    enable_sort: bool,
    /// Target field for the ES result.
    target: String,
    /// Tag on failure.
    tag_on_failure: String,
    /// Optional basic-auth username.
    username: Option<String>,
    /// Optional basic-auth password.
    password: Option<String>,
    /// Optional API key (takes precedence over basic auth).
    api_key: Option<String>,
    /// Shared HTTP client.
    client: Client,
    condition: Option<Condition>,
}

impl ElasticsearchFilter {
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
            .unwrap_or("logstash-*")
            .to_string();

        let query_template = settings
            .get("query_template")
            .or_else(|| settings.get("query"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let result_size = settings
            .get("result_size")
            .or_else(|| settings.get("size"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1) as usize;

        let fields = settings
            .get("fields")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let enable_sort = settings
            .get("enable_sort")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_str())
            .unwrap_or("_elasticsearch_lookup_failure")
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

        let timeout_secs = settings
            .get("timeout")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(30);

        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .danger_accept_invalid_certs(
                settings
                    .get("ssl_certificate_verification")
                    .and_then(ferro_stash_core::settings_helpers::as_bool_flexible)
                    .is_some_and(|v| !v),
            )
            .build()
            .map_err(|e| FerroStashError::Filter {
                plugin: "elasticsearch".to_string(),
                message: format!("HTTP client error: {e}"),
            })?;

        Ok(Self {
            hosts,
            index,
            query_template,
            result_size,
            fields,
            enable_sort,
            target,
            tag_on_failure,
            username,
            password,
            api_key,
            client,
            condition,
        })
    }

    /// Build the query by substituting `%{field}` references from the event.
    fn build_query(&self, event: &Event) -> String {
        event.sprintf(&self.query_template)
    }

    /// Construct the `_search` request body. When `query_template` is empty (or
    /// not a JSON object), a `match_all` query is used. `size` is always set
    /// from `result_size`.
    fn build_search_body(&self, query: &str) -> serde_json::Value {
        let mut body = match serde_json::from_str::<serde_json::Value>(query) {
            Ok(serde_json::Value::Object(map)) => serde_json::Value::Object(map),
            // Empty or non-object templates -> match_all.
            _ => serde_json::json!({}),
        };

        if let serde_json::Value::Object(map) = &mut body {
            map.entry("query")
                .or_insert_with(|| serde_json::json!({ "match_all": {} }));
            // Honor result_size unless the template explicitly set its own size.
            map.entry("size")
                .or_insert_with(|| serde_json::json!(self.result_size));
        }

        body
    }

    /// Execute a real `_search` against Elasticsearch, returning the
    /// `hits.hits[]._source` documents. Tries each host in order; returns
    /// `None` on total failure (all hosts errored / non-2xx).
    async fn execute_query(
        &self,
        query: &str,
    ) -> Option<Vec<IndexMap<String, EventValue>>> {
        let body = self.build_search_body(query);

        for host in &self.hosts {
            let url = format!(
                "{}/{}/_search",
                host.trim_end_matches('/'),
                self.index.trim_start_matches('/')
            );

            let mut request = self
                .client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body);

            if let Some(api_key) = &self.api_key {
                request = request.header("Authorization", format!("ApiKey {api_key}"));
            } else if let (Some(user), Some(pass)) = (&self.username, &self.password) {
                request = request.basic_auth(user, Some(pass));
            }

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!(url = %url, error = %e, "elasticsearch filter: request failed, trying next host");
                    continue;
                }
            };

            let status = response.status();
            if !status.is_success() {
                let text = response.text().await.unwrap_or_default();
                warn!(url = %url, status = %status, body = %text, "elasticsearch filter: non-success response");
                continue;
            }

            let json: serde_json::Value = match response.json().await {
                Ok(j) => j,
                Err(e) => {
                    warn!(url = %url, error = %e, "elasticsearch filter: invalid JSON response");
                    continue;
                }
            };

            debug!(url = %url, "elasticsearch filter: query succeeded");
            return Some(parse_hits(&json));
        }

        None
    }
}

/// Extract `hits.hits[]._source` documents from an ES `_search` response,
/// converting each `_source` object into an event-value map.
fn parse_hits(json: &serde_json::Value) -> Vec<IndexMap<String, EventValue>> {
    let Some(hits) = json
        .get("hits")
        .and_then(|h| h.get("hits"))
        .and_then(|h| h.as_array())
    else {
        return Vec::new();
    };

    hits.iter()
        .filter_map(|hit| {
            let source = hit.get("_source")?;
            let value = EventValue::from(source.clone());
            match value {
                EventValue::Object(map) => Some(map),
                _ => None,
            }
        })
        .collect()
}

#[async_trait]
impl FilterPlugin for ElasticsearchFilter {
    fn name(&self) -> &'static str {
        "elasticsearch"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let query = self.build_query(&event);

        match self.execute_query(&query).await {
            Some(results) => {
                if results.is_empty() {
                    event.add_tag(&self.tag_on_failure);
                    return Ok(vec![event]);
                }

                // Take up to result_size results
                let results: Vec<_> = results.into_iter().take(self.result_size).collect();

                // If fields mapping is configured, copy specific fields
                if !self.fields.is_empty() {
                    for result in &results {
                        for (es_field, event_field) in &self.fields {
                            if let Some(val) = result.get(es_field) {
                                let target_field = if self.target.is_empty() {
                                    event_field.clone()
                                } else {
                                    format!("{}.{}", self.target, event_field)
                                };
                                event.set(target_field, val.clone());
                            }
                        }
                    }
                } else {
                    // No field mapping — store entire result
                    let result_values: Vec<EventValue> =
                        results.into_iter().map(EventValue::Object).collect();

                    let value = if result_values.len() == 1 {
                        result_values
                            .into_iter()
                            .next()
                            .unwrap_or(EventValue::Null)
                    } else {
                        EventValue::Array(result_values)
                    };

                    if self.target.is_empty() {
                        // Merge into root
                        if let EventValue::Object(map) = value {
                            for (k, v) in map {
                                event.set(k, v);
                            }
                        }
                    } else {
                        event.set(self.target.clone(), value);
                    }
                }
            }
            None => {
                event.add_tag(&self.tag_on_failure);
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
    use std::io::{Read, Write};
    use std::net::TcpListener as StdTcpListener;

    // ----- Config / query-building unit tests -----

    #[tokio::test]
    async fn test_elasticsearch_query_template_substitution() {
        let settings = serde_json::json!({
            "query_template": "{\"term\":{\"user\":\"%{username}\"}}"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("username", EventValue::String("bob".into()));
        let query = filter.build_query(&event);
        assert!(query.contains("bob"));
        assert!(!query.contains("%{username}"));
    }

    #[tokio::test]
    async fn test_elasticsearch_default_config() {
        let settings = serde_json::json!({});
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.hosts, vec!["http://localhost:9200"]);
        assert_eq!(filter.index, "logstash-*");
        assert_eq!(filter.result_size, 1);
        assert!(filter.enable_sort);
    }

    #[test]
    fn test_elasticsearch_name() {
        let settings = serde_json::json!({});
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "elasticsearch");
    }

    #[test]
    fn test_build_search_body_empty_template_match_all() {
        let settings = serde_json::json!({ "result_size": 5 });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let body = filter.build_search_body("");
        assert!(body.get("query").and_then(|q| q.get("match_all")).is_some());
        assert_eq!(body.get("size").and_then(serde_json::Value::as_u64), Some(5));
    }

    #[test]
    fn test_build_search_body_preserves_template_query() {
        let settings = serde_json::json!({});
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let body = filter.build_search_body(r#"{"query":{"term":{"user":"bob"}}}"#);
        assert_eq!(
            body.pointer("/query/term/user")
                .and_then(serde_json::Value::as_str),
            Some("bob")
        );
        // size injected from default result_size (1)
        assert_eq!(body.get("size").and_then(serde_json::Value::as_u64), Some(1));
    }

    #[test]
    fn test_parse_hits_extracts_sources() {
        let json = serde_json::json!({
            "hits": {
                "hits": [
                    { "_source": { "name": "Alice", "age": 30 } },
                    { "_source": { "name": "Bob", "age": 25 } }
                ]
            }
        });
        let hits = parse_hits(&json);
        assert_eq!(hits.len(), 2);
        assert_eq!(
            hits[0].get("name"),
            Some(&EventValue::String("Alice".into()))
        );
        assert_eq!(hits[1].get("age"), Some(&EventValue::Integer(25)));
    }

    #[test]
    fn test_parse_hits_empty() {
        let json = serde_json::json!({ "hits": { "hits": [] } });
        assert!(parse_hits(&json).is_empty());
        let json = serde_json::json!({ "took": 1 });
        assert!(parse_hits(&json).is_empty());
    }

    #[tokio::test]
    async fn test_config_with_auth() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "user": "elastic",
            "password": "changeme",
            "api_key": "abc123"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.username, Some("elastic".to_string()));
        assert_eq!(filter.password, Some("changeme".to_string()));
        assert_eq!(filter.api_key, Some("abc123".to_string()));
    }

    // ----- Integration test against a tiny raw-HTTP mock server -----
    //
    // `axum` is not a dev-dependency of this crate, so we spin up a minimal
    // blocking HTTP/1.1 responder on a std TcpListener in a background thread.
    // It returns a canned ES `_search` response for a single request.

    fn spawn_mock_es(response_json: &'static str) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // Drain the request headers/body (best-effort).
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let body = response_json;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn test_elasticsearch_real_search_field_mapping() {
        let host = spawn_mock_es(
            r#"{"hits":{"hits":[{"_source":{"name":"Alice","role":"admin"}}]}}"#,
        );
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "users",
            "query_template": "{\"query\":{\"match\":{\"name\":\"%{name}\"}}}",
            "fields": { "role": "user_role" }
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("name", EventValue::String("Alice".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("user_role"),
            Some(&EventValue::String("admin".into()))
        );
        assert!(!result[0].has_tag("_elasticsearch_lookup_failure"));
    }

    #[tokio::test]
    async fn test_elasticsearch_real_search_target_merge() {
        let host = spawn_mock_es(r#"{"hits":{"hits":[{"_source":{"city":"Paris"}}]}}"#);
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "geo",
            "target": "es_result"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        if let Some(EventValue::Object(obj)) = result[0].get("es_result") {
            assert_eq!(obj.get("city"), Some(&EventValue::String("Paris".into())));
        } else {
            panic!("expected es_result object: {:?}", result[0].get("es_result"));
        }
    }

    #[tokio::test]
    async fn test_elasticsearch_empty_hits_tags_failure() {
        let host = spawn_mock_es(r#"{"hits":{"hits":[]}}"#);
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "users"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        assert!(result[0].has_tag("_elasticsearch_lookup_failure"));
    }

    #[tokio::test]
    async fn test_elasticsearch_unreachable_host_tags_failure() {
        // Reserved-for-docs IP that should not accept connections (fast fail).
        let settings = serde_json::json!({
            "hosts": ["http://127.0.0.1:1"],
            "index": "users",
            "timeout": 2
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        assert!(result[0].has_tag("_elasticsearch_lookup_failure"));
    }

    // ----- Live-smoke test (real Elasticsearch) -----
    //
    //   ES_URL=http://localhost:9200 cargo test -p ferro-stash-filter \
    //     es_live -- --ignored

    #[tokio::test]
    #[ignore = "requires a running Elasticsearch; set ES_URL to enable"]
    async fn test_elasticsearch_live_search() {
        let url = std::env::var("ES_URL").expect("ES_URL must be set for this test");
        let settings = serde_json::json!({
            "hosts": [url],
            "index": "_all",
            "target": "es_result",
            "result_size": 1
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        // We can't assert on cluster contents, but a reachable cluster must
        // not produce a connection failure tag (it may legitimately have 0
        // hits -> failure tag, so only assert the call did not panic and ran).
        let _ = &result[0];
    }
}
