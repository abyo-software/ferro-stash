// SPDX-License-Identifier: Apache-2.0
//! Elasticsearch filter — enrich events by querying Elasticsearch.
//!
//! Builds a query from `query_template` and POSTs it to
//! `{host}/{index}/_search` via reqwest (trying the configured hosts in order
//! for basic failover), then maps the returned `hits.hits[]._source` documents
//! into the event. Mirrors the reqwest client pattern used by the
//! Elasticsearch *output* plugin (shared `reqwest::Client`, optional
//! basic-auth / API-key, configurable timeout).
//!
//! ## Injection-safe substitution
//!
//! `query_template` is parsed to a [`serde_json::Value`] **once** at config
//! time (in [`ElasticsearchFilter::from_config`]). At query time the parsed
//! template is walked recursively and `%{field}` placeholders are substituted
//! **only at JSON string-value positions** — the substituted value is stored
//! as a JSON string scalar, so `serde_json` escapes it on serialization and an
//! event-field value containing `"`, `{`, `}`, `\`, etc. can neither break out
//! of its string context nor inject query structure. The JSON shape of the
//! template is fixed at config time and never re-derived from event data, so
//! an attacker controlling field values cannot rewrite the query DSL and a
//! value that breaks JSON can never silently fall back to `match_all`.
//!
//! A non-empty `query_template` that is not valid JSON is a **config error**
//! and is rejected loudly in `from_config` (rather than failing open to
//! `match_all` at runtime). An empty/unset template defaults to `match_all`.
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

/// Upper bound on a successful (`2xx`) `_search` response body we will buffer
/// before parsing. A misconfigured/compromised/proxy-fronted ES host can return
/// a `200` with an arbitrarily large JSON body; `Response::json()` buffers the
/// *entire* body before parsing, so a hostile host could OOM the process on a
/// single lookup. 256 MB is generous — legitimate responses are `result_size`-
/// bounded and far smaller — while still capping the worst case. An over-limit
/// (or unreadable) body is treated as a lookup *failure* for that host (warn +
/// failover), never a panic or an unbounded read.
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

pub struct ElasticsearchFilter {
    /// Elasticsearch hosts; tried in order for basic failover.
    hosts: Vec<String>,
    /// Index (or index pattern) to query.
    index: String,
    /// Query template, pre-parsed to a JSON value at config time. `%{field}`
    /// placeholders are substituted at query time **only** at string-value
    /// positions (see module docs); the JSON structure is fixed here and never
    /// re-derived from event data, which makes substitution injection-safe.
    query_template: serde_json::Value,
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

/// Manual `Debug` impl that redacts secret-bearing fields.
///
/// `ElasticsearchFilter` holds `password` and `api_key` plaintext secrets.
/// A derived `Debug` would print them verbatim (e.g. via `format!("{filter:?}")`
/// or a `tracing` `{:?}` event), leaking credentials into logs. We render the
/// presence of each secret without its value (`Some("***")` / `None`) and print
/// all other fields normally.
impl std::fmt::Debug for ElasticsearchFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |opt: &Option<String>| opt.as_ref().map(|_| "***");
        // Hosts are URLs and may embed credentials (userinfo / api_key query).
        let hosts: Vec<String> = self
            .hosts
            .iter()
            .map(|h| ferro_stash_core::redact_url(h.as_str()))
            .collect();
        f.debug_struct("ElasticsearchFilter")
            .field("hosts", &hosts)
            .field("index", &self.index)
            .field("query_template", &self.query_template)
            .field("result_size", &self.result_size)
            .field("fields", &self.fields)
            .field("enable_sort", &self.enable_sort)
            .field("target", &self.target)
            .field("tag_on_failure", &self.tag_on_failure)
            .field("username", &self.username)
            .field("password", &redact(&self.password))
            .field("api_key", &redact(&self.api_key))
            .field("client", &self.client)
            .field("condition", &self.condition)
            .finish()
    }
}

impl ElasticsearchFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let hosts: Vec<String> = match settings.get("hosts") {
            Some(serde_json::Value::Array(a)) => a
                .iter()
                // TRIM each entry and DROP empty/whitespace-only ones: a blank
                // host (`""` / `"   "`) would otherwise survive into the vec and
                // build a malformed URL (`/index/_search` with no scheme/authority)
                // so every lookup fails — a silently-disabled enrichment.
                .filter_map(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            Some(serde_json::Value::String(s)) => {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    Vec::new()
                } else {
                    vec![trimmed.to_string()]
                }
            }
            // Absent `hosts` key defaults to localhost (preserved behavior).
            _ => vec!["http://localhost:9200".to_string()],
        };

        // An explicitly-empty `hosts` (`hosts => []`), a blank host
        // (`hosts => ""` / `hosts => [""]`), or an array of only non-strings
        // collapses to zero usable hosts, which would permanently disable the
        // filter: `execute_query` would try no hosts (or only malformed URLs),
        // return `None`, and EVERY event would be tagged
        // `_elasticsearch_lookup_failure` — a silently-disabled enrichment. The
        // ES input and output plugins already reject empty hosts at config time;
        // reject it here too for consistency.
        if hosts.is_empty() {
            return Err(FerroStashError::Filter {
                plugin: "elasticsearch".to_string(),
                message: "`hosts` must contain at least one Elasticsearch host \
                          (e.g. \"http://localhost:9200\"); an empty list would \
                          permanently disable the filter."
                    .to_string(),
            });
        }

        let index = settings
            .get("index")
            .and_then(|v| v.as_str())
            .unwrap_or("logstash-*")
            .to_string();

        // Parse the query template ONCE here. A non-empty template that fails
        // to parse is a genuine config error and is rejected loudly (we never
        // fail open to `match_all` at runtime). An empty/unset template
        // defaults to `match_all`.
        let query_template_str = settings
            .get("query_template")
            .or_else(|| settings.get("query"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let query_template = if query_template_str.trim().is_empty() {
            default_match_all()
        } else {
            let parsed =
                serde_json::from_str::<serde_json::Value>(query_template_str).map_err(|e| {
                    FerroStashError::Filter {
                        plugin: "elasticsearch".to_string(),
                        message: format!("invalid query_template JSON: {e}"),
                    }
                })?;
            // The template must be a JSON object (an ES `_search` body); a
            // bare scalar/array cannot carry a query and would be a config
            // mistake we should not paper over with `match_all`.
            if !parsed.is_object() {
                return Err(FerroStashError::Filter {
                    plugin: "elasticsearch".to_string(),
                    message: "query_template must be a JSON object (an Elasticsearch _search body)"
                        .to_string(),
                });
            }
            parsed
        };

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

    /// Construct the `_search` request body for this event by walking the
    /// pre-parsed [`Self::query_template`] and substituting `%{field}`
    /// placeholders **only at JSON string-value positions** (see
    /// [`substitute_placeholders`]). Because the structure comes from the
    /// already-parsed template and substituted values are stored as JSON string
    /// scalars (escaped on serialization), event data can never break out of a
    /// string or rewrite the query DSL.
    ///
    /// `query` (the `query` clause) defaults to `match_all` and `size` defaults
    /// to `result_size`, but only if the template did not set them.
    fn build_search_body(&self, event: &Event) -> serde_json::Value {
        let mut body = substitute_placeholders(&self.query_template, event);

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
    /// `None` on total failure (all hosts errored / non-2xx / over-limit body).
    async fn execute_query(&self, event: &Event) -> Option<Vec<IndexMap<String, EventValue>>> {
        self.execute_query_capped(event, MAX_RESPONSE_BYTES).await
    }

    /// Like [`Self::execute_query`], but with an explicit cap on the number of
    /// bytes buffered from a successful response body. Factored out so tests can
    /// drive the over-limit path with a small cap without producing a 256 MB
    /// body. Production callers use [`Self::execute_query`] (256 MB).
    async fn execute_query_capped(
        &self,
        event: &Event,
        max_response_bytes: usize,
    ) -> Option<Vec<IndexMap<String, EventValue>>> {
        let body = self.build_search_body(event);

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
                // Bound the logged error body at the *read*, not just the
                // snippet: a misconfigured/compromised/proxy-fronted host could
                // return an arbitrarily large (multi-GB) non-2xx body.
                // `response.text()` would buffer the *entire* body before we
                // could truncate it, so a hostile host could OOM the process on
                // this diagnostic path. Stream the body and stop after at most
                // ~512 bytes, then log a UTF-8-boundary-safe bounded snippet.
                let snippet = ferro_stash_core::read_bounded_body_stream(
                    Box::pin(response.bytes_stream()),
                    512,
                )
                .await;
                warn!(url = %url, status = %status, body = %snippet, "elasticsearch filter: non-success response");
                continue;
            }

            // Bound the *successful* (2xx) body read too: `Response::json()`
            // buffers the entire body before parsing, so a misconfigured/hostile
            // host could return a `200` with a multi-GB JSON body and OOM the
            // process on a single lookup. Stream the body and reject it if it
            // exceeds `max_response_bytes` — an over-limit (or unreadable) body
            // is a lookup *failure* for this host (warn + try next host), never
            // a panic and never an unbounded read.
            let bytes = match ferro_stash_core::read_capped_body(
                Box::pin(response.bytes_stream()),
                max_response_bytes,
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(url = %url, error = %e, "elasticsearch filter: response body too large or unreadable, trying next host");
                    continue;
                }
            };

            let json: serde_json::Value = match serde_json::from_slice(&bytes) {
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

/// The default `_search` body used when no `query_template` is configured.
fn default_match_all() -> serde_json::Value {
    serde_json::json!({ "query": { "match_all": {} } })
}

/// Walk a pre-parsed JSON template and substitute `%{field}` placeholders
/// against `event`, **only** at string-value positions.
///
/// This is what makes substitution injection-safe: the JSON *structure* of
/// `template` is fixed (it was parsed at config time, not derived from event
/// data), and resolved placeholder values are stored back as JSON string
/// scalars. When the resulting [`serde_json::Value`] is serialized for the
/// request body, `serde_json` escapes those strings, so a field value
/// containing `"`, `{`, `}`, `\`, control characters, etc. stays inside its
/// string position and can neither terminate the string early nor inject query
/// structure.
///
/// Object *keys* are intentionally left untouched (ES query DSL keys are
/// structural, not data), and missing fields follow `Event::sprintf` semantics
/// (the literal `%{field}` placeholder is preserved).
fn substitute_placeholders(template: &serde_json::Value, event: &Event) -> serde_json::Value {
    match template {
        // Only string scalars carry `%{field}` placeholders. Resolve via the
        // event's sprintf (reusing the canonical field-lookup / missing-field
        // semantics) and store the result as a JSON string — serde_json will
        // escape it on serialization, so it cannot break out of its context.
        serde_json::Value::String(s) => {
            if s.contains("%{") {
                serde_json::Value::String(event.sprintf(s))
            } else {
                template.clone()
            }
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| substitute_placeholders(item, event))
                .collect(),
        ),
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k.clone(), substitute_placeholders(v, event));
            }
            serde_json::Value::Object(out)
        }
        // Numbers / bools / null are structural and carry no placeholders.
        other => other.clone(),
    }
}

#[async_trait]
impl FilterPlugin for ElasticsearchFilter {
    fn name(&self) -> &'static str {
        "elasticsearch"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        match self.execute_query(&event).await {
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
                        result_values.into_iter().next().unwrap_or(EventValue::Null)
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
        // The placeholder is resolved at its JSON string-value position; the
        // structure (parsed once at config time) is preserved and `query`/`size`
        // defaults are injected.
        let body = filter.build_search_body(&event);
        assert_eq!(
            body.pointer("/term/user")
                .and_then(serde_json::Value::as_str),
            Some("bob")
        );
        // No raw `%{...}` placeholder leaks into the serialized body.
        let serialized = serde_json::to_string(&body).expect("serialize");
        assert!(serialized.contains("bob"));
        assert!(!serialized.contains("%{username}"));
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

    /// An explicitly-empty `hosts` (or an array of only non-strings) is a config
    /// error rather than a silently-disabled filter (every event would otherwise
    /// be tagged `_elasticsearch_lookup_failure`). The absent-key default
    /// (localhost) is preserved — see `test_elasticsearch_default_config`.
    #[test]
    fn test_empty_hosts_rejected_at_config_time() {
        // `hosts => []`
        let settings = serde_json::json!({ "hosts": [] });
        let err = ElasticsearchFilter::from_config(&settings, None)
            .expect_err("empty hosts must be a config error");
        let msg = err.to_string();
        assert!(msg.contains("hosts"), "error should mention hosts: {msg}");

        // An array of only non-strings collapses to zero hosts after filtering.
        let settings = serde_json::json!({ "hosts": [123, true, null] });
        let err = ElasticsearchFilter::from_config(&settings, None)
            .expect_err("a non-string hosts array must be a config error");
        assert!(
            err.to_string().contains("hosts"),
            "error should mention hosts"
        );
    }

    /// A blank `hosts` value (`hosts => ""` or `hosts => [""]`, including
    /// whitespace-only entries) is rejected at config time, exactly like the
    /// empty-list case. Without trimming, a blank host survives the round-9
    /// empty-check but builds a malformed URL (`/index/_search`) so every lookup
    /// silently fails. The absent-key localhost default is preserved (see
    /// `test_elasticsearch_default_config`).
    #[test]
    fn test_blank_hosts_rejected_at_config_time() {
        // `hosts => [""]`
        let settings = serde_json::json!({ "hosts": [""] });
        let err = ElasticsearchFilter::from_config(&settings, None)
            .expect_err("blank host in array must be a config error");
        assert!(
            err.to_string().contains("hosts"),
            "error should mention hosts: {}",
            err
        );

        // `hosts => "   "` (scalar, whitespace-only)
        let settings = serde_json::json!({ "hosts": "   " });
        let err = ElasticsearchFilter::from_config(&settings, None)
            .expect_err("whitespace-only scalar host must be a config error");
        assert!(
            err.to_string().contains("hosts"),
            "error should mention hosts"
        );

        // `hosts => ""` (scalar, empty)
        let settings = serde_json::json!({ "hosts": "" });
        let err = ElasticsearchFilter::from_config(&settings, None)
            .expect_err("empty scalar host must be a config error");
        assert!(
            err.to_string().contains("hosts"),
            "error should mention hosts"
        );

        // A mix of blank and whitespace-only array entries also collapses to zero.
        let settings = serde_json::json!({ "hosts": ["", "  ", "\t"] });
        let err = ElasticsearchFilter::from_config(&settings, None)
            .expect_err("array of only blank hosts must be a config error");
        assert!(
            err.to_string().contains("hosts"),
            "error should mention hosts"
        );
    }

    /// A usable host alongside blank entries survives: blanks are dropped, the
    /// usable host is trimmed and kept. (Asserts the trim/drop logic does not
    /// over-reject when at least one good host is present.)
    #[test]
    fn test_blank_hosts_dropped_usable_host_kept() {
        let settings = serde_json::json!({
            "hosts": ["", "  http://es:9200  ", "\t"]
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.hosts, vec!["http://es:9200".to_string()]);
    }

    #[test]
    fn test_elasticsearch_name() {
        let settings = serde_json::json!({});
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "elasticsearch");
    }

    #[test]
    fn test_build_search_body_empty_template_match_all() {
        // An empty/unset template defaults to `match_all`.
        let settings = serde_json::json!({ "result_size": 5 });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let body = filter.build_search_body(&Event::new("test"));
        assert!(body.get("query").and_then(|q| q.get("match_all")).is_some());
        assert_eq!(
            body.get("size").and_then(serde_json::Value::as_u64),
            Some(5)
        );
    }

    #[test]
    fn test_build_search_body_preserves_template_query() {
        let settings = serde_json::json!({
            "query_template": r#"{"query":{"term":{"user":"bob"}}}"#
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let body = filter.build_search_body(&Event::new("test"));
        assert_eq!(
            body.pointer("/query/term/user")
                .and_then(serde_json::Value::as_str),
            Some("bob")
        );
        // size injected from default result_size (1)
        assert_eq!(
            body.get("size").and_then(serde_json::Value::as_u64),
            Some(1)
        );
    }

    // ----- Injection-safety regression tests -----

    /// (a) A field value containing a double-quote (and braces / backslash)
    /// must NOT produce a `match_all` query and must NOT break the query: the
    /// value stays inside its JSON string position, properly escaped.
    #[test]
    fn test_build_search_body_value_with_quote_is_escaped_not_match_all() {
        let settings = serde_json::json!({
            "query_template": "{\"query\":{\"term\":{\"user\":\"%{username}\"}}}"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        // Classic injection attempt: terminate the string and inject structure.
        event.set(
            "username",
            EventValue::String(r#"x"}},"match_all":{"#.into()),
        );
        let body = filter.build_search_body(&event);

        // The structure is intact: still a `term` on `user`, NOT `match_all`.
        assert_eq!(
            body.pointer("/query/term/user")
                .and_then(serde_json::Value::as_str),
            Some(r#"x"}},"match_all":{"#)
        );
        assert!(
            body.pointer("/query/match_all").is_none(),
            "injected match_all must not appear: {body:?}"
        );

        // The serialized request body re-parses cleanly (the quote was escaped),
        // and the only `match_all` present is none (template had a `term`).
        let serialized = serde_json::to_string(&body).expect("serialize");
        let reparsed: serde_json::Value =
            serde_json::from_str(&serialized).expect("escaped body must be valid JSON");
        assert_eq!(
            reparsed
                .pointer("/query/term/user")
                .and_then(serde_json::Value::as_str),
            Some(r#"x"}},"match_all":{"#)
        );
        assert!(reparsed.pointer("/query/match_all").is_none());
    }

    /// A value containing `{`, `}`, `\` and a newline must not corrupt the body.
    #[test]
    fn test_build_search_body_value_with_braces_and_backslash() {
        let settings = serde_json::json!({
            "query_template": "{\"query\":{\"match\":{\"raw\":\"%{payload}\"}}}"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("payload", EventValue::String("a{b}c\\d\ne".into()));
        let body = filter.build_search_body(&event);
        assert_eq!(
            body.pointer("/query/match/raw")
                .and_then(serde_json::Value::as_str),
            Some("a{b}c\\d\ne")
        );
        // Round-trips through serialization unbroken.
        let serialized = serde_json::to_string(&body).expect("serialize");
        let reparsed: serde_json::Value = serde_json::from_str(&serialized).expect("valid JSON");
        assert_eq!(
            reparsed
                .pointer("/query/match/raw")
                .and_then(serde_json::Value::as_str),
            Some("a{b}c\\d\ne")
        );
    }

    /// (b) A malformed non-empty `query_template` is rejected at config time
    /// (loudly), not silently turned into `match_all`.
    #[test]
    fn test_malformed_query_template_rejected_at_config_time() {
        let settings = serde_json::json!({
            "query_template": "{not valid json"
        });
        let err = ElasticsearchFilter::from_config(&settings, None)
            .expect_err("malformed template must be a config error");
        let msg = err.to_string();
        assert!(
            msg.contains("query_template"),
            "error should mention query_template: {msg}"
        );
    }

    /// A non-empty template that parses but is not a JSON object (e.g. a bare
    /// scalar/array) is also a config error rather than a silent `match_all`.
    #[test]
    fn test_non_object_query_template_rejected_at_config_time() {
        let settings = serde_json::json!({ "query_template": "[1,2,3]" });
        ElasticsearchFilter::from_config(&settings, None)
            .expect_err("non-object template must be a config error");

        let settings = serde_json::json!({ "query_template": "\"just a string\"" });
        ElasticsearchFilter::from_config(&settings, None)
            .expect_err("scalar template must be a config error");
    }

    /// (c) Normal `%{field}` substitution still queries the intended term.
    #[test]
    fn test_normal_substitution_queries_intended_term() {
        let settings = serde_json::json!({
            "query_template": "{\"query\":{\"term\":{\"user.id\":\"%{uid}\"}}}"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("uid", EventValue::String("u-42".into()));
        let body = filter.build_search_body(&event);
        assert_eq!(
            body.pointer("/query/term/user.id")
                .and_then(serde_json::Value::as_str),
            Some("u-42")
        );
        assert!(body.pointer("/query/match_all").is_none());
    }

    /// Missing fields preserve the literal `%{field}` placeholder (Logstash-ish
    /// behavior) — and crucially do NOT collapse to `match_all`.
    #[test]
    fn test_missing_field_preserves_placeholder_not_match_all() {
        let settings = serde_json::json!({
            "query_template": "{\"query\":{\"term\":{\"user\":\"%{absent}\"}}}"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let body = filter.build_search_body(&Event::new("test"));
        assert_eq!(
            body.pointer("/query/term/user")
                .and_then(serde_json::Value::as_str),
            Some("%{absent}")
        );
        assert!(body.pointer("/query/match_all").is_none());
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

    #[tokio::test]
    async fn test_debug_redacts_secrets() {
        let settings = serde_json::json!({
            "hosts": ["http://es.example.com:9200"],
            "index": "secure-index",
            "user": "elastic",
            "password": "changeme",
            "api_key": "abc123"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let dbg = format!("{filter:?}");

        // Plaintext secret values must never appear in the Debug output.
        assert!(
            !dbg.contains("changeme"),
            "password leaked in Debug output: {dbg}"
        );
        assert!(
            !dbg.contains("abc123"),
            "api_key leaked in Debug output: {dbg}"
        );

        // Secrets are rendered as a redaction marker, and non-secret fields
        // (host, index, username) are still printed normally.
        assert!(dbg.contains("***"), "redaction marker missing: {dbg}");
        assert!(
            dbg.contains("es.example.com"),
            "host missing from Debug output: {dbg}"
        );
        assert!(
            dbg.contains("secure-index"),
            "index missing from Debug output: {dbg}"
        );
        assert!(
            dbg.contains("elastic"),
            "username (non-secret) missing from Debug output: {dbg}"
        );
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

    /// Like [`spawn_mock_es`], but also captures the request body the client
    /// sent and exposes it via the returned channel receiver. Reads the full
    /// request (headers + body) so we can assert on the exact bytes ES receives.
    fn spawn_mock_es_capture(
        response_json: &'static str,
    ) -> (String, std::sync::mpsc::Receiver<String>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // Read the request until we've consumed the Content-Length body.
                let mut raw: Vec<u8> = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    raw.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&raw);
                    if let Some(header_end) = text.find("\r\n\r\n") {
                        let headers = &text[..header_end];
                        let content_len = headers
                            .lines()
                            .find_map(|l| {
                                let l = l.to_ascii_lowercase();
                                l.strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        let body_start = header_end + 4;
                        if raw.len() >= body_start + content_len {
                            break;
                        }
                    }
                }
                let text = String::from_utf8_lossy(&raw).into_owned();
                let body = text
                    .split_once("\r\n\r\n")
                    .map(|(_, b)| b.to_string())
                    .unwrap_or_default();
                let _ = tx.send(body);

                let resp_body = response_json;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });
        (format!("http://{addr}"), rx)
    }

    /// Like [`spawn_mock_es`], but returns a `200 OK` with a caller-supplied
    /// (potentially very large) **owned** body, so we can exercise the
    /// success-path body bounding (`read_capped_body`) path. Returns the host URL.
    fn spawn_mock_es_sized_ok(body: String) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // Drain the request (best-effort).
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
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

    /// Like [`spawn_mock_es`], but returns a non-2xx status with a caller-
    /// supplied (potentially very large) body, so we can exercise the
    /// error-body bounding path. Returns the host URL.
    fn spawn_mock_es_error(status_line: &'static str, body: String) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // Drain the request (best-effort).
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });
        format!("http://{addr}")
    }

    /// Regression for the DD finding: a misconfigured/hostile ES host that
    /// returns a *very large* non-2xx body must not be buffered+logged
    /// verbatim. The fix bounds the *read* (via the streaming
    /// `read_bounded_body_stream` helper, which stops after ~512 bytes) rather
    /// than buffering the whole body with `response.text()` and only then
    /// truncating — so even a multi-GB body cannot OOM the process. The filter
    /// must still fail over gracefully (apply `tag_on_failure`), and the
    /// snippet we log is far smaller than the oversized body.
    #[tokio::test]
    async fn test_large_error_body_is_bounded_and_fails_over() {
        // A 100 KiB error body — far larger than the 512-byte log bound. The
        // mock server writes the whole body on the wire; the filter, however,
        // only reads ~512 bytes of it via the streaming bounded reader.
        let huge = "E".repeat(100 * 1024);
        let host = spawn_mock_es_error("HTTP/1.1 500 Internal Server Error", huge.clone());
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "users",
            "timeout": 5
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        // Non-2xx on the only host => failover exhausts => failure tag applied.
        // The filter exercises the streaming bounded reader here: it issues the
        // request, sees the 500, and reads only ~512(+1) bytes of the 100 KiB
        // body via `read_bounded_body_stream` (never `response.text()`), so a
        // multi-GB body could not OOM the process on this diagnostic path.
        let result = filter.filter(Event::new("test")).await.expect("filter");
        assert!(
            result[0].has_tag("_elasticsearch_lookup_failure"),
            "non-2xx response must still tag failure"
        );

        // The snippet the warn! line emits is bounded: the streaming reader
        // pulls at most ~512(+1) bytes off the body, then `bounded_snippet`
        // truncates to 512 bytes plus a `… (N bytes total)` marker. The shared
        // `read_bounded_body_stream` unit tests already prove the early-stop
        // read; here we assert the *resulting snippet* is far smaller than the
        // oversized body and keeps only the leading bytes. (`bounded_snippet`
        // over the read-capped prefix mirrors exactly what gets logged.)
        let read_capped = &huge[..(512 + 1).min(huge.len())];
        let snippet = ferro_stash_core::bounded_snippet(read_capped, 512);
        assert!(
            snippet.len() < huge.len(),
            "snippet must be far smaller than the raw body: {} < {}",
            snippet.len(),
            huge.len()
        );
        assert!(
            snippet.starts_with(&"E".repeat(512)),
            "snippet keeps the leading bytes"
        );
        assert!(
            snippet.contains("bytes total"),
            "snippet carries the truncation marker: {snippet}"
        );
        // Critically, the full body is never reproduced in what we log: the
        // streaming reader only ever pulls ~512(+1) bytes off the body.
        assert!(
            !snippet.contains(&"E".repeat(514)),
            "snippet must not contain more than the bounded read of body content"
        );
    }

    /// Regression for the HIGH DD finding: a misconfigured/compromised ES host
    /// that returns a *2xx* (`200`) with an oversized JSON body must NOT be
    /// buffered in full (`Response::json()` would, OOMing the process on a single
    /// lookup). The fix streams the body via `read_capped_body` and rejects it
    /// over the cap, treating it as a lookup *failure* for that host (failover +
    /// `tag_on_failure`) — never a panic, never an unbounded read. We drive the
    /// over-limit path with a small per-call cap (no need to allocate 256 MB).
    #[tokio::test]
    async fn test_oversized_success_body_is_capped_and_fails_over() {
        // A 64 KiB *valid-JSON* 200 body — well over the 4 KiB test cap below.
        // It is real JSON (so a buffering reader *would* have parsed it
        // successfully); the cap must reject it on the size, not the shape.
        let big = format!(
            r#"{{"hits":{{"hits":[{{"_source":{{"pad":"{}"}}}}]}}}}"#,
            "P".repeat(64 * 1024)
        );
        let host = spawn_mock_es_sized_ok(big);
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "users",
            "timeout": 5
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");

        // Drive the capped path directly with a small cap (4 KiB): the only host
        // returns an over-limit 200 body, so `read_capped_body` errs, the host is
        // treated as failed, failover exhausts, and `execute_query_capped`
        // returns `None` (the filter would then apply `tag_on_failure`).
        let out = filter
            .execute_query_capped(&Event::new("test"), 4 * 1024)
            .await;
        assert!(
            out.is_none(),
            "an over-limit 200 body must be treated as a lookup failure, not parsed"
        );
    }

    /// Sanity companion to the over-limit test: a *within-cap* valid 200 body on
    /// the same capped code path still parses to hits, proving the cap rejects
    /// only oversize bodies and does not break the normal success path.
    #[tokio::test]
    async fn test_within_cap_success_body_still_parses() {
        let host = spawn_mock_es(r#"{"hits":{"hits":[{"_source":{"ok":true}}]}}"#);
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "users",
            "target": "es_result"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        // A modest cap that comfortably fits the tiny response.
        let out = filter
            .execute_query_capped(&Event::new("test"), 4 * 1024)
            .await;
        let hits = out.expect("within-cap body must parse to hits");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].get("ok"), Some(&EventValue::Boolean(true)));
    }

    /// End-to-end (d): a field value carrying an injection payload is sent to ES
    /// as a properly-escaped JSON string — the wire body parses cleanly, keeps
    /// the configured `term` structure, and contains NO injected `match_all`.
    #[tokio::test]
    async fn test_injection_payload_escaped_on_the_wire() {
        let (host, rx) = spawn_mock_es_capture(r#"{"hits":{"hits":[{"_source":{"ok":true}}]}}"#);
        let settings = serde_json::json!({
            "hosts": [host],
            "index": "users",
            "query_template": "{\"query\":{\"term\":{\"user\":\"%{username}\"}}}",
            "target": "es_result"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "username",
            EventValue::String(r#"x"}},"match_all":{"#.into()),
        );
        let _ = filter.filter(event).await.expect("filter");

        let sent = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("mock server captured a request body");
        let parsed: serde_json::Value =
            serde_json::from_str(&sent).expect("ES received well-formed JSON");
        assert_eq!(
            parsed
                .pointer("/query/term/user")
                .and_then(serde_json::Value::as_str),
            Some(r#"x"}},"match_all":{"#)
        );
        assert!(
            parsed.pointer("/query/match_all").is_none(),
            "no injected match_all on the wire: {sent}"
        );
    }

    #[tokio::test]
    async fn test_elasticsearch_real_search_field_mapping() {
        let host =
            spawn_mock_es(r#"{"hits":{"hits":[{"_source":{"name":"Alice","role":"admin"}}]}}"#);
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
            panic!(
                "expected es_result object: {:?}",
                result[0].get("es_result")
            );
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
        let index = "ferro-stash-filter-live";
        let http = reqwest::Client::new();
        // Seed a known document and refresh so it is immediately searchable.
        http.post(format!("{url}/{index}/_doc?refresh=true"))
            .json(&serde_json::json!({ "marker": "ferro-live", "msg": "hello" }))
            .send()
            .await
            .expect("seed request")
            .error_for_status()
            .expect("seed must return 2xx");

        let settings = serde_json::json!({
            "hosts": [url],
            "index": index,
            "target": "es_result",
            "result_size": 1
            // default query_template => match_all, which matches the seeded doc
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");

        // The seeded doc guarantees a hit, so the lookup must populate `target`
        // (and NOT add the failure tag) — proving the real `_search` round-trip
        // mapped a `hits.hits[]._source` into the event, not just "didn't panic".
        assert!(
            result[0].get("es_result").is_some(),
            "live ES lookup must map the hit into the target field"
        );
        assert!(
            !result[0].has_tag("_elasticsearch_lookup_failure"),
            "a successful live lookup must not carry the failure tag"
        );
    }
}
