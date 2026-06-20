// SPDX-License-Identifier: Apache-2.0
//! HTTP filter — enrich events by issuing an HTTP request per event.
//!
//! For every event the filter builds a request from the configured `url`
//! (with `%{field}` sprintf interpolation), `verb`, `headers`, and optional
//! `body`, sends it via reqwest (the same client used by the Elasticsearch
//! filter / HTTP output), and stores the response on the event:
//!
//! - the response **body** is written to `target_body` (default
//!   `http_response`). If the body parses as JSON it is stored as the parsed
//!   structure; otherwise it is stored as a raw string.
//! - the response **headers** are written to `target_headers` (only when that
//!   key is configured), as an object of `header-name => value` strings.
//!
//! On a transport error (connection refused, timeout, …) **or** a non-2xx
//! response the event is tagged `_httprequestfailure`. A non-2xx response
//! still stores whatever body was returned so the failure can be inspected.
//!
//! Logstash-compatible config keys: `url`, `verb`, `headers`, `body`,
//! `target_body`, `target_headers`.

use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;
use reqwest::{Client, Method};
use tracing::warn;

/// Upper bound on a response body buffered before it is stored on the event.
///
/// `read_capped_body` streams the body and rejects it once it exceeds this
/// limit, so a misconfigured/hostile endpoint returning a multi-GB body cannot
/// OOM the process on a single event. An over-limit (or unreadable) body is
/// treated as a request *failure* (`_httprequestfailure`), never an unbounded
/// read.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

pub struct HttpFilter {
    /// Target URL template; `%{field}` placeholders are resolved per event.
    url: String,
    /// HTTP method (default `GET`).
    method: Method,
    /// Request headers; values support `%{field}` interpolation. Names are
    /// static.
    headers: Vec<(String, String)>,
    /// Optional request body template (`%{field}` interpolation).
    body: Option<String>,
    /// Event field that receives the response body (default `http_response`).
    target_body: String,
    /// Optional event field that receives the response headers map.
    target_headers: Option<String>,
    /// Tag applied on a request failure or non-2xx response.
    tag_on_failure: String,
    /// Shared HTTP client.
    client: Client,
    condition: Option<Condition>,
}

/// Manual `Debug` impl that redacts header values and the request body.
///
/// Header values frequently carry credentials (`Authorization: Bearer …`) and
/// the body may carry secrets; a derived `Debug` would print them verbatim into
/// logs. We render header *names* (structural) but mask their values, and mask
/// the presence of a body.
impl std::fmt::Debug for HttpFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted_headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(k, _)| (k.as_str(), "***"))
            .collect();
        f.debug_struct("HttpFilter")
            .field("url", &ferro_stash_core::redact_url(&self.url))
            .field("method", &self.method)
            .field("headers", &redacted_headers)
            .field("body", &self.body.as_ref().map(|_| "***"))
            .field("target_body", &self.target_body)
            .field("target_headers", &self.target_headers)
            .field("tag_on_failure", &self.tag_on_failure)
            .field("client", &self.client)
            .field("condition", &self.condition)
            .finish()
    }
}

impl HttpFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let url = settings
            .get("url")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| FerroStashError::Filter {
                plugin: "http".to_string(),
                message: "`url` is required".to_string(),
            })?
            .to_string();

        // `verb` defaults to GET. A present-but-malformed verb is a config error
        // (rejected loudly) rather than silently falling back to GET.
        let verb = settings
            .get("verb")
            .and_then(|v| v.as_str())
            .unwrap_or("get");
        let method = Method::from_bytes(verb.to_ascii_uppercase().as_bytes()).map_err(|_| {
            FerroStashError::Filter {
                plugin: "http".to_string(),
                message: format!("invalid verb: {verb}"),
            }
        })?;

        let headers: Vec<(String, String)> = settings
            .get("headers")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let body = settings
            .get("body")
            .and_then(|v| v.as_str())
            .map(String::from);

        // The response body is stored here; documented default `http_response`.
        let target_body = settings
            .get("target_body")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("http_response")
            .to_string();

        let target_headers = settings
            .get("target_headers")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from);

        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_str())
            .unwrap_or("_httprequestfailure")
            .to_string();

        let timeout_secs = settings
            .get("timeout")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(30);

        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| FerroStashError::Filter {
                plugin: "http".to_string(),
                message: format!("HTTP client error: {e}"),
            })?;

        Ok(Self {
            url,
            method,
            headers,
            body,
            target_body,
            target_headers,
            tag_on_failure,
            client,
            condition,
        })
    }

    /// Store a response body on the event: parse it as JSON when possible
    /// (storing the parsed structure), otherwise store the raw string.
    fn store_body(&self, event: &mut Event, raw: String) {
        let value = match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(json) => EventValue::from(json),
            Err(_) => EventValue::String(raw),
        };
        event.set(self.target_body.clone(), value);
    }
}

#[async_trait]
impl FilterPlugin for HttpFilter {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let url = event.sprintf(&self.url);
        // Log-safe rendering: the real `url` is used for the request, but a
        // url can carry userinfo / signed query params, so warnings log the
        // redacted form.
        let safe_url = ferro_stash_core::redact_url(&url);
        let mut request = self.client.request(self.method.clone(), &url);
        for (key, value) in &self.headers {
            request = request.header(key, event.sprintf(value));
        }
        if let Some(body) = &self.body {
            request = request.body(event.sprintf(body));
        }

        let response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                warn!(url = %safe_url, error = %e, "http filter: request failed");
                event.add_tag(&self.tag_on_failure);
                return Ok(vec![event]);
            }
        };

        let status = response.status();

        // Capture headers before consuming the response into a body stream.
        if let Some(target) = &self.target_headers {
            let mut map: IndexMap<String, EventValue> = IndexMap::new();
            for (name, value) in response.headers() {
                map.insert(
                    name.as_str().to_string(),
                    EventValue::String(value.to_str().unwrap_or("").to_string()),
                );
            }
            event.set(target.clone(), EventValue::Object(map));
        }

        match ferro_stash_core::read_capped_body(
            Box::pin(response.bytes_stream()),
            MAX_RESPONSE_BYTES,
        )
        .await
        {
            Ok(bytes) => {
                let raw = String::from_utf8_lossy(&bytes).into_owned();
                self.store_body(&mut event, raw);
            }
            Err(e) => {
                warn!(url = %safe_url, error = %e, "http filter: response body too large or unreadable");
                event.add_tag(&self.tag_on_failure);
                return Ok(vec![event]);
            }
        }

        // A non-2xx response stored its body above but still flags failure so a
        // downstream conditional can route it.
        if !status.is_success() {
            warn!(url = %safe_url, status = %status, "http filter: non-success response");
            event.add_tag(&self.tag_on_failure);
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

    // ----- Config tests -----

    #[test]
    fn test_http_filter_requires_url() {
        let settings = serde_json::json!({});
        let err = HttpFilter::from_config(&settings, None).expect_err("url is required");
        assert!(
            err.to_string().contains("url"),
            "error should mention url: {err}"
        );
    }

    #[test]
    fn test_http_filter_blank_url_rejected() {
        let settings = serde_json::json!({ "url": "   " });
        assert!(HttpFilter::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_http_filter_defaults() {
        let settings = serde_json::json!({ "url": "http://example.com/%{id}" });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.method, Method::GET);
        assert_eq!(filter.target_body, "http_response");
        assert!(filter.target_headers.is_none());
        assert_eq!(filter.tag_on_failure, "_httprequestfailure");
        assert_eq!(filter.name(), "http");
    }

    #[test]
    fn test_http_filter_custom_verb_and_target() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "verb": "post",
            "body": "%{message}",
            "target_body": "resp",
            "target_headers": "resp_headers",
            "headers": { "Authorization": "Bearer %{token}" }
        });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.method, Method::POST);
        assert_eq!(filter.target_body, "resp");
        assert_eq!(filter.target_headers.as_deref(), Some("resp_headers"));
        assert_eq!(filter.body.as_deref(), Some("%{message}"));
        assert_eq!(filter.headers.len(), 1);
    }

    #[test]
    fn test_http_filter_invalid_verb_rejected() {
        let settings = serde_json::json!({ "url": "http://example.com", "verb": "no good" });
        let err = HttpFilter::from_config(&settings, None).expect_err("invalid verb must error");
        assert!(
            err.to_string().contains("verb"),
            "error should mention verb: {err}"
        );
    }

    #[test]
    fn test_http_filter_debug_redacts_header_values_and_body() {
        let settings = serde_json::json!({
            "url": "https://user:urlpw@example.com/p?api_key=urltoken",
            "headers": { "Authorization": "Bearer s3cr3t" },
            "body": "topsecretbody"
        });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        let dbg = format!("{filter:?}");
        assert!(!dbg.contains("s3cr3t"), "header value leaked: {dbg}");
        assert!(!dbg.contains("topsecretbody"), "body leaked: {dbg}");
        assert!(
            dbg.contains("Authorization"),
            "header name should be visible: {dbg}"
        );
        assert!(dbg.contains("***"), "redaction marker missing: {dbg}");
        // The url is routed through redact_url: userinfo and signed params gone.
        assert!(!dbg.contains("urlpw"), "url userinfo leaked: {dbg}");
        assert!(!dbg.contains("urltoken"), "url api_key value leaked: {dbg}");
        assert!(dbg.contains("example.com"), "host should be visible: {dbg}");
    }

    // ----- Behaviour tests against a tiny raw-HTTP mock server -----
    //
    // `axum` is not a dev-dependency of this crate, so we spin up a minimal
    // blocking HTTP/1.1 responder on a std TcpListener in a background thread
    // (the same pattern the Elasticsearch filter uses).

    fn spawn_mock(status_line: &'static str, body: &'static str) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
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

    #[tokio::test]
    async fn test_http_filter_stores_json_response() {
        let host = spawn_mock("HTTP/1.1 200 OK", r#"{"ok":true,"n":7}"#);
        let settings = serde_json::json!({ "url": host, "target_body": "resp" });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        // JSON body is parsed into a structured value at the target field.
        if let Some(EventValue::Object(obj)) = result[0].get("resp") {
            assert_eq!(obj.get("ok"), Some(&EventValue::Boolean(true)));
            assert_eq!(obj.get("n"), Some(&EventValue::Integer(7)));
        } else {
            panic!(
                "expected parsed object at resp: {:?}",
                result[0].get("resp")
            );
        }
        assert!(!result[0].has_tag("_httprequestfailure"));
    }

    #[tokio::test]
    async fn test_http_filter_stores_plain_text_response() {
        let host = spawn_mock("HTTP/1.1 200 OK", "just text");
        let settings = serde_json::json!({ "url": host });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        // Non-JSON body is stored as a raw string at the default target field.
        assert_eq!(
            result[0].get("http_response"),
            Some(&EventValue::String("just text".into()))
        );
    }

    #[tokio::test]
    async fn test_http_filter_non_2xx_tags_failure_and_stores_body() {
        let host = spawn_mock("HTTP/1.1 500 Internal Server Error", "boom");
        let settings = serde_json::json!({ "url": host });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        assert!(result[0].has_tag("_httprequestfailure"));
        assert_eq!(
            result[0].get("http_response"),
            Some(&EventValue::String("boom".into()))
        );
    }

    #[tokio::test]
    async fn test_http_filter_unreachable_tags_failure() {
        // Port 1 should refuse fast.
        let settings = serde_json::json!({ "url": "http://127.0.0.1:1", "timeout": 2 });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        assert!(result[0].has_tag("_httprequestfailure"));
    }

    // ----- Live smoke (real HTTP endpoint) -----
    //
    //   HTTP_FILTER_URL=https://httpbin.org/get \
    //     cargo test -p ferro-stash-filter -- http_filter_live --ignored

    #[tokio::test]
    #[ignore = "requires a reachable HTTP endpoint; set HTTP_FILTER_URL to enable"]
    async fn test_http_filter_live() {
        let url = std::env::var("HTTP_FILTER_URL").expect("HTTP_FILTER_URL must be set");
        let settings = serde_json::json!({ "url": url, "target_body": "resp" });
        let filter = HttpFilter::from_config(&settings, None).expect("config");
        let result = filter.filter(Event::new("test")).await.expect("filter");
        assert!(
            result[0].get("resp").is_some(),
            "a successful live request must populate the target field"
        );
        assert!(!result[0].has_tag("_httprequestfailure"));
    }
}
