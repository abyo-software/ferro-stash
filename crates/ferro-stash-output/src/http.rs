// SPDX-License-Identifier: Apache-2.0
//! HTTP output plugin — sends events via HTTP POST.

use std::time::Duration;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use reqwest::Client;
use tracing::warn;

#[allow(dead_code)]
#[derive(Debug)]
pub struct HttpOutput {
    url: String,
    method: HttpMethod,
    headers: Vec<(String, String)>,
    content_type: String,
    client: Client,
    format: HttpFormat,
    retry_count: usize,
    condition: Option<Condition>,
}

#[derive(Debug)]
enum HttpMethod {
    Post,
    Put,
    Patch,
}

#[derive(Debug)]
enum HttpFormat {
    Json,
    JsonArray,
    Form,
    Text,
}

impl HttpOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let url = settings
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FerroStashError::Output {
                plugin: "http".to_string(),
                message: "url is required".to_string(),
            })?
            .to_string();

        let method = match settings
            .get("http_method")
            .and_then(|v| v.as_str())
            .unwrap_or("post")
        {
            "put" => HttpMethod::Put,
            "patch" => HttpMethod::Patch,
            _ => HttpMethod::Post,
        };

        let headers: Vec<(String, String)> = settings
            .get("headers")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let content_type = settings
            .get("content_type")
            .and_then(|v| v.as_str())
            .unwrap_or("application/json")
            .to_string();

        let format = match settings
            .get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("json")
        {
            "json_batch" | "json_array" => HttpFormat::JsonArray,
            "form" => HttpFormat::Form,
            "message" | "text" => HttpFormat::Text,
            _ => HttpFormat::Json,
        };

        let retry_count = settings
            .get("retry_count")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(3) as usize;

        let timeout_secs = settings
            .get("timeout")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(30);

        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| FerroStashError::Output {
                plugin: "http".to_string(),
                message: format!("HTTP client error: {e}"),
            })?;

        Ok(Self {
            url,
            method,
            headers,
            content_type,
            client,
            format,
            retry_count,
            condition,
        })
    }

    /// Send a single body to the configured URL/method, honoring the
    /// configured headers and retry/backoff policy. Returns `Ok(())` on the
    /// first 2xx, and `Err` once retries are exhausted (or a non-retryable
    /// status is seen). Shared by every format so the retry semantics are
    /// identical for batched bodies and per-event form posts.
    async fn send_body_with_retry(&self, body: &str, content_type: &str) -> Result<()> {
        let mut last_error = None;
        for attempt in 0..=self.retry_count {
            let mut request = match self.method {
                HttpMethod::Post => self.client.post(&self.url),
                HttpMethod::Put => self.client.put(&self.url),
                HttpMethod::Patch => self.client.patch(&self.url),
            };

            request = request.header("Content-Type", content_type);
            for (key, value) in &self.headers {
                request = request.header(key, value);
            }

            let response = match request.body(body.to_string()).send().await {
                Ok(response) => response,
                Err(e) => {
                    last_error = Some(format!("request failed: {e}"));
                    if attempt < self.retry_count {
                        continue;
                    }
                    break;
                }
            };

            if response.status().is_success() {
                return Ok(());
            }

            let status = response.status();
            let snippet = ferro_stash_core::read_bounded_body_stream(
                Box::pin(response.bytes_stream()),
                crate::ERROR_BODY_SNIPPET_LIMIT,
            )
            .await;
            warn!(status = %status, body = %snippet, attempt, "HTTP output error");
            last_error = Some(format!("HTTP {status}"));
            if !(status.is_server_error() || status.as_u16() == 429) || attempt >= self.retry_count
            {
                break;
            }
        }

        Err(FerroStashError::Output {
            plugin: "http".to_string(),
            message: last_error.unwrap_or_else(|| "request failed".to_string()),
        })
    }

    /// Form-encode and POST one request PER EVENT, aggregating results like the
    /// kafka output. Every event is attempted regardless of individual
    /// failures (no short-circuit that would DROP later events). If ANY request
    /// fails, an `Err` is returned so the pipeline can DLQ/retry the batch; on
    /// full success `Ok(())` is returned. This NEVER returns `Ok` while having
    /// dropped or failed to send any event.
    ///
    /// Delivery semantics: like the kafka output, this is AT-LEAST-ONCE. On a
    /// partial-batch failure we return `Err` for the whole batch, so a
    /// pipeline retry may RE-SEND the events that already succeeded
    /// (duplicates). That is inherent to per-record at-least-once delivery and
    /// is accepted; the contract here is that no event is ever silently
    /// DROPPED, not that there are zero duplicates.
    async fn output_form(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let total = events.len();
        let mut succeeded = 0usize;
        let mut first_error: Option<String> = None;

        for event in &events {
            // Build a proper `application/x-www-form-urlencoded` body by
            // percent-encoding every key AND value. Hand-joining raw
            // `k=v` pairs with `&` corrupts the event when a key or value
            // contains `&`, `=`, `%`, or whitespace: e.g.
            // `message = "login ok&admin=true"` would be split into multiple
            // fields (silent corruption/injection) while output() still
            // returned Ok. Over-encoding via `NON_ALPHANUMERIC` is always
            // valid form-urlencoded (form receivers accept %20 for space).
            let form_body = event
                .fields()
                .iter()
                .map(|(k, v)| {
                    let key = percent_encoding::utf8_percent_encode(
                        k,
                        percent_encoding::NON_ALPHANUMERIC,
                    );
                    let value = v.to_string_lossy();
                    let value = percent_encoding::utf8_percent_encode(
                        &value,
                        percent_encoding::NON_ALPHANUMERIC,
                    );
                    format!("{key}={value}")
                })
                .collect::<Vec<_>>()
                .join("&");

            match self
                .send_body_with_retry(&form_body, "application/x-www-form-urlencoded")
                .await
            {
                Ok(()) => succeeded += 1,
                Err(e) => {
                    // Record the first failure for the surfaced error, but keep
                    // going so the remaining events are still attempted.
                    if first_error.is_none() {
                        first_error = Some(e.to_string());
                    }
                }
            }
        }

        let failed = total - succeeded;
        if let Some(err) = first_error {
            warn!(
                succeeded,
                failed, total, "HTTP form output: partial batch delivery; all events attempted"
            );
            return Err(FerroStashError::Output {
                plugin: "http".to_string(),
                message: format!(
                    "HTTP form delivery failed for {failed}/{total} events (first error: {err}); \
                     {succeeded} succeeded — whole-batch retry/DLQ may duplicate the sent events"
                ),
            });
        }

        Ok(())
    }
}

#[async_trait]
impl OutputPlugin for HttpOutput {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        // Form-encoding is one record per request: a single
        // `application/x-www-form-urlencoded` body cannot carry more than one
        // event's worth of key/value pairs without ambiguous key collisions.
        // The pipeline hands the WHOLE batch to one `output()` call, so the
        // `form` format must send one POST PER EVENT and aggregate the results
        // (like the kafka output) — otherwise every event after the first is
        // silently dropped while the pipeline records the batch as delivered.
        if matches!(self.format, HttpFormat::Form) {
            return self.output_form(events).await;
        }

        let (body, content_type_override) = match self.format {
            HttpFormat::Json => {
                if events.len() == 1 {
                    // Single event: valid JSON, application/json
                    (events[0].to_json_string(), None)
                } else {
                    // Multiple events: NDJSON format, application/x-ndjson
                    let mut ndjson = String::with_capacity(events.len() * 256);
                    for event in &events {
                        ndjson.push_str(&event.to_json_string());
                        ndjson.push('\n');
                    }
                    (ndjson, Some("application/x-ndjson"))
                }
            }
            HttpFormat::JsonArray => {
                let arr: Vec<serde_json::Value> = events
                    .iter()
                    .map(ferro_stash_core::Event::to_json)
                    .collect();
                (serde_json::to_string(&arr).unwrap_or_default(), None)
            }
            HttpFormat::Text => (
                events
                    .iter()
                    .map(|e| e.message().unwrap_or("").to_string())
                    .collect::<Vec<_>>()
                    .join("\n"),
                Some("text/plain"),
            ),
            HttpFormat::Form => {
                // Handled above by `output_form`; unreachable here. Keep an
                // explicit body so the match stays exhaustive without panics.
                (String::new(), Some("application/x-www-form-urlencoded"))
            }
        };

        let ct = content_type_override.unwrap_or(&self.content_type);
        self.send_body_with_retry(&body, ct).await
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Router};
    use ferro_stash_core::event::EventValue;
    use ferro_stash_core::Event;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use tokio::net::TcpListener;

    #[test]
    fn test_http_output_config() {
        let settings = serde_json::json!({ "url": "http://example.com/api" });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.url, "http://example.com/api");
        assert_eq!(output.name(), "http");
    }

    #[test]
    fn test_http_output_missing_url() {
        let settings = serde_json::json!({});
        assert!(HttpOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_http_output_put_method() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "http_method": "put"
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.method, HttpMethod::Put));
    }

    #[test]
    fn test_http_output_patch_method() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "http_method": "patch"
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.method, HttpMethod::Patch));
    }

    #[test]
    fn test_http_output_json_array_format() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "format": "json_batch"
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.format, HttpFormat::JsonArray));
    }

    #[test]
    fn test_http_output_text_format() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "format": "text"
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.format, HttpFormat::Text));
    }

    #[test]
    fn test_http_output_form_format() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "format": "form"
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.format, HttpFormat::Form));
    }

    #[test]
    fn test_http_output_custom_headers() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "headers": { "X-Custom": "value" }
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.headers.len(), 1);
    }

    #[test]
    fn test_http_output_retry_count() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "retry_count": 5
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.retry_count, 5);
    }

    #[test]
    fn test_http_output_content_type() {
        let settings = serde_json::json!({
            "url": "http://example.com",
            "content_type": "text/plain"
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.content_type, "text/plain");
    }

    #[tokio::test]
    async fn test_http_output_retries_transient_server_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let handler_attempts = Arc::clone(&attempts);
        let app = Router::new().route(
            "/events",
            post(move || {
                let handler_attempts = Arc::clone(&handler_attempts);
                async move {
                    let attempt = handler_attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "try again")
                    } else {
                        (axum::http::StatusCode::OK, "ok")
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
            "url": format!("http://{addr}/events"),
            "retry_count": 1
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");

        let result = output.output(vec![Event::new("hello")]).await;

        assert!(
            result.is_ok(),
            "HTTP output should retry once and publish successfully"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    fn form_event(message: &str, id: i64) -> Event {
        let mut event = Event::new(message);
        event.set("id", EventValue::Integer(id));
        event
    }

    #[tokio::test]
    async fn test_http_output_form_sends_one_request_per_event() {
        // Regression for the round-14 HIGH data-loss finding: with
        // `format => form` and a batch of N>1 events, the output MUST send one
        // form-encoded request PER EVENT (not just `events.first()`), and all
        // event data must arrive.
        let count = Arc::new(AtomicUsize::new(0));
        let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let handler_count = Arc::clone(&count);
        let handler_bodies = Arc::clone(&bodies);
        let app = Router::new().route(
            "/events",
            post(move |body: String| {
                let handler_count = Arc::clone(&handler_count);
                let handler_bodies = Arc::clone(&handler_bodies);
                async move {
                    handler_count.fetch_add(1, Ordering::SeqCst);
                    if let Ok(mut guard) = handler_bodies.lock() {
                        guard.push(body);
                    }
                    (axum::http::StatusCode::OK, "ok")
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let settings = serde_json::json!({
            "url": format!("http://{addr}/events"),
            "format": "form",
            "retry_count": 0
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");

        let batch = vec![
            form_event("alpha", 1),
            form_event("bravo", 2),
            form_event("charlie", 3),
        ];
        let result = output.output(batch).await;

        assert!(result.is_ok(), "all form posts succeeded => Ok");
        // One request per event, NOT a single request that drops events[1..].
        assert_eq!(
            count.load(Ordering::SeqCst),
            3,
            "form output must send one request per event"
        );

        let received = bodies.lock().expect("bodies").clone();
        assert_eq!(received.len(), 3, "server received three form bodies");
        // Each event's data must arrive (message + id) — no event is dropped.
        let joined = received.join("\n");
        for (msg, id) in [("alpha", "1"), ("bravo", "2"), ("charlie", "3")] {
            assert!(
                received.iter().any(|b| b.contains(&format!("message={msg}"))
                    && b.contains(&format!("id={id}"))),
                "expected a form body containing message={msg} and id={id}; got: {joined}"
            );
        }
    }

    #[tokio::test]
    async fn test_http_output_form_percent_encodes_special_chars() {
        // Round-15 MEDIUM regression: a form-encoded value containing `&`, `=`,
        // or whitespace must be percent-encoded so the receiver decodes it back
        // to the ORIGINAL value — not split into extra/injected fields. Before
        // the fix, `message = "login ok&admin=true"` was joined raw and arrived
        // as several fields (silent corruption/injection) while output() still
        // returned Ok.
        let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let handler_bodies = Arc::clone(&bodies);
        let app = Router::new().route(
            "/events",
            post(move |body: String| {
                let handler_bodies = Arc::clone(&handler_bodies);
                async move {
                    if let Ok(mut guard) = handler_bodies.lock() {
                        guard.push(body);
                    }
                    (axum::http::StatusCode::OK, "ok")
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let settings = serde_json::json!({
            "url": format!("http://{addr}/events"),
            "format": "form",
            "retry_count": 0
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");

        // A value with `&`, `=`, and a space — the exact injection vector.
        let nasty_message = "login ok&admin=true";
        let mut event = Event::new(nasty_message);
        event.set("note", EventValue::String("a=b & c".into()));
        let result = output.output(vec![event]).await;

        assert!(result.is_ok(), "form post succeeded => Ok: {result:?}");

        let received = bodies.lock().expect("bodies").clone();
        assert_eq!(received.len(), 1, "exactly one form body received");
        let raw = &received[0];

        // The raw wire form must NOT contain the literal special characters of
        // the value (they must be percent-encoded), so the receiver can't be
        // fooled into reading extra fields.
        assert!(
            !raw.contains("login ok"),
            "spaces must be percent-encoded on the wire: {raw}"
        );

        // Decode the form body the way any form receiver would. The decoded
        // pairs must contain EXACTLY the original fields with the original
        // values — no field was split off by an embedded `&`/`=`.
        let decoded: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(raw.as_bytes())
                .into_owned()
                .collect();

        assert_eq!(
            decoded.get("message").map(String::as_str),
            Some(nasty_message),
            "message must decode back to the original value, not be split: decoded={decoded:?}"
        );
        assert_eq!(
            decoded.get("note").map(String::as_str),
            Some("a=b & c"),
            "note must decode back to the original value: decoded={decoded:?}"
        );
        // The injection would have created spurious keys like `admin`/`true`/`c`.
        assert!(
            !decoded.contains_key("admin"),
            "no injected `admin` field must appear: decoded={decoded:?}"
        );
        assert_eq!(
            decoded.len(),
            2,
            "exactly the two original fields must arrive (message, note): decoded={decoded:?}"
        );
    }

    #[tokio::test]
    async fn test_http_output_form_failing_post_returns_err() {
        // A failing POST must make output() return Err so the pipeline can
        // DLQ/retry — events are NEVER silently dropped while returning Ok.
        let count = Arc::new(AtomicUsize::new(0));
        let handler_count = Arc::clone(&count);
        let app = Router::new().route(
            "/events",
            post(move |_body: String| {
                let handler_count = Arc::clone(&handler_count);
                async move {
                    handler_count.fetch_add(1, Ordering::SeqCst);
                    // Non-retryable client error => fail fast per request.
                    (axum::http::StatusCode::BAD_REQUEST, "nope")
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let settings = serde_json::json!({
            "url": format!("http://{addr}/events"),
            "format": "form",
            "retry_count": 0
        });
        let output = HttpOutput::from_config(&settings, None).expect("config");

        let batch = vec![form_event("alpha", 1), form_event("bravo", 2)];
        let result = output.output(batch).await;

        assert!(
            result.is_err(),
            "a failing form POST must surface Err (no silent drop)"
        );
        // Every event is still attempted (no short-circuit that would drop the
        // later events after the first failure).
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "every event must be attempted even when one fails"
        );
    }
}
