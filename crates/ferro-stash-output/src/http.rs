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
}

#[async_trait]
impl OutputPlugin for HttpOutput {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
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
                let form_body = if let Some(event) = events.first() {
                    event
                        .fields()
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v.to_string_lossy()))
                        .collect::<Vec<_>>()
                        .join("&")
                } else {
                    String::new()
                };
                (form_body, Some("application/x-www-form-urlencoded"))
            }
        };

        let ct = content_type_override.unwrap_or(&self.content_type);
        let mut last_error = None;
        for attempt in 0..=self.retry_count {
            let mut request = match self.method {
                HttpMethod::Post => self.client.post(&self.url),
                HttpMethod::Put => self.client.put(&self.url),
                HttpMethod::Patch => self.client.patch(&self.url),
            };

            request = request.header("Content-Type", ct);
            for (key, value) in &self.headers {
                request = request.header(key, value);
            }

            let response = match request.body(body.clone()).send().await {
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
            let body = ferro_stash_core::read_bounded_body_stream(
                Box::pin(response.bytes_stream()),
                crate::ERROR_BODY_SNIPPET_LIMIT,
            )
            .await;
            warn!(status = %status, body = %body, attempt, "HTTP output error");
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

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Router};
    use ferro_stash_core::Event;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
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
}
