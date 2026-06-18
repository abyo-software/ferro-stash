// SPDX-License-Identifier: Apache-2.0
//! HTTP input plugin — receives events via HTTP POST.

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Debug)]
pub struct HttpInput {
    host: String,
    port: u16,
    tags: Vec<String>,
    response_code: u16,
}

impl HttpInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        let port = settings
            .get_port("port", 8080)
            .map_err(|message| FerroStashError::Input {
                plugin: "http".to_string(),
                message,
            })?;
        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let response_code = settings
            .get("response_code")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(200) as u16;

        Ok(Self {
            host,
            port,
            tags,
            response_code,
        })
    }
}

#[derive(Clone)]
struct AppState {
    sender: mpsc::Sender<Event>,
    tags: Vec<String>,
    response_code: u16,
}

async fn handle_post(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let text = String::from_utf8_lossy(&body);

    // Try to parse as JSON
    let events = if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
        match json {
            serde_json::Value::Array(arr) => arr
                .into_iter()
                .map(|v| {
                    let mut event = Event::from_json(v);
                    for tag in &state.tags {
                        event.add_tag(tag);
                    }
                    event
                })
                .collect(),
            other => {
                let mut event = Event::from_json(other);
                for tag in &state.tags {
                    event.add_tag(tag);
                }
                vec![event]
            }
        }
    } else {
        // Treat as plain text, one event per line
        text.lines()
            .filter(|l| !l.is_empty())
            .map(|line| {
                let mut event = Event::new(line);
                for tag in &state.tags {
                    event.add_tag(tag);
                }
                event
            })
            .collect()
    };

    for event in events {
        if state.sender.send(event).await.is_err() {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }

    StatusCode::from_u16(state.response_code).unwrap_or(StatusCode::OK)
}

#[async_trait]
impl InputPlugin for HttpInput {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let state = Arc::new(AppState {
            sender,
            tags: self.tags.clone(),
            response_code: self.response_code,
        });

        let app = Router::new()
            .route("/", post(handle_post))
            .route("/events", post(handle_post))
            .with_state(state);

        let addr = format!("{}:{}", self.host, self.port);
        let listener =
            tokio::net::TcpListener::bind(&addr)
                .await
                .map_err(|e| FerroStashError::Input {
                    plugin: "http".to_string(),
                    message: format!("bind error: {e}"),
                })?;

        info!(address = %addr, "HTTP input listening");

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                shutdown.wait().await;
            })
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "http".to_string(),
                message: format!("server error: {e}"),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_config_defaults() {
        let settings = serde_json::json!({});
        let input = HttpInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "0.0.0.0");
        assert_eq!(input.port, 8080);
        assert_eq!(input.response_code, 200);
    }

    #[test]
    fn test_http_config_out_of_range_port_rejected() {
        // Regression: the HTTP *listen* port above 65535 must fail loudly at
        // config time rather than silently truncating via `as u16`
        // (70000 as u16 == 4464). `response_code` is a status code, not a port.
        let settings = serde_json::json!({ "port": 70000 });
        let err = HttpInput::from_config(&settings).expect_err("port 70000 must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("70000") && msg.contains("port"),
            "expected an out-of-range port error, got: {msg}"
        );
    }

    #[test]
    fn test_http_config_custom() {
        let settings = serde_json::json!({
            "host": "127.0.0.1",
            "port": 9090,
            "response_code": 201,
            "tags": ["http_in"]
        });
        let input = HttpInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "127.0.0.1");
        assert_eq!(input.port, 9090);
        assert_eq!(input.response_code, 201);
        assert_eq!(input.tags, vec!["http_in"]);
    }

    #[test]
    fn test_http_name() {
        let settings = serde_json::json!({});
        let input = HttpInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "http");
    }
}
