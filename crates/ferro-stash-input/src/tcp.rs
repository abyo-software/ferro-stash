// SPDX-License-Identifier: Apache-2.0
//! TCP input plugin — accepts connections and reads line-delimited data.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct TcpInput {
    host: String,
    port: u16,
    tags: Vec<String>,
}

impl TcpInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        let port = ferro_stash_core::settings_helpers::SettingsExt::get_u64(settings, "port")
            .ok_or_else(|| FerroStashError::Input {
                plugin: "tcp".to_string(),
                message: "port is required".to_string(),
            })? as u16;
        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self { host, port, tags })
    }
}

#[async_trait]
impl InputPlugin for TcpInput {
    fn name(&self) -> &'static str {
        "tcp"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "tcp".to_string(),
                message: format!("bind error: {e}"),
            })?;

        info!(address = %addr, "TCP input listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer = %peer_addr, "new TCP connection");
                            let tx = sender.clone();
                            let tags = self.tags.clone();
                            let peer = peer_addr.to_string();
                            tokio::spawn(async move {
                                let reader = tokio::io::BufReader::new(stream);
                                let mut lines = reader.lines();
                                while let Ok(Some(line)) = lines.next_line().await {
                                    if line.is_empty() {
                                        continue;
                                    }
                                    let mut event = Event::new(&line);
                                    event.set("host", EventValue::String(peer.clone()));
                                    for tag in &tags {
                                        event.add_tag(tag);
                                    }
                                    if tx.send(event).await.is_err() {
                                        break;
                                    }
                                }
                                debug!(peer = %peer, "TCP connection closed");
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "TCP accept error");
                        }
                    }
                }
                () = shutdown.wait() => {
                    info!("TCP input shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_config_with_port() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = TcpInput::from_config(&settings).expect("config");
        assert_eq!(input.port, 5000);
        assert_eq!(input.host, "0.0.0.0");
    }

    #[test]
    fn test_tcp_config_missing_port() {
        let settings = serde_json::json!({});
        assert!(TcpInput::from_config(&settings).is_err());
    }

    #[test]
    fn test_tcp_config_custom_host() {
        let settings = serde_json::json!({ "host": "127.0.0.1", "port": 9000 });
        let input = TcpInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "127.0.0.1");
    }

    #[test]
    fn test_tcp_config_with_tags() {
        let settings = serde_json::json!({ "port": 5000, "tags": ["tcp_input"] });
        let input = TcpInput::from_config(&settings).expect("config");
        assert_eq!(input.tags, vec!["tcp_input"]);
    }

    #[test]
    fn test_tcp_name() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = TcpInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "tcp");
    }

    #[tokio::test]
    async fn test_tcp_input_accept_and_shutdown() {
        let settings = serde_json::json!({ "port": 19876, "host": "127.0.0.1" });
        let mut input = TcpInput::from_config(&settings).expect("config");
        let (tx, _rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        // Give it a moment to bind, then send data
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect and send a line
        if let Ok(mut stream) = tokio::net::TcpStream::connect("127.0.0.1:19876").await {
            use tokio::io::AsyncWriteExt;
            let _ = stream.write_all(b"hello from test\n").await;
            let _ = stream.flush().await;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        ctrl.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }
}
