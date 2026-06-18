// SPDX-License-Identifier: Apache-2.0
//! UDP input plugin — receives datagrams.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug)]
pub struct UdpInput {
    host: String,
    port: u16,
    buffer_size: usize,
    tags: Vec<String>,
}

impl UdpInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        let port = settings
            .get("port")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .ok_or_else(|| FerroStashError::Input {
                plugin: "udp".to_string(),
                message: "port is required".to_string(),
            })? as u16;
        let buffer_size = settings
            .get("buffer_size")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(65536) as usize;
        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            host,
            port,
            buffer_size,
            tags,
        })
    }
}

#[async_trait]
impl InputPlugin for UdpInput {
    fn name(&self) -> &'static str {
        "udp"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let socket = UdpSocket::bind(&addr)
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "udp".to_string(),
                message: format!("bind error: {e}"),
            })?;

        info!(address = %addr, "UDP input listening");

        let mut buf = vec![0u8; self.buffer_size];

        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, peer_addr)) => {
                            let data = &buf[..len];
                            let text = String::from_utf8_lossy(data);
                            let trimmed = text.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            let mut event = Event::new(trimmed);
                            event.set("host", EventValue::String(peer_addr.ip().to_string()));
                            event.set("port", EventValue::Integer(i64::from(peer_addr.port())));
                            for tag in &self.tags {
                                event.add_tag(tag);
                            }
                            if sender.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "UDP recv error");
                        }
                    }
                }
                () = shutdown.wait() => {
                    info!("UDP input shutting down");
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
    fn test_udp_config_with_port() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = UdpInput::from_config(&settings).expect("config");
        assert_eq!(input.port, 5000);
        assert_eq!(input.host, "0.0.0.0");
        assert_eq!(input.buffer_size, 65536);
    }

    #[test]
    fn test_udp_config_missing_port() {
        let settings = serde_json::json!({});
        assert!(UdpInput::from_config(&settings).is_err());
    }

    #[test]
    fn test_udp_config_custom_buffer() {
        let settings = serde_json::json!({ "port": 5000, "buffer_size": 1024 });
        let input = UdpInput::from_config(&settings).expect("config");
        assert_eq!(input.buffer_size, 1024);
    }

    #[test]
    fn test_udp_config_with_tags() {
        let settings = serde_json::json!({ "port": 5000, "tags": ["udp_in"] });
        let input = UdpInput::from_config(&settings).expect("config");
        assert_eq!(input.tags, vec!["udp_in"]);
    }

    #[test]
    fn test_udp_name() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = UdpInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "udp");
    }

    #[tokio::test]
    async fn test_udp_input_recv_and_shutdown() {
        let settings = serde_json::json!({ "port": 19877, "host": "127.0.0.1" });
        let mut input = UdpInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send a UDP datagram
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind");
        sock.send_to(b"hello udp", "127.0.0.1:19877")
            .await
            .expect("send");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let event = rx.try_recv();
        assert!(event.is_ok());
        assert_eq!(event.expect("udp event").message(), Some("hello udp"));

        ctrl.shutdown();
        let _ = handle.await;
    }
}
