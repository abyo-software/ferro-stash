// SPDX-License-Identifier: Apache-2.0
//! UDP output plugin — sends each event (codec-encoded) as a UDP datagram.
//!
//! ```logstash
//! output {
//!   udp {
//!     host  => "127.0.0.1"
//!     port  => 9000
//!     codec => "json"
//!   }
//! }
//! ```
//!
//! Each event is encoded with the configured codec and sent as a single
//! datagram via [`tokio::net::UdpSocket`]. UDP is connectionless and unreliable
//! by design: datagrams may be dropped or reordered by the network, and an
//! over-MTU payload may be fragmented or rejected by the OS — this mirrors
//! Logstash's `udp` output (best-effort, fire-and-forget).

use async_trait::async_trait;
use ferro_stash_codec::{create_codec, resolve_codec, Codec};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use tokio::net::UdpSocket;

#[derive(Debug)]
pub struct UdpOutput {
    host: String,
    port: u16,
    codec: Box<dyn Codec>,
    condition: Option<Condition>,
}

impl UdpOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let err = |m: String| FerroStashError::Output {
            plugin: "udp".to_string(),
            message: m,
        };

        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .ok_or_else(|| err("host is required".to_string()))?
            .to_string();

        // `port` is mandatory; absence is an error and a present-but-out-of-range
        // value must fail loudly (rather than truncating via `as u16`).
        if settings.get("port").is_none() {
            return Err(err("port is required".to_string()));
        }
        let port = ferro_stash_core::settings_helpers::SettingsExt::get_port(settings, "port", 0)
            .map_err(err)?;

        let (codec_name, codec_settings) = resolve_codec(settings, "json");
        let codec = create_codec(&codec_name, &codec_settings)
            .map_err(|e| err(format!("unknown/invalid codec '{codec_name}': {e}")))?;

        Ok(Self {
            host,
            port,
            codec,
            condition,
        })
    }
}

#[async_trait]
impl OutputPlugin for UdpOutput {
    fn name(&self) -> &'static str {
        "udp"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        // Bind an ephemeral local socket. `0.0.0.0:0` works for IPv4 targets;
        // fall back to the IPv6 unspecified address for IPv6-only hosts.
        let socket = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => s,
            Err(_) => UdpSocket::bind("[::]:0")
                .await
                .map_err(|e| FerroStashError::Output {
                    plugin: "udp".to_string(),
                    message: format!("bind error: {e}"),
                })?,
        };

        let addr = (self.host.as_str(), self.port);
        for event in &events {
            let bytes = self.codec.encode(event).map_err(|e| FerroStashError::Output {
                plugin: "udp".to_string(),
                message: format!("codec encode error: {e}"),
            })?;
            socket
                .send_to(&bytes, addr)
                .await
                .map_err(|e| FerroStashError::Output {
                    plugin: "udp".to_string(),
                    message: format!("send error to {}:{}: {e}", self.host, self.port),
                })?;
        }
        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udp_output_config() {
        let settings = serde_json::json!({ "host": "127.0.0.1", "port": 9000 });
        let output = UdpOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.host, "127.0.0.1");
        assert_eq!(output.port, 9000);
        assert_eq!(output.name(), "udp");
    }

    #[test]
    fn test_udp_output_missing_host() {
        let settings = serde_json::json!({ "port": 9000 });
        assert!(UdpOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_udp_output_missing_port() {
        let settings = serde_json::json!({ "host": "127.0.0.1" });
        assert!(UdpOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_udp_output_port_out_of_range_rejected() {
        let settings = serde_json::json!({ "host": "127.0.0.1", "port": 70000 });
        let err = UdpOutput::from_config(&settings, None)
            .expect_err("out-of-range port must be rejected");
        assert!(format!("{err}").contains("70000"));
    }

    #[tokio::test]
    async fn test_udp_output_sends_datagram() {
        // Bind a receiver on an ephemeral port and send one event to it.
        let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("bind receiver");
        let addr = receiver.local_addr().expect("local addr");

        let settings = serde_json::json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "codec": "json"
        });
        let output = UdpOutput::from_config(&settings, None).expect("config");
        output
            .output(vec![Event::new("hello-udp")])
            .await
            .expect("output");

        let mut buf = [0u8; 4096];
        let (n, _src) = receiver.recv_from(&mut buf).await.expect("recv");
        let received = String::from_utf8_lossy(&buf[..n]);
        assert!(
            received.contains("hello-udp"),
            "datagram should contain the message: {received}"
        );
    }

    #[tokio::test]
    async fn test_udp_output_multiple_events_multiple_datagrams() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("bind receiver");
        let addr = receiver.local_addr().expect("local addr");
        let settings = serde_json::json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "codec": "json"
        });
        let output = UdpOutput::from_config(&settings, None).expect("config");
        output
            .output(vec![Event::new("one"), Event::new("two")])
            .await
            .expect("output");

        let mut buf = [0u8; 4096];
        let (n1, _) = receiver.recv_from(&mut buf).await.expect("recv 1");
        let d1 = String::from_utf8_lossy(&buf[..n1]).to_string();
        let (n2, _) = receiver.recv_from(&mut buf).await.expect("recv 2");
        let d2 = String::from_utf8_lossy(&buf[..n2]).to_string();
        let combined = format!("{d1}{d2}");
        assert!(combined.contains("one") && combined.contains("two"));
    }
}
