// SPDX-License-Identifier: Apache-2.0
//! TCP output plugin — sends events to a TCP endpoint.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[allow(dead_code)]
#[derive(Debug)]
pub struct TcpOutput {
    host: String,
    port: u16,
    codec: TcpCodec,
    reconnect_interval_secs: u64,
    condition: Option<Condition>,
}

#[derive(Debug)]
enum TcpCodec {
    JsonLines,
    Line,
}

impl TcpOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FerroStashError::Output {
                plugin: "tcp".to_string(),
                message: "host is required".to_string(),
            })?
            .to_string();
        // `port` is mandatory for the TCP output: absence is an error, and a
        // present-but-out-of-range value must fail loudly (not truncate via
        // `as u16`). `get_port` validates the range; we reject absence first so
        // the historical "port is required" error is preserved.
        if settings.get("port").is_none() {
            return Err(FerroStashError::Output {
                plugin: "tcp".to_string(),
                message: "port is required".to_string(),
            });
        }
        let port = settings
            .get_port("port", 0)
            .map_err(|message| FerroStashError::Output {
                plugin: "tcp".to_string(),
                message,
            })?;
        let codec = match settings
            .get("codec")
            .and_then(|v| v.as_str())
            .unwrap_or("json_lines")
        {
            "line" => TcpCodec::Line,
            _ => TcpCodec::JsonLines,
        };
        let reconnect_interval_secs = settings
            .get("reconnect_interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(10);

        Ok(Self {
            host,
            port,
            codec,
            reconnect_interval_secs,
            condition,
        })
    }
}

#[async_trait]
impl OutputPlugin for TcpOutput {
    fn name(&self) -> &'static str {
        "tcp"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| FerroStashError::Output {
                plugin: "tcp".to_string(),
                message: format!("connect error to {addr}: {e}"),
            })?;

        for event in &events {
            let line = match self.codec {
                TcpCodec::JsonLines => format!("{}\n", event.to_json_string()),
                TcpCodec::Line => format!("{}\n", event.message().unwrap_or("")),
            };
            stream
                .write_all(line.as_bytes())
                .await
                .map_err(|e| FerroStashError::Output {
                    plugin: "tcp".to_string(),
                    message: format!("write error: {e}"),
                })?;
        }

        stream.flush().await.map_err(|e| FerroStashError::Output {
            plugin: "tcp".to_string(),
            message: format!("flush error: {e}"),
        })?;

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
    fn test_tcp_output_config() {
        let settings = serde_json::json!({
            "host": "localhost",
            "port": 5000
        });
        let output = TcpOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.host, "localhost");
        assert_eq!(output.port, 5000);
        assert_eq!(output.name(), "tcp");
    }

    #[test]
    fn test_tcp_output_missing_host() {
        let settings = serde_json::json!({ "port": 5000 });
        assert!(TcpOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_tcp_output_missing_port() {
        let settings = serde_json::json!({ "host": "localhost" });
        assert!(TcpOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_tcp_output_port_out_of_range_rejected() {
        // An out-of-range port (e.g. 70000) must fail loudly at config time
        // rather than silently truncating (70000 as u16 == 4464) and
        // connecting to the WRONG endpoint.
        let settings = serde_json::json!({ "host": "localhost", "port": 70000 });
        let err = TcpOutput::from_config(&settings, None)
            .expect_err("out-of-range port must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("70000"), "error should mention the bad port: {msg}");
    }

    #[test]
    fn test_tcp_output_line_codec() {
        let settings = serde_json::json!({
            "host": "localhost",
            "port": 5000,
            "codec": "line"
        });
        let output = TcpOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, TcpCodec::Line));
    }

    #[test]
    fn test_tcp_output_json_codec() {
        let settings = serde_json::json!({
            "host": "localhost",
            "port": 5000,
            "codec": "json_lines"
        });
        let output = TcpOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, TcpCodec::JsonLines));
    }

    #[test]
    fn test_tcp_output_reconnect_interval() {
        let settings = serde_json::json!({
            "host": "localhost",
            "port": 5000,
            "reconnect_interval": 30
        });
        let output = TcpOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.reconnect_interval_secs, 30);
    }

    #[test]
    fn test_tcp_output_no_condition() {
        let settings = serde_json::json!({
            "host": "localhost",
            "port": 5000
        });
        let output = TcpOutput::from_config(&settings, None).expect("config");
        assert!(output.condition().is_none());
    }
}
