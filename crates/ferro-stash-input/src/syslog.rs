// SPDX-License-Identifier: Apache-2.0
//! Syslog input plugin — receives syslog messages via TCP or UDP.
//!
//! Supports RFC 3164 and RFC 5424 formats.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug)]
pub struct SyslogInput {
    host: String,
    port: u16,
    protocol: SyslogProtocol,
    tags: Vec<String>,
}

#[derive(Debug, Clone)]
enum SyslogProtocol {
    Tcp,
    Udp,
}

impl SyslogInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        let port = settings
            .get("port")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(514) as u16;
        let protocol = match settings
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("udp")
        {
            "tcp" => SyslogProtocol::Tcp,
            _ => SyslogProtocol::Udp,
        };
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
            protocol,
            tags,
        })
    }
}

/// Parse a syslog priority value into facility and severity.
fn parse_priority(pri: u32) -> (u32, u32) {
    let facility = pri >> 3;
    let severity = pri & 0x07;
    (facility, severity)
}

fn severity_name(severity: u32) -> &'static str {
    match severity {
        0 => "emergency",
        1 => "alert",
        2 => "critical",
        3 => "error",
        4 => "warning",
        5 => "notice",
        6 => "informational",
        7 => "debug",
        _ => "unknown",
    }
}

fn facility_name(facility: u32) -> &'static str {
    match facility {
        0 => "kernel",
        1 => "user",
        2 => "mail",
        3 => "daemon",
        4 => "auth",
        5 => "syslog",
        6 => "lpr",
        7 => "news",
        8 => "uucp",
        9 => "cron",
        10 => "authpriv",
        11 => "ftp",
        16..=23 => "local",
        _ => "unknown",
    }
}

fn parse_syslog_message(raw: &str) -> Event {
    let mut event = Event::new(raw);

    // Try to parse RFC 3164: <PRI>TIMESTAMP HOSTNAME APP-NAME: MESSAGE
    if raw.starts_with('<') {
        if let Some(end_pri) = raw.find('>') {
            if let Ok(pri) = raw[1..end_pri].parse::<u32>() {
                let (facility, severity) = parse_priority(pri);
                event.set(
                    "facility",
                    EventValue::String(facility_name(facility).to_string()),
                );
                event.set("facility_code", EventValue::Integer(i64::from(facility)));
                event.set(
                    "severity",
                    EventValue::String(severity_name(severity).to_string()),
                );
                event.set("severity_code", EventValue::Integer(i64::from(severity)));

                let rest = &raw[end_pri + 1..];
                // Try to find hostname and message
                let parts: Vec<&str> = rest.splitn(3, ' ').collect();
                if parts.len() >= 2 {
                    // Skip timestamp portion, try hostname
                    // RFC 3164 timestamp is like "Jan  1 00:00:00"
                    // For simplicity, set the remaining as message
                    event.set_message(rest.trim());
                }
            }
        }
    }

    event.set("type", EventValue::String("syslog".to_string()));
    event
}

#[async_trait]
impl InputPlugin for SyslogInput {
    fn name(&self) -> &'static str {
        "syslog"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        info!(address = %addr, protocol = ?self.protocol, "syslog input starting");

        match self.protocol {
            SyslogProtocol::Udp => {
                let socket = UdpSocket::bind(&addr)
                    .await
                    .map_err(|e| FerroStashError::Input {
                        plugin: "syslog".to_string(),
                        message: format!("bind error: {e}"),
                    })?;

                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        result = socket.recv_from(&mut buf) => {
                            match result {
                                Ok((len, peer)) => {
                                    let data = &buf[..len];
                                    let text = String::from_utf8_lossy(data);
                                    let mut event = parse_syslog_message(text.trim());
                                    event.set("host", EventValue::String(peer.ip().to_string()));
                                    for tag in &self.tags {
                                        event.add_tag(tag);
                                    }
                                    if sender.send(event).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => warn!(error = %e, "syslog UDP recv error"),
                            }
                        }
                        () = shutdown.wait() => break,
                    }
                }
            }
            SyslogProtocol::Tcp => {
                let listener =
                    TcpListener::bind(&addr)
                        .await
                        .map_err(|e| FerroStashError::Input {
                            plugin: "syslog".to_string(),
                            message: format!("bind error: {e}"),
                        })?;

                loop {
                    tokio::select! {
                        result = listener.accept() => {
                            match result {
                                Ok((stream, peer)) => {
                                    let tx = sender.clone();
                                    let tags = self.tags.clone();
                                    let peer_str = peer.ip().to_string();
                                    tokio::spawn(async move {
                                        let reader = tokio::io::BufReader::new(stream);
                                        let mut lines = reader.lines();
                                        while let Ok(Some(line)) = lines.next_line().await {
                                            if line.is_empty() { continue; }
                                            let mut event = parse_syslog_message(&line);
                                            event.set("host", EventValue::String(peer_str.clone()));
                                            for tag in &tags {
                                                event.add_tag(tag);
                                            }
                                            if tx.send(event).await.is_err() {
                                                break;
                                            }
                                        }
                                    });
                                }
                                Err(e) => warn!(error = %e, "syslog TCP accept error"),
                            }
                        }
                        () = shutdown.wait() => break,
                    }
                }
            }
        }

        info!("syslog input stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_priority() {
        let (facility, severity) = parse_priority(13); // user.notice
        assert_eq!(facility, 1); // user
        assert_eq!(severity, 5); // notice
    }

    #[test]
    fn test_parse_syslog_message() {
        let msg = "<13>Jan  1 00:00:00 myhost myapp: test message";
        let event = parse_syslog_message(msg);
        assert_eq!(
            event.get("facility"),
            Some(&EventValue::String("user".to_string()))
        );
        assert_eq!(
            event.get("severity"),
            Some(&EventValue::String("notice".to_string()))
        );
    }

    #[test]
    fn test_severity_names() {
        assert_eq!(severity_name(0), "emergency");
        assert_eq!(severity_name(3), "error");
        assert_eq!(severity_name(7), "debug");
    }

    #[test]
    fn test_severity_names_all() {
        assert_eq!(severity_name(1), "alert");
        assert_eq!(severity_name(2), "critical");
        assert_eq!(severity_name(4), "warning");
        assert_eq!(severity_name(5), "notice");
        assert_eq!(severity_name(6), "informational");
        assert_eq!(severity_name(99), "unknown");
    }

    #[test]
    fn test_facility_names() {
        assert_eq!(facility_name(0), "kernel");
        assert_eq!(facility_name(1), "user");
        assert_eq!(facility_name(2), "mail");
        assert_eq!(facility_name(3), "daemon");
        assert_eq!(facility_name(4), "auth");
        assert_eq!(facility_name(5), "syslog");
        assert_eq!(facility_name(16), "local");
        assert_eq!(facility_name(99), "unknown");
    }

    #[test]
    fn test_parse_priority_kernel_emergency() {
        let (facility, severity) = parse_priority(0);
        assert_eq!(facility, 0); // kernel
        assert_eq!(severity, 0); // emergency
    }

    #[test]
    fn test_parse_priority_auth_error() {
        // auth=4, error=3 → pri = 4*8+3 = 35
        let (facility, severity) = parse_priority(35);
        assert_eq!(facility, 4);
        assert_eq!(severity, 3);
    }

    #[test]
    fn test_parse_syslog_message_with_hostname() {
        let msg = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed";
        let event = parse_syslog_message(msg);
        assert_eq!(
            event.get("facility"),
            Some(&EventValue::String("auth".to_string()))
        );
        assert_eq!(
            event.get("severity"),
            Some(&EventValue::String("critical".to_string()))
        );
        assert_eq!(
            event.get("type"),
            Some(&EventValue::String("syslog".to_string()))
        );
    }

    #[test]
    fn test_parse_syslog_message_no_pri() {
        let msg = "just a plain message";
        let event = parse_syslog_message(msg);
        assert_eq!(event.message(), Some("just a plain message"));
        assert_eq!(
            event.get("type"),
            Some(&EventValue::String("syslog".to_string()))
        );
    }

    #[test]
    fn test_syslog_config_defaults() {
        let settings = serde_json::json!({});
        let input = SyslogInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "0.0.0.0");
        assert_eq!(input.port, 514);
        assert_eq!(input.name(), "syslog");
    }

    #[test]
    fn test_syslog_config_tcp() {
        let settings = serde_json::json!({
            "protocol": "tcp",
            "port": 1514,
            "tags": ["syslog_tcp"]
        });
        let input = SyslogInput::from_config(&settings).expect("config");
        assert_eq!(input.port, 1514);
        assert!(matches!(input.protocol, SyslogProtocol::Tcp));
    }
}
