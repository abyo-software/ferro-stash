// SPDX-License-Identifier: Apache-2.0
//! Syslog input plugin — receives syslog messages via TCP or UDP.
//!
//! Supports RFC 3164 and RFC 5424 formats.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Maximum number of bytes a single syslog line may accumulate before a
/// newline is seen on a TCP connection.
///
/// As with the TCP input, `BufReader::lines()` grows an unbounded `String` per
/// connection, so an unauthenticated client streaming bytes without `\n` could
/// OOM the process. We cap the accumulated line length and drop the connection
/// on overflow. (UDP is already bounded by a fixed 65536-byte datagram
/// buffer and is left unchanged.) 16 MB matches the TCP input cap.
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Outcome of a single bounded line read from a syslog TCP connection.
#[derive(Debug, PartialEq, Eq)]
enum LineRead {
    /// A complete line (terminated by `\n`) was read into the buffer.
    Line,
    /// The peer closed the connection; the buffer holds any trailing bytes.
    Eof,
    /// The accumulated bytes exceeded the cap before a newline was seen.
    Overflow,
}

/// Read one line from `reader` into `buf`, enforcing a hard cap on the number
/// of bytes accumulated before a newline (mirrors `tcp::read_line_capped`).
///
/// We drive the `BufReader`'s internal buffer via `fill_buf`/`consume` rather
/// than `read_until` (which would buffer the whole unterminated stream in a
/// single call) so the per-connection buffer can never exceed `cap`.
async fn read_line_capped<R>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    cap: usize,
) -> std::io::Result<LineRead>
where
    R: AsyncBufReadExt + Unpin,
{
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(LineRead::Eof);
        }
        match available.iter().position(|&b| b == b'\n') {
            Some(idx) => {
                let take = idx + 1;
                if buf.len() + take > cap {
                    reader.consume(take);
                    return Ok(LineRead::Overflow);
                }
                buf.extend_from_slice(&available[..take]);
                reader.consume(take);
                return Ok(LineRead::Line);
            }
            None => {
                let len = available.len();
                if buf.len() + len > cap {
                    reader.consume(len);
                    return Ok(LineRead::Overflow);
                }
                buf.extend_from_slice(available);
                reader.consume(len);
            }
        }
    }
}

/// Decode a raw syslog line buffer into a `String`, stripping a trailing `\n`
/// and an optional preceding `\r`. Returns `None` for an empty buffer.
fn decode_line(buf: &[u8]) -> Option<String> {
    if buf.is_empty() {
        return None;
    }
    let mut end = buf.len();
    if end > 0 && buf[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && buf[end - 1] == b'\r' {
            end -= 1;
        }
    }
    Some(String::from_utf8_lossy(&buf[..end]).into_owned())
}

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
            .get_port("port", 514)
            .map_err(|message| FerroStashError::Input {
                plugin: "syslog".to_string(),
                message,
            })?;
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
                                        let mut reader = tokio::io::BufReader::new(stream);
                                        let mut buf: Vec<u8> = Vec::new();
                                        loop {
                                            buf.clear();
                                            // Bounded line read: cap accumulated bytes so an
                                            // unauthenticated client streaming without a
                                            // newline cannot grow the buffer until OOM.
                                            match read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES).await {
                                                Ok(LineRead::Line) => {
                                                    if let Some(line) = decode_line(&buf) {
                                                        if !line.is_empty() {
                                                            let mut event = parse_syslog_message(&line);
                                                            event.set("host", EventValue::String(peer_str.clone()));
                                                            for tag in &tags {
                                                                event.add_tag(tag);
                                                            }
                                                            if tx.send(event).await.is_err() {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                Ok(LineRead::Eof) => {
                                                    if let Some(line) = decode_line(&buf) {
                                                        if !line.is_empty() {
                                                            let mut event = parse_syslog_message(&line);
                                                            event.set("host", EventValue::String(peer_str.clone()));
                                                            for tag in &tags {
                                                                event.add_tag(tag);
                                                            }
                                                            let _ = tx.send(event).await;
                                                        }
                                                    }
                                                    break;
                                                }
                                                Ok(LineRead::Overflow) => {
                                                    warn!(
                                                        peer = %peer_str,
                                                        cap = MAX_LINE_BYTES,
                                                        "syslog TCP line exceeds max length; dropping connection"
                                                    );
                                                    break;
                                                }
                                                Err(e) => {
                                                    warn!(peer = %peer_str, error = %e, "syslog TCP read error");
                                                    break;
                                                }
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

    #[tokio::test]
    async fn test_syslog_read_line_capped_reads_lines() {
        let data = b"<13>line one\n<13>line two\n".to_vec();
        let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(data));
        let mut buf = Vec::new();

        let r = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Line);
        assert_eq!(decode_line(&buf).as_deref(), Some("<13>line one"));

        buf.clear();
        let r = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Line);
        assert_eq!(decode_line(&buf).as_deref(), Some("<13>line two"));
    }

    #[tokio::test]
    async fn test_syslog_read_line_capped_rejects_overlong_line() {
        // Regression (HIGH): a syslog TCP peer streaming bytes with NO newline
        // must not grow the per-connection buffer past the cap.
        let cap: usize = 1024;
        let data = vec![b'x'; cap + 1]; // larger than cap, no newline
        let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(data));
        let mut buf = Vec::new();
        let r = read_line_capped(&mut reader, &mut buf, cap)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Overflow);
        assert!(
            buf.len() <= cap,
            "accumulated buffer must stay within cap, got {}",
            buf.len()
        );
    }

    #[test]
    fn test_syslog_decode_line_strips_terminators() {
        assert_eq!(decode_line(b"<13>msg\n").as_deref(), Some("<13>msg"));
        assert_eq!(decode_line(b"<13>msg\r\n").as_deref(), Some("<13>msg"));
        assert_eq!(decode_line(b"<13>msg").as_deref(), Some("<13>msg"));
        assert_eq!(decode_line(b"").as_deref(), None);
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
