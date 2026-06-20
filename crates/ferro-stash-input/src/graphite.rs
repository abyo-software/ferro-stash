// SPDX-License-Identifier: Apache-2.0
//! Graphite input plugin — accepts the Carbon plaintext protocol over TCP.
//!
//! Listens on TCP and parses newline-delimited metric lines of the form
//! `metric.path value timestamp` into events with the fields:
//!
//! - `metric` (string) — the metric path,
//! - `value` (float) — the numeric sample,
//! - `timestamp` (integer) — the Unix epoch seconds, when present.
//!
//! Config keys (Logstash-compatible): `host` (default `0.0.0.0`), `port`
//! (default `2003`).

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Maximum bytes a single line may accumulate before a newline is seen. The
/// listener is unauthenticated; a peer streaming without `\n` could otherwise
/// grow the per-connection buffer until OOM. 1 MB is far above any legitimate
/// Graphite metric line.
const MAX_LINE_BYTES: usize = 1024 * 1024;

/// Outcome of a single bounded line read.
#[derive(Debug, PartialEq, Eq)]
enum LineRead {
    Line,
    Eof,
    Overflow,
}

/// Read one line into `buf`, enforcing a hard cap on accumulated bytes before a
/// newline (mirrors the TCP input's DoS-safe reader rather than the unbounded
/// `BufReader::lines()`).
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

/// Strip a trailing `\n` (and a preceding `\r`) and decode to a `String`.
fn decode_line(buf: &[u8]) -> Option<String> {
    if buf.is_empty() {
        return None;
    }
    let mut end = buf.len();
    if buf[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && buf[end - 1] == b'\r' {
            end -= 1;
        }
    }
    Some(String::from_utf8_lossy(&buf[..end]).into_owned())
}

/// Parse a Carbon plaintext line `metric.path value [timestamp]` into an event.
///
/// Returns `None` when the line is blank, missing the value, or the value does
/// not parse as a float. The timestamp is optional and tolerated when missing
/// or non-integer (some senders use `-1` for "now").
fn parse_graphite_line(line: &str) -> Option<Event> {
    let mut parts = line.split_whitespace();
    let metric = parts.next()?;
    let value: f64 = parts.next()?.parse().ok()?;

    let mut event = Event::empty();
    event.set("metric", EventValue::String(metric.to_string()));
    event.set("value", EventValue::Float(value));
    if let Some(ts) = parts.next() {
        if let Ok(t) = ts.parse::<i64>() {
            event.set("timestamp", EventValue::Integer(t));
        }
    }
    Some(event)
}

#[derive(Debug)]
pub struct GraphiteInput {
    host: String,
    port: u16,
    tags: Vec<String>,
}

impl GraphiteInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        // `port` defaults to the Carbon plaintext port 2003. A present-but-
        // invalid value fails loudly (not truncating via `as u16`).
        let port = settings
            .get_port("port", 2003)
            .map_err(|message| FerroStashError::Input {
                plugin: "graphite".to_string(),
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

        Ok(Self { host, port, tags })
    }
}

#[async_trait]
impl InputPlugin for GraphiteInput {
    fn name(&self) -> &'static str {
        "graphite"
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
                plugin: "graphite".to_string(),
                message: format!("bind error: {e}"),
            })?;
        info!(address = %addr, "Graphite input listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer = %peer_addr, "new Graphite connection");
                            let tx = sender.clone();
                            let tags = self.tags.clone();
                            let peer = peer_addr.ip().to_string();
                            tokio::spawn(async move {
                                let mut reader = tokio::io::BufReader::new(stream);
                                let mut buf: Vec<u8> = Vec::new();
                                loop {
                                    buf.clear();
                                    match read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES).await {
                                        Ok(LineRead::Line) | Ok(LineRead::Eof) => {
                                            let is_eof = buf.last() != Some(&b'\n');
                                            if let Some(line) = decode_line(&buf) {
                                                if let Some(mut event) = parse_graphite_line(&line) {
                                                    event.set("host", EventValue::String(peer.clone()));
                                                    for tag in &tags {
                                                        event.add_tag(tag);
                                                    }
                                                    if tx.send(event).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                            if is_eof {
                                                break;
                                            }
                                        }
                                        Ok(LineRead::Overflow) => {
                                            warn!(peer = %peer, cap = MAX_LINE_BYTES, "Graphite line exceeds max length; dropping connection");
                                            break;
                                        }
                                        Err(e) => {
                                            warn!(peer = %peer, error = %e, "Graphite read error");
                                            break;
                                        }
                                    }
                                }
                                debug!(peer = %peer, "Graphite connection closed");
                            });
                        }
                        Err(e) => warn!(error = %e, "Graphite accept error"),
                    }
                }
                () = shutdown.wait() => {
                    info!("Graphite input shutting down");
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

    // ----- Config tests -----

    #[test]
    fn test_graphite_config_defaults() {
        let input = GraphiteInput::from_config(&serde_json::json!({})).expect("config");
        assert_eq!(input.host, "0.0.0.0");
        assert_eq!(input.port, 2003);
        assert_eq!(input.name(), "graphite");
    }

    #[test]
    fn test_graphite_config_custom() {
        let settings = serde_json::json!({ "host": "127.0.0.1", "port": 2004, "tags": ["g"] });
        let input = GraphiteInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "127.0.0.1");
        assert_eq!(input.port, 2004);
        assert_eq!(input.tags, vec!["g"]);
    }

    #[test]
    fn test_graphite_config_out_of_range_port_rejected() {
        let settings = serde_json::json!({ "port": 70000 });
        let err = GraphiteInput::from_config(&settings).expect_err("port 70000 must be rejected");
        assert!(err.to_string().contains("70000"), "got: {err}");
    }

    // ----- Parse tests -----

    #[test]
    fn test_parse_graphite_line_full() {
        let event = parse_graphite_line("cpu.load 0.9 1592000000").expect("parse");
        assert_eq!(
            event.get("metric"),
            Some(&EventValue::String("cpu.load".into()))
        );
        assert_eq!(event.get("value"), Some(&EventValue::Float(0.9)));
        assert_eq!(
            event.get("timestamp"),
            Some(&EventValue::Integer(1592000000))
        );
    }

    #[test]
    fn test_parse_graphite_line_integer_value() {
        let event = parse_graphite_line("mem.used 42 1592000000").expect("parse");
        assert_eq!(event.get("value"), Some(&EventValue::Float(42.0)));
    }

    #[test]
    fn test_parse_graphite_line_missing_timestamp_tolerated() {
        let event = parse_graphite_line("disk.free 1.5").expect("parse");
        assert_eq!(event.get("value"), Some(&EventValue::Float(1.5)));
        assert!(event.get("timestamp").is_none());
    }

    #[test]
    fn test_parse_graphite_line_rejects_non_numeric_value() {
        assert!(parse_graphite_line("metric notanumber 1").is_none());
    }

    #[test]
    fn test_parse_graphite_line_rejects_value_only() {
        assert!(parse_graphite_line("metric_only").is_none());
        assert!(parse_graphite_line("").is_none());
    }

    #[test]
    fn test_decode_line_strips_terminators() {
        assert_eq!(decode_line(b"a b 1\n").as_deref(), Some("a b 1"));
        assert_eq!(decode_line(b"a b 1\r\n").as_deref(), Some("a b 1"));
        assert_eq!(decode_line(b"a b 1").as_deref(), Some("a b 1"));
        assert_eq!(decode_line(b""), None);
    }

    // ----- Behaviour test over a loopback TCP socket -----

    #[tokio::test]
    async fn test_graphite_input_receives_metric() {
        let settings = serde_json::json!({ "port": 19913, "host": "127.0.0.1" });
        let mut input = GraphiteInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(16);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut stream = tokio::net::TcpStream::connect("127.0.0.1:19913")
            .await
            .expect("connect");
        use tokio::io::AsyncWriteExt;
        stream
            .write_all(b"servers.web1.cpu 0.75 1592000000\n")
            .await
            .expect("write");
        stream.flush().await.expect("flush");

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("recv timeout")
            .expect("event");
        assert_eq!(
            event.get("metric"),
            Some(&EventValue::String("servers.web1.cpu".into()))
        );
        assert_eq!(event.get("value"), Some(&EventValue::Float(0.75)));
        assert_eq!(
            event.get("timestamp"),
            Some(&EventValue::Integer(1592000000))
        );

        ctrl.shutdown();
        let _ = handle.await;
    }
}
