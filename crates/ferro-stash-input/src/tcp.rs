// SPDX-License-Identifier: Apache-2.0
//! TCP input plugin — accepts connections and reads line-delimited data.

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

use crate::exec::{event_from_line, LineCodec};

/// Maximum number of bytes a single line may accumulate before a newline is
/// seen on an accepted TCP connection.
///
/// `BufReader::lines()` grows an unbounded `String` per connection; a remote
/// client (this listener is unauthenticated) can stream bytes without ever
/// sending `\n`, inflating the per-connection buffer until the process OOMs,
/// and many connections amplify it. We cap the accumulated line length and
/// drop the connection once it is exceeded. 16 MB is far above any legitimate
/// single log line while keeping per-connection memory bounded.
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Outcome of a single bounded line read from a TCP connection.
#[derive(Debug, PartialEq, Eq)]
enum LineRead {
    /// A complete line (terminated by `\n`) was read into the buffer.
    Line,
    /// The peer closed the connection. The buffer holds any trailing bytes
    /// received before EOF (a final line without a newline, within the cap).
    Eof,
    /// The accumulated bytes exceeded the cap before a newline was seen.
    Overflow,
}

/// Read one line from `reader` into `buf`, enforcing a hard cap on the number
/// of bytes accumulated before a newline.
///
/// This replaces `BufReader::lines()` (which grows an unbounded `String`) so an
/// unauthenticated client cannot stream bytes without a `\n` to OOM the
/// process. Crucially we do *not* use `read_until`, which would itself buffer
/// the whole unterminated stream in a single call; instead we drive the
/// `BufReader`'s internal buffer directly via `fill_buf`/`consume` and refuse
/// to grow `buf` past `cap`. The trailing `\n` (and a preceding `\r`) is
/// stripped by [`decode_line`], not here, so callers see the raw line bytes
/// up to and including the newline.
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
            // EOF: report whether any unterminated bytes remain (the caller
            // emits them if within cap).
            return Ok(LineRead::Eof);
        }
        // Find a newline within the currently-buffered chunk.
        match available.iter().position(|&b| b == b'\n') {
            Some(idx) => {
                let take = idx + 1; // include the '\n'
                                    // Adding this chunk-up-to-newline must not exceed the cap.
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
                // No newline in this chunk; accumulating it would exceed the
                // cap → the peer is streaming an over-long line. Refuse.
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

/// Decode a raw line buffer into a `String`, stripping a trailing `\n` and an
/// optional preceding `\r` (matching the previous `lines()` behavior). Returns
/// `None` for an empty buffer.
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
pub struct TcpInput {
    host: String,
    port: u16,
    tags: Vec<String>,
    codec: LineCodec,
}

impl TcpInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        // `port` is required for the TCP input; absence is an error rather than
        // a default. When present, validate the range so `port => 70000` fails
        // loudly instead of silently truncating via `as u16`.
        if settings.get_u64("port").is_none() {
            return Err(FerroStashError::Input {
                plugin: "tcp".to_string(),
                message: "port is required".to_string(),
            });
        }
        let port = settings
            .get_port("port", 0)
            .map_err(|message| FerroStashError::Input {
                plugin: "tcp".to_string(),
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

        let codec = LineCodec::from_settings(settings);

        Ok(Self {
            host,
            port,
            tags,
            codec,
        })
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
                            // Logstash's tcp input sets `host` to the peer's hostname/IP
                            // (no port). We use the IP so multiple connections from the
                            // same client collapse to a single value, matching Logstash.
                            let peer_host = peer_addr.ip().to_string();
                            // Keep the full peer (IP:port) for log lines so an operator
                            // can still correlate connections in error / overflow logs.
                            let peer_log = peer_addr.to_string();
                            let codec = self.codec;
                            tokio::spawn(async move {
                                let mut reader = tokio::io::BufReader::new(stream);
                                let mut buf: Vec<u8> = Vec::new();
                                loop {
                                    buf.clear();
                                    // Bounded line read: cap the accumulated bytes so an
                                    // unauthenticated client streaming without a newline
                                    // cannot grow this buffer until OOM.
                                    match read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES)
                                        .await
                                    {
                                        Ok(LineRead::Line) => {
                                            if let Some(line) = decode_line(&buf) {
                                                if !line.is_empty() {
                                                    let mut event = event_from_line(&line, codec);
                                                    event.set(
                                                        "host",
                                                        EventValue::String(peer_host.clone()),
                                                    );
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
                                            // Final line without a trailing newline (within cap).
                                            if let Some(line) = decode_line(&buf) {
                                                if !line.is_empty() {
                                                    let mut event = event_from_line(&line, codec);
                                                    event.set(
                                                        "host",
                                                        EventValue::String(peer_host.clone()),
                                                    );
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
                                                peer = %peer_log,
                                                cap = MAX_LINE_BYTES,
                                                "TCP line exceeds max length; dropping connection"
                                            );
                                            break;
                                        }
                                        Err(e) => {
                                            warn!(peer = %peer_log, error = %e, "TCP read error");
                                            break;
                                        }
                                    }
                                }
                                debug!(peer = %peer_log, "TCP connection closed");
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
    fn test_tcp_config_out_of_range_port_rejected() {
        // Regression: a port above 65535 must fail loudly at config time rather
        // than silently truncating via `as u16` (70000 as u16 == 4464).
        let settings = serde_json::json!({ "port": 70000 });
        let err = TcpInput::from_config(&settings).expect_err("port 70000 must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("70000") && msg.contains("port"),
            "expected an out-of-range port error, got: {msg}"
        );
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
    fn test_tcp_config_codec_defaults_to_plain() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = TcpInput::from_config(&settings).expect("config");
        assert_eq!(input.codec, LineCodec::Plain);
    }

    #[test]
    fn test_tcp_config_codec_json_and_json_lines() {
        let s_json = TcpInput::from_config(&serde_json::json!({ "port": 5000, "codec": "json" }))
            .expect("config json");
        assert_eq!(s_json.codec, LineCodec::Json);
        let s_jl =
            TcpInput::from_config(&serde_json::json!({ "port": 5000, "codec": "json_lines" }))
                .expect("config json_lines");
        assert_eq!(s_jl.codec, LineCodec::Json);
    }

    /// Round-trip: send NDJSON over a real TCP socket with `codec => "json_lines"`
    /// and verify that the JSON object's keys become top-level fields on the
    /// event (the bug the Marketplace E2E surfaced: TCP input was emitting the
    /// raw JSON text as `message` regardless of codec).
    #[tokio::test]
    async fn test_tcp_json_lines_codec_decodes_to_top_level_fields() {
        use ferro_stash_core::shutdown::ShutdownController;
        // Reserve an ephemeral port without holding it: bind, read, drop.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind probe");
        let port = probe.local_addr().expect("local_addr").port();
        drop(probe);

        let mut input = TcpInput::from_config(&serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "codec": "json_lines"
        }))
        .expect("config");

        let (tx, mut rx) = mpsc::channel(8);
        let (controller, signal) = ShutdownController::new();
        let task = tokio::spawn(async move { input.run(tx, signal).await });

        // Wait until the listener is up by retrying connect briefly.
        let mut stream = None;
        for _ in 0..50 {
            if let Ok(s) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                stream = Some(s);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        use tokio::io::AsyncWriteExt;
        let mut stream = stream.expect("connect");
        stream
            .write_all(b"{\"id\":42,\"msg\":\"hello\"}\n")
            .await
            .expect("write_all");
        stream.flush().await.expect("flush");
        drop(stream);

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("event timeout")
            .expect("event");

        assert_eq!(event.get("id"), Some(&EventValue::Integer(42)));
        assert_eq!(event.get("msg"), Some(&EventValue::String("hello".into())));
        // host is the peer IP only (no port), matching Logstash's tcp input.
        assert_eq!(
            event.get("host"),
            Some(&EventValue::String("127.0.0.1".into()))
        );

        controller.shutdown();
        let _ = task.await;
    }

    #[test]
    fn test_tcp_name() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = TcpInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "tcp");
    }

    #[test]
    fn test_decode_line_strips_terminators() {
        assert_eq!(decode_line(b"hello\n").as_deref(), Some("hello"));
        assert_eq!(decode_line(b"hello\r\n").as_deref(), Some("hello"));
        assert_eq!(decode_line(b"hello").as_deref(), Some("hello"));
        assert_eq!(decode_line(b"").as_deref(), None);
        // A lone trailing '\r' without '\n' is preserved (only '\r\n' is stripped).
        assert_eq!(decode_line(b"hello\r").as_deref(), Some("hello\r"));
    }

    #[tokio::test]
    async fn test_read_line_capped_reads_lines() {
        let data = b"line one\nline two\n".to_vec();
        let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(data));
        let mut buf = Vec::new();

        let r = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Line);
        assert_eq!(decode_line(&buf).as_deref(), Some("line one"));

        buf.clear();
        let r = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Line);
        assert_eq!(decode_line(&buf).as_deref(), Some("line two"));

        buf.clear();
        let r = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Eof);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn test_read_line_capped_emits_final_unterminated_line() {
        // A last line without a trailing newline is delivered on EOF.
        let data = b"no newline at end".to_vec();
        let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(data));
        let mut buf = Vec::new();
        let r = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Eof);
        assert_eq!(decode_line(&buf).as_deref(), Some("no newline at end"));
    }

    #[tokio::test]
    async fn test_read_line_capped_rejects_overlong_line_without_unbounded_growth() {
        // Regression (HIGH): a remote peer streaming bytes with NO newline must
        // not grow the per-connection buffer past the cap. We feed `cap + 1`
        // bytes (no '\n') with a small cap and assert Overflow is returned and
        // the accumulated buffer never exceeds the cap.
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

    #[tokio::test]
    async fn test_read_line_capped_accepts_line_exactly_at_cap() {
        // A line whose bytes (incl. the newline) total exactly `cap` is allowed.
        let cap: usize = 16;
        let mut data = vec![b'a'; cap - 1];
        data.push(b'\n'); // total == cap
        let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(data));
        let mut buf = Vec::new();
        let r = read_line_capped(&mut reader, &mut buf, cap)
            .await
            .expect("read");
        assert_eq!(r, LineRead::Line);
        assert_eq!(buf.len(), cap);
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
