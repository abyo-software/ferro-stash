// SPDX-License-Identifier: Apache-2.0
//! GELF input plugin — receives Graylog Extended Log Format messages.
//!
//! Listens on UDP (default) or TCP and decodes GELF payloads into events:
//!
//! - The payload is JSON, optionally gzip- or zlib-compressed. Compression is
//!   detected by magic bytes: `0x1f 0x8b` = gzip, leading `0x78` = zlib;
//!   anything else is treated as raw JSON (a JSON object always starts with
//!   `{` = `0x7b`, so this never collides with a zlib header).
//! - The standard GELF fields are mapped: `short_message` → `message`,
//!   `full_message`, `host`, `level`, `timestamp`. Every additional field is a
//!   custom field prefixed with `_`; the underscore is stripped (e.g.
//!   `_user_id` → `user_id`).
//! - Over TCP, GELF frames are NUL-delimited (`\0`); each frame is decoded
//!   independently.
//!
//! Config keys (Logstash-compatible): `host` (default `0.0.0.0`), `port`
//! (default `12201`), `use_tcp` (default `false`, i.e. UDP).
//!
//! ## Residual
//!
//! **Chunked GELF is not reassembled.** A chunked datagram (magic `0x1e 0x0f`)
//! is detected and dropped with a warning rather than buffered/reassembled.
//! Senders that exceed a single datagram (Graylog chunks payloads above the
//! configured MTU) should disable chunking or use TCP. Single-datagram UDP and
//! NUL-delimited TCP frames are fully supported.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use std::io::Read;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Maximum bytes a single GELF UDP datagram may carry (the max UDP payload).
const UDP_BUFFER_SIZE: usize = 65536;

/// Maximum bytes a single NUL-delimited TCP frame may accumulate before the
/// connection is dropped. This listener is unauthenticated, so a peer streaming
/// bytes without a `\0` delimiter could otherwise grow the per-connection
/// buffer until OOM. 16 MB is far above any legitimate GELF message.
const MAX_TCP_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Outcome of detecting/decompressing a GELF payload prefix.
enum Payload {
    /// Uncompressed-or-decompressed JSON bytes ready to parse.
    Data(Vec<u8>),
    /// A chunked GELF datagram (magic `0x1e 0x0f`) — not reassembled.
    Chunked,
    /// Decompression failed.
    Failed,
}

/// Detect compression by magic bytes and return the JSON payload.
fn decompress(data: &[u8]) -> Payload {
    if data.len() >= 2 && data[0] == 0x1e && data[1] == 0x0f {
        return Payload::Chunked;
    }
    if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
        let mut out = Vec::new();
        let mut decoder = flate2::read::GzDecoder::new(data);
        return match decoder.read_to_end(&mut out) {
            Ok(_) => Payload::Data(out),
            Err(_) => Payload::Failed,
        };
    }
    if !data.is_empty() && data[0] == 0x78 {
        let mut out = Vec::new();
        let mut decoder = flate2::read::ZlibDecoder::new(data);
        return match decoder.read_to_end(&mut out) {
            Ok(_) => Payload::Data(out),
            Err(_) => Payload::Failed,
        };
    }
    Payload::Data(data.to_vec())
}

/// Map a parsed GELF JSON object into an [`Event`]. Returns `None` if the JSON
/// is not an object.
fn gelf_json_to_event(json: &serde_json::Value) -> Option<Event> {
    let obj = json.as_object()?;
    let mut event = Event::empty();

    if let Some(sm) = obj.get("short_message").and_then(|v| v.as_str()) {
        event.set_message(sm);
    }
    if let Some(fm) = obj.get("full_message") {
        event.set("full_message", EventValue::from(fm.clone()));
    }
    if let Some(host) = obj.get("host") {
        event.set("host", EventValue::from(host.clone()));
    }
    if let Some(level) = obj.get("level") {
        event.set("level", EventValue::from(level.clone()));
    }
    if let Some(ts) = obj.get("timestamp") {
        event.set("timestamp", EventValue::from(ts.clone()));
    }

    // Custom fields: GELF requires them to be prefixed with `_`; strip it.
    for (key, value) in obj {
        if let Some(stripped) = key.strip_prefix('_') {
            if !stripped.is_empty() {
                event.set(stripped, EventValue::from(value.clone()));
            }
        }
    }

    Some(event)
}

/// Decode a single raw GELF payload (one datagram or one TCP frame) into an
/// event. Returns `None` for chunked, undecodable, or non-object payloads;
/// callers log the reason.
fn decode_gelf(data: &[u8]) -> Option<Event> {
    let bytes = match decompress(data) {
        Payload::Data(bytes) => bytes,
        Payload::Chunked => {
            warn!("gelf input: chunked datagram not supported (reassembly skipped); dropping");
            return None;
        }
        Payload::Failed => {
            warn!("gelf input: payload decompression failed; dropping");
            return None;
        }
    };
    let json: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(json) => json,
        Err(e) => {
            warn!(error = %e, "gelf input: invalid JSON payload; dropping");
            return None;
        }
    };
    gelf_json_to_event(&json)
}

#[derive(Debug)]
pub struct GelfInput {
    host: String,
    port: u16,
    use_tcp: bool,
    tags: Vec<String>,
}

impl GelfInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        // `port` defaults to the GELF standard 12201. A present-but-invalid
        // value fails loudly (not silently truncating via `as u16`).
        let port = settings
            .get_port("port", 12201)
            .map_err(|message| FerroStashError::Input {
                plugin: "gelf".to_string(),
                message,
            })?;
        let use_tcp = settings.get_bool("use_tcp").unwrap_or(false);
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
            use_tcp,
            tags,
        })
    }

    /// Finalize a decoded event: stamp the peer host when GELF omitted one, and
    /// apply configured tags.
    fn finalize(&self, mut event: Event, peer_ip: &str) -> Event {
        if !event.has_field("host") {
            event.set("host", EventValue::String(peer_ip.to_string()));
        }
        for tag in &self.tags {
            event.add_tag(tag);
        }
        event
    }

    async fn run_udp(
        &self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let socket = UdpSocket::bind(&addr)
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "gelf".to_string(),
                message: format!("bind error: {e}"),
            })?;
        info!(address = %addr, "GELF input listening (UDP)");

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, peer)) => {
                            if let Some(event) = decode_gelf(&buf[..len]) {
                                let event = self.finalize(event, &peer.ip().to_string());
                                if sender.send(event).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => warn!(error = %e, "GELF UDP recv error"),
                    }
                }
                () = shutdown.wait() => {
                    info!("GELF input shutting down");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn run_tcp(
        &self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "gelf".to_string(),
                message: format!("bind error: {e}"),
            })?;
        info!(address = %addr, "GELF input listening (TCP)");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let tx = sender.clone();
                            let tags = self.tags.clone();
                            let peer = peer_addr.ip().to_string();
                            tokio::spawn(async move {
                                handle_tcp_conn(stream, tx, tags, peer).await;
                            });
                        }
                        Err(e) => warn!(error = %e, "GELF TCP accept error"),
                    }
                }
                () = shutdown.wait() => {
                    info!("GELF input shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Read NUL-delimited GELF frames from one TCP connection until EOF, the cap is
/// exceeded, or the receiver is dropped.
async fn handle_tcp_conn(
    mut stream: tokio::net::TcpStream,
    tx: mpsc::Sender<Event>,
    tags: Vec<String>,
    peer: String,
) {
    let mut acc: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = match stream.read(&mut chunk).await {
            Ok(0) => {
                // EOF: tolerate a final, un-terminated frame.
                if !acc.is_empty() {
                    if let Some(event) = decode_gelf(&acc) {
                        let _ = tx.send(finalize_tcp(event, &tags, &peer)).await;
                    }
                }
                break;
            }
            Ok(n) => n,
            Err(e) => {
                warn!(peer = %peer, error = %e, "GELF TCP read error");
                break;
            }
        };
        acc.extend_from_slice(&chunk[..n]);
        if acc.len() > MAX_TCP_FRAME_BYTES {
            warn!(peer = %peer, cap = MAX_TCP_FRAME_BYTES, "GELF TCP frame exceeds max length; dropping connection");
            break;
        }
        // Extract every complete (NUL-terminated) frame currently buffered.
        while let Some(pos) = acc.iter().position(|&b| b == 0) {
            let frame: Vec<u8> = acc.drain(..=pos).collect();
            let frame = &frame[..frame.len() - 1]; // strip the trailing NUL
            if frame.is_empty() {
                continue;
            }
            if let Some(event) = decode_gelf(frame) {
                if tx.send(finalize_tcp(event, &tags, &peer)).await.is_err() {
                    return;
                }
            }
        }
    }
    debug!(peer = %peer, "GELF TCP connection closed");
}

/// Stamp the peer host (when absent) and apply tags — the free-function analog
/// of [`GelfInput::finalize`] for the spawned TCP connection task.
fn finalize_tcp(mut event: Event, tags: &[String], peer_ip: &str) -> Event {
    if !event.has_field("host") {
        event.set("host", EventValue::String(peer_ip.to_string()));
    }
    for tag in tags {
        event.add_tag(tag);
    }
    event
}

#[async_trait]
impl InputPlugin for GelfInput {
    fn name(&self) -> &'static str {
        "gelf"
    }

    async fn run(&mut self, sender: mpsc::Sender<Event>, shutdown: ShutdownSignal) -> Result<()> {
        if self.use_tcp {
            self.run_tcp(sender, shutdown).await
        } else {
            self.run_udp(sender, shutdown).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // ----- Config tests -----

    #[test]
    fn test_gelf_config_defaults() {
        let input = GelfInput::from_config(&serde_json::json!({})).expect("config");
        assert_eq!(input.host, "0.0.0.0");
        assert_eq!(input.port, 12201);
        assert!(!input.use_tcp);
        assert_eq!(input.name(), "gelf");
    }

    #[test]
    fn test_gelf_config_custom() {
        let settings = serde_json::json!({ "host": "127.0.0.1", "port": 13000, "use_tcp": true });
        let input = GelfInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "127.0.0.1");
        assert_eq!(input.port, 13000);
        assert!(input.use_tcp);
    }

    #[test]
    fn test_gelf_config_out_of_range_port_rejected() {
        let settings = serde_json::json!({ "port": 70000 });
        let err = GelfInput::from_config(&settings).expect_err("port 70000 must be rejected");
        assert!(err.to_string().contains("70000"), "got: {err}");
    }

    #[test]
    fn test_gelf_config_tags() {
        let settings = serde_json::json!({ "tags": ["gelf_in"] });
        let input = GelfInput::from_config(&settings).expect("config");
        assert_eq!(input.tags, vec!["gelf_in"]);
    }

    // ----- Decode tests -----

    #[test]
    fn test_decode_gelf_maps_standard_and_custom_fields() {
        let payload = br#"{"version":"1.1","host":"web1","short_message":"hi","level":6,"timestamp":1592000000.5,"_user_id":42,"_env":"prod"}"#;
        let event = decode_gelf(payload).expect("decode");
        assert_eq!(event.message(), Some("hi"));
        assert_eq!(event.get("host"), Some(&EventValue::String("web1".into())));
        assert_eq!(event.get("level"), Some(&EventValue::Integer(6)));
        assert_eq!(
            event.get("timestamp"),
            Some(&EventValue::Float(1592000000.5))
        );
        // Custom fields are stored with the leading underscore stripped.
        assert_eq!(event.get("user_id"), Some(&EventValue::Integer(42)));
        assert_eq!(event.get("env"), Some(&EventValue::String("prod".into())));
    }

    #[test]
    fn test_decode_gelf_gzip_compressed() {
        let json = br#"{"short_message":"gz","host":"h"}"#;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(json).expect("gz write");
        let compressed = encoder.finish().expect("gz finish");
        assert_eq!(compressed[0], 0x1f);
        let event = decode_gelf(&compressed).expect("decode gzip");
        assert_eq!(event.message(), Some("gz"));
    }

    #[test]
    fn test_decode_gelf_zlib_compressed() {
        let json = br#"{"short_message":"zl","host":"h"}"#;
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(json).expect("zlib write");
        let compressed = encoder.finish().expect("zlib finish");
        assert_eq!(compressed[0], 0x78);
        let event = decode_gelf(&compressed).expect("decode zlib");
        assert_eq!(event.message(), Some("zl"));
    }

    #[test]
    fn test_decode_gelf_chunked_is_skipped() {
        // Chunked magic 0x1e 0x0f — documented residual: dropped, not decoded.
        let chunked = [0x1e, 0x0f, 0x01, 0x02, 0x03];
        assert!(decode_gelf(&chunked).is_none());
    }

    #[test]
    fn test_decode_gelf_invalid_json_dropped() {
        assert!(decode_gelf(b"not json").is_none());
    }

    // ----- Behaviour test over a loopback UDP socket -----

    #[tokio::test]
    async fn test_gelf_udp_receives_event() {
        let settings = serde_json::json!({ "port": 19911, "host": "127.0.0.1" });
        let mut input = GelfInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(16);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind");
        sock.send_to(
            br#"{"short_message":"hello gelf","host":"src","_app":"x"}"#,
            "127.0.0.1:19911",
        )
        .await
        .expect("send");

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("recv timeout")
            .expect("event");
        assert_eq!(event.message(), Some("hello gelf"));
        assert_eq!(event.get("host"), Some(&EventValue::String("src".into())));
        assert_eq!(event.get("app"), Some(&EventValue::String("x".into())));

        ctrl.shutdown();
        let _ = handle.await;
    }

    // ----- Behaviour test over a loopback TCP socket (NUL-delimited frames) -----

    #[tokio::test]
    async fn test_gelf_tcp_receives_nul_delimited_frames() {
        let settings = serde_json::json!({ "port": 19912, "host": "127.0.0.1", "use_tcp": true });
        let mut input = GelfInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(16);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut stream = tokio::net::TcpStream::connect("127.0.0.1:19912")
            .await
            .expect("connect");
        use tokio::io::AsyncWriteExt;
        // Two NUL-terminated GELF frames in one write.
        stream
            .write_all(b"{\"short_message\":\"one\"}\0{\"short_message\":\"two\"}\0")
            .await
            .expect("write");
        stream.flush().await.expect("flush");

        let first = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("recv timeout")
            .expect("event");
        let second = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("recv timeout")
            .expect("event");
        let mut messages = [
            first.message().unwrap_or("").to_string(),
            second.message().unwrap_or("").to_string(),
        ];
        messages.sort();
        assert_eq!(messages, ["one".to_string(), "two".to_string()]);

        ctrl.shutdown();
        let _ = handle.await;
    }
}
