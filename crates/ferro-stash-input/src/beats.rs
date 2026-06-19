// SPDX-License-Identifier: Apache-2.0
//! Beats input plugin — receives events via the Lumberjack v2 (Beats) protocol.
//!
//! Compatible with Filebeat, Metricbeat, `FerroBeat`, and all Elastic Beats.
//!
//! Protocol frames:
//! - Version: 1 byte ('2')
//! - Window: 'W' + 4-byte count
//! - Data: 'D' + 4-byte seq + key-value pairs
//! - JSON: 'J' + 4-byte seq + 4-byte `payload_len` + JSON payload
//! - Compressed: 'C' + 4-byte `payload_len` + zlib-compressed inner frames
//! - ACK: 'A' + 4-byte seq (sent by receiver)

use std::io::Read;

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Upper bound on a single on-the-wire Beats frame body announced by an
/// attacker-controlled 32-bit length prefix.
///
/// The Lumberjack/Beats protocol carries no authentication, so any client that
/// can reach the listening port can announce an arbitrary `payload_len` (up to
/// `0xFFFF_FFFF` ≈ 4 GiB). Pre-allocating a zero-filled buffer of that size
/// *before* the body arrives is an unauthenticated remote OOM amplifier (a
/// ~10-byte header → ~4 GiB allocation; many connections multiply it). We
/// refuse to allocate for any announced length above this ceiling and drop the
/// connection instead. 100 MB mirrors Logstash's beats input default
/// (`client_inactivity_timeout` aside, its max message size is in this range)
/// and is comfortably above any legitimate batch.
const MAX_BEATS_FRAME_BYTES: usize = 100_000_000;

/// Upper bound on the *decompressed* size of a `C` (compressed) frame.
///
/// zlib is a defense gap independent of the wire-length cap: a small compressed
/// `payload_len` (which itself passes [`MAX_BEATS_FRAME_BYTES`]) can expand to
/// gigabytes (a "zip bomb"). We cap the decoder's output so a malicious stream
/// cannot inflate past this bound. Allow a small multiple of the frame cap
/// because legitimate inner frames compress well; anything beyond is hostile.
const MAX_DECOMPRESSED_BYTES: usize = 4 * MAX_BEATS_FRAME_BYTES;

/// Upper bound on the number of inner frames/events materialized from a single
/// `C` (compressed) frame.
///
/// The byte caps above bound the *compressed* wire length and the *decompressed*
/// size, but [`parse_inner_frames`] then turns every decoded inner frame into
/// an in-memory [`Event`]. A small zlib payload can decompress to hundreds of
/// MB of minimal `J`/`D` inner frames (each only a handful of bytes), yielding
/// tens of millions of `Event` objects — each far larger than its wire footprint
/// — before a single one is sent downstream. That heap blow-up defeats the byte
/// caps, so we additionally bound the *count* of decoded events from one frame.
/// 1,000,000 events is far above any legitimate Beats batch (typical windows are
/// in the thousands) while keeping the peak per-frame `Event` count bounded.
const MAX_INNER_FRAMES: usize = 1_000_000;

/// Returns `true` if an attacker-announced on-the-wire length is within the
/// allocation cap and therefore safe to allocate a buffer for.
///
/// Factored out as a pure function so the cap policy is unit-testable without a
/// live socket (see the `beats` tests).
#[inline]
fn beats_len_within_cap(announced: usize) -> bool {
    announced <= MAX_BEATS_FRAME_BYTES
}

#[derive(Debug)]
pub struct BeatsInput {
    host: String,
    port: u16,
    tags: Vec<String>,
}

impl BeatsInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0.0")
            .to_string();
        let port = settings
            .get_port("port", 5044)
            .map_err(|message| FerroStashError::Input {
                plugin: "beats".to_string(),
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
impl InputPlugin for BeatsInput {
    fn name(&self) -> &'static str {
        "beats"
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
                plugin: "beats".to_string(),
                message: format!("bind error: {e}"),
            })?;

        info!(address = %addr, "Beats input listening (Lumberjack v2)");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            debug!(peer = %peer, "new Beats connection");
                            let tx = sender.clone();
                            let tags = self.tags.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_beats_connection(stream, peer, tx, tags).await {
                                    warn!(peer = %peer, error = %e, "Beats connection error");
                                }
                            });
                        }
                        Err(e) => warn!(error = %e, "Beats accept error"),
                    }
                }
                () = shutdown.wait() => {
                    info!("Beats input shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

async fn handle_beats_connection(
    mut stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    sender: mpsc::Sender<Event>,
    tags: Vec<String>,
) -> Result<()> {
    let mut buf = vec![0u8; 65536];

    loop {
        // Read version byte
        let version = match stream.read_u8().await {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(FerroStashError::Io(e)),
        };

        if version != b'2' {
            warn!(version, "unsupported Lumberjack protocol version");
            return Ok(());
        }

        // Read frame type
        let frame_type = stream.read_u8().await.map_err(FerroStashError::Io)?;

        match frame_type {
            b'W' => {
                // Window frame: 4-byte count
                let _window_size = stream.read_u32().await.map_err(FerroStashError::Io)?;
            }
            b'J' => {
                // JSON frame: 4-byte seq + 4-byte payload_len + payload
                let seq = stream.read_u32().await.map_err(FerroStashError::Io)?;
                let payload_len = stream.read_u32().await.map_err(FerroStashError::Io)? as usize;

                if !beats_len_within_cap(payload_len) {
                    warn!(
                        peer = %peer,
                        payload_len,
                        cap = MAX_BEATS_FRAME_BYTES,
                        "Beats J frame length exceeds cap; dropping connection"
                    );
                    return Ok(());
                }

                if payload_len > buf.len() {
                    buf.resize(payload_len, 0);
                }
                stream
                    .read_exact(&mut buf[..payload_len])
                    .await
                    .map_err(FerroStashError::Io)?;

                let json_str = String::from_utf8_lossy(&buf[..payload_len]);
                let mut event =
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                        Event::from_json(json)
                    } else {
                        Event::new(json_str.as_ref())
                    };

                for tag in &tags {
                    event.add_tag(tag);
                }

                if sender.send(event).await.is_err() {
                    return Ok(());
                }

                // Send ACK
                send_ack(&mut stream, seq).await?;
            }
            b'D' => {
                // Data frame: 4-byte seq + pairs (4-byte key_len + key + 4-byte val_len + val)
                let seq = stream.read_u32().await.map_err(FerroStashError::Io)?;
                let pair_count = stream.read_u32().await.map_err(FerroStashError::Io)? as usize;

                let mut event = Event::empty();
                for _ in 0..pair_count {
                    let key_len = stream.read_u32().await.map_err(FerroStashError::Io)? as usize;
                    if !beats_len_within_cap(key_len) {
                        warn!(
                            peer = %peer,
                            key_len,
                            cap = MAX_BEATS_FRAME_BYTES,
                            "Beats D frame key length exceeds cap; dropping connection"
                        );
                        return Ok(());
                    }
                    let mut key_buf = vec![0u8; key_len];
                    stream
                        .read_exact(&mut key_buf)
                        .await
                        .map_err(FerroStashError::Io)?;

                    let val_len = stream.read_u32().await.map_err(FerroStashError::Io)? as usize;
                    if !beats_len_within_cap(val_len) {
                        warn!(
                            peer = %peer,
                            val_len,
                            cap = MAX_BEATS_FRAME_BYTES,
                            "Beats D frame value length exceeds cap; dropping connection"
                        );
                        return Ok(());
                    }
                    let mut val_buf = vec![0u8; val_len];
                    stream
                        .read_exact(&mut val_buf)
                        .await
                        .map_err(FerroStashError::Io)?;

                    let key = String::from_utf8_lossy(&key_buf).to_string();
                    let val = String::from_utf8_lossy(&val_buf).to_string();
                    event.set(key, EventValue::String(val));
                }

                for tag in &tags {
                    event.add_tag(tag);
                }

                if sender.send(event).await.is_err() {
                    return Ok(());
                }

                send_ack(&mut stream, seq).await?;
            }
            b'C' => {
                // Compressed frame: 4-byte payload_len + zlib-compressed inner frames
                let payload_len = stream.read_u32().await.map_err(FerroStashError::Io)? as usize;

                if !beats_len_within_cap(payload_len) {
                    warn!(
                        peer = %peer,
                        payload_len,
                        cap = MAX_BEATS_FRAME_BYTES,
                        "Beats C frame compressed length exceeds cap; dropping connection"
                    );
                    return Ok(());
                }

                let mut compressed = vec![0u8; payload_len];
                stream
                    .read_exact(&mut compressed)
                    .await
                    .map_err(FerroStashError::Io)?;

                // Decompress with zlib, bounding the *output* so a zip-bomb
                // (small compressed body → gigabytes decompressed) cannot
                // exhaust memory. `take` caps how many bytes the decoder may
                // produce; if the limit is hit we cannot trust the stream and
                // drop the connection. We read one byte past the cap to detect
                // overrun (a fully-consumed cap-sized stream is ambiguous).
                let mut decoder = flate2::read::ZlibDecoder::new(&compressed[..])
                    .take(MAX_DECOMPRESSED_BYTES as u64 + 1);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| FerroStashError::Input {
                        plugin: "beats".to_string(),
                        message: format!("zlib decompress error: {e}"),
                    })?;
                if decompressed.len() > MAX_DECOMPRESSED_BYTES {
                    warn!(
                        peer = %peer,
                        decompressed_len = decompressed.len(),
                        cap = MAX_DECOMPRESSED_BYTES,
                        "Beats C frame decompressed size exceeds cap (possible zip bomb); dropping connection"
                    );
                    return Ok(());
                }

                // Parse inner frames from decompressed data
                let parsed = parse_inner_frames(&decompressed)?;
                let mut max_seq = 0u32;
                for (seq, mut event) in parsed {
                    for tag in &tags {
                        event.add_tag(tag);
                    }
                    if sender.send(event).await.is_err() {
                        return Ok(());
                    }
                    if seq > max_seq {
                        max_seq = seq;
                    }
                }

                if max_seq > 0 {
                    send_ack(&mut stream, max_seq).await?;
                }
            }
            other => {
                warn!(frame_type = other, "unknown Beats frame type");
            }
        }
    }
}

fn parse_inner_frames(data: &[u8]) -> Result<Vec<(u32, Event)>> {
    let mut events = Vec::new();
    let mut pos = 0;

    fn read_u32_at(data: &[u8], pos: usize) -> u32 {
        u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
    }

    while pos + 2 <= data.len() {
        // Bound the number of materialized events from a single compressed
        // frame: a small zlib payload can decompress to hundreds of MB of
        // minimal J/D frames, expanding into tens of millions of `Event`s and
        // exhausting the heap despite the decompressed-byte cap. Once the cap
        // is reached we stop decoding the rest of this frame.
        if events.len() >= MAX_INNER_FRAMES {
            warn!(
                decoded = events.len(),
                cap = MAX_INNER_FRAMES,
                "Beats compressed frame yields too many inner events; truncating decode"
            );
            break;
        }

        let _version = data[pos];
        let frame_type = data[pos + 1];
        pos += 2;

        match frame_type {
            b'W' => {
                if pos + 4 > data.len() {
                    break;
                }
                pos += 4; // skip window count
            }
            b'J' => {
                if pos + 8 > data.len() {
                    break;
                }
                let seq = read_u32_at(data, pos);
                pos += 4;
                let payload_len = read_u32_at(data, pos) as usize;
                pos += 4;

                if pos + payload_len > data.len() {
                    break;
                }
                let json_str = String::from_utf8_lossy(&data[pos..pos + payload_len]);
                let event = if let Ok(json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                    Event::from_json(json)
                } else {
                    Event::new(json_str.as_ref())
                };
                events.push((seq, event));
                pos += payload_len;
            }
            b'D' => {
                // Data frame: 4-byte seq + 4-byte pair_count + key/value pairs
                if pos + 8 > data.len() {
                    break;
                }
                let seq = read_u32_at(data, pos);
                pos += 4;
                let pair_count = read_u32_at(data, pos) as usize;
                pos += 4;

                let mut event = Event::empty();
                let mut pairs_read = 0usize;
                let mut truncated = false;
                for _ in 0..pair_count {
                    if pos + 4 > data.len() {
                        truncated = true;
                        break;
                    }
                    let key_len = read_u32_at(data, pos) as usize;
                    pos += 4;
                    if pos + key_len > data.len() {
                        truncated = true;
                        break;
                    }
                    let key = String::from_utf8_lossy(&data[pos..pos + key_len]).to_string();
                    pos += key_len;

                    if pos + 4 > data.len() {
                        truncated = true;
                        break;
                    }
                    let val_len = read_u32_at(data, pos) as usize;
                    pos += 4;
                    if pos + val_len > data.len() {
                        truncated = true;
                        break;
                    }
                    let val = String::from_utf8_lossy(&data[pos..pos + val_len]).to_string();
                    pos += val_len;

                    event.set(key, EventValue::String(val));
                    pairs_read += 1;
                }
                // Only emit complete frames — reject truncated ones
                if !truncated && pairs_read == pair_count {
                    events.push((seq, event));
                } else {
                    tracing::warn!(seq, pairs_read, pair_count, "truncated D frame, dropping");
                    break; // stop parsing — remaining data is unreliable
                }
            }
            _ => {
                // Unknown frame type — skip (don't break, try to continue)
                break;
            }
        }
    }

    Ok(events)
}

async fn send_ack(stream: &mut tokio::net::TcpStream, seq: u32) -> Result<()> {
    let mut ack = [0u8; 6];
    ack[0] = b'2'; // version
    ack[1] = b'A'; // ACK
    ack[2..6].copy_from_slice(&seq.to_be_bytes());
    stream
        .write_all(&ack)
        .await
        .map_err(|e| FerroStashError::Input {
            plugin: "beats".to_string(),
            message: format!("ACK write error: {e}"),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_inner_json_frame() {
        // Build a JSON frame: version='2', type='J', seq=1, payload
        let payload = br#"{"message":"hello from beat"}"#;
        let mut data = Vec::new();
        data.push(b'2');
        data.push(b'J');
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        data.extend_from_slice(payload);

        let parsed = parse_inner_frames(&data).expect("parse");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, 1); // seq
        assert_eq!(parsed[0].1.message(), Some("hello from beat"));
    }

    #[test]
    fn test_parse_multiple_frames() {
        let mut data = Vec::new();
        for i in 0..3u32 {
            let payload = format!(r#"{{"message":"event {i}"}}"#);
            let payload_bytes = payload.as_bytes();
            data.push(b'2');
            data.push(b'J');
            data.extend_from_slice(&(i + 1).to_be_bytes());
            data.extend_from_slice(&(payload_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(payload_bytes);
        }

        let parsed = parse_inner_frames(&data).expect("parse");
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].0, 1);
        assert_eq!(parsed[2].0, 3);
    }

    #[test]
    fn test_beats_config_defaults() {
        let settings = serde_json::json!({});
        let input = BeatsInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "0.0.0.0");
        assert_eq!(input.port, 5044);
    }

    #[test]
    fn test_beats_config_custom() {
        let settings = serde_json::json!({
            "host": "127.0.0.1",
            "port": 5055,
            "tags": ["beats"]
        });
        let input = BeatsInput::from_config(&settings).expect("config");
        assert_eq!(input.host, "127.0.0.1");
        assert_eq!(input.port, 5055);
        assert_eq!(input.tags, vec!["beats"]);
    }

    #[test]
    fn test_beats_name() {
        let settings = serde_json::json!({});
        let input = BeatsInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "beats");
    }

    #[test]
    fn test_parse_inner_window_frame() {
        let mut data = Vec::new();
        data.push(b'2');
        data.push(b'W');
        data.extend_from_slice(&10u32.to_be_bytes());
        // followed by a J frame
        let payload = br#"{"message":"after window"}"#;
        data.push(b'2');
        data.push(b'J');
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        data.extend_from_slice(payload);

        let parsed = parse_inner_frames(&data).expect("parse");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].1.message(), Some("after window"));
    }

    #[test]
    fn test_parse_inner_data_frame() {
        let mut data = Vec::new();
        data.push(b'2');
        data.push(b'D');
        data.extend_from_slice(&1u32.to_be_bytes()); // seq
        data.extend_from_slice(&2u32.to_be_bytes()); // pair count

        // pair 1: key="message", value="hello"
        let key = b"message";
        let val = b"hello";
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&(val.len() as u32).to_be_bytes());
        data.extend_from_slice(val);

        // pair 2: key="host", value="server01"
        let key2 = b"host";
        let val2 = b"server01";
        data.extend_from_slice(&(key2.len() as u32).to_be_bytes());
        data.extend_from_slice(key2);
        data.extend_from_slice(&(val2.len() as u32).to_be_bytes());
        data.extend_from_slice(val2);

        let parsed = parse_inner_frames(&data).expect("parse");
        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed[0].1.get("message"),
            Some(&EventValue::String("hello".into()))
        );
        assert_eq!(
            parsed[0].1.get("host"),
            Some(&EventValue::String("server01".into()))
        );
    }

    #[test]
    fn test_parse_inner_empty() {
        let data: Vec<u8> = Vec::new();
        let parsed = parse_inner_frames(&data).expect("parse");
        assert_eq!(parsed.len(), 0);
    }

    #[test]
    fn test_parse_inner_truncated() {
        // Truncated J frame
        let mut data = Vec::new();
        data.push(b'2');
        data.push(b'J');
        data.extend_from_slice(&1u32.to_be_bytes());
        // Missing payload_len and payload
        let parsed = parse_inner_frames(&data).expect("parse");
        assert_eq!(parsed.len(), 0);
    }

    #[test]
    fn test_beats_len_cap_rejects_oversize() {
        // Regression (HIGH): an attacker-announced frame length above the cap
        // must be rejected so we never pre-allocate a multi-GiB buffer from a
        // ~10-byte header (unauthenticated remote OOM). 0xFFFF_FFFF ≈ 4 GiB.
        let attacker_announced = u32::MAX as usize; // 4_294_967_295
        assert!(
            !beats_len_within_cap(attacker_announced),
            "a ~4 GiB announced length must be refused (no allocation)"
        );
        // One byte over the cap is also refused.
        assert!(!beats_len_within_cap(MAX_BEATS_FRAME_BYTES + 1));
    }

    #[test]
    fn test_beats_len_cap_accepts_legitimate() {
        // A legitimate small frame and an exactly-at-cap frame are allowed.
        assert!(beats_len_within_cap(0));
        assert!(beats_len_within_cap(65_536));
        assert!(beats_len_within_cap(MAX_BEATS_FRAME_BYTES));
    }

    // The decompression bound must be a finite ceiling strictly larger than the
    // wire-length cap (so legitimate well-compressing inner frames are allowed).
    // Checked at compile time to avoid a runtime constant assertion.
    const _: () = assert!(MAX_DECOMPRESSED_BYTES > MAX_BEATS_FRAME_BYTES);

    #[test]
    fn test_parse_inner_frames_caps_event_count() {
        // Regression (HIGH, event-count expansion): a decompressed buffer of
        // many minimal inner frames must not materialize an unbounded number of
        // `Event`s. We craft a buffer holding MAX_INNER_FRAMES + extra minimal
        // J frames (each a 0-byte payload = 10 bytes on the wire) and assert the
        // decoded event count is capped at MAX_INNER_FRAMES rather than growing
        // to the full frame count.
        let extra = 50usize;
        let total = MAX_INNER_FRAMES + extra;
        // Each minimal J frame: '2','J', seq(4), payload_len=0(4) → 10 bytes.
        let mut data = Vec::with_capacity(total * 10);
        for _ in 0..total {
            data.push(b'2');
            data.push(b'J');
            data.extend_from_slice(&0u32.to_be_bytes()); // seq
            data.extend_from_slice(&0u32.to_be_bytes()); // payload_len = 0
        }

        let parsed = parse_inner_frames(&data).expect("parse");
        assert_eq!(
            parsed.len(),
            MAX_INNER_FRAMES,
            "decoded event count must be capped at MAX_INNER_FRAMES, not the full {total}"
        );
    }

    #[test]
    fn test_beats_zip_bomb_decompression_is_bounded() {
        // Regression (HIGH, zlib-bomb): a small compressed body that expands to
        // far more than MAX_DECOMPRESSED_BYTES must be caught by the `take`
        // bound used in the C-frame handler, so the connection is dropped
        // instead of inflating to gigabytes. We reproduce the exact decoder
        // pipeline (ZlibDecoder + take(cap + 1)) against a highly-compressible
        // payload and assert the overrun is detected.
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        // Use a small local cap to keep the test fast; the production constant
        // is exercised by the same `take`-based logic below.
        let test_cap: usize = 1_000_000; // 1 MB
        let raw = vec![0u8; test_cap + 10_000]; // > cap, but trivially compressible

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(&raw).expect("compress");
        let compressed = encoder.finish().expect("finish");
        // The bomb: a tiny compressed body vs a large decompressed one.
        assert!(
            compressed.len() < test_cap,
            "test payload should compress well: {} bytes",
            compressed.len()
        );

        // Mirror the C-frame decode: decoder.take(cap + 1).read_to_end(...).
        let mut decoder = flate2::read::ZlibDecoder::new(&compressed[..]).take(test_cap as u64 + 1);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).expect("decode");

        // The overrun check (len > cap) must fire — the connection would be
        // dropped, and memory stays bounded at cap + 1 (not the full raw size).
        assert!(
            decompressed.len() > test_cap,
            "the bound must observe more than the cap so the overrun is detected"
        );
        assert!(
            decompressed.len() <= test_cap + 1,
            "decoder output must be bounded to cap + 1, got {}",
            decompressed.len()
        );
    }
}
