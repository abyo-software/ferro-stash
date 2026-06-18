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
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

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
            .get("port")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(5044) as u16;
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
                                if let Err(e) = handle_beats_connection(stream, tx, tags).await {
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
                    let mut key_buf = vec![0u8; key_len];
                    stream
                        .read_exact(&mut key_buf)
                        .await
                        .map_err(FerroStashError::Io)?;

                    let val_len = stream.read_u32().await.map_err(FerroStashError::Io)? as usize;
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

                let mut compressed = vec![0u8; payload_len];
                stream
                    .read_exact(&mut compressed)
                    .await
                    .map_err(FerroStashError::Io)?;

                // Decompress with zlib
                let mut decoder = flate2::read::ZlibDecoder::new(&compressed[..]);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| FerroStashError::Input {
                        plugin: "beats".to_string(),
                        message: format!("zlib decompress error: {e}"),
                    })?;

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
}
