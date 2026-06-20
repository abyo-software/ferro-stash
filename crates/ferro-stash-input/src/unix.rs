// SPDX-License-Identifier: Apache-2.0
//! Unix domain socket input (Unix-only).
//!
//! Mirrors Logstash's `unix` input. In the default `server` mode it binds a
//! Unix domain socket at `path`, accepts connections, and reads them
//! line-by-line; in `client` mode it connects to an existing socket at `path`
//! and reads it line-by-line. Each line becomes an event via the configured
//! `codec`:
//!
//! - `line` / `plain` (default): one event per line (`message => line`).
//! - `json`: one JSON document per line (NDJSON); a non-object / unparseable
//!   line becomes a `message` event (tagged `_jsonparsefailure` on parse error).
//!
//! ```logstash
//! input {
//!   unix {
//!     path => "/tmp/ferro-stash.sock"
//!     mode => "server"        # or "client"
//!     codec => "line"
//!   }
//! }
//! ```
//!
//! Residuals (honest limitations):
//! - **Unix-only** — the factory returns a clear error on non-Unix platforms.
//! - **No `data_timeout` / `socket_not_present_retry_interval`** tuning; client
//!   mode reconnects on a fixed backoff.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::exec::{event_from_line, read_line_capped, CappedLine, LineCodec, MAX_LINE_BYTES};

/// Backoff between client-mode reconnect attempts.
const CLIENT_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Server,
    Client,
}

#[derive(Debug, Clone)]
pub struct UnixInput {
    path: String,
    mode: Mode,
    codec: LineCodec,
}

impl UnixInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let path = settings
            .get_string("path")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| input_err("unix input requires a non-empty `path`".to_string()))?;
        let mode = match settings
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("server")
            .to_ascii_lowercase()
            .as_str()
        {
            "client" => Mode::Client,
            "server" => Mode::Server,
            other => {
                return Err(input_err(format!(
                    "unix input `mode` must be \"server\" or \"client\", got \"{other}\""
                )))
            }
        };
        // `line` is the Logstash default for this input; accept it as an alias of
        // plain (one event per line).
        let codec = if settings
            .get("codec")
            .and_then(|v| v.as_str())
            .is_some_and(|c| c.eq_ignore_ascii_case("line"))
        {
            LineCodec::Plain
        } else {
            LineCodec::from_settings(settings)
        };
        Ok(Self { path, mode, codec })
    }

    /// Reads a connected stream line-by-line, emitting events until EOF, error,
    /// or shutdown. Returns `true` if shutdown was requested.
    async fn read_stream(
        &self,
        stream: UnixStream,
        sender: &mpsc::Sender<Event>,
        shutdown: &mut ShutdownSignal,
    ) -> bool {
        let mut reader = BufReader::new(stream);
        loop {
            tokio::select! {
                next = read_line_capped(&mut reader, MAX_LINE_BYTES) => {
                    match next {
                        Ok(CappedLine::Line(line)) => {
                            if line.trim().is_empty() {
                                continue;
                            }
                            let mut event = event_from_line(&line, self.codec);
                            event.set("path", EventValue::String(self.path.clone()));
                            if sender.send(event).await.is_err() {
                                info!("unix input: downstream closed, stopping");
                                return true;
                            }
                        }
                        Ok(CappedLine::Overflow) => {
                            warn!(path = %self.path, cap = MAX_LINE_BYTES, "unix input: line exceeds max length; dropping connection");
                            return false;
                        }
                        Ok(CappedLine::Eof) => return false, // peer closed
                        Err(e) => {
                            warn!(error = %e, "unix input: read error");
                            return false;
                        }
                    }
                }
                () = shutdown.wait() => return true,
            }
        }
    }

    async fn run_server(
        &self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        // A stale socket file from a previous unclean shutdown would make `bind`
        // fail with EADDRINUSE; remove it first (best-effort), matching common
        // Unix-socket server behaviour.
        let _ = std::fs::remove_file(&self.path);
        let listener = UnixListener::bind(&self.path)
            .map_err(|e| input_err(format!("failed to bind unix socket '{}': {e}", self.path)))?;
        info!(path = %self.path, "unix input listening (server mode)");

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _addr)) => {
                            debug!("unix input: new connection");
                            let tx = sender.clone();
                            let codec = self.codec;
                            let path = self.path.clone();
                            let sub = shutdown.clone();
                            tokio::spawn(async move {
                                let conn = UnixInput { path, mode: Mode::Server, codec };
                                let mut sub = sub;
                                let _ = conn.read_stream(stream, &tx, &mut sub).await;
                            });
                        }
                        Err(e) => warn!(error = %e, "unix input: accept error"),
                    }
                }
                () = shutdown.wait() => {
                    info!("unix input shutting down");
                    break;
                }
            }
        }
        // Remove the socket file on shutdown (best-effort).
        let _ = std::fs::remove_file(&self.path);
        Ok(())
    }

    async fn run_client(
        &self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        info!(path = %self.path, "unix input connecting (client mode)");
        loop {
            match UnixStream::connect(&self.path).await {
                Ok(stream) => {
                    if self.read_stream(stream, &sender, &mut shutdown).await {
                        break;
                    }
                }
                Err(e) => warn!(error = %e, path = %self.path, "unix input: connect failed"),
            }
            tokio::select! {
                () = tokio::time::sleep(CLIENT_RECONNECT_BACKOFF) => {}
                () = shutdown.wait() => {
                    info!("unix input shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl InputPlugin for UnixInput {
    fn name(&self) -> &'static str {
        "unix"
    }

    async fn run(&mut self, sender: mpsc::Sender<Event>, shutdown: ShutdownSignal) -> Result<()> {
        match self.mode {
            Mode::Server => self.run_server(sender, shutdown).await,
            Mode::Client => self.run_client(sender, shutdown).await,
        }
    }
}

fn input_err(message: String) -> FerroStashError {
    FerroStashError::Input {
        plugin: "unix".to_string(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_stash_core::shutdown::ShutdownController;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn from_config_requires_path() {
        assert!(UnixInput::from_config(&serde_json::json!({})).is_err());
        let i =
            UnixInput::from_config(&serde_json::json!({ "path": "/tmp/x.sock" })).expect("config");
        assert_eq!(i.path, "/tmp/x.sock");
        assert_eq!(i.mode, Mode::Server);
        assert_eq!(i.codec, LineCodec::Plain);
        assert_eq!(i.name(), "unix");
    }

    #[test]
    fn from_config_mode_and_codec() {
        let i = UnixInput::from_config(
            &serde_json::json!({ "path": "/tmp/x.sock", "mode": "client", "codec": "json" }),
        )
        .expect("config");
        assert_eq!(i.mode, Mode::Client);
        assert_eq!(i.codec, LineCodec::Json);
        // `line` codec is an alias of plain.
        let i =
            UnixInput::from_config(&serde_json::json!({ "path": "/tmp/x.sock", "codec": "line" }))
                .expect("config");
        assert_eq!(i.codec, LineCodec::Plain);
        // Invalid mode rejected.
        assert!(UnixInput::from_config(
            &serde_json::json!({ "path": "/tmp/x.sock", "mode": "bogus" })
        )
        .is_err());
    }

    #[tokio::test]
    async fn unix_server_accepts_and_reads_lines() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sock = dir.path().join("in.sock");
        let sock_str = sock.to_string_lossy().into_owned();

        let mut input = UnixInput::from_config(&serde_json::json!({ "path": sock_str.clone() }))
            .expect("config");
        let (tx, mut rx) = mpsc::channel(8);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });

        // Wait for the socket to appear, then connect and send a line.
        for _ in 0..100 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut client = UnixStream::connect(&sock).await.expect("connect");
        client.write_all(b"hello unix\n").await.expect("write");
        client.flush().await.expect("flush");

        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert_eq!(ev.message(), Some("hello unix"));
        assert_eq!(ev.get("path"), Some(&EventValue::String(sock_str)));

        controller.shutdown();
        let _ = handle.await;
    }
}
