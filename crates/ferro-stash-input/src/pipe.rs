// SPDX-License-Identifier: Apache-2.0
//! Pipe input — runs a long-running command and streams its stdout line-by-line
//! as events, restarting the child (with a small backoff) whenever it exits.
//!
//! Mirrors Logstash's `pipe` input. The configured `command` is launched via
//! `sh -c`, its standard output is read line-by-line, and each line becomes an
//! event via the configured `codec`:
//!
//! - `plain` (default): one event per line (`message => line`).
//! - `json`: one JSON document per line (NDJSON); a non-object / unparseable
//!   line becomes a `message` event (tagged `_jsonparsefailure` on parse error).
//!
//! ```logstash
//! input {
//!   pipe {
//!     command => "tail -F /var/log/app.log"
//!     codec   => "plain"
//!   }
//! }
//! ```
//!
//! Residuals (honest limitations):
//! - **stdout only** — the child's stderr is logged (warn) but not emitted.
//! - **Restart-on-exit** — a child that exits is relaunched after a short
//!   backoff; there is no max-restart / circuit-breaker policy.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::exec::{event_from_line, LineCodec};

/// Backoff between a child exiting and being relaunched. Small enough to be
/// responsive but large enough to avoid a tight crash-loop spinning the CPU.
const RESTART_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct PipeInput {
    command: String,
    codec: LineCodec,
}

impl PipeInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let command = settings
            .get_string("command")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| FerroStashError::Input {
                plugin: "pipe".to_string(),
                message: "pipe input requires a non-empty `command`".to_string(),
            })?;
        Ok(Self {
            command,
            codec: LineCodec::from_settings(settings),
        })
    }

    /// Streams one child's stdout to `sender` until it exits or shutdown is
    /// requested. Returns `Ok(true)` when shutdown was requested (the caller
    /// should stop), `Ok(false)` when the child merely exited (relaunch).
    async fn stream_child(
        &self,
        sender: &mpsc::Sender<Event>,
        shutdown: &mut ShutdownSignal,
    ) -> Result<bool> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .stdout(Stdio::piped())
            .stdin(Stdio::null())
            .spawn()
            .map_err(|e| FerroStashError::Input {
                plugin: "pipe".to_string(),
                message: format!("failed to spawn command: {e}"),
            })?;

        let stdout = match child.stdout.take() {
            Some(out) => out,
            None => {
                let _ = child.kill().await;
                return Err(FerroStashError::Input {
                    plugin: "pipe".to_string(),
                    message: "child stdout was not captured".to_string(),
                });
            }
        };
        let mut lines = BufReader::new(stdout).lines();

        loop {
            tokio::select! {
                next = lines.next_line() => {
                    match next {
                        Ok(Some(line)) => {
                            if line.trim().is_empty() {
                                continue;
                            }
                            let event = event_from_line(&line, self.codec);
                            if sender.send(event).await.is_err() {
                                info!("pipe input: downstream closed, stopping");
                                let _ = child.kill().await;
                                return Ok(true);
                            }
                        }
                        Ok(None) => {
                            debug!(command = %self.command, "pipe input: child stdout closed");
                            // Reap the child to avoid a zombie before relaunch.
                            let _ = child.wait().await;
                            return Ok(false);
                        }
                        Err(e) => {
                            warn!(error = %e, "pipe input: read error");
                            let _ = child.kill().await;
                            return Ok(false);
                        }
                    }
                }
                () = shutdown.wait() => {
                    info!("pipe input shutting down");
                    let _ = child.kill().await;
                    return Ok(true);
                }
            }
        }
    }
}

#[async_trait]
impl InputPlugin for PipeInput {
    fn name(&self) -> &'static str {
        "pipe"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        info!(command = %self.command, "pipe input starting");
        loop {
            let stop = self.stream_child(&sender, &mut shutdown).await?;
            if stop {
                break;
            }
            // Child exited; back off, then relaunch unless shutdown intervenes.
            tokio::select! {
                () = tokio::time::sleep(RESTART_BACKOFF) => {
                    debug!(command = %self.command, "pipe input: relaunching child");
                }
                () = shutdown.wait() => {
                    info!("pipe input shutting down");
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
    use ferro_stash_core::event::EventValue;
    use ferro_stash_core::shutdown::ShutdownController;

    #[test]
    fn from_config_requires_command() {
        assert!(PipeInput::from_config(&serde_json::json!({})).is_err());
        assert!(PipeInput::from_config(&serde_json::json!({ "command": "" })).is_err());
        let i =
            PipeInput::from_config(&serde_json::json!({ "command": "tail -F x" })).expect("config");
        assert_eq!(i.command, "tail -F x");
        assert_eq!(i.codec, LineCodec::Plain);
        assert_eq!(i.name(), "pipe");
    }

    #[tokio::test]
    async fn pipe_streams_stdout_lines() {
        let mut input = PipeInput::from_config(&serde_json::json!({
            "command": "printf 'a\\nb\\n'"
        }))
        .expect("config");
        let (tx, mut rx) = mpsc::channel(8);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });

        let first = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out on first")
            .expect("channel closed");
        let second = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out on second")
            .expect("channel closed");
        assert_eq!(first.message(), Some("a"));
        assert_eq!(second.message(), Some("b"));

        controller.shutdown();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn pipe_json_codec_parses_object() {
        let mut input = PipeInput::from_config(&serde_json::json!({
            "command": "printf '{\"x\":1}\\n'",
            "codec": "json"
        }))
        .expect("config");
        let (tx, mut rx) = mpsc::channel(8);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });

        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert_eq!(ev.get("x"), Some(&EventValue::Integer(1)));

        controller.shutdown();
        let _ = handle.await;
    }
}
