// SPDX-License-Identifier: Apache-2.0
//! Pipe output — writes each codec-encoded event to a subprocess's stdin.
//!
//! Mirrors Logstash's `pipe` output. A single long-lived child is launched via
//! `sh -c <command>` with its stdin piped; each event is encoded (one line per
//! event) and written to that stdin. If the child exits (broken pipe on write),
//! it is relaunched once and the write is retried.
//!
//! ```logstash
//! output {
//!   pipe {
//!     command => "logger -t ferro-stash"
//!     codec   => "json"
//!     # message_format => "%{host} %{message}"   # optional %{}-template
//!   }
//! }
//! ```
//!
//! - `codec` (default `json`): `json` writes `event.to_json_string()`; `line` /
//!   `plain` writes the event's `message`.
//! - `message_format` (optional): a `%{field}` template; when set, the rendered
//!   template is written instead of the codec output.
//!
//! Residuals (honest limitations):
//! - **One child, one write per event** (no batching of multiple events into a
//!   single write syscall; each event is its own newline-terminated line).
//! - **Restart-on-exit only** — a child that exits is relaunched on the next
//!   failing write; there is no max-restart policy.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipeCodec {
    Json,
    Line,
}

/// A running child and a handle to its piped stdin.
#[derive(Debug)]
struct ChildState {
    child: Child,
    stdin: ChildStdin,
}

#[derive(Debug)]
pub struct PipeOutput {
    command: String,
    codec: PipeCodec,
    message_format: Option<String>,
    condition: Option<Condition>,
    /// The long-lived child, created lazily on first output and relaunched on
    /// exit. Interior mutability because `OutputPlugin::output` takes `&self`.
    child: Mutex<Option<ChildState>>,
}

impl PipeOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let command = settings
            .get_string("command")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| output_err("pipe output requires a non-empty `command`".to_string()))?;
        let codec = match settings
            .get("codec")
            .and_then(|v| v.as_str())
            .unwrap_or("json")
            .to_ascii_lowercase()
            .as_str()
        {
            "line" | "plain" => PipeCodec::Line,
            _ => PipeCodec::Json,
        };
        let message_format = settings
            .get_string("message_format")
            .filter(|s| !s.is_empty());
        Ok(Self {
            command,
            codec,
            message_format,
            condition,
            child: Mutex::new(None),
        })
    }

    /// Encodes one event into the line written to the child's stdin (without the
    /// trailing newline).
    fn encode(&self, event: &Event) -> String {
        if let Some(fmt) = &self.message_format {
            return event.sprintf(fmt);
        }
        match self.codec {
            PipeCodec::Json => event.to_json_string(),
            PipeCodec::Line => event.message().unwrap_or("").to_string(),
        }
    }

    async fn spawn_child(&self) -> Result<ChildState> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .stdin(Stdio::piped())
            .spawn()
            .map_err(|e| output_err(format!("failed to spawn command: {e}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| output_err("child stdin was not captured".to_string()))?;
        Ok(ChildState { child, stdin })
    }
}

#[async_trait]
impl OutputPlugin for PipeOutput {
    fn name(&self) -> &'static str {
        "pipe"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut guard = self.child.lock().await;

        for event in &events {
            let bytes = format!("{}\n", self.encode(event));
            let mut last_err: Option<FerroStashError> = None;
            // Attempt 0 = normal write; attempt 1 = after a one-shot restart.
            for attempt in 0..2 {
                if guard.is_none() {
                    match self.spawn_child().await {
                        Ok(state) => *guard = Some(state),
                        Err(e) => {
                            last_err = Some(e);
                            break;
                        }
                    }
                }
                let write_result = {
                    let state = guard.as_mut().expect("child present after spawn");
                    state.stdin.write_all(bytes.as_bytes()).await
                };
                match write_result {
                    Ok(()) => {
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        last_err = Some(output_err(format!("write error: {e}")));
                        // Broken pipe almost certainly means the child exited;
                        // tear it down so the next attempt relaunches it.
                        if let Some(state) = guard.take() {
                            let mut child = state.child;
                            let _ = child.kill().await;
                        }
                        if attempt == 0 {
                            warn!(error = %e, "pipe output: write failed, restarting child");
                        }
                    }
                }
            }
            if let Some(e) = last_err {
                return Err(e);
            }
        }

        if let Some(state) = guard.as_mut() {
            state
                .stdin
                .flush()
                .await
                .map_err(|e| output_err(format!("flush error: {e}")))?;
        }
        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }

    async fn close(&self) -> Result<()> {
        let mut guard = self.child.lock().await;
        if let Some(state) = guard.take() {
            let ChildState {
                mut child,
                mut stdin,
            } = state;
            let _ = stdin.flush().await;
            // Dropping stdin sends EOF so a read-loop child can finish + flush.
            drop(stdin);
            let _ = child.wait().await;
        }
        info!("pipe output closed");
        Ok(())
    }
}

fn output_err(message: String) -> FerroStashError {
    FerroStashError::Output {
        plugin: "pipe".to_string(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_stash_core::event::EventValue;

    #[test]
    fn from_config_requires_command() {
        assert!(PipeOutput::from_config(&serde_json::json!({}), None).is_err());
        assert!(PipeOutput::from_config(&serde_json::json!({ "command": "" }), None).is_err());
        let o = PipeOutput::from_config(&serde_json::json!({ "command": "cat" }), None)
            .expect("config");
        assert_eq!(o.command, "cat");
        assert_eq!(o.codec, PipeCodec::Json);
        assert!(o.message_format.is_none());
        assert_eq!(o.name(), "pipe");
    }

    #[test]
    fn from_config_codec_and_format() {
        let o = PipeOutput::from_config(
            &serde_json::json!({ "command": "cat", "codec": "line", "message_format": "%{x}" }),
            None,
        )
        .expect("config");
        assert_eq!(o.codec, PipeCodec::Line);
        assert_eq!(o.message_format.as_deref(), Some("%{x}"));
    }

    #[test]
    fn encode_uses_message_format_over_codec() {
        let o = PipeOutput::from_config(
            &serde_json::json!({ "command": "cat", "codec": "json", "message_format": "v=%{n}" }),
            None,
        )
        .expect("config");
        let mut ev = Event::new("ignored");
        ev.set("n", EventValue::Integer(5));
        assert_eq!(o.encode(&ev), "v=5");
    }

    /// Writes events to a child that appends each line to a tempfile, proving the
    /// codec-encoded lines reach the subprocess's stdin. No external infra.
    #[tokio::test]
    async fn pipe_output_writes_to_subprocess_stdin() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.txt");
        let path_str = path.to_string_lossy().into_owned();
        // A read-loop that appends each received line to the file (each `>>`
        // append flushes immediately, so the file is observable after close()).
        let command =
            format!("while IFS= read -r line; do printf '%s\\n' \"$line\" >> '{path_str}'; done");
        let out = PipeOutput::from_config(
            &serde_json::json!({ "command": command, "codec": "line" }),
            None,
        )
        .expect("config");

        out.output(vec![Event::new("first"), Event::new("second")])
            .await
            .expect("output");
        // close() flushes + closes stdin (EOF) and waits for the child to finish.
        out.close().await.expect("close");

        let contents = std::fs::read_to_string(&path).expect("read back");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines, vec!["first", "second"], "got: {contents:?}");
    }

    #[tokio::test]
    async fn pipe_output_message_format_template() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.txt");
        let path_str = path.to_string_lossy().into_owned();
        let command =
            format!("while IFS= read -r line; do printf '%s\\n' \"$line\" >> '{path_str}'; done");
        let out = PipeOutput::from_config(
            &serde_json::json!({ "command": command, "message_format": "n=%{n}" }),
            None,
        )
        .expect("config");

        let mut ev = Event::new("ignored");
        ev.set("n", EventValue::Integer(42));
        out.output(vec![ev]).await.expect("output");
        out.close().await.expect("close");

        let contents = std::fs::read_to_string(&path).expect("read back");
        assert_eq!(contents.trim(), "n=42");
    }
}
