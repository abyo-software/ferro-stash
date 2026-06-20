// SPDX-License-Identifier: Apache-2.0
//! Exec input — runs a shell command on an interval and emits its stdout.
//!
//! Mirrors Logstash's `exec` input for the periodic-command case: the configured
//! `command` is run via `sh -c` every `interval` seconds (or
//! `schedule => { every => "Ns" }`), and its standard output is turned into
//! events by the configured `codec`:
//!
//! - `plain` (default): one event per non-empty stdout line (`message => line`).
//! - `json`: one JSON document per stdout line (NDJSON). A line that parses to a
//!   JSON object becomes a structured event; a line that is not valid JSON
//!   becomes a `message` event tagged `_jsonparsefailure`.
//!
//! Each event carries `[@metadata][exec][command]` (the command run) and
//! `[@metadata][exec][duration]` (wall-clock seconds the command took).
//!
//! ```logstash
//! input {
//!   exec {
//!     command  => "df -k /"
//!     interval => 30          # seconds (also: schedule => { every => "30s" })
//!     codec    => "plain"
//!   }
//! }
//! ```
//!
//! Residuals (honest limitations):
//! - **Interval scheduling only** — `schedule => { every => "10s" }` is parsed to
//!   a fixed interval; full cron expressions are not supported.
//! - **stdout only** — the command's stderr is logged (warn) but not emitted as
//!   events.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use std::time::{Duration, Instant};
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Maximum bytes a single line may accumulate before a newline is seen, used by
/// the line-streaming inputs (`pipe`, `unix`). An upstream (child process / socket
/// peer) that never emits a newline could otherwise grow the per-line buffer
/// until OOM. 1 MiB is far above any legitimate log line (mirrors the
/// graphite/tcp inputs' caps).
pub(crate) const MAX_LINE_BYTES: usize = 1024 * 1024;

/// Outcome of a single bounded line read.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CappedLine {
    /// A complete (or final EOF-terminated) line, with the trailing `\n`/`\r\n`
    /// stripped.
    Line(String),
    /// The stream reached EOF with no further data.
    Eof,
    /// A line exceeded the cap before a newline was seen; its bytes were
    /// discarded. The caller should stop reading the stream.
    Overflow,
}

/// Read one `\n`-delimited line into a `String`, enforcing a hard cap on the
/// bytes buffered before a newline is seen. This is a DoS-safe replacement for
/// `AsyncBufReadExt::lines()` (which is unbounded). Mirrors the graphite/tcp
/// inputs' bounded readers.
pub(crate) async fn read_line_capped<R>(reader: &mut R, cap: usize) -> std::io::Result<CappedLine>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if buf.is_empty() {
                return Ok(CappedLine::Eof);
            }
            // EOF with a trailing, unterminated line.
            return Ok(CappedLine::Line(strip_eol(&buf)));
        }
        match available.iter().position(|&b| b == b'\n') {
            Some(idx) => {
                let take = idx + 1;
                if buf.len() + take > cap {
                    reader.consume(take);
                    return Ok(CappedLine::Overflow);
                }
                buf.extend_from_slice(&available[..take]);
                reader.consume(take);
                return Ok(CappedLine::Line(strip_eol(&buf)));
            }
            None => {
                let len = available.len();
                if buf.len() + len > cap {
                    reader.consume(len);
                    return Ok(CappedLine::Overflow);
                }
                buf.extend_from_slice(available);
                reader.consume(len);
            }
        }
    }
}

/// Strip a trailing `\n` (and a preceding `\r`) and decode to a `String`.
fn strip_eol(buf: &[u8]) -> String {
    let mut end = buf.len();
    if end > 0 && buf[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && buf[end - 1] == b'\r' {
            end -= 1;
        }
    }
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// How stdout bytes are turned into events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineCodec {
    Plain,
    Json,
}

impl LineCodec {
    /// Parses a `codec` setting (default [`LineCodec::Plain`]).
    pub(crate) fn from_settings(settings: &serde_json::Value) -> Self {
        match settings
            .get("codec")
            .and_then(|v| v.as_str())
            .unwrap_or("plain")
            .to_ascii_lowercase()
            .as_str()
        {
            "json" | "json_lines" => Self::Json,
            _ => Self::Plain,
        }
    }
}

/// Turns one output line into an [`Event`] according to the codec.
pub(crate) fn event_from_line(line: &str, codec: LineCodec) -> Event {
    match codec {
        LineCodec::Plain => Event::new(line),
        LineCodec::Json => match serde_json::from_str::<serde_json::Value>(line) {
            Ok(serde_json::Value::Object(map)) => Event::from_json(serde_json::Value::Object(map)),
            // Valid JSON that is not an object (array / scalar): keep the raw line
            // as the message (no structured fields to merge at the root).
            Ok(_) => Event::new(line),
            Err(_) => {
                let mut event = Event::new(line);
                event.add_tag("_jsonparsefailure");
                event
            }
        },
    }
}

/// Builds events for every non-empty line of `text`.
pub(crate) fn events_from_output(text: &str, codec: LineCodec) -> Vec<Event> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| event_from_line(line, codec))
        .collect()
}

#[derive(Debug, Clone)]
pub struct ExecInput {
    command: String,
    interval: u64,
    codec: LineCodec,
}

impl ExecInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let command = settings
            .get_string("command")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| input_err("exec input requires a non-empty `command`".to_string()))?;

        // Scheduling: prefer `schedule => { every => "10s" }`, else `interval`
        // seconds; floored to >= 1s (matches the jdbc input's scheduling).
        let scheduled = settings
            .get("schedule")
            .and_then(|s| s.get("every"))
            .and_then(serde_json::Value::as_str)
            .and_then(parse_every);
        let interval = scheduled
            .or_else(|| settings.get_u64("interval"))
            .unwrap_or(60)
            .max(1);

        Ok(Self {
            command,
            interval,
            codec: LineCodec::from_settings(settings),
        })
    }

    /// Runs the command once and returns its captured stdout.
    async fn run_once(&self) -> std::io::Result<(String, Duration)> {
        let started = Instant::now();
        let output = Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            // Kill the child if this future is dropped (e.g. shutdown interrupts a
            // hung command) so it is not orphaned.
            .kill_on_drop(true)
            .output()
            .await?;
        let elapsed = started.elapsed();
        if !output.stderr.is_empty() {
            warn!(
                command = %self.command,
                stderr = %String::from_utf8_lossy(&output.stderr),
                "exec input: command wrote to stderr"
            );
        }
        Ok((
            String::from_utf8_lossy(&output.stdout).into_owned(),
            elapsed,
        ))
    }

    fn stamp_metadata(&self, event: &mut Event, duration: Duration) {
        let meta = EventValue::from(serde_json::json!({
            "command": self.command,
            "duration": duration.as_secs_f64(),
        }));
        event.metadata.set("exec".to_string(), meta);
    }
}

#[async_trait]
impl InputPlugin for ExecInput {
    fn name(&self) -> &'static str {
        "exec"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        info!(command = %self.command, interval = self.interval, "exec input starting");
        loop {
            // Run the command under shutdown.wait() so a hung/long-running command
            // cannot block pipeline shutdown (kill_on_drop reaps the child).
            let result = tokio::select! {
                r = self.run_once() => r,
                () = shutdown.wait() => {
                    info!("exec input shutting down");
                    break;
                }
            };
            match result {
                Ok((stdout, duration)) => {
                    for mut event in events_from_output(&stdout, self.codec) {
                        self.stamp_metadata(&mut event, duration);
                        if sender.send(event).await.is_err() {
                            info!("exec input: downstream closed, stopping");
                            return Ok(());
                        }
                    }
                }
                Err(e) => warn!(error = %e, command = %self.command, "exec input: command failed"),
            }

            debug!("exec input: cycle complete");
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(self.interval)) => {}
                () = shutdown.wait() => {
                    info!("exec input shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

fn input_err(message: String) -> FerroStashError {
    FerroStashError::Input {
        plugin: "exec".to_string(),
        message,
    }
}

/// Parses a `schedule => { every => "10s" }` duration into seconds. Accepts a
/// bare number (seconds) or a `<n><unit>` form (s/m/h/d).
fn parse_every(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let split = s.find(|c: char| c.is_alphabetic())?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.trim().parse().ok()?;
    let mult = match unit.trim().to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hour" | "hours" => 3600,
        "d" | "day" | "days" => 86400,
        _ => return None,
    };
    Some(n.saturating_mul(mult))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_stash_core::shutdown::ShutdownController;

    #[test]
    fn from_config_requires_command() {
        assert!(ExecInput::from_config(&serde_json::json!({})).is_err());
        assert!(ExecInput::from_config(&serde_json::json!({ "command": "   " })).is_err());
        let i =
            ExecInput::from_config(&serde_json::json!({ "command": "echo hi" })).expect("config");
        assert_eq!(i.command, "echo hi");
        assert_eq!(i.interval, 60);
        assert_eq!(i.codec, LineCodec::Plain);
        assert_eq!(i.name(), "exec");
    }

    #[test]
    fn schedule_and_interval_parsed() {
        let i = ExecInput::from_config(&serde_json::json!({
            "command": "echo hi", "schedule": { "every": "5m" }
        }))
        .expect("config");
        assert_eq!(i.interval, 300);

        let i = ExecInput::from_config(&serde_json::json!({
            "command": "echo hi", "interval": 0
        }))
        .expect("config");
        assert_eq!(i.interval, 1); // floored

        let i = ExecInput::from_config(&serde_json::json!({
            "command": "echo hi", "codec": "json"
        }))
        .expect("config");
        assert_eq!(i.codec, LineCodec::Json);
    }

    #[test]
    fn events_from_output_plain_one_per_line() {
        let events = events_from_output("a\n\nb\n", LineCodec::Plain);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message(), Some("a"));
        assert_eq!(events[1].message(), Some("b"));
    }

    #[test]
    fn events_from_output_json_object_lines() {
        let events = events_from_output("{\"k\":1}\nnotjson\n", LineCodec::Json);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].get("k"), Some(&EventValue::Integer(1)));
        assert!(events[1].has_tag("_jsonparsefailure"));
        assert_eq!(events[1].message(), Some("notjson"));
    }

    #[tokio::test]
    async fn read_line_capped_splits_and_eofs() {
        use tokio::io::BufReader;
        let data = b"alpha\nbeta\n";
        let mut r = BufReader::new(&data[..]);
        assert_eq!(
            read_line_capped(&mut r, 1024).await.expect("read"),
            CappedLine::Line("alpha".to_string())
        );
        assert_eq!(
            read_line_capped(&mut r, 1024).await.expect("read"),
            CappedLine::Line("beta".to_string())
        );
        assert_eq!(
            read_line_capped(&mut r, 1024).await.expect("read"),
            CappedLine::Eof
        );
    }

    #[tokio::test]
    async fn read_line_capped_rejects_overlong_line() {
        use tokio::io::BufReader;
        // 64 bytes with no newline within a 16-byte cap → Overflow (DoS-safe).
        let mut data = vec![b'x'; 64];
        data.push(b'\n');
        let mut r = BufReader::new(&data[..]);
        assert_eq!(
            read_line_capped(&mut r, 16).await.expect("read"),
            CappedLine::Overflow
        );
    }

    #[tokio::test]
    async fn exec_shutdown_interrupts_running_command() {
        // A long-running command must not block shutdown: requesting shutdown
        // should return run() promptly, well before the command would finish.
        let mut input =
            ExecInput::from_config(&serde_json::json!({ "command": "sleep 30", "interval": 1 }))
                .expect("config");
        let (tx, _rx) = mpsc::channel(8);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });
        // Let the command start, then request shutdown.
        tokio::time::sleep(Duration::from_millis(100)).await;
        controller.shutdown();
        let joined = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            joined.is_ok(),
            "exec input did not shut down while the command was still running"
        );
    }

    #[tokio::test]
    async fn exec_runs_command_and_emits_event() {
        let mut input =
            ExecInput::from_config(&serde_json::json!({ "command": "echo hello", "interval": 1 }))
                .expect("config");
        let (tx, mut rx) = mpsc::channel(8);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });

        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert_eq!(ev.message(), Some("hello"));
        // Metadata carries the command + duration.
        assert!(ev.metadata.get("exec").is_some());

        controller.shutdown();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn exec_json_codec_parses_object() {
        let mut input = ExecInput::from_config(&serde_json::json!({
            "command": "echo '{\"level\":\"info\",\"n\":7}'",
            "interval": 1,
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
        assert_eq!(ev.get("level"), Some(&EventValue::String("info".into())));
        assert_eq!(ev.get("n"), Some(&EventValue::Integer(7)));

        controller.shutdown();
        let _ = handle.await;
    }
}
