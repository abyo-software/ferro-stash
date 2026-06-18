// SPDX-License-Identifier: Apache-2.0
//! File input plugin — tails files like Logstash's file input.
//!
//! Features:
//! - Glob patterns for file discovery
//! - Sincedb tracking (file position persistence)
//! - `start_position`: beginning / end
//! - File rotation detection
//! - Configurable stat interval

use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{debug, info, warn};

#[allow(dead_code)]
#[derive(Debug)]
pub struct FileInput {
    paths: Vec<String>,
    exclude: Vec<String>,
    start_position: StartPosition,
    sincedb_path: Option<String>,
    stat_interval_secs: u64,
    discover_interval_secs: u64,
    delimiter: String,
    tags: Vec<String>,
    add_field: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
enum StartPosition {
    Beginning,
    End,
}

impl FileInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let paths = match settings.get("path") {
            Some(serde_json::Value::String(s)) => vec![s.clone()],
            Some(serde_json::Value::Array(a)) => a
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
            _ => {
                return Err(FerroStashError::Input {
                    plugin: "file".to_string(),
                    message: "path is required".to_string(),
                });
            }
        };

        let exclude = settings
            .get("exclude")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let start_position = match settings
            .get("start_position")
            .and_then(|v| v.as_str())
            .unwrap_or("end")
        {
            "beginning" => StartPosition::Beginning,
            _ => StartPosition::End,
        };

        let sincedb_path = settings
            .get("sincedb_path")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Clamp both intervals to a minimum of 1s. A DSL-accepted
        // `discover_interval => 0` would feed `Duration::from_secs(0)` to
        // `tokio::time::interval` (see `run`), which PANICS
        // ("`period` must be non-zero") and aborts the file input task. Same
        // zero-period class as heartbeat (DD round-20). `stat_interval` is
        // clamped for parity / future-proofing.
        let stat_interval_secs = settings
            .get("stat_interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(1)
            .max(1);

        let discover_interval_secs = settings
            .get("discover_interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(15)
            .max(1);

        let delimiter = settings
            .get("delimiter")
            .and_then(|v| v.as_str())
            .unwrap_or("\n")
            .to_string();

        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let add_field = settings
            .get("add_field")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            paths,
            exclude,
            start_position,
            sincedb_path,
            stat_interval_secs,
            discover_interval_secs,
            delimiter,
            tags,
            add_field,
        })
    }

    fn discover_files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for pattern in &self.paths {
            if let Ok(entries) = glob::glob(pattern) {
                for entry in entries.flatten() {
                    if entry.is_file() && !self.is_excluded(&entry) {
                        files.push(entry);
                    }
                }
            }
        }
        files.sort();
        files.dedup();
        files
    }

    fn is_excluded(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();
        self.exclude
            .iter()
            .any(|pattern| glob::Pattern::new(pattern).is_ok_and(|p| p.matches(&path_str)))
    }

    fn load_sincedb(&self) -> HashMap<String, u64> {
        let sincedb_path = match &self.sincedb_path {
            Some(p) if p != "/dev/null" && p != "NUL" => p.clone(),
            _ => return HashMap::new(),
        };

        std::fs::read_to_string(&sincedb_path)
            .ok()
            .map(|content| {
                content
                    .lines()
                    .filter_map(|line| {
                        let (path, offset) = line.rsplit_once(' ')?;
                        let offset: u64 = offset.parse().ok()?;
                        let path = path.to_string();
                        Some((path, offset))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn save_sincedb(&self, positions: &HashMap<String, u64>) {
        let sincedb_path = match &self.sincedb_path {
            Some(p) if p != "/dev/null" && p != "NUL" => p.clone(),
            _ => return,
        };

        let content: String = positions
            .iter()
            .map(|(path, offset)| format!("{path} {offset}"))
            .collect::<Vec<_>>()
            .join("\n");

        if let Err(e) = std::fs::write(&sincedb_path, content) {
            warn!(error = %e, "failed to write sincedb");
        }
    }
}

#[async_trait]
impl InputPlugin for FileInput {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let mut positions = self.load_sincedb();
        let mut discover_timer = time::interval(Duration::from_secs(self.discover_interval_secs));
        let _stat_interval = Duration::from_secs(self.stat_interval_secs);

        info!(paths = ?self.paths, "file input starting");

        loop {
            // Discover files
            let files = self.discover_files();

            for file_path in &files {
                let path_str = file_path.to_string_lossy().to_string();

                let offset = if let Some(&pos) = positions.get(&path_str) {
                    pos
                } else if self.start_position == StartPosition::Beginning {
                    0
                } else {
                    // Start from end
                    std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0)
                };

                match self.read_file(file_path, offset, &sender).await {
                    Ok(new_offset) => {
                        if new_offset != offset {
                            positions.insert(path_str, new_offset);
                        }
                    }
                    Err(e) => {
                        warn!(path = %path_str, error = %e, "file read error");
                    }
                }
            }

            self.save_sincedb(&positions);

            // Wait before next check
            tokio::select! {
                _ = discover_timer.tick() => {}
                () = shutdown.wait() => {
                    info!("file input shutting down");
                    self.save_sincedb(&positions);
                    break;
                }
            }
        }

        Ok(())
    }
}

impl FileInput {
    async fn read_file(
        &self,
        path: &Path,
        start_offset: u64,
        sender: &mpsc::Sender<Event>,
    ) -> Result<u64> {
        let file = File::open(path).await.map_err(|e| FerroStashError::Input {
            plugin: "file".to_string(),
            message: format!("cannot open {}: {e}", path.display()),
        })?;

        let metadata = file.metadata().await.map_err(|e| FerroStashError::Input {
            plugin: "file".to_string(),
            message: format!("cannot stat {}: {e}", path.display()),
        })?;

        // Check for file truncation / rotation
        let file_size = metadata.len();
        let seek_pos = if start_offset > file_size {
            // File was truncated or rotated
            debug!(path = %path.display(), "file appears truncated, reading from beginning");
            0
        } else {
            start_offset
        };

        let mut reader = BufReader::new(file);
        if seek_pos > 0 {
            reader
                .seek(SeekFrom::Start(seek_pos))
                .await
                .map_err(|e| FerroStashError::Input {
                    plugin: "file".to_string(),
                    message: format!("seek error: {e}"),
                })?;
        }

        let mut current_offset = seek_pos;
        let path_str = path.to_string_lossy().to_string();
        let delim_byte = self.delimiter.as_bytes().first().copied().unwrap_or(b'\n');
        let use_custom_delimiter = delim_byte != b'\n';

        loop {
            let (n, record) = if use_custom_delimiter {
                let mut buf = Vec::new();
                match reader.read_until(delim_byte, &mut buf).await {
                    Ok(0) => (0, String::new()),
                    Ok(n) => {
                        let s = String::from_utf8_lossy(&buf).to_string();
                        (n, s)
                    }
                    Err(e) => {
                        warn!(path = %path_str, error = %e, "file read error");
                        break;
                    }
                }
            } else {
                let mut line = String::new();
                match reader.read_line(&mut line).await {
                    Ok(n) => (n, line),
                    Err(e) => {
                        warn!(path = %path_str, error = %e, "file read error");
                        break;
                    }
                }
            };

            match n {
                0 => break,
                n => {
                    current_offset += n as u64;
                    let trimmed = record
                        .trim_end_matches(self.delimiter.as_str())
                        .trim_end_matches('\n')
                        .trim_end_matches('\r');
                    if trimmed.is_empty() {
                        continue;
                    }

                    let mut event = Event::new(trimmed);
                    event.set("path", EventValue::String(path_str.clone()));
                    event.set(
                        "host",
                        EventValue::String(hostname::get().map_or_else(
                            |_| "unknown".to_string(),
                            |h| h.to_string_lossy().to_string(),
                        )),
                    );
                    for (k, v) in &self.add_field {
                        event.set(k.clone(), EventValue::String(v.clone()));
                    }
                    for tag in &self.tags {
                        event.add_tag(tag);
                    }

                    if sender.send(event).await.is_err() {
                        return Ok(current_offset);
                    }
                }
            }
        }

        Ok(current_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_config_single_path() {
        let settings = serde_json::json!({ "path": "/var/log/*.log" });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.paths, vec!["/var/log/*.log"]);
    }

    #[test]
    fn test_file_config_array_paths() {
        let settings = serde_json::json!({
            "path": ["/var/log/syslog", "/var/log/auth.log"]
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.paths.len(), 2);
    }

    #[test]
    fn test_file_config_missing_path() {
        let settings = serde_json::json!({});
        assert!(FileInput::from_config(&settings).is_err());
    }

    #[test]
    fn test_file_config_zero_intervals_clamped() {
        // A DSL-accepted `discover_interval => 0` / `stat_interval => 0` must be
        // clamped to >=1 so the `tokio::time::interval` timer in `run` is never
        // built with a zero period (which would panic and abort the task).
        let settings = serde_json::json!({
            "path": "/tmp/test.log",
            "discover_interval": 0,
            "stat_interval": 0
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.discover_interval_secs, 1);
        assert_eq!(input.stat_interval_secs, 1);
    }

    #[test]
    fn test_file_config_intervals_preserved() {
        let settings = serde_json::json!({
            "path": "/tmp/test.log",
            "discover_interval": 30,
            "stat_interval": 5
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.discover_interval_secs, 30);
        assert_eq!(input.stat_interval_secs, 5);
    }

    #[test]
    fn test_file_config_start_position_beginning() {
        let settings = serde_json::json!({
            "path": "/tmp/test.log",
            "start_position": "beginning"
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.start_position, StartPosition::Beginning);
    }

    #[test]
    fn test_file_config_start_position_default() {
        let settings = serde_json::json!({ "path": "/tmp/test.log" });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.start_position, StartPosition::End);
    }

    #[test]
    fn test_file_config_with_tags() {
        let settings = serde_json::json!({
            "path": "/tmp/test.log",
            "tags": ["file_input"]
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.tags, vec!["file_input"]);
    }

    #[test]
    fn test_file_config_with_exclude() {
        let settings = serde_json::json!({
            "path": "/var/log/*.log",
            "exclude": ["*.gz"]
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.exclude, vec!["*.gz"]);
    }

    #[test]
    fn test_file_config_sincedb() {
        let settings = serde_json::json!({
            "path": "/tmp/test.log",
            "sincedb_path": "/dev/null"
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.sincedb_path, Some("/dev/null".to_string()));
    }

    #[test]
    fn test_file_name() {
        let settings = serde_json::json!({ "path": "/tmp/test.log" });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "file");
    }

    #[test]
    fn test_file_load_sincedb_null() {
        let settings = serde_json::json!({
            "path": "/tmp/test.log",
            "sincedb_path": "/dev/null"
        });
        let input = FileInput::from_config(&settings).expect("config");
        let positions = input.load_sincedb();
        assert!(positions.is_empty());
    }

    #[test]
    fn test_file_config_add_field() {
        let settings = serde_json::json!({
            "path": "/tmp/test.log",
            "add_field": { "env": "test" }
        });
        let input = FileInput::from_config(&settings).expect("config");
        assert_eq!(input.add_field.len(), 1);
    }

    #[tokio::test]
    async fn test_file_read_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("test.log");
        std::fs::write(&file_path, "line1\nline2\nline3\n").expect("write");

        let settings = serde_json::json!({
            "path": file_path.to_string_lossy().to_string(),
            "start_position": "beginning"
        });
        let input = FileInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);

        let offset = input.read_file(&file_path, 0, &tx).await.expect("read");
        assert!(offset > 0);

        let mut messages = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let Some(msg) = event.message() {
                messages.push(msg.to_string());
            }
        }
        assert_eq!(messages, vec!["line1", "line2", "line3"]);
    }

    #[test]
    fn test_file_sincedb_roundtrips_paths_with_spaces() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sincedb_path = dir.path().join("sincedb");
        let log_path = dir.path().join("app logs").join("service.log");

        let settings = serde_json::json!({
            "path": log_path.to_string_lossy().to_string(),
            "sincedb_path": sincedb_path.to_string_lossy().to_string()
        });
        let input = FileInput::from_config(&settings).expect("config");

        let mut positions = HashMap::new();
        positions.insert(log_path.to_string_lossy().to_string(), 42);
        input.save_sincedb(&positions);

        let loaded = input.load_sincedb();
        assert_eq!(
            loaded.get(&log_path.to_string_lossy().to_string()),
            Some(&42),
            "sincedb must preserve file paths containing spaces"
        );
    }
}
