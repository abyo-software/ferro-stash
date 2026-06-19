// SPDX-License-Identifier: Apache-2.0
//! Dead Letter Queue (DLQ) — stores events that failed processing.
//!
//! When a filter or output fails to process an event, it can be sent to the DLQ
//! rather than being dropped. Events in the DLQ can be read back via a DLQ input
//! for reprocessing or manual inspection.
//!
//! # Convenience API
//!
//! ```ignore
//! let mut dlq = DeadLetterQueue::new("/tmp/dlq", 104_857_600)?;
//! dlq.push(&event, "mapping error", "elasticsearch", Utc::now())?;
//! for entry in dlq.iter()? { /* inspect */ }
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::{FerroStashError, Result};
use crate::event::Event;

/// DLQ configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqConfig {
    /// Directory for DLQ files.
    pub path: String,
    /// Maximum total DLQ size in bytes (default: 1GB).
    #[serde(default = "default_dlq_max_bytes")]
    pub max_bytes: u64,
    /// Flush interval in events.
    #[serde(default = "default_dlq_flush_interval")]
    pub flush_interval: usize,
    /// `fsync` each captured record to disk (power-loss durable). Off by default
    /// (process-crash durable via `flush`). Match this to the persistent queue's
    /// `fsync` when delivery failures are acked on DLQ capture and the host can
    /// lose power — otherwise a power loss could drop a DLQ record whose source
    /// queue entry was already acknowledged.
    #[serde(default)]
    pub fsync: bool,
}

fn default_dlq_max_bytes() -> u64 {
    1_073_741_824
}
fn default_dlq_flush_interval() -> usize {
    100
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            path: "data/dead_letter_queue".to_string(),
            max_bytes: default_dlq_max_bytes(),
            flush_interval: default_dlq_flush_interval(),
            fsync: false,
        }
    }
}

/// A dead letter entry (internal serialization format).
#[derive(Debug, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub timestamp: String,
    pub plugin_type: String,
    pub plugin_name: String,
    pub reason: String,
    pub event: serde_json::Value,
    /// Unique identifier for this DLQ entry.
    #[serde(default = "generate_entry_id")]
    pub entry_id: String,
}

fn generate_entry_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// High-level DLQ entry returned by the convenience API.
///
/// Contains the deserialized event along with failure metadata.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    /// The original event that failed.
    pub event: Event,
    /// Error message describing why processing failed.
    pub error: String,
    /// Name of the plugin that failed.
    pub plugin: String,
    /// Timestamp when the failure occurred.
    pub timestamp: DateTime<Utc>,
    /// Unique entry identifier.
    pub entry_id: String,
}

/// Dead Letter Queue writer.
#[allow(dead_code)]
pub struct DeadLetterQueue {
    config: DlqConfig,
    writer: Option<BufWriter<File>>,
    current_file: PathBuf,
    events_written: usize,
    total_bytes: u64,
}

impl DeadLetterQueue {
    pub fn open(config: DlqConfig) -> Result<Self> {
        fs::create_dir_all(&config.path)
            .map_err(|e| FerroStashError::Pipeline(format!("cannot create DLQ directory: {e}")))?;

        let current_file = Path::new(&config.path)
            .join(format!("dlq_{}.jsonl", Utc::now().format("%Y%m%d_%H%M%S")));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_file)
            .map_err(|e| FerroStashError::Pipeline(format!("DLQ open error: {e}")))?;

        let total_bytes = Self::calculate_total_size(&config.path);

        Ok(Self {
            config,
            writer: Some(BufWriter::new(file)),
            current_file,
            events_written: 0,
            total_bytes,
        })
    }

    fn calculate_total_size(path: &str) -> u64 {
        fs::read_dir(path)
            .map(|entries| {
                entries
                    .filter_map(std::result::Result::ok)
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Write a dead letter event, returning whether it was **durably captured**.
    ///
    /// - `Ok(true)`  — the record was written AND flushed (durable to a *process*
    ///   crash; see the durability note below).
    /// - `Ok(false)` — the DLQ is full, so the record was dropped (a warning is
    ///   logged). The event is NOT captured.
    /// - `Err(..)`   — serialization or I/O error; the record was NOT captured.
    ///
    /// The boolean exists so an at-least-once caller can distinguish "captured"
    /// from "dropped": a popped persistent-queue entry whose delivery failed must
    /// only be acknowledged when its failure is durably captured here (`Ok(true)`)
    /// — on `Ok(false)`/`Err` the caller must leave it un-acked so it replays.
    /// For that reason the record is flushed immediately rather than batched: a
    /// record buffered-but-not-flushed would be lost by a crash even though the
    /// source PQ entry was already acked.
    ///
    /// Durability scope: `flush()` pushes the record to the OS (page cache), which
    /// survives a *process* crash but not a power loss / kernel panic (there is no
    /// `fsync`). This matches the persistent queue's own checkpoint/segment
    /// durability — the whole at-least-once guarantee is process-crash-scoped, not
    /// power-loss-scoped.
    pub fn write(
        &mut self,
        plugin_type: &str,
        plugin_name: &str,
        reason: &str,
        event_json: serde_json::Value,
    ) -> Result<bool> {
        if self.total_bytes >= self.config.max_bytes {
            warn!("DLQ full, dropping dead letter event");
            return Ok(false);
        }

        let entry = DeadLetterEntry {
            timestamp: Utc::now().to_rfc3339(),
            plugin_type: plugin_type.to_string(),
            plugin_name: plugin_name.to_string(),
            reason: reason.to_string(),
            event: event_json,
            entry_id: generate_entry_id(),
        };

        let line = serde_json::to_string(&entry)
            .map_err(|e| FerroStashError::Pipeline(format!("DLQ serialize error: {e}")))?;

        if let Some(ref mut writer) = self.writer {
            writeln!(writer, "{line}")
                .map_err(|e| FerroStashError::Pipeline(format!("DLQ write error: {e}")))?;
            self.total_bytes += line.len() as u64 + 1;
            self.events_written += 1;
            // Flush immediately so `Ok(true)` means durable (see doc comment).
            writer
                .flush()
                .map_err(|e| FerroStashError::Pipeline(format!("DLQ flush error: {e}")))?;
            // Power-loss durability: fsync the record so `Ok(true)` survives a
            // power loss, not just a process crash. Gated on `fsync`.
            if self.config.fsync {
                writer
                    .get_ref()
                    .sync_data()
                    .map_err(|e| FerroStashError::Pipeline(format!("DLQ fsync error: {e}")))?;
            }
            return Ok(true);
        }

        Ok(false)
    }

    /// Read all entries from the DLQ.
    pub fn read_all(&self) -> Result<Vec<DeadLetterEntry>> {
        let mut entries = Vec::new();
        let dir_entries = fs::read_dir(&self.config.path)
            .map_err(|e| FerroStashError::Pipeline(format!("DLQ read error: {e}")))?;

        for entry in dir_entries.filter_map(std::result::Result::ok) {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "jsonl") {
                let content = fs::read_to_string(&path).unwrap_or_default();
                for line in content.lines() {
                    if let Ok(dle) = serde_json::from_str::<DeadLetterEntry>(line) {
                        entries.push(dle);
                    }
                }
            }
        }

        Ok(entries)
    }

    /// Number of events in DLQ.
    pub fn count(&self) -> usize {
        self.events_written
    }

    /// Total bytes on disk.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Flush and close.
    pub fn close(&mut self) -> Result<()> {
        if let Some(ref mut w) = self.writer {
            w.flush()
                .map_err(|e| FerroStashError::Pipeline(format!("DLQ close error: {e}")))?;
        }
        Ok(())
    }

    // ----- Convenience Event-level API -----

    /// Create a new DLQ at `path` with a maximum size of `max_size_bytes`.
    pub fn new(path: impl Into<String>, max_size_bytes: u64) -> Result<Self> {
        let config = DlqConfig {
            path: path.into(),
            max_bytes: max_size_bytes,
            ..DlqConfig::default()
        };
        Self::open(config)
    }

    /// Push a failed event into the DLQ with metadata.
    pub fn push(
        &mut self,
        event: &Event,
        error_message: &str,
        plugin_name: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let entry = DeadLetterEntry {
            timestamp: timestamp.to_rfc3339(),
            plugin_type: String::new(),
            plugin_name: plugin_name.to_string(),
            reason: error_message.to_string(),
            event: event.to_json(),
            entry_id: generate_entry_id(),
        };

        let line = serde_json::to_string(&entry)
            .map_err(|e| FerroStashError::Pipeline(format!("DLQ serialize error: {e}")))?;

        if self.total_bytes >= self.config.max_bytes {
            warn!("DLQ full, dropping dead letter event");
            return Ok(());
        }

        if let Some(ref mut writer) = self.writer {
            writeln!(writer, "{line}")
                .map_err(|e| FerroStashError::Pipeline(format!("DLQ write error: {e}")))?;
            self.total_bytes += line.len() as u64 + 1;
            self.events_written += 1;

            // `flush_interval` is an unvalidated `usize` from config; a value of
            // 0 would make this modulo a division-by-zero panic (same zero-period
            // class as the output flush timer). Clamp the divisor to >=1.
            if self.events_written % self.config.flush_interval.max(1) == 0 {
                writer
                    .flush()
                    .map_err(|e| FerroStashError::Pipeline(format!("DLQ flush error: {e}")))?;
            }
        }

        Ok(())
    }

    /// Returns all DLQ entries as high-level [`DlqEntry`] values.
    pub fn entries(&self) -> Result<Vec<DlqEntry>> {
        let raw = self.read_all()?;
        let mut entries = Vec::with_capacity(raw.len());
        for dle in raw {
            let event = Event::from_json(dle.event);
            let timestamp = dle
                .timestamp
                .parse::<DateTime<Utc>>()
                .unwrap_or_else(|_| Utc::now());
            entries.push(DlqEntry {
                event,
                error: dle.reason,
                plugin: dle.plugin_name,
                timestamp,
                entry_id: dle.entry_id,
            });
        }
        Ok(entries)
    }

    /// Number of events written in this session.
    pub fn len(&self) -> usize {
        self.events_written
    }

    /// Returns `true` if no events have been written in this session.
    pub fn is_empty(&self) -> bool {
        self.events_written == 0
    }
}

/// Thread-safe wrapper around [`DeadLetterQueue`].
pub struct SharedDeadLetterQueue {
    inner: Arc<Mutex<DeadLetterQueue>>,
}

impl SharedDeadLetterQueue {
    /// Create a new shared DLQ.
    pub fn new(path: impl Into<String>, max_size_bytes: u64) -> Result<Self> {
        let dlq = DeadLetterQueue::new(path, max_size_bytes)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(dlq)),
        })
    }

    /// Open with full configuration.
    pub fn open(config: DlqConfig) -> Result<Self> {
        let dlq = DeadLetterQueue::open(config)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(dlq)),
        })
    }

    /// Push a failed event into the DLQ.
    pub fn push(
        &self,
        event: &Event,
        error_message: &str,
        plugin_name: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("DLQ lock poisoned: {e}")))?
            .push(event, error_message, plugin_name, timestamp)
    }

    /// Write a raw dead letter entry (low-level API). Returns `Ok(true)` when the
    /// record is durably captured, `Ok(false)` when dropped because the DLQ is
    /// full, `Err` on serialize/I/O failure. See [`DeadLetterQueue::write`].
    pub fn write(
        &self,
        plugin_type: &str,
        plugin_name: &str,
        reason: &str,
        event_json: serde_json::Value,
    ) -> Result<bool> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("DLQ lock poisoned: {e}")))?
            .write(plugin_type, plugin_name, reason, event_json)
    }

    /// Number of events written.
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |g| g.len())
    }

    /// Returns `true` if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Close the DLQ.
    pub fn close(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("DLQ lock poisoned: {e}")))?
            .close()
    }
}

impl Clone for SharedDeadLetterQueue {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_write_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let mut dlq = DeadLetterQueue::open(config.clone()).expect("open");
        dlq.write(
            "filter",
            "grok",
            "pattern mismatch",
            serde_json::json!({"message": "bad line"}),
        )
        .expect("write");
        dlq.write(
            "output",
            "elasticsearch",
            "mapping error",
            serde_json::json!({"message": "wrong type"}),
        )
        .expect("write");
        dlq.close().expect("close");

        let entries = dlq.read_all().expect("read");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].plugin_name, "grok");
        assert_eq!(entries[1].reason, "mapping error");
    }

    #[test]
    fn test_dlq_zero_flush_interval_does_not_panic() {
        // A config of `flush_interval: 0` must not cause a division-by-zero
        // panic on the modulo in `write` (zero-period class). Writing must
        // succeed and the events must be readable.
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            flush_interval: 0,
            ..Default::default()
        };
        let mut dlq = DeadLetterQueue::open(config).expect("open");
        for i in 0..3 {
            dlq.write(
                "filter",
                "grok",
                "boom",
                serde_json::json!({"message": format!("line {i}")}),
            )
            .expect("write must not panic with flush_interval=0");
        }
        // Also exercise the convenience push() path which shares the modulo.
        dlq.push(&Event::new("pushed"), "err", "plugin", Utc::now())
            .expect("push must not panic with flush_interval=0");
        dlq.close().expect("close");
        let entries = dlq.read_all().expect("read");
        assert_eq!(entries.len(), 4);
    }

    #[test]
    fn test_dlq_max_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            max_bytes: 50, // tiny
            ..Default::default()
        };

        let mut dlq = DeadLetterQueue::open(config).expect("open");
        // Should not error even when full (just warns and drops)
        for i in 0..100 {
            dlq.write("test", "test", "reason", serde_json::json!({"n": i}))
                .expect("write should not fail");
        }
    }

    #[test]
    fn test_dlq_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let mut dlq = DeadLetterQueue::open(config).expect("open");
        assert_eq!(dlq.count(), 0);
        dlq.write("f", "n", "r", serde_json::json!({"x": 1}))
            .expect("write");
        assert_eq!(dlq.count(), 1);
    }

    #[test]
    fn test_dlq_total_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let dlq = DeadLetterQueue::open(config).expect("open");
        // New DLQ should have minimal bytes (empty file)
        assert!(dlq.total_bytes() < 100);
    }

    #[test]
    fn test_dlq_config_default() {
        let config = DlqConfig::default();
        assert_eq!(config.max_bytes, 1_073_741_824);
        assert_eq!(config.flush_interval, 100);
    }

    #[test]
    fn test_dlq_close() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let mut dlq = DeadLetterQueue::open(config).expect("open");
        dlq.write("f", "n", "r", serde_json::json!({"x": 1}))
            .expect("write");
        let result = dlq.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_dlq_read_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let dlq = DeadLetterQueue::open(config).expect("open");
        let entries = dlq.read_all().expect("read");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_dlq_entry_fields() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DlqConfig {
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let mut dlq = DeadLetterQueue::open(config.clone()).expect("open");
        dlq.write(
            "output",
            "elasticsearch",
            "mapping error",
            serde_json::json!({"message": "test"}),
        )
        .expect("write");
        dlq.close().expect("close");

        let entries = dlq.read_all().expect("read");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].plugin_type, "output");
        assert_eq!(entries[0].plugin_name, "elasticsearch");
        assert_eq!(entries[0].reason, "mapping error");
        assert!(!entries[0].timestamp.is_empty());
    }

    // ---- Tests for convenience Event-level API ----

    #[test]
    fn test_dlq_new_push_iter() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq = DeadLetterQueue::new(path, 104_857_600).expect("new");

        let event = Event::new("bad event");
        let ts = Utc::now();
        dlq.push(&event, "parse failure", "grok", ts).expect("push");

        assert_eq!(dlq.len(), 1);
        assert!(!dlq.is_empty());

        dlq.close().expect("close");

        let entries = dlq.entries().expect("entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event.message(), Some("bad event"));
        assert_eq!(entries[0].error, "parse failure");
        assert_eq!(entries[0].plugin, "grok");
        assert!(!entries[0].entry_id.is_empty());
    }

    #[test]
    fn test_dlq_push_multiple() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq = DeadLetterQueue::new(path, 104_857_600).expect("new");

        for i in 0..5 {
            let event = Event::new(format!("fail-{i}"));
            dlq.push(&event, &format!("error-{i}"), "output_es", Utc::now())
                .expect("push");
        }
        assert_eq!(dlq.len(), 5);
        dlq.close().expect("close");

        let entries = dlq.entries().expect("entries");
        assert_eq!(entries.len(), 5);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.event.message(), Some(format!("fail-{i}").as_str()));
            assert_eq!(entry.error, format!("error-{i}"));
        }
    }

    #[test]
    fn test_dlq_empty_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let dlq = DeadLetterQueue::new(path, 104_857_600).expect("new");
        assert_eq!(dlq.len(), 0);
        assert!(dlq.is_empty());
    }

    #[test]
    fn test_dlq_entry_preserves_event_fields() {
        use crate::event::EventValue;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq = DeadLetterQueue::new(path, 104_857_600).expect("new");

        let mut event = Event::new("test");
        event.set("host", EventValue::String("server01".into()));
        event.set("level", EventValue::Integer(3));

        dlq.push(&event, "mapping error", "elasticsearch", Utc::now())
            .expect("push");
        dlq.close().expect("close");

        let entries = dlq.entries().expect("entries");
        assert_eq!(entries.len(), 1);
        let restored = &entries[0].event;
        assert_eq!(restored.message(), Some("test"));
        assert_eq!(
            restored.get("host"),
            Some(&EventValue::String("server01".into()))
        );
        assert_eq!(restored.get("level"), Some(&EventValue::Integer(3)));
    }

    #[test]
    fn test_shared_dlq_thread_safe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let sdlq = SharedDeadLetterQueue::new(path, 104_857_600).expect("new");

        let sdlq2 = sdlq.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..5 {
                sdlq2
                    .push(&Event::new(format!("t-{i}")), "err", "plug", Utc::now())
                    .expect("push");
            }
        });
        handle.join().expect("thread join");

        assert_eq!(sdlq.len(), 5);
        assert!(!sdlq.is_empty());
        sdlq.close().expect("close");
    }

    #[test]
    fn test_dlq_entry_has_unique_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq = DeadLetterQueue::new(path, 104_857_600).expect("new");

        for _ in 0..3 {
            dlq.push(&Event::new("x"), "err", "p", Utc::now())
                .expect("push");
        }
        dlq.close().expect("close");

        let entries = dlq.entries().expect("entries");
        let ids: Vec<&str> = entries.iter().map(|e| e.entry_id.as_str()).collect();
        assert_eq!(ids.len(), 3);
        // All IDs should be unique
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3);
    }

    // ---- Fault injection and durability tests ----

    #[test]
    fn test_dlq_metadata_preservation() {
        use crate::event::EventValue;

        let dir = tempfile::tempdir().expect("tempdir for metadata_preservation");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq =
            DeadLetterQueue::new(path, 104_857_600).expect("new DLQ for metadata_preservation");

        let mut event = Event::new("metadata test event");
        event.set("host", EventValue::String("prod-server-01".into()));
        event.set("severity", EventValue::Integer(5));
        event.set("tags", EventValue::String("important,urgent".into()));

        let ts = Utc::now();
        let plugin_name = "elasticsearch_output";
        let error_msg = "mapping exception: field type conflict";

        dlq.push(&event, error_msg, plugin_name, ts)
            .expect("push with metadata");
        dlq.close().expect("close after metadata push");

        let entries = dlq.entries().expect("entries for metadata_preservation");
        assert_eq!(entries.len(), 1, "should have exactly 1 entry");

        let entry = &entries[0];
        assert_eq!(
            entry.event.message(),
            Some("metadata test event"),
            "event message mismatch"
        );
        assert_eq!(
            entry.event.get("host"),
            Some(&EventValue::String("prod-server-01".into())),
            "host field mismatch"
        );
        assert_eq!(
            entry.event.get("severity"),
            Some(&EventValue::Integer(5)),
            "severity field mismatch"
        );
        assert_eq!(entry.error, error_msg, "error message mismatch");
        assert_eq!(entry.plugin, plugin_name, "plugin name mismatch");
        assert!(!entry.entry_id.is_empty(), "entry_id should not be empty");
    }

    #[test]
    fn test_dlq_large_dlq() {
        let dir = tempfile::tempdir().expect("tempdir for large_dlq");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq = DeadLetterQueue::new(path, 100_000_000).expect("new DLQ for large_dlq");

        for i in 0..1000 {
            dlq.push(
                &Event::new(format!("failed-event-{i}")),
                &format!("error-{i}"),
                "test_plugin",
                Utc::now(),
            )
            .expect("push to large DLQ");
        }
        dlq.close().expect("close large DLQ");

        let entries = dlq.entries().expect("entries from large DLQ");
        assert_eq!(
            entries.len(),
            1000,
            "all 1000 entries should be recoverable"
        );
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.event.message(),
                Some(format!("failed-event-{i}").as_str()),
                "event {i} message mismatch in large DLQ"
            );
        }
    }

    #[test]
    fn test_dlq_max_size_enforcement() {
        let dir = tempfile::tempdir().expect("tempdir for max_size_enforcement");
        let path = dir.path().to_string_lossy().to_string();
        let config = DlqConfig {
            path: path.clone(),
            max_bytes: 500, // very small
            ..Default::default()
        };

        let mut dlq = DeadLetterQueue::open(config).expect("open DLQ for max_size_enforcement");

        // Push events with substantial payload until max_bytes is exceeded
        let mut accepted = 0;
        for i in 0..1000 {
            dlq.push(
                &Event::new(format!("bounded-{i}-{}", "X".repeat(100))),
                "error",
                "plugin",
                Utc::now(),
            )
            .expect("push should not panic even when full");
            // DLQ silently drops when full — check total_bytes stays bounded
            if dlq.total_bytes() <= 500 {
                accepted = dlq.len();
            }
        }

        // The total bytes on the DLQ should be bounded
        // DLQ drops events (with warning) when full, so accepted count should be small
        assert!(
            accepted < 1000,
            "DLQ should stop accepting before 1000 events with 500 byte limit"
        );
        // Verify it did not panic and some events were accepted
        assert!(accepted > 0, "DLQ should accept at least some events");
    }

    #[test]
    fn test_dlq_concurrent_writes() {
        let dir = tempfile::tempdir().expect("tempdir for concurrent_writes");
        let path = dir.path().to_string_lossy().to_string();
        let sdlq = SharedDeadLetterQueue::new(path, 104_857_600).expect("new SharedDLQ");

        let mut handles = Vec::new();
        for t in 0..4 {
            let q = sdlq.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..50 {
                    q.push(
                        &Event::new(format!("thread{t}-event{i}")),
                        &format!("err-t{t}-{i}"),
                        &format!("plugin-{t}"),
                        Utc::now(),
                    )
                    .expect("concurrent DLQ push");
                }
            }));
        }
        for h in handles {
            h.join().expect("thread join in concurrent_writes");
        }

        assert_eq!(
            sdlq.len(),
            200,
            "all 200 events (4 threads x 50) should be written"
        );
        sdlq.close().expect("close shared DLQ");
    }

    #[test]
    fn test_dlq_unicode_in_error_messages() {
        let dir = tempfile::tempdir().expect("tempdir for unicode_errors");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq = DeadLetterQueue::new(path, 104_857_600).expect("new DLQ for unicode");

        let unicode_errors = [
            "エラー: マッピングに失敗しました",
            "Fehler: Ungültiges Feld 🔥",
            "错误: 字段类型冲突 ❌",
            "Ошибка: поле не найдено",
            "emoji test: 🎉🚀💥🔧",
        ];

        for (i, err_msg) in unicode_errors.iter().enumerate() {
            dlq.push(
                &Event::new(format!("unicode-event-{i}")),
                err_msg,
                "test_plugin",
                Utc::now(),
            )
            .expect("push with unicode error message");
        }
        dlq.close().expect("close DLQ with unicode");

        let entries = dlq.entries().expect("entries with unicode");
        assert_eq!(
            entries.len(),
            unicode_errors.len(),
            "all unicode entries should persist"
        );
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.error, unicode_errors[i],
                "unicode error message {i} mismatch"
            );
        }
    }

    #[test]
    fn test_dlq_reopen_after_writes() {
        let dir = tempfile::tempdir().expect("tempdir for reopen_after_writes");
        let path = dir.path().to_string_lossy().to_string();

        // Write events and close
        {
            let mut dlq =
                DeadLetterQueue::new(path.clone(), 104_857_600).expect("new DLQ for reopen");
            for i in 0..10 {
                dlq.push(
                    &Event::new(format!("persist-{i}")),
                    &format!("reason-{i}"),
                    "plugin_x",
                    Utc::now(),
                )
                .expect("push for reopen test");
            }
            dlq.close().expect("close before reopen");
        }

        // Reopen and verify entries persist
        let dlq2 = DeadLetterQueue::new(path, 104_857_600).expect("reopen DLQ");
        let entries = dlq2.entries().expect("entries after reopen");
        assert_eq!(
            entries.len(),
            10,
            "all 10 entries should persist after reopen"
        );
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.event.message(),
                Some(format!("persist-{i}").as_str()),
                "event {i} mismatch after reopen"
            );
            assert_eq!(
                entry.error,
                format!("reason-{i}"),
                "reason {i} mismatch after reopen"
            );
        }
    }

    #[test]
    fn test_dlq_entry_id_uniqueness_100() {
        let dir = tempfile::tempdir().expect("tempdir for entry_id_uniqueness");
        let path = dir.path().to_string_lossy().to_string();
        let mut dlq = DeadLetterQueue::new(path, 104_857_600).expect("new DLQ for id uniqueness");

        for i in 0..100 {
            dlq.push(
                &Event::new(format!("id-test-{i}")),
                "err",
                "plug",
                Utc::now(),
            )
            .expect("push for id uniqueness");
        }
        dlq.close().expect("close for id uniqueness");

        let entries = dlq.entries().expect("entries for id uniqueness");
        assert_eq!(entries.len(), 100, "should have 100 entries");

        let mut ids: Vec<String> = entries.iter().map(|e| e.entry_id.clone()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 100, "all 100 entry_ids should be unique");
    }
}
