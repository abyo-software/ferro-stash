// SPDX-License-Identifier: Apache-2.0
//! Persistent Queue (PQ) — disk-backed queue for crash resilience.
//!
//! Logstash 9.2+ compatible:
//! - WAL-based append-only segments
//! - ZSTD compression at 3 levels: Speed, Balanced, Size
//! - Checkpoint tracking for consumer position
//! - Configurable `max_bytes` and segment size
//!
//! # Convenience API
//!
//! In addition to the batch-oriented `enqueue`/`dequeue` API, a higher-level
//! Event-aware API is provided:
//!
//! ```ignore
//! let mut q = PersistentQueue::new("/tmp/pq", 1_073_741_824)?;
//! q.push(&event)?;
//! let event = q.pop()?;
//! q.checkpoint()?;
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::{FerroStashError, Result};
use crate::event::Event;

/// Compression level for PQ.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum PqCompression {
    #[default]
    None,
    Speed,
    Balanced,
    Size,
}

impl PqCompression {
    fn zstd_level(self) -> Option<i32> {
        match self {
            Self::None => None,
            Self::Speed => Some(1),
            Self::Balanced => Some(3),
            Self::Size => Some(9),
        }
    }
}

/// PQ configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PqConfig {
    /// Directory to store PQ segments.
    pub path: String,
    /// Maximum total size in bytes (default: 1GB).
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,
    /// Maximum events per segment file.
    #[serde(default = "default_segment_size")]
    pub segment_size: usize,
    /// Compression level.
    #[serde(default)]
    pub compression: PqCompression,
    /// Checkpoint interval (events).
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: usize,
}

fn default_max_bytes() -> u64 {
    1_073_741_824 // 1GB
}
fn default_segment_size() -> usize {
    10_000
}
fn default_checkpoint_interval() -> usize {
    1024
}

impl Default for PqConfig {
    fn default() -> Self {
        Self {
            path: "data/queue".to_string(),
            max_bytes: default_max_bytes(),
            segment_size: default_segment_size(),
            compression: PqCompression::None,
            checkpoint_interval: default_checkpoint_interval(),
        }
    }
}

/// A serialized event in the queue.
#[derive(Debug, Serialize, Deserialize)]
struct QueueEntry {
    seq: u64,
    data: String, // JSON-serialized event
}

/// Persistent Queue implementation.
pub struct PersistentQueue {
    config: PqConfig,
    write_seq: u64,
    read_seq: u64,
    current_segment: Option<BufWriter<File>>,
    current_segment_count: usize,
    segment_index: u64,
    total_bytes: u64,
}

impl PersistentQueue {
    /// Create or open a persistent queue.
    pub fn open(config: PqConfig) -> Result<Self> {
        fs::create_dir_all(&config.path)
            .map_err(|e| FerroStashError::Pipeline(format!("cannot create PQ directory: {e}")))?;

        // Load checkpoint
        let checkpoint_path = Path::new(&config.path).join("checkpoint.json");
        let (checkpoint_write_seq, read_seq, checkpoint_segment_index) = if checkpoint_path.exists()
        {
            let content = fs::read_to_string(&checkpoint_path).unwrap_or_default();
            if let Ok(cp) = serde_json::from_str::<serde_json::Value>(&content) {
                (
                    cp["write_seq"].as_u64().unwrap_or(0),
                    cp["read_seq"].as_u64().unwrap_or(0),
                    cp["segment_index"].as_u64().unwrap_or(0),
                )
            } else {
                (0, 0, 0)
            }
        } else {
            (0, 0, 0)
        };

        // Calculate total size
        let total_bytes = Self::calculate_total_size(&config.path);
        let (segment_write_seq, last_segment_index) = Self::recover_segment_state(&config.path);
        let write_seq = checkpoint_write_seq.max(segment_write_seq);
        let segment_index = checkpoint_segment_index.max(last_segment_index);

        info!(
            path = %config.path,
            write_seq,
            read_seq,
            compression = ?config.compression,
            "persistent queue opened"
        );

        Ok(Self {
            config,
            write_seq,
            read_seq,
            current_segment: None,
            current_segment_count: 0,
            segment_index,
            total_bytes,
        })
    }

    fn calculate_total_size(path: &str) -> u64 {
        fs::read_dir(path)
            .map(|entries| {
                entries
                    .filter_map(std::result::Result::ok)
                    .filter(|e| {
                        let name = e.file_name().to_string_lossy().to_string();
                        name.starts_with("segment_")
                            && (name.ends_with(".seg") || name.ends_with(".seg.zst"))
                    })
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }

    fn recover_segment_state(path: &str) -> (u64, u64) {
        let mut next_write_seq = 0_u64;
        let mut last_segment_index = 0_u64;

        let Ok(entries) = fs::read_dir(path) else {
            return (next_write_seq, last_segment_index);
        };

        for entry in entries.filter_map(std::result::Result::ok) {
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !file_name.starts_with("segment_")
                || !(file_name.ends_with(".seg") || file_name.ends_with(".seg.zst"))
            {
                continue;
            }

            if let Some(index) = Self::parse_segment_index(file_name) {
                last_segment_index = last_segment_index.max(index);
            }

            let content = Self::read_segment_to_string(&path);
            for line in content.lines() {
                if let Ok(entry) = serde_json::from_str::<QueueEntry>(line) {
                    next_write_seq = next_write_seq.max(entry.seq.saturating_add(1));
                }
            }
        }

        (next_write_seq, last_segment_index)
    }

    fn parse_segment_index(file_name: &str) -> Option<u64> {
        file_name
            .strip_prefix("segment_")?
            .split('.')
            .next()?
            .parse()
            .ok()
    }

    fn read_segment_to_string(path: &Path) -> String {
        if path.extension().is_some_and(|e| e == "zst") {
            File::open(path)
                .ok()
                .and_then(|f| zstd::Decoder::new(f).ok())
                .and_then(|mut d| {
                    let mut s = String::new();
                    d.read_to_string(&mut s).ok().map(|_| s)
                })
                .unwrap_or_default()
        } else {
            fs::read_to_string(path).unwrap_or_default()
        }
    }

    /// Enqueue a serialized event.
    pub fn enqueue(&mut self, event_json: &str) -> Result<()> {
        if self.total_bytes >= self.config.max_bytes {
            return Err(FerroStashError::Pipeline(
                "persistent queue full".to_string(),
            ));
        }

        // Rotate segment if needed
        if self.current_segment.is_none() || self.current_segment_count >= self.config.segment_size
        {
            self.rotate_segment()?;
        }

        let entry = QueueEntry {
            seq: self.write_seq,
            data: event_json.to_string(),
        };
        let line = serde_json::to_string(&entry)
            .map_err(|e| FerroStashError::Pipeline(format!("PQ serialize error: {e}")))?;

        if let Some(ref mut writer) = self.current_segment {
            writeln!(writer, "{line}")
                .map_err(|e| FerroStashError::Pipeline(format!("PQ write error: {e}")))?;
            self.total_bytes += line.len() as u64 + 1;
        }

        self.write_seq += 1;
        self.current_segment_count += 1;

        // Checkpoint periodically
        if self.write_seq % self.config.checkpoint_interval as u64 == 0 {
            self.checkpoint()?;
        }

        Ok(())
    }

    /// Dequeue events up to `batch_size`.
    pub fn dequeue(&mut self, batch_size: usize) -> Result<Vec<String>> {
        let mut results = Vec::with_capacity(batch_size);

        // Flush current writer before reading
        if let Some(ref mut writer) = self.current_segment {
            writer
                .flush()
                .map_err(|e| FerroStashError::Pipeline(format!("PQ flush error: {e}")))?;
        }

        // Find and read segment files
        let mut segments = self.list_segments()?;
        segments.sort();

        for seg_path in segments {
            if results.len() >= batch_size {
                break;
            }

            let data = if seg_path.extension().is_some_and(|e| e == "zst") {
                // ZSTD compressed
                let file = File::open(&seg_path)
                    .map_err(|e| FerroStashError::Pipeline(format!("PQ read error: {e}")))?;
                let mut decoder = zstd::Decoder::new(file)
                    .map_err(|e| FerroStashError::Pipeline(format!("PQ zstd decode error: {e}")))?;
                let mut content = String::new();
                decoder
                    .read_to_string(&mut content)
                    .map_err(|e| FerroStashError::Pipeline(format!("PQ read error: {e}")))?;
                content
            } else {
                fs::read_to_string(&seg_path)
                    .map_err(|e| FerroStashError::Pipeline(format!("PQ read error: {e}")))?
            };

            for line in data.lines() {
                if results.len() >= batch_size {
                    break;
                }
                if let Ok(entry) = serde_json::from_str::<QueueEntry>(line) {
                    if entry.seq >= self.read_seq {
                        results.push(entry.data);
                        self.read_seq = entry.seq + 1;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Acknowledge processed events and clean up consumed segments.
    pub fn ack(&mut self, up_to_seq: u64) -> Result<()> {
        self.read_seq = up_to_seq;
        self.checkpoint()?;
        self.gc()?;
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<()> {
        // Flush and optionally compress current segment
        if let Some(mut writer) = self.current_segment.take() {
            writer
                .flush()
                .map_err(|e| FerroStashError::Pipeline(format!("PQ flush error: {e}")))?;
            drop(writer);

            // Compress if configured
            if let Some(level) = self.config.compression.zstd_level() {
                let seg_path = Path::new(&self.config.path)
                    .join(format!("segment_{:08}.seg", self.segment_index));
                let compressed_path = Path::new(&self.config.path)
                    .join(format!("segment_{:08}.seg.zst", self.segment_index));

                if seg_path.exists() {
                    let input = fs::read(&seg_path).map_err(|e| {
                        FerroStashError::Pipeline(format!("PQ compress read error: {e}"))
                    })?;
                    let compressed = zstd::encode_all(&input[..], level).map_err(|e| {
                        FerroStashError::Pipeline(format!("PQ zstd compress error: {e}"))
                    })?;
                    fs::write(&compressed_path, &compressed).map_err(|e| {
                        FerroStashError::Pipeline(format!("PQ compress write error: {e}"))
                    })?;
                    let _ = fs::remove_file(&seg_path);
                    let saved = input.len() as i64 - compressed.len() as i64;
                    debug!(
                        segment = self.segment_index,
                        saved_bytes = saved,
                        "segment compressed with ZSTD"
                    );
                }
            }
        }

        self.segment_index += 1;
        self.current_segment_count = 0;

        let seg_path =
            Path::new(&self.config.path).join(format!("segment_{:08}.seg", self.segment_index));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&seg_path)
            .map_err(|e| FerroStashError::Pipeline(format!("PQ segment create error: {e}")))?;
        self.current_segment = Some(BufWriter::new(file));

        Ok(())
    }

    fn list_segments(&self) -> Result<Vec<PathBuf>> {
        let entries = fs::read_dir(&self.config.path)
            .map_err(|e| FerroStashError::Pipeline(format!("PQ list error: {e}")))?;
        let mut segments: Vec<PathBuf> = entries
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("segment_"))
            })
            .collect();
        segments.sort();
        Ok(segments)
    }

    fn gc(&mut self) -> Result<()> {
        let segments = self.list_segments()?;
        for seg_path in segments {
            // Check if all entries in this segment are consumed
            let max_seq = self.extract_max_seq(&seg_path);
            if max_seq < self.read_seq {
                let size = fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0);
                let _ = fs::remove_file(&seg_path);
                self.total_bytes = self.total_bytes.saturating_sub(size);
                debug!(path = %seg_path.display(), "consumed segment removed");
            }
        }
        Ok(())
    }

    fn extract_max_seq(&self, path: &Path) -> u64 {
        // Read last line to get max seq
        let content = Self::read_segment_to_string(path);
        content
            .lines()
            .last()
            .and_then(|line| serde_json::from_str::<QueueEntry>(line).ok())
            .map_or(0, |e| e.seq)
    }

    fn checkpoint(&self) -> Result<()> {
        let checkpoint = serde_json::json!({
            "write_seq": self.write_seq,
            "read_seq": self.read_seq,
            "segment_index": self.segment_index,
        });
        let path = Path::new(&self.config.path).join("checkpoint.json");
        fs::write(
            &path,
            serde_json::to_string_pretty(&checkpoint).unwrap_or_default(),
        )
        .map_err(|e| FerroStashError::Pipeline(format!("checkpoint write error: {e}")))?;
        Ok(())
    }

    /// Pending event count.
    pub fn pending(&self) -> u64 {
        self.write_seq.saturating_sub(self.read_seq)
    }

    /// Total bytes on disk.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Flush and close.
    pub fn close(&mut self) -> Result<()> {
        if let Some(mut w) = self.current_segment.take() {
            let _ = w.flush();
        }
        self.checkpoint()
    }

    // ----- Convenience Event-level API -----

    /// Create a new persistent queue at `path` with a maximum size of `max_size_bytes`.
    ///
    /// This is a convenience constructor that uses default settings for segment size,
    /// compression, and checkpoint interval.
    pub fn new(path: impl Into<String>, max_size_bytes: u64) -> Result<Self> {
        let config = PqConfig {
            path: path.into(),
            max_bytes: max_size_bytes,
            ..PqConfig::default()
        };
        Self::open(config)
    }

    /// Push a single event onto the queue (serialized as JSON).
    pub fn push(&mut self, event: &Event) -> Result<()> {
        let json = event.to_json_string();
        self.enqueue(&json)
    }

    /// Pop a single event from the queue, returning `None` if empty.
    pub fn pop(&mut self) -> Result<Option<Event>> {
        let batch = self.dequeue(1)?;
        if let Some(json_str) = batch.into_iter().next() {
            let value: serde_json::Value = serde_json::from_str(&json_str)
                .map_err(|e| FerroStashError::Pipeline(format!("PQ deserialize error: {e}")))?;
            Ok(Some(Event::from_json(value)))
        } else {
            Ok(None)
        }
    }

    /// Number of unconsumed events in the queue.
    pub fn len(&self) -> u64 {
        self.pending()
    }

    /// Returns `true` if no unconsumed events remain.
    pub fn is_empty(&self) -> bool {
        self.pending() == 0
    }
}

/// Thread-safe wrapper around [`PersistentQueue`].
///
/// All operations acquire an internal mutex, making it safe to share across
/// threads via `Arc<SharedPersistentQueue>`.
pub struct SharedPersistentQueue {
    inner: Arc<Mutex<PersistentQueue>>,
}

impl SharedPersistentQueue {
    /// Create a new shared persistent queue.
    pub fn new(path: impl Into<String>, max_size_bytes: u64) -> Result<Self> {
        let pq = PersistentQueue::new(path, max_size_bytes)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(pq)),
        })
    }

    /// Open with full configuration.
    pub fn open(config: PqConfig) -> Result<Self> {
        let pq = PersistentQueue::open(config)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(pq)),
        })
    }

    /// Push a single event.
    pub fn push(&self, event: &Event) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ lock poisoned: {e}")))?
            .push(event)
    }

    /// Pop a single event.
    pub fn pop(&self) -> Result<Option<Event>> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ lock poisoned: {e}")))?
            .pop()
    }

    /// Number of unconsumed events.
    pub fn len(&self) -> u64 {
        self.inner.lock().map_or(0, |g| g.len())
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Persist the current checkpoint to disk.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ lock poisoned: {e}")))?
            .checkpoint()
    }

    /// Flush and close.
    pub fn close(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ lock poisoned: {e}")))?
            .close()
    }
}

impl Clone for SharedPersistentQueue {
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
    fn test_pq_enqueue_dequeue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = PqConfig {
            path: dir.path().to_string_lossy().to_string(),
            segment_size: 5,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");
        for i in 0..10 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }
        assert_eq!(pq.pending(), 10);

        let batch = pq.dequeue(5).expect("dequeue");
        assert_eq!(batch.len(), 5);
        assert!(batch[0].contains("event 0"));

        let batch2 = pq.dequeue(10).expect("dequeue");
        assert_eq!(batch2.len(), 5);
        assert_eq!(pq.pending(), 0);
    }

    #[test]
    fn test_pq_checkpoint_resume() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 1,
            ..Default::default()
        };

        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            pq.enqueue(r#"{"msg":"a"}"#).expect("enqueue");
            pq.enqueue(r#"{"msg":"b"}"#).expect("enqueue");
            pq.dequeue(1).expect("dequeue");
            pq.close().expect("close");
        }

        {
            let mut pq = PersistentQueue::open(config).expect("reopen");
            let batch = pq.dequeue(10).expect("dequeue");
            assert_eq!(batch.len(), 1);
            assert!(batch[0].contains('b'));
        }
    }

    #[test]
    fn test_pq_compression() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = PqConfig {
            path: dir.path().to_string_lossy().to_string(),
            segment_size: 3,
            compression: PqCompression::Speed,
            checkpoint_interval: 1,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        // Write more than one segment to trigger rotation + compression
        for i in 0..6 {
            pq.enqueue(&format!(
                r#"{{"msg":"event {i} with some padding data for compression ratio"}}"#
            ))
            .expect("enqueue");
        }

        // Check that .zst files exist
        let zst_files: Vec<_> = fs::read_dir(dir.path())
            .expect("readdir")
            .filter_map(std::result::Result::ok)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "zst"))
            .collect();
        assert!(!zst_files.is_empty(), "should have compressed segments");

        // Can still read
        let batch = pq.dequeue(6).expect("dequeue");
        assert_eq!(batch.len(), 6);
    }

    #[test]
    fn test_pq_full() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = PqConfig {
            path: dir.path().to_string_lossy().to_string(),
            max_bytes: 100, // very small
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");
        // Should eventually fail
        let mut failed = false;
        for i in 0..100 {
            if pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#)).is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed, "PQ should reject when full");
    }

    #[test]
    fn test_pq_rebuilds_uncheckpointed_write_position_after_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = PqConfig {
            path: dir.path().to_string_lossy().to_string(),
            checkpoint_interval: 1024,
            ..Default::default()
        };

        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            pq.enqueue(r#"{"msg":"before-crash"}"#).expect("enqueue");
            // Drop without close/checkpoint to simulate a process crash.
        }

        let mut pq = PersistentQueue::open(config).expect("reopen");
        pq.enqueue(r#"{"msg":"after-restart"}"#)
            .expect("enqueue after restart");

        let batch = pq.dequeue(10).expect("dequeue");
        assert_eq!(
            batch,
            vec![
                r#"{"msg":"before-crash"}"#.to_string(),
                r#"{"msg":"after-restart"}"#.to_string(),
            ],
            "reopen must recover write_seq from existing segments before appending new records"
        );
    }

    // ---- Tests for convenience Event-level API ----

    #[test]
    fn test_pq_new_push_pop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq = PersistentQueue::new(path, 1_073_741_824).expect("new");

        let event = Event::new("hello world");
        pq.push(&event).expect("push");
        assert_eq!(pq.len(), 1);
        assert!(!pq.is_empty());

        let popped = pq.pop().expect("pop");
        assert!(popped.is_some());
        let popped = popped.expect("event");
        assert_eq!(popped.message(), Some("hello world"));
        assert_eq!(pq.len(), 0);
        assert!(pq.is_empty());
    }

    #[test]
    fn test_pq_pop_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq = PersistentQueue::new(path, 1_073_741_824).expect("new");
        let popped = pq.pop().expect("pop");
        assert!(popped.is_none());
    }

    #[test]
    fn test_pq_push_multiple_pop_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq = PersistentQueue::new(path, 1_073_741_824).expect("new");

        for i in 0..5 {
            let event = Event::new(format!("msg-{i}"));
            pq.push(&event).expect("push");
        }
        assert_eq!(pq.len(), 5);

        for i in 0..5 {
            let ev = pq.pop().expect("pop").expect("event");
            assert_eq!(ev.message(), Some(format!("msg-{i}").as_str()));
        }
        assert!(pq.is_empty());
    }

    #[test]
    fn test_pq_checkpoint_convenience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq = PersistentQueue::new(path.clone(), 1_073_741_824).expect("new");

        pq.push(&Event::new("ev1")).expect("push");
        pq.push(&Event::new("ev2")).expect("push");
        let _ = pq.pop().expect("pop");
        pq.checkpoint().expect("checkpoint");
        pq.close().expect("close");

        // Reopen — should resume from checkpoint
        let mut pq2 = PersistentQueue::new(path, 1_073_741_824).expect("reopen");
        let ev = pq2.pop().expect("pop").expect("event");
        assert_eq!(ev.message(), Some("ev2"));
        assert!(pq2.pop().expect("pop").is_none());
    }

    #[test]
    fn test_pq_event_with_fields_roundtrip() {
        use crate::event::EventValue;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq = PersistentQueue::new(path, 1_073_741_824).expect("new");

        let mut event = Event::new("test msg");
        event.set("host", EventValue::String("server01".into()));
        event.set("port", EventValue::Integer(8080));
        pq.push(&event).expect("push");

        let restored = pq.pop().expect("pop").expect("event");
        assert_eq!(restored.message(), Some("test msg"));
        assert_eq!(
            restored.get("host"),
            Some(&EventValue::String("server01".into()))
        );
        assert_eq!(restored.get("port"), Some(&EventValue::Integer(8080)));
    }

    #[test]
    fn test_shared_pq_thread_safe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let spq = SharedPersistentQueue::new(path, 1_073_741_824).expect("new");

        let spq2 = spq.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..10 {
                spq2.push(&Event::new(format!("thread-{i}"))).expect("push");
            }
        });
        handle.join().expect("thread join");

        assert_eq!(spq.len(), 10);
        assert!(!spq.is_empty());

        for _ in 0..10 {
            let ev = spq.pop().expect("pop");
            assert!(ev.is_some());
        }
        assert!(spq.is_empty());
        spq.checkpoint().expect("checkpoint");
        spq.close().expect("close");
    }

    // ---- Fault injection and durability tests ----

    #[test]
    fn test_pq_crash_recovery() {
        let dir = tempfile::tempdir().expect("tempdir for crash_recovery");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 200, // high so no auto-checkpoint
            ..Default::default()
        };

        // Push 100 events, then drop without checkpoint (simulate crash)
        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open for crash_recovery");
            for i in 0..100 {
                pq.enqueue(&format!(r#"{{"msg":"crash-{i}"}}"#))
                    .expect("enqueue during crash_recovery");
            }
            // Flush the current segment so data is on disk
            if let Some(ref mut w) = pq.current_segment {
                w.flush().expect("flush before simulated crash");
            }
            // Drop without close/checkpoint — simulates process crash
        }

        // Reopen — all 100 uncommitted events should be recoverable
        let mut pq = PersistentQueue::open(config).expect("reopen after crash");
        let batch = pq.dequeue(200).expect("dequeue after crash");
        assert_eq!(
            batch.len(),
            100,
            "all uncommitted events must survive crash recovery"
        );
        for (i, entry) in batch.iter().enumerate().take(100) {
            assert!(
                entry.contains(&format!("crash-{i}")),
                "event {i} content mismatch after crash recovery"
            );
        }
    }

    #[test]
    fn test_pq_checkpoint_durability() {
        let dir = tempfile::tempdir().expect("tempdir for checkpoint_durability");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 200,
            ..Default::default()
        };

        // Push 100, dequeue 50, checkpoint, close
        {
            let mut pq =
                PersistentQueue::open(config.clone()).expect("open for checkpoint_durability");
            for i in 0..100 {
                pq.enqueue(&format!(r#"{{"msg":"ckpt-{i}"}}"#))
                    .expect("enqueue for checkpoint_durability");
            }
            let _first_50 = pq.dequeue(50).expect("dequeue first 50");
            pq.checkpoint().expect("checkpoint at 50");
            pq.close().expect("close after checkpoint");
        }

        // Reopen — only the remaining 50 should be available
        let mut pq = PersistentQueue::open(config).expect("reopen after checkpoint");
        let remaining = pq.dequeue(200).expect("dequeue remaining");
        assert_eq!(
            remaining.len(),
            50,
            "only 50 events should remain after checkpoint at 50"
        );
        assert!(
            remaining[0].contains("ckpt-50"),
            "first remaining event should be ckpt-50"
        );
    }

    #[test]
    fn test_pq_large_event_handling() {
        let dir = tempfile::tempdir().expect("tempdir for large_event_handling");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq =
            PersistentQueue::new(path, 100_000_000).expect("new PQ for large_event_handling");

        // Create a 100KB+ payload
        let large_payload = "x".repeat(100_000);
        let large_json = format!(r#"{{"message":"{large_payload}"}}"#);

        for i in 0..5 {
            pq.enqueue(&format!(
                r#"{{"msg":"large-{i}","data":"{large_payload}"}}"#
            ))
            .expect("enqueue large event");
        }

        let batch = pq.dequeue(10).expect("dequeue large events");
        assert_eq!(batch.len(), 5, "all large events should dequeue");
        for (i, item) in batch.iter().enumerate() {
            assert!(
                item.contains(&format!("large-{i}")),
                "large event {i} content mismatch"
            );
            assert!(
                item.contains(&large_payload),
                "large event {i} payload truncated"
            );
        }
        drop(large_json); // suppress unused warning
    }

    #[test]
    fn test_pq_concurrent_access() {
        let dir = tempfile::tempdir().expect("tempdir for concurrent_access");
        let path = dir.path().to_string_lossy().to_string();
        let spq = SharedPersistentQueue::new(path, 1_073_741_824).expect("new SharedPQ");

        // Spawn 4 threads, each pushes 250 events = 1000 total
        let mut handles = Vec::new();
        for t in 0..4 {
            let q = spq.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..250 {
                    q.push(&Event::new(format!("t{t}-{i}")))
                        .expect("concurrent push");
                }
            }));
        }
        for h in handles {
            h.join().expect("thread join in concurrent_access");
        }

        assert_eq!(spq.len(), 1000, "all 1000 events should be in the queue");

        // Pop all 1000
        let mut popped = 0;
        while let Some(_ev) = spq.pop().expect("concurrent pop") {
            popped += 1;
        }
        assert_eq!(popped, 1000, "should pop exactly 1000 events");
        assert!(spq.is_empty(), "queue should be empty after popping all");
    }

    #[test]
    fn test_pq_disk_full_simulation() {
        let dir = tempfile::tempdir().expect("tempdir for disk_full_simulation");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path,
            max_bytes: 1024, // 1KB — very small
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open PQ for disk_full_simulation");

        // Push events until we get an error — must NOT panic
        let mut error_count = 0;
        for i in 0..1000 {
            match pq.enqueue(&format!(
                r#"{{"msg":"fill-{i}","pad":"{}"}}"#,
                "A".repeat(100)
            )) {
                Ok(()) => {}
                Err(e) => {
                    let err_msg = format!("{e}");
                    assert!(
                        err_msg.contains("full"),
                        "error should mention 'full', got: {err_msg}"
                    );
                    error_count += 1;
                    break;
                }
            }
        }
        assert!(
            error_count > 0,
            "should eventually get a 'queue full' error"
        );
    }

    #[test]
    fn test_pq_corruption_recovery() {
        let dir = tempfile::tempdir().expect("tempdir for corruption_recovery");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 200,
            ..Default::default()
        };

        // Write some valid events first
        {
            let mut pq =
                PersistentQueue::open(config.clone()).expect("open for corruption_recovery");
            for i in 0..5 {
                pq.enqueue(&format!(r#"{{"msg":"valid-{i}"}}"#))
                    .expect("enqueue valid event");
            }
            pq.close().expect("close before corruption");
        }

        // Write garbage bytes to the segment file
        let entries = fs::read_dir(&path).expect("read dir for corruption");
        for entry in entries.filter_map(std::result::Result::ok) {
            let p = entry.path();
            if p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("segment_"))
            {
                // Append garbage (valid UTF-8 but invalid JSON)
                let mut f = OpenOptions::new()
                    .append(true)
                    .open(&p)
                    .expect("open segment for corruption");
                f.write_all(b"\nGARBAGE LINE NOT JSON\n{broken json without closing\n!@#$%^&*()\n")
                    .expect("write garbage");
            }
        }

        // Reopen should not panic
        let mut pq = PersistentQueue::open(config).expect("reopen after corruption");
        // Dequeue should skip corrupted lines and still return valid entries
        let batch = pq.dequeue(100).expect("dequeue after corruption");
        assert!(
            batch.len() >= 5,
            "should recover at least the 5 valid events, got {}",
            batch.len()
        );
    }

    #[test]
    fn test_pq_empty_file_recovery() {
        let dir = tempfile::tempdir().expect("tempdir for empty_file_recovery");
        let path = dir.path().to_string_lossy().to_string();
        fs::create_dir_all(&path).expect("create PQ dir");

        // Create an empty segment file
        let seg_path = Path::new(&path).join("segment_00000001.seg");
        File::create(&seg_path).expect("create empty segment file");

        let config = PqConfig {
            path: path.clone(),
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open with empty segment file");

        // Should be able to enqueue/dequeue normally
        pq.enqueue(r#"{"msg":"after-empty"}"#)
            .expect("enqueue after empty file");
        let batch = pq.dequeue(10).expect("dequeue after empty file");
        assert!(
            !batch.is_empty(),
            "should dequeue the event written after empty file recovery"
        );
    }

    #[test]
    fn test_pq_rapid_push_pop() {
        let dir = tempfile::tempdir().expect("tempdir for rapid_push_pop");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq = PersistentQueue::new(path, 1_073_741_824).expect("new PQ for rapid_push_pop");

        // Alternating push/pop of 1000 events
        let mut push_count = 0_u64;
        let mut pop_count = 0_u64;
        for i in 0..1_000 {
            pq.push(&Event::new(format!("rapid-{i}")))
                .expect("rapid push");
            push_count += 1;

            if let Some(_ev) = pq.pop().expect("rapid pop") {
                pop_count += 1;
            }
        }

        // Drain any remaining
        while let Some(_ev) = pq.pop().expect("drain pop") {
            pop_count += 1;
        }

        assert_eq!(
            push_count, pop_count,
            "push count ({push_count}) must equal pop count ({pop_count})"
        );
        assert!(pq.is_empty(), "queue should be empty after drain");
    }

    #[test]
    fn test_pq_checkpoint_after_empty() {
        let dir = tempfile::tempdir().expect("tempdir for checkpoint_after_empty");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 200,
            ..Default::default()
        };

        {
            let mut pq =
                PersistentQueue::open(config.clone()).expect("open for checkpoint_after_empty");
            // Push then pop all
            for i in 0..10 {
                pq.enqueue(&format!(r#"{{"msg":"empty-{i}"}}"#))
                    .expect("enqueue for checkpoint_after_empty");
            }
            let _ = pq.dequeue(10).expect("dequeue all");
            pq.checkpoint().expect("checkpoint after emptying");
            pq.close().expect("close after checkpoint_after_empty");
        }

        // Reopen — should be empty
        let mut pq = PersistentQueue::open(config).expect("reopen after checkpoint_after_empty");
        let batch = pq.dequeue(100).expect("dequeue after reopen");
        assert!(
            batch.is_empty(),
            "queue should be empty after checkpoint on drained queue"
        );
    }

    #[test]
    fn test_pq_multiple_reopen() {
        let dir = tempfile::tempdir().expect("tempdir for multiple_reopen");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 1,
            ..Default::default()
        };

        // Open/close/reopen cycle 10 times with events in between
        for cycle in 0..10 {
            let mut pq = PersistentQueue::open(config.clone())
                .unwrap_or_else(|e| panic!("open cycle {cycle}: {e}"));
            pq.enqueue(&format!(r#"{{"msg":"cycle-{cycle}"}}"#))
                .unwrap_or_else(|e| panic!("enqueue cycle {cycle}: {e}"));
            pq.close()
                .unwrap_or_else(|e| panic!("close cycle {cycle}: {e}"));
        }

        // Final reopen — dequeue all events
        let mut pq = PersistentQueue::open(config).expect("final reopen");
        let batch = pq.dequeue(100).expect("final dequeue");
        assert_eq!(
            batch.len(),
            10,
            "should have one event from each of the 10 cycles"
        );
        for (cycle, entry) in batch.iter().enumerate().take(10) {
            assert!(
                entry.contains(&format!("cycle-{cycle}")),
                "cycle {cycle} event mismatch"
            );
        }
    }
}
