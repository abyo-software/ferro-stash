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
    /// `fsync` every segment append and every checkpoint to disk (power-loss
    /// durable). Off by default: a plain `flush` makes the queue durable to a
    /// *process* crash but not a power loss / kernel panic, which is the right
    /// trade for most pipelines. Turn it on when the host can lose power and you
    /// need committed events to survive — at a significant throughput cost
    /// (a disk sync per append).
    #[serde(default)]
    pub fsync: bool,
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
            fsync: false,
        }
    }
}

/// `fsync` a directory so a newly-created or renamed entry within it is durably
/// linked (POSIX: the file's data being fsync'd does not guarantee its directory
/// entry survives a power loss until the directory itself is fsync'd). Best
/// effort — a platform that cannot open a directory as a file simply skips it.
pub(crate) fn sync_dir(dir: &str) -> Result<()> {
    if let Ok(handle) = File::open(dir) {
        handle
            .sync_all()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ dir fsync error: {e}")))?;
    }
    Ok(())
}

/// A serialized event in the queue.
#[derive(Debug, Serialize, Deserialize)]
struct QueueEntry {
    seq: u64,
    data: String, // JSON-serialized event
}

/// Persistent Queue implementation.
///
/// Two read cursors give at-least-once delivery:
///
/// - `read_seq` — the in-memory *pop* cursor. [`dequeue`](Self::dequeue)/`pop`
///   advance it so a single run never re-reads an entry. It is NOT the durable
///   recovery point.
/// - `ack_seq` — the *durable* cursor. It only advances via [`ack`](Self::ack),
///   which is called after the consumer has delivered the event downstream. It
///   is the value persisted in `checkpoint.json`, the floor `gc` reclaims below,
///   and the position `read_seq` is reset to on reopen. An entry that was popped
///   but not yet acked (in flight, or lost to a crash) therefore replays on the
///   next start — that is the at-least-once guarantee. (Before this split the
///   single `read_seq` was checkpointed on read, making the PQ a durable buffer
///   but NOT at-least-once: popped-then-crashed events were lost.)
pub struct PersistentQueue {
    config: PqConfig,
    write_seq: u64,
    read_seq: u64,
    ack_seq: u64,
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

        // Load checkpoint. The durable cursor is `ack_seq`; older checkpoints
        // (pre at-least-once) only wrote `read_seq`, which was the
        // checkpoint-on-read position, so fall back to it for back-compat. The
        // in-memory pop cursor `read_seq` is reset to `ack_seq` so any entry that
        // was popped but not durably acknowledged replays on reopen.
        let checkpoint_path = Path::new(&config.path).join("checkpoint.json");
        let (checkpoint_write_seq, ack_seq, checkpoint_segment_index) = if checkpoint_path.exists()
        {
            let content = fs::read_to_string(&checkpoint_path).unwrap_or_default();
            if let Ok(cp) = serde_json::from_str::<serde_json::Value>(&content) {
                (
                    cp["write_seq"].as_u64().unwrap_or(0),
                    cp["ack_seq"]
                        .as_u64()
                        .or_else(|| cp["read_seq"].as_u64())
                        .unwrap_or(0),
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
        let (segment_write_seq, last_segment_index) =
            Self::recover_segment_state(&config.path, config.segment_size);
        let write_seq = checkpoint_write_seq.max(segment_write_seq);
        let segment_index = checkpoint_segment_index.max(last_segment_index);

        info!(
            path = %config.path,
            write_seq,
            ack_seq,
            compression = ?config.compression,
            "persistent queue opened"
        );

        Ok(Self {
            config,
            write_seq,
            // In-memory pop cursor starts at the durable ack point so unacked
            // entries are re-delivered (at-least-once) on reopen.
            read_seq: ack_seq,
            ack_seq,
            current_segment: None,
            current_segment_count: 0,
            segment_index,
            total_bytes,
        })
    }

    /// Sum the on-disk sizes of every current segment file (`segment_*.seg`
    /// and `segment_*.seg.zst`) in `path`.
    ///
    /// This is the single source of truth for `total_bytes`: a `stat` per file
    /// (no content read), so it is cheap to call after operations that change
    /// the on-disk segment set. It is used both on reopen (when there is no
    /// live writer) and — via [`Self::recompute_total_bytes`] — after
    /// `rotate_segment`/`gc` to keep `total_bytes` authoritative.
    fn calculate_total_size(path: &str) -> u64 {
        // Sum the deduped segment set (one file per index) so a transient
        // `.seg`+`.seg.zst` pair left by a crash mid-rotation is not double-counted
        // toward `max_bytes` (which could falsely wedge `enqueue` at "queue full").
        Self::segment_files(path)
            .iter()
            .filter_map(|p| fs::metadata(p).ok())
            .map(|m| m.len())
            .sum()
    }

    /// Recompute `total_bytes` from the actual on-disk segment files, making it
    /// authoritative.
    ///
    /// Call this after any operation that changes the on-disk segment set
    /// (`rotate_segment`, which compresses `.seg` → `.seg.zst`, and `gc`, which
    /// deletes consumed segments). The per-`enqueue` increment is only a
    /// fast-path estimate between recomputes; recomputing here is what prevents
    /// accounting drift — most importantly the compression drift where a
    /// segment is enqueued by its uncompressed size but reclaimed by its
    /// (smaller) compressed size, leaving the difference stuck in `total_bytes`
    /// until the next restart. Once that drift reaches `max_bytes`, `enqueue`
    /// wrongly reports "persistent queue full" and the producer silently falls
    /// back to the in-memory channel, defeating the PQ's crash-durability
    /// guarantee (same failure class as the round-23 gc-wiring bug).
    ///
    /// The active write segment's `BufWriter` may hold bytes not yet visible to
    /// `fs::metadata`, so it is flushed first; after the flush the on-disk image
    /// matches what reopen would see, so [`Self::calculate_total_size`] yields
    /// the same value reopen computes — `total_bytes == sum of on-disk segment
    /// sizes`.
    fn recompute_total_bytes(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.current_segment {
            writer
                .flush()
                .map_err(|e| FerroStashError::Pipeline(format!("PQ flush error: {e}")))?;
        }
        self.total_bytes = Self::calculate_total_size(&self.config.path);
        Ok(())
    }

    fn recover_segment_state(path: &str, segment_size: usize) -> (u64, u64) {
        let mut next_write_seq = 0_u64;
        let mut last_segment_index = 0_u64;
        let mut unreadable = 0_u64;

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

            match Self::try_read_segment_to_string(&path) {
                Some(content) => {
                    for line in content.lines() {
                        if let Ok(entry) = serde_json::from_str::<QueueEntry>(line) {
                            next_write_seq = next_write_seq.max(entry.seq.saturating_add(1));
                        }
                    }
                }
                // An unreadable segment hides the seqs it assigned. If we recovered
                // `write_seq` only from the readable segments (and the checkpoint
                // lags), a new enqueue could REUSE a seq the corrupt segment already
                // holds; a later dequeue would then skip the duplicate-new record
                // and gc could delete it — data loss. Reserve a conservative gap of
                // one full segment's worth of seqs per unreadable segment so
                // `write_seq` always clears anything they could contain. Over-
                // reserving just skips seq numbers (harmless, u64); reuse is loss.
                None => unreadable = unreadable.saturating_add(1),
            }
        }

        let next_write_seq =
            next_write_seq.saturating_add(unreadable.saturating_mul(segment_size.max(1) as u64));
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

    /// Read a segment's text, distinguishing a read/decode **failure** (`None`)
    /// from a legitimately **empty** file (`Some("")`). gc and recovery need this
    /// distinction so they never mistake an *unreadable* segment for an
    /// empty-and-reclaimable one (which would silently delete unacked data or
    /// rewind the write cursor into reused sequence numbers).
    fn try_read_segment_to_string(path: &Path) -> Option<String> {
        if path.extension().is_some_and(|e| e == "zst") {
            let file = File::open(path).ok()?;
            let mut decoder = zstd::Decoder::new(file).ok()?;
            let mut s = String::new();
            decoder.read_to_string(&mut s).ok()?;
            Some(s)
        } else {
            fs::read_to_string(path).ok()
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
            // Power-loss durability: push the append out of the BufWriter and
            // fsync the segment so the committed event survives a power loss, not
            // just a process crash. Gated on `fsync` (a disk sync per append is
            // expensive); `sync_data` (not `sync_all`) skips the inode-metadata
            // sync since the size is recovered by re-reading the segment.
            if self.config.fsync {
                writer
                    .flush()
                    .map_err(|e| FerroStashError::Pipeline(format!("PQ flush error: {e}")))?;
                writer
                    .get_ref()
                    .sync_data()
                    .map_err(|e| FerroStashError::Pipeline(format!("PQ fsync error: {e}")))?;
            }
        }

        self.write_seq += 1;
        self.current_segment_count += 1;

        // Checkpoint periodically. `checkpoint_interval` is an unvalidated
        // `usize` from config; a value of 0 would make this modulo a
        // division-by-zero panic (same zero-period class as the output flush
        // timer). Clamp the divisor to >=1.
        if self.write_seq % (self.config.checkpoint_interval.max(1) as u64) == 0 {
            self.checkpoint()?;
        }

        Ok(())
    }

    /// Dequeue events up to `batch_size`, returning only their payloads.
    ///
    /// Advances the in-memory pop cursor (`read_seq`) but NOT the durable
    /// `ack_seq` — call [`ack`](Self::ack) after the events have been delivered
    /// downstream to make the consumption durable. See
    /// [`dequeue_with_seq`](Self::dequeue_with_seq) for the variant that returns
    /// each entry's sequence (needed to acknowledge it).
    pub fn dequeue(&mut self, batch_size: usize) -> Result<Vec<String>> {
        Ok(self
            .dequeue_with_seq(batch_size)?
            .into_iter()
            .map(|(_, data)| data)
            .collect())
    }

    /// Dequeue events up to `batch_size`, returning `(seq, payload)` pairs.
    ///
    /// The `seq` is the durable queue-entry sequence; the caller passes the
    /// highest contiguously-delivered `seq + 1` back to [`ack`](Self::ack) to
    /// advance the durable cursor. Advances `read_seq` only (the in-memory pop
    /// cursor), so an entry that is popped but never acked replays after a
    /// restart — the at-least-once guarantee.
    pub fn dequeue_with_seq(&mut self, batch_size: usize) -> Result<Vec<(u64, String)>> {
        let mut results = Vec::with_capacity(batch_size);
        // Advance a LOCAL cursor and commit it to `self.read_seq` only once the
        // whole batch is assembled without error. If a later segment errors
        // mid-batch (the `?` below), `self.read_seq` is left untouched so the
        // retry re-reads the earlier segments' entries — otherwise the entries
        // already pushed would have advanced `read_seq`, be dropped with the
        // errored batch, and then be skipped (and gc-deleted) on the next call
        // (silent loss for a multi-segment `batch_size >= 2` read).
        let mut next_read_seq = self.read_seq;

        // Flush current writer before reading
        if let Some(ref mut writer) = self.current_segment {
            writer
                .flush()
                .map_err(|e| FerroStashError::Pipeline(format!("PQ flush error: {e}")))?;
        }

        // Find and read segment files. `list_segments` (via `segment_files`)
        // already returns them ordered by NUMERIC index. Do NOT re-sort
        // lexicographically: names use minimum-width `{:08}`, so once the index
        // passes 8 digits `segment_100000000` would sort *before*
        // `segment_99999999`, causing dequeue to read a higher-index segment
        // first, advance `read_seq`, and silently skip unread entries in the
        // lower-index one (data loss).
        let segments = self.list_segments()?;

        for seg_path in segments {
            if results.len() >= batch_size {
                break;
            }

            // Propagate read/decode errors rather than swallowing them. The
            // deduped listing (`segment_files`) already drops a duplicate
            // `.seg.zst` in favour of the intact `.seg`, so the only segment that
            // can fail to read here is a *lone* genuinely-unreadable one. For that,
            // a hard error (the drainer warns + retries — a safe wedge) is correct:
            // a TRANSIENT fault (EMFILE/ENOMEM/EIO) clears on retry, and a partial
            // corruption is preserved for salvage. Swallowing the error instead
            // would skip the segment, let `ack_point` jump the gap, and let gc
            // delete intact-but-temporarily-unreadable data — silent loss.
            let data = if seg_path.extension().is_some_and(|e| e == "zst") {
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
                    if entry.seq >= next_read_seq {
                        next_read_seq = entry.seq + 1;
                        results.push((entry.seq, entry.data));
                    }
                }
            }
        }

        // Commit the cursor only after the whole batch read without error.
        self.read_seq = next_read_seq;
        Ok(results)
    }

    /// Acknowledge durable delivery up to (but not including) `up_to_seq`, then
    /// reclaim any segments now fully below the durable cursor.
    ///
    /// `up_to_seq` is the exclusive high-water mark of contiguously-delivered
    /// entries (i.e. the next sequence still in flight). This advances the
    /// durable `ack_seq` — and pulls the in-memory `read_seq` forward to match if
    /// it somehow lagged — checkpoints it, and gcs. Both cursors move
    /// monotonically (`max`), so an out-of-order or stale ack can never rewind
    /// the queue and re-deliver already-acked data.
    pub fn ack(&mut self, up_to_seq: u64) -> Result<()> {
        self.ack_seq = self.ack_seq.max(up_to_seq);
        // Acked entries are durably done; never re-pop them in this run either.
        self.read_seq = self.read_seq.max(self.ack_seq);
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
                    // Make the compressed segment durable+linked BEFORE removing
                    // the (already per-append-fsync'd) original, so a power loss in
                    // fsync mode can never lose BOTH: fsync the `.zst` contents,
                    // fsync the directory to durably link it, remove the original,
                    // then fsync the directory again so the removal is durable. The
                    // original `.seg` remains the fallback until the `.zst` is
                    // confirmed on disk; if both transiently exist, reads dedupe by
                    // `seq`. Without this, the later active-segment dir fsync could
                    // durably record the removal while the `.zst` is still only in
                    // page cache — losing unacked events.
                    if self.config.fsync {
                        File::open(&compressed_path)
                            .and_then(|f| f.sync_all())
                            .map_err(|e| {
                                FerroStashError::Pipeline(format!("PQ compress fsync error: {e}"))
                            })?;
                        sync_dir(&self.config.path)?;
                    }
                    let _ = fs::remove_file(&seg_path);
                    if self.config.fsync {
                        sync_dir(&self.config.path)?;
                    }
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

        // Durably link the freshly-created segment file (its directory entry) so
        // a power loss right after the first append cannot leave the committed
        // event in an unlinked file. Only under `fsync` (a dir sync per rotation
        // is cheap relative to per-append syncs but still pointless otherwise).
        if self.config.fsync {
            sync_dir(&self.config.path)?;
        }

        // The on-disk segment set just changed: the rotated segment may have
        // been compressed (its on-disk size shrank), and a fresh empty active
        // segment was created. Recompute `total_bytes` from disk so it reflects
        // real usage and the uncompressed-vs-compressed delta does not drift
        // upward (see `recompute_total_bytes`). The freshly-opened active
        // segment is empty, so no BufWriter bytes are lost by recomputing here.
        self.recompute_total_bytes()?;

        Ok(())
    }

    fn list_segments(&self) -> Result<Vec<PathBuf>> {
        Ok(Self::segment_files(&self.config.path))
    }

    /// The current segment files, **deduplicated by index** and sorted by index.
    ///
    /// When both `segment_N.seg` and `segment_N.seg.zst` exist for the same index
    /// — a transient state after a crash between writing the compressed `.zst`
    /// and removing the original `.seg` during compression-rotation — the
    /// uncompressed `.seg` is preferred: it is the durable original and is
    /// definitely valid, whereas the `.zst` may be partial/corrupt; gc removes
    /// the leftover later. Without this dedup, recovery would read both copies
    /// (`dequeue` could error on the corrupt `.zst` and wedge) and
    /// `calculate_total_size` would double-count the index toward `max_bytes`.
    fn segment_files(path: &str) -> Vec<PathBuf> {
        let Ok(entries) = fs::read_dir(path) else {
            return Vec::new();
        };
        let mut by_index: std::collections::BTreeMap<u64, PathBuf> =
            std::collections::BTreeMap::new();
        for entry in entries.filter_map(std::result::Result::ok) {
            let p = entry.path();
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with("segment_")
                || !(name.ends_with(".seg") || name.ends_with(".seg.zst"))
            {
                continue;
            }
            let Some(idx) = Self::parse_segment_index(name) else {
                continue;
            };
            let is_plain = name.ends_with(".seg");
            // Take this file if there is nothing for the index yet, or if this is
            // the plain `.seg` replacing a previously-seen compressed `.zst`.
            let take = match by_index.get(&idx) {
                None => true,
                Some(existing) => is_plain && existing.extension().is_some_and(|e| e == "zst"),
            };
            if take {
                by_index.insert(idx, p);
            }
        }
        by_index.into_values().collect()
    }

    /// Reclaim fully-consumed segment files.
    ///
    /// A segment is removed only when EVERY entry in it has `seq < read_seq`
    /// (its highest sequence number is below the read cursor), so no un-read
    /// data is ever discarded. Removing a segment decrements `total_bytes`,
    /// which is what keeps `enqueue` from permanently wedging at `max_bytes`
    /// in a long-running pipeline that is actively consuming events, and bounds
    /// the per-`dequeue` re-scan cost (consumed segments are gone).
    ///
    /// The currently-open write segment is never collected: its `BufWriter`
    /// may hold un-flushed bytes (so its on-disk image can read as empty/stale),
    /// and deleting the file out from under the live writer would lose data.
    ///
    /// The delete decision is made by [`Self::segment_fully_consumed`], which
    /// scans EVERY line of the segment (not just the last) and refuses to
    /// reclaim a segment that contains any unparsable line. A crash or partial
    /// append can leave valid un-read entries followed by a truncated/garbage
    /// trailing line; a last-line-only check would mis-read that as
    /// fully-consumed and drop the recoverable data, so the scan is mandatory.
    pub fn gc(&mut self) -> Result<()> {
        let active_segment =
            Path::new(&self.config.path).join(format!("segment_{:08}.seg", self.segment_index));
        let segments = self.list_segments()?;
        for seg_path in segments {
            // Never reclaim the active (currently-written) segment.
            if self.current_segment.is_some() && seg_path == active_segment {
                continue;
            }
            // Only reclaim a segment when every line parsed cleanly AND every
            // entry's `seq` is below the durable ACK cursor (fully delivered). A
            // segment with any unparsable line is conservatively kept: corruption
            // is recoverable (see `test_pq_corruption_recovery`) and there may be
            // valid un-acked entries we must not lose. Gating on `ack_seq` (not
            // the in-memory `read_seq`) is what makes reclamation safe under
            // at-least-once: an entry that was popped but not yet delivered is
            // never deleted, so it can replay after a crash/restart.
            if self.segment_fully_consumed(&seg_path) {
                let _ = fs::remove_file(&seg_path);
                // Also remove the same-index duplicate sibling (the other form),
                // if any. A crash mid-compression-rotation can leave both
                // `segment_N.seg` and a (possibly corrupt) `segment_N.seg.zst`;
                // `segment_files` dedups to the `.seg`, so this loop only sees and
                // removes the `.seg` — leaving the `.zst` as a now-lone orphan that
                // would re-wedge dequeue on the next pass (DD r4). Removing the
                // sibling here is loss-safe: the (readable) twin was just confirmed
                // fully-acked, and both forms hold identical data, so the sibling's
                // data is provably acked too.
                let sibling = if seg_path.extension().is_some_and(|e| e == "zst") {
                    seg_path.with_extension("") // strip `.zst` -> `…segment_N.seg`
                } else {
                    let mut s = seg_path.clone().into_os_string();
                    s.push(".zst"); // `…segment_N.seg` -> `…segment_N.seg.zst`
                    PathBuf::from(s)
                };
                let _ = fs::remove_file(&sibling);
                debug!(path = %seg_path.display(), "consumed segment removed");
            }
        }
        // Recompute `total_bytes` from the surviving on-disk segments rather
        // than subtracting each deleted file's size. The per-file subtraction
        // was only ever correct for uncompressed segments; with compression
        // enabled the enqueue increment (uncompressed) and the delete decrement
        // (compressed) disagree, so the difference accumulated in `total_bytes`
        // and eventually wedged `enqueue` at `max_bytes` mid-run. Recomputing
        // from disk makes `total_bytes` authoritative and closes that drift
        // class for good (see `recompute_total_bytes`).
        self.recompute_total_bytes()?;
        Ok(())
    }

    /// Decide whether `path` is safe to reclaim, i.e. every entry it holds has
    /// already been durably acknowledged (`seq < ack_seq`).
    ///
    /// This is the corruption-aware GC delete predicate. Unlike a last-line
    /// peek it scans ALL lines:
    ///
    /// - Blank lines are ignored (trailing newlines etc.).
    /// - If ANY non-blank line fails to parse as a [`QueueEntry`], the segment
    ///   is treated as NOT consumed (returns `false`) — corruption may sit
    ///   *after* still-valid un-acked entries (e.g. a truncated final append
    ///   following good records), and those recoverable entries must not be
    ///   dropped. This includes a fully-acked segment whose trailing line is
    ///   corrupted: it is conservatively kept (safe — keeping a file never
    ///   loses data; the cost is at most a delayed reclaim).
    /// - An empty/whitespace-only segment has no entries to lose; it is
    ///   reclaimed (max over zero parsed entries is 0, which is `< ack_seq`
    ///   once `ack_seq > 0`, matching the pre-existing empty-segment behavior).
    fn segment_fully_consumed(&self, path: &Path) -> bool {
        // An unreadable / decode-failing segment is conservatively KEPT (returns
        // false = not consumed): treating it as empty would let gc reclaim a lone
        // corrupt or transiently-unreadable segment whose unacked entries are not
        // visible (`max_seq` would stay 0 < ack_seq), silently deleting data the
        // PQ claims at-least-once for. This mirrors the dequeue wedge-and-retry:
        // a bad segment is preserved for retry/salvage, never silently dropped.
        let Some(content) = Self::try_read_segment_to_string(path) else {
            return false;
        };
        let mut max_seq = 0_u64;
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<QueueEntry>(line) {
                Ok(entry) => max_seq = max_seq.max(entry.seq),
                // Any unparsable non-blank line -> conservatively keep.
                Err(_) => return false,
            }
        }
        max_seq < self.ack_seq
    }

    fn checkpoint(&self) -> Result<()> {
        // `ack_seq` is the durable recovery cursor. `read_seq` is mirrored to it
        // (NOT the in-memory pop cursor) so an older reader that only knows the
        // `read_seq` field still recovers at the at-least-once-correct floor and
        // replays anything popped-but-unacked.
        let checkpoint = serde_json::json!({
            "write_seq": self.write_seq,
            "ack_seq": self.ack_seq,
            "read_seq": self.ack_seq,
            "segment_index": self.segment_index,
        });
        let path = Path::new(&self.config.path).join("checkpoint.json");
        let body = serde_json::to_string_pretty(&checkpoint).unwrap_or_default();
        if self.config.fsync {
            // Atomic + durable: write a temp file, fsync it, rename over the
            // checkpoint (atomic on POSIX), then fsync the directory so the
            // rename itself is durable. A power loss thus leaves either the old or
            // the new checkpoint — never a torn one — and the survivor is on disk.
            let tmp = Path::new(&self.config.path).join("checkpoint.json.tmp");
            {
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp)
                    .map_err(|e| {
                        FerroStashError::Pipeline(format!("checkpoint tmp open error: {e}"))
                    })?;
                f.write_all(body.as_bytes())
                    .map_err(|e| FerroStashError::Pipeline(format!("checkpoint write error: {e}")))?;
                f.sync_all()
                    .map_err(|e| FerroStashError::Pipeline(format!("checkpoint fsync error: {e}")))?;
            }
            fs::rename(&tmp, &path)
                .map_err(|e| FerroStashError::Pipeline(format!("checkpoint rename error: {e}")))?;
            sync_dir(&self.config.path)?;
        } else {
            fs::write(&path, &body)
                .map_err(|e| FerroStashError::Pipeline(format!("checkpoint write error: {e}")))?;
        }
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
        Ok(self.pop_with_seq()?.map(|(_, event)| event))
    }

    /// Pop a single event together with its durable queue sequence, returning
    /// `None` if empty.
    ///
    /// The `seq` must be acknowledged via [`ack`](Self::ack) (as `seq + 1`, the
    /// exclusive high-water mark) only AFTER the event has been delivered
    /// downstream — that ack-after-delivery ordering is what makes the queue
    /// at-least-once. The drainer stamps this `seq` onto the event so the output
    /// path can acknowledge it once delivery completes.
    pub fn pop_with_seq(&mut self) -> Result<Option<(u64, Event)>> {
        let batch = self.dequeue_with_seq(1)?;
        if let Some((seq, json_str)) = batch.into_iter().next() {
            let value: serde_json::Value = serde_json::from_str(&json_str)
                .map_err(|e| FerroStashError::Pipeline(format!("PQ deserialize error: {e}")))?;
            Ok(Some((seq, Event::from_json(value))))
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

    /// Pop a single event together with its durable queue sequence.
    ///
    /// The caller acknowledges it via [`ack`](Self::ack) (as `seq + 1`) only
    /// after the event has been delivered downstream — see
    /// [`PersistentQueue::pop_with_seq`].
    pub fn pop_with_seq(&self) -> Result<Option<(u64, Event)>> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ lock poisoned: {e}")))?
            .pop_with_seq()
    }

    /// Acknowledge durable delivery up to (exclusive) `up_to_seq`, advancing the
    /// durable cursor, checkpointing, and reclaiming fully-acked segments.
    pub fn ack(&self, up_to_seq: u64) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ lock poisoned: {e}")))?
            .ack(up_to_seq)
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

    /// Reclaim fully-acknowledged segment files (segments whose every entry has
    /// `seq < ack_seq`), recomputing `total_bytes`.
    ///
    /// Gated on the durable `ack_seq`, not the in-memory pop cursor, so an entry
    /// that was popped but not yet acknowledged is never removed and can replay
    /// after a restart (at-least-once). Reclamation therefore follows
    /// [`SharedPersistentQueue::ack`], not [`SharedPersistentQueue::pop`].
    pub fn gc(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| FerroStashError::Pipeline(format!("PQ lock poisoned: {e}")))?
            .gc()
    }

    /// Current total bytes held on disk by segment files.
    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().map_or(0, |g| g.total_bytes())
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
    fn test_pq_zero_checkpoint_interval_does_not_panic() {
        // A config of `checkpoint_interval: 0` must not cause a
        // division-by-zero panic on the modulo in `enqueue` (zero-period
        // class). Enqueue must succeed and events remain readable.
        let dir = tempfile::tempdir().expect("tempdir");
        let config = PqConfig {
            path: dir.path().to_string_lossy().to_string(),
            checkpoint_interval: 0,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");
        for i in 0..5 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue must not panic with checkpoint_interval=0");
        }
        assert_eq!(pq.pending(), 5);
        let batch = pq.dequeue(5).expect("dequeue");
        assert_eq!(batch.len(), 5);
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
            // At-least-once: only acked consumption is durable. Acknowledge the
            // first entry so "a" is not replayed on reopen (a bare dequeue
            // without ack would replay both, by design).
            pq.ack(1).expect("ack first");
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
        let (seq, _ev1) = pq.pop_with_seq().expect("pop").expect("event");
        // At-least-once: acknowledge the delivered entry so it is durable and not
        // replayed on reopen (a bare pop without ack would replay it, by design).
        pq.ack(seq + 1).expect("ack");
        pq.checkpoint().expect("checkpoint");
        pq.close().expect("close");

        // Reopen — should resume from the acked checkpoint
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
            // At-least-once: only ACKed consumption is durable. Acknowledge the
            // first 50 so they are not replayed on reopen (a bare dequeue without
            // ack would, by design, replay all 100).
            pq.ack(50).expect("ack first 50");
            pq.close().expect("close after checkpoint");
        }

        // Reopen — only the remaining 50 should be available
        let mut pq = PersistentQueue::open(config).expect("reopen after checkpoint");
        let remaining = pq.dequeue(200).expect("dequeue remaining");
        assert_eq!(
            remaining.len(),
            50,
            "only 50 events should remain after ack at 50"
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
            // At-least-once: durability follows ack, not read. Acknowledge all 10
            // so the drained queue stays empty across reopen.
            pq.ack(10).expect("ack all");
            pq.checkpoint().expect("checkpoint after emptying");
            pq.close().expect("close after checkpoint_after_empty");
        }

        // Reopen — should be empty
        let mut pq = PersistentQueue::open(config).expect("reopen after checkpoint_after_empty");
        let batch = pq.dequeue(100).expect("dequeue after reopen");
        assert!(
            batch.is_empty(),
            "queue should be empty after acking a drained queue"
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

    // ---- GC / segment-reclamation regression tests (DD round-23) ----

    /// Counts the number of `segment_*` files currently on disk.
    fn count_segment_files(path: &str) -> usize {
        fs::read_dir(path)
            .map(|entries| {
                entries
                    .filter_map(std::result::Result::ok)
                    .filter(|e| e.file_name().to_string_lossy().starts_with("segment_"))
                    .count()
            })
            .unwrap_or(0)
    }

    #[test]
    fn test_pq_gc_reclaims_total_bytes_after_consumption() {
        // Regression (a): once events are consumed, `gc` must reclaim the
        // segment bytes so `total_bytes` does not stay monotonic. Without
        // reclamation, `total_bytes` would hit `max_bytes` and `enqueue` would
        // wedge forever even though the queue is being drained.
        let dir = tempfile::tempdir().expect("tempdir");
        let config = PqConfig {
            path: dir.path().to_string_lossy().to_string(),
            segment_size: 5, // small segments so several get fully consumed
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        // Fill several full segments, then start a fresh one so earlier
        // segments are no longer the active write segment.
        for i in 0..25 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }
        let bytes_before = pq.total_bytes();
        assert!(bytes_before > 0, "should have written bytes");

        // Consume AND acknowledge everything. Under at-least-once, gc reclaims
        // below the durable ack cursor, so consumption alone (dequeue) is not
        // enough — the consumer must ack.
        let batch = pq.dequeue(25).expect("dequeue");
        assert_eq!(batch.len(), 25);
        assert_eq!(pq.pending(), 0);
        pq.ack(25).expect("ack");

        // gc (already run by ack) must have reclaimed the acked (non-active)
        // segments.
        pq.gc().expect("gc");
        assert!(
            pq.total_bytes() < bytes_before,
            "gc must reclaim acked segment bytes (before={bytes_before}, after={})",
            pq.total_bytes()
        );
    }

    #[test]
    fn test_pq_gc_deletes_consumed_segment_files() {
        // Regression (b): consumed (fully-read, non-active) segment files must
        // be deleted from disk, not merely accounted for.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        for i in 0..25 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }
        let files_before = count_segment_files(&path);
        assert!(files_before > 1, "should have rotated multiple segments");

        let batch = pq.dequeue(25).expect("dequeue");
        assert_eq!(batch.len(), 25);
        // Acknowledge so the consumed segments fall below the durable cursor and
        // become reclaimable.
        pq.ack(25).expect("ack");

        pq.gc().expect("gc");
        let files_after = count_segment_files(&path);
        assert!(
            files_after < files_before,
            "gc must delete acked segment files (before={files_before}, after={files_after})"
        );
    }

    #[test]
    fn test_pq_gc_does_not_remove_unread_data() {
        // Regression (c): gc must NOT remove segments that still contain
        // un-read entries (`seq >= read_seq`), even partially-read ones.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        for i in 0..25 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }

        // Consume only the first 12 — segment boundaries (size 5) mean the
        // 3rd segment (seqs 10..14) is only partially read.
        let consumed = pq.dequeue(12).expect("dequeue");
        assert_eq!(consumed.len(), 12);
        assert_eq!(pq.pending(), 13);

        pq.gc().expect("gc");

        // The remaining 13 un-read events must still be fully recoverable.
        let remaining = pq.dequeue(100).expect("dequeue remaining");
        assert_eq!(
            remaining.len(),
            13,
            "gc must not drop the 13 un-read events"
        );
        for (idx, entry) in remaining.iter().enumerate() {
            let expected = 12 + idx; // events 12..24 should remain
            assert!(
                entry.contains(&format!("event {expected}")),
                "remaining event {idx} should be 'event {expected}', got {entry}"
            );
        }
    }

    #[test]
    fn test_pq_does_not_wedge_when_consumed_via_gc() {
        // Regression: a producer must be able to keep enqueuing well past a
        // `max_bytes`-worth of cumulative throughput as long as events are
        // consumed and reclaimed. Without gc wiring, `total_bytes` is monotonic
        // and `enqueue` returns "queue full" after one `max_bytes` of lifetime
        // throughput even though `pending()` stays near zero.
        let dir = tempfile::tempdir().expect("tempdir");
        let config = PqConfig {
            path: dir.path().to_string_lossy().to_string(),
            segment_size: 4,
            max_bytes: 4_096, // small cap; cumulative throughput will far exceed it
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        // Each event is ~30+ bytes; 4096 cap holds ~100 events of standing
        // backlog. Push+drain 2000 events: cumulative bytes (~60KB) >> max_bytes,
        // so this would wedge without reclamation.
        for i in 0..2_000 {
            pq.enqueue(&format!(r#"{{"msg":"throughput-event-{i}"}}"#))
                .unwrap_or_else(|e| {
                    panic!("enqueue {i} wedged at max_bytes despite consumption: {e}")
                });
            // Consume immediately and acknowledge periodically. Under
            // at-least-once, reclamation follows the durable ack cursor, so the
            // consumer must ack (a bare dequeue would back the queue up); `ack`
            // both checkpoints and gcs.
            let _ = pq.dequeue(1).expect("dequeue");
            if i % 8 == 0 {
                pq.ack(i + 1).expect("ack");
            }
        }
        // Final drain + ack; queue should be empty and well under max_bytes.
        pq.ack(2_000).expect("final ack");
        assert_eq!(pq.pending(), 0, "all events should be consumed");
        assert!(
            pq.total_bytes() < 4_096,
            "total_bytes ({}) must stay under max_bytes after reclamation",
            pq.total_bytes()
        );
    }

    #[test]
    fn test_pq_ack_reclaims_consumed_segments() {
        // `ack(up_to_seq)` sets the read cursor, checkpoints, and gcs in one
        // call. After acking past fully-consumed segments their files and
        // bytes must be reclaimed.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        for i in 0..25 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }
        let files_before = count_segment_files(&path);
        let bytes_before = pq.total_bytes();

        // Ack the first 20 events (seqs 0..19 consumed; cursor at 20).
        pq.ack(20).expect("ack");
        let files_after = count_segment_files(&path);
        assert!(
            files_after < files_before,
            "ack must reclaim fully-consumed segment files"
        );
        assert!(
            pq.total_bytes() < bytes_before,
            "ack must reclaim consumed bytes"
        );

        // The remaining 5 events (seqs 20..24) must still be readable.
        let remaining = pq.dequeue(100).expect("dequeue remaining");
        assert_eq!(remaining.len(), 5, "ack must not drop un-acked events");
        assert!(remaining[0].contains("event 20"));
    }

    #[test]
    fn test_pq_gc_preserves_active_write_segment() {
        // The currently-open write segment must never be reclaimed even if its
        // on-disk image looks empty (un-flushed BufWriter) and read_seq > 0.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        // Fill and consume one full segment, then write one more event into a
        // fresh (active) segment WITHOUT flushing.
        for i in 0..6 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }
        // Read+ack the first 5 so the durable cursor advances past the first full
        // segment (acked => reclaimable), leaving the un-read seq 5 in the active
        // segment.
        let _ = pq.dequeue(5).expect("dequeue");
        pq.ack(5).expect("ack first segment");
        // gc with an active, possibly-unflushed segment must not delete it and
        // must not lose the un-read event(s).
        pq.gc().expect("gc with active segment");

        // The 6th event (seq 5) must still be readable from the active segment.
        let remaining = pq.dequeue(100).expect("dequeue after gc");
        assert_eq!(
            remaining.len(),
            1,
            "the un-read event in the active segment must survive gc"
        );
        assert!(remaining[0].contains("event 5"));
    }

    // ---- GC corruption-robustness regression tests (DD round-24) ----

    /// Appends raw bytes to the first non-active `segment_*` file on disk and
    /// returns the path that was touched. Used to inject a truncated/garbage
    /// trailing line into a segment that already holds valid entries.
    fn append_raw_to_first_segment(path: &str, active_index: u64, bytes: &[u8]) -> PathBuf {
        let active_name = format!("segment_{active_index:08}.seg");
        let active_name_zst = format!("segment_{active_index:08}.seg.zst");
        let mut segs: Vec<PathBuf> = fs::read_dir(path)
            .expect("read dir")
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("segment_"))
            })
            .filter(|p| {
                let n = p.file_name().map(|n| n.to_string_lossy().to_string());
                n.as_deref() != Some(active_name.as_str())
                    && n.as_deref() != Some(active_name_zst.as_str())
            })
            .collect();
        segs.sort();
        let target = segs
            .first()
            .cloned()
            .expect("at least one non-active segment");
        let mut f = OpenOptions::new()
            .append(true)
            .open(&target)
            .expect("open segment for raw append");
        f.write_all(bytes).expect("append raw bytes");
        f.flush().expect("flush raw append");
        target
    }

    #[test]
    fn test_pq_gc_keeps_segment_with_unread_then_corrupt_tail() {
        // DD round-24 regression (a): a segment holding valid UNREAD entries
        // (seq >= read_seq) followed by a truncated/garbage trailing line must
        // NOT be reclaimed by gc, and those unread entries must remain
        // recoverable. A last-line-only `extract_max_seq` would parse only the
        // garbage tail, return 0, and delete the whole segment — losing data.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5, // 5 entries per segment
            checkpoint_interval: 1,
            ..Default::default()
        };

        // Write 5 full segments worth (25 entries), then one more event into a
        // fresh active segment so the earlier segments are non-active and
        // closed on disk.
        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            for i in 0..26 {
                pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                    .expect("enqueue");
            }
            pq.close().expect("close");
        }

        // Corrupt the FIRST segment (seqs 0..4) by appending a truncated line
        // AFTER its valid entries. The active (last) segment index is 6 because
        // rotate_segment pre-increments and the first segment is index 1.
        // We discover the active index from the queue state instead.
        let active_index = {
            let pq = PersistentQueue::open(config.clone()).expect("reopen to read index");
            pq.segment_index
        };
        let corrupted = append_raw_to_first_segment(
            &path,
            active_index,
            b"{truncated json without closing brace, valid utf8 but not parseable\n",
        );

        // Reopen, consume only the FIRST segment's worth (seqs 0..4) so its
        // valid entries are read but the LATER segments are still un-read.
        let mut pq = PersistentQueue::open(config).expect("reopen after corruption");
        // Note: dequeue itself skips the corrupt line; we only read the 5 valid
        // entries of the corrupted segment to set read_seq to 5.
        let consumed = pq.dequeue(5).expect("dequeue first segment");
        assert_eq!(consumed.len(), 5, "first segment's 5 valid entries read");
        // Acknowledge the first 5 so the corrupted segment is fully below the ACK
        // cursor — its retention must be driven by corruption-conservatism, not
        // by the entries being un-acked.
        pq.ack(5).expect("ack first segment");

        // gc now. ack_seq == 5. The corrupted segment holds entries 0..4
        // (all < 5) BUT also a corrupt line. Conservative policy keeps it.
        pq.gc().expect("gc");
        assert!(
            corrupted.exists(),
            "gc must NOT delete a segment containing a corrupt (possibly unread) line"
        );

        // The later segments' un-read entries (5..25) must all be recoverable.
        let remaining = pq.dequeue(1000).expect("dequeue remaining");
        assert_eq!(
            remaining.len(),
            21,
            "all 21 later un-read events must survive gc (got {})",
            remaining.len()
        );
        for (idx, entry) in remaining.iter().enumerate() {
            let expected = 5 + idx;
            assert!(
                entry.contains(&format!("event {expected}")),
                "remaining event {idx} should be 'event {expected}', got {entry}"
            );
        }
    }

    #[test]
    fn test_pq_gc_keeps_fully_unread_segment_with_corrupt_tail() {
        // Sharper variant of (a): a segment whose valid entries are ALL un-read
        // (seq >= read_seq) and which also has a corrupt trailing line must be
        // kept AND its valid entries must still dequeue. This is the direct
        // data-loss case: last-line-only -> max_seq 0 -> 0 < read_seq -> delete.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5,
            checkpoint_interval: 1,
            ..Default::default()
        };

        // Write 2 full segments (seqs 0..9) plus an active segment, then
        // consume the first segment so read_seq == 5 while the SECOND segment
        // (seqs 5..9) is entirely un-read.
        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            for i in 0..11 {
                pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                    .expect("enqueue");
            }
            pq.close().expect("close");
        }

        let active_index = {
            let pq = PersistentQueue::open(config.clone()).expect("reopen for index");
            pq.segment_index
        };

        // Corrupt the SECOND segment (seqs 5..9), which is entirely un-read once
        // read_seq advances to 5. Find it: the second-smallest non-active seg.
        let second_seg = {
            let active_name = format!("segment_{active_index:08}.seg");
            let active_name_zst = format!("segment_{active_index:08}.seg.zst");
            let mut segs: Vec<PathBuf> = fs::read_dir(&path)
                .expect("read dir")
                .filter_map(std::result::Result::ok)
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .is_some_and(|n| n.to_string_lossy().starts_with("segment_"))
                })
                .filter(|p| {
                    let n = p.file_name().map(|n| n.to_string_lossy().to_string());
                    n.as_deref() != Some(active_name.as_str())
                        && n.as_deref() != Some(active_name_zst.as_str())
                })
                .collect();
            segs.sort();
            segs.get(1).cloned().expect("second non-active segment")
        };
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(&second_seg)
                .expect("open second segment");
            f.write_all(b"GARBAGE TRUNCATED TAIL\n")
                .expect("append garbage");
            f.flush().expect("flush");
        }

        let mut pq = PersistentQueue::open(config).expect("reopen");
        // Read the first segment only (seqs 0..4) -> read_seq == 5.
        let first = pq.dequeue(5).expect("dequeue first segment");
        assert_eq!(first.len(), 5);
        // Acknowledge the first segment so the still-unread second segment is the
        // genuine subject of the test (kept because it holds un-acked entries
        // behind a corrupt tail).
        pq.ack(5).expect("ack first segment");

        pq.gc().expect("gc");
        assert!(
            second_seg.exists(),
            "gc must NOT delete a fully-unread segment with a corrupt tail"
        );

        // Its 5 valid un-read entries (5..9) plus the active segment's entry
        // (seq 10) must still be recoverable.
        let remaining = pq.dequeue(1000).expect("dequeue remaining");
        assert_eq!(
            remaining.len(),
            6,
            "all 6 later un-read events must survive gc (got {})",
            remaining.len()
        );
        assert!(remaining[0].contains("event 5"));
        assert!(remaining.last().is_some_and(|e| e.contains("event 10")));
    }

    #[test]
    fn test_pq_gc_reclaims_fully_read_clean_segment() {
        // DD round-24 regression (b): a fully-read segment with NO corruption
        // (every entry seq < read_seq, all lines parse) must STILL be reclaimed
        // — the corruption-aware predicate must not regress the round-23
        // reclaim behavior.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        for i in 0..25 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }
        let files_before = count_segment_files(&path);
        assert!(files_before > 1, "should have rotated multiple segments");

        let batch = pq.dequeue(25).expect("dequeue");
        assert_eq!(batch.len(), 25);
        // Acknowledge so the clean, fully-read segments fall below the ack cursor.
        pq.ack(25).expect("ack");

        pq.gc().expect("gc");
        let files_after = count_segment_files(&path);
        assert!(
            files_after < files_before,
            "gc must still reclaim clean, fully-acked segments (before={files_before}, after={files_after})"
        );
    }

    #[test]
    fn test_pq_gc_keeps_fully_read_segment_with_corrupt_tail_conservative() {
        // DD round-24 regression (c): a segment whose valid entries are ALL
        // already read (seq < read_seq) but whose last line is corrupted.
        //
        // DOCUMENTED CHOICE: conservative — the segment is KEPT (not reclaimed).
        // This is always data-loss-safe (keeping a file never drops entries; the
        // only cost is a delayed reclaim). We cannot distinguish "corrupt tail
        // after only-read entries" from "corrupt tail hiding unread entries"
        // without trusting the corrupt bytes, so we keep it. Reclamation
        // resumes for this segment if/when the corruption is repaired or the
        // segment is otherwise removed.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 5,
            checkpoint_interval: 1,
            ..Default::default()
        };

        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            for i in 0..11 {
                pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                    .expect("enqueue");
            }
            pq.close().expect("close");
        }

        let active_index = {
            let pq = PersistentQueue::open(config.clone()).expect("reopen for index");
            pq.segment_index
        };
        // Corrupt the FIRST segment (seqs 0..4).
        let corrupted = append_raw_to_first_segment(&path, active_index, b"NOT JSON TAIL\n");

        // Consume past ALL entries (read_seq advances to 11) so the corrupted
        // first segment is genuinely fully-read.
        let mut pq = PersistentQueue::open(config).expect("reopen");
        let drained = pq.dequeue(1000).expect("dequeue all");
        assert_eq!(drained.len(), 11, "all 11 valid entries should drain");
        assert_eq!(pq.pending(), 0);
        // Acknowledge everything so the corrupt first segment is fully below the
        // ack cursor: its retention must be driven by corruption, not by un-acked
        // entries.
        pq.ack(11).expect("ack all");

        pq.gc().expect("gc");
        // Conservative: corrupt first segment is KEPT despite being fully-acked.
        assert!(
            corrupted.exists(),
            "conservative policy keeps a corrupt segment even when fully-acked (safe)"
        );
    }

    // ---- Compression accounting-drift regression tests (DD round-25) ----

    /// Sums the actual on-disk sizes of all current `segment_*` files. This is
    /// the ground truth `total_bytes` must agree with after rotate/gc.
    fn on_disk_segment_bytes(path: &str) -> u64 {
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

    #[test]
    fn test_pq_compression_does_not_drift_total_bytes_and_does_not_wedge() {
        // DD round-25 regression: with COMPRESSION enabled, `enqueue` adds the
        // UNCOMPRESSED line size to `total_bytes` while `rotate_segment`
        // compresses the segment on disk. Before the fix, rotate only LOGGED the
        // savings and gc subtracted only the COMPRESSED size, so every consumed
        // compressed segment left `(uncompressed - compressed)` stuck in
        // `total_bytes`. Over a long run that drift reaches `max_bytes` even
        // though the on-disk segments were reclaimed, and `enqueue` wrongly
        // returns "persistent queue full" — silently defeating PQ durability
        // (same class as the round-23 gc-wiring bug).
        //
        // The fix recomputes `total_bytes` from the on-disk segment sizes after
        // rotate/gc, so it stays equal to real disk usage and never wedges.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 4, // small segments => frequent rotation + compression
            // Small cap: cumulative UNCOMPRESSED throughput will far exceed it,
            // so the pre-fix drift would wedge `enqueue` well before the loop
            // ends even though events are being drained and reclaimed.
            max_bytes: 8_192,
            compression: PqCompression::Size, // max compression => max drift
            checkpoint_interval: 1,
            fsync: false,
        };
        let mut pq = PersistentQueue::open(config).expect("open");

        // Highly compressible payload (long run of repeated bytes) so the
        // uncompressed/compressed gap per segment is large.
        let pad = "A".repeat(400);

        // Push + drain many events. Cumulative uncompressed bytes (~2000 *
        // ~430B ≈ 860KB) dwarf the 8KB cap; this only stays under the cap if
        // reclamation accounting is authoritative.
        for i in 0..2_000 {
            pq.enqueue(&format!(r#"{{"msg":"compressible-{i}","pad":"{pad}"}}"#))
                .unwrap_or_else(|e| {
                    panic!(
                        "enqueue {i} wedged at max_bytes despite consumption \
                         (compression accounting drift): {e}"
                    )
                });
            let _ = pq.dequeue(1).expect("dequeue");
            // Acknowledge periodically so reclamation can run (ack gcs); under
            // at-least-once a bare dequeue does not advance the durable cursor.
            if i % 4 == 0 {
                pq.ack(i + 1).expect("ack");
            }
        }

        // Final drain + ack. The queue is empty; only the (empty) active write
        // segment should remain on disk.
        pq.ack(2_000).expect("final ack");
        assert_eq!(pq.pending(), 0, "all events should be consumed");

        let on_disk = on_disk_segment_bytes(&path);
        assert_eq!(
            pq.total_bytes(),
            on_disk,
            "total_bytes ({}) must equal actual on-disk segment bytes ({on_disk}) after gc",
            pq.total_bytes()
        );
        // After draining everything, the active segment is the fresh empty one
        // (it never compresses), so total_bytes returns to ~0 — NOT the
        // cumulative uncompressed-minus-compressed residue.
        assert!(
            pq.total_bytes() < 4_096,
            "total_bytes ({}) must collapse back to ~active-segment size after \
             draining, not accumulate compression drift",
            pq.total_bytes()
        );
        // And the producer must still be able to enqueue (not wedged).
        pq.enqueue(r#"{"msg":"post-drain still accepts writes"}"#)
            .expect("enqueue must still succeed after a long compressed run");
    }

    // ---- At-least-once delivery (read_seq / ack_seq split) tests ----

    #[test]
    fn test_pq_at_least_once_replays_popped_but_unacked_after_restart() {
        // The core at-least-once guarantee: entries popped (read) but NOT
        // acknowledged before a crash MUST replay on reopen. Before the
        // read_seq/ack_seq split, a bare dequeue checkpointed the read cursor and
        // these events were silently lost.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 1,
            ..Default::default()
        };

        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            for i in 0..10 {
                pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                    .expect("enqueue");
            }
            // Pop ALL 10 but acknowledge NONE (simulates events in flight to the
            // output when the process dies). Checkpoint to make the durable state
            // observable, then drop without acking.
            let popped = pq.dequeue_with_seq(10).expect("dequeue");
            assert_eq!(popped.len(), 10, "all 10 read in this run");
            pq.checkpoint().expect("checkpoint");
            pq.close().expect("close (no ack)");
        }

        // Reopen: every un-acked entry must replay.
        let mut pq = PersistentQueue::open(config).expect("reopen");
        let replayed = pq.dequeue(100).expect("dequeue after restart");
        assert_eq!(
            replayed.len(),
            10,
            "all 10 popped-but-unacked events must replay (at-least-once)"
        );
        for (i, entry) in replayed.iter().enumerate() {
            assert!(
                entry.contains(&format!("event {i}")),
                "replayed event {i} mismatch: {entry}"
            );
        }
    }

    #[test]
    fn test_pq_partial_ack_replays_only_unacked_tail_after_restart() {
        // A consumer that acked the first 6 of 10 delivered events: on restart
        // only the 4 un-acked tail entries replay (at-least-once with no
        // unnecessary duplication of the acked prefix).
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 1,
            ..Default::default()
        };

        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            for i in 0..10 {
                pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                    .expect("enqueue");
            }
            let _ = pq.dequeue_with_seq(10).expect("dequeue all");
            // Acknowledge only the first 6 (seqs 0..5 delivered; 6 is the next
            // exclusive high-water mark). The remaining 4 are still in flight.
            pq.ack(6).expect("ack first 6");
            pq.close().expect("close");
        }

        let mut pq = PersistentQueue::open(config).expect("reopen");
        let replayed = pq.dequeue(100).expect("dequeue after restart");
        assert_eq!(
            replayed.len(),
            4,
            "only the 4 un-acked tail events should replay"
        );
        assert!(replayed[0].contains("event 6"), "tail starts at event 6");
        assert!(replayed[3].contains("event 9"), "tail ends at event 9");
    }

    #[test]
    fn test_pq_ack_is_monotonic_and_ignores_stale_acks() {
        // An out-of-order / stale ack must never rewind the durable cursor and
        // re-expose already-acked entries.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            checkpoint_interval: 1,
            ..Default::default()
        };
        let mut pq = PersistentQueue::open(config.clone()).expect("open");
        for i in 0..10 {
            pq.enqueue(&format!(r#"{{"msg":"event {i}"}}"#))
                .expect("enqueue");
        }
        let _ = pq.dequeue(10).expect("dequeue all");
        pq.ack(8).expect("ack 8");
        // A stale/duplicate lower ack must be a no-op (no rewind).
        pq.ack(3).expect("stale ack");
        pq.close().expect("close");

        let mut pq = PersistentQueue::open(config).expect("reopen");
        let replayed = pq.dequeue(100).expect("dequeue after restart");
        assert_eq!(
            replayed.len(),
            2,
            "ack must be monotonic: only seqs 8,9 remain despite the stale ack(3)"
        );
        assert!(replayed[0].contains("event 8"));
        assert!(replayed[1].contains("event 9"));
    }

    #[test]
    fn test_pq_pop_with_seq_returns_contiguous_sequences() {
        // `pop_with_seq` must hand back the durable queue sequence so the caller
        // can acknowledge it; sequences are contiguous from 0 in FIFO order.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let mut pq = PersistentQueue::new(path, 1_073_741_824).expect("new");
        for i in 0..5 {
            pq.push(&Event::new(format!("e{i}"))).expect("push");
        }
        for expected_seq in 0..5 {
            let (seq, ev) = pq.pop_with_seq().expect("pop").expect("event");
            assert_eq!(
                seq, expected_seq,
                "sequence must be contiguous and in order"
            );
            assert_eq!(ev.message(), Some(format!("e{expected_seq}").as_str()));
        }
        assert!(pq.pop_with_seq().expect("pop").is_none(), "drained");
    }

    #[test]
    fn test_pq_fsync_mode_roundtrips_durably() {
        // With `fsync` enabled, enqueue fsyncs each segment append, and the
        // checkpoint is written atomically (temp -> fsync -> rename -> dir fsync).
        // Exercise the whole fsync path and confirm the durable state is correct
        // across a reopen (unit tests can't cut power, but this proves the fsync
        // code paths produce a consistent, recoverable queue — no torn checkpoint,
        // segments readable).
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 4, // force rotations (exercises the dir fsync on rotate)
            checkpoint_interval: 1,
            fsync: true,
            // Compression ON so rotations exercise the durable `.seg`->`.seg.zst`
            // replacement path under fsync (DD found that rewrite was not durable).
            compression: PqCompression::Speed,
            ..Default::default()
        };
        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open fsync");
            for i in 0..10 {
                pq.enqueue(&format!(r#"{{"msg":"fsync-{i}"}}"#))
                    .expect("enqueue fsync");
            }
            let popped = pq.dequeue(10).expect("dequeue");
            assert_eq!(popped.len(), 10);
            // Acknowledge 6; the atomic+fsync checkpoint must persist ack_seq=6.
            pq.ack(6).expect("ack");
            pq.close().expect("close");
        }
        // Reopen: the durable checkpoint (written via the atomic fsync path) must
        // be intact, so exactly the 4 un-acked tail entries replay.
        let mut pq = PersistentQueue::open(config).expect("reopen fsync");
        let remaining = pq.dequeue(100).expect("dequeue after reopen");
        assert_eq!(
            remaining.len(),
            4,
            "fsync-mode atomic checkpoint must recover ack_seq=6 (4 entries replay)"
        );
        assert!(remaining[0].contains("fsync-6"));
    }

    #[test]
    fn test_pq_dedup_prefers_seg_over_corrupt_duplicate_zst() {
        // DD round-2 regression: a crash mid-compression-rotation can leave both
        // `segment_N.seg` (the durable original) and a partial `segment_N.seg.zst`
        // for the same index. Recovery must prefer the intact `.seg` and must NOT
        // error the whole dequeue on the corrupt `.zst` (which would wedge the
        // drainer), nor double-count both toward `max_bytes`.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = PqConfig {
            path: path.clone(),
            segment_size: 100, // single segment
            ..Default::default()
        };
        {
            let mut pq = PersistentQueue::open(config.clone()).expect("open");
            for i in 0..3 {
                pq.enqueue(&format!(r#"{{"msg":"e{i}"}}"#)).expect("enqueue");
            }
            pq.close().expect("close"); // flushes segment_00000001.seg
        }
        // Simulate the crash artifact: a CORRUPT `.zst` next to the intact `.seg`.
        let zst = Path::new(&path).join("segment_00000001.seg.zst");
        fs::write(&zst, b"this is not a valid zstd stream").expect("write corrupt zst");
        assert!(
            Path::new(&path).join("segment_00000001.seg").exists(),
            "the intact .seg must still be present"
        );

        // Reopen + dequeue: must read the intact `.seg` (3 entries), ignoring the
        // corrupt duplicate `.zst` — no error, no wedge.
        let mut pq = PersistentQueue::open(config.clone()).expect("reopen");
        let batch = pq.dequeue(100).expect("dequeue must not error on corrupt duplicate");
        assert_eq!(
            batch.len(),
            3,
            "must recover all 3 entries from the intact .seg, ignoring the corrupt .zst"
        );
        assert!(batch[0].contains("e0") && batch[2].contains("e2"));

        // Ack + gc must reclaim BOTH the `.seg` AND its corrupt `.zst` sibling, so
        // the `.zst` is not left as a lone orphan that would re-wedge dequeue on
        // the next pass (DD r4).
        pq.ack(3).expect("ack");
        pq.gc().expect("gc");
        assert!(
            !Path::new(&path).join("segment_00000001.seg").exists(),
            "the fully-acked .seg must be reclaimed"
        );
        assert!(
            !zst.exists(),
            "gc must also remove the corrupt duplicate .zst sibling (no lone orphan)"
        );

        // Reopen and dequeue: with the orphan gone, the queue is cleanly empty —
        // no wedge on a leftover corrupt `.zst`.
        let mut pq = PersistentQueue::open(config).expect("reopen after gc");
        let after = pq.dequeue(100).expect("dequeue after gc must not wedge");
        assert!(after.is_empty(), "queue must be cleanly empty after gc");
    }
}
