// SPDX-License-Identifier: Apache-2.0
//! Pipeline engine — orchestrates input → filter → output flow.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::buffer::{BatchCollector, BufferConfig};
use crate::dead_letter_queue::{DlqConfig, SharedDeadLetterQueue};
use crate::error::Result;
use crate::event::Event;
use crate::metrics::PipelineMetrics;
use crate::persistent_queue::{PqConfig, SharedPersistentQueue};
use crate::plugin::{FilterPlugin, InputPlugin, OutputPlugin};
use crate::shutdown::ShutdownSignal;

/// Persistent queue mode for the pipeline.
#[derive(Debug, Clone, Default)]
pub enum QueueType {
    /// In-memory channel (default, no persistence).
    #[default]
    Memory,
    /// Disk-backed persistent queue with the given configuration.
    Persisted(PqConfig),
}

/// Dead letter queue configuration for the pipeline.
#[derive(Debug, Clone, Default)]
pub struct DlqSettings {
    /// Enable the DLQ.
    pub enable: bool,
    /// DLQ configuration (path, max_bytes, etc.).
    pub config: Option<DlqConfig>,
}

/// Pipeline configuration.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Number of worker threads for filter processing.
    pub workers: usize,
    /// Buffer configuration between stages.
    pub buffer: BufferConfig,
    /// Pipeline ID.
    pub id: String,
    /// When `true`, the pipeline exits automatically once all inputs complete
    /// and the queue is drained (Logstash behavior for multi-pipeline generators).
    /// When `false` (default for single-pipeline mode), the pipeline stays alive
    /// until an explicit shutdown signal.
    pub auto_exit_on_inputs_done: bool,
    /// Queue type — memory (default) or persisted.
    pub queue: QueueType,
    /// Dead letter queue settings.
    pub dead_letter_queue: DlqSettings,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            workers: num_cpus::get(),
            buffer: BufferConfig::default(),
            id: "main".to_string(),
            auto_exit_on_inputs_done: false,
            queue: QueueType::Memory,
            dead_letter_queue: DlqSettings::default(),
        }
    }
}

impl PipelineConfig {
    fn _num_cpus() -> usize {
        std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1)
    }
}

/// The pipeline engine.
///
/// Tracks metrics on two levels, Logstash-compatible:
/// - `instance_metrics`: process-wide cumulative (shared across pipeline reloads)
/// - `pipeline_metrics`: per-pipeline, reset on reload
pub struct Pipeline {
    config: PipelineConfig,
    inputs: Vec<Box<dyn InputPlugin>>,
    filters: Vec<Box<dyn FilterPlugin>>,
    outputs: Vec<Box<dyn OutputPlugin>>,
    /// Per-pipeline metrics (reset on reload).
    metrics: Arc<PipelineMetrics>,
    /// Instance-wide metrics (shared across pipeline instances/reloads).
    instance_metrics: Option<Arc<PipelineMetrics>>,
    /// Optional persistent queue (set when `queue.type = persisted`).
    persistent_queue: Option<SharedPersistentQueue>,
    /// Optional dead letter queue.
    dead_letter_queue: Option<SharedDeadLetterQueue>,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Self::new_with_metrics(config, PipelineMetrics::new())
    }

    /// Creates a pipeline that ALSO updates a shared instance-wide metrics.
    /// Pipeline metrics (returned by `metrics()`) are reset on each reload,
    /// instance metrics are cumulative for the whole process lifetime.
    pub fn new_with_instance_metrics(
        config: PipelineConfig,
        instance_metrics: Arc<PipelineMetrics>,
    ) -> Self {
        let (pq, dlq) = Self::init_queue_and_dlq(&config);
        Self {
            config,
            inputs: Vec::new(),
            filters: Vec::new(),
            outputs: Vec::new(),
            metrics: PipelineMetrics::new(),
            instance_metrics: Some(instance_metrics),
            persistent_queue: pq,
            dead_letter_queue: dlq,
        }
    }

    /// Legacy: shares one metrics instance for both roles (no reload distinction).
    pub fn new_with_metrics(config: PipelineConfig, metrics: Arc<PipelineMetrics>) -> Self {
        let (pq, dlq) = Self::init_queue_and_dlq(&config);
        Self {
            config,
            inputs: Vec::new(),
            filters: Vec::new(),
            outputs: Vec::new(),
            metrics,
            instance_metrics: None,
            persistent_queue: pq,
            dead_letter_queue: dlq,
        }
    }

    fn init_queue_and_dlq(
        config: &PipelineConfig,
    ) -> (Option<SharedPersistentQueue>, Option<SharedDeadLetterQueue>) {
        let pq = match &config.queue {
            QueueType::Memory => None,
            QueueType::Persisted(pq_config) => {
                match SharedPersistentQueue::open(pq_config.clone()) {
                    Ok(q) => {
                        info!(path = %pq_config.path, "persistent queue enabled");
                        Some(q)
                    }
                    Err(e) => {
                        error!(error = %e, "failed to open persistent queue, falling back to memory");
                        None
                    }
                }
            }
        };

        let dlq = if config.dead_letter_queue.enable {
            let dlq_config = config.dead_letter_queue.config.clone().unwrap_or_default();
            match SharedDeadLetterQueue::open(dlq_config.clone()) {
                Ok(q) => {
                    info!(path = %dlq_config.path, "dead letter queue enabled");
                    Some(q)
                }
                Err(e) => {
                    error!(error = %e, "failed to open dead letter queue");
                    None
                }
            }
        } else {
            None
        };

        (pq, dlq)
    }

    pub fn add_input(&mut self, input: Box<dyn InputPlugin>) {
        self.inputs.push(input);
    }

    pub fn add_filter(&mut self, filter: Box<dyn FilterPlugin>) {
        self.filters.push(filter);
    }

    pub fn add_output(&mut self, output: Box<dyn OutputPlugin>) {
        self.outputs.push(output);
    }

    pub fn metrics(&self) -> Arc<PipelineMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Returns a reference to the persistent queue, if enabled.
    pub fn persistent_queue(&self) -> Option<&SharedPersistentQueue> {
        self.persistent_queue.as_ref()
    }

    /// Returns a reference to the dead letter queue, if enabled.
    pub fn dead_letter_queue(&self) -> Option<&SharedDeadLetterQueue> {
        self.dead_letter_queue.as_ref()
    }

    /// Runs the pipeline until shutdown.
    pub async fn run(self, mut shutdown: ShutdownSignal) -> Result<()> {
        let pipeline_id = self.config.id.clone();
        info!(pipeline = %pipeline_id, "starting pipeline");

        let metrics = self.metrics;
        let instance_metrics = self.instance_metrics;
        let buffer_config = self.config.buffer.clone();
        let auto_exit = self.config.auto_exit_on_inputs_done;
        let pq = self.persistent_queue;
        let dlq = self.dead_letter_queue.clone();

        // At-least-once delivery tracker: present only when a persistent queue is
        // configured. The drainer registers popped entries, filter workers record
        // fan-out, and the output task acknowledges (durably checkpoints) each
        // entry only after delivery. Memory-queue pipelines carry no tracker, so
        // the whole accounting path compiles to None-checks with zero overhead.
        let ack_tracker: Option<Arc<AckTracker>> = pq.as_ref().map(|_| Arc::new(AckTracker::new()));

        // Channel capacity is derived from the unvalidated `max_events` config
        // (`pipeline.buffer_size`, a `usize`). `tokio::sync::mpsc::channel` asserts
        // `buffer > 0` and PANICS on a zero capacity, so a `buffer_size: 0` config
        // would panic the pipeline at startup. Clamp to >=1 (same zero-config class
        // as the output flush interval and the PQ/DLQ modulo clamps).
        let cap = buffer_config.max_events.max(1);
        // Input → Filter channel
        let (input_tx, input_rx) = mpsc::channel::<Event>(cap);
        // Filter → Output channel
        let (output_tx, output_rx) = mpsc::channel::<Event>(cap);

        // Spawn input tasks
        // When a persistent queue is enabled, inputs write to PQ first.
        // A separate drainer task reads from PQ into the filter channel.
        let mut input_handles = Vec::new();
        let pq_for_inputs = pq.clone();
        for mut input in self.inputs {
            let tx = input_tx.clone();
            let sig = shutdown.clone();
            let m = Arc::clone(&metrics);
            let pq_ref = pq_for_inputs.clone();
            let handle = tokio::spawn(async move {
                info!(input = input.name(), "starting input plugin");
                if let Some(ref pq_handle) = pq_ref {
                    // PQ-enabled: write to a temporary channel, then persist
                    let (pq_tx, mut pq_rx) = mpsc::channel::<Event>(1024);
                    let pq_writer = pq_handle.clone();
                    let writer_handle = tokio::spawn(async move {
                        while let Some(event) = pq_rx.recv().await {
                            if let Err(e) = pq_writer.push(&event) {
                                warn!(error = %e, "failed to persist event to PQ");
                                // Fall through: send directly to filter channel
                                let _ = tx.send(event).await;
                            }
                        }
                    });
                    if let Err(e) = input.run(pq_tx, sig.clone()).await {
                        if !sig.is_shutdown() {
                            error!(input = input.name(), error = %e, "input plugin error");
                        }
                    }
                    let _ = writer_handle.await;
                } else if let Err(e) = input.run(tx, sig.clone()).await {
                    if !sig.is_shutdown() {
                        error!(input = input.name(), error = %e, "input plugin error");
                    }
                }
                info!(input = input.name(), "input plugin stopped");
                drop(m);
            });
            input_handles.push(handle);
        }

        // PQ drainer: reads persisted events and sends them to the filter channel.
        //
        // The drainer owns a CLONE of `input_tx`. In auto-exit mode the pipeline
        // signals completion by dropping the ORIGINAL `input_tx` once finite inputs
        // finish; but the drainer's clone would otherwise keep the filter channel
        // open forever while it loops over an empty PQ, so the filter workers would
        // block in `recv()` and the pipeline would never terminate. To avoid that
        // hang we hand the drainer an `inputs_done` flag: once it is set AND the PQ
        // is empty, the drainer drains-then-exits, dropping its `input_tx` clone so
        // the filter workers see channel-closed. In non-auto-exit (long-running)
        // mode the flag is never set, preserving the previous shutdown-only behavior.
        let inputs_done = Arc::new(AtomicBool::new(false));
        let mut drainer_handle = None;
        if let Some(ref pq_handle) = pq {
            let pq_drain = pq_handle.clone();
            let tx = input_tx.clone();
            let sig = shutdown.clone();
            let done_flag = Arc::clone(&inputs_done);
            let tracker = ack_tracker.clone();
            drainer_handle = Some(tokio::spawn(async move {
                run_pq_drainer(pq_drain, tx, sig, done_flag, tracker).await;
            }));
        }

        // Keep input_tx alive — don't close the channel yet.
        // Logstash-compatible: pipeline stays alive until explicit shutdown,
        // even after all inputs finish (e.g., stdin EOF).

        // Spawn filter workers (multiple, based on `workers` config)
        let filters: Arc<Vec<Box<dyn FilterPlugin>>> = Arc::new(self.filters);
        let num_workers = self.config.workers.max(1);
        let input_rx = Arc::new(tokio::sync::Mutex::new(input_rx));
        let mut filter_handles = Vec::new();
        for worker_id in 0..num_workers {
            let rx = Arc::clone(&input_rx);
            let tx = output_tx.clone();
            let f = Arc::clone(&filters);
            let ctx = WorkerCtx {
                metrics: Arc::clone(&metrics),
                instance_metrics: instance_metrics.clone(),
                dlq: dlq.clone(),
                ack_tracker: ack_tracker.clone(),
            };
            let handle = tokio::spawn(async move {
                run_filter_worker(worker_id, rx, tx, f, ctx).await;
            });
            filter_handles.push(handle);
        }
        // Drop original output_tx so outputs know when all filter workers are done
        drop(output_tx);
        info!(workers = num_workers, "filter workers started");

        // Spawn output worker
        let outputs: Arc<Vec<Box<dyn OutputPlugin>>> = Arc::new(self.outputs);
        let output_config = buffer_config.clone();
        let output_pq = pq.clone();
        let output_ctx = WorkerCtx {
            metrics: Arc::clone(&metrics),
            instance_metrics: instance_metrics.clone(),
            dlq: self.dead_letter_queue.clone(),
            ack_tracker: ack_tracker.clone(),
        };
        let output_handle = tokio::spawn(async move {
            run_outputs(output_rx, outputs, output_config, output_pq, output_ctx).await;
        });

        // Wait for all inputs to finish, but DON'T close the pipeline yet.
        // Then wait for explicit shutdown signal (Logstash-compatible behavior).
        // We use an Arc<Mutex> so we can await the same set of handles after shutdown
        // if needed (ensuring input plugins — and their sockets — are fully stopped
        // before pipeline.run returns).
        let input_handles_shared = std::sync::Arc::new(tokio::sync::Mutex::new(input_handles));
        let handles_for_select = std::sync::Arc::clone(&input_handles_shared);

        tokio::select! {
            () = shutdown.wait() => {
                info!(pipeline = %pipeline_id, "shutdown signal received");
            }
            () = async move {
                let mut guard = handles_for_select.lock().await;
                while let Some(handle) = guard.pop() {
                    let _ = handle.await;
                }
            } => {
                if auto_exit {
                    info!(pipeline = %pipeline_id, "all inputs completed, draining pipeline");
                } else {
                    info!(pipeline = %pipeline_id, "all inputs completed, waiting for shutdown signal");
                    // Inputs are done but pipeline stays alive (API, TCP listeners, etc.)
                    shutdown.wait().await;
                    info!(pipeline = %pipeline_id, "shutdown signal received");
                }
            }
        }

        // Ensure all input plugins have exited (TCP listener sockets dropped, etc.).
        // On shutdown_wait branch, inputs may still be running — await them here.
        {
            let mut guard = input_handles_shared.lock().await;
            while let Some(handle) = guard.pop() {
                let _ = handle.await;
            }
        }

        // Inputs (and any PQ writer) are done. Signal the drainer so that, once
        // the PQ is empty, it drains-then-exits and drops its `input_tx` clone.
        // Without this, the drainer's clone keeps the filter channel open forever
        // in auto-exit mode and the pipeline never terminates. We reach this point
        // either after a shutdown signal (drainer already stopping) or after finite
        // inputs completed (auto-exit), so flagging inputs-done here is always safe.
        inputs_done.store(true, Ordering::SeqCst);

        // Now close the input channel to drain filters/outputs.
        drop(input_tx);

        // Await the PQ drainer so its `input_tx` clone is dropped before we wait on
        // the filter workers — otherwise the workers would block on a channel the
        // drainer still holds open.
        if let Some(handle) = drainer_handle {
            let _ = handle.await;
        }

        // Wait for filter and output workers to drain
        for handle in filter_handles {
            let _ = handle.await;
        }
        let _ = output_handle.await;

        let snap = metrics.snapshot();
        info!(
            pipeline = %pipeline_id,
            events_in = snap.events_in,
            events_out = snap.events_out,
            events_filtered = snap.events_filtered,
            uptime_secs = snap.uptime_secs,
            "pipeline stopped"
        );

        Ok(())
    }
}

/// In-flight accounting for at-least-once persistent-queue delivery.
///
/// This is the pipeline-engine half of the guarantee; the storage half is
/// [`PersistentQueue`](crate::persistent_queue::PersistentQueue)'s split
/// `read_seq` (pop cursor) / `ack_seq` (durable cursor). The tracker decides WHEN
/// it is safe to advance the durable cursor: only after every event derived from
/// a popped queue entry has reached a terminal state downstream.
///
/// Lifecycle of one popped entry with sequence `seq`:
/// 1. The drainer pops `(seq, event)`, stamps `event`, and calls
///    [`register`](Self::register) — outstanding count 1 (the single event
///    entering the filter stage).
/// 2. A filter worker runs the chain, yielding `k` derived events (0 = dropped,
///    1 = transformed, N = cloned/split), and calls
///    [`set_fanout`](Self::set_fanout)`(seq, k)`. `k == 0` completes the entry at
///    once (every derivation was dropped — still "handled", so it is acked and
///    does not replay).
/// 3. The output path calls [`complete`](Self::complete)`(seq)` once per derived
///    event that reaches a terminal state (delivered, or written to the DLQ).
///    The entry is done when its count returns to 0.
///
/// [`ack_point`](Self::ack_point) returns the exclusive high-water mark for
/// `PersistentQueue::ack`: the smallest still-outstanding sequence (everything
/// below it is durably done) or `highest_registered + 1` when nothing is in
/// flight. Because it is the *minimum* outstanding sequence, an interleaved
/// fan-out/complete can never advance the ack past an entry still in flight, so
/// acking is always a safe lower bound (at-least-once, never at-most-once).
struct AckState {
    /// `seq` -> number of derived events not yet terminal.
    outstanding: BTreeMap<u64, u64>,
    /// The smallest sequence that must NEVER be acknowledged in this run: an event
    /// derived from it was neither delivered nor durably captured (output failure
    /// with no/failed DLQ, filter error with no/failed DLQ). Pinning the ack cursor
    /// at or below it makes it (and everything after) replay on restart — the
    /// pipeline has no in-run retry, so a genuinely-undelivered entry stays
    /// un-acked until a restart re-reads it. Only the minimum is load-bearing
    /// (it caps `ack_point`), so a single `min`-folded value suffices; never
    /// cleared within a run.
    min_blocked: Option<u64>,
    /// Highest sequence ever registered, so the ack can advance past the final
    /// entry once nothing is outstanding.
    highest_registered: Option<u64>,
}

struct AckTracker {
    state: Mutex<AckState>,
}

impl AckTracker {
    fn new() -> Self {
        Self {
            state: Mutex::new(AckState {
                outstanding: BTreeMap::new(),
                min_blocked: None,
                highest_registered: None,
            }),
        }
    }

    /// Lock the state, recovering a poisoned mutex (panic in another holder)
    /// instead of propagating: losing some ack precision (at worst a few
    /// duplicate replays) is far preferable to wedging delivery.
    fn lock(&self) -> std::sync::MutexGuard<'_, AckState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record a freshly-popped entry as one in-flight event.
    fn register(&self, seq: u64) {
        let mut s = self.lock();
        s.outstanding.insert(seq, 1);
        s.highest_registered = Some(s.highest_registered.map_or(seq, |h| h.max(seq)));
    }

    /// Set how many derived events remain in flight for `seq` after the filter
    /// chain. `0` completes the entry immediately.
    fn set_fanout(&self, seq: u64, derived: usize) {
        let mut s = self.lock();
        if derived == 0 {
            s.outstanding.remove(&seq);
        } else {
            s.outstanding.insert(seq, derived as u64);
        }
    }

    /// Mark one derived event of `seq` terminal (delivered or durably DLQ'd).
    /// Removes the entry once the last completes. A no-op for an unknown/already-
    /// done seq.
    fn complete(&self, seq: u64) {
        let mut s = self.lock();
        if let Some(remaining) = s.outstanding.get_mut(&seq) {
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                s.outstanding.remove(&seq);
            }
        }
    }

    /// Permanently prevent `seq` (and everything after it) from being
    /// acknowledged in this run: an event derived from it was lost (delivery
    /// failed AND it was not durably captured in the DLQ). The entry replays on
    /// the next start. See [`AckState::min_blocked`].
    fn block(&self, seq: u64) {
        let mut s = self.lock();
        s.min_blocked = Some(s.min_blocked.map_or(seq, |m| m.min(seq)));
    }

    /// The exclusive ack high-water mark, or `None` if nothing has been
    /// registered yet. Never advances past the smallest still-in-flight OR
    /// blocked sequence, so neither an undelivered nor a lost entry can be acked.
    fn ack_point(&self) -> Option<u64> {
        let s = self.lock();
        let min_outstanding = s.outstanding.keys().next().copied();
        match (min_outstanding, s.min_blocked) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            // Nothing in flight and nothing blocked: everything registered is done.
            (None, None) => s.highest_registered.map(|h| h.saturating_add(1)),
        }
    }
}

/// Cross-cutting context shared by the filter and output workers: metrics sinks,
/// the optional DLQ, and the at-least-once ack tracker.
///
/// Bundled into one value so the worker entry points stay within the
/// argument-count budget and so adding another cross-cutting concern does not
/// ripple through every call site. Cheap to `clone` (all fields are `Arc`s or
/// `Option<Arc>`s), so each spawned task gets its own owned copy.
#[derive(Clone)]
struct WorkerCtx {
    metrics: Arc<PipelineMetrics>,
    instance_metrics: Option<Arc<PipelineMetrics>>,
    dlq: Option<SharedDeadLetterQueue>,
    ack_tracker: Option<Arc<AckTracker>>,
}

/// Drains a persistent queue into the filter channel until shutdown or, in
/// auto-exit mode, until inputs are done and the PQ is fully drained.
///
/// Stop conditions, in priority order:
/// 1. Shutdown requested (`sig.is_shutdown()`) — exit immediately (after the
///    in-flight checkpoint), matching the previous behavior.
/// 2. The downstream channel is closed (`tx.send` errors) — nothing left to
///    feed; exit.
/// 3. `inputs_done` is set AND the PQ yields no more events — drain-then-exit:
///    the loop keeps popping while events remain and only exits once the queue
///    is observed empty, so PQ-buffered events are NOT dropped on the way out.
///
/// When `inputs_done` is never set (long-running, non-auto-exit pipelines), the
/// drainer keeps polling the empty PQ until shutdown, preserving prior behavior.
async fn run_pq_drainer(
    pq_drain: SharedPersistentQueue,
    tx: mpsc::Sender<Event>,
    sig: ShutdownSignal,
    inputs_done: Arc<AtomicBool>,
    ack_tracker: Option<Arc<AckTracker>>,
) {
    // Reclaim fully-consumed segments on a small cadence rather than after
    // every single pop: `gc` re-lists/re-reads segment files, so running it
    // each event would be wasteful. Running it every `GC_EVERY` successful
    // pops, plus once on each empty-poll branch, keeps consumed segment files
    // (and their bytes) from accumulating. Without this, `total_bytes` is
    // monotonic and `enqueue` eventually wedges at `max_bytes` even though
    // events are being consumed, silently defeating the crash-durability
    // guarantee (the producer fallback would route everything to the in-memory
    // channel). It also bounds the per-`pop` re-scan cost (consumed segments
    // are deleted instead of re-read/re-decompressed every call).
    const GC_EVERY: u32 = 256;
    let mut pops_since_gc: u32 = 0;
    // `gc` only removes segments whose every entry is already durably acked
    // (`seq < ack_seq`), so it never drops un-delivered events; failures are
    // non-fatal (next cadence retries), so we only warn. Reclamation now follows
    // the output path's acks (see `AckTracker`); this cadence just bounds how
    // often the drainer re-scans for newly-acked segments to reclaim.
    let run_gc = |pq: &SharedPersistentQueue| {
        if let Err(e) = pq.gc() {
            warn!(error = %e, "PQ gc error");
        }
    };

    loop {
        if sig.is_shutdown() {
            break;
        }
        match pq_drain.pop_with_seq() {
            Ok(Some((seq, mut event))) => {
                // Stamp the originating queue sequence and register it as
                // in-flight BEFORE handing the event downstream, so the entry is
                // tracked the instant it can be observed by a filter worker.
                if let Some(ref tracker) = ack_tracker {
                    event.set_pq_seq(seq);
                    tracker.register(seq);
                }
                if tx.send(event).await.is_err() {
                    break;
                }
                pops_since_gc += 1;
                if pops_since_gc >= GC_EVERY {
                    run_gc(&pq_drain);
                    pops_since_gc = 0;
                }
            }
            Ok(None) => {
                // PQ is currently empty. Reclaim any segments fully consumed by
                // the pops above before sleeping or exiting.
                if pops_since_gc > 0 {
                    run_gc(&pq_drain);
                    pops_since_gc = 0;
                }
                // If inputs are done, no more events can ever arrive, so
                // drain-then-exit (we already popped until empty above, so it
                // is safe to leave now without dropping events).
                if inputs_done.load(Ordering::SeqCst) {
                    break;
                }
                // Otherwise wait briefly for new persisted events.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(e) => {
                warn!(error = %e, "PQ drain error");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    // Final reclamation + checkpoint on exit so segments acked right before
    // shutdown are released and the durable ack cursor is flushed. The output
    // path owns advancing `ack_seq` (see `run_outputs`); this only persists
    // whatever it has acked so far. Entries popped but not yet acked are left in
    // place on purpose — they replay on the next start (at-least-once).
    run_gc(&pq_drain);
    if let Err(e) = pq_drain.checkpoint() {
        warn!(error = %e, "PQ checkpoint error on shutdown");
    }
}

async fn run_filter_worker(
    _worker_id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Event>>>,
    tx: mpsc::Sender<Event>,
    filters: Arc<Vec<Box<dyn FilterPlugin>>>,
    ctx: WorkerCtx,
) {
    let WorkerCtx {
        metrics,
        instance_metrics,
        dlq,
        ack_tracker,
    } = ctx;
    // Batch size for filter processing — amortises Mutex lock and Vec
    // allocation overhead across multiple events, matching Logstash's
    // pipeline.batch.size approach.
    const FILTER_BATCH: usize = 256;
    let mut batch = Vec::with_capacity(FILTER_BATCH);

    loop {
        batch.clear();

        // Drain up to FILTER_BATCH events under a single lock acquisition.
        {
            let mut guard = rx.lock().await;
            for _ in 0..FILTER_BATCH {
                match guard.try_recv() {
                    Ok(event) => batch.push(event),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        // Channel closed — process remaining batch then exit
                        break;
                    }
                }
            }
            // If batch is empty, do a blocking recv to avoid busy-spin
            if batch.is_empty() {
                match guard.recv().await {
                    Some(event) => batch.push(event),
                    None => break, // channel fully closed
                }
            }
        }

        let batch_len = batch.len() as u64;
        let batch_bytes: u64 = batch
            .iter()
            .map(|e| e.message().map_or(0, str::len) as u64)
            .sum();
        metrics.record_in(batch_len, batch_bytes);
        metrics.record_filtered(batch_len);
        if let Some(im) = instance_metrics.as_ref() {
            im.record_in(batch_len, batch_bytes);
            im.record_filtered(batch_len);
        }

        // Process each event through the filter chain
        for event in batch.drain(..) {
            // Capture the originating PQ sequence (if any) before the chain may
            // clone/split/drop the event into derivations that don't carry it.
            let pq_seq = event.pq_seq();
            let mut events = vec![event];
            // Set when a derivation of this entry was lost during filtering (a
            // filter errored and the event was NOT durably captured in the DLQ).
            // Such an entry must never be acked — it replays on restart.
            let mut lost_uncaptured = false;

            for filter in filters.iter() {
                let mut next_events = Vec::with_capacity(events.len());
                for ev in events {
                    if ev.is_cancelled() {
                        continue;
                    }
                    if let Some(cond) = filter.condition() {
                        if !cond.evaluate(&ev) {
                            next_events.push(ev);
                            continue;
                        }
                    }
                    // `filter.filter(ev)` consumes the event; keep a copy (only
                    // when a DLQ exists) so a filter error can capture the real
                    // payload rather than a marker.
                    let ev_backup = dlq.as_ref().map(|_| ev.clone());
                    match filter.filter(ev).await {
                        Ok(filtered) => {
                            for fe in filtered {
                                if !fe.is_cancelled() {
                                    next_events.push(fe);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(filter = filter.name(), error = %e, "filter error");
                            metrics.record_failed(1);
                            if let Some(im) = instance_metrics.as_ref() {
                                im.record_failed(1);
                            }
                            // Capture the failed event (real payload) in the DLQ.
                            // It is terminal only if DURABLY captured (`Ok(true)`);
                            // a full DLQ (`Ok(false)`), a write error, or no DLQ
                            // leaves the originating PQ entry un-acked for replay.
                            let captured = match (dlq.as_ref(), ev_backup) {
                                (Some(dlq_handle), Some(failed_ev)) => match dlq_handle.write(
                                    "filter",
                                    filter.name(),
                                    &e.to_string(),
                                    failed_ev.to_json(),
                                ) {
                                    Ok(true) => true,
                                    Ok(false) => false,
                                    Err(dlq_err) => {
                                        warn!(error = %dlq_err, "failed to write to DLQ");
                                        false
                                    }
                                },
                                _ => false,
                            };
                            if !captured {
                                lost_uncaptured = true;
                            }
                        }
                    }
                }
                events = next_events;
            }

            // At-least-once accounting: the popped queue entry `pq_seq` fans out
            // to `events.len()` derived events here. Record that count (0
            // completes the entry immediately — every derivation was dropped or
            // durably DLQ'd during filtering) and re-stamp each surviving event
            // with the originating seq so the output path can acknowledge the
            // entry once all derivations are delivered/DLQ'd. Re-stamping is
            // required (not mere propagation) because clone/split produce fresh
            // events with `pq_seq == None`. If any derivation was lost without
            // durable capture, block the entry so it is never acked (replays).
            if let (Some(tracker), Some(seq)) = (ack_tracker.as_ref(), pq_seq) {
                if lost_uncaptured {
                    tracker.block(seq);
                }
                tracker.set_fanout(seq, events.len());
                for ev in &mut events {
                    ev.set_pq_seq(seq);
                }
            }

            for ev in events {
                if tx.send(ev).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Clamps a flush interval to a non-zero minimum.
///
/// `tokio::time::interval` panics if constructed with a period of zero, and the
/// output flush interval is derived from the unvalidated `batch_delay_ms`
/// config value. A `batch_delay_ms: 0` (config typo or `--batch-delay 0`) would
/// otherwise panic the spawned output task and silently stop all delivery.
/// The 1ms floor avoids the panic while preserving fast-flush intent without
/// imposing extra batching latency on legitimate small values.
fn clamp_flush_interval(interval: std::time::Duration) -> std::time::Duration {
    interval.max(std::time::Duration::from_millis(1))
}

/// Advance the persistent queue's durable ack cursor to the tracker's current
/// high-water mark, checkpointing + reclaiming only when it actually moved.
///
/// This is where popped queue entries become durably acknowledged: the tracker
/// reports the smallest still-in-flight sequence (everything below it delivered),
/// and the PQ checkpoints that as `ack_seq` and gcs below it. No-op unless both a
/// PQ and a tracker are present (memory-queue pipelines have nothing to ack). The
/// `> last_acked` guard skips redundant checkpoint+gc IO when nothing advanced.
fn advance_durable_ack(
    pq: Option<&SharedPersistentQueue>,
    ack_tracker: Option<&Arc<AckTracker>>,
    last_acked: &mut u64,
) {
    if let (Some(pq), Some(tracker)) = (pq, ack_tracker) {
        if let Some(point) = tracker.ack_point() {
            if point > *last_acked {
                if let Err(e) = pq.ack(point) {
                    warn!(error = %e, "PQ ack error");
                } else {
                    *last_acked = point;
                }
            }
        }
    }
}

/// Flush every output to make completed-but-buffered deliveries durable, then
/// advance the durable PQ ack — but only when the ack point actually moved (skips
/// the flush + checkpoint + gc otherwise) and only if every flush succeeded.
///
/// Flushing first is what closes the durability gap for outputs that buffer
/// internally and upload on flush/rotation (only **s3**): `output()` returning
/// `Ok` merely buffers the event, so without this the periodic ack could mark an
/// entry durable while it still sits in the output's RAM buffer, and a crash
/// would lose it. A flush failure leaves the entries un-acked so they replay
/// rather than being lost. For the common synchronous outputs `flush()` is a
/// cheap no-op, so the cost falls on the buffering output that actually needs it.
async fn flush_and_ack(
    outputs: &[Box<dyn OutputPlugin>],
    pq: Option<&SharedPersistentQueue>,
    ack_tracker: Option<&Arc<AckTracker>>,
    last_acked: &mut u64,
) {
    let (Some(pq), Some(tracker)) = (pq, ack_tracker) else {
        return;
    };
    let Some(point) = tracker.ack_point() else {
        return;
    };
    if point <= *last_acked {
        return;
    }
    // Make every output's accepted-but-buffered events durable before acking.
    for output in outputs {
        if let Err(e) = output.flush().await {
            warn!(output = output.name(), error = %e, "output flush failed; deferring PQ ack");
            return;
        }
    }
    match pq.ack(point) {
        Ok(()) => *last_acked = point,
        Err(e) => warn!(error = %e, "PQ ack error"),
    }
}

async fn run_outputs(
    mut rx: mpsc::Receiver<Event>,
    outputs: Arc<Vec<Box<dyn OutputPlugin>>>,
    config: BufferConfig,
    pq: Option<SharedPersistentQueue>,
    ctx: WorkerCtx,
) {
    let mut collector = BatchCollector::new(config);
    // `flush_interval` is derived from the unvalidated `batch_delay_ms` config
    // (`Duration::from_millis(batch_delay_ms)`). `tokio::time::interval` PANICS
    // if given a zero period, which would silently kill this spawned output task
    // and halt all delivery. Clamp to a 1ms floor: enough to avoid the panic
    // while preserving fast-flush intent for legitimate small values.
    let flush_interval = clamp_flush_interval(collector.flush_interval());
    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Highest durable ack point already persisted, so the periodic ack skips the
    // checkpoint+gc when the in-flight set hasn't advanced.
    let mut last_acked: u64 = 0;

    loop {
        tokio::select! {
            event = rx.recv() => {
                if let Some(ev) = event {
                    if let Some(batch) = collector.add(ev) {
                        send_batch(&outputs, batch, &ctx).await;
                    }
                } else {
                    // Channel closed: flush remaining, then break. The FINAL ack
                    // is deferred until AFTER outputs are flushed/closed below, so
                    // a clean shutdown only acks events the outputs have durably
                    // taken (important for buffering outputs like s3 that upload on
                    // flush/rotation rather than per `output()` call).
                    if !collector.is_empty() {
                        let batch = collector.flush();
                        send_batch(&outputs, batch, &ctx).await;
                    }
                    break;
                }
            }
            _ = flush_timer.tick() => {
                if !collector.is_empty() {
                    let batch = collector.flush();
                    send_batch(&outputs, batch, &ctx).await;
                }
                // Periodically flush outputs to durability, then advance the
                // durable ack cursor for everything delivered so far. Flushing
                // first ensures a buffering output (s3) has uploaded the events
                // before their entries are acked, bounding the crash-replay window
                // to ~one flush interval of in-flight events. Also covers the
                // all-dropped case (events completed in the filter stage never
                // reach this task, so only the timer advances their ack).
                flush_and_ack(&outputs, pq.as_ref(), ctx.ack_tracker.as_ref(), &mut last_acked).await;
            }
        }
    }

    // Close all outputs (flush buffering outputs to their durable store first).
    // Track whether every flush/close succeeded: if a buffering output's final
    // upload fails, its delivered-but-unflushed events are NOT durable, so the
    // final ack below must be skipped (those entries replay rather than being
    // acked-and-lost).
    let mut all_durable = true;
    for output in outputs.iter() {
        if let Err(e) = output.flush().await {
            warn!(output = output.name(), error = %e, "output flush error");
            all_durable = false;
        }
        if let Err(e) = output.close().await {
            warn!(output = output.name(), error = %e, "output close error");
            all_durable = false;
        }
    }

    // Final durable ack AFTER outputs are flushed/closed, and ONLY if every flush
    // and close succeeded. On a clean shutdown with healthy outputs every
    // delivered event is now durable, so acking here strands nothing; if any flush
    // failed, the not-yet-acked entries are left to replay (at-least-once) instead
    // of being acked while their events sit lost in a failed output's buffer.
    if all_durable {
        advance_durable_ack(pq.as_ref(), ctx.ack_tracker.as_ref(), &mut last_acked);
    }
}

async fn send_batch(outputs: &[Box<dyn OutputPlugin>], batch: Vec<Event>, ctx: &WorkerCtx) {
    let metrics = &ctx.metrics;
    let instance_metrics = ctx.instance_metrics.as_ref();
    let dlq = ctx.dlq.as_ref();
    let ack_tracker = ctx.ack_tracker.as_ref();

    // Per-event terminal tracking (index-aligned with `batch`). An event is
    // "lost" once some matching output FAILS to deliver it AND it is not durably
    // captured in the DLQ. A lost event's PQ entry is left un-acked → it replays
    // on restart (at-least-once). Events matching no output, or delivered to
    // every matching output, stay non-lost and are acknowledged.
    let mut lost = vec![false; batch.len()];

    // Attempt EVERY output. Do NOT abort the batch after the first failing output:
    // a healthy output must still receive the event even when another is down, and
    // aborting early would acknowledge entries that the skipped outputs never got
    // (data loss across a restart, since the entry would not replay).
    for output in outputs {
        let matching: Vec<usize> = batch
            .iter()
            .enumerate()
            .filter(|(_, ev)| output.condition().map_or(true, |cond| cond.evaluate(ev)))
            .map(|(i, _)| i)
            .collect();
        if matching.is_empty() {
            continue;
        }

        let filtered: Vec<Event> = matching.iter().map(|&i| batch[i].clone()).collect();
        let n = matching.len() as u64;
        // Message-length proxy for bytes (full JSON only matters to the plugin).
        let out_bytes: u64 = filtered
            .iter()
            .map(|e| e.message().map_or(64, str::len) as u64)
            .sum();

        if let Err(e) = output.output(filtered).await {
            error!(output = output.name(), error = %e, "output error");
            metrics.record_failed(n);
            if let Some(im) = instance_metrics {
                im.record_failed(n);
            }
            // Capture each failed event in the DLQ. It is terminal only if DURABLY
            // captured (`Ok(true)`); a full DLQ (`Ok(false)`), a write error, or no
            // DLQ marks it lost so its PQ entry replays instead of being acked.
            for &i in &matching {
                let captured = match dlq {
                    Some(dlq_handle) => match dlq_handle.write(
                        "output",
                        output.name(),
                        &e.to_string(),
                        batch[i].to_json(),
                    ) {
                        Ok(true) => true,
                        Ok(false) => false,
                        Err(dlq_err) => {
                            warn!(error = %dlq_err, "failed to write to DLQ");
                            false
                        }
                    },
                    None => false,
                };
                if !captured {
                    lost[i] = true;
                }
            }
        } else {
            metrics.record_out(n, out_bytes);
            if let Some(im) = instance_metrics {
                im.record_out(n, out_bytes);
            }
        }
    }

    // Acknowledge every entry whose event reached a terminal state on ALL of its
    // matching outputs (delivered, or durably DLQ'd on failure). Lost events are
    // skipped — their entries stay un-acked and replay on the next start.
    if let Some(tracker) = ack_tracker {
        for (i, ev) in batch.iter().enumerate() {
            if !lost[i] {
                if let Some(seq) = ev.pq_seq() {
                    tracker.complete(seq);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown::ShutdownController;
    use std::sync::atomic::AtomicUsize;

    /// A finite input that emits `count` events then returns (completes).
    #[derive(Debug)]
    struct FiniteInput {
        count: usize,
    }

    #[async_trait::async_trait]
    impl InputPlugin for FiniteInput {
        fn name(&self) -> &str {
            "finite"
        }
        async fn run(
            &mut self,
            sender: mpsc::Sender<Event>,
            _shutdown: ShutdownSignal,
        ) -> Result<()> {
            for i in 0..self.count {
                if sender.send(Event::new(format!("ev-{i}"))).await.is_err() {
                    break;
                }
            }
            Ok(())
        }
    }

    /// An output that counts every event it receives.
    #[derive(Debug)]
    struct CountingOutput {
        seen: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl OutputPlugin for CountingOutput {
        fn name(&self) -> &str {
            "counting"
        }
        async fn output(&self, events: Vec<Event>) -> Result<()> {
            self.seen.fetch_add(events.len(), Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(config.id, "main");
        assert!(config.workers > 0);
    }

    #[tokio::test]
    async fn test_empty_pipeline_runs_and_stops() {
        let config = PipelineConfig::default();
        let pipeline = Pipeline::new(config);
        let (controller, signal) = ShutdownController::new();

        // Shutdown immediately since there are no inputs
        controller.shutdown();
        let result = pipeline.run(signal).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_pipeline_metrics() {
        let config = PipelineConfig::default();
        let pipeline = Pipeline::new(config);
        let metrics = pipeline.metrics();
        let snap = metrics.snapshot();
        assert_eq!(snap.events_in, 0);
        assert_eq!(snap.events_out, 0);
    }

    #[test]
    fn test_clamp_flush_interval_floors_zero() {
        // A `batch_delay_ms: 0` config yields `Duration::ZERO`, which would
        // panic `tokio::time::interval`. The clamp must floor it to >=1ms.
        let clamped = clamp_flush_interval(std::time::Duration::ZERO);
        assert!(clamped >= std::time::Duration::from_millis(1));
        assert_eq!(clamped, std::time::Duration::from_millis(1));
    }

    #[test]
    fn test_clamp_flush_interval_preserves_nonzero() {
        // Legitimate non-zero values pass through unchanged (no large floor
        // imposed that would alter batching latency).
        for ms in [1u64, 2, 5, 100, 5000] {
            let d = std::time::Duration::from_millis(ms);
            assert_eq!(clamp_flush_interval(d), d);
        }
    }

    #[tokio::test]
    async fn test_zero_flush_interval_does_not_panic_interval() {
        // End-to-end: constructing the output flush timer from a clamped
        // zero-period flush interval must not panic.
        let clamped = clamp_flush_interval(std::time::Duration::ZERO);
        let _timer = tokio::time::interval(clamped);
    }

    #[test]
    fn test_pipeline_config_custom() {
        let config = PipelineConfig {
            workers: 4,
            id: "custom".to_string(),
            buffer: crate::buffer::BufferConfig {
                max_events: 500,
                batch_size: 50,
                flush_interval: std::time::Duration::from_secs(1),
            },
            auto_exit_on_inputs_done: false,
            queue: QueueType::Memory,
            dead_letter_queue: DlqSettings::default(),
        };
        assert_eq!(config.workers, 4);
        assert_eq!(config.id, "custom");
        assert_eq!(config.buffer.max_events, 500);
    }

    #[test]
    fn test_pipeline_config_with_persisted_queue() {
        let config = PipelineConfig {
            queue: QueueType::Persisted(PqConfig {
                path: "/tmp/test-pq".to_string(),
                ..PqConfig::default()
            }),
            ..PipelineConfig::default()
        };
        assert!(matches!(config.queue, QueueType::Persisted(_)));
    }

    #[test]
    fn test_pipeline_config_with_dlq() {
        let config = PipelineConfig {
            dead_letter_queue: DlqSettings {
                enable: true,
                config: Some(DlqConfig {
                    path: "/tmp/test-dlq".to_string(),
                    ..DlqConfig::default()
                }),
            },
            ..PipelineConfig::default()
        };
        assert!(config.dead_letter_queue.enable);
    }

    // ---- Pipeline integration tests for PQ and DLQ ----

    #[test]
    fn test_pipeline_with_pq() {
        let dir = tempfile::tempdir().expect("tempdir for pipeline_with_pq");
        let pq_path = dir.path().to_string_lossy().to_string();

        let config = PipelineConfig {
            queue: QueueType::Persisted(PqConfig {
                path: pq_path.clone(),
                ..PqConfig::default()
            }),
            ..PipelineConfig::default()
        };
        let pipeline = Pipeline::new(config);

        // Verify PQ was created
        let pq = pipeline
            .persistent_queue()
            .expect("pipeline should have a persistent queue");
        assert!(pq.is_empty(), "PQ should start empty");

        // Push events through the PQ
        for i in 0..20 {
            pq.push(&Event::new(format!("pipeline-event-{i}")))
                .expect("push to pipeline PQ");
        }
        assert_eq!(pq.len(), 20, "PQ should have 20 events");

        // Pop and verify order
        for i in 0..20 {
            let ev = pq
                .pop()
                .expect("pop from pipeline PQ")
                .expect("event should exist");
            assert_eq!(
                ev.message(),
                Some(format!("pipeline-event-{i}").as_str()),
                "pipeline PQ event {i} mismatch"
            );
        }
        assert!(pq.is_empty(), "PQ should be empty after popping all");
        pq.close().expect("close pipeline PQ");
    }

    #[test]
    fn test_pipeline_dlq_on_filter_error() {
        let dir = tempfile::tempdir().expect("tempdir for pipeline_dlq_on_filter_error");
        let dlq_path = dir.path().join("dlq").to_string_lossy().to_string();

        let config = PipelineConfig {
            dead_letter_queue: DlqSettings {
                enable: true,
                config: Some(DlqConfig {
                    path: dlq_path.clone(),
                    ..DlqConfig::default()
                }),
            },
            ..PipelineConfig::default()
        };
        let pipeline = Pipeline::new(config);

        // Verify DLQ was created
        let dlq = pipeline
            .dead_letter_queue()
            .expect("pipeline should have a DLQ");
        assert!(dlq.is_empty(), "DLQ should start empty");

        // Simulate writing failed events to the DLQ (as the pipeline engine would)
        for i in 0..5 {
            dlq.write(
                "filter",
                "grok",
                &format!("pattern mismatch on event {i}"),
                serde_json::json!({"message": format!("bad-event-{i}")}),
            )
            .expect("write failed event to DLQ");
        }

        assert_eq!(dlq.len(), 5, "DLQ should have 5 failed events");
        dlq.close().expect("close pipeline DLQ");
    }

    #[test]
    fn test_pipeline_pq_recovery() {
        let dir = tempfile::tempdir().expect("tempdir for pipeline_pq_recovery");
        let pq_path = dir.path().to_string_lossy().to_string();

        // First pipeline instance: push events, then close
        {
            let config = PipelineConfig {
                queue: QueueType::Persisted(PqConfig {
                    path: pq_path.clone(),
                    checkpoint_interval: 1,
                    ..PqConfig::default()
                }),
                ..PipelineConfig::default()
            };
            let pipeline = Pipeline::new(config);
            let pq = pipeline
                .persistent_queue()
                .expect("pipeline should have PQ");

            for i in 0..15 {
                pq.push(&Event::new(format!("recover-{i}")))
                    .expect("push for recovery test");
            }
            pq.checkpoint().expect("checkpoint before stop");
            pq.close().expect("close first pipeline");
        }

        // Second pipeline instance: reopen and verify events survived
        {
            let config = PipelineConfig {
                queue: QueueType::Persisted(PqConfig {
                    path: pq_path.clone(),
                    checkpoint_interval: 1,
                    ..PqConfig::default()
                }),
                ..PipelineConfig::default()
            };
            let pipeline = Pipeline::new(config);
            let pq = pipeline
                .persistent_queue()
                .expect("reopened pipeline should have PQ");

            // All 15 events should be recoverable
            let mut recovered = 0;
            while let Some(_ev) = pq.pop().expect("pop during recovery") {
                recovered += 1;
            }
            assert_eq!(
                recovered, 15,
                "all 15 events should survive pipeline restart"
            );
            pq.close().expect("close recovered pipeline");
        }
    }

    #[tokio::test]
    async fn test_pipeline_zero_buffer_size_does_not_panic() {
        // A `pipeline.buffer_size: 0` config yields `max_events: 0`. The channel
        // construction in `run` would panic (`mpsc::channel` asserts buffer > 0)
        // without the `.max(1)` clamp. With an immediate shutdown and no inputs,
        // the pipeline must start, clamp the capacity, and stop cleanly.
        let config = PipelineConfig {
            buffer: crate::buffer::BufferConfig {
                max_events: 0,
                ..crate::buffer::BufferConfig::default()
            },
            ..PipelineConfig::default()
        };
        let pipeline = Pipeline::new(config);
        let (controller, signal) = ShutdownController::new();
        controller.shutdown();
        let result = pipeline.run(signal).await;
        assert!(
            result.is_ok(),
            "zero buffer_size must not panic the pipeline"
        );
    }

    #[tokio::test]
    async fn test_pq_drainer_stops_on_inputs_done_after_draining() {
        // Isolated drainer lifecycle test: the drainer must drain ALL persisted
        // events first, then exit once `inputs_done` is set and the PQ is empty
        // (drain-then-exit, not exit-immediately, so no events are dropped).
        let dir = tempfile::tempdir().expect("tempdir for drainer test");
        let pq_path = dir.path().to_string_lossy().to_string();
        let pq = SharedPersistentQueue::new(pq_path, 1_073_741_824).expect("open PQ");

        // Pre-load 25 events.
        for i in 0..25 {
            pq.push(&Event::new(format!("drain-{i}"))).expect("push");
        }

        let (tx, mut rx) = mpsc::channel::<Event>(64);
        let (_controller, signal) = ShutdownController::new();
        let inputs_done = Arc::new(AtomicBool::new(false));

        // Signal inputs-done up front: there is no live producer, so the drainer
        // should drain the 25 buffered events and then exit on observed-empty.
        inputs_done.store(true, Ordering::SeqCst);

        let drainer = tokio::spawn(run_pq_drainer(
            pq.clone(),
            tx,
            signal,
            Arc::clone(&inputs_done),
            None,
        ));

        // The drainer must terminate (its tx clone dropped) within the timeout.
        tokio::time::timeout(std::time::Duration::from_secs(5), drainer)
            .await
            .expect("drainer must exit once inputs are done and PQ is drained")
            .expect("drainer task should not panic");

        // All 25 events must have been forwarded (none dropped on exit).
        let mut received = 0;
        while rx.recv().await.is_some() {
            received += 1;
        }
        assert_eq!(
            received, 25,
            "drainer must forward every PQ event before exit"
        );
    }

    #[tokio::test]
    async fn test_auto_exit_pipeline_with_pq_terminates() {
        // End-to-end regression for the drainer hang: an auto-exit pipeline with a
        // persisted queue and a FINITE input must terminate within a timeout.
        // Before the fix, the drainer's `input_tx` clone kept the filter channel
        // open forever, so the filter workers blocked in `recv()` and `run` never
        // returned.
        let dir = tempfile::tempdir().expect("tempdir for auto-exit PQ test");
        let pq_path = dir.path().to_string_lossy().to_string();

        let config = PipelineConfig {
            auto_exit_on_inputs_done: true,
            workers: 2,
            queue: QueueType::Persisted(PqConfig {
                path: pq_path,
                checkpoint_interval: 1,
                ..PqConfig::default()
            }),
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);

        let seen = Arc::new(AtomicUsize::new(0));
        pipeline.add_input(Box::new(FiniteInput { count: 10 }));
        pipeline.add_output(Box::new(CountingOutput {
            seen: Arc::clone(&seen),
        }));

        let (_controller, signal) = ShutdownController::new();
        // No shutdown is triggered: termination must come solely from inputs-done
        // + drainer drain-then-exit, NOT from an external shutdown signal.
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(15), pipeline.run(signal)).await;

        let run_result = result.expect("auto-exit + PQ pipeline must terminate without shutdown");
        assert!(run_result.is_ok(), "pipeline run should succeed");
        assert_eq!(
            seen.load(Ordering::SeqCst),
            10,
            "all 10 finite-input events should flow through PQ to the output"
        );
    }

    /// An output that always fails (no successful delivery).
    #[derive(Debug)]
    struct FailingOutput;

    #[async_trait::async_trait]
    impl OutputPlugin for FailingOutput {
        fn name(&self) -> &str {
            "failing"
        }
        async fn output(&self, _events: Vec<Event>) -> Result<()> {
            Err(crate::error::FerroStashError::Pipeline(
                "simulated permanent output failure".to_string(),
            ))
        }
    }

    /// An output that ACCEPTS events in `output()` (as a buffering output like s3
    /// does) but always FAILS to make them durable in `flush()`/`close()`.
    #[derive(Debug)]
    struct FlushFailingOutput {
        seen: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl OutputPlugin for FlushFailingOutput {
        fn name(&self) -> &str {
            "flush-failing"
        }
        async fn output(&self, events: Vec<Event>) -> Result<()> {
            // Accept (buffer) the events — like s3 returning Ok before upload.
            self.seen.fetch_add(events.len(), Ordering::SeqCst);
            Ok(())
        }
        async fn flush(&self) -> Result<()> {
            Err(crate::error::FerroStashError::Pipeline(
                "simulated flush/upload failure".to_string(),
            ))
        }
        async fn close(&self) -> Result<()> {
            Err(crate::error::FerroStashError::Pipeline(
                "simulated close/upload failure".to_string(),
            ))
        }
    }

    /// A filter that always errors.
    #[derive(Debug)]
    struct FailingFilter;

    #[async_trait::async_trait]
    impl FilterPlugin for FailingFilter {
        fn name(&self) -> &str {
            "failing-filter"
        }
        async fn filter(&self, _event: Event) -> Result<Vec<Event>> {
            Err(crate::error::FerroStashError::Pipeline(
                "simulated filter failure".to_string(),
            ))
        }
    }

    /// Build a PQ-backed auto-exit pipeline, run it to completion within a
    /// timeout, then return how many events remain in the queue (i.e. were NOT
    /// acknowledged and would replay on the next start).
    async fn run_and_count_replayable(
        pq_path: &str,
        dlq: DlqSettings,
        inputs: usize,
        filters: Vec<Box<dyn FilterPlugin>>,
        outputs: Vec<Box<dyn OutputPlugin>>,
    ) -> usize {
        {
            let config = PipelineConfig {
                auto_exit_on_inputs_done: true,
                workers: 2,
                queue: QueueType::Persisted(PqConfig {
                    path: pq_path.to_string(),
                    checkpoint_interval: 1,
                    ..PqConfig::default()
                }),
                dead_letter_queue: dlq,
                ..PipelineConfig::default()
            };
            let mut pipeline = Pipeline::new(config);
            pipeline.add_input(Box::new(FiniteInput { count: inputs }));
            for f in filters {
                pipeline.add_filter(f);
            }
            for o in outputs {
                pipeline.add_output(o);
            }
            let (_controller, signal) = ShutdownController::new();
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(15), pipeline.run(signal))
                    .await;
            assert!(
                result.expect("pipeline must terminate").is_ok(),
                "pipeline run should succeed"
            );
        }
        // Reopen and drain to count what survived (un-acked => replay).
        let reopened = SharedPersistentQueue::open(PqConfig {
            path: pq_path.to_string(),
            checkpoint_interval: 1,
            ..PqConfig::default()
        })
        .expect("reopen PQ");
        let mut n = 0;
        while reopened.pop().expect("pop").is_some() {
            n += 1;
        }
        n
    }

    #[tokio::test]
    async fn test_pipeline_multi_output_partial_failure_replays() {
        // DD round-1 (Critical): with two outputs where one succeeds and one
        // fails (no DLQ), an entry must NOT be acked just because the first output
        // delivered it — the failing output never received it, so it must replay.
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();
        let seen = Arc::new(AtomicUsize::new(0));
        let replayable = run_and_count_replayable(
            &pq_path,
            DlqSettings::default(), // no DLQ
            10,
            vec![],
            vec![
                Box::new(CountingOutput {
                    seen: Arc::clone(&seen),
                }),
                Box::new(FailingOutput),
            ],
        )
        .await;
        assert_eq!(
            seen.load(Ordering::SeqCst),
            10,
            "the healthy output should still receive every event"
        );
        assert_eq!(
            replayable, 10,
            "every entry must replay because the second output never delivered it"
        );
    }

    #[tokio::test]
    async fn test_pipeline_filter_error_no_dlq_replays() {
        // DD round-1 (Critical): a filter error with NO DLQ must NOT ack the
        // entry (the event was neither delivered nor captured) — it replays.
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();
        let seen = Arc::new(AtomicUsize::new(0));
        let replayable = run_and_count_replayable(
            &pq_path,
            DlqSettings::default(),
            10,
            vec![Box::new(FailingFilter)],
            vec![Box::new(CountingOutput {
                seen: Arc::clone(&seen),
            })],
        )
        .await;
        assert_eq!(seen.load(Ordering::SeqCst), 0, "filter dropped all events");
        assert_eq!(
            replayable, 10,
            "filter-errored events with no DLQ must replay"
        );
    }

    #[tokio::test]
    async fn test_pipeline_filter_error_with_dlq_captures_payload_and_acks() {
        // DD round-1: a filter error WITH a DLQ must capture the REAL event
        // payload (not a marker) and, because it is durably captured, ack the
        // entry so it does NOT replay.
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();
        let dlq_path = dir.path().join("dlq").to_string_lossy().to_string();
        let seen = Arc::new(AtomicUsize::new(0));
        let replayable = run_and_count_replayable(
            &pq_path,
            DlqSettings {
                enable: true,
                config: Some(DlqConfig {
                    path: dlq_path.clone(),
                    ..DlqConfig::default()
                }),
            },
            10,
            vec![Box::new(FailingFilter)],
            vec![Box::new(CountingOutput {
                seen: Arc::clone(&seen),
            })],
        )
        .await;
        assert_eq!(seen.load(Ordering::SeqCst), 0, "filter dropped all events");
        assert_eq!(
            replayable, 0,
            "durably DLQ-captured filter errors must be acked (no replay)"
        );
        // The DLQ must contain the real event payloads, not a marker.
        let dlq = crate::dead_letter_queue::DeadLetterQueue::open(DlqConfig {
            path: dlq_path,
            ..DlqConfig::default()
        })
        .expect("reopen DLQ");
        let entries = dlq.read_all().expect("read DLQ");
        assert_eq!(entries.len(), 10, "all 10 filter failures captured");
        let has_real_payload = entries.iter().any(|e| {
            e.event
                .get("message")
                .and_then(|m| m.as_str())
                .is_some_and(|s| s.starts_with("ev-"))
        });
        assert!(
            has_real_payload,
            "DLQ must store the real event payload, not just a marker"
        );
    }

    #[tokio::test]
    async fn test_pipeline_buffering_output_flush_failure_replays() {
        // DD round-2 (Critical): an output that ACCEPTS events in output() but
        // FAILS to make them durable in flush()/close() must NOT have its entries
        // acked — output() returning Ok is not durability. Every event must
        // replay because no flush ever succeeded.
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();
        let seen = Arc::new(AtomicUsize::new(0));
        let replayable = run_and_count_replayable(
            &pq_path,
            DlqSettings::default(), // no DLQ
            10,
            vec![],
            vec![Box::new(FlushFailingOutput {
                seen: Arc::clone(&seen),
            })],
        )
        .await;
        assert_eq!(
            seen.load(Ordering::SeqCst),
            10,
            "the output accepted (buffered) every event"
        );
        assert_eq!(
            replayable, 10,
            "buffered events whose flush never succeeded must replay, not be acked"
        );
    }

    #[tokio::test]
    async fn test_pipeline_output_failure_full_dlq_replays() {
        // DD round-1 (Critical): a full DLQ silently dropping a failed event must
        // NOT cause the entry to be acked. With max_bytes=0 the DLQ is full from
        // the start (captures nothing), so every failed delivery must replay.
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();
        let dlq_path = dir.path().join("dlq").to_string_lossy().to_string();
        let replayable = run_and_count_replayable(
            &pq_path,
            DlqSettings {
                enable: true,
                config: Some(DlqConfig {
                    path: dlq_path,
                    max_bytes: 0, // always "full" => captures nothing
                    ..DlqConfig::default()
                }),
            },
            10,
            vec![],
            vec![Box::new(FailingOutput)],
        )
        .await;
        assert_eq!(
            replayable, 10,
            "events the full DLQ could not capture must replay, not be acked"
        );
    }

    #[tokio::test]
    async fn test_pipeline_at_least_once_failing_output_no_dlq_replays() {
        // End-to-end at-least-once: with a persistent queue and an output that
        // never succeeds (and NO DLQ), events flow PQ -> filter -> output, fail
        // delivery, and must be left UN-acknowledged. After the run, reopening the
        // queue must still hold every event so they replay on the next start.
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();

        let config = PipelineConfig {
            auto_exit_on_inputs_done: true,
            workers: 2,
            queue: QueueType::Persisted(PqConfig {
                path: pq_path.clone(),
                checkpoint_interval: 1,
                ..PqConfig::default()
            }),
            // No DLQ: failed events must not be acked (they replay).
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.add_input(Box::new(FiniteInput { count: 10 }));
        pipeline.add_output(Box::new(FailingOutput));

        let (_controller, signal) = ShutdownController::new();
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(15), pipeline.run(signal)).await;
        assert!(
            result.expect("pipeline must terminate").is_ok(),
            "pipeline run should succeed even when the output fails"
        );

        // Reopen the queue: nothing was delivered, so nothing was acked — all 10
        // events must still be present to replay.
        let reopened = SharedPersistentQueue::open(PqConfig {
            path: pq_path,
            checkpoint_interval: 1,
            ..PqConfig::default()
        })
        .expect("reopen PQ");
        let mut replayed = 0;
        while reopened.pop().expect("pop").is_some() {
            replayed += 1;
        }
        assert_eq!(
            replayed, 10,
            "all 10 undelivered events must survive for replay (at-least-once)"
        );
    }

    #[tokio::test]
    async fn test_pipeline_at_least_once_successful_output_acks_and_drains() {
        // The complement: when the output succeeds, every event is acknowledged
        // after delivery, so reopening the queue finds it drained (no needless
        // replay).
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();

        let config = PipelineConfig {
            auto_exit_on_inputs_done: true,
            workers: 2,
            queue: QueueType::Persisted(PqConfig {
                path: pq_path.clone(),
                checkpoint_interval: 1,
                ..PqConfig::default()
            }),
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        let seen = Arc::new(AtomicUsize::new(0));
        pipeline.add_input(Box::new(FiniteInput { count: 10 }));
        pipeline.add_output(Box::new(CountingOutput {
            seen: Arc::clone(&seen),
        }));

        let (_controller, signal) = ShutdownController::new();
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(15), pipeline.run(signal)).await;
        assert!(result.expect("pipeline must terminate").is_ok());
        assert_eq!(seen.load(Ordering::SeqCst), 10, "all 10 delivered");

        // Reopen: everything was delivered and acked, so the queue is drained.
        let reopened = SharedPersistentQueue::open(PqConfig {
            path: pq_path,
            checkpoint_interval: 1,
            ..PqConfig::default()
        })
        .expect("reopen PQ");
        assert!(
            reopened.pop().expect("pop").is_none(),
            "all delivered events must be acked; nothing should replay"
        );
    }

    /// An output that deterministically fails every `fail_every`-th batch (0 =
    /// never) and records the IDs of events it *does* deliver, for the soak test.
    #[derive(Debug)]
    struct FlakyOutput {
        delivered: Arc<std::sync::Mutex<std::collections::HashSet<i64>>>,
        attempts: Arc<AtomicUsize>,
        fail_every: usize,
        batch_ctr: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl OutputPlugin for FlakyOutput {
        fn name(&self) -> &str {
            "flaky"
        }
        async fn output(&self, events: Vec<Event>) -> Result<()> {
            self.attempts.fetch_add(events.len(), Ordering::SeqCst);
            let n = self.batch_ctr.fetch_add(1, Ordering::SeqCst);
            if self.fail_every > 0 && n % self.fail_every == 0 {
                return Err(crate::error::FerroStashError::Pipeline(
                    "flaky batch failure".to_string(),
                ));
            }
            let mut d = self.delivered.lock().expect("delivered lock");
            for ev in &events {
                if let Some(crate::event::EventValue::Integer(id)) = ev.get("id") {
                    d.insert(*id);
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    #[ignore = "soak: PQ at-least-once must lose nothing under repeated failure + restart"]
    async fn soak_pq_at_least_once_no_loss_under_failure_and_restart() {
        // Chaos/soak: seed N unique events into a persistent queue, then run many
        // pipeline passes against a flaky output (fails every 3rd batch, no DLQ).
        // Each failed batch leaves its entries un-acked, so they replay when the
        // next pass reopens the queue — simulating crash/restart cycles under
        // sustained failure. A final reliable pass drains the remainder. The
        // invariant: EVERY event is delivered at least once (zero loss), and the
        // failure/replay path was actually exercised (re-attempts > N).
        let dir = tempfile::tempdir().expect("tempdir");
        let pq_path = dir.path().to_string_lossy().to_string();
        const N: i64 = 2000;
        let delivered = Arc::new(std::sync::Mutex::new(
            std::collections::HashSet::<i64>::new(),
        ));
        let attempts = Arc::new(AtomicUsize::new(0));

        // Seed N unique events into the PQ.
        {
            let pq = SharedPersistentQueue::open(PqConfig {
                path: pq_path.clone(),
                checkpoint_interval: 1,
                ..Default::default()
            })
            .expect("open seed PQ");
            for i in 0..N {
                let mut ev = Event::new(format!("evt-{i}"));
                ev.set("id", crate::event::EventValue::Integer(i));
                pq.push(&ev).expect("push");
            }
        }

        // One pass: a PQ pipeline (no inputs, auto-exit) draining into `output`.
        async fn run_pass(pq_path: &str, output: Box<dyn OutputPlugin>) {
            let config = PipelineConfig {
                auto_exit_on_inputs_done: true,
                workers: 2,
                queue: QueueType::Persisted(PqConfig {
                    path: pq_path.to_string(),
                    checkpoint_interval: 1,
                    ..Default::default()
                }),
                ..PipelineConfig::default()
            };
            let mut pipeline = Pipeline::new(config);
            pipeline.add_output(output);
            let (_ctrl, sig) = ShutdownController::new();
            tokio::time::timeout(std::time::Duration::from_secs(60), pipeline.run(sig))
                .await
                .expect("soak pass must terminate")
                .expect("pass run ok");
        }

        // Flaky passes; replayed (failed) events get re-tried each pass.
        for _ in 0..20 {
            if delivered.lock().expect("lock").len() as i64 == N {
                break;
            }
            run_pass(
                &pq_path,
                Box::new(FlakyOutput {
                    delivered: Arc::clone(&delivered),
                    attempts: Arc::clone(&attempts),
                    fail_every: 3,
                    batch_ctr: AtomicUsize::new(0),
                }),
            )
            .await;
        }
        // Final reliable pass drains anything still un-acked.
        run_pass(
            &pq_path,
            Box::new(FlakyOutput {
                delivered: Arc::clone(&delivered),
                attempts: Arc::clone(&attempts),
                fail_every: 0,
                batch_ctr: AtomicUsize::new(0),
            }),
        )
        .await;

        let got = delivered.lock().expect("lock").len() as i64;
        assert_eq!(
            got, N,
            "at-least-once must lose nothing under failure+restart: delivered {got}/{N}"
        );
        assert!(
            attempts.load(Ordering::SeqCst) as i64 > N,
            "failed batches must have been re-attempted across restarts (replay path exercised)"
        );
    }
}
