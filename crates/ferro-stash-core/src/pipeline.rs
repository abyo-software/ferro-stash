// SPDX-License-Identifier: Apache-2.0
//! Pipeline engine — orchestrates input → filter → output flow.

use std::sync::Arc;
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

        // Input → Filter channel
        let (input_tx, input_rx) = mpsc::channel::<Event>(buffer_config.max_events);
        // Filter → Output channel
        let (output_tx, output_rx) = mpsc::channel::<Event>(buffer_config.max_events);

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

        // PQ drainer: reads persisted events and sends them to the filter channel
        if let Some(ref pq_handle) = pq {
            let pq_drain = pq_handle.clone();
            let tx = input_tx.clone();
            let sig = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    if sig.is_shutdown() {
                        break;
                    }
                    match pq_drain.pop() {
                        Ok(Some(event)) => {
                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => {
                            // No events in PQ, wait briefly
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            warn!(error = %e, "PQ drain error");
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
                // Checkpoint on exit
                if let Err(e) = pq_drain.checkpoint() {
                    warn!(error = %e, "PQ checkpoint error on shutdown");
                }
            });
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
            let m = Arc::clone(&metrics);
            let im = instance_metrics.clone();
            let dlq_ref = dlq.clone();
            let handle = tokio::spawn(async move {
                run_filter_worker(worker_id, rx, tx, f, m, im, dlq_ref).await;
            });
            filter_handles.push(handle);
        }
        // Drop original output_tx so outputs know when all filter workers are done
        drop(output_tx);
        info!(workers = num_workers, "filter workers started");

        // Spawn output worker
        let outputs: Arc<Vec<Box<dyn OutputPlugin>>> = Arc::new(self.outputs);
        let output_metrics = Arc::clone(&metrics);
        let output_instance_metrics = instance_metrics.clone();
        let output_config = buffer_config.clone();
        let output_dlq = self.dead_letter_queue.clone();
        let output_handle = tokio::spawn(async move {
            run_outputs(
                output_rx,
                outputs,
                output_metrics,
                output_instance_metrics,
                output_config,
                output_dlq,
            )
            .await;
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

        // Now close the input channel to drain filters/outputs
        drop(input_tx);

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

async fn run_filter_worker(
    _worker_id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Event>>>,
    tx: mpsc::Sender<Event>,
    filters: Arc<Vec<Box<dyn FilterPlugin>>>,
    metrics: Arc<PipelineMetrics>,
    instance_metrics: Option<Arc<PipelineMetrics>>,
    dlq: Option<SharedDeadLetterQueue>,
) {
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
            let mut events = vec![event];

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
                            if let Some(ref dlq_handle) = dlq {
                                if let Err(dlq_err) = dlq_handle.write(
                                    "filter",
                                    filter.name(),
                                    &e.to_string(),
                                    serde_json::json!({"_dlq_filter_error": true}),
                                ) {
                                    warn!(error = %dlq_err, "failed to write to DLQ");
                                }
                            }
                        }
                    }
                }
                events = next_events;
            }

            for ev in events {
                if tx.send(ev).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn run_outputs(
    mut rx: mpsc::Receiver<Event>,
    outputs: Arc<Vec<Box<dyn OutputPlugin>>>,
    metrics: Arc<PipelineMetrics>,
    instance_metrics: Option<Arc<PipelineMetrics>>,
    config: BufferConfig,
    dlq: Option<SharedDeadLetterQueue>,
) {
    let mut collector = BatchCollector::new(config);
    let flush_interval = collector.flush_interval();
    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = rx.recv() => {
                if let Some(ev) = event {
                    if let Some(batch) = collector.add(ev) {
                        send_batch(&outputs, batch, &metrics, instance_metrics.as_ref(), dlq.as_ref()).await;
                    }
                } else {
                    // Channel closed, flush remaining
                    if !collector.is_empty() {
                        let batch = collector.flush();
                        send_batch(&outputs, batch, &metrics, instance_metrics.as_ref(), dlq.as_ref()).await;
                    }
                    break;
                }
            }
            _ = flush_timer.tick() => {
                if !collector.is_empty() {
                    let batch = collector.flush();
                    send_batch(&outputs, batch, &metrics, instance_metrics.as_ref(), dlq.as_ref()).await;
                }
            }
        }
    }

    // Close all outputs
    for output in outputs.iter() {
        if let Err(e) = output.flush().await {
            warn!(output = output.name(), error = %e, "output flush error");
        }
        if let Err(e) = output.close().await {
            warn!(output = output.name(), error = %e, "output close error");
        }
    }
}

async fn send_batch(
    outputs: &[Box<dyn OutputPlugin>],
    batch: Vec<Event>,
    metrics: &Arc<PipelineMetrics>,
    instance_metrics: Option<&Arc<PipelineMetrics>>,
    dlq: Option<&SharedDeadLetterQueue>,
) {
    let count = batch.len() as u64;
    // Estimate bytes without expensive JSON serialization — use message
    // length as a proxy. Full JSON is only needed for actual output plugins.
    let bytes: u64 = batch
        .iter()
        .map(|e| e.message().map_or(64, str::len) as u64)
        .sum();

    for output in outputs {
        // Filter events by output condition
        let filtered: Vec<Event> = batch
            .iter()
            .filter(|ev| output.condition().map_or(true, |cond| cond.evaluate(ev)))
            .cloned()
            .collect();

        if filtered.is_empty() {
            continue;
        }

        if let Err(e) = output.output(filtered.clone()).await {
            error!(output = output.name(), error = %e, "output error");
            metrics.record_failed(count);
            if let Some(im) = instance_metrics {
                im.record_failed(count);
            }
            // Send failed events to DLQ if enabled
            if let Some(dlq_handle) = dlq {
                for ev in &filtered {
                    if let Err(dlq_err) =
                        dlq_handle.write("output", output.name(), &e.to_string(), ev.to_json())
                    {
                        warn!(error = %dlq_err, "failed to write to DLQ");
                    }
                }
            }
            return;
        }
    }

    metrics.record_out(count, bytes);
    if let Some(im) = instance_metrics {
        im.record_out(count, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown::ShutdownController;

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
}
