// SPDX-License-Identifier: Apache-2.0
//! Pipeline metrics collection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;

/// Global pipeline metrics.
#[derive(Debug)]
pub struct PipelineMetrics {
    pub events_in: AtomicU64,
    pub events_out: AtomicU64,
    pub events_filtered: AtomicU64,
    pub events_failed: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub started_at: Instant,
    // Reload statistics
    pub reload_successes: AtomicU64,
    pub reload_failures: AtomicU64,
    pub last_reload_success: std::sync::Mutex<Option<String>>,
    pub last_reload_failure: std::sync::Mutex<Option<String>>,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events_in: AtomicU64::new(0),
            events_out: AtomicU64::new(0),
            events_filtered: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            started_at: Instant::now(),
            reload_successes: AtomicU64::new(0),
            reload_failures: AtomicU64::new(0),
            last_reload_success: std::sync::Mutex::new(None),
            last_reload_failure: std::sync::Mutex::new(None),
        })
    }

    /// Record a successful config reload.
    pub fn record_reload_success(&self) {
        self.reload_successes.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut ts) = self.last_reload_success.lock() {
            *ts = Some(chrono::Utc::now().to_rfc3339());
        }
    }

    /// Record a failed config reload.
    pub fn record_reload_failure(&self) {
        self.reload_failures.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut ts) = self.last_reload_failure.lock() {
            *ts = Some(chrono::Utc::now().to_rfc3339());
        }
    }

    /// Reset event counters (used on pipeline reload — keeps reload stats).
    pub fn reset_events(&self) {
        self.events_in.store(0, Ordering::Relaxed);
        self.events_out.store(0, Ordering::Relaxed);
        self.events_filtered.store(0, Ordering::Relaxed);
        self.events_failed.store(0, Ordering::Relaxed);
        self.bytes_in.store(0, Ordering::Relaxed);
        self.bytes_out.store(0, Ordering::Relaxed);
    }

    pub fn record_in(&self, count: u64, bytes: u64) {
        self.events_in.fetch_add(count, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_out(&self, count: u64, bytes: u64) {
        self.events_out.fetch_add(count, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_filtered(&self, count: u64) {
        self.events_filtered.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_failed(&self, count: u64) {
        self.events_failed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_in: self.events_in.load(Ordering::Relaxed),
            events_out: self.events_out.load(Ordering::Relaxed),
            events_filtered: self.events_filtered.load(Ordering::Relaxed),
            events_failed: self.events_failed.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            uptime_secs: self.started_at.elapsed().as_secs(),
            uptime_millis: self.started_at.elapsed().as_millis() as u64,
            reload_successes: self.reload_successes.load(Ordering::Relaxed),
            reload_failures: self.reload_failures.load(Ordering::Relaxed),
            last_reload_success: self.last_reload_success.lock().ok().and_then(|g| g.clone()),
            last_reload_failure: self.last_reload_failure.lock().ok().and_then(|g| g.clone()),
        }
    }
}

impl Default for PipelineMetrics {
    fn default() -> Self {
        Self {
            events_in: AtomicU64::new(0),
            events_out: AtomicU64::new(0),
            events_filtered: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            started_at: Instant::now(),
            reload_successes: AtomicU64::new(0),
            reload_failures: AtomicU64::new(0),
            last_reload_success: std::sync::Mutex::new(None),
            last_reload_failure: std::sync::Mutex::new(None),
        }
    }
}

/// A point-in-time snapshot of metrics.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub events_in: u64,
    pub events_out: u64,
    pub events_filtered: u64,
    pub events_failed: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub uptime_secs: u64,
    pub uptime_millis: u64,
    pub reload_successes: u64,
    pub reload_failures: u64,
    pub last_reload_success: Option<String>,
    pub last_reload_failure: Option<String>,
}

impl MetricsSnapshot {
    /// Input throughput (events entering the pipeline per second).
    pub fn input_rate(&self) -> f64 {
        rate_for(self.events_in, self.uptime_millis)
    }

    /// Output throughput (events leaving the pipeline per second).
    pub fn output_rate(&self) -> f64 {
        rate_for(self.events_out, self.uptime_millis)
    }

    /// Filter throughput (events that reached the filter stage, per second).
    pub fn filter_rate(&self) -> f64 {
        rate_for(self.events_in, self.uptime_millis)
    }

    /// Backwards-compatible "events per second" = output rate.
    pub fn events_per_second(&self) -> f64 {
        self.output_rate()
    }
}

fn rate_for(count: u64, uptime_millis: u64) -> f64 {
    if uptime_millis == 0 {
        return 0.0;
    }
    count as f64 / (uptime_millis as f64 / 1000.0)
}

/// Per-plugin metrics.
#[derive(Debug)]
pub struct PluginMetrics {
    pub name: String,
    pub plugin_type: String,
    pub events_in: AtomicU64,
    pub events_out: AtomicU64,
    pub duration_ns: AtomicU64,
}

impl PluginMetrics {
    pub fn new(name: impl Into<String>, plugin_type: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            plugin_type: plugin_type.into(),
            events_in: AtomicU64::new(0),
            events_out: AtomicU64::new(0),
            duration_ns: AtomicU64::new(0),
        })
    }

    pub fn record(&self, events_in: u64, events_out: u64, duration_ns: u64) {
        self.events_in.fetch_add(events_in, Ordering::Relaxed);
        self.events_out.fetch_add(events_out, Ordering::Relaxed);
        self.duration_ns.fetch_add(duration_ns, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_record() {
        let m = PipelineMetrics::new();
        m.record_in(10, 1000);
        m.record_out(8, 800);
        m.record_filtered(2);
        let snap = m.snapshot();
        assert_eq!(snap.events_in, 10);
        assert_eq!(snap.events_out, 8);
        assert_eq!(snap.events_filtered, 2);
        assert_eq!(snap.bytes_in, 1000);
    }

    #[test]
    fn test_plugin_metrics() {
        let pm = PluginMetrics::new("grok", "filter");
        pm.record(100, 95, 50_000);
        assert_eq!(pm.events_in.load(Ordering::Relaxed), 100);
        assert_eq!(pm.events_out.load(Ordering::Relaxed), 95);
    }

    #[test]
    fn test_metrics_failed() {
        let m = PipelineMetrics::new();
        m.record_failed(5);
        let snap = m.snapshot();
        assert_eq!(snap.events_failed, 5);
    }

    #[test]
    fn test_metrics_snapshot_eps() {
        let snap = MetricsSnapshot {
            events_in: 100,
            events_out: 80,
            events_filtered: 20,
            events_failed: 0,
            bytes_in: 10000,
            bytes_out: 8000,
            uptime_secs: 10,
            uptime_millis: 10000,
            reload_successes: 0,
            reload_failures: 0,
            last_reload_success: None,
            last_reload_failure: None,
        };
        assert!((snap.events_per_second() - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_metrics_snapshot_eps_zero_uptime() {
        let snap = MetricsSnapshot {
            events_in: 100,
            events_out: 80,
            events_filtered: 20,
            events_failed: 0,
            bytes_in: 10000,
            bytes_out: 8000,
            uptime_secs: 0,
            uptime_millis: 0,
            reload_successes: 0,
            reload_failures: 0,
            last_reload_success: None,
            last_reload_failure: None,
        };
        assert!((snap.events_per_second() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_metrics_bytes() {
        let m = PipelineMetrics::new();
        m.record_in(5, 500);
        m.record_out(5, 450);
        let snap = m.snapshot();
        assert_eq!(snap.bytes_in, 500);
        assert_eq!(snap.bytes_out, 450);
    }

    #[test]
    fn test_metrics_default() {
        let m = PipelineMetrics::default();
        assert_eq!(m.events_in.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_plugin_metrics_accumulate() {
        let pm = PluginMetrics::new("mutate", "filter");
        pm.record(10, 10, 1000);
        pm.record(20, 18, 2000);
        assert_eq!(pm.events_in.load(Ordering::Relaxed), 30);
        assert_eq!(pm.events_out.load(Ordering::Relaxed), 28);
        assert_eq!(pm.duration_ns.load(Ordering::Relaxed), 3000);
    }

    #[test]
    fn test_plugin_metrics_fields() {
        let pm = PluginMetrics::new("elasticsearch", "output");
        assert_eq!(pm.name, "elasticsearch");
        assert_eq!(pm.plugin_type, "output");
    }
}
