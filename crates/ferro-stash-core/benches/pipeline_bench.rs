// SPDX-License-Identifier: Apache-2.0
//! Pipeline throughput benchmarks.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use ferro_stash_core::buffer::BufferConfig;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::pipeline::{Pipeline, PipelineConfig};
use ferro_stash_core::shutdown::ShutdownController;

use async_trait::async_trait;
use ferro_stash_core::error::Result;
use ferro_stash_core::plugin::{FilterPlugin, InputPlugin, OutputPlugin};
use tokio::sync::mpsc;

// --- Bench input: generates N events then stops ---
#[derive(Debug)]
struct BenchInput {
    count: usize,
    message: String,
}

#[async_trait]
impl InputPlugin for BenchInput {
    fn name(&self) -> &'static str {
        "bench_input"
    }
    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        _shutdown: ferro_stash_core::shutdown::ShutdownSignal,
    ) -> Result<()> {
        for i in 0..self.count {
            let mut event = Event::new(&self.message);
            event.set("sequence", EventValue::Integer(i as i64));
            event.set("host", EventValue::String("bench-host".into()));
            if sender.send(event).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

// --- Bench filter: noop passthrough ---
#[derive(Debug)]
struct NoopFilter;

#[async_trait]
impl FilterPlugin for NoopFilter {
    fn name(&self) -> &'static str {
        "noop"
    }
    async fn filter(&self, event: Event) -> Result<Vec<Event>> {
        Ok(vec![event])
    }
}

// --- Bench output: counts events ---
#[derive(Debug)]
struct CountingOutput {
    count: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl OutputPlugin for CountingOutput {
    fn name(&self) -> &'static str {
        "counting"
    }
    async fn output(&self, events: Vec<Event>) -> Result<()> {
        self.count
            .fetch_add(events.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

fn bench_pipeline_passthrough(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let mut group = c.benchmark_group("pipeline");
    let event_count = 10_000u64;
    group.throughput(Throughput::Elements(event_count));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("passthrough_10k", |b| {
        b.to_async(&rt).iter(|| async {
            let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let config = PipelineConfig {
                workers: 1,
                buffer: BufferConfig {
                    max_events: 10_000,
                    flush_interval: Duration::from_millis(100),
                    batch_size: 500,
                },
                id: "bench".to_string(),
                ..PipelineConfig::default()
            };
            let mut pipeline = Pipeline::new(config);
            pipeline.add_input(Box::new(BenchInput {
                count: event_count as usize,
                message: "192.168.1.1 GET /api/v1/users 200 1234 \"Mozilla/5.0\" 0.003".to_string(),
            }));
            pipeline.add_output(Box::new(CountingOutput {
                count: Arc::clone(&counter),
            }));

            let (_ctrl, signal) = ShutdownController::new();
            pipeline.run(signal).await.expect("pipeline");
            assert_eq!(
                counter.load(std::sync::atomic::Ordering::Relaxed),
                event_count
            );
        });
    });

    group.bench_function("passthrough_1_filter_10k", |b| {
        b.to_async(&rt).iter(|| async {
            let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let config = PipelineConfig {
                workers: 1,
                buffer: BufferConfig {
                    max_events: 10_000,
                    flush_interval: Duration::from_millis(100),
                    batch_size: 500,
                },
                id: "bench".to_string(),
                ..PipelineConfig::default()
            };
            let mut pipeline = Pipeline::new(config);
            pipeline.add_input(Box::new(BenchInput {
                count: event_count as usize,
                message: "192.168.1.1 GET /api/v1/users 200 1234 \"Mozilla/5.0\" 0.003".to_string(),
            }));
            pipeline.add_filter(Box::new(NoopFilter));
            pipeline.add_output(Box::new(CountingOutput {
                count: Arc::clone(&counter),
            }));

            let (_ctrl, signal) = ShutdownController::new();
            pipeline.run(signal).await.expect("pipeline");
            assert_eq!(
                counter.load(std::sync::atomic::Ordering::Relaxed),
                event_count
            );
        });
    });

    group.bench_function("passthrough_4_workers_10k", |b| {
        b.to_async(&rt).iter(|| async {
            let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let config = PipelineConfig {
                workers: 4,
                buffer: BufferConfig {
                    max_events: 10_000,
                    flush_interval: Duration::from_millis(100),
                    batch_size: 500,
                },
                id: "bench".to_string(),
                ..PipelineConfig::default()
            };
            let mut pipeline = Pipeline::new(config);
            pipeline.add_input(Box::new(BenchInput {
                count: event_count as usize,
                message: "192.168.1.1 GET /api/v1/users 200 1234 \"Mozilla/5.0\" 0.003".to_string(),
            }));
            pipeline.add_filter(Box::new(NoopFilter));
            pipeline.add_output(Box::new(CountingOutput {
                count: Arc::clone(&counter),
            }));

            let (_ctrl, signal) = ShutdownController::new();
            pipeline.run(signal).await.expect("pipeline");
            assert_eq!(
                counter.load(std::sync::atomic::Ordering::Relaxed),
                event_count
            );
        });
    });

    group.finish();
}

criterion_group!(benches, bench_pipeline_passthrough);
criterion_main!(benches);
