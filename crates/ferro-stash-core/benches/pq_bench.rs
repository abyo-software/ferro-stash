// SPDX-License-Identifier: Apache-2.0
//! Persistent queue benchmarks — write, read, round-trip.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::persistent_queue::{PersistentQueue, PqCompression, PqConfig};
use tempfile::TempDir;

fn make_event(i: usize) -> Event {
    let mut event = Event::new("192.168.1.1 GET /api/v1/users 200 1234 \"Mozilla/5.0\" 0.003");
    event.set("host", EventValue::String("web-server-01".into()));
    event.set("status", EventValue::Integer(200));
    event.set("latency_ms", EventValue::Float(15.3));
    event.set("sequence", EventValue::Integer(i as i64));
    event.set("request_id", EventValue::String("abc-123-def-456".into()));
    event
}

fn bench_pq_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistent_queue_write");
    group.throughput(Throughput::Elements(1));

    group.bench_function("push_no_compression", |b| {
        let tmpdir = TempDir::new().expect("tmpdir");
        let config = PqConfig {
            path: tmpdir.path().to_str().expect("path").to_string(),
            max_bytes: 1_073_741_824,
            segment_size: 100_000,
            compression: PqCompression::None,
            checkpoint_interval: 10_000,
        };
        let mut pq = PersistentQueue::open(config).expect("open pq");
        let mut i = 0usize;
        b.iter(|| {
            let event = make_event(i);
            pq.push(black_box(&event)).expect("push");
            i += 1;
        });
    });

    group.bench_function("push_zstd_speed", |b| {
        let tmpdir = TempDir::new().expect("tmpdir");
        let config = PqConfig {
            path: tmpdir.path().to_str().expect("path").to_string(),
            max_bytes: 1_073_741_824,
            segment_size: 100_000,
            compression: PqCompression::Speed,
            checkpoint_interval: 10_000,
        };
        let mut pq = PersistentQueue::open(config).expect("open pq");
        let mut i = 0usize;
        b.iter(|| {
            let event = make_event(i);
            pq.push(black_box(&event)).expect("push");
            i += 1;
        });
    });

    group.finish();
}

fn bench_pq_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistent_queue_roundtrip");

    let batch_size = 100u64;
    group.throughput(Throughput::Elements(batch_size));
    group.sample_size(30);

    group.bench_function("push_pop_100", |b| {
        b.iter(|| {
            let tmpdir = TempDir::new().expect("tmpdir");
            let config = PqConfig {
                path: tmpdir.path().to_str().expect("path").to_string(),
                max_bytes: 1_073_741_824,
                segment_size: 100_000,
                compression: PqCompression::None,
                checkpoint_interval: 10_000,
            };
            let mut pq = PersistentQueue::open(config).expect("open pq");

            // Write batch
            for i in 0..batch_size as usize {
                let event = make_event(i);
                pq.push(&event).expect("push");
            }

            // Read batch
            for _ in 0..batch_size {
                let ev = pq.pop().expect("pop");
                black_box(ev);
            }

            pq.close().expect("close");
        });
    });

    group.finish();
}

criterion_group!(benches, bench_pq_write, bench_pq_roundtrip);
criterion_main!(benches);
