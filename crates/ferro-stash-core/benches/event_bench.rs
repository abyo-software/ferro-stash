// SPDX-License-Identifier: Apache-2.0
//! Event model benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ferro_stash_core::event::{Event, EventValue};

fn bench_event_create(c: &mut Criterion) {
    c.bench_function("event::new", |b| {
        b.iter(|| {
            black_box(Event::new(
                "192.168.1.1 GET /index.html 200 1234 \"Mozilla/5.0\" 0.003",
            ));
        });
    });
}

fn bench_event_from_json(c: &mut Criterion) {
    let json_str = r#"{"message":"test log line","host":"server01","port":8080,"level":"INFO","timestamp":"2024-01-15T10:30:00Z","request_id":"abc-123","method":"GET","path":"/api/v1/users","status":200,"latency_ms":15.3}"#;

    c.bench_function("event::from_json", |b| {
        b.iter(|| {
            let v: serde_json::Value = serde_json::from_str(json_str).expect("parse");
            black_box(Event::from_json(v));
        });
    });
}

fn bench_event_to_json(c: &mut Criterion) {
    let mut event = Event::new("test message");
    event.set("host", EventValue::String("server01".into()));
    event.set("port", EventValue::Integer(8080));
    event.set("level", EventValue::String("INFO".into()));
    event.set("latency", EventValue::Float(1.5));
    event.add_tag("web");

    c.bench_function("event::to_json_string", |b| {
        b.iter(|| {
            black_box(event.to_json_string());
        });
    });
}

fn bench_event_set_get(c: &mut Criterion) {
    let mut event = Event::new("test");
    event.set("host", EventValue::String("server01".into()));

    c.bench_function("event::get_field", |b| {
        b.iter(|| {
            black_box(event.get("host"));
        });
    });

    c.bench_function("event::set_field", |b| {
        b.iter(|| {
            event.set("counter", EventValue::Integer(black_box(42)));
        });
    });
}

fn bench_event_nested_field(c: &mut Criterion) {
    let mut event = Event::new("test");
    event.set("a.b.c", EventValue::Integer(1));

    c.bench_function("event::get_nested_field", |b| {
        b.iter(|| {
            black_box(event.get("a.b.c"));
        });
    });

    c.bench_function("event::set_nested_field", |b| {
        b.iter(|| {
            event.set("x.y.z", EventValue::Integer(black_box(99)));
        });
    });
}

fn bench_event_sprintf(c: &mut Criterion) {
    let mut event = Event::new("hello");
    event.set("host", EventValue::String("server01".into()));
    event.set("status", EventValue::Integer(200));

    c.bench_function("event::sprintf", |b| {
        b.iter(|| {
            black_box(event.sprintf("msg=%{message} host=%{host} status=%{status}"));
        });
    });
}

fn bench_event_tags(c: &mut Criterion) {
    let mut event = Event::new("test");
    for i in 0..10 {
        event.add_tag(format!("tag_{i}"));
    }

    c.bench_function("event::has_tag (10 tags)", |b| {
        b.iter(|| {
            black_box(event.has_tag("tag_9"));
        });
    });
}

fn bench_event_throughput(c: &mut Criterion) {
    let json_str = r#"{"message":"192.168.1.1 GET /api/users 200","host":"web01","status":200}"#;

    let mut group = c.benchmark_group("event_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("create+serialize", |b| {
        b.iter(|| {
            let v: serde_json::Value = serde_json::from_str(json_str).expect("parse");
            let event = Event::from_json(v);
            black_box(event.to_json_string());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_event_create,
    bench_event_from_json,
    bench_event_to_json,
    bench_event_set_get,
    bench_event_nested_field,
    bench_event_sprintf,
    bench_event_tags,
    bench_event_throughput,
);
criterion_main!(benches);
