// SPDX-License-Identifier: Apache-2.0
//! Codec benchmarks — JSON encode/decode, plain, msgpack.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ferro_stash_codec::Codec;
use ferro_stash_core::event::{Event, EventValue};

fn make_event() -> Event {
    let mut event = Event::new("192.168.1.1 GET /api/v1/users 200 1234 \"Mozilla/5.0\" 0.003");
    event.set("host", EventValue::String("web-server-01".into()));
    event.set("status", EventValue::Integer(200));
    event.set("latency_ms", EventValue::Float(15.3));
    event.set("method", EventValue::String("GET".into()));
    event.set("path", EventValue::String("/api/v1/users".into()));
    event.set(
        "user_agent",
        EventValue::String("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)".into()),
    );
    event.set(
        "request_id",
        EventValue::String("abc-123-def-456-ghi-789".into()),
    );
    event.set("level", EventValue::String("INFO".into()));
    event.set("region", EventValue::String("us-east-1".into()));
    event.set("service", EventValue::String("api-gateway".into()));
    event.add_tag("web");
    event.add_tag("production");
    event
}

fn bench_json_encode(c: &mut Criterion) {
    let codec = ferro_stash_codec::json::JsonCodec::from_config(&serde_json::json!({}))
        .expect("json codec");
    let event = make_event();

    let mut group = c.benchmark_group("codec_json");
    group.throughput(Throughput::Elements(1));

    group.bench_function("encode", |b| {
        b.iter(|| {
            black_box(codec.encode(black_box(&event)).expect("encode"));
        });
    });

    let encoded = codec.encode(&event).expect("encode");
    group.bench_function("decode", |b| {
        b.iter(|| {
            black_box(codec.decode(black_box(&encoded)).expect("decode"));
        });
    });

    group.bench_function("roundtrip", |b| {
        b.iter(|| {
            let bytes = codec.encode(&event).expect("encode");
            black_box(codec.decode(&bytes).expect("decode"));
        });
    });

    group.finish();
}

fn bench_plain_codec(c: &mut Criterion) {
    let codec = ferro_stash_codec::plain::PlainCodec::from_config(&serde_json::json!({}))
        .expect("plain codec");
    let event = make_event();

    let mut group = c.benchmark_group("codec_plain");
    group.throughput(Throughput::Elements(1));

    group.bench_function("encode", |b| {
        b.iter(|| {
            black_box(codec.encode(black_box(&event)).expect("encode"));
        });
    });

    let encoded = codec.encode(&event).expect("encode");
    group.bench_function("decode", |b| {
        b.iter(|| {
            black_box(codec.decode(black_box(&encoded)).expect("decode"));
        });
    });

    group.finish();
}

fn bench_msgpack_codec(c: &mut Criterion) {
    let codec = ferro_stash_codec::msgpack::MsgpackCodec::from_config(&serde_json::json!({}))
        .expect("msgpack codec");
    let event = make_event();

    let mut group = c.benchmark_group("codec_msgpack");
    group.throughput(Throughput::Elements(1));

    group.bench_function("encode", |b| {
        b.iter(|| {
            black_box(codec.encode(black_box(&event)).expect("encode"));
        });
    });

    let encoded = codec.encode(&event).expect("encode");
    group.bench_function("decode", |b| {
        b.iter(|| {
            black_box(codec.decode(black_box(&encoded)).expect("decode"));
        });
    });

    group.bench_function("roundtrip", |b| {
        b.iter(|| {
            let bytes = codec.encode(&event).expect("encode");
            black_box(codec.decode(&bytes).expect("decode"));
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_json_encode,
    bench_plain_codec,
    bench_msgpack_codec
);
criterion_main!(benches);
