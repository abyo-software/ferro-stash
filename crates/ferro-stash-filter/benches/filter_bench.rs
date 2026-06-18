// SPDX-License-Identifier: Apache-2.0
//! Filter plugin benchmarks — grok, mutate, json, dissect, kv, date.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

fn bench_grok_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({
        "match": { "message": "%{IP:client_ip} %{NOTSPACE:method} %{URIPATH:path} %{INT:status:int}" }
    });
    let filter = ferro_stash_filter::grok::GrokFilter::from_config(&settings, None).expect("grok");

    let mut group = c.benchmark_group("filter_grok");
    group.throughput(Throughput::Elements(1));

    group.bench_function("simple_4fields", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new("192.168.1.100 GET /api/v1/users 200");
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    // Grok miss (no match → tag)
    group.bench_function("no_match", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new("this line does not match the pattern at all");
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();

    // COMBINEDAPACHELOG pattern
    let settings2 = serde_json::json!({
        "match": { "message": "%{COMBINEDAPACHELOG}" }
    });
    let filter2 =
        ferro_stash_filter::grok::GrokFilter::from_config(&settings2, None).expect("grok");

    let mut group2 = c.benchmark_group("filter_grok_apache");
    group2.throughput(Throughput::Elements(1));

    group2.bench_function("combinedapachelog", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new(
                r#"192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08""#,
            );
            let f = &filter2;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group2.finish();
}

fn bench_mutate_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({
        "lowercase": ["method"],
        "convert": { "status": "integer" },
        "add_field": { "pipeline": "ferro-stash" },
        "remove_field": ["raw_message"]
    });
    let filter =
        ferro_stash_filter::mutate::MutateFilter::from_config(&settings, None).expect("mutate");

    let mut group = c.benchmark_group("filter_mutate");
    group.throughput(Throughput::Elements(1));

    group.bench_function("4_operations", |b| {
        b.to_async(&rt).iter(|| {
            let mut event = Event::new("test");
            event.set("method", EventValue::String("GET".into()));
            event.set("status", EventValue::String("200".into()));
            event.set("raw_message", EventValue::String("raw data".into()));
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();
}

fn bench_json_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({ "source": "message" });
    let filter =
        ferro_stash_filter::json_filter::JsonFilter::from_config(&settings, None).expect("json");

    let mut group = c.benchmark_group("filter_json");
    group.throughput(Throughput::Elements(1));

    group.bench_function("parse_10_fields", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new(
                r#"{"host":"server01","port":8080,"method":"GET","path":"/api","status":200,"latency":1.5,"user":"alice","ip":"10.0.0.1","agent":"curl","id":"abc123"}"#,
            );
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.bench_function("invalid_json", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new("this is not json at all");
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();
}

fn bench_dissect_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({
        "mapping": { "message": "%{ip} %{method} %{path} %{status}" }
    });
    let filter =
        ferro_stash_filter::dissect::DissectFilter::from_config(&settings, None).expect("dissect");

    let mut group = c.benchmark_group("filter_dissect");
    group.throughput(Throughput::Elements(1));

    group.bench_function("4_fields", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new("192.168.1.1 GET /index.html 200");
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();
}

fn bench_kv_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({ "source": "message" });
    let filter = ferro_stash_filter::kv::KvFilter::from_config(&settings, None).expect("kv");

    let mut group = c.benchmark_group("filter_kv");
    group.throughput(Throughput::Elements(1));

    group.bench_function("5_pairs", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new("host=server01 port=8080 status=200 method=GET path=/api");
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();
}

fn bench_date_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({
        "match": ["timestamp", "ISO8601"]
    });
    let filter = ferro_stash_filter::date::DateFilter::from_config(&settings, None).expect("date");

    let mut group = c.benchmark_group("filter_date");
    group.throughput(Throughput::Elements(1));

    group.bench_function("iso8601", |b| {
        b.to_async(&rt).iter(|| {
            let mut event = Event::new("test");
            event.set(
                "timestamp",
                EventValue::String("2024-01-15T10:30:00Z".into()),
            );
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();
}

fn bench_ruby_filter_simple(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({
        "code": "event.set('processed', true)"
    });
    let filter = ferro_stash_filter::ruby::RubyFilter::from_config(&settings, None).expect("ruby");

    let mut group = c.benchmark_group("filter_ruby");
    group.throughput(Throughput::Elements(1));

    group.bench_function("simple_set", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new("test log line for ruby processing");
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();
}

fn bench_ruby_filter_complex(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let settings = serde_json::json!({
        "code": r"
            msg = event.get('message')
            if msg.include?('GET')
              event.set('method', 'GET')
              event.set('is_read', true)
            end
            event.set('msg_length', msg.length)
        "
    });
    let filter = ferro_stash_filter::ruby::RubyFilter::from_config(&settings, None).expect("ruby");

    let mut group = c.benchmark_group("filter_ruby_complex");
    group.throughput(Throughput::Elements(1));

    group.bench_function("regex_conditional", |b| {
        b.to_async(&rt).iter(|| {
            let event = Event::new("192.168.1.1 GET /api/v1/users 200");
            let f = &filter;
            async move {
                black_box(f.filter(event).await.expect("filter"));
            }
        });
    });

    group.finish();
}

fn bench_multi_filter_chain(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    // Build a chain: grok → mutate → date
    let grok_settings = serde_json::json!({
        "match": { "message": "%{IP:client_ip} %{NOTSPACE:method} %{URIPATH:path} %{INT:status:int}" }
    });
    let grok =
        ferro_stash_filter::grok::GrokFilter::from_config(&grok_settings, None).expect("grok");

    let mutate_settings = serde_json::json!({
        "lowercase": ["method"],
        "add_field": { "pipeline": "ferro-stash" }
    });
    let mutate = ferro_stash_filter::mutate::MutateFilter::from_config(&mutate_settings, None)
        .expect("mutate");

    let date_settings = serde_json::json!({
        "match": ["timestamp", "ISO8601"]
    });
    let date =
        ferro_stash_filter::date::DateFilter::from_config(&date_settings, None).expect("date");

    let mut group = c.benchmark_group("filter_chain");
    group.throughput(Throughput::Elements(1));

    group.bench_function("grok_mutate_date", |b| {
        b.to_async(&rt).iter(|| {
            let mut event = Event::new("192.168.1.100 GET /api/v1/users 200");
            event.set(
                "timestamp",
                EventValue::String("2024-01-15T10:30:00Z".into()),
            );
            let g = &grok;
            let m = &mutate;
            let d = &date;
            async move {
                let events = g.filter(event).await.expect("grok");
                let mut results = Vec::new();
                for ev in events {
                    let evs = m.filter(ev).await.expect("mutate");
                    for ev2 in evs {
                        let evs2 = d.filter(ev2).await.expect("date");
                        results.extend(evs2);
                    }
                }
                black_box(results);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_grok_filter,
    bench_mutate_filter,
    bench_json_filter,
    bench_dissect_filter,
    bench_kv_filter,
    bench_date_filter,
    bench_ruby_filter_simple,
    bench_ruby_filter_complex,
    bench_multi_filter_chain,
);
criterion_main!(benches);
