// SPDX-License-Identifier: Apache-2.0
//! Config parsing benchmarks — Logstash DSL and YAML.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ferro_stash_config::{parse_config, ConfigFormat};

const SIMPLE_DSL: &str = r#"
input {
  stdin { }
}
filter {
  grok {
    match => { "message" => "%{IP:client_ip} %{NOTSPACE:method} %{URIPATH:path}" }
  }
}
output {
  stdout { codec => rubydebug }
}
"#;

const COMPLEX_DSL: &str = r#"
input {
  beats {
    port => 5044
    ssl => true
    ssl_certificate => "/etc/pki/tls/certs/logstash.crt"
    ssl_key => "/etc/pki/tls/private/logstash.key"
  }
  kafka {
    bootstrap_servers => "kafka1:9092,kafka2:9092,kafka3:9092"
    topics => ["app-logs", "system-logs", "audit-logs"]
    group_id => "logstash-prod"
    consumer_threads => 4
    auto_offset_reset => "latest"
  }
  http {
    port => 8080
  }
}
filter {
  grok {
    match => { "message" => "%{COMBINEDAPACHELOG}" }
    tag_on_failure => ["_grokparsefailure"]
  }
  date {
    match => ["timestamp", "ISO8601"]
    target => "@timestamp"
  }
  mutate {
    convert => { "response" => "integer" }
    lowercase => ["method", "host"]
    remove_field => ["message", "beat"]
    add_field => { "environment" => "production" }
    rename => { "clientip" => "client_ip" }
  }
  geoip {
    source => "client_ip"
    target => "geoip"
  }
  useragent {
    source => "agent"
    target => "user_agent"
  }
  ruby {
    code => "event.set('processed_at', Time.now.to_s)"
  }
}
output {
  elasticsearch {
    hosts => ["https://es1:9200", "https://es2:9200"]
    index => "logs-%{+YYYY.MM.dd}"
    user => "elastic"
    password => "changeme"
  }
  stdout { codec => "dots" }
}
"#;

const SIMPLE_YAML: &str = r#"
pipeline:
  workers: 4
  batch_size: 250
input:
  - type: stdin
filter:
  - type: grok
    match:
      message: "%{IP:client_ip} %{NOTSPACE:method} %{URIPATH:path}"
output:
  - type: stdout
    codec: rubydebug
"#;

fn bench_parse_simple_dsl(c: &mut Criterion) {
    let mut group = c.benchmark_group("config_parse");
    group.throughput(Throughput::Elements(1));

    group.bench_function("simple_dsl", |b| {
        b.iter(|| {
            black_box(
                parse_config(black_box(SIMPLE_DSL), ConfigFormat::LogstashDsl).expect("parse"),
            );
        });
    });

    group.bench_function("complex_dsl_8_plugins", |b| {
        b.iter(|| {
            black_box(
                parse_config(black_box(COMPLEX_DSL), ConfigFormat::LogstashDsl).expect("parse"),
            );
        });
    });

    group.bench_function("simple_yaml", |b| {
        b.iter(|| {
            black_box(parse_config(black_box(SIMPLE_YAML), ConfigFormat::Yaml).expect("parse"));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_parse_simple_dsl);
criterion_main!(benches);
