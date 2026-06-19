# FerroStash

[![CI](https://github.com/abyo-software/ferro-stash/actions/workflows/ci.yml/badge.svg)](https://github.com/abyo-software/ferro-stash/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

## Drop the JVM: a Logstash-compatible data pipeline in Rust — ~10× less memory, instant start

**FerroStash** ingests, transforms, and routes events through the same
`input → filter → output` model as [Logstash](https://www.elastic.co/logstash),
parsing the Logstash `pipeline.conf` DSL natively so your existing pipelines run
**without a JVM**. Same config language, same event model (`@timestamp`, tags,
`[a][b]` field references, `%{field}` interpolation) — but a single ~14 MB static
binary that starts in milliseconds and holds tens of MB of RAM instead of ~1 GB.

```
  inputs  ──▶   filters   ──▶   outputs
  stdin, file,      grok, mutate, json,     elasticsearch, kafka,
  tcp, http,        kv, dissect, date,      s3, http, file, datadog,
  syslog, kafka,    ruby (mruby) +          tcp, stdout, …
  beats, redis, …   native `script`, …
```

- **Drop-in config** — runs your `pipeline.conf` unchanged; output verified
  byte-for-byte against Logstash 9.4.2 across 24 parity fixtures.
- **A fraction of the footprint** — ~8–13× lower RSS and ~700× faster cold start
  than Logstash in our benchmark (see [Performance](#performance)).
- **Keep your Ruby, or go fast** — an embedded [Artichoke](https://www.artichokeruby.org/)
  (mruby) interpreter runs your `ruby { }` unchanged for migration; the native
  `script { }` filter (Painless subset) runs custom logic ~3.6× faster than JRuby.
- **No JVM, no GC pauses** — one static binary, deterministic latency.

## Status

**v1.0.0 — first stable release**, with a SemVer-stable surface (config DSL,
event model, CLI flags, and plugin set are frozen for the 1.x line). This is a
contract on API/behaviour stability, **not** a production track record:
single-developer project, **no public production deployments yet** — run it
beside your existing pipeline before trusting it with irreplaceable data.
`cargo test --workspace` runs **1,400+ tests, 0 failing**, with `cargo clippy
-D warnings`, `cargo fmt --check`, and `cargo deny check` clean, and output
verified byte-for-byte against Logstash 9.4.2 (24/24 parity fixtures).

The ten previously-stubbed connector plugins (input/output `kafka`,
`redis`, `s3`; output `datadog`; filters `geoip`, `dns`,
`elasticsearch`) now have **real external integrations** and are
**live-validated against real services** (real Apache Kafka, Redis, AWS
S3, Elasticsearch, and the DataDog intake) via `#[ignore]`, env-gated
smoke tests. Those smoke tests are run manually with the service
available — they are **not** part of CI (which has no brokers or
credentials) and verify reachability + a round-trip, not exhaustive
conformance. See [Honest limitations](#honest-limitations) for exactly
what was validated and the feature residuals per plugin; read those
caveats before deploying any connector.

## Why teams use FerroStash

| Need | What FerroStash gives you |
|---|---|
| Logstash's JVM holds ~1 GB RAM per pipeline | A native binary that holds tens of MB — pack far more shippers per host |
| ~8–30 s JVM cold start hurts sidecars, autoscaling, short-lived jobs | Sub-second start (~10 ms in practice) |
| ~350 MB install + a JDK to ship everywhere | One ~14 MB static binary, no runtime to install |
| You can't rewrite hundreds of existing `pipeline.conf` files | They run unchanged — same DSL and event model, byte-eq verified against Logstash 9.4.2 |
| Custom `ruby { }` logic is your escape hatch | mruby runs it as-is for migration; rewrite the hot path in native `script { }` for ~3.6× JRuby |
| GC pauses jitter your tail latency | No GC — deterministic latency |

**At a glance** (vs Logstash on the JVM):

| Property | Logstash (JVM) | FerroStash (native) |
|----------|----------------|---------------------|
| Runtime | JVM (Java) + JRuby | Native Rust binary |
| Idle memory (RSS) | ~0.5–1 GB+ | ~10–50 MB |
| Cold start | ~8–30 s (JVM warm-up) | < 1 s (~10 ms) |
| Install / binary size | ~300–400 MB (+JVM) | ~12–15 MB stripped |
| GC pauses | Yes (G1GC) | None (deterministic) |

Figures are measured on a single host; treat all comparative numbers as evidence
from one environment (see [Performance](#performance)), not a universal guarantee.

## Performance

Measured on a single dedicated host (AWS `c7i.2xlarge`, 8 vCPU, x86-64) against
**Logstash 9.4.2** — identical pipeline and byte-identical input on both
engines, output to `null` (so the sink is never the bottleneck), 8 workers,
throughput startup-subtracted and reported as the mean of 3 runs. Reproduce with
[`bench/`](bench/) (`./bench/run_bench.sh`). These are **one-environment
numbers, not a universal guarantee.**

### Throughput and memory (native filters, 5M events)

| Filter | Logstash 9.4.2 | FerroStash | Throughput | Logstash RSS | FerroStash RSS | Memory |
|--------|---------------:|-----------:|:----------:|-------------:|---------------:|:------:|
| grok          | 193k ev/s | 332k ev/s | **1.7×** | 1,113 MB | 98 MB  | **11× less** |
| dissect       | 193k      | 318k      | **1.6×** | 1,098 MB | 106 MB | 10× less |
| json          | 174k      | 255k      | **1.5×** | 1,100 MB | 133 MB | 8× less  |
| kv            | 171k      | 258k      | **1.5×** | 1,170 MB | 126 MB | 9× less  |
| csv           | 25k       | 79k       | **3.2×** | 1,471 MB | 117 MB | 13× less |
| grok + mutate | 186k      | 251k      | **1.35×**| 1,099 MB | 120 MB | 9× less  |

Cold start is **~0.01 s for FerroStash vs ~7 s** (JVM warm-up) for Logstash on
every workload. The throughput edge is modest-but-consistent; the decisive wins
are **~8–13× lower memory** and near-instant startup.

### Custom logic: Painless vs Ruby

Logstash's only in-pipeline custom-logic path is `ruby { }`. FerroStash runs that
same Ruby (Artichoke/mruby, for drop-in migration) **and** offers a native
`script { }` filter (a Painless subset) for the hot path. Same transformation,
same input:

| Engine | Throughput |
|--------|-----------:|
| FerroStash `script` (native Painless) | **525k ev/s** |
| Logstash `ruby` (JRuby)               | ~145k ev/s |
| FerroStash `ruby` (mruby)             | 11k ev/s |

- **`script` is ~3.6× faster than Logstash's `ruby`/JRuby** (and ~48× faster
  than FerroStash's own mruby filter) — native execution, no JVM, the script is
  parsed once and the cached AST is reused per event.
- **`ruby` (mruby) is ~13× slower than JRuby.** It exists for *migration
  compatibility* — your existing `ruby { }` configs run unchanged — **not** for
  speed. Move hot-path logic to `script { }` to get native, JRuby-beating
  throughput. The mruby gap is inherent (no JIT + per-event marshalling);
  parallelizing it across workers narrows but does not close it.

The JRuby custom-logic figure is corroborated across runs (~143–147k); see
[`bench/`](bench/) for the full methodology and configs.

## Logstash compatibility scope

FerroStash targets the **production-common subset** of Logstash, not its
full plugin catalogue. It implements **~74% of the plugins bundled with
Logstash 9.4.2** (82 / 111) — **codecs 100%, filters 86%, inputs 53%, outputs 65%** — weighted toward the parse/filter hot path; the long tail of connectors
(AWS CloudWatch, RabbitMQ, …) is the main gap. A
config that uses a missing plugin **fails fast** at load, so check the full
**[compatibility matrix](docs/COMPATIBILITY.md)** before migrating. The
default event shape (`@timestamp`, tags, bracket-notation field
references `[a][b]`, `%{field}` interpolation) and the `.conf` DSL follow
Logstash semantics; the docker-driven regression harness asserts
field-by-field equality against **Logstash 9.4.2** for a curated fixture
set (see below). Throughput/memory benchmarks (see
[Performance](#performance)) were also run against **Logstash 9.4.2**.
There is no single pinned "target Logstash version" —
the config language and event model are broadly stable across Logstash
5.x–9.x, and compatibility is asserted per-fixture rather than claimed
wholesale.

### Verified parity evidence

`tests/e2e/logstash_docker_compat_test.rs` pipes the same `pipeline.conf`
+ input through both `target/debug/ferro-stash` and
`docker.elastic.co/logstash/logstash:9.4.2`, then asserts every event
payload equal field-by-field after stripping only runtime-only fields
(`@timestamp`, `@version`, `host`, `event.original`). **24/24 fixtures
pass byte-equal**, covering `stdin → stdout(json)`, grok, mutate
(rename/case/gsub/convert/copy/strip), json, kv, dissect, fingerprint,
date, clone, csv, truncate, translate, split, urldecode, drop, conditional
`if/else if/else`, and unicode inputs. These tests are `#[ignore]` (they
require Docker); run them with:

```bash
cargo build --bin ferro-stash
docker pull docker.elastic.co/logstash/logstash:9.4.2
cargo test -p ferro-stash-e2e --test logstash_docker_compat_test \
    -- --ignored --nocapture --test-threads=1
```

The docker-driven regression harness under
[`tests/logstash-compat/`](tests/logstash-compat/) is the authoritative,
runnable record of what this evidence does and does not substantiate.

## When NOT to use FerroStash

Honest list of cases where FerroStash isn't the right call — better to know now:

- **You need the full Logstash plugin catalogue.** FerroStash implements the
  production-common subset (see [Plugins](#plugins)); the 200+ community plugins
  are out of scope. If your pipeline leans on a niche plugin, check the list first.
- **You need a battle-tested tool with a production track record today.** This is
  a single-developer project with **no public production deployments yet**. For
  irreplaceable data, run it alongside your existing pipeline first.
- **Your hot path is heavy custom Ruby.** The `ruby { }` filter (mruby) is for
  *migration* and is ~13× slower than Logstash's JRuby — port hot logic to the
  native `script { }` filter, or stay on Logstash if you can't.
- **You're already throughput-saturated and memory isn't a concern.** The native
  throughput edge is modest (~1.4–1.7×); FerroStash's decisive wins are memory
  (~8–13× less) and cold start (~700×). If neither helps you, the upside is small.
- **You need SOC2 / ISO 27001 / FedRAMP evidence.** Those reports don't exist
  yet.

> If you run Logstash on the JVM, want your existing `.conf` to keep working, and
> care about memory or startup time, FerroStash is built for you — point one
> pipeline at it next to your current one and compare.

## Plugins

Counts below reflect what is **registered in the plugin factories**
(`create_input` / `create_filter` / `create_output` / `create_codec`),
verified against source. The ten connector plugins that were formerly
stubs now perform real external integrations and are **live-validated**
(`live-validated` in the **Status** column) against real services via
`#[ignore]`, env-gated smoke tests — run manually with the service
available, not in CI. See the Notes column and
[Honest limitations](#honest-limitations) for exactly what each smoke
test exercises and the per-plugin feature residuals.

### Input plugins (18 registered)

| Plugin | Status | Notes |
|--------|--------|-------|
| `stdin` | functional | one event per line |
| `file` | functional | tailing, glob, sincedb, rotation detection |
| `tcp` | functional | TLS via rustls |
| `udp` | functional | datagram input |
| `http` | functional | HTTP POST (JSON / plain) |
| `syslog` | functional | RFC 3164 / RFC 5424, TCP + UDP |
| `generator` | functional | synthetic events for test/bench |
| `heartbeat` | functional | periodic events |
| `beats` | functional | Lumberjack v2 (Beats) protocol over TCP |
| `elasticsearch` | functional | `search_after` + Point-in-Time pagination (reqwest) |
| `dead_letter_queue` | functional | reads from the on-disk DLQ |
| `pipeline` | functional | pipeline-to-pipeline (multi-pipeline mode) |
| `kafka` | real (live-validated) | `rdkafka` async `StreamConsumer`: subscribe, recv loop, codec decode, auto offset commit. Live round-trip validated against real Apache Kafka 3.9.1 (and redpanda) via an `#[ignore]` smoke test (`KAFKA_BROKERS`); not run in CI. `consumer_threads`/`max_poll_records` parsed but not yet wired; no SASL/SSL passthrough; auto-commit only |
| `redis` | real (live-validated) | async client: `BLPOP` (list), `SUBSCRIBE`/`PSUBSCRIBE` (channel/pattern), `AUTH` + `SELECT`. Password-only AUTH (no username/ACL), no TLS (`rediss://`), pub/sub `key` is a single channel/pattern |
| `s3` | real (live-validated) | `aws-sdk-s3`: paginated `ListObjectsV2` + `GetObject` poll, in-memory seen-key dedup, optional `delete_after_read`. Seen-key set is not persisted (reprocesses non-deleted objects after restart — no sincedb); no SQS-notification mode |

### Filter plugins (33 registered)

| Plugin | Status | Notes |
|--------|--------|-------|
| `grok` | functional | ~50 built-in patterns (IP, TIMESTAMP_ISO8601, COMBINEDAPACHELOG, …) via the `regex` crate |
| `mutate` | functional | rename/replace/uppercase/lowercase/strip/gsub/convert/split/join/add/remove |
| `json` | functional | parse JSON strings into fields |
| `date` | functional | ISO8601, UNIX, UNIX_MS, custom formats |
| `dissect` | functional | delimiter-based extraction (no regex) |
| `kv` | functional | key=value extraction |
| `drop` | functional | drop events |
| `clone` | functional | duplicate events |
| `ruby` | functional | full Ruby via embedded Artichoke interpreter |
| `script` / `painless` | functional | native Painless-style DSL (`ferro-script`), parsed once + interpreted natively (a Cranelift JIT path exists for numeric scoring) |
| `sleep` | functional | rate limiting / delay |
| `aggregate` | functional | stateful cross-event aggregation |
| `throttle` | functional | rate-based throttling |
| `translate` | functional | dictionary / file-based lookup |
| `fingerprint` | functional | MD5, SHA1, SHA256, etc. |
| `useragent` | functional | UA parsing via built-in regex patterns (not the full uap database) |
| `csv` | functional | CSV field extraction |
| `urldecode` | functional | percent-decoding |
| `split` | functional | split a field into multiple events |
| `truncate` | functional | length capping |
| `prune` | functional | allowlist/denylist of fields |
| `xml` | functional | XML parsing into fields |
| `metrics` | functional | meter/counter events |
| `de_dot` | functional | replace `.` in field names |
| `json_encode` | functional | serialize a field to a JSON string |
| `bytes` | functional | parse human byte sizes (e.g. `1.5kB`) |
| `cidr` | functional | match address(es) against CIDR network(s) (IPv4/IPv6); on match applies `add_field` / `add_tag` |
| `uuid` | functional | set a v4 UUID into `target` (with `overwrite`) |
| `syslog_pri` | functional | decode syslog PRI into facility/severity codes + labels (default PRI 13) |
| `anonymize` | functional | replace field values with a consistent hash (SHA1/256/384/512, MD5, MURMUR3; optional HMAC `key`) |
| `geoip` | real (live-validated) | `maxminddb` lookups against a configured `.mmdb` (`database` field), full Logstash-style subfields. Falls back to private/loopback/public classification when no `database` is set. Validated against a real GeoLite2-City database |
| `dns` | real (live-validated) | `hickory-resolver` forward (A/AAAA) and reverse (PTR) lookups, custom `nameserver`, `Replace`/`Append` action. Validated against `8.8.8.8` |
| `elasticsearch` | real (live-validated) | `reqwest` `_search` with host failover, query-template `%{field}` sprintf, hits→field mapping. Live-validated against real Elasticsearch 8.15.3 (a seeded hit is mapped into the target field) via an `#[ignore]` smoke test (`ES_URL`); not run in CI |

### Output plugins (16 registered)

| Plugin | Status | Notes |
|--------|--------|-------|
| `stdout` | functional | json, rubydebug, line, dots |
| `elasticsearch` (aliases `ferrosearch`, `opensearch`) | functional | Bulk `_bulk` API via reqwest |
| `file` | functional | JSON lines or custom format |
| `http` | functional | POST/PUT/PATCH |
| `tcp` | functional | TLS via rustls |
| `udp` | functional | codec-encoded datagrams via `tokio::net::UdpSocket` (best-effort, fire-and-forget) |
| `csv` | functional | append CSV rows to a file; `fields` define column order, `csv_options` (separator/quote) |
| `null` | functional | discard (benchmarking) |
| `pipeline` | functional | pipeline-to-pipeline (multi-pipeline mode) |
| `kafka` | real (live-validated) | `rdkafka` `FutureProducer`: codec serialize, key sprintf, compression/acks/retries, flush. Live round-trip validated against real Apache Kafka 3.9.1 (and redpanda) via an `#[ignore]` smoke test (`KAFKA_BROKERS`); not run in CI |
| `redis` | real (live-validated) | async `ConnectionManager`: `RPUSH` (list) / `PUBLISH` (channel). Password-only AUTH (no username/ACL), no TLS (`rediss://`), `key` is a single channel |
| `s3` | real (live-validated) | `aws-sdk-s3` `PutObject` on rotation/flush (+gzip when `encoding => "gzip"`). New `endpoint` / `force_path_style` fields for MinIO/LocalStack/S3-compatible stores. Single `PutObject` (no multipart upload) in v1. Live-validated against real AWS S3 (write/list/read-back) and MinIO via an `#[ignore]` smoke test |
| `datadog` | real (live-validated) | `reqwest` POST to `/api/v2/logs` (`DD-API-KEY`, batched, retry/backoff). Live-validated against the real DataDog Log Intake (AP1) via an `#[ignore]` smoke test; a `site` shorthand selects the region |

### Codecs (21 registered)

`plain`/`line`, `json`/`json_lines`, `multiline`, `csv`, `script`/`ruby`,
`rubydebug`, `dots`, `bytes`, `es_bulk`, `msgpack`, `fluent`, `graphite`,
`cef`, `netflow` (v5/v9/IPFIX), `collectd`, `avro`, `protobuf`,
`cloudfront`, `cloudtrail`, `nmap`, `edn`/`edn_lines`.

## Configuration

- **Logstash DSL** (`.conf`) — `input`/`filter`/`output` blocks, plugin
  options with `=>`, hash and array literals.
- **YAML** — an alternative structured format.
- **Conditionals** — `if` / `else if` / `else` chains with mutually
  exclusive branch semantics; operators `==`, `!=`, `<`, `>`, `>=`,
  `<=`, `=~`, `!~`, `in`, `not in`, `and`, `or`, `nand`, `xor`.
- **Field references** — bracket notation `[a][b][c]`.
- **Interpolation** — `%{field}` in strings; `${ENV_VAR}` /
  `${ENV_VAR:default}` environment expansion.

## Quick start

```bash
# Build (requires a C compiler for the Artichoke/mruby FFI and cmake for
# the rdkafka-backed kafka plugins — see Prerequisites)
cargo build --release

# Run with a Logstash DSL config
./target/release/ferro-stash -f config/example.conf

# Run with a YAML config
./target/release/ferro-stash -f config/example.yml

# Inline pipeline
./target/release/ferro-stash -e 'input { stdin { } } output { stdout { } }'

# Validate a config without running it
./target/release/ferro-stash --config.test_and_exit -f config/example.conf

# Enable the metrics API
./target/release/ferro-stash -f config/example.conf --api.enabled --api.http.host 127.0.0.1:9600
```

The CLI mirrors Logstash flag names (`-f`/`--path.config`,
`-e`/`--config.string`, `-w`/`--pipeline.workers`,
`-b`/`--pipeline.batch.size`, `--log.level`, `--config.reload.automatic`,
etc.).

### Logstash DSL example

```
input {
  file {
    path => "/var/log/nginx/access.log"
    start_position => "beginning"
  }
}

filter {
  grok {
    match => { "message" => "%{COMBINEDAPACHELOG}" }
  }
  date {
    match => ["timestamp", "dd/MMM/yyyy:HH:mm:ss Z"]
  }
  mutate {
    convert => { "response" => "integer" }
  }
}

output {
  elasticsearch {
    hosts => ["http://localhost:9200"]
    index => "logs-%{+%Y.%m.%d}"
  }
}
```

## The Ruby / Artichoke compatibility story

Logstash's `ruby { code => "..." }` filter is used heavily in real
deployments, so FerroStash embeds a Ruby interpreter to run that code
unchanged. It does **not** shell out to CRuby or JRuby; instead it links
[Artichoke](https://www.artichokeruby.org/), an mruby-based Ruby
implementation written in Rust, through the `ferro-stash-ruby` crate.
Events are marshalled to a Ruby `Hash` at the FFI boundary and read back
afterwards, so Ruby code cannot corrupt Rust memory.

**Performance trade-off (honest):** the Artichoke (mruby) interpreter has
no JIT and pays a per-event Rust↔Ruby serialization cost, so the Ruby
filter is measurably *slower* than Logstash's JRuby on the same code
(~13× slower against Logstash 9.4.2 in our benchmark — see
[Performance](#performance)). The Ruby filter exists for **migration
compatibility**, not throughput. For custom logic that needs to be fast,
prefer the native `script` (Painless-style) filter: it is executed natively
(parsed once, no JVM) and in our benchmark ran ~3.6× faster than Logstash's
JRuby and ~48× faster than the mruby filter.

**Optional, off by default.** The Ruby filter lives behind the `ruby` cargo
feature and is **not** built by default, so the common build is light and
needs no extra toolchain:

```bash
cargo build                                        # default — no Ruby/Artichoke
cargo build -p ferro-stash --features ruby         # CLI with the Ruby filter
```

A pipeline that uses `ruby { ... }` in a binary built without the feature fails
fast with a clear "rebuild with `--features ruby`" error rather than silently
dropping the filter.

**Fork dependency (maintenance note):** `ferro-stash-ruby` depends on a fork of
Artichoke pulled as a **rev-pinned git dependency**, so a fresh clone builds
the Ruby feature with no sibling checkout:

```toml
# crates/ferro-stash-ruby/Cargo.toml
artichoke-backend = { git = "https://github.com/abyo-software/artichoke-extended", rev = "245b894...", ... }
artichoke-core    = { git = "https://github.com/abyo-software/artichoke-extended", rev = "245b894..." }
```

The fork (branch `extended`) carries local patches needed for Logstash
Ruby-filter compatibility. Notes:

- **A fresh clone builds** — `cargo build --features ruby` fetches the pinned
  fork revision automatically; there is no submodule or sibling-checkout
  requirement. The default build doesn't fetch it at all.
- **Reproducible pin.** The exact `rev` is recorded in `Cargo.toml` /
  `Cargo.lock`; bump it deliberately to adopt fork updates.
- **Bus-factor and upstream risk.** The Ruby filter's long-term viability is
  tied to maintaining this fork. It is deliberately isolated in its own crate
  (and behind a feature) so the rest of the pipeline is unaffected if Ruby
  support is dropped or reworked.

## Architecture

```
┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
│  Input  │──▶│  Codec  │──▶│ Filter  │──▶│ Output  │
│ plugins │   │ decode  │   │ plugins │   │ plugins │
└─────────┘   └─────────┘   └─────────┘   └─────────┘
        tokio async runtime (mpsc channels + backpressure)
```

| Crate | Responsibility |
|-------|----------------|
| `ferro-stash-core` | Event model, plugin traits, pipeline engine, conditions, buffering, metrics, DLQ |
| `ferro-stash-config` | Logstash DSL parser + YAML config parser |
| `ferro-stash-codec` | Codecs (21 registered) |
| `ferro-stash-input` | Input plugins |
| `ferro-stash-filter` | Filter plugins |
| `ferro-stash-output` | Output plugins |
| `ferro-stash-ruby` | Artichoke (mruby) Ruby interpreter bridge for the `ruby` filter |
| `ferro-script` | Native Painless-style scripting engine — tree-walking interpreter (parsed once, reused per event); a Cranelift JIT path exists for numeric scoring. Powers the `script` filter/codec |
| `ferro-stash-cli` | `ferro-stash` binary: CLI, signal handling, metrics API |
| `ferro-stash-e2e` | Integration / Logstash-parity test harness (no library code) |

More detail: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Build, test, run

```bash
# Default build: light, no Ruby/Artichoke (operates on default-members)
cargo build
cargo test
cargo clippy --all-targets
cargo fmt --all -- --check
cargo deny check

# Optional Ruby filter (pulls Artichoke from its git dependency; needs clang/gcc + cmake)
cargo build -p ferro-stash --features ruby
cargo test  -p ferro-stash-filter --features ruby
```

The default build excludes `ferro-stash-ruby` (it is not a `default-member`), so
it is fast and toolchain-light. `cargo build --workspace` additionally compiles
the Ruby crate.

### Prerequisites

- Rust stable (`rust-version = 1.75` workspace-wide; `ferro-stash-ruby`
  raises its own MSRV to 1.88 and uses edition 2024).
- **`cmake`** — required by the `kafka` plugins, which pull `rdkafka` and
  build a vendored `librdkafka` via CMake. (TLS in the connectors uses
  rustls, so no system OpenSSL is needed.)
- A C compiler (clang or gcc) — **only** for the optional `ruby` feature
  (Artichoke/mruby FFI). The default build does not need it.
- **Runtime, not build-time:** the `geoip` filter needs a user-supplied
  `.mmdb` (GeoLite2/GeoIP2) database file at the configured `database`
  path; it is not vendored.

## Fuzzing

Four `cargo-fuzz` targets live under [`fuzz/`](fuzz/): `codec_decode`,
`logstash_dsl_parse`, `netflow_decode`, and `cef_decode`. The 2026-05-02
wave surfaced and fixed three production DoS panics (protobuf/avro offset
overflow, DSL UTF-8 char-boundary panic); regression seeds are committed.

## FerroSearch integration

The `elasticsearch` output (aliases `ferrosearch` / `opensearch`) speaks
the Elasticsearch Bulk (`_bulk`) API and is the intended sink for
[FerroSearch](https://github.com/ferrosearch/ferrosearch):

```
Data sources → FerroStash → FerroSearch → Applications
```

## Honest limitations

- **Connector plugins are live-validated, but via manual smoke tests
  (not continuous CI).** The ten formerly-stub plugins (`kafka`, `redis`,
  `s3` input and output; `datadog` output; `geoip`, `dns`,
  `elasticsearch` filters) perform real external integrations and are now
  **live-validated against real services** by `#[ignore]`, env-gated smoke
  tests. Those tests are run manually with the service available (locally
  via Docker, or against a real cloud account for S3/DataDog) — they are
  **not** part of the automated CI run, which has no brokers or
  credentials. The smoke tests verify reachability + a real round-trip
  (and, for the ES filter, that a seeded hit is mapped); they are **not**
  exhaustive conformance suites. What was validated, and the feature
  residuals that remain regardless of validation:
  - **kafka** in/out — produce/consume round-trip against **real Apache
    Kafka 3.9.1** (and redpanda). Residuals:
    `consumer_threads`/`max_poll_records` parsed but not yet wired; no
    SASL/SSL `security.protocol` passthrough; auto-commit only.
  - **redis** in/out — against **real Redis**. Residuals: password-only
    `AUTH` (no `username`/ACL); no TLS (`rediss://`); a pub/sub `key` is a
    single channel/pattern (no comma-split list).
  - **s3** in/out — against **real AWS S3** (object written, listed, and
    read back) and **MinIO** (via `endpoint`/`force_path_style`, supported
    on both input and output). Residuals: input seen-key dedup is
    in-memory, so non-deleted objects are reprocessed after a restart (no
    sincedb); no SQS-notification mode; `delete_after_read` deletes
    immediately after emit; output is a single `PutObject` (no multipart)
    in v1.
  - **datadog** output — against the **real DataDog Log Intake** (AP1
    account). A `site` shorthand (`us1`/`us3`/`us5`/`eu`/`ap1`/`us1-fed`)
    selects the intake host; `host` overrides it for proxies.
  - **elasticsearch** filter + output — against **real Elasticsearch
    8.15.3**: the filter maps a seeded hit into the target field; the
    output bulk-indexes an event and the document is counted back.
  - **geoip** filter — `maxminddb` lookups, against a real GeoLite2-City
    database (user-supplied `.mmdb` at `database`, not vendored; falls back
    to private/loopback/public classification when unset).
  - **dns** filter — `hickory-resolver` forward (A/AAAA) and reverse (PTR)
    lookups, against `8.8.8.8`.
- **Plugin catalogue is scoped.** The registered surface covers
  production-common Logstash usage, not Logstash's full 200+ plugin
  ecosystem. There is no dynamic plugin loading; everything is compiled
  in.
- **Logstash DSL coverage is a subset.** Common syntax (plugin blocks,
  conditionals, hash/array literals, interpolation, field references) is
  supported; exotic operators and unusual array-indexing forms are not
  exhaustively covered.
- **Ruby filter is slower than JRuby and depends on a local fork.** See
  the Ruby/Artichoke section. The fork is a path dependency with no
  in-repo pin or vendored copy.
- **Enterprise features absent.** No centralized/Kibana management, no
  X-Pack security, no keystore. A persistent queue and DLQ exist in the
  core crate but are not a full Logstash-parity feature set.
- **Persistent queue provides at-least-once delivery (duplicates possible,
  not exactly-once).** `queue.type: persisted` advances its durable cursor
  only *after* an event reaches a terminal state — not when it is dequeued
  for processing. An entry is terminal once **every** matching output's
  `output()` returned `Ok` for it (the delivery point), OR it was
  intentionally dropped by a filter, OR its delivery/filter failure was
  **durably captured** in the DLQ. An entry that is read but not yet terminal
  when the process crashes is **replayed** on restart. Consequences to know:
  - **Duplicates.** An event delivered in the window after delivery but
    before its ack is checkpointed re-delivers on restart; with multiple
    outputs, replaying an entry that one output already took re-delivers to
    that output. Make outputs idempotent (e.g. document IDs) where it
    matters — exactly-once is **not** provided.
  - **Buffering outputs are flushed before their entries are acked.** Most
    outputs deliver synchronously within `output()`; **s3** buffers and
    uploads on rotation. To keep the guarantee for s3, the pipeline flushes
    *every* output to durability before each durable ack (periodic and at
    shutdown) and only advances the cursor when the flush succeeds — so an
    entry is acked only after the output it was delivered to has durably
    persisted it. The cost: with a persistent queue, s3 uploads on the ack
    cadence (`pipeline.batch.delay`) rather than only on its own rotation, so
    set `pipeline.batch.delay` high enough to keep object sizes reasonable. A
    crash between a successful flush and the checkpoint just re-delivers
    (duplicate), never loses.
  - **Durability scope: process crash by default; power loss with `fsync`.**
    By default the PQ segments/checkpoint and the DLQ are flushed to the OS
    (page cache), not `fsync`'d — so the at-least-once guarantee covers a
    process crash/restart but not a power loss / kernel panic. Set
    `queue.fsync: true` (and `dead_letter_queue.fsync: true`) to fsync every
    append and an atomic, fsync'd checkpoint (temp→fsync→rename→dir-fsync), at a
    significant throughput cost (a disk sync per append) — use it when the host
    can lose power and committed events must survive.
  - **Failure handling.** A failed delivery (or filter error) is acked only
    if it is durably captured in the DLQ; if there is no DLQ, the DLQ is full,
    or the DLQ write fails, the entry is left un-acked and replays. A
    persistently failing output with no DLQ therefore backs the queue up (the
    durable buffer) rather than dropping. Enable the **dead-letter queue** to
    capture failures (with the real event payload) for replay via the
    `dead_letter_queue` input.
  - **Sizing.** Because entries are retained until *terminal* (not just until
    read), size `queue.max_bytes` for the in-flight/undelivered window: if the
    queue reaches `max_bytes` while delivery lags, new events fall back to the
    non-durable in-memory path (best-effort, not replayable), re-opening a
    durability gap. The duplicate/replay window is otherwise bounded by the
    output flush interval (`pipeline.batch.delay`).
- **Single developer; no production deployments.** Bus factor 1; no
  operational history. Performance numbers come from one benchmark
  environment.
- **Parity evidence is per-fixture.** The 24 byte-equal fixtures (run
  in-process by `logstash_compat_test` and end-to-end by `runner.py`)
  cover ~17 filters and the stdin/stdout path against Logstash 9.4.2;
  they do not cover every implemented plugin, codec, or edge case. Each
  golden file is generated from the real Logstash oracle via
  `tests/logstash-compat/gen_expected.py`. See the compatibility matrix
  for the explicit scope.
- **Dotted JSON keys are auto-nested.** The `json` filter expands a key
  containing dots (e.g. `"app.name"`) into a nested object
  (`app: { name }`), whereas Logstash keeps it as a single literal field
  name. Consequently the `de_dot` filter — whose purpose is to flatten
  such keys — is a no-op on keys that arrived through `json`, since they
  are already nested by the time it runs. `de_dot` still works on
  genuinely flat dotted field names. Aligning the `json` filter's
  dotted-key handling with Logstash is tracked as future work.

## Documentation

| Area | Docs |
|---|---|
| Get started | [Quick start](#quick-start) · [onboarding / build](docs/onboarding.md) · [configuration](#configuration) |
| Reference | [architecture](docs/ARCHITECTURE.md) · [plugins](#plugins) · [Logstash compatibility scope](#logstash-compatibility-scope) · [compatibility matrix](docs/COMPATIBILITY.md) |
| Proof & trust | [Performance](#performance) · [parity harness](tests/logstash-compat/) · [benchmarks](bench/) · [honest limitations](#honest-limitations) |
| Project | [CHANGELOG](CHANGELOG.md) · [release notes](RELEASE_NOTES_1.0.0.md) · [security](docs/SECURITY.md) · [contributing](docs/CONTRIBUTING.md) |

## More from abyo software

FerroStash is part of a family of Rust infrastructure tools from **abyo
software**; several ship on AWS Marketplace under one seller account — browse the
catalog at **[abyo software on AWS Marketplace](https://aws.amazon.com/marketplace/seller-profile?id=seller-65lhisp4ppavm)**.

| Product | What it does |
|---|---|
| **FerroStash** | This project: a Logstash-compatible data pipeline in Rust. |
| **S4 — Squished S3** | Transparent GPU/CPU compression gateway in front of S3 — cut storage 50–80%. |
| **S4 Logs** | CloudWatch Logs → S3 archiver that cuts log-storage cost. |
| **S4 Scan** | Amazon Athena scan-cost reducer. |
| **S4 NAT** | Cost-optimized NAT for Amazon VPC. |
| **S4 MockAPI** | Security API simulator for testing and demos. |

## Contributing

Pull requests welcome — see [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for
setup, conventions, and the test/fuzz protocol. Contributions are licensed under
Apache-2.0 (no separate CLA).

## Security

Found a vulnerability? Please **do not open a public issue** — follow
[docs/SECURITY.md](docs/SECURITY.md) for coordinated disclosure.

## License

Apache-2.0 — see [LICENSE](LICENSE). Third-party license summary:
[LICENSES.md](LICENSES.md). The optional `ruby` feature pulls a fork of the
Artichoke (mruby) interpreter at build time (Apache-2.0/MIT; see its repo).
Changelog: [CHANGELOG.md](CHANGELOG.md); GA release notes:
[RELEASE_NOTES_1.0.0.md](RELEASE_NOTES_1.0.0.md).

`"FerroStash"` is an unregistered trademark of abyo software 合同会社.
`"Logstash"`, `"Elasticsearch"`, and `"Elastic"` are trademarks of Elasticsearch
B.V.; FerroStash is an independent reimplementation and is not affiliated with,
endorsed by, or sponsored by Elastic.

## Authors

- abyo software 合同会社 — sponsoring organization, commercial distribution
- masumi-ryugo — original author / maintainer
