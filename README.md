# FerroStash

**A Logstash-compatible data pipeline written in Rust.**

FerroStash ingests, transforms, and routes events through the same
three-stage `input → filter → output` model as
[Logstash](https://www.elastic.co/logstash), parsing the Logstash
configuration language (`pipeline.conf`) natively so existing pipelines
can run without a JVM. The Ruby filter is supported through an embedded
[Artichoke](https://www.artichokeruby.org/) (mruby-based) Ruby
interpreter, and an alternative native scripting filter (Painless-style,
Cranelift-JIT-backed) is provided for high-throughput custom logic.

## Status

**v0.1.0 — first stable tag (`v0.1.0`, 2026-05-05; latest commit
2026-05-24).** Single-developer project; not yet deployed in production.
The current `cargo test --workspace` run measures **1,165 tests passing /
16 ignored / 0 failing** across 15 binary test targets, with `cargo
clippy -D warnings`, `cargo fmt --check`, and `cargo deny check` clean.

Several plugins are **production-shaped stubs** (the trait, config
parsing, and lifecycle are wired, but the external integration is not
implemented). These are called out explicitly in the tables below — do
not assume a plugin is functional just because it is registered. See
[Honest limitations](#honest-limitations).

## Why FerroStash

| Property | Logstash (JVM) | FerroStash (native) |
|----------|----------------|---------------------|
| Runtime | JVM (Java) + JRuby | Native Rust binary |
| Idle memory (RSS) | ~0.5–1 GB+ | ~10–50 MB |
| Cold start | ~8–30 s (JVM warm-up) | < 1 s |
| Install / binary size | ~300–400 MB (+JVM) | ~12–15 MB stripped |
| GC pauses | Yes (G1GC) | None (deterministic) |

The memory/startup/binary-size figures above are measured on a single
benchmark host; treat all comparative throughput numbers as evidence
from one environment, not a universal guarantee.

## Logstash compatibility scope

FerroStash targets the **production-common subset** of Logstash, not its
full plugin catalogue (200+ community plugins are out of scope). The
default event shape (`@timestamp`, tags, bracket-notation field
references `[a][b]`, `%{field}` interpolation) and the `.conf` DSL follow
Logstash semantics; the docker-driven regression harness asserts
field-by-field equality against **Logstash 8.15.3** for a curated fixture
set (see below). Benchmark comparisons were run against
**Logstash 9.3.3**. There is no single pinned "target Logstash version" —
the config language and event model are broadly stable across Logstash
5.x–9.x, and compatibility is asserted per-fixture rather than claimed
wholesale.

### Verified parity evidence

`tests/e2e/logstash_docker_compat_test.rs` pipes the same `pipeline.conf`
+ input through both `target/debug/ferro-stash` and
`docker.elastic.co/logstash/logstash:8.15.3`, then asserts every event
payload equal field-by-field after stripping only runtime-only fields
(`@timestamp`, `@version`, `host`, `event.original`). **13/13 fixtures
pass byte-equal**, covering `stdin → stdout(json)`, grok, mutate, json,
kv, dissect, fingerprint, conditional `if/else if/else`, and unicode
inputs. These tests are `#[ignore]` (they require Docker); run them with:

```bash
cargo build --bin ferro-stash
docker pull docker.elastic.co/logstash/logstash:8.15.3
cargo test -p ferro-stash-e2e --test logstash_docker_compat_test \
    -- --ignored --nocapture --test-threads=1
```

The docker-driven regression harness under
[`tests/logstash-compat/`](tests/logstash-compat/) is the authoritative,
runnable record of what this evidence does and does not substantiate.

## Plugins

Counts below reflect what is **registered in the plugin factories**
(`create_input` / `create_filter` / `create_output` / `create_codec`),
verified against source. "Stub" means the plugin loads and runs but does
not perform the real external integration.

### Input plugins (15 registered)

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
| `kafka` | **stub** | config + consumer-loop skeleton only; no broker connection |
| `redis` | **stub** | config + loop skeleton only; no Redis connection |
| `s3` | **stub** | config + polling skeleton only; no AWS SDK / S3 calls |

### Filter plugins (29 registered)

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
| `script` / `painless` | functional | native Painless-style DSL, Cranelift JIT (`ferro-script`) |
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
| `geoip` | **stub** | private/public/loopback IP classification only; no MaxMind GeoLite2 database |
| `dns` | **stub** | no actual resolution; tags events to indicate stub behaviour |
| `elasticsearch` | **stub** | echoes the event's own data; no ES query is issued |

### Output plugins (11 registered)

| Plugin | Status | Notes |
|--------|--------|-------|
| `stdout` | functional | json, rubydebug, line, dots |
| `elasticsearch` (aliases `ferrosearch`, `opensearch`) | functional | Bulk `_bulk` API via reqwest |
| `file` | functional | JSON lines or custom format |
| `http` | functional | POST/PUT/PATCH |
| `tcp` | functional | TLS via rustls |
| `null` | functional | discard (benchmarking) |
| `pipeline` | functional | pipeline-to-pipeline (multi-pipeline mode) |
| `kafka` | **stub** | config only; no broker connection |
| `redis` | **stub** | config only; no Redis connection |
| `s3` | **stub** | config only; no AWS SDK / S3 calls |
| `datadog` | **stub** | config only; no Datadog API calls |

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
# Build (requires a C compiler for the Artichoke/mruby FFI — see Prerequisites)
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
(roughly 3–7× slower in our benchmarks). The Ruby filter exists for
**migration compatibility**, not throughput. For custom logic that needs
to be fast, prefer the native `script` (Painless-style) filter, which is
JIT-compiled and considerably faster than either Ruby engine.

**Fork dependency (important maintenance note):**
`ferro-stash-ruby` does not depend on a published Artichoke crate. It
uses a **filesystem path dependency** to a local fork checked out
alongside this repository:

```toml
# crates/ferro-stash-ruby/Cargo.toml
artichoke-backend = { path = "../../../../artichoke-extended/artichoke-backend", ... }
artichoke-core    = { path = "../../../../artichoke-extended/artichoke-core" }
```

Relative to the crate, that path resolves to a sibling checkout named
`artichoke-extended` (e.g. `/home/y1/git/artichoke-extended`). Because it
is a **path** dependency, not a git dependency, no upstream commit or
branch is pinned inside this repository's `Cargo.toml` or `Cargo.lock` —
the build simply uses whatever is currently checked out at that path.
Upstream Artichoke is a low-activity project; the fork carries local
patches needed for Logstash Ruby-filter compatibility.

The maintenance implications are real and should not be glossed over:

- **The build will not work from a fresh clone alone** — the
  `artichoke-extended` fork must be present at the expected relative
  path. There is no vendored copy or git submodule in this repo.
- **No reproducible pin.** Whichever branch/commit the fork happens to
  be on is what gets compiled. (At time of writing the fork was on a
  branch named `extended`; some docs in this repo refer to an
  `integration/logstash-compat` branch — treat the on-disk checkout as
  the source of truth, not the prose.)
- **Bus-factor and upstream risk.** The Ruby filter's long-term
  viability is tied to maintaining this fork. It is deliberately isolated
  in its own crate so the rest of the pipeline is unaffected if Ruby
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
| `ferro-script` | Native Painless-style scripting engine with a Cranelift JIT backend (powers the `script` filter/codec) |
| `ferro-stash-cli` | `ferro-stash` binary: CLI, signal handling, metrics API |
| `ferro-stash-e2e` | Integration / Logstash-parity test harness (no library code) |

More detail: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Build, test, run

```bash
cargo build --workspace
cargo test  --workspace          # 1,165 pass / 16 ignored on the current HEAD
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
cargo deny check
```

### Prerequisites

- Rust stable (`rust-version = 1.75` workspace-wide; `ferro-stash-ruby`
  raises its own MSRV to 1.88 and uses edition 2024).
- A C compiler (clang or gcc) — required to compile the Artichoke/mruby
  FFI in `ferro-stash-ruby`.
- The `artichoke-extended` fork checked out at the path described above.

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

- **Stub plugins.** `kafka`, `redis`, `s3` (input and output), `datadog`
  (output), and the `geoip`, `dns`, `elasticsearch` filters are
  production-shaped stubs — they parse config and run but do not perform
  the real external integration. Do not deploy them expecting a live
  connection. (Real implementations require the respective client SDKs,
  which are not currently dependencies.)
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
- **Single developer; no production deployments.** Bus factor 1; no
  operational history. Performance numbers come from one benchmark
  environment.
- **Parity evidence is per-fixture.** The 13 docker fixtures that pass
  byte-equal cover ~6 filters and the stdin/stdout path against Logstash
  8.15.3; they do not cover every implemented plugin, codec, or edge
  case. See the compatibility matrix for the explicit scope.

## License

Apache-2.0. See [LICENSE](LICENSE). Third-party license summary:
[LICENSES.md](LICENSES.md). Changelog: [CHANGELOG.md](CHANGELOG.md);
GA release notes: [RELEASE_NOTES_0.1.0.md](RELEASE_NOTES_0.1.0.md).
