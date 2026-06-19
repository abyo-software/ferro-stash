# FerroStash — Build & Development Guide

## Quick Start

```bash
# Build (default — light, no Ruby/Artichoke)
cargo build

# Test
cargo test

# Clippy (must be 0 warnings)
cargo clippy --all-targets

# Format check
cargo fmt --all -- --check

# License audit
cargo deny check

# Optional: build with the `ruby` filter (pulls Artichoke; needs clang/gcc + cmake)
cargo build -p ferro-stash-filter --features ruby
cargo build -p ferro-stash      --features ruby   # the CLI with ruby enabled
```

The default build operates on `default-members`, which excludes
`ferro-stash-ruby`, so it is fast and needs no extra toolchain. `cargo build
--workspace` additionally compiles `ferro-stash-ruby` (pulling Artichoke from
its git dependency).

## Prerequisites

- Rust 1.75+ (stable)
- `cmake` — required by the kafka plugins (rdkafka builds a vendored librdkafka via CMake). Connector TLS uses rustls, so no system OpenSSL is needed.
- C compiler (clang or gcc) — **only** needed for the optional `ruby` feature (Artichoke/mruby FFI). Not required for the default build.
- macOS or Linux
- Runtime (not build-time): the geoip filter needs a user-supplied `.mmdb` (GeoLite2/GeoIP2) database at the configured `database` path — not vendored.

## Project Structure

Plugin counts below are what is registered in the factory functions
(`create_input`/`create_filter`/`create_output`/`create_codec`), verified
against source. The ten connector plugins that were formerly stubs now
perform real external integrations (input/output kafka/redis/s3, output
datadog, filters geoip/dns/elasticsearch); their validation levels differ
(compile-only / mock / live) — see README "Honest limitations". Config
fields of note: geoip `database` (path to the `.mmdb`), s3 output
`endpoint` / `force_path_style` (MinIO/LocalStack/S3-compatible).

| Crate | Purpose |
|-------|---------|
| `ferro-stash-core` | Event model, pipeline engine, conditions, buffering, DLQ, metrics |
| `ferro-stash-config` | Logstash DSL + YAML config parsing |
| `ferro-stash-input` | Input plugins (15 registered: stdin, file, tcp, udp, http, syslog, generator, heartbeat, beats, elasticsearch, dead_letter_queue, pipeline + real kafka/redis/s3, compile-validated) |
| `ferro-stash-filter` | Filter plugins (29 registered: grok, mutate, json, date, dissect, kv, drop, clone, ruby, script, aggregate, throttle, … + real geoip/dns (live-validated), elasticsearch (mock-validated)) |
| `ferro-stash-output` | Output plugins (11 registered: stdout, elasticsearch, file, http, tcp, null, pipeline + real kafka/redis (compile-validated), s3/datadog (mock-validated)) |
| `ferro-stash-codec` | Codecs (21 registered: json, plain, multiline, csv, msgpack, cef, netflow, avro, protobuf, edn, …) |
| `ferro-stash-ruby` | Artichoke (mruby) Ruby interpreter bridge for the `ruby` filter |
| `ferro-script` | Native Painless-style scripting engine (Cranelift JIT) for the `script` filter/codec |
| `ferro-stash-cli` | `ferro-stash` CLI binary |
| `ferro-stash-e2e` | Integration / Logstash-parity test harness |

## Code Quality Requirements

- `clippy -D warnings`: 0 warnings (the `clippy::pedantic` *group* is set
  to `allow` workspace-wide under Rust 1.95; `unwrap_used` stays `deny`)
- `unsafe_code = "deny"` workspace-wide, with `allow` overrides in
  `ferro-stash-ruby` (mruby FFI) and `ferro-script` (Cranelift JIT FFI)
- Zero `unwrap()` in production code (`unwrap_used = "deny"`)
- SPDX-License-Identifier on every `.rs` file (119/119 at last check)
- GPL-family dependencies blocked via `deny.toml`

## Running

```bash
# With Logstash DSL config
cargo run -- -f config/pipeline.conf

# With YAML config
cargo run -- -f config/pipeline.yml
```

## Artichoke fork (Ruby interpreter)

The `ruby` filter depends on a **fork of Artichoke pulled as a rev-pinned git
dependency**. `crates/ferro-stash-ruby/Cargo.toml` references
<https://github.com/abyo-software/artichoke-extended> (branch `extended`) at a
fixed `rev`, so a fresh clone builds the ruby feature with no sibling checkout.

The `ruby` filter is **optional and off by default**: `ferro-stash-ruby` is not
a `default-member` and is an optional dependency of `ferro-stash-filter` behind
the `ruby` cargo feature. A plain `cargo build` therefore never touches
Artichoke; enable it with `--features ruby` (on `ferro-stash-filter` or the
`ferro-stash` CLI). A config that uses the `ruby` filter in a binary built
without the feature fails with a clear "rebuild with `--features ruby`" error.

Maintenance notes:

- To adopt fork updates, push to `abyo-software/artichoke-extended` and bump the
  `rev` in `crates/ferro-stash-ruby/Cargo.toml` (then `cargo update -p
  artichoke-backend`).
- The `ruby` feature build requires clang/gcc for the mruby C compilation; the
  default build does not.
- `ferro-stash-ruby` uses edition 2024 and raises its MSRV to 1.88.
