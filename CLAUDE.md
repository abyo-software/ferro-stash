# FerroStash — Build & Development Guide

## Quick Start

```bash
# Build
cargo build --workspace

# Test
cargo test --workspace

# Clippy (must be 0 warnings)
cargo clippy --workspace -- -D warnings

# Format check
cargo fmt --all -- --check

# License audit
cargo deny check
```

## Prerequisites

- Rust 1.75+ (stable)
- C compiler (clang or gcc) — required for Artichoke/mruby FFI in ferro-stash-ruby
- macOS or Linux

## Project Structure

Plugin counts below are what is registered in the factory functions
(`create_input`/`create_filter`/`create_output`/`create_codec`), verified
against source. Some registered plugins are production-shaped stubs — see
README "Honest limitations" (stubs: input/output kafka/redis/s3, output
datadog, filters geoip/dns/elasticsearch).

| Crate | Purpose |
|-------|---------|
| `ferro-stash-core` | Event model, pipeline engine, conditions, buffering, DLQ, metrics |
| `ferro-stash-config` | Logstash DSL + YAML config parsing |
| `ferro-stash-input` | Input plugins (15 registered: stdin, file, tcp, udp, http, syslog, generator, heartbeat, beats, elasticsearch, dead_letter_queue, pipeline + stubs kafka/redis/s3) |
| `ferro-stash-filter` | Filter plugins (29 registered: grok, mutate, json, date, dissect, kv, drop, clone, ruby, script, aggregate, throttle, … + stubs geoip/dns/elasticsearch) |
| `ferro-stash-output` | Output plugins (11 registered: stdout, elasticsearch, file, http, tcp, null, pipeline + stubs kafka/redis/s3/datadog) |
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

The `ruby` filter depends on a **local fork of Artichoke via a filesystem
path dependency** (not a published crate, not a git pin). The path in
`crates/ferro-stash-ruby/Cargo.toml` is
`../../../../artichoke-extended/artichoke-backend`, which resolves to a
sibling checkout named `artichoke-extended` placed next to this
repository's checkout. The fork lives at
<https://github.com/masumi-ryugo/artichoke-extended>; clone it alongside
this repo (see `.github/workflows/ci.yml` for the exact layout CI uses).

Maintenance notes:

- Because it is a path dependency, **no upstream branch/commit is pinned**
  in this repo's `Cargo.toml`/`Cargo.lock`; the build uses whatever is
  checked out at that path. A fresh clone of `ferro-stash` alone will not
  build — the fork must be present.
- Build requires clang/gcc for the mruby C compilation.
- `ferro-stash-ruby` uses edition 2024 and raises its MSRV to 1.88.
