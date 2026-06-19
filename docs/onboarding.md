# Ferro-Stash Onboarding Guide

## Prerequisites

- **Rust stable** (`rust-version = 1.75` workspace-wide; `ferro-stash-ruby`
  raises its MSRV to 1.88 and uses edition 2024) — install via [rustup](https://rustup.rs/)
- **cmake** — required for the vendored librdkafka (kafka plugins)
- **pkg-config** (macOS: `brew install pkg-config`)
- **Git**
- **C/C++ compiler** (clang recommended) — **only** for the optional `ruby`
  feature (Artichoke/mruby FFI). The default build does not need it. Artichoke
  is a rev-pinned git dependency (`abyo-software/artichoke-extended`), so a
  fresh clone builds `--features ruby` with no sibling checkout or submodule.

On macOS:

```bash
xcode-select --install   # provides clang
```

On Linux (Debian/Ubuntu):

```bash
apt install build-essential clang pkg-config
```

## Clone and Build

```bash
git clone git@github.com:abyo-software/ferro-stash.git
cd ferro-stash
cargo build
```

The default build is light (no Artichoke). Building `--features ruby` takes
longer the first time due to artichoke-backend (mruby) compilation.

Release build:

```bash
cargo build --release
```

## Running Tests

```bash
cargo test --workspace              # ~1,165 tests (16 ignored)
cargo test -p ferro-stash-core      # single crate
```

## Project Structure

The workspace contains 10 members:

| Crate | Purpose |
|-------|---------|
| `ferro-stash-cli` | `ferro-stash` binary: CLI (clap), config loading, signal handling, metrics API |
| `ferro-stash-core` | Pipeline engine, event model, plugin traits, conditions, buffering, DLQ, metrics |
| `ferro-stash-config` | Logstash-compatible config file parsing and validation |
| `ferro-stash-codec` | Codecs (json, plain, multiline, msgpack, cef, netflow, avro, …) |
| `ferro-stash-input` | Input plugins (stdin, file, beats, http, tcp, udp, syslog, … + kafka/redis/s3 stubs) |
| `ferro-stash-output` | Output plugins (stdout, elasticsearch, file, http, tcp, null, … + kafka/redis/s3/datadog stubs) |
| `ferro-stash-filter` | Filter plugins (grok, mutate, date, dissect, kv, … + geoip/dns/elasticsearch stubs) |
| `ferro-stash-ruby` | Ruby filter bridge via artichoke-backend (local fork, path dep) |
| `ferro-script` | Native Painless-style scripting engine (Cranelift JIT) for the `script` filter/codec |
| `ferro-stash-e2e` | Integration / Logstash-parity test harness |

## Adding a New Filter Plugin

1. Create `crates/ferro-stash-filter/src/your_filter.rs`
2. Implement the `Filter` trait from `ferro-stash-core`:
   ```rust
   pub struct YourFilter { /* config fields */ }

   impl Filter for YourFilter {
       fn filter(&self, event: &mut Event) -> FilterResult { ... }
   }
   ```
3. Add config deserialization (serde) for your filter's parameters
4. Register the plugin in `crates/ferro-stash-filter/src/lib.rs`
5. Add tests in the same file or under `crates/ferro-stash-filter/tests/`
6. Add SPDX license header: `// SPDX-License-Identifier: Apache-2.0`

## Adding a New Input/Output Plugin

Same pattern as filters, but:

- Inputs go in `crates/ferro-stash-input/src/` and implement the `Input` trait
- Outputs go in `crates/ferro-stash-output/src/` and implement the `Output` trait
- Input plugins are typically async (tokio) and produce events into the pipeline
- Output plugins consume events and send them to external systems

## Debugging Tips

Enable tracing output with `RUST_LOG`:

```bash
RUST_LOG=debug cargo run -- -f config/example.conf
RUST_LOG=ferro_stash_core=trace cargo run -- -f config/example.conf
```

For specific crate debugging:

```bash
RUST_LOG=ferro_stash_filter::grok=trace cargo run -- -f config/example.conf
```

## Common Issues

| Issue | Fix |
|-------|-----|
| artichoke build fails | Install clang: `xcode-select --install` (macOS) or `apt install clang` (Linux) |
| linker errors on mruby | Ensure C compiler is in PATH, check `cc --version` |
| slow first build | Normal -- artichoke/mruby compiles C code. Subsequent builds are incremental. |
| OpenSSL errors | ferro-stash uses rustls (no OpenSSL dependency). If you see OpenSSL errors, check for system-level conflicts. |
