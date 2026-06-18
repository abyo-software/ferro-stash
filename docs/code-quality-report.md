# Ferro-Stash Code Quality Report

Generated: 2026-04-15. Counts re-verified against source: 2026-06-18.

## Static Analysis

| Check | Status (2026-06-18) |
|-------|--------|
| `cargo clippy --workspace -- -D warnings` | 0 warnings (the `clippy::pedantic` group is set to `allow` under Rust 1.95; `unwrap_used` kept `deny`) |
| `cargo fmt --check` | Clean |
| `unsafe_code` | `deny` workspace-wide, `allow` override in 2 crates (see below) |
| `unwrap()` in production code | enforced by `unwrap_used = "deny"` |
| SPDX-License-Identifier headers | 119/119 `.rs` files (100%) |

### unsafe_code Exception

`unsafe_code` is `deny` workspace-wide (relaxed from `forbid` so it can be
overridden per crate). Two crates opt in: `ferro-stash-ruby` (FFI interop
with the Artichoke/mruby C runtime) and `ferro-script` (Cranelift JIT FFI).
All unsafe usage is confined to those two boundaries.

## Dependency Policy

Enforced via `deny.toml`:

- **GPL dependencies blocked** -- build fails if any GPL-licensed crate is introduced
- **Advisory database checked** -- known vulnerabilities cause build failure
- **All current dependencies use permissive licenses**: Apache-2.0, MIT, BSD, ISC

Run the check:

```bash
cargo deny check
```

## Test Coverage

| Metric | Value (measured 2026-06-18) |
|--------|-------|
| Total tests | 1,165 passing / 16 ignored / 0 failing |
| Workspace members | 10 (8 `ferro-stash-*` + `ferro-script` + `ferro-stash-e2e`) |
| Integration / parity suites | `tests/e2e/` + `tests/logstash-compat/` (docker fixtures are `#[ignore]`) |

Run all tests:

```bash
cargo test --workspace
```

## Key Dependencies

All permissively licensed:

| Dependency | Version | License |
|------------|---------|---------|
| tokio | 1 | MIT |
| serde / serde_json | 1 | Apache-2.0 OR MIT |
| reqwest | 0.12 | Apache-2.0 OR MIT |
| hyper | 1 | MIT |
| axum | 0.7 | MIT |
| tracing | 0.1 | MIT |
| clap | 4 | Apache-2.0 OR MIT |
| regex | 1 | Apache-2.0 OR MIT |
| chrono | 0.4 | Apache-2.0 OR MIT |
| rustls | 0.23 | Apache-2.0 OR ISC OR MIT |
| flate2 | 1 | Apache-2.0 OR MIT |
| uuid | 1 | Apache-2.0 OR MIT |
| dashmap | 6 | MIT |
| crossbeam-channel | 0.5 | Apache-2.0 OR MIT |
| artichoke-backend | path (forked) | MIT |
