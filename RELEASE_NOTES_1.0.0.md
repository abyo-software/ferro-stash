<!-- SPDX-License-Identifier: Apache-2.0 -->
# FerroStash 1.0.0

**First stable release** of FerroStash — a Logstash-compatible data pipeline
written in Rust. It parses the Logstash `pipeline.conf` DSL natively and runs
your existing pipelines **without a JVM**.

This is a contract on **API/behaviour stability** for the 1.x line (config DSL,
event model, CLI flags, and plugin set are frozen) — **not** a production track
record. It remains a single-developer project with **no public production
deployments yet**; run it beside your existing pipeline before trusting it with
irreplaceable data.

## Highlights

- **Logstash 9.4.2 parity, verified.** Output matches real Logstash 9.4.2 for
  expected event fields across **24 parity fixtures** (~17 filters), with
  runtime-only fields normalized. The in-process harness runs in CI; the Docker
  side-by-side harness covers a 13-fixture subset. Reproduce with
  `tests/logstash-compat/`.
- **A fraction of the footprint.** Benchmarked vs Logstash 9.4.2 on one host
  (`c7i.2xlarge`, 8 vCPU): native filters at **~1.4–1.7× throughput** (csv
  ~3.2×), **~8–13× lower RSS**, and **~700× faster cold start** (~10 ms vs ~7 s).
  Reproduce with `bench/run_bench.sh`. One-environment numbers, not a universal
  guarantee.
- **Custom logic: migrate, then go fast.** The embedded Artichoke (mruby) `ruby`
  filter runs your existing `ruby { }` unchanged (migration path; ~13× slower
  than JRuby). The native `script` filter (Painless subset) runs custom logic
  **~3.6× faster than Logstash's JRuby** — parsed once, no JVM.
- **At-least-once persistent queue.** `queue.type: persisted` acknowledges only
  after delivery (durable `ack_seq` cursor); popped-but-undelivered events replay
  on restart. Opt-in `fsync` adds power-loss durability.
- **Real connector integrations.** kafka / redis / s3 (in + out), datadog,
  geoip / dns / elasticsearch filters perform real I/O; live-validated against
  real services via `#[ignore]` smoke tests (validation level documented per
  plugin — see README "Honest limitations").
- **Optional, light by default.** The `ruby` filter is behind a cargo feature
  (off by default), so the common build is a fast ~14 MB binary with no native
  toolchain; Artichoke is a rev-pinned git dependency (no sibling checkout).

## Performance fixes in this release

- `script` (Painless) filter no longer re-parses the source per event (~2–4×).
- `ruby` (mruby) filter runs per-worker in parallel (was serialized through one
  interpreter).
- Filter dispatch is lock-free (MPMC) instead of a shared `Mutex<Receiver>`.

## Quality gates

`cargo test --workspace` (1,400+ tests, 0 failing), `cargo clippy -D warnings`,
`cargo fmt --check`, and `cargo deny check` all clean; the at-least-once PQ soak
shows zero loss under failure + restart.

See [CHANGELOG.md](CHANGELOG.md) for the full list of changes.
