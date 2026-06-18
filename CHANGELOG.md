# Changelog

All notable changes to `ferro-stash` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once `v1.0.0`
is cut. Pre-1.0 releases may introduce breaking changes between minor tags.

## [Unreleased]

### Documentation

- **Docs audit (2026-06-18).** README and the docs/ pack were rewritten
  to match source. The previously-documented "8 input / 10 filter / 6
  output / 6 codec" surface was an understatement: the factory functions
  actually register **15 inputs / 29 filters / 11 outputs / 21 codecs**.
  Of those, **10 are production-shaped stubs** (input/output
  kafka/redis/s3, output datadog, filters geoip/dns/elasticsearch) — now
  labelled as such everywhere. Corrected: test count (measured **1,165
  passing / 16 ignored**, not 670/1,151), workspace member count (10),
  `unsafe_code` posture (`deny` + 2 overrides, not `forbid`), the
  non-existent `rdkafka`/`maxminddb`/`aws-sdk` dependency claims, the
  `120+` grok-pattern claim (actually ~50), and the Artichoke fork
  description (a filesystem **path** dependency to `artichoke-extended`,
  not a git pin on `integration/logstash-compat`, and not `ferro-artichoke`).

## [0.1.0] — 2026-05-05

First stable release. The Logstash 5.0+ wire-compatible surface
(8 input / 10 filter / 6 output plugins, 6 codecs, Logstash DSL
parser covering the production-common subset of `pipeline.conf`)
is promoted to GA.

### Added

- **24h fuzz wave hardening (2026-05-02).** Three production DoS
  panics surfaced and fixed under `fix(codec,config)`:
  - **Avro OCF block_size signed-varint underflow** → out-of-range
    slice panic in `ferro-stash-codec`. Cap + bounds check landed.
  - **EDN parser hot-loop DoS** — depth cap + skip
    structural-tag stringify (`6934553`).
  - Plus DSL config-side panics caught during the same fuzz wave
    (`d1103a1`).
- **Three cargo-fuzz harnesses** wired (`ae4a75a`):
  - `codec_decode` — cross-codec entry fuzzer.
  - `logstash_dsl_parse` — Logstash DSL grammar.
  - `netflow_decode` — netflow v5 / v9 / IPFIX decoders.
  - Plus a fourth focused `cef_decode` target (`162338d`).
- **JRuby Logstash parity diff harness** (`b0cfbb6`,
  `cbdb144`) — real fixture-driven harness + Rust integration
  test against upstream Logstash 5+ behaviour.
- **5 regression seeds promoted** out of quarantine
  (`35a12cd`) once the matching fixes landed; the
  known-crash directory is now empty.

### Changed

- **`serde_yaml` → `serde_yaml_ng`** (drop-in alias). Upstream
  `serde_yaml` is unmaintained; the workspace uses the
  maintained `serde_yaml_ng` crate via a renamed dependency.
  No source-level changes.
- **CI runner switched** to a self-hosted Linux KVM x86_64
  runner; the GitHub-hosted Mac path was retired for slow
  throughput. Cargo / rustup paths migrated; rustup proxy
  resolved via PATH instead of hardcoded toolchain env.
- **clippy::pedantic disabled workspace-wide** under Rust 1.95
  (`15b2340`, `eca9f0b`). The newer toolchain surfaced dozens
  of cosmetic pedantic lints on existing code that the
  Mac-runner-cached older toolchain treated as silent; the
  group is disabled rather than chasing each call site.
  `unwrap_used` remains `deny`.
- **`unsafe_code` relaxed from `forbid` to `deny`** with
  `ferro-script` (cranelift JIT FFI) and `ferro-stash-ruby`
  (mruby FFI Send impl) opting in via inlined lints
  (`777f9da`, `4b69f3f`, `68fa002`). Both opt-ins are
  documented at the crate boundary.

### Fixed

- **Quarantined known-crash regression seeds** out of the corpus
  pre-fix (`de25f33`), then promoted them back as positive
  regression tests post-fix.
- **`cargo fmt` + `clippy::unreadable_literal`** allow on the
  Hinnant date algorithm (`889802e`); `unreadable_literal +
  similar_names` allow at the evaluator module (`408da93`,
  `934e2d6`); `iter().cloned().collect() -> to_vec()` and doc
  list indent (`68ee7d6`).

### Known limitations

> Plugin-count figures in this historical 0.1.0 entry were corrected by
> the 2026-06-18 docs audit (see Unreleased). The real registered surface
> is 15 input / 29 filter / 11 output / 21 codec, with 10 stubs. The
> bullets below are retained for historical record.

- **Plugin counts are scoped, not catalog-complete.** The registered
  surface covers the production-common Logstash configuration; Logstash's
  full plugin catalog (200+ community plugins) is out of scope. Several
  registered plugins are stubs (see [`README.md`](README.md) "Honest
  limitations").
- **Logstash DSL parser** covers the production-common subset of
  `pipeline.conf` syntax (input / filter / output blocks,
  conditionals, `%{field}` interpolation, regex match). Less
  common DSL surface (e.g. obscure operators, the more exotic
  array indexing forms) is not yet exhaustively covered.
- **Ruby filter** is backed by a local Artichoke fork consumed as a
  filesystem **path** dependency (`artichoke-extended`), not a git pin.
  Upstream Artichoke parity is tracked separately and is not the
  load-bearing contract for this release.

[Unreleased]: https://github.com/abyo-software/ferro-stash/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/abyo-software/ferro-stash/releases/tag/v0.1.0
