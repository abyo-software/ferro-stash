# ferro-stash 0.1.0

**Release date:** 2026-05-05
**Status:** General Availability — first stable tag.

> **Correction (2026-06-18 docs audit):** the plugin-surface and
> test-count figures in this frozen release note had drifted. The factory
> functions actually register **15 inputs / 29 filters / 11 outputs / 21
> codecs** (10 of which are stubs), and the measured workspace test count
> is **1,165 passing / 16 ignored**, not 1,151/3. The Artichoke fork is a
> filesystem **path** dependency (`artichoke-extended`), not a git pin on
> `integration/logstash-compat`. See [`README.md`](README.md)
> for the authoritative current numbers; the original wording below is kept
> for historical record.

`ferro-stash` is a Rust drop-in alternative to Logstash 5.0+ for
the production-common pipeline-configuration surface (Logstash DSL
or YAML, 8 inputs / 10 filters / 6 outputs / 6 codecs, JVM-free).
This is the first stable release. The 24h fuzz wave hardening
that landed as the project crossed v0.1.0 quality is promoted to
GA unchanged.

## Headline

| Metric                                 | Value |
| -------------------------------------- | --- |
| Plugin surface                         | 8 inputs / 10 filters / 6 outputs / 6 codecs |
| Configuration                          | Logstash DSL (`.conf`) + YAML, both first-class |
| Workspace tests                        | 1,151 passing / 3 ignored (23 suites, `cargo test --workspace`) |
| Clippy / fmt / deny                    | clean (`-D warnings`, fmt check, deny check) |
| Memory (steady state)                  | ~50–100 MB RSS vs Logstash 1–4 GB (JVM) |
| Startup                                | < 1 s (vs 15–30 s JVM warm-up) |
| Binary size                            | ~15 MB stripped (vs ~300 MB Logstash + JVM) |

## What changed

### Added

- **24h fuzz wave hardening (2026-05-02).** Three production
  DoS panics surfaced and fixed:
  - Avro OCF `block_size` signed-varint underflow → out-of-range
    slice panic in `ferro-stash-codec`.
  - EDN parser hot-loop DoS — depth cap + skip structural-tag
    stringify.
  - DSL config-side panics caught during the same wave.
- **Four cargo-fuzz harnesses** wired: `codec_decode`,
  `logstash_dsl_parse`, `netflow_decode`, plus the focused
  `cef_decode` target.
- **JRuby Logstash parity diff harness** under
  `tests/logstash-compat/` — real fixture-driven harness +
  Rust integration test against upstream Logstash 5+ behaviour.
- **5 regression seeds promoted** out of quarantine once their
  fixes landed; the known-crash directory is now empty.

### Changed

- `serde_yaml` → `serde_yaml_ng` (drop-in alias).
- CI moved from a self-hosted Mac runner to a self-hosted
  Linux KVM x86_64 runner. Cargo / rustup paths migrated.
- `clippy::pedantic` disabled workspace-wide under Rust 1.95
  (newer toolchain surfaced cosmetic lints on existing code).
  `unwrap_used = "deny"` retained.
- `unsafe_code` relaxed `forbid` → `deny` with `ferro-script`
  (cranelift JIT FFI) and `ferro-stash-ruby` (mruby FFI Send
  impl) opting in at the crate boundary.

### Fixed

- Quarantined known-crash regression seeds out of the corpus
  pre-fix, then promoted them back as positive regression
  tests post-fix.
- `cargo fmt` + selected `clippy` allow lists (`unreadable_literal`,
  `similar_names`) at well-scoped sites (Hinnant date algorithm,
  evaluator module).

## Install

Source-only for `0.1.0`:

```bash
# Build (requires clang for Artichoke / mruby FFI)
cargo build --release

# Run with a Logstash DSL config
./target/release/ferro-stash -f config/example.conf

# Or with YAML
./target/release/ferro-stash -f config/example.yml

# Inline pipeline
./target/release/ferro-stash -e 'input { stdin { } } output { stdout { } }'
```

## Breaking changes

None. The Logstash DSL surface, YAML schema, and metrics API
shipped here are stable.

## Known limitations

- **Plugin counts are scoped, not catalog-complete.** The 8 input
  / 10 filter / 6 output / 6 codec surface covers the
  production-common Logstash configuration. Logstash's full
  plugin catalog (200+ community plugins) is out of scope.
- **Logstash DSL parser** covers the production-common subset of
  `pipeline.conf`; less common DSL surface (obscure operators,
  exotic array indexing forms) is not yet exhaustively covered.
- **Ruby filter** is backed by an Artichoke fork pinned to the
  `integration/logstash-compat` branch. Upstream Artichoke
  parity is tracked separately and is not the load-bearing
  contract for this release.
- **Test count drift**: earlier portfolio surveys cited "2,900+
  workspace tests"; the measured count on this HEAD is 1,151
  passing / 3 ignored across 23 suites. The README and these
  release notes reflect the measured number, not the earlier
  estimate. Fuzz harnesses (4 targets) and Logstash-compat
  parity (`tests/logstash-compat/`) sit outside `cargo test
  --workspace`.

## Security

- `unsafe_code` denied workspace-wide. Two crates opt in
  (`ferro-script` for cranelift JIT FFI, `ferro-stash-ruby`
  for mruby FFI `Send` impl); both are documented at the
  crate boundary.
- `unwrap_used = "deny"` retained even after the
  `clippy::pedantic` group was disabled.
- TLS via `rustls`; `cargo deny check` clean.
- Three production DoS panics surfaced + fixed under the
  2026-05-02 24h fuzz wave (Avro OCF, EDN parser, DSL config).

## Acknowledgements

Built on `tokio`, `tokio-rustls`, `rustls`, `reqwest`, `axum`,
`hyper`, `clap`, `regex`, `chrono`, `dashmap`, `crossbeam-channel`,
the Artichoke `integration/logstash-compat` Ruby interpreter
fork, and the broader Rust crate ecosystem.
