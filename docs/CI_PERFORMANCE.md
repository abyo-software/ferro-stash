<!-- SPDX-License-Identifier: Apache-2.0 -->
# CI Performance & Strategy

This document explains the per-PR vs nightly testing structure for the
`ferro-stash` workspace and the rationale behind the job sizing. It
mirrors the org-wide strategy applied in
[`ferro-protocols` `c0f945d`](../../ferro-protocols/docs/CI_PERFORMANCE.md)
(Wave 13 capacity reduction).

## TL;DR

* PR CI on the self-hosted KVM runner pool: test / clippy / fmt /
  audit / deny. `cargo check` job dropped (clippy is a strict
  superset).
* Coverage: **push-to-main only**, not per PR.

## Per-PR jobs (`.github/workflows/ci.yml`)

| job   | purpose                                                 |
| ----- | ------------------------------------------------------- |
| test  | `cargo test --workspace`                                |
| clippy| `cargo clippy --workspace --all-targets`                |
| fmt   | per-package `cargo fmt -- --check` (artichoke vendored) |
| audit | `cargo audit --deny warnings`                           |
| deny  | `cargo deny check`                                      |

`cargo install cargo-audit` / `cargo install cargo-deny` / `cargo
install cargo-llvm-cov` were already replaced with
`taiki-e/install-action@v2` in earlier waves; nothing else to convert
here.

## Why coverage is not on PR

Coverage runs `cargo llvm-cov --workspace`, which re-instruments the
entire compile graph and is 2-5x slower than a plain `cargo test`.
Codecov reports coverage on **merged main**, not on PR-WIP commits,
so per-PR coverage runs produce numbers that no one acts on.
Push-to-main + manual dispatch is sufficient.

## Why no cross-OS matrix here

This repo doesn't currently have a cross-OS test matrix; Linux KVM is
the canonical CI target and gates every PR. If macOS / Windows
coverage is added later, follow the
[`ferro-protocols` cross-os.yml](../../ferro-protocols/.github/workflows/cross-os.yml)
weekly cron pattern.

## Capacity diagnosis (Wave 13 → Wave 14)

The 16h-queued nightly CI we observed was caused by **runner pool
saturation across 13 ferro-* org repos sharing one self-hosted KVM
host**, not by individual job runtime. The fix is at the **workflow
shape** layer (drop redundant `cargo check`, move coverage off PR),
not at the test-content layer. No fixtures were gated or skipped.
