<!-- SPDX-License-Identifier: Apache-2.0 -->
## What & why

Briefly describe the change and the motivation.

## Checklist

- [ ] `cargo fmt --all -- --check` clean
- [ ] `cargo clippy --all-targets` clean (0 warnings)
- [ ] `cargo test` passes (and `--features ruby` if the ruby filter is touched)
- [ ] New behavior has tests; Logstash-parity changes update the
      `tests/logstash-compat/` fixtures or `docs/COMPATIBILITY.md`
- [ ] No new exaggerated claims; honest limitations updated if scope changed
- [ ] SPDX header on new `.rs` files

## Notes

Anything reviewers should know (compat nuances, follow-ups, residuals).
