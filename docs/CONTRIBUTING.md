# Contributing to Ferro-Stash

## Development Setup

### Prerequisites

- Rust 1.75 or later for the default build; Rust 1.88+ for `cargo build --workspace` or `--features ruby`
- cargo, clippy, rustfmt (included with rustup)

### Build

```bash
cargo build
cargo build --workspace  # also builds the optional Ruby crate; needs Rust 1.88+
```

### Run

```bash
cargo run -- -f config/example.conf
```

## Code Quality Requirements

All code must pass these checks before merge:

```bash
# Lint -- zero warnings required
cargo clippy --workspace --all-targets -- -D warnings

# Format -- must match rustfmt output exactly
cargo fmt --all -- --check

# Deny -- license and vulnerability audit
cargo deny check
```

### SPDX Headers

Every `.rs` file must include the SPDX license header as the first line:

```rust
// SPDX-License-Identifier: Apache-2.0
```

### Safety Rules

- `unsafe` code is `deny` workspace-wide (`unsafe_code = "deny"`). Two crates opt in: `ferro-stash-ruby` (Artichoke/mruby FFI) and `ferro-script` (Cranelift JIT FFI).
- Do not use `.unwrap()` on fallible operations in production code. Use `?`, `.ok()`, or explicit error handling.
- Do not introduce GPL-licensed dependencies. Run `cargo deny check` to verify.

## Testing

```bash
# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p ferro-stash-filter

# Run a specific test
cargo test -p ferro-stash-filter grok::tests::test_common_apache_log
```

The test suite must pass before submitting a PR. The default workspace has 1,400+ passing tests; external-service smoke tests are ignored unless explicitly enabled.

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Make your changes. Keep commits focused -- one logical change per commit.
3. Ensure `cargo clippy`, `cargo fmt --check`, `cargo test --workspace`, and `cargo deny check` all pass.
4. Open a pull request against `main` with a clear description of what and why.
5. Address review feedback. Force-push to the feature branch is acceptable during review.
6. A maintainer will merge once approved and CI passes.

## Plugin Development Guide

### Adding a New Filter Plugin

1. Create a new module in `ferro-stash-filter/src/`.
2. Implement the `FilterPlugin` trait:

```rust
use ferro_stash_core::{Event, FilterPlugin, FilterResult};

pub struct MyFilter {
    // plugin configuration fields
}

impl FilterPlugin for MyFilter {
    fn filter(&self, event: &mut Event) -> FilterResult {
        // Transform the event in place
        // Return FilterResult::Ok to continue, FilterResult::Drop to discard
        FilterResult::Ok
    }

    fn plugin_name(&self) -> &'static str {
        "my_filter"
    }
}
```

3. Register the plugin in the filter crate's plugin registry.
4. Add configuration parsing in `ferro-stash-config`.
5. Write tests covering: normal operation, edge cases, error paths, and interaction with event tags/metadata.

### Adding a New Input or Output Plugin

The pattern is identical: implement `InputPlugin` or `OutputPlugin` from `ferro-stash-core`, register in the appropriate crate, and add config support. Input and output plugins are async (`#[async_trait]`).

## Commit Message Conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(filter): add csv filter plugin
fix(input/kafka): handle broker disconnect during rebalance
test(output/elasticsearch): add bulk retry integration tests
refactor(core): simplify event field path resolution
docs: update plugin development guide
chore: bump dependencies
```

The scope in parentheses should identify the crate or plugin being changed. Keep the subject line under 72 characters.
