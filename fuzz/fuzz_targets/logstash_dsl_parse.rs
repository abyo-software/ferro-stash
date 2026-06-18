// SPDX-License-Identifier: Apache-2.0
//! Fuzz the FerroStash configuration parser.
//!
//! `parse_config` accepts user-supplied pipeline configuration in two
//! formats:
//!  - **Logstash DSL** (`.conf`): a custom block-structured DSL with
//!    `input { ... } filter { ... } output { ... }` plus arbitrary
//!    `if [field] == "value" { ... }` conditionals. Hand-rolled
//!    recursive-descent parser in `logstash_dsl.rs` (~900 LOC).
//!  - **YAML** (`.yml`/`.yaml`): `serde_yaml`-driven, but the resulting
//!    typed model still goes through a custom validator that walks
//!    plugin name → settings shape.
//!
//! ROI: any FerroStash deployment loads at least one config file at
//! startup and on `SIGHUP` reload. The DSL parser specifically is the
//! most likely place to find:
//!  - **Stack overflow** from deeply nested `if` blocks (sibling
//!    repos: 2026-05-01 ferrosearch eql/fql `MAX_RECURSION_DEPTH`
//!    sweep showed 3 production stack-OOM bugs in similar
//!    recursive-descent parsers — `ferro-script` / `ferro-eql` /
//!    `ferro-fql`).
//!  - **Infinite loop** when the tokenizer fails to advance on an
//!    unterminated string / comment / nested block. The
//!    ferrosearch `ferro-esql` 1-byte `@` infinite-loop OOM
//!    (2026-05-01) was exactly this shape.
//!  - **Panic** in `expect("valid regex")` or other latent
//!    `unwrap`s exposed by adversarial DSL input.
//!  - **OOM** from environment variable expansion expanding to
//!    pathological lengths before parse, or from the parser
//!    pre-allocating buffers based on attacker-controlled length
//!    fields.
//!
//! Body layout: `[format_selector, body...]` — we sweep all three
//! `ConfigFormat` variants so the fuzzer exercises both the DSL and
//! YAML decoders plus the `Auto` fallback that runs YAML first then
//! falls through to DSL on error (a path that's interesting because
//! both decoders see the same bytes).
//!
//! Watch points:
//! - any panic / abort
//! - `OutOfMemory` from runaway `expand_env_vars` regex backtracking
//!   on adversarial `${VAR:default}` chains
//! - infinite loops on unterminated quoted strings / block comments
//! - stack overflow on deeply nested `if`/`else` conditionals

#![no_main]

use libfuzzer_sys::fuzz_target;

use ferro_stash_config::{ConfigFormat, parse_config};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // Both decoders ultimately operate on `&str`; non-UTF-8 bytes
    // would be rejected by the file-read path in production
    // (`fs::read_to_string` -> `io::Error`), so short-circuit the
    // same way to keep the fuzzer focused on the parser itself.
    let Ok(src) = std::str::from_utf8(&data[1..]) else {
        return;
    };

    let format = match data[0] % 3 {
        0 => ConfigFormat::LogstashDsl,
        1 => ConfigFormat::Yaml,
        _ => ConfigFormat::Auto,
    };

    let _ = parse_config(src, format);
});
