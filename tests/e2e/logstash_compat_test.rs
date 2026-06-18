// SPDX-License-Identifier: Apache-2.0
//! Fixture-driven Logstash compatibility test runner.
//!
//! This test discovers every directory under `tests/logstash-compat/fixtures/`
//! and treats each as a single compatibility case:
//!
//!   * `pipeline.conf` — Logstash DSL pipeline (parsed by `ferro_stash_config::logstash_dsl`)
//!   * `input.txt`     — raw lines fed to the stdin input plugin; each line
//!     becomes one event with the line stored under `message`
//!   * `expected.json` — JSON array of expected output events
//!
//! It feeds the events through the configured filter chain (in-process, using
//! the same crates that the production CLI uses) and asserts that the output
//! matches the golden file — ignoring `@timestamp` and event ordering, exactly
//! the same comparison policy as `tests/e2e/compatibility_test.rs`.
//!
//! ## Wiring
//!
//! The companion crate `ferro-stash-e2e` already registers
//! `tests/e2e/compatibility_test.rs` as an integration test via
//! `[[test]]` in its `Cargo.toml`. To pick this file up too, add **one**
//! block to `crates/ferro-stash-e2e/Cargo.toml`:
//!
//! ```toml
//! [[test]]
//! name = "logstash_compat_test"
//! path = "../../tests/e2e/logstash_compat_test.rs"
//! ```
//!
//! The orchestrator can land that change in a follow-up commit; this file
//! is intentionally written to compile cleanly under the same dependency
//! set the existing test already uses (no new crates required).
//!
//! Until then this file is *evidence-only*: the fixtures it consumes are
//! the same ones the Python runner at `tests/logstash-compat/runner.py`
//! drives end-to-end against the real binary.

use std::path::{Path, PathBuf};

use ferro_stash_config::logstash_dsl;
use ferro_stash_core::event::Event;
use ferro_stash_filter::create_filter;
use serde_json::Value as JsonValue;

/// Root of the fixtures tree (workspace-relative).
fn fixtures_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points to crates/ferro-stash-e2e once wired up;
    // hop two levels to reach the workspace root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/logstash-compat/fixtures")
}

/// Discovers every directory directly under `fixtures/`.
fn discover_fixtures() -> Vec<PathBuf> {
    let root = fixtures_root();
    let mut out = Vec::new();
    let entries = std::fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("cannot read fixtures dir {}: {}", root.display(), e));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.push(path);
        }
    }
    out.sort();
    out
}

fn read_text(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read {}: {}", path.display(), e))
}

/// Loads input events from a fixture directory.
///
/// Prefers `input.txt` (raw lines, mirrors how the stdin input plugin
/// constructs events at runtime). Falls back to `input.json` (NDJSON,
/// each line a JSON object representing pre-built event fields) if that
/// is the only file present, so legacy fixtures keep working.
fn load_input_events(fixture_dir: &Path) -> Vec<Event> {
    let txt = fixture_dir.join("input.txt");
    if txt.exists() {
        let content = read_text(&txt);
        return content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(Event::new)
            .collect();
    }
    let json_path = fixture_dir.join("input.json");
    let content = read_text(&json_path);
    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let json: JsonValue = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("invalid JSON in {}: {}", json_path.display(), e));
            Event::from_json(json)
        })
        .collect()
}

fn load_expected(path: &Path) -> Vec<JsonValue> {
    let content = read_text(path);
    let parsed: JsonValue = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("invalid JSON in {}: {}", path.display(), e));
    parsed
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array in {}", path.display()))
        .clone()
}

async fn run_filters(config: &ferro_stash_config::Config, mut events: Vec<Event>) -> Vec<Event> {
    let mut filters: Vec<Box<dyn ferro_stash_core::plugin::FilterPlugin>> = Vec::new();
    for fc in &config.filters {
        let filter = create_filter(&fc.plugin_type, &fc.settings, fc.condition.clone())
            .unwrap_or_else(|e| panic!("cannot create filter {}: {}", fc.plugin_type, e));
        filters.push(filter);
    }

    for filter in &filters {
        let mut next = Vec::new();
        for ev in events {
            if ev.is_cancelled() {
                continue;
            }
            if let Some(cond) = filter.condition() {
                if !cond.evaluate(&ev) {
                    next.push(ev);
                    continue;
                }
            }
            match filter.filter(ev).await {
                Ok(filtered) => {
                    for fe in filtered {
                        if !fe.is_cancelled() {
                            next.push(fe);
                        }
                    }
                }
                Err(e) => panic!("filter {} error: {}", filter.name(), e),
            }
        }
        events = next;
    }
    events
}

/// Order-insensitive comparison: every expected event must match some unused
/// actual event on every expected field. `@timestamp`, `@version`, and `host`
/// are ignored (they are environment-dependent).
fn assert_match(actual: &[Event], expected: &[JsonValue], ctx: &str) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "[{ctx}] event count mismatch: got {} expected {}",
        actual.len(),
        expected.len()
    );

    let actual_json: Vec<JsonValue> = actual.iter().map(Event::to_json).collect();
    let mut used = vec![false; actual_json.len()];

    for (i, exp) in expected.iter().enumerate() {
        let exp_obj = exp
            .as_object()
            .unwrap_or_else(|| panic!("[{ctx}] expected[{i}] is not an object"));

        let mut found = None;
        'scan: for (j, act) in actual_json.iter().enumerate() {
            if used[j] {
                continue;
            }
            let act_obj = match act.as_object() {
                Some(o) => o,
                None => continue,
            };
            for (k, exp_v) in exp_obj {
                if k == "@timestamp" || k == "@version" || k == "host" {
                    continue;
                }
                match act_obj.get(k) {
                    Some(v) if v == exp_v => {}
                    _ => continue 'scan,
                }
            }
            found = Some(j);
            break;
        }

        match found {
            Some(j) => used[j] = true,
            None => panic!(
                "[{ctx}] no actual event matched expected[{i}]:\n  expected: {}\n  actuals: {}",
                serde_json::to_string_pretty(exp).unwrap_or_default(),
                serde_json::to_string_pretty(&actual_json).unwrap_or_default(),
            ),
        }
    }
}

#[tokio::test]
async fn run_all_logstash_compat_fixtures() {
    let fixtures = discover_fixtures();
    assert!(
        !fixtures.is_empty(),
        "no fixtures discovered at {}",
        fixtures_root().display()
    );

    let mut failures: Vec<(String, String)> = Vec::new();

    for fixture in &fixtures {
        let name = fixture
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("<unnamed>")
            .to_string();

        let pipeline_path = fixture.join("pipeline.conf");
        let expected_path = fixture.join("expected.json");
        let has_input = fixture.join("input.txt").exists() || fixture.join("input.json").exists();

        if !pipeline_path.exists() {
            failures.push((
                name,
                format!("missing pipeline.conf in {}", fixture.display()),
            ));
            continue;
        }
        if !expected_path.exists() {
            failures.push((
                name,
                format!("missing expected.json in {}", fixture.display()),
            ));
            continue;
        }
        if !has_input {
            failures.push((
                name,
                format!("missing input.txt or input.json in {}", fixture.display()),
            ));
            continue;
        }

        let pipeline_text = read_text(&pipeline_path);
        let config = match logstash_dsl::parse(&pipeline_text) {
            Ok(c) => c,
            Err(e) => {
                failures.push((name.clone(), format!("parse error: {e}")));
                continue;
            }
        };

        let inputs = load_input_events(fixture);
        let expected = load_expected(&expected_path);

        let actual = run_filters(&config, inputs).await;

        // Use std::panic::catch_unwind so one failed fixture doesn't mask
        // every later one. We collect failures and assert at the end.
        let actual_clone: Vec<Event> = actual.to_vec();
        let expected_clone = expected.clone();
        let name_for_assert = name.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            assert_match(&actual_clone, &expected_clone, &name_for_assert);
        }));

        if let Err(payload) = result {
            let msg = if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else {
                "panic with unknown payload".to_string()
            };
            failures.push((name, msg));
        }
    }

    if !failures.is_empty() {
        let mut report = String::from("Logstash compatibility fixtures failed:\n");
        for (name, msg) in &failures {
            report.push_str(&format!("  - {name}: {msg}\n"));
        }
        panic!("{report}");
    }
}
