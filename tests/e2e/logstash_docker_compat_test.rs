// SPDX-License-Identifier: Apache-2.0
//! Side-by-side regression harness — ferro-stash vs Logstash 8.x reference.
//!
//! Each fixture under `tests/logstash-compat/fixtures/<name>/` carries a
//! `pipeline.conf`, an `input.txt`, and a hand-curated `expected.json`.
//! For the docker compat test we additionally drive **the same pipeline
//! and the same input** through the upstream Logstash 8.x docker image
//! and compare the two output sets event-by-event.
//!
//! The point is not byte-equality of `@timestamp`, `host`, or `@version`
//! — those are inherently runtime-dependent. The point is field-by-field
//! parity of every other key produced by the filter chain. Divergence is
//! a real implementation gap (or a deliberate, documented one).
//!
//! ## Status
//!
//! All tests are `#[ignore]` because they require:
//!   1. `docker` available + the Logstash 8.x image pulled
//!      (`docker pull docker.elastic.co/logstash/logstash:9.4.2`)
//!   2. The `ferro-stash` binary built (`cargo build --bin ferro-stash`)
//!
//! Running:
//!
//! ```bash
//! cargo build --bin ferro-stash
//! docker pull docker.elastic.co/logstash/logstash:9.4.2
//! cargo test --workspace --test logstash_docker_compat_test \
//!     -- --ignored --nocapture
//! ```
//!
//! ## Comparison policy
//!
//! Each side's stdout is parsed line-by-line as JSON objects. Lines that
//! don't start with `{` are skipped (Logstash and ferro-stash both
//! interleave plain-text log lines with JSON output). For each event:
//!
//! * `@timestamp`, `@version`, `host`, `event.original`, `tags` (when
//!   the only entry is the runtime's own `_grokparsefailure` etc) are
//!   ignored — they're added by the runtime, not the pipeline under
//!   test.
//! * Field set parity is asserted (any field present on one side and
//!   absent from the other is a divergence).
//! * Per-field value equality is asserted with `serde_json::Value` PartialEq.
//! * Event ordering is **not** asserted — both engines run multi-worker
//!   pipelines so the order is unstable. We canonicalise by sorting the
//!   event list by `serde_json::to_string` of the cleaned object.
//!
//! ## Failure mode
//!
//! Each fixture is a separate `#[test]` so cargo test reports per-fixture
//! pass/fail. On divergence, the test panics with a structured diff
//! showing which fields differ and on which side.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{Map, Value};

const LOGSTASH_IMAGE: &str = "docker.elastic.co/logstash/logstash:9.4.2";

/// Fields we never compare — they're populated by the runtime, not the
/// pipeline under test, and necessarily differ between two engines.
const RUNTIME_FIELDS: &[&str] = &["@timestamp", "@version", "host", "event.original"];

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/logstash-compat/fixtures")
}

fn ferro_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/debug/ferro-stash")
}

/// Strip runtime-only keys and return a stable form for comparison.
fn clean(obj: &Map<String, Value>) -> Map<String, Value> {
    let mut out = Map::new();
    for (k, v) in obj {
        if RUNTIME_FIELDS.contains(&k.as_str()) {
            continue;
        }
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Parse stdout: each line beginning with `{` is one JSON event.
fn parse_events(stdout: &str) -> Vec<Map<String, Value>> {
    let mut events = Vec::new();
    // Logstash sometimes concatenates JSON objects without newlines.
    // Use a streaming deserializer to peel them off one at a time.
    let mut de = serde_json::Deserializer::from_str(stdout).into_iter::<Value>();
    while let Some(item) = de.next() {
        match item {
            Ok(Value::Object(o)) => events.push(o),
            Ok(_) => {}
            Err(_) => {
                // Advance past the bad bit by finding next `{`.
                let pos = de.byte_offset();
                let rest = &stdout[pos..];
                if let Some(next_brace) = rest.find('{') {
                    de = serde_json::Deserializer::from_str(&rest[next_brace..])
                        .into_iter::<Value>();
                } else {
                    break;
                }
            }
        }
    }
    events
}

fn canonical_sort(mut events: Vec<Map<String, Value>>) -> Vec<Map<String, Value>> {
    events.sort_by_cached_key(|m| {
        // Sort by deterministic JSON serialisation of the cleaned form.
        let cleaned = clean(m);
        serde_json::to_string(&Value::Object(cleaned)).unwrap_or_default()
    });
    events
}

/// Run the local ferro-stash binary against a fixture's pipeline.conf,
/// piping input.txt to its stdin.
fn run_ferro(pipeline_conf: &str, input: &[u8]) -> Result<Vec<Map<String, Value>>, String> {
    let bin = ferro_binary();
    if !bin.exists() {
        return Err(format!(
            "ferro-stash binary not found at {} — run `cargo build --bin ferro-stash` first",
            bin.display()
        ));
    }
    let mut child = Command::new(&bin)
        .args(["-e", pipeline_conf, "--log.level", "error"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn ferro-stash: {e}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(input)
            .map_err(|e| format!("write to ferro stdin: {e}"))?;
        // Dropping closes stdin; ferro-stash's stdin input then drains and exits.
    }

    // Bound the run time — ferro-stash exits after ~10s of stdin EOF.
    let output = wait_with_timeout(child, Duration::from_secs(30))
        .map_err(|e| format!("ferro-stash run: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "ferro-stash exited {:?}: stderr=\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(parse_events(&String::from_utf8_lossy(&output.stdout)))
}

/// Run the upstream Logstash 8.x docker image with the same pipeline + input.
fn run_logstash(pipeline_conf: &str, input: &[u8]) -> Result<Vec<Map<String, Value>>, String> {
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--network=none",
            "-e",
            "LOG_LEVEL=error",
            "-e",
            "XPACK_MONITORING_ENABLED=false",
            "-e",
            "PIPELINE_ECS_COMPATIBILITY=disabled",
            LOGSTASH_IMAGE,
            "-e",
            pipeline_conf,
            "--log.level",
            "error",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn docker: {e} (is docker available?)"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(input)
            .map_err(|e| format!("write to logstash stdin: {e}"))?;
    }

    // Logstash JVM startup is ~10-15s; allow up to 90s for the whole run.
    let output = wait_with_timeout(child, Duration::from_secs(90))
        .map_err(|e| format!("logstash run: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "logstash exited {:?}: stderr=\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(parse_events(&String::from_utf8_lossy(&output.stdout)))
}

/// Cross-platform process-wait with a wall-clock timeout. Kills the
/// child on overshoot.
fn wait_with_timeout(
    mut child: std::process::Child,
    timeout: Duration,
) -> Result<std::process::Output, String> {
    use std::sync::mpsc;
    use std::thread;

    let (tx, rx) = mpsc::channel();
    let pid = child.id();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // Drain stdout/stderr on side threads — small outputs but be safe.
    let stdout_h = stdout.map(|mut o| {
        thread::spawn(move || {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut o, &mut buf).ok();
            buf
        })
    });
    let stderr_h = stderr.map(|mut e| {
        thread::spawn(move || {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut e, &mut buf).ok();
            buf
        })
    });

    thread::spawn(move || {
        let status = child.wait();
        let _ = tx.send(status);
    });

    let status = match rx.recv_timeout(timeout) {
        Ok(s) => s.map_err(|e| format!("wait: {e}"))?,
        Err(_) => {
            // Timeout — best-effort kill.
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
            return Err(format!("timed out after {timeout:?}"));
        }
    };

    let stdout = stdout_h.and_then(|h| h.join().ok()).unwrap_or_default();
    let stderr = stderr_h.and_then(|h| h.join().ok()).unwrap_or_default();

    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

/// Field-by-field diff between two cleaned event lists. Returns a list of
/// human-readable divergences. Empty list = parity.
fn diff_event_sets(ferro: &[Map<String, Value>], logstash: &[Map<String, Value>]) -> Vec<String> {
    let mut diffs = Vec::new();

    if ferro.len() != logstash.len() {
        diffs.push(format!(
            "event count: ferro={}, logstash={}",
            ferro.len(),
            logstash.len()
        ));
    }

    let n = ferro.len().min(logstash.len());
    for i in 0..n {
        let a = clean(&ferro[i]);
        let b = clean(&logstash[i]);

        let only_a: Vec<&String> = a.keys().filter(|k| !b.contains_key(*k)).collect();
        let only_b: Vec<&String> = b.keys().filter(|k| !a.contains_key(*k)).collect();
        if !only_a.is_empty() {
            diffs.push(format!("event[{i}]: only in ferro: {only_a:?}"));
        }
        if !only_b.is_empty() {
            diffs.push(format!("event[{i}]: only in logstash: {only_b:?}"));
        }
        for k in a.keys().filter(|k| b.contains_key(*k)) {
            let av = &a[k];
            let bv = &b[k];
            if av != bv {
                diffs.push(format!("event[{i}].{k}: ferro={av}, logstash={bv}"));
            }
        }
    }
    diffs
}

fn run_fixture(name: &str) {
    let fixture = fixtures_root().join(name);
    let pipeline_path = fixture.join("pipeline.conf");
    let input_path = fixture.join("input.txt");

    assert!(
        pipeline_path.exists(),
        "fixture {name}: missing pipeline.conf at {}",
        pipeline_path.display()
    );
    assert!(
        input_path.exists(),
        "fixture {name}: missing input.txt at {}",
        input_path.display()
    );

    let pipeline_conf = std::fs::read_to_string(&pipeline_path)
        .unwrap_or_else(|e| panic!("read pipeline.conf: {e}"));
    let input = std::fs::read(&input_path).unwrap_or_else(|e| panic!("read input.txt: {e}"));

    let ferro_events = match run_ferro(&pipeline_conf, &input) {
        Ok(e) => canonical_sort(e),
        Err(e) => panic!("[{name}] ferro-stash failed: {e}"),
    };
    let logstash_events = match run_logstash(&pipeline_conf, &input) {
        Ok(e) => canonical_sort(e),
        Err(e) => panic!("[{name}] logstash failed: {e}"),
    };

    eprintln!(
        "[{name}] ferro={} events, logstash={} events",
        ferro_events.len(),
        logstash_events.len()
    );

    let diffs = diff_event_sets(&ferro_events, &logstash_events);
    if !diffs.is_empty() {
        let mut report = format!("[{name}] DIVERGENCE ({} field(s)):\n", diffs.len());
        for d in &diffs {
            report.push_str(&format!("  - {d}\n"));
        }
        report.push_str(&format!(
            "\nferro raw:\n{}\n\nlogstash raw:\n{}\n",
            serde_json::to_string_pretty(&ferro_events).unwrap_or_default(),
            serde_json::to_string_pretty(&logstash_events).unwrap_or_default()
        ));
        panic!("{report}");
    }
}

// ----- one #[test] per fixture so cargo test reports per-fixture status -----

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_passthrough() {
    run_fixture("passthrough");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_grok_syslog() {
    run_fixture("grok_syslog");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_json_parse() {
    run_fixture("json_parse");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_mutate_basic() {
    run_fixture("mutate_basic");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_kv_extract() {
    run_fixture("kv_extract");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_dissect_pipe() {
    run_fixture("dissect_pipe");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_fingerprint_sha256() {
    run_fixture("fingerprint_sha256");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_drop_conditional() {
    run_fixture("drop_conditional");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_dissect_skip() {
    run_fixture("dissect_skip");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_conditional_branch() {
    run_fixture("conditional_branch");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_unicode_grok() {
    run_fixture("unicode_grok");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_malformed_json() {
    run_fixture("malformed_json");
}

#[test]
#[ignore = "requires docker + Logstash 8.x image (cargo test --ignored logstash_compat)"]
fn logstash_compat_date_iso8601() {
    // Wave 5.3 divergence #1 closed: `date` target now formats as
    // `%Y-%m-%dT%H:%M:%S%.3fZ` to match Logstash's `LogStash::Timestamp`
    // wire format (was `to_rfc3339()` → `+00:00` offset).
    run_fixture("date_iso8601");
}
