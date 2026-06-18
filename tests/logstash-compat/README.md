# Logstash compatibility test harness

Black-box compatibility tests for ferro-stash against the Logstash 9.3.2
pipeline contract. Each test is a self-contained directory containing a
Logstash DSL pipeline, NDJSON input, and a golden output JSON array.

## Layout

```
tests/logstash-compat/
├── README.md                  ← this file
├── runner.py                  ← Python harness, drives the real binary
├── ec2_*.sh, run_smoke_tests.sh  ← pre-existing EC2 / smoke scripts (untouched)
└── fixtures/
    ├── passthrough/           ← no filters, plumbing-only check
    ├── grok_syslog/           ← grok against an RFC3164 syslog line
    ├── json_parse/            ← json filter merging a JSON-encoded message
    ├── mutate_basic/          ← rename / uppercase / lowercase / add_field / add_tag
    ├── kv_extract/            ← kv filter on `key=value` pairs
    └── dissect_pipe/          ← dissect on a pipe-delimited record
```

Each fixture directory has exactly three files:

| file            | content                                                            |
|-----------------|--------------------------------------------------------------------|
| `pipeline.conf` | Logstash DSL config (`input` / `filter` / `output`)                |
| `input.txt`     | Raw lines fed to stdin — each line becomes one event's `message`   |
| `expected.json` | JSON array of expected output events                               |

The stdin input plugin in ferro-stash creates an event per line with the
literal line stored under `message` (matching Logstash's `stdin { codec => "plain" }`
default), so `input.txt` is plain text — not NDJSON. Fixtures that need a
JSON-shaped event use the `json { source => "message" }` filter to merge
the JSON payload into top-level fields.

## Comparison policy

The runner compares actual output to `expected.json` field-by-field, with two
deliberate relaxations so the tests are reproducible:

1. **`@timestamp`, `@version`, `host` are ignored** — these are populated by
   the runtime/environment, not the pipeline under test.
2. **Event ordering is ignored** — the pipeline is multi-worker, so we do an
   order-insensitive set match (every expected event must match exactly one
   unused actual event on every expected field).

If `actual` carries fields the fixture did not list, that's tolerated too —
the fixture asserts presence, not absence. Use the existing exhaustive
`tests/e2e/compatibility_test.rs` if you need strict equality.

## Running — Python

```bash
# From repo root, with the binary already built:
cargo build --release --bin logstash
python3 tests/logstash-compat/runner.py

# Or let the runner shell out to `cargo run`:
python3 tests/logstash-compat/runner.py

# Run a single case:
python3 tests/logstash-compat/runner.py --filter grok_syslog

# Point at a specific binary (useful in CI):
python3 tests/logstash-compat/runner.py --binary target/release/logstash
```

The runner spawns `logstash -e <pipeline.conf>` per fixture, pipes the
NDJSON to stdin, captures stdout, and parses every JSON-object line. It
exits non-zero on any mismatch.

## Running — Rust

`tests/e2e/logstash_compat_test.rs` is a fixture-driven Rust test that
reuses the same comparison logic in-process (no binary spawn). It is **not
yet wired into Cargo** — to enable it, add the following block to
`crates/ferro-stash-e2e/Cargo.toml`:

```toml
[[test]]
name = "logstash_compat_test"
path = "../../tests/e2e/logstash_compat_test.rs"
```

After that, `cargo test --workspace` will exercise every fixture under
`fixtures/` automatically.

## Adding a new fixture

1. `mkdir tests/logstash-compat/fixtures/<name>/`
2. Drop a Logstash DSL `pipeline.conf` in.
3. Provide `input.txt` (one event per line).
4. Run the pipeline once by hand to capture the actual output, prune the
   environment-dependent fields (`@timestamp`, `@version`, `host`), and
   save as `expected.json`.
5. Re-run the runner to confirm the new case is green.

## Current coverage

| fixture        | filters exercised               | runner.py vs golden | docker harness vs Logstash 8.15.3 |
|----------------|----------------------------------|---------------------|-----------------------------------|
| passthrough    | (none)                           | PASS                | PASS                              |
| grok_syslog    | grok                             | PASS                | PASS                              |
| json_parse     | json + mutate (remove_field)     | PASS                | PASS                              |
| mutate_basic   | json + mutate (rename / case / add_field / add_tag / remove_field) | PASS | PASS  |
| kv_extract     | kv                               | PASS                | PASS                              |
| dissect_pipe   | dissect                          | PASS                | PASS                              |

Last verified: 2026-05-07. All six fixtures green on both runners.

## Docker side-by-side regression harness

`tests/e2e/logstash_docker_compat_test.rs` is the side-by-side regression
suite. It runs the **same** `pipeline.conf` and `input.txt` through:

  * the local `target/debug/ferro-stash` binary, AND
  * `docker.elastic.co/logstash/logstash:8.15.3` (pinned)

…then field-by-field diffs the JSON event output, ignoring only the
runtime-only fields `@timestamp`, `@version`, `host`, `event.original`.
This is the *external truth* check (the Python `runner.py` checks
ferro-stash against a hand-curated `expected.json`; this harness checks
ferro-stash against upstream Logstash itself).

### Running

```bash
# 1. Build the binary
cargo build --bin ferro-stash

# 2. Pre-pull the reference image (~525 MB, one-time)
docker pull docker.elastic.co/logstash/logstash:8.15.3
# or:
docker compose -f tests/logstash-compat/docker-compose.yml pull

# 3. Run the harness — one #[test] per fixture, all #[ignore] by default
cargo test -p ferro-stash-e2e --test logstash_docker_compat_test \
    -- --ignored --nocapture --test-threads=1
```

`--test-threads=1` is recommended: each fixture spawns a fresh JVM via
`docker run --rm -i`, and serialising avoids the ~3 GB peak RSS spikes
otherwise. Wall-clock for the 6-fixture suite is ~70 s on a typical
workstation (~10–12 s per Logstash cold-start).

### Comparison policy (docker harness)

| field                          | treatment                                    |
|--------------------------------|----------------------------------------------|
| `@timestamp`                   | ignored (runtime-stamped, never identical)   |
| `@version`                     | ignored (Logstash always "1"; ferro absent)  |
| `host`                         | ignored (Logstash sets container hostname)   |
| `event.original`               | ignored (Logstash 8 ECS field)               |
| every other field              | strict `serde_json::Value` equality          |
| event ordering                 | not asserted — set comparison after sort     |

### Known divergences

None as of 2026-05-07. The six bundled fixtures cover stdin input,
six filter plugins (grok / mutate / json / kv / dissect / pipeline
pass-through), and stdout JSON output. They produce **byte-identical
event payloads** between Logstash 8.15.3 and ferro-stash 0.1.0,
modulo the four runtime-only fields above.

This does *not* substantiate full Logstash compat — it substantiates
the surface tested (43 of 165+ Logstash plugins implemented; the
docker harness covers the subset we ship).
