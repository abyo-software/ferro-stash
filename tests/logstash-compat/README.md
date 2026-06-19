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

## Prerequisites — obtaining Logstash

The side-by-side harnesses compare ferro-stash against a **real Logstash**.
Logstash is distributed by Elastic under the Elastic License v2 / SSPL and
is **not vendored in this repository** — you provide your own copy. There
are two supported ways, depending on which harness you run:

**1. Docker (recommended; used by the Rust side-by-side harness).**
No local install needed — the harness shells out to a pinned image:

```bash
docker pull docker.elastic.co/logstash/logstash:9.4.2
# or: docker compose -f tests/logstash-compat/docker-compose.yml pull
```

See [Docker side-by-side regression harness](#docker-side-by-side-regression-harness)
below.

**2. Local tarball install (used by `runner.py`, `jruby_parity.py`, and
`run_smoke_tests.sh`).** Download a Logstash 9.3.x release from Elastic,
extract it anywhere, and point the harness at it via environment
variables:

```bash
# Pick the build for your platform from https://www.elastic.co/downloads/logstash
curl -fsSLO https://artifacts.elastic.co/downloads/logstash/logstash-9.3.2-linux-x86_64.tar.gz
tar xzf logstash-9.3.2-linux-x86_64.tar.gz

# jruby_parity.py reads LOGSTASH_HOME (the extracted directory):
export LOGSTASH_HOME="$PWD/logstash-9.3.2"

# run_smoke_tests.sh reads LOGSTASH_BIN (defaults to `logstash` on PATH):
export LOGSTASH_BIN="$LOGSTASH_HOME/bin/logstash"
```

The bundled JVM ships inside the tarball, so no separate Java install is
required. All harnesses **skip gracefully** when Logstash is absent:
`jruby_parity.py` exits 0 with a `SKIPPED: requires LOGSTASH_HOME`
message, and the Rust docker tests are `#[ignore]` by default.

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

**24 fixtures**, all golden files generated from the real **Logstash 9.4.2**
oracle (`gen_expected.py`) and green on both runners (in-process
`logstash_compat_test` + Python `runner.py`):

| fixture | filters exercised |
|---------|-------------------|
| passthrough | (none) |
| grok_syslog / unicode_grok | grok |
| dissect_pipe / dissect_skip | dissect |
| json_parse / malformed_json | json (+ `_jsonparsefailure` path) |
| kv_extract / kv_custom_split | kv (default + custom split/target) |
| mutate_basic / mutate_gsub / mutate_convert / mutate_copy_strip | mutate (rename/case/add/remove/gsub/convert/copy/strip) |
| date_iso8601 / date_target | date (ISO8601 + Joda → named target) |
| translate_lookup | translate (source/target dictionary) |
| csv_columns | csv |
| truncate_bytes | truncate |
| split_array | split (array fan-out) |
| clone_fanout | clone (event fan-out + name tags) |
| urldecode_field | urldecode |
| fingerprint_sha256 | fingerprint |
| drop_conditional / conditional_branch | conditional `if/else if/else` (+ drop) |

Last verified: 2026-06-20 against **Logstash 9.4.2** — all 24 fixtures green on
both runners.

## Docker side-by-side regression harness

`tests/e2e/logstash_docker_compat_test.rs` is the side-by-side regression
suite. It runs the **same** `pipeline.conf` and `input.txt` through:

  * the local `target/debug/ferro-stash` binary, AND
  * `docker.elastic.co/logstash/logstash:9.4.2` (pinned)

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
docker pull docker.elastic.co/logstash/logstash:9.4.2
# or:
docker compose -f tests/logstash-compat/docker-compose.yml pull

# 3. Run the harness — one #[test] per fixture, all #[ignore] by default
cargo test -p ferro-stash-e2e --test logstash_docker_compat_test \
    -- --ignored --nocapture --test-threads=1
```

`--test-threads=1` is recommended: each fixture spawns a fresh JVM via
`docker run --rm -i`, and serialising avoids the ~3 GB peak RSS spikes
otherwise. Each fixture pays a Logstash cold-start (~7–12 s), so the full
24-fixture docker suite takes several minutes; the in-process
`logstash_compat_test` (no Docker) runs the same fixtures in milliseconds.

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

None as of 2026-06-20. The 24 bundled fixtures cover stdin input, ~17 filter
plugins (grok, dissect, json, kv, mutate, date, translate, csv, truncate,
split, clone, urldecode, fingerprint, drop, conditionals), and stdout JSON
output. They produce **byte-identical event payloads** between Logstash 9.4.2
and ferro-stash 0.1.0, modulo the runtime-only fields above.

This does *not* substantiate full Logstash compat — it substantiates
the surface tested (43 of 165+ Logstash plugins implemented; the
docker harness covers the subset we ship).
