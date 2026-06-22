#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
JRuby Logstash <-> ferro-stash parity diff harness (scaffold).

Phase 2 / external-infra-required. Runs every fixture under
``tests/logstash-compat/fixtures/`` against BOTH:

  1. ferro-stash binary (located via ``--ferro-binary`` or ``FERRO_STASH_BIN``)
  2. JRuby Logstash 9.4.2 (located via ``--logstash-bin`` or ``LOGSTASH_HOME``)

For each fixture:
  - Run pipeline.conf with input.txt against each binary
  - Capture stdout JSON event(s)
  - Compare event sets field-by-field (ignoring @timestamp / @version /
    host / event.original — same ignore-list policy as runner.py)
  - On mismatch: report which fields differ and which side produced them

This is the *real* parity check: not "ferro-stash output matches a
hand-curated expected.json" (runner.py) but "ferro-stash output matches
JRuby Logstash output for the same pipeline + input".

## Why scaffolded

JRuby Logstash is not vendored in this workspace — the ~300 MB JVM dist
is not redistributable here, so you supply your own install.

To wire this harness:

  1. Download the Logstash 9.4.2 tarball from elastic.co
  2. Extract it anywhere on disk
  3. Set ``LOGSTASH_HOME=/path/to/logstash-9.4.2``
  4. Run ``python3 tests/logstash-compat/jruby_parity.py``

The harness will start, walk every fixture, and report parity per
fixture. Without ``LOGSTASH_HOME`` set, it exits 0 with a clear
"SKIPPED: requires LOGSTASH_HOME" message — matching the
ferro-auth/external-conformance scaffold convention.

## Honest scope

- Fixtures use the same pipeline.conf form Logstash accepts unmodified.
  (No ferro-stash-specific extensions in the wave-1 fixture set —
  verified by the wave-1 runner against the binary.)
- Event ordering is NOT asserted (Logstash threads + workers can
  re-order); set comparison after canonical sort by stable key.
- @timestamp drift is expected and ignored (each binary stamps its
  own clock).
- @version is set differently by Logstash (always "1") vs ferro-stash
  (configurable per pipeline) — ignored.

## Phase 2 deliverable

Once wired and run against real Logstash, output a per-fixture report:

    fixture                    parity    differing fields
    grok_syslog                ✓
    json_parse                 ✗         tags (Logstash adds _grokparsefailure on partial match)
    mutate_basic               ✓
    ...

Failures = real implementation gaps to close in ferro-stash, NOT
"expected.json wrong". The expected.json files in fixtures/ stay as
authoritative for the in-process Rust runner; this harness is the
external-truth check.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = REPO_ROOT / "tests" / "logstash-compat" / "fixtures"
IGNORE_FIELDS = {"@timestamp", "@version", "host", "event.original"}


def normalise(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop ignored fields + sort deterministically."""
    out = []
    for ev in events:
        cleaned = {k: v for k, v in ev.items() if k not in IGNORE_FIELDS}
        out.append(cleaned)
    return sorted(out, key=lambda d: json.dumps(d, sort_keys=True, default=str))


def run_pipeline(binary: str, pipeline_conf: Path, input_data: bytes) -> list[dict[str, Any]]:
    """Invoke `binary -e <conf>` with input_data on stdin, parse JSON-line output."""
    cmd = [binary, "-e", pipeline_conf.read_text(), "--log.level", "warn"]
    proc = subprocess.run(
        cmd,
        input=input_data,
        capture_output=True,
        timeout=60,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"{binary} failed (rc={proc.returncode}): {proc.stderr.decode(errors='replace')[:500]}"
        )
    events = []
    for line in proc.stdout.decode(errors="replace").splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def diff_events(
    a: list[dict[str, Any]],
    b: list[dict[str, Any]],
) -> list[str]:
    """Return human-readable diffs between two normalised event lists."""
    diffs = []
    if len(a) != len(b):
        diffs.append(f"event count: ferro={len(a)}, logstash={len(b)}")
    for i, (ea, eb) in enumerate(zip(a, b)):
        only_a = set(ea.keys()) - set(eb.keys())
        only_b = set(eb.keys()) - set(ea.keys())
        if only_a:
            diffs.append(f"event {i}: only in ferro: {sorted(only_a)}")
        if only_b:
            diffs.append(f"event {i}: only in logstash: {sorted(only_b)}")
        for key in set(ea.keys()) & set(eb.keys()):
            if ea[key] != eb[key]:
                diffs.append(f"event {i}: {key}: ferro={ea[key]!r}, logstash={eb[key]!r}")
    return diffs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ferro-binary", default=os.environ.get("FERRO_STASH_BIN"))
    parser.add_argument("--logstash-bin", default=None)
    parser.add_argument("--filter", default=None, help="run only fixtures matching this name")
    args = parser.parse_args()

    logstash_home = os.environ.get("LOGSTASH_HOME")
    logstash_bin = args.logstash_bin or (
        f"{logstash_home}/bin/logstash" if logstash_home else None
    )

    if not logstash_bin or not Path(logstash_bin).exists():
        print(
            "SKIPPED: requires JRuby Logstash 9.4.2. Set LOGSTASH_HOME or pass --logstash-bin.\n"
            "  See module docstring for how to obtain.",
            file=sys.stderr,
        )
        return 0

    ferro_bin = args.ferro_binary or str(REPO_ROOT / "target" / "release" / "ferro-stash")
    if not Path(ferro_bin).exists():
        print(f"FAIL: ferro-stash binary not found at {ferro_bin}", file=sys.stderr)
        print("  Build first: cargo build --release", file=sys.stderr)
        return 1

    fixtures = sorted(p for p in FIXTURES_DIR.iterdir() if p.is_dir())
    if args.filter:
        fixtures = [f for f in fixtures if args.filter in f.name]

    failures = 0
    for fx in fixtures:
        pipeline = fx / "pipeline.conf"
        input_path = fx / "input.txt"
        if not pipeline.exists() or not input_path.exists():
            print(f"SKIP {fx.name}: missing pipeline.conf or input.txt")
            continue
        input_bytes = input_path.read_bytes()
        try:
            ferro_events = normalise(run_pipeline(ferro_bin, pipeline, input_bytes))
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            print(f"FAIL {fx.name}: ferro: {e}")
            failures += 1
            continue
        try:
            logstash_events = normalise(run_pipeline(logstash_bin, pipeline, input_bytes))
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            print(f"FAIL {fx.name}: logstash: {e}")
            failures += 1
            continue
        diffs = diff_events(ferro_events, logstash_events)
        if diffs:
            failures += 1
            print(f"DIFF {fx.name}:")
            for d in diffs:
                print(f"  - {d}")
        else:
            print(f"OK   {fx.name}")

    if failures:
        print(f"\n{failures} fixture(s) diverge from upstream Logstash.")
        return 1
    print(
        f"\nAll {len(fixtures)} fixtures match upstream Logstash field-by-field "
        "(runtime-only fields normalized)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
