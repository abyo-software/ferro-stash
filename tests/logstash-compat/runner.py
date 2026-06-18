#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Logstash compatibility runner for ferro-stash.

Iterates every fixture directory under ``tests/logstash-compat/fixtures/``,
each of which must contain three files:

    pipeline.conf  — Logstash DSL pipeline config
    input.json     — NDJSON, one event per line
    expected.json  — JSON array of expected output events

For each fixture, the runner invokes the ferro-stash CLI (which presents
itself as ``logstash``) with the pipeline config inline (``-e``), feeds
the NDJSON to stdin, captures stdout, parses every line that looks like a
JSON object, and compares the resulting event set against ``expected.json``
field-by-field — ignoring ``@timestamp`` (allowed to differ) and event
ordering.

Usage:

    python3 tests/logstash-compat/runner.py
    python3 tests/logstash-compat/runner.py --binary target/release/logstash
    python3 tests/logstash-compat/runner.py --filter grok_syslog

Exit code is 0 when every fixture matches, 1 otherwise. The runner does
not depend on Java / upstream Logstash — it exercises the Rust CLI alone.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
FIXTURES_DIR = HERE / "fixtures"
REPO_ROOT = HERE.parent.parent


def find_binary(explicit: str | None) -> list[str]:
    """Locate the ferro-stash CLI. Falls back to ``cargo run``."""
    if explicit:
        path = Path(explicit)
        if not path.exists():
            sys.exit(f"--binary {explicit} not found")
        return [str(path)]
    # Prefer release, then debug, then cargo run.
    # The bin target is `ferro-stash` (see crates/ferro-stash-cli/Cargo.toml)
    # but the CLI presents itself as `logstash` at runtime.
    for candidate in (
        REPO_ROOT / "target" / "release" / "ferro-stash",
        REPO_ROOT / "target" / "release" / "logstash",
        REPO_ROOT / "target" / "debug" / "ferro-stash",
        REPO_ROOT / "target" / "debug" / "logstash",
    ):
        if candidate.exists():
            return [str(candidate)]
    if shutil.which("cargo"):
        return [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(REPO_ROOT / "Cargo.toml"),
            "--bin",
            "ferro-stash",
            "--",
        ]
    sys.exit(
        "no ferro-stash binary found; build with `cargo build --bin logstash` "
        "or pass --binary <path>"
    )


def run_pipeline(
    binary: list[str],
    pipeline_conf: str,
    stdin_payload: str,
    timeout: float,
) -> str:
    """Run one pipeline against stdin_payload and return raw stdout."""
    with tempfile.TemporaryDirectory(prefix="ferro-stash-compat-") as data_dir:
        cmd = [
            *binary,
            "-e",
            pipeline_conf,
            "--path.data",
            data_dir,
            "--api.enabled",
            "false",
            "--log.level",
            "error",
        ]
        try:
            proc = subprocess.run(
                cmd,
                input=stdin_payload,
                capture_output=True,
                text=True,
                timeout=timeout,
                env={**os.environ, "RUST_LOG": "error"},
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return exc.stdout or ""
        return proc.stdout


def extract_events(stdout: str) -> list[dict[str, Any]]:
    """Pick up every line that parses as a JSON object."""
    events: list[dict[str, Any]] = []
    for raw in stdout.splitlines():
        line = raw.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            events.append(obj)
    return events


def normalize(event: dict[str, Any]) -> dict[str, Any]:
    """Strip volatile fields before comparison."""
    return {k: v for k, v in event.items() if k not in {"@timestamp", "@version", "host"}}


def match_events(actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> tuple[bool, str]:
    """Order-insensitive set comparison on normalized events."""
    if len(actual) != len(expected):
        return False, f"event-count mismatch: got {len(actual)} expected {len(expected)}"

    used: set[int] = set()
    for i, exp in enumerate(expected):
        exp_norm = normalize(exp)
        match_idx: int | None = None
        for j, act in enumerate(actual):
            if j in used:
                continue
            act_norm = normalize(act)
            # All expected fields must match exactly.
            if all(act_norm.get(k) == v for k, v in exp_norm.items()):
                match_idx = j
                break
        if match_idx is None:
            return False, f"no actual event matches expected[{i}] = {exp_norm!r}"
        used.add(match_idx)
    return True, ""


def run_fixture(name: str, binary: list[str], timeout: float) -> tuple[bool, str]:
    fixture = FIXTURES_DIR / name
    pipeline_conf = (fixture / "pipeline.conf").read_text()
    # Fixtures use plain text NDJSON or one-line-per-event raw text fed to
    # the stdin input plugin. Prefer input.txt; fall back to input.json so
    # legacy fixtures keep working.
    input_txt = fixture / "input.txt"
    input_json = fixture / "input.json"
    if input_txt.exists():
        stdin_payload = input_txt.read_text()
    elif input_json.exists():
        stdin_payload = input_json.read_text()
    else:
        return False, "missing input.txt or input.json"
    expected = json.loads((fixture / "expected.json").read_text())

    if not isinstance(expected, list):
        return False, "expected.json must be a JSON array"

    stdout = run_pipeline(binary, pipeline_conf, stdin_payload, timeout)
    actual = extract_events(stdout)
    ok, why = match_events(actual, expected)
    if ok:
        return True, ""
    return False, f"{why}\n  --- actual events ---\n  {actual}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        help="path to the ferro-stash logstash binary (default: auto-detect)",
    )
    parser.add_argument(
        "--filter",
        help="run only fixtures whose directory name contains this substring",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="per-fixture timeout in seconds (default: 30)",
    )
    args = parser.parse_args()

    if not FIXTURES_DIR.is_dir():
        sys.exit(f"missing fixtures dir {FIXTURES_DIR}")

    binary = find_binary(args.binary)
    fixtures = sorted(p.name for p in FIXTURES_DIR.iterdir() if p.is_dir())
    if args.filter:
        fixtures = [f for f in fixtures if args.filter in f]
    if not fixtures:
        sys.exit("no fixtures matched")

    print(f"running {len(fixtures)} Logstash compatibility fixture(s)")
    print(f"binary: {' '.join(binary)}")
    print()

    passed: list[str] = []
    failed: list[tuple[str, str]] = []
    started = time.monotonic()

    for name in fixtures:
        t0 = time.monotonic()
        ok, why = run_fixture(name, binary, args.timeout)
        dt = time.monotonic() - t0
        if ok:
            print(f"  PASS  {name}  ({dt:.1f}s)")
            passed.append(name)
        else:
            print(f"  FAIL  {name}  ({dt:.1f}s)")
            print(f"        {why}")
            failed.append((name, why))

    print()
    print(
        f"summary: {len(passed)} passed / {len(failed)} failed "
        f"in {time.monotonic() - started:.1f}s"
    )
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
