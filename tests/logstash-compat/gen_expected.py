#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Generate ``expected.json`` for a Logstash-compatibility fixture by running the
**real** upstream Logstash as an oracle.

For each fixture directory it reads ``pipeline.conf`` + ``input.txt``, runs them
through a Dockerised Logstash, captures the emitted events, strips the volatile
and Logstash-internal keys that the parity runner ignores (``@timestamp``,
``@version``, ``host``, ``event`` (ECS ``event.original``), ``log``, ``agent``,
``ecs``, ``@metadata``), and writes the remaining deterministic fields to
``expected.json``.

This is the authoritative way to produce a byte-eq baseline: the expected output
is whatever real Logstash does, not a hand-written guess. ``runner.py`` then
asserts the ferro-stash CLI reproduces it field-for-field.

Usage:

    # Generate / refresh one or more fixtures
    python3 tests/logstash-compat/gen_expected.py clone_fanout csv_columns

    # Regenerate every fixture (slow — one JVM boot each)
    python3 tests/logstash-compat/gen_expected.py --all

Requires Docker and pulls ``docker.elastic.co/logstash/logstash:9.4.2`` on
first use. Logstash's JVM boot is slow (~60-90s); the default per-fixture
timeout is 300s.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
FIXTURES_DIR = HERE / "fixtures"
LOGSTASH_IMAGE = "docker.elastic.co/logstash/logstash:9.4.2"

# Keys that real Logstash adds at runtime which the parity runner ignores and
# which must not be baked into the expected baseline.
VOLATILE_KEYS = {
    "@timestamp",
    "@version",
    "host",
    "event",       # ECS event.original (raw input echo)
    "log",
    "agent",
    "ecs",
    "@metadata",
}


def strip(event: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in event.items() if k not in VOLATILE_KEYS}


def parse_events(raw: str) -> list[dict[str, Any]]:
    """Peel every JSON object out of mixed stdout (logs + concatenated events)."""
    decoder = json.JSONDecoder()
    events: list[dict[str, Any]] = []
    idx = 0
    n = len(raw)
    while idx < n:
        ch = raw[idx]
        if ch != "{":
            idx += 1
            continue
        try:
            obj, end = decoder.raw_decode(raw, idx)
        except json.JSONDecodeError:
            idx += 1
            continue
        if isinstance(obj, dict):
            events.append(obj)
        idx = end
    return events


def run_logstash(pipeline_conf: str, stdin_payload: str, timeout: float) -> str:
    cmd = [
        "docker", "run", "--rm", "-i",
        "-e", "LS_JAVA_OPTS=-Xms512m -Xmx512m",
        LOGSTASH_IMAGE,
        "logstash",
        "-e", pipeline_conf,
        "--log.level", "error",
    ]
    proc = subprocess.run(
        cmd,
        input=stdin_payload,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return proc.stdout


def gen_fixture(name: str, timeout: float) -> bool:
    fixture = FIXTURES_DIR / name
    conf = fixture / "pipeline.conf"
    inp = fixture / "input.txt"
    if not conf.exists() or not inp.exists():
        print(f"  SKIP  {name}: missing pipeline.conf or input.txt")
        return False
    pipeline = conf.read_text()
    payload = inp.read_text()
    print(f"  RUN   {name} (real Logstash, may take ~90s) ...", flush=True)
    raw = run_logstash(pipeline, payload, timeout)
    events = [strip(e) for e in parse_events(raw)]
    # Logstash emits a private `tags: ["_jsonparsefailure"]` etc. on errors; keep
    # them — divergence there is a real parity signal worth asserting.
    if not events:
        print(f"  FAIL  {name}: Logstash produced no parseable events")
        print(f"        raw stdout head: {raw[:400]!r}")
        return False
    out = fixture / "expected.json"
    # sort_keys: the parity matcher is order-insensitive, so emit a canonical
    # key order — keeps regeneration diffs to *real* value changes, not Logstash
    # output-order churn.
    out.write_text(json.dumps(events, indent=2, ensure_ascii=False, sort_keys=True) + "\n")
    print(f"  OK    {name}: {len(events)} event(s) -> {out.relative_to(HERE.parent.parent)}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("fixtures", nargs="*", help="fixture directory names to (re)generate")
    ap.add_argument("--all", action="store_true", help="regenerate every fixture")
    ap.add_argument("--timeout", type=float, default=300.0, help="per-fixture timeout (s)")
    args = ap.parse_args()

    if args.all:
        names = sorted(p.name for p in FIXTURES_DIR.iterdir() if p.is_dir())
    elif args.fixtures:
        names = args.fixtures
    else:
        ap.error("pass fixture names or --all")

    ok = 0
    for name in names:
        if gen_fixture(name, args.timeout):
            ok += 1
    print(f"\ngenerated {ok}/{len(names)} fixture(s)")
    return 0 if ok == len(names) else 1


if __name__ == "__main__":
    sys.exit(main())
