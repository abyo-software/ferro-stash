#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Deterministic synthetic input generator for the FerroStash vs Logstash
benchmark. Output is fully deterministic (index-derived, no RNG) so a run is
reproducible and both engines see byte-identical input.

Formats (`--format`):
  accesslog (default) — `%{IPORHOST} %{WORD} %{URIPATH} %{INT} %{INT} %{QS} %{NUMBER}`
                        (drives the grok / dissect / script / ruby benches)
  json              — one JSON object per line (drives the json-filter bench)
  kv                — `k=v k=v ...` (drives the kv-filter bench)
  csv               — comma-separated row (drives the csv-filter bench)

Usage:  python3 gen_input.py <num_lines> [--format accesslog|json|kv|csv] > input
"""

import json
import sys

METHODS = ["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"]
PATHS = [
    "/",
    "/api/v1/users",
    "/api/v1/orders/42",
    "/static/app.js",
    "/health",
    "/login",
    "/search",
    "/api/v2/items/9999",
]
AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64)",
    "curl/8.4.0",
    "Go-http-client/2.0",
    "python-requests/2.31",
]
STATUSES = [200, 200, 200, 301, 404, 500]
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
SERVICES = ["auth", "payment", "inventory", "search"]


def fields(i):
    return (
        f"{(i % 223) + 1}.{(i // 223) % 256}.{(i // 7) % 256}.{(i * 13) % 256}",  # ip
        METHODS[i % len(METHODS)],
        PATHS[i % len(PATHS)],
        STATUSES[i % len(STATUSES)],
        100 + (i * 37) % 9000,  # bytes
        AGENTS[i % len(AGENTS)],
        ((i * 7) % 1000) / 1000.0,  # latency
    )


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    fmt = "accesslog"
    if "--format" in sys.argv:
        fmt = sys.argv[sys.argv.index("--format") + 1]
    write = sys.stdout.write
    for i in range(n):
        ip, method, path, status, nbytes, agent, latency = fields(i)
        if fmt == "accesslog":
            write(f'{ip} {method} {path} {status} {nbytes} "{agent}" {latency:.3f}\n')
        elif fmt == "json":
            write(
                json.dumps(
                    {
                        "ip": ip,
                        "method": method,
                        "path": path,
                        "status": status,
                        "bytes": nbytes,
                        "agent": agent,
                        "latency": latency,
                        "level": LEVELS[i % len(LEVELS)],
                        "service": SERVICES[i % len(SERVICES)],
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )
        elif fmt == "kv":
            write(
                f"ip={ip} method={method} path={path} status={status} "
                f"bytes={nbytes} latency={latency:.3f} "
                f"level={LEVELS[i % len(LEVELS)]} service={SERVICES[i % len(SERVICES)]}\n"
            )
        elif fmt == "csv":
            write(
                f"{ip},{method},{path},{status},{nbytes},"
                f"{LEVELS[i % len(LEVELS)]},{SERVICES[i % len(SERVICES)]},{latency:.3f}\n"
            )
        else:
            sys.exit(f"unknown --format {fmt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
