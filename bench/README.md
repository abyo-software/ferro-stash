<!-- SPDX-License-Identifier: Apache-2.0 -->
# FerroStash vs Logstash benchmark

Reproducible, single-host throughput / memory / startup comparison. The whole
point is that the numbers in the project README come from **this harness** —
run it yourself and check.

## Methodology

- **Identical pipeline + byte-identical input** on both engines. Configs live in
  `configs/` as matched `*.fs.conf` / `*.ls.conf` pairs using the same grok
  pattern / dissect mapping / etc.
- **Input** is generated deterministically by `gen_input.py` (no RNG), so a run
  is reproducible and both engines see the same bytes. Formats: `accesslog`
  (grok/dissect/script/ruby), `json`, `kv`, `csv`.
- **Both engines read stdin and exit at EOF** — FerroStash via `--auto-exit`,
  Logstash's `stdin` input stops the pipeline on EOF. Output is `null` so the
  sink is never the bottleneck.
- **Timing** uses `/usr/bin/time -v`: wall clock and peak RSS. Throughput is
  reported **startup-subtracted** (a separate 1-line run measures cold-start,
  which is then removed from each timed run) so it reflects steady-state
  processing, not JVM boot. Cold-start is reported as its own column.
- **N runs + 1 warmup**, throughput as mean ± population stddev; peak RSS is the
  max observed.
- Workers = host cores for both engines (Logstash also `pipeline.batch` defaults).

## Workloads

| group | what | engines |
|---|---|---|
| native | grok + mutate | FS, LS |
| per-filter | grok / dissect / json / kv / csv, each alone | FS, LS |
| custom (Painless vs Ruby) | same transform via FerroStash `script` (Cranelift JIT), FerroStash `ruby` (Artichoke mruby), Logstash `ruby` (JRuby) | FS, FS, LS |

The custom group is the interesting one: Logstash's only in-pipeline custom-logic
path is `ruby{}`. FerroStash runs that same Ruby (for drop-in migration) **and**
offers a native JIT `script{}` (Painless subset) for the hot path.

## Running

```bash
# Build FerroStash with the ruby filter (needed for the ruby custom bench)
cargo build --release -p ferro-stash --features ruby

# Install a pinned Logstash (native tarball, bundled JDK)
eval "$(./bench/setup_logstash.sh 8.17.0)"   # sets LS_HOME

# Run: LINES RUNS WORKERS
LS_HOME="$LS_HOME" ./bench/run_bench.sh 5000000 5 "$(nproc)"
```

The slow `ruby` (mruby) custom filter uses a smaller line count than the native
workloads so wall time stays sane — override with `CUSTOM_LINES` (default
500,000). Without `LS_HOME` set, only the FerroStash side runs (useful for
FS-only regression checks). Results are printed and saved to
`$BENCH_DIR/results.md` (default `/tmp/ferro-bench/results.md`).

## Honesty notes

- Numbers are **single-host, single-environment** — directional, not a universal
  guarantee. The host arch / cores / version are printed in the run header.
- The `ruby` filter is *expected* to be slower than Logstash's JRuby (no JIT,
  per-event FFI marshalling); the comparison documents the **migration cost** and
  the speedup available by moving hot logic to `script{}`.
- `null` output isolates input+filter cost; real sinks (ES/S3/kafka) add their
  own overhead on both engines.
