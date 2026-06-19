#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# FerroStash vs Logstash benchmark runner (Linux, reproducible).
#
# Measures, for an identical pipeline + byte-identical input on both engines:
#   * steady-state throughput (events/sec, startup-subtracted), mean ± stddev
#   * peak RSS (MB)
#   * cold-start (s) — separately, from a 1-line run
#
# Both engines read the input on stdin and exit at EOF (FerroStash via
# --auto-exit; Logstash's stdin input stops the pipeline on EOF). Each is timed
# with `/usr/bin/time -v`. Output goes to `null` so the sink is never the
# bottleneck. Workloads:
#   A native      grok + mutate                 (FS vs LS)
#   D per-filter  grok / dissect / json / kv / csv each alone   (FS vs LS)
#   B/C custom    same transform as Painless(script) / Ruby(mruby) / Ruby(JRuby)
#
# Usage:
#   LS_HOME=/opt/logstash ./bench/run_bench.sh [LINES] [RUNS] [WORKERS]
# Defaults: LINES=5000000 RUNS=5 WORKERS=$(nproc). Set FERRO_BIN to override the
# binary (must be built WITH --features ruby for the ruby custom bench).
set -euo pipefail

LINES="${1:-5000000}"
RUNS="${2:-5}"
WORKERS="${3:-$(nproc)}"
# The ruby(mruby) custom filter is ~100x slower than the native path, so the
# custom group uses a smaller line count (still >> startup for steady-state)
# to keep wall time sane. Override with CUSTOM_LINES.
CUSTOM_LINES="${CUSTOM_LINES:-500000}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PD="${BENCH_DIR:-/tmp/ferro-bench}"
FS="${FERRO_BIN:-$ROOT/target/release/ferro-stash}"
LS_HOME="${LS_HOME:-}"
CONF="$HERE/configs"
TIME=/usr/bin/time
mkdir -p "$PD"

[ -x "$FS" ] || { echo "FerroStash binary not found: $FS (build: cargo build --release -p ferro-stash --features ruby)"; exit 1; }

gen() { local fmt="$1" n="$2"; local f="$PD/input_${fmt}_${n}.log"; [ -f "$f" ] || python3 "$HERE/gen_input.py" "$n" --format "$fmt" > "$f"; echo "$f"; }
one_line() { local fmt="$1"; local f="$PD/oneline_$fmt.log"; [ -f "$f" ] || python3 "$HERE/gen_input.py" 1 --format "$fmt" > "$f"; echo "$f"; }

rss_kb()  { awk -F': ' '/Maximum resident set size/{print $2}' "$1"; }

# run_once: sets RC and WALL (wall-clock seconds, HIGH RESOLUTION via
# `date +%s.%N`). `/usr/bin/time -v`'s own "Elapsed" is only 0.01s-granular, so
# at sub-second runs throughput quantizes to suspiciously round numbers (e.g.
# 200000/0.25 = exactly 800000). We time the whole invocation with nanosecond
# `date` instead and keep time -v solely for peak RSS (written to $LOG).
run_once() { # engine conf input  -> sets RC, WALL; writes time -v to $LOG
  local engine="$1" conf="$2" input="$3"
  set +e
  local t0 t1
  t0=$(date +%s.%N)
  case "$engine" in
    fs) $TIME -v "$FS" -f "$conf" --auto-exit -w "$WORKERS" --api.enabled false -l error < "$input" >/dev/null 2>"$LOG" ;;
    ls) rm -rf "$PD/lsdata"; mkdir -p "$PD/lsdata"
        $TIME -v "$LS_HOME/bin/logstash" -f "$conf" --pipeline.workers "$WORKERS" \
              --path.data "$PD/lsdata" --log.level error < "$input" >/dev/null 2>"$LOG" ;;
  esac
  RC=$?            # /usr/bin/time -v propagates the command's exit code
  t1=$(date +%s.%N)
  WALL=$(python3 -c "print($t1 - $t0)")
  set -e
}

fail_row() { # label rc
  echo "| $1 | FAILED (rc=$2) | n/a | n/a | n/a |" >> "$RESULTS"
  echo "  FAILED $1 (rc=$2):"; grep -iE 'error|caused by' "$LOG" 2>/dev/null | head -3
}

measure() { # label engine conf fmt [lines]  -> appends a row to $RESULTS
  local label="$1" engine="$2" conf="$3" fmt="$4" n="${5:-$LINES}"
  [ "$engine" = ls ] && [ -z "$LS_HOME" ] && { echo "  skip $label (LS_HOME unset)"; return; }
  local input; input="$(gen "$fmt" "$n")"
  # Cold-start: a 1-line run. A non-zero exit here means the config/engine is
  # broken — record FAILED, never a throughput number.
  LOG=$(mktemp); run_once "$engine" "$conf" "$(one_line "$fmt")"
  if [ "$RC" -ne 0 ]; then fail_row "$label" "$RC"; rm -f "$LOG"; return; fi
  local start="$WALL"; rm -f "$LOG"
  local secs=() rss=()
  for r in $(seq 0 "$RUNS"); do
    LOG=$(mktemp); run_once "$engine" "$conf" "$input"
    if [ "$RC" -ne 0 ]; then fail_row "$label" "$RC"; rm -f "$LOG"; return; fi
    if [ "$r" -gt 0 ]; then secs+=("$WALL"); rss+=("$(rss_kb "$LOG")"); fi
    rm -f "$LOG"
  done
  python3 - "$label" "$n" "$start" "${secs[*]}" "${rss[*]}" >> "$RESULTS" <<'PY'
import sys, statistics
label, lines, start = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
secs = [float(x) for x in sys.argv[4].split()]
rss  = [float(x) for x in sys.argv[5].split()]
proc = [s - start for s in secs]                     # startup-subtracted
# With high-resolution timing the timer floor is a non-issue, but the
# startup-subtraction is only well-conditioned when processing time exceeds the
# startup we subtracted (and isn't trivially short). Otherwise the input is too
# small — fall back to end-to-end and flag it (raise LINES) rather than divide
# by a noisy near-zero difference.
if min(proc) > 0.1 and min(proc) > start:
    eps = [lines / p for p in proc]
    tput, sd = f"{statistics.mean(eps):,.0f}", f"±{statistics.pstdev(eps):,.0f}"
else:
    ee = [lines / s for s in secs]
    tput, sd = f"~{statistics.mean(ee):,.0f} (raise LINES)", "n/a"
print(f"| {label} | {tput} | {sd} | {max(rss)/1024:,.0f} | {start:.2f} |")
PY
  echo "  done $label"
}

echo "FerroStash: $("$FS" --version 2>/dev/null | head -1)"
[ -n "$LS_HOME" ] && echo "Logstash:   $("$LS_HOME/bin/logstash" --version 2>/dev/null | tail -1)" || echo "Logstash:   (LS_HOME unset — FerroStash-only run)"
echo "lines=$LINES (custom=$CUSTOM_LINES) runs=$RUNS workers=$WORKERS host=$(uname -m) cores=$(nproc)"
echo

RESULTS="$PD/results.md"
: > "$RESULTS"
echo "| workload | throughput (ev/s) | stddev | peak RSS (MB) | cold-start (s) |" >> "$RESULTS"
echo "|---|---|---|---|---|" >> "$RESULTS"

for f in grok dissect json kv csv; do
  fmt=accesslog; case "$f" in json) fmt=json;; kv) fmt=kv;; csv) fmt=csv;; esac
  measure "FS $f"        fs "$CONF/filter_$f.fs.conf" "$fmt"
  measure "LS $f"        ls "$CONF/filter_$f.ls.conf" "$fmt"
done
measure "FS native(grok+mutate)" fs "$CONF/native.fs.conf" accesslog
measure "LS native(grok+mutate)" ls "$CONF/native.ls.conf" accesslog
measure "FS custom: script(JIT)" fs "$CONF/custom_script.fs.conf" accesslog "$CUSTOM_LINES"
measure "FS custom: ruby(mruby)" fs "$CONF/custom_ruby.fs.conf"   accesslog "$CUSTOM_LINES"
measure "LS custom: ruby(JRuby)" ls "$CONF/custom_ruby.ls.conf"   accesslog "$CUSTOM_LINES"

echo; echo "================ RESULTS ================"; cat "$RESULTS"
echo; echo "(saved to $RESULTS)"
