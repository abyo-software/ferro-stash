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
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PD="${BENCH_DIR:-/tmp/ferro-bench}"
FS="${FERRO_BIN:-$ROOT/target/release/ferro-stash}"
LS_HOME="${LS_HOME:-}"
CONF="$HERE/configs"
TIME=/usr/bin/time
mkdir -p "$PD"

[ -x "$FS" ] || { echo "FerroStash binary not found: $FS (build: cargo build --release -p ferro-stash --features ruby)"; exit 1; }

gen() { local fmt="$1"; local f="$PD/input_$fmt.log"; [ -f "$f" ] || python3 "$HERE/gen_input.py" "$LINES" --format "$fmt" > "$f"; echo "$f"; }
one_line() { local fmt="$1"; local f="$PD/oneline_$fmt.log"; [ -f "$f" ] || python3 "$HERE/gen_input.py" 1 --format "$fmt" > "$f"; echo "$f"; }

# wall_seconds <logfile-of-time-v>
wall_s() { awk -F': ' '/Elapsed \(wall clock\)/{print $2}' "$1" | python3 -c "import sys;t=sys.stdin.read().strip().split(':');print(sum(float(x)*60**i for i,x in enumerate(reversed(t))))"; }
rss_kb()  { awk -F': ' '/Maximum resident set size/{print $2}' "$1"; }

run_once() { # engine conf input  -> sets RC, writes time -v to $LOG
  local engine="$1" conf="$2" input="$3"
  set +e
  case "$engine" in
    fs) $TIME -v "$FS" -f "$conf" --auto-exit -w "$WORKERS" --api.enabled false -l error < "$input" >/dev/null 2>"$LOG" ;;
    ls) rm -rf "$PD/lsdata"; mkdir -p "$PD/lsdata"
        $TIME -v "$LS_HOME/bin/logstash" -f "$conf" --pipeline.workers "$WORKERS" \
              --path.data "$PD/lsdata" --log.level error < "$input" >/dev/null 2>"$LOG" ;;
  esac
  RC=$?            # /usr/bin/time -v propagates the command's exit code
  set -e
}

fail_row() { # label rc
  echo "| $1 | FAILED (rc=$2) | n/a | n/a | n/a |" >> "$RESULTS"
  echo "  FAILED $1 (rc=$2):"; grep -iE 'error|caused by' "$LOG" 2>/dev/null | head -3
}

measure() { # label engine conf fmt  -> appends a row to $RESULTS
  local label="$1" engine="$2" conf="$3" fmt="$4"
  [ "$engine" = ls ] && [ -z "$LS_HOME" ] && { echo "  skip $label (LS_HOME unset)"; return; }
  local input; input="$(gen "$fmt")"
  # Cold-start: a 1-line run. A non-zero exit here means the config/engine is
  # broken — record FAILED, never a throughput number.
  LOG=$(mktemp); run_once "$engine" "$conf" "$(one_line "$fmt")"
  if [ "$RC" -ne 0 ]; then fail_row "$label" "$RC"; rm -f "$LOG"; return; fi
  local start; start="$(wall_s "$LOG")"; rm -f "$LOG"
  local secs=() rss=()
  for r in $(seq 0 "$RUNS"); do
    LOG=$(mktemp); run_once "$engine" "$conf" "$input"
    if [ "$RC" -ne 0 ]; then fail_row "$label" "$RC"; rm -f "$LOG"; return; fi
    if [ "$r" -gt 0 ]; then secs+=("$(wall_s "$LOG")"); rss+=("$(rss_kb "$LOG")"); fi
    rm -f "$LOG"
  done
  python3 - "$label" "$LINES" "$start" "${secs[*]}" "${rss[*]}" >> "$RESULTS" <<'PY'
import sys, statistics
label, lines, start = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
secs = [float(x) for x in sys.argv[4].split()]
rss  = [float(x) for x in sys.argv[5].split()]
proc = [s - start for s in secs]                     # startup-subtracted
wall_mean = statistics.mean(secs)
# Steady-state is only trustworthy when processing time clearly exceeds both the
# timer floor and the startup we subtracted. Otherwise the input is too small —
# fall back to end-to-end and flag it (raise LINES) instead of emitting garbage.
if min(proc) > 0.2 and min(proc) > 0.05 * wall_mean:
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
echo "lines=$LINES runs=$RUNS workers=$WORKERS host=$(uname -m) cores=$(nproc)"
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
measure "FS custom: script(JIT)" fs "$CONF/custom_script.fs.conf" accesslog
measure "FS custom: ruby(mruby)" fs "$CONF/custom_ruby.fs.conf"   accesslog
measure "LS custom: ruby(JRuby)" ls "$CONF/custom_ruby.ls.conf"   accesslog

echo; echo "================ RESULTS ================"; cat "$RESULTS"
echo; echo "(saved to $RESULTS)"
