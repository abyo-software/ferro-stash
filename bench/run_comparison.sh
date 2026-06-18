#!/bin/bash
# FerroStash vs Logstash benchmark comparison
# Run from ferro-stash project root

set -e

LOGSTASH_HOME="D:/dev/logstash-8.17.0"
FERRO_BIN="./target/release/ferro-stash.exe"
INPUT_FILE="/tmp/bench_input.log"
LINES=$(wc -l < "$INPUT_FILE")

echo "============================================"
echo "FerroStash vs Logstash Benchmark"
echo "============================================"
echo "Input: $INPUT_FILE ($LINES lines, $(du -h $INPUT_FILE | cut -f1))"
echo "CPU: $(nproc 2>/dev/null || echo 'N/A') cores"
echo "Date: $(date)"
echo ""

# ---- FerroStash ----
echo "=== FerroStash ==="
echo "Binary: $FERRO_BIN ($(ls -lh $FERRO_BIN | awk '{print $5}'))"

echo "--- Run 1 (warmup) ---"
time $FERRO_BIN -f bench/ferrostash_bench.conf -l warn -w 12 2>/dev/null
echo ""

echo "--- Run 2 (measured) ---"
time $FERRO_BIN -f bench/ferrostash_bench.conf -l warn -w 12 2>/dev/null
echo ""

echo "--- Run 3 (measured) ---"
time $FERRO_BIN -f bench/ferrostash_bench.conf -l warn -w 12 2>/dev/null
echo ""

# ---- Logstash ----
echo "=== Logstash 8.17.0 (tuned) ==="
echo "JVM: -Xms4g -Xmx4g, workers=12, batch_size=500"
echo ""

echo "--- Run 1 (includes JVM warmup) ---"
time "$LOGSTASH_HOME/bin/logstash.bat" \
  -f "$(pwd)/bench/logstash_bench.conf" \
  --pipeline.workers 12 \
  --pipeline.batch.size 500 \
  --pipeline.batch.delay 5 \
  --log.level warn \
  --path.data "/tmp/logstash_bench_data" \
  2>/dev/null
echo ""

echo "--- Run 2 ---"
time "$LOGSTASH_HOME/bin/logstash.bat" \
  -f "$(pwd)/bench/logstash_bench.conf" \
  --pipeline.workers 12 \
  --pipeline.batch.size 500 \
  --pipeline.batch.delay 5 \
  --log.level warn \
  --path.data "/tmp/logstash_bench_data2" \
  2>/dev/null
echo ""

echo "============================================"
echo "Done"
echo "============================================"
