#!/usr/bin/env bash
# Logstash E2E compatibility smoke tests
# Tests that ferro-stash behaves like Logstash when renamed to logstash
set -euo pipefail

LOGSTASH_BIN="${LOGSTASH_BIN:-logstash}"
PASS=0
FAIL=0
SKIP=0

pass() { echo "  PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }
skip() { echo "  SKIP: $1"; SKIP=$((SKIP+1)); }

echo "=== Logstash Compatibility Smoke Tests ==="
echo "Binary: $LOGSTASH_BIN"
echo ""

# -------------------------------------------------------
# Test 1: --version output
# -------------------------------------------------------
echo "--- Test 1: --version ---"
VERSION_OUT=$("$LOGSTASH_BIN" --version 2>&1)
if echo "$VERSION_OUT" | grep -q "^logstash 9\.4\.2$"; then
    pass "--version outputs 'logstash 9.4.2'"
else
    fail "--version output: '$VERSION_OUT'"
fi

# -------------------------------------------------------
# Test 2: --help contains expected strings
# -------------------------------------------------------
echo "--- Test 2: --help ---"
HELP_OUT=$("$LOGSTASH_BIN" --help 2>&1)
if echo "$HELP_OUT" | grep -q "pipeline.id"; then
    pass "--help contains --pipeline.id"
else
    fail "--help missing --pipeline.id"
fi

if echo "$HELP_OUT" | grep -q "logstash \[OPTIONS\]"; then
    pass "--help contains 'logstash [OPTIONS]'"
else
    fail "--help missing 'logstash [OPTIONS]'"
fi

# -------------------------------------------------------
# Test 3: -e inline config with stdin/stdout
# -------------------------------------------------------
echo "--- Test 3: -e inline config ---"
TMPOUT=$(mktemp)
echo '{"message":"hello from e2e test"}' | timeout 5 "$LOGSTASH_BIN" \
    -e 'input { stdin { } } output { stdout { codec => "json" } }' \
    --path.data "$(mktemp -d)" \
    --api.enabled false \
    > "$TMPOUT" 2>&1 || true

# Check that the output contains our message (skip startup lines)
if grep -q "hello from e2e test" "$TMPOUT"; then
    pass "-e inline config processes stdin → stdout"
else
    fail "-e inline config: output does not contain expected message"
    echo "    Output was: $(cat "$TMPOUT")"
fi
rm -f "$TMPOUT"

# -------------------------------------------------------
# Test 4: -f and -e mutual exclusion
# -------------------------------------------------------
echo "--- Test 4: -f and -e mutual exclusion ---"
MUTUAL_OUT=$("$LOGSTASH_BIN" -f /nonexistent -e 'input { stdin {} }' --path.data "$(mktemp -d)" 2>&1 || true)
if echo "$MUTUAL_OUT" | grep -qi "can't be used simultaneously"; then
    pass "-f and -e rejected when used together"
else
    fail "-f/-e mutual exclusion not enforced: '$MUTUAL_OUT'"
fi

# -------------------------------------------------------
# Test 5: --config.test_and_exit with -e
# -------------------------------------------------------
echo "--- Test 5: --config.test_and_exit ---"
TMPCONF=$(mktemp --suffix=.conf)
cat > "$TMPCONF" << 'EOF'
input { stdin { } }
filter { grok { match => { "message" => "%{IP:ip}" } } }
output { stdout { } }
EOF

TEST_OUT=$("$LOGSTASH_BIN" --config.test_and_exit -f "$TMPCONF" --path.data "$(mktemp -d)" 2>&1)
if echo "$TEST_OUT" | grep -q "Configuration OK"; then
    pass "--config.test_and_exit validates config"
else
    fail "--config.test_and_exit output: '$TEST_OUT'"
fi
rm -f "$TMPCONF"

# -------------------------------------------------------
# Test 6: Startup messages (Using bundled JDK / Sending Logstash logs)
# -------------------------------------------------------
echo "--- Test 6: Startup messages ---"
TMPOUT2=$(mktemp)
echo '{}' | timeout 5 "$LOGSTASH_BIN" \
    -e 'input { stdin { } } output { null { } }' \
    --path.data "$(mktemp -d)" \
    --api.enabled false \
    > "$TMPOUT2" 2>&1 || true

if grep -q "Using bundled JDK" "$TMPOUT2"; then
    pass "Startup: 'Using bundled JDK' message present"
else
    fail "Startup: missing 'Using bundled JDK'"
fi

if grep -q "Sending Logstash logs to" "$TMPOUT2"; then
    pass "Startup: 'Sending Logstash logs to' message present"
else
    fail "Startup: missing 'Sending Logstash logs to'"
fi
rm -f "$TMPOUT2"

# -------------------------------------------------------
# Test 7: Monitoring API (GET / with id field)
# -------------------------------------------------------
echo "--- Test 7: Monitoring API ---"
TMPDATA=$(mktemp -d)
# Use generator input (no count = infinite) so the pipeline stays alive
timeout 15 "$LOGSTASH_BIN" \
    -e 'input { generator { interval => 1000 } } output { null { } }' \
    --path.data "$TMPDATA" \
    --api.enabled true \
    --api.http.host "127.0.0.1:19600" \
    > /dev/null 2>&1 &
LS_PID=$!

# Wait for API to be ready
API_READY=false
for i in $(seq 1 30); do
    if curl -s http://127.0.0.1:19600/ > /dev/null 2>&1; then
        API_READY=true
        break
    fi
    sleep 0.5
done

if $API_READY; then
    ROOT_RESP=$(curl -s http://127.0.0.1:19600/)
    if echo "$ROOT_RESP" | grep -q '"id"'; then
        pass "GET / returns JSON with 'id' field"
    else
        fail "GET / missing 'id' field: $ROOT_RESP"
    fi

    if echo "$ROOT_RESP" | grep -q '"version"'; then
        pass "GET / returns JSON with 'version' field"
    else
        fail "GET / missing 'version' field"
    fi

    STATS_RESP=$(curl -s http://127.0.0.1:19600/_node/stats)
    if echo "$STATS_RESP" | grep -q '"events"'; then
        pass "GET /_node/stats returns events"
    else
        fail "GET /_node/stats missing events: $STATS_RESP"
    fi

    if echo "$STATS_RESP" | grep -q '"jvm"'; then
        pass "GET /_node/stats returns jvm section"
    else
        fail "GET /_node/stats missing jvm"
    fi

    if echo "$STATS_RESP" | grep -q '"flow"'; then
        pass "GET /_node/stats returns flow metrics"
    else
        fail "GET /_node/stats missing flow metrics"
    fi

    HEALTH_RESP=$(curl -s http://127.0.0.1:19600/_health_report)
    if echo "$HEALTH_RESP" | grep -q '"status".*"green"'; then
        pass "GET /_health_report returns green"
    else
        fail "GET /_health_report: $HEALTH_RESP"
    fi

    PLUGINS_RESP=$(curl -s http://127.0.0.1:19600/_node/plugins)
    if echo "$PLUGINS_RESP" | grep -q '"plugins"'; then
        pass "GET /_node/plugins returns plugins list"
    else
        fail "GET /_node/plugins: $PLUGINS_RESP"
    fi
else
    fail "Monitoring API did not start within 15s"
fi

kill $LS_PID 2>/dev/null || true
wait $LS_PID 2>/dev/null || true
rm -rf "$TMPDATA"

# -------------------------------------------------------
# Test 8: Environment variable substitution in config
# -------------------------------------------------------
echo "--- Test 8: Environment variable substitution ---"
TMPOUT3=$(mktemp)
export FERROSTASH_E2E_MSG="env-var-test-message"
echo '{"message":"trigger"}' | timeout 5 "$LOGSTASH_BIN" \
    -e 'input { stdin { } } filter { mutate { add_field => { "env_test" => "${FERROSTASH_E2E_MSG}" } } } output { stdout { codec => "json" } }' \
    --path.data "$(mktemp -d)" \
    --api.enabled false \
    > "$TMPOUT3" 2>&1 || true

if grep -q "env-var-test-message" "$TMPOUT3"; then
    pass "Environment variable \${FERROSTASH_E2E_MSG} expanded in config"
else
    fail "Environment variable not expanded"
    echo "    Output: $(cat "$TMPOUT3")"
fi
unset FERROSTASH_E2E_MSG
rm -f "$TMPOUT3"

# -------------------------------------------------------
# Test 9: --path.data exclusivity
# -------------------------------------------------------
echo "--- Test 9: --path.data exclusivity ---"
skip "--path.data exclusivity (requires Unix flock, skipped on Windows)"

# -------------------------------------------------------
# Summary
# -------------------------------------------------------
echo ""
echo "=== Results ==="
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  SKIP: $SKIP"
echo "  TOTAL: $((PASS + FAIL + SKIP))"

if [ $FAIL -gt 0 ]; then
    echo ""
    echo "SOME TESTS FAILED"
    exit 1
else
    echo ""
    echo "ALL TESTS PASSED"
    exit 0
fi
