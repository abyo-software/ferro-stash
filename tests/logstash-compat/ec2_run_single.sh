#!/bin/bash
# Run a single spec — argument: spec name (e.g. command_line_spec)
set +e
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$HOME/logstash/vendor/jruby/bin:$PATH"

SPEC_NAME="$1"
if [ -z "$SPEC_NAME" ]; then
  echo "Usage: $0 <spec_name>"
  exit 1
fi

# Clean up any stale ferro-stash processes (safely — no $() with possibly empty output)
pkill -f "/home/ubuntu/logstash/bin/logstash" >/dev/null 2>&1 || true
sleep 2

rm -rf ~/logstash/data
mkdir -p ~/logstash/data

cd ~/logstash
timeout 360 ./gradlew :logstash-integration-tests:integrationTests \
  -x copyEs -x copyFilebeat -x checkEsSHA -x downloadEs \
  -PrubyIntegrationSpecs="specs/${SPEC_NAME}.rb" \
  -PintegrationTests.rerun=true \
  --console=plain > "/tmp/result_${SPEC_NAME}.txt" 2>&1

echo "GRADLE_EXIT: $?" >> "/tmp/result_${SPEC_NAME}.txt"
touch "/tmp/done_${SPEC_NAME}"
