#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$HOME/logstash/vendor/jruby/bin:$PATH"

> /tmp/raw_results.txt

for spec in specs/command_line_spec.rb specs/01_logstash_bin_smoke_spec.rb specs/monitoring_api_spec.rb; do
  name=$(basename $spec .rb)
  echo "=== $name ===" >> /tmp/raw_results.txt

  rm -rf ~/logstash/data; mkdir -p ~/logstash/data
  kill $(pgrep -f "bin/logstash") 2>/dev/null
  sleep 1

  cd ~/logstash
  timeout 600 ./gradlew :logstash-integration-tests:integrationTests \
    -x copyEs -x copyFilebeat -x checkEsSHA -x downloadEs \
    -PrubyIntegrationSpecs="$spec" \
    -PintegrationTests.rerun=true \
    --console=plain >> /tmp/raw_results.txt 2>&1

  echo "GRADLE_EXIT: $?" >> /tmp/raw_results.txt
done

touch /tmp/done_v3
