#!/bin/bash
# Run all Logstash E2E specs sequentially with nohup redirection
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$HOME/logstash/vendor/jruby/bin:$PATH"

rm -f /tmp/done_final /tmp/final_summary.txt
> /tmp/final_summary.txt

SPECS=(
  "command_line_spec"
  "01_logstash_bin_smoke_spec"
  "env_variables_config_spec"
  "monitoring_api_spec"
  "multiple_pipeline_spec"
  "reload_config_spec"
)

for spec in "${SPECS[@]}"; do
  echo "=========== $spec ===========" >> /tmp/final_summary.txt

  rm -rf ~/logstash/data
  mkdir -p ~/logstash/data
  pkill -9 -f "/home/ubuntu/logstash/bin/logstash" 2>/dev/null || true
  sleep 1

  cd ~/logstash
  timeout 480 ./gradlew :logstash-integration-tests:integrationTests \
    -x copyEs -x copyFilebeat -x checkEsSHA -x downloadEs \
    -PrubyIntegrationSpecs="specs/${spec}.rb" \
    -PintegrationTests.rerun=true \
    --console=plain > "/tmp/result_final_${spec}.txt" 2>&1

  pass=$(grep -cE "\[32m " "/tmp/result_final_${spec}.txt" || echo 0)
  fail=$(grep -cE "\[31m " "/tmp/result_final_${spec}.txt" || echo 0)
  echo "PASS: $pass, FAIL: $fail" >> /tmp/final_summary.txt
  grep -E "\[3[12]m " "/tmp/result_final_${spec}.txt" | grep -vE "INFO|WARN|starting|Logstash|pipeline|filter|monitoring|configuration|TCP|stdin|input|received|Using|Sending|Failed \(" | sort -u | head -20 >> /tmp/final_summary.txt
  grep -E "[0-9]+ examples?, [0-9]+ failures?" "/tmp/result_final_${spec}.txt" | head -1 >> /tmp/final_summary.txt
  echo "" >> /tmp/final_summary.txt
done

touch /tmp/done_final
