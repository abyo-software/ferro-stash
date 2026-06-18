#!/bin/bash
# Run spec with stdout/stderr fully redirected to avoid Tl stopped state
SPEC_NAME="$1"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$HOME/logstash/vendor/jruby/bin:$PATH"

rm -rf ~/logstash/data
mkdir -p ~/logstash/data

cd ~/logstash
nohup bash -c "
exec > /tmp/result_${SPEC_NAME}.txt 2>&1
timeout 360 ./gradlew :logstash-integration-tests:integrationTests \
  -x copyEs -x copyFilebeat -x checkEsSHA -x downloadEs \
  -PrubyIntegrationSpecs='specs/${SPEC_NAME}.rb' \
  -PintegrationTests.rerun=true \
  --console=plain
echo 'GRADLE_EXIT:' \$?
touch /tmp/done_${SPEC_NAME}
" < /dev/null > /dev/null 2>&1 &
disown
echo "Started PID: $!"
