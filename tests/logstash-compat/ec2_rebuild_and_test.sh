#!/bin/bash
# Rebuild ferro-stash and run all specs
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$HOME/.cargo/bin:$HOME/logstash/vendor/jruby/bin:$PATH"

# Kill any leftover
kill $(pgrep -f "^/home/ubuntu/logstash/bin/logstash") 2>/dev/null
sleep 1

# Rebuild
cd ~ && rm -rf ferro-stash && tar xzf /tmp/ferro-stash-src.tar.gz && cd ferro-stash
LS_VERSION=$(grep "^logstash:" ~/logstash/versions.yml | awk '{print $2}')
LOGSTASH_COMPAT_VERSION="$LS_VERSION" cargo build --release --package ferro-stash 2>&1 | tail -3
cp target/release/ferro-stash ~/logstash/bin/logstash
chmod +x ~/logstash/bin/logstash

# Run specs
> /tmp/all_results_v4.txt
for spec in specs/command_line_spec.rb specs/01_logstash_bin_smoke_spec.rb specs/monitoring_api_spec.rb; do
  name=$(basename $spec .rb)
  echo "=========== $name ===========" >> /tmp/all_results_v4.txt

  rm -rf ~/logstash/data; mkdir -p ~/logstash/data
  kill $(pgrep -f "^/home/ubuntu/logstash/bin/logstash") 2>/dev/null
  sleep 1

  cd ~/logstash
  timeout 360 ./gradlew :logstash-integration-tests:integrationTests \
    -x copyEs -x copyFilebeat -x checkEsSHA -x downloadEs \
    -PrubyIntegrationSpecs="$spec" \
    -PintegrationTests.rerun=true \
    --console=plain >> /tmp/all_results_v4.txt 2>&1

  echo "GRADLE_EXIT: $?" >> /tmp/all_results_v4.txt
done

touch /tmp/done_v4
