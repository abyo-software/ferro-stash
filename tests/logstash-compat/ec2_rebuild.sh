#!/bin/bash
set -e
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$HOME/.cargo/bin:$PATH"

pkill -9 -f "/home/ubuntu/logstash/bin/logstash" 2>/dev/null || true
sleep 1

cd ~ && rm -rf ferro-stash && tar xzf /tmp/ferro-stash-src.tar.gz && cd ferro-stash
LS_VERSION=$(grep "^logstash:" ~/logstash/versions.yml | awk '{print $2}')
LOGSTASH_COMPAT_VERSION="$LS_VERSION" cargo build --release --package ferro-stash 2>&1 | tail -3

cp target/release/ferro-stash ~/logstash/bin/logstash
chmod +x ~/logstash/bin/logstash

echo "BUILD_DONE"
touch /tmp/build_done
