#!/usr/bin/env bash
# EC2 setup for Logstash E2E tests against FerroStash
# Run on Ubuntu 24.04 ARM64 (c7g.xlarge)
set -euo pipefail

echo "=== Installing dependencies ==="
sudo apt-get update -qq
sudo apt-get install -y -qq openjdk-17-jdk git curl unzip build-essential

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc

echo "=== Installing Rust (for cross-compile) ==="
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

echo "=== Cloning Logstash ==="
cd ~
if [ ! -d logstash ]; then
    git clone --depth 1 --branch v9.0.0 https://github.com/elastic/logstash.git logstash || \
    git clone --depth 1 https://github.com/elastic/logstash.git logstash
fi

echo "=== Installing JRuby ==="
JRUBY_VERSION="9.4.12.0"
if [ ! -d /opt/jruby ]; then
    curl -sL "https://repo1.maven.org/maven2/org/jruby/jruby-dist/${JRUBY_VERSION}/jruby-dist-${JRUBY_VERSION}-bin.tar.gz" -o /tmp/jruby.tar.gz
    sudo mkdir -p /opt/jruby
    sudo tar -xzf /tmp/jruby.tar.gz -C /opt/jruby --strip-components=1
fi
export PATH="/opt/jruby/bin:$PATH"
echo 'export PATH="/opt/jruby/bin:$PATH"' >> ~/.bashrc

jruby --version
echo "=== Installing JRuby gems ==="
jruby -S gem install childprocess rspec stud rspec-wait manticore pry flores rubyzip bigdecimal --no-document

echo "=== Setup complete ==="
java -version 2>&1
jruby --version
rustc --version
