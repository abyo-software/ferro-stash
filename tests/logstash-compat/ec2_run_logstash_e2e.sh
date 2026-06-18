#!/usr/bin/env bash
# Run Logstash E2E integration tests against FerroStash on EC2
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="/opt/jruby/bin:$HOME/.cargo/bin:$HOME/.local/share/gem/jruby/3.1.0/bin:$PATH"

echo "=== Step 1: Build FerroStash ==="
cd ~
if [ ! -d ferro-stash ]; then
    tar xzf /tmp/ferro-stash-src.tar.gz
fi
cd ferro-stash
cargo build --release --package ferro-stash 2>&1 | tail -5
echo "Binary: $(ls -lh target/release/ferro-stash)"

echo ""
echo "=== Step 2: Create Logstash-compatible directory ==="
LS_HOME="$HOME/logstash-home"
rm -rf "$LS_HOME"
mkdir -p "$LS_HOME/bin" "$LS_HOME/config" "$LS_HOME/data" "$LS_HOME/logs"

# Copy binary as 'logstash'
cp target/release/ferro-stash "$LS_HOME/bin/logstash"
chmod +x "$LS_HOME/bin/logstash"

# Minimal logstash.yml
cat > "$LS_HOME/config/logstash.yml" << 'CONF'
path.data: data
path.logs: logs
CONF

# Verify
"$LS_HOME/bin/logstash" --version

echo ""
echo "=== Step 3: Prepare Logstash test framework ==="
cd ~/logstash

# Stub logstash-core.rb (Java JAR loader) — test specs are untouched
cp logstash-core/lib/logstash-core/logstash-core.rb logstash-core/lib/logstash-core/logstash-core.rb.orig
echo '# Stubbed — no Java JARs needed for E2E binary tests' > logstash-core/lib/logstash-core/logstash-core.rb

# Create versions.yml if not present
if [ ! -f versions.yml ]; then
    cat > versions.yml << 'VER'
logstash: 9.3.2
logstash-core: 9.3.2
logstash-core-plugin-api: 2.1.16
VER
fi

# Point suite.yml to our ferro-stash directory
cd qa/integration
cp suite.yml suite.yml.orig
cat > suite.yml << SUITE
---
verbose_mode: true
ls_home_abs_path: $LS_HOME
feature_flag: <%= ENV['FEATURE_FLAG'] %>
SUITE

echo "suite.yml configured with ls_home_abs_path: $LS_HOME"

echo ""
echo "=== Step 4: Run smoke test ==="
cd ~/logstash/qa/integration

# First, test the simplest spec: command_line_spec
echo ""
echo "--- Running: command_line_spec ---"
jruby -S rspec specs/command_line_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Running: 01_logstash_bin_smoke_spec ---"
jruby -S rspec specs/01_logstash_bin_smoke_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Running: env_variables_config_spec ---"
jruby -S rspec specs/env_variables_config_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Running: monitoring_api_spec ---"
jruby -S rspec specs/monitoring_api_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Running: reload_config_spec ---"
jruby -S rspec specs/reload_config_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Running: multiple_pipeline_spec ---"
jruby -S rspec specs/multiple_pipeline_spec.rb --format documentation 2>&1 || true

echo ""
echo "=== ALL TESTS COMPLETE ==="
