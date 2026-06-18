#!/usr/bin/env bash
# Run Logstash E2E integration tests against FerroStash
# Uses Logstash's own bootstrapped test environment (Gradle + JRuby + gems)
# Only bin/logstash is replaced with ferro-stash binary
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$HOME/.cargo/bin:$PATH"

echo "=== Step 1: Build FerroStash ==="
cd ~
rm -rf ferro-stash
tar xzf /tmp/ferro-stash-src.tar.gz
cd ferro-stash

# Read Logstash version from the bootstrapped repo
LS_VERSION=$(grep "^logstash:" ~/logstash/versions.yml | awk '{print $2}')
echo "Logstash version from versions.yml: $LS_VERSION"

# Build with matching version
LOGSTASH_COMPAT_VERSION="$LS_VERSION" cargo build --release --package ferro-stash 2>&1 | tail -3
echo "Binary built: $(ls -lh target/release/ferro-stash)"
./target/release/ferro-stash --version

echo ""
echo "=== Step 2: Replace bin/logstash with FerroStash ==="
cd ~/logstash

# Backup original bin/logstash
cp bin/logstash bin/logstash.orig.rb 2>/dev/null || true

# Create a wrapper script that invokes ferro-stash binary
# Logstash's bin/logstash is a Ruby script that starts the JVM.
# We replace it with a shell script that runs ferro-stash directly.
cat > bin/logstash << 'WRAPPER'
#!/usr/bin/env bash
exec ~/ferro-stash/target/release/ferro-stash "$@"
WRAPPER
chmod +x bin/logstash

# Verify
bin/logstash --version

echo ""
echo "=== Step 3: Run integration tests ==="
cd ~/logstash/qa/integration

# Use Logstash's bundled JRuby
LS_JRUBY="$HOME/logstash/vendor/jruby/bin/jruby"
if [ ! -f "$LS_JRUBY" ]; then
    echo "Logstash bundled JRuby not found, using system JRuby"
    LS_JRUBY="/opt/jruby/bin/jruby"
fi
echo "Using JRuby: $LS_JRUBY"
$LS_JRUBY --version

# Install test dependencies using Logstash's bundled bundle
LS_BUNDLE="$HOME/logstash/vendor/jruby/bin/jruby -S bundle"

# Set GEM_HOME/GEM_PATH to include Logstash's gems
export GEM_HOME="$HOME/logstash/vendor/bundle/jruby/3.1.0"
export GEM_PATH="$GEM_HOME:$HOME/logstash/vendor/jruby/lib/ruby/gems/shared"

echo ""
echo "--- Test: command_line_spec ---"
$LS_JRUBY -S rspec specs/command_line_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Test: 01_logstash_bin_smoke_spec ---"
$LS_JRUBY -S rspec specs/01_logstash_bin_smoke_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Test: env_variables_config_spec ---"
$LS_JRUBY -S rspec specs/env_variables_config_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Test: monitoring_api_spec ---"
$LS_JRUBY -S rspec specs/monitoring_api_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Test: reload_config_spec ---"
$LS_JRUBY -S rspec specs/reload_config_spec.rb --format documentation 2>&1 || true

echo ""
echo "--- Test: multiple_pipeline_spec ---"
$LS_JRUBY -S rspec specs/multiple_pipeline_spec.rb --format documentation 2>&1 || true

echo ""
echo "=== ALL TESTS COMPLETE ==="
