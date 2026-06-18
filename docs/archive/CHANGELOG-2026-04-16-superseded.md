<!--
SUPERSEDED (archived 2026-06-18). This was an older, divergent duplicate
changelog (dated 2026-04-16) that lived at docs/CHANGELOG.md. The
authoritative changelog is the repository-root /CHANGELOG.md. The figures
below (670 tests, "8 crates", Logstash 9.3.2 RSpec 31/31, the plugin
counts) are stale and were corrected by the 2026-06-18 docs audit; this
file is retained only for historical record. Do not update it.
-->

# Changelog (ARCHIVED / SUPERSEDED)

All notable changes to Ferro-Stash are documented in this file. Format follows [Keep a Changelog](https://keepachangelog.com/).

## [0.1.0] - 2026-04-16

### Added

- **Pipeline engine**: Async pipeline orchestration on tokio with bounded-channel backpressure and graceful shutdown.
- **Input plugins** (8): stdin, file, tcp, udp, http, beats, kafka, syslog.
- **Filter plugins** (11): grok, mutate, date, json, kv, drop, clone, dissect, ruby, geoip, sleep.
- **Output plugins** (6): stdout, elasticsearch, file, tcp, kafka, http.
- **Codecs** (6): json, plain, rubydebug, line, multiline, msgpack.
- **Configuration**: Full Logstash DSL parser (conditionals, string interpolation, nested blocks) and YAML config support.
- **Ruby filter**: Embedded Artichoke Ruby interpreter for inline and file-based Ruby event transformation.
- **TLS**: rustls-based TLS for all network plugins. No OpenSSL dependency.
- **Safety**: `unsafe_code = "forbid"` workspace-wide (except ruby FFI bridge). `deny.toml` blocks GPL dependencies.
- **Testing**: 670 tests across 8 crates. Zero clippy warnings.
- **Logstash compatibility**: Compatible with Logstash configuration files and plugin option naming. Verified against Logstash 9.3.2 official RSpec suite (31/31 pass).
