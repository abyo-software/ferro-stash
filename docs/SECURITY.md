# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x     | Yes       |
| < 1.0   | No        |

## Reporting Vulnerabilities

Report security vulnerabilities to **aws-support@abyo.net**. Do not open public issues for security reports.

We will acknowledge receipt within 48 hours and provide an initial assessment within 7 business days. Critical vulnerabilities receive patches within 30 days of confirmed triage.

If you believe the vulnerability is actively exploited, include "URGENT" in the subject line.

## Security Design Principles

### Memory Safety

- `unsafe_code = "deny"` is enforced workspace-wide via `Cargo.toml` workspace lints.
- Two crates opt in narrowly: `ferro-stash-ruby` (FFI with the Artichoke/mruby C API) and `ferro-script` (Cranelift JIT FFI). All unsafe blocks are confined to those boundaries.
- `unwrap_used = "deny"` is enforced; fallible paths return `Result`.

### Dependency Auditing

- `deny.toml` blocks GPL-licensed dependencies and gates known-vulnerable crates.
- `cargo deny check` runs in CI on every pull request.
- Advisory ignores in `deny.toml` are documented with scope/impact notes and should be revisited as upstream dependencies update.
- Dependencies are reviewed for license compatibility and supply-chain risk.

### TLS Configuration

Network-facing plugins use rustls-backed TLS where TLS is implemented. Connector support varies by plugin; see `README.md` and `docs/COMPATIBILITY.md` for exact residuals.

- **Protocol**: TLS 1.2 and 1.3 only where rustls is used. TLS 1.0 and 1.1 are not supported.
- **Certificate verification**: Enabled by default. Some Logstash-compatible Elasticsearch options can disable verification; do not disable it outside controlled test environments.
- **Client certificates**: Supported on the TCP input/output where configured.
- **No OpenSSL requirement**: Runtime TLS paths use rustls and system CA certificates.

### Connector Security Scope

Kafka, Redis, S3, Datadog, GeoIP, DNS, Elasticsearch, SQS, SNS, CloudWatch, RabbitMQ, JDBC, email, and memcached integrations are real implementations rather than stubs. Several live smoke tests are `#[ignore]` and require external services or credentials, so they are not part of default CI.

Known connector residuals include: no Kafka SASL/SSL passthrough yet, Redis password-only AUTH and no `rediss://`, S3 input seen-key state not persisted, single `PutObject` S3 output, and no RabbitMQ TLS URI support yet. Treat plugin configuration as trusted operator input.

## Input Validation

- **Grok patterns**: User-supplied regex patterns are compiled at config load time and rejected if invalid. Regex execution uses the `regex` crate, which guarantees linear-time matching.
- **Config parsing**: The Logstash DSL parser validates configuration at startup. Invalid configs are rejected before the pipeline starts.
- **Field references**: Bracket-notation field paths are validated during parsing.
- **Network inputs**: Network input plugins enforce size limits on incoming data to reduce memory-exhaustion risk.

## Administrative API

The monitoring API is enabled by default on `127.0.0.1:9600` and exposes unauthenticated read-only node/pipeline stats for Logstash compatibility. Keep it on loopback or a trusted network; do not expose it directly to the internet.

Runtime log-level mutation (`PUT /_node/logging` and reset) is disabled by default and only registered when `--api.runtime_logging.enabled=true` is set.

## Ruby Filter Sandboxing

The Ruby filter uses the Artichoke interpreter and is optional/off by default. Marketplace artifacts use the default no-Ruby build.

- Ruby code cannot directly access Rust memory or pipeline internals.
- Event data is serialized to Ruby `Hash` objects at the FFI boundary and deserialized back.
- File system and network operations from Ruby are limited by Artichoke's available standard library.

**Limitation**: The Artichoke sandbox is not a security boundary equivalent to a process-level sandbox or container isolation. Do not run untrusted Ruby code in the ruby filter. Treat ruby filter scripts with the same trust level as the FerroStash configuration itself.

## Known Limitations

1. **Pipeline configuration is trusted code**: `exec` and `pipe` plugins intentionally run operator-supplied shell commands via `sh -c`, matching Logstash-style behavior. Do not allow untrusted users to edit pipeline configs.
2. **Ruby filter is not a full sandbox**: See above.
3. **No authentication on HTTP input**: The HTTP input plugin does not implement authentication. Deploy behind a reverse proxy or firewall for production use.
4. **Secrets in config files**: Passwords and API keys in Logstash DSL or YAML configs are stored in plaintext. Use environment variable interpolation and restrict file permissions on config files.
5. **Monitoring API is not an internet-facing admin API**: Keep port 9600 private; enable runtime logging mutation only in trusted environments.
6. **No audit logging**: FerroStash does not currently produce an audit log of configuration changes or administrative actions.
