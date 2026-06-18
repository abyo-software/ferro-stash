# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting Vulnerabilities

Report security vulnerabilities to **security@ferrosearch.dev**. Do not open public issues for security reports.

We will acknowledge receipt within 48 hours and provide an initial assessment within 7 business days. Critical vulnerabilities receive patches within 30 days of confirmed triage.

If you believe the vulnerability is actively exploited, include "URGENT" in the subject line.

## Security Design Principles

### Memory Safety

- `unsafe_code = "deny"` is enforced workspace-wide via `Cargo.toml` workspace lints (relaxed from `forbid` only so it can be overridden per crate).
- Two crates opt in: `ferro-stash-ruby` (FFI with the Artichoke/mruby C API) and `ferro-script` (Cranelift JIT FFI). All unsafe blocks are confined to those boundaries.
- `unwrap_used = "deny"` is enforced; fallible paths return `Result`.

### Dependency Auditing

- `deny.toml` blocks GPL-licensed dependencies and known-vulnerable crates.
- `cargo deny check` runs in CI on every pull request.
- Dependencies are reviewed for license compatibility (Apache-2.0) and supply chain risk.

### No OpenSSL

Ferro-Stash uses **rustls** for all TLS operations. There is no OpenSSL dependency, eliminating an entire class of C memory-safety vulnerabilities.

## TLS Configuration

Network-facing plugins that are functional (tcp, http, beats, elasticsearch)
support TLS via rustls (the `kafka`/`redis`/`s3` plugins are stubs and do
not establish real connections — see the Compatibility Matrix):

- **Protocol**: TLS 1.2 and 1.3 only. TLS 1.0 and 1.1 are not supported.
- **Certificate verification**: Enabled by default. Can be configured with custom CA bundles.
- **Client certificates**: Supported for mutual TLS (mTLS) authentication.
- **Configuration**: Each plugin accepts `ssl_certificate`, `ssl_key`, and `ssl_certificate_authorities` options, matching the Logstash configuration interface.

## Input Validation

- **Grok patterns**: User-supplied regex patterns are compiled at config load time and rejected if invalid. Regex execution uses the `regex` crate, which guarantees linear-time matching (no ReDoS).
- **Config parsing**: The Logstash DSL parser validates all configuration at startup. Invalid configs are rejected with descriptive error messages before the pipeline starts.
- **Field references**: Bracket-notation field paths (e.g., `[nested][field]`) are validated during parsing.
- **Network inputs**: All network input plugins enforce configurable size limits on incoming data to prevent memory exhaustion.

## Ruby Filter Sandboxing

The Ruby filter uses the Artichoke interpreter, which provides partial sandboxing:

- Ruby code cannot directly access Rust memory or pipeline internals.
- Event data is serialized to Ruby `Hash` objects at the FFI boundary and deserialized back, preventing memory corruption.
- File system and network operations from within Ruby code are restricted by Artichoke's limited standard library implementation.

**Limitation**: The Artichoke sandbox is not a security boundary equivalent to a process-level sandbox or container isolation. Do not run untrusted Ruby code in the ruby filter. Treat ruby filter scripts with the same trust level as the Ferro-Stash configuration itself.

## Known Limitations

1. **Ruby filter is not a full sandbox**: See above. Artichoke restricts but does not fully isolate Ruby execution.
2. **No authentication on HTTP input**: The HTTP input plugin does not implement authentication. Deploy behind a reverse proxy or firewall for production use.
3. **Secrets in config files**: Passwords and API keys in Logstash DSL or YAML configs are stored in plaintext. Use environment variable interpolation (`${ENV_VAR}`) and restrict file permissions on config files.
4. **No audit logging**: Ferro-Stash does not currently produce an audit log of configuration changes or administrative actions.
