# Third-Party License Summary

Ferro-Stash is licensed under Apache-2.0. All third-party dependencies use permissive open-source licenses.

## Dependency Licenses

| Dependency | Version | License | Notes |
|------------|---------|---------|-------|
| tokio | 1 | MIT | Async runtime |
| serde | 1 | Apache-2.0 OR MIT | Serialization |
| serde_json | 1 | Apache-2.0 OR MIT | JSON support |
| reqwest | 0.12 | Apache-2.0 OR MIT | HTTP client |
| hyper | 1 | MIT | HTTP library |
| axum | 0.7 | MIT | HTTP framework |
| tracing | 0.1 | MIT | Logging/tracing |
| clap | 4 | Apache-2.0 OR MIT | CLI parsing |
| regex | 1 | Apache-2.0 OR MIT | Regular expressions |
| chrono | 0.4 | Apache-2.0 OR MIT | Date/time |
| rustls | 0.23 | Apache-2.0 OR ISC OR MIT | TLS (no OpenSSL) |
| flate2 | 1 | Apache-2.0 OR MIT | Compression |
| uuid | 1 | Apache-2.0 OR MIT | UUID generation |
| dashmap | 6 | MIT | Concurrent hashmap |
| crossbeam-channel | 0.5 | Apache-2.0 OR MIT | Channel primitives |

## Special: artichoke-backend

- **License**: MIT
- **Status**: Forked (path dependency)
- **Purpose**: Embeds mruby for Logstash Ruby filter compatibility
- **Note**: This is the only dependency that requires a C compiler at build time

## License Policy Enforcement

`deny.toml` enforces the following at build time:

- **GPL-family licenses are blocked** -- any transitive dependency with GPL, LGPL, AGPL, or SSPL will fail the build
- **Advisory database is checked** -- known security vulnerabilities cause build failure
- **Allowed licenses**: Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-DFS-2016, Zlib

Run verification:

```bash
cargo deny check licenses
cargo deny check advisories
```

## Summary

All dependencies are permissively licensed (Apache-2.0, MIT, BSD, ISC). No copyleft or restrictive licenses exist in the dependency tree. This is continuously enforced via `deny.toml`.
