# Changelog

All notable changes to `ferro-stash` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once `v1.0.0`
is cut. Pre-1.0 releases may introduce breaking changes between minor tags.

## [Unreleased]

### Added

- **DataDog output `site` shorthand** (`us1`/`us3`/`us5`/`eu`/`ap1`/`us1-fed`)
  maps to the correct Log Intake host, so operators no longer need the full
  intake hostname. An explicit `host` still wins (proxy override); an unknown
  `site` is a loud config error (a typo silently defaulting to US1 otherwise
  yields a confusing "API key is invalid" against the wrong region).
- **S3 input now supports `endpoint` / `force_path_style`** for S3-compatible
  object stores (MinIO, LocalStack, Ceph, …), matching the S3 output. Previously
  only the output could target non-AWS stores; the input was AWS-only. This also
  lets the S3 input be validated live against MinIO. Live-validated: S3 in/out
  (MinIO), Kafka in/out (redpanda), Redis in/out, Elasticsearch filter, and the
  DataDog output (real intake) now pass against real services.

- **Real external integrations for the ten formerly-stub connector
  plugins.** The plugins that previously parsed config and ran a
  lifecycle skeleton without any external call now perform real I/O. New
  dependencies: `rdkafka` (input + output kafka), `redis` (input +
  output redis), `aws-config` + `aws-sdk-s3` (input + output s3),
  `maxminddb` (geoip filter), `hickory-resolver` (dns filter). Validation
  status is **not** uniform and is documented honestly per plugin in
  [`README.md`](README.md) "Honest limitations":
  - **kafka input** — `rdkafka` async `StreamConsumer` (subscribe, recv
    loop, codec decode, auto offset commit). *Compile-validated only*;
    live round-trip covered by an `#[ignore]` smoke test (`KAFKA_BROKERS`).
  - **kafka output** — `rdkafka` `FutureProducer` (codec serialize, key
    sprintf, compression/acks/retries, flush). *Compile-validated only*;
    `#[ignore]` smoke test (`KAFKA_BROKERS`).
  - **redis input** — async client: `BLPOP` (list),
    `SUBSCRIBE`/`PSUBSCRIBE` (channel/pattern), `AUTH` + `SELECT`.
    *Compile-validated only.*
  - **redis output** — async `ConnectionManager`: `RPUSH` (list) /
    `PUBLISH` (channel). *Compile-validated only.*
  - **s3 input** — `aws-sdk-s3` paginated `ListObjectsV2` + `GetObject`
    poll, in-memory seen-key dedup, optional `delete_after_read`.
    *Compile-validated only.*
  - **s3 output** — `aws-sdk-s3` `PutObject` on rotation/flush, gzip when
    `encoding => "gzip"`. *Validated against a local mock S3 server.* New
    config fields **`endpoint`** and **`force_path_style`** for MinIO /
    LocalStack / other S3-compatible stores.
  - **datadog output** — `reqwest` POST to `/api/v2/logs` (`DD-API-KEY`,
    batched, retry/backoff). *Validated against a local mock HTTP server.*
  - **geoip filter** — `maxminddb` lookups against a configured `.mmdb`,
    full Logstash-style subfields, falling back to
    private/loopback/public classification when no database is set.
    *Live-validated against a real GeoLite2-City database.* New config
    field **`database`** (path to the GeoLite2/GeoIP2 database).
  - **dns filter** — `hickory-resolver` forward (A/AAAA) and reverse
    (PTR) lookups, custom `nameserver`, `Replace`/`Append` action.
    *Live-validated against `8.8.8.8`.*
  - **elasticsearch filter** — `reqwest` `_search` with host failover,
    query-template sprintf, hits→field mapping. *Validated against a local
    mock HTTP server, not against a live Elasticsearch cluster.*

### Changed

- **Security: bumped `hickory-resolver` 0.25 → 0.26** (dns filter), clearing
  RUSTSEC-2026-0118 (NSEC3 closest-encloser unbounded loop) and
  RUSTSEC-2026-0119 (O(n²) name compression). The 0.26 resolver-builder API
  change is internal to the dns filter. The remaining `rustls-webpki` 0.101
  advisories (RUSTSEC-2026-0098/0099/0104) come only from the AWS SDK's legacy
  TLS connector — the latest `aws-smithy-http-client` still ships rustls 0.21
  and there is no upstream fix; they are not reachable for S3/STS/SSO endpoint
  TLS, so they are documented and ignored in `deny.toml` / the CI audit step
  (revisit when the AWS SDK's default connector moves to rustls 0.23).
- **Persistent queue is now at-least-once, not just a durable buffer.**
  `queue.type: persisted` previously checkpointed the read cursor when an
  event was *dequeued for processing*, so an event read but not yet
  delivered was lost on a crash. The queue now splits the in-memory pop
  cursor (`read_seq`) from a durable acknowledgement cursor (`ack_seq`)
  that advances **only after the output delivers** the event (or it is
  dropped by a filter / captured by the DLQ). Events popped but not
  delivered before a crash **replay** on restart. The engine tracks
  per-entry fan-out (clone/split/drop) so a queue entry is acknowledged
  only once all of its derived events are terminal. Trade-off: at-least-once
  implies **duplicates** are possible on replay (make outputs idempotent
  for exactly-once needs); a persistently failing output with no DLQ backs
  the queue up rather than dropping. See [`README.md`](README.md) "Honest
  limitations". New API: `PersistentQueue::pop_with_seq` / `dequeue_with_seq`
  and `SharedPersistentQueue::{pop_with_seq, ack}`. To make the durable cursor
  safe, supporting changes: `send_batch` now attempts **every** output (it no
  longer aborts the whole batch after the first failing output, which would
  have acked entries the later outputs never received); the DLQ `write` now
  returns `Result<bool>` (`true` = durably persisted+flushed, `false` =
  dropped because full) and flushes per write, so an entry is acked on a
  delivery/filter failure only when its DLQ capture is durable (a full or
  failing DLQ leaves it un-acked to replay); and filter-error DLQ records now
  store the real event payload instead of a `{"_dlq_filter_error": true}`
  marker.
- **New build requirement: `cmake`.** The kafka plugins pull `rdkafka`,
  which builds a vendored `librdkafka` via CMake, so `cmake` is now
  required at build time (in addition to the existing clang/gcc for the
  Artichoke/mruby FFI). Connector TLS uses rustls, so no system OpenSSL
  is needed. The `geoip` filter additionally needs a user-supplied
  `.mmdb` database file at runtime (not vendored).

### Honest residual limitations

- **kafka input**: `consumer_threads` / `max_poll_records` are parsed but
  not yet wired to behaviour; no SASL/SSL `security.protocol` passthrough;
  auto-commit only.
- **redis (input + output)**: password-only `AUTH` (no `username`/ACL);
  no TLS (`rediss://`); a pub/sub `key` is treated as a single
  channel/pattern (no comma-split list).
- **s3 input**: the seen-key dedup set is in-memory only — non-deleted
  objects are reprocessed after a restart (no sincedb); no
  SQS-notification mode; `delete_after_read` deletes immediately after
  emit.
- **s3 output**: single `PutObject` (no multipart upload) in v1.
- **kafka & redis** produce/consume round-trips are not live-validated
  (compile + `#[ignore]` smoke tests only).
- **elasticsearch filter** is not validated against a real ES cluster
  (mock only).

### Documentation

- **Docs audit (2026-06-18).** README and the docs/ pack were rewritten
  to match source. The previously-documented "8 input / 10 filter / 6
  output / 6 codec" surface was an understatement: the factory functions
  actually register **15 inputs / 29 filters / 11 outputs / 21 codecs**.
  Of those, **10 were production-shaped stubs** (input/output
  kafka/redis/s3, output datadog, filters geoip/dns/elasticsearch) at the
  time of that audit — labelled as such. (Those 10 plugins have since
  gained real external integrations; see the **Added** section above.)
  Corrected: test count (measured **1,165 passing / 16 ignored**, not
  670/1,151), workspace member count (10), `unsafe_code` posture (`deny` +
  2 overrides, not `forbid`), the then-non-existent
  `rdkafka`/`maxminddb`/`aws-sdk` dependency claims (these crates are now
  real dependencies, added with the connector implementations above), the
  `120+` grok-pattern claim (actually ~50), and the Artichoke fork
  description (a filesystem **path** dependency to `artichoke-extended`,
  not a git pin on `integration/logstash-compat`, and not `ferro-artichoke`).

## [0.1.0] — 2026-05-05

First stable release. The Logstash 5.0+ wire-compatible surface
(8 input / 10 filter / 6 output plugins, 6 codecs, Logstash DSL
parser covering the production-common subset of `pipeline.conf`)
is promoted to GA.

### Added

- **24h fuzz wave hardening (2026-05-02).** Three production DoS
  panics surfaced and fixed under `fix(codec,config)`:
  - **Avro OCF block_size signed-varint underflow** → out-of-range
    slice panic in `ferro-stash-codec`. Cap + bounds check landed.
  - **EDN parser hot-loop DoS** — depth cap + skip
    structural-tag stringify (`6934553`).
  - Plus DSL config-side panics caught during the same fuzz wave
    (`d1103a1`).
- **Three cargo-fuzz harnesses** wired (`ae4a75a`):
  - `codec_decode` — cross-codec entry fuzzer.
  - `logstash_dsl_parse` — Logstash DSL grammar.
  - `netflow_decode` — netflow v5 / v9 / IPFIX decoders.
  - Plus a fourth focused `cef_decode` target (`162338d`).
- **JRuby Logstash parity diff harness** (`b0cfbb6`,
  `cbdb144`) — real fixture-driven harness + Rust integration
  test against upstream Logstash 5+ behaviour.
- **5 regression seeds promoted** out of quarantine
  (`35a12cd`) once the matching fixes landed; the
  known-crash directory is now empty.

### Changed

- **`serde_yaml` → `serde_yaml_ng`** (drop-in alias). Upstream
  `serde_yaml` is unmaintained; the workspace uses the
  maintained `serde_yaml_ng` crate via a renamed dependency.
  No source-level changes.
- **CI runner switched** to a self-hosted Linux KVM x86_64
  runner; the GitHub-hosted Mac path was retired for slow
  throughput. Cargo / rustup paths migrated; rustup proxy
  resolved via PATH instead of hardcoded toolchain env.
- **clippy::pedantic disabled workspace-wide** under Rust 1.95
  (`15b2340`, `eca9f0b`). The newer toolchain surfaced dozens
  of cosmetic pedantic lints on existing code that the
  Mac-runner-cached older toolchain treated as silent; the
  group is disabled rather than chasing each call site.
  `unwrap_used` remains `deny`.
- **`unsafe_code` relaxed from `forbid` to `deny`** with
  `ferro-script` (cranelift JIT FFI) and `ferro-stash-ruby`
  (mruby FFI Send impl) opting in via inlined lints
  (`777f9da`, `4b69f3f`, `68fa002`). Both opt-ins are
  documented at the crate boundary.

### Fixed

- **Quarantined known-crash regression seeds** out of the corpus
  pre-fix (`de25f33`), then promoted them back as positive
  regression tests post-fix.
- **`cargo fmt` + `clippy::unreadable_literal`** allow on the
  Hinnant date algorithm (`889802e`); `unreadable_literal +
  similar_names` allow at the evaluator module (`408da93`,
  `934e2d6`); `iter().cloned().collect() -> to_vec()` and doc
  list indent (`68ee7d6`).

### Known limitations

> Plugin-count figures in this historical 0.1.0 entry were corrected by
> the 2026-06-18 docs audit (see Unreleased). The real registered surface
> is 15 input / 29 filter / 11 output / 21 codec; at 0.1.0 ten of those
> were production-shaped stubs. (Those ten have since gained real external
> integrations — see the **Added** section under [Unreleased].) The
> bullets below are retained for historical record and describe the 0.1.0
> state.

- **Plugin counts are scoped, not catalog-complete.** The registered
  surface covers the production-common Logstash configuration; Logstash's
  full plugin catalog (200+ community plugins) is out of scope. Several
  registered plugins are stubs (see [`README.md`](README.md) "Honest
  limitations").
- **Logstash DSL parser** covers the production-common subset of
  `pipeline.conf` syntax (input / filter / output blocks,
  conditionals, `%{field}` interpolation, regex match). Less
  common DSL surface (e.g. obscure operators, the more exotic
  array indexing forms) is not yet exhaustively covered.
- **Ruby filter** is backed by a local Artichoke fork consumed as a
  filesystem **path** dependency (`artichoke-extended`), not a git pin.
  Upstream Artichoke parity is tracked separately and is not the
  load-bearing contract for this release.

[Unreleased]: https://github.com/abyo-software/ferro-stash/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/abyo-software/ferro-stash/releases/tag/v0.1.0
