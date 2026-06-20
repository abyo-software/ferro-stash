<!-- SPDX-License-Identifier: Apache-2.0 -->
# Logstash 9.4.2 compatibility matrix

**Short answer: FerroStash is not feature-complete with Logstash.** It
implements the production-common subset — **~83% of the plugins bundled with
Logstash 9.4.2** (92 of 111), heavily weighted toward the parsing/filtering hot
path. Connector breadth (AWS, enterprise messaging) is the main gap.

Two distinct claims, don't conflate them:

- **Behaviour parity (verified):** for the filters covered by the
  [parity fixtures](../tests/logstash-compat/), FerroStash output is
  **byte-for-byte identical** to Logstash 9.4.2 (24/24 fixtures, ~17 filters).
- **Plugin coverage (this document):** *which* Logstash 9.4.2 plugins exist at
  all in FerroStash. This is plugin-level, **not** option-level — a "covered"
  plugin may still implement only a subset of that plugin's Logstash options
  (per-plugin residuals are in the README [Honest limitations](../README.md#honest-limitations)).

How measured: Logstash side = `bin/logstash-plugin list` inside the
`docker.elastic.co/logstash/logstash:9.4.2` image (integration bundles expanded
to the standalone plugins they provide); FerroStash side = the
`create_input/filter/output/codec` factory registries in source.

## Summary

| Category | Logstash 9.4.2 | FerroStash | Coverage |
|----------|---------------:|-----------:|:--------:|
| Codecs   | 19  | 19 | **100%** |
| Filters  | 35  | 32 | **91%**  |
| Inputs   | 34  | 22 | **65%**  |
| Outputs  | 23  | 19 | **83%**  |
| **Total**| **111** | **92** | **~83%** |

The hot path most pipelines actually use — `grok` / `dissect` / `kv` / `json` /
`mutate` / `date` / parse-and-route — is well covered. What's thin is the long
tail of source/sink connectors.

## Filters — 32/35

**Covered:** aggregate, anonymize, cidr, clone, csv, date, de_dot, dissect, dns,
drop, elasticsearch, fingerprint, geoip, grok, http, json, kv, **memcached**,
metrics, mutate, prune, ruby, sleep, split, syslog_pri, throttle, translate,
truncate, urldecode, useragent, uuid, xml

**Missing:** `elastic_integration`, `jdbc_static`, `jdbc_streaming`

**Beyond Logstash (FerroStash extras):** `script` / `painless` (native
Painless-subset scripting — the fast alternative to `ruby`), `bytes`,
`json_encode`

## Inputs — 22/34

**Covered:** beats (also serves `elastic_agent`), **cloudwatch**,
dead_letter_queue, elasticsearch, file, **gelf**, generator, **graphite**,
heartbeat, http, **http_poller**, **jdbc**, kafka, pipeline (Logstash's
`logstash` integration input), **rabbitmq**, redis, s3, **sqs**, stdin, syslog,
tcp, udp

**Missing:** `azure_event_hubs`, `couchdb_changes`,
`elastic_serverless_forwarder`, `exec`, `ganglia`,
`jms`, `pipe`, `snmp`, `snmptrap`, `twitter`, `unix`

The remaining absences are mostly enterprise/niche sources (`jms`,
`azure_event_hubs`, `snmp`). _(`http_poller`, `sqs`, `jdbc`: added — Wave 1.
`rabbitmq`, `cloudwatch`: added — Wave 3b.)_ Note the `jdbc_static` /
`jdbc_streaming` *filters* remain missing — those are separate plugins.

## Outputs — 19/23

**Covered:** **cloudwatch**, **csv**, elasticsearch (alias: opensearch /
ferrosearch), **email**, file, **graphite**, http, **jdbc**, kafka, null,
pipeline (Logstash `logstash` integration output), **rabbitmq**, redis, s3,
**sqs**, **sns**, stdout, tcp, **udp**

**Missing:** `lumberjack`, `nagios`, `pipe`, `webhdfs`

**Beyond Logstash (FerroStash extras):** `datadog`

The remaining absences are niche sinks (`lumberjack`, `nagios`, `webhdfs`).
_(`sqs`, `sns`, `jdbc`: added — Wave 1. `udp`, `csv`: added — Wave 2.
`rabbitmq`, `email`, `cloudwatch`: added — Wave 3b.)_

## Codecs — 19/19

**Covered:** avro, cef, cloudfront, cloudtrail, collectd, dots, edn, edn_lines,
es_bulk, fluent, graphite, json, json_lines, line, msgpack, multiline, netflow,
plain, rubydebug

**Beyond Logstash:** `protobuf`, `nmap`, `csv`, plus internal `bytes` / `data` /
`ruby` / `script` codecs.

## Honest caveats

- **Plugin-level, not option-level.** Coverage above counts whether a plugin
  exists, not whether every Logstash option of that plugin is implemented. Some
  covered plugins are partial — see the README
  [Honest limitations](../README.md#honest-limitations) for per-plugin residuals
  (e.g. kafka `consumer_threads`, redis ACL/TLS, s3 sincedb).
- **`grok` pattern library.** The common pattern set is supported; the full
  upstream pattern catalogue is not guaranteed complete.
- **A `pipeline.conf` that uses a missing plugin fails fast** at config load with
  an "unknown … plugin" error — it does not silently drop. Check this matrix
  before migrating a pipeline that leans on a connector in the *Missing* lists.

## Gap-closure plan

Closing the connector gap toward broad coverage, in priority waves. Each new
plugin keeps Logstash config-key compatibility and ships with compile + unit
tests plus an `#[ignore]` live smoke test (LocalStack for AWS, a local DB for
JDBC, etc.), matching the existing connector discipline.

| Wave | Plugins | Notes / infra |
|------|---------|---------------|
| **1 — migration blockers** | `http_poller` input · `sqs` input/output · `sns` output · `jdbc` input/output | The connectors real pipelines most depend on. http_poller = reqwest (no infra); sqs/sns = AWS SDK (LocalStack); jdbc = native Rust drivers via `sqlx` (Postgres/MySQL/SQLite/MSSQL), not a Java JDBC driver |
| **2 — cheap, no external infra** | filters `cidr` · `uuid` · `syslog_pri` · `anonymize`; outputs `udp` · `csv` | Pure logic / reuse — quick coverage bumps |
| **3 — messaging & web** | `rabbitmq` in/out · `gelf` input · `graphite` in/out · `email` output · `http` filter · `memcached` filter · `cloudwatch` in/out | Each needs its client lib + a local service for the live smoke |
| **4 — heavy / niche** | `snmp`/`snmptrap` · `jms` · `azure_event_hubs` · `webhdfs` · `lumberjack` · `nagios` · `twitter` · … | Case-by-case as demand warrants |

Status (this branch): **Waves 1 & 2 complete, Wave 3 complete (3a + 3b)** — Wave 1: `http_poller`, `sqs` in/out, `sns` out, `jdbc` in/out done; Wave 2: filters `cidr` / `uuid` / `syslog_pri` / `anonymize` and outputs `udp` / `csv` done; **Wave 3a** (the lighter, no-heavy-dep subset of Wave 3): `http` filter, `gelf` input, `graphite` in/out done; **Wave 3b** (the heavy-dependency subset): `rabbitmq` in/out (lapin), `email` output (lettre/rustls), `memcached` filter (memcache via spawn_blocking), `cloudwatch` in/out (aws-sdk-cloudwatch) done. (`jdbc_static` / `jdbc_streaming` *filters* are a separate, still-missing pair.) Next up: Wave 4 heavy/niche connectors (`snmp`/`snmptrap`, `jms`, `azure_event_hubs`, `webhdfs`, `lumberjack`, `nagios`, …). File an issue to bump a plugin's
priority.
