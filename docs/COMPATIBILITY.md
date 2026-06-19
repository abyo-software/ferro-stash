<!-- SPDX-License-Identifier: Apache-2.0 -->
# Logstash 9.4.2 compatibility matrix

**Short answer: FerroStash is not feature-complete with Logstash.** It
implements the production-common subset — **~68% of the plugins bundled with
Logstash 9.4.2** (76 of 111), heavily weighted toward the parsing/filtering hot
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
| Filters  | 35  | 26 | **74%**  |
| Inputs   | 34  | 18 | **53%**  |
| Outputs  | 23  | 13 | **57%**  |
| **Total**| **111** | **76** | **~68%** |

The hot path most pipelines actually use — `grok` / `dissect` / `kv` / `json` /
`mutate` / `date` / parse-and-route — is well covered. What's thin is the long
tail of source/sink connectors.

## Filters — 26/35

**Covered:** aggregate, clone, csv, date, de_dot, dissect, dns, drop,
elasticsearch, fingerprint, geoip, grok, json, kv, metrics, mutate, prune, ruby,
sleep, split, throttle, translate, truncate, urldecode, useragent, xml

**Missing:** `anonymize`, `cidr`, `elastic_integration`, `http`,
`jdbc_static`, `jdbc_streaming`, `memcached`, `syslog_pri`, `uuid`

**Beyond Logstash (FerroStash extras):** `script` / `painless` (native
Painless-subset scripting — the fast alternative to `ruby`), `bytes`,
`json_encode`

## Inputs — 18/34

**Covered:** beats (also serves `elastic_agent`), dead_letter_queue,
elasticsearch, file, generator, heartbeat, http, **http_poller**, **jdbc**, kafka,
pipeline (Logstash's `logstash` integration input), redis, s3, **sqs**, stdin,
syslog, tcp, udp

**Missing:** `azure_event_hubs`, `cloudwatch`, `couchdb_changes`,
`elastic_serverless_forwarder`, `exec`, `ganglia`, `gelf`, `graphite`,
`jms`, `pipe`, `rabbitmq`, `snmp`, `snmptrap`, `twitter`, `unix`

The notable absences for migrations are now the AWS pull inputs (`cloudwatch`).
_(`http_poller`, `sqs`, `jdbc`: added — Wave 1.)_ Note the `jdbc_static` /
`jdbc_streaming` *filters* remain missing — those are separate plugins.

## Outputs — 13/23

**Covered:** elasticsearch (alias: opensearch / ferrosearch), file, http, **jdbc**,
kafka, null, pipeline (Logstash `logstash` integration output), redis, s3,
**sqs**, **sns**, stdout, tcp

**Missing:** `cloudwatch`, `csv`, `email`, `graphite`, `lumberjack`,
`nagios`, `pipe`, `rabbitmq`, `udp`, `webhdfs`

**Beyond Logstash (FerroStash extras):** `datadog`

The notable absences are `cloudwatch`, `email`, `rabbitmq`, and — minor but easy
to hit — `udp` and `csv` outputs. _(`sqs`, `sns`, `jdbc`: added — Wave 1.)_

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

Status (this branch): **Wave 1 complete** — `http_poller`, `sqs` in/out, `sns` out, `jdbc` in/out done. (`jdbc_static` / `jdbc_streaming` *filters* are a separate, still-missing pair.) Next up: Wave 2. File an issue to bump a plugin's
priority.
