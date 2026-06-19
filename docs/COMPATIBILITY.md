<!-- SPDX-License-Identifier: Apache-2.0 -->
# Logstash 9.4.2 compatibility matrix

**Short answer: FerroStash is not feature-complete with Logstash.** It
implements the production-common subset — **~63% of the plugins bundled with
Logstash 9.4.2** (70 of 111), heavily weighted toward the parsing/filtering hot
path. Connector breadth (AWS, JDBC, enterprise messaging) is the main gap.

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
| Inputs   | 34  | 15 | **44%**  |
| Outputs  | 23  | 10 | **43%**  |
| **Total**| **111** | **70** | **~63%** |

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

## Inputs — 15/34

**Covered:** beats (also serves `elastic_agent`), dead_letter_queue,
elasticsearch, file, generator, heartbeat, http, kafka, pipeline (Logstash's
`logstash` integration input), redis, s3, stdin, syslog, tcp, udp

**Missing:** `azure_event_hubs`, `cloudwatch`, `couchdb_changes`,
`elastic_serverless_forwarder`, `exec`, `ganglia`, `gelf`, `graphite`,
**`http_poller`**, **`jdbc`**, `jms`, `pipe`, `rabbitmq`, `snmp`, `snmptrap`,
**`sqs`**, `twitter`, `unix`

The notable absences for migrations are **`jdbc`** (database ingestion),
**`http_poller`**, and the AWS pull inputs (**`sqs`**, `cloudwatch`).

## Outputs — 10/23

**Covered:** elasticsearch (alias: opensearch / ferrosearch), file, http, kafka,
null, pipeline (Logstash `logstash` integration output), redis, s3, stdout, tcp

**Missing:** `cloudwatch`, `csv`, `email`, `graphite`, **`jdbc`**, `lumberjack`,
`nagios`, `pipe`, `rabbitmq`, `sns`, `sqs`, `udp`, `webhdfs`

**Beyond Logstash (FerroStash extras):** `datadog`

The notable absences are the AWS push outputs (**`sqs`**, `sns`, `cloudwatch`),
`email`, `rabbitmq`, and — minor but easy to hit — `udp` and `csv` outputs.

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

## Roadmap signal

The cheapest high-value additions for migration coverage are the connectors most
real pipelines depend on: `jdbc` (input + output), `http_poller` input, the AWS
`sqs` / `sns` / `cloudwatch` set, and the `udp` / `csv` / `email` outputs. File
an issue if one of these blocks you — it helps prioritise.
