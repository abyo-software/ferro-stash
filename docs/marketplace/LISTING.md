<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# FerroStash -- AWS Marketplace Listing Copy

> This document is the canonical source for the AWS Marketplace product
> load form for the **FerroStash (AMI)** product -- a paid Amazon Machine
> Image, priced hourly + annual, metered automatically by AWS (there is
> **no in-product metering code**; AWS attaches the product code to the
> published AMI and meters instance-hours).
>
> The software itself is and remains **Apache-2.0** open source. The paid
> listing sells a hardened, scanned, supported, one-click distribution (a
> standard open-core commercial model). The code stays Apache-2.0 and
> nothing in the listing relicenses it.
>
> **Accuracy contract**: every capability claim below is cross-checked
> against the repository `README.md` (Honest limitations) and
> `docs/COMPATIBILITY.md`. The honest-scope notes in
> [section 8](#8-honest-scope-notes-do-not-delete-before-submit) MUST stay
> in the listing copy or be folded verbatim into the long description.
>
> **Two hard rules for this listing:**
> 1. **No github.com URL anywhere in the listing or support text.** The
>    source repository is private; a github link 404s for the AWS reviewer
>    and gets the listing rejected (the FerroDruid / FerroSCA lesson).
>    Support is **email only**: `aws-support@abyo.net`.
> 2. **Do not position FerroStash as an alternative or replacement to any
>    named AWS service.** "Logstash-compatible" is fine (Logstash is
>    Elastic's product, not an AWS service) -- but describe it as **config /
>    pipeline compatibility**, never "100% compatible" or "drop-in".

---

## 1. Product identity

| Field | Value |
|---|---|
| Product title | **FerroStash - Rust-native, Logstash-compatible log and event pipeline** |
| Seller / vendor | abyo software 合同会社 (abyo software LLC) |
| Product version | v1.0 line (v1.0.0) |
| License model | Apache-2.0 software + paid supported distribution (open-core) |
| Standard contract | AWS Marketplace Standard Contract (SCMP) |
| Support contact | aws-support@abyo.net (email only) |

### Short description (<= ~150 chars)

*"Rust-native, Logstash-compatible log and event pipeline. One static binary, no JVM. 90+ inputs/filters/codecs/outputs; on-disk queue. Hardened AMI."*

---

## 2. Long description

**FerroStash is a Rust-native, Logstash-compatible log and event
pipeline.** It ingests, transforms, and routes events through the same
`input -> filter -> output` model as Logstash, parsing the Logstash
`pipeline.conf` DSL (and an equivalent YAML form) natively -- without a
JVM and without a separate agent runtime.

Where a Logstash pipeline commonly holds about a gigabyte of JVM heap and
takes tens of seconds to start, FerroStash runs as a single static binary
(about 14 MB) that starts in milliseconds and holds tens of MB of RAM, so
you can pack far more shippers per host.

**What FerroStash does today (v1.0 line):**

- **Pipeline compatibility**: parses the Logstash `pipeline.conf` DSL and
  the event model (`@timestamp`, tags, `[a][b]` field references,
  `%{field}` interpolation) natively; a YAML pipeline form is also
  supported. Multi-pipeline (`pipelines.yml`) and automatic config reload
  are supported.
- **Plugin breadth**: about **88% of the plugins bundled with Logstash
  9.x (98 of 111)**, weighted toward the parsing/filtering hot path.
  - **Inputs**: beats, file, tcp, udp, http, http_poller, syslog, kafka,
    redis, s3, sqs, jdbc, elasticsearch, cloudwatch, rabbitmq, exec, pipe,
    unix, generator, heartbeat, stdin, dead_letter_queue, pipeline.
  - **Filters**: grok, dissect, kv, json, mutate, date, geoip, dns, csv,
    xml, useragent, cidr, fingerprint, translate, prune, split, truncate,
    aggregate, throttle, anonymize, syslog_pri, uuid, ruby, and a native
    Painless-style `script` filter (a fast alternative to ruby).
  - **Outputs**: elasticsearch / opensearch, kafka, s3, http, tcp, udp,
    file, redis, sqs, sns, cloudwatch, email, datadog, csv, stdout, null,
    pipeline.
  - **Codecs**: json, json_lines, plain, line, multiline, cef, netflow,
    avro, msgpack, protobuf, and more.
- **Reliability**: an optional on-disk **persistent queue** with full
  **at-least-once delivery** (read/ack cursor separation,
  checkpoint-after-output-ack) and a **dead-letter queue**, with opt-in
  **fsync** for power-loss durability.
- **Observability**: a built-in monitoring API (node + pipeline stats),
  bound to localhost by default.

**Engineering posture (verifiable):** `unsafe_code` is denied
workspace-wide (with narrow, audited exceptions for the optional mruby FFI
and the script-filter JIT), clippy is clean at `-D warnings` with
`unwrap()` denied in production code, an SPDX header is on every source
file, a `cargo deny` supply-chain gate runs in CI, and the test suite runs
**1,400+ tests** with output **verified byte-for-byte against Logstash
9.4.2 across 24 parity fixtures**.

**Honest scope (read before you buy):** FerroStash is Logstash **config /
pipeline compatible, not a byte-identical drop-in**. Coverage is
**plugin-level** (about 88% of bundled plugins), and a covered plugin may
implement only a subset of that plugin's options; a config that uses a
plugin FerroStash does not implement **fails fast at load** with a clear
error rather than silently dropping events. The remaining gaps are mostly
enterprise / niche connectors (for example `jms`, `azure_event_hubs`,
`snmp`, `lumberjack`, `webhdfs`). This is a **single-developer project**
with a SemVer-stable surface but **no public production deployments yet** --
run it beside your existing pipeline before trusting it with irreplaceable
data. The connector live-validation smoke tests verify reachability and a
round-trip against real services, **not** exhaustive conformance. The
optional `ruby` filter (Artichoke/mruby) is **excluded from the default
build**. The supported Marketplace topology is **single-node**.

This listing sells a hardened, scanned, supported, one-click distribution
built from the Apache-2.0 source at a pinned release version. The AMI is
metered automatically by AWS per running instance-hour -- there is no
metering code in the product.

---

## 3. Highlights (EXACTLY 3 -- AWS maximum is 3, plain ASCII)

1. **Rust-native with no JVM:** a single static binary runs Logstash-style
   pipeline configs (DSL or YAML) with low memory use and fast startup.
2. **90+ inputs, filters, codecs and outputs** including grok, mutate,
   JSON, Painless-style scripting, Kafka, S3, and
   Elasticsearch/OpenSearch.
3. **At-least-once delivery** with an on-disk persistent queue and
   dead-letter queue, plus optional fsync durability.

> These three are the exact strings baked into the Catalog-API
> `UpdateInformation` call in `deploy/marketplace-create.sh`. Keep them in
> sync if either changes.

---

## 4. Categories and search keywords

### Categories (AWS Marketplace product categories)

- Primary: **Data Analytics**
- Secondary: **Monitoring**

(Both are in the known-valid AWS category vocabulary; if one errors at
apply time the operator adjusts in the console.)

### Search keywords

`logstash`, `log pipeline`, `observability`, `etl`, `grok`,
`elasticsearch`, `opensearch`, `kafka`, `data pipeline`, `log shipping`,
`rust`, `siem`

---

## 5. "What you get" (purchaser value section)

When you subscribe to the paid FerroStash AMI you get:

- **A hardened, scan-ready distribution.** The AMI is built to AWS
  Marketplace self-service AMI scanning policy: SELinux enforcing,
  fail2ban on sshd, automatic security errata, root SSH + password auth
  disabled, no baked-in SSH keys, and **no default credentials** (the
  service runs unprivileged and has no admin login). Built from the
  Apache-2.0 source at a pinned release version (no `latest`).
- **One-click deployment.** Launch on EC2 (Graviton / arm64); the
  `ferro-stash` systemd service starts on first boot running a default
  pipeline. Point `/etc/ferro-stash/pipeline.conf` at your real inputs and
  outputs and restart.
- **Commercial support** by email under a published SLA at
  `aws-support@abyo.net`.
- **Full transparency** -- a published Logstash compatibility matrix, an
  honest-limitations document, and a due-diligence pack ship with the
  product.

> You are **not** buying a feature the open-source code lacks. You are
> buying a packaged, scanned, supported, one-click distribution plus an
> SLA. The code stays Apache-2.0.

---

## 6. Pricing dimensions (AMI -- hourly + annual, by instance size class)

> **PROPOSED amounts -- the owner confirms before publish.** The AMI is
> metered automatically by AWS per running instance-hour; there is no
> metering code. These are the **software** charges only; AWS bills EC2
> infrastructure (instance, EBS, data transfer) separately. Once Public a
> released price can only be **lowered**, never raised, so the anchor is
> deliberately conservative.

| Instance size class | vCPU | Hourly (USD) | Annual / instance (USD) |
|---|---|---|---|
| `c7g.medium`  | 1 | `$0.03` | `$185`  |
| `t4g.small`   | 2 | `$0.04` | `$240`  |
| `t4g.medium`  | 2 | `$0.05` | `$300`  |
| `c7g.large` (recommended) | 2 | `$0.06` | `$370` |
| `c7g.xlarge`  | 4 | `$0.12` | `$740`  |
| `c7g.2xlarge` | 8 | `$0.24` | `$1,480` |

- **Recommended instance type:** `c7g.large`.
- A **Free Trial** (e.g. 30 days) is recommended for a new product --
  configurable in the console.
- The full per-instance-type ladder is in
  `deploy/marketplace-pricing.sh` (the script that prints the portal rate
  table and, with `APPLY=1`, sets the Catalog-API rate card).

---

## 7. Supported regions and architecture

- **Architecture:** the AMI ships on **ARM64 (AWS Graviton)** for the
  initial product. The Packer build also supports x86_64; the Rust binary
  builds for both from source.
- **Instance types:** `t4g.small`, `t4g.medium`, `t4g.large`,
  `c7g.medium`, `c7g.large`, `c7g.xlarge`, `c7g.2xlarge`, `m7g.large`,
  `m7g.xlarge`, `r7g.large` (recommended `c7g.large`).
- **Regions:** the standard abyo commercial set (see
  `deploy/marketplace-create.sh`): `us-east-1`, `us-east-2`, `us-west-1`,
  `us-west-2`, `ca-central-1`, `sa-east-1`, `eu-west-1`, `eu-west-2`,
  `eu-west-3`, `eu-central-1`, `eu-north-1`, `ap-south-1`,
  `ap-southeast-1`, `ap-southeast-2`, `ap-northeast-1`, `ap-northeast-2`,
  `ap-northeast-3`. Region availability is set at the Catalog
  `AddRegions` step.

---

## 8. Honest-scope notes (DO NOT delete before submit)

These hedges keep the listing consistent with the repository's
`README.md` (Honest limitations) and `docs/COMPATIBILITY.md`. Each is a
deliberate downgrade of a claim a less honest listing might make:

1. **Logstash compatibility**: marketed as "Logstash config / pipeline
   compatible", **never** "100% compatible" or "drop-in". Coverage is
   **plugin-level (~88%, 98 of 111 bundled plugins)**; a covered plugin
   may implement a subset of its options; a config using a missing plugin
   fails fast at load.
2. **No production track record**: single-developer project, **no public
   production deployments yet**. The SemVer-stable surface is a contract on
   API/behaviour stability, not a maturity claim.
3. **Connector validation**: the previously-stubbed connectors (kafka,
   redis, s3, datadog, geoip, dns, elasticsearch) have real integrations
   that are **live-validated via env-gated smoke tests** (reachability +
   round-trip), **not** exhaustive conformance, and those smoke tests are
   **not** part of CI.
4. **Default build excludes `ruby`**: the optional Artichoke/mruby `ruby`
   filter is not in the default build (the native `script` filter is the
   in-build alternative). Do not advertise the ruby filter as shipping in
   the AMI.
5. **Topology**: single-node is the supported Marketplace topology.
6. **No named-AWS-service positioning**: do not describe FerroStash as an
   alternative/replacement to any named AWS service. "Logstash-compatible"
   (Elastic's product) is the only compatibility claim.
7. **No github URL**: the source repo is private; the listing and all
   support text are email-only (`aws-support@abyo.net`) with zero
   github.com links.
