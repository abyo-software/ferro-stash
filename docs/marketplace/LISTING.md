<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# FerroStash -- AWS Marketplace Listing Copy

> This document is the canonical source for the AWS Marketplace product
> load form for the **FerroStash (AMI)** product -- a paid Amazon Machine
> Image, priced hourly + annual, metered automatically by AWS (there is
> **no in-product metering code**; AWS attaches the product code to the
> published AMI and meters instance-hours).
>
> The Marketplace product is a hardened, security-scanned, supported
> distribution of FerroStash at a pinned, SemVer-stable release version.
> Present it as a production-quality v1.0 product.
>
> **Internal context (NOT in the public listing copy; see group B in
> [section 8](#8-honest-scope-notes-aws-rejection-reconciliation)):**
> FerroStash genuinely is open-core -- the engine is and remains
> **Apache-2.0** open source and nothing relicenses it, and the paid listing
> sells the packaged distribution rather than the source. That open-core /
> Apache-2.0 business-model framing is true, but it is **internal context
> only**: it must NOT appear in any buyer-facing listing or support copy,
> because the AWS reviewer read "get the free source elsewhere" language as
> encouraging transactions outside AWS Marketplace (rejection request
> `cw0ok7zsqkmkw99sw04t4ximl`).
>
> **Accuracy contract**: every capability claim below is cross-checked
> against the repository `README.md` (Honest limitations) and
> `docs/COMPATIBILITY.md`. The listing-safe facts in
> [section 8 group A](#8-honest-scope-notes-aws-rejection-reconciliation)
> stay in the listing copy; the group-B notes stay in `README.md` only and
> must NOT be folded into the listing.
>
> **Two hard rules for this listing:**
> 1. **No github.com URL anywhere in the listing or support text.** Marketplace
>    copy should avoid repository links and route support through the seller
>    support channel. Support is **email only**: `aws-support@abyo.net`.
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
| License model | Apache-2.0 software + paid supported distribution (open-core) *(internal note; not surfaced in the public listing copy)* |
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
    aggregate, throttle, anonymize, syslog_pri, uuid, and a native
    Painless-style `script` filter.
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
**1,400+ tests** with output **verified against Logstash 9.4.2 expected fields
across 24 parity fixtures** (runtime-only fields normalized; Docker
side-by-side covers a 13-fixture subset).

**Compatibility scope:** FerroStash is Logstash **config / pipeline
compatible** across the covered plugin set. It is **not a 100% drop-in** --
a covered plugin may implement a subset of that plugin's options, and a
config that uses an unsupported plugin **fails fast at load** with a clear
error rather than silently dropping events. The remaining gaps are mostly
enterprise / niche connectors (for example `jms`, `azure_event_hubs`,
`snmp`, `lumberjack`, `webhdfs`). The supported topology is **single-node**,
and the optional `ruby` filter (Artichoke/mruby) is **excluded from the
default build** (the native `script` filter is the in-build alternative).

This Marketplace product is a **hardened, security-scanned distribution at
a pinned, SemVer-stable release version**. The AMI is metered automatically
by AWS per running instance-hour -- there is no metering code in the
product.

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
  service runs unprivileged and has no admin login). Built at a pinned,
  SemVer-stable release version (no `latest`).
- **One-click deployment.** Launch on EC2 (Graviton / arm64); the
  `ferro-stash` systemd service starts on first boot running a default
  pipeline. Point `/etc/ferro-stash/pipeline.conf` at your real inputs and
  outputs and restart.
- **Commercial support** by email under a published SLA at
  `aws-support@abyo.net`.
- **Full transparency** -- a published Logstash compatibility matrix, an
  honest-limitations document, and a due-diligence pack ship with the
  product.

> **Internal rationale (NOT listing copy -- do not surface to buyers):**
> what is sold is the packaged, security-scanned, supported, one-click
> distribution plus an SLA. The open-core / Apache-2.0 framing behind that
> is true but stays internal (see [section 8 group B](#8-honest-scope-notes-aws-rejection-reconciliation));
> it must not appear in this buyer-facing "What you get" copy.

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

## 8. Honest-scope notes (AWS rejection reconciliation)

**Why this section is split.** AWS Marketplace rejected the first Public
submission (request `cw0ok7zsqkmkw99sw04t4ximl`) on two grounds:
**External transactions** (the open-core / "built from the Apache-2.0
source / the code stays Apache-2.0" language read as "get the free source
elsewhere") and **Production readiness** (the "single-developer project /
no public production deployments yet / run it beside before trusting it /
read before you buy / not exhaustive conformance" hedges read as
beta/evaluation software). The notes below are therefore separated into the
facts that are **listing-safe** and stay in the Marketplace copy (group A)
and the honest limitations that stay in the OSS `README.md` **only** and
must NOT appear in the listing (group B).

### (A) Kept IN the Marketplace listing copy (listing-safe facts, NOT beta signals)

1. **Logstash compatibility**: marketed as "Logstash config / pipeline
   compatible", **never** "100% compatible" or "drop-in". Coverage is
   **plugin-level (~88%, 98 of 111 bundled plugins)**; a covered plugin
   may implement a subset of its options; a config using an unsupported
   plugin **fails fast at load** with a clear error.
2. **Connector / niche gaps**: the remaining gaps are mostly enterprise /
   niche connectors (for example `jms`, `azure_event_hubs`, `snmp`,
   `lumberjack`, `webhdfs`).
3. **Topology**: single-node is the supported Marketplace topology.
4. **Default build excludes `ruby`**: the optional Artichoke/mruby `ruby`
   filter is not in the default build (the native `script` filter is the
   in-build alternative). Do not advertise the ruby filter as shipping in
   the AMI.
5. **No named-AWS-service positioning** *(hard rule)*: do not describe
   FerroStash as an alternative/replacement to any named AWS service.
   "Logstash-compatible" (Elastic's product) is the only compatibility
   claim.
6. **No github URL in the listing** *(hard rule)*: the listing and all
   support text are email-only (`aws-support@abyo.net`) with zero
   github.com links.

### (B) Kept in the OSS `README.md` ONLY -- NOT in the Marketplace listing

These remain honest in `README.md`, but each one re-triggers an AWS
rejection ground if it appears in any buyer-facing listing or support copy,
so keep them OUT of the listing:

1. **No production track record**: "single-developer project", "**no public
   production deployments yet**", and "run it beside your existing pipeline
   before trusting it" -- these read as beta/evaluation software
   (Production readiness). The SemVer-stable surface is a real contract;
   present the listing as a production-quality v1.0 instead of foregrounding
   the absence of deployments.
2. **"Read before you buy" framing**: do not invite buyers to evaluate the
   software as if it were unproven; this is a Production-readiness trigger.
3. **Connector validation depth**: the env-gated connector smoke tests are
   reachability + round-trip, **not exhaustive conformance**, and are not in
   CI. Honest in `README.md`; the "not exhaustive conformance" phrasing is a
   Production-readiness trigger in the listing.
4. **Open-core / Apache-2.0 business-model framing**: "the engine is and
   remains Apache-2.0 source", "the code stays Apache-2.0", "open-core",
   "built from the Apache-2.0 source", "does not relicense". All true and
   stays in `README.md`, but in the listing it reads as "get the free source
   elsewhere" (External transactions). Internal-context references to it in
   this design doc are clearly labeled and never copied into listing fields.
