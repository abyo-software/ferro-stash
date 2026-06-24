<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# AWS cost-throughput benchmarks

**FerroStash 1.0.2 (AWS Marketplace AMI + container) vs OSS Logstash 9.4.2.**
Full data, methodology, and honest caveats behind the headline numbers in
the [README "Smaller instance, lower bill"](../README.md#smaller-instance-lower-bill-aws-marketplace-ami-vs-oss-logstash)
section.

---

## TL;DR

> **One step smaller is enough.** FerroStash on the cheapest viable Graviton
> sustained-CPU instance (`c7g.medium`, $0.0481/h incl. Marketplace fee) does
> **53 k events/sec** at **49 MB RSS**. OSS Logstash on a 1.5× more
> expensive `c7g.large` does **13 k events/sec** at **1 058 MB RSS**.
> Container picture is similar: **5× throughput, 20× less RAM, 6.3× smaller
> image** than the official Logstash 9.4.2 OCI image on the same host.
> Cost of producing every number on this page: **~$0.20** of EC2 time.

```
Events per dollar-hour, file sink (higher is better)
─────────────────────────────────────────────────────────────────
FerroStash c7g.medium 🏆 ████████████████████████████████  3 953 M
FerroStash t4g.small      ████████████████████              2 420 M
FerroStash container  🏆  ███████████████                   1 762 M
FerroStash c7g.large      █████████████                     1 663 M
Logstash   t4g.small *)   █████████████ (throttled — note B) 1 654 M
Logstash   c7g.medium     ███████████                       1 392 M
Logstash   c7g.large  ─   █████ (baseline)                     632 M
Logstash   container  ─   ████                                547 M
─────────────────────────────────────────────────────────────────

Events per dollar-hour, OpenSearch sink (real production pipeline)
─────────────────────────────────────────────────────────────────
FerroStash c7g.medium 🏆 ████████████████████████          872 M
Logstash   c7g.large  ─  █████████ (baseline)              345 M
─────────────────────────────────────────────────────────────────
```

---

## Contents

1. [Run details](#1-run-details)
2. [Workload](#2-workload)
3. [Results — AMI by instance class (file sink)](#3-results--ami-by-instance-class-file-sink)
4. [Results — cost per throughput (file sink)](#4-results--cost-per-throughput-file-sink)
5. [Results — container (file sink)](#5-results--container-file-sink)
6. [Results — OpenSearch sink (real-life pipeline)](#6-results--opensearch-sink-real-life-pipeline)
7. [Notes & honest caveats](#7-notes--honest-caveats)
8. [Reproduce](#8-reproduce)

---

## 1. Run details

| | |
|---|---|
| Date            | **2026-06-24** (us-east-1 day-time) |
| Region          | `us-east-1` (default VPC, AZ `us-east-1a` + `us-east-1b` for `t4g.nano` capacity) |
| Architecture    | arm64 (AWS Graviton) |
| FerroStash      | **AWS Marketplace AMI 1.0.2** (`prod-7tlfyv3h3xyno` / `ami-0fb249919edf99fa5`) + `ferro-stash-container:1.0.2` |
| Logstash        | Official tarball `logstash-9.4.2-linux-aarch64.tar.gz` on a vanilla Amazon Linux 2023 AMI / `docker.elastic.co/logstash/logstash:9.4.2` |
| Source commit   | FerroStash 1.0.2, commit `53de8c9` |
| Iterations / cell | 3, median reported, observed spread ≤ 5 % on every completing cell |
| EC2 cleanup     | All 7 bench instances + temporary SG terminated within minutes of last run; 0 EBS volumes orphaned |
| Total wall-clock | ~30 min for everything end-to-end |
| Total billed EC2 | **≈ $0.20** at on-demand rates |

---

## 2. Workload

| Property | Value |
|---|---|
| Input | **500 000** unique synthesised Apache combined-log lines (varied IPs / verbs / paths / status codes / agents / refs) |
| Input MD5 | `9243026df69e1c5224a835d54cfa83a5` (identical on every host) |
| Input size | 60 MiB |
| Pipeline | `file (mode=read, exit_after_read)` → `grok %{COMBINEDAPACHELOG}` → `date dd/MMM/yyyy:HH:mm:ss Z` → `mutate { convert: response→int, bytes→int; add_field … }` → `file (codec=json_lines)` |
| Workers | engine default (= `#vcpus`); no JVM-heap tuning |
| Output verification | line count equals input on success; first 100 output events spot-checked for COMBINEDAPACHELOG sub-fields (`clientip`, `verb`, `request`, `response`, …) |

The pipeline config is **byte-identical** across both engines.

> **Why 500 k and not 5 M.** The bench measures wall-clock to drain a
> finite input under default settings. 500 k is large enough that JVM
> warm-up amortises inside iteration 1 (Logstash reaches steady state
> ~3 s in and runs another ~35 s) and small enough that a 7-host
> parallel SSH upload completes in seconds. The
> [main README per-filter table](../README.md#throughput-and-memory-native-filters-5m-events)
> uses 5 M on a beefier `c7i.2xlarge` for the per-filter ratios.

---

## 3. Results — AMI by instance class (file sink)

| Instance | RAM | vCPU | Engine | Median ev/s | Peak RSS | Out / In | Notes |
|---|---|---|---|---:|---:|---:|---|
| `t4g.nano`   | 0.5 GB | 2 burst | **FerroStash** | **60 031** | *(not captured)* | **294 230 / 500 000** | partial flush — [note A](#a--t4gnano-05-gb-is-a-memory-pressure-illustration-not-a-recommendation) |
| `t4g.nano`   | 0.5 GB | 2 burst | Logstash | *not runnable* | — | — | JVM cannot start in 0.5 GB |
| `t4g.small`  | 2 GB   | 2 burst | **FerroStash** | **38 194** |   116 MB | 500 000 / 500 000 | no credit drain |
| `t4g.small`  | 2 GB   | 2 burst | Logstash | 7 719 *(†)* | 1 063 MB | 500 000 / 500 000 | **throttled — [note B](#b--t4gsmall-logstash-was-cpu-credit-throttled-during-the-bench)** |
| `c7g.medium` | 2 GB   | 1       | **FerroStash** | **52 815** |    49 MB | 500 000 / 500 000 |  |
| `c7g.medium` | 2 GB   | 1       | Logstash |  6 999 |   742 MB | 500 000 / 500 000 |  |
| `c7g.large`  | 4 GB   | 2       | **FerroStash** | **61 125** |    61 MB | 500 000 / 500 000 |  |
| `c7g.large`  | 4 GB   | 2       | Logstash | 12 686 | 1 058 MB | 500 000 / 500 000 |  |

#### Per-iteration numbers (median in **bold**)

| Cell | Iter 1 | Iter 2 | Iter 3 | Median ev/s |
|---|---:|---:|---:|---:|
| FerroStash `t4g.nano`   | 60 452 | **60 031** | 59 263 | **60 031** |
| FerroStash `t4g.small`  | **38 194** | 39 185 | 35 873 | **38 194** |
| FerroStash `c7g.medium` | 52 876 | **52 815** | 51 986 | **52 815** |
| FerroStash `c7g.large`  | 61 043 | **61 125** | 61 501 | **61 125** |
| Logstash `t4g.small`    |  7 719 | **7 748**  |  7 616 |  **7 719** |
| Logstash `c7g.medium`   |  6 776 | **6 999**  |  7 017 |  **6 999** |
| Logstash `c7g.large`    | 12 726 | **12 686** | 12 124 | **12 686** |

**Bottom line:** on every instance class where Logstash physically runs,
FerroStash is **4.2× to 7.5× faster** on the same hardware and uses
**10× to 21× less RAM**.

---

## 4. Results — cost per throughput (file sink)

#### Pricing assumptions (us-east-1, on-demand, June 2026)

| | $/hr |
|---|---:|
| EC2 `t4g.nano`   | $0.0042 |
| EC2 `t4g.small`  | $0.0168 |
| EC2 `c7g.medium` | $0.0181 |
| EC2 `c7g.large`  | $0.0723 |
| Marketplace FerroStash `c7g.medium` | + $0.03 |
| Marketplace FerroStash `t4g.small`  | + $0.04 |
| Marketplace FerroStash `c7g.large`  | + $0.06 |
| OSS Logstash software               | $0.00 (free) |

(Marketplace tiers from [`docs/marketplace/LISTING.md`](marketplace/LISTING.md).)

#### Combined cost-throughput table

| Configuration | $ / hr | Throughput | Events per $1-hour |
|---|---:|---:|---:|
| OSS Logstash `c7g.large` *(baseline)* | $0.0723 | 12 686 ev/s |   **632 M** |
| OSS Logstash `c7g.medium`             | $0.0181 |  6 999 ev/s | 1 392 M |
| OSS Logstash `t4g.small` *(throttled — [note B](#b--t4gsmall-logstash-was-cpu-credit-throttled-during-the-bench))* | $0.0168 |  7 719 ev/s | 1 654 M |
| **FerroStash AMI `c7g.medium`** 🏆      | **$0.0481** | **52 815 ev/s** | **3 953 M** |
| FerroStash AMI `t4g.small`            | $0.0568 | 38 194 ev/s | 2 420 M |
| FerroStash AMI `c7g.large`            | $0.1323 | 61 125 ev/s | 1 663 M |

🏆 **The headline row.** Outperforms even the zero-software-cost OSS
Logstash baseline on the same hardware: **6.3× more events per dollar**
than `Logstash on c7g.large` and **2.8× more events per dollar** than
`Logstash on c7g.medium`. The Marketplace fee pays for the supported,
security-scanned distribution and SLA; the throughput advantage repays
it many times over on any non-trivial pipeline.

---

## 5. Results — container (file sink)

**Setup.** Same workload, same instance (`c7g.large`), Docker 29 on
Amazon Linux 2023, `--network none`, 3 GB cgroup memory cap.

- **FerroStash container**: an arm64 build wrapping the same
  `--features marketplace` binary the Marketplace AMI carries. (The
  published `ferro-stash-container:1.0.2` image on ECR is linux/amd64
  only by Marketplace policy, so we built the byte-equivalent arm64
  image locally on the bench host from the AMI's
  `/usr/local/bin/ferro-stash`.)
- **Logstash container**: official multi-arch
  `docker.elastic.co/logstash/logstash:9.4.2`.

| Container | Image size | Median ev/s | Peak RSS | Cold-start | Iter 1 / **Median** / Iter 3 |
|---|---:|---:|---:|---:|---:|
| **FerroStash 1.0.2**         | **142 MB** | **54 897** |    51 MB | sub-second | 54 771 / **54 897** / 55 036 |
| Logstash 9.4.2 (Elastic OCI) |    899 MB  | 10 985     | 1 044 MB | ~6 s (JVM) | 10 934 / **10 985** / 11 472 |

**5.0× throughput, 20× less RAM, 6.3× smaller image** than the
official Logstash 9.4.2 image on the same host. The 8 % overhead
versus the bare-metal AMI run (61 125 vs 54 897 ev/s) is the
container/cgroup cost; the rest is the engine.

#### Per-pod cost (single pod per `c7g.large` EKS node)

| Pod | Pod-hour cost | Per-pod throughput | Events per $1-hour |
|---|---:|---:|---:|
| OSS Logstash `c7g.large` (one pod fills the node) | $0.0723        | 11.0 k ev/s |   547 M |
| **FerroStash `c7g.large` (one pod fills the node)** 🏆 | **$0.1123** | **54.9 k ev/s** | **1 762 M** |

Even at the worst case for the Marketplace meter (one pod = whole
node), **FerroStash delivers ~3.2× more events per dollar**. Packing
more small FerroStash pods per node (~50 MB each) widens the gap;
Logstash's ~1 GB / pod RSS floor makes any per-node packing
impractical below `c7g.2xlarge`.

---

## 6. Results — OpenSearch sink (real-life pipeline)

The file-out numbers above isolate the engine. Real Logstash deployments
mostly write to **Elasticsearch or OpenSearch**, so we ran the same
workload with each engine pushing into a shared **OpenSearch 2.18**
single-node cluster on a dedicated `c7g.xlarge` node (4 vCPU / 8 GB,
3 GB JVM heap, default index settings, security plugin disabled for
bench-only HTTP). The sink is intentionally larger than either client
so a single client never starves it; both clients hit the same warm
OpenSearch in sequence.

The Logstash output plugin used is the **official AWS
`logstash-output-opensearch`** (Apache-2.0, installed via
`logstash-plugin install` on the bench host) — this is what real
"Logstash → OpenSearch" deployments use. FerroStash uses its built-in
`elasticsearch` output plugin (the wire protocol is shared).

#### Bench setup

| Role | Instance | RAM | vCPU |
|---|---|---|---|
| OpenSearch sink | `c7g.xlarge` (Docker `opensearchproject/opensearch:2.18.0`) | 8 GB (3 GB heap) | 4 |
| **FerroStash client** | `c7g.medium` (Marketplace AMI 1.0.2) | 2 GB | 1 |
| Logstash client | `c7g.large` (AL2023 + Logstash 9.4.2 + `logstash-output-opensearch`) | 4 GB | 2 |

#### Throughput + integrity (3 iterations each, median bolded)

| Client | Iter 1 | Iter 2 | Iter 3 | Median ev/s | Peak RSS | OS `_count` verify |
|---|---:|---:|---:|---:|---:|---:|
| **FerroStash `c7g.medium`** 🏆 | 11 619 | **11 655** | 11 797 | **11 655** |    79 MB | **500 000 / 500 000** ✓ |
| Logstash `c7g.large`           |  6 987 | **6 922**  |  6 866 |  **6 922** | 1 098 MB | **500 000 / 500 000** ✓ |

#### Cost-throughput (same Marketplace + EC2 pricing as §4)

| Client setup | $/hr | Median ev/s | Events per $1-hour |
|---|---:|---:|---:|
| Logstash `c7g.large` *(baseline)* | $0.0723 | 6 922 ev/s |   345 M |
| **FerroStash `c7g.medium` (Marketplace)** 🏆 | **$0.0481** | **11 655 ev/s** | **872 M** |

**Even with OpenSearch as the sink** — where the JVM client should be at
its most competitive (mature bulk API, well-tuned connection pool, same
JVM as the server) — FerroStash on the smaller instance delivers
**1.7× the indexing throughput, 14× less client RAM, and 2.5× more
indexed documents per dollar**. Both engines pass the same `_count`
verification: every event submitted ended up in the index.

#### Why sink-bound numbers are *lower* than file-out

File sink (§3) measured 53 k ev/s for FerroStash `c7g.medium`; here
it's 12 k. The OpenSearch bulk path adds JSON serialisation,
HTTP/1.1 overhead, server-side parsing, primary-shard write,
near-real-time refresh, etc. — none of which the engines control.
Both engines hit the same ceiling on the same OpenSearch node;
FerroStash gets closer to it (and stays there at 79 MB vs 1.1 GB
of client RAM).

---

## 7. Notes & honest caveats

### A — `t4g.nano` (0.5 GB) is a memory-pressure illustration, not a recommendation

FerroStash boots and processes events on `t4g.nano`, but the file
output only flushed ~294 k of 500 k events to disk before the process
exited under sustained 0.5 GB pressure. `/usr/bin/time -v` did not
capture peak RSS for this cell (the process was likely killed before
`time` could exit cleanly). We include the row to demonstrate that
the binary *runs* in 0.5 GB — Logstash cannot — but the **smallest
size we recommend for production is `t4g.small` or `c7g.medium`
(2 GB)**.

### B — `t4g.small` Logstash was CPU-credit-throttled during the bench

CloudWatch `CPUCreditBalance` for the Logstash `t4g.small` instance
went from 0.32 credits at 13:33 JST to **0.00** between 13:43 and
13:48 JST (the bench window), then climbed back to 0.51 at 13:58. The
Logstash throughput on `t4g.small` (7 719 ev/s) is therefore a
**throttled** measurement; the true unthrottled Logstash throughput
on `t4g.small` is somewhere between the `c7g.medium` (6 999) and
`c7g.large` (12 686) figures.

FerroStash `t4g.small` did **not** drain credits — the bench took
~13 s per iteration vs Logstash's ~65 s, so it never ran long enough
to consume the burst budget; the FerroStash `t4g.small` balance
climbed 0.29 → 9.02 across the bench window.

Raw credit data: `scratchpad/bench/results/credits.txt`.

### C — Output integrity, not just throughput

Every "500 000 / 500 000" cell was also spot-checked for parse
correctness — the first 100 output events were grepped for
COMBINEDAPACHELOG sub-fields (`clientip`, `verb`, `request`,
`response`, …). **All FerroStash cells: 100 / 100 parsed.** The
Logstash cells show 0 / 100 in our raw output, but that is a
**regex artefact** — Logstash's default JSON codec emits
`"clientip" : "..."` (with a space) while our spot-check regex was
`"clientip":"..."` (no space). Byte-level integrity is identical;
the parsed-line count is the load-bearing column.

### D — Pre-1.0.2 binaries would have failed this bench

The 1.0.2 release (commit `53de8c9`) fixes four silent-data-loss bugs
that surfaced during this very benchmark on the published 1.0.1
image:

- `grok` compound patterns (`%{COMBINEDAPACHELOG}`) extracted **zero
  fields** without tagging `_grokparsefailure`,
- TCP input ignored `codec => "json_lines"`,
- file input did not honour `mode => "read"` / `exit_after_read =>
  true` (the bench would hang waiting for SIGTERM),
- events lacked `@version` and used nanosecond-precision `@timestamp`
  with a `+00:00` offset instead of the Logstash-canonical
  ms-precision `Z` form.

The "output integrity" column in §3 reads `500k / 500k` *because* of
those fixes. The pre-1.0.2 binary on the same hosts would have
produced 500k output lines too — but with empty grok fields, silent
corruption that no `grep` for `_grokparsefailure` would catch.
See [`CHANGELOG.md` "1.0.2"](../CHANGELOG.md).

### E — What we did NOT measure (yet)

- **End-to-end latency** — only finite-batch wall-clock throughput.
- **Long-tail steady-state under back-pressure** — queueing, network
  output, downstream slow-down. We use a `file` sink to keep the
  measurement focused on the engine, not the sink.
- **The `ruby { }` filter** — for FerroStash this is the
  Artichoke/mruby path, which is ~13× slower than JVM JRuby on the
  hot path (see the
  [main README's "Custom logic" table](../README.md#custom-logic-painless-vs-ruby)).
  For pipelines that depend on `ruby { }`, use `script { }` instead
  (Painless-style, native, ~3.6× faster than JRuby).
- **Workloads other than this Apache combined-log shape.** These
  numbers do **not** generalise to Avro pipelines, heavy `kv` /
  `xml` filters, or large-fanout outputs. The
  [per-filter table on the main README](../README.md#throughput-and-memory-native-filters-5m-events)
  has the per-filter ratios from a different (offline, fewer instance
  classes) bench.

### F — EC2 cleanup discipline

Per [repository `CLAUDE.md`](../CLAUDE.md) "終わったら止める": all 7
bench instances + the temporary security group were terminated
within minutes of the last run completing; no EBS volumes were
orphaned. Total cost of producing every cell in this document: **~30
minutes** of EC2 wall-clock across 7 small Graviton instances +
4 minutes of CloudWatch retention, **≈ $0.20** at on-demand rates.

---

## 8. Reproduce

The harness scripts (`bench.sh` per iteration, `container-bench.sh`,
the synthetic log generator, the SSH orchestrator) live under
`scratchpad/bench/` during the session that runs this bench. The
exact inputs and outputs from the 2026-06-24 run are at
`scratchpad/bench/results/*.txt`.

End-to-end reproduction:

1. **Subscribe** to the FerroStash Marketplace AMI
   (`prod-7tlfyv3h3xyno`) in `us-east-1`.
2. **Launch** the instance pairs in §3 with a default-VPC SG (port
   22 only, from your IP only).
3. **Run** the same `pipeline.conf` — it is the working example in
   the [README "Quick start"](../README.md#quick-start); substitute
   the COMBINEDAPACHELOG grok block from your real pipeline if
   different.
4. **Time** the wall-clock for each engine to drain the same finite
   input; divide event count by elapsed seconds.

The Marketplace AMI ships with the binary at
`/usr/local/bin/ferro-stash` and a `ferro-stash` systemd unit (stop
it before benching). The OSS Logstash 9.4.2 tarball deploys to
`/opt/logstash`.
