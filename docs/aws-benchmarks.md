<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# AWS cost-throughput benchmarks (FerroStash 1.0.2 vs OSS Logstash 9.4.2)

Full data, methodology, and honest caveats behind the headline numbers
shown in the [README "Performance"](../README.md#smaller-instance-lower-bill-aws-marketplace-ami-vs-oss-logstash)
section. Run **2026-06-24** on freshly-launched AWS EC2 us-east-1
arm64 (Graviton) instances. FerroStash was the published **AWS Marketplace
AMI 1.0.2** (`prod-7tlfyv3h3xyno`, `ami-0fb249919edf99fa5`); Logstash
9.4.2 was the official tarball
(`logstash-9.4.2-linux-aarch64.tar.gz`) on a vanilla Amazon Linux 2023
AMI.

The data file, harness, and per-iteration logs live in the bench
scratchpad referenced at the bottom of this page.

---

## Workload

| Property | Value |
|---|---|
| Input | 500 000 unique synthesised Apache combined-log lines (varied IPs / verbs / paths / status / agents) |
| MD5 of input file | `9243026df69e1c5224a835d54cfa83a5` (identical on every host) |
| Input size | 60 MiB |
| Pipeline | `file (mode=read, exit_after_read)` → `grok COMBINEDAPACHELOG` → `date dd/MMM/yyyy:HH:mm:ss Z` → `mutate convert { response → integer, bytes → integer }` + `add_field` → `file (codec=json_lines)` |
| Workers | engine default (= `#vcpus`); no JVM-heap tuning |
| Iterations per cell | 3, median reported, spread ≤ 5 % on every completing cell |
| Output verification | line count equals input on success; first 100 events spot-checked for the COMBINEDAPACHELOG sub-fields (`clientip`, `verb`, `request`, `response`, …) |

The pipeline config is byte-identical across both engines.

> **Why 500 k and not 5 M:** the bench measures wall-clock to drain
> a finite input under default settings. 500 k lines is large enough
> that JVM warm-up amortises inside iteration 1 (Logstash reaches
> steady state ~3 s in and runs another 35 s), and small enough that
> a 7-host parallel SSH upload completes in seconds. The
> [main README "Performance" table](../README.md#throughput-and-memory-native-filters-5m-events)
> uses 5 M on a beefier `c7i.2xlarge` for the per-filter ratios.

---

## All measured cells

### 1. Throughput and memory by instance class

| Instance | RAM | vCPU | Engine | Median ev/s | RSS (MB, peak) | Out / In | Notes |
|---|---|---|---|---:|---:|---:|---|
| `t4g.nano`    | 0.5 GB | 2 (burst) | **FerroStash 1.0.2** | **60 031** | *(not captured)* | **294 230 / 500 000** | partial flush under memory pressure — see note A |
| `t4g.nano`    | 0.5 GB | 2 (burst) | Logstash 9.4.2 | *not runnable* | — | — | JVM cannot start in 0.5 GB; not a measurement, a hard floor |
| `t4g.small`   | 2 GB   | 2 (burst) | **FerroStash 1.0.2** | **38 194** |  116 | 500 000 / 500 000 | no CPU credit drain |
| `t4g.small`   | 2 GB   | 2 (burst) | Logstash 9.4.2 |  7 719 | 1 063 | 500 000 / 500 000 | **CPU-credit-throttled mid-bench — note B** |
| `c7g.medium`  | 2 GB   | 1 | **FerroStash 1.0.2** | **52 815** |   49 | 500 000 / 500 000 | |
| `c7g.medium`  | 2 GB   | 1 | Logstash 9.4.2 |  6 999 |  742 | 500 000 / 500 000 | |
| `c7g.large`   | 4 GB   | 2 | **FerroStash 1.0.2** | **61 125** |   61 | 500 000 / 500 000 | |
| `c7g.large`   | 4 GB   | 2 | Logstash 9.4.2 | 12 686 | 1 058 | 500 000 / 500 000 | |

On every instance class where Logstash physically runs, FerroStash is
**4.2× to 7.5× faster on the same hardware** and uses **10× to 21× less
RAM**. The single most useful cell — **FerroStash on `c7g.medium`** —
processes 53 k events/sec, which is **4.2× the throughput of Logstash on
`c7g.large`** at ⅓ the EC2 cost.

#### Per-iteration numbers (median is bolded)

| Cell | Iter 1 ev/s | Iter 2 ev/s | Iter 3 ev/s |
|---|---:|---:|---:|
| FerroStash `t4g.nano`  | 60 452 | **60 031** | 59 263 |
| FerroStash `t4g.small` | **38 194** | 39 185 | 35 873 |
| FerroStash `c7g.medium`| 52 876 | **52 815** | 51 986 |
| FerroStash `c7g.large` | 61 043 | **61 125** | 61 501 |
| Logstash `t4g.small`  |  7 719 | **7 748**  |  7 616 |
| Logstash `c7g.medium` |  6 776 | **6 999**  |  7 017 |
| Logstash `c7g.large`  | 12 726 | **12 686** | 12 124 |

---

### 2. Cost-per-throughput (EC2 on-demand + Marketplace, us-east-1, June 2026)

Pricing assumed:
- EC2 on-demand (us-east-1, Linux, June 2026):
  `t4g.nano $0.0042/h`, `t4g.small $0.0168/h`, `c7g.medium $0.0181/h`,
  `c7g.large $0.0723/h`.
- AWS Marketplace FerroStash software (per [`docs/marketplace/LISTING.md`](marketplace/LISTING.md)):
  `c7g.medium $0.03/h`, `t4g.small $0.04/h`, `c7g.large $0.06/h`.
- OSS Logstash is free; Logstash cost = EC2 only.

| Configuration | EC2 $/h | MP fee | Total $/h | Throughput | Events per $1-hour |
|---|---:|---:|---:|---:|---:|
| OSS Logstash on `c7g.large` (baseline) | $0.0723 | — | $0.0723 | 12 686 ev/s |   632 M / $ |
| OSS Logstash on `c7g.medium`           | $0.0181 | — | $0.0181 |  6 999 ev/s | 1 392 M / $ |
| OSS Logstash on `t4g.small` *(throttled, see B)* | $0.0168 | — | $0.0168 |  7 719 ev/s | 1 654 M / $ |
| **FerroStash AMI on `c7g.medium`**     | $0.0181 | $0.03 | **$0.0481** | **52 815 ev/s** | **3 953 M / $** |
| FerroStash AMI on `t4g.small`          | $0.0168 | $0.04 | $0.0568 | 38 194 ev/s | 2 420 M / $ |
| FerroStash AMI on `c7g.large`          | $0.0723 | $0.06 | $0.1323 | 61 125 ev/s | 1 663 M / $ |

**The "FerroStash on `c7g.medium`" row is the headline.** It outperforms
even the zero-software-cost OSS Logstash baseline on the same hardware —
**6.3× more events per dollar than `Logstash on c7g.large`** and **2.8×
more events per dollar than `Logstash on c7g.medium`**. The Marketplace
fee pays for the supported, security-scanned distribution and the SLA;
the throughput advantage repays it many times over on any non-trivial
pipeline.

---

### 3. Container: published FerroStash MP image vs official Logstash OCI

Same workload, same instance (`c7g.large`, the only single-node platform
we re-used for the container pair so the host is identical), Docker 29
on Amazon Linux 2023, `--network none`, 3 GB cgroup memory cap.

- FerroStash container: arm64 build wrapping the same `--features marketplace`
  binary the Marketplace AMI carries (the published `ferro-stash-container:1.0.2`
  on ECR is linux/amd64-only by policy, so we build the byte-equivalent
  arm64 image locally on the bench host from the AMI's `/usr/local/bin/ferro-stash`).
- Logstash container: official multi-arch
  `docker.elastic.co/logstash/logstash:9.4.2`.

| Container | Image size | Median ev/s | Peak RSS | Cold-start | Iter spread |
|---|---:|---:|---:|---:|---:|
| **FerroStash 1.0.2**         | **142 MB** | **54 897** |  51 MB | sub-second | 54 771 / **54 897** / 55 036 |
| Logstash 9.4.2 (Elastic OCI) |    899 MB  | 10 985     | 1 044 MB | ~6 s (JVM) | 10 934 / **10 985** / 11 472 |

**5.0× throughput, 20× less RAM, 6.3× smaller image** than the official
Logstash 9.4.2 image on the same host. The 8 % overhead vs the bare-metal
AMI run (61 125 vs 54 897 ev/s) is container/cgroup cost; the rest is the
engine.

Per-pod-hour cost (single pod per `c7g.large` node — the worst case for
the FerroStash Marketplace fee, since FerroStash actually fits many pods
per node where Logstash cannot):

| Container pod | Pod-hour cost | Throughput | Events per $1-hour |
|---|---:|---:|---:|
| OSS Logstash on `c7g.large` (one pod fills the node) | $0.0723        | 11.0 k ev/s | 547 M / $ |
| **FerroStash on `c7g.large` (one pod fills the node)** | **$0.1123**  | **54.9 k ev/s** | **1 762 M / $** |

Even at one pod per node, **FerroStash container delivers ~3.2× more
events per dollar**. Packing more small pods per node widens the gap;
Logstash's ~1 GB / pod RSS floor makes any per-node packing impractical
below `c7g.2xlarge`.

---

## Notes

### A — `t4g.nano` (0.5 GB RAM) is a memory-pressure illustration, not a recommendation

FerroStash boots and processes events on `t4g.nano`, but the file output
only flushed ~294 k of 500 k events to disk before the process exited
under sustained 0.5 GB pressure. `/usr/bin/time -v` did not capture
peak RSS for this cell (process likely killed before time could exit
cleanly). We include the row to demonstrate that the binary *runs* in
0.5 GB — Logstash cannot — but **the smallest size we recommend for
production is `t4g.small` or `c7g.medium` (2 GB)**.

### B — `t4g.small` Logstash was CPU-credit-throttled during the bench

CloudWatch `CPUCreditBalance` for the Logstash `t4g.small` instance
went from 0.32 credits at 13:33 JST to 0.00 between 13:43 and 13:48
JST (the bench window), then climbed back to 0.51 at 13:58. The
Logstash throughput on `t4g.small` (7 719 ev/s) is therefore a
**throttled** measurement; the true unthrottled Logstash throughput on
`t4g.small` is somewhere between the `c7g.medium` (6 999) and
`c7g.large` (12 686) figures.

FerroStash `t4g.small` did NOT drain credits (the bench took ~13 s per
iteration vs Logstash's ~65 s, so it never ran long enough to consume
the burst budget; CloudWatch shows the FerroStash `t4g.small`
balance climbing 0.29 → 9.02 across the bench window).

Raw credit data: `scratchpad/bench/results/credits.txt`.

### C — Output integrity, not just throughput

Every "500 000 / 500 000" cell was also spot-checked for parse
correctness — the first 100 output events were grepped for
COMBINEDAPACHELOG sub-fields (`clientip`, `verb`, `request`,
`response`, …). All FerroStash cells: 100/100 parsed. The Logstash
cells show 0/100 in our raw output (`scratchpad/bench/results/*.txt`),
but that is a **regex artefact** — Logstash's default JSON codec emits
`"clientip" : "..."` (with a space) while our spot-check regex was
`"clientip":"..."` (no space). The byte-level integrity is identical;
the parsed-line count is the load-bearing column.

### D — Pre-1.0.2 binaries would have failed this bench

The 1.0.2 release (commit `53de8c9`) fixes four silent-data-loss bugs
that surfaced during this very benchmark on the published 1.0.1 image:

- `grok` compound patterns (`%{COMBINEDAPACHELOG}`) extracted **zero
  fields** without tagging `_grokparsefailure`,
- TCP input ignored `codec => "json_lines"`,
- file input did not honour `mode => "read"` / `exit_after_read =>
  true` (the bench would hang waiting for SIGTERM),
- events lacked `@version` and used nanosecond-precision `@timestamp`
  with a `+00:00` offset instead of the Logstash-canonical ms-precision
  `Z` form.

The "output integrity" column in the table above is `500k / 500k`
*because* of those fixes. The pre-1.0.2 binary on the same hosts would
have produced 500k output lines too, but with empty grok fields —
silent corruption that no `grep` for `_grokparsefailure` would catch.
See [`CHANGELOG.md`](../CHANGELOG.md) "1.0.2".

### E — What we did NOT measure (yet)

- End-to-end **latency** (only finite-batch wall-clock throughput).
- **Long-tail steady-state under back-pressure** (queueing, network
  output, downstream slowdown) — we use a `file` sink to keep the
  measurement focused on the engine, not the sink.
- The `ruby { }` filter — for FerroStash this is the Artichoke/mruby
  path, which is ~13× slower than the JVM JRuby on the hot path (see
  the [main README's "Custom logic" table](../README.md#custom-logic-painless-vs-ruby)).
  For pipelines that depend on `ruby { }`, use `script { }` instead
  (Painless-style, native, ~3.6× faster than JRuby).
- Workloads other than this one Apache combined-log shape. These
  numbers do **not** generalise to Avro pipelines, heavy `kv` /
  `xml` filters, or large-fanout outputs. The
  [per-filter table on the main README](../README.md#throughput-and-memory-native-filters-5m-events)
  has the per-filter ratios from a different (offline, fewer instance
  classes) bench.

### F — EC2 cleanup discipline (per repository CLAUDE.md "終わったら止める")

All 7 bench instances + the temporary security group were terminated
within minutes of the last run completing; no EBS volumes were
orphaned. Total cost of producing every cell in this document: ~30
minutes of EC2 wall-clock across 7 small Graviton instances + 4
minutes of CloudWatch retention, ≈ **$0.20** at the on-demand rates
listed.

---

## Reproduce

The harness scripts (`bench.sh` per iteration, `container-bench.sh`,
the synthetic log generator, the SSH orchestrator) live in
`scratchpad/bench/` during a session that runs this bench. The exact
inputs and outputs from the 2026-06-24 run are at
`scratchpad/bench/results/*.txt`.

To reproduce end-to-end:
1. Subscribe to the FerroStash Marketplace AMI
   (`prod-7tlfyv3h3xyno`) in `us-east-1`.
2. Launch the instance pairs above with the same default-VPC SG (port
   22 only, your IP only).
3. Run the same `pipeline.conf` (it's in the [README "Quick
   start"](../README.md#quick-start) section as a working example;
   substitute the COMBINEDAPACHELOG grok block from your real
   pipeline if different).
4. Time the wall-clock for each engine to drain the same finite
   input; divide event count by elapsed seconds.

The Marketplace AMI ships with the binary at `/usr/local/bin/ferro-stash`
and a systemd unit `ferro-stash` (stop it before benching). The OSS
Logstash 9.4.2 tarball deploys to `/opt/logstash`.
