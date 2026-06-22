<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# FerroStash -- AWS Marketplace Submission Checklist

This checklist maps each AWS Marketplace requirement for the **FerroStash
(AMI)** product to its current status, so the owner knows exactly what is
left to publish.

> **Status legend**
> - **Done** -- produced in this repository / already satisfied.
> - **In progress** -- partially done; a specific gap remains (named).
> - **🔒 OWNER** -- requires the AWS seller account, the console / Catalog
>   API run, or a human business decision. This tooling cannot perform it.
>
> The publish flow is the Catalog-API trilogy + release in `deploy/`:
> `marketplace-create.sh` (Draft: product + info + regions + instance
> types) -> `marketplace-ami.sh` (build + share + AddDeliveryOptions ->
> AWS scan) -> `marketplace-pricing.sh` (offer + dimensions + pricing +
> legal + support) -> `marketplace-release.sh` (IRREVERSIBLE, `CONFIRM=yes`).

---

## A. Seller registration & account -- 🔒 OWNER

| # | Requirement | Status | Notes |
|---|---|---|---|
| A1 | AWS Marketplace **seller registration** | Done (account `as`) | abyo software 合同会社 is a registered seller (sibling FerroSCA / FerroDruid / S4 offers are live). |
| A2 | **Tax** information submitted | 🔒 OWNER | Console -> Settings -> Tax. |
| A3 | **Banking / disbursement** details | 🔒 OWNER | Console -> Settings -> Bank. |
| A4 | **Seller legal name / public profile** | Done | abyo software 合同会社 (abyo software LLC). |
| A5 | **Standard Contract** accepted (or custom EULA) | 🔒 OWNER (console accept) | `marketplace-pricing.sh` attaches `StandardEula` (2022-07-14) to the offer. |

## B. Listing content

| # | Requirement | Status | Notes |
|---|---|---|---|
| B1 | Product **title** | Done | `LISTING.md` 1; baked into `marketplace-create.sh`. |
| B2 | **Short description** (<= ~150 chars) | Done | `LISTING.md` 1; baked into the `UpdateInformation` call. |
| B3 | **Long description** | Done | `LISTING.md` 2; baked into `marketplace-create.sh`. |
| B4 | **Highlights** (EXACTLY 3, AWS max) | Done | `LISTING.md` 3; baked verbatim into `marketplace-create.sh`. |
| B5 | **Categories** | Done | Data Analytics + Monitoring (`marketplace-create.sh` `CATS`). |
| B6 | **Search keywords** | Done | `LISTING.md` 4; baked into `marketplace-create.sh`. |
| B7 | **Product logo** (square PNG) | Done | `marketplace/assets/ferro-stash-logo-square.png` (900x900); `marketplace-create.sh` stages it to the seller media bucket + presigns it for `LogoUrl`. |
| B8 | **Claims do not over-state capability** | Done | Cross-checked against `README.md` (Honest limitations) + `docs/COMPATIBILITY.md`; see `LISTING.md` 8. **Must not market "100% compatible" / "drop-in", and must not position against any named AWS service.** |
| B9 | **Support contact** surfaced | Done | **Email only**: `aws-support@abyo.net` (`SupportDescription` in `marketplace-create.sh`). |
| B10 | **Refund policy** | Done (text) / 🔒 OWNER (console) | `marketplace-pricing.sh` sets a `SupportTerm` RefundPolicy (<= 500 chars). |
| B11 | **No github.com URL in listing/support copy** | Done | Marketplace listing/support text stays email-only. `marketplace-create.sh` and `marketplace-ami.sh` each `grep`-guard the generated JSON and abort on any github.com URL; `LISTING.md` and the AMI-shipped files are github-free. |

## C. AMI product -- self-service AMI scanning policy

| # | Requirement | Status | Notes |
|---|---|---|---|
| C1 | AMI built from a pinned release version (no `latest`) | In progress (build pipeline) | `marketplace/packer/` bakes the version into `/etc/aws-marketplace/ferro-stash-version`; pin the release tag + a base-AMI ID at build for full reproducibility. |
| C2 | **AMI scanning** passes (no disallowed ports, no high CVEs, no hard-coded secrets) | 🔒 OWNER | `marketplace-ami.sh` AddDeliveryOptions triggers the async AWS scan; remediate findings. The AMI has **no default credentials** and **no baked secret** (FerroStash has no auth surface). |
| C3 | **Single, hardened OS**, no default/blank passwords, SSH locked down | Done (Packer) / 🔒 OWNER (verify built AMI) | `60-harden.sh` (SELinux enforcing, fail2ban, dnf-automatic, root/password SSH off); `90-marketplace-finalise.sh` removes host keys + authorized_keys. |
| C4 | AMI **region availability** | 🔒 OWNER (Catalog run) | `marketplace-create.sh` AddRegions sets the 17-region set. |
| C5 | **Usage instructions** for buyers | Done | `marketplace-ami.sh` AddDeliveryOptions `UsageInstructions` + first-boot `/var/lib/ferro-stash/initial-info.txt`. |
| C6 | **Recommended instance types** declared | Done | t4g / c7g / m7g / r7g classes; recommended `c7g.large` (`LISTING.md` 6/7). |
| C7 | No overly-permissive default security group | Done (delivery option) / 🔒 OWNER (verify) | The delivery option opens only the Beats input port (TCP 5044) to the private `10.0.0.0/8` range; the monitoring API binds localhost only and is not opened. |
| C8 | AMI **product code** present for paid metering | In progress (placeholder) / 🔒 OWNER | `90-marketplace-finalise.sh` bakes `var.marketplace_product_code` into `/etc/aws-marketplace/productcode` (placeholder `REPLACE-WITH-SELLER-PRODUCT-CODE`); set the real AWS-issued code at build. **No metering code needed** (AWS auto-meters instance-hours). |

## D. Pricing & contract -- mostly 🔒 OWNER

| # | Requirement | Status | Notes |
|---|---|---|---|
| D1 | **AMI hourly + annual** dimensions (per instance type) | Done (structure) / 🔒 OWNER (confirm amounts) | `marketplace-pricing.sh` AddDimensions + UpdatePricingTerms; ladder anchored at `c7g.large` `$0.06`/hr, `$370`/yr (PROPOSED). |
| D2 | **Free trial** decision | 🔒 OWNER | Recommended (e.g. 30 days); set in console. |
| D3 | **Private offers** plan | 🔒 OWNER | Configure in console if desired. |
| D4 | **EULA / Standard Contract** | Done (attached) / 🔒 OWNER (accept) | `StandardEula` 2022-07-14 (`marketplace-pricing.sh`). |
| D5 | **Refund policy** field | Done (text) / 🔒 OWNER (console) | `marketplace-pricing.sh` (<= 500 chars). |

## E. Support & SLA

| # | Requirement | Status | Notes |
|---|---|---|---|
| E1 | **Support channel** documented | Done | **Email only**: `aws-support@abyo.net`. |
| E2 | **Refund / billing-error path** | Done | RefundPolicy text routes to `aws-support@abyo.net`. |
| E3 | `aws-support@abyo.net` mailbox provisioned | 🔒 OWNER | Confirm the alias is monitored before publish. |

## F. Versioning & lifecycle

| # | Requirement | Status | Notes |
|---|---|---|---|
| F1 | Listed artifacts tagged to the **current GA version** | 🔒 OWNER (confirm at go-live) | Build the AMI from the `v1.0.0` tag; the version is baked into the AMI tags + `/etc/aws-marketplace/ferro-stash-version`. |
| F2 | Honest maturity representation | Done | The listing states "no public production deployments yet" (`LISTING.md` 8). Do not represent the listing as production-proven. |

## G. Build / engineering prerequisites (this repo)

| # | Item | Status | Notes |
|---|---|---|---|
| G1 | Packer toolkit | Done | `marketplace/packer/` (HCL + 8 scripts + units + default pipeline + tests). `packer validate` + `bash -n` + `shellcheck` clean. |
| G2 | Cross-build wiring | Done | `marketplace/packer/build.sh` + `Cross.toml`: `cross` build of `aarch64-unknown-linux-gnu` with cmake/clang in the image for rdkafka's vendored librdkafka; sccache disabled. |
| G3 | rdkafka / glibc caveat documented | Done | See `marketplace/packer/README.md` "rdkafka / glibc caveat": GNU target -> glibc-dynamic binary; keep the cross image glibc <= AL2023's. |
| G4 | Catalog-API trilogy + release | Done | `deploy/marketplace-{create,ami,pricing,release}.sh`. |
| G5 | Logo asset | Done | `marketplace/assets/ferro-stash-logo-square.png` (900x900) + 640/wide/transparent variants via `make_logo.py`. |

---

## What remains to publish (owner summary)

1. **🔒 Seller account**: complete A2-A3 (tax, banking) if not already
   done for the sibling listings.
2. **🔒 Build + scan the AMI**: run `marketplace/packer/build.sh --arch
   arm64` (with Docker + `cross`) from the `v1.0.0` tag with the real
   product code, then `deploy/marketplace-ami.sh` (AddDeliveryOptions
   triggers the AWS scan). Remediate any scan findings.
3. **🔒 Confirm pricing**: confirm the PROPOSED ladder (anchor
   `c7g.large` `$0.06`/hr, `$370`/yr) and decide on a free trial, then run
   `deploy/marketplace-pricing.sh APPLY=1`.
4. **🔒 Mailbox**: confirm `aws-support@abyo.net` is monitored.
5. **🔒 Release**: after the scan is clean and pricing is set, run
   `CONFIRM=yes deploy/marketplace-release.sh` (IRREVERSIBLE; product moves
   Draft -> Limited -> Public after AWS review).
