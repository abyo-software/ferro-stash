#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Create + populate the FerroStash CONTAINER product (ContainerProduct@1.0) via
# the AWS Marketplace Catalog API, on the `as` seller account (393886308285).
# This is the metered-container sibling of the AMI tooling
# (deploy/marketplace-create.sh + marketplace-pricing.sh + marketplace-release.sh).
#
# The product is a PAID, RegisterUsage-METERED container: a single hourly
# dimension "Hours" (ExternallyMetered). The image calls RegisterUsage once at
# startup as an entitlement gate (the binary built `--features marketplace`, see
# crates/ferro-stash-cli/src/marketplace.rs); AWS meters per running pod-hour.
# Delivery is a Helm chart on Amazon EKS.
#
# WHAT RUNS BY DEFAULT (all reversible Draft steps, NOT buyer-visible):
#   1. CreateProduct (ContainerProduct@1.0)   - reversible
#   2. UpdateInformation                       - descriptions, highlights, cats, logo, email support
#   3. AddDimensions                           - single Hours/ExternallyMetered (BARE ARRAY)
#   4. AddRepositories                         - ECR repos `ferro-stash` and `ferro-stash-helm`
# It then PRINTS the product id AND the product code (needed for the Helm value).
#
# WHAT IS GATED (printed by default; run only when its gate is set) - mirrors the
# AMI scripts' APPLY=1 / CONFIRM=yes discipline:
#   * docker build + push and helm push          -> ALWAYS PRINTED, never run here
#                                                   (build/push happens outside; see
#                                                    marketplace/docker/README.md).
#   * OFFER=1   -> CreateOffer + UpdatePricingTerms ($0.04/Hours) + UpdateLegalTerms
#                  (StandardEula) + UpdateSupportTerms (RefundPolicy<=500) + offer
#                  UpdateInformation (Name+Desc<=255).
#   * RELEASE=1 CONFIRM=yes  (needs OFFER=offer-xxxx) -> IRREVERSIBLE
#                  ReleaseProduct + ReleaseOffer together (product -> Limited).
#   * DELIVERY=1  (needs IMAGE_TAG=... CHART_VERSION=...; product must be Limited)
#                  -> AddDeliveryOptions (EcrDeliveryOptionDetails: ContainerImages,
#                     HelmDeploymentTemplate, CompatibleServices ["EKS"]).
#
# ORDERING NOTE (containers are the REVERSE of AMIs): AddDeliveryOptions requires
# the product be Limited/Restricted/Public, NOT Draft. So: set up Draft -> push
# image + chart -> OFFER -> RELEASE (-> Limited) -> DELIVERY -> AWS scans the
# container -> the version goes Public.
#
# All field text is plain ASCII. The accuracy contract from
# docs/marketplace/LISTING.md is honoured: FerroStash is "Logstash-compatible"
# (config/pipeline), NEVER "100% compatible" or "drop-in", positioned against no
# named AWS service. The repo is PRIVATE, so NO github.com URL appears in any
# listing field (a github link 404s for the reviewer and gets the listing
# rejected). Support is EMAIL ONLY (aws-support@abyo.net).
#
# Usage:
#   PROFILE=as REGION=us-east-1 deploy/marketplace-container.sh
#   # resume an existing draft (do NOT re-run CreateProduct - it duplicates):
#   PID=prod-xxxx deploy/marketplace-container.sh
#   # offer + pricing + legal/support:
#   PID=prod-xxxx OFFER_STEP=1 deploy/marketplace-container.sh
#   # IRREVERSIBLE release:
#   PID=prod-xxxx OFFER=offer-xxxx RELEASE=1 CONFIRM=yes deploy/marketplace-container.sh
#   # post-release delivery option (after image+chart are pushed):
#   PID=prod-xxxx DELIVERY=1 IMAGE_TAG=1.0.0 CHART_VERSION=1.0.0 deploy/marketplace-container.sh
set -euo pipefail

PROFILE="${PROFILE:-as}"
REGION="${REGION:-us-east-1}"        # Marketplace Catalog API home region
CATALOG=AWSMarketplace
TITLE="${TITLE:-FerroStash Container - Rust-native Logstash-compatible log pipeline}"
PID="${PID:-}"                       # set to skip CreateProduct and only (re)populate
OFFER="${OFFER:-}"                   # set to skip CreateOffer / for RELEASE

# Single ExternallyMetered hourly dimension. $0.04/hr, NO annual.
HOURLY_USD="${HOURLY_USD:-0.04}"
CURRENCY="${CURRENCY:-USD}"

# Marketplace ECR registry + the two repos AddRepositories provisions. AWS
# prefixes the seller namespace, so the bare RepositoryName is `ferro-stash` /
# `ferro-stash-helm` and the resulting refs are under abyo-software/.
ECR_REGISTRY="${ECR_REGISTRY:-709825985650.dkr.ecr.us-east-1.amazonaws.com}"
ECR_NAMESPACE="${ECR_NAMESPACE:-abyo-software}"
IMAGE_REPO="${IMAGE_REPO:-ferro-stash}"
HELM_REPO="${HELM_REPO:-ferro-stash-helm}"
IMAGE_TAG="${IMAGE_TAG:-1.0.0}"
CHART_VERSION="${CHART_VERSION:-1.0.0}"

# Logo reuse: same square logo as the AMI product, staged to the seller's
# private media bucket and handed to AWS as a presigned GET URL.
LOGO_URL="${LOGO_URL:-}"
LOGO_BUCKET="${LOGO_BUCKET:-abyo-mp-media-393886308285}"
LOGO_KEY="${LOGO_KEY:-ferrostash-square.png}"
LOGO_FILE="${LOGO_FILE:-marketplace/assets/ferro-stash-logo-square.png}"

# Valid AWS Marketplace categories.
CATS="${CATS:-[\"Data Analytics\",\"Monitoring\"]}"

mc() { aws --profile "$PROFILE" --region "$REGION" marketplace-catalog "$@"; }
wait_done() {
  local id="$1" st
  while :; do
    st=$(mc describe-change-set --catalog "$CATALOG" --change-set-id "$id" --query Status --output text)
    case "$st" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED) echo "change set $id $st:"; mc describe-change-set --catalog "$CATALOG" --change-set-id "$id" --query 'ChangeSet[].ErrorDetailList' --output json; return 1 ;;
      *) sleep 8 ;;
    esac
  done
}
# Guard a generated JSON doc: plain ASCII only, and no github.com URL (private repo).
guard_json() {
  local f="$1"
  if LC_ALL=C grep -qP '[^\x09\x0a\x0d\x20-\x7e]' "$f"; then
    echo "ERROR: non-ASCII byte in $f; aborting." >&2; exit 1
  fi
  if grep -qi 'github\.com' "$f"; then
    echo "ERROR: github.com URL in $f; the repo is private - aborting." >&2; exit 1
  fi
}
# Best-effort extraction of the container ProductCode from describe-entity.
get_product_code() {
  local pid="$1"
  mc describe-entity --catalog "$CATALOG" --entity-id "$pid" --query 'DetailsDocument' --output text 2>/dev/null \
    | python3 -c '
import json, sys
raw = sys.stdin.read().strip()
try:
    d = json.loads(raw)
except Exception:
    print(""); sys.exit(0)
def find(o):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == "ProductCode" and isinstance(v, str):
                return v
            r = find(v)
            if r:
                return r
    elif isinstance(o, list):
        for v in o:
            r = find(v)
            if r:
                return r
    return None
print(find(d) or "")
'
}

# ===========================================================================
# 1. CreateProduct (Draft, reversible)
# ===========================================================================
if [ -z "$PID" ]; then
  echo "1/4 CreateProduct (ContainerProduct@1.0, Draft)..."
  CID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-create \
    --change-set '[{"ChangeType":"CreateProduct","Entity":{"Type":"ContainerProduct@1.0"},"DetailsDocument":{"ProductTitle":"'"$TITLE"'"}}]' \
    --query ChangeSetId --output text)
  wait_done "$CID"
  PID=$(mc describe-change-set --catalog "$CATALOG" --change-set-id "$CID" --query 'ChangeSet[0].Entity.Identifier' --output text)
  PID="${PID%@*}"
  echo "  product id: $PID"
else
  echo "1/4 CreateProduct - SKIPPED (resuming existing PID=$PID)"
  PID="${PID%@*}"
fi

# Gated invocations (OFFER_STEP / RELEASE / DELIVERY) act on an existing,
# already-populated product. Re-running UpdateInformation / AddDimensions /
# AddRepositories then is pointless and HARMFUL: on a Limited product
# UpdateInformation enters a slow APPLYING cycle that blocks the gated step for
# 30-60+ min (and AddRepositories always fails as a duplicate). Skip them.
GATED=0
if [ "${OFFER_STEP:-0}" = "1" ] || [ "${RELEASE:-0}" = "1" ] || [ "${DELIVERY:-0}" = "1" ]; then
  GATED=1
  echo "Gated step (OFFER_STEP/RELEASE/DELIVERY) - skipping UpdateInformation/AddDimensions/AddRepositories."
fi
if [ "$GATED" != "1" ]; then

# Stage the logo if no URL was supplied (UpdateInformation requires one).
if [ -z "$LOGO_URL" ]; then
  if [ -f "$LOGO_FILE" ]; then
    echo "  staging logo $LOGO_FILE -> s3://$LOGO_BUCKET/$LOGO_KEY (private; presigned for AWS)..."
    aws --profile "$PROFILE" --region "$REGION" s3 cp "$LOGO_FILE" "s3://$LOGO_BUCKET/$LOGO_KEY" --content-type image/png >/dev/null
    LOGO_URL=$(aws --profile "$PROFILE" --region "$REGION" s3 presign "s3://$LOGO_BUCKET/$LOGO_KEY" --expires-in 604800)
  else
    echo "ERROR: no LOGO_URL and $LOGO_FILE not found; UpdateInformation requires a logo." >&2; exit 1
  fi
fi

# ===========================================================================
# 2. UpdateInformation (descriptions + highlights + categories + logo + support)
# ===========================================================================
echo "2/4 UpdateInformation (container listing copy + logo + email support)..."
python3 - "$PID" "$LOGO_URL" "$CATS" > /tmp/cs-ferrostash-ctr-info.json <<'PY'
import json, sys
pid, logo, cats = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
short = ("Rust-native, Logstash-compatible log and event pipeline as a container "
  "for Amazon EKS. One static binary, no JVM. 90+ inputs/filters/codecs/outputs; "
  "on-disk queue. Deployed by Helm; metered per pod-hour.")
long = ("FerroStash is a Rust-native, Logstash-compatible log and event pipeline, "
  "packaged here as a container for Amazon EKS and delivered by a Helm chart. It "
  "ingests, transforms, and routes events through the same input -> filter -> "
  "output model as Logstash, parsing the Logstash pipeline.conf DSL (and an "
  "equivalent YAML form) natively - without a JVM and without a separate agent "
  "runtime. Where a Logstash pipeline commonly holds about a gigabyte of JVM heap "
  "and takes tens of seconds to start, FerroStash runs as a single static binary "
  "(about 14 MB) that starts in milliseconds and holds tens of MB of RAM, so you "
  "can pack far more shippers per node. "
  "What FerroStash does today (v1.0 line): it implements the production-common "
  "subset of the Logstash 9.x plugin set - about 88 percent of the bundled plugins "
  "(98 of 111), weighted toward the parsing and filtering hot path. Inputs include "
  "beats, file, tcp, udp, http, http_poller, syslog, kafka, redis, s3, sqs, jdbc, "
  "elasticsearch, cloudwatch, rabbitmq, and the dead-letter-queue. Filters include "
  "grok, dissect, kv, json, mutate, date, geoip, dns, csv, xml, useragent, cidr, "
  "fingerprint, translate, aggregate, throttle, and ruby, plus a native "
  "Painless-style script filter. Outputs include elasticsearch / opensearch, "
  "kafka, s3, http, tcp, udp, file, redis, sqs, sns, cloudwatch, email, and "
  "datadog. Codecs include json, json_lines, multiline, cef, netflow, avro, "
  "msgpack, and protobuf. "
  "Reliability: an optional on-disk persistent queue with full at-least-once "
  "delivery (read/ack cursor separation, checkpoint-after-output-ack) and a "
  "dead-letter queue, with opt-in fsync for power-loss durability. A built-in "
  "monitoring API exposes node and pipeline stats. "
  "Deployment and billing: the chart deploys a single-node pipeline; configure it "
  "via the chart pipelineConf value (rendered into a ConfigMap). The container is "
  "metered by AWS per running pod-hour - it calls RegisterUsage once at startup as "
  "an entitlement check, so the pod needs AWS credentials (IRSA / Pod Identity) "
  "with aws-marketplace:RegisterUsage and the product code wired in by the chart. "
  "Engineering posture (verifiable): unsafe_code is denied workspace-wide (with "
  "narrow, audited exceptions for the optional mruby FFI and the script-filter "
  "JIT), clippy is clean at -D warnings with unwrap() denied in production code, a "
  "cargo deny supply-chain gate runs in CI, and the test suite runs 1,400+ tests "
  "with output verified byte-for-byte against Logstash 9.4.2 across 24 parity "
  "fixtures. "
  "Honest scope (read before you buy): FerroStash is Logstash config / pipeline "
  "compatible, NOT a byte-identical 100 percent drop-in - coverage is plugin-level "
  "(about 88 percent of bundled plugins), and a covered plugin may implement only "
  "a subset of that plugin's options; a config that uses a missing plugin fails "
  "fast at load with a clear error rather than silently dropping events. The "
  "remaining gaps are mostly enterprise / niche connectors (for example jms, "
  "azure_event_hubs, snmp, lumberjack, webhdfs). This is a single-developer "
  "project with a SemVer-stable surface but NO public production deployments yet - "
  "run it beside your existing pipeline before trusting it with irreplaceable "
  "data. The supported deployment topology is single-node. The optional ruby "
  "filter (Artichoke/mruby) is excluded from this container build. "
  "This listing sells a hardened, scanned, supported distribution built from the "
  "Apache-2.0 source at a pinned release version (a standard open-core commercial "
  "model); the code itself stays Apache-2.0 and the listing does not relicense it.")
highlights = [
  ("Rust-native with no JVM: a single static container runs Logstash-style "
   "pipeline configs (DSL or YAML) on EKS with low memory use and fast startup."),
  ("90+ inputs, filters, codecs and outputs including grok, mutate, JSON, "
   "Painless-style scripting, Kafka, S3, and Elasticsearch/OpenSearch."),
  ("At-least-once delivery with an on-disk persistent queue and dead-letter "
   "queue; deployed by Helm and metered per pod-hour."),
]
det = {
  "ProductTitle": "FerroStash Container - Rust-native Logstash-compatible log pipeline",
  "ShortDescription": short,
  "LongDescription": long,
  "Highlights": highlights,
  "Categories": cats,
  "SearchKeywords": ["logstash","log pipeline","observability","etl","grok",
    "elasticsearch","opensearch","kafka","data pipeline","log shipping",
    "rust","eks","kubernetes","helm"],
  # Email-only support: the source repository is private, so a GitHub URL 404s
  # for the AWS reviewer and gets the listing rejected.
  "SupportDescription": ("Marketplace subscribers receive support by email at "
    "aws-support@abyo.net under the published SLA. Include your AWS account id and "
    "the EKS cluster / pod details when you open a ticket. A limitations document, "
    "a Logstash compatibility matrix, and a due-diligence pack ship with the "
    "product."),
}
if logo:
    det["LogoUrl"] = logo
print(json.dumps([{
  "ChangeType": "UpdateInformation",
  "Entity": {"Type": "ContainerProduct@1.0", "Identifier": pid},
  "DetailsDocument": det,
}]))
PY
guard_json /tmp/cs-ferrostash-ctr-info.json
UCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-info \
  --change-set "file:///tmp/cs-ferrostash-ctr-info.json" --query ChangeSetId --output text)
wait_done "$UCID"

# INFO_ONLY re-applies just the listing copy (e.g. a corrected description on an
# already-released Limited product before AWS review) without re-touching the
# dimension / repositories.
if [ "${INFO_ONLY:-0}" = "1" ]; then
  echo "INFO_ONLY=1: UpdateInformation applied to $PID; skipping AddDimensions/AddRepositories."
  exit 0
fi

# ===========================================================================
# 3. AddDimensions - single Hours / ExternallyMetered. DetailsDocument is a
#    BARE ARRAY (wrapping it in {"Dimensions":...} -> ValidationException).
# ===========================================================================
echo "3/4 AddDimensions (single Hours / ExternallyMetered, bare array)..."
cat > /tmp/cs-ferrostash-ctr-dims.json <<EOF
[{"ChangeType":"AddDimensions","Entity":{"Type":"ContainerProduct@1.0","Identifier":"$PID"},
  "DetailsDocument":[{"Name":"Hours","Description":"Per pod-hour usage","Key":"Hours","Unit":"UnitHrs","Types":["ExternallyMetered"]}]}]
EOF
guard_json /tmp/cs-ferrostash-ctr-dims.json
if DCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-dims \
    --change-set "file:///tmp/cs-ferrostash-ctr-dims.json" --query ChangeSetId --output text 2>/dev/null); then
  wait_done "$DCID" || echo "  (AddDimensions failed/duplicate - continuing; the dimension may already exist)"
fi

# ===========================================================================
# 4. AddRepositories - provision the two Marketplace ECR repos.
# ===========================================================================
echo "4/4 AddRepositories ($IMAGE_REPO + $HELM_REPO, ECR)..."
cat > /tmp/cs-ferrostash-ctr-repos.json <<EOF
[{"ChangeType":"AddRepositories","Entity":{"Type":"ContainerProduct@1.0","Identifier":"$PID"},
  "DetailsDocument":{"Repositories":[{"RepositoryName":"$IMAGE_REPO","RepositoryType":"ECR"},{"RepositoryName":"$HELM_REPO","RepositoryType":"ECR"}]}}]
EOF
guard_json /tmp/cs-ferrostash-ctr-repos.json
if RCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-repos \
    --change-set "file:///tmp/cs-ferrostash-ctr-repos.json" --query ChangeSetId --output text 2>/dev/null); then
  wait_done "$RCID" || echo "  (AddRepositories failed/duplicate - continuing; repos may already exist)"
fi

fi  # end of non-gated reversible pre-steps (GATED skip)

PRODUCT_CODE="$(get_product_code "$PID")"
echo
echo "Draft container product populated: PID=$PID"
echo "  ProductCode: ${PRODUCT_CODE:-<not yet available - re-query describe-entity>}"
echo "  ECR image repo: $ECR_REGISTRY/$ECR_NAMESPACE/$IMAGE_REPO"
echo "  ECR helm repo:  $ECR_REGISTRY/$ECR_NAMESPACE/$HELM_REPO"
echo "  Set the Helm value marketplace.productCode=$PRODUCT_CODE at deploy time."

# ===========================================================================
# GATED: CreateOffer + pricing + legal + support + offer info  (OFFER_STEP=1)
# ===========================================================================
if [ "${OFFER_STEP:-0}" = "1" ]; then
  if [ -z "$OFFER" ]; then
    echo "OFFER_STEP=1: CreateOffer for product $PID..."
    OCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-offer \
      --change-set '[{"ChangeType":"CreateOffer","Entity":{"Type":"Offer@1.0"},"DetailsDocument":{"ProductId":"'"$PID"'"}}]' \
      --query ChangeSetId --output text)
    wait_done "$OCID"
    OFFER=$(mc describe-change-set --catalog "$CATALOG" --change-set-id "$OCID" --query 'ChangeSet[0].Entity.Identifier' --output text)
    OFFER="${OFFER%@*}"
    echo "  offer id: $OFFER"
  fi

  # Read the ACTUAL dimension key AWS assigned (it may be "PID1", not the "Key"
  # we passed to AddDimensions) and price THAT key. Hardcoding "Hours" produced a
  # rate card that did not match the real dimension -> metered usage had no price
  # and the released offer could not be corrected afterwards.
  DIM_KEY=$(mc describe-entity --catalog "$CATALOG" --entity-id "$PID" --query 'DetailsDocument.Dimensions[0].Key' --output text)
  echo "UpdatePricingTerms (Usage; \$$HOURLY_USD per dimension '$DIM_KEY'; no annual)..."
  python3 - "$OFFER" "$CURRENCY" "$HOURLY_USD" "$DIM_KEY" > /tmp/cs-ferrostash-ctr-pricing.json <<'PY'
import json, sys
offer, ccy, price, dimkey = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
print(json.dumps([{
  "ChangeType": "UpdatePricingTerms",
  "Entity": {"Type": "Offer@1.0", "Identifier": offer},
  "DetailsDocument": {
    "PricingModel": "Usage",
    "Terms": [{
      "Type": "UsageBasedPricingTerm", "CurrencyCode": ccy,
      "RateCards": [{"RateCard": [{"DimensionKey": dimkey, "Price": price}]}],
    }],
  },
}]))
PY
  guard_json /tmp/cs-ferrostash-ctr-pricing.json
  PCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-pricing \
    --change-set "file:///tmp/cs-ferrostash-ctr-pricing.json" --query ChangeSetId --output text)
  wait_done "$PCID" || echo "  (pricing change-set issue - check)"

  echo "UpdateLegalTerms (StandardEula)..."
  LCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-legal --change-set '[{
    "ChangeType":"UpdateLegalTerms","Entity":{"Type":"Offer@1.0","Identifier":"'"$OFFER"'"},
    "DetailsDocument":{"Terms":[{"Type":"LegalTerm","Documents":[{"Type":"StandardEula","Version":"2022-07-14"}]}]}
  }]' --query ChangeSetId --output text)
  wait_done "$LCID" || echo "  (legal terms change-set issue - check)"

  echo "UpdateSupportTerms (RefundPolicy)..."
  python3 - "$OFFER" > /tmp/cs-ferrostash-ctr-support.json <<'PY'
import json, sys
offer = sys.argv[1]
refund = ("Contact aws-support@abyo.net within 30 days of a charge to request a refund for a "
  "billing error or a documented defect in the supported distribution. Refunds are processed "
  "in accordance with the AWS Marketplace Standard Contract.")
assert len(refund) <= 500, len(refund)
print(json.dumps([{"ChangeType":"UpdateSupportTerms","Entity":{"Type":"Offer@1.0","Identifier":offer},
  "DetailsDocument":{"Terms":[{"Type":"SupportTerm","RefundPolicy":refund}]}}]))
PY
  guard_json /tmp/cs-ferrostash-ctr-support.json
  SCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-support \
    --change-set "file:///tmp/cs-ferrostash-ctr-support.json" --query ChangeSetId --output text)
  wait_done "$SCID" || echo "  (support terms change-set issue - check)"

  echo "UpdateInformation on the offer (Name + Description)..."
  python3 - "$OFFER" "$HOURLY_USD" > /tmp/cs-ferrostash-ctr-offer-info.json <<'PY'
import json, sys
offer, price = sys.argv[1], sys.argv[2]
name = "FerroStash - hourly metered container (EKS / Helm)"
desc = ("Pay-as-you-go pricing for the FerroStash container on Amazon EKS, metered by AWS per "
  "running pod-hour at $" + price + "/hour (no annual commitment). Delivered as a Helm chart. "
  "Apache-2.0 open-core; hardened, scanned distribution.")
assert len(desc) <= 255, len(desc)
print(json.dumps([{"ChangeType":"UpdateInformation","Entity":{"Type":"Offer@1.0","Identifier":offer},
  "DetailsDocument":{"Name":name,"Description":desc}}]))
PY
  guard_json /tmp/cs-ferrostash-ctr-offer-info.json
  ICID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-offer-info \
    --change-set "file:///tmp/cs-ferrostash-ctr-offer-info.json" --query ChangeSetId --output text)
  wait_done "$ICID" || echo "  (offer info change-set issue - check)"
  echo "Offer $OFFER: pricing + legal + support + identity set."
fi

# ===========================================================================
# GATED: IRREVERSIBLE ReleaseProduct + ReleaseOffer  (RELEASE=1 CONFIRM=yes)
# ===========================================================================
if [ "${RELEASE:-0}" = "1" ]; then
  if [ "${CONFIRM:-}" != "yes" ]; then
    echo "REFUSING release: set CONFIRM=yes to submit the IRREVERSIBLE release of $PID + $OFFER." >&2
    exit 2
  fi
  [ -n "$OFFER" ] || { echo "RELEASE=1 requires OFFER=offer-xxxx (run OFFER_STEP=1 first)." >&2; exit 2; }
  echo "RELEASE: ReleaseProduct + ReleaseOffer (IRREVERSIBLE)..."
  cat > /tmp/cs-ferrostash-ctr-release.json <<EOF
[
 {"ChangeType":"ReleaseProduct","Entity":{"Type":"ContainerProduct@1.0","Identifier":"$PID"},"DetailsDocument":{}},
 {"ChangeType":"ReleaseOffer","Entity":{"Type":"Offer@1.0","Identifier":"$OFFER"},"DetailsDocument":{}}
]
EOF
  RLID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-release \
    --change-set "file:///tmp/cs-ferrostash-ctr-release.json" --query ChangeSetId --output text)
  echo "  change-set: $RLID (the first release to Limited can sit APPLYING 30-60 min)"
  wait_done "$RLID" && echo "  RELEASED -> product Limited. Now run DELIVERY=1 (after image+chart pushed)."
fi

# ===========================================================================
# GATED: post-release AddDeliveryOptions  (DELIVERY=1; product must be Limited)
# ===========================================================================
if [ "${DELIVERY:-0}" = "1" ]; then
  IMG_REF="$ECR_REGISTRY/$ECR_NAMESPACE/$IMAGE_REPO:$IMAGE_TAG"
  HELM_REF="$ECR_REGISTRY/$ECR_NAMESPACE/$HELM_REPO:$CHART_VERSION"
  echo "DELIVERY: AddDeliveryOptions (image $IMG_REF + helm $HELM_REF, EKS)..."
  python3 - "$PID" "$IMG_REF" "$HELM_REF" "$IMAGE_TAG" > /tmp/cs-ferrostash-ctr-delivery.json <<'PY'
import json, sys
pid, img, helm, vtitle = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
details = {
  "Version": {
    "VersionTitle": vtitle,
    "ReleaseNotes": ("FerroStash v1.0 line, container for Amazon EKS (Helm). Rust-native, "
      "Logstash-compatible log and event pipeline: a single static binary - no JVM, no "
      "separate agent runtime - that starts in milliseconds and holds tens of MB of RAM. "
      "Implements about 88 percent of the bundled Logstash 9.x plugins (98 of 111), weighted "
      "toward the parsing and filtering hot path. Optional on-disk persistent queue with "
      "at-least-once delivery and a dead-letter queue; built-in monitoring API. Honest scope: "
      "Logstash config / pipeline compatible, NOT a byte-identical 100 percent drop-in; a "
      "config using a missing plugin fails fast at load. Single-developer project, single-node "
      "topology, no public production deployments yet; the optional ruby filter is excluded "
      "from this build. Metered per pod-hour via RegisterUsage."),
  },
  "DeliveryOptions": [{
    "DeliveryOptionTitle": "FerroStash on Amazon EKS (Helm)",
    "Details": {"EcrDeliveryOptionDetails": {
      "ContainerImages": [img],
      "DeploymentResources": [{"Name": "HelmDeploymentTemplate", "Url": helm}],
      "CompatibleServices": ["EKS"],
      "Description": ("Deploy FerroStash on Amazon EKS with the bundled Helm chart "
        "(ferro-stash-helm). Set marketplace.productCode to this product's code and ensure the "
        "pod has AWS credentials (IRSA / Pod Identity) with aws-marketplace:RegisterUsage; the "
        "container verifies entitlement once at startup and is metered per pod-hour."),
      "UsageInstructions": ("helm install ferro-stash oci://" + helm.rsplit(':', 1)[0] +
        " --version " + vtitle + " --set marketplace.productCode=<PRODUCT_CODE> "
        "--set marketplace.awsRegion=<REGION>. Edit the pipelineConf value (rendered into a "
        "ConfigMap at /etc/ferro-stash/pipeline.conf) to point at your real inputs and outputs "
        "(for example Elasticsearch/OpenSearch, Kafka, or S3). The monitoring API is exposed on "
        "port 9600 via the chart Service; the default pipeline accepts Elastic Beats on TCP "
        "5044. The supported topology is single-node."),
    }},
  }],
}
print(json.dumps([{
  "ChangeType": "AddDeliveryOptions",
  "Entity": {"Type": "ContainerProduct@1.0", "Identifier": pid},
  "DetailsDocument": details,
}]))
PY
  guard_json /tmp/cs-ferrostash-ctr-delivery.json
  DLID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ctr-delivery \
    --change-set "file:///tmp/cs-ferrostash-ctr-delivery.json" --query ChangeSetId --output text)
  wait_done "$DLID" && echo "  Submitted. AWS now scans the container image; then make the version Public."
fi

# ===========================================================================
# NEXT STEPS (always printed; the push commands are NEVER run by this script)
# ===========================================================================
cat <<EOF

================================================================
FerroStash CONTAINER product: PID=$PID  ProductCode=${PRODUCT_CODE:-<query describe-entity>}
================================================================

A) BUILD + PUSH the single-manifest amd64 image (see marketplace/docker/README.md).
   ECR tags are IMMUTABLE - a wrong push cannot be overwritten; bump the tag.

   IMG=$ECR_REGISTRY/$ECR_NAMESPACE/$IMAGE_REPO:$IMAGE_TAG
   docker build --provenance=false --sbom=false --platform linux/amd64 \\
     -f marketplace/docker/Dockerfile.marketplace -t "\$IMG" .
   docker buildx imagetools inspect "\$IMG" --format '{{.Manifest.MediaType}}'
     # must be application/vnd.docker.distribution.manifest.v2+json (NOT an index)
   aws --profile $PROFILE ecr get-login-password --region $REGION \\
     | docker login --username AWS --password-stdin $ECR_REGISTRY
   docker push "\$IMG"

B) PUSH the Helm chart (chart name MUST be ferro-stash-helm so it lands at $HELM_REPO):
   helm registry login --username AWS \\
     --password "\$(aws --profile $PROFILE ecr get-login-password --region $REGION)" $ECR_REGISTRY
   helm package marketplace/helm/ferro-stash-helm   # -> ferro-stash-helm-$CHART_VERSION.tgz
   helm push ferro-stash-helm-$CHART_VERSION.tgz oci://$ECR_REGISTRY/$ECR_NAMESPACE

C) Offer + pricing + legal/support (\$$HOURLY_USD/Hours, no annual):
   PID=$PID OFFER_STEP=1 deploy/marketplace-container.sh

D) IRREVERSIBLE release (after A+B+C and a final GO):
   PID=$PID OFFER=offer-xxxx RELEASE=1 CONFIRM=yes deploy/marketplace-container.sh

E) Post-release delivery option (after the product is Limited and A+B are pushed):
   PID=$PID DELIVERY=1 IMAGE_TAG=$IMAGE_TAG CHART_VERSION=$CHART_VERSION deploy/marketplace-container.sh
   # AWS then scans the container; on a clean scan, make the version Public.
================================================================
EOF
