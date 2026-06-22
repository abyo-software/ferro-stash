#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Create + populate the FerroStash AMI product via the AWS Marketplace Catalog
# API, on the `as` seller account (393886308285). This script runs the
# NON-destructive Draft steps that need NO built AMI and NO pricing decision:
#   1. CreateProduct (Draft)            - reversible, not buyer-visible
#   2. UpdateInformation                - descriptions, highlights, category, keywords
#   3. AddRegions                       - the standard abyo commercial region set
#   4. AddInstanceTypes                 - Graviton/arm64 instance set
# The remaining steps are PRINTED, not run, because they each have a hard gate:
#   - AddDeliveryOptions needs a BUILT, seller-account-owned arm64 AMI shared with
#     the ingestion role (deploy/marketplace-ami.sh) and triggers a billable build
#     + an AWS security scan;
#   - hourly + annual PRICING is a business decision set on the Management Portal
#     AMI pricing page (per-instance-type hourly is NOT a Catalog AddDimensions
#     change for a standard hourly AMI - RUN_DIMENSIONS stays off, see
#     deploy/marketplace-pricing.sh);
#   - ReleaseProduct is IRREVERSIBLE (deploy/marketplace-release.sh, CONFIRM=yes).
#
# FerroStash is a STANDARD hourly + annual per-instance-type AMI product: AWS
# auto-meters instance-hours, so there is NO metering code in the AMI product
# (mirroring the sibling FerroSCA / FerroDruid / S4 hourly AMIs).
#
# All field text is plain ASCII (straight hyphens, no em/en-dash, no curly quotes).
# The accuracy contract from docs/marketplace/LISTING.md is honoured: FerroStash
# is marketed as "Logstash-compatible" (config / pipeline compatibility), NEVER as
# "100% compatible" or "drop-in", and the listing positions it against no named
# AWS service. NO github.com URL appears in any listing/support field; support is
# EMAIL ONLY through the seller support channel.
#
# PREREQUISITE: AWS Marketplace seller registration complete (it is for `as`).
# A logo is REQUIRED by UpdateInformation; if LOGO_URL is unset we stage the
# square logo into the seller's private media bucket and hand AWS a presigned
# GET URL.
#
# Usage:
#   PROFILE=as REGION=us-east-1 deploy/marketplace-create.sh
#   # resume an existing draft (do NOT re-run CreateProduct - it makes a duplicate):
#   PID=prod-xxxxxxxx deploy/marketplace-create.sh
set -euo pipefail

PROFILE="${PROFILE:-as}"
REGION="${REGION:-us-east-1}"        # Marketplace Catalog API home region
CATALOG=AWSMarketplace
TITLE="FerroStash - Rust-native, Logstash-compatible log and event pipeline"
PID="${PID:-}"                       # set to skip CreateProduct and only (re)populate
# UpdateInformation REQUIRES a LogoUrl. If LOGO_URL is unset we stage the square
# logo into the seller's private media bucket and hand AWS a presigned GET URL
# (AWS copies it into awsmp-logos.s3; the bucket stays private). Mirrors the
# FerroSCA + FerroDruid + S4 siblings (abyo-mp-media-393886308285).
LOGO_URL="${LOGO_URL:-}"
LOGO_BUCKET="${LOGO_BUCKET:-abyo-mp-media-393886308285}"
LOGO_KEY="${LOGO_KEY:-ferrostash-square.png}"
LOGO_FILE="${LOGO_FILE:-marketplace/assets/ferro-stash-logo-square.png}"
# FerroStash is a lightweight JVM-free single static binary (starts in ms, holds
# tens of MB of RAM); Graviton-first, ships an arm64 AMI for the initial product.
# The recommended type MUST be a member of INSTANCE_TYPES below.
INSTANCE_TYPE="${INSTANCE_TYPE:-c7g.large}"
INSTANCE_TYPES="${INSTANCE_TYPES:-t4g.small t4g.medium t4g.large c7g.medium c7g.large c7g.xlarge c7g.2xlarge m7g.large m7g.xlarge r7g.large}"
HOURLY_USD="${HOURLY_USD:-0.06}"     # reference hourly software fee (c7g.large / 2 vCPU); see deploy/marketplace-pricing.sh
ANNUAL_USD="${ANNUAL_USD:-370}"      # reference annual (~30% below 24x7 hourly); see deploy/marketplace-pricing.sh
# "Data Analytics" + "Monitoring" are valid AWS Marketplace categories. If a
# category errors at apply time the operator adjusts (both are in the known-valid
# set). Do NOT use singular / alternate forms.
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

if [ -z "$PID" ]; then
  echo "1/4 CreateProduct (Draft)..."
  CID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-create \
    --change-set '[{"ChangeType":"CreateProduct","Entity":{"Type":"AmiProduct@1.0"},"DetailsDocument":{"ProductTitle":"'"$TITLE"'"}}]' \
    --query ChangeSetId --output text)
  wait_done "$CID"
  PID=$(mc describe-change-set --catalog "$CATALOG" --change-set-id "$CID" --query 'ChangeSet[0].Entity.Identifier' --output text)
  PID="${PID%@*}"   # strip @revision
  echo "  product id: $PID"
else
  echo "1/4 CreateProduct - SKIPPED (resuming existing PID=$PID)"
  PID="${PID%@*}"
fi

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

echo "2/4 UpdateInformation (descriptions + highlights + category + keywords + logo)..."
python3 - "$PID" "$LOGO_URL" "$CATS" > /tmp/cs-ferrostash-info.json <<'PY'
import json, sys
pid, logo, cats = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
short = ("Rust-native, Logstash-compatible log and event pipeline. One static "
  "binary, no JVM. 90+ inputs/filters/codecs/outputs; on-disk queue. Hardened AMI.")
long = ("FerroStash is a Rust-native, Logstash-compatible log and event pipeline. "
  "It ingests, transforms, and routes events through the same input -> filter -> "
  "output model as Logstash, parsing the Logstash pipeline.conf DSL (and an "
  "equivalent YAML form) natively - without a JVM and without a separate agent "
  "runtime. Where a Logstash pipeline commonly holds about a gigabyte of JVM heap "
  "and takes tens of seconds to start, FerroStash runs as a single static binary "
  "(about 14 MB) that starts in milliseconds and holds tens of MB of RAM, so you "
  "can pack far more shippers per host. "
  "What FerroStash does today (v1.0 line): it implements the production-common "
  "subset of the Logstash 9.x plugin set - about 88 percent of the bundled plugins "
  "(98 of 111), weighted toward the parsing and filtering hot path. Inputs include "
  "beats, file, tcp, udp, http, http_poller, syslog, kafka, redis, s3, sqs, jdbc, "
  "elasticsearch, cloudwatch, rabbitmq, and the dead-letter-queue. Filters include "
  "grok, dissect, kv, json, mutate, date, geoip, dns, csv, xml, useragent, cidr, "
  "fingerprint, translate, aggregate, throttle, plus a native "
  "Painless-style script filter. Outputs include "
  "elasticsearch / opensearch, kafka, s3, http, tcp, udp, file, redis, sqs, sns, "
  "cloudwatch, email, and datadog. Codecs include json, json_lines, multiline, "
  "cef, netflow, avro, msgpack, and protobuf. "
  "Reliability: an optional on-disk persistent queue with full at-least-once "
  "delivery (read/ack cursor separation, checkpoint-after-output-ack) and a "
  "dead-letter queue, with opt-in fsync for power-loss durability. A built-in "
  "monitoring API exposes node and pipeline stats. "
  "Engineering posture (verifiable): unsafe_code is denied workspace-wide (with "
  "narrow, audited exceptions for the optional mruby FFI and the script-filter "
  "JIT), clippy is clean at -D warnings with unwrap() denied in production code, "
  "an SPDX header is on every source file, a cargo deny supply-chain gate runs in "
  "CI, and the test suite runs 1,400+ tests with output verified against "
  "Logstash 9.4.2 expected fields across 24 parity fixtures. "
  "Honest scope (read before you buy): FerroStash is Logstash config / pipeline "
  "compatible, NOT a byte-identical 100 percent drop-in - coverage is plugin-level "
  "(about 88 percent of bundled plugins), and a covered plugin may implement only "
  "a subset of that plugin's options; a config that uses a missing plugin fails "
  "fast at load with a clear error rather than silently dropping events. The "
  "remaining gaps are mostly enterprise / niche connectors (for example jms, "
  "azure_event_hubs, snmp, lumberjack, webhdfs). This is a single-developer "
  "project with a SemVer-stable surface but NO public production deployments yet - "
  "run it beside your existing pipeline before trusting it with irreplaceable "
  "data. The connector live-validation smoke tests verify reachability and a "
  "round-trip against real services, not exhaustive conformance. The supported "
  "Marketplace topology is single-node. The optional ruby filter (Artichoke/mruby) "
  "is excluded from the default build. "
  "This listing sells a hardened, scanned, supported, one-click distribution built "
  "from the Apache-2.0 source at a pinned release version (a standard open-core "
  "commercial model); the code itself stays Apache-2.0 and the listing does not "
  "relicense it. The AMI is metered automatically by AWS per running instance-hour "
  "- there is no metering code in the product.")
highlights = [
  ("Rust-native with no JVM: a single static binary runs Logstash-style pipeline "
   "configs (DSL or YAML) with low memory use and fast startup."),
  ("90+ inputs, filters, codecs and outputs including grok, mutate, JSON, "
   "Painless-style scripting, Kafka, S3, and Elasticsearch/OpenSearch."),
  ("At-least-once delivery with an on-disk persistent queue and dead-letter "
   "queue, plus optional fsync durability."),
]
det = {
  "ProductTitle": "FerroStash - Rust-native, Logstash-compatible log and event pipeline",
  "ShortDescription": short,
  "LongDescription": long,
  "Highlights": highlights,
  "Categories": cats,
  "SearchKeywords": ["logstash","log pipeline","observability","etl","grok",
    "elasticsearch","opensearch","kafka","data pipeline","log shipping",
    "rust","siem"],
  # Email-only support: Marketplace listing/support copy intentionally avoids
  # GitHub URLs and routes support through the seller support channel.
  "SupportDescription": ("Marketplace subscribers receive support by email at "
    "aws-support@abyo.net under the published SLA. Include your AWS account id and "
    "the EC2 instance id when you open a ticket. A limitations document, a Logstash "
    "compatibility matrix, and a due-diligence pack ship with the product."),
}
if logo:
    det["LogoUrl"] = logo
print(json.dumps([{
  "ChangeType": "UpdateInformation",
  "Entity": {"Type": "AmiProduct@1.0", "Identifier": pid},
  "DetailsDocument": det,
}]))
PY
# ASCII guard: the listing must be plain ASCII or the portal mangles it.
if LC_ALL=C grep -qP '[^\x09\x0a\x0d\x20-\x7e]' /tmp/cs-ferrostash-info.json; then
  echo "ERROR: non-ASCII byte in the UpdateInformation document; aborting." >&2; exit 1
fi
# github.com guard: Marketplace listing/support copy stays email-only.
if grep -qi 'github\.com' /tmp/cs-ferrostash-info.json; then
  echo "ERROR: github.com URL in the UpdateInformation document; listing/support copy must be email-only - aborting." >&2; exit 1
fi
UCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-info \
  --change-set "file:///tmp/cs-ferrostash-info.json" --query ChangeSetId --output text)
wait_done "$UCID"

# INFO_ONLY re-applies just the listing copy (e.g. to push a corrected
# description onto an already-released Limited product before AWS review)
# without re-touching regions / instance types that are already set.
if [ "${INFO_ONLY:-0}" = "1" ]; then
  echo "INFO_ONLY=1: UpdateInformation applied to $PID; skipping AddRegions/AddInstanceTypes."
  exit 0
fi

echo "3/4 AddRegions (abyo standard commercial region set)..."
RID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-regions --change-set '[{
  "ChangeType":"AddRegions",
  "Entity":{"Type":"AmiProduct@1.0","Identifier":"'"$PID"'"},
  "DetailsDocument":{"Regions":["ap-south-1","eu-north-1","eu-west-3","eu-west-2","eu-west-1","ap-northeast-3","ap-northeast-2","ap-northeast-1","ca-central-1","sa-east-1","ap-southeast-1","ap-southeast-2","eu-central-1","us-east-1","us-east-2","us-west-1","us-west-2"]}
}]' --query ChangeSetId --output text)
wait_done "$RID"

echo "4/4 AddInstanceTypes ($INSTANCE_TYPES)..."
ITJSON=$(printf '"%s",' $INSTANCE_TYPES); ITJSON="[${ITJSON%,}]"
ITID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-itypes --change-set '[{
  "ChangeType":"AddInstanceTypes",
  "Entity":{"Type":"AmiProduct@1.0","Identifier":"'"$PID"'"},
  "DetailsDocument":{"InstanceTypes":'"$ITJSON"'}
}]' --query ChangeSetId --output text)
wait_done "$ITID"

cat <<EOF

Draft AMI product populated: PID=$PID
  region: $REGION   recommended instance type: $INSTANCE_TYPE (arm64/Graviton)

This product is a DRAFT - not buyer-visible, not billable, deletable. The
remaining steps each have a hard gate and are NOT run here:

  A) Build + share the arm64 AMI, then AddDeliveryOptions (triggers AWS scan):
       PID=$PID deploy/marketplace-ami.sh
     (Builds a billable AMI via marketplace/packer/ and shares it with the
      ingestion role; the scan runs ~30-60 min async. Pass AMI_ID=ami-xxxx to
      skip the build and register an already-built, seller-owned AMI.)

  B) Set hourly + annual pricing (PROPOSED ladder; CONFIRM-gated, lower-only):
       hourly  \$$HOURLY_USD  (c7g.large class)   annual  \$$ANNUAL_USD   [PROPOSED - confirm]
       deploy/marketplace-pricing.sh   # prints the portal rate table (print-only)
     (Standard hourly AMI pricing is set in the portal / via APPLY=1; the
      ladder is a PROPOSAL the orchestrator confirms with the owner first.
      A released price can only be LOWERED later, never raised.)

  C) IRREVERSIBLE public release (after scan clean + price set + final GO):
       CONFIRM=yes PID=$PID OFFER=offer-xxxx deploy/marketplace-release.sh
EOF
