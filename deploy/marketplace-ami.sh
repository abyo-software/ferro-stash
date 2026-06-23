#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Build (or reuse) a seller-owned arm64 FerroStash AMI, share it for ingestion,
# and AddDeliveryOptions on the FerroStash AMI product. This registers the BUILT
# AMI as the product version and points AWS Marketplace at the ingestion role so
# it can copy + security-scan the image. This is the BILLABLE, GATED step (it
# launches a build EC2 instance and triggers the AWS security scan, ~30-60 min
# async). For a STANDARD hourly AMI there is no usage-metering record to verify
# (AWS auto-meters instance-hours; the AMI product has NO metering code).
#
# WHAT THIS SCRIPT DOES (in order):
#   1. BUILD (unless AMI_ID is provided): invoke marketplace/packer/build.sh to
#      cross-compile the FerroStash binary and run Packer for arm64 ONLY, in
#      BUILD_REGION (default us-east-1, the catalog home region so no cross-region
#      AMI copy is needed for the initial product). The boot snapshot is built
#      UNENCRYPTED (encrypt_boot_volume=false) - Marketplace rejects an
#      encrypted-boot AMI, and an AWS-managed-CMK-encrypted snapshot cannot be
#      cross-account-shared (the two traps baked into the Packer defaults; see
#      reference_aws_marketplace_ami_build_gotchas).
#   2. SHARE: grant the Marketplace assurance account (679593333241) launch
#      permission on the AMI AND createVolume permission on every backing
#      snapshot, so the ingestion role can copy and the assurance account can
#      validate it.
#   3. AddDeliveryOptions: register the AMI + ingestion role via the Catalog API.
#
# NOTE on the build target: unlike the musl-static sibling builds, FerroStash's
# default build pulls rdkafka (vendored librdkafka, built via CMake + a C
# toolchain), so the cross-build targets aarch64-unknown-linux-gnu and the
# resulting binary is dynamically linked against AL2023's glibc. See
# marketplace/packer/build.sh + Cross.toml for the cross image (cmake/clang) and
# the glibc-version caveat.
#
# PREREQUISITES:
#   - CreateProduct + UpdateInformation + AddRegions + AddInstanceTypes done
#     (deploy/marketplace-create.sh). The recommended instance type below MUST be
#     in the product's registered InstanceTypes set.
#   - The ingestion role (trusts assets.marketplace.amazonaws.com, managed policy
#     AWSMarketplaceAmiIngestion): create it once as
#       arn:aws:iam::393886308285:role/FerroStashMarketplaceAmiIngestion
#     mirroring the sibling FerroSCA / FerroDruid / S4*MarketplaceAmiIngestion
#     roles.
#   - For a real build: AWS credentials in BUILD_REGION with EC2 RunInstances,
#     EBS, CreateImage, and CopyImage permissions (see
#     marketplace/packer/README.md), Docker + the `cross` tool with the
#     aarch64-unknown-linux-gnu target image, and packer.
#
# Usage:
#   # full path - build arm64, share, register:
#   PID=prod-xxxx deploy/marketplace-ami.sh
#   # reuse an already-built, seller-owned, UNENCRYPTED-boot arm64 AMI:
#   PID=prod-xxxx AMI_ID=ami-xxxx deploy/marketplace-ami.sh
set -euo pipefail

PROFILE="${PROFILE:-as}"
REGION="${REGION:-us-east-1}"        # Marketplace Catalog API home region
BUILD_REGION="${BUILD_REGION:-us-east-1}"  # where the AMI is built/registered
CATALOG=AWSMarketplace
PID="${PID:?set PID to the product id (prod-...)}"
AMI_ID="${AMI_ID:-}"                 # set to skip the Packer build and reuse an AMI
INGEST_ROLE_ARN="${INGEST_ROLE_ARN:-arn:aws:iam::393886308285:role/FerroStashMarketplaceAmiIngestion}"
ASSURANCE_ACCOUNT="${ASSURANCE_ACCOUNT:-679593333241}"  # Marketplace AMI assurance account
VERSION_TITLE="${VERSION_TITLE:-1.0.0}"
FERROSTASH_VERSION="${FERROSTASH_VERSION:-1.0.0}"
RECOMMENDED_INSTANCE_TYPE="${RECOMMENDED_INSTANCE_TYPE:-c7g.large}"
PID="${PID%@*}"

# Repo root (this script lives in deploy/); the Packer build script is relative.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd -P)"
PACKER_BUILD="${ROOT}/marketplace/packer/build.sh"

mc() { aws --profile "$PROFILE" --region "$REGION" marketplace-catalog "$@"; }
ec2_build() { aws --profile "$PROFILE" --region "$BUILD_REGION" ec2 "$@"; }
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

# ---------------------------------------------------------------------------
# 1. BUILD the arm64 AMI (unless an AMI_ID was supplied).
# ---------------------------------------------------------------------------
if [ -z "$AMI_ID" ]; then
  echo "1/3 BUILD arm64 AMI via $PACKER_BUILD (region $BUILD_REGION, UNENCRYPTED boot)..."
  if [ ! -x "$PACKER_BUILD" ]; then
    echo "ERROR: $PACKER_BUILD not found/executable. chmod +x it or pass AMI_ID=ami-xxxx." >&2
    exit 1
  fi
  # arm64 ONLY for the initial product; encrypt_boot_volume=false so the boot
  # snapshot is shareable and Marketplace-acceptable. build.sh runs the cross
  # build + packer and writes marketplace/packer/manifest.json.
  AWS_REGION="$BUILD_REGION" AWS_PROFILE="$PROFILE" \
    "$PACKER_BUILD" \
      --arch arm64 \
      --var "region=$BUILD_REGION" \
      --var "ferrostash_version=$FERROSTASH_VERSION" \
      --var "encrypt_boot_volume=false"
  # Recover the AMI id from the Packer manifest.
  MANIFEST="${ROOT}/marketplace/packer/manifest.json"
  if [ ! -f "$MANIFEST" ]; then
    echo "ERROR: Packer manifest $MANIFEST not written; build failed." >&2; exit 1
  fi
  # manifest "artifact_id" looks like "<region>:ami-xxxx[,<region>:ami-yyyy]";
  # pick the arm64 builder's AMI in BUILD_REGION.
  AMI_ID=$(python3 - "$MANIFEST" "$BUILD_REGION" <<'PY'
import json, sys
manifest, region = sys.argv[1], sys.argv[2]
data = json.load(open(manifest))
ami = ""
for b in data.get("builds", []):
    # prefer the arm64 builder; fall back to any build in the region.
    for pair in b.get("artifact_id", "").split(","):
        if pair.startswith(region + ":"):
            cand = pair.split(":", 1)[1]
            if "arm64" in b.get("name", "") or "arm64" in str(b.get("custom_data", {})):
                ami = cand
            elif not ami:
                ami = cand
print(ami)
PY
)
  if [ -z "$AMI_ID" ]; then
    echo "ERROR: could not parse an AMI id for $BUILD_REGION from $MANIFEST." >&2; exit 1
  fi
  echo "  built AMI: $AMI_ID"
else
  echo "1/3 BUILD - SKIPPED (reusing AMI_ID=$AMI_ID)"
fi

# ---------------------------------------------------------------------------
# 2. SHARE the AMI + its backing snapshots with the assurance account.
#    Marketplace ingestion needs launch permission on the AMI AND createVolume on
#    every backing EBS snapshot (an encrypted snapshot under an AWS-managed CMK is
#    NOT shareable - that is why the boot snapshot is built unencrypted).
# ---------------------------------------------------------------------------
echo "2/3 SHARE $AMI_ID + snapshots with assurance account $ASSURANCE_ACCOUNT..."
ec2_build modify-image-attribute --image-id "$AMI_ID" \
  --launch-permission "Add=[{UserId=$ASSURANCE_ACCOUNT}]"
SNAP_IDS=$(ec2_build describe-images --image-ids "$AMI_ID" \
  --query 'Images[0].BlockDeviceMappings[?Ebs!=`null`].Ebs.SnapshotId' --output text)
for snap in $SNAP_IDS; do
  [ -n "$snap" ] || continue
  echo "  createVolume on $snap -> $ASSURANCE_ACCOUNT"
  ec2_build modify-snapshot-attribute --snapshot-id "$snap" \
    --attribute createVolumePermission --operation-type add \
    --user-ids "$ASSURANCE_ACCOUNT"
done

# ---------------------------------------------------------------------------
# 3. AddDeliveryOptions (registers the AMI + ingestion role -> triggers AWS scan).
# ---------------------------------------------------------------------------
echo "3/3 AddDeliveryOptions (AMI $AMI_ID via $INGEST_ROLE_ARN -> triggers AWS scan)..."

# Release notes: per-version override via env, else the v1.0-line baseline.
# The baseline stays stable across versions so a missing override never lies
# about what shipped; pass `RELEASE_NOTES=...` to surface version-specific
# changes. The baseline is buyer-facing and intentionally omits the
# production-readiness hedges AWS flagged on the container submission
# (those stay in README.md per docs/marketplace/LISTING.md group B).
DEFAULT_AMI_NOTES="FerroStash v1.0 line. Rust-native, Logstash-compatible log and event pipeline. A single static binary - no JVM, no separate agent runtime - that starts in milliseconds and holds tens of MB of RAM. Parses the Logstash pipeline.conf DSL (and an equivalent YAML form) natively and implements about 88 percent of the bundled Logstash 9.x plugins (98 of 111), weighted toward the parsing and filtering hot path: inputs such as beats, file, tcp/udp, http, syslog, kafka, redis, s3, sqs, and jdbc; filters such as grok, dissect, kv, json, mutate, date, geoip, and a native Painless-style script filter; outputs such as Elasticsearch/OpenSearch, kafka, s3, http, and file. Reliability: an optional on-disk persistent queue with at-least-once delivery and a dead-letter queue, plus opt-in fsync durability; a built-in monitoring API. Compatibility: Logstash config / pipeline compatible, not a byte-identical 100 percent drop-in; coverage is plugin-level and a covered plugin may implement a subset of its options; a config using a missing plugin fails fast at load. The supported Marketplace topology is single-node; the optional ruby filter is excluded from the default build. Engineering posture: unsafe_code denied workspace-wide (narrow audited exceptions), clippy clean at -D warnings, unwrap() denied in production code, cargo deny gate, output verified against Logstash 9.4.2 expected fields across 24 parity fixtures. Metered automatically by AWS per instance-hour (no metering code in the AMI)."
RELEASE_NOTES="${RELEASE_NOTES:-$DEFAULT_AMI_NOTES}"

python3 - "$PID" "$AMI_ID" "$INGEST_ROLE_ARN" "$VERSION_TITLE" "$RECOMMENDED_INSTANCE_TYPE" "$RELEASE_NOTES" \
  > /tmp/cs-ferrostash-ami.json <<'PY'
import json, sys
pid, ami, role, vtitle, rec, release_notes = sys.argv[1:7]
details = {
  "Version": {
    "VersionTitle": vtitle,
    "ReleaseNotes": release_notes,
  },
  "DeliveryOptions": [{
    "Details": {"AmiDeliveryOptionDetails": {
      "AmiSource": {
        "AmiId": ami,
        "AccessRoleArn": role,
        "UserName": "ec2-user",
        "OperatingSystemName": "AMAZONLINUX",
        "OperatingSystemVersion": "2023",
        "ScanningPort": 22,
      },
      "UsageInstructions": ("Launch the self-contained FerroStash AMI on EC2 "
        "(Graviton/arm64). FerroStash starts as a systemd service (ferro-stash) "
        "running a single pipeline from /etc/ferro-stash/pipeline.conf with its "
        "data directory under /var/lib/ferro-stash. The shipped default pipeline "
        "accepts Elastic Beats input on TCP 5044 and writes events to a local "
        "file; edit /etc/ferro-stash/pipeline.conf to point at your real inputs "
        "and outputs (for example Elasticsearch/OpenSearch, Kafka, or S3) and "
        "restart the service. Send Beats / TCP traffic only from inside your VPC "
        "via the security group; do not expose pipeline input ports to the public "
        "internet. The built-in monitoring API binds to localhost:9600 only; "
        "reach it over SSH or a private tunnel. There is no admin login and no "
        "baked-in secret: the service runs unprivileged and reads its config from "
        "disk. A first-boot note with the instance id and quick-start guidance is "
        "written to /var/lib/ferro-stash/initial-info.txt. The supported topology "
        "is single-node."),
      "RecommendedInstanceType": rec,
      # The shipped default pipeline listens for Elastic Beats on TCP 5044;
      # restrict it to the private VPC range. The monitoring API binds to
      # localhost only and is intentionally NOT opened here.
      "SecurityGroups": [{"FromPort": 5044, "ToPort": 5044, "IpProtocol": "tcp",
                          "IpRanges": ["10.0.0.0/8"]}],
    }},
  }],
}
print(json.dumps([{
  "ChangeType": "AddDeliveryOptions",
  "Entity": {"Type": "AmiProduct@1.0", "Identifier": pid},
  "DetailsDocument": details,
}]))
PY
if LC_ALL=C grep -qP '[^\x09\x0a\x0d\x20-\x7e]' /tmp/cs-ferrostash-ami.json; then
  echo "ERROR: non-ASCII byte in the AddDeliveryOptions document; aborting." >&2; exit 1
fi
if grep -qi 'github\.com' /tmp/cs-ferrostash-ami.json; then
  echo "ERROR: github.com URL in the AddDeliveryOptions document; listing/support copy must be email-only - aborting." >&2; exit 1
fi
DID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-ami \
  --change-set "file:///tmp/cs-ferrostash-ami.json" --query ChangeSetId --output text)
wait_done "$DID"
echo "Submitted. AWS security scan runs ~30-60 min (check the Management Portal or"
echo "list change sets). Then set pricing (deploy/marketplace-pricing.sh) and run"
echo "deploy/marketplace-release.sh (CONFIRM=yes, IRREVERSIBLE)."
