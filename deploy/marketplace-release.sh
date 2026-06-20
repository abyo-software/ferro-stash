#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# IRREVERSIBLE: release the FerroStash AMI product to the public. ReleaseProduct
# validates everything and moves the product Draft -> Limited (released); AWS then
# runs its listing review and the product goes Public. A released product CANNOT
# be un-published, and a released hourly/annual price can only be LOWERED later,
# never raised.
#
# Run ONLY after ALL of:
#   - AddDeliveryOptions done and the AWS security scan is CLEAN
#     (deploy/marketplace-ami.sh),
#   - hourly + annual pricing set + confirmed (deploy/marketplace-pricing.sh - the
#     ladder is a PROPOSAL; the owner must confirm the final numbers; permanent-
#     once-public, lower-only),
#   - support/legal/EULA fields set,
#   - a deliberate, final human GO.
#
# Requires CONFIRM=yes to actually submit.
#
# Usage: CONFIRM=yes PID=prod-xxxx OFFER=offer-xxxx deploy/marketplace-release.sh
set -euo pipefail
PROFILE="${PROFILE:-as}"; REGION="${REGION:-us-east-1}"; CATALOG=AWSMarketplace
PID="${PID:?set PID to the product id}"; PID="${PID%@*}"
OFFER="${OFFER:?set OFFER to the offer id (its pricing must be set)}"; OFFER="${OFFER%@*}"
mc() { aws --profile "$PROFILE" --region "$REGION" marketplace-catalog "$@"; }

if [ "${CONFIRM:-}" != "yes" ]; then
  echo "REFUSING: this is the IRREVERSIBLE public release of $PID + $OFFER."
  echo "Preconditions: AMI scan CLEAN, pricing set + confirmed, legal/support set, final GO."
  echo "Re-run with CONFIRM=yes once everything is verified."
  exit 2
fi

# For an AmiProduct the product and its priced offer are released TOGETHER in ONE
# change-set (ReleaseProduct + ReleaseOffer, each with an empty DetailsDocument).
cat > /tmp/cs-ferrostash-release.json <<EOF
[
 {"ChangeType":"ReleaseProduct","Entity":{"Type":"AmiProduct@1.0","Identifier":"$PID"},"DetailsDocument":{}},
 {"ChangeType":"ReleaseOffer","Entity":{"Type":"Offer@1.0","Identifier":"$OFFER"},"DetailsDocument":{}}
]
EOF
CID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-release \
  --change-set "file:///tmp/cs-ferrostash-release.json" --query ChangeSetId --output text)
echo "change-set: $CID (IRREVERSIBLE release submitted)"
while :; do
  ST=$(mc describe-change-set --catalog "$CATALOG" --change-set-id "$CID" --query Status --output text)
  case "$ST" in
    SUCCEEDED) echo "RELEASED. AWS listing review now runs; product moves to Limited then Public."; break;;
    FAILED|CANCELLED) echo "FAILED:"; mc describe-change-set --catalog "$CATALOG" --change-set-id "$CID" --query 'ChangeSet[].ErrorDetailList' --output json; exit 1;;
    *) sleep 8;;
  esac
done
