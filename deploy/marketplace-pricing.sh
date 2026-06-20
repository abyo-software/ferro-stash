#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# FerroStash AMI product pricing (hourly + annual, per instance type).
#
# Standard hourly+annual AMI pricing is set per instance type. This script:
#   1. ALWAYS prints the exact per-instance-type rate table (paste into the AWS
#      Marketplace Management Portal AMI pricing page - the reliable path that
#      the sibling FerroSCA / FerroDruid / S4 hourly AMI products use).
#   2. With APPLY=1, creates an Offer and attempts to set the hourly + annual rate
#      card via the Catalog API (best-effort; some hourly-AMI pricing models
#      require the portal / Seller Operations - if the change-set is rejected,
#      fall back to the printed table).
#
# *** THE LADDER BELOW IS A PROPOSAL, NOT A COMMITTED PRICE. ***
# FerroStash is a lightweight JVM-free single static binary (starts in ms, holds
# tens of MB), so it is anchored at the same conservative point as the sibling
# FerroSCA hourly AMI. The ladder is roughly proportional to vCPU, anchored at
# c7g.large (2 vCPU) at $0.06/hr / $370/yr. The orchestrator MUST confirm these
# numbers with the owner before running with APPLY=1 or setting them in the
# portal. Once the product is Public a released price can only be LOWERED, never
# raised - so a conservative anchor is deliberate.
#
# Usage:
#   deploy/marketplace-pricing.sh                 # print the portal rate table only
#   APPLY=1 PID=prod-xxxx deploy/marketplace-pricing.sh   # also create offer + try API
set -euo pipefail

PROFILE="${PROFILE:-as}"
REGION="${REGION:-us-east-1}"
CATALOG=AWSMarketplace
PID="${PID:-}"; PID="${PID%@*}"
OFFER="${OFFER:-}"
CURRENCY="${CURRENCY:-USD}"

# instance_type  vcpu  hourly  annual   (PROPOSED - confirm before applying)
# Anchor: c7g.large (2 vCPU) = $0.06/hr, $370/yr. Roughly vCPU-proportional;
# t4g.small (burstable, low-volume shippers) is shaded a notch below the 2-vCPU band.
LADDER=(
  "c7g.medium   1  0.03  185"
  "t4g.small    2  0.04  240"
  "t4g.medium   2  0.05  300"
  "t4g.large    2  0.06  370"
  "c7g.large    2  0.06  370"
  "m7g.large    2  0.06  370"
  "r7g.large    2  0.06  370"
  "c7g.xlarge   4  0.12  740"
  "m7g.xlarge   4  0.12  740"
  "c7g.2xlarge  8  0.24 1480"
)

echo "================================================================"
echo "FerroStash AMI per-instance-type pricing (PROPOSED - paste into portal)"
echo "Anchor: c7g.large \$0.06/hr, \$370/yr. Once Public: lower-only."
echo "*** PROPOSAL - the orchestrator confirms with the owner first. ***"
echo "================================================================"
printf "%-13s %5s %10s %10s\n" "InstanceType" "vCPU" "Hourly\$" "Annual\$"
for row in "${LADDER[@]}"; do
  set -- $row
  printf "%-13s %5s %10s %10s\n" "$1" "$2" "$3" "$4"
done
echo "================================================================"

if [ "${APPLY:-0}" != "1" ]; then
  echo "(print-only. Re-run with APPLY=1 PID=prod-xxxx to create the offer + attempt the API.)"
  exit 0
fi

if [ -z "$PID" ]; then
  echo "ERROR: APPLY=1 requires PID=prod-xxxx." >&2; exit 2
fi

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

# AddDimensions FIRST: standard hourly+annual AMI pricing references one pricing
# DIMENSION per instance type (Unit=Hrs, Types=["Metered"]). AddInstanceTypes does
# NOT create these - without them UpdatePricingTerms fails INCOMPATIBLE_PRODUCT
# ("Use existing, available dimensions"). The AddDimensions DetailsDocument is a
# BARE ARRAY of dimension objects (NOT wrapped in {"Dimensions":...}). Run after
# the AMI delivery option is applied. Idempotent-safe to re-run (re-adding the
# same dimensions is accepted/no-op).
echo "AddDimensions (per-instance-type Metered/Hrs) ..."
python3 - "$PID" > /tmp/cs-ferrostash-dims.json <<'PY'
import json, sys
pid = sys.argv[1]
types = ["c7g.medium","t4g.small","t4g.medium","t4g.large","c7g.large","m7g.large",
         "r7g.large","c7g.xlarge","m7g.xlarge","c7g.2xlarge"]
dims = [{"Name": t, "Description": t, "Key": t, "Unit": "Hrs", "Types": ["Metered"]} for t in types]
print(json.dumps([{"ChangeType": "AddDimensions",
                   "Entity": {"Type": "AmiProduct@1.0", "Identifier": pid},
                   "DetailsDocument": dims}]))
PY
if DCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-dims \
    --change-set "file:///tmp/cs-ferrostash-dims.json" --query ChangeSetId --output text 2>/dev/null); then
  wait_done "$DCID" || echo "  (AddDimensions failed/duplicate - continuing; dims may already exist)"
fi

if [ -z "$OFFER" ]; then
  echo "CreateOffer for product $PID..."
  OCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-offer \
    --change-set '[{"ChangeType":"CreateOffer","Entity":{"Type":"Offer@1.0"},"DetailsDocument":{"ProductId":"'"$PID"'"}}]' \
    --query ChangeSetId --output text)
  wait_done "$OCID"
  OFFER=$(mc describe-change-set --catalog "$CATALOG" --change-set-id "$OCID" --query 'ChangeSet[0].Entity.Identifier' --output text)
  OFFER="${OFFER%@*}"
  echo "  offer id: $OFFER"
fi

# Build the hourly + annual rate cards (DimensionKey = instance type) and attempt
# UpdatePricingTerms. NOTE: the exact term shape for standard hourly-AMI pricing
# can require the portal; this is best-effort and prints the error if rejected.
python3 - "$PID" "$OFFER" "$CURRENCY" > /tmp/cs-ferrostash-pricing.json <<'PY'
import json, sys
pid, offer, ccy = sys.argv[1:4]
ladder = [
  ("c7g.medium","0.03","185"),("t4g.small","0.04","240"),("t4g.medium","0.05","300"),
  ("t4g.large","0.06","370"),("c7g.large","0.06","370"),("m7g.large","0.06","370"),
  ("r7g.large","0.06","370"),("c7g.xlarge","0.12","740"),("m7g.xlarge","0.12","740"),
  ("c7g.2xlarge","0.24","1480"),
]
hourly = [{"DimensionKey": it, "Price": hr} for (it, hr, _an) in ladder]
annual = [{"DimensionKey": it, "Price": an} for (it, _hr, an) in ladder]
print(json.dumps([{
  "ChangeType": "UpdatePricingTerms",
  "Entity": {"Type": "Offer@1.0", "Identifier": offer},
  "DetailsDocument": {
    "PricingModel": "Usage",
    "Terms": [
      {"Type": "UsageBasedPricingTerm", "CurrencyCode": ccy,
       "RateCards": [{"RateCard": hourly}]},
      {"Type": "ConfigurableUpfrontPricingTerm", "CurrencyCode": ccy,
       "RateCards": [{"Selector": {"Type": "Duration", "Value": "P365D"},
                      "Constraints": {"MultipleDimensionSelection": "Allowed",
                                      "QuantityConfiguration": "Allowed"},
                      "RateCard": annual}]},
    ],
  },
}]))
PY
echo "Attempting UpdatePricingTerms (best-effort)..."
if PCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-pricing \
    --change-set "file:///tmp/cs-ferrostash-pricing.json" --query ChangeSetId --output text 2>/tmp/pricing-err.txt); then
  if wait_done "$PCID"; then
    echo "PRICING SET via Catalog API (offer $OFFER)."
  else
    echo "Catalog API pricing rejected - use the portal rate table above. Offer $OFFER exists."
  fi
else
  echo "start-change-set failed: $(cat /tmp/pricing-err.txt)"
  echo "Use the portal rate table above. (Offer $OFFER may or may not exist.)"
fi

# ---------------------------------------------------------------------------
# Offer legal / support / identity. ReleaseOffer FAILS without ALL of these:
#   - a StandardEula legal term, - a SupportTerm RefundPolicy (<=500 chars),
#   - an offer Name + Description (Description <=255 chars).
# ---------------------------------------------------------------------------
echo "UpdateLegalTerms (StandardEula) ..."
LCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-offer-legal --change-set '[{
  "ChangeType":"UpdateLegalTerms","Entity":{"Type":"Offer@1.0","Identifier":"'"$OFFER"'"},
  "DetailsDocument":{"Terms":[{"Type":"LegalTerm","Documents":[{"Type":"StandardEula","Version":"2022-07-14"}]}]}
}]' --query ChangeSetId --output text)
wait_done "$LCID" || echo "  (legal terms change-set issue - check)"

echo "UpdateSupportTerms (RefundPolicy) ..."
python3 - "$OFFER" > /tmp/cs-ferrostash-support.json <<'PY'
import json, sys
offer = sys.argv[1]
refund = ("Contact aws-support@abyo.net within 30 days of a charge to request a refund for a "
  "billing error or a documented defect in the supported distribution. Refunds are processed "
  "in accordance with the AWS Marketplace Standard Contract.")
assert len(refund) <= 500, len(refund)
print(json.dumps([{"ChangeType":"UpdateSupportTerms","Entity":{"Type":"Offer@1.0","Identifier":offer},
  "DetailsDocument":{"Terms":[{"Type":"SupportTerm","RefundPolicy":refund}]}}]))
PY
if LC_ALL=C grep -qP '[^\x09\x0a\x0d\x20-\x7e]' /tmp/cs-ferrostash-support.json; then
  echo "ERROR: non-ASCII in support terms; aborting." >&2; exit 1; fi
SCID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-offer-support \
  --change-set "file:///tmp/cs-ferrostash-support.json" --query ChangeSetId --output text)
wait_done "$SCID" || echo "  (support terms change-set issue - check)"

echo "UpdateInformation on the offer (Name + Description) ..."
python3 - "$OFFER" > /tmp/cs-ferrostash-offer-info.json <<'PY'
import json, sys
offer = sys.argv[1]
name = "FerroStash - hourly and annual (Graviton / arm64)"
desc = ("Hourly and annual pricing for the FerroStash AMI on Graviton / arm64. Metered "
  "automatically by AWS per running instance-hour; annual subscriptions per the "
  "per-instance-type rate card. Apache-2.0 open-core; hardened, scanned distribution.")
assert len(desc) <= 255, len(desc)
print(json.dumps([{"ChangeType":"UpdateInformation","Entity":{"Type":"Offer@1.0","Identifier":offer},
  "DetailsDocument":{"Name":name,"Description":desc}}]))
PY
if LC_ALL=C grep -qP '[^\x09\x0a\x0d\x20-\x7e]' /tmp/cs-ferrostash-offer-info.json; then
  echo "ERROR: non-ASCII in offer info; aborting." >&2; exit 1; fi
ICID=$(mc start-change-set --catalog "$CATALOG" --change-set-name ferrostash-offer-info \
  --change-set "file:///tmp/cs-ferrostash-offer-info.json" --query ChangeSetId --output text)
wait_done "$ICID" || echo "  (offer info change-set issue - check)"

echo "================================================================"
echo "Offer $OFFER: dimensions + pricing + legal + support + identity set."
echo "Next (IRREVERSIBLE, after a final GO):"
echo "  CONFIRM=yes PID=$PID OFFER=$OFFER deploy/marketplace-release.sh"
echo "================================================================"
