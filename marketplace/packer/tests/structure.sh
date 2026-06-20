#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# structure.sh -- assert the Packer toolkit has every file the Marketplace
# AMI build flow depends on, with the right shebang / SPDX / magic strings.
#
# Layered companion to `lint.sh`: `lint.sh` checks *content*
# (formatting, static analysis, validate) while `structure.sh` checks
# *shape* (presence, shebang, SPDX, marketplace requirements).

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
readonly SCRIPT_DIR
PACKER_DIR="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
readonly PACKER_DIR

CHECKS_OK=0
CHECKS_FAILED=0
ok()   { CHECKS_OK=$((CHECKS_OK + 1));         printf '  [ok]   %s\n' "$1"; }
fail() { CHECKS_FAILED=$((CHECKS_FAILED + 1)); printf '  [FAIL] %s\n' "$1"; }

# ---------------------------------------------------------------------
# Required files
# ---------------------------------------------------------------------
REQUIRED_FILES=(
    "ferro-stash.pkr.hcl"
    "variables.pkr.hcl"
    "build.sh"
    "Cross.toml"
    "README.md"
    "scripts/00-install-deps.sh"
    "scripts/10-create-ferrostash-user.sh"
    "scripts/20-install-binaries.sh"
    "scripts/30-install-config.sh"
    "scripts/40-install-systemd-units.sh"
    "scripts/50-firstboot.sh"
    "scripts/60-harden.sh"
    "scripts/90-marketplace-finalise.sh"
    "files/ferro-stash.service"
    "files/pipeline.conf"
    "files/firstboot-systemd.service"
    "tests/lint.sh"
    "tests/structure.sh"
)

for relpath in "${REQUIRED_FILES[@]}"; do
    if [[ -f "${PACKER_DIR}/${relpath}" ]]; then
        ok "exists: ${relpath}"
    else
        fail "missing: ${relpath}"
    fi
done

# ---------------------------------------------------------------------
# Shebang / SPDX checks for every shell script.
# ---------------------------------------------------------------------
mapfile -t SH_FILES < <(find "${PACKER_DIR}" -type f -name '*.sh' | sort)
for sh in "${SH_FILES[@]}"; do
    rel="${sh#"${PACKER_DIR}"/}"
    first_line=$(head -n 1 "${sh}")
    if [[ "${first_line}" == "#!/usr/bin/env bash" || "${first_line}" == "#!/bin/bash" ]]; then
        ok "shebang ${rel}"
    else
        fail "shebang ${rel} (got: ${first_line})"
    fi
    if grep -q 'SPDX-License-Identifier: Apache-2.0' "${sh}"; then
        ok "spdx ${rel}"
    else
        fail "spdx ${rel}"
    fi
done

# ---------------------------------------------------------------------
# SPDX header on the HCL / unit / template / config files.
# ---------------------------------------------------------------------
for relpath in \
    "ferro-stash.pkr.hcl" \
    "variables.pkr.hcl" \
    "Cross.toml" \
    "files/ferro-stash.service" \
    "files/firstboot-systemd.service" \
    "files/pipeline.conf"; do
    if grep -q 'SPDX-License-Identifier: Apache-2.0' "${PACKER_DIR}/${relpath}"; then
        ok "spdx ${relpath}"
    else
        fail "spdx ${relpath}"
    fi
done

# ---------------------------------------------------------------------
# Marketplace placeholder hygiene -- the seller product code MUST remain
# literal in `variables.pkr.hcl` (operators override via -var) but no
# other file is permitted to hard-code a fake.
# ---------------------------------------------------------------------
PLACEHOLDER="REPLACE-WITH-SELLER-PRODUCT-CODE"
if grep -q "${PLACEHOLDER}" "${PACKER_DIR}/variables.pkr.hcl"; then
    ok "placeholder retained: variables.pkr.hcl"
else
    fail "placeholder removed: variables.pkr.hcl"
fi

DISALLOWED_FILES=(
    "ferro-stash.pkr.hcl"
    "files/ferro-stash.service"
    "files/firstboot-systemd.service"
    "files/pipeline.conf"
)
for relpath in "${DISALLOWED_FILES[@]}"; do
    if grep -q "${PLACEHOLDER}" "${PACKER_DIR}/${relpath}"; then
        fail "${relpath} bakes the placeholder seller product code"
    else
        ok "${relpath} free of placeholder"
    fi
done

# ---------------------------------------------------------------------
# No github.com URL in the AMI-shipped files -- the source repo is
# private, so a github link is dead weight (and a listing-reject smell).
# ---------------------------------------------------------------------
for relpath in \
    "files/ferro-stash.service" \
    "files/firstboot-systemd.service" \
    "files/pipeline.conf"; do
    if grep -qi 'github\.com' "${PACKER_DIR}/${relpath}"; then
        fail "${relpath} contains a github.com URL (repo is private)"
    else
        ok "${relpath} free of github.com URL"
    fi
done

# ---------------------------------------------------------------------
# Sanity: no plaintext secrets accidentally checked in.
# ---------------------------------------------------------------------
SECRET_PATTERNS=(
    "AKIA[0-9A-Z]{16}"
    "-----BEGIN [A-Z ]*PRIVATE KEY-----"
)
for pat in "${SECRET_PATTERNS[@]}"; do
    if grep -rE "${pat}" "${PACKER_DIR}" --include='*.sh' --include='*.hcl' --include='*.toml' --include='*.conf' --include='*.service' --include='*.md' >/dev/null 2>&1; then
        fail "secret pattern '${pat}' found"
    else
        ok "no secrets matching '${pat}'"
    fi
done

# ---------------------------------------------------------------------
# Mandatory Marketplace requirements expressed in scripts/files.
# ---------------------------------------------------------------------
declare -A MARKETPLACE_REQUIREMENTS=(
    ["scripts/60-harden.sh"]="PermitRootLogin no"
    ["scripts/60-harden.sh:passwordauth"]="PasswordAuthentication no"
    ["scripts/60-harden.sh:selinux"]="SELINUX=enforcing"
    ["scripts/60-harden.sh:fail2ban"]="fail2ban"
    ["scripts/60-harden.sh:dnfauto"]="dnf-automatic"
    ["scripts/90-marketplace-finalise.sh"]="/etc/aws-marketplace/productcode"
    ["scripts/90-marketplace-finalise.sh:authorizedkeys"]="authorized_keys"
    ["scripts/90-marketplace-finalise.sh:hostkeys"]="ssh_host_"
    ["scripts/50-firstboot.sh"]="initial-info.txt"
    ["scripts/50-firstboot.sh:imdsv2"]="X-aws-ec2-metadata-token-ttl-seconds"
    ["files/ferro-stash.service"]="NoNewPrivileges=true"
    ["files/ferro-stash.service:protectsystem"]="ProtectSystem=strict"
    ["files/ferro-stash.service:protecthome"]="ProtectHome=true"
    ["files/ferro-stash.service:privatetmp"]="PrivateTmp=true"
    ["files/ferro-stash.service:typesimple"]="Type=simple"
    ["files/ferro-stash.service:restart"]="Restart=on-failure"
    ["files/ferro-stash.service:user"]="User=ferrostash"
    ["files/pipeline.conf"]="input {"
    ["files/pipeline.conf:output"]="output {"
)

# Note: bash assoc-array iteration order is unspecified; sort the keys so the
# test summary is stable.
mapfile -t REQ_KEYS < <(printf '%s\n' "${!MARKETPLACE_REQUIREMENTS[@]}" | sort)
for key in "${REQ_KEYS[@]}"; do
    file="${key%%:*}"
    needle="${MARKETPLACE_REQUIREMENTS[$key]}"
    if grep -qF "${needle}" "${PACKER_DIR}/${file}"; then
        ok "marketplace req: ${file} contains '${needle}'"
    else
        fail "marketplace req: ${file} missing '${needle}'"
    fi
done

# ---------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------
printf '\n[structure.sh] summary: ok=%d failed=%d\n' \
    "${CHECKS_OK}" "${CHECKS_FAILED}"

if [[ "${CHECKS_FAILED}" -gt 0 ]]; then
    exit 1
fi
exit 0
