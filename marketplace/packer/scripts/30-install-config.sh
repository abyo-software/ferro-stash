#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 30-install-config.sh -- install the default FerroStash pipeline into
# `/etc/ferro-stash/pipeline.conf`.
#
# Marketplace-compliance posture: FerroStash has no auth / admin / session
# secret, so there is nothing to stamp at first boot. The shipped pipeline is
# a safe self-contained default (Beats in -> local file out). We still guard
# against an operator accidentally baking a real credential (AWS access key,
# private key) into the pipeline before the Packer run.

set -euo pipefail

readonly CONFIG_DIR="/etc/ferro-stash"
readonly CONFIG_FILE="${CONFIG_DIR}/pipeline.conf"
readonly STAGED="/tmp/pipeline.conf"

if [[ ! -f "${STAGED}" ]]; then
    echo "[30-install-config] FATAL: ${STAGED} not staged by Packer file provisioner." >&2
    exit 1
fi

install -d -m 0755 -o root -g root "${CONFIG_DIR}"
install -m 0644 -o root -g ferrostash "${STAGED}" "${CONFIG_FILE}"
rm -f "${STAGED}"

# Refuse to ship if we accidentally baked in a REAL secret.
if grep -Eq 'AKIA[0-9A-Z]{16}' "${CONFIG_FILE}" \
   || grep -Eq -- '-----BEGIN [A-Z ]*PRIVATE KEY-----' "${CONFIG_FILE}"; then
    echo "[30-install-config] FATAL: pipeline.conf contains what looks like a real credential. Marketplace policy forbids baked-in secrets." >&2
    exit 1
fi

echo "[30-install-config] Default pipeline installed at ${CONFIG_FILE}."
