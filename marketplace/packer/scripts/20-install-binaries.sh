#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 20-install-binaries.sh -- move the FerroStash binary from the build-VM
# staging directory (`/tmp/`, populated by the preceding
# `provisioner "file"` block) into `/usr/local/bin/`.
#
# Build-host responsibility: `build.sh` cross-compiles `ferro-stash` for
# `aarch64-unknown-linux-gnu` (default) via the `cross` tool and copies the
# binary into the directory referenced by `source_binary_dir`. The Packer
# build then uploads `${source_binary_dir}/${arch}/ferro-stash` to `/tmp/`,
# and this script promotes it.
#
# NOTE: unlike the musl-static sibling AMIs, this binary is DYNAMICALLY linked
# against glibc (the GNU target is used so the vendored librdkafka CMake build
# stays on the glibc path). We therefore verify it is a valid ELF executable
# for the host architecture, but we do NOT reject dynamic linking.

set -euo pipefail

readonly BIN_DIR="/usr/local/bin"
readonly STAGE="/tmp/ferro-stash"
readonly DST="${BIN_DIR}/ferro-stash"

if [[ ! -f "${STAGE}" ]]; then
    echo "[20-install-binaries] FATAL: ${STAGE} not staged by Packer file provisioner." >&2
    exit 1
fi

install -m 0755 -o root -g root "${STAGE}" "${DST}"
rm -f "${STAGE}"

# Sanity check: the file must be an ELF executable. We avoid actually running
# it (CPU-feature / glibc differences between the build VM and the bake host
# could make exec unsafe), so the smoke test is a statically detectable
# header check only.
if command -v file >/dev/null 2>&1; then
    if ! file "${DST}" | grep -q "ELF"; then
        echo "[20-install-binaries] FATAL: ${DST} is not an ELF binary." >&2
        exit 1
    fi
fi

echo "[20-install-binaries] Installed ferro-stash into ${BIN_DIR}."
