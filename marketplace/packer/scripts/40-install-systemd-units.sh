#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 40-install-systemd-units.sh -- install + enable the systemd units that
# drive the FerroStash service and the once-per-AMI first-boot initialiser.
#
# Two units are installed:
#   * ferro-stash.service           -- the long-running pipeline daemon.
#   * ferro-stash-firstboot.service -- runs `50-firstboot.sh` exactly once
#                                      per AMI launch (gated by the
#                                      `/var/lib/ferro-stash/.firstboot-done`
#                                      sentinel) before the main service.

set -euo pipefail

readonly UNIT_DIR="/etc/systemd/system"
readonly FIRSTBOOT_SCRIPT_DST="/usr/local/sbin/ferro-stash-firstboot"

# 1. Main service unit ------------------------------------------------
if [[ ! -f /tmp/ferro-stash.service ]]; then
    echo "[40-install-systemd-units] FATAL: /tmp/ferro-stash.service not staged." >&2
    exit 1
fi
install -m 0644 -o root -g root /tmp/ferro-stash.service "${UNIT_DIR}/ferro-stash.service"
rm -f /tmp/ferro-stash.service

# 2. First-boot oneshot unit + script --------------------------------
if [[ ! -f /tmp/firstboot-systemd.service ]]; then
    echo "[40-install-systemd-units] FATAL: /tmp/firstboot-systemd.service not staged." >&2
    exit 1
fi
install -m 0644 -o root -g root /tmp/firstboot-systemd.service "${UNIT_DIR}/ferro-stash-firstboot.service"
rm -f /tmp/firstboot-systemd.service

if [[ ! -f /tmp/50-firstboot.sh ]]; then
    echo "[40-install-systemd-units] FATAL: /tmp/50-firstboot.sh not staged." >&2
    exit 1
fi
install -m 0750 -o root -g root /tmp/50-firstboot.sh "${FIRSTBOOT_SCRIPT_DST}"
rm -f /tmp/50-firstboot.sh

# 3. Enable units. We do not start them -- the AMI is shut down at the
#    end of the Packer build, so the first launch on the buyer's account
#    is what actually fires `ferro-stash-firstboot.service`.
systemctl daemon-reload
systemctl enable ferro-stash-firstboot.service
systemctl enable ferro-stash.service

echo "[40-install-systemd-units] systemd units installed + enabled."
