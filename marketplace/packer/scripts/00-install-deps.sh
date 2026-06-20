#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 00-install-deps.sh -- install OS-level dependencies on the
# Amazon Linux 2023 build VM.
#
# FerroStash ships as a single prebuilt binary, so there is NO build
# toolchain here -- only the runtime daemons + hardening tooling.

set -euo pipefail

# Refresh metadata; AL2023's hourly cache TTL means a fresh build VM
# may have stale metadata from the AMI bake date.
dnf -y makecache

# Apply security errata before installing anything else.
dnf -y --security upgrade || dnf -y upgrade

# Core daemons + tooling. systemd is already present on AL2023; we
# request it explicitly so the build fails loudly if a future minimal
# AMI ever drops it.
dnf -y install \
    systemd \
    fail2ban \
    fail2ban-firewalld \
    dnf-automatic \
    awscli-2 \
    jq \
    openssl \
    ca-certificates \
    libcap \
    policycoreutils \
    policycoreutils-python-utils \
    selinux-policy \
    selinux-policy-targeted \
    chrony

# Time sync is mandatory for AWS Marketplace -- metering pulses depend
# on accurate clocks. Enable now; on first boot chronyd already has the
# Amazon Time Sync Service (169.254.169.123) configured by AL2023.
systemctl enable chronyd

# Pre-create the upload staging path so the `provisioner "file"` SFTP
# transfer never races a missing directory (the AMI-build gotcha).
install -d -m 1777 /tmp

echo "[00-install-deps] OS dependencies installed."
