#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 60-harden.sh -- enforce the AWS Marketplace AMI hardening baseline.
#
# Hardening posture:
#   * SELinux:                enforcing, targeted policy.
#   * fail2ban:               sshd jail enabled, default ban = 1 hour.
#   * dnf-automatic:          security errata only, applied daily.
#   * sshd:                   PermitRootLogin no, PasswordAuthentication no,
#                              ChallengeResponseAuthentication no, KbdInteractive no.
#   * Kernel sysctls:         IP forward off, ignore broadcasts, log
#                              martians, RP filter on.
#   * journalctl:             persistent storage in /var/log/journal so
#                              audit evidence survives reboots.

set -euo pipefail

# ---------------------------------------------------------------------
# SELinux -- AL2023 ships with SELinux disabled by default. The
# Marketplace baseline is `enforcing`. We flip the persistent flag in
# /etc/selinux/config; the actual enforcement transition happens on the
# next boot so the buyer's first boot already has it on.
# ---------------------------------------------------------------------
if [[ -f /etc/selinux/config ]]; then
    sed -i 's/^SELINUX=.*/SELINUX=enforcing/' /etc/selinux/config
    sed -i 's/^SELINUXTYPE=.*/SELINUXTYPE=targeted/' /etc/selinux/config
fi

# Ensure the policy is loaded on the next boot. `setenforce 1` is not
# called in the Packer bake VM because that risks denials against the
# Packer SSH session.
if command -v fixfiles >/dev/null 2>&1; then
    # Schedule a relabel on next boot. AL2023 honours `/.autorelabel`.
    touch /.autorelabel
fi

# ---------------------------------------------------------------------
# fail2ban
# ---------------------------------------------------------------------
install -d -m 0755 /etc/fail2ban
install -d -m 0755 /etc/fail2ban/jail.d

cat >/etc/fail2ban/jail.d/00-ferro-stash.local <<'EOF'
[DEFAULT]
banaction = firewallcmd-rich-rules
backend = systemd
findtime = 10m
maxretry = 5
bantime  = 1h

[sshd]
enabled = true
port    = ssh
logpath = %(sshd_log)s
EOF

systemctl enable fail2ban

# ---------------------------------------------------------------------
# dnf-automatic -- security errata only, daily timer enabled.
# ---------------------------------------------------------------------
if [[ -f /etc/dnf/automatic.conf ]]; then
    sed -i 's/^upgrade_type = .*/upgrade_type = security/' /etc/dnf/automatic.conf
    sed -i 's/^apply_updates = .*/apply_updates = yes/'    /etc/dnf/automatic.conf
    sed -i 's/^download_updates = .*/download_updates = yes/' /etc/dnf/automatic.conf
fi
systemctl enable dnf-automatic.timer

# ---------------------------------------------------------------------
# sshd -- root login + password auth disabled.
# ---------------------------------------------------------------------
sshd_config="/etc/ssh/sshd_config"
sshd_conf_d="/etc/ssh/sshd_config.d"
install -d -m 0755 "${sshd_conf_d}"
cat >"${sshd_conf_d}/00-ferro-stash-hardening.conf" <<'EOF'
# Managed by the FerroStash Marketplace AMI. Do not edit; override via a
# higher-numbered drop-in file.
PermitRootLogin no
PasswordAuthentication no
ChallengeResponseAuthentication no
KbdInteractiveAuthentication no
PermitEmptyPasswords no
X11Forwarding no
AllowAgentForwarding no
AllowTcpForwarding no
ClientAliveInterval 300
ClientAliveCountMax 2
LoginGraceTime 30
MaxAuthTries 4
Banner none
EOF

# Backwards compatibility for AL2023 sshd builds that still parse the
# main config file directly. We patch in-place but keep a backup so an
# operator can revert during incident response.
if [[ -f "${sshd_config}" ]]; then
    cp -a "${sshd_config}" "${sshd_config}.ferro-stash-bak"
    sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/'                 "${sshd_config}"
    sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/'   "${sshd_config}"
fi

# ---------------------------------------------------------------------
# Kernel sysctls -- common CIS hardening.
# ---------------------------------------------------------------------
cat >/etc/sysctl.d/99-ferro-stash-hardening.conf <<'EOF'
net.ipv4.ip_forward = 0
net.ipv4.conf.all.send_redirects = 0
net.ipv4.conf.default.send_redirects = 0
net.ipv4.conf.all.accept_redirects = 0
net.ipv4.conf.default.accept_redirects = 0
net.ipv4.conf.all.secure_redirects = 0
net.ipv4.conf.default.secure_redirects = 0
net.ipv4.conf.all.accept_source_route = 0
net.ipv4.conf.default.accept_source_route = 0
net.ipv4.conf.all.log_martians = 1
net.ipv4.conf.default.log_martians = 1
net.ipv4.conf.all.rp_filter = 1
net.ipv4.conf.default.rp_filter = 1
net.ipv4.icmp_echo_ignore_broadcasts = 1
net.ipv4.icmp_ignore_bogus_error_responses = 1
kernel.dmesg_restrict = 1
kernel.kptr_restrict = 2
fs.protected_hardlinks = 1
fs.protected_symlinks = 1
EOF

# ---------------------------------------------------------------------
# Persistent journald.
# ---------------------------------------------------------------------
install -d -m 02755 -o root -g systemd-journal /var/log/journal || \
    install -d -m 02755 /var/log/journal
systemctl restart systemd-journald || true

echo "[60-harden] Hardening baseline applied."
