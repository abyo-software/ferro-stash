#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 10-create-ferrostash-user.sh -- create the unprivileged `ferrostash`
# system user that owns the runtime data dir.
#
# The user has no login shell (`/sbin/nologin`) and no home directory
# beyond `/var/lib/ferro-stash`. systemd's `DynamicUser=` is *not* used
# because the persistent queue / instance uuid / output dir must persist
# across restarts.

set -euo pipefail

readonly FS_USER="ferrostash"
readonly FS_GROUP="ferrostash"
readonly FS_HOME="/var/lib/ferro-stash"
readonly FS_DATA="/var/lib/ferro-stash/data"
readonly FS_OUTPUT="/var/lib/ferro-stash/output"
readonly FS_CONFIG_DIR="/etc/ferro-stash"
readonly FS_LOG_DIR="/var/log/ferro-stash"

if ! getent group "${FS_GROUP}" >/dev/null; then
    groupadd --system "${FS_GROUP}"
fi

if ! id -u "${FS_USER}" >/dev/null 2>&1; then
    useradd \
        --system \
        --gid "${FS_GROUP}" \
        --home-dir "${FS_HOME}" \
        --no-create-home \
        --shell /sbin/nologin \
        --comment "FerroStash service account" \
        "${FS_USER}"
fi

install -d -m 0750 -o "${FS_USER}" -g "${FS_GROUP}" "${FS_HOME}"
install -d -m 0750 -o "${FS_USER}" -g "${FS_GROUP}" "${FS_DATA}"
install -d -m 0750 -o "${FS_USER}" -g "${FS_GROUP}" "${FS_OUTPUT}"
install -d -m 0755 -o root         -g root          "${FS_CONFIG_DIR}"
install -d -m 0750 -o "${FS_USER}" -g "${FS_GROUP}" "${FS_LOG_DIR}"

echo "[10-create-ferrostash-user] User '${FS_USER}' ready (uid=$(id -u "${FS_USER}"))."
