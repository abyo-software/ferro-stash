#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 50-firstboot.sh -- first-boot initialiser for the FerroStash AMI.
#
# Installed at `/usr/local/sbin/ferro-stash-firstboot` by
# `40-install-systemd-units.sh` and fired by `ferro-stash-firstboot.service`
# BEFORE `ferro-stash.service`.
#
# FerroStash has NO auth / admin user / session secret / API key, so there is
# nothing to generate or stamp (a key Marketplace win: there is no default
# credential to leak). The first boot therefore only:
#
#   1. Pulls the EC2 instance id + region via IMDSv2 (token-protected; IMDSv1
#      is intentionally not used).
#   2. Ensures the data + output dirs exist and are owned by `ferrostash`.
#   3. Writes a quick-start note to /var/lib/ferro-stash/initial-info.txt
#      (mode 0644) so the buyer knows the config path, the monitoring API
#      address, and how to point the pipeline at their real inputs/outputs.
#   4. Drops `/var/lib/ferro-stash/.firstboot-done` so subsequent reboots are
#      no-ops.

set -euo pipefail

readonly DATA_DIR="/var/lib/ferro-stash"
readonly OUTPUT_DIR="/var/lib/ferro-stash/output"
readonly INFO_FILE="${DATA_DIR}/initial-info.txt"
readonly SENTINEL="${DATA_DIR}/.firstboot-done"
readonly LOG_TAG="ferro-stash-firstboot"

log() {
    logger -t "${LOG_TAG}" -- "$*" 2>/dev/null || true
    echo "[${LOG_TAG}] $*"
}

if [[ -f "${SENTINEL}" ]]; then
    log "Sentinel ${SENTINEL} present; first-boot already completed. Exiting."
    exit 0
fi

umask 022

# ---------------------------------------------------------------------
# 1. Pull EC2 metadata via IMDSv2.
# ---------------------------------------------------------------------
imds_token=""
if command -v curl >/dev/null 2>&1; then
    if ! imds_token=$(curl --silent --show-error --max-time 3 \
            -X PUT "http://169.254.169.254/latest/api/token" \
            -H "X-aws-ec2-metadata-token-ttl-seconds: 60"); then
        log "WARNING: IMDSv2 token request failed; running with placeholder metadata."
        imds_token=""
    fi
fi

imds_get() {
    local path="$1"
    if [[ -z "${imds_token}" ]]; then
        echo "unknown"
        return 0
    fi
    curl --silent --show-error --max-time 3 \
        -H "X-aws-ec2-metadata-token: ${imds_token}" \
        "http://169.254.169.254/latest/meta-data/${path}" \
        || echo "unknown"
}

instance_id=$(imds_get "instance-id")
region_az=$(imds_get "placement/availability-zone")
local_ipv4=$(imds_get "local-ipv4")

# ---------------------------------------------------------------------
# 2. Ensure runtime dirs exist (idempotent; the bake also created them).
# ---------------------------------------------------------------------
install -d -m 0750 -o ferrostash -g ferrostash "${DATA_DIR}"
install -d -m 0750 -o ferrostash -g ferrostash "${DATA_DIR}/data"
install -d -m 0750 -o ferrostash -g ferrostash "${OUTPUT_DIR}"

# ---------------------------------------------------------------------
# 3. Write the quick-start note.
# ---------------------------------------------------------------------
cat >"${INFO_FILE}" <<EOF
# FerroStash -- instance info / quick start (generated $(date --utc --iso-8601=seconds))
# Instance: ${instance_id} (${region_az})
# Private IP: ${local_ipv4}

FerroStash runs as the systemd service 'ferro-stash' (unprivileged user
'ferrostash'). There is no admin login and no baked-in secret.

  Pipeline config : /etc/ferro-stash/pipeline.conf
  Data directory  : /var/lib/ferro-stash/data   (uuid, lock, persistent queue, DLQ)
  Output (default): /var/lib/ferro-stash/output  (the shipped pipeline writes here)
  Logs            : /var/log/ferro-stash  +  journalctl -u ferro-stash
  Monitoring API  : http://127.0.0.1:9600  (localhost only; use SSH/tunnel)

The shipped default pipeline accepts Elastic Beats on TCP 5044 and writes
events to a local file. To use it for real:

  1. sudo \$EDITOR /etc/ferro-stash/pipeline.conf
     - point the output at Elasticsearch/OpenSearch, Kafka, S3, etc.
     - add filters (grok, dissect, kv, json, mutate, date, geoip, ...).
  2. sudo systemctl restart ferro-stash
  3. sudo systemctl status ferro-stash   # confirm it is active

Open only the pipeline input ports you need, and only to your VPC, via the
instance security group. Do not expose input ports to the public internet.

Support: aws-support@abyo.net  (include the instance id above).
EOF

chown ferrostash:ferrostash "${INFO_FILE}"
chmod 0644 "${INFO_FILE}"

# ---------------------------------------------------------------------
# 4. Sentinel last so the file is fully populated before we declare the
#    stage successful.
# ---------------------------------------------------------------------
install -m 0644 -o root -g root /dev/null "${SENTINEL}"

log "First-boot complete. Quick-start note at ${INFO_FILE}."
