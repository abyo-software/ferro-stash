#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Install a pinned Logstash tarball for the benchmark (no Docker — native, so
# `/usr/bin/time -v` measures the JVM process RSS directly). Prints the LS_HOME
# to export. Tested on Amazon Linux 2023 / Debian (needs curl, tar, a JDK is
# bundled with the Logstash tarball).
#
# Usage:  source <(./bench/setup_logstash.sh 9.4.2)   # or just run + export
set -euo pipefail

VERSION="${1:-9.4.2}"
DEST="${LS_DEST:-/opt}"
HOME_DIR="$DEST/logstash-$VERSION"

if [ ! -d "$HOME_DIR" ]; then
  ARCH="$(uname -m)"; case "$ARCH" in x86_64) LSARCH=x86_64;; aarch64|arm64) LSARCH=aarch64;; *) echo "unsupported arch $ARCH"; exit 1;; esac
  TARBALL="logstash-$VERSION-linux-$LSARCH.tar.gz"
  URL="https://artifacts.elastic.co/downloads/logstash/$TARBALL"
  echo "downloading $URL ..." >&2
  mkdir -p "$DEST"
  curl -fsSL "$URL" -o "/tmp/$TARBALL"
  tar -xzf "/tmp/$TARBALL" -C "$DEST"
  rm -f "/tmp/$TARBALL"
fi

echo "export LS_HOME=$HOME_DIR"
