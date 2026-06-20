#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# build.sh -- end-to-end Marketplace AMI build wrapper for FerroStash.
#
# The wrapper does three things:
#
#   1. Cross-compiles the `ferro-stash` binary on the build host, producing
#      architecture-specific staging trees:
#        ./build/arm64/  -> aarch64 binary
#        ./build/x86_64/ -> x86_64  binary
#      (Packer's `source.name` labels are `arm64` / `x86_64`, hence the
#      directory names; see `ferro-stash.pkr.hcl`.)
#
#      *** TARGET CHOICE: aarch64-unknown-linux-GNU (not musl). ***
#      FerroStash's DEFAULT build pulls `rdkafka`, which vendors librdkafka
#      and builds it with CMake + a C toolchain. The GNU target keeps that
#      C build on the well-trodden glibc path; a musl-static build of the
#      vendored librdkafka is far more fragile. The trade-off is that the
#      resulting binary is DYNAMICALLY linked against glibc, so the cross
#      image's glibc must be <= Amazon Linux 2023's. We therefore build via
#      the `cross` tool with a pinned image (see Cross.toml) rather than the
#      host toolchain. See README.md "rdkafka / glibc caveat".
#
#      The default build excludes the optional `ruby` (Artichoke/mruby)
#      feature, so no extra C++/mruby toolchain is needed.
#
#   2. Invokes `packer init` + `packer build` against the resulting
#      directory (arm64 only by default for the initial product).
#   3. Offers a `--dry-run` mode that exercises only the offline lints
#      (`packer fmt --check`, `packer validate`, `shellcheck`, structure
#      checks). The dry-run requires NO AWS credentials and NO cargo.
#
# Real builds require AWS credentials with EC2 RunInstances, EBS,
# CreateImage, and CopyImage permissions, plus Docker + the `cross` tool.
# See `README.md` for the minimum IAM policy.

set -euo pipefail

# ---------------------------------------------------------------------
# Self-locate. Robust against `bash build.sh`, `./build.sh`, or
# absolute-path invocation.
# ---------------------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
readonly SCRIPT_DIR
WORKSPACE_ROOT="$(cd -- "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd -P)"
readonly WORKSPACE_ROOT

readonly BUILD_DIR="${SCRIPT_DIR}/build"
readonly TESTS_DIR="${SCRIPT_DIR}/tests"

DRY_RUN=0
SKIP_BINARIES=0
# arm64 only by default -- the initial Marketplace product is Graviton.
ARCHS=("arm64")
PACKER_VARS=()

usage() {
    cat <<'USAGE'
Usage: build.sh [options]

Options:
  --dry-run                Run offline lints only (no AWS, no cargo).
  --skip-binaries          Skip the cross build; assume ./build/<arch>/ is
                           already populated. Useful for CI splits where the
                           build runs in a different job.
  --arch <arm64|x86_64>    Restrict / set the build architecture (repeatable).
                           Default: arm64.
  --var KEY=VALUE          Forwarded to `packer build -var KEY=VALUE`
                           (repeatable).
  -h | --help              Show this help.

Environment:
  AWS_REGION               Default region for the real build (default: us-east-1).
  CARGO_BUILD_TARGET_ARM64 Override the arm64 Rust target
                           (default: aarch64-unknown-linux-gnu).
  CARGO_BUILD_TARGET_X86   Override the x86_64 Rust target
                           (default: x86_64-unknown-linux-gnu).
  USE_CROSS                "1" (default) builds via the `cross` tool + Docker;
                           "0" falls back to host `cargo` (only safe if the host
                           glibc is <= the target's, i.e. AL2023's).
USAGE
}

# ---------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------
ARCH_OVERRIDE=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)        DRY_RUN=1; shift ;;
        --skip-binaries)  SKIP_BINARIES=1; shift ;;
        --arch)           ARCH_OVERRIDE+=("$2"); shift 2 ;;
        --var)            PACKER_VARS+=("-var" "$2"); shift 2 ;;
        -h|--help)        usage; exit 0 ;;
        *)                echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ ${#ARCH_OVERRIDE[@]} -gt 0 ]]; then
    ARCHS=("${ARCH_OVERRIDE[@]}")
fi

readonly TARGET_ARM64="${CARGO_BUILD_TARGET_ARM64:-aarch64-unknown-linux-gnu}"
readonly TARGET_X86="${CARGO_BUILD_TARGET_X86:-x86_64-unknown-linux-gnu}"
readonly USE_CROSS="${USE_CROSS:-1}"

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
log() {
    printf '[build.sh] %s\n' "$*"
}

have() {
    command -v "$1" >/dev/null 2>&1
}

run_lints() {
    log "Running lint suite (structure.sh + lint.sh)..."
    bash "${TESTS_DIR}/structure.sh"
    bash "${TESTS_DIR}/lint.sh"
}

run_cargo_builds() {
    if [[ "${SKIP_BINARIES}" == "1" ]]; then
        log "--skip-binaries set; not invoking the cross build."
        return 0
    fi

    # Disable any sccache wrapper. `cross` runs cargo inside a container where
    # an inherited RUSTC_WRAPPER=sccache (host path) does not exist, which
    # silently fails the build -- the classic sccache trap. Force it off for
    # both the host and the in-container cargo.
    export RUSTC_WRAPPER=""
    export CARGO_BUILD_RUSTC_WRAPPER=""
    export SCCACHE_DISABLE=1
    # `cross` reads the project Cross.toml; point at ours so the image gets
    # cmake + a C toolchain for the vendored librdkafka.
    export CROSS_CONFIG="${SCRIPT_DIR}/Cross.toml"

    local builder
    if [[ "${USE_CROSS}" == "1" ]]; then
        if ! have cross; then
            log "cross not found on PATH. Install it (cargo install cross) or set USE_CROSS=0."
            exit 1
        fi
        if ! have docker; then
            log "docker not found on PATH. cross needs a container engine. Install Docker or set USE_CROSS=0."
            exit 1
        fi
        builder="cross"
    else
        if ! have cargo; then
            log "cargo not found on PATH; cannot build. Install rustup or pass --skip-binaries."
            exit 1
        fi
        builder="cargo"
        log "WARNING: USE_CROSS=0 -- building with host cargo. The resulting glibc-linked"
        log "         binary only runs on AL2023 if the host glibc <= AL2023's. Prefer cross."
    fi

    install -d "${BUILD_DIR}"
    for arch in "${ARCHS[@]}"; do
        local rust_target
        case "${arch}" in
            arm64)   rust_target="${TARGET_ARM64}" ;;
            x86_64)  rust_target="${TARGET_X86}" ;;
            *)       echo "Unsupported arch: ${arch}" >&2; exit 1 ;;
        esac

        log "Building ferro-stash for ${arch} (${rust_target}) via ${builder}..."
        # Build ONLY the CLI crate, release profile, DEFAULT features
        # (no `ruby`). rdkafka's vendored librdkafka builds via CMake in the
        # cross image (see Cross.toml).
        (
            cd "${WORKSPACE_ROOT}"
            "${builder}" build --release --target "${rust_target}" -p ferro-stash
        )

        local out_dir="${BUILD_DIR}/${arch}"
        install -d "${out_dir}"
        install -m 0755 \
            "${WORKSPACE_ROOT}/target/${rust_target}/release/ferro-stash" \
            "${out_dir}/ferro-stash"
        log "Staged ${arch} binary at ${out_dir}/ferro-stash"
    done
}

run_packer() {
    if ! have packer; then
        log "packer not found on PATH. Install from https://developer.hashicorp.com/packer/downloads"
        exit 1
    fi
    log "packer init..."
    packer init "${SCRIPT_DIR}"
    log "packer build (${ARCHS[*]})..."

    local only_args=()
    for arch in "${ARCHS[@]}"; do
        only_args+=("-only=ferro-stash-marketplace.amazon-ebs.${arch}")
    done

    packer build \
        "${only_args[@]}" \
        "${PACKER_VARS[@]}" \
        -var "source_binary_dir=${BUILD_DIR}" \
        "${SCRIPT_DIR}"
}

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
log "FerroStash Marketplace AMI build wrapper"
log "  workspace : ${WORKSPACE_ROOT}"
log "  packer dir: ${SCRIPT_DIR}"
log "  archs     : ${ARCHS[*]}"
log "  dry-run   : ${DRY_RUN}"

run_lints

if [[ "${DRY_RUN}" == "1" ]]; then
    log "Dry-run complete. Skipping cross + packer build."
    exit 0
fi

run_cargo_builds
run_packer

log "Build complete."
