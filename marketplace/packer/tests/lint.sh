#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# lint.sh -- offline lint pass for the Packer toolkit.
#
# Tools honoured when present (silently skipped otherwise so the gate is
# portable across CI runners that don't preinstall every tool):
#
#   * packer       -- `packer fmt --check`, `packer validate -syntax-only`
#   * shellcheck   -- shell-script static analysis
#   * bash -n      -- always available; syntax-only parse fallback
#
# Exit non-zero on any failure. Prints a summary footer.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
readonly SCRIPT_DIR
PACKER_DIR="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
readonly PACKER_DIR

CHECKS_OK=0
CHECKS_SKIPPED=0
CHECKS_FAILED=0

ok()   { CHECKS_OK=$((CHECKS_OK + 1));           printf '  [ok]   %s\n' "$1"; }
skip() { CHECKS_SKIPPED=$((CHECKS_SKIPPED + 1)); printf '  [skip] %s (%s)\n' "$1" "${2:-tool missing}"; }
fail() { CHECKS_FAILED=$((CHECKS_FAILED + 1));   printf '  [FAIL] %s\n' "$1"; }

have() { command -v "$1" >/dev/null 2>&1; }

echo "[lint.sh] Linting Packer toolkit at ${PACKER_DIR}"

# ---------------------------------------------------------------------
# packer fmt --check
# ---------------------------------------------------------------------
if have packer; then
    if packer fmt -check -recursive "${PACKER_DIR}" >/dev/null; then
        ok "packer fmt --check"
    else
        fail "packer fmt --check (run 'packer fmt -recursive ${PACKER_DIR}')"
    fi
else
    skip "packer fmt --check"
fi

# ---------------------------------------------------------------------
# packer validate -syntax-only
# ---------------------------------------------------------------------
if have packer; then
    if packer validate -syntax-only "${PACKER_DIR}" >/dev/null 2>&1; then
        ok "packer validate -syntax-only"
    else
        # `packer validate` emits the diagnostic to stderr; rerun without
        # redirection so the operator sees it.
        if packer validate -syntax-only "${PACKER_DIR}"; then
            ok "packer validate -syntax-only (recheck)"
        else
            fail "packer validate -syntax-only"
        fi
    fi
else
    skip "packer validate -syntax-only"
fi

# ---------------------------------------------------------------------
# Lint every .sh with the shellcheck static analyser.
# ---------------------------------------------------------------------
mapfile -t SH_FILES < <(find "${PACKER_DIR}" -type f -name '*.sh' | sort)

if [[ ${#SH_FILES[@]} -eq 0 ]]; then
    fail "no .sh files found under ${PACKER_DIR}"
elif have shellcheck; then
    for sh in "${SH_FILES[@]}"; do
        rel="${sh#"${PACKER_DIR}"/}"
        if shellcheck --severity=warning --shell=bash "${sh}"; then
            ok "shellcheck ${rel}"
        else
            fail "shellcheck ${rel}"
        fi
    done
else
    for sh in "${SH_FILES[@]}"; do
        rel="${sh#"${PACKER_DIR}"/}"
        skip "shellcheck ${rel}"
    done
fi

# ---------------------------------------------------------------------
# bash -n (syntax-only parse) on every .sh -- always runs.
# ---------------------------------------------------------------------
for sh in "${SH_FILES[@]}"; do
    rel="${sh#"${PACKER_DIR}"/}"
    if bash -n "${sh}"; then
        ok "bash -n ${rel}"
    else
        fail "bash -n ${rel}"
    fi
done

# ---------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------
printf '\n[lint.sh] summary: ok=%d skipped=%d failed=%d\n' \
    "${CHECKS_OK}" "${CHECKS_SKIPPED}" "${CHECKS_FAILED}"

if [[ "${CHECKS_FAILED}" -gt 0 ]]; then
    exit 1
fi
exit 0
