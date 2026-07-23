#!/usr/bin/env bash
# Release-blocking full-history secret scan. Keep the version synchronized with
# install-gitleaks.sh so local and release scans use the same default rules.
set -euo pipefail

readonly EXPECTED_GITLEAKS_VERSION="8.30.1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly REPO_ROOT

if (($# != 0)); then
    echo "usage: bash scripts/ci/gitleaks-gate.sh" >&2
    exit 64
fi
if ! command -v gitleaks >/dev/null 2>&1; then
    echo "gitleaks-gate: gitleaks ${EXPECTED_GITLEAKS_VERSION} is required" >&2
    exit 1
fi

actual_version="$(gitleaks version)"
if [[ "${actual_version}" != "${EXPECTED_GITLEAKS_VERSION}" ]]; then
    echo "gitleaks-gate: expected ${EXPECTED_GITLEAKS_VERSION}, found ${actual_version}" >&2
    exit 1
fi

cd "${REPO_ROOT}"
gitleaks git --redact --exit-code 1
