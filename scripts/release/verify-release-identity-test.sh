#!/usr/bin/env bash
# Focused negatives/positives for P17v release-identity freeze.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$REPO_ROOT"

CASE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-release-identity.XXXXXX")"
cleanup() { rm -rf "$CASE_ROOT"; }
trap cleanup EXIT

fail() {
    echo "verify-release-identity-test: $*" >&2
    exit 1
}

# Positive path against the real tree.
bash "${SCRIPT_DIR}/verify-release-identity.sh" --version 0.31.2

# Inventory classifies independent coordinates and reports tag reservation.
inventory="$(bash "${SCRIPT_DIR}/list-public-version-sources.sh" v0.31.2)"
printf '%s\n' "$inventory" | grep -Fq 'crates/fireweed-bench/Cargo.toml: package.version=0.3.1; treatment=independent tool coordinate' ||
    fail "bench independent classification missing"
printf '%s\n' "$inventory" | grep -Fq 'treatment=independent gate-set identity; owner=P13a' ||
    fail "gate-set independent classification missing"
printf '%s\n' "$inventory" | grep -Fq 'git tag v0.31.2: state=absent' ||
    fail "tag reservation absent state missing"
printf '%s\n' "$inventory" | grep -Fq 'Cargo.toml: workspace.package.version=0.31.2; treatment=release-synchronized' ||
    fail "workspace synchronized classification missing"

# Negative: wrong expected version fails.
if bash "${SCRIPT_DIR}/verify-release-identity.sh" --version 0.30.0 >/dev/null 2>"${CASE_ROOT}/wrong-version.err"; then
    fail "expected version mismatch to fail"
fi
grep -Fq 'workspace.package.version=0.31.2 != 0.30.0' "${CASE_ROOT}/wrong-version.err" ||
    fail "wrong-version diagnostic missing"

# Negative: missing usage args fail closed.
if bash "${SCRIPT_DIR}/verify-release-identity.sh" >/dev/null 2>"${CASE_ROOT}/usage.err"; then
    fail "missing --version should fail"
fi
grep -Fq 'usage:' "${CASE_ROOT}/usage.err" || fail "usage diagnostic missing"

# Independent gate-set identity test remains green and is not package SemVer.
bash scripts/ci/public-release-gates-identity-test.sh

echo "verify-release-identity-test: ok"
