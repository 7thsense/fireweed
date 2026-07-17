#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HELPER="${SCRIPT_DIR}/governed-release-evidence-gate.sh"
VALID_MANIFEST="${SCRIPT_DIR}/fixtures/release-manifest/manifest.json"
CASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pqueue-governed-gate.XXXXXX")"
trap 'rm -rf "${CASE_DIR}"' EXIT

fail() {
    echo "governed-release-evidence-gate-test: $*" >&2
    exit 1
}

expect_failure() {
    local label="$1"
    shift
    if "$@" >"${CASE_DIR}/${label}.out" 2>&1; then
        fail "${label} unexpectedly passed"
    fi
}

# Exercise the real semantic verifier, not a command logger.
bash "${HELPER}" --mode semantic --manifest "${VALID_MANIFEST}"

# A former test hook allowed /bin/true to replace Cargo. It must now be inert: malformed evidence stays red.
printf '{}\n' >"${CASE_DIR}/invalid-manifest.json"
expect_failure cargo_override_cannot_bypass \
    env PQUEUE_CARGO_BIN=/bin/true bash "${HELPER}" \
    --mode semantic --manifest "${CASE_DIR}/invalid-manifest.json"

expect_failure duplicate_mode bash "${HELPER}" \
    --mode semantic --mode semantic --manifest "${VALID_MANIFEST}"
expect_failure duplicate_manifest bash "${HELPER}" \
    --mode semantic --manifest "${VALID_MANIFEST}" --manifest "${VALID_MANIFEST}"

touch "${CASE_DIR}/attestation.json"
wrong_commit=0000000000000000000000000000000000000000
expect_failure commit_not_head bash "${HELPER}" \
    --mode exact-tag --manifest "${VALID_MANIFEST}" \
    --attestation "${CASE_DIR}/attestation.json" --tag does-not-matter --commit "${wrong_commit}"
grep -Fq -- '--commit must equal checked-out HEAD' "${CASE_DIR}/commit_not_head.out" ||
    fail "exact-tag mode did not reject a commit other than HEAD"

head_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
expect_failure missing_tag bash "${HELPER}" \
    --mode exact-tag --manifest "${VALID_MANIFEST}" \
    --attestation "${CASE_DIR}/attestation.json" --tag definitely-not-a-real-tag --commit "${head_commit}"
grep -Fq -- 'does not resolve to a commit' "${CASE_DIR}/missing_tag.out" ||
    fail "exact-tag mode did not require a real Git tag"

echo "governed release evidence helper behavioral contract: PASS"
