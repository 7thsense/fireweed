#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HELPER="${SCRIPT_DIR}/governed-release-evidence-gate.sh"
VALID_MANIFEST="${SCRIPT_DIR}/fixtures/release-manifest/manifest.json"
E3_REVISION=0123456789abcdef0123456789abcdef01234567
CASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pqueue-governed-gate.XXXXXX")"
E3_CONTRACT="${CASE_DIR}/e3-contract.json"
trap 'rm -rf "${CASE_DIR}"' EXIT
printf '{}\n' >"${E3_CONTRACT}"

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

# Exercise real helper dispatch. The branch intentionally predates the hardened E3 CLI, so a rustup shim
# records argv token-by-token; the integrated branch runs the same command against the real verifier.
mkdir "${CASE_DIR}/bin"
cat >"${CASE_DIR}/bin/rustup" <<'EOF'
#!/usr/bin/env bash
printf '%q ' "$@" >>"${COMMAND_LOG}"
printf '\n' >>"${COMMAND_LOG}"
EOF
chmod +x "${CASE_DIR}/bin/rustup"
COMMAND_LOG="${CASE_DIR}/commands.log" PATH="${CASE_DIR}/bin:${PATH}" \
    bash "${HELPER}" --mode semantic --manifest "${VALID_MANIFEST}" \
    --e3-contract "${E3_CONTRACT}" --expected-revision "${E3_REVISION}"
[[ "$(wc -l <"${CASE_DIR}/commands.log")" -eq 2 ]] || fail "semantic mode must run both verifiers"
grep -Fq -- "--bin pqueue-verify-e3-contract -- --manifest ${E3_CONTRACT} --expected-revision ${E3_REVISION}" \
    "${CASE_DIR}/commands.log" || fail "semantic mode did not dispatch the source-pinned E3 contract verifier"

# A former test hook allowed /bin/true to replace Cargo. It must now be inert: malformed evidence stays red.
printf '{}\n' >"${CASE_DIR}/invalid-manifest.json"
expect_failure cargo_override_cannot_bypass \
    env PQUEUE_CARGO_BIN=/bin/true bash "${HELPER}" \
    --mode semantic --manifest "${CASE_DIR}/invalid-manifest.json" \
    --e3-contract "${E3_CONTRACT}" --expected-revision "${E3_REVISION}"

expect_failure duplicate_mode bash "${HELPER}" \
    --mode semantic --mode semantic --manifest "${VALID_MANIFEST}" \
    --e3-contract "${E3_CONTRACT}" --expected-revision "${E3_REVISION}"
expect_failure duplicate_manifest bash "${HELPER}" \
    --mode semantic --manifest "${VALID_MANIFEST}" --manifest "${VALID_MANIFEST}" \
    --e3-contract "${E3_CONTRACT}" --expected-revision "${E3_REVISION}"

touch "${CASE_DIR}/attestation.json"
wrong_commit=0000000000000000000000000000000000000000
expect_failure commit_not_head bash "${HELPER}" \
    --mode exact-tag --manifest "${VALID_MANIFEST}" \
    --e3-contract "${E3_CONTRACT}" \
    --attestation "${CASE_DIR}/attestation.json" --tag does-not-matter --commit "${wrong_commit}"
grep -Fq -- '--commit must equal checked-out HEAD' "${CASE_DIR}/commit_not_head.out" ||
    fail "exact-tag mode did not reject a commit other than HEAD"

head_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
expect_failure missing_tag bash "${HELPER}" \
    --mode exact-tag --manifest "${VALID_MANIFEST}" \
    --e3-contract "${E3_CONTRACT}" \
    --attestation "${CASE_DIR}/attestation.json" --tag definitely-not-a-real-tag --commit "${head_commit}"
grep -Fq -- 'does not resolve to a commit' "${CASE_DIR}/missing_tag.out" ||
    fail "exact-tag mode did not require a real Git tag"

echo "governed release evidence helper behavioral contract: PASS"
