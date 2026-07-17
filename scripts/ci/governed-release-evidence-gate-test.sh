#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HELPER="${SCRIPT_DIR}/governed-release-evidence-gate.sh"
FIXTURES="${SCRIPT_DIR}/fixtures/governed-release-wiring"
CASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pqueue-governed-gate.XXXXXX")"
trap 'rm -rf "${CASE_DIR}"' EXIT

touch "${CASE_DIR}/manifest.json" "${CASE_DIR}/attestation.json"
cat >"${CASE_DIR}/fake-cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${PQUEUE_TEST_COMMAND_LOG}"
EOF
chmod +x "${CASE_DIR}/fake-cargo"
export PQUEUE_CARGO_BIN="${CASE_DIR}/fake-cargo"
export PQUEUE_TEST_COMMAND_LOG="${CASE_DIR}/commands.log"

fail() {
    echo "governed-release-evidence-gate-test: $*" >&2
    exit 1
}

line_of() {
    local pattern="$1"
    local file="$2"
    grep -nF -- "${pattern}" "${file}" | head -n 1 | cut -d: -f1
}

assert_no_directory_scan() {
    local file="$1"
    if grep -Eq -- '--ledger-dir|find[[:space:]].*(docs/perf/evidence|evidence)|docs/perf/evidence/?[[:space:]]*$' "${file}"; then
        fail "${file} scans or delegates an evidence directory"
    fi
}

: >"${PQUEUE_TEST_COMMAND_LOG}"
bash "${HELPER}" --mode semantic --manifest "${CASE_DIR}/manifest.json"
[[ "$(wc -l <"${PQUEUE_TEST_COMMAND_LOG}")" -eq 1 ]] || fail "semantic mode must run one verifier"
grep -Fq -- '--bin pqueue-verify-ledger -- --manifest' "${PQUEUE_TEST_COMMAND_LOG}" ||
    fail "semantic mode did not invoke the release-manifest verifier"
grep -Fq -- '--require-evidence E0,E1,E2,E3' "${PQUEUE_TEST_COMMAND_LOG}" ||
    fail "semantic mode did not require exact E0-E3 coverage"
assert_no_directory_scan "${PQUEUE_TEST_COMMAND_LOG}"

: >"${PQUEUE_TEST_COMMAND_LOG}"
tag='v9.8.7'
commit='0123456789abcdef0123456789abcdef01234567'
bash "${HELPER}" \
    --mode exact-tag \
    --manifest "${CASE_DIR}/manifest.json" \
    --attestation "${CASE_DIR}/attestation.json" \
    --tag "${tag}" \
    --commit "${commit}"
[[ "$(wc -l <"${PQUEUE_TEST_COMMAND_LOG}")" -eq 2 ]] || fail "exact-tag mode must run two verifiers"
grep -Fq -- "--bin pqueue-verify-evidence-attestation -- --manifest ${CASE_DIR}/attestation.json --repo-root ${REPO_ROOT} --tag ${tag} --commit ${commit}" "${PQUEUE_TEST_COMMAND_LOG}" ||
    fail "exact-tag mode did not pass the exact tag and commit to attestation verification"
assert_no_directory_scan "${PQUEUE_TEST_COMMAND_LOG}"

smoke_line="$(line_of '--require-smoke-evidence E2,E3' "${FIXTURES}/release-gate.wiring")"
governed_line="$(line_of 'bash scripts/ci/governed-release-evidence-gate.sh' "${FIXTURES}/release-gate.wiring")"
[[ -n "${smoke_line}" && -n "${governed_line}" && "${smoke_line}" -lt "${governed_line}" ]] ||
    fail "semantic wiring must run smoke before governed verification"

workflow_smoke_line="$(line_of 'bash scripts/ci/release-gate.sh' "${FIXTURES}/release-workflow.wiring")"
workflow_governed_line="$(line_of 'bash scripts/ci/governed-release-evidence-gate.sh' "${FIXTURES}/release-workflow.wiring")"
[[ -n "${workflow_smoke_line}" && -n "${workflow_governed_line}" && "${workflow_smoke_line}" -lt "${workflow_governed_line}" ]] ||
    fail "exact-tag workflow wiring must run smoke before governed verification"
# shellcheck disable=SC2016 # These are the literal GitHub expression and shell variable under test.
grep -Fq -- '--tag "${{ steps.release.outputs.tag }}"' "${FIXTURES}/release-workflow.wiring" ||
    fail "workflow wiring must pass the exact resolved release tag"
# shellcheck disable=SC2016 # This is the literal workflow variable under test.
grep -Fq -- '--commit "${GITHUB_SHA}"' "${FIXTURES}/release-workflow.wiring" ||
    fail "workflow wiring must pass the exact checked-out commit"

assert_no_directory_scan "${HELPER}"
assert_no_directory_scan "${FIXTURES}/release-workflow.wiring"

echo "governed release evidence helper and wiring contracts: PASS"
