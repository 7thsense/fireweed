#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HELPER="${SCRIPT_DIR}/governed-release-evidence-gate.sh"
VALID_MANIFEST="${SCRIPT_DIR}/fixtures/release-manifest/manifest.json"
VALID_E3_CONTRACT="${SCRIPT_DIR}/fixtures/e3-contract/valid/contract.json"
E3_REVISION=0123456789abcdef0123456789abcdef01234567
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

# Exercise both real semantic verifiers. No command shim may turn malformed evidence green.
bash "${HELPER}" --mode semantic --manifest "${VALID_MANIFEST}" \
    --e3-contract "${VALID_E3_CONTRACT}" --expected-revision "${E3_REVISION}"

# A former test hook allowed /bin/true to replace Cargo. It must now be inert: malformed evidence stays red.
printf '{}\n' >"${CASE_DIR}/invalid-manifest.json"
expect_failure cargo_override_cannot_bypass \
    env PQUEUE_CARGO_BIN=/bin/true bash "${HELPER}" \
    --mode semantic --manifest "${CASE_DIR}/invalid-manifest.json" \
    --e3-contract "${VALID_E3_CONTRACT}" --expected-revision "${E3_REVISION}"

# Each required authority fails closed even when its row remains beside the manifest. A coexisting TP-003
# JSONL is ignored because only explicitly listed TP-002 authority files are parsed.
for missing in E0 E1 E2 E3; do
    fixture_dir="${CASE_DIR}/missing-${missing}"
    mkdir -p "${fixture_dir}"
    cp "${SCRIPT_DIR}/fixtures/release-manifest/"*.jsonl "${fixture_dir}/"
    cp "${VALID_MANIFEST}" "${fixture_dir}/manifest.json"
    printf '%s\n' '{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003","ac":"AC-TXN-1","result":"pass"}' \
        >"${fixture_dir}/tp003.jsonl"
    MISSING="${missing}" MANIFEST="${fixture_dir}/manifest.json" python3 - <<'PY'
import json
import os
from pathlib import Path

path = Path(os.environ["MANIFEST"])
manifest = json.loads(path.read_text())
manifest["authorities"] = [
    authority
    for authority in manifest["authorities"]
    if authority["evidence_id"] != os.environ["MISSING"]
]
path.write_text(json.dumps(manifest, indent=2) + "\n")
PY
    expect_failure "missing-${missing}" bash "${HELPER}" \
        --mode semantic --manifest "${fixture_dir}/manifest.json" \
        --e3-contract "${VALID_E3_CONTRACT}" --expected-revision "${E3_REVISION}"
    grep -Fq -- "missing authority for ${missing}" "${CASE_DIR}/missing-${missing}.out" ||
        fail "missing ${missing} did not produce the fail-closed diagnostic"
done

mixed_dir="${CASE_DIR}/mixed-contracts"
mkdir -p "${mixed_dir}"
cp "${SCRIPT_DIR}/fixtures/release-manifest/"*.jsonl "${mixed_dir}/"
cp "${VALID_MANIFEST}" "${mixed_dir}/manifest.json"
printf '%s\n' '{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003","ac":"AC-TXN-1","result":"pass"}' \
    >"${mixed_dir}/tp003.jsonl"
bash "${HELPER}" --mode semantic --manifest "${mixed_dir}/manifest.json" \
    --e3-contract "${VALID_E3_CONTRACT}" --expected-revision "${E3_REVISION}"

expect_failure duplicate_mode bash "${HELPER}" \
    --mode semantic --mode semantic --manifest "${VALID_MANIFEST}" \
    --e3-contract "${VALID_E3_CONTRACT}" --expected-revision "${E3_REVISION}"
expect_failure duplicate_manifest bash "${HELPER}" \
    --mode semantic --manifest "${VALID_MANIFEST}" --manifest "${VALID_MANIFEST}" \
    --e3-contract "${VALID_E3_CONTRACT}" --expected-revision "${E3_REVISION}"

touch "${CASE_DIR}/attestation.json"
wrong_commit=0000000000000000000000000000000000000000
expect_failure commit_not_head bash "${HELPER}" \
    --mode exact-tag --manifest "${VALID_MANIFEST}" \
    --e3-contract "${VALID_E3_CONTRACT}" \
    --attestation "${CASE_DIR}/attestation.json" --tag does-not-matter --commit "${wrong_commit}"
grep -Fq -- '--commit must equal checked-out HEAD' "${CASE_DIR}/commit_not_head.out" ||
    fail "exact-tag mode did not reject a commit other than HEAD"

head_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
expect_failure missing_tag bash "${HELPER}" \
    --mode exact-tag --manifest "${VALID_MANIFEST}" \
    --e3-contract "${VALID_E3_CONTRACT}" \
    --attestation "${CASE_DIR}/attestation.json" --tag definitely-not-a-real-tag --commit "${head_commit}"
grep -Fq -- 'does not resolve to a commit' "${CASE_DIR}/missing_tag.out" ||
    fail "exact-tag mode did not require a real Git tag"

line_of() {
    local pattern="$1"
    local file="$2"
    grep -nF -- "${pattern}" "${file}" | head -n 1 | cut -d: -f1
}

release_gate="${SCRIPT_DIR}/release-gate.sh"
release_workflow="${REPO_ROOT}/.github/workflows/release.yml"
smoke_line="$(line_of '--require-smoke-evidence E2,E3' "${release_gate}")"
governed_line="$(line_of 'verify-governed-release-composite.sh' "${release_gate}")"
[[ -n "${smoke_line}" && -n "${governed_line}" && "${smoke_line}" -lt "${governed_line}" ]] ||
    fail "release gate must run fresh smoke before governed semantic verification"

workflow_smoke_line="$(line_of 'bash scripts/ci/release-gate.sh' "${release_workflow}")"
workflow_exact_line="$(line_of '--bin pqueue-verify-evidence-attestation' "${release_workflow}")"
[[ -n "${workflow_smoke_line}" && -n "${workflow_exact_line}" && "${workflow_smoke_line}" -lt "${workflow_exact_line}" ]] ||
    fail "release workflow must run fresh smoke before exact-tag governed verification"
# shellcheck disable=SC2016 # Literal GitHub expression under test.
grep -Fq -- '--tag "${{ steps.release.outputs.tag }}"' "${release_workflow}" ||
    fail "release workflow must pass the exact resolved tag"
# shellcheck disable=SC2016 # Literal workflow variable under test.
grep -Fq -- '--commit "${GITHUB_SHA}"' "${release_workflow}" ||
    fail "release workflow must pass the exact checked-out commit"
if grep -Eq -- '--ledger-dir|find[[:space:]].*docs/perf/evidence' "${HELPER}"; then
    fail "governed helper must not scan an evidence directory"
fi

echo "governed release evidence helper behavioral contract: PASS"
