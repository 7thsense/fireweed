#!/usr/bin/env bash
# Composable governed TP-002 gate. This helper validates an explicitly named manifest; it never scans an
# evidence directory. The smoke lane remains a separate prerequisite owned by its caller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

mode=""
manifest=""
attestation=""
e3_contract=""
expected_revision=""
tag=""
commit=""
declare -A seen=()

usage() {
    cat >&2 <<'EOF'
usage:
  governed-release-evidence-gate.sh --mode semantic --manifest <path> \
      --e3-contract <path> --expected-revision <40-char-sha>
  governed-release-evidence-gate.sh --mode exact-tag --manifest <path> --e3-contract <path> \
      --attestation <path> --tag <tag> --commit <40-char-sha>
EOF
    exit 64
}

while (($# > 0)); do
    case "$1" in
        --mode|--manifest|--e3-contract|--expected-revision|--attestation|--tag|--commit)
            (($# >= 2)) || usage
            [[ -z "${seen[$1]:-}" ]] || usage
            seen[$1]=1
            case "$1" in
                --mode) mode="$2" ;;
                --manifest) manifest="$2" ;;
                --e3-contract) e3_contract="$2" ;;
                --expected-revision) expected_revision="$2" ;;
                --attestation) attestation="$2" ;;
                --tag) tag="$2" ;;
                --commit) commit="$2" ;;
            esac
            shift 2
            ;;
        *) usage ;;
    esac
done

[[ -n "${manifest}" && -f "${manifest}" && -n "${e3_contract}" && -f "${e3_contract}" ]] || usage
case "${mode}" in
    semantic)
        [[ -z "${attestation}${tag}${commit}" ]] || usage
        [[ "${expected_revision}" =~ ^[0-9a-f]{40}$ ]] || usage
        ;;
    exact-tag)
        [[ -z "${expected_revision}" ]] || usage
        [[ -n "${attestation}" && -f "${attestation}" && -n "${tag}" ]] || usage
        [[ "${commit}" =~ ^[0-9a-f]{40}$ ]] || usage
        head_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
        [[ "${commit}" == "${head_commit}" ]] || {
            echo "governed-release-evidence-gate: --commit must equal checked-out HEAD (${head_commit})" >&2
            exit 1
        }
        tag_commit="$(git -C "${REPO_ROOT}" rev-parse --verify "refs/tags/${tag}^{commit}" 2>/dev/null)" || {
            echo "governed-release-evidence-gate: release tag ${tag@Q} does not resolve to a commit" >&2
            exit 1
        }
        [[ "${tag_commit}" == "${commit}" ]] || {
            echo "governed-release-evidence-gate: release tag ${tag@Q} targets ${tag_commit}, not ${commit}" >&2
            exit 1
        }
        expected_revision="${commit}"
        ;;
    *) usage ;;
esac

run_cargo() {
    rustup run 1.97.1 cargo "$@"
}

echo "--- governed TP-002 semantic release manifest ---"
run_cargo run -p fireweed-release --bin fireweed-verify-ledger -- \
    --manifest "${manifest}" \
    --require-evidence E0,E1,E2,E3

echo "--- governed E3 contract ---"
run_cargo run -p fireweed-release --bin fireweed-verify-e3-contract -- \
    --manifest "${e3_contract}" \
    --expected-revision "${expected_revision}"

if [[ "${mode}" == "exact-tag" ]]; then
    echo "--- exact-tag governed evidence attestation ---"
    run_cargo run -p fireweed-release --bin fireweed-verify-evidence-attestation -- \
        --manifest "${attestation}" \
        --repo-root "${REPO_ROOT}" \
        --tag "${tag}" \
        --commit "${commit}"
fi

echo "governed release evidence gate PASSED (${mode})"
