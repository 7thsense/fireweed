#!/usr/bin/env bash
# Composable governed TP-002 gate. This helper validates an explicitly named manifest; it never scans an
# evidence directory. The smoke lane remains a separate prerequisite owned by its caller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

mode=""
manifest=""
attestation=""
tag=""
commit=""

usage() {
    cat >&2 <<'EOF'
usage:
  governed-release-evidence-gate.sh --mode semantic --manifest <path>
  governed-release-evidence-gate.sh --mode exact-tag --manifest <path> \
      --attestation <path> --tag <tag> --commit <40-char-sha>
EOF
    exit 64
}

while (($# > 0)); do
    case "$1" in
        --mode|--manifest|--attestation|--tag|--commit)
            (($# >= 2)) || usage
            case "$1" in
                --mode) mode="$2" ;;
                --manifest) manifest="$2" ;;
                --attestation) attestation="$2" ;;
                --tag) tag="$2" ;;
                --commit) commit="$2" ;;
            esac
            shift 2
            ;;
        *) usage ;;
    esac
done

[[ -n "${manifest}" && -f "${manifest}" ]] || usage
case "${mode}" in
    semantic)
        [[ -z "${attestation}${tag}${commit}" ]] || usage
        ;;
    exact-tag)
        [[ -n "${attestation}" && -f "${attestation}" && -n "${tag}" ]] || usage
        [[ "${commit}" =~ ^[0-9a-f]{40}$ ]] || usage
        ;;
    *) usage ;;
esac

run_cargo() {
    if [[ -n "${PQUEUE_CARGO_BIN:-}" ]]; then
        "${PQUEUE_CARGO_BIN}" "$@"
    else
        rustup run 1.92.0 cargo "$@"
    fi
}

echo "--- governed TP-002 semantic release manifest ---"
run_cargo run -p pqueue-release --bin pqueue-verify-ledger -- \
    --manifest "${manifest}" \
    --require-evidence E0,E1,E2,E3

if [[ "${mode}" == "exact-tag" ]]; then
    echo "--- exact-tag governed evidence attestation ---"
    run_cargo run -p pqueue-release --bin pqueue-verify-evidence-attestation -- \
        --manifest "${attestation}" \
        --repo-root "${REPO_ROOT}" \
        --tag "${tag}" \
        --commit "${commit}"
fi

echo "governed release evidence gate PASSED (${mode})"
