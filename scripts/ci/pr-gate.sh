#!/usr/bin/env bash
# Pull-request gate runner for Fireweed Queue.
#
# Usage: pr-gate.sh --mode bootstrap|enforcing|remediation|closure
#
# bootstrap  Run fmt / clippy / test / cargo-deny / cargo-machete /
#            coverage-report (no thresholds) / property+fuzz smoke.
#            Hard coverage enforcement (fireweed-core >=90% line / >=85%
#            branch; fireweed-engine >=80% line) runs in the enforcing mode /
#            release-gate.sh; bootstrap only reports.
# enforcing  Run the release-enforced local gate: fmt check, focused ledger
#            validator tests, live coverage thresholds, closure checks, release
#            gate, and nightly wrapper.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode) MODE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$MODE" ]]; then
    echo "Usage: $(basename "$0") --mode <bootstrap|enforcing|remediation|closure>" >&2
    exit 1
fi

case "$MODE" in
    bootstrap|enforcing|remediation|closure) ;;
    *) echo "Unknown mode: $MODE (supported: bootstrap, enforcing, remediation, closure)" >&2; exit 1 ;;
esac

echo "=== pr-gate [mode=${MODE}] ==="

echo "--- release artifact verifier fixtures ---"
bash "${SCRIPT_DIR}/../release/verify-release-artifacts-test.sh"

echo "--- governed evidence archive fixtures ---"
bash "${SCRIPT_DIR}/../release/build-governed-evidence-bundle-test.sh"

POLICY_MODE="remediation"
if [[ "$MODE" == "enforcing" || "$MODE" == "closure" ]]; then
    POLICY_MODE="closure"
fi

echo "--- storage remediation policy [${POLICY_MODE}] ---"
bash "${SCRIPT_DIR}/storage-remediation-policy.sh" --policy "${POLICY_MODE}"

if [[ "$MODE" == "remediation" || "$MODE" == "closure" ]]; then
    echo "=== pr-gate [${MODE}] PASSED ==="
    exit 0
fi

if [[ "$MODE" == "enforcing" ]]; then
    echo "--- fmt ---"
    rustup run 1.92.0 cargo fmt --all --check

    echo "--- ledger validator tests ---"
    rustup run 1.92.0 cargo test -p fireweed-release -- --nocapture

    echo "--- coverage threshold parser fixtures ---"
    bash "${SCRIPT_DIR}/check-lcov-coverage.py" --fixture "${SCRIPT_DIR}/fixtures/lcov/core-pass.info" --crate fireweed-core --min-lines 90 --min-branches 85
    bash "${SCRIPT_DIR}/check-lcov-coverage.py" --fixture "${SCRIPT_DIR}/fixtures/lcov/engine-pass.info" --crate fireweed-engine --min-lines 80

    echo "--- product workflow suite names ---"
    bash "${SCRIPT_DIR}/verify-product-workflow-names.sh" "${SCRIPT_DIR}/release-repeat-suites.toml"

    # nightly-gate.sh already runs release-gate.sh (then adds deferral linting), so we invoke it
    # ONCE here rather than running the (now heavy) release gate twice.
    echo "--- nightly gate (wraps release gate) ---"
    bash "${SCRIPT_DIR}/nightly-gate.sh"

    echo "=== pr-gate [${MODE}] PASSED ==="
    exit 0
fi

echo "--- fmt ---"
rustup run 1.92.0 cargo fmt --all --check

echo "--- clippy ---"
rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings

echo "--- test ---"
rustup run 1.92.0 cargo test --workspace

echo "--- cargo deny ---"
cargo deny check

echo "--- cargo machete ---"
cargo machete

echo "--- coverage ---"
bash "${SCRIPT_DIR}/coverage-report.sh"

echo "--- property + fuzz smoke ---"
bash "${SCRIPT_DIR}/property-fuzz-smoke.sh"

echo "--- microsite gate ---"
bash "${SCRIPT_DIR}/microsite-gate.sh"

echo "--- API-005 suite ownership map ---"
python3 "${SCRIPT_DIR}/api005_suite_ownership.py" --self-test

echo "--- functional-matrix route sources (P10r exact leaves) ---"
python3 "${SCRIPT_DIR}/functional_matrix_route_sources.py" --check --self-test

echo "=== pr-gate [${MODE}] PASSED ==="
