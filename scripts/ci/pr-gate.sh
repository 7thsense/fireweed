#!/usr/bin/env bash
# PR gate runner for pqueue.
#
# Usage: pr-gate.sh --mode bootstrap|enforcing
#
# bootstrap  Run fmt / clippy / test / cargo-deny / cargo-machete /
#            coverage-report (no thresholds) / property+fuzz smoke.
#            Hard coverage enforcement (pqueue-core >=90% line / >=85%
#            branch; pqueue-service >=80% line) is deferred to the release
#            orchestrator bead, the required successor that flips
#            --fail-under thresholds once bars are proven in the release
#            lane.
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
    echo "Usage: $(basename "$0") --mode <bootstrap|enforcing>" >&2
    exit 1
fi

case "$MODE" in
    bootstrap|enforcing) ;;
    *) echo "Unknown mode: $MODE (supported: bootstrap, enforcing)" >&2; exit 1 ;;
esac

echo "=== pr-gate [mode=${MODE}] ==="

if [[ "$MODE" == "enforcing" ]]; then
    echo "--- fmt ---"
    cargo +1.92.0 fmt --all --check

    echo "--- ledger validator tests ---"
    cargo +1.92.0 test -p pqueue-service verification_ledger_tests -- --nocapture

    echo "--- coverage threshold parser fixtures ---"
    bash "${SCRIPT_DIR}/check-lcov-coverage.py" --fixture "${SCRIPT_DIR}/fixtures/lcov/core-pass.info" --crate pqueue-core --min-lines 90 --min-branches 85
    bash "${SCRIPT_DIR}/check-lcov-coverage.py" --fixture "${SCRIPT_DIR}/fixtures/lcov/service-pass.info" --crate pqueue-service --min-lines 80

    echo "--- product workflow suite names ---"
    bash "${SCRIPT_DIR}/verify-product-workflow-names.sh" "${SCRIPT_DIR}/release-repeat-suites.toml"

    echo "--- release gate ---"
    bash "${SCRIPT_DIR}/release-gate.sh" --require-tp002-evidence E0,E1,E2,E3 \
        --tp002-e0e1-source pqueue-7e2b3132 \
        --tp002-e2-source pqueue-9afd88cc,pqueue-76d92a33 \
        --tp002-e3-source pqueue-b1abd895,pqueue-472a09d4

    echo "--- nightly gate ---"
    bash "${SCRIPT_DIR}/nightly-gate.sh"

    echo "=== pr-gate [${MODE}] PASSED ==="
    exit 0
fi

echo "--- fmt ---"
cargo +1.92.0 fmt --all --check

echo "--- clippy ---"
cargo +1.92.0 clippy --workspace --all-targets -- -D warnings

echo "--- test ---"
cargo +1.92.0 test --workspace

echo "--- cargo deny ---"
cargo deny check

echo "--- cargo machete ---"
cargo machete

echo "--- coverage ---"
bash "${SCRIPT_DIR}/coverage-report.sh"

echo "--- property + fuzz smoke ---"
bash "${SCRIPT_DIR}/property-fuzz-smoke.sh"

echo "=== pr-gate [${MODE}] PASSED ==="
