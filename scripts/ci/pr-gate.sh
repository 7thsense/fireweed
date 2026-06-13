#!/usr/bin/env bash
# PR gate runner for pqueue.
#
# Usage: pr-gate.sh --mode bootstrap
#
# bootstrap  Run fmt / clippy / test / cargo-deny / cargo-machete /
#            coverage-report (no thresholds) / property+fuzz smoke.
#            Hard coverage enforcement (pqueue-core >=90% line / >=85%
#            branch; pqueue-service >=80% line) is deferred to the release
#            orchestrator bead, the required successor that flips
#            --fail-under thresholds once bars are proven in the release
#            lane.
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
    echo "Usage: $(basename "$0") --mode <bootstrap>" >&2
    exit 1
fi

case "$MODE" in
    bootstrap) ;;
    *) echo "Unknown mode: $MODE (supported: bootstrap)" >&2; exit 1 ;;
esac

echo "=== pr-gate [mode=${MODE}] ==="

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
