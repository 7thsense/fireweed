#!/usr/bin/env bash
# Coverage report for pqueue (bootstrap mode; no fail-under thresholds).
#
# Prints pqueue-core line coverage and pqueue-service line coverage.
# Thresholds are intentionally NOT enforced here.
#
# TP-003 §5 per-PR coverage targets (deferred to the release orchestrator
# bead, the required successor that flips --fail-under-lines and
# adds --fail-under-branches once bars are proven in the release lane):
#   pqueue-core  : >= 90% line, >= 85% branch
#   pqueue-service: >= 80% line
#
# NOTE: Branch coverage (--branch / -Z coverage-options=branch) requires
# a nightly toolchain and is not available on stable 1.92.0. The release
# orchestrator bead is the required successor that adds nightly-gated
# branch coverage enforcement alongside the hard threshold flip.
set -euo pipefail

echo "=== coverage (bootstrap; thresholds not enforced) ==="
echo "    Targets (deferred): pqueue-core >=90% line / >=85% branch; pqueue-service >=80% line"
echo "    Branch coverage deferred: requires nightly toolchain (--branch/-Z coverage-options=branch)"

echo "--- pqueue-core (line) ---"
cargo +1.92.0 llvm-cov \
    --package pqueue-core \
    --text \
    --summary-only \
    --fail-under-lines 0

echo "--- pqueue-service (line) ---"
cargo +1.92.0 llvm-cov \
    --package pqueue-service \
    --text \
    --summary-only \
    --fail-under-lines 0
