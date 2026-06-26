#!/usr/bin/env bash
# Coverage report for pqueue (bootstrap mode; no fail-under thresholds).
#
# Reports pqueue-core and pqueue-engine line coverage. (pqueue-service was
# deleted in the Phase-6 hexagonal migration; the command/workflow logic it
# held now lives in pqueue-engine + the pqueue library facade, so pqueue-engine
# is the post-migration successor to the old pqueue-service coverage target.)
# Thresholds are intentionally NOT enforced here — the release gate
# (release-gate.sh) enforces the live bars; this is the per-PR bootstrap report.
#
# TP-003 §5 per-PR coverage targets (enforced live in release-gate.sh; reported
# without thresholds here):
#   pqueue-core   : >= 90% line, >= 85% branch
#   pqueue-engine : >= 80% line
#
# NOTE: Branch coverage (--branch / -Z coverage-options=branch) requires
# a nightly toolchain and is not available on stable 1.92.0.
set -euo pipefail

echo "=== coverage (bootstrap; thresholds not enforced) ==="
echo "    Targets (enforced live in release-gate.sh): pqueue-core >=90% line / >=85% branch; pqueue-engine >=80% line"
echo "    Branch coverage deferred here: requires nightly toolchain (--branch/-Z coverage-options=branch)"

# Clean instrumentation so artifacts from deleted crates can't contaminate the
# reported numbers.
cargo +1.92.0 llvm-cov clean --workspace

echo "--- pqueue-core (line) ---"
cargo +1.92.0 llvm-cov \
    --package pqueue-core \
    --summary-only \
    --fail-under-lines 0

echo "--- pqueue-engine (line) ---"
cargo +1.92.0 llvm-cov \
    --package pqueue-engine \
    --summary-only \
    --fail-under-lines 0
