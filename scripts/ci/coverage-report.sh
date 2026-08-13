#!/usr/bin/env bash
# Coverage report for Fireweed Queue (bootstrap mode; no fail-under thresholds).
#
# Reports fireweed-core and fireweed-engine line coverage. (The old service was
# deleted in the Phase-6 hexagonal migration; the command/workflow logic it
# held now lives in fireweed-engine + the fireweed library facade, so fireweed-engine
# is the post-migration successor to that old coverage target.)
# Thresholds are intentionally NOT enforced here — the release gate
# (release-gate.sh) enforces the live bars; this is the per-PR bootstrap report.
#
# TP-003 §5 per-PR coverage targets (enforced live in release-gate.sh; reported
# without thresholds here):
#   fireweed-core   : >= 90% line, >= 85% branch
#   fireweed-engine : >= 80% line
#
# NOTE: Branch coverage (--branch / -Z coverage-options=branch) requires
# a nightly toolchain and is not available on stable 1.97.1.
set -euo pipefail

echo "=== coverage (bootstrap; thresholds not enforced) ==="
echo "    Targets (enforced live in release-gate.sh): fireweed-core >=90% line / >=85% branch; fireweed-engine >=80% line"
echo "    Branch coverage deferred here: requires nightly toolchain (--branch/-Z coverage-options=branch)"

# Clean instrumentation so artifacts from deleted crates can't contaminate the
# reported numbers.
rustup run 1.97.1 cargo llvm-cov clean --workspace

echo "--- fireweed-core (line) ---"
rustup run 1.97.1 cargo llvm-cov \
    --package fireweed-core \
    --summary-only \
    --fail-under-lines 0

echo "--- fireweed-engine (line) ---"
rustup run 1.97.1 cargo llvm-cov \
    --package fireweed-engine \
    --summary-only \
    --fail-under-lines 0
