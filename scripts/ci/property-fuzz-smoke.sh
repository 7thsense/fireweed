#!/usr/bin/env bash
# Property test and fuzz smoke runner (PR tier; bootstrap scaffold).
#
# Runs proptest suites and cargo-fuzz targets that exist; passes
# immediately when none are registered. Feature beads populate these:
#   B-011: priority-decode fuzz + AC-CORE-1 proptest
#   B-020: command-envelope-decode fuzz
#   B-100: operator-selector fuzz
#
# PR tier: >=10,000 proptest cases per property, >=10 s fuzz per target.
set -euo pipefail

echo "=== property + fuzz smoke ==="

FOUND_PROPERTY=0
FOUND_FUZZ=0

if rustup run 1.97.1 cargo test --workspace --list 2>/dev/null \
        | grep -qE 'proptest|_property_tests'; then
    FOUND_PROPERTY=1
fi

if command -v cargo-fuzz >/dev/null 2>&1 \
        && cargo fuzz list 2>/dev/null | grep -q .; then
    FOUND_FUZZ=1
fi

if [[ $FOUND_PROPERTY -eq 0 && $FOUND_FUZZ -eq 0 ]]; then
    echo "No property tests or fuzz targets registered yet (scaffold passes)."
    echo "=== property + fuzz smoke PASSED (no targets) ==="
    exit 0
fi

if [[ $FOUND_PROPERTY -eq 1 ]]; then
    echo "--- property tests ---"
    rustup run 1.97.1 cargo test --workspace -- --include-ignored
fi

if [[ $FOUND_FUZZ -eq 1 ]]; then
    echo "--- fuzz smoke (10 s per target) ---"
    while IFS= read -r target; do
        echo "  fuzz: ${target}"
        cargo fuzz run "${target}" -- -max_total_time=10 -timeout=5
    done < <(cargo fuzz list)
fi

echo "=== property + fuzz smoke PASSED ==="
