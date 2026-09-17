#!/usr/bin/env bash
# Property test and fuzz smoke runner (PR tier).
#
# PR tier: >=10,000 proptest cases per property, >=10 s fuzz per target.
# Keep the property target list explicit: property names do not necessarily
# contain "proptest", and a discovery failure must never mean a passing skip.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

export PROPTEST_CASES=10000
export RUST_TEST_THREADS=1

echo "=== property + fuzz smoke ==="
echo "--- core priority ordering properties (10,000 cases per property) ---"
rustup run 1.97.1 cargo test --locked -p fireweed-core \
    --test core_priority_model_tests -- --test-threads=1

if [[ -f fuzz/Cargo.toml ]]; then
    if ! command -v cargo-fuzz >/dev/null 2>&1; then
        echo "property-fuzz-smoke: fuzz/Cargo.toml exists but cargo-fuzz is unavailable" >&2
        exit 1
    fi
    # Capture discovery before iteration so command failures propagate. LibFuzzer
    # requires nightly; an unavailable toolchain or broken target fails this gate.
    targets="$(rustup run nightly cargo fuzz list)"
    echo "--- fuzz smoke (10 s per target) ---"
    while IFS= read -r target; do
        [[ -n "${target}" ]] || continue
        echo "  fuzz: ${target}"
        rustup run nightly cargo fuzz run "${target}" -- \
            -max_total_time=10 -timeout=5 -jobs=1 -workers=1
    done <<< "${targets}"
else
    echo "No cargo-fuzz project registered; fuzz smoke not applicable."
fi

echo "=== property + fuzz smoke PASSED ==="
