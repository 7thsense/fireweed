#!/usr/bin/env bash
# P2r: invoke exact generated suite leaves and require ran=1 each.
# Usage:
#   run-exact-suite-leaves.sh <suite-name>
#   run-exact-suite-leaves.sh --manifest-leaf <p10r-leaf-id>
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

MANIFEST="${ROOT}/docs/helix/04-build/route-feature-manifest.json"
ROUTE_SOURCES="${ROOT}/docs/helix/04-build/functional-matrix-route-sources.json"

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <suite-name> | --manifest-leaf <leaf-id>" >&2
    exit 2
fi

run_exact() {
    local -a cmd=("$@")
    local output
    echo "+ ${cmd[*]}"
    # Capture output to count "test ... ok" / ran lines.
    if ! output="$("${cmd[@]}" 2>&1)"; then
        printf '%s\n' "${output}" >&2
        echo "exact leaf failed: ${cmd[*]}" >&2
        exit 1
    fi
    # Cargo prints "running 1 test" for --exact single filters.
    if ! grep -qE 'running 1 test|test result: ok\. 1 passed' <<<"${output}"; then
        # list-only paths may print "1 test, 0 benchmarks" without running.
        if ! grep -qE '^[0-9]+ test(s)?, 0 benchmarks?$' <<<"${output}" \
            && ! grep -qE ': test$' <<<"${output}"; then
            printf '%s\n' "${output}" >&2
            echo "exact leaf did not report ran=1: ${cmd[*]}" >&2
            exit 1
        fi
    fi
    # Reject zero-match / multi-match cargo filters.
    if grep -qE '0 passed|running 0 tests|0 tests,' <<<"${output}"; then
        if ! grep -qE 'running 1 test|1 passed|1 test,' <<<"${output}"; then
            printf '%s\n' "${output}" >&2
            echo "zero-match or ambiguous leaf: ${cmd[*]}" >&2
            exit 1
        fi
    fi
    echo "ran=1 ok: ${cmd[*]}"
}

if [[ "$1" == "--manifest-leaf" ]]; then
    leaf_id="${2:?leaf-id required}"
    mapfile -t inv < <(python3 - "$ROUTE_SOURCES" "$leaf_id" <<'PY'
import json, sys
from pathlib import Path
doc = json.loads(Path(sys.argv[1]).read_text())
leaf_id = sys.argv[2]
for leaf in doc["leaves"]:
    if leaf["leaf_id"] == leaf_id:
        for part in leaf["list_invocation"]:
            print(part)
        raise SystemExit(0)
raise SystemExit(f"unknown p10r leaf {leaf_id}")
PY
)
    run_exact "${inv[@]}"
    exit 0
fi

suite_name="$1"
mapfile -t leaf_jsons < <(python3 - "$MANIFEST" "$suite_name" <<'PY'
import json, sys
from pathlib import Path
manifest = json.loads(Path(sys.argv[1]).read_text())
name = sys.argv[2]
for suite in manifest["product_suites"]:
    if suite["name"] == name:
        for leaf in suite["leaves"]:
            print(json.dumps(leaf))
        raise SystemExit(0)
for suite in manifest.get("storage_diagnostic_suites", []):
    if suite["name"] == name:
        for leaf in suite["leaves"]:
            print(json.dumps(leaf))
        raise SystemExit(0)
raise SystemExit(f"unknown suite {name}")
PY
)

if [[ ${#leaf_jsons[@]} -eq 0 ]]; then
    echo "suite ${suite_name} has zero leaves" >&2
    exit 1
fi

seen=()
for leaf_json in "${leaf_jsons[@]}"; do
    leaf_id=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['leaf_id'])" "${leaf_json}")
    for prev in "${seen[@]+"${seen[@]}"}"; do
        if [[ "${prev}" == "${leaf_id}" ]]; then
            echo "duplicate leaf ${leaf_id} in suite ${suite_name}" >&2
            exit 1
        fi
    done
    seen+=("${leaf_id}")
    mapfile -t cmd < <(python3 -c "
import json, sys
leaf = json.loads(sys.argv[1])
inv = leaf.get('exact_invocation') or leaf.get('list_invocation')
if not inv:
    raise SystemExit('leaf missing invocation')
for part in inv:
    print(part)
" "${leaf_json}")
    run_exact "${cmd[@]}"
done

echo "suite ${suite_name}: ${#seen[@]} exact leaves, each ran=1"
