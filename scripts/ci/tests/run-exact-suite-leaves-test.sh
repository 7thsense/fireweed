#!/usr/bin/env bash
# Exercise the actual runner with a fake tool; no Rust build or workload is started.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUNNER="${ROOT}/scripts/ci/run-exact-suite-leaves.sh"
CASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-exact-suite-test.XXXXXX")"
trap 'rm -rf "$CASE_DIR"' EXIT
mkdir -p "$CASE_DIR/bin"
cat >"$CASE_DIR/bin/rustup" <<'FAKE'
#!/usr/bin/env bash
cat "$EXACT_SUITE_FIXTURE"
exit "${EXACT_SUITE_EXIT:-0}"
FAKE
chmod +x "$CASE_DIR/bin/rustup"
LEAF=$(python3 - "$ROOT/docs/helix/04-build/functional-matrix-route-sources.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["leaves"][0]["leaf_id"])
PY
)

check_case() {
    local name=$1 mode=$2 expected=$3 output=$4 tool_exit=${5:-0}
    local -a args=(product_workflow_scheduled_action_delivery_e2e)
    [[ "$mode" == list ]] && args=(--manifest-leaf "$LEAF")
    printf '%s\n' "$output" >"$CASE_DIR/output"
    local status=0
    PATH="$CASE_DIR/bin:$PATH" EXACT_SUITE_FIXTURE="$CASE_DIR/output" EXACT_SUITE_EXIT="$tool_exit" \
        bash "$RUNNER" "${args[@]}" >"$CASE_DIR/$name.log" 2>&1 || status=$?
    if { [[ "$expected" == pass ]] && (( status != 0 )); } ||
       { [[ "$expected" == fail ]] && (( status == 0 )); }; then
        cat "$CASE_DIR/$name.log" >&2
        echo "case $name expected $expected, got exit $status" >&2
        exit 1
    fi
}

run_one=$'running 1 test\ntest selected ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.00s'
run_zero=$'running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.00s'
run_two=$'running 2 tests\ntest first ... ok\ntest second ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s'
list_one=$'selected: test\n\n1 test, 0 benchmarks'
check_case run-one run pass "$run_one"
check_case run-one-plus-empty-harness run pass "$run_one"$'\n'"$run_zero"
check_case run-zero run fail "$run_zero"
check_case run-two run fail "$run_two"
check_case run-two-harnesses run fail "$run_one"$'\n'"$run_one"
check_case run-ignored run fail $'running 1 test\ntest result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s'
check_case run-rejects-list run fail "$list_one"
check_case list-one list pass "$list_one"
check_case list-one-plus-empty-harness list pass "$list_one"$'\n0 tests, 0 benchmarks'
check_case list-zero list fail '0 tests, 0 benchmarks'
check_case list-two list fail $'first: test\nsecond: test\n\n2 tests, 0 benchmarks'
check_case list-inconsistent-summary list fail $'first: test\nsecond: test\n\n1 test, 0 benchmarks'
check_case list-two-harnesses list fail "$list_one"$'\n'"$list_one"
check_case list-rejects-execution list fail "$run_one"
check_case child-failure run fail "$run_one" 9
printf '%s\n' 'run-exact-suite-leaves contract tests passed'
