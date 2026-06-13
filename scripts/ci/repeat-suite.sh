#!/usr/bin/env bash
# Repeat suite harness for flaky-rate measurement (TP-003 §5 release gate).
#
# Usage:
#   repeat-suite.sh [--count N] [--max-flaky-rate R] -- CMD [ARGS...]
#   repeat-suite.sh [--count N] [--max-flaky-rate R] --suite-list FILE.toml
#
# Options:
#   --count N             Number of repeats per suite (default: 10).
#   --max-flaky-rate R    Max allowed flaky rate as a decimal (default: 1.0).
#                         flaky_rate = total_failures / total_runs.
#                         Exit nonzero when flaky_rate > R.
#   --suite-list FILE     TOML file listing suites (mutually exclusive with --).
#   -- CMD [ARGS...]      Command to repeat (mutually exclusive with --suite-list).
#
# TP-003 §5 release bar: < 0.1% over 100 repeats.
# At 100 repeats, 0.1% == 0.001; any single failure yields 1.0% and fails.
# Use --max-flaky-rate 0.000999 to enforce the strict TP-003 gate.
#
# Report (from repeat-suite-report.py) includes:
#   run_count, failures, flaky_rate, failing_selectors.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COUNT=10
MAX_FLAKY_RATE="1.0"
SUITE_LIST=""
CMD_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --count)           COUNT="$2";           shift 2 ;;
        --max-flaky-rate)  MAX_FLAKY_RATE="$2";  shift 2 ;;
        --suite-list)      SUITE_LIST="$2";       shift 2 ;;
        --)                shift; CMD_ARGS=("$@"); break ;;
        *) echo "Unknown argument: $1" >&2
           echo "Usage: $(basename "$0") [--count N] [--max-flaky-rate R] (--suite-list FILE | -- CMD [ARGS...])" >&2
           exit 1 ;;
    esac
done

if [[ -z "$SUITE_LIST" && ${#CMD_ARGS[@]} -eq 0 ]]; then
    echo "Error: specify --suite-list FILE or -- CMD [ARGS...]" >&2
    echo "Usage: $(basename "$0") [--count N] [--max-flaky-rate R] (--suite-list FILE | -- CMD [ARGS...])" >&2
    exit 1
fi

if [[ -n "$SUITE_LIST" && ${#CMD_ARGS[@]} -gt 0 ]]; then
    echo "Error: --suite-list and -- CMD are mutually exclusive" >&2
    exit 1
fi

RESULTS_FILE=$(mktemp)
trap 'rm -f "$RESULTS_FILE"' EXIT

echo "=== repeat-suite harness ==="
echo "  count         : $COUNT"
echo "  max_flaky_rate: $MAX_FLAKY_RATE"
if [[ -n "$SUITE_LIST" ]]; then
    echo "  suite_list    : $SUITE_LIST"
else
    echo "  command       : ${CMD_ARGS[*]}"
fi
echo ""

# Run CMD N times; record TSV results: suite_name<TAB>pass|fail
run_suite_n() {
    local name="$1"; shift
    local -a cmd=("$@")

    echo "--- suite: $name ---"
    local i
    for ((i = 1; i <= COUNT; i++)); do
        if "${cmd[@]}" >/dev/null 2>&1; then
            printf '%s\tpass\n' "$name" >> "$RESULTS_FILE"
        else
            printf '  run %d/%d: FAIL\n' "$i" "$COUNT" >&2
            printf '%s\tfail\n' "$name" >> "$RESULTS_FILE"
        fi
    done
}

if [[ -n "$SUITE_LIST" ]]; then
    # Parse TOML suite list via report script; each line is one JSON object.
    SUITE_JSON_FILE=$(mktemp)
    trap 'rm -f "$RESULTS_FILE" "$SUITE_JSON_FILE"' EXIT

    python3 "${SCRIPT_DIR}/repeat-suite-report.py" --parse-suites "$SUITE_LIST" \
        > "$SUITE_JSON_FILE"

    while IFS= read -r suite_json; do
        [[ -z "$suite_json" ]] && continue
        suite_name=$(python3 -c \
            "import json,sys; print(json.loads(sys.argv[1])['name'])" \
            "$suite_json")
        mapfile -t suite_cmd < <(python3 -c "
import json, sys
for a in json.loads(sys.argv[1])['command']:
    print(a)
" "$suite_json")
        run_suite_n "$suite_name" "${suite_cmd[@]}"
    done < "$SUITE_JSON_FILE"
    rm -f "$SUITE_JSON_FILE"
else
    run_suite_n "${CMD_ARGS[*]}" "${CMD_ARGS[@]}"
fi

echo ""

# Generate report and apply flaky-rate gate (exits nonzero on failure).
python3 "${SCRIPT_DIR}/repeat-suite-report.py" \
    --report "$RESULTS_FILE" \
    --max-flaky-rate "$MAX_FLAKY_RATE"
