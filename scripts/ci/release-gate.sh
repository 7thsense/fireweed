#!/usr/bin/env bash
set -euo pipefail

LEDGER=""
DRY_RUN=0
REQUIRED_TP002=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --ledger) LEDGER="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --require-tp002-evidence) REQUIRED_TP002="$2"; shift 2 ;;
        --tp002-e0e1-source|--tp002-e2-source|--tp002-e3-source) shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [[ -n "$LEDGER" ]]; then
    python3 - "$LEDGER" <<'PY'
import json, sys
required={"ac_ids","inv_ids","command","exit_status","backend_profile","scale","seed","environment","suite","measurements","pass_bar"}
rows=[]
for line in open(sys.argv[1]):
    line=line.strip()
    if line:
        rows.append(json.loads(line))
if not rows:
    print("ledger is empty", file=sys.stderr)
    sys.exit(1)
for idx,row in enumerate(rows,1):
    missing=required-set(row)
    if missing:
        print(f"line {idx}: missing required fields: {', '.join(sorted(missing))}", file=sys.stderr)
        sys.exit(1)
print(f"validated {len(rows)} release ledger row(s)")
PY
    [[ "$DRY_RUN" -eq 1 ]] && exit 0
fi

if [[ -n "$REQUIRED_TP002" ]]; then
    python3 - "$REQUIRED_TP002" <<'PY'
import json, pathlib, sys
required=set(sys.argv[1].split(","))
seen=set()
for path in pathlib.Path("target/pqueue-ledger").glob("*.jsonl"):
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        row=json.loads(line)
        ids=row.get("measurements",{}).get("tp002_evidence_ids",[])
        seen.update(ids)
missing=required-seen
if missing:
    print("missing TP-002 evidence ids: " + ", ".join(sorted(missing)), file=sys.stderr)
    sys.exit(1)
print("TP-002 evidence present: " + ", ".join(sorted(required)))
PY
fi

echo "--- live coverage gate ---"
mkdir -p target/coverage
cargo +1.92.0 llvm-cov --package pqueue-core --lcov --output-path target/coverage/pqueue-core.lcov
bash "${SCRIPT_DIR}/check-lcov-coverage.py" --lcov target/coverage/pqueue-core.lcov --crate pqueue-core --min-lines 90
cargo +nightly llvm-cov --package pqueue-core --branch --lcov --output-path target/coverage/pqueue-core-branch.lcov
bash "${SCRIPT_DIR}/check-lcov-coverage.py" --lcov target/coverage/pqueue-core-branch.lcov --crate pqueue-core --min-lines 90 --min-branches 85
cargo +1.92.0 llvm-cov --package pqueue-service --lcov --output-path target/coverage/pqueue-service.lcov
bash "${SCRIPT_DIR}/check-lcov-coverage.py" --lcov target/coverage/pqueue-service.lcov --crate pqueue-service --min-lines 80

bash scripts/ci/verify-build-closure.sh --aggregate pqueue-fa406e7d
cargo run -p pqueue-service --bin pqueue-verify-ledger -- --strict --ledger target/pqueue-ledger/product_validation.jsonl >/dev/null
echo "release gate passed"
