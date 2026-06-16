#!/usr/bin/env bash
set -euo pipefail

LEDGER=""
DRY_RUN=0
REQUIRED_TP002=""
TP002_E0E1_SOURCE=""
TP002_E2_SOURCE=""
TP002_E3_SOURCE=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --ledger) LEDGER="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --require-tp002-evidence) REQUIRED_TP002="$2"; shift 2 ;;
        --tp002-e0e1-source) TP002_E0E1_SOURCE="$2"; shift 2 ;;
        --tp002-e2-source) TP002_E2_SOURCE="$2"; shift 2 ;;
        --tp002-e3-source) TP002_E3_SOURCE="$2"; shift 2 ;;
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
    python3 - "$REQUIRED_TP002" "$TP002_E0E1_SOURCE" "$TP002_E2_SOURCE" "$TP002_E3_SOURCE" <<'PY'
import json, pathlib, sys
required=set(sys.argv[1].split(","))
source_groups={
    "E0,E1": sys.argv[2],
    "E2": sys.argv[3],
    "E3": sys.argv[4],
}
seen=set()

beads={}
for line in pathlib.Path(".ddx/beads.jsonl").read_text().splitlines():
    if not line.strip():
        continue
    bead=json.loads(line)
    beads[bead.get("id")]=bead

for group, source_arg in source_groups.items():
    if not source_arg:
        continue
    group_ids=set(group.split(","))
    for bead_id in [part.strip() for part in source_arg.split(",") if part.strip()]:
        bead=beads.get(bead_id)
        if bead is None:
            print(f"TP-002 source bead not found: {bead_id}", file=sys.stderr)
            sys.exit(1)
        if bead.get("status")!="closed":
            print(f"TP-002 source bead is not closed: {bead_id} ({bead.get('status')})", file=sys.stderr)
            sys.exit(1)
        if not bead.get("closing_commit_sha"):
            print(f"TP-002 source bead lacks closing_commit_sha: {bead_id}", file=sys.stderr)
            sys.exit(1)
        source_text=" ".join(str(bead.get(k,"")) for k in ("title","description","acceptance"))
        for evidence_id in group_ids:
            if evidence_id not in source_text and evidence_id not in source_arg:
                print(
                    f"TP-002 source bead {bead_id} does not cite {evidence_id}",
                    file=sys.stderr,
                )
                sys.exit(1)
        seen.update(group_ids)

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
PQUEUE_E2E_SCALE=release cargo +1.92.0 test -p pqueue-service product_validation_tests -- --ignored --nocapture
cargo run -p pqueue-service --bin pqueue-verify-ledger -- --strict --ledger target/pqueue-ledger/product_validation.jsonl >/dev/null
echo "release gate passed"
