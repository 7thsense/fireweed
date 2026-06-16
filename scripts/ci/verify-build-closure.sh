#!/usr/bin/env bash
set -euo pipefail

FIXTURE=""
AGGREGATE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --fixture) FIXTURE="$2"; shift 2 ;;
        --aggregate) AGGREGATE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$AGGREGATE" ]]; then
    echo "--aggregate is required" >&2
    exit 2
fi

if [[ -n "$FIXTURE" ]]; then
    python3 - "$FIXTURE" <<'PY'
import json, sys
data=json.load(open(sys.argv[1]))
open_items=[i["id"] for i in data.get("required", []) if i.get("status")!="closed"]
if open_items:
    print("open required beads: " + ", ".join(open_items), file=sys.stderr)
    sys.exit(1)
print("fixture closure verified")
PY
    exit 0
fi

python3 - "$AGGREGATE" <<'PY'
import json, pathlib, sys
aggregate=sys.argv[1]
items=[]
for line in pathlib.Path(".ddx/beads.jsonl").read_text().splitlines():
    if line.strip():
        items.append(json.loads(line))
by_id={i["id"]:i for i in items}
agg=by_id.get(aggregate)
if not agg:
    print(f"{aggregate}: not found", file=sys.stderr)
    sys.exit(1)
if agg.get("status")!="closed":
    print(f"{aggregate}: aggregate is not closed", file=sys.stderr)
    sys.exit(1)
open_deps=[]
missing_evidence=[]
for dep in agg.get("dependencies", []):
    dep_id=dep["depends_on_id"]
    item=by_id.get(dep_id)
    if not item or item.get("status")!="closed":
        open_deps.append(dep_id)
    elif not item.get("closing_commit_sha"):
        missing_evidence.append(dep_id)
if open_deps:
    print("open required dependency beads: " + ", ".join(open_deps), file=sys.stderr)
    sys.exit(1)
if missing_evidence:
    print("closed dependency beads without closing_commit_sha: " + ", ".join(missing_evidence), file=sys.stderr)
    sys.exit(1)
if not agg.get("closing_commit_sha"):
    print(f"{aggregate}: aggregate lacks closing_commit_sha", file=sys.stderr)
    sys.exit(1)
print(f"{aggregate}: live closure verified")
PY
