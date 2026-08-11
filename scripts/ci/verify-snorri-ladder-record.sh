#!/usr/bin/env bash
# Validate docs/perf/evidence/tp005/snorri-ladder-candidate.json RESULT block.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
json="${1:-$repo_root/docs/perf/evidence/tp005/snorri-ladder-candidate.json}"
python3 - "$json" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as f:
    data = json.load(f)
cand = data.get("candidate_rev")
res = data.get("RESULT") or {}
status = res.get("status")
if status == "pending" or res.get("verdict") in (None, "pending"):
    print(f"pending record: RESULT not filled in {path}", file=sys.stderr)
    sys.exit(2)
if res.get("measured_rev") != cand:
    print(
        f"revision mismatch: measured_rev={res.get('measured_rev')!r} candidate_rev={cand!r}",
        file=sys.stderr,
    )
    sys.exit(3)
if res.get("delivery_assertions") != "green":
    print(f"delivery-assertion failure: {res.get('delivery_assertions')!r}", file=sys.stderr)
    sys.exit(4)
tps = res.get("tps_w8")
minimum = (data.get("pass_condition") or {}).get("tps_w8_min", 3692)
if tps is None or float(tps) < float(minimum):
    print(f"tps below baseline: tps_w8={tps} min={minimum}", file=sys.stderr)
    sys.exit(5)
print(f"ok: snorri ladder PASS tps_w8={tps} rev={cand}")
sys.exit(0)
PY
