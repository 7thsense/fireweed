#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
v="$repo_root/scripts/ci/verify-snorri-ladder-record.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

base="$repo_root/docs/perf/evidence/tp005/snorri-ladder-candidate.json"
cp "$base" "$tmp/pending.json"
# pending must fail
if bash "$v" "$tmp/pending.json"; then echo "expected pending fail"; exit 1; fi
code=0
bash "$v" "$tmp/pending.json" || code=$?
test "$code" -eq 2

python3 - <<PY
import json, pathlib
p = pathlib.Path("$tmp/ok.json")
d = json.loads(pathlib.Path("$base").read_text())
d["candidate_rev"] = "abc123"
d["RESULT"] = {
  "status": "filled",
  "measured_rev": "abc123",
  "measured_at": "2026-08-11T00:00:00Z",
  "tps_w1": 2100,
  "tps_w4": 3400,
  "tps_w8": 3700,
  "durable_queue_commit_seconds": 36.0,
  "delivery_assertions": "green",
  "verdict": "pass",
}
p.write_text(json.dumps(d))
# mismatch
d2 = json.loads(p.read_text())
d2["RESULT"]["measured_rev"] = "zzz"
pathlib.Path("$tmp/mismatch.json").write_text(json.dumps(d2))
# red delivery
d3 = json.loads(p.read_text())
d3["RESULT"]["delivery_assertions"] = "red"
pathlib.Path("$tmp/red.json").write_text(json.dumps(d3))
# low tps
d4 = json.loads(p.read_text())
d4["RESULT"]["tps_w8"] = 100
pathlib.Path("$tmp/low.json").write_text(json.dumps(d4))
PY

bash "$v" "$tmp/ok.json"
code=0; bash "$v" "$tmp/mismatch.json" || code=$?; test "$code" -eq 3
code=0; bash "$v" "$tmp/red.json" || code=$?; test "$code" -eq 4
code=0; bash "$v" "$tmp/low.json" || code=$?; test "$code" -eq 5
echo "verify-snorri-ladder-record-test: PASS"
