#!/usr/bin/env bash
# Source-of-truth semantic dispatch for the TP-002 composite release contract.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
contract="" revision=""
while (($#)); do
  case "$1" in
    --contract) contract="$2"; shift 2 ;;
    --expected-revision) revision="$2"; shift 2 ;;
    *) exit 64 ;;
  esac
done
[[ -f "$contract" && "$revision" =~ ^[0-9a-f]{40}$ ]] || exit 64
mapfile -t fields < <(python3 - "$contract" "$revision" <<'PY'
import json, os, sys

def unique_object(pairs):
  value = {}
  for key, item in pairs:
    if key in value:
      raise ValueError(f"duplicate JSON key: {key}")
    value[key] = item
  return value

p, expected = sys.argv[1:]
with open(p, encoding="utf-8") as f:
  try: c=json.load(f, object_pairs_hook=unique_object)
  except (json.JSONDecodeError, ValueError) as error: raise SystemExit(f"invalid composite JSON: {error}")
if set(c) != {"schema_version","source_revision","authorities"} or c["schema_version"] != 1 or c["source_revision"] != expected: raise SystemExit("invalid composite identity")
keys=("e0","e1","e2_scale","e2_density","e2_failover","e3_contract")
if set(c["authorities"]) != set(keys): raise SystemExit("invalid composite authority set")
base=os.path.realpath(os.path.dirname(p))
for k in keys:
  rel=c["authorities"][k]
  if os.path.isabs(rel) or ".." in rel.split(os.sep): raise SystemExit("unsafe authority path")
  full=os.path.realpath(os.path.join(base,rel))
  if os.path.commonpath((base,full)) != base or not os.path.isfile(full) or os.path.islink(os.path.join(base,rel)): raise SystemExit("invalid authority file")
  print(full)
PY
)
[[ ${#fields[@]} -eq 6 ]] || exit 1
CARGO=(rustup run 1.92.0 cargo)
"${CARGO[@]}" run -q -p fireweed-release --bin fireweed-verify-e0-e1-evidence -- --e0 "${fields[0]}" --e1 "${fields[1]}" --expected-revision "$revision"
"${CARGO[@]}" run -q -p fireweed-release --bin fireweed-verify-e2-scale-evidence -- "${fields[2]}" --expected-revision "$revision"
"${CARGO[@]}" run -q -p fireweed-release --bin fireweed-verify-density-evidence -- "${fields[3]}"
python3 - "${fields[3]}" "${fields[4]}" "$revision" <<'PY'
import json, sys

with open(sys.argv[1], encoding="utf-8") as source:
  density_rows=[json.loads(line) for line in source if line.strip()]
for index, row in enumerate(density_rows, start=1):
  if row.get("measurements",{}).get("revision") != sys.argv[3]:
    raise SystemExit(f"E2 density row {index} revision mismatch")
if len(density_rows) != 1:
  raise SystemExit(f"E2 density authority must contain exactly one row, found {len(density_rows)}")
d=density_rows[0]
f=json.load(open(sys.argv[2], encoding="utf-8"))
if f.get("source_revision") != sys.argv[3]: raise SystemExit("E2 failover revision mismatch")
PY
"${CARGO[@]}" run -q -p fireweed-release --bin fireweed-verify-e2-failover -- "${fields[4]}"
"${CARGO[@]}" run -q -p fireweed-release --bin fireweed-verify-e3-contract -- --manifest "${fields[5]}" --expected-revision "$revision"
echo "governed release composite valid for $revision"
