#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CASE="$(mktemp -d)"; trap 'rm -rf "$CASE"' EXIT
REV=0123456789abcdef0123456789abcdef01234567
mkdir -p "$CASE/bin" "$CASE/bundle/e3"
cat >"$CASE/bin/rustup" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$COMMANDS"
exit 0
SH
chmod +x "$CASE/bin/rustup"
for name in e0 e1 e2-scale; do printf '{}\n' >"$CASE/bundle/$name.jsonl"; done
printf '{"measurements":{"revision":"%s"}}\n' "$REV" >"$CASE/bundle/e2-density.jsonl"
printf '{"source_revision":"%s"}\n' "$REV" >"$CASE/bundle/e2-failover.json"
printf '{}\n' >"$CASE/bundle/e3/e3-contract.json"
python3 - "$CASE/bundle/composite-contract.json" "$REV" <<'PY'
import json,sys
json.dump({"schema_version":1,"source_revision":sys.argv[2],"authorities":{"e0":"e0.jsonl","e1":"e1.jsonl","e2_scale":"e2-scale.jsonl","e2_density":"e2-density.jsonl","e2_failover":"e2-failover.json","e3_contract":"e3/e3-contract.json"}},open(sys.argv[1],"w"))
PY
export COMMANDS="$CASE/commands"
PATH="$CASE/bin:$PATH" bash "$ROOT/scripts/ci/verify-governed-release-composite.sh" --contract "$CASE/bundle/composite-contract.json" --expected-revision "$REV"
for bin in pqueue-verify-e0-e1-evidence pqueue-verify-e2-scale-evidence pqueue-verify-density-evidence pqueue-verify-e2-failover pqueue-verify-e3-contract; do grep -q -- "--bin $bin" "$COMMANDS"; done
if PATH="$CASE/bin:$PATH" bash "$ROOT/scripts/ci/verify-governed-release-composite.sh" --contract "$CASE/bundle/composite-contract.json" --expected-revision ffffffffffffffffffffffffffffffffffffffff; then exit 1; fi
echo "verify-governed-release-composite-test: PASS"
