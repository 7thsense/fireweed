#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
case_root="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-public-release-gate.XXXXXX")"; trap 'rm -rf "$case_root"' EXIT
repo="$case_root/repo"; mkdir -p "$repo/scripts/ci"; git -C "$case_root" init -q repo
git -C "$repo" config user.name Test; git -C "$repo" config user.email test@example.invalid
printf 'fixture\n' >"$repo/README.md"; git -C "$repo" add .; git -C "$repo" commit -qm fixture
cp "$SCRIPT_DIR/public-release-gate.py" "$repo/scripts/ci/"
cat >"$repo/pass.json" <<'JSON'
{"schema_version":1,"version":"test-pass","gates":[{"id":"pass","command":["echo","constituent-output"]}]}
JSON
python3 "$repo/scripts/ci/public-release-gate.py" --repo "$repo" --manifest pass.json --evidence pass-evidence.json
python3 - "$repo/pass-evidence.json" "$(git -C "$repo" rev-parse HEAD)" <<'PY'
import json, sys
evidence=json.load(open(sys.argv[1])); assert evidence["passed"] is True
assert evidence["revision"]==sys.argv[2] and evidence["tool_versions"]["git"]
assert evidence["results"]==[{"command":["echo","constituent-output"],"exit_status":0,"id":"pass"}]
PY
cat >"$repo/fail.json" <<'JSON'
{"schema_version":1,"version":"test-fail","gates":[{"id":"fail","command":["bash","-c","echo useful-failure >&2; exit 7"]},{"id":"must-not-run","command":["true"]}]}
JSON
if python3 "$repo/scripts/ci/public-release-gate.py" --repo "$repo" --manifest fail.json --evidence fail-evidence.json; then echo "failing constituent was accepted" >&2; exit 1; fi
python3 - "$repo/fail-evidence.json" <<'PY'
import json, sys
evidence=json.load(open(sys.argv[1])); assert evidence["passed"] is False
assert evidence["results"][-1]["exit_status"]==7 and len(evidence["results"])==1
PY
echo "public-release-gate-test: PASS"
