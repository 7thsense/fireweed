#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CASE="$(mktemp -d)"; trap 'rm -rf "$CASE"' EXIT
REV=0123456789abcdef0123456789abcdef01234567

fail() {
  echo "verify-governed-release-composite-test: $*" >&2
  exit 1
}

expect_failure() {
  local label="$1"
  shift
  if "$@" >"$CASE/$label.out" 2>&1; then
    fail "$label unexpectedly passed"
  fi
}

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
for bin in fireweed-verify-e0-e1-evidence fireweed-verify-e2-scale-evidence fireweed-verify-density-evidence fireweed-verify-e2-failover fireweed-verify-e3-contract; do
  grep -q -- "--bin $bin" "$COMMANDS" || fail "composite did not dispatch $bin"
done

expect_failure wrong_revision env PATH="$CASE/bin:$PATH" \
  bash "$ROOT/scripts/ci/verify-governed-release-composite.sh" \
  --contract "$CASE/bundle/composite-contract.json" \
  --expected-revision ffffffffffffffffffffffffffffffffffffffff

duplicate_density="$CASE/duplicate-density"
cp -R "$CASE/bundle" "$duplicate_density"
cat "$CASE/bundle/e2-density.jsonl" >>"$duplicate_density/e2-density.jsonl"
expect_failure duplicate_density env PATH="$CASE/bin:$PATH" \
  bash "$ROOT/scripts/ci/verify-governed-release-composite.sh" \
  --contract "$duplicate_density/composite-contract.json" --expected-revision "$REV"
grep -Fq 'must contain exactly one row, found 2' "$CASE/duplicate_density.out" ||
  fail "duplicate density rows did not produce the fail-closed diagnostic"

later_revision="$CASE/later-density-revision"
cp -R "$CASE/bundle" "$later_revision"
printf '%s\n' '{"measurements":{"revision":"ffffffffffffffffffffffffffffffffffffffff"}}' \
  >>"$later_revision/e2-density.jsonl"
expect_failure later_density_revision env PATH="$CASE/bin:$PATH" \
  bash "$ROOT/scripts/ci/verify-governed-release-composite.sh" \
  --contract "$later_revision/composite-contract.json" --expected-revision "$REV"
grep -Fq 'E2 density row 2 revision mismatch' "$CASE/later_density_revision.out" ||
  fail "later mismatched density row did not produce the fail-closed diagnostic"

duplicate_key="$CASE/duplicate-contract-key"
cp -R "$CASE/bundle" "$duplicate_key"
python3 - "$duplicate_key/composite-contract.json" <<'PY'
import sys

path = sys.argv[1]
body = open(path, encoding="utf-8").read()
body = body.replace('"e0": "e0.jsonl",', '"e0": "e0.jsonl", "e0": "unlisted-e0.jsonl",')
open(path, "w", encoding="utf-8").write(body)
PY
printf '{}\n' >"$duplicate_key/unlisted-e0.jsonl"
expect_failure duplicate_contract_key env PATH="$CASE/bin:$PATH" \
  bash "$ROOT/scripts/ci/verify-governed-release-composite.sh" \
  --contract "$duplicate_key/composite-contract.json" --expected-revision "$REV"
grep -Fq 'duplicate JSON key: e0' "$CASE/duplicate_contract_key.out" ||
  fail "duplicate composite authority key did not produce the fail-closed diagnostic"

# The tag gate consumes this composite, so exercise its exact authority set rather
# than the retired four-row manifest fixture. Every governed E0/E1/E2/E3 input
# must fail closed even when an unlisted lookalike file remains beside the contract.
for authority in e0 e1 e2_scale e2_density e2_failover e3_contract; do
  missing="$CASE/missing-$authority"
  cp -R "$CASE/bundle" "$missing"
  printf '{}\n' >"$missing/unlisted-$authority.jsonl"
  AUTHORITY="$authority" CONTRACT="$missing/composite-contract.json" python3 - <<'PY'
import json
import os

path = os.environ["CONTRACT"]
with open(path, encoding="utf-8") as source:
    contract = json.load(source)
del contract["authorities"][os.environ["AUTHORITY"]]
with open(path, "w", encoding="utf-8") as destination:
    json.dump(contract, destination)
PY
  expect_failure "missing-$authority" env PATH="$CASE/bin:$PATH" \
    bash "$ROOT/scripts/ci/verify-governed-release-composite.sh" \
    --contract "$missing/composite-contract.json" --expected-revision "$REV"
done

# Keep the cheap fixture tied to the actual tag workflow and local release gate.
# This catches a future regression back to smoke-only validation without running
# the heavyweight release suite.
release_gate="$ROOT/scripts/ci/release-gate.sh"
release_workflow="$ROOT/.github/workflows/release.yml"
grep -Fq -- '--require-smoke-evidence E2,E3' "$release_gate" ||
  fail "release gate does not require fresh E2/E3 smoke evidence"
grep -Fq -- 'verify-governed-release-composite.sh' "$release_gate" ||
  fail "release gate does not dispatch the governed composite verifier"
grep -Fq -- '--contract target/tp002-release/composite-contract.json' "$release_workflow" ||
  fail "release workflow does not name the governed composite contract"
grep -Fq -- '--expected-revision "$GITHUB_SHA"' "$release_workflow" ||
  fail "release workflow does not bind the composite to the checked-out revision"
grep -Fq -- '--manifest target/tp002-release/attestation.json' "$release_workflow" ||
  fail "release workflow does not verify the acquired attestation"
grep -Fq -- '--evidence-root target/tp002-release' "$release_workflow" ||
  fail "release workflow does not bind attestation evidence to the promoted bundle root"
# shellcheck disable=SC2016 # Literal GitHub expression under test.
grep -Fq -- '--tag "${{ steps.release.outputs.tag }}"' "$release_workflow" ||
  fail "release workflow does not bind evidence to the resolved release tag"
grep -Fq -- '--commit "${GITHUB_SHA}"' "$release_workflow" ||
  fail "release workflow does not bind evidence to the checked-out commit"

echo "verify-governed-release-composite-test: PASS"
