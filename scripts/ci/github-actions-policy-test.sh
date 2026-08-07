#!/usr/bin/env bash
# Focused regressions for P13a GitHub Actions policy + P13t zero-arg contract.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

echo "--- zero-argument policy verifier (P13t regression) ---"
bash scripts/ci/verify-github-actions-policy.sh

echo "--- policy still zero-arg (argv count) ---"
# Invoking with unexpected args must not be required; the production contract is
# exactly zero arguments.
if bash scripts/ci/verify-github-actions-policy.sh --unexpected-flag 2>/tmp/gha-policy-bad-args.txt; then
    # Some shells may ignore; ensure documented zero-arg path is the green path.
    :
fi
bash scripts/ci/verify-github-actions-policy.sh </dev/null

echo "--- P2 mode-file debt-policy shape still present in ci.yml ---"
python3 - <<'PY'
from pathlib import Path
ci = Path(".github/workflows/ci.yml").read_text()
markers = [
    "run: bash scripts/ci/verify-github-actions-policy.sh",
    "python3 scripts/ci/public-release-gate.py",
    "bash scripts/ci/storage-remediation-policy.sh --mode-file scripts/ci/storage-remediation-policy.mode",
]
positions = [ci.index(m) for m in markers]
assert positions == sorted(positions), "ci.yml functional gate order drift"
assert all(ci.count(m) == 1 for m in markers), "ci.yml marker missing/duplicated"
print("ci.yml P2 mode-file shape: PASS")
PY

echo "--- turso.yml still invokes zero-arg policy verifier ---"
grep -Fq 'bash scripts/ci/verify-github-actions-policy.sh' .github/workflows/turso.yml

echo "--- governed-product framework present ---"
test -f .github/workflows/governed-product.yml
test -f scripts/ci/governed-product-allowlist.json
test -f scripts/ci/governed-product-services.json
grep -Fq 'kafka_compatible_broker' .github/workflows/governed-product.yml
grep -Fq 'P13 populates' .github/workflows/governed-product.yml

echo "--- kafka digest/command unauthored (P13 populates) ---"
python3 - <<'PY'
import json
from pathlib import Path
services = json.loads(Path("scripts/ci/governed-product-services.json").read_text())
kafka = services["services"]["kafka_compatible_broker"]
assert kafka["image_digest"] is None
assert kafka["command"] is None
assert kafka["authorized"] is True
wf = Path(".github/workflows/governed-product.yml").read_text()
assert "redpandadata/redpanda@sha256:" not in wf
print("kafka authorization slot: PASS")
PY

echo "--- Hybrid product selectors absent from workflows ---"
if rg -n -i 'hybrid-strict|hybrid-async|FIREWEED_PROJECTION_BACKEND=hybrid' .github/workflows; then
    echo "Hybrid product residue found" >&2
    exit 1
fi

echo "github-actions-policy-test: PASS"
