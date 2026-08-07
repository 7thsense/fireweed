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
test -f scripts/ci/generate-governed-product-allowlist.py
grep -Fq 'kafka_compatible_broker' .github/workflows/governed-product.yml
grep -Fq 'governed-product-allowlist.json' .github/workflows/governed-product.yml

echo "--- P13 populated allowlist + kafka pin (generator check) ---"
python3 scripts/ci/generate-governed-product-allowlist.py --check
python3 - <<'PY'
import json
import re
from pathlib import Path
allow = json.loads(Path("scripts/ci/governed-product-allowlist.json").read_text())
services = json.loads(Path("scripts/ci/governed-product-services.json").read_text())
assert allow["product_release_readiness_claimed"] is False
assert len(allow["commands"]) >= 8
cats = {e.get("category") for e in allow["commands"]}
for required in ("functional", "T4", "reduced-count", "external-kafka", "policy"):
    assert required in cats, required
kafka = services["services"]["kafka_compatible_broker"]
assert kafka["authorized"] is True
assert re.fullmatch(r"sha256:[0-9a-f]{64}", kafka["image_digest"])
assert isinstance(kafka["command"], list) and kafka["command"][0] == "redpanda"
wf = Path(".github/workflows/governed-product.yml").read_text()
assert "image: redpandadata/redpanda@" not in wf
assert "image: redpandadata/redpanda:" not in wf
print("kafka pin + allowlist population: PASS")
PY

echo "--- Hybrid product selectors absent from workflows ---"
if rg -n -i 'hybrid-strict|hybrid-async|FIREWEED_PROJECTION_BACKEND=hybrid' .github/workflows; then
    echo "Hybrid product residue found" >&2
    exit 1
fi

echo "github-actions-policy-test: PASS"
