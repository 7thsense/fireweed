#!/usr/bin/env bash
# P2r: exact-set equality for product_workflow namespace suite names against the
# generated required-name document. Consumes only generated inputs — no
# hard-coded nine-name array.
#
# Usage:
#   verify-product-workflow-names.sh <suite-list.toml> [required-names.json]
#   verify-product-workflow-names.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REQUIRED="${SCRIPT_DIR}/product-workflow-required-names.json"

if [[ "${1:-}" == "--self-test" ]]; then
    python3 - "$ROOT" "$DEFAULT_REQUIRED" <<'PY'
import copy
import json
import sys
import tempfile
from pathlib import Path

root = Path(sys.argv[1])
required_path = Path(sys.argv[2])
required_doc = json.loads(required_path.read_text())
required = list(required_doc["names"])
namespace = required_doc["namespace"]
assert namespace == "product_workflow"
assert "product_validation_tests" in required
assert len(required) == 10

# Import verifier logic by executing the same comparison the script uses.

def check(suite_rows, required_names, namespace):
    product = [s["name"] for s in suite_rows if s.get("namespace") == namespace]
    if len(product) != len(set(product)):
        raise AssertionError("duplicate product-workflow suite names")
    actual = set(product)
    expect = set(required_names)
    if actual != expect:
        missing = sorted(expect - actual)
        extra = sorted(actual - expect)
        raise AssertionError(f"exact-set mismatch missing={missing} extra={extra}")

# Positive: exact set from generated suites.
suites_path = root / "scripts/ci/release-repeat-suites.toml"
import tomllib
suites = tomllib.loads(suites_path.read_text())["suites"]
check(suites, required, namespace)

# Legitimate non-product suite is not rejected (already present as storage diagnostic).
assert any(s.get("namespace") != namespace for s in suites)

# Missing name fails.
broken = [s for s in suites if not (s.get("namespace") == namespace and s["name"] == required[0])]
try:
    check(broken, required, namespace)
except AssertionError:
    pass
else:
    raise SystemExit("missing-name negative unexpectedly passed")

# Duplicate product name fails.
dup = copy.deepcopy([s for s in suites if s.get("namespace") == namespace])
dup.append(copy.deepcopy(dup[0]))
try:
    check(dup, required, namespace)
except AssertionError:
    pass
else:
    raise SystemExit("duplicate negative unexpectedly passed")

# Unauthorized extra product-namespace name fails.
extra = copy.deepcopy([s for s in suites if s.get("namespace") == namespace])
extra.append(
    {
        "name": "product_workflow_unauthorized_extra",
        "namespace": namespace,
        "kind": "product_workflow",
        "command": ["true"],
    }
)
try:
    check(extra, required, namespace)
except AssertionError:
    pass
else:
    raise SystemExit("unauthorized-extra negative unexpectedly passed")

# Nine-name subset fails.
subset = [s for s in suites if s.get("namespace") == namespace][:9]
try:
    check(subset, required, namespace)
except AssertionError:
    pass
else:
    raise SystemExit("nine-name subset negative unexpectedly passed")

# Non-prefixed required member included.
assert "product_validation_tests" in {
    s["name"] for s in suites if s.get("namespace") == namespace
}

print("verify-product-workflow-names self-test passed")
PY
    exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: verify-product-workflow-names.sh <suite-list.toml> [required-names.json]" >&2
    echo "       verify-product-workflow-names.sh --self-test" >&2
    exit 2
fi

SUITE_LIST="$1"
REQUIRED_JSON="${2:-$DEFAULT_REQUIRED}"

python3 - "$SUITE_LIST" "$REQUIRED_JSON" <<'PY'
import json
import sys
import tomllib
from pathlib import Path

suite_path = Path(sys.argv[1])
required_path = Path(sys.argv[2])
if not suite_path.is_file():
    print(f"suite list missing: {suite_path}", file=sys.stderr)
    sys.exit(1)
if not required_path.is_file():
    print(f"required-names document missing: {required_path}", file=sys.stderr)
    sys.exit(1)

required_doc = json.loads(required_path.read_text())
required_names = required_doc["names"]
namespace = required_doc["namespace"]
if required_doc.get("verifier_semantics") != "exact_set_product_workflow_namespace":
    print("required-names document has wrong verifier_semantics", file=sys.stderr)
    sys.exit(1)

data = tomllib.loads(suite_path.read_text())
suites = data.get("suites", [])
product_names = [s.get("name") for s in suites if s.get("namespace") == namespace]

if len(product_names) != len(set(product_names)):
    print("duplicate suite names in product-workflow namespace", file=sys.stderr)
    sys.exit(1)

actual = set(product_names)
expect = set(required_names)
if actual != expect:
    missing = sorted(expect - actual)
    extra = sorted(actual - expect)
    if missing:
        print("missing suite names: " + ", ".join(missing), file=sys.stderr)
    if extra:
        print("unauthorized extra product-workflow suite names: " + ", ".join(extra), file=sys.stderr)
    sys.exit(1)

# Non-prefixed member must be present (part of exact set; explicit positive).
if "product_validation_tests" not in actual:
    print("missing non-prefixed required member product_validation_tests", file=sys.stderr)
    sys.exit(1)

print("product workflow suite names verified (exact set, product_workflow namespace)")
PY
