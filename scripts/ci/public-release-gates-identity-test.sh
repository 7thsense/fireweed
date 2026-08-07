#!/usr/bin/env bash
# Pairwise gate-set identity test for public-release manifests (P13a).
#
# JSON pointer /version inside scripts/ci/public-release-gates.json and
# /version inside scripts/ci/public-release-gates-ci.json are two independent
# gate-set identities — not paths and not package SemVer. Each is bumped iff
# its governed content changes. Pairwise drift (same version, different gates;
# or different versions with identical gates) fails closed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

python3 - <<'PY'
import copy
import hashlib
import json
from pathlib import Path

root = Path(".")
full_path = root / "scripts/ci/public-release-gates.json"
ci_path = root / "scripts/ci/public-release-gates-ci.json"
full = json.loads(full_path.read_text())
ci = json.loads(ci_path.read_text())


def content_fingerprint(document: dict) -> str:
    """Identity-bearing content excluding the version field itself."""
    body = {key: value for key, value in document.items() if key != "version"}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


require(full.get("schema_version") == 1, "full schema_version")
require(ci.get("schema_version") == 1, "ci schema_version")
require(isinstance(full.get("version"), str) and full["version"], "full /version identity")
require(isinstance(ci.get("version"), str) and ci["version"], "ci /version identity")
require(isinstance(full.get("gates"), list) and full["gates"], "full gates")
require(isinstance(ci.get("gates"), list) and ci["gates"], "ci gates")

full_fp = content_fingerprint(full)
ci_fp = content_fingerprint(ci)

# Independent identities: when gate content differs, versions must differ.
if full_fp != ci_fp:
    require(
        full["version"] != ci["version"],
        "pairwise drift: distinct gate content must not share /version identity "
        f"(full={full['version']!r} ci={ci['version']!r})",
    )
else:
    # Identical content may share a version only if both documents are truly equal.
    require(
        full["version"] == ci["version"],
        "identical gate content must share /version identity",
    )

# Tracked manifests currently intentionally differ (ci is a subset) and must
# keep distinct identities.
require(full_fp != ci_fp, "expected full vs ci gate-set content to differ")
require(full["version"] != ci["version"], "full and ci gate-set /version must be independent")

# Negative fixtures: same version with different gates fails.
def must_fail(label: str, left: dict, right: dict) -> None:
    try:
        if content_fingerprint(left) != content_fingerprint(right):
            assert left["version"] != right["version"]
        else:
            assert left["version"] == right["version"]
    except AssertionError:
        return
    raise AssertionError(f"negative unexpectedly passed: {label}")


drift_left = copy.deepcopy(full)
drift_right = copy.deepcopy(ci)
drift_right["version"] = drift_left["version"]
must_fail("same version different content", drift_left, drift_right)

# Different versions with identical gates also fails.
same_content = copy.deepcopy(full)
twin = copy.deepcopy(full)
twin["version"] = full["version"] + "-alias"
must_fail("different version identical content", same_content, twin)

print(
    "public-release-gates-identity-test: PASS "
    f"(full={full['version']!r} ci={ci['version']!r})"
)
PY
