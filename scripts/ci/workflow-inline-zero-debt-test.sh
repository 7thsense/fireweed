#!/usr/bin/env bash
# P10w: prove every workflow-inline early-success/skip/no-op is an executed
# policy-positive (or would require exclusive-owner residual debt), regenerate
# the P2 workflow_inline registry classification, and emit the zero-debt report
# for P2f. This script never edits workflow files.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

REPORT_PATH="${WORKFLOW_INLINE_ZERO_DEBT_REPORT:-scripts/ci/workflow-inline-zero-debt-report.json}"
WRITE_REPORT=1
if [[ "${1:-}" == "--check" ]]; then
  WRITE_REPORT=0
fi

echo "--- P10w workflow-inline zero-debt classification ---"
python3 - "$REPORT_PATH" "$WRITE_REPORT" <<'PY'
from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(".").resolve()
REPORT_PATH = Path(sys.argv[1])
WRITE_REPORT = sys.argv[2] == "1"

# Inventory generator imports sibling modules under scripts/ci/.
sys.path.insert(0, str(ROOT / "scripts/ci"))

# Load hyphenated inventory generator module (not a package import).
_spec = importlib.util.spec_from_file_location(
    "inventory_storage_remediation",
    ROOT / "scripts/ci/inventory-storage-remediation.py",
)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
WORKFLOW_INLINE_POLICY_POSITIVES = _mod.WORKFLOW_INLINE_POLICY_POSITIVES
classify_workflow_inline = _mod.classify_workflow_inline

SCAN_RE = re.compile(
    r"continue-on-error:|\|\|\s*true|exit\s+0|if:.*false|skip",
    re.IGNORECASE,
)


def tracked_workflows() -> list[str]:
    raw = subprocess.check_output(
        ["git", "ls-files", "-z", "--", ".github/workflows"],
        cwd=ROOT,
    )
    return sorted(
        path for path in raw.decode().split("\0") if path.endswith((".yml", ".yaml"))
    )


def scan_hits() -> list[dict[str, object]]:
    hits: list[dict[str, object]] = []
    for path in tracked_workflows():
        text = (ROOT / path).read_text(encoding="utf-8")
        lines = text.splitlines()
        for index, line_text in enumerate(lines, start=1):
            if not SCAN_RE.search(line_text):
                continue
            surrounding = "\n".join(lines[max(0, index - 12) : min(len(lines), index + 12)])
            policy = classify_workflow_inline(path, line_text, surrounding)
            hits.append(
                {
                    "path": path,
                    "line": index,
                    "identity": line_text.strip()[:120],
                    "policy_positive": policy is not None,
                    "reason": None if policy is None else policy["reason"],
                    "detail": None if policy is None else policy["detail"],
                    "exclusive_owner": None if policy is None else policy["exclusive_owner"],
                    "exact_route": None if policy is None else policy["exact_route"],
                }
            )
    return hits


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"workflow-inline-zero-debt: {message}")


hits = scan_hits()
require(hits, "scanner found zero workflow-inline candidates; allowlist drift?")

observed_reasons = [h["reason"] for h in hits if h["policy_positive"]]
expected_reasons = [str(e["reason"]) for e in WORKFLOW_INLINE_POLICY_POSITIVES]
require(
    sorted(observed_reasons) == sorted(expected_reasons),
    "policy-positive observation set drift: "
    f"observed={sorted(observed_reasons)!r} expected={sorted(expected_reasons)!r}",
)

require(
    any(h["reason"] == "pages_readiness_success_branch" for h in hits),
    "missing Pages readiness success branch (exit 0 under Wait for Pages)",
)

residuals = [h for h in hits if not h["policy_positive"]]
require(
    not residuals,
    "residual workflow-inline debt (exclusive owner must remediate with exact route): "
    + "; ".join(f"{r['path']}:{r['line']}:{r['identity']}" for r in residuals),
)

completed = subprocess.run(
    [sys.executable, "scripts/ci/inventory-storage-remediation.py", "--emit"],
    cwd=ROOT,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
require(completed.returncode == 0, f"inventory emit failed: {completed.stderr}")
document = json.loads(completed.stdout)
rows = document["debt_registries"]["workflow_inline"]
require(rows, "inventory workflow_inline empty after regeneration")
debt_rows = [row for row in rows if row["status"] != "discovery_negative"]
require(
    not debt_rows,
    "inventory workflow_inline still has debt: "
    + "; ".join(f"{r['path']}:{r['line']}" for r in debt_rows),
)
for row in rows:
    require(
        str(row.get("detail", "")).startswith("policy_positive:"),
        f"row {row['id']} missing policy_positive detail",
    )

checked_in = json.loads((ROOT / "scripts/ci/storage-remediation-inventory.json").read_text())
ci_rows = checked_in["debt_registries"]["workflow_inline"]
ci_debt = [row for row in ci_rows if row["status"] != "discovery_negative"]

if not WRITE_REPORT:
    require(not ci_debt, "checked-in inventory has workflow_inline debt; regenerate")
    require(
        [(r["path"], r["line"], r["status"], r["detail"]) for r in ci_rows]
        == [(r["path"], r["line"], r["status"], r["detail"]) for r in rows],
        "checked-in workflow_inline classification drift; regenerate inventory",
    )
else:
    require(not debt_rows, "live inventory emit has workflow_inline debt")

report = {
    "schema_version": 1,
    "plan_key": "P10w",
    "bead_id": "fireweed-2b96f2a9",
    "consumer": "P2f",
    "title": "workflow-inline zero-debt report",
    "summary": (
        "Every workflow-inline early-success/skip/no-op is an executed policy-positive "
        "(Pages readiness branch + release best-effort RO/disk hygiene). Zero residual "
        "debt; exclusive owners P13t/P13a/P17r already landed. P10w edits no workflow file."
    ),
    "scanner": {
        "pattern": SCAN_RE.pattern,
        "workflows_scanned": tracked_workflows(),
        "hit_count": len(hits),
    },
    "policy_positives": [
        {
            "path": h["path"],
            "line": h["line"],
            "identity": h["identity"],
            "reason": h["reason"],
            "detail": h["detail"],
            "exclusive_owner": h["exclusive_owner"],
            "exact_route": h["exact_route"],
            "status": "discovery_negative",
        }
        for h in hits
    ],
    "residuals": residuals,
    "inventory": {
        "workflow_inline_row_count": len(rows),
        "workflow_inline_debt_count": 0,
        "workflow_inline_ids": [row["id"] for row in rows],
    },
    "zero_debt": True,
    "executed_commands": [
        "python3 scripts/ci/inventory-storage-remediation.py --emit",
        "bash scripts/ci/workflow-inline-zero-debt-test.sh",
    ],
}
report["report_sha256"] = hashlib.sha256(
    json.dumps(
        {k: v for k, v in report.items() if k != "report_sha256"},
        sort_keys=True,
    ).encode()
).hexdigest()

if WRITE_REPORT:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {REPORT_PATH}")
else:
    if REPORT_PATH.is_file():
        existing = json.loads(REPORT_PATH.read_text(encoding="utf-8"))

        def core(doc: dict) -> dict:
            return {
                "zero_debt": doc.get("zero_debt"),
                "residuals": doc.get("residuals"),
                "policy_positives": [
                    {
                        "path": p["path"],
                        "line": p["line"],
                        "reason": p["reason"],
                        "status": p.get("status"),
                    }
                    for p in doc.get("policy_positives", [])
                ],
                "inventory_debt": doc.get("inventory", {}).get("workflow_inline_debt_count"),
            }

        require(
            core(existing) == core(report),
            f"{REPORT_PATH} stale; re-run without --check",
        )
    print(f"checked {REPORT_PATH}")

print(f"workflow-inline zero-debt: PASS (hits={len(hits)} debt=0)")
PY

echo "=== workflow-inline-zero-debt-test PASSED ==="
