#!/usr/bin/env bash
# Fixture + self-test harness for P13a external-proof CLI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

case_root="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-external-proof.XXXXXX")"
trap 'rm -rf "$case_root"' EXIT

echo "--- embedded self-test ---"
python3 scripts/ci/verify-github-external-proof.py --self-test

echo "--- positive fixture: schema + API + snorri + candidate closure ---"
python3 - "$case_root" <<'PY'
import hashlib
import json
import subprocess
import sys
from pathlib import Path

case_root = Path(sys.argv[1])
source = "a" * 40
ruleset = {"id": "ruleset-1", "version": "3", "digest": "sha256:" + ("b" * 64)}
check_run_payload = [
    {
        "workflow_id": "wf-governed-product",
        "context": "governed-product / product",
        "check_run_id": "cr-9",
        "attempt": 2,
    }
]
check_digest = "sha256:" + hashlib.sha256(
    json.dumps(check_run_payload, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
proof = {
    "schema": "fireweed.github_external_proof.v1",
    "schema_version": 1,
    "repository_id": "repo-42",
    "ruleset": ruleset,
    "required_contexts": ["governed-product / product"],
    "candidate_source": source,
    "checks": [
        {
            "workflow_id": "wf-governed-product",
            "context": "governed-product / product",
            "check_suite_id": "cs-1",
            "check_run_id": "cr-9",
            "conclusion": "success",
            "head_sha": source,
            "attempt": 2,
            "status": "completed",
        }
    ],
    "response_digests": {"ruleset": ruleset["digest"], "check_runs": check_digest},
    "recorded_at": "2026-08-07T00:00:00Z",
    "product_release_readiness_claimed": True,
}
api = {
    "repository_id": "repo-42",
    "ruleset": {**ruleset, "current": True},
    "check_attempts": [
        {
            "workflow_id": "wf-governed-product",
            "context": "governed-product / product",
            "head_sha": source,
            "attempt": 1,
            "conclusion": "failure",
            "status": "completed",
            "check_suite_id": "cs-0",
            "check_run_id": "cr-1",
        },
        {
            "workflow_id": "wf-governed-product",
            "context": "governed-product / product",
            "head_sha": source,
            "attempt": 2,
            "conclusion": "success",
            "status": "completed",
            "check_suite_id": "cs-1",
            "check_run_id": "cr-9",
        },
    ],
    "expected_check_runs_digest": check_digest,
}
snorri = {
    "schema": "fireweed.snorri_external_acceptance.v1",
    "source_revision": source,
    "suite_id": "SNORRI-MATRIX-LIFECYCLE",
    "conclusion": "success",
    "evidence_digest": "sha256:" + ("c" * 64),
}
candidate = {
    "candidate_source": source,
    "tracker_state_read": False,
    "closed": True,
}
proof_path = case_root / "proof.json"
api_path = case_root / "api.json"
snorri_path = case_root / "snorri.json"
candidate_path = case_root / "candidate.json"
proof_path.write_text(json.dumps(proof, indent=2) + "\n")
api_path.write_text(json.dumps(api, indent=2) + "\n")
snorri_path.write_text(json.dumps(snorri, indent=2) + "\n")
candidate_path.write_text(json.dumps(candidate, indent=2) + "\n")

cmd = [
    "python3",
    "scripts/ci/verify-github-external-proof.py",
    "--github-proof",
    str(proof_path),
    "--expected-source",
    source,
    "--repository-id",
    "repo-42",
    "--api-fixture",
    str(api_path),
    "--snorri-proof",
    str(snorri_path),
    "--candidate-closure",
    "--candidate-fixture",
    str(candidate_path),
]
subprocess.check_call(cmd)

# Stale ruleset fails.
api["ruleset"]["current"] = False
api_path.write_text(json.dumps(api, indent=2) + "\n")
stale = subprocess.run(cmd, check=False)
assert stale.returncode == 1, "stale ruleset must fail"

# Refuse .ddx paths.
ddx_proof = Path(".ddx") / "beads.jsonl"
bad = subprocess.run(
    [
        "python3",
        "scripts/ci/verify-github-external-proof.py",
        "--github-proof",
        str(ddx_proof),
        "--expected-source",
        source,
        "--repository-id",
        "repo-42",
    ],
    check=False,
    capture_output=True,
    text=True,
)
assert bad.returncode == 1, ".ddx proof path must fail"
assert ".ddx" in (bad.stderr or "").lower() or "ddx" in (bad.stderr or "").lower()

print("verify-github-external-proof-test: PASS")
PY
