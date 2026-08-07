#!/usr/bin/env bash
# P2r: campaign-aware operator_validation_tests job.
# Invokes exact generated leaves for stage/campaign; fails on empty, duplicate,
# or non-ran=1 children. Never an always-pass / Cargo leaf row.
#
# Usage:
#   run-operator-validation-job.sh --stage pre_s --campaign shared
#   run-operator-validation-job.sh --stage S --campaign product-ready
#   run-operator-validation-job.sh --stage S --campaign storage
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"
MANIFEST="${ROOT}/docs/helix/04-build/route-feature-manifest.json"

STAGE=""
CAMPAIGN="shared"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stage) STAGE="${2:?}"; shift 2 ;;
        --campaign) CAMPAIGN="${2:?}"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "${STAGE}" ]]; then
    echo "usage: $0 --stage pre_s|S --campaign shared|storage|product-ready" >&2
    exit 2
fi

# Storage campaign marks only stage=S out-of-campaign: record and exit 0 without
# claiming empty execution — still requires non-empty bound leaves in the manifest.
if [[ "${STAGE}" == "S" && "${CAMPAIGN}" == "storage" ]]; then
    python3 - "$MANIFEST" <<'PY'
import json, sys
from pathlib import Path
manifest = json.loads(Path(sys.argv[1]).read_text())
job = manifest["operator_validation_job"]
binding = job["stages"]["S"]["campaigns"]["storage"]
assert binding.get("out_of_campaign") is True, "storage stage-S must be out_of_campaign"
leaves = binding.get("leaves") or []
assert leaves, "storage stage-S binding must not be empty"
print(
    f"operator_validation_tests stage=S campaign=storage: "
    f"out_of_campaign ({len(leaves)} bound leaves; not invoked)"
)
PY
    exit 0
fi

mapfile -t leaf_jsons < <(python3 - "$MANIFEST" "$STAGE" "$CAMPAIGN" <<'PY'
import json, sys
from pathlib import Path
manifest = json.loads(Path(sys.argv[1]).read_text())
stage = sys.argv[2]
campaign = sys.argv[3]
job = manifest["operator_validation_job"]
if stage == "pre_s":
    leaves = job["stages"]["pre_s"]["leaves"]
elif stage == "S":
    leaves = job["stages"]["S"]["campaigns"][campaign]["leaves"]
else:
    raise SystemExit(f"unknown stage {stage}")
if not leaves:
    raise SystemExit(f"empty leaf set for stage={stage} campaign={campaign}")
for leaf in leaves:
    print(json.dumps(leaf))
PY
)

if [[ ${#leaf_jsons[@]} -eq 0 ]]; then
    echo "empty operator_validation_tests invocation for stage=${STAGE} campaign=${CAMPAIGN}" >&2
    exit 1
fi

seen=()
for leaf_json in "${leaf_jsons[@]}"; do
    leaf_id=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['leaf_id'])" "${leaf_json}")
    for prev in "${seen[@]+"${seen[@]}"}"; do
        if [[ "${prev}" == "${leaf_id}" ]]; then
            echo "duplicate operator leaf ${leaf_id}" >&2
            exit 1
        fi
    done
    seen+=("${leaf_id}")
    mapfile -t cmd < <(python3 -c "
import json, sys
leaf = json.loads(sys.argv[1])
for part in leaf['exact_invocation']:
    print(part)
" "${leaf_json}")
    echo "+ ${cmd[*]}"
    output="$("${cmd[@]}" 2>&1)" || {
        printf '%s\n' "${output}" >&2
        echo "operator leaf failed: ${leaf_id}" >&2
        exit 1
    }
    if ! grep -qE 'running 1 test|test result: ok\. 1 passed' <<<"${output}"; then
        printf '%s\n' "${output}" >&2
        echo "operator leaf did not report ran=1: ${leaf_id}" >&2
        exit 1
    fi
    echo "ran=1 ok: ${leaf_id}"
done

echo "operator_validation_tests stage=${STAGE} campaign=${CAMPAIGN}: ${#seen[@]} leaves, each ran=1"
