#!/usr/bin/env bash
# Repository GitHub Actions policy verifier (P13a owns policy; P13t requires zero-arg).
#
# Zero-argument invocation must remain valid: every caller (ci.yml, turso.yml,
# nightly.yml, governed-product.yml) runs:
#   bash scripts/ci/verify-github-actions-policy.sh
#
# Context-aware rules:
#   - Hosted fast lanes (ci.yml, pages.yml): no services/matrix/docker/kind/perf.
#   - Focused turso.yml (P13t): path-filtered public-default lane; no services.
#   - nightly.yml: manual extended lib tests; no services/perf.
#   - release.yml: owned by P17r — no services/kind; Docker publication exception
#     only (docker/build-push-action + GHCR login).
#   - governed-product.yml: sole lane authorized for service-backed matrix/kind/S3
#     and P8k kafka-compatible broker service *slots*. Exact digests/commands are
#     P13-populated in governed-product-services.json / allowlist (not workflow YAML).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow_root="${repo_root}/.github/workflows"
allowlist_path="${repo_root}/scripts/ci/governed-product-allowlist.json"
services_path="${repo_root}/scripts/ci/governed-product-services.json"
governed_workflow="${workflow_root}/governed-product.yml"

forbidden_perf='scripts/perf/|fireweed-bench|cargo[[:space:]]+bench|performance_[A-Za-z0-9_]*|queue_density|fireweed-matrix|fireweed-loadgen'
if rg -n -i --glob '*.yml' --glob '*.yaml' "${forbidden_perf}" "${workflow_root}"; then
    echo "GitHub Actions must not execute performance tooling or benchmark suites" >&2
    exit 1
fi

if rg -n --glob '*.yml' --glob '*.yaml' '(^|[[:space:]/])release-gate\.sh([[:space:]]|$)' "${workflow_root}" |
    rg -v -- '--governed-performance-only([[:space:]]|$)'; then
    echo "GitHub release jobs must use preverified governed performance evidence" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Hosted fast lane: ci.yml
# ---------------------------------------------------------------------------
ci="${workflow_root}/ci.yml"
grep -Fq 'timeout-minutes: 3' "${ci}"
if rg -n 'services:|matrix:|cargo install|rustup toolchain install nightly|docker run|kind-helm|(^|[[:space:]/])release-gate\.sh([[:space:]]|$)|(^|[[:space:]/])nightly-gate\.sh([[:space:]]|$)|cargo test --workspace' "${ci}"; then
    echo "default CI contains an unbounded or duplicated heavy lane" >&2
    exit 1
fi

# P2 exact mode-file-driven debt-policy invocation (do not delete or weaken).
grep -Fq 'bash scripts/ci/storage-remediation-policy.sh --mode-file scripts/ci/storage-remediation-policy.mode' "${ci}"
grep -Fq 'run: bash scripts/ci/verify-github-actions-policy.sh' "${ci}"
grep -Fq 'python3 scripts/ci/public-release-gate.py' "${ci}"

release_gate="${repo_root}/scripts/ci/release-gate.sh"
grep -Fq -- '--governed-performance-only' "${release_gate}"
grep -Fq 'RUN_LOCAL_PERFORMANCE=false' "${release_gate}"

echo "GitHub Actions policy valid: default CI is bounded and Actions contain no performance execution"

# ---------------------------------------------------------------------------
# Focused Turso lane (P13t): path-filtered; zero-arg policy invocation preserved.
# ---------------------------------------------------------------------------
turso="${workflow_root}/turso.yml"
if [[ ! -f "${turso}" ]]; then
    echo "missing governed focused lane: .github/workflows/turso.yml" >&2
    exit 1
fi
grep -Fq 'pull_request:' "${turso}"
grep -Fq 'paths:' "${turso}"
grep -Fq '1.97.1' "${turso}"
grep -Fq 'turso_projection_is_the_public_env_default' "${turso}"
grep -Fq 'objectlog_turso_profile_rebuilds_deleted_projection_from_authoritative_log' "${turso}"
grep -Fq 'storage_matrix_t0_t2_all_twenty_cells' "${turso}"
# Zero-argument policy-verifier invocation (exact regression for P13t).
grep -Fq 'bash scripts/ci/verify-github-actions-policy.sh' "${turso}"
if rg -n 'services:' "${turso}"; then
    echo "turso.yml must not declare GitHub Actions services" >&2
    exit 1
fi
echo "GitHub Actions policy valid: turso.yml is a governed focused public-default lane"

# ---------------------------------------------------------------------------
# Hosted pages / nightly: no services, kind, or performance.
# ---------------------------------------------------------------------------
for hosted in pages.yml nightly.yml; do
    path="${workflow_root}/${hosted}"
    [[ -f "${path}" ]] || continue
    if rg -n 'services:|kind-helm|kindest/node|scripts/perf/|cargo bench' "${path}"; then
        echo "${hosted} must not introduce services/kind/performance execution" >&2
        exit 1
    fi
done

# Hybrid product selectors remain forbidden in every workflow.
if rg -n -i --glob '*.yml' --glob '*.yaml' \
    'hybrid-strict|hybrid-async|projection_backend:[[:space:]]*hybrid|FIREWEED_PROJECTION_BACKEND=hybrid' \
    "${workflow_root}"; then
    echo "workflows must not select retired Hybrid product projections" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Governed product lane framework (P13a): allowlist + service authorization.
# ---------------------------------------------------------------------------
if [[ ! -f "${governed_workflow}" ]]; then
    echo "missing governed product workflow: .github/workflows/governed-product.yml" >&2
    exit 1
fi
if [[ ! -f "${allowlist_path}" ]]; then
    echo "missing governed allowlist: scripts/ci/governed-product-allowlist.json" >&2
    exit 1
fi
if [[ ! -f "${services_path}" ]]; then
    echo "missing governed services authorization: scripts/ci/governed-product-services.json" >&2
    exit 1
fi

grep -Fq 'bash scripts/ci/verify-github-actions-policy.sh' "${governed_workflow}"
grep -Fq 'governed-product' "${governed_workflow}"
# Product-release readiness must not be silently claimed by the framework alone.
if rg -n -i 'product.release.readiness.claimed:[[:space:]]*true|claims product-release readiness' "${governed_workflow}"; then
    echo "governed-product.yml must not claim product-release readiness without P13b" >&2
    exit 1
fi
# Authoritative performance remains forbidden even on the governed lane.
# Match executable forms only (comments may document the ban).
if rg -n -i \
    '(^|[[:space:]])(bash[[:space:]]+)?scripts/perf/|cargo[[:space:]]+bench|fireweed-bench|fireweed-loadgen' \
    "${governed_workflow}"; then
    echo "governed-product.yml must not run authoritative performance" >&2
    exit 1
fi

python3 - "${allowlist_path}" "${services_path}" "${governed_workflow}" <<'PY'
import json
import re
import subprocess
import sys
from pathlib import Path

allowlist_path, services_path, workflow_path = map(Path, sys.argv[1:])
repo_root = workflow_path.resolve().parents[2]
allowlist = json.loads(allowlist_path.read_text())
services = json.loads(services_path.read_text())
workflow = workflow_path.read_text()

assert allowlist["schema_version"] == 1, "allowlist schema_version"
assert allowlist["workflow"] == ".github/workflows/governed-product.yml"
assert allowlist["lane"] == "governed-product"
assert allowlist["product_release_readiness_claimed"] is False, (
    "product_release_readiness_claimed must stay false until P13b"
)
assert allowlist["command_population_owner"] == "P13"
assert isinstance(allowlist["commands"], list), "commands must be a list"
assert len(allowlist["commands"]) >= 8, "P13 must populate the governed command set"
assert "P13" in allowlist["disclaimer"] or "P13b" in allowlist["disclaimer"]

forbidden = allowlist.get("forbidden_in_lane") or [
    "scripts/perf/",
    "fireweed-bench",
    "cargo bench",
    "authoritative-performance",
    "performance_",
]
seen_ids: set[str] = set()
categories: set[str] = set()
for entry in allowlist["commands"]:
    assert isinstance(entry, dict), "each allowlist command must be an object"
    assert "id" in entry and "command" in entry, "command entries need id+command"
    assert entry["id"] not in seen_ids, f"duplicate allowlist id {entry['id']}"
    seen_ids.add(entry["id"])
    cmd = entry["command"]
    assert isinstance(cmd, list) and cmd and all(isinstance(p, str) for p in cmd), (
        f"{entry['id']} command must be a non-empty string list"
    )
    joined = " ".join(cmd)
    for token in forbidden:
        assert token not in joined, f"{entry['id']} hits forbidden_in_lane {token!r}"
    categories.add(entry.get("category") or "")

for required_cat in ("functional", "T4", "reduced-count", "external-kafka", "policy"):
    assert required_cat in categories, f"allowlist missing category {required_cat}"

assert services["schema_version"] == 1, "services schema_version"
assert services["workflow"] == ".github/workflows/governed-product.yml"
kafka = services["services"]["kafka_compatible_broker"]
assert kafka["authorized"] is True
assert kafka["digest_population_owner"] == "P13"
assert kafka["command_population_owner"] == "P13"
assert kafka["requirements"]["image_must_be_digest_pinned"] is True
assert kafka["requirements"]["tag_only_image_forbidden"] is True
assert "sha256" in kafka["requirements"]["immutable_digest_form"]
digest = kafka["image_digest"]
assert isinstance(digest, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", digest), (
    "P13 must populate kafka image_digest as sha256:<64-hex>"
)
command = kafka["command"]
assert isinstance(command, list) and command and command[0] == "redpanda", (
    "P13 must populate kafka command as redpanda start argv list"
)
assert kafka["image_repository"] == "redpandadata/redpanda"
pinned = kafka.get("image_pinned") or f"{kafka['image_repository']}@{digest}"
assert pinned == f"redpandadata/redpanda@{digest}"

# Workflow references authorization docs; pin lives in services JSON only (not YAML image:).
assert "governed-product-services.json" in workflow or "kafka_compatible_broker" in workflow
assert "kafka_compatible_broker" in workflow
assert "governed-product-allowlist.json" in workflow
if re.search(r"image:\s*redpandadata/redpanda@", workflow):
    raise SystemExit(
        "governed-product.yml must not embed kafka image pin in workflow YAML; "
        "pin lives in governed-product-services.json"
    )
if re.search(r"image:\s*redpandadata/redpanda:", workflow):
    raise SystemExit("tag-only redpanda image is forbidden; digest pin required")

# On-disk allowlist/services must match the P13 generator (manifest-derived).
gen = repo_root / "scripts/ci/generate-governed-product-allowlist.py"
assert gen.is_file(), "missing generate-governed-product-allowlist.py"
check = subprocess.run(
    [sys.executable, str(gen), "--check"],
    cwd=repo_root,
    check=False,
    capture_output=True,
    text=True,
)
if check.returncode != 0:
    sys.stderr.write(check.stdout + check.stderr)
    raise SystemExit("governed allowlist/services drift from P13 generator")

print(
    "GitHub Actions policy valid: governed-product allowlist populated "
    f"({len(allowlist['commands'])} commands); kafka digest pinned; "
    "product_release_readiness_claimed=false"
)
PY

# Hosted lanes other than governed-product.yml must not declare services.
# release.yml (P17r) retains only the Docker publication exception — no services.
while IFS= read -r -d '' wf; do
    base="$(basename "${wf}")"
    case "${base}" in
        governed-product.yml) continue ;;
    esac
    if rg -n 'services:' "${wf}"; then
        echo "hosted workflow ${base} must not declare services (move to governed-product.yml)" >&2
        exit 1
    fi
done < <(find "${workflow_root}" -maxdepth 1 \( -name '*.yml' -o -name '*.yaml' \) -print0)

# P17r release shape: dual checkout, tag trigger, no ambient GITHUB_SHA source binding.
release_wf="${workflow_root}/release.yml"
if [[ -f "${release_wf}" ]]; then
    grep -Fq 'tags:' "${release_wf}"
    grep -Fq 'path: fireweed-evidence' "${release_wf}"
    grep -Fq 'path: fireweed-source' "${release_wf}"
    grep -Fq 'resolve-release-identity.sh' "${release_wf}"
    grep -Fq 'docker/build-push-action' "${release_wf}"
    if rg -n 'services:' "${release_wf}"; then
        echo "release.yml must not declare services after P17r" >&2
        exit 1
    fi
    if rg -n 'kindest/node|KIND_NODE_IMAGE' "${release_wf}"; then
        echo "release.yml must not pin kind after P17r" >&2
        exit 1
    fi
    echo "GitHub Actions policy valid: release.yml dual-checkout tag lane (Docker exception only)"
fi

echo "GitHub Actions policy valid: repository-side governed lane + external-proof prerequisites"
