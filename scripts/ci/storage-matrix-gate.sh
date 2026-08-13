#!/usr/bin/env bash
# storage-matrix-gate.sh — Release/CI gate for the public 20-cell storage matrix.
#
# The public product storage surface is exactly the 5×4 matrix
# (log ∈ {memory,sqlite,postgres,filesystem,s3} ×
#  projection ∈ {memory,sqlite,turso,postgres}).
# This gate binds exact P10r functional-matrix source leaves (compile/list +
# focused cargo invocations), legacy product-name hygiene, and Helm matrix
# fixtures so a release cannot ship with a failed required cell.
#
# Broad substring cargo filters are forbidden (P10r). Every cargo test filter is
# an exact harness ID from:
#   docs/helix/04-build/functional-matrix-route-sources.json
#
# Governing bars:
#   docs/helix/04-build/storage-matrix-completion-brief.md §2 (Phase 6)
#   docs/helix/04-build/DEPLOYMENT-READINESS.md (product storage model)
#   scripts/ci/s3-matrix-job-requirements.md (generated from authority manifest)
#   docs/helix/04-build/storage-authority-manifest.json
#
# Usage:
#   bash scripts/ci/storage-matrix-gate.sh
#
# Exit codes:
#   0  all steps passed
#   non-zero  any step failed (set -e) or full-matrix fixtures missing under REQUIRE_FULL
#
# Fixture policy
# --------------
# Local/dev runs may execute without live S3 or Postgres: cargo tests document
# skip for cells that need FIREWEED_S3_TEST_ENDPOINT / FIREWEED_PG_TEST_URL.
#
# Required product / release CI that claims the full 20-cell surface MUST:
#   1. export FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1
#   2. provision S3-compatible + Postgres fixtures (see Fixture requirements)
#   3. treat a missing fixture as gate failure (this script enforces that)
#
# Optional flags:
#   --skip-helm     skip helm-gate.sh (matrix suite + legacy assert still run)
#   --skip-cargo    skip cargo matrix suites (helm + legacy still run)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

CARGO="${CARGO:-rustup run 1.97.1 cargo}"
REQUIRE_FULL="${FIREWEED_STORAGE_MATRIX_REQUIRE_FULL:-0}"
SKIP_HELM=0
SKIP_CARGO=0
ROUTE_SOURCES="${REPO_ROOT}/docs/helix/04-build/functional-matrix-route-sources.json"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-helm) SKIP_HELM=1; shift ;;
        --skip-cargo) SKIP_CARGO=1; shift ;;
        -h|--help)
            sed -n '2,50p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "storage-matrix-gate: unknown argument: $1" >&2
            echo "usage: bash scripts/ci/storage-matrix-gate.sh [--skip-helm] [--skip-cargo]" >&2
            exit 64
            ;;
    esac
done

err() { echo "storage-matrix-gate: $*" >&2; }

echo "=== storage-matrix-gate: public 20-cell StorageConfig matrix ==="
echo "repo: ${REPO_ROOT}"
echo "REQUIRE_FULL=${REQUIRE_FULL}"

# ---------------------------------------------------------------------------
# Full-matrix fixture hard-fail (required CI only)
# ---------------------------------------------------------------------------
if [[ "${REQUIRE_FULL}" == "1" || "${REQUIRE_FULL}" == "true" || "${REQUIRE_FULL}" == "yes" ]]; then
    echo "--- full-matrix fixture check (FIREWEED_STORAGE_MATRIX_REQUIRE_FULL) ---"
    missing=0
    if [[ -z "${FIREWEED_S3_TEST_ENDPOINT:-}" ]]; then
        err "FIREWEED_S3_TEST_ENDPOINT is required for full-matrix release CI"
        err "  see scripts/ci/s3-matrix-job-requirements.md"
        missing=1
    else
        echo "  FIREWEED_S3_TEST_ENDPOINT=${FIREWEED_S3_TEST_ENDPOINT}"
    fi
    if [[ -z "${FIREWEED_PG_TEST_URL:-}" ]]; then
        err "FIREWEED_PG_TEST_URL is required for full-matrix release CI"
        missing=1
    else
        # Do not print credentials; show only that it is set.
        echo "  FIREWEED_PG_TEST_URL is set"
    fi
    if ((missing != 0)); then
        err "refusing to claim full 20-cell matrix with missing fixtures (skip ≠ pass)"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# 0. Manifest selectors + exact route source registry (P10r)
# ---------------------------------------------------------------------------
echo "--- functional-matrix route sources (manifest selectors; exact leaves) ---"
if [[ ! -f "${ROUTE_SOURCES}" ]]; then
    err "missing ${ROUTE_SOURCES}; run:"
    err "  python3 scripts/ci/functional_matrix_route_sources.py --write"
    exit 1
fi
python3 "${SCRIPT_DIR}/functional_matrix_route_sources.py" --check --self-test

# ---------------------------------------------------------------------------
# 1. Legacy product-name hygiene (always; fast fail)
# ---------------------------------------------------------------------------
echo "--- assert-no-legacy-storage-product-names ---"
bash "${SCRIPT_DIR}/assert-no-legacy-storage-product-names.sh"

# ---------------------------------------------------------------------------
# 2. Exact cargo source leaves (no substring filters)
# ---------------------------------------------------------------------------
if ((SKIP_CARGO == 0)); then
    FIREWEED_FEATURES="memory,sqlite,objectlog,postgres,turso"

    echo "--- compile/list functional-matrix route source leaves ---"
    python3 "${SCRIPT_DIR}/functional_matrix_route_sources.py" --list-leaves

    echo "--- cargo test -p fireweed --test functional_matrix_route_sources (dry-run leaves) ---"
    # Execute only the P10r dry-run source module. Full T0–T2 / live matrix execution is P10.
    ${CARGO} test -p fireweed --features "${FIREWEED_FEATURES}" \
        --test functional_matrix_route_sources -- --nocapture

    echo "--- exact T0–T2 registration leaf (no live fixture execution claim) ---"
    ${CARGO} test -p fireweed --features "${FIREWEED_FEATURES}" \
        --test storage_matrix_t0_t2 -- \
        storage_matrix_registers_exactly_20_distinct_cells --exact --nocapture

    # Exact server --lib + external-kafka leaves: compile/list only (P10r boundary).
    # Full execution of fixture-bound cells is owned by P10 after P2r bindings.
    echo "--- exact fireweed-server --lib leaves (compile/list) ---"
    REPO_ROOT="${REPO_ROOT}" CARGO="${CARGO}" python3 - <<'PY'
import json
import os
import subprocess
import sys
from pathlib import Path

root = Path(os.environ["REPO_ROOT"])
registry = json.loads((root / "docs/helix/04-build/functional-matrix-route-sources.json").read_text())
cargo = os.environ.get("CARGO", "rustup run 1.97.1 cargo").split()
kinds = {"class_b_server", "inline_lib", "external_kafka"}
groups: dict[tuple[str, ...], list[tuple[str, str]]] = {}
for leaf in registry["leaves"]:
    if leaf["kind"] not in kinds:
        continue
    key = tuple(leaf["cargo_args"])
    groups.setdefault(key, []).append((leaf["leaf_id"], leaf["test_filter"]))

seen_kafka = set()
for cargo_args, items in sorted(groups.items()):
    list_cmd = cargo + list(cargo_args) + ["--", "--list"]
    print("list:", " ".join(list_cmd), flush=True)
    listed = subprocess.run(list_cmd, cwd=root, text=True, capture_output=True)
    if listed.returncode != 0:
        sys.stderr.write(listed.stdout + listed.stderr)
        sys.exit(listed.returncode)
    text = listed.stdout + listed.stderr
    for leaf_id, filt in items:
        short = filt.split("::")[-1]
        if short not in text and filt not in text:
            sys.stderr.write(f"exact leaf not listed: {leaf_id} filter={filt}\n")
            sys.exit(1)
        print(f"  listed {leaf_id}", flush=True)
        # Registry leaf_ids use feature_on/feature_off (underscore); feature_tuple
        # strings use hyphen. Accept either so the pair cannot silently empty-out.
        if leaf_id.startswith("external_kafka:"):
            if "feature_on" in leaf_id or "feature-on" in leaf_id:
                seen_kafka.add("feature-on")
            if "feature_off" in leaf_id or "feature-off" in leaf_id:
                seen_kafka.add("feature-off")

if seen_kafka != {"feature-on", "feature-off"}:
    sys.stderr.write(f"external-kafka tuples incomplete: {seen_kafka}\n")
    sys.exit(1)
print("exact server --lib + external-kafka leaves listed")
PY
else
    if [[ "${REQUIRE_FULL}" == "1" || "${REQUIRE_FULL}" == "true" || "${REQUIRE_FULL}" == "yes" ]]; then
        err "refusing --skip-cargo under FIREWEED_STORAGE_MATRIX_REQUIRE_FULL (skip ≠ pass)"
        exit 1
    fi
    echo "--- cargo matrix suites: local-only skip (--skip-cargo; non-full local only) ---"
fi

# ---------------------------------------------------------------------------
# 3. Helm gate — lint/render/kubeconform for all matrix CI values fixtures
#    Deployment/T4 portion: 20 canonical cells, Turso default, topology variants.
# ---------------------------------------------------------------------------
if ((SKIP_HELM == 0)); then
    echo "--- helm-gate (20-cell matrix fixtures + shared variants + turso default) ---"
    bash "${SCRIPT_DIR}/helm-gate.sh"
else
    if [[ "${REQUIRE_FULL}" == "1" || "${REQUIRE_FULL}" == "true" || "${REQUIRE_FULL}" == "yes" ]]; then
        err "refusing --skip-helm under FIREWEED_STORAGE_MATRIX_REQUIRE_FULL (skip ≠ pass)"
        exit 1
    fi
    echo "--- helm-gate: local-only skip (--skip-helm; non-full local only) ---"
fi

echo "=== storage-matrix-gate: PASSED ==="
echo "Release storage surface: 20 cells (StorageConfig log × projection)."
echo "AsyncProjection: 8 object-log positives + 12 non-object-log pre-I/O negatives (manifest)."
echo "Route sources: ${ROUTE_SOURCES}"
echo "Full CI claims require FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1 + S3/PG fixtures."
echo "S3 fixture contract: scripts/ci/s3-matrix-job-requirements.md"
exit 0
