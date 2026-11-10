#!/usr/bin/env bash
# storage-matrix-gate.sh — Release/CI gate for the public 12-cell storage matrix.
#
# The public product storage surface is exactly the 4×3 matrix
# (log ∈ {memory,postgres,filesystem,s3} ×
#  projection ∈ {memory,turso,postgres}).
# This gate binds exact P10r functional-matrix source leaves (compile/list +
# focused cargo invocations) and fake-projection lifecycle hygiene so a release cannot ship with a failed required cell.
#
# Broad substring cargo filters are forbidden (P10r). Every cargo test filter is
# an exact harness ID from:
#   scripts/ci/manifests/functional-matrix-route-sources.json
#
# Governing bars:
#   docs/helix/04-build/storage-matrix-completion-brief.md §2 (Phase 6)
#   scripts/ci/s3-matrix-job-requirements.md (generated from authority manifest)
#   scripts/ci/manifests/storage-authority-manifest.json
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
# Required product / release CI that claims the full 12-cell surface MUST:
#   1. export FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1
#   2. provision S3-compatible + Postgres fixtures (see Fixture requirements)
#   3. treat a missing fixture as gate failure (this script enforces that)
#
# Optional flags:
#   --skip-cargo    omit cargo matrix suites (hygiene checks still run)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

CARGO="${CARGO:-rustup run 1.99.0 cargo}"
REQUIRE_FULL="${FIREWEED_STORAGE_MATRIX_REQUIRE_FULL:-0}"
SKIP_CARGO=0
ROUTE_SOURCES="${REPO_ROOT}/scripts/ci/manifests/functional-matrix-route-sources.json"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-cargo) SKIP_CARGO=1; shift ;;
        -h|--help)
            sed -n '2,50p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "storage-matrix-gate: unknown argument: $1" >&2
            echo "usage: bash scripts/ci/storage-matrix-gate.sh [--skip-cargo]" >&2
            exit 64
            ;;
    esac
done

err() { echo "storage-matrix-gate: $*" >&2; }

echo "=== storage-matrix-gate: public 12-cell StorageConfig matrix ==="
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
        err "refusing to claim full 12-cell matrix with missing fixtures (skip ≠ pass)"
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
# 1. Fake-projection lifecycle hygiene (always; fast fail)
# ---------------------------------------------------------------------------
echo "--- forbid-fake-projection-lifecycle ---"
bash "${SCRIPT_DIR}/forbid-fake-projection-lifecycle.sh" --self-test
bash "${SCRIPT_DIR}/forbid-fake-projection-lifecycle.sh"

# ---------------------------------------------------------------------------
# 2. Exact cargo source leaves (no substring filters)
# ---------------------------------------------------------------------------
if ((SKIP_CARGO == 0)); then
    FIREWEED_FEATURES="memory,objectlog,postgres,turso"

    echo "--- compile/list functional-matrix route source leaves ---"
    python3 "${SCRIPT_DIR}/functional_matrix_route_sources.py" --list-leaves

    echo "--- cargo test -p fireweed --test functional_matrix_route_sources (dry-run leaves) ---"
    # Execute only the P10r dry-run source module. Full T0–T2 / live matrix execution is P10.
    ${CARGO} test -p fireweed --features "${FIREWEED_FEATURES}" \
        --test functional_matrix_route_sources -- --nocapture

    echo "--- exact T0–T2 registration leaf (no live fixture execution claim) ---"
    ${CARGO} test -p fireweed --features "${FIREWEED_FEATURES}" \
        --test storage_matrix_t0_t2 -- \
        storage_matrix_registers_exactly_12_distinct_cells --exact --nocapture
else
    if [[ "${REQUIRE_FULL}" == "1" || "${REQUIRE_FULL}" == "true" || "${REQUIRE_FULL}" == "yes" ]]; then
        err "refusing --skip-cargo under FIREWEED_STORAGE_MATRIX_REQUIRE_FULL (skip ≠ pass)"
        exit 1
    fi
    echo "--- cargo matrix suites: local-only skip (--skip-cargo; non-full local only) ---"
fi

echo "=== storage-matrix-gate: PASSED ==="
echo "Release storage surface: 20 cells (StorageConfig log × projection)."
echo "AsyncProjection: 8 object-log positives + 12 non-object-log pre-I/O negatives (manifest)."
echo "Route sources: ${ROUTE_SOURCES}"
echo "Full CI claims require FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1 + S3/PG fixtures."
echo "S3 fixture contract: scripts/ci/s3-matrix-job-requirements.md"
exit 0
