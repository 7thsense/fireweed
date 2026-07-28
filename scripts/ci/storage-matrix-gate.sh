#!/usr/bin/env bash
# storage-matrix-gate.sh — Release/CI gate for the public 15-cell storage matrix.
#
# The public product storage surface is exactly the 5×3 matrix
# (log ∈ {memory,sqlite,postgres,filesystem,s3} × projection ∈ {memory,sqlite,postgres}).
# This gate binds T0–T2 / focused server matrix suites, legacy product-name
# hygiene, and Helm matrix fixtures so a release cannot ship with a failed
# required cell.
#
# Governing bars:
#   docs/helix/04-build/storage-matrix-completion-brief.md §2 (Phase 6)
#   docs/helix/04-build/DEPLOYMENT-READINESS.md (product storage model)
#   scripts/ci/s3-matrix-job-requirements.md
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
# Required product / release CI that claims the full 15-cell surface MUST:
#   1. export FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1
#   2. provision S3-compatible + Postgres fixtures (see Fixture requirements)
#   3. treat a missing fixture as gate failure (this script enforces that)
#
# Fixture requirements (full matrix)
# ----------------------------------
# Postgres (any cell with postgres log or projection):
#   FIREWEED_PG_TEST_URL=postgres://user:pass@host:5432/db
#   cargo builds use --features postgres on the fireweed library package
#
# S3 (log axis s3 — three cells):
#   FIREWEED_S3_TEST_ENDPOINT=http://<minio-or-compatible>:9000
#   FIREWEED_S3_TEST_BUCKET=fireweed-test          # recommended
#   FIREWEED_S3_TEST_ACCESS_KEY=minioadmin         # recommended
#   FIREWEED_S3_TEST_SECRET_KEY=minioadmin         # recommended
#   FIREWEED_S3_TEST_REGION=us-east-1              # optional
# Endpoint must support native create-only (If-None-Match: *). See:
#   scripts/ci/s3-matrix-job-requirements.md
#
# Optional flags:
#   --skip-helm     skip helm-gate.sh (matrix suite + legacy assert still run)
#   --skip-cargo    skip cargo matrix suites (helm + legacy still run)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

CARGO="${CARGO:-rustup run 1.92.0 cargo}"
REQUIRE_FULL="${FIREWEED_STORAGE_MATRIX_REQUIRE_FULL:-0}"
SKIP_HELM=0
SKIP_CARGO=0

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

echo "=== storage-matrix-gate: public 15-cell StorageConfig matrix ==="
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
        err "refusing to claim full 15-cell matrix with missing fixtures (skip ≠ pass)"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# 1. Legacy product-name hygiene (always; fast fail)
# ---------------------------------------------------------------------------
echo "--- assert-no-legacy-storage-product-names ---"
bash "${SCRIPT_DIR}/assert-no-legacy-storage-product-names.sh"

# ---------------------------------------------------------------------------
# 2. Library 15-cell T0–T2 harness
# ---------------------------------------------------------------------------
if ((SKIP_CARGO == 0)); then
    # Prefer postgres feature so postgres-axis cells can run when FIREWEED_PG_TEST_URL is set.
    FIREWEED_FEATURES="memory,sqlite,objectlog,postgres"

    echo "--- cargo test -p fireweed --test storage_matrix_t0_t2 (features=${FIREWEED_FEATURES}) ---"
    ${CARGO} test -p fireweed --features "${FIREWEED_FEATURES}" --test storage_matrix_t0_t2 -- --nocapture

    # ---------------------------------------------------------------------------
    # 3. Server composition-root matrix suites (filter substrings match module names)
    #    class_b           — memory log × {memory,sqlite,postgres} Class B T0–T3
    #    sqlite_log_matrix — sqlite log three projections
    #    filesystem_matrix — filesystem object-log three projections
    #    s3_object_log     — s3 log three projections (+ unit/T4)
    # ---------------------------------------------------------------------------
    SERVER_FILTERS=(class_b sqlite_log_matrix filesystem_matrix s3_object_log)
    for filter in "${SERVER_FILTERS[@]}"; do
        echo "--- cargo test -p fireweed-server --lib ${filter} ---"
        # fireweed-server wires postgres behind its own `postgres` feature.
        ${CARGO} test -p fireweed-server --features postgres --lib "${filter}" -- --nocapture
    done
else
    echo "--- cargo matrix suites: SKIPPED (--skip-cargo) ---"
fi

# ---------------------------------------------------------------------------
# 4. Helm gate — lint/render/kubeconform for all 15-cell CI values fixtures
# ---------------------------------------------------------------------------
if ((SKIP_HELM == 0)); then
    echo "--- helm-gate (15-cell matrix fixtures + shared variants) ---"
    bash "${SCRIPT_DIR}/helm-gate.sh"
else
    echo "--- helm-gate: SKIPPED (--skip-helm) ---"
fi

echo "=== storage-matrix-gate: PASSED ==="
echo "Release storage surface: 15 cells (StorageConfig log × projection)."
echo "Full CI claims require FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1 + S3/PG fixtures."
echo "S3 fixture contract: scripts/ci/s3-matrix-job-requirements.md"
exit 0
