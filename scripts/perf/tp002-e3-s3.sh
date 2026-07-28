#!/usr/bin/env bash
# TP-002 E3 provider-neutral live S3 release evidence wrapper.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)

if [ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]; then
  echo "E3 provenance check failed: worktree must be clean before release measurement" >&2
  exit 2
fi
SOURCE_REVISION=$(git -C "$REPO_ROOT" rev-parse HEAD)

FIREWEED_E3_RESIDENT=${FIREWEED_E3_RESIDENT:-10000000}
FIREWEED_E3_LOAD_BATCH=${FIREWEED_E3_LOAD_BATCH:-1000}
FIREWEED_E3_ACK_PUSHES=${FIREWEED_E3_ACK_PUSHES:-100000}
FIREWEED_E3_ACK_CONCURRENCY=${FIREWEED_E3_ACK_CONCURRENCY:-384}
FIREWEED_E3_LOAD_CONCURRENCY=${FIREWEED_E3_LOAD_CONCURRENCY:-8}
FIREWEED_RECOVERY_MAX_TAIL_COMMANDS=${FIREWEED_RECOVERY_MAX_TAIL_COMMANDS:-1000000}
if [[ "$FIREWEED_E3_RESIDENT" != 10000000 || "$FIREWEED_E3_LOAD_BATCH" != 1000 \
   || "$FIREWEED_E3_ACK_PUSHES" != 100000 || "$FIREWEED_E3_ACK_CONCURRENCY" != 384 \
   || "$FIREWEED_E3_LOAD_CONCURRENCY" != 8 || "$FIREWEED_RECOVERY_MAX_TAIL_COMMANDS" != 1000000 ]]; then
  echo "E3 shape check failed: release requires resident=10000000 load_batch=1000 ack_pushes=100000 ack_concurrency=384 load_concurrency=8 recovery_max_tail_commands=1000000" >&2
  exit 2
fi

: "${FIREWEED_S3_TEST_ENDPOINT:?set the S3-compatible endpoint}"
: "${FIREWEED_S3_TEST_REGION:?set the S3 signing region}"
: "${FIREWEED_S3_TEST_BUCKET:?set the isolated E3 bucket}"
: "${FIREWEED_S3_TEST_ACCESS_KEY:?set the S3 access key}"
: "${FIREWEED_S3_TEST_SECRET_KEY:?set the S3 secret key}"
: "${FIREWEED_E3_STORAGE_TOPOLOGY_ID:?set a stable topology identifier}"
: "${FIREWEED_E3_STORAGE_TOPOLOGY:?describe the measured storage topology}"
: "${FIREWEED_E3_STORAGE_DURABILITY_CLAIM:?declare the durability claim}"
: "${FIREWEED_E3_AUTHORITY_MODE:?set native-create-only or postgres-pointer}"
: "${FIREWEED_E3_POSTGRES_POINTER_DATABASE_URL:?set the Postgres DSN used for the independent no-CAS pointer fence proof}"
: "${FIREWEED_E3_FENCE_EVIDENCE_OUT:?set the output path for source-bound fencing.json evidence}"
: "${FIREWEED_E3_TRANSACTION_EVIDENCE_OUT:?set the output path for the executed 48-row TP-003 matrix}"

if [[ ! "$FIREWEED_E3_STORAGE_TOPOLOGY_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$ ]]; then
  echo "E3 topology check failed: topology id must be a 3-128 character stable token" >&2
  exit 2
fi
if [ "$FIREWEED_E3_STORAGE_DURABILITY_CLAIM" != excluded ]; then
  echo "E3 topology check failed: this release profile currently requires storage durability claim=excluded" >&2
  exit 2
fi
case "$FIREWEED_E3_AUTHORITY_MODE" in
  native-create-only) ;;
  postgres-pointer)
    echo "E3 authority check failed: postgres-pointer measurement is not yet implemented; the current harness uses the native create-only backend and proves the Postgres pointer independently" >&2
    exit 2
    ;;
  *)
    echo "E3 authority check failed: authority mode must be native-create-only or postgres-pointer" >&2
    exit 2
    ;;
esac

FIREWEED_E3_RECORDED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

set +e
env \
  FIREWEED_E3_SOURCE_REVISION="$SOURCE_REVISION" \
  FIREWEED_E3_RECORDED_AT="$FIREWEED_E3_RECORDED_AT" \
  FIREWEED_E3_TRANSACTION_EVIDENCE_OUT="$FIREWEED_E3_TRANSACTION_EVIDENCE_OUT" \
  cargo test -p fireweed-conformance --release --test external_transaction_contract_matrix_tests \
    e3_governed_transaction_evidence_matrix -- --nocapture
TXN_STATUS=$?
if [ "$TXN_STATUS" -ne 0 ]; then
  set -e
  exit "$TXN_STATUS"
fi

# The matrix executes exact snapshot-tail and genesis recovery. Standalone recovery entrypoints remain
# available for focused runs without duplicating both 10M datasets in this governed run.
env \
  FIREWEED_PERF_ENV="${FIREWEED_PERF_ENV:-1}" \
  FIREWEED_E3_RESIDENT="$FIREWEED_E3_RESIDENT" \
  FIREWEED_E3_LOAD_BATCH="$FIREWEED_E3_LOAD_BATCH" \
  FIREWEED_E3_ACK_PUSHES="$FIREWEED_E3_ACK_PUSHES" \
  FIREWEED_E3_ACK_CONCURRENCY="$FIREWEED_E3_ACK_CONCURRENCY" \
  FIREWEED_E3_LOAD_CONCURRENCY="$FIREWEED_E3_LOAD_CONCURRENCY" \
  FIREWEED_RECOVERY_MAX_TAIL_COMMANDS="$FIREWEED_RECOVERY_MAX_TAIL_COMMANDS" \
  FIREWEED_E3_STORAGE_TOPOLOGY="$FIREWEED_E3_STORAGE_TOPOLOGY" \
  FIREWEED_E3_STORAGE_TOPOLOGY_ID="$FIREWEED_E3_STORAGE_TOPOLOGY_ID" \
  FIREWEED_E3_STORAGE_DURABILITY_CLAIM="$FIREWEED_E3_STORAGE_DURABILITY_CLAIM" \
  FIREWEED_E3_AUTHORITY_MODE="$FIREWEED_E3_AUTHORITY_MODE" \
  FIREWEED_E3_SOURCE_REVISION="$SOURCE_REVISION" \
  FIREWEED_E3_POSTGRES_POINTER_DATABASE_URL="$FIREWEED_E3_POSTGRES_POINTER_DATABASE_URL" \
  FIREWEED_E3_FENCE_EVIDENCE_OUT="$FIREWEED_E3_FENCE_EVIDENCE_OUT" \
  FIREWEED_S3_TEST_ENDPOINT="$FIREWEED_S3_TEST_ENDPOINT" \
  FIREWEED_S3_TEST_REGION="$FIREWEED_S3_TEST_REGION" \
  FIREWEED_S3_TEST_BUCKET="$FIREWEED_S3_TEST_BUCKET" \
  FIREWEED_S3_TEST_ACCESS_KEY="$FIREWEED_S3_TEST_ACCESS_KEY" \
  FIREWEED_S3_TEST_SECRET_KEY="$FIREWEED_S3_TEST_SECRET_KEY" \
  cargo test -p fireweed-server --release --test performance_object_log_e3_live_tests \
    performance_object_log_e3_live_tests -- --nocapture
TEST_STATUS=$?
set -e

if [ "$TEST_STATUS" -ne 0 ]; then
  exit "$TEST_STATUS"
fi
if [ "$(git -C "$REPO_ROOT" rev-parse HEAD)" != "$SOURCE_REVISION" ]; then
  echo "E3 provenance check failed: HEAD changed during release measurement" >&2
  exit 2
fi
if [ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]; then
  echo "E3 provenance check failed: worktree changed during release measurement" >&2
  exit 2
fi
