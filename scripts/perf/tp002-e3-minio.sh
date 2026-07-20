#!/usr/bin/env bash
# TP-002 E3 live MinIO release evidence wrapper.
#
# Runs the object-log matrix harness over both projection variants:
#   - object_log_inmemory_projection
#   - object_log_sqlite_projection
#
# Each profile is measured at 1/5/20/100ms-equivalent commit-latency bounds.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)

if [ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]; then
  echo "E3 provenance check failed: worktree must be clean before release measurement" >&2
  exit 2
fi
SOURCE_REVISION=$(git -C "$REPO_ROOT" rev-parse HEAD)

PQUEUE_E3_RESIDENT=${PQUEUE_E3_RESIDENT:-10000000}
PQUEUE_E3_LOAD_BATCH=${PQUEUE_E3_LOAD_BATCH:-1000}
PQUEUE_E3_ACK_PUSHES=${PQUEUE_E3_ACK_PUSHES:-100000}
PQUEUE_E3_ACK_CONCURRENCY=${PQUEUE_E3_ACK_CONCURRENCY:-384}
PQUEUE_E3_LOAD_CONCURRENCY=${PQUEUE_E3_LOAD_CONCURRENCY:-8}
PQUEUE_RECOVERY_MAX_TAIL_COMMANDS=${PQUEUE_RECOVERY_MAX_TAIL_COMMANDS:-1000000}
if [[ "$PQUEUE_E3_RESIDENT" != 10000000 || "$PQUEUE_E3_LOAD_BATCH" != 1000 \
   || "$PQUEUE_E3_ACK_PUSHES" != 100000 || "$PQUEUE_E3_ACK_CONCURRENCY" != 384 \
   || "$PQUEUE_E3_LOAD_CONCURRENCY" != 8 || "$PQUEUE_RECOVERY_MAX_TAIL_COMMANDS" != 1000000 ]]; then
  echo "E3 shape check failed: release requires resident=10000000 load_batch=1000 ack_pushes=100000 ack_concurrency=384 load_concurrency=8 recovery_max_tail_commands=1000000" >&2
  exit 2
fi

: "${PQUEUE_S3_TEST_ENDPOINT:?set PQUEUE_S3_TEST_ENDPOINT to the MinIO endpoint, e.g. http://<container-ip>:9000}"
: "${PQUEUE_E3_MINIO_CONTAINER:?set PQUEUE_E3_MINIO_CONTAINER to the fresh MinIO container name}"
: "${PQUEUE_E3_POSTGRES_POINTER_DATABASE_URL:?set the Postgres DSN used for the executed no-CAS pointer fence proof}"
: "${PQUEUE_E3_FENCE_EVIDENCE_OUT:?set the output path for source-bound fencing.json evidence}"
: "${PQUEUE_E3_TRANSACTION_EVIDENCE_OUT:?set the output path for the executed 48-row TP-003 matrix}"
PQUEUE_E3_RECORDED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

MINIO_IP=$(docker inspect "$PQUEUE_E3_MINIO_CONTAINER" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
MINIO_TMPFS=$(docker inspect "$PQUEUE_E3_MINIO_CONTAINER" --format '{{index .HostConfig.Tmpfs "/data"}}')
if [ "$PQUEUE_S3_TEST_ENDPOINT" != "http://$MINIO_IP:9000" ]; then
  echo "E3 topology check failed: endpoint does not match container bridge IP" >&2
  exit 2
fi
case ",$MINIO_TMPFS," in
  *,size=8g,*) ;;
  *) echo "E3 topology check failed: /data is not an 8g tmpfs" >&2; exit 2 ;;
esac

set +e
PQUEUE_E3_SOURCE_REVISION="$SOURCE_REVISION" \
PQUEUE_E3_RECORDED_AT="$PQUEUE_E3_RECORDED_AT" \
PQUEUE_E3_TRANSACTION_EVIDENCE_OUT="$PQUEUE_E3_TRANSACTION_EVIDENCE_OUT" \
cargo test -p pqueue-conformance --release --test external_transaction_contract_matrix_tests \
  e3_governed_transaction_evidence_matrix -- --nocapture
TXN_STATUS=$?
if [ "$TXN_STATUS" -ne 0 ]; then
  set -e
  exit "$TXN_STATUS"
fi

PQUEUE_PERF_ENV=${PQUEUE_PERF_ENV:-1} \
PQUEUE_E3_RESIDENT="$PQUEUE_E3_RESIDENT" \
PQUEUE_E3_LOAD_BATCH="$PQUEUE_E3_LOAD_BATCH" \
PQUEUE_E3_ACK_PUSHES="$PQUEUE_E3_ACK_PUSHES" \
PQUEUE_E3_ACK_CONCURRENCY="$PQUEUE_E3_ACK_CONCURRENCY" \
PQUEUE_E3_LOAD_CONCURRENCY="$PQUEUE_E3_LOAD_CONCURRENCY" \
PQUEUE_RECOVERY_MAX_TAIL_COMMANDS="$PQUEUE_RECOVERY_MAX_TAIL_COMMANDS" \
PQUEUE_E3_STORAGE_TOPOLOGY="wrapper-verified MinIO /data 8g tmpfs; live HTTP/S3 semantics; host durability and restart excluded" \
PQUEUE_E3_STORAGE_TOPOLOGY_ID=minio-tmpfs-8g \
PQUEUE_E3_STORAGE_DURABILITY_CLAIM=excluded \
PQUEUE_E3_SOURCE_REVISION="$SOURCE_REVISION" \
PQUEUE_E3_POSTGRES_POINTER_DATABASE_URL="$PQUEUE_E3_POSTGRES_POINTER_DATABASE_URL" \
PQUEUE_E3_FENCE_EVIDENCE_OUT="$PQUEUE_E3_FENCE_EVIDENCE_OUT" \
PQUEUE_S3_TEST_ENDPOINT="$PQUEUE_S3_TEST_ENDPOINT" \
PQUEUE_S3_TEST_BUCKET=${PQUEUE_S3_TEST_BUCKET:-pqueue-test} \
PQUEUE_S3_TEST_ACCESS_KEY=${PQUEUE_S3_TEST_ACCESS_KEY:-minioadmin} \
PQUEUE_S3_TEST_SECRET_KEY=${PQUEUE_S3_TEST_SECRET_KEY:-minioadmin} \
cargo test -p pqueue-server --release --test performance_object_log_e3_live_tests -- --nocapture
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
