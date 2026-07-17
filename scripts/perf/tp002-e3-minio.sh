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

: "${PQUEUE_S3_TEST_ENDPOINT:?set PQUEUE_S3_TEST_ENDPOINT to the MinIO endpoint, e.g. http://<container-ip>:9000}"

PQUEUE_PERF_ENV=${PQUEUE_PERF_ENV:-1} \
PQUEUE_E3_RESIDENT=${PQUEUE_E3_RESIDENT:-10000000} \
PQUEUE_E3_LOAD_BATCH=${PQUEUE_E3_LOAD_BATCH:-1000} \
PQUEUE_E3_ACK_PUSHES=${PQUEUE_E3_ACK_PUSHES:-100000} \
PQUEUE_E3_ACK_CONCURRENCY=${PQUEUE_E3_ACK_CONCURRENCY:-384} \
PQUEUE_E3_LOAD_CONCURRENCY=${PQUEUE_E3_LOAD_CONCURRENCY:-8} \
PQUEUE_S3_TEST_ENDPOINT="$PQUEUE_S3_TEST_ENDPOINT" \
PQUEUE_S3_TEST_BUCKET=${PQUEUE_S3_TEST_BUCKET:-pqueue-test} \
PQUEUE_S3_TEST_ACCESS_KEY=${PQUEUE_S3_TEST_ACCESS_KEY:-minioadmin} \
PQUEUE_S3_TEST_SECRET_KEY=${PQUEUE_S3_TEST_SECRET_KEY:-minioadmin} \
cargo test -p pqueue-server --release --test performance_object_log_e3_live_tests -- --nocapture
