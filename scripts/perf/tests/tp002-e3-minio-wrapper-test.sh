#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
DIRTY_SENTINEL="$REPO_ROOT/.e3-wrapper-dirty-test-$$"
DRIFT_SENTINEL="$REPO_ROOT/.e3-wrapper-drift-test-$$"
OUTPUT=$(mktemp)
FENCE_OUTPUT=$(mktemp)
TRANSACTION_OUTPUT=$(mktemp)
CARGO_ARGS_OUTPUT=$(mktemp)
EVIDENCE_ROOT=$(mktemp -d)
trap 'rm -rf "$EVIDENCE_ROOT"; rm -f "$DIRTY_SENTINEL" "$DRIFT_SENTINEL" "$OUTPUT" "$FENCE_OUTPUT" "$TRANSACTION_OUTPUT" "$CARGO_ARGS_OUTPUT"' EXIT

touch "$DIRTY_SENTINEL"
set +e
"$REPO_ROOT/scripts/perf/tp002-e3-minio.sh" >"$OUTPUT" 2>&1
STATUS=$?
set -e

if [ "$STATUS" -ne 2 ]; then
  echo "expected dirty-worktree rejection exit 2, got $STATUS" >&2
  cat "$OUTPUT" >&2
  exit 1
fi
grep -q "worktree must be clean before release measurement" "$OUTPUT"

rm -f "$DIRTY_SENTINEL"
: >"$OUTPUT"
set +e
FIREWEED_E3_RESIDENT=9999999 \
FIREWEED_E3_MINIO_CONTAINER=fake-minio \
FIREWEED_S3_TEST_ENDPOINT=http://127.0.0.2:9000 \
"$REPO_ROOT/scripts/perf/tp002-e3-minio.sh" >"$OUTPUT" 2>&1
STATUS=$?
set -e
if [ "$STATUS" -ne 2 ]; then
  echo "expected noncanonical-shape rejection exit 2, got $STATUS" >&2
  cat "$OUTPUT" >&2
  exit 1
fi
grep -q "release requires resident=10000000" "$OUTPUT"

: >"$OUTPUT"
set +e
PATH="$REPO_ROOT/scripts/perf/tests/fixtures:$PATH" \
FIREWEED_E3_DRIFT_SENTINEL="$DRIFT_SENTINEL" \
FIREWEED_E3_CARGO_ARGS_OUT="$CARGO_ARGS_OUTPUT" \
FIREWEED_E3_MINIO_CONTAINER=fake-minio \
FIREWEED_E3_FENCE_EVIDENCE_OUT="$FENCE_OUTPUT" \
FIREWEED_E3_TRANSACTION_EVIDENCE_OUT="$TRANSACTION_OUTPUT" \
FIREWEED_E3_S3_BUCKET_MODE=create \
FIREWEED_E3_S3_BUCKET_ACK=fireweed-test \
FIREWEED_E3_RUN_ID=minio-fixture-run-0001 \
FIREWEED_E3_EVIDENCE_DIR="$EVIDENCE_ROOT" \
FIREWEED_E3_S3_PROVIDER_IDENTITY=minio-fixture-control-plane \
FIREWEED_E3_S3_PROVIDER_ADAPTER="$REPO_ROOT/scripts/perf/tests/fixtures/e3-s3-provider-adapter" \
FIREWEED_S3_TEST_ENDPOINT=http://127.0.0.2:9000 \
"$REPO_ROOT/scripts/perf/tp002-e3-minio.sh" >"$OUTPUT" 2>&1
STATUS=$?
set -e

if [ "$STATUS" -ne 2 ]; then
  echo "expected end-of-run source-drift rejection exit 2, got $STATUS" >&2
  cat "$OUTPUT" >&2
  exit 1
fi
grep -q "worktree changed during release measurement" "$OUTPUT"
grep -Fq \
  "test -p fireweed-server --release --test performance_object_log_e3_live_tests performance_object_log_e3_live_tests -- --nocapture" \
  "$CARGO_ARGS_OUTPUT"
