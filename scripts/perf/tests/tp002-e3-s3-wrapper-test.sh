#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
OUTPUT=$(mktemp)
FENCE_OUTPUT=$(mktemp)
TRANSACTION_OUTPUT=$(mktemp)
CARGO_ARGS_OUTPUT=$(mktemp)
DRIFT_SENTINEL="$REPO_ROOT/.e3-s3-wrapper-drift-test-$$"
trap 'rm -f "$OUTPUT" "$FENCE_OUTPUT" "$TRANSACTION_OUTPUT" "$CARGO_ARGS_OUTPUT" "$DRIFT_SENTINEL"' EXIT

COMMON_ENV=(
  FIREWEED_S3_TEST_ENDPOINT=http://127.0.0.2:3900
  FIREWEED_S3_TEST_REGION=test-region-1
  FIREWEED_S3_TEST_BUCKET=fireweed-e3
  FIREWEED_S3_TEST_ACCESS_KEY=test-access
  FIREWEED_S3_TEST_SECRET_KEY=test-secret
  FIREWEED_E3_STORAGE_TOPOLOGY_ID=generic-s3-test
  FIREWEED_E3_STORAGE_TOPOLOGY="test-only S3 topology; durability excluded"
  FIREWEED_E3_STORAGE_DURABILITY_CLAIM=excluded
  FIREWEED_E3_POSTGRES_POINTER_DATABASE_URL=postgres://test.invalid/e3
  FIREWEED_E3_FENCE_EVIDENCE_OUT="$FENCE_OUTPUT"
  FIREWEED_E3_TRANSACTION_EVIDENCE_OUT="$TRANSACTION_OUTPUT"
)

set +e
env "${COMMON_ENV[@]}" FIREWEED_E3_AUTHORITY_MODE=postgres-pointer \
  "$REPO_ROOT/scripts/perf/tp002-e3-s3.sh" >"$OUTPUT" 2>&1
STATUS=$?
set -e
if [ "$STATUS" -ne 2 ]; then
  echo "expected unsupported pointer-backed measurement rejection exit 2, got $STATUS" >&2
  cat "$OUTPUT" >&2
  exit 1
fi
grep -q "postgres-pointer measurement is not yet implemented" "$OUTPUT"

: >"$OUTPUT"
set +e
env "${COMMON_ENV[@]}" \
  FIREWEED_E3_AUTHORITY_MODE=native-create-only \
  FIREWEED_E3_DRIFT_SENTINEL="$DRIFT_SENTINEL" \
  FIREWEED_E3_CARGO_ARGS_OUT="$CARGO_ARGS_OUTPUT" \
  PATH="$REPO_ROOT/scripts/perf/tests/fixtures:$PATH" \
  "$REPO_ROOT/scripts/perf/tp002-e3-s3.sh" >"$OUTPUT" 2>&1
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
