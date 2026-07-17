#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
DIRTY_SENTINEL="$REPO_ROOT/.e3-wrapper-dirty-test-$$"
DRIFT_SENTINEL="$REPO_ROOT/.e3-wrapper-drift-test-$$"
OUTPUT=$(mktemp)
trap 'rm -f "$DIRTY_SENTINEL" "$DRIFT_SENTINEL" "$OUTPUT"' EXIT

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
PATH="$REPO_ROOT/scripts/perf/tests/fixtures:$PATH" \
PQUEUE_E3_DRIFT_SENTINEL="$DRIFT_SENTINEL" \
PQUEUE_E3_MINIO_CONTAINER=fake-minio \
PQUEUE_S3_TEST_ENDPOINT=http://127.0.0.2:9000 \
"$REPO_ROOT/scripts/perf/tp002-e3-minio.sh" >"$OUTPUT" 2>&1
STATUS=$?
set -e

if [ "$STATUS" -ne 2 ]; then
  echo "expected end-of-run source-drift rejection exit 2, got $STATUS" >&2
  cat "$OUTPUT" >&2
  exit 1
fi
grep -q "worktree changed during release measurement" "$OUTPUT"
