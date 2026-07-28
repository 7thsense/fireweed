#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
OUTPUT=$(mktemp)
CARGO_ARGS_OUTPUT=$(mktemp)
EVENT_LOG=$(mktemp)
EVIDENCE_ROOT=$(mktemp -d)
DRIFT_EVIDENCE_ROOT=$(mktemp -d)
PREFLIGHT_EVIDENCE_ROOT=$(mktemp -d)
DRIFT_SENTINEL="$REPO_ROOT/.e3-s3-wrapper-drift-test-$$"
trap 'rm -rf "$EVIDENCE_ROOT" "$DRIFT_EVIDENCE_ROOT" "$PREFLIGHT_EVIDENCE_ROOT"; rm -f "$OUTPUT" "$CARGO_ARGS_OUTPUT" "$EVENT_LOG" "$DRIFT_SENTINEL"' EXIT

COMMON_ENV=(
  FIREWEED_S3_TEST_ENDPOINT=http://127.0.0.2:3900
  FIREWEED_S3_TEST_REGION=test-region-1
  FIREWEED_S3_TEST_BUCKET=fireweed-e3
  FIREWEED_S3_TEST_ACCESS_KEY=test-access
  FIREWEED_S3_TEST_SECRET_KEY=fixture-super-secret
  FIREWEED_E3_STORAGE_TOPOLOGY_ID=generic-s3-test
  FIREWEED_E3_STORAGE_TOPOLOGY="test-only S3 topology; durability excluded"
  FIREWEED_E3_STORAGE_DURABILITY_CLAIM=excluded
  FIREWEED_E3_POSTGRES_POINTER_DATABASE_URL=postgres://test.invalid/e3
  FIREWEED_E3_S3_BUCKET_MODE=create
  FIREWEED_E3_S3_BUCKET_ACK=fireweed-e3
  FIREWEED_E3_RUN_ID=fixture-run-0001
  FIREWEED_E3_S3_PROVIDER_IDENTITY=fixture-s3-control-plane
  FIREWEED_E3_S3_PROVIDER_ADAPTER="$REPO_ROOT/scripts/perf/tests/fixtures/e3-s3-provider-adapter"
)

set +e
env "${COMMON_ENV[@]}" FIREWEED_E3_AUTHORITY_MODE=postgres-pointer \
  FIREWEED_E3_EVIDENCE_DIR="$PREFLIGHT_EVIDENCE_ROOT" \
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
: >"$EVENT_LOG"
env "${COMMON_ENV[@]}" \
  FIREWEED_E3_AUTHORITY_MODE=native-create-only \
  FIREWEED_E3_EVIDENCE_DIR="$EVIDENCE_ROOT" \
  FIREWEED_E3_CARGO_ARGS_OUT="$CARGO_ARGS_OUTPUT" \
  FIREWEED_E3_TEST_ARTIFACTS=1 \
  FIREWEED_E3_PROVIDER_FIXTURE_LOG="$EVENT_LOG" \
  FIREWEED_E3_CARGO_EVENT_LOG="$EVENT_LOG" \
  PATH="$REPO_ROOT/scripts/perf/tests/fixtures:$PATH" \
  "$REPO_ROOT/scripts/perf/tp002-e3-s3.sh" >"$OUTPUT" 2>&1

grep -Fq \
  "test -p fireweed-server --release --test performance_object_log_e3_live_tests performance_object_log_e3_live_tests -- --nocapture" \
  "$CARGO_ARGS_OUTPUT"
grep -qx 'capabilities' "$EVENT_LOG"
grep -qx 'create-bucket' "$EVENT_LOG"
grep -qx 'prefix-empty' "$EVENT_LOG"
grep -qx 'nonce-write-read' "$EVENT_LOG"
grep -qx 'nonce-validate' "$EVENT_LOG"
grep -qx 'cleanup-prefix' "$EVENT_LOG"
if [ "$(tail -n 1 "$EVENT_LOG")" != cleanup-prefix ]; then
  echo "expected cleanup only after the semantic verifier" >&2
  cat "$EVENT_LOG" >&2
  exit 1
fi
grep -q '^cargo run -q -p fireweed-release --bin fireweed-build-e3-contract ' "$EVENT_LOG"
test -s "$EVIDENCE_ROOT/composition-fingerprint.sha256"
test -s "$EVIDENCE_ROOT/e3-contract.json"
if grep -R -Fq fixture-super-secret "$EVIDENCE_ROOT" "$OUTPUT"; then
  echo "wrapper leaked a supplied secret into output or evidence" >&2
  exit 1
fi

: >"$OUTPUT"
: >"$EVENT_LOG"
set +e
env "${COMMON_ENV[@]}" \
  FIREWEED_E3_AUTHORITY_MODE=native-create-only \
  FIREWEED_E3_EVIDENCE_DIR="$PREFLIGHT_EVIDENCE_ROOT" \
  FIREWEED_E3_PROVIDER_FIXTURE_FAIL_ACTION=prefix-empty \
  FIREWEED_E3_PROVIDER_FIXTURE_LOG="$EVENT_LOG" \
  PATH="$REPO_ROOT/scripts/perf/tests/fixtures:$PATH" \
  "$REPO_ROOT/scripts/perf/tp002-e3-s3.sh" >"$OUTPUT" 2>&1
STATUS=$?
set -e
if [ "$STATUS" -ne 2 ]; then
  echo "expected nonempty-run-prefix rejection exit 2, got $STATUS" >&2
  cat "$OUTPUT" >&2
  exit 1
fi
grep -q 'provider adapter rejected prefix-empty; the run namespace has been preserved' "$OUTPUT"
if grep -qx cleanup-prefix "$EVENT_LOG"; then
  echo "failed preflight must preserve the provider namespace" >&2
  exit 1
fi

: >"$OUTPUT"
set +e
env "${COMMON_ENV[@]}" \
  FIREWEED_E3_AUTHORITY_MODE=native-create-only \
  FIREWEED_E3_EVIDENCE_DIR="$DRIFT_EVIDENCE_ROOT" \
  FIREWEED_E3_DRIFT_SENTINEL="$DRIFT_SENTINEL" \
  FIREWEED_E3_CARGO_ARGS_OUT="$CARGO_ARGS_OUTPUT" \
  FIREWEED_E3_PROVIDER_FIXTURE_LOG="$EVENT_LOG" \
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
