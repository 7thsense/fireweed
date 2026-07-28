#!/usr/bin/env bash
# TP-002 E3 provider-neutral live S3 release evidence wrapper.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)

fail() {
  echo "E3 S3 safety check failed: $1" >&2
  exit 2
}

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
: "${FIREWEED_E3_AUTHORITY_MODE:?set native-create-only}"
: "${FIREWEED_E3_S3_BUCKET_MODE:?set preexisting or create}"
: "${FIREWEED_E3_S3_BUCKET_ACK:?acknowledge the exact isolated E3 bucket name}"
: "${FIREWEED_E3_RUN_ID:?set a unique E3 run id}"
: "${FIREWEED_E3_EVIDENCE_DIR:?set a newly created, empty evidence directory}"
: "${FIREWEED_E3_S3_PROVIDER_IDENTITY:?declare the provider/control-plane identity}"
: "${FIREWEED_E3_S3_PROVIDER_ADAPTER:?set the executable provider safety adapter}"

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
  *)
    echo "E3 authority check failed: authority mode must be native-create-only" >&2
    exit 2
    ;;
esac
case "$FIREWEED_E3_S3_BUCKET_MODE" in
  preexisting|create) ;;
  *) fail "bucket mode must be preexisting or create" ;;
esac
if [ "$FIREWEED_E3_S3_BUCKET_ACK" != "$FIREWEED_S3_TEST_BUCKET" ]; then
  fail "bucket acknowledgement must exactly match FIREWEED_S3_TEST_BUCKET"
fi
if [[ ! "$FIREWEED_E3_RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{7,63}$ ]]; then
  fail "run id must be an 8-64 character stable token"
fi
if [[ ! "$FIREWEED_E3_S3_PROVIDER_IDENTITY" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]{2,127}$ ]]; then
  fail "provider identity must be a 3-128 character non-secret token"
fi
if [[ ! -x "$FIREWEED_E3_S3_PROVIDER_ADAPTER" ]]; then
  fail "provider adapter must name an executable file"
fi
if [[ ! -d "$FIREWEED_E3_EVIDENCE_DIR" ]]; then
  fail "evidence directory must already exist"
fi
EVIDENCE_DIR=$(cd "$FIREWEED_E3_EVIDENCE_DIR" && pwd -P)
if [ "$EVIDENCE_DIR" = / ]; then
  fail "evidence directory must not be the filesystem root"
fi
if [ -n "$(find "$EVIDENCE_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
  fail "evidence directory must be empty for a fresh run"
fi

FIREWEED_E3_FENCE_EVIDENCE_OUT="$EVIDENCE_DIR/fencing.json"
FIREWEED_E3_TRANSACTION_EVIDENCE_OUT="$EVIDENCE_DIR/tp003-transaction.jsonl"
FIREWEED_LEDGER_DIR="$EVIDENCE_DIR/ledger"
CONTRACT_OUT="$EVIDENCE_DIR/e3-contract.json"
COMPOSITION_OUT="$EVIDENCE_DIR/composition-fingerprint.sha256"
NONCE_OUT="$EVIDENCE_DIR/.runner-nonce"
for artifact in "$FIREWEED_E3_FENCE_EVIDENCE_OUT" "$FIREWEED_E3_TRANSACTION_EVIDENCE_OUT" "$CONTRACT_OUT" "$COMPOSITION_OUT" "$NONCE_OUT"; do
  if [ -e "$artifact" ]; then
    fail "fresh evidence artifact path already exists"
  fi
done
mkdir "$FIREWEED_LEDGER_DIR"

RUN_PREFIX="fireweed-e3-control/v1/${SOURCE_REVISION:0:12}/${FIREWEED_E3_RUN_ID}/"
NONCE=$(od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]')
if [ "${#NONCE}" -ne 32 ]; then
  fail "could not create a fresh provider nonce"
fi
printf '%s\n' "$NONCE" >"$NONCE_OUT"
COMPOSITION_FINGERPRINT=$(printf '%s\0' \
  'tp002-e3-s3-runner-v1' "$SOURCE_REVISION" "$FIREWEED_E3_S3_BUCKET_MODE" \
  "$FIREWEED_E3_RUN_ID" "$FIREWEED_E3_S3_PROVIDER_IDENTITY" "$FIREWEED_E3_STORAGE_TOPOLOGY_ID" \
  "$FIREWEED_E3_AUTHORITY_MODE" "$RUN_PREFIX" "$FIREWEED_S3_TEST_ENDPOINT" \
  "$FIREWEED_S3_TEST_REGION" "$FIREWEED_S3_TEST_BUCKET" | sha256sum | awk '{print $1}')
printf '%s\n' "$COMPOSITION_FINGERPRINT" >"$COMPOSITION_OUT"

provider_call() {
  local action=$1
  local adapter_output
  adapter_output=$(mktemp "${TMPDIR:-/tmp}/fireweed-e3-provider-${action}.XXXXXX")
  if ! "$FIREWEED_E3_S3_PROVIDER_ADAPTER" "$action" \
    --provider-identity "$FIREWEED_E3_S3_PROVIDER_IDENTITY" \
    --endpoint "$FIREWEED_S3_TEST_ENDPOINT" \
    --region "$FIREWEED_S3_TEST_REGION" \
    --bucket "$FIREWEED_S3_TEST_BUCKET" \
    --bucket-mode "$FIREWEED_E3_S3_BUCKET_MODE" \
    --bucket-ack "$FIREWEED_E3_S3_BUCKET_ACK" \
    --run-id "$FIREWEED_E3_RUN_ID" \
    --run-prefix "$RUN_PREFIX" \
    --nonce "$NONCE" >"$adapter_output" 2>&1; then
    rm -f "$adapter_output"
    fail "provider adapter rejected $action; the run namespace has been preserved"
  fi
  rm -f "$adapter_output"
}

# The adapter is the provider-specific authority: it must authenticate using inherited credentials, verify
# its declared identity/capabilities, and never print credentials. The generic wrapper never passes secrets
# as command arguments or persists adapter output.
provider_call capabilities
if [ "$FIREWEED_E3_S3_BUCKET_MODE" = create ]; then
  provider_call create-bucket
fi
provider_call prefix-empty
provider_call nonce-write-read

FIREWEED_E3_RECORDED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

set +e
env \
  FIREWEED_E3_SOURCE_REVISION="$SOURCE_REVISION" \
  FIREWEED_E3_RUN_ID="$FIREWEED_E3_RUN_ID" \
  FIREWEED_E3_COMPOSITION_FINGERPRINT="$COMPOSITION_FINGERPRINT" \
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
  FIREWEED_E3_FENCE_EVIDENCE_OUT="$FIREWEED_E3_FENCE_EVIDENCE_OUT" \
  FIREWEED_LEDGER_DIR="$FIREWEED_LEDGER_DIR" \
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

LEDGER_OUT="$FIREWEED_LEDGER_DIR/performance_object_log_e3_live_tests.jsonl"
for artifact in "$FIREWEED_E3_FENCE_EVIDENCE_OUT" "$FIREWEED_E3_TRANSACTION_EVIDENCE_OUT" "$LEDGER_OUT"; do
  if [[ ! -f "$artifact" || ! -s "$artifact" ]]; then
    fail "required fresh evidence artifact was not produced"
  fi
done
if [ -e "$CONTRACT_OUT" ]; then
  fail "fresh contract artifact path already exists"
fi

# This recomputes the E3 ledger, TP-003 matrix, and executed fence proof. It must succeed before cleanup.
cargo run -q -p fireweed-release --bin fireweed-build-e3-contract -- \
  --out "$CONTRACT_OUT" \
  --source-revision "$SOURCE_REVISION" \
  --e3-ledger "$LEDGER_OUT" \
  --transaction-evidence "$FIREWEED_E3_TRANSACTION_EVIDENCE_OUT" \
  --fencing-evidence "$FIREWEED_E3_FENCE_EVIDENCE_OUT" \
  --run-id "$FIREWEED_E3_RUN_ID" \
  --composition-fingerprint "$COMPOSITION_FINGERPRINT" \
  --authority-mode "$FIREWEED_E3_AUTHORITY_MODE"
if [[ ! -f "$CONTRACT_OUT" || ! -s "$CONTRACT_OUT" ]]; then
  fail "semantic verifier did not produce a fresh E3 contract"
fi
provider_call nonce-validate

# Cleanup is deliberately last. The adapter is permitted to list/delete only RUN_PREFIX and must re-list
# it empty; any earlier failure leaves the namespace for investigation. It never receives a bucket-delete action.
provider_call cleanup-prefix
