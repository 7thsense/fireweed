#!/usr/bin/env bash
# Provider-safety adapter for local MinIO used by scripts/perf/tp002-e3-s3.sh.
#
# Authenticates via dockerized minio/mc on the same Docker bridge as the MinIO
# container. Credentials are read only from the environment
# (FIREWEED_S3_TEST_ACCESS_KEY / FIREWEED_S3_TEST_SECRET_KEY) and never printed.
set -euo pipefail

action=${1:?action required}
shift

PROVIDER_IDENTITY=
ENDPOINT=
REGION=
BUCKET=
BUCKET_MODE=
BUCKET_ACK=
RUN_ID=
RUN_PREFIX=
NONCE=

while [[ $# -gt 0 ]]; do
  case "$1" in
    --provider-identity) PROVIDER_IDENTITY=$2; shift 2 ;;
    --endpoint) ENDPOINT=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
    --bucket) BUCKET=$2; shift 2 ;;
    --bucket-mode) BUCKET_MODE=$2; shift 2 ;;
    --bucket-ack) BUCKET_ACK=$2; shift 2 ;;
    --run-id) RUN_ID=$2; shift 2 ;;
    --run-prefix) RUN_PREFIX=$2; shift 2 ;;
    --nonce) NONCE=$2; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

: "${PROVIDER_IDENTITY:?}"
: "${ENDPOINT:?}"
: "${BUCKET:?}"
: "${BUCKET_ACK:?}"
: "${RUN_PREFIX:?}"
: "${FIREWEED_S3_TEST_ACCESS_KEY:?FIREWEED_S3_TEST_ACCESS_KEY required in environment}"
: "${FIREWEED_S3_TEST_SECRET_KEY:?FIREWEED_S3_TEST_SECRET_KEY required in environment}"

if [ "$BUCKET_ACK" != "$BUCKET" ]; then
  echo "bucket ack mismatch" >&2
  exit 2
fi
if [ -z "$RUN_PREFIX" ] || [ "$RUN_PREFIX" = / ]; then
  echo "run prefix must be nonempty and not bucket root" >&2
  exit 2
fi
case "$RUN_PREFIX" in
  */) ;;
  *) echo "run prefix must end with /" >&2; exit 2 ;;
esac

# Endpoint must be host:port reachable from a docker-run mc container.
# Prefer an explicit container name when the MinIO convenience topology is used.
MC_IMAGE=${FIREWEED_E3_MC_IMAGE:-minio/mc:latest}
MINIO_CONTAINER=${FIREWEED_E3_MINIO_CONTAINER:-}
MC_NETWORK=${FIREWEED_E3_MC_NETWORK:-}
MC_ENDPOINT=$ENDPOINT

if [ -n "$MINIO_CONTAINER" ]; then
  if ! docker inspect "$MINIO_CONTAINER" >/dev/null 2>&1; then
    echo "MinIO container $MINIO_CONTAINER not found" >&2
    exit 2
  fi
  MC_NETWORK=$(docker inspect "$MINIO_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
  MINIO_IP=$(docker inspect "$MINIO_CONTAINER" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
  MC_ENDPOINT="http://${MINIO_IP}:9000"
fi

if [ -z "$MC_NETWORK" ]; then
  # Fall back to host networking only when ports are published.
  MC_NETWORK=host
fi

mc_host_url() {
  # MC_HOST_<alias>=http://access:secret@host:port — never log this value.
  printf 'http://%s:%s@%s' \
    "$FIREWEED_S3_TEST_ACCESS_KEY" \
    "$FIREWEED_S3_TEST_SECRET_KEY" \
    "${MC_ENDPOINT#http://}"
}

mc() {
  docker run --rm --network "$MC_NETWORK" \
    -e "MC_HOST_e3=$(mc_host_url)" \
    "$MC_IMAGE" "$@"
}

# stdin → object (needs docker -i for the pipe).
mc_pipe() {
  local target=$1
  docker run --rm -i --network "$MC_NETWORK" \
    -e "MC_HOST_e3=$(mc_host_url)" \
    "$MC_IMAGE" pipe "$target" >/dev/null
}

mc_quiet() {
  mc "$@" >/dev/null
}

ensure_bucket() {
  mc_quiet mb --ignore-existing "e3/${BUCKET}"
}

case "$action" in
  capabilities)
    # capabilities runs before create-bucket in the governed wrapper; ensure the
    # acknowledged bucket exists, then prove write/delete under the run prefix.
    ensure_bucket
    probe="${RUN_PREFIX}.adapter-capabilities-probe"
    printf 'capabilities-ok\n' | mc_pipe "e3/${BUCKET}/${probe}"
    mc_quiet rm --force "e3/${BUCKET}/${probe}"
    ;;
  create-bucket)
    ensure_bucket
    ;;
  prefix-empty)
    ensure_bucket
    # Reject nonempty run prefix (must be empty before measurement).
    listing=$(mc ls "e3/${BUCKET}/${RUN_PREFIX}" 2>/dev/null || true)
    if [ -n "$listing" ]; then
      echo "run prefix is not empty" >&2
      exit 1
    fi
    ;;
  nonce-write-read)
    : "${NONCE:?nonce required}"
    ensure_bucket
    key="${RUN_PREFIX}.runner-nonce"
    printf '%s' "$NONCE" | mc_pipe "e3/${BUCKET}/${key}"
    got=$(mc cat "e3/${BUCKET}/${key}" 2>/dev/null || true)
    if [ "$got" != "$NONCE" ]; then
      echo "nonce round-trip mismatch" >&2
      exit 1
    fi
    ;;
  nonce-validate)
    : "${NONCE:?nonce required}"
    key="${RUN_PREFIX}.runner-nonce"
    got=$(mc cat "e3/${BUCKET}/${key}" 2>/dev/null || true)
    if [ "$got" != "$NONCE" ]; then
      echo "nonce validation failed" >&2
      exit 1
    fi
    ;;
  cleanup-prefix)
    # Delete only objects under RUN_PREFIX; never the bucket root.
    mc_quiet rm --recursive --force --dangerous "e3/${BUCKET}/${RUN_PREFIX}" || true
    listing=$(mc ls "e3/${BUCKET}/${RUN_PREFIX}" 2>/dev/null || true)
    if [ -n "$listing" ]; then
      echo "cleanup left objects under run prefix" >&2
      exit 1
    fi
    ;;
  *)
    echo "unsupported action: $action" >&2
    exit 2
    ;;
esac
