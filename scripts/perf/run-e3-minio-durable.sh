#!/usr/bin/env bash
# Durable local E3 release runner: MinIO 16g tmpfs + real safety adapter.
#
# Survives SSH disconnects: nohup + log under target/e3-runs/.
# Requires a clean git worktree (release provenance).
#
# Usage:
#   scripts/perf/run-e3-minio-durable.sh
#   FIREWEED_E3_MINIO_CONTAINER=fireweed-e3-minio-16g scripts/perf/run-e3-minio-durable.sh
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
  echo "worktree must be clean for release E3 measurement" >&2
  exit 2
fi

MINIO_CONTAINER=${FIREWEED_E3_MINIO_CONTAINER:-fireweed-e3-minio-16g}
if ! docker inspect "$MINIO_CONTAINER" >/dev/null 2>&1; then
  echo "starting MinIO 16g tmpfs as $MINIO_CONTAINER" >&2
  docker run -d --name "$MINIO_CONTAINER" \
    --tmpfs /data:rw,size=16g \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data
  # wait for ready
  for _ in $(seq 1 60); do
    IP=$(docker inspect "$MINIO_CONTAINER" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
    if curl -fsS "http://${IP}:9000/minio/health/ready" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

MINIO_IP=$(docker inspect "$MINIO_CONTAINER" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
TMPFS=$(docker inspect "$MINIO_CONTAINER" --format '{{index .HostConfig.Tmpfs "/data"}}')
case ",$TMPFS," in
  *,size=16g,*) ;;
  *) echo "refusing: $MINIO_CONTAINER /data is not a 16g tmpfs ($TMPFS)" >&2; exit 2 ;;
esac

RUN_ID=${FIREWEED_E3_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-local}
REV=$(git rev-parse --short=12 HEAD)
# Governed wrapper requires EVIDENCE_DIR to be empty; keep runner bookkeeping
# in the parent run dir so launch.env / logs do not fail the emptiness check.
RUN_DIR=${FIREWEED_E3_RUN_DIR:-$REPO_ROOT/target/e3-runs/${REV}-${RUN_ID}}
EVIDENCE_DIR=${FIREWEED_E3_EVIDENCE_DIR:-$RUN_DIR/evidence}
mkdir -p "$REPO_ROOT/target/e3-runs" "$RUN_DIR"
if [ -e "$EVIDENCE_DIR" ] && [ -n "$(find "$EVIDENCE_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
  echo "evidence dir not empty: $EVIDENCE_DIR" >&2
  exit 2
fi
mkdir -p "$EVIDENCE_DIR"

LOG="$RUN_DIR/runner.log"
PID_FILE="$RUN_DIR/runner.pid"

export FIREWEED_S3_TEST_ENDPOINT="http://${MINIO_IP}:9000"
export FIREWEED_S3_TEST_REGION=${FIREWEED_S3_TEST_REGION:-us-east-1}
export FIREWEED_S3_TEST_BUCKET=${FIREWEED_S3_TEST_BUCKET:-fireweed-e3-release}
export FIREWEED_S3_TEST_ACCESS_KEY=${FIREWEED_S3_TEST_ACCESS_KEY:-minioadmin}
export FIREWEED_S3_TEST_SECRET_KEY=${FIREWEED_S3_TEST_SECRET_KEY:-minioadmin}
export FIREWEED_E3_MINIO_CONTAINER="$MINIO_CONTAINER"
export FIREWEED_E3_STORAGE_TOPOLOGY="wrapper-verified MinIO /data 16g tmpfs; live HTTP/S3 semantics; host durability and restart excluded"
export FIREWEED_E3_STORAGE_TOPOLOGY_ID=minio-tmpfs-16g
export FIREWEED_E3_STORAGE_DURABILITY_CLAIM=excluded
export FIREWEED_E3_AUTHORITY_MODE=native-create-only
export FIREWEED_E3_S3_BUCKET_MODE=create
export FIREWEED_E3_S3_BUCKET_ACK="$FIREWEED_S3_TEST_BUCKET"
export FIREWEED_E3_RUN_ID="$RUN_ID"
export FIREWEED_E3_EVIDENCE_DIR="$EVIDENCE_DIR"
export FIREWEED_E3_S3_PROVIDER_IDENTITY=minio-local-control-plane
export FIREWEED_E3_S3_PROVIDER_ADAPTER="$REPO_ROOT/scripts/perf/e3-minio-provider-adapter.sh"

# Canonical release shape (enforced by wrapper).
export FIREWEED_E3_RESIDENT=10000000
export FIREWEED_E3_LOAD_BATCH=1000
export FIREWEED_E3_ACK_PUSHES=100000
export FIREWEED_E3_ACK_CONCURRENCY=384
export FIREWEED_E3_LOAD_CONCURRENCY=8
export FIREWEED_RECOVERY_MAX_TAIL_COMMANDS=1000000

cat >"$RUN_DIR/launch.env" <<EOF
# non-secret launch record
endpoint=$FIREWEED_S3_TEST_ENDPOINT
bucket=$FIREWEED_S3_TEST_BUCKET
run_id=$RUN_ID
revision=$(git rev-parse HEAD)
minio_container=$MINIO_CONTAINER
evidence_dir=$EVIDENCE_DIR
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

echo "launching E3 into $EVIDENCE_DIR (log: $LOG)" >&2
nohup bash -c "
  set -euo pipefail
  cd '$REPO_ROOT'
  export FIREWEED_S3_TEST_ENDPOINT='$FIREWEED_S3_TEST_ENDPOINT'
  export FIREWEED_S3_TEST_REGION='$FIREWEED_S3_TEST_REGION'
  export FIREWEED_S3_TEST_BUCKET='$FIREWEED_S3_TEST_BUCKET'
  export FIREWEED_S3_TEST_ACCESS_KEY='$FIREWEED_S3_TEST_ACCESS_KEY'
  export FIREWEED_S3_TEST_SECRET_KEY='$FIREWEED_S3_TEST_SECRET_KEY'
  export FIREWEED_E3_MINIO_CONTAINER='$MINIO_CONTAINER'
  export FIREWEED_E3_STORAGE_TOPOLOGY='$FIREWEED_E3_STORAGE_TOPOLOGY'
  export FIREWEED_E3_STORAGE_TOPOLOGY_ID='$FIREWEED_E3_STORAGE_TOPOLOGY_ID'
  export FIREWEED_E3_STORAGE_DURABILITY_CLAIM=excluded
  export FIREWEED_E3_AUTHORITY_MODE=native-create-only
  export FIREWEED_E3_S3_BUCKET_MODE=create
  export FIREWEED_E3_S3_BUCKET_ACK='$FIREWEED_S3_TEST_BUCKET'
  export FIREWEED_E3_RUN_ID='$RUN_ID'
  export FIREWEED_E3_EVIDENCE_DIR='$EVIDENCE_DIR'
  export FIREWEED_E3_S3_PROVIDER_IDENTITY=minio-local-control-plane
  export FIREWEED_E3_S3_PROVIDER_ADAPTER='$FIREWEED_E3_S3_PROVIDER_ADAPTER'
  export FIREWEED_E3_RESIDENT=10000000
  export FIREWEED_E3_LOAD_BATCH=1000
  export FIREWEED_E3_ACK_PUSHES=100000
  export FIREWEED_E3_ACK_CONCURRENCY=384
  export FIREWEED_E3_LOAD_CONCURRENCY=8
  export FIREWEED_RECOVERY_MAX_TAIL_COMMANDS=1000000
  echo \"E3 start \$(date -u +%Y-%m-%dT%H:%M:%SZ)\" 
  '$REPO_ROOT/scripts/perf/tp002-e3-s3.sh'
  status=\$?
  echo \"E3 end \$(date -u +%Y-%m-%dT%H:%M:%SZ) status=\$status\"
  exit \$status
" >"$LOG" 2>&1 &
echo $! >"$PID_FILE"
echo "pid=$(cat "$PID_FILE") log=$LOG evidence=$EVIDENCE_DIR run_dir=$RUN_DIR"
echo "monitor: tail -F $LOG"
echo "status:  kill -0 \$(cat $PID_FILE) && echo running || echo exited"
