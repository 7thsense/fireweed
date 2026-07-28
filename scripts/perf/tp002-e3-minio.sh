#!/usr/bin/env bash
# Local MinIO convenience profile for the provider-neutral TP-002 E3 S3 wrapper.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)

if [ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]; then
  echo "E3 provenance check failed: worktree must be clean before release measurement" >&2
  exit 2
fi

: "${FIREWEED_S3_TEST_ENDPOINT:?set FIREWEED_S3_TEST_ENDPOINT to the MinIO endpoint, e.g. http://<container-ip>:9000}"
: "${FIREWEED_E3_MINIO_CONTAINER:?set FIREWEED_E3_MINIO_CONTAINER to the fresh MinIO container name}"

MINIO_IP=$(docker inspect "$FIREWEED_E3_MINIO_CONTAINER" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
MINIO_TMPFS=$(docker inspect "$FIREWEED_E3_MINIO_CONTAINER" --format '{{index .HostConfig.Tmpfs "/data"}}')
if [ "$FIREWEED_S3_TEST_ENDPOINT" != "http://$MINIO_IP:9000" ]; then
  echo "E3 topology check failed: endpoint does not match container bridge IP" >&2
  exit 2
fi
case ",$MINIO_TMPFS," in
  *,size=16g,*) ;;
  *) echo "E3 topology check failed: /data is not a 16g tmpfs" >&2; exit 2 ;;
esac

export FIREWEED_S3_TEST_REGION=${FIREWEED_S3_TEST_REGION:-us-east-1}
export FIREWEED_S3_TEST_BUCKET=${FIREWEED_S3_TEST_BUCKET:-fireweed-test}
export FIREWEED_S3_TEST_ACCESS_KEY=${FIREWEED_S3_TEST_ACCESS_KEY:-minioadmin}
export FIREWEED_S3_TEST_SECRET_KEY=${FIREWEED_S3_TEST_SECRET_KEY:-minioadmin}
export FIREWEED_E3_STORAGE_TOPOLOGY="wrapper-verified MinIO /data 16g tmpfs; live HTTP/S3 semantics; host durability and restart excluded"
export FIREWEED_E3_STORAGE_TOPOLOGY_ID=minio-tmpfs-16g
export FIREWEED_E3_STORAGE_DURABILITY_CLAIM=excluded
export FIREWEED_E3_AUTHORITY_MODE=native-create-only

exec "$REPO_ROOT/scripts/perf/tp002-e3-s3.sh"
