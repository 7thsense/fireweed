#!/usr/bin/env bash
set -euo pipefail

IMAGE=""
VERSION=""
VERSION_TAG=""
SHA_TAG=""
DIGEST=""
COMMIT=""
OUTPUT="target/release-dist/fireweed-service-image.txt"
DOCKERFILE="Dockerfile"

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --image) IMAGE="${2:-}"; shift 2 ;;
        --version) VERSION="${2:-}"; shift 2 ;;
        --version-tag) VERSION_TAG="${2:-}"; shift 2 ;;
        --sha-tag) SHA_TAG="${2:-}"; shift 2 ;;
        --digest) DIGEST="${2:-}"; shift 2 ;;
        --commit) COMMIT="${2:-}"; shift 2 ;;
        --dockerfile) DOCKERFILE="${2:-}"; shift 2 ;;
        --output) OUTPUT="${2:-}"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

for required in IMAGE VERSION VERSION_TAG SHA_TAG DIGEST COMMIT OUTPUT; do
    if [[ -z "${!required}" ]]; then
        echo "missing required value: $required" >&2
        exit 2
    fi
done

if [[ "$DIGEST" != sha256:* ]]; then
    echo "container image digest must be a sha256 digest: $DIGEST" >&2
    exit 2
fi

mkdir -p "$(dirname "$OUTPUT")"

cat > "$OUTPUT" <<EOF
artifact=fireweed-service-container-image
image=${IMAGE}
version=${VERSION}
version_tag=${VERSION_TAG}
sha_tag=${SHA_TAG}
digest=${DIGEST}
digest_coordinate=${IMAGE}@${DIGEST}
version_coordinate=${VERSION_TAG}
sha_coordinate=${SHA_TAG}
source_commit=${COMMIT}
dockerfile=${DOCKERFILE}
EOF
