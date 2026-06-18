#!/usr/bin/env bash
set -euo pipefail

VERSION=""
TAG=""
DIST_DIR="target/release-dist"
CHART_DIR="charts/pqueue"

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --version) VERSION="${2:-}"; shift 2 ;;
        --tag) TAG="${2:-}"; shift 2 ;;
        --destination) DIST_DIR="${2:-}"; shift 2 ;;
        --chart-dir) CHART_DIR="${2:-}"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$VERSION" ]]; then
    echo "missing required value: --version" >&2
    exit 2
fi

if [[ -z "$TAG" ]]; then
    TAG="v${VERSION}"
fi

require() {
    command -v "$1" >/dev/null 2>&1 || { echo "required tool not found: $1" >&2; exit 1; }
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

require helm

mkdir -p "$DIST_DIR"

package_path="$(helm package "$CHART_DIR" \
    --version "$VERSION" \
    --app-version "$VERSION" \
    --destination "$DIST_DIR" | awk -F': ' '/Successfully packaged chart and saved it to:/ {print $2}')"

if [[ -z "$package_path" || ! -f "$package_path" ]]; then
    echo "helm package did not produce an archive" >&2
    exit 1
fi

package_name="$(basename "$package_path")"
package_sha256="$(sha256_of "$package_path")"
evidence_path="${DIST_DIR}/pqueue-helm-chart.txt"

cat > "$evidence_path" <<EOF
artifact=pqueue-helm-chart
chart=pqueue
version=${VERSION}
app_version=${VERSION}
package=${package_name}
package_sha256=${package_sha256}
source_chart=${CHART_DIR}
release_tag=${TAG}
release_asset_coordinate=github-release:${TAG}/${package_name}
EOF

bash scripts/release/write-checksums.sh "$DIST_DIR"

echo "$package_path"
echo "$evidence_path"
echo "${DIST_DIR}/SHA256SUMS"
