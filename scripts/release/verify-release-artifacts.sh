#!/usr/bin/env bash
set -euo pipefail

VERSION=""
DIST_DIR="target/release-dist"

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --version) VERSION="${2:-}"; shift 2 ;;
        --dist|--dist-dir) DIST_DIR="${2:-}"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$VERSION" ]]; then
    echo "missing required value: --version" >&2
    exit 2
fi

fail() {
    echo "$1" >&2
    exit 1
}

require_file() {
    [[ -f "$1" ]] || fail "missing required release artifact: $1"
}

checksum_has_artifact() {
    local artifact="$1"
    awk -v artifact="$artifact" '
        NF >= 2 {
            name = $2
            sub(/^\*/, "", name)
            if (name == artifact) {
                found = 1
            }
        }
        END { exit found ? 0 : 1 }
    ' "${DIST_DIR}/SHA256SUMS"
}

shopt -s nullglob
binary_archives=("${DIST_DIR}/pqueue-${VERSION}-"*.tar.gz)
chart_packages=("${DIST_DIR}/pqueue-${VERSION}.tgz")
shopt -u nullglob

[[ "${#binary_archives[@]}" -gt 0 ]] || fail "missing binary archive matching ${DIST_DIR}/pqueue-${VERSION}-*.tar.gz"
[[ "${#chart_packages[@]}" -gt 0 ]] || fail "missing Helm chart package matching ${DIST_DIR}/pqueue-${VERSION}.tgz"

require_file "${DIST_DIR}/pqueue-helm-chart.txt"
require_file "${DIST_DIR}/pqueue-service-image.txt"
require_file "${DIST_DIR}/deployment-proof.json"
require_file "${DIST_DIR}/deployment-proof.md"
require_file "${DIST_DIR}/SHA256SUMS"

grep -Eq '^digest=sha256:[0-9a-fA-F]{64}$' "${DIST_DIR}/pqueue-service-image.txt" \
    || fail "image evidence must contain a sha256 digest"

for artifact in "${DIST_DIR}"/*; do
    [[ -f "$artifact" ]] || continue
    artifact_name="$(basename "$artifact")"
    [[ "$artifact_name" == "SHA256SUMS" ]] && continue
    checksum_has_artifact "$artifact_name" || fail "SHA256SUMS missing entry for ${artifact_name}"
done

while read -r _ artifact_name _; do
    [[ -n "${artifact_name:-}" ]] || continue
    artifact_name="${artifact_name#\*}"
    require_file "${DIST_DIR}/${artifact_name}"
done < "${DIST_DIR}/SHA256SUMS"

if command -v sha256sum >/dev/null 2>&1; then
    (cd "$DIST_DIR" && sha256sum -c SHA256SUMS >/dev/null)
else
    (cd "$DIST_DIR" && shasum -a 256 -c SHA256SUMS >/dev/null)
fi

echo "release artifact set verified in ${DIST_DIR}"
