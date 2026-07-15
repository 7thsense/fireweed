#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pqueue-release-artifacts-test.XXXXXX")"
trap 'rm -rf "${DIST_DIR}"' EXIT

VERSION="0.0.0-test"
touch "${DIST_DIR}/pqueue-${VERSION}-x86_64-linux.tar.gz"
touch "${DIST_DIR}/pqueue-${VERSION}.tgz"
touch "${DIST_DIR}/pqueue-helm-chart.txt"
printf 'digest=sha256:%064d\n' 0 > "${DIST_DIR}/pqueue-service-image.txt"
printf '{"status":"pass"}\n' > "${DIST_DIR}/deployment-proof.json"
printf '# Deployment proof\n' > "${DIST_DIR}/deployment-proof.md"
bash "${SCRIPT_DIR}/write-checksums.sh" "${DIST_DIR}"

bash "${SCRIPT_DIR}/verify-release-artifacts.sh" \
  --version "${VERSION}" \
  --dist "${DIST_DIR}"

rm "${DIST_DIR}/deployment-proof.json"
if bash "${SCRIPT_DIR}/verify-release-artifacts.sh" \
  --version "${VERSION}" \
  --dist "${DIST_DIR}" >"${DIST_DIR}/missing-proof.out" 2>&1; then
  echo "verifier unexpectedly accepted a distribution without deployment-proof.json" >&2
  exit 1
fi

grep -F "missing required release artifact: ${DIST_DIR}/deployment-proof.json" \
  "${DIST_DIR}/missing-proof.out"

echo "release artifact verifier tests passed"
