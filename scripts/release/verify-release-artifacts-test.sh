#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pqueue-release-artifacts-test.XXXXXX")"
trap 'rm -rf "${DIST_DIR}"' EXIT

VERSION="0.0.0-test"
touch "${DIST_DIR}/fireweed-${VERSION}-x86_64-linux.tar.gz"
touch "${DIST_DIR}/fireweed-queue-${VERSION}.tgz"
touch "${DIST_DIR}/fireweed-queue-helm-chart.txt"
cat > "${DIST_DIR}/fireweed-service-image.txt" <<EOF
version=${VERSION}
digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
source_commit=test-commit
version_coordinate=ghcr.io/example/fireweed-service:${VERSION}
digest_coordinate=ghcr.io/example/fireweed-service@sha256:0000000000000000000000000000000000000000000000000000000000000000
EOF
cat > "${DIST_DIR}/deployment-proof.json" <<EOF
{
  "schema": "fireweed.deployment_proof.v1",
  "status": "passed",
  "exit_status": 0,
  "commit_sha": "test-commit",
  "chart": {
    "version": "${VERSION}",
    "package": "target/release-dist/fireweed-queue-${VERSION}.tgz",
    "package_exists": true
  },
  "image": {
    "tag": "ghcr.io/example/fireweed-service:${VERSION}",
    "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    "coordinate": "ghcr.io/example/fireweed-service@sha256:0000000000000000000000000000000000000000000000000000000000000000"
  }
}
EOF
cat > "${DIST_DIR}/deployment-proof.md" <<EOF
# Deployment proof

- commit: test-commit
- chart: ${VERSION}
- image digest: sha256:0000000000000000000000000000000000000000000000000000000000000000
EOF
bash "${SCRIPT_DIR}/write-checksums.sh" "${DIST_DIR}"

bash "${SCRIPT_DIR}/verify-release-artifacts.sh" \
  --version "${VERSION}" \
  --commit "test-commit" \
  --dist "${DIST_DIR}"

VALID_PROOF_JSON="$(<"${DIST_DIR}/deployment-proof.json")"
VALID_PROOF_MD="$(<"${DIST_DIR}/deployment-proof.md")"
VALID_IMAGE_EVIDENCE="$(<"${DIST_DIR}/fireweed-service-image.txt")"

rm "${DIST_DIR}/deployment-proof.json"
if bash "${SCRIPT_DIR}/verify-release-artifacts.sh" \
  --version "${VERSION}" \
  --commit "test-commit" \
  --dist "${DIST_DIR}" >"${DIST_DIR}/missing-proof.out" 2>&1; then
  echo "verifier unexpectedly accepted a distribution without deployment-proof.json" >&2
  exit 1
fi

grep -F "missing required release artifact: ${DIST_DIR}/deployment-proof.json" \
  "${DIST_DIR}/missing-proof.out"

printf '%s\n' "${VALID_PROOF_JSON}" > "${DIST_DIR}/deployment-proof.json"
rm "${DIST_DIR}/deployment-proof.md"
if bash "${SCRIPT_DIR}/verify-release-artifacts.sh" \
  --version "${VERSION}" \
  --commit "test-commit" \
  --dist "${DIST_DIR}" >"${DIST_DIR}/missing-proof-md.out" 2>&1; then
  echo "verifier unexpectedly accepted a distribution without deployment-proof.md" >&2
  exit 1
fi
grep -F "missing required release artifact: ${DIST_DIR}/deployment-proof.md" \
  "${DIST_DIR}/missing-proof-md.out"

printf '%s\n' "${VALID_PROOF_MD}" > "${DIST_DIR}/deployment-proof.md"
git_like_json="${DIST_DIR}/deployment-proof.json"
printf '{"schema":"fireweed.deployment_proof.v1","status":"failed"}\n' > "${git_like_json}"
bash "${SCRIPT_DIR}/write-checksums.sh" "${DIST_DIR}"
if bash "${SCRIPT_DIR}/verify-release-artifacts.sh" \
  --version "${VERSION}" \
  --commit "test-commit" \
  --dist "${DIST_DIR}" >"${DIST_DIR}/invalid-proof.out" 2>&1; then
  echo "verifier unexpectedly accepted a failed deployment proof" >&2
  exit 1
fi
grep -F "deployment proof status must be passed" "${DIST_DIR}/invalid-proof.out"

printf '%s\n' "${VALID_PROOF_JSON}" > "${DIST_DIR}/deployment-proof.json"
printf '%s\n' "${VALID_IMAGE_EVIDENCE}" > "${DIST_DIR}/fireweed-service-image.txt"
sed -i \
  's#digest_coordinate=.*#digest_coordinate=ghcr.io/example/fireweed-service@sha256:1111111111111111111111111111111111111111111111111111111111111111#' \
  "${DIST_DIR}/fireweed-service-image.txt"
bash "${SCRIPT_DIR}/write-checksums.sh" "${DIST_DIR}"
if bash "${SCRIPT_DIR}/verify-release-artifacts.sh" \
  --version "${VERSION}" \
  --commit "test-commit" \
  --dist "${DIST_DIR}" >"${DIST_DIR}/mismatched-image.out" 2>&1; then
  echo "verifier unexpectedly accepted a mismatched image coordinate" >&2
  exit 1
fi
grep -F "deployment proof image coordinate does not match image evidence" \
  "${DIST_DIR}/mismatched-image.out"

echo "release artifact verifier tests passed"
