#!/usr/bin/env bash
set -euo pipefail

VERSION=""
COMMIT=""
DIST_DIR="target/release-dist"

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --version) VERSION="${2:-}"; shift 2 ;;
        --commit) COMMIT="${2:-}"; shift 2 ;;
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

python3 - "${DIST_DIR}/deployment-proof.json" "${DIST_DIR}/deployment-proof.md" \
    "${DIST_DIR}/pqueue-service-image.txt" "${VERSION}" "${COMMIT}" <<'PY'
import json
import pathlib
import re
import sys

proof_path, markdown_path, image_path = map(pathlib.Path, sys.argv[1:4])
version, expected_commit = sys.argv[4:6]

try:
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"invalid deployment proof JSON: {exc}")

image = {}
for line in image_path.read_text(encoding="utf-8").splitlines():
    if "=" in line:
        key, value = line.split("=", 1)
        image[key] = value

errors = []
if proof.get("schema") != "pqueue.deployment_proof.v1":
    errors.append("unexpected deployment proof schema")
if proof.get("status") != "passed" or proof.get("exit_status") != 0:
    errors.append("deployment proof status must be passed with exit_status 0")
if proof.get("chart", {}).get("version") != version:
    errors.append(f"deployment proof chart version must equal {version}")
package = pathlib.Path(proof.get("chart", {}).get("package", "unavailable")).name
if package != f"pqueue-{version}.tgz" or not proof.get("chart", {}).get("package_exists"):
    errors.append("deployment proof chart package is missing or mismatched")
digest = image.get("digest", "")
if not re.fullmatch(r"sha256:[0-9a-fA-F]{64}", digest):
    errors.append("image evidence digest is malformed")
if proof.get("image", {}).get("digest") != digest:
    errors.append("deployment proof image digest does not match image evidence")
if proof.get("image", {}).get("tag") != image.get("version_coordinate"):
    errors.append("deployment proof image tag does not match image evidence")
if proof.get("image", {}).get("coordinate") != image.get("digest_coordinate"):
    errors.append("deployment proof image coordinate does not match image evidence")
if image.get("version") != version:
    errors.append("container image evidence version does not match release version")
if image.get("source_commit") != proof.get("commit_sha"):
    errors.append("container image evidence commit does not match deployment proof")
if "unavailable" in {
    proof.get("image", {}).get("tag"),
    proof.get("image", {}).get("digest"),
    proof.get("image", {}).get("coordinate"),
}:
    errors.append("deployment proof image identity is unavailable")
if expected_commit and proof.get("commit_sha") != expected_commit:
    errors.append("deployment proof commit does not match expected release commit")

markdown = markdown_path.read_text(encoding="utf-8")
for value, label in [
    (proof.get("commit_sha", ""), "commit"),
    (version, "chart version"),
    (digest, "image digest"),
]:
    if not value or value not in markdown:
        errors.append(f"deployment proof Markdown omits {label}")

if errors:
    raise SystemExit("invalid deployment proof: " + "; ".join(errors))
PY

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
