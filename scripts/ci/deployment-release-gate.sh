#!/usr/bin/env bash
# Local deployment release gate for pqueue.
#
# This gate composes the existing source/release checks with deployment checks.
# The only tolerated local skip is the disposable kind backend matrix when the
# local Docker/kind toolchain is unavailable or Docker cannot be used.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_ROOT}/charts/pqueue"
PROOF_DIR="${PQUEUE_DEPLOYMENT_PROOF_DIR:-${REPO_ROOT}/target/deployment-release-gate}"
PACKAGE_DIR="${PROOF_DIR}/release-dist"
PROOF_JSON="${PROOF_DIR}/deployment-proof.json"
PROOF_MD="${PROOF_DIR}/deployment-proof.md"
COMMAND_LOG="${PROOF_DIR}/commands.tsv"
BACKEND_LOG="${PROOF_DIR}/backend-profiles.tsv"
SKIP_LOG="${PROOF_DIR}/local-skips.txt"
SUPPORT_LOG="${PROOF_DIR}/supporting-artifacts.tsv"

BACKENDS=(postgres_native object_log_sqlite_projection)
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.31.0}"
export KIND_NODE_IMAGE

err() { echo "deployment-release-gate: $*" >&2; }

command_display() {
    local out="" quoted arg
    for arg in "$@"; do
        printf -v quoted '%q' "$arg"
        out+=" ${quoted}"
    done
    printf '%s' "${out# }"
}

init_proof_logs() {
    rm -rf "${PROOF_DIR}"
    mkdir -p "${PROOF_DIR}"
    : >"${COMMAND_LOG}"
    : >"${BACKEND_LOG}"
    : >"${SKIP_LOG}"
    : >"${SUPPORT_LOG}"
}

record_command() {
    local status="$1" display="$2"
    shift 2
    printf '%s\t%s' "${status}" "${display}" >>"${COMMAND_LOG}"
    local arg
    for arg in "$@"; do
        printf '\t%s' "${arg}" >>"${COMMAND_LOG}"
    done
    printf '\n' >>"${COMMAND_LOG}"
}

record_backend_profile() {
    local profile="$1" status="$2" reason="${3:-}"
    printf '%s\t%s\t%s\n' "${profile}" "${status}" "${reason}" >>"${BACKEND_LOG}"
}

record_skip() {
    printf '%s\n' "$1" >>"${SKIP_LOG}"
}

record_supporting_artifact() {
    local path="$1" description="$2"
    printf '%s\t%s\n' "${path}" "${description}" >>"${SUPPORT_LOG}"
}

run_cmd() {
    local display status
    display="$(command_display "$@")"
    printf '+++'
    printf ' %q' "$@"
    printf '\n'
    set +e
    "$@"
    status=$?
    set -e
    record_command "${status}" "${display}" "$@"
    return "${status}"
}

run_cmd_capture() {
    local capture_path="$1"
    shift
    local display status
    display="$(command_display "$@")"
    printf '+++'
    printf ' %q' "$@"
    printf '\n'
    set +e
    "$@" 2>&1 | tee "${capture_path}"
    status=${PIPESTATUS[0]}
    set -e
    record_command "${status}" "${display}" "$@"
    return "${status}"
}

run_step() {
    local label="$1"
    shift
    local status
    echo "+++ ${label}"
    set +e
    "$@"
    status=$?
    set -e
    record_command "${status}" "${label}" "${label}"
    return "${status}"
}

chart_version() {
    awk -F': *' '$1 == "version" { print $2; exit }' "${CHART_DIR}/Chart.yaml"
}

write_deployment_proof() {
    local exit_code="$1"
    local version commit
    version="$(chart_version 2>/dev/null || true)"
    commit="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || printf 'unavailable')"

    DEPLOYMENT_PROOF_EXIT_CODE="${exit_code}" \
    DEPLOYMENT_PROOF_COMMIT="${commit}" \
    DEPLOYMENT_PROOF_CHART_VERSION="${version:-unavailable}" \
    DEPLOYMENT_PROOF_BACKENDS="${BACKENDS[*]}" \
    DEPLOYMENT_PROOF_COMMAND_LOG="${COMMAND_LOG}" \
    DEPLOYMENT_PROOF_BACKEND_LOG="${BACKEND_LOG}" \
    DEPLOYMENT_PROOF_SKIP_LOG="${SKIP_LOG}" \
    DEPLOYMENT_PROOF_SUPPORT_LOG="${SUPPORT_LOG}" \
    DEPLOYMENT_PROOF_JSON="${PROOF_JSON}" \
    DEPLOYMENT_PROOF_MD="${PROOF_MD}" \
    DEPLOYMENT_PROOF_PACKAGE_DIR="${PACKAGE_DIR}" \
    DEPLOYMENT_PROOF_REPO_ROOT="${REPO_ROOT}" \
    python3 - <<'PY'
import json
import os
import shlex
from pathlib import Path

repo_root = Path(os.environ["DEPLOYMENT_PROOF_REPO_ROOT"])
proof_json = Path(os.environ["DEPLOYMENT_PROOF_JSON"])
proof_md = Path(os.environ["DEPLOYMENT_PROOF_MD"])
package_dir = Path(os.environ["DEPLOYMENT_PROOF_PACKAGE_DIR"])
chart_version = os.environ["DEPLOYMENT_PROOF_CHART_VERSION"]
exit_code = int(os.environ["DEPLOYMENT_PROOF_EXIT_CODE"])

def rel(path):
    path = Path(path)
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()

def read_tsv(path):
    p = Path(path)
    if not p.is_file():
        return []
    return [line.rstrip("\n").split("\t") for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

commands = []
for row in read_tsv(os.environ["DEPLOYMENT_PROOF_COMMAND_LOG"]):
    if len(row) < 2:
        continue
    argv = row[2:]
    commands.append({
        "display": row[1] if row[1] else shlex.join(argv),
        "argv": argv,
        "exit_status": int(row[0]),
    })

skip_reasons = []
skip_path = Path(os.environ["DEPLOYMENT_PROOF_SKIP_LOG"])
if skip_path.is_file():
    skip_reasons = [line for line in skip_path.read_text(encoding="utf-8").splitlines() if line.strip()]

backend_status = {}
for row in read_tsv(os.environ["DEPLOYMENT_PROOF_BACKEND_LOG"]):
    if len(row) >= 2:
        backend_status[row[0]] = {
            "profile": row[0],
            "status": row[1],
            "reason": row[2] if len(row) > 2 else "",
        }

backend_profiles = []
for profile in os.environ["DEPLOYMENT_PROOF_BACKENDS"].split():
    status = backend_status.get(profile)
    if status is None:
        if skip_reasons:
            status = {"profile": profile, "status": "skipped_local_environment", "reason": "; ".join(skip_reasons)}
        else:
            status = {"profile": profile, "status": "not_run", "reason": "gate failed before this profile ran"}
    backend_profiles.append(status)

supporting_artifacts = []
for row in read_tsv(os.environ["DEPLOYMENT_PROOF_SUPPORT_LOG"]):
    if len(row) >= 2:
        path = Path(row[0])
        supporting_artifacts.append({
            "path": rel(path),
            "description": row[1],
            "exists": path.is_file(),
        })

chart_package = package_dir / f"pqueue-{chart_version}.tgz"
chart_evidence = package_dir / "pqueue-helm-chart.txt"
checksums = package_dir / "SHA256SUMS"
for path, description in [
    (chart_package, "Helm chart package"),
    (chart_evidence, "Helm chart evidence"),
    (checksums, "release distribution checksums"),
]:
    item = {"path": rel(path), "description": description, "exists": path.is_file()}
    if item not in supporting_artifacts:
        supporting_artifacts.append(item)

def parse_kv(path):
    data = {}
    p = Path(path)
    if not p.is_file():
        return data
    for line in p.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            data[key] = value
    return data

image_evidence_candidates = []
if os.environ.get("PQUEUE_IMAGE_EVIDENCE_FILE"):
    image_evidence_candidates.append(Path(os.environ["PQUEUE_IMAGE_EVIDENCE_FILE"]))
else:
    if os.environ.get("PQUEUE_RELEASE_DIST"):
        image_evidence_candidates.append(Path(os.environ["PQUEUE_RELEASE_DIST"]) / "pqueue-service-image.txt")
    image_evidence_candidates.extend([
        repo_root / "target/release-dist/pqueue-service-image.txt",
        package_dir / "pqueue-service-image.txt",
    ])

image_file = {}
image_file_path = None
for candidate in image_evidence_candidates:
    image_file = parse_kv(candidate)
    if image_file:
        image_file_path = candidate
        break

image_tag = (
    os.environ.get("PQUEUE_IMAGE_TAG")
    or image_file.get("version_coordinate")
    or image_file.get("sha_coordinate")
    or "unavailable"
)
image_digest = os.environ.get("PQUEUE_IMAGE_DIGEST") or image_file.get("digest") or "unavailable"
image_coordinate = (
    os.environ.get("PQUEUE_IMAGE_COORDINATE")
    or image_file.get("digest_coordinate")
    or image_tag
)
image_source = "environment" if os.environ.get("PQUEUE_IMAGE_TAG") or os.environ.get("PQUEUE_IMAGE_DIGEST") or os.environ.get("PQUEUE_IMAGE_COORDINATE") else "unavailable"
if image_file_path is not None:
    image_source = rel(image_file_path)

if exit_code == 0 and skip_reasons:
    status = "passed_with_local_environment_skip"
elif exit_code == 0:
    status = "passed"
else:
    status = "failed"

proof = {
    "schema": "pqueue.deployment_proof.v1",
    "status": status,
    "exit_status": exit_code,
    "commit_sha": os.environ["DEPLOYMENT_PROOF_COMMIT"],
    "chart": {
        "name": "pqueue",
        "version": chart_version,
        "package": rel(chart_package) if chart_package.is_file() else "unavailable",
        "package_exists": chart_package.is_file(),
        "evidence": rel(chart_evidence) if chart_evidence.is_file() else "unavailable",
    },
    "image": {
        "tag": image_tag,
        "digest": image_digest,
        "coordinate": image_coordinate,
        "source": image_source,
        "unavailable_reason": "" if image_tag != "unavailable" or image_digest != "unavailable" else "no PQUEUE_IMAGE_* environment values or pqueue-service-image.txt release artifact were available",
    },
    "backend_profiles": backend_profiles,
    "commands": commands,
    "local_environment_skip": {
        "scope": "kind backend matrix only" if skip_reasons else "",
        "reasons": skip_reasons,
        "ci_matrix_proof": not skip_reasons and exit_code == 0,
    },
    "supporting_artifacts": supporting_artifacts,
    "release_notes": {
        "summary": f"Deployment release gate {status} for commit {os.environ['DEPLOYMENT_PROOF_COMMIT']} and chart {chart_version}.",
        "command_list": [command["display"] for command in commands],
        "backend_profile_matrix": [profile["profile"] + ":" + profile["status"] for profile in backend_profiles],
        "artifact_paths": [artifact["path"] for artifact in supporting_artifacts],
    },
}

proof_json.write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n", encoding="utf-8")

lines = [
    "# Deployment Release Proof",
    "",
    f"- status: `{status}`",
    f"- exit status: `{exit_code}`",
    f"- commit: `{proof['commit_sha']}`",
    f"- chart: `pqueue` `{chart_version}`",
    f"- image tag: `{image_tag}`",
    f"- image digest: `{image_digest}`",
    "",
    "## Commands",
    "",
]
for command in commands:
    lines.append(f"- `{command['display']}` -> `{command['exit_status']}`")
lines.extend(["", "## Backend Profiles", ""])
for profile in backend_profiles:
    reason = f" ({profile['reason']})" if profile.get("reason") else ""
    lines.append(f"- `{profile['profile']}`: `{profile['status']}`{reason}")
lines.extend(["", "## Supporting Artifacts", ""])
for artifact in supporting_artifacts:
    exists = "present" if artifact["exists"] else "unavailable"
    lines.append(f"- `{artifact['path']}`: {artifact['description']} ({exists})")
if skip_reasons:
    lines.extend(["", "## Local Environment Skip", ""])
    lines.append("The local skip applies only to the kind backend matrix; CI matrix proof still requires successful kind runs.")
    for reason in skip_reasons:
        lines.append(f"- {reason}")
lines.append("")
proof_md.write_text("\n".join(lines), encoding="utf-8")
PY
    echo "deployment proof: ${PROOF_JSON}"
    echo "deployment proof summary: ${PROOF_MD}"
}

validate_docs_microsite() {
    python3 - <<'PY'
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlparse
import sys

root = Path.cwd()
required_docs = [
    Path("docs/helix/04-build/DEPLOYMENT-READINESS.md"),
    Path("docs/deployment/helm-static-validation.md"),
    Path("docs/deployment/kind-helm-integration.md"),
    Path("docs/deployment/operator-guide.md"),
]
index = Path("docs/operator/index.html")

for path in required_docs + [index]:
    if not (root / path).is_file():
        print(f"missing docs/microsite file: {path}", file=sys.stderr)
        sys.exit(1)

required_phrases = {
    Path("docs/deployment/helm-static-validation.md"): [
        "bash scripts/ci/helm-gate.sh",
        "object_log_sqlite_projection",
    ],
    Path("docs/deployment/kind-helm-integration.md"): [
        "bash scripts/ci/kind-helm-test.sh --backend postgres_native",
        "bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection",
    ],
}
for path, phrases in required_phrases.items():
    text = (root / path).read_text(encoding="utf-8")
    for phrase in phrases:
        if phrase not in text:
            print(f"{path} missing documented command or profile: {phrase}", file=sys.stderr)
            sys.exit(1)

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                self.hrefs.append(value)

parser = LinkParser()
parser.feed((root / index).read_text(encoding="utf-8"))

checked_links = 0
for href in parser.hrefs:
    parsed = urlparse(href)
    if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:")):
        continue
    local = unquote(parsed.path)
    if not local:
        continue
    target = (root / index).parent / local
    if not target.is_file():
        print(f"{index} has broken local link: {href}", file=sys.stderr)
        sys.exit(1)
    checked_links += 1

print(f"validated {len(required_docs)} deployment docs and {checked_links} microsite local link(s)")
PY
}

kind_unavailable_reasons() {
    local reasons=()
    local tool
    for tool in docker kind kubectl helm; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            reasons+=("missing tool: ${tool}")
        fi
    done

    if command -v docker >/dev/null 2>&1; then
        if ! docker info >/dev/null 2>&1; then
            reasons+=("docker daemon not usable: docker info failed")
        fi
    fi

    if [[ "${#reasons[@]}" -gt 0 ]]; then
        printf '%s\n' "${reasons[@]}"
    fi
}

run_non_cluster_gates() {
    echo "=== deployment release gate: non-cluster checks ==="
    run_cmd bash scripts/ci/release-gate.sh
    run_cmd bash scripts/ci/helm-gate.sh

    local version
    version="$(chart_version)"
    if [[ -z "${version}" ]]; then
        err "could not read chart version from charts/pqueue/Chart.yaml"
        exit 1
    fi
    local package_output="${PROOF_DIR}/package-helm-chart.out"
    run_cmd_capture "${package_output}" bash scripts/release/package-helm-chart.sh \
        --version "${version}" \
        --destination "${PACKAGE_DIR}" \
        --chart-dir charts/pqueue
    record_supporting_artifact "${package_output}" "chart packaging command output"
    record_supporting_artifact "${PACKAGE_DIR}/pqueue-${version}.tgz" "Helm chart package"
    record_supporting_artifact "${PACKAGE_DIR}/pqueue-helm-chart.txt" "Helm chart evidence"
    record_supporting_artifact "${PACKAGE_DIR}/SHA256SUMS" "release distribution checksums"

    run_step "validate docs/microsite" validate_docs_microsite
}

run_kind_matrix() {
    echo "=== deployment release gate: kind backend matrix ==="
    local reasons
    reasons="$(kind_unavailable_reasons)"
    if [[ -n "${reasons}" ]]; then
        echo "=== deployment release gate: SKIPPED kind backend matrix ==="
        echo "skip scope: kind backend matrix only (${BACKENDS[*]})"
        echo "missing local capability:"
        while IFS= read -r reason; do
            if [[ -n "${reason}" ]]; then
                echo "  - ${reason}"
                record_skip "${reason}"
            fi
        done <<<"${reasons}"
        local backend
        for backend in "${BACKENDS[@]}"; do
            record_backend_profile "${backend}" "skipped_local_environment" "${reasons//$'\n'/; }"
        done
        echo "non-cluster deployment release checks passed before this kind-only skip"
        return 0
    fi

    local backend
    for backend in "${BACKENDS[@]}"; do
        local backend_output="${PROOF_DIR}/kind-${backend}.out"
        record_supporting_artifact "${backend_output}" "kind Helm test output for ${backend}"
        if run_cmd_capture "${backend_output}" bash scripts/ci/kind-helm-test.sh --backend "${backend}"; then
            record_backend_profile "${backend}" "tested" ""
        else
            local status=$?
            if [[ "${CI:-}" != "true" ]] && grep -q "timed out waiting for Kubernetes API" "${backend_output}"; then
                local reason="local kind Kubernetes API did not become reachable"
                record_skip "${reason}"
                record_backend_profile "${backend}" "skipped_local_environment" "${reason}"
                local skipped_backend
                for skipped_backend in "${BACKENDS[@]}"; do
                    if [[ "${skipped_backend}" == "${backend}" ]]; then
                        continue
                    fi
                    record_backend_profile "${skipped_backend}" "skipped_local_environment" "${reason}"
                done
                echo "=== deployment release gate: SKIPPED remaining kind backend matrix ==="
                echo "skip scope: kind backend matrix only (${BACKENDS[*]})"
                echo "missing local capability: ${reason}"
                return 0
            fi
            record_backend_profile "${backend}" "failed" "kind Helm test exited ${status}"
            return "${status}"
        fi
    done
}

main() {
    cd "${REPO_ROOT}"
    init_proof_logs
    trap 'status=$?; write_deployment_proof "${status}" || true; exit "${status}"' EXIT
    run_non_cluster_gates
    run_kind_matrix
    echo "=== deployment release gate PASSED ==="
}

main "$@"
