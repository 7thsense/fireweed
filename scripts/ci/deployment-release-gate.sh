#!/usr/bin/env bash
# Local deployment release gate for Fireweed Queue.
#
# This gate composes the existing source/release checks with deployment checks.
# The only tolerated local skip is the disposable kind storage matrix when the
# local Docker/kind toolchain is unavailable or Docker cannot be used.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_ROOT}/charts/fireweed-queue"
PROOF_DIR="${FIREWEED_DEPLOYMENT_PROOF_DIR:-${REPO_ROOT}/target/deployment-release-gate}"
PACKAGE_DIR="${FIREWEED_RELEASE_DIST:-${PROOF_DIR}/release-dist}"
PROOF_JSON="${PROOF_DIR}/deployment-proof.json"
PROOF_MD="${PROOF_DIR}/deployment-proof.md"
COMMAND_LOG="${PROOF_DIR}/commands.tsv"
STORAGE_LOG="${PROOF_DIR}/storage-combinations.tsv"
SKIP_LOG="${PROOF_DIR}/local-skips.txt"
SUPPORT_LOG="${PROOF_DIR}/supporting-artifacts.tsv"
KIND_IMAGE_CONTEXT="${PROOF_DIR}/kind-image"
KIND_IMAGE_DOCKERFILE="${KIND_IMAGE_CONTEXT}/Dockerfile"

# Public projection axis (memory|sqlite|turso|postgres); live-kind sample of deployable cells.
STORAGE_COMBINATIONS=("filesystem:memory" "filesystem:sqlite" "filesystem:turso")
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
    : >"${STORAGE_LOG}"
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

record_storage_combination() {
    local combination="$1" status="$2" reason="${3:-}"
    printf '%s\t%s\t%s\n' "${combination}" "${status}" "${reason}" >>"${STORAGE_LOG}"
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
    if [[ -n "${FIREWEED_RELEASE_VERSION:-}" ]]; then
        printf '%s\n' "${FIREWEED_RELEASE_VERSION}"
        return
    fi
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
    DEPLOYMENT_PROOF_STORAGE_COMBINATIONS="${STORAGE_COMBINATIONS[*]}" \
    DEPLOYMENT_PROOF_COMMAND_LOG="${COMMAND_LOG}" \
    DEPLOYMENT_PROOF_STORAGE_LOG="${STORAGE_LOG}" \
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

e2_smoke_status = "run"

skip_reasons = []
skip_path = Path(os.environ["DEPLOYMENT_PROOF_SKIP_LOG"])
if skip_path.is_file():
    skip_reasons = [line for line in skip_path.read_text(encoding="utf-8").splitlines() if line.strip()]

storage_status = {}
for row in read_tsv(os.environ["DEPLOYMENT_PROOF_STORAGE_LOG"]):
    if len(row) >= 2:
        storage_status[row[0]] = {
            "combination": row[0],
            "status": row[1],
            "reason": row[2] if len(row) > 2 else "",
        }

storage_combinations = []
for combination in os.environ["DEPLOYMENT_PROOF_STORAGE_COMBINATIONS"].split():
    status = storage_status.get(combination)
    if status is None:
        if skip_reasons:
            status = {"combination": combination, "status": "skipped_local_environment", "reason": "; ".join(skip_reasons)}
        else:
            status = {"combination": combination, "status": "not_run", "reason": "gate failed before this storage combination ran"}
    storage_combinations.append(status)

supporting_artifacts = []
for row in read_tsv(os.environ["DEPLOYMENT_PROOF_SUPPORT_LOG"]):
    if len(row) >= 2:
        path = Path(row[0])
        supporting_artifacts.append({
            "path": rel(path),
            "description": row[1],
            "exists": path.is_file(),
        })

chart_package = package_dir / f"fireweed-queue-{chart_version}.tgz"
chart_evidence = package_dir / "fireweed-queue-helm-chart.txt"
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
if os.environ.get("FIREWEED_IMAGE_EVIDENCE_FILE"):
    image_evidence_candidates.append(Path(os.environ["FIREWEED_IMAGE_EVIDENCE_FILE"]))
else:
    if os.environ.get("FIREWEED_RELEASE_DIST"):
        image_evidence_candidates.append(Path(os.environ["FIREWEED_RELEASE_DIST"]) / "fireweed-service-image.txt")
    image_evidence_candidates.append(package_dir / "fireweed-service-image.txt")

image_file = {}
image_file_path = None
for candidate in image_evidence_candidates:
    image_file = parse_kv(candidate)
    if image_file:
        image_file_path = candidate
        break

image_tag = (
    os.environ.get("FIREWEED_IMAGE_TAG")
    or image_file.get("version_coordinate")
    or image_file.get("sha_coordinate")
    or "unavailable"
)
image_digest = os.environ.get("FIREWEED_IMAGE_DIGEST") or image_file.get("digest") or "unavailable"
image_coordinate = (
    os.environ.get("FIREWEED_IMAGE_COORDINATE")
    or image_file.get("digest_coordinate")
    or image_tag
)
image_source = "environment" if os.environ.get("FIREWEED_IMAGE_TAG") or os.environ.get("FIREWEED_IMAGE_DIGEST") or os.environ.get("FIREWEED_IMAGE_COORDINATE") else "unavailable"
if image_file_path is not None:
    image_source = rel(image_file_path)

if exit_code == 0 and skip_reasons:
    status = "passed_with_local_environment_skip"
elif exit_code == 0:
    status = "passed"
else:
    status = "failed"

proof = {
    "schema": "fireweed.deployment_proof.v1",
    "status": status,
    "exit_status": exit_code,
    "commit_sha": os.environ["DEPLOYMENT_PROOF_COMMIT"],
    "chart": {
        "name": "fireweed-queue",
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
        "unavailable_reason": "" if image_tag != "unavailable" or image_digest != "unavailable" else "no FIREWEED_IMAGE_* environment values or fireweed-service-image.txt release artifact were available",
    },
    "storage_combinations": storage_combinations,
    "performance_evidence": {
        "e2_smoke_status": e2_smoke_status,
    },
    "commands": commands,
    "local_environment_skip": {
        "scope": "kind storage matrix only" if skip_reasons else "",
        "reasons": skip_reasons,
        "ci_matrix_proof": not skip_reasons and exit_code == 0,
    },
    "supporting_artifacts": supporting_artifacts,
    "release_notes": {
        "summary": f"Deployment release gate {status} for commit {os.environ['DEPLOYMENT_PROOF_COMMIT']} and chart {chart_version}.",
        "command_list": [command["display"] for command in commands],
        "storage_matrix": [item["combination"] + ":" + item["status"] for item in storage_combinations],
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
    f"- chart: `fireweed-queue` `{chart_version}`",
    f"- image tag: `{image_tag}`",
    f"- image digest: `{image_digest}`",
    f"- E2 smoke status: `{e2_smoke_status}`",
    "",
    "## Commands",
    "",
]
for command in commands:
    lines.append(f"- `{command['display']}` -> `{command['exit_status']}`")
lines.extend(["", "## Storage Combinations", ""])
for item in storage_combinations:
    reason = f" ({item['reason']})" if item.get("reason") else ""
    lines.append(f"- `{item['combination']}`: `{item['status']}`{reason}")
lines.extend(["", "## Supporting Artifacts", ""])
for artifact in supporting_artifacts:
    exists = "present" if artifact["exists"] else "unavailable"
    lines.append(f"- `{artifact['path']}`: {artifact['description']} ({exists})")
if skip_reasons:
    lines.extend(["", "## Local Environment Skip", ""])
    lines.append("The local skip applies only to the kind storage matrix; CI matrix proof still requires successful kind runs.")
    for reason in skip_reasons:
        lines.append(f"- {reason}")
lines.append("")
proof_md.write_text("\n".join(lines), encoding="utf-8")
PY
    echo "deployment proof: ${PROOF_JSON}"
    echo "deployment proof summary: ${PROOF_MD}"
}

validate_docs_microsite() {
    required_docs=(
        docs/helix/04-build/DEPLOYMENT-READINESS.md
        docs/deployment/helm-static-validation.md
        docs/deployment/kind-helm-integration.md
        docs/deployment/operator-guide.md
        docs/operator/index.html
        docs/site/index.html
        docs/site/deploy/index.html
        docs/site/_meta/example-manifest.yaml
        docs/site/_meta/site-meta.json
    )
    local path
    for path in "${required_docs[@]}"; do
        if [[ ! -f "$path" ]]; then
            echo "missing docs/microsite file: $path" >&2
            return 1
        fi
    done

    if ! grep -q 'bash scripts/ci/helm-gate.sh' docs/deployment/helm-static-validation.md; then
        echo "docs/deployment/helm-static-validation.md missing helm-gate command" >&2
        return 1
    fi
    if ! grep -q 'storage.log.backend' docs/deployment/helm-static-validation.md; then
        echo "docs/deployment/helm-static-validation.md missing storage.log.backend" >&2
        return 1
    fi
    local kind_cmd
    for kind_cmd in \
        'bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend memory' \
        'bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend sqlite' \
        'bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend memory' \
        'bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend sqlite' \
        'bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend postgres'
    do
        if ! grep -Fq "$kind_cmd" docs/deployment/kind-helm-integration.md; then
            echo "docs/deployment/kind-helm-integration.md missing: $kind_cmd" >&2
            return 1
        fi
    done

    bash scripts/ci/microsite-gate.sh
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
    # release-gate.sh already runs storage-matrix-gate.sh --skip-helm (cargo matrix + legacy
    # product-name ban). Re-run the gate here for helm only so the deployment path still owns
    # chart fixtures without double-compiling the matrix cargo suites.
    run_cmd bash scripts/ci/release-gate.sh
    run_cmd bash scripts/ci/storage-matrix-gate.sh --skip-cargo

    local version
    version="$(chart_version)"
    if [[ -z "${version}" ]]; then
        err "could not read chart version from charts/fireweed-queue/Chart.yaml"
        exit 1
    fi
    local package_output="${PROOF_DIR}/package-helm-chart.out"
    run_cmd_capture "${package_output}" bash scripts/release/package-helm-chart.sh \
        --version "${version}" \
        --destination "${PACKAGE_DIR}" \
        --chart-dir charts/fireweed-queue
    record_supporting_artifact "${package_output}" "chart packaging command output"
    record_supporting_artifact "${PACKAGE_DIR}/fireweed-queue-${version}.tgz" "Helm chart package"
    record_supporting_artifact "${PACKAGE_DIR}/fireweed-queue-helm-chart.txt" "Helm chart evidence"
    record_supporting_artifact "${PACKAGE_DIR}/SHA256SUMS" "release distribution checksums"

    run_step "validate docs/microsite" validate_docs_microsite
}

free_kind_build_space() {
    echo "=== deployment release gate: free kind image build space ==="
    run_cmd rm -rf \
        "${REPO_ROOT}/target/debug" \
        "${REPO_ROOT}/target/release" \
        "${REPO_ROOT}/target/coverage" \
        "${REPO_ROOT}/target/llvm-cov-target"
}

prepare_kind_image_context() {
    echo "=== deployment release gate: prepare kind image context ==="
    free_kind_build_space
    run_cmd rustup run 1.97.1 cargo build --release --bin fireweed-verify-ledger
    run_cmd rustup run 1.97.1 cargo build --release --bin fireweed-service

    rm -rf "${KIND_IMAGE_CONTEXT}"
    mkdir -p "${KIND_IMAGE_CONTEXT}"
    cp "${REPO_ROOT}/target/release/fireweed-service" "${KIND_IMAGE_CONTEXT}/"
    cp "${REPO_ROOT}/target/release/fireweed-verify-ledger" "${KIND_IMAGE_CONTEXT}/"
    cp "${REPO_ROOT}/Dockerfile.prebuilt" "${KIND_IMAGE_DOCKERFILE}"
    chmod 0755 "${KIND_IMAGE_CONTEXT}/fireweed-service" "${KIND_IMAGE_CONTEXT}/fireweed-verify-ledger"

    record_supporting_artifact "${KIND_IMAGE_CONTEXT}/fireweed-service" "kind smoke prebuilt fireweed-service binary"
    record_supporting_artifact "${KIND_IMAGE_CONTEXT}/fireweed-verify-ledger" "kind smoke prebuilt fireweed-verify-ledger binary"
    record_supporting_artifact "${KIND_IMAGE_DOCKERFILE}" "kind smoke prebuilt Dockerfile"
}

run_kind_matrix() {
    echo "=== deployment release gate: kind storage matrix ==="
    local reasons
    reasons="$(kind_unavailable_reasons)"
    if [[ -n "${reasons}" ]]; then
        echo "=== deployment release gate: SKIPPED kind storage matrix ==="
        echo "skip scope: kind storage matrix only (${STORAGE_COMBINATIONS[*]})"
        echo "missing local capability:"
        while IFS= read -r reason; do
            if [[ -n "${reason}" ]]; then
                echo "  - ${reason}"
                record_skip "${reason}"
            fi
        done <<<"${reasons}"
        local combination
        for combination in "${STORAGE_COMBINATIONS[@]}"; do
            record_storage_combination "${combination}" "skipped_local_environment" "${reasons//$'\n'/; }"
        done
        echo "non-cluster deployment release checks passed before this kind-only skip"
        return 0
    fi

    prepare_kind_image_context

    local combination
    for combination in "${STORAGE_COMBINATIONS[@]}"; do
        local log_backend="${combination%%:*}"
        local projection_backend="${combination##*:}"
        local backend_output="${PROOF_DIR}/kind-${log_backend}-${projection_backend}.out"
        record_supporting_artifact "${backend_output}" "kind Helm test output for ${combination}"
        if run_cmd_capture "${backend_output}" bash scripts/ci/kind-helm-test.sh \
            --log-backend "${log_backend}" \
            --projection-backend "${projection_backend}" \
            --image-context "${KIND_IMAGE_CONTEXT}" \
            --image-dockerfile "${KIND_IMAGE_DOCKERFILE}"; then
            record_storage_combination "${combination}" "tested" ""
        else
            local status=$?
            if [[ "${CI:-}" != "true" ]] && grep -q "timed out waiting for Kubernetes API" "${backend_output}"; then
                local reason="local kind Kubernetes API did not become reachable"
                record_skip "${reason}"
                record_storage_combination "${combination}" "skipped_local_environment" "${reason}"
                local skipped_combination
                for skipped_combination in "${STORAGE_COMBINATIONS[@]}"; do
                    if [[ "${skipped_combination}" == "${combination}" ]]; then
                        continue
                    fi
                    record_storage_combination "${skipped_combination}" "skipped_local_environment" "${reason}"
                done
                echo "=== deployment release gate: SKIPPED remaining kind storage matrix ==="
                echo "skip scope: kind storage matrix only (${STORAGE_COMBINATIONS[*]})"
                echo "missing local capability: ${reason}"
                return 0
            fi
            record_storage_combination "${combination}" "failed" "kind Helm test exited ${status}"
            return "${status}"
        fi
    done
}

main() {
    cd "${REPO_ROOT}"
    if [[ "${1:-}" == "--finalize-proof" ]]; then
        if (($# != 1)); then
            err "--finalize-proof does not accept additional arguments"
            return 64
        fi
        local required_log
        for required_log in "${COMMAND_LOG}" "${STORAGE_LOG}" "${SKIP_LOG}" "${SUPPORT_LOG}"; do
            if [[ ! -f "${required_log}" ]]; then
                err "cannot finalize proof; missing gate log: ${required_log}"
                return 1
            fi
        done
        write_deployment_proof 0
        echo "deployment proof finalized: ${PROOF_JSON}"
        return
    fi
    if (($# != 0)); then
        err "unexpected argument(s): $*"
        return 64
    fi
    init_proof_logs
    trap 'status=$?; write_deployment_proof "${status}" || true; exit "${status}"' EXIT
    run_non_cluster_gates
    run_kind_matrix
    echo "=== deployment release gate PASSED ==="
}

main "$@"
