#!/usr/bin/env bash
# Disposable kind-based Helm install smoke harness for the pqueue chart.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_ROOT}/charts/pqueue"
KIND_DIR="${SCRIPT_DIR}/kind"

BACKEND=""
CLUSTER_NAME=""
RELEASE_NAME="pqueue"
NAMESPACE="pqueue"
IMAGE="pqueue:ci"
TIMEOUT="180s"
SMOKE_PORT="18080"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
DRY_RUN=false
KEEP_CLUSTER=false
PF_PID=""
CLUSTER_CREATED=false

err() { echo "kind-helm-test: $*" >&2; }
die() { err "$*"; exit 1; }

usage() {
    cat <<'EOF'
kind-helm-test.sh - run a disposable kind Helm install smoke test for pqueue

USAGE:
  bash scripts/ci/kind-helm-test.sh --backend <profile> [OPTIONS]

REQUIRED TOOLS FOR REAL RUNS:
  docker    build the pqueue image
  kind      create/delete the disposable Kubernetes cluster and load the image
  kubectl   apply helper manifests, wait for rollout, and run the smoke check
  helm      install/upgrade the charts/pqueue release

BACKEND PROFILES:
  postgres_native
  object_log_sqlite_projection

OPTIONS:
  --backend <profile>      Required backend profile.
  --dry-run                Print the planned commands and values without
                           checking tools or creating a cluster.
  --cluster-name <name>    kind cluster name. Defaults to a disposable
                           pqueue-<backend>-<pid> name.
  --release-name <name>    Helm release name. Default: pqueue.
  --namespace <name>       Kubernetes namespace. Default: pqueue.
  --image <repo:tag>       Image to build, load, and install. Default: pqueue:ci.
  --timeout <duration>     Helm/kubectl readiness timeout. Default: 180s.
  --smoke-port <port>      Local port used for kubectl port-forward. Default: 18080.
  --kind-node-image <img>  Optional kind node image. Can also be set with
                           KIND_NODE_IMAGE.
  --keep-cluster           Do not delete the kind cluster on exit.
  -h, --help               Show this help text and exit.

The harness builds the pqueue container image, creates a kind cluster, loads the
image into that cluster, applies local runtime-secret fixtures, installs the
postgres_native PostgreSQL dependency when required, installs the Helm chart
with the selected CI backend values file, waits for readiness, checks GET
/readyz through kubectl port-forward, and deletes the cluster by default.
EOF
}

print_cmd() {
    printf '+'
    printf ' %q' "$@"
    printf '\n'
}

run() {
    print_cmd "$@"
    "$@"
}

require_tool() {
    command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

require_tools() {
    require_tool docker
    require_tool kind
    require_tool kubectl
    require_tool helm
}

kubectl_cmd() {
    kubectl --context "kind-${CLUSTER_NAME}" "$@"
}

values_file_for() {
    case "$1" in
        postgres_native) echo "${CHART_DIR}/ci/postgres-native-values.yaml" ;;
        object_log_sqlite_projection) echo "${CHART_DIR}/ci/object-log-sqlite-projection-values.yaml" ;;
        *) die "unsupported backend profile: $1" ;;
    esac
}

cleanup() {
    stop_port_forward

    if [[ "${DRY_RUN}" == false && "${KEEP_CLUSTER}" == false && "${CLUSTER_CREATED}" == true && -n "${CLUSTER_NAME}" ]]; then
        if command -v kind >/dev/null 2>&1 && kind get clusters | grep -Fxq "${CLUSTER_NAME}"; then
            run kind delete cluster --name "${CLUSTER_NAME}"
        fi
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --backend)
                [[ $# -ge 2 ]] || die "--backend requires a value"
                BACKEND="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --cluster-name)
                [[ $# -ge 2 ]] || die "--cluster-name requires a value"
                CLUSTER_NAME="$2"
                shift 2
                ;;
            --release-name)
                [[ $# -ge 2 ]] || die "--release-name requires a value"
                RELEASE_NAME="$2"
                shift 2
                ;;
            --namespace)
                [[ $# -ge 2 ]] || die "--namespace requires a value"
                NAMESPACE="$2"
                shift 2
                ;;
            --image)
                [[ $# -ge 2 ]] || die "--image requires a value"
                IMAGE="$2"
                shift 2
                ;;
            --timeout)
                [[ $# -ge 2 ]] || die "--timeout requires a value"
                TIMEOUT="$2"
                shift 2
                ;;
            --smoke-port)
                [[ $# -ge 2 ]] || die "--smoke-port requires a value"
                SMOKE_PORT="$2"
                shift 2
                ;;
            --kind-node-image)
                [[ $# -ge 2 ]] || die "--kind-node-image requires a value"
                KIND_NODE_IMAGE="$2"
                shift 2
                ;;
            --keep-cluster)
                KEEP_CLUSTER=true
                shift
                ;;
            -h | --help)
                usage
                exit 0
                ;;
            *)
                die "unknown argument: $1"
                ;;
        esac
    done
}

validate_config() {
    [[ -n "${BACKEND}" ]] || die "--backend is required"
    case "${BACKEND}" in
        postgres_native | object_log_sqlite_projection) ;;
        *) die "unsupported backend profile: ${BACKEND}" ;;
    esac
    [[ "${IMAGE}" == *:* ]] || die "--image must include an explicit tag, for example pqueue:ci"
    [[ "${SMOKE_PORT}" =~ ^[0-9]+$ ]] || die "--smoke-port must be a TCP port number"
    [[ -f "$(values_file_for "${BACKEND}")" ]] || die "missing values file for backend: ${BACKEND}"
    [[ -f "${KIND_DIR}/runtime-secrets.yaml" ]] || die "missing helper manifest: ${KIND_DIR}/runtime-secrets.yaml"
    if [[ "${BACKEND}" == "postgres_native" ]]; then
        [[ -f "${KIND_DIR}/postgres.yaml" ]] || die "missing postgres helper manifest: ${KIND_DIR}/postgres.yaml"
    else
        [[ -f "${KIND_DIR}/object-log.yaml" ]] || die "missing object-log helper manifest: ${KIND_DIR}/object-log.yaml"
    fi

    if [[ -z "${CLUSTER_NAME}" ]]; then
        CLUSTER_NAME="pqueue-${BACKEND//_/-}-$$"
    fi
}

dry_run_plan() {
    local values image_repository image_tag
    values="$(values_file_for "${BACKEND}")"
    image_repository="${IMAGE%:*}"
    image_tag="${IMAGE##*:}"

    echo "=== kind Helm integration dry run ==="
    echo "backend:       ${BACKEND}"
    echo "cluster:       ${CLUSTER_NAME}"
    echo "namespace:     ${NAMESPACE}"
    echo "release:       ${RELEASE_NAME}"
    echo "image:         ${IMAGE}"
    echo "values:        ${values}"
    echo "helper:        ${KIND_DIR}/runtime-secrets.yaml"
    echo "required tools for real runs: docker kind kubectl helm"
    echo
    echo "--- planned commands ---"
    print_cmd docker build -t "${IMAGE}" "${REPO_ROOT}"
    if [[ -n "${KIND_NODE_IMAGE}" ]]; then
        print_cmd kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}"
    else
        print_cmd kind create cluster --name "${CLUSTER_NAME}"
    fi
    print_cmd kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" cluster-info
    echo "+ kubectl --context kind-${CLUSTER_NAME} create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl --context kind-${CLUSTER_NAME} apply -f -"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" apply -f "${KIND_DIR}/runtime-secrets.yaml"
    if [[ "${BACKEND}" == "postgres_native" ]]; then
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" apply -f "${KIND_DIR}/postgres.yaml"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status deployment/postgres --timeout "${TIMEOUT}"
    else
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" apply -f "${KIND_DIR}/object-log.yaml"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status deployment/minio --timeout "${TIMEOUT}"
    fi
    print_cmd helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" --kube-context "kind-${CLUSTER_NAME}" --namespace "${NAMESPACE}" --values "${values}" --set "fullnameOverride=${RELEASE_NAME}" --set "image.repository=${image_repository}" --set "image.tag=${image_tag}" --set "image.pullPolicy=IfNotPresent" --wait --timeout "${TIMEOUT}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" port-forward "service/${RELEASE_NAME}" "${SMOKE_PORT}:8080"
    echo "+ GET http://127.0.0.1:${SMOKE_PORT}/readyz"
    if [[ "${BACKEND}" == "object_log_sqlite_projection" ]]; then
        echo "+ POST http://127.0.0.1:${SMOKE_PORT}/__pqueue/deployment/object-log-smoke/<proof-id>"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" exec deployment/minio -- test -s "/data/pqueue-object-log/pqueue/deployment-smoke/<proof-id>.json"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" exec "deployment/${RELEASE_NAME}" -- test -s "/var/lib/pqueue/projection/deployment-smoke/<proof-id>.json"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
        echo "+ GET http://127.0.0.1:${SMOKE_PORT}/__pqueue/deployment/object-log-smoke/<proof-id>"
    fi
    if [[ "${KEEP_CLUSTER}" == false ]]; then
        print_cmd kind delete cluster --name "${CLUSTER_NAME}"
    fi
    echo
    echo "--- selected values file ---"
    sed -n '1,160p' "${values}"
}

wait_for_port_forward() {
    local log_path="$1"
    for _ in {1..30}; do
        if (exec 3<>"/dev/tcp/127.0.0.1/${SMOKE_PORT}") >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "${PF_PID}" >/dev/null 2>&1; then
            err "kubectl port-forward exited before the smoke check could connect"
            sed -n '1,120p' "${log_path}" >&2 || true
            return 1
        fi
        sleep 1
    done
    err "timed out waiting for kubectl port-forward on 127.0.0.1:${SMOKE_PORT}"
    sed -n '1,120p' "${log_path}" >&2 || true
    return 1
}

stop_port_forward() {
    if [[ -n "${PF_PID}" ]]; then
        kill "${PF_PID}" >/dev/null 2>&1 || true
        wait "${PF_PID}" >/dev/null 2>&1 || true
        PF_PID=""
    fi
}

wait_for_kubernetes_api() {
    echo "waiting for Kubernetes API for kind cluster ${CLUSTER_NAME}"
    for _ in {1..60}; do
        if kubectl_cmd cluster-info >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    die "timed out waiting for Kubernetes API for kind cluster ${CLUSTER_NAME}"
}

smoke_readyz() {
    local run_dir log_path response_path
    run_dir="${REPO_ROOT}/target/kind-helm-test/${CLUSTER_NAME}"
    mkdir -p "${run_dir}"
    log_path="${run_dir}/port-forward.log"
    response_path="${run_dir}/readyz.response"

    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" port-forward "service/${RELEASE_NAME}" "${SMOKE_PORT}:8080"
    kubectl_cmd -n "${NAMESPACE}" port-forward "service/${RELEASE_NAME}" "${SMOKE_PORT}:8080" >"${log_path}" 2>&1 &
    PF_PID=$!

    wait_for_port_forward "${log_path}"

    (
        exec 3<>"/dev/tcp/127.0.0.1/${SMOKE_PORT}"
        printf 'GET /readyz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n' >&3
        cat <&3
    ) >"${response_path}"

    if ! sed -n '1p' "${response_path}" | grep -Eq '^HTTP/[0-9.]+ 200 '; then
        err "GET /readyz did not return HTTP 200"
        sed -n '1,80p' "${response_path}" >&2 || true
        return 1
    fi

    echo "GET /readyz returned HTTP 200"
}

service_http_request() {
    local method="$1"
    local path="$2"
    local response_path="$3"

    (
        exec 3<>"/dev/tcp/127.0.0.1/${SMOKE_PORT}"
        printf '%s %s HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n' "${method}" "${path}" >&3
        cat <&3
    ) >"${response_path}"

    if ! sed -n '1p' "${response_path}" | grep -Eq '^HTTP/[0-9.]+ 200 '; then
        err "${method} ${path} did not return HTTP 200"
        sed -n '1,120p' "${response_path}" >&2 || true
        return 1
    fi
}

smoke_object_log_runtime() {
    [[ "${BACKEND}" == "object_log_sqlite_projection" ]] || return 0

    local run_dir proof_id smoke_path post_response get_response object_path marker_path
    run_dir="${REPO_ROOT}/target/kind-helm-test/${CLUSTER_NAME}"
    proof_id="$(printf '%s' "${CLUSTER_NAME}-$(date +%s)" | tr -c 'A-Za-z0-9_-' '_')"
    smoke_path="/__pqueue/deployment/object-log-smoke/${proof_id}"
    post_response="${run_dir}/object-log-smoke-post.response"
    get_response="${run_dir}/object-log-smoke-get.response"
    object_path="/data/pqueue-object-log/pqueue/deployment-smoke/${proof_id}.json"
    marker_path="/var/lib/pqueue/projection/deployment-smoke/${proof_id}.json"

    echo "+ POST http://127.0.0.1:${SMOKE_PORT}${smoke_path}"
    service_http_request POST "${smoke_path}" "${post_response}"
    grep -F '"recovered":false' "${post_response}" >/dev/null || {
        err "object-log deployment smoke POST did not report recovered=false"
        sed -n '1,120p' "${post_response}" >&2 || true
        return 1
    }

    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" exec deployment/minio -- test -s "${object_path}"
    kubectl_cmd -n "${NAMESPACE}" exec deployment/minio -- test -s "${object_path}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" exec "deployment/${RELEASE_NAME}" -- test -s "${marker_path}"
    kubectl_cmd -n "${NAMESPACE}" exec "deployment/${RELEASE_NAME}" -- test -s "${marker_path}"

    stop_port_forward
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
    kubectl_cmd -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    kubectl_cmd -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"

    smoke_readyz
    echo "+ GET http://127.0.0.1:${SMOKE_PORT}${smoke_path}"
    service_http_request GET "${smoke_path}" "${get_response}"
    grep -F '"recovered":true' "${get_response}" >/dev/null || {
        err "object-log deployment smoke GET did not report recovered=true"
        sed -n '1,120p' "${get_response}" >&2 || true
        return 1
    }
    echo "object-log write and restart recovery smoke passed"
}

create_namespace() {
    echo "+ kubectl --context kind-${CLUSTER_NAME} create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl --context kind-${CLUSTER_NAME} apply -f -"
    kubectl_cmd create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl_cmd apply -f -
}

main() {
    parse_args "$@"
    validate_config

    if [[ "${DRY_RUN}" == true ]]; then
        dry_run_plan
        exit 0
    fi

    require_tools
    trap cleanup EXIT

    local values image_repository image_tag
    values="$(values_file_for "${BACKEND}")"
    image_repository="${IMAGE%:*}"
    image_tag="${IMAGE##*:}"

    echo "=== kind Helm integration smoke ==="
    echo "backend:   ${BACKEND}"
    echo "cluster:   ${CLUSTER_NAME}"
    echo "namespace: ${NAMESPACE}"
    echo "release:   ${RELEASE_NAME}"
    echo "image:     ${IMAGE}"

    run docker build -t "${IMAGE}" "${REPO_ROOT}"
    if [[ -n "${KIND_NODE_IMAGE}" ]]; then
        run kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}"
    else
        run kind create cluster --name "${CLUSTER_NAME}"
    fi
    CLUSTER_CREATED=true
    run kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"
    wait_for_kubernetes_api
    create_namespace
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" apply -f "${KIND_DIR}/runtime-secrets.yaml"
    kubectl_cmd -n "${NAMESPACE}" apply -f "${KIND_DIR}/runtime-secrets.yaml"
    if [[ "${BACKEND}" == "postgres_native" ]]; then
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" apply -f "${KIND_DIR}/postgres.yaml"
        kubectl_cmd -n "${NAMESPACE}" apply -f "${KIND_DIR}/postgres.yaml"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status deployment/postgres --timeout "${TIMEOUT}"
        kubectl_cmd -n "${NAMESPACE}" rollout status deployment/postgres --timeout "${TIMEOUT}"
    else
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" apply -f "${KIND_DIR}/object-log.yaml"
        kubectl_cmd -n "${NAMESPACE}" apply -f "${KIND_DIR}/object-log.yaml"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status deployment/minio --timeout "${TIMEOUT}"
        kubectl_cmd -n "${NAMESPACE}" rollout status deployment/minio --timeout "${TIMEOUT}"
    fi
    run helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
        --kube-context "kind-${CLUSTER_NAME}" \
        --namespace "${NAMESPACE}" \
        --values "${values}" \
        --set "fullnameOverride=${RELEASE_NAME}" \
        --set "image.repository=${image_repository}" \
        --set "image.tag=${image_tag}" \
        --set "image.pullPolicy=IfNotPresent" \
        --wait \
        --timeout "${TIMEOUT}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    kubectl_cmd -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    smoke_readyz
    smoke_object_log_runtime

    echo "=== kind Helm integration smoke PASSED ==="
}

main "$@"
