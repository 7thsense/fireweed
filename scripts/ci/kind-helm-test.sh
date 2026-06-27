#!/usr/bin/env bash
# Disposable kind-based Helm install smoke harness for the pqueue chart.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_ROOT}/charts/pqueue"
KIND_DIR="${SCRIPT_DIR}/kind"

LOG_BACKEND=""
PROJECTION_BACKEND=""
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
  bash scripts/ci/kind-helm-test.sh --log-backend <backend> --projection-backend <backend> [OPTIONS]

REQUIRED TOOLS FOR REAL RUNS:
  docker    build the pqueue image
  kind      create/delete the disposable Kubernetes cluster and load the image
  kubectl   apply helper manifests, wait for rollout, and run the smoke check
  helm      install/upgrade the charts/pqueue release

STORAGE BACKENDS:
  log:        objectlog
  projection: inmemory

OPTIONS:
  --log-backend <backend>  Required log backend for this runtime smoke.
  --projection-backend <backend>
                           Required projection backend for this runtime smoke.
  --dry-run                Print the planned commands and values without
                           checking tools or creating a cluster.
  --cluster-name <name>    kind cluster name. Defaults to a disposable
                           pqueue-<log>-<projection>-<pid> name.
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
image into that cluster, installs the Helm chart with the selected CI storage
values file, waits for readiness, checks RESP PING through kubectl
port-forward, and deletes the cluster by default.
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
    case "$1:$2" in
        objectlog:inmemory) echo "${CHART_DIR}/ci/objectlog-inmemory-values.yaml" ;;
        *) die "no runtime CI values file for log=$1 projection=$2" ;;
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
            --log-backend)
                [[ $# -ge 2 ]] || die "--log-backend requires a value"
                LOG_BACKEND="$2"
                shift 2
                ;;
            --projection-backend)
                [[ $# -ge 2 ]] || die "--projection-backend requires a value"
                PROJECTION_BACKEND="$2"
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
    [[ -n "${LOG_BACKEND}" ]] || die "--log-backend is required"
    [[ -n "${PROJECTION_BACKEND}" ]] || die "--projection-backend is required"
    case "${LOG_BACKEND}:${PROJECTION_BACKEND}" in
        objectlog:inmemory) ;;
        *) die "runtime smoke currently supports only log=objectlog projection=inmemory; requested log=${LOG_BACKEND} projection=${PROJECTION_BACKEND}" ;;
    esac
    [[ "${IMAGE}" == *:* ]] || die "--image must include an explicit tag, for example pqueue:ci"
    [[ "${SMOKE_PORT}" =~ ^[0-9]+$ ]] || die "--smoke-port must be a TCP port number"
    [[ -f "$(values_file_for "${LOG_BACKEND}" "${PROJECTION_BACKEND}")" ]] || die "missing values file for log=${LOG_BACKEND} projection=${PROJECTION_BACKEND}"

    if [[ -z "${CLUSTER_NAME}" ]]; then
        CLUSTER_NAME="pqueue-${LOG_BACKEND}-${PROJECTION_BACKEND}-$$"
    fi
}

dry_run_plan() {
    local values image_repository image_tag
    values="$(values_file_for "${LOG_BACKEND}" "${PROJECTION_BACKEND}")"
    image_repository="${IMAGE%:*}"
    image_tag="${IMAGE##*:}"

    echo "=== kind Helm integration dry run ==="
    echo "log backend:   ${LOG_BACKEND}"
    echo "projection:    ${PROJECTION_BACKEND}"
    echo "cluster:       ${CLUSTER_NAME}"
    echo "namespace:     ${NAMESPACE}"
    echo "release:       ${RELEASE_NAME}"
    echo "image:         ${IMAGE}"
    echo "values:        ${values}"
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
    print_cmd helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" --kube-context "kind-${CLUSTER_NAME}" --namespace "${NAMESPACE}" --values "${values}" --set "fullnameOverride=${RELEASE_NAME}" --set "image.repository=${image_repository}" --set "image.tag=${image_tag}" --set "image.pullPolicy=IfNotPresent" --wait --timeout "${TIMEOUT}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" port-forward "service/${RELEASE_NAME}" "${SMOKE_PORT}:8080"
    echo "+ RESP PING 127.0.0.1:${SMOKE_PORT}"
    echo "+ RESP XADD/XREADGROUP 127.0.0.1:${SMOKE_PORT}"
    if [[ "${LOG_BACKEND}" == "objectlog" ]]; then
        echo "+ RESP XADD before restart"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
        echo "+ RESP XREADGROUP after restart"
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

resp_request() {
    local response_path="$1"
    local payload="$2"
    (
        exec 3<>"/dev/tcp/127.0.0.1/${SMOKE_PORT}"
        printf '%b' "${payload}" >&3
        cat <&3
    ) >"${response_path}"
}

smoke_resp() {
    local run_dir log_path response_path
    run_dir="${REPO_ROOT}/target/kind-helm-test/${CLUSTER_NAME}"
    mkdir -p "${run_dir}"
    log_path="${run_dir}/port-forward.log"
    response_path="${run_dir}/resp.response"

    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" port-forward "service/${RELEASE_NAME}" "${SMOKE_PORT}:8080"
    kubectl_cmd -n "${NAMESPACE}" port-forward "service/${RELEASE_NAME}" "${SMOKE_PORT}:8080" >"${log_path}" 2>&1 &
    PF_PID=$!

    wait_for_port_forward "${log_path}"

    echo "+ RESP PING 127.0.0.1:${SMOKE_PORT}"
    resp_request "${response_path}" '*1\r\n$4\r\nPING\r\n'
    if ! grep -Fq '+PONG' "${response_path}"; then
        err "RESP PING did not return PONG"
        sed -n '1,80p' "${response_path}" >&2 || true
        return 1
    fi

    echo "+ RESP XADD 127.0.0.1:${SMOKE_PORT}"
    resp_request "${response_path}" '*5\r\n$4\r\nXADD\r\n$5\r\nt1:q1\r\n$1\r\n*\r\n$8\r\npriority\r\n$1\r\n1\r\n'
    if ! grep -Eq '^\$[0-9]+' "${response_path}"; then
        err "RESP XADD did not return a bulk item id"
        sed -n '1,80p' "${response_path}" >&2 || true
        return 1
    fi

    echo "+ RESP XREADGROUP 127.0.0.1:${SMOKE_PORT}"
    resp_request "${response_path}" '*9\r\n$10\r\nXREADGROUP\r\n$5\r\nGROUP\r\n$1\r\ng\r\n$1\r\nc\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n$7\r\nSTREAMS\r\n$5\r\nt1:q1\r\n$1\r\n>\r\n'
    if ! grep -Fq 't1:q1' "${response_path}"; then
        err "RESP XREADGROUP did not return the bootstrap queue"
        sed -n '1,120p' "${response_path}" >&2 || true
        return 1
    fi

    echo "RESP smoke passed"
}

smoke_object_log_runtime() {
    [[ "${LOG_BACKEND}" == "objectlog" ]] || return 0

    local run_dir response_path
    run_dir="${REPO_ROOT}/target/kind-helm-test/${CLUSTER_NAME}"
    response_path="${run_dir}/object-log-recovery.response"

    echo "+ RESP XADD before restart"
    resp_request "${response_path}" '*5\r\n$4\r\nXADD\r\n$5\r\nt1:q1\r\n$1\r\n*\r\n$8\r\npriority\r\n$1\r\n2\r\n'
    if ! grep -Eq '^\$[0-9]+' "${response_path}"; then
        err "object-log pre-restart XADD did not return a bulk item id"
        sed -n '1,80p' "${response_path}" >&2 || true
        return 1
    fi

    stop_port_forward
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
    kubectl_cmd -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    kubectl_cmd -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"

    smoke_resp
    echo "+ RESP XREADGROUP after restart"
    resp_request "${response_path}" '*9\r\n$10\r\nXREADGROUP\r\n$5\r\nGROUP\r\n$1\r\ng\r\n$1\r\nrestarted\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n$7\r\nSTREAMS\r\n$5\r\nt1:q1\r\n$1\r\n>\r\n'
    if ! grep -Fq 't1:q1' "${response_path}"; then
        err "object-log post-restart XREADGROUP did not recover queue data"
        sed -n '1,120p' "${response_path}" >&2 || true
        return 1
    fi
    echo "object-log restart recovery smoke passed"
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
    values="$(values_file_for "${LOG_BACKEND}" "${PROJECTION_BACKEND}")"
    image_repository="${IMAGE%:*}"
    image_tag="${IMAGE##*:}"

    echo "=== kind Helm integration smoke ==="
    echo "log:       ${LOG_BACKEND}"
    echo "projection:${PROJECTION_BACKEND}"
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
    smoke_resp
    smoke_object_log_runtime

    echo "=== kind Helm integration smoke PASSED ==="
}

main "$@"
