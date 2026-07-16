#!/usr/bin/env bash
# Disposable kind-based Helm install smoke harness for the pqueue chart.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_ROOT}/charts/pqueue"

LOG_BACKEND=""
PROJECTION_BACKEND=""
CLUSTER_NAME=""
RELEASE_NAME="pqueue"
NAMESPACE="pqueue"
IMAGE="pqueue:ci"
IMAGE_CONTEXT="${PQUEUE_KIND_IMAGE_CONTEXT:-${REPO_ROOT}}"
IMAGE_DOCKERFILE="${PQUEUE_KIND_IMAGE_DOCKERFILE:-}"
TIMEOUT="180s"
SMOKE_PORT="18080"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
DRY_RUN=false
KEEP_CLUSTER=false
BOOTSTRAP_GENERATED_COUNT=0
BOOTSTRAP_GENERATED_TENANT=t1
BOOTSTRAP_GENERATED_PREFIX=q
SMOKE_QUEUE=t1:q1
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

STORAGE BACKENDS (runnable live smokes):
  objectlog + inmemory   ephemeral projection over the durable object log
  objectlog + sqlite     durable SQLite relational projection over the object log,
                         persisted on the chart's storage volume
  objectlog + hybrid     durable SQLite projection plus the hot in-memory serving
                         image over the object log, persisted on the chart's volume
  objectlog + hybrid-async
                         hot in-memory serving over an asynchronous durable SQLite
                         checkpoint, with fail-closed debt/backpressure thresholds
  postgres  + inmemory   durable postgres command log + in-memory projection
                         (the wired managed-postgres profile). The harness stands
                         up a throwaway in-cluster postgres and injects its DSN as
                         the pqueue-postgres-log Secret before installing the chart.
  postgres  + sqlite     durable postgres command log + a derived SQLite relational
                         projection on the chart's storage volume. Same in-cluster
                         postgres as above for the log axis; no projection Secret.
  postgres  + postgres   durable postgres command log + a SEPARATE postgres-backed
                         relational projection (distinct table sets, no collision).
                         The harness reuses the one throwaway in-cluster postgres for
                         both axes and injects its DSN as both the pqueue-postgres-log
                         and pqueue-postgres-projection Secrets.

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
  --image-context <path>   Docker build context. Default: repository root.
  --image-dockerfile <path>
                           Optional Dockerfile path for docker build -f.
  --timeout <duration>     Helm/kubectl readiness timeout. Default: 180s.
  --smoke-port <port>      Local port used for kubectl port-forward. Default: 18080.
  --kind-node-image <img>  Optional kind node image. Can also be set with
                           KIND_NODE_IMAGE.
  --bootstrap-generated-count <n>
                           Deterministically provision n queues (1..10000).
  --bootstrap-generated-tenant <tenant>
                           Tenant for generated queues. Default: t1.
  --bootstrap-generated-prefix <prefix>
                           Queue prefix; creates prefix0..prefixN-1. Default: q.
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
        objectlog:sqlite) echo "${CHART_DIR}/ci/objectlog-sqlite-values.yaml" ;;
        objectlog:hybrid) echo "${CHART_DIR}/ci/objectlog-hybrid-values.yaml" ;;
        objectlog:hybrid-async) echo "${CHART_DIR}/ci/objectlog-hybrid-async-values.yaml" ;;
        postgres:inmemory) echo "${CHART_DIR}/ci/postgres-inmemory-values.yaml" ;;
        postgres:sqlite) echo "${CHART_DIR}/ci/postgres-sqlite-values.yaml" ;;
        postgres:postgres) echo "${CHART_DIR}/ci/postgres-postgres-values.yaml" ;;
        *) die "no runtime CI values file for log=$1 projection=$2" ;;
    esac
}

# The Kubernetes Secret name + key the postgres-inmemory/postgres-sqlite values files expect the log DSN
# under (must match charts/pqueue/ci/postgres-inmemory-values.yaml and postgres-sqlite-values.yaml:
# storage.log.postgres.existingSecret/databaseUrlKey).
PG_SECRET_NAME="pqueue-postgres-log"
PG_SECRET_KEY="database-url"
# The Kubernetes Secret name + key the postgres-postgres values file expects the projection DSN under (must
# match charts/pqueue/ci/postgres-postgres-values.yaml: storage.projection.postgres.existingSecret/databaseUrlKey).
PG_PROJECTION_SECRET_NAME="pqueue-postgres-projection"
PG_PROJECTION_SECRET_KEY="database-url"
# In-cluster throwaway postgres coordinates (Deployment/Service applied by deploy_in_cluster_postgres).
PG_IN_CLUSTER_IMAGE="postgres:16"
PG_IN_CLUSTER_HOST="pqueue-ci-postgres"
PG_IN_CLUSTER_USER="pqueue"
PG_IN_CLUSTER_PASSWORD="pqueue"
PG_IN_CLUSTER_DB="pqueue"

# True when this smoke needs a database (the postgres log axis).
needs_in_cluster_postgres() {
    [[ "${LOG_BACKEND}" == "postgres" || "${PROJECTION_BACKEND}" == "postgres" ]]
}

# The cargo features the pqueue image must be built with for the selected backend. The postgres log axis
# needs the `postgres` feature (Backend::PostgresNative); everything else ships the default (no-feature) image.
image_cargo_features() {
    if needs_in_cluster_postgres; then
        echo "postgres"
    else
        echo ""
    fi
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
            --image-context)
                [[ $# -ge 2 ]] || die "--image-context requires a value"
                IMAGE_CONTEXT="$2"
                shift 2
                ;;
            --image-dockerfile)
                [[ $# -ge 2 ]] || die "--image-dockerfile requires a value"
                IMAGE_DOCKERFILE="$2"
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
            --bootstrap-generated-count)
                [[ $# -ge 2 ]] || die "--bootstrap-generated-count requires a value"
                BOOTSTRAP_GENERATED_COUNT="$2"
                shift 2
                ;;
            --bootstrap-generated-tenant)
                [[ $# -ge 2 ]] || die "--bootstrap-generated-tenant requires a value"
                BOOTSTRAP_GENERATED_TENANT="$2"
                shift 2
                ;;
            --bootstrap-generated-prefix)
                [[ $# -ge 2 ]] || die "--bootstrap-generated-prefix requires a value"
                BOOTSTRAP_GENERATED_PREFIX="$2"
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
        objectlog:sqlite) ;;
        objectlog:hybrid) ;;
        objectlog:hybrid-async) ;;
        postgres:inmemory) ;;
        postgres:sqlite) ;;
        postgres:postgres) ;;
        *) die "runtime smoke supports log=objectlog projection={inmemory,sqlite,hybrid,hybrid-async}, and log=postgres projection={inmemory,sqlite,postgres}; requested log=${LOG_BACKEND} projection=${PROJECTION_BACKEND}" ;;
    esac
    [[ "${IMAGE}" == *:* ]] || die "--image must include an explicit tag, for example pqueue:ci"
    [[ -d "${IMAGE_CONTEXT}" ]] || die "--image-context must be an existing directory: ${IMAGE_CONTEXT}"
    if [[ -n "${IMAGE_DOCKERFILE}" && ! -f "${IMAGE_DOCKERFILE}" ]]; then
        die "--image-dockerfile must be an existing file: ${IMAGE_DOCKERFILE}"
    fi
    [[ "${SMOKE_PORT}" =~ ^[0-9]+$ ]] || die "--smoke-port must be a TCP port number"
    [[ "${BOOTSTRAP_GENERATED_COUNT}" =~ ^[0-9]+$ ]] || die "--bootstrap-generated-count must be an integer"
    ((BOOTSTRAP_GENERATED_COUNT <= 10000)) || die "--bootstrap-generated-count must not exceed 10000"
    if ((BOOTSTRAP_GENERATED_COUNT > 0)); then
        [[ -n "${BOOTSTRAP_GENERATED_TENANT}" ]] || die "--bootstrap-generated-tenant must not be empty"
        [[ -n "${BOOTSTRAP_GENERATED_PREFIX}" ]] || die "--bootstrap-generated-prefix must not be empty"
        SMOKE_QUEUE="${BOOTSTRAP_GENERATED_TENANT}:${BOOTSTRAP_GENERATED_PREFIX}0"
    fi
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
    echo "context:       ${IMAGE_CONTEXT}"
    echo "bootstrap:     generated count=${BOOTSTRAP_GENERATED_COUNT} tenant=${BOOTSTRAP_GENERATED_TENANT} prefix=${BOOTSTRAP_GENERATED_PREFIX}"
    echo "smoke queue:   ${SMOKE_QUEUE}"
    if [[ -n "${IMAGE_DOCKERFILE}" ]]; then
        echo "dockerfile:    ${IMAGE_DOCKERFILE}"
    fi
    echo "values:        ${values}"
    echo "required tools for real runs: docker kind kubectl helm"
    echo
    echo "--- planned commands ---"
    if [[ -n "${IMAGE_DOCKERFILE}" ]]; then
        print_cmd docker build -f "${IMAGE_DOCKERFILE}" -t "${IMAGE}" "${IMAGE_CONTEXT}"
    else
        print_cmd docker build -t "${IMAGE}" "${IMAGE_CONTEXT}"
    fi
    if [[ -n "${KIND_NODE_IMAGE}" ]]; then
        print_cmd kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}"
    else
        print_cmd kind create cluster --name "${CLUSTER_NAME}"
    fi
    print_cmd kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" cluster-info
    echo "+ kubectl --context kind-${CLUSTER_NAME} create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl --context kind-${CLUSTER_NAME} apply -f -"
    if needs_in_cluster_postgres; then
        print_cmd docker pull "${PG_IN_CLUSTER_IMAGE}"
        print_cmd kind load docker-image "${PG_IN_CLUSTER_IMAGE}" --name "${CLUSTER_NAME}"
        echo "+ kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} apply -f - (in-cluster postgres Deployment + Service ${PG_IN_CLUSTER_HOST})"
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${PG_IN_CLUSTER_HOST}" --timeout "${TIMEOUT}"
        echo "+ kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} create secret generic ${PG_SECRET_NAME} --from-literal=${PG_SECRET_KEY}=<in-cluster DSN>"
        if [[ "${PROJECTION_BACKEND}" == "postgres" ]]; then
            echo "+ kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} create secret generic ${PG_PROJECTION_SECRET_NAME} --from-literal=${PG_PROJECTION_SECRET_KEY}=<in-cluster DSN>"
        fi
    fi
    print_cmd helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" --kube-context "kind-${CLUSTER_NAME}" --namespace "${NAMESPACE}" --values "${values}" --set "fullnameOverride=${RELEASE_NAME}" --set "image.repository=${image_repository}" --set "image.tag=${image_tag}" --set "image.pullPolicy=IfNotPresent" --set "bootstrap.generated.count=${BOOTSTRAP_GENERATED_COUNT}" --set-string "bootstrap.generated.tenant=${BOOTSTRAP_GENERATED_TENANT}" --set-string "bootstrap.generated.prefix=${BOOTSTRAP_GENERATED_PREFIX}" --wait --timeout "${TIMEOUT}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    echo "+ kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} port-forward pod/<ready-pqueue-pod> ${SMOKE_PORT}:8080"
    echo "+ RESP PING 127.0.0.1:${SMOKE_PORT}"
    echo "+ RESP XADD/XREADGROUP 127.0.0.1:${SMOKE_PORT}"
    case "${LOG_BACKEND}" in
        objectlog | postgres)
            echo "+ RESP XADD before restart"
            print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
            print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
            echo "+ RESP XREADGROUP after restart"
            ;;
    esac
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

pod_selector() {
    printf 'app.kubernetes.io/instance=%s,app.kubernetes.io/name=pqueue' "${RELEASE_NAME}"
}

current_ready_pod() {
    local selector pod_name
    selector="$(pod_selector)"
    pod_name="$(
        kubectl_cmd -n "${NAMESPACE}" get pods \
            -l "${selector}" \
            --field-selector status.phase=Running \
            -o jsonpath='{range .items[*]}{.metadata.creationTimestamp}{" "}{.metadata.name}{"\n"}{end}' |
            sort |
            tail -n 1 |
            awk '{print $2}'
    )"
    [[ -n "${pod_name}" ]] || die "no running pqueue pod found for selector ${selector}"
    {
        print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" wait --for=condition=Ready "pod/${pod_name}" --timeout "${TIMEOUT}"
    } >&2
    kubectl_cmd -n "${NAMESPACE}" wait --for=condition=Ready "pod/${pod_name}" --timeout "${TIMEOUT}" >&2
    printf '%s\n' "${pod_name}"
}

start_resp_port_forward() {
    local run_dir log_path pod_name
    run_dir="${REPO_ROOT}/target/kind-helm-test/${CLUSTER_NAME}"
    mkdir -p "${run_dir}"
    log_path="${run_dir}/port-forward.log"
    pod_name="$(current_ready_pod)"

    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" port-forward "pod/${pod_name}" "${SMOKE_PORT}:8080"
    kubectl_cmd -n "${NAMESPACE}" port-forward "pod/${pod_name}" "${SMOKE_PORT}:8080" >"${log_path}" 2>&1 &
    PF_PID=$!

    wait_for_port_forward "${log_path}"
}

smoke_resp_ping() {
    local response_path="$1"

    echo "+ RESP PING 127.0.0.1:${SMOKE_PORT}"
    # shellcheck disable=SC2016 # RESP bulk-string length is literal protocol data.
    resp_request "${response_path}" '*1\r\n$4\r\nPING\r\n'
    if ! grep -Fq '+PONG' "${response_path}"; then
        err "RESP PING did not return PONG"
        sed -n '1,80p' "${response_path}" >&2 || true
        return 1
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
    RESP_SMOKE_PORT="${SMOKE_PORT}" \
    RESP_SMOKE_RESPONSE="${response_path}" \
    RESP_SMOKE_PAYLOAD="${payload}" \
    RESP_SMOKE_ARGS="${RESP_SMOKE_ARGS:-}" \
    python3 - <<'PY'
import os
import socket
import sys
import time
from pathlib import Path

port = int(os.environ["RESP_SMOKE_PORT"])
response = Path(os.environ["RESP_SMOKE_RESPONSE"])
args = os.environ.get("RESP_SMOKE_ARGS", "").splitlines()
if args:
    encoded = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        value = arg.encode()
        encoded.extend((f"${len(value)}\r\n".encode(), value, b"\r\n"))
    payload = b"".join(encoded)
else:
    payload = os.environ["RESP_SMOKE_PAYLOAD"].encode("utf-8").decode("unicode_escape").encode("latin1")
deadline = time.monotonic() + 5.0
chunks = []

try:
    with socket.create_connection(("127.0.0.1", port), timeout=2.0) as sock:
        sock.settimeout(0.25)
        sock.sendall(payload)
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass

        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                if chunks:
                    break
                continue
            if not chunk:
                break
            chunks.append(chunk)
except OSError as exc:
    print(f"RESP request failed: {exc}", file=sys.stderr)
    sys.exit(1)

if not chunks:
    print("RESP request timed out waiting for a response", file=sys.stderr)
    sys.exit(1)

response.write_bytes(b"".join(chunks))
PY
}

resp_command() {
    local response_path="$1"
    shift
    local encoded_args
    encoded_args="$(printf '%s\n' "$@")"
    RESP_SMOKE_ARGS="$encoded_args" resp_request "$response_path" ""
}

smoke_resp() {
    local run_dir response_path
    run_dir="${REPO_ROOT}/target/kind-helm-test/${CLUSTER_NAME}"
    mkdir -p "${run_dir}"
    response_path="${run_dir}/resp.response"

    start_resp_port_forward
    smoke_resp_ping "${response_path}"

    echo "+ RESP XADD 127.0.0.1:${SMOKE_PORT}"
    resp_command "${response_path}" XADD "${SMOKE_QUEUE}" '*' priority 1
    if ! grep -Eq '^\$[0-9]+' "${response_path}"; then
        err "RESP XADD did not return a bulk item id"
        sed -n '1,80p' "${response_path}" >&2 || true
        return 1
    fi

    echo "+ RESP XREADGROUP 127.0.0.1:${SMOKE_PORT}"
    resp_command "${response_path}" XREADGROUP GROUP g c COUNT 1 STREAMS "${SMOKE_QUEUE}" '>'
    if ! grep -Fq "${SMOKE_QUEUE}" "${response_path}"; then
        err "RESP XREADGROUP did not return the bootstrap queue"
        sed -n '1,120p' "${response_path}" >&2 || true
        return 1
    fi

    echo "RESP smoke passed"
}

# Durable-backend restart recovery: push an item, restart the pqueue Deployment, and prove the item is
# recovered after restart. Runs for the durable log axes (objectlog and postgres); the in-memory-only
# combos have nothing durable to recover, so it is a no-op there.
smoke_durable_restart_runtime() {
    case "${LOG_BACKEND}" in
        objectlog | postgres) ;;
        *) return 0 ;;
    esac

    local run_dir response_path
    run_dir="${REPO_ROOT}/target/kind-helm-test/${CLUSTER_NAME}"
    response_path="${run_dir}/durable-restart-recovery.response"

    echo "+ RESP XADD before restart (durable backend: ${LOG_BACKEND})"
    resp_command "${response_path}" XADD "${SMOKE_QUEUE}" '*' priority 2
    if ! grep -Eq '^\$[0-9]+' "${response_path}"; then
        err "durable pre-restart XADD did not return a bulk item id"
        sed -n '1,80p' "${response_path}" >&2 || true
        return 1
    fi

    stop_port_forward
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
    kubectl_cmd -n "${NAMESPACE}" rollout restart "deployment/${RELEASE_NAME}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    kubectl_cmd -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"

    start_resp_port_forward
    smoke_resp_ping "${response_path}"
    echo "+ RESP XREADGROUP after restart"
    resp_command "${response_path}" XREADGROUP GROUP g restarted COUNT 1 STREAMS "${SMOKE_QUEUE}" '>'
    if ! grep -Fq "${SMOKE_QUEUE}" "${response_path}"; then
        err "durable post-restart XREADGROUP did not recover queue data (${LOG_BACKEND})"
        sed -n '1,120p' "${response_path}" >&2 || true
        return 1
    fi
    echo "durable (${LOG_BACKEND}) restart recovery smoke passed"
}

create_namespace() {
    echo "+ kubectl --context kind-${CLUSTER_NAME} create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl --context kind-${CLUSTER_NAME} apply -f -"
    kubectl_cmd create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl_cmd apply -f -
}

# Stand up a throwaway in-cluster postgres (Deployment + ClusterIP Service) and publish its DSN as the
# Secret the postgres-inmemory values file references (${PG_SECRET_NAME}/${PG_SECRET_KEY}). Ephemeral
# (emptyDir) — the smoke only needs a live database for the RESP round-trip, not cross-pod durability.
deploy_in_cluster_postgres() {
    needs_in_cluster_postgres || return 0

    echo "=== deploying throwaway in-cluster postgres (${PG_IN_CLUSTER_IMAGE}) ==="
    # Preload the postgres image into the kind node so the pod does not depend on a registry pull.
    run docker pull "${PG_IN_CLUSTER_IMAGE}"
    run kind load docker-image "${PG_IN_CLUSTER_IMAGE}" --name "${CLUSTER_NAME}"

    kubectl_cmd -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${PG_IN_CLUSTER_HOST}
  labels: { app: ${PG_IN_CLUSTER_HOST} }
spec:
  replicas: 1
  selector: { matchLabels: { app: ${PG_IN_CLUSTER_HOST} } }
  template:
    metadata:
      labels: { app: ${PG_IN_CLUSTER_HOST} }
    spec:
      containers:
        - name: postgres
          image: ${PG_IN_CLUSTER_IMAGE}
          imagePullPolicy: IfNotPresent
          env:
            - { name: POSTGRES_USER, value: "${PG_IN_CLUSTER_USER}" }
            - { name: POSTGRES_PASSWORD, value: "${PG_IN_CLUSTER_PASSWORD}" }
            - { name: POSTGRES_DB, value: "${PG_IN_CLUSTER_DB}" }
            - { name: PGDATA, value: "/var/lib/postgresql/data/pgdata" }
          ports: [ { containerPort: 5432 } ]
          readinessProbe:
            exec: { command: ["pg_isready", "-U", "${PG_IN_CLUSTER_USER}", "-d", "${PG_IN_CLUSTER_DB}"] }
            initialDelaySeconds: 3
            periodSeconds: 3
          volumeMounts:
            - { name: data, mountPath: /var/lib/postgresql/data }
      volumes:
        - { name: data, emptyDir: {} }
---
apiVersion: v1
kind: Service
metadata:
  name: ${PG_IN_CLUSTER_HOST}
spec:
  selector: { app: ${PG_IN_CLUSTER_HOST} }
  ports: [ { port: 5432, targetPort: 5432 } ]
EOF

    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${PG_IN_CLUSTER_HOST}" --timeout "${TIMEOUT}"
    kubectl_cmd -n "${NAMESPACE}" rollout status "deployment/${PG_IN_CLUSTER_HOST}" --timeout "${TIMEOUT}"

    local dsn="postgres://${PG_IN_CLUSTER_USER}:${PG_IN_CLUSTER_PASSWORD}@${PG_IN_CLUSTER_HOST}:5432/${PG_IN_CLUSTER_DB}?sslmode=disable"
    echo "+ kubectl create secret generic ${PG_SECRET_NAME} (${PG_SECRET_KEY}=<in-cluster DSN>)"
    kubectl_cmd -n "${NAMESPACE}" create secret generic "${PG_SECRET_NAME}" \
        --from-literal="${PG_SECRET_KEY}=${dsn}" \
        --dry-run=client -o yaml | kubectl_cmd apply -f -

    # The postgres/postgres combo drives its projection axis through a second postgres connection
    # (distinct table sets from the log axis, no collision - see crates/pqueue-server/src/lib.rs's
    # postgres/postgres composition). Reuse the same throwaway in-cluster postgres instance and DSN, under
    # the projection Secret name the postgres-postgres values file expects.
    if [[ "${PROJECTION_BACKEND}" == "postgres" ]]; then
        echo "+ kubectl create secret generic ${PG_PROJECTION_SECRET_NAME} (${PG_PROJECTION_SECRET_KEY}=<in-cluster DSN>)"
        kubectl_cmd -n "${NAMESPACE}" create secret generic "${PG_PROJECTION_SECRET_NAME}" \
            --from-literal="${PG_PROJECTION_SECRET_KEY}=${dsn}" \
            --dry-run=client -o yaml | kubectl_cmd apply -f -
    fi
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
    echo "context:   ${IMAGE_CONTEXT}"
    if [[ -n "${IMAGE_DOCKERFILE}" ]]; then
        echo "dockerfile:${IMAGE_DOCKERFILE}"
    fi

    local cargo_features build_args=()
    cargo_features="$(image_cargo_features)"
    if [[ -n "${cargo_features}" ]]; then
        echo "features:  ${cargo_features}"
        build_args=(--build-arg "CARGO_FEATURES=${cargo_features}")
    fi

    if [[ -n "${IMAGE_DOCKERFILE}" ]]; then
        run docker build "${build_args[@]}" -f "${IMAGE_DOCKERFILE}" -t "${IMAGE}" "${IMAGE_CONTEXT}"
    else
        run docker build "${build_args[@]}" -t "${IMAGE}" "${IMAGE_CONTEXT}"
    fi
    if [[ -n "${KIND_NODE_IMAGE}" ]]; then
        run kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}"
    else
        run kind create cluster --name "${CLUSTER_NAME}"
    fi
    CLUSTER_CREATED=true
    run kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"
    wait_for_kubernetes_api
    create_namespace
    deploy_in_cluster_postgres
    run helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
        --kube-context "kind-${CLUSTER_NAME}" \
        --namespace "${NAMESPACE}" \
        --values "${values}" \
        --set "fullnameOverride=${RELEASE_NAME}" \
        --set "image.repository=${image_repository}" \
        --set "image.tag=${image_tag}" \
        --set "image.pullPolicy=IfNotPresent" \
        --set "bootstrap.generated.count=${BOOTSTRAP_GENERATED_COUNT}" \
        --set-string "bootstrap.generated.tenant=${BOOTSTRAP_GENERATED_TENANT}" \
        --set-string "bootstrap.generated.prefix=${BOOTSTRAP_GENERATED_PREFIX}" \
        --wait \
        --timeout "${TIMEOUT}"
    print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    kubectl_cmd -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
    smoke_resp
    smoke_durable_restart_runtime

    echo "=== kind Helm integration smoke PASSED ==="
}

main "$@"
