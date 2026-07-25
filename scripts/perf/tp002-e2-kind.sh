#!/usr/bin/env bash
# TP-002 E2 multi-node object_log_sqlite_projection release evidence on a kind cluster (ADR-008: the queue is
# the unit of sharding). Beads pqueue-a983b5e2 + pqueue-b5af53fb.
#
# WHY KIND (the design): prior raw-docker E2 missed the bars because a single fat ~64-thread HOST load driver
# stole cores from the 8 co-located server containers, unmanaged, starving each below the per-queue floor.
# kind enforces the fix: (1) CPU-LIMITED server pods so each owner gets a guaranteed slice (constant per-pod
# CPU -> constant per-pod throughput -> linear 2->4->8 scaling -> the 8/2 ratio holds by construction), and
# (2) a LEAN, SEPARATED, IN-CLUSTER load Job with a BOUNDED CPU limit driving pod->pod over Service ClusterIP
# (immune to this sandbox's host->pod sustained-loopback signal-16 kill).
#
# For each owner count K in {2,4,8}: deploy K independent fireweed-service Deployments+Services (segmented
# object_log_sqlite_projection, distinct FIREWEED_NODE_ID, DISJOINT FIREWEED_BOOTSTRAP_QUEUES, emptyDir
# medium=Memory tmpfs), run a load Job that drives the workload + proves one-owner-per-queue, collect the
# measured RESULT json. Three full sweeps; each sweep folds its 2/4/8 results into one E2 ledger row
# (release-tier only when all four bars hold). Reliable pass == all sweeps release-tier.
set -euo pipefail

CLUSTER=${CLUSTER:-fireweed-e2}
IMAGE=${IMAGE:-fireweed-service:e2}
SERVER_CPU_LIMIT=${SERVER_CPU_LIMIT:-1300m}
SERVER_CPU_REQUEST=${SERVER_CPU_REQUEST:-1000m}
LOADGEN_CPU_LIMIT=${LOADGEN_CPU_LIMIT:-2000m}
LOADGEN_CPU_REQUEST=${LOADGEN_CPU_REQUEST:-1000m}
WORKER_THREADS=${WORKER_THREADS:-2}
SEG_LATENCY_MS=${SEG_LATENCY_MS:-1}
SEG_TARGET_BYTES=${SEG_TARGET_BYTES:-262144}
ITEMS_PER_QUEUE=${ITEMS_PER_QUEUE:-12000}
CONNS_PER_QUEUE=${CONNS_PER_QUEUE:-8}
PIPE=${PIPE:-1000}
BATCH=${BATCH:-1000}
QUEUES_PER_OWNER=${QUEUES_PER_OWNER:-1}
SWEEPS=${SWEEPS:-3}
COUNTS=(2 4 8)
SKIP_BUILD=${SKIP_BUILD:-0}

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
WORKDIR=$(mktemp -d)
LEDGER_OUT=${LEDGER_OUT:-$WORKDIR/tp002-e2-multinode-kind.jsonl}
LOADGEN_BIN=${LOADGEN_BIN:-$REPO_ROOT/target/release/fireweed-loadgen}
CORES=$(nproc)
NODE_IMAGE=""

log() { printf '\n=== %s ===\n' "$*" >&2; }

build_loadgen_host() {
  if [[ ! -x "$LOADGEN_BIN" ]]; then
    log "building host fireweed-loadgen (for emit-row)"
    (cd "$REPO_ROOT" && cargo build --release -p fireweed-loadgen --bin fireweed-loadgen >&2)
  fi
}

ensure_cluster() {
  if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    log "creating kind cluster $CLUSTER"
    kind create cluster --name "$CLUSTER" >&2
  fi
  NODE_IMAGE=$(docker inspect "${CLUSTER}-control-plane" --format '{{.Config.Image}}' 2>/dev/null || echo "kindest/node")
  # This sandbox kills sustained traffic over the host->container PUBLISHED loopback port (127.0.0.1:<port>),
  # which is how kind's kubeconfig reaches the API server (signal-16, the same wall the findings describe).
  # The control-plane container's BRIDGE IP survives (the apiserver serving cert SANs include it), so repoint
  # the kubeconfig at https://<bridge-ip>:6443. (In-cluster pod->pod load is unaffected.)
  local cp_ip
  cp_ip=$(docker inspect "${CLUSTER}-control-plane" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
  kubectl config set-cluster "kind-${CLUSTER}" --server "https://${cp_ip}:6443" >&2
  kubectl config use-context "kind-${CLUSTER}" >&2
}

build_and_load() {
  if [[ "$SKIP_BUILD" != "1" ]]; then
    log "building $IMAGE (Dockerfile.e2: service + loadgen)"
    (cd "$REPO_ROOT" && docker build -f Dockerfile.e2 -t "$IMAGE" . >&2)
  fi
  log "loading $IMAGE into kind/$CLUSTER"
  kind load docker-image "$IMAGE" --name "$CLUSTER" >&2
}

# Emit a server Deployment+Service for one owner into stdout.
server_manifest() {
  local ns=$1 k=$2 idx=$3
  local name="fireweed-o${idx}"
  local node_id=$((idx + 1))
  local queues=""
  for ((j = 0; j < QUEUES_PER_OWNER; j++)); do
    [[ -n "$queues" ]] && queues+=","
    queues+="t1:o${k}n${idx}q${j}"
  done
  cat <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  namespace: ${ns}
spec:
  replicas: 1
  selector:
    matchLabels: { app: ${name} }
  template:
    metadata:
      labels: { app: ${name} }
    spec:
      containers:
        - name: fireweed
          image: ${IMAGE}
          imagePullPolicy: Never
          ports: [ { containerPort: 8080 } ]
          resources:
            requests: { cpu: "${SERVER_CPU_REQUEST}", memory: "256Mi" }
            limits: { cpu: "${SERVER_CPU_LIMIT}", memory: "1Gi" }
          env:
            - { name: FIREWEED_LOG_BACKEND, value: "objectlog" }
            - { name: FIREWEED_PROJECTION_BACKEND, value: "sqlite" }
            - { name: FIREWEED_OBJECT_LOG_MODE, value: "segmented" }
            - { name: FIREWEED_SEGMENT_TARGET_BYTES, value: "${SEG_TARGET_BYTES}" }
            - { name: FIREWEED_SEGMENT_MAX_LATENCY_MS, value: "${SEG_LATENCY_MS}" }
            - { name: FIREWEED_WORKER_THREADS, value: "${WORKER_THREADS}" }
            - { name: FIREWEED_NODE_ID, value: "${node_id}" }
            - { name: FIREWEED_OBJECT_LOG_ROOT, value: "/data/olog" }
            - { name: FIREWEED_SQLITE_PROJECTION_PATH, value: "/data/proj.db" }
            - { name: FIREWEED_LISTEN_ADDR, value: "0.0.0.0:8080" }
            - { name: FIREWEED_BOOTSTRAP_QUEUES, value: "${queues}" }
            - { name: FIREWEED_RECLAIM_INTERVAL_MS, value: "60000" }
          readinessProbe:
            tcpSocket: { port: 8080 }
            periodSeconds: 1
            failureThreshold: 60
          volumeMounts: [ { name: data, mountPath: /data } ]
      volumes:
        - name: data
          emptyDir: { medium: Memory }
---
apiVersion: v1
kind: Service
metadata:
  name: ${name}
  namespace: ${ns}
spec:
  selector: { app: ${name} }
  ports: [ { port: 8080, targetPort: 8080 } ]
YAML
}

# Build the loadgen RunSpec JSON for one owner count.
spec_json() {
  local ns=$1 k=$2
  local nodes=""
  for ((idx = 0; idx < k; idx++)); do
    local qs=""
    for ((j = 0; j < QUEUES_PER_OWNER; j++)); do
      [[ -n "$qs" ]] && qs+=","
      qs+="\"t1:o${k}n${idx}q${j}\""
    done
    [[ -n "$nodes" ]] && nodes+=","
    nodes+="{\"addr\":\"fireweed-o${idx}.${ns}.svc.cluster.local:8080\",\"queues\":[${qs}]}"
  done
  printf '{"owners":%d,"nodes":[%s]}' "$k" "$nodes"
}

loadgen_job_manifest() {
  local ns=$1
  cat <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: loadgen
  namespace: ${ns}
spec:
  backoffLimit: 0
  completions: 1
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: loadgen
          image: ${IMAGE}
          imagePullPolicy: Never
          command:
            - /usr/local/bin/fireweed-loadgen
            - run
            - --spec-file
            - /spec/spec.json
            - --items-per-queue
            - "${ITEMS_PER_QUEUE}"
            - --conns-per-queue
            - "${CONNS_PER_QUEUE}"
            - --pipe
            - "${PIPE}"
            - --batch
            - "${BATCH}"
          resources:
            requests: { cpu: "${LOADGEN_CPU_REQUEST}", memory: "256Mi" }
            limits: { cpu: "${LOADGEN_CPU_LIMIT}", memory: "1Gi" }
          volumeMounts: [ { name: spec, mountPath: /spec } ]
      volumes:
        - name: spec
          configMap: { name: loadgen-spec }
YAML
}

wait_job() {
  local ns=$1 name=$2 timeout=${3:-420}
  local end=$((SECONDS + timeout))
  while ((SECONDS < end)); do
    local succ fail
    succ=$(kubectl -n "$ns" get job "$name" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
    fail=$(kubectl -n "$ns" get job "$name" -o jsonpath='{.status.failed}' 2>/dev/null || true)
    [[ "$succ" == "1" ]] && return 0
    [[ -n "$fail" && "$fail" != "0" ]] && return 1
    sleep 2
  done
  return 2
}

run_count() {
  local sweep=$1 k=$2
  local ns="e2-s${sweep}-k${k}"
  kubectl create namespace "$ns" >&2
  log "sweep ${sweep} / owners ${k}: deploying ${k} owner pods in ${ns}"
  for ((idx = 0; idx < k; idx++)); do
    server_manifest "$ns" "$k" "$idx" | kubectl apply -f - >&2
  done
  for ((idx = 0; idx < k; idx++)); do
    kubectl -n "$ns" rollout status deploy/"fireweed-o${idx}" --timeout=120s >&2
  done
  kubectl -n "$ns" create configmap loadgen-spec \
    --from-literal=spec.json="$(spec_json "$ns" "$k")" >&2
  loadgen_job_manifest "$ns" | kubectl apply -f - >&2
  local rc=0
  wait_job "$ns" loadgen 420 || rc=$?
  local pod
  pod=$(kubectl -n "$ns" get pods -l job-name=loadgen -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  local logs
  logs=$(kubectl -n "$ns" logs "$pod" 2>/dev/null || true)
  printf '%s\n' "$logs" >&2
  if [[ "$rc" != "0" ]]; then
    log "sweep ${sweep} / owners ${k}: load Job did not complete (rc=$rc)"
    kubectl delete namespace "$ns" --wait=false >&2 || true
    return 1
  fi
  printf '%s\n' "$logs" | grep '^RESULT ' >"$WORKDIR/s${sweep}-k${k}.json"
  if [[ ! -s "$WORKDIR/s${sweep}-k${k}.json" ]]; then
    log "sweep ${sweep} / owners ${k}: no RESULT line in load logs"
    kubectl delete namespace "$ns" --wait=false >&2 || true
    return 1
  fi
  kubectl delete namespace "$ns" --wait=false >&2 || true
  return 0
}

main() {
  build_loadgen_host
  ensure_cluster
  build_and_load
  : >"$LEDGER_OUT"
  local sweeps_pass=0 sweeps_run=0
  for ((sweep = 1; sweep <= SWEEPS; sweep++)); do
    sweeps_run=$((sweeps_run + 1))
    local ok=1
    for k in "${COUNTS[@]}"; do
      run_count "$sweep" "$k" || { ok=0; break; }
    done
    if [[ "$ok" != "1" ]]; then
      log "sweep ${sweep}: a count failed to produce a result; aborting sweep"
      continue
    fi
    local tuning
    tuning=$(printf '{"source_revision":"%s","segment_max_latency_ms":%d,"segment_target_bytes":%d,"worker_threads_per_node":%d,"server_cpu_limit":"%s","server_cpu_request":"%s","loadgen_cpu_limit":"%s","cores":%d,"kind_node_image":"%s","pipe_size":%d,"batch_size":%d,"sweep":%d}' \
      "$(git -C "$REPO_ROOT" rev-parse HEAD)" "$SEG_LATENCY_MS" "$SEG_TARGET_BYTES" "$WORKER_THREADS" "$SERVER_CPU_LIMIT" "$SERVER_CPU_REQUEST" "$LOADGEN_CPU_LIMIT" "$CORES" "$NODE_IMAGE" "$PIPE" "$BATCH" "$sweep")
    local erc=0
    "$LOADGEN_BIN" emit-row \
      --result "$WORKDIR/s${sweep}-k2.json" \
      --result "$WORKDIR/s${sweep}-k4.json" \
      --result "$WORKDIR/s${sweep}-k8.json" \
      --tuning "$tuning" \
      --out "$LEDGER_OUT" >&2 || erc=$?
    [[ "$erc" == "0" ]] && sweeps_pass=$((sweeps_pass + 1))
  done
  log "SUMMARY: ${sweeps_pass}/${sweeps_run} sweeps met all release bars; ledger -> $LEDGER_OUT"
  printf 'LEDGER_OUT=%s\n' "$LEDGER_OUT"
  printf 'SWEEPS_PASS=%d SWEEPS_RUN=%d\n' "$sweeps_pass" "$sweeps_run"
  [[ "$sweeps_pass" == "$SWEEPS" && "$SWEEPS" -ge 1 ]]
}

main "$@"
