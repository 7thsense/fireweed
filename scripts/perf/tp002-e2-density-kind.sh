#!/usr/bin/env bash
# Live TP-002 E2 density proof: one durable objectlog/sqlite service with 1001 generated queues.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=${CLUSTER:-pqueue-density}
IMAGE=${IMAGE:-pqueue:density-e2}
QUEUE_COUNT=${QUEUE_COUNT:-1001}
ITEMS=${ITEMS:-300000}
CONTROL_ITEMS=${CONTROL_ITEMS:-10000}
HOT_CONNECTIONS=${HOT_CONNECTIONS:-8}
NOISY_WORKERS=${NOISY_WORKERS:-8}
SERVER_WORKERS=${SERVER_WORKERS:-4}
SEED=${SEED:-42}
PROGRESS_BOUND_MS=${PROGRESS_BOUND_MS:-60000}
EVIDENCE_MODE=${EVIDENCE_MODE:-release}
PROJECTION_BACKEND=${PROJECTION_BACKEND:-sqlite}
THREAD_LIMIT=4
CONNECTION_LIMIT=32
TASK_LIMIT=64
LEDGER_OUT=${LEDGER_OUT:-$REPO_ROOT/target/pqueue-ledger/tp002-e2-density-kind.jsonl}
DIAGNOSTICS_DIR=${DIAGNOSTICS_DIR:-$REPO_ROOT/target/pqueue-ledger/tp002-e2-density-kind-diagnostics}
KUBECONFIG_FILE=$(mktemp)
RESULT_FILE=$(mktemp)
RESOURCE_FILE=$(mktemp)
SAMPLER_STOP=$(mktemp)
rm -f "$SAMPLER_STOP"
NAMESPACE="pqueue-density-${RANDOM}"
SAMPLER_PID=
LOG_WATCH_PID=
OWNER_BASHPID=$BASHPID
mkdir -p "$DIAGNOSTICS_DIR"
PHASE_LOG="$DIAGNOSTICS_DIR/load-follow.log"
rm -f "$PHASE_LOG"

case "$EVIDENCE_MODE" in
  release)
    [[ "$PROJECTION_BACKEND" == sqlite ]]
    [[ "$QUEUE_COUNT" == 1001 && "$ITEMS" == 300000 && "$CONTROL_ITEMS" == 10000 ]]
    [[ "$HOT_CONNECTIONS" == 8 && "$NOISY_WORKERS" == 8 && "$SERVER_WORKERS" == 4 && "$SEED" == 42 ]]
    ;;
  d5-diagnostic)
    [[ "$PROJECTION_BACKEND" == sqlite ]]
    [[ "$QUEUE_COUNT" == 1001 && "$ITEMS" == 10000 && "$CONTROL_ITEMS" == 10000 ]]
    [[ "$HOT_CONNECTIONS" == 64 && "$NOISY_WORKERS" == 8 && "$SERVER_WORKERS" == 4 && "$SEED" == 42 ]]
    ;;
  *)
    echo "unknown EVIDENCE_MODE=$EVIDENCE_MODE" >&2
    exit 2
    ;;
esac

assert_source_unchanged() {
  [[ "$(git -C "$REPO_ROOT" rev-parse HEAD)" == "$REVISION" ]]
  [[ -z "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal)" ]]
}

capture_diagnostics() {
  kubectl -n "$NAMESPACE" get job density-load -o yaml >"$DIAGNOSTICS_DIR/job.yaml" 2>"$DIAGNOSTICS_DIR/job.err" || true
  kubectl -n "$NAMESPACE" get pods -o wide >"$DIAGNOSTICS_DIR/pods.txt" 2>"$DIAGNOSTICS_DIR/pods.err" || true
  kubectl -n "$NAMESPACE" logs job/density-load >"$DIAGNOSTICS_DIR/load.log" 2>"$DIAGNOSTICS_DIR/load.err" || true
  kubectl -n "$NAMESPACE" logs deployment/pqueue >"$DIAGNOSTICS_DIR/server.log" 2>"$DIAGNOSTICS_DIR/server.err" || true
}

cleanup() {
  # Bash subshells inherit EXIT traps. Only the original script process may own cluster cleanup.
  [[ "$BASHPID" == "$OWNER_BASHPID" ]] || return
  if [[ -n "$SAMPLER_PID" ]]; then
    touch "$SAMPLER_STOP"
    kill "$SAMPLER_PID" 2>/dev/null || true
    wait "$SAMPLER_PID" 2>/dev/null || true
  fi
  if [[ -n "$LOG_WATCH_PID" ]]; then
    kill "$LOG_WATCH_PID" 2>/dev/null || true
    wait "$LOG_WATCH_PID" 2>/dev/null || true
  fi
  capture_diagnostics
  KUBECONFIG="$KUBECONFIG_FILE" kubectl delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
  rm -f "$KUBECONFIG_FILE" "$RESULT_FILE" "$RESOURCE_FILE" "$SAMPLER_STOP"
}
trap cleanup EXIT

if [[ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal)" ]]; then
  echo "release evidence requires a clean worktree" >&2
  exit 1
fi
REVISION=$(git -C "$REPO_ROOT" rev-parse HEAD)

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER"
fi
kind get kubeconfig --name "$CLUSTER" >"$KUBECONFIG_FILE"
export KUBECONFIG="$KUBECONFIG_FILE"

docker build \
  --label "org.opencontainers.image.revision=$REVISION" \
  -f "$REPO_ROOT/Dockerfile.e2" -t "$IMAGE" "$REPO_ROOT"
DOCKER_IMAGE_ID=$(docker image inspect "$IMAGE" --format '{{.Id}}')
[[ "$DOCKER_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]]
[[ "$(docker image inspect "$IMAGE" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" == "$REVISION" ]]
kind load docker-image "$IMAGE" --name "$CLUSTER"
IMAGE_DIGEST=$(docker exec "${CLUSTER}-control-plane" crictl images -o json | jq -r \
  --arg id "$DOCKER_IMAGE_ID" '.images[] | select(.id == $id) | .repoDigests[0] | split("@")[1]' | head -n1)
[[ "$IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]

kubectl create namespace "$NAMESPACE"
cat <<YAML | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata: { name: pqueue, namespace: $NAMESPACE }
spec:
  replicas: 1
  selector: { matchLabels: { app: pqueue } }
  template:
    metadata: { labels: { app: pqueue } }
    spec:
      containers:
        - name: pqueue
          image: $IMAGE
          imagePullPolicy: Never
          ports: [ { containerPort: 8080 } ]
          env:
            - { name: PQUEUE_LOG_BACKEND, value: objectlog }
            # Keep the governed E2 identity exact. PQUEUE_OBJECT_LOG_MODE is a retired pseudo-axis and
            # is intentionally absent; setting it would falsely imply that it selects behavior.
            - { name: PQUEUE_PROJECTION_BACKEND, value: "$PROJECTION_BACKEND" }
            - { name: PQUEUE_OBJECT_LOG_ROOT, value: /data/object-log }
            - { name: PQUEUE_SQLITE_PROJECTION_PATH, value: /data/projection.db }
            - { name: PQUEUE_LISTEN_ADDR, value: "0.0.0.0:8080" }
            - { name: PQUEUE_WORKER_THREADS, value: "$SERVER_WORKERS" }
            - { name: PQUEUE_RUNTIME_RESOURCE_METRICS_PATH, value: /tmp/pqueue-runtime-resources.json }
            - { name: PQUEUE_BOOTSTRAP_GENERATED_COUNT, value: "$QUEUE_COUNT" }
            - { name: PQUEUE_BOOTSTRAP_GENERATED_TENANT, value: density }
            - { name: PQUEUE_BOOTSTRAP_GENERATED_PREFIX, value: q }
          readinessProbe:
            tcpSocket: { port: 8080 }
            periodSeconds: 1
            failureThreshold: 180
          resources:
            requests: { cpu: "1000m", memory: "512Mi" }
            limits: { cpu: "4000m", memory: "4Gi" }
          volumeMounts: [ { name: data, mountPath: /data } ]
      # Keep storage bounded without charging object-log and SQLite files to the container's 4 GiB
      # memory cgroup. The workload has no elapsed-time or throughput gate, so host-disk contention
      # changes capacity observations only and cannot become an implicit release condition.
      volumes: [ { name: data, emptyDir: { sizeLimit: 64Gi } } ]
---
apiVersion: v1
kind: Service
metadata: { name: pqueue, namespace: $NAMESPACE }
spec:
  selector: { app: pqueue }
  ports: [ { port: 8080, targetPort: 8080 } ]
YAML
until [[ "$(kubectl -n "$NAMESPACE" get deployment pqueue -o jsonpath='{.status.availableReplicas}' 2>/dev/null)" == 1 ]]; do
  failed_reason=$(kubectl -n "$NAMESPACE" get pods -l app=pqueue -o jsonpath='{range .items[*].status.containerStatuses[*].state.terminated}{.reason}{end}' 2>/dev/null || true)
  if [[ -n "$failed_reason" ]]; then
    capture_diagnostics
    echo "pqueue deployment terminated before readiness: $failed_reason" >&2
    exit 1
  fi
  sleep 2
done
SERVER_POD=$(kubectl -n "$NAMESPACE" get pod -l app=pqueue -o jsonpath='{.items[0].metadata.name}')
SERVER_IMAGE_ID=$(kubectl -n "$NAMESPACE" get pod "$SERVER_POD" -o jsonpath='{.status.containerStatuses[0].imageID}')
[[ "$SERVER_IMAGE_ID" == *"$IMAGE_DIGEST" ]]

NODE_IMAGE=$(docker inspect "${CLUSTER}-control-plane" --format '{{.Config.Image}}')
NODE_CAPACITY=$(kubectl get node -o jsonpath='{.items[0].status.capacity.cpu} {.items[0].status.capacity.memory}')
HARDWARE="$(nproc) host cores; $(awk '/MemTotal/ {printf "%.1f GiB RAM", $2/1024/1024}' /proc/meminfo); kind node $NODE_IMAGE capacity $NODE_CAPACITY; server limit 4 cores/4 GiB RAM"
TOPOLOGY="live one-node kind deployment; direct objectlog/sqlite projection on bounded 64 GiB disk-backed emptyDir; one service pod; $QUEUE_COUNT generated queues; one in-cluster load job"

cat <<YAML | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata: { name: density-load, namespace: $NAMESPACE }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: loadgen
          image: $IMAGE
          imagePullPolicy: Never
          command:
            - /usr/local/bin/pqueue-loadgen
            - density-run
            - --addr
            - pqueue.$NAMESPACE.svc.cluster.local:8080
            - --queue-count
            - "$QUEUE_COUNT"
            - --items
            - "$ITEMS"
            - --control-items
            - "$CONTROL_ITEMS"
            - --hot-connections
            - "$HOT_CONNECTIONS"
            - --noisy-workers
            - "$NOISY_WORKERS"
            - --seed
            - "$SEED"
            - --progress-bound-ms
            - "$PROGRESS_BOUND_MS"
          resources:
            requests: { cpu: "1000m", memory: "512Mi" }
            limits: { cpu: "4000m", memory: "4Gi" }
YAML

until [[ "$(kubectl -n "$NAMESPACE" get pod -l job-name=density-load -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null)" == true ]]; do
  failed_reason=$(kubectl -n "$NAMESPACE" get pod -l job-name=density-load -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.reason}' 2>/dev/null || true)
  if [[ -n "$failed_reason" ]]; then
    capture_diagnostics
    echo "density load pod terminated before readiness: $failed_reason" >&2
    exit 1
  fi
  sleep 2
done
LOAD_POD=$(kubectl -n "$NAMESPACE" get pod -l job-name=density-load -o jsonpath='{.items[0].metadata.name}')
LOAD_IMAGE_ID=$(kubectl -n "$NAMESPACE" get pod "$LOAD_POD" -o jsonpath='{.status.containerStatuses[0].imageID}')
[[ "$LOAD_IMAGE_ID" == *"$IMAGE_DIGEST" ]]
kubectl -n "$NAMESPACE" logs -f "$LOAD_POD" >"$PHASE_LOG" 2>&1 &
LOG_WATCH_PID=$!

# Sample only between the load generator's explicit HOT_START/HOT_END markers. Worker/task counts come
# from Tokio's live RuntimeMetrics reporter; connections come from the server network namespace.
(
  trap - EXIT
  while [[ ! -e "$SAMPLER_STOP" ]]; do
    if grep -q '^DENSITY_PHASE HOT_START ' "$PHASE_LOG" && ! grep -q '^DENSITY_PHASE HOT_END ' "$PHASE_LOG"; then
      runtime=$(kubectl -n "$NAMESPACE" exec "$SERVER_POD" -- cat /tmp/pqueue-runtime-resources.json 2>/dev/null) || continue
      threads=$(jq -r '.tokio_worker_threads' <<<"$runtime")
      tasks=$(jq -r '.tokio_alive_tasks' <<<"$runtime")
      connections=$(kubectl -n "$NAMESPACE" exec "$SERVER_POD" -- sh -c \
        'awk '\''$2 ~ /:1F90$/ && $4 == "01" {n++} END {print n+0}'\'' /proc/1/net/tcp' 2>/dev/null) || continue
      printf '%s %s %s %s\n' "$(date +%s%3N)" "$threads" "$connections" "$tasks" >>"$RESOURCE_FILE"
    fi
    sleep 0.25
  done
) &
SAMPLER_PID=$!

while true; do
  if ! job_json=$(kubectl -n "$NAMESPACE" get job density-load -o json 2>"$DIAGNOSTICS_DIR/job-poll.err"); then
    capture_diagnostics
    echo "density Job or namespace disappeared; diagnostics retained at $DIAGNOSTICS_DIR" >&2
    exit 1
  fi
  printf '%s\n' "$job_json" >"$DIAGNOSTICS_DIR/job-latest.json"
  succeeded=$(jq -r '.status.succeeded // 0' <<<"$job_json")
  failed=$(jq -r '.status.failed // 0' <<<"$job_json")
  [[ "$succeeded" == "1" ]] && break
  if [[ "$failed" != "0" ]]; then
    capture_diagnostics
    cat "$DIAGNOSTICS_DIR/load.log" || true
    exit 1
  fi
  sleep 2
done

logs=$(kubectl -n "$NAMESPACE" logs job/density-load)
printf '%s\n' "$logs"
printf '%s\n' "$logs" | sed -n 's/^DENSITY_RESULT //p' >"$RESULT_FILE"
test -s "$RESULT_FILE"
touch "$SAMPLER_STOP"
wait "$SAMPLER_PID"
SAMPLER_PID=
kill "$LOG_WATCH_PID" 2>/dev/null || true
wait "$LOG_WATCH_PID" 2>/dev/null || true
LOG_WATCH_PID=
# Use one atomic, post-completion copy for counter validation and the retained log hash; the streaming
# copy exists only so the resource sampler can observe HOT_START/HOT_END while the Job is live.
printf '%s\n' "$logs" >"$PHASE_LOG"
HOT_START_MS=$(jq -r '.hot_phase_started_unix_ms' "$RESULT_FILE")
HOT_END_MS=$(jq -r '.hot_phase_ended_unix_ms' "$RESULT_FILE")
read -r OBSERVED_THREADS OBSERVED_CONNECTIONS OBSERVED_TASKS HOT_PHASE_RESOURCE_SAMPLES FIRST_HOT_SAMPLE_MS LAST_HOT_SAMPLE_MS < <(
  awk -v start="$HOT_START_MS" -v end="$HOT_END_MS" '
    $1 >= start && $1 <= end {
      if (samples == 0) first=$1
      last=$1
      if ($2 > threads) threads=$2
      if ($3 > connections) connections=$3
      if ($4 > tasks) tasks=$4
      samples++
    }
    END { print threads+0, connections+0, tasks+0, samples+0, first+0, last+0 }
  ' "$RESOURCE_FILE"
)
(( HOT_PHASE_RESOURCE_SAMPLES > 0 ))

mkdir -p "$(dirname "$LEDGER_OUT")"
assert_source_unchanged
if [[ "$EVIDENCE_MODE" == d5-diagnostic ]]; then
  grep -q '^DENSITY_STAGE stage=READINESS status=DONE$' "$PHASE_LOG"
  grep -q '^DENSITY_STAGE stage=INVENTORY status=DONE completed=1001 total=1001$' "$PHASE_LOG"
  grep -q '^DENSITY_STAGE stage=COLD_PRIME status=DONE completed=1000 total=1000$' "$PHASE_LOG"
  for stage in BASELINE_INGEST BASELINE_CLAIM_FINALIZE LOADED_INGEST LOADED_CLAIM_FINALIZE BASELINE_AFTER_INGEST BASELINE_AFTER_CLAIM_FINALIZE; do
    awk -v stage="$stage" '
      $0 ~ "^DENSITY_PROGRESS stage=" stage " " {
        for (i = 1; i <= NF; i++) {
          if ($i ~ /^completed=/) { split($i, a, "="); completed=a[2] }
          if ($i ~ /^total=/) { split($i, a, "="); total=a[2] }
        }
      }
      END { exit !(completed == 10000 && total == 10000) }
    ' "$PHASE_LOG"
  done
  PHASE_LOG_SHA256=$(sha256sum "$PHASE_LOG" | awk '{print $1}')
  jq -n \
    --slurpfile result "$RESULT_FILE" \
    --arg revision "$REVISION" --arg image_digest "$IMAGE_DIGEST" \
    --arg topology "$TOPOLOGY" --arg hardware "$HARDWARE" \
    --arg phase_log_sha256 "$PHASE_LOG_SHA256" \
    --argjson observed_threads "$OBSERVED_THREADS" \
    --argjson observed_connections "$OBSERVED_CONNECTIONS" \
    --argjson observed_tasks "$OBSERVED_TASKS" \
    --argjson resource_samples "$HOT_PHASE_RESOURCE_SAMPLES" '
      {
        suite: "pqueue-d5-live-density-diagnostic",
        revision: $revision,
        image_digest: $image_digest,
        topology: $topology,
        hardware: $hardware,
        configuration: {
          total_queues: 1001, hot_items: 10000, control_items: 10000,
          hot_connections: 64, cold_workers: 8, server_workers: 4, seed: 42
        },
        stage_counters: {
          readiness: 1, inventory: 1001, baseline_ingest: 10000,
          baseline_claim_finalize: 10000, cold_prime: 1000,
          loaded_ingest: 10000, loaded_claim_finalize: 10000,
          baseline_after_ingest: 10000, baseline_after_claim_finalize: 10000
        },
        observed_resource_highs: {
          tokio_worker_threads: $observed_threads,
          established_connections: $observed_connections,
          live_tasks: $observed_tasks,
          hot_phase_samples: $resource_samples
        },
        phase_log_sha256: $phase_log_sha256,
        result: $result[0]
      }
      | .bars_met = (
          (.revision | length) == 40
          and (.image_digest | startswith("sha256:"))
          and .result.hot_items == 10000
          and .result.control_items == 10000
          and .result.hot_connections == 64
          and .result.cold_worker_count == 8
          and .result.seed == 42
          and .result.total_queues == 1001
          and .result.cold_queues_active == 1000
          and .result.cold_queues_progress_eligible == 1000
          and .result.cold_empty_claim_responses == 0
          and .result.hot_ingest_per_s > 0
          and .result.hot_claim_finalize_per_s > 0
          and .observed_resource_highs.tokio_worker_threads == 4
          and .observed_resource_highs.established_connections > 0
          and .observed_resource_highs.live_tasks > 0
          and .observed_resource_highs.hot_phase_samples > 0
        )
    ' >"$LEDGER_OUT"
  jq -e '.bars_met == true' "$LEDGER_OUT" >/dev/null
  assert_source_unchanged
  printf 'DIAGNOSTIC_OUT=%s\n' "$LEDGER_OUT"
  exit 0
fi
rustup run 1.92.0 cargo run --locked --quiet -p pqueue-loadgen -- density-emit-row \
  --result "$RESULT_FILE" \
  --observed-threads "$OBSERVED_THREADS" --thread-limit "$THREAD_LIMIT" \
  --observed-connections "$OBSERVED_CONNECTIONS" --connection-limit "$CONNECTION_LIMIT" \
  --observed-tasks "$OBSERVED_TASKS" --task-limit "$TASK_LIMIT" \
  --hot-phase-resource-samples "$HOT_PHASE_RESOURCE_SAMPLES" \
  --first-hot-resource-sample-ms "$FIRST_HOT_SAMPLE_MS" \
  --last-hot-resource-sample-ms "$LAST_HOT_SAMPLE_MS" \
  --revision "$REVISION" --image-digest "$IMAGE_DIGEST" \
  --topology "$TOPOLOGY" --hardware "$HARDWARE" --out "$LEDGER_OUT"
assert_source_unchanged
rustup run 1.92.0 cargo run --locked --quiet -p pqueue-release --bin pqueue-verify-density-evidence -- "$LEDGER_OUT"
assert_source_unchanged
printf 'LEDGER_OUT=%s\n' "$LEDGER_OUT"
