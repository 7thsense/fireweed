#!/usr/bin/env bash
# Live TP-002 E2 density proof: one durable objectlog/sqlite service with 1001 generated queues.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=${CLUSTER:-pqueue-density}
IMAGE=${IMAGE:-pqueue:density-e2}
QUEUE_COUNT=${QUEUE_COUNT:-1001}
ITEMS=${ITEMS:-300000}
HOT_CONNECTIONS=${HOT_CONNECTIONS:-8}
NOISY_WORKERS=${NOISY_WORKERS:-8}
SERVER_WORKERS=${SERVER_WORKERS:-4}
SEED=${SEED:-42}
PROGRESS_BOUND_MS=${PROGRESS_BOUND_MS:-60000}
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
            # TD-004's async profile is the production group-commit object-log + durable SQLite
            # projection path for sustained load. Its bounded apply-debt controls remain at their
            # governed defaults. PQUEUE_OBJECT_LOG_MODE is a retired pseudo-axis and is intentionally
            # absent; setting it would falsely imply that it selects segmented/group-commit behavior.
            - { name: PQUEUE_PROJECTION_BACKEND, value: hybrid-async }
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
      # Match the governed TP-002 kind substrate: exercise the real object-log fsync and SQLite
      # projection paths without turning a co-located host disk's unrelated I/O queue into an
      # implicit release condition. Kubernetes charges this bounded tmpfs to the pod's memory limit.
      volumes: [ { name: data, emptyDir: { medium: Memory, sizeLimit: 4Gi } } ]
---
apiVersion: v1
kind: Service
metadata: { name: pqueue, namespace: $NAMESPACE }
spec:
  selector: { app: pqueue }
  ports: [ { port: 8080, targetPort: 8080 } ]
YAML
kubectl -n "$NAMESPACE" rollout status deployment/pqueue --timeout=300s
SERVER_POD=$(kubectl -n "$NAMESPACE" get pod -l app=pqueue -o jsonpath='{.items[0].metadata.name}')
SERVER_IMAGE_ID=$(kubectl -n "$NAMESPACE" get pod "$SERVER_POD" -o jsonpath='{.status.containerStatuses[0].imageID}')
[[ "$SERVER_IMAGE_ID" == *"$IMAGE_DIGEST" ]]

NODE_IMAGE=$(docker inspect "${CLUSTER}-control-plane" --format '{{.Config.Image}}')
NODE_CAPACITY=$(kubectl get node -o jsonpath='{.items[0].status.capacity.cpu} {.items[0].status.capacity.memory}')
HARDWARE="$(nproc) host cores; $(awk '/MemTotal/ {printf "%.1f GiB RAM", $2/1024/1024}' /proc/meminfo); kind node $NODE_IMAGE capacity $NODE_CAPACITY; server limit 4 cores/4 GiB RAM"
TOPOLOGY="live one-node kind deployment; TD-004 objectlog/hybrid-async bounded-debt SQLite projection on bounded 4 GiB emptyDir tmpfs; one service pod; $QUEUE_COUNT generated queues; one in-cluster load job"

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

kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l job-name=density-load --timeout=300s
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

deadline=$((SECONDS + 1800))
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
  if (( SECONDS >= deadline )); then
    capture_diagnostics
    cat "$DIAGNOSTICS_DIR/load.log" || true
    echo "density load timed out; diagnostics retained at $DIAGNOSTICS_DIR" >&2
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
