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
THREAD_LIMIT=64
CONNECTION_LIMIT=32
FD_LIMIT=256
LEDGER_OUT=${LEDGER_OUT:-$REPO_ROOT/target/pqueue-ledger/tp002-e2-density-kind.jsonl}
KUBECONFIG_FILE=$(mktemp)
RESULT_FILE=$(mktemp)
RESOURCE_FILE=$(mktemp)
SAMPLER_STOP=$(mktemp)
rm -f "$SAMPLER_STOP"
NAMESPACE="pqueue-density-${RANDOM}"
SAMPLER_PID=

cleanup() {
  if [[ -n "$SAMPLER_PID" ]]; then
    touch "$SAMPLER_STOP"
    wait "$SAMPLER_PID" 2>/dev/null || true
  fi
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
            - { name: PQUEUE_PROJECTION_BACKEND, value: sqlite }
            - { name: PQUEUE_OBJECT_LOG_MODE, value: segmented }
            - { name: PQUEUE_OBJECT_LOG_ROOT, value: /data/object-log }
            - { name: PQUEUE_SQLITE_PROJECTION_PATH, value: /data/projection.db }
            - { name: PQUEUE_LISTEN_ADDR, value: "0.0.0.0:8080" }
            - { name: PQUEUE_WORKER_THREADS, value: "$SERVER_WORKERS" }
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
      volumes: [ { name: data, emptyDir: {} } ]
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
TOPOLOGY="live one-node kind deployment; objectlog/sqlite; one service pod; $QUEUE_COUNT generated queues; one in-cluster load job"

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

# Sample the live server process throughout the hot run. Values are max observed process threads,
# established server-port TCP connections, and open file descriptors; they are not loadgen settings.
(
  max_threads=0
  max_connections=0
  max_fds=0
  while [[ ! -e "$SAMPLER_STOP" ]]; do
    sample=$(kubectl -n "$NAMESPACE" exec "$SERVER_POD" -- sh -c \
      'printf "%s " "$(find /proc/1/task -mindepth 1 -maxdepth 1 | wc -l)"; printf "%s " "$(awk '\''$2 ~ /:1F90$/ && $4 == "01" {n++} END {print n+0}'\'' /proc/1/net/tcp)"; find /proc/1/fd -mindepth 1 -maxdepth 1 | wc -l' 2>/dev/null) || continue
    read -r threads connections fds <<<"$sample"
    (( threads > max_threads )) && max_threads=$threads
    (( connections > max_connections )) && max_connections=$connections
    (( fds > max_fds )) && max_fds=$fds
    printf '%s %s %s\n' "$max_threads" "$max_connections" "$max_fds" >"$RESOURCE_FILE"
    sleep 1
  done
) &
SAMPLER_PID=$!

deadline=$((SECONDS + 1800))
while true; do
  succeeded=$(kubectl -n "$NAMESPACE" get job density-load -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
  failed=$(kubectl -n "$NAMESPACE" get job density-load -o jsonpath='{.status.failed}' 2>/dev/null || true)
  [[ "$succeeded" == "1" ]] && break
  if [[ -n "$failed" && "$failed" != "0" ]]; then
    kubectl -n "$NAMESPACE" logs job/density-load || true
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    kubectl -n "$NAMESPACE" logs job/density-load || true
    echo "density load timed out" >&2
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
read -r OBSERVED_THREADS OBSERVED_CONNECTIONS OBSERVED_FDS <"$RESOURCE_FILE"

mkdir -p "$(dirname "$LEDGER_OUT")"
rustup run 1.92.0 cargo run --quiet -p pqueue-loadgen -- density-emit-row \
  --result "$RESULT_FILE" \
  --observed-threads "$OBSERVED_THREADS" --thread-limit "$THREAD_LIMIT" \
  --observed-connections "$OBSERVED_CONNECTIONS" --connection-limit "$CONNECTION_LIMIT" \
  --observed-fds "$OBSERVED_FDS" --fd-limit "$FD_LIMIT" \
  --revision "$REVISION" --image-digest "$IMAGE_DIGEST" \
  --topology "$TOPOLOGY" --hardware "$HARDWARE" --seed "$SEED" --out "$LEDGER_OUT"
rustup run 1.92.0 cargo run --quiet -p pqueue-release --bin pqueue-verify-density-evidence -- "$LEDGER_OUT"
printf 'LEDGER_OUT=%s\n' "$LEDGER_OUT"
