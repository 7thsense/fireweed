#!/usr/bin/env bash
# Live TP-002 E2 density proof: one durable objectlog/sqlite service with 1001 generated queues.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=${CLUSTER:-pqueue-density}
IMAGE=${IMAGE:-pqueue:density-e2}
QUEUE_COUNT=${QUEUE_COUNT:-1001}
ITEMS=${ITEMS:-30000}
HOT_CONNECTIONS=${HOT_CONNECTIONS:-8}
NOISY_WORKERS=${NOISY_WORKERS:-8}
SERVER_WORKERS=${SERVER_WORKERS:-4}
SEED=${SEED:-42}
SKIP_BUILD=${SKIP_BUILD:-0}
LEDGER_OUT=${LEDGER_OUT:-$REPO_ROOT/target/pqueue-ledger/tp002-e2-density-kind.jsonl}
KUBECONFIG_FILE=$(mktemp)
NAMESPACE="pqueue-density-${RANDOM}"

cleanup() {
  KUBECONFIG="$KUBECONFIG_FILE" kubectl delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
  rm -f "$KUBECONFIG_FILE"
}
trap cleanup EXIT

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER"
fi
kind get kubeconfig --name "$CLUSTER" >"$KUBECONFIG_FILE"
export KUBECONFIG="$KUBECONFIG_FILE"

if [[ "$SKIP_BUILD" != "1" ]]; then
  docker build -f "$REPO_ROOT/Dockerfile.e2" -t "$IMAGE" "$REPO_ROOT"
fi
kind load docker-image "$IMAGE" --name "$CLUSTER"

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

REVISION=$(git -C "$REPO_ROOT" rev-parse HEAD)
NODE_IMAGE=$(docker inspect "${CLUSTER}-control-plane" --format '{{.Config.Image}}')
HARDWARE="$(nproc) host cores; $(awk '/MemTotal/ {printf "%.1f GiB RAM", $2/1024/1024}' /proc/meminfo); kind node $NODE_IMAGE"
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
            - --server-workers
            - "$SERVER_WORKERS"
            - --revision
            - "$REVISION"
            - --hardware
            - "$HARDWARE"
            - --seed
            - "$SEED"
            - --out
            - /tmp/density.jsonl
          resources:
            requests: { cpu: "1000m", memory: "512Mi" }
            limits: { cpu: "4000m", memory: "4Gi" }
YAML

if ! kubectl -n "$NAMESPACE" wait --for=condition=complete job/density-load --timeout=1800s; then
  kubectl -n "$NAMESPACE" logs job/density-load || true
  exit 1
fi
logs=$(kubectl -n "$NAMESPACE" logs job/density-load)
printf '%s\n' "$logs"
mkdir -p "$(dirname "$LEDGER_OUT")"
printf '%s\n' "$logs" | sed -n 's/^DENSITY_ROW //p' >"$LEDGER_OUT"
test -s "$LEDGER_OUT"
cargo run --quiet -p pqueue-release --bin pqueue-verify-density-evidence -- "$LEDGER_OUT"
printf 'LEDGER_OUT=%s\n' "$LEDGER_OUT"
