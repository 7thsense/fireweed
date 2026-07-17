#!/usr/bin/env bash
# TP-002 E2: live shared-object-log owner failover, fencing, recovery, and redirect proof.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="${ROOT}/charts/pqueue"
CLUSTER="${PQUEUE_E2_CLUSTER:-pqueue-e2-failover-$$}"
NS="pqueue-e2"
IMAGE="${PQUEUE_E2_IMAGE:-pqueue:e2-failover}"
TIMEOUT="${PQUEUE_E2_TIMEOUT:-240s}"
PORT="${PQUEUE_E2_PORT:-18180}"
PG_PORT="${PQUEUE_E2_PG_PORT:-15433}"
S3_PORT="${PQUEUE_E2_S3_PORT:-19001}"
SEED="${PQUEUE_E2_SEED:-2002}"
OUT="${PQUEUE_E2_EVIDENCE:-${ROOT}/target/tp002-e2-failover/evidence.json}"
KEEP="${PQUEUE_E2_KEEP_CLUSTER:-0}"
PG_IMAGE="${PQUEUE_E2_POSTGRES_IMAGE:-postgres:16}"
MINIO_IMAGE="${PQUEUE_E2_MINIO_IMAGE:-minio/minio:latest}"
MC_IMAGE="${PQUEUE_E2_MC_IMAGE:-minio/mc:latest}"
PF_PID=""
CLUSTER_CREATED=0
START_MS="$(date +%s%3N)"
RUN_DIR="${ROOT}/target/tp002-e2-failover/${CLUSTER}"
mkdir -p "${RUN_DIR}" "$(dirname "${OUT}")"

die() { echo "tp002-e2-failover: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || die "required tool not found: $1"; }
k() { kubectl --context "kind-${CLUSTER}" "$@"; }
stop_pf() { if [[ -n "${PF_PID}" ]]; then kill "${PF_PID}" 2>/dev/null || true; wait "${PF_PID}" 2>/dev/null || true; PF_PID=""; fi; }
cleanup() {
  stop_pf
  if [[ "${KEEP}" != 1 && "${CLUSTER_CREATED}" == 1 ]]; then kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

start_pf() {
  local resource="$1" local_port="$2" remote_port="$3"
  stop_pf
  kubectl --context "kind-${CLUSTER}" -n "${NS}" port-forward \
    "${resource}" "${local_port}:${remote_port}" >"${RUN_DIR}/port-forward.log" 2>&1 &
  PF_PID=$!
  for _ in {1..40}; do
    (exec 3<>"/dev/tcp/127.0.0.1/${local_port}") >/dev/null 2>&1 && return 0
    kill -0 "${PF_PID}" 2>/dev/null || { sed -n '1,120p' "${RUN_DIR}/port-forward.log" >&2; die "port-forward exited"; }
    sleep 1
  done
  die "timed out waiting for port-forward ${resource}"
}

resp() {
  local output="$1"; shift
  RESP_PORT="${PORT}" RESP_OUT="${output}" RESP_ARGS="$(printf '%s\n' "$@")" python3 - <<'PY'
import os, socket
from pathlib import Path
args=os.environ["RESP_ARGS"].splitlines()
wire=[f"*{len(args)}\r\n".encode()]
for arg in args:
    b=arg.encode(); wire += [f"${len(b)}\r\n".encode(), b, b"\r\n"]
with socket.create_connection(("127.0.0.1", int(os.environ["RESP_PORT"])), timeout=5) as s:
    s.settimeout(1); s.sendall(b"".join(wire)); chunks=[]
    while True:
        try: chunk=s.recv(65536)
        except TimeoutError: break
        if not chunk: break
        chunks.append(chunk)
Path(os.environ["RESP_OUT"]).write_bytes(b"".join(chunks))
PY
}

resp_claim_counts() {
  python3 - "$1" <<'PY'
import sys
from pathlib import Path

data = Path(sys.argv[1]).read_bytes()
offset = 0

def read_line():
    global offset
    end = data.index(b"\r\n", offset)
    value = data[offset:end]
    offset = end + 2
    return value

def parse():
    global offset
    kind = chr(data[offset])
    offset += 1
    header = read_line()
    if kind == "*":
        count = int(header)
        return None if count == -1 else [parse() for _ in range(count)]
    if kind == "$":
        length = int(header)
        if length == -1:
            return None
        value = data[offset:offset + length]
        offset += length
        if data[offset:offset + 2] != b"\r\n":
            raise ValueError("bulk string is missing its terminator")
        offset += 2
        return value.decode()
    if kind in "+-":
        return header.decode()
    if kind == ":":
        return int(header)
    raise ValueError(f"unsupported RESP type: {kind!r}")

response = parse()
if offset != len(data):
    raise ValueError("trailing bytes in RESP response")
if not isinstance(response, list) or len(response) != 1:
    raise ValueError("expected one XREADGROUP stream")
entries = response[0][1]
ids = [entry[0] for entry in entries]
print(len(ids), len(set(ids)))
PY
}

pg_scalar() {
  local sql="$1"
  k -n "${NS}" exec deploy/pqueue-e2-postgres -- psql -U pqueue -d pqueue -Atqc "${sql}"
}

owner_row() {
  pg_scalar "SELECT active_owner_id || '|' || assignment_epoch FROM pqueue_queue_owner WHERE tenant='t1' AND queue='q1' AND state='assigned';" | tail -1
}

wait_stable_owner() {
  local reject="${1:-}" row last="" stable=0
  for _ in {1..180}; do
    row="$(owner_row 2>/dev/null || true)"
    if [[ -n "${row}" && "${row%%|*}" != "${reject}" ]]; then
      if [[ "${row}" == "${last}" ]]; then ((stable += 1)); else last="${row}"; stable=1; fi
      if ((stable >= 10)); then printf '%s\n' "${row}"; return 0; fi
    else
      last=""; stable=0
    fi
    sleep 1
  done
  die "timed out waiting for a stable assigned owner distinct from ${reject:-<none>}"
}

pod_for_uid() {
  k -n "${NS}" get pods -l app.kubernetes.io/instance=pqueue -o jsonpath="{range .items[?(@.metadata.uid=='$1')]}{.metadata.name}{end}"
}

pod_ip() { k -n "${NS}" get pod "$1" -o jsonpath='{.status.podIP}'; }

wait_owner_usable() {
  local pod="$1" response="${RUN_DIR}/owner-ready.resp"
  start_pf "pod/${pod}" "${PORT}" 8080
  for _ in {1..60}; do
    resp "${response}" XLEN t1:q1
    if grep -Eq '^:[0-9]+' "${response}"; then return 0; fi
    sleep 1
  done
  cat "${response}" >&2
  die "assigned owner never became locally usable"
}

for tool in docker kind kubectl helm python3 cargo git; do need "${tool}"; done

SOURCE_REV="$(git -C "${ROOT}" rev-parse HEAD)"
CHART_REV="$(cd "${CHART}" && rg --files -0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')"
HARDWARE="$(uname -srm); cpu=$(getconf _NPROCESSORS_ONLN); memory_bytes=$(awk '/MemTotal/{print $2*1024}' /proc/meminfo)"
echo "TP-002 E2 source=${SOURCE_REV} chart=${CHART_REV} seed=${SEED}"

docker build --build-arg CARGO_FEATURES=postgres -t "${IMAGE}" "${ROOT}"
IMAGE_ID="$(docker image inspect "${IMAGE}" --format '{{.Id}}')"
kind create cluster --name "${CLUSTER}"
CLUSTER_CREATED=1
kind load docker-image "${IMAGE}" --name "${CLUSTER}"
for dep in "${PG_IMAGE}" "${MINIO_IMAGE}" "${MC_IMAGE}"; do docker pull "${dep}"; kind load docker-image "${dep}" --name "${CLUSTER}"; done
PG_IMAGE_REF="${PG_IMAGE}@$(docker image inspect "${PG_IMAGE}" --format '{{index .RepoDigests 0}}' | cut -d@ -f2)"
MINIO_IMAGE_REF="${MINIO_IMAGE}@$(docker image inspect "${MINIO_IMAGE}" --format '{{index .RepoDigests 0}}' | cut -d@ -f2)"
k create namespace "${NS}"

k -n "${NS}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: pqueue-e2-postgres}
spec:
  replicas: 1
  selector: {matchLabels: {app: pqueue-e2-postgres}}
  template:
    metadata: {labels: {app: pqueue-e2-postgres}}
    spec:
      containers:
      - name: postgres
        image: ${PG_IMAGE}
        imagePullPolicy: IfNotPresent
        env:
        - {name: POSTGRES_USER, value: pqueue}
        - {name: POSTGRES_PASSWORD, value: pqueue}
        - {name: POSTGRES_DB, value: pqueue}
        readinessProbe: {exec: {command: [pg_isready, -U, pqueue]}, periodSeconds: 2}
---
apiVersion: v1
kind: Service
metadata: {name: pqueue-e2-postgres}
spec: {selector: {app: pqueue-e2-postgres}, ports: [{port: 5432}]}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: pqueue-e2-minio}
spec:
  replicas: 1
  selector: {matchLabels: {app: pqueue-e2-minio}}
  template:
    metadata: {labels: {app: pqueue-e2-minio}}
    spec:
      containers:
      - name: minio
        image: ${MINIO_IMAGE}
        imagePullPolicy: IfNotPresent
        args: [server, /data]
        env:
        - {name: MINIO_ROOT_USER, value: minioadmin}
        - {name: MINIO_ROOT_PASSWORD, value: minioadmin}
        readinessProbe: {httpGet: {path: /minio/health/ready, port: 9000}, periodSeconds: 2}
---
apiVersion: v1
kind: Service
metadata: {name: pqueue-e2-minio}
spec: {selector: {app: pqueue-e2-minio}, ports: [{port: 9000}]}
EOF
k -n "${NS}" rollout status deploy/pqueue-e2-postgres --timeout "${TIMEOUT}"
k -n "${NS}" rollout status deploy/pqueue-e2-minio --timeout "${TIMEOUT}"
k -n "${NS}" run pqueue-e2-mc --restart=Never --image="${MC_IMAGE}" --image-pull-policy=IfNotPresent --command -- \
  sh -c 'mc alias set e2 http://pqueue-e2-minio:9000 minioadmin minioadmin && mc mb --ignore-existing e2/pqueue-e2'
k -n "${NS}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/pqueue-e2-mc --timeout "${TIMEOUT}"
k -n "${NS}" logs pqueue-e2-mc

k -n "${NS}" create secret generic pqueue-objectlog-s3 --from-literal=access-key-id=minioadmin --from-literal=secret-access-key=minioadmin
k -n "${NS}" create secret generic pqueue-control-plane \
  --from-literal=database-url='postgres://pqueue:pqueue@pqueue-e2-postgres:5432/pqueue?sslmode=disable'

REPO="${IMAGE%:*}"; TAG="${IMAGE##*:}"
helm upgrade --install pqueue "${CHART}" --kube-context "kind-${CLUSTER}" -n "${NS}" \
  -f "${CHART}/values-shared-s3.yaml" \
  --set fullnameOverride=pqueue --set image.repository="${REPO}" --set image.tag="${TAG}" \
  --set image.pullPolicy=IfNotPresent --set bootstrap.queues[0]=t1:q1 \
  --set storage.log.objectLog.s3.endpoint=http://pqueue-e2-minio:9000 \
  --set storage.log.objectLog.s3.bucket=pqueue-e2 \
  --set storage.log.objectLog.s3.allowInsecureHttp=true --wait --timeout "${TIMEOUT}"
k -n "${NS}" rollout status deploy/pqueue --timeout "${TIMEOUT}"
[[ "$(k -n "${NS}" get deploy pqueue -o jsonpath='{.status.readyReplicas}')" == 3 ]] || die "three replicas are not ready"

OLD="$(wait_stable_owner)"; OLD_OWNER="${OLD%%|*}"; OLD_EPOCH="${OLD##*|}"
OWNER_POD="$(pod_for_uid "${OLD_OWNER}")"; [[ -n "${OWNER_POD}" ]] || die "owner UID does not map to a pod"
OWNER_IP="$(pod_ip "${OWNER_POD}")"
NONOWNER_POD="$(k -n "${NS}" get pods -l app.kubernetes.io/instance=pqueue -o name | sed 's#pod/##' | grep -v -Fx "${OWNER_POD}" | head -1)"
[[ -n "${NONOWNER_POD}" ]] || die "no non-owner pod"

{
  echo "selected_owner_row=${OLD}"
  echo "selected_owner_pod=${OWNER_POD} uid=$(k -n "${NS}" get pod "${OWNER_POD}" -o jsonpath='{.metadata.uid}') ip=${OWNER_IP}"
  echo "selected_nonowner_pod=${NONOWNER_POD} uid=$(k -n "${NS}" get pod "${NONOWNER_POD}" -o jsonpath='{.metadata.uid}') ip=$(pod_ip "${NONOWNER_POD}")"
  echo "workers=$(pg_scalar "SELECT owner_id || '@' || endpoint FROM pqueue_workers ORDER BY owner_id;")"
} | tee "${RUN_DIR}/routing-selection.log"

# One request to a non-owner must redirect to the pod-reachable active endpoint; retry exactly once.
wait_owner_usable "${OWNER_POD}"
stop_pf
[[ "$(owner_row)" == "${OLD}" ]] || die "owner changed while establishing routing readiness"
owner_row >"${RUN_DIR}/owner-immediately-before-request.log"
start_pf "pod/${NONOWNER_POD}" "${PORT}" 8080
resp "${RUN_DIR}/moved.resp" XADD t1:q1 '*' priority 9
owner_row >"${RUN_DIR}/owner-immediately-after-request.log"
grep -Eq "^-MOVED .* ${OWNER_IP}:8080" "${RUN_DIR}/moved.resp" || { cat "${RUN_DIR}/moved.resp" >&2; die "non-owner did not return owner MOVED"; }
stop_pf
start_pf "pod/${OWNER_POD}" "${PORT}" 8080
resp "${RUN_DIR}/retry.resp" XADD t1:q1 '*' priority 9
grep -Eq '^\$[0-9]+' "${RUN_DIR}/retry.resp" || die "one-hop retry failed"
for p in 1 2 3; do resp "${RUN_DIR}/push-${p}.resp" XADD t1:q1 '*' priority "${p}"; grep -Eq '^\$[0-9]+' "${RUN_DIR}/push-${p}.resp" || die "pre-fault push failed"; done
resp "${RUN_DIR}/before.resp" XLEN t1:q1
BEFORE="$(tr -dc '0-9' <"${RUN_DIR}/before.resp")"; [[ "${BEFORE}" == 4 ]] || die "expected 4 visible items, got ${BEFORE}"
stop_pf

# Kill the active owner and require a different Kubernetes UID at a strictly larger control-plane epoch.
k -n "${NS}" delete pod "${OWNER_POD}" --wait=false
NEW="$(wait_stable_owner "${OLD_OWNER}")"; NEW_OWNER="${NEW%%|*}"; NEW_EPOCH="${NEW##*|}"
((NEW_EPOCH > OLD_EPOCH)) || die "takeover epoch did not increase (${OLD_EPOCH} -> ${NEW_EPOCH})"
NEW_POD="$(pod_for_uid "${NEW_OWNER}")"; [[ -n "${NEW_POD}" ]] || die "replacement owner UID does not map to a pod"
k -n "${NS}" wait --for=condition=Ready "pod/${NEW_POD}" --timeout "${TIMEOUT}"
start_pf "pod/${NEW_POD}" "${PORT}" 8080
resp "${RUN_DIR}/after.resp" XLEN t1:q1
AFTER="$(tr -dc '0-9' <"${RUN_DIR}/after.resp")"; [[ "${AFTER}" == "${BEFORE}" ]] || die "visible state changed: ${BEFORE} -> ${AFTER}"

# Claim every item once, then prove a second consumer cannot concurrently lease any of them.
resp "${RUN_DIR}/claim-one.resp" XREADGROUP GROUP failover-g c1 COUNT 10 STREAMS t1:q1 '>'
read -r IDS_ONE UNIQUE_IDS_ONE < <(resp_claim_counts "${RUN_DIR}/claim-one.resp")
[[ "${IDS_ONE}" == "${BEFORE}" && "${UNIQUE_IDS_ONE}" == "${BEFORE}" ]] || \
  die "first consumer did not receive each item exactly once (${IDS_ONE} entries, ${UNIQUE_IDS_ONE} unique, expected ${BEFORE})"
resp "${RUN_DIR}/claim-two.resp" XREADGROUP GROUP failover-g c2 COUNT 10 STREAMS t1:q1 '>'
grep -Eq '^\*0|^\*-1|^\$-1' "${RUN_DIR}/claim-two.resp" || { cat "${RUN_DIR}/claim-two.resp" >&2; die "second consumer acquired a double lease"; }
stop_pf

# Use the same live dependencies to prove the epoch-stale append cut and true snapshot+tail reopen seam.
start_pf svc/pqueue-e2-postgres "${PG_PORT}" 5432
PG_PF="${PF_PID}"; PF_PID=""
kubectl --context "kind-${CLUSTER}" -n "${NS}" port-forward \
  svc/pqueue-e2-minio "${S3_PORT}:9000" >"${RUN_DIR}/s3-port-forward.log" 2>&1 & S3_PF=$!
trap 'kill "${PG_PF:-}" "${S3_PF:-}" 2>/dev/null || true; cleanup' EXIT
sleep 2
PQUEUE_PG_TEST_URL="postgres://pqueue:pqueue@127.0.0.1:${PG_PORT}/pqueue?sslmode=disable" \
PQUEUE_S3_TEST_ENDPOINT="http://127.0.0.1:${S3_PORT}" \
  cargo test -p pqueue-server --test objectlog_shared_ownership \
  stale_append_paused_before_authority_cannot_survive_handoff -- --nocapture
PQUEUE_S3_TEST_ENDPOINT="http://127.0.0.1:${S3_PORT}" PQUEUE_E3_RESIDENT=40 \
PQUEUE_E3_ACK_PUSHES=16 PQUEUE_E3_ACK_CONCURRENCY=4 PQUEUE_E3_LOAD_CONCURRENCY=2 \
  cargo test -p pqueue-server --release --test performance_object_log_e3_live_tests \
  performance_object_log_e3_live_tests -- --nocapture 2>&1 | tee "${RUN_DIR}/snapshot-tail.log"
grep -Eq '"snapshot_used"[[:space:]]*:[[:space:]]*true' "${RUN_DIR}/snapshot-tail.log" || die "snapshot+tail seam was not exercised"
grep -Eq '"bar_met"[[:space:]]*:[[:space:]]*true' "${RUN_DIR}/snapshot-tail.log" || die "snapshot+tail recovery bar failed"
kill "${PG_PF}" "${S3_PF}" 2>/dev/null || true; wait "${PG_PF}" "${S3_PF}" 2>/dev/null || true

DURATION_MS="$(( $(date +%s%3N) - START_MS ))"
E2_COMMAND="PQUEUE_E2_SEED=${SEED} bash scripts/perf/tp002-e2-failover-kind.sh"
python3 - "${OUT}" <<PY
import json, sys
row = {
 "schema_version":1,"suite":"tp002_e2_live_owner_failover","command":${E2_COMMAND@Q},
 "evidence_id":"E2_FAILOVER","evidence_tier":"release","scale":"release",
 "backend_profile":"object_log_sqlite_projection","bars_met":True,"replicas":3,
 "image":${IMAGE@Q},"image_id":${IMAGE_ID@Q},"source_revision":${SOURCE_REV@Q},
 "chart_revision":${CHART_REV@Q},"postgres_image":${PG_IMAGE_REF@Q},"minio_image":${MINIO_IMAGE_REF@Q},
 "old_owner_id":${OLD_OWNER@Q},"new_owner_id":${NEW_OWNER@Q},"old_epoch":int(${OLD_EPOCH@Q}),"new_epoch":int(${NEW_EPOCH@Q}),
 "stale_append_rejected_before_mutation":True,"snapshot_tail_recovered":True,
 "visible_items_before":int(${BEFORE@Q}),"visible_items_after":int(${AFTER@Q}),
 "lost_work":0,"double_leases":0,"corrupt_writes":0,"moved_count":1,"retry_count":1,"retry_succeeded":True,
 "moved_endpoint":${OWNER_IP@Q}+":8080",
 "topology":"kind: 3 pqueue pods; shared MinIO S3 object log; Postgres ownership; per-pod SQLite projection",
 "hardware":${HARDWARE@Q},"seed":int(${SEED@Q}),"duration_ms":int(${DURATION_MS@Q}),
 "fault_schedule":"after one redirected/retried push plus three owner pushes, delete active owner pod; await distinct owner and larger epoch",
 "exclusions":"density throughput and managed-cloud S3/Postgres; performance is covered by the separate E3 lane"
}
with open(sys.argv[1], "w") as f: json.dump(row, f, indent=2); f.write("\n")
PY
cargo run -p pqueue-release --bin pqueue-verify-e2-failover -- "${OUT}"
echo "TP-002 E2 PASS: ${OUT}"
