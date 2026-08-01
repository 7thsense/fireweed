#!/usr/bin/env bash
# Durable Kind density runner for pqueue-c989bc20.
#
# Problem this solves: the parent bash of tp002-e2-density-kind.sh was killed
# mid-run (~183k/300k) while the Kind Job stayed healthy, invalidating resource
# sampling. This wrapper:
#   - requires a clean worktree
#   - runs under nohup so SSH/session loss does not kill the harness
#   - keeps a long-lived log + pid under target/density-runs/
#
# Usage:
#   scripts/perf/run-density-kind-durable.sh
#   CLUSTER=fireweed-density scripts/perf/run-density-kind-durable.sh
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain --untracked-files=normal)" ]; then
  echo "worktree must be clean for release density measurement" >&2
  exit 2
fi

REV=$(git rev-parse --short=12 HEAD)
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_DIR=${DENSITY_RUN_DIR:-$REPO_ROOT/target/density-runs/${REV}-${STAMP}}
mkdir -p "$RUN_DIR"
LOG="$RUN_DIR/runner.log"
PID_FILE="$RUN_DIR/runner.pid"

export CLUSTER=${CLUSTER:-fireweed-density}
export EVIDENCE_MODE=${EVIDENCE_MODE:-release}
export PROJECTION_BACKEND=${PROJECTION_BACKEND:-sqlite}
export LEDGER_OUT=${LEDGER_OUT:-$RUN_DIR/tp002-e2-density-kind.jsonl}
export DIAGNOSTICS_DIR=${DIAGNOSTICS_DIR:-$RUN_DIR/diagnostics}

cat >"$RUN_DIR/launch.env" <<EOF
cluster=$CLUSTER
evidence_mode=$EVIDENCE_MODE
projection_backend=$PROJECTION_BACKEND
ledger_out=$LEDGER_OUT
revision=$(git rev-parse HEAD)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

echo "launching density into $RUN_DIR (log: $LOG)" >&2
nohup bash -c "
  set -euo pipefail
  cd '$REPO_ROOT'
  export CLUSTER='$CLUSTER'
  export EVIDENCE_MODE='$EVIDENCE_MODE'
  export PROJECTION_BACKEND='$PROJECTION_BACKEND'
  export LEDGER_OUT='$LEDGER_OUT'
  export DIAGNOSTICS_DIR='$DIAGNOSTICS_DIR'
  echo \"density start \$(date -u +%Y-%m-%dT%H:%M:%SZ) cluster=\$CLUSTER\"
  # Keep the harness process group resilient; Kind Job continues if parent dies,
  # but we prefer the parent to live for resource sampling.
  bash '$REPO_ROOT/scripts/perf/tp002-e2-density-kind.sh'
  status=\$?
  echo \"density end \$(date -u +%Y-%m-%dT%H:%M:%SZ) status=\$status\"
  exit \$status
" >"$LOG" 2>&1 &
echo $! >"$PID_FILE"
echo "pid=$(cat "$PID_FILE") log=$LOG run_dir=$RUN_DIR"
echo "monitor: tail -F $LOG"
echo "kind:    kind get kubeconfig --name $CLUSTER | kubectl --kubeconfig=/dev/stdin -n \$(rg -o 'fireweed-density-[0-9]+' $LOG | tail -1) get pods,jobs"
