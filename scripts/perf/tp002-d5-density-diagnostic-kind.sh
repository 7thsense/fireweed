#!/usr/bin/env bash
# Finite under-load reproduction for pqueue-d5d58afd. This is diagnostic evidence, not the governed
# pqueue-c989bc20 release row: its 64 client connections intentionally exceed that row's 32-connection cap.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export EVIDENCE_MODE=d5-diagnostic
export PROJECTION_BACKEND=sqlite
export QUEUE_COUNT=1001
export ITEMS=10000
export CONTROL_ITEMS=10000
export HOT_CONNECTIONS=64
export NOISY_WORKERS=8
export SERVER_WORKERS=4
export SEED=42
export LEDGER_OUT=${LEDGER_OUT:-target/fireweed-ledger/tp002-d5-density-diagnostic-kind.json}
export DIAGNOSTICS_DIR=${DIAGNOSTICS_DIR:-target/fireweed-ledger/tp002-d5-density-diagnostic-kind-diagnostics}

exec "$SCRIPT_DIR/tp002-e2-density-kind.sh"
