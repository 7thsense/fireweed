#!/usr/bin/env bash
# Start fireweed-service for the Seventh Sense RESP black-box profile.
# Three bootstrap queues model jobs / actions / scheduled_actions.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

export FIREWEED_LISTEN_ADDR="${FIREWEED_LISTEN_ADDR:-127.0.0.1:8080}"
export FIREWEED_LOG_BACKEND="${FIREWEED_LOG_BACKEND:-memory}"
export FIREWEED_PROJECTION_BACKEND="${FIREWEED_PROJECTION_BACKEND:-memory}"
export FIREWEED_BOOTSTRAP_QUEUES="${FIREWEED_BOOTSTRAP_QUEUES:-ss:jobs,ss:actions,ss:scheduled}"

echo "Starting fireweed-service for Seventh Sense RESP black box"
echo "  listen=${FIREWEED_LISTEN_ADDR}"
echo "  log=${FIREWEED_LOG_BACKEND} projection=${FIREWEED_PROJECTION_BACKEND}"
echo "  queues=${FIREWEED_BOOTSTRAP_QUEUES}"
echo "  Profile: docs/perf/workload-seventh-sense-actions-scheduler.md"
echo "  Client:  FIREWEED_RESP_URL=redis://${FIREWEED_LISTEN_ADDR} \\"
echo "           SS_N=5000 python examples/python-resp/run_e2e.py --suite ss"
echo "  Note: bootstrap max_claim_batch_size=100 (COUNT above that is capped)."
exec cargo run -p fireweed-server --release --bin fireweed-service
