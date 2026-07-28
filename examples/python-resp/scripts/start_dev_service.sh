#!/usr/bin/env bash
# Start a local in-memory fireweed-service for Python RESP examples.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

export FIREWEED_LISTEN_ADDR="${FIREWEED_LISTEN_ADDR:-127.0.0.1:8080}"
export FIREWEED_LOG_BACKEND="${FIREWEED_LOG_BACKEND:-memory}"
export FIREWEED_PROJECTION_BACKEND="${FIREWEED_PROJECTION_BACKEND:-memory}"
export FIREWEED_BOOTSTRAP_QUEUES="${FIREWEED_BOOTSTRAP_QUEUES:-demo:work}"

echo "Starting fireweed-service on ${FIREWEED_LISTEN_ADDR}"
echo "  log=${FIREWEED_LOG_BACKEND} projection=${FIREWEED_PROJECTION_BACKEND}"
echo "  bootstrap queues=${FIREWEED_BOOTSTRAP_QUEUES}"
echo "  Python: FIREWEED_RESP_URL=redis://${FIREWEED_LISTEN_ADDR} \\"
echo "          python examples/python-resp/run_e2e.py"
echo "  Perf smoke: PERF_N=10000 python examples/python-resp/run_perf.py"
echo "  Note: bootstrap max_claim_batch_size=100 (COUNT 1000 is capped unless raised in server config)."
exec cargo run -p fireweed-server --bin fireweed-service
