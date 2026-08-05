#!/usr/bin/env bash
# fireweed-80af5cdb — re-runnable ≥10k state-transition TPS proof on durable projections.
#
# Primary gate: sqlite log-replay claim+commit with batching (post-a355d82b linear validation).
# Optional: sqlite relational evidence sample (not the 10k hard gate).
#
# Usage:
#   bash scripts/perf/durable-tps-proof.sh
#   bash scripts/perf/durable-tps-proof.sh --relational-evidence
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

RELATIONAL=0
for arg in "$@"; do
  case "$arg" in
    --relational-evidence) RELATIONAL=1 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
  esac
done

echo "=== durable-tps-proof (release) ==="
cargo test -p fireweed --release --test durable_tps_proof \
  sqlite_log_replay_concurrent_meets_10k_tps -- --nocapture --ignored

if [[ "$RELATIONAL" -eq 1 ]]; then
  echo "=== sqlite relational evidence (not the 10k gate) ==="
  cargo test -p fireweed --release --test durable_tps_proof \
    sqlite_relational_records_tps_evidence -- --nocapture --ignored || true
fi

echo "=== durable-tps-proof PASSED ==="
