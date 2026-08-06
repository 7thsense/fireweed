#!/usr/bin/env bash
# Local TP-005 million-cycle functional probe (memory--memory) or production sizes.
# Usage:
#   bash scripts/perf/run-million-cycle-local.sh          # unit probe (2k)
#   FIREWEED_MC_FULL=1 bash scripts/perf/run-million-cycle-local.sh  # 1M (slow)
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
if [[ "${FIREWEED_MC_FULL:-}" == "1" ]]; then
  echo "production 1M cycle not yet CLI-wired; use WorkSizes::production via future --tier million-cycle"
  exit 2
fi
cargo test --manifest-path crates/fireweed-bench/Cargo.toml --lib \
  performance_matrix_million_cycle::tests::probe_cycle_on_memory_memory -- --nocapture
