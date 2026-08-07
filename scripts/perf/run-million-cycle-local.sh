#!/usr/bin/env bash
# Local TP-005 million-cycle-v1 probe (or production sizes).
# Usage:
#   bash scripts/perf/run-million-cycle-local.sh
#       # probe sizes (2k items), memory--memory only
#   FIREWEED_MC_CELL=sqlite--sqlite bash scripts/perf/run-million-cycle-local.sh
#   FIREWEED_MC_FULL=1 bash scripts/perf/run-million-cycle-local.sh
#       # production 1M sizes; requires services for non-local cells
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

tier=probe
if [[ "${FIREWEED_MC_FULL:-}" == "1" ]]; then
  tier=production
fi
cell="${FIREWEED_MC_CELL:-memory--memory}"
out_dir="${FIREWEED_MC_OUT:-target/tp005-million-cycle}"
mkdir -p "$out_dir"
ts=$(date -u +%Y%m%dT%H%M%SZ)
out="$out_dir/${cell//--/-}-${tier}-${ts}.json"

cargo build --manifest-path crates/fireweed-bench/Cargo.toml --release --locked \
  --bin fireweed-million-cycle
runner=crates/fireweed-bench/target/release/fireweed-million-cycle
"$runner" --tier "$tier" --cell "$cell" --output "$out"
echo "million-cycle evidence: $out"
