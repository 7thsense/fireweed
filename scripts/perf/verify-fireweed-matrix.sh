#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

runner=crates/fireweed-bench/target/release/fireweed-performance-matrix
if [[ ! -x "$runner" ]]; then
  cargo build --manifest-path crates/fireweed-bench/Cargo.toml --release --locked --bin fireweed-performance-matrix
fi
"$runner" verify "$1"
