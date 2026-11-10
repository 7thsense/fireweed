#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

echo "--- formatting ---"
rustup run 1.99.0 cargo fmt --all --check

echo "--- public identity ---"
bash scripts/verify-public-identity.sh
bash scripts/test-public-identity-residue.sh

# Queue lib suite: queue_recovery_pipeline and queue::s3_turso unit tests. Tests that need a live
# S3 fixture start the pinned RustFS from FIREWEED_RUSTFS_BIN when it is set. Large-scale tests live in
# `large` submodules and run in the separate large tier (scripts/ci/large-tier-gate.sh, run daily by
# nightly.yml), which never blocks a release and does not run on pull requests.
echo "--- fireweed Queue lib suite (fast tier) ---"
rustup run 1.99.0 cargo test --locked -p fireweed --lib -- --skip ::large::

echo "--- Queue PostgreSQL tests ---"
rustup run 1.99.0 cargo test --locked -p fireweed --features postgres --test simple_queue_postgres
rustup run 1.99.0 cargo test --locked -p fireweed-postgres --test queue_contract --test queue_process_loss --test queue_server_crash

echo "fast functional gate passed"
