#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

echo "--- formatting ---"
rustup run 1.99.0 cargo fmt --all --check

echo "--- public identity ---"
bash scripts/verify-public-identity.sh
bash scripts/test-public-identity-residue.sh

echo "--- public Fireweed interface and mutation behavior ---"
rustup run 1.99.0 cargo test --locked -p fireweed \
    --test concrete_fireweed \
    --test item_mutation

rustup run 1.99.0 cargo test --locked -p fireweed --features objectlog,turso \
    --test public_interface_conformance filesystem_turso_async_public_interface -- --exact

# Facade lib suite must compile (v0.24 process gap fireweed-c1dc998a). Ignored tests are
# allowed only with an explicit #[ignore = "..."] reason on the incomplete product surface.
# Large-scale tests live in `large` submodules and run in the separate large tier
# (scripts/ci/large-tier-gate.sh, run daily by nightly.yml), which never blocks a
# release and does not run on pull requests.
echo "--- fireweed facade lib suite (fast tier) ---"
rustup run 1.99.0 cargo test --locked -p fireweed --lib -- --skip ::large::

# v0.25 headline proofs (integration targets, not covered by --lib alone)
echo "--- claim_by_item_ids + eligibility recovery ---"
rustup run 1.99.0 cargo test --locked -p fireweed-engine --test claim_by_item_ids
rustup run 1.99.0 cargo test --locked -p fireweed --features objectlog,turso --test eligibility_index_recovery

echo "fast functional gate passed"
