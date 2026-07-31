#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

echo "--- formatting ---"
rustup run 1.92.0 cargo fmt --all --check

echo "--- public identity ---"
bash scripts/verify-public-identity.sh
bash scripts/test-public-identity-residue.sh

echo "--- public Fireweed interface and mutation behavior ---"
rustup run 1.92.0 cargo test --locked -p fireweed \
    --test concrete_fireweed \
    --test item_mutation

rustup run 1.92.0 cargo test --locked -p fireweed \
    --test public_interface_conformance memory_public_interface -- --exact

# Facade lib suite must compile (v0.24 process gap fireweed-c1dc998a). Ignored tests are
# allowed only with an explicit #[ignore = "..."] reason on the incomplete product surface.
echo "--- fireweed facade lib suite ---"
rustup run 1.92.0 cargo test --locked -p fireweed --lib

echo "fast functional gate passed"
