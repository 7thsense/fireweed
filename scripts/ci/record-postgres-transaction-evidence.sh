#!/usr/bin/env bash
# Local-only release gate: execute the exact live PostgreSQL transaction cells and replace their evidence.
# GitHub Actions deliberately does not run this service-backed matrix.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
matrix_evidence="${repo_root}/docs/perf/evidence/tp003-ac-txn-matrix-postgres-storage-pairs.jsonl"
parity_evidence="${repo_root}/docs/perf/evidence/tp003-ac-txn-parity-postgres-storage-pairs.jsonl"

: "${FIREWEED_PG_TEST_URL:?set FIREWEED_PG_TEST_URL to a disposable PostgreSQL database}"

cd "${repo_root}"
rm -f "$matrix_evidence" "$parity_evidence"

cargo test --locked -p fireweed-conformance \
  --test external_transaction_contract_matrix_tests \
  ac_txn_contract_matrix_postgres_storage_pairs -- --exact --nocapture
test -s "$matrix_evidence"

cargo test --locked -p fireweed-conformance \
  --test external_transaction_contract_matrix_tests \
  ac_txn_6_postgres_storage_pair_parity -- --exact --nocapture
test -s "$parity_evidence"

cargo run --locked -p fireweed-release --bin fireweed-verify-transaction-evidence -- \
  --evidence "$matrix_evidence" \
  --evidence "$parity_evidence"
