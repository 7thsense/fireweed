#!/usr/bin/env bash
# Local-only release gate: execute the exact live PostgreSQL transaction cells into a run-owned directory.
# GitHub Actions deliberately does not run this service-backed matrix.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cargo_cmd=(rustup run 1.92.0 cargo)

: "${FIREWEED_PG_TEST_URL:?set FIREWEED_PG_TEST_URL to a disposable PostgreSQL database}"
: "${FIREWEED_TP003_EVIDENCE_DIR:?set FIREWEED_TP003_EVIDENCE_DIR to a newly created external directory}"
: "${FIREWEED_TP003_PARITY_EVIDENCE:?set FIREWEED_TP003_PARITY_EVIDENCE to an externally promoted AC-TXN-6 artifact}"

if [[ ! -d "$FIREWEED_TP003_EVIDENCE_DIR" ]]; then
  echo "TP-003 evidence directory must already exist" >&2
  exit 2
fi
evidence_dir=$(cd "$FIREWEED_TP003_EVIDENCE_DIR" && pwd -P)
case "$evidence_dir/" in
  "$repo_root"/*)
    echo "TP-003 evidence directory must be outside the repository" >&2
    exit 2
    ;;
esac
if [[ -n "$(find "$evidence_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "TP-003 evidence directory must be empty" >&2
  exit 2
fi
matrix_evidence="$evidence_dir/tp003-ac-txn-matrix-postgres-storage-pairs.jsonl"
parity_evidence=$(realpath "$FIREWEED_TP003_PARITY_EVIDENCE")
case "$parity_evidence" in
  "$repo_root"/*)
    echo "TP-003 parity evidence must be promoted outside the repository" >&2
    exit 2
    ;;
esac
if [[ ! -s "$parity_evidence" ]]; then
  echo "TP-003 parity evidence must be a nonempty promoted artifact" >&2
  exit 2
fi

cd "${repo_root}"
FIREWEED_TP003_POSTGRES_EVIDENCE_OUT="$matrix_evidence" \
  "${cargo_cmd[@]}" test --locked -p fireweed-server --lib \
    postgres_log_matrix_tests::postgres_log_t3_tp003_ac_txn_exact_pairs -- --exact --nocapture
test -s "$matrix_evidence"

"${cargo_cmd[@]}" run --locked -p fireweed-release --bin fireweed-verify-transaction-evidence -- \
  --evidence "$matrix_evidence" \
  --evidence "$parity_evidence"
