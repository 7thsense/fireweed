# Current TP-003 T3 evidence (P11)

Historical files under `docs/perf/evidence/tp003-*.jsonl` are **immutable** and
must not be rewritten. They do not qualify current product evidence
(`historical_files_may_qualify_current: false`).

## Semantic current ID

`CURRENT-TP003-TRANSACTION-MATRIX`

## How to generate run-owned artifacts

```sh
export FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:55432/fireweed
export FIREWEED_P11_EVIDENCE_DIR=$(mktemp -d /tmp/fireweed-p11-XXXXXX)
bash scripts/ci/record-current-tp003-td008-evidence.sh
```

Artifacts land **outside** the repository (RunOwned). Digests of a successful
local generation are recorded in `run-owned-digests.sha256` and
`p11-current-tp003-note.json`. P18 alone promotes allowlisted paths.

## Class A cells with AC-TXN-1/2/3 pass (this generation)

| Cell | AC-TXN-1 | AC-TXN-2 | AC-TXN-3 |
|------|----------|----------|----------|
| sqlite×memory | pass | pass | pass |
| sqlite×sqlite | pass | pass | pass |
| sqlite×postgres | pass | pass | pass |
| postgres×memory | pass | pass | pass |
| postgres×sqlite | pass | pass | pass |
| postgres×postgres | pass | pass | pass |

Object-log filesystem T0–T3: `filesystem_log_three_cells_t0_t3_contract` (3/3).
S3 T0–T3 requires the P1s native-CAS endpoint (not the non-enforcing MinIO on :9000).

## Product fixes shipped with P11

1. **SQLite projection** — implement `ProjectionStore::restore_process_state` to
   rehydrate lease cleartext after snapshot-tail recovery.
2. **Postgres projection** — restore live tokens for **fenced** leased rows so
   post-reopen Unfence→finalize succeeds (AC-TXN-2).
