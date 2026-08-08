# P17 storage campaign class regeneration run

Measured source **S** = `23bb355043c2d7c0bc2e28c6491592aecc75e841`  
Source ref: `refs/heads/release-source/v0.30.1`

## Pre-execution

- P13b (`fireweed-e7de00bf`) is **blocked** (product external CI), **not** on `ddx bead ready --execution`
- Source predicate at freeze: OK (see `../p17s-freeze-attestation.md`)

## Gates executed this run

| Gate | Result |
|---|---|
| `run-operator-validation-job.sh --stage S --campaign storage` | out_of_campaign |
| `run-operator-validation-job.sh --stage S --campaign shared` | 6 leaves ran=1 |
| `FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1 storage-matrix-gate.sh` | **PASSED** (20-cell + helm) |
| `pr-gate.sh --mode closure` | **PASSED** (zero debt) |

## Prior evidence at/before S

- TP-005 full LKG + host floors (25% median)
- P2f pre_s/S preflight + closure
- Push floor decomposition (~0.005 ms/item relational)

## In progress

- Million-cycle production cells (1M insert / 500k modify) — long-running; partial PASS for memory--memory, memory--sqlite

## Artifacts

- `discharge-report.json` — requirement disposition join
- `stage-S-*.log`, `storage-matrix-gate-*.log`, `pr-gate-closure.log`
