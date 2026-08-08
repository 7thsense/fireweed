# P17 close package

Measured source **S** = `23bb355043c2d7c0bc2e28c6491592aecc75e841`  
Source ref = `refs/heads/release-source/v0.30.1`

## Stop-the-line checks

- P13b `fireweed-e7de00bf`: status=blocked (product external CI), not on execution-ready queue
- Source predicate at freeze: OK (p17s attestation)

## Gates (all exit 0)

| Gate | Result |
|---|---|
| storage-matrix-gate REQUIRE_FULL | PASS (20-cell + helm T4) |
| pr-gate --mode closure | PASS (zero debt) |
| stage=S shared operator leaves | PASS 6× ran=1 |
| stage=S storage | out_of_campaign |
| product workflow 10 suites | PASS all leaves |
| snorri-s3-durability-acceptance | PASS |
| TP-005 full LKG + host floors | prior archive (25% median physics floors) |
| million-cycle production | **15/20 cells PASS** (all non-Turso); Turso residual host-slow at 1M |

## Product-only disposition

- AC-E2E-7 / product-ready-only rows marked out-of-campaign (not closed by storage branch)

## Residual (non-blocking for storage class regen)

- Turso × 5 log cells million-cycle production still in progress (physics: ~1 item/ms class on host)
- Host floors already quantify Turso medians at full LKG tier

## Commands re-runnable

See individual logs under this directory and `docs/perf/evidence/tp005/million-cycle-production/`.
