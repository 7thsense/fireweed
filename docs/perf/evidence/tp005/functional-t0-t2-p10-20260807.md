# P10 — exact initial 20-cell functional matrix through T2

- date: 2026-08-07
- commit: `277a7faf3eb7947e56e8851fc9b95135325cdc30`
- bead: fireweed-173a72bc

## Commands (all exit 0)

```
export FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:55432/fireweed
# + live FIREWEED_S3_TEST_* MinIO fixtures

cargo test -p fireweed --features memory,sqlite,objectlog,postgres,turso \
  --test storage_matrix_t0_t2 -- --nocapture
# ran=20 skipped=0; 9 tests ok

cargo test -p fireweed --features memory,sqlite,objectlog,postgres,turso \
  --test functional_matrix_route_sources -- --nocapture
# 45 dry-run/exact leaves ok (20 strict + 8 async + 12 invalid + AC-TXN + guards)
```

Zero LOUD skips. Exact leaves via P10r registry; no broad filters.
