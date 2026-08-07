# Functional matrix T0–T2 — 20 cells (fail-closed)

- date: 2026-08-06
- commit: f80f99df
- command: `cargo test -p fireweed --features memory,sqlite,objectlog,postgres --test storage_matrix_t0_t2 -- --nocapture`
- fixtures: `FIREWEED_PG_TEST_URL` live, `FIREWEED_S3_TEST_*` MinIO qualification bucket

## Result

```
storage_matrix_t0_t2: ran=20 skipped=0 local_turso_ran=3 (of 20 registered cells)
test result: ok. 9 passed; 0 failed
```

All five log axes (memory, sqlite, postgres, filesystem, s3) × four projections
(memory, sqlite, turso, postgres) ran T0–T2 with **zero LOUD skips**. Class B
cells assert the no-durable_log_replay contract; Class A cells recover pending
from log on reopen.
