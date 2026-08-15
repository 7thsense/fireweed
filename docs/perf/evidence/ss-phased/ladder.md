# SS phased capacity ladder

Same-host before/after only. G-gates require N=1,000,000. N=10k/100k are not comparable to G-rows.

Host for rows below: WSL2 `sindri`, AMD Ryzen 9 5950X 16C/32T, 94 GiB RAM, workspace on `/dev/sdd` ext4 virtual disk. **Not** H-server NVMe+PLP. rustc 1.97.1.

| utc | sha | N | note | p1_items_s | p2_items_s | p3_items_s | p4_items_s | wall_s | p1_p99_ms | p2_p99_ms | p3_p99_ms | p4_claim_p99_ms | residual |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-15 | 7faa3588 | 10000 | smoke | 21020 | 15317 | 21808 | 14550 | 2.28 | 25.22 | 22.73 | 15.58 | 10.26 | 0 |
| 2026-08-15 | 7faa3588 | 1000000 | baseline (run 1; WSL virt disk) | 12188 | 7721 | 8365 | 8514 | 448.6 | 41.59 | 78.64 | 76.39 | 12.42 | 0 |
| 2026-08-15 | 32ef65d3 | 100000 | tmpfs `/dev/shm` after UpdateFields skip | 73743 | 45912 | 71545 | 62143 | 6.54 | 4.94 | 6.01 | 3.24 | 0.99 | 0 |
| 2026-08-15 | 32ef65d3 | 1000000 | tmpfs `/dev/shm` push=100 | 67177 | 43814 | 55586 | 50397 | 75.5 | 5.10 | 6.55 | 3.97 | 1.43 | 0 |
| 2026-08-15 | 32ef65d3 | 1000000 | tmpfs `/dev/shm` **push=1000** run1 — **G1–G5 met** | 107719 | 44614 | 67658 | 58504 | 63.6 | 13.67 | 6.57 | 3.34 | 1.10 | 0 |
| 2026-08-15 | 32ef65d3 | 1000000 | tmpfs `/dev/shm` **push=1000** run2 — **G1–G5 met** | 109724 | 43403 | 70016 | 61533 | 62.7 | 13.71 | 6.47 | 3.17 | 1.09 | 0 |

**Stop (success):** both best-of-2 rows meet G1–G5 on `sqlite--memory` via `open_sqlite` with the log on tmpfs (`SS_LOG_DIR=/dev/shm`). Ingest batch 1000 is allowed by the plan; claim batch remains 100. Stretch P4≥100k is **not** met (best P4 61.5k). I5 dense `index_fields` skipped — gated harness has zero typed indexes.

## SQLite command-log cell (`sqlite--memory`) — calibration only

This table is **not** the production log. Production deploys an object log (filesystem/S3 protocol).
Do not quote these rates as object-storage capacity. `SS_SQLITE_SYNC=off` is a sqlite WAL knob.

On-disk after `SqliteLogSync` + `wal_autocheckpoint=0` for Normal/Off (same virt disk, `SS_PUSH_BATCH=1000`):

| utc | sha | sync | claim | p1 | p2 | p3 | p4 | wall_s |
|---|---|---|---:|---:|---:|---:|---:|---:|
| 2026-08-15 | post-sync-knob | normal | 100 | 72371 | 30698 | 58416 | 63893 | 79.2 |
| 2026-08-15 | post-sync-knob | off | 500 | 90451 | 54386 | 79805 | 98617 | 52.1 |
| 2026-08-15 | wal_auto=0 | **off** | **1000** | **89848** | **52878** | **63246** | **197370** | **50.9** |
| 2026-08-15 | wal_auto=0 | **normal** | 500 | **91857** | **39891** | **94865** | **137221** | **53.8** |
| 2026-08-15 | UpdateFieldsBatch | **off** | **1000** | **101342** | **75712** | **193340** | **194776** | **33.4** |

`open_sqlite` default remains `synchronous=FULL` (Class A). `open_sqlite_with_sync(..., Normal|Off)` is the sqlite-log throughput dial. Those knobs do not exist on the object log.

Gates (H-server) were written against this sqlite-log cell. They are not object-log SLAs.

Evidence: `docs/perf/evidence/ss-phased/1786760930/summary.json`.

## Object-log cell (`filesystem--memory`) — production log axis

Filesystem object log (same protocol as S3) × in-memory projection. `open_objectlog` product defaults (256 KiB / 50 ms linger). This is not a SQLite WAL. Local directory, not a remote S3 endpoint.

| utc | sha | N | note | p1_items_s | p2_items_s | p3_items_s | p4_items_s | wall_s |
|---|---|---:|---|---:|---:|---:|---:|---:|
| 2026-08-15 | harness+UpdateFieldsBatch | 1000000 | `open_objectlog` 256KiB/50ms linger, push=1000 claim=1000 | 15314 | 15246 | 16666 | 4370 | 419.7 |

P50 on this row: P1 48.7 ms, P2 57.4 ms, P3 51.2 ms, P4 claim 94.2 ms + finalize 96.8 ms. One worker, one in-flight call: small claim/finalize envelopes wait the 50 ms linger instead of co-buffering. That is an object-log group-commit shape, not a sqlite fsync number.

Evidence: `docs/perf/evidence/ss-phased/1786761950/summary.json`.
