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
| 2026-08-15 | 6d5e929d | 1000000 | product linger 50ms + S3 50 PUT/s budget (serial waiter) | 15314 | 15246 | 16666 | 4370 | 419.7 |
| 2026-08-15 | flush+FWB1 | 1000000 | idle early-flush 1ms, FWB1 batch, high-water every 64, inflight=1 | 24302 | 21729 | 25678 | 15808 | 189.4 |

Same-host durable PUT probe (temp + fdatasync + rename + dir fsync, 20 samples): **1 MiB p50 18.0 ms**, **tiny p50 13.7 ms**. A sequenced produce is a data object plus a manifest object. P1/P3 p50 ~33 ms ≈ two local PUTs. P4 wall 63 s ≈ 2000 tiny produces × ~32 ms. That is this virt disk's object-log I/O floor, not projection CPU.

`SS_INFLIGHT=8` on this disk is slower (wall 244 s): concurrent fsyncs thrash. Raise in-flight on PLP NVMe or real S3.

Evidence: `docs/perf/evidence/ss-phased/1786761950/summary.json` (linger-bound), `docs/perf/evidence/ss-phased/1786763497/summary.json` (I/O-bound).
