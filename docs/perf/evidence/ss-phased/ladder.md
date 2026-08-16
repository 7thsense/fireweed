# SS phased capacity ladder

Same-host before/after only. Sqlite-log G-gates require N=1,000,000. N=10k/100k are not comparable to those G-rows.

**Active program:** object-log × Turso (`filesystem--turso`) plus RSS. Goal: [ss-objectlog-turso-memory-goal.md](../../../helix/04-build/ss-objectlog-turso-memory-goal.md).

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
| 2026-08-15 | ObjectLogPacker | 1000000 | pack 8 BatchUpdates / 4MiB / 20ms; P1/P4 still 1 PUT/command (admit) | 25932 | **56278** | **95293** | 12868 | 144.5 |

Packed run tree: **6806 objects, 1.79 GiB, ~269 KiB/object**. P2/P3 share one PUT across 8 in-flight BatchUpdates (4 MiB / 20 ms packer). P1 push and P4 claim/finalize still take the per-queue admit permit, so they cannot join a pack — those remain one object per public call. That is the remaining small-object storm, not a hardware floor.

`SS_INFLIGHT=8` on this disk is slower (wall 244 s): concurrent fsyncs thrash. Raise in-flight on PLP NVMe or real S3.

Evidence: `docs/perf/evidence/ss-phased/1786761950/summary.json` (linger-bound), `docs/perf/evidence/ss-phased/1786763497/summary.json` (I/O-bound).

## Object-log × Turso (`filesystem--turso`) — production pair

RSS is the second scoreboard. `filesystem--memory` is the O(N) control, not the product serving store.

| utc | sha | N | note | p1 | p2 | p3 | p4 | wall_s | rss_delta_MiB | B/item | proj_MiB |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1786847365 | harness | 10000 | `filesystem--memory` control | 16152 | 13586 | 16648 | 2107 | 11.8 | **46.1** | 4834 | — |
| 1786848162 | ebd375f3 | 10000 | `filesystem--turso` first baseline (N UpdateFields envelopes) | 112 | **48** | 66 | 82 | 618 | 93.9 | 9845 | 23.6 |
| 1786850873 | da18e3d7 | 10000 | Turso `UpdateFieldsBatch` + set-based apply | 134 | **77** | 68 | 86 | 502 | 93.7 | 9829 | 23.6 |

Turso at N=10k is **slower and fatter** than memory. RSS is fixed overhead + page cache + WAL, not yet the win (M2 needs N=100k). P2 is the long pole: the Turso `BatchUpdate` port still appends **one `UpdateFields` envelope per item**. Memory uses `UpdateFieldsBatch`. Packed apply on Turso was tried and rejected (`expected sequence 300, got 500`) — Turso apply is ordered; out-of-order waiter apply is illegal.
