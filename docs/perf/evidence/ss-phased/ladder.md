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
| 1786891285 | this | 10000 | default cell; `cache_size` 16 MiB; inflight=1; ordered pack path | 120 | 79 | 82 | **101** | 469 | **55.9** | 5865 | 23.6 |
| 1786911948 | incremental group-summary | 1000 | set-based P2 + skip non-eligibility refresh + incremental Push/Claim/P3 | **1259** | **1153** | **1142** | **1498** | 3.2 | **35.7** | 37482 | 8.2 |
| 1786912158 | incremental group-summary | 10000 | same; still O(table) Turso apply/append | 225 | 232 | 231 | 402 | 156 | 57.2 | 6003 | 22.4 |
| 1786915554 | gather→log→apply→ack | 1000 | pack all ops; one apply/PUT; `AsyncProjection` (ack after log) | **4888** | **2146** | **1377** | **1939** | 1.9 | 42.7 | 44798 | 9.0 |
| 1786916098 | gather→log→apply→ack | 10000 | same; P1 still apply/plan-bound on one Turso writer | 228 | 223 | 268 | 399 | 151 | 98.5 | 10333 | 21.5 |
| 1786977588 | planner map | 1000 | Push/BatchUpdate plan from log-ordered map; no Turso on produce | **12255** | **9911** | **10853** | 1060 | 1.2 | 49.3 | 51700 | 8.1 |
| 1786977711 | planner map | 10000 | 1k→10k produce slope fixed (P1/P2/P3 faster at 10k); P4 still Turso catch-up | **21022** | **15392** | **20447** | 315 | 33.4 | 141.9 | 14877 | 20.1 |
| 1786981218 | map claim+finalize | 10000 | claims/finalizes plan from map; claim apply still ordered Turso+summary | **17781** | **14336** | **32354** | 354 | 30.0 | 115.7 | 12129 | 20.1 |
| 1787070603 | be5c6111 | 10000 | v0.31.17 Class S (lease in Turso txn then log); no planner map; inflight=1 | 115 | 73 | 73 | **86** | 515 | 99.7 | 10454 | 454.9 |
| 1787108514 | 5ad99cac | 1000 | reader + produce cursor; inflight=8 | **14885** | 681 | **7093** | 788 | 37.5 | 51.8 | 54297 | 31.0 |
| 1787110479 | f09f45a2 | 10000 | reader + produce cursor; no group-summary on item claim/complete; inflight=8 | **29073** | 262 | **7576** | **508** | 233 | 138.6 | 14529 | 293 |
| 1787153500 | ingest-bump | 10000 | push group summary O(batch); apply starts after packer window; inflight=8 | **31226** | **3686** | 2749 | 502 | 164 | 142.3 | 14920 | 275 |
| 1787166918 | 8355e5e3 | 10000 | shared RelTx hop + `block_in_place`; ingest serialized | 298 | 2635 | 2665 | 311 | 176 | 121.1 | 12703 | 369 |
| 1787169221 | hop-fix | 10000 | RelTx hop on spawn_blocking; group refs from PushItem; per-shard produce delay | **30569** | 3042 | 2720 | 316 | 138 | 131.1 | 13745 | 318 |
| 1787186218 | no apply delay | 10000 | dropped `apply_start_delay_ms.max(300)`; ingest starved | 308 | 2784 | 2836 | 350 | 166 | 119.1 | 12487 | 367 |
| 1787186460 | pipeline peek | 10000 | BatchUpdate one peek; Keep-payload no blob; reader finalize; sequential P4 | **31197** | **4139** | 2795 | 350 | 134 | 126.2 | 13233 | 306 |
| 1787198675 | no-peek + LIMIT 1 relect | 10000 | BatchUpdate no SQL plan; group re-elect LIMIT 1; overlap P4 | **32378** | **34569** | **46642** | 371 | 131 | 142.5 | 14943 | 253 |
| 1787259350 | per-group RelTx relect | 10000 | deleted member dump; COUNT+LIMIT 1 **per group** (200 writer hops) | **31392** | **32088** | **52712** | 175 | 171 | 142.3 | 14919 | 253 |
| 1787259713 | batched relect, no dump | 10000 | COUNT GROUP BY + UNION of LIMIT 1 per 50 groups; dump gone | **31692** | **33465** | **51361** | 382 | 139 | 127.7 | 13389 | 253 |
| 1787266565 | skip already-leased apply | 10000 | Class S apply skips groups; thin SELECT; sequential P4 overlap | **30167** | **33185** | **45500** | 490 | — | 142.3 | 14925 | 252 |
| 1787269858 | inflight=8 P4 | 10000 | waves of 8 claims; coordinator applies out-of-order Ready; still writer-bound | **32030** | **35636** | **49552** | **913** | 126 | 139.7 | 14647 | 314 |
| 1787274546 | lease group-commit | 10000 | **diagnostic/fidelity-reduced**: 8 Class S waiters one IMMEDIATE; Claim omitted fields/metadata/entity/satisfied gates | **31780** | **34898** | **49912** | **1290** | 119 | 139.9 | 14667 | 214 |
| 1787301436 | B-1 worktree | 10000 | fidelity-restored diagnostic; anomalous P1, not an S0 baseline | 420 | 15559 | 35950 | 1240 | 142 | 133.0 | 13945 | 247.1 |
| 1787310542 | b64d68fc | 10000 | **S0 v4 settled baseline**; fidelity-restored; same SHA as mixed control | **12628** | **284** | **317** | **1057** | 108.6 | 146.7 | 15380 | 215.0 |

### S0 settlement-aware same-SHA controls

The authoritative pre-activation controls are [phased v4](1787310542/summary.json)
and [mixed v1](1787310419/mixed-summary.json), both from
`b64d68fc36a45d6563a83bcc1023a730f6d227b9` on `sindri`. All rates below end
at a projection-settlement barrier. The phased residual is exactly
`pending=0, leased=0, complete=10000, failed=0, eligible=0`; the mixed lane
completed every original ready-item ID and retained the intentionally
far-future items as pending.

| phase | ack items/s | settled items/s | settled mutations/s | settlement lag s | service p95/p99 ms |
|---|---:|---:|---:|---:|---:|
| P1 ingest | 25957 | 12628 | 12628 | 0.407 | 34.68 / 45.96 |
| P2 enrich | 29163 | **284** | 284 | **34.906** | 29.71 / 37.03 |
| P3 schedule | 41633 | **317** | 317 | **31.335** | 20.76 / 37.59 |
| P4 Claim/Complete | 1339 | 1057 | 2113 | 1.997 | Claim 362.31 / 362.80; Complete 592.01 / 649.93 |

The ack-only view overstates the two BatchUpdate stages by roughly 103× and
131×. S0 therefore confirms the current bottleneck is background Turso apply,
not public batch admission: further packing or pipelining is not a valid win
unless settled throughput moves with it.

| mixed N=10k control | settled items/s | fill p50/p95/p99 | response bytes p50/p95/p99 | admitted service p95/p99 ms | wall s |
|---|---:|---:|---:|---:|---:|
| far-future Push + Claim/Complete | **48.4** | 100 / 100 / 100 | 143700 / 143700 / 143700 | append 3021.92 / 3118.78; Claim 401.34 / 1311.71; Complete 2669.32 / 2756.07 | 261.2 |

| fixed-25 ms retry cohort | requests / units | capacity rejections | settled units/s | admitted service p50/p95/p99 ms | original age p99 ms |
|---|---:|---:|---:|---:|---:|
| compatible BatchUpdate | 32 / 64 | 0 | 154.6 | 60.35 / 71.67 / 71.68 | 71.68 |
| four incompatible legal Claim keys | 4 / 4 | 0 | 5.65 | 143.92 / 144.05 / 144.05 | 144.05 |
| mixed renew/reassign/purge, one KeyedQueueGate key | 32 / 32 | 0 | 9.43 | 1354.94 / 2260.66 / 2423.08 | 2423.08 |

Every cohort records each original request ID; the incompatible Claim cohort
also records all four distinct `group_key` compatibility values, and the mixed
cohort records each item's declared terminal or retained outcome.

| committed-reader observation (16 samples each) | rate/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|
| `server_peek` | 2501 | 0.105 | 0.136 | 4.773 |
| `server_pending` | 6553 | 0.139 | 0.182 | 0.313 |
| `server_pending_page` | 6431 | 0.152 | 0.162 | 0.188 |
| `server_pending_range` | 6482 | 0.148 | 0.162 | 0.224 |
| `server_live_items` | 759 | 1.315 | 1.348 | 1.389 |
| `server_metrics` | 7435 | 0.133 | 0.140 | 0.157 |

The mixed control measured epoch acquisition at 13.89 ms and terminal-emission
cursor observation at 141.51 ms with zero emission lag. Turso WAL grew from
482072 to 586210112 bytes. The public erased handle does not expose direct
packer-wait counters at S0; the evidence records the configured 20 ms linger
and the compatible-mutation admitted-service proxy (p50/p95/p99
60.35/71.67/71.68 ms) without relabeling it as direct pack wait.

Exact commands, also embedded in the artifacts:

```text
SS_CELL=filesystem--turso SS_N=10000 SS_PUSH_BATCH=100 SS_CLAIM_BATCH=100 SS_INFLIGHT=8 cargo test -p fireweed --test ss_phased_capacity --release ss_phased_capacity_smoke -- --exact --nocapture
SS_MIXED_N=10000 cargo test -p fireweed --test ss_mixed_overlap --release ss_mixed_overlap_baseline -- --exact --nocapture
```

Evidence `1787274546` measured the thin Class-S regression from `5999aa77` and is
useful only for diagnosing transaction cost. Evidence `1787301436` is the first
post-fix diagnostic: its contract-faithful P4 rate is 1,240 items/s versus 1,290
on the reduced response, while its 420-item/s P1 is an obvious same-host outlier.
Neither row is a promotable rate baseline; S0 owns the fidelity-restored,
settlement-aware same-SHA control.

N=10k produce is no longer super-linear in the earlier uncontended diagnostics:
P1 p50 35 ms / 100 items vs 39 ms at N=1k. Objects at N=10k: 484 (was 648).
P4 is the remaining pole (claim still selects on Turso after catch-up of apply
debt). T1 (8k/s P1 at N=100k) is a plausible later measurement; T2 is not until
claim leaves the Turso writer.
