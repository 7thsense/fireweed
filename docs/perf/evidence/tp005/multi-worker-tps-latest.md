# Multi-worker durable tps (in-tree, open_sqlite snorri-shaped)

**Cell:** `open_sqlite` (sqlite log × memory projection).  
**Shape:** 19 typed indexes, ~2.3 KB payload, claim-batch 500, 8000 entries/weight.

## Command

```sh
cargo test -p fireweed --test sqlite_multi_worker_tps_probe --release --features sqlite -- --nocapture
```

## 10k result (single-fsync claim_finalize_push_cycle)

Worker loop uses `AsyncLogReplayBackend::claim_finalize_push_cycle` — Claim + Finalize +
Push in **one** durable seal (one `synchronous=FULL` fsync) instead of separate claim then
commit_transition (two seals).

Tip at measure: includes `f4225198` hot-path opts + cycle API.

### Confirmed quiet runs (3× best-of-2 per weight)

| run | w1 tps | w4 tps | w8 tps |
|----:|-------:|-------:|-------:|
| 1 | **10,473** | 9,775 | 9,830 |
| 2 | **11,791** | **11,603** | 9,234 |
| 3 | 9,253 | **10,600** | 8,998 |

**Campaign pass:** durable tps **≥10,000** achieved (w=1 and w=4 repeatedly; peak **~11.8k**).  
**Goal met → release cut v0.31.4.**

### Prior two-seal path (historical)

Separate `claim` + `commit` paid two FULL fsyncs per 500-entry step → best quiet ~8.6k (~86% of 10k).

### Group-commit (direct concurrent appends)

```
cargo test -p fireweed --test sqlite_log_group_commit_stress --release --features sqlite -- --nocapture
# seals=2 appends=64
```

## Implications

1. **10k product shape:** one seal per claim→finalize→push cycle via `claim_finalize_push_cycle`.
2. w=8 can dip under 10k under contention; w=1/w=4 clear 10k with margin.
3. Next 1k steps (11k, 12k, …) continue exclusive-path software + multi-worker contention work; cut a release each +1k.
