# Multi-worker durable tps (sqlite log × sqlite projection)

**Cell:** `open_sqlite_sqlite_projection`.  
**Target:** **~13k** durable tps — same as sqlite × memory on this shape.  
**Concurrency goal:** **w8 ≈ w1 (~12k)**. One queue cannot be 8×13k; eight workers
must not collapse below the single-worker seal. Cycle group-commit coalesces
concurrent `claim_finalize_push_cycle` callers into one ordered append+apply.
**Shape:** 19 typed indexes, ~2.3 KB payload, claim-batch 500, `claim_finalize_push_cycle`.

```sh
cargo test -p fireweed --test sqlite_sqlite_projection_tps_probe --release --features sqlite -- --nocapture
```

## This-host measure (v0.31.9 peel `c880bc91`, 2026-08-14)

Host load ~4.8–5.1 with a competing `niflheim` process (~160% CPU). Two
independent best-of-2 ladders (the in-tree probe already takes the better of
two runs per worker count):

| ladder | w1 | w4 | w8 | w8/w1 |
|--------|---:|---:|---:|------:|
| 1 (post-compile) | **8,153** | 7,664 | **9,836** | 1.21 |
| 2 (same host, hotter) | 6,893 | 6,699 | 8,194 | 1.19 |

**This-host baseline (best independent ladder):** w1 **8,153** / w4 **7,664** / w8 **9,836**.  
**Concurrency:** w8 / w1 = **1.21** (no collapse).  
**13k cell / +10% vs v0.31.6 w1 (12,221 → 13,443):** not met on this host.

This is a **noisy-host** floor, not a quiet-host increment. Do not treat 8,153
as a regression of the 13,141 quiet-host number below.

## Last quiet-host measure (v0.31.7 tip)

| workers | durable_tps | seals | cycles |
|--------:|------------:|------:|-------:|
| 1 | **13,141** | 16 | 16 |
| 4 | **13,422** | 11 | 16 |
| 8 | **14,092** | 5 | 16 |

**Concurrency:** w8 / w1 = **1.07** (no collapse).  
**13k cell:** met. **+10% vs v0.31.6 w1 (12,221 → 13,443):** not yet (13,141 = +7.5%).

Earlier noisy-host best-of-2 on that same sealer was w1 9,061 / w4 10,333 / w8 10,697.

v0.31.6 on the previous cut machine: w1 **12,221** / w4 **11,975** / w8 **7,214**.

Apply writes: item row + **unique** `fireweed_item_index` rows only. All index field values persist as a postcard blob on the item so query indexes rebuild from native fields (not 19 B-tree inserts per item).
