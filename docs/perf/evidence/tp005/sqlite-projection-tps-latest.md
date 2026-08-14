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

## Quiet-host measure (v0.31.7 tip)

| workers | durable_tps | seals | cycles |
|--------:|------------:|------:|-------:|
| 1 | **13,141** | 16 | 16 |
| 4 | **13,422** | 11 | 16 |
| 8 | **14,092** | 5 | 16 |

**Concurrency:** w8 / w1 = **1.07** (no collapse).  
**13k cell:** met. **+10% vs v0.31.6 w1 (12,221 → 13,443):** not yet (13,141 = +7.5%).

Next performance cut waits for sqlite×sqlite w1 ≥ **13,443**, then ratchets +10% from that new w1.

Earlier noisy-host best-of-2 was w1 9,061 / w4 10,333 / w8 10,697 (same sealer).

v0.31.6 on the previous cut machine: w1 **12,221** / w4 **11,975** / w8 **7,214**.

Apply writes: item row + **unique** `fireweed_item_index` rows only. All index field values persist as a postcard blob on the item so query indexes rebuild from native fields (not 19 B-tree inserts per item).
