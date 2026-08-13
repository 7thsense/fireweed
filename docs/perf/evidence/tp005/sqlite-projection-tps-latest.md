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

## Measure (cycle group-commit sealer)

Concurrent `claim_finalize_push_cycle` callers on one shard enqueue; one leader
selects the combined eligible set and issues a single append+apply. w1 is one
waiter → one seal (no extra dispatcher hop). `seals < cycles` proves coalescing.

| workers | durable_tps | seals | cycles |
|--------:|------------:|------:|-------:|
| 1 | **9,061** | 16 | 16 |
| 4 | **10,333** | 11 | 16 |
| 8 | **10,697** | 5 | 16 |

**Concurrency goal met:** w8 / w1 = **1.18** (w8 no longer collapses).  
Host is slower than the v0.31.6 cut machine (that cut was w1 **12,221** / w4 **11,975** / w8 **7,214**). Same-host sqlite×memory w1 was ~8.3k.

Apply writes: item row + **unique** `fireweed_item_index` rows only. All index field values persist as a postcard blob on the item so query indexes rebuild from native fields (not 19 B-tree inserts per item).
