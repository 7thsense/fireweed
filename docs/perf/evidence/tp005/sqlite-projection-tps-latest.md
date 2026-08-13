# Multi-worker durable tps (sqlite log × sqlite projection)

**Cell:** `open_sqlite_sqlite_projection`.  
**Target:** **~13k** durable tps — same as sqlite × memory on this shape.  
**Shape:** 19 typed indexes, ~2.3 KB payload, claim-batch 500, `claim_finalize_push_cycle`.

```sh
cargo test -p fireweed --test sqlite_sqlite_projection_tps_probe --release --features sqlite -- --nocapture
```

## Measure (unique SQL indexes + native `index_fields` blob; non-unique keys not written as SQL rows)

| workers | durable_tps |
|--------:|------------:|
| 1 | **12,221** |
| 4 | **11,975** |
| 8 | **7,214** |

w1 is in the 13k band (sqlite × memory w1 was ~13.1k). w8 still contends on one queue writer.

Apply writes: item row + **unique** `fireweed_item_index` rows only. All index field values persist as a postcard blob on the item so query indexes rebuild from native fields (not 19 B-tree inserts per item).
