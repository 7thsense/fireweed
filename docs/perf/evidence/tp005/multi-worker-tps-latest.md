# Multi-worker durable tps (in-tree, open_sqlite snorri-shaped)

**Cell:** `open_sqlite` (sqlite log × **memory** projection).  
**Shape:** 19 typed indexes, ~2.3 KB payload, claim-batch 500, 8000 entries/weight.

## Command

```sh
cargo test -p fireweed --test sqlite_multi_worker_tps_probe --release --features sqlite -- --nocapture
```

## v0.31.5 — native FWC1 + `index_fields` (TypedValue)

Worker loop uses `claim_finalize_push_cycle` (one FULL fsync per claim→finalize→push)
with durable FWC1 envelopes and native `index_fields` (no entity JSON / Base64 on the log).

### Cut measure (release tip)

| workers | committed | wall_s | ms/entry | durable_tps |
|--------:|----------:|-------:|---------:|------------:|
| 1 | 8000 | 0.611 | 0.0764 | **13,089** |
| 4 | 8000 | 0.585 | 0.0731 | **13,680** |
| 8 | 8000 | 0.484 | 0.0606 | **16,514** |

**Campaign:** durable tps **≥12k and ≥13k** cleared on w1/w4; w8 **~16.5k**.  
**Goal met → release cut v0.31.5.**

### Prior v0.31.4 (JSON log, cycle API)

| run | w1 tps | w4 tps | w8 tps |
|----:|-------:|-------:|-------:|
| quiet peak | ~11.8k | ~11.6k | ~9–10k |

## Implications

1. Product cell remains **sqlite log × memory projection** until sqlite-projection work lands.
2. Native codec removes log encode tax; next ceiling is projection durability / apply on sqlite projection.
3. Continue +1k release ladder on product cells as they move.
