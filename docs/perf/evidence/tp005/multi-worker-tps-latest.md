# Multi-worker durable tps (in-tree, open_sqlite snorri-shaped)

**Bead:** fireweed-c842dbda  
**Tip:** measured on workspace tip at write time (see git log for `sqlite_multi_worker_tps_probe`).  
**Cell:** `open_sqlite` (sqlite log × memory projection, concurrent projection reads).  
**Shape:** 19 typed indexes, ~2.3 KB payload, finalize + lifecycle-push, claim-batch 500, 8000 entries/weight, best-of-2.

## Command

```sh
cargo test -p fireweed --test sqlite_multi_worker_tps_probe --release --features sqlite -- --nocapture
```

## Results (this host, 2026-08-12)

### Baseline (probe land, pre off-lock encode)

| workers | committed | wall_s | ms/entry | **durable_tps** |
|--------:|----------:|-------:|---------:|----------------:|
| 1 | 8000 | 1.377 | 0.172 | **5,809** |
| 4 | 8000 | 1.288 | 0.161 | **6,210** |
| 8 | 8000 | 1.342 | 0.168 | **5,961** |

### After off-lock JSON encode (`append_serialized` on SqliteLog + BlockingLogStore)

| workers | committed | wall_s | ms/entry | **durable_tps** |
|--------:|----------:|-------:|---------:|----------------:|
| 1 | 8000 | 1.181 | 0.148 | **6,772** |
| 4 | 8000 | 1.050 | 0.131 | **7,618** |
| 8 | 8000 | 1.067 | 0.133 | **7,496** |

**Scoreboard:** w=8 **~75% of 10k goal** (was ~60%). Off-lock encode recovered ~1.5k tps by letting workers serde in parallel; seal is still one FULL fsync stream.

## Vs external snorri

| Source | w=8 tps | Notes |
|--------|--------:|-------|
| Snorri v0.31.3 quiet | 4,318 | Full workflow (claim + commit + caller reads/work) |
| This probe (fireweed-only claim+commit) | ~5,961 | No point-read interleave, no snorri adapter |

Snorri is lower because mixed-op/caller work sits on top of this floor. The **irreducible product floor for pure durable claim+commit** on this host is ~6k — still short of 10k.

## Implications for remaining work

1. **Software ≤0.1 ms/entry** on this shape → single-stream ceiling ~10k (`1/0.0001s`). Current ~0.17 ms → ~6k. (fireweed-9d2281f0)
2. **SqliteLog group-commit** to amortize FULL fsync when multiple workers wait → only path to multi-worker lift on one queue. (fireweed-2a564ff7)
3. Do **not** treat recovery floor 3,692 or prior 4,318 as done.

## Pass criteria for campaign

- Durable tps ≥10,000 on this probe (or snorri quiet ladder) at w≥4, **or**
- Irreducible ceiling proven + group-commit/shard lands with re-measure past 10k.
