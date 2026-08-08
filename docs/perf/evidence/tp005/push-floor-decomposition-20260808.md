# Push floor decomposition (fireweed-a83e87be)

Host: local workstation, 2026-08-08. Release-profile probes on tip after P2f.
Snorri reported **0.28 ms/item** enroll via facade spans (sqlite, w=1, 2000-item batches).

## Probe results (this host)

### probe1 — `SqliteRelationalBackend` direct (`push_floor_probe`)

| posture | @10k pending | @30k pending |
|---|---:|---:|
| in_memory | **0.0045 ms/item** | **0.0048 ms/item** |
| file | **0.0052 ms/item** | **0.0053 ms/item** |

### probe3 — `SqliteLog::append` only (`push_floor_probe3`)

| posture | @10k | @30k |
|---|---:|---:|
| in_memory | 0.0032 ms/item | 0.0034 ms/item |
| file `synchronous=FULL` | 0.0142 ms/item | 0.0246 ms/item |

File FULL is dominated by fsync of durable envelopes; cost grows with segment size.

### probe2 — facade `push_batch_with_request_id` (`push_floor_probe2`)

| cell | @10k | @30k |
|---|---:|---:|
| sqlite-relational (unified file) | 0.0068 ms/item | 0.0480 ms/item |
| sqlite-log × sqlite-projection | 0.0125 ms/item | 0.0539 ms/item |
| sqlite-log × memory-projection | 0.0068 ms/item | 0.0062 ms/item |

### `push_cost_scale` (median of 3, in-process relational)

| corpus | median per-item | ratio 30k/10k |
|---|---:|---:|
| ~10k | **0.0046 ms/item** | |
| ~30k | **0.0050 ms/item** | **1.08** (bar ≤1.25×) |

## Decomposition (what spends the cycle)

| layer | ~share of 0.005 ms floor | notes |
|---|---|---|
| Durable log append (memory) | ~60–70% | envelope encode + WAL write |
| Durable log append (file FULL) | 3–5× memory | fsync physics; host disk bound |
| Projection apply + key probes | ~25–35% | batched `IN (...)` occupancy/retention |
| Facade + request_id ledger | +~30–50% at 10k | modest overhead when projection is memory |
| Dual-file log×projection at large corpus | up to ~10× growth 10k→30k | two durable SQLite files + apply lag |

**Conclusion:** the **achievable floor** on this host for enroll-style **2k-item batches** on the unified sqlite relational path is **~0.005 ms/item** (median), **well under** snorri's **0.02 ms/item** target and ~**56× under** the 0.28 ms gate-buster. The 0.28 ms figure is not the bare fireweed relational push floor; it is an end-to-end enroll span that includes caller-side work and/or a heavier composition. Re-measure on snorri must pin cell + batch size + whether request_id retention is enabled.

## Caller batch guidance

1. Prefer **≥2000 items per `push_batch` / `push_batch_with_request_id`**. Sub-hundred batches pay fixed per-txn overhead and will not hit the 0.005 ms floor.
2. For mass enroll on a single host, use **`open_sqlite_relational`** (unified file) or **sqlite-log × memory-projection** when durability of the projection is not required mid-enroll; avoid dual durable log×projection for pure bulk enroll if latency is critical.
3. File durability (`synchronous=FULL`) is **physics-limited** by fsync. Budget ~0.01–0.03 ms/item for log-only file appends; do not treat that as a software regression.
4. 1M members × 0.005 ms ≈ **5 s** of pure push; ×0.02 ms ≈ **20 s**. The historical 280 s (=0.28×1M) is not the current in-process floor.

## Commands

```sh
cargo build -p fireweed-sqlite --example push_floor_probe --example push_floor_probe3 --release
cargo build -p fireweed --example push_floor_probe2 --release --features sqlite
./target/release/examples/push_floor_probe
./target/release/examples/push_floor_probe3
./target/release/examples/push_floor_probe2
cargo test -p fireweed-sqlite --test push_cost_scale --release -- --nocapture
```

## Residual

- AC3: snorri end-to-end re-measure of enroll spans (external).
- Optional follow-up: facade dual-file 10k→30k growth (0.0068→0.048) — not blocking the 0.02 ms floor on the recommended path.
