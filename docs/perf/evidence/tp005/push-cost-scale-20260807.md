# fireweed-310f7a64 — push cost floor and store-size growth

## Measurement (in-process sqlite relational, this host)

`cargo test -p fireweed-sqlite --test push_cost_scale -- --nocapture`

| corpus (pending) | 2k-batch wall | per item |
|---|---:|---:|
| ~10k | 24.5 ms | **0.0123 ms** |
| ~30k | 25.5 ms | **0.0128 ms** |

Ratio 30k/10k = **1.04** (growth bar ≤1.25×).

## Budget

Snorri stated ~0.02 ms/item in 2k batches. Observed floor is **~0.012 ms/item** after
batching conflict/retention probes on the apply path (commit `a0e14454`).

## Change

`SqliteRelationalBackend` push apply previously issued two point queries per item
(occupancy + key retention). Those are now two indexed `IN (...)` probes per batch.

Snorri remeasure via enroll spans remains external confirmation.
