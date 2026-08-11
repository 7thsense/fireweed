# Log × projection performance (product cells)

Host: local linux, ManualClock, release profile.  
Tip: measured with `sqlite_commit_snorri_shaped_ladder_probe`.  
Model: ADR-012 — **log = durability**, **projection = performance**. Memory projection is the extreme software baseline.

## Product cells (gated)

Snorri-shaped: 19 typed indexes, ~2.3 KB payload, entity docs, finalize+lifecycle.

| Cell | Constructor | @64 ms/e | @500 | @512 | 512/64 | Floor |
|------|-------------|----------|------|------|--------|-------|
| memory×memory | `open_memory` | 0.058 | 0.049 | **0.037** | 0.64 | ≤0.05 @512 ✓ |
| sqlite×memory | `open_sqlite` | 0.245 | 0.101 | **0.080** | 0.33 | ≤0.15 @512 ✓ |
| objectlog×memory | `open_objectlog` | 1.535 | 0.119 | 0.183 | 0.12 | amortize ✓ |
| sqlite×sqlite | `open_sqlite_sqlite_projection` | 1.304 | 0.420 | 0.547 | 0.42 | amortize ✓ |

## Non-product (print-only)

| Cell | @64 | @500 | @512 |
|------|-----|------|------|
| unified sqlite (`open_sqlite_relational`) | 0.531 | 0.332 | 0.576 |

Unified same-store is **not** the product durability model. Do not use it for release absolute floors.

## Contract

1. Amortization: ms/entry falls (or flat within gate noise) as batch grows on product cells.
2. Memory projection absolute: ≤0.05 ms/entry @512 snorri-shaped.
3. sqlite×memory absolute: ≤0.15 ms/entry @512 snorri-shaped (software + local FS log).
4. Durable projections (sqlite/objectlog) may be slower; log remains authority.
