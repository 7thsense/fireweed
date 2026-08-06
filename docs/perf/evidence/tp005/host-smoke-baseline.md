# TP-005 smoke host baseline (non-authoritative)
- commit: `59124799a660f73a5d49da0e8934655521b9c762`
- host: 32 logical CPUs, ~94 GiB RAM, Linux
- tier: smoke (9 local cells, 512 items, batch 64)

## Observed throughput (items/s)

| cell | append | claim | finalize |
|---|---:|---:|---:|
| memory--memory | 90724 | 244815 | 225842 |
| memory--sqlite | 23148 | 24832 | 22403 |
| memory--turso | 2041 | 5548 | 4197 |
| sqlite--memory | 3076 | 6425 | 4956 |
| sqlite--sqlite | 5599 | 5132 | 4589 |
| sqlite--turso | 1303 | 1718 | 2150 |
| filesystem--memory | 812 | 779 | 827 |
| filesystem--sqlite | 705 | 808 | 817 |
| filesystem--turso | 534 | 530 | 654 |

## Host-scaled regression floors (this machine class)

Floors are ~25% of observed smoke rates (physics-scaled, not portable SLAs).

| cell class | append floor (items/s) |
|---|---:|
| memory--memory | 20000 |
| memory--sqlite | 5000 |
| *--turso (local) | 300 |
| filesystem--* | 100 |

Smoke run is **non-authoritative**. Full-tier LKG remains the release record.
