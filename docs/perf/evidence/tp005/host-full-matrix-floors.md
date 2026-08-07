# TP-005 full-tier host floors (physics-scaled)

- freeze_source_commit: `25f593013c798671f8c35fcd994a7f18a9b30c50`
- run_id: `1786055456146-1870305`
- tier: `full` status: `passed`
- rows: 400 recovery: 114 maintenance: 12
- host logical CPUs: 32
- host total_memory_kib: 98871364
- archived_at_utc: 2026-08-07T13:10:39Z

## Policy

Host floors are **25% of the per-cell, per-shape median** items/s observed on this full-tier LKG.
They are machine-physics regression bounds for this host class — not portable SLAs.
Use medians (not mins) so a single cold or noisy round does not collapse the floor.
Shapes differ in payload cost; floors are shape-aware.

## Attestation notes

- Matrix coordinator wrote evidence with `status=passed` (400 common + 114 recovery + 12 maintenance).
- Post-write `verify_file` refused because freeze SHA ≠ current `origin/main` and the worktree
  carried the maintenance-cell filter patch (`source_clean=false`). Product data is intact;
  SHA256 of the evidence file matches the sidecar digest.
- Maintenance schedule is filesystem|s3 × sqlite|postgres only (Turso lacks rebuild control plane).

## Append floors (items/s) = 0.25 × median

| cell | shape | median_append | floor_25pct | n |
|---|---|---:|---:|---:|
| filesystem--memory | group-keyed-256 | 1537.0 | 384.3 | 5 |
| filesystem--memory | large-16k | 147.1 | 36.8 | 5 |
| filesystem--memory | minimal | 1655.8 | 414.0 | 5 |
| filesystem--memory | record-1k | 1792.0 | 448.0 | 5 |
| filesystem--postgres | group-keyed-256 | 449.9 | 112.5 | 5 |
| filesystem--postgres | large-16k | 159.1 | 39.8 | 5 |
| filesystem--postgres | minimal | 1211.3 | 302.8 | 5 |
| filesystem--postgres | record-1k | 1155.7 | 288.9 | 5 |
| filesystem--sqlite | group-keyed-256 | 1036.3 | 259.1 | 5 |
| filesystem--sqlite | large-16k | 199.6 | 49.9 | 5 |
| filesystem--sqlite | minimal | 1739.6 | 434.9 | 5 |
| filesystem--sqlite | record-1k | 1327.4 | 331.9 | 5 |
| filesystem--turso | group-keyed-256 | 76.3 | 19.1 | 5 |
| filesystem--turso | large-16k | 80.2 | 20.0 | 5 |
| filesystem--turso | minimal | 169.5 | 42.4 | 5 |
| filesystem--turso | record-1k | 38.9 | 9.7 | 5 |
| memory--memory | group-keyed-256 | 70163.7 | 17540.9 | 5 |
| memory--memory | large-16k | 5052.1 | 1263.0 | 5 |
| memory--memory | minimal | 194616.1 | 48654.0 | 5 |
| memory--memory | record-1k | 19035.9 | 4759.0 | 5 |
| memory--postgres | group-keyed-256 | 703.8 | 176.0 | 5 |
| memory--postgres | large-16k | 822.4 | 205.6 | 5 |
| memory--postgres | minimal | 6448.6 | 1612.2 | 5 |
| memory--postgres | record-1k | 2099.2 | 524.8 | 5 |
| memory--sqlite | group-keyed-256 | 3581.6 | 895.4 | 5 |
| memory--sqlite | large-16k | 1302.8 | 325.7 | 5 |
| memory--sqlite | minimal | 32655.4 | 8163.8 | 5 |
| memory--sqlite | record-1k | 4303.8 | 1075.9 | 5 |
| memory--turso | group-keyed-256 | 55.8 | 13.9 | 5 |
| memory--turso | large-16k | 182.3 | 45.6 | 5 |
| memory--turso | minimal | 194.9 | 48.7 | 5 |
| memory--turso | record-1k | 32.9 | 8.2 | 5 |
| postgres--memory | group-keyed-256 | 11420.9 | 2855.2 | 5 |
| postgres--memory | large-16k | 740.8 | 185.2 | 5 |
| postgres--memory | minimal | 17144.0 | 4286.0 | 5 |
| postgres--memory | record-1k | 4087.5 | 1021.9 | 5 |
| postgres--postgres | group-keyed-256 | 704.5 | 176.1 | 5 |
| postgres--postgres | large-16k | 626.7 | 156.7 | 5 |
| postgres--postgres | minimal | 6747.4 | 1686.9 | 5 |
| postgres--postgres | record-1k | 2931.1 | 732.8 | 5 |
| postgres--sqlite | group-keyed-256 | 2344.3 | 586.1 | 5 |
| postgres--sqlite | large-16k | 414.3 | 103.6 | 5 |
| postgres--sqlite | minimal | 12955.6 | 3238.9 | 5 |
| postgres--sqlite | record-1k | 2592.0 | 648.0 | 5 |
| postgres--turso | group-keyed-256 | 75.7 | 18.9 | 5 |
| postgres--turso | large-16k | 148.5 | 37.1 | 5 |
| postgres--turso | minimal | 160.1 | 40.0 | 5 |
| postgres--turso | record-1k | 30.4 | 7.6 | 5 |
| s3--memory | group-keyed-256 | 1340.8 | 335.2 | 5 |
| s3--memory | large-16k | 222.9 | 55.7 | 5 |
| s3--memory | minimal | 1915.6 | 478.9 | 5 |
| s3--memory | record-1k | 1683.1 | 420.8 | 5 |
| s3--postgres | group-keyed-256 | 390.8 | 97.7 | 5 |
| s3--postgres | large-16k | 150.2 | 37.6 | 5 |
| s3--postgres | minimal | 1509.7 | 377.4 | 5 |
| s3--postgres | record-1k | 945.1 | 236.3 | 5 |
| s3--sqlite | group-keyed-256 | 1162.8 | 290.7 | 5 |
| s3--sqlite | large-16k | 202.1 | 50.5 | 5 |
| s3--sqlite | minimal | 1655.0 | 413.8 | 5 |
| s3--sqlite | record-1k | 1357.4 | 339.3 | 5 |
| s3--turso | group-keyed-256 | 69.0 | 17.3 | 5 |
| s3--turso | large-16k | 108.2 | 27.1 | 5 |
| s3--turso | minimal | 168.0 | 42.0 | 5 |
| s3--turso | record-1k | 36.3 | 9.1 | 5 |
| sqlite--memory | group-keyed-256 | 6627.8 | 1656.9 | 5 |
| sqlite--memory | large-16k | 434.2 | 108.6 | 5 |
| sqlite--memory | minimal | 13161.6 | 3290.4 | 5 |
| sqlite--memory | record-1k | 2177.9 | 544.5 | 5 |
| sqlite--postgres | group-keyed-256 | 591.2 | 147.8 | 5 |
| sqlite--postgres | large-16k | 357.3 | 89.3 | 5 |
| sqlite--postgres | minimal | 4431.3 | 1107.8 | 5 |
| sqlite--postgres | record-1k | 1570.0 | 392.5 | 5 |
| sqlite--sqlite | group-keyed-256 | 2280.6 | 570.1 | 5 |
| sqlite--sqlite | large-16k | 341.8 | 85.5 | 5 |
| sqlite--sqlite | minimal | 7597.5 | 1899.4 | 5 |
| sqlite--sqlite | record-1k | 1824.5 | 456.1 | 5 |
| sqlite--turso | group-keyed-256 | 74.5 | 18.6 | 5 |
| sqlite--turso | large-16k | 138.3 | 34.6 | 5 |
| sqlite--turso | minimal | 207.2 | 51.8 | 5 |
| sqlite--turso | record-1k | 33.7 | 8.4 | 5 |

## Class summary (all shapes pooled median append)

| cell | median_append | floor_25pct | min_round |
|---|---:|---:|---:|
| filesystem--memory | 1477.2 | 369.3 | 93.3 |
| filesystem--postgres | 999.1 | 249.8 | 90.4 |
| filesystem--sqlite | 1191.6 | 297.9 | 86.3 |
| filesystem--turso | 74.1 | 18.5 | 17.9 |
| memory--memory | 36126.4 | 9031.6 | 4614.7 |
| memory--postgres | 1106.2 | 276.6 | 330.1 |
| memory--sqlite | 3872.1 | 968.0 | 1056.9 |
| memory--turso | 118.6 | 29.7 | 27.8 |
| postgres--memory | 5346.1 | 1336.5 | 378.4 |
| postgres--postgres | 1355.1 | 338.8 | 263.5 |
| postgres--sqlite | 2468.2 | 617.0 | 301.4 |
| postgres--turso | 91.3 | 22.8 | 25.8 |
| s3--memory | 1395.6 | 348.9 | 110.0 |
| s3--postgres | 693.7 | 173.4 | 91.1 |
| s3--sqlite | 1159.5 | 289.9 | 132.2 |
| s3--turso | 78.2 | 19.6 | 16.3 |
| sqlite--memory | 3180.3 | 795.1 | 346.3 |
| sqlite--postgres | 798.0 | 199.5 | 114.4 |
| sqlite--sqlite | 2135.1 | 533.8 | 154.0 |
| sqlite--turso | 80.2 | 20.0 | 29.6 |

## Functional coverage

- 5×4 log×projection matrix: 20 cells × 4 shapes × 5 rounds = 400 common measurements
- Recovery: 19 durable cells × 2 shapes × 3 rounds = 114
- Maintenance: 4 rebuildable cells × 3 rounds = 12
- Failures: none
