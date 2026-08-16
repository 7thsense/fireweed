# Goal: object-log × Turso capacity with a cache-bound working set

**Status**: active iteration (2026-08-16). Not yet at T/M gates.  
**Cell**: `filesystem--turso` via public `open(StorageConfig)` — filesystem object log (same protocol as S3) × Turso ordinary-WAL projection. The SS harness **defaults to this cell**; no env var is required.  
**Not this program**: sqlite command log; in-memory projection as the production serving store.

In-memory projection is the log-axis calibration cell (`filesystem--memory`). It keeps every live item resident. Turso exists so the serving set can sit on disk and evict pages: **RSS is a cache, not a function of N**.

## Work

Same SS phased harness as Program 1 (`ss_phased_capacity`):

- N default 10k (smoke); capacity rows at N=100k then N=1M
- P1 `BatchPush`, P2/P3 pending `BatchUpdate`, P4 unfiltered `BatchClaim` + `complete`
- 512 B stub ingest, 1 KiB profile blob
- Public facade only

## Throughput (same-host, not an H-server SLA)

Match the object-log packing trajectory, not sqlite-log Off rates.

| Gate | Metric | Floor on this host (WSL virt disk) |
|---|---|---|
| T1 | P1 ingest items/s at N=100k | ≥ 8,000 |
| T2 | P4 deliver items/s at N=100k | ≥ 4,000 |
| T3 | Correctness | exact N through every phase; residual pending=leased=0 |

Stretch after packing lands on this cell: P1 ≥ 20k, P4 ≥ 10k at N=100k. N=1M G-gates from the sqlite-log program are **not** imported.

## Memory (why Turso)

Compare the same harness, same host, same N.

| Gate | Metric | Floor |
|---|---|---|
| M1 | Peak RSS delta (after-run − before-open) at N=100k | Turso ≤ **50%** of `filesystem--memory` |
| M2 | RSS delta per item | Turso N=100k **<** Turso N=10k (not O(N)) |
| M3 | After P4 (queue empty of live work) | Turso RSS does not stay at the P2/P3 peak solely because item bodies are pinned in process memory |

Stretch: N=1M Turso peak RSS delta ≤ **512 MiB** (page cache + WAL + object-log buffers). In-memory at 1M M-class items is expected in the multi-GB range.

`/proc/self/status` `VmRSS` / `VmHWM` are the instruments. Also record Turso file+WAL bytes and object-log tree size.

## Non-goals

- Collapsing sqlite-log into object-log
- Changing Class A `open_sqlite` default FULL
- Making `BatchUpdate` apply to leased items
- Treating Turso as a second in-memory map that happens to fsync

## Iteration

Measure first on `filesystem--turso`. One slice per commit. Re-measure throughput **and** RSS. Stop when T1–T3 and M1–M3 hold on the same N=100k run.
