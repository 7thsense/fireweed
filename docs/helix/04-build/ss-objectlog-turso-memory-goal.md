# Goal: object-log × Turso capacity with a cache-bound working set

**Status**: active iteration (2026-09-08). N=10k P1–P4 settled ≥10k with T3
exact on `filesystem--turso` (see current table). T1 met at N=100k (P1 settled
10,656/s). T2 unmet on settled P4 (3,162/s); P4 ack is 9,014/s (Claim p50
31 ms). Claim SELECT orders by indexed `priority_sort,created_seq` or FIFO
`rowid`, never payload. Profile blobs live in `fireweed_item_payloads` and
JOIN after LIMIT. Uniform-priority queues advance a process-owned rowid
floor at SELECT. Residual eligibility is applied in-process. Turso remains
the serving store; pending bodies are not duplicated in process memory.
Apply still bounds N=100k P3/P4 settlement.

The 2026-08-17 planner-map artifacts (`1786977588` and `1786977711`) remain
historical diagnostics. They are not the current design or release evidence:
ack-only P2/P3 throughput hid 31–35 seconds of projection debt at N=10k. The
process-lifetime planner map is out of scope.

**Cell**: `filesystem--turso` via public `open(StorageConfig)` — filesystem object log (same protocol as S3) × Turso ordinary-WAL projection. The SS harness **defaults to this cell**; no env var is required.  
**Not this program**: sqlite command log; in-memory projection as the production serving store.

## Governing lifecycle contract

The object log is authoritative; Turso is the serving projection and rebuild
target. The lifecycle optimization batches compatible public requests behind
the facade while preserving the public maximum of 100 items per request.

1. A compatible generation contains at most eight FIFO requests, 800 requested
   rows, 4 MiB of rendered response data, and 20 ms of linger. Same-queue
   mutations retain at most two generations or sixteen requests.
2. Item Claim is log-first Claim. A statement-level autocommit SELECT on Turso
   takes the next LIMIT rows by indexed `priority_sort,created_seq` or FIFO
   `rowid`. Payload is stored in `fireweed_item_payloads` and JOINed only for
   those LIMIT rows; it is never a sort or filter key, and Claim/Complete
   UPDATE does not rewrite it. No live Deferred snapshot pins WAL across that
   SELECT. Each public request retains its own response, outcome vector, and
   lease token until Turso applies its authoritative position.
3. Response continuation after publication neither renders from Turso nor
   borrows a projection pool. Queued generations keep request structs; they do
   not clone payloads or pre-render bodies.
4. Compatible Push, BatchUpdate, Claim, and Complete envelopes stay distinct in
   the packed object and apply intact, in log order, in one Turso writer
   transaction.
5. The normal retained-response memory ceiling is 128 MiB. After activation,
   the configured writer and serving-pool page-cache ceiling is 224 MiB. These
   structural bounds do not replace the measured M1/M2/M3 gates.
6. During the migration window, new serving uses the log-first path and writes
   no SQL-first lease or Claim outbox row. The legacy schema and recovery-only
   outbox drain remain for at least one release so pre-upgrade leases reopen.

## Current settled evidence

S0 evidence at source `b64d68fc36a45d6563a83bcc1023a730f6d227b9`
is recorded in `docs/perf/evidence/ss-phased/1787310542/summary.json` and
`docs/perf/evidence/ss-phased/1787310419/mixed-summary.json`.

| N | P1 settled | P2 settled | P3 settled | P4 settled | RSS delta |
|---|---:|---:|---:|---:|---:|
| 10,000 | 12,628/s | 284/s | 317/s | 1,057/s | 146.7 MiB |

Post-S8c measured row (2026-08-28) at source
`a7b04a50deffd3c2fc5092f967e899539d5fd6a9` is recorded in
`docs/perf/evidence/ss-phased/1787954751/summary.json` (P1 142/s, P4 168/s).
That row is historical. N=100k was not run.

Current N=10k floor (produce-path identity in process; no Turso snapshot on
Push/pipelined Update) is recorded in
`docs/perf/evidence/ss-phased/1788813701/summary.json`. Occupancy + overlay
prune is `1788659385`. v0.31.25 cut evidence is `1788626038`.

| date | utc | evidence | N | P1 settled | P2 settled | P3 settled | P4 settled |
|---|---|---|---:|---:|---:|---:|---:|
| 2026-08-28 | 1787954751 | historical S8c | 10,000 | 142/s | 658/s | 555/s | 168/s |
| 2026-09-05 | 1788626038 | v0.31.25 | 10,000 | 13,667/s | 17,067/s | 12,575/s | 16,124/s |
| 2026-09-05 | 1788659385 | occupancy + overlay prune | 10,000 | 14,493/s | 18,843/s | 13,367/s | 17,086/s |
| 2026-09-07 | 1788813701 | no produce snapshot | 10,000 | 14,497/s | 18,511/s | 13,408/s | 17,245/s |
| 2026-09-07 | 1788813970 | no produce snapshot | 100,000 | 9,590/s | 3,595/s | 1,688/s | 1,164/s |
| 2026-09-07 | 1788814883 | in-process Claim (reverted) | 10,000 | 14,084/s | 17,990/s | 12,897/s | 17,152/s |
| 2026-09-07 | 1788815083 | in-process Claim (reverted) | 100,000 | 9,691/s | 3,672/s | 1,721/s | 1,151/s |
| 2026-09-07 | 1788816244 | WAL TRUNCATE | 10,000 | 14,305/s | 8,055/s | 13,076/s | 10,033/s |
| 2026-09-07 | 1788816402 | WAL TRUNCATE | 100,000 | 14,520/s | 6,261/s | 2,296/s | 1,413/s |
| 2026-09-07 | 1788817556 | Claim autocommit + TRUNCATE | 10,000 | 14,222/s | 14,685/s | 14,988/s | 14,914/s |
| 2026-09-07 | 1788817726 | Claim autocommit + TRUNCATE | 100,000 | 14,566/s | 6,085/s | 2,195/s | 1,317/s |
| 2026-09-07 | 1788835331 | index-shaped Claim SELECT | 10,000 | 15,374/s | 22,828/s | 16,328/s | 20,220/s |
| 2026-09-07 | 1788835486 | index-shaped Claim SELECT | 100,000 | 16,357/s | 6,581/s | 2,306/s | 1,398/s |
| 2026-09-07 | 1788836783 | last-claim wait | 10,000 | 15,247/s | 23,225/s | 16,847/s | 11,605/s |
| 2026-09-07 | 1788836943 | last-claim wait | 100,000 | 16,730/s | 7,027/s | 2,416/s | 1,227/s |
| 2026-09-07 | 1788837840 | payload sidecar + FIFO BETWEEN | 10,000 | 14,675/s | 17,657/s | 20,172/s | 11,979/s |
| 2026-09-07 | 1788837978 | payload sidecar + FIFO BETWEEN | 100,000 | 15,492/s | 7,272/s | 2,767/s | 1,548/s |
| 2026-09-08 | 1788869692 | Claim SELECT mutex | 10,000 | 14,220/s | 16,658/s | 19,908/s | 18,659/s |
| 2026-09-08 | 1788869837 | Claim SELECT mutex | 100,000 | 14,651/s | 6,898/s | 2,587/s | 1,465/s |
| 2026-09-08 | 1788905429 | group-head idx + FIFO floor | 10,000 | 12,541/s | 16,405/s | 15,954/s | 19,712/s |
| 2026-09-08 | 1788905551 | group-head idx + FIFO floor | 100,000 | 10,656/s | 5,854/s | 2,245/s | 3,162/s |

Gate score on the current working tree: N=10k P1–P4 settled ≥10,000 and T3 exact
(`1788905429`). T1 met at N=100k (P1 settled 10,656/s ≥ 8,000). T2 unmet on
**settled** P4 (3,162/s); P4 **ack** is 9,014/s (Claim p50 31 ms, p99 507 ms).
Claim does not ORDER BY or WHERE payload. Projection file is 22.9 MiB at 10k
and 182.2 MiB at 100k. RSS/item at 100k is 3.4 kB. N=1M was not re-run.

P2/P3 append acknowledgements were 29,163/s and 41,633/s, but settlement lag
was 34.906 s and 31.335 s. The result isolates ordered background Turso apply,
not append packing, as the dominant current bottleneck. The mixed same-SHA
control settled Claim/Complete at 48.37 items/s while overlapping far-future
Push, observations, compatible and incompatible cohorts, and same-key lifecycle
mutations without capacity rejection.

The phased settled lane keeps barriers for attribution and settles projection
debt before each next phase. The mixed lane measures admission and interference.
A later continuous lane removes `join_all` wave barriers with bounded stage
queues while still terminating on exact N and final settlement.

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
| T2 | P4 settled deliver items/s at N=100k | ≥ 4,000 |
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

Measure first on `filesystem--turso`. One slice per commit. Re-measure throughput
and RSS on the same source SHA. T2 can be scored only from the settled interval;
ack-only rates remain diagnostic. Stop when T1–T3 and M1–M3 hold on the same
process-complete N=100k run.
