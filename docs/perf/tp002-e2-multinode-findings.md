# TP-002 E2 — multi-node object_log_sqlite_projection: harness + measured ceiling

**Beads:** `pqueue-b5af53fb` (containerized live multi-node E2 harness) and `pqueue-a983b5e2` (run the
provisioned multi-node E2 evidence). **Status: BOTH OPEN** — the harness is delivered and correct and the
single-node backend is now fast, but the release bars are only **marginally and not reproducibly** met at 8
co-located owners on this 12-core box (a hardware/co-location ceiling, characterized below). **Date:**
2026-06-29.

## Delivered in this pass (Part B)

1. **Containerized, robust harness** —
   `crates/pqueue-bench/tests/performance_multi_node_object_log_e2_tests.rs`. Each owner node is now an
   independent `pqueue-service` **docker container on the bridge** (was host processes on loopback — see
   Finding 1), segmented group-commit mode, tmpfs `/data`, distinct `PQUEUE_NODE_ID`, disjoint
   `PQUEUE_BOOTSTRAP_QUEUES`, reached by container IP. A `Drop` guard force-removes every container. The run
   measures INGEST and CLAIM+FINALIZE as **two separate** per-queue throughputs (mirroring the postgres E0
   baseline, which holds ingest and claim+finalize each to the floor), in two sequential spawn→barrier→work→
   join phases (no post-work barrier, so a worker failure surfaces as a clean test failure, never a hang).
   It self-skips loudly without `PQUEUE_E2_MULTINODE=1` and emits `evidence_tier=release` rows ONLY on a full
   pass (smoke otherwise).

2. **Four real server perf fixes** that made the single-node segmented backend genuinely fast (all land under
   `[ddx-pqueue-b5af53fb]`, all keep `cargo test --workspace` green):
   - `pqueue-resp`: **set `TCP_NODELAY`** on every accepted RESP connection. Without it, the small-reply
     request/response loop pairs with the peer's delayed-ACK and stalls a connection ~40 ms/command over a
     real (non-loopback) bridge link — ~25 items/s. (RESP is a small-message protocol; Nagle must be off.)
   - `pqueue-objectlog` (`segmented.rs`): **`recover_manifest` now reads only the manifest TAIL** instead of
     listing+parsing every manifest object on every seal, and **`LocalFsBlobStore::list` walks only the
     prefix subtree** instead of the whole root. The old code made a sustained push **O(n²)** in the segment
     count; a single queue's seal rate jumped from ~240/s to ~2,400/s. `read_all` still does a full scan for
     recovery (semantics unchanged; all `pqueue-objectlog` tests, incl. MinIO CAS/fence + recovery, pass).
   - `pqueue-sqlite`: the projection store now opens with **`journal_mode=WAL` + `synchronous=NORMAL`** (the
     projection is rebuildable from the durable object log, so this trades nothing the log does not already
     guarantee) — cheaper per-segment commit.
   - `pqueue-server`: new **`PQUEUE_WORKER_THREADS`** knob caps each node's tokio worker pool (default = one
     per core). Essential when many owner containers are co-located on one host, where the default per-process
     `num_cpus` pool oversubscribes the shared cores.

## Finding 1 — the environment kills *sustained loopback* traffic (topology, solved)
A sustained loopback (`127.0.0.1`) RESP connection is terminated with **signal 16 (exit 144)** in this
sandbox once load ramps (a quick connect survives; a benchmark load does not). Container **bridge-IP**
traffic survives indefinitely (proven by the 36-min postgres E0/E1 run). **Resolution:** nodes run as bridge
containers; the driver speaks RESP to each container IP. Confirmed working end-to-end.

## Finding 2 — the segmented backend clears the floor SINGLE-NODE (solved)
With the Part-A segmented group-commit substrate + the Part-B fixes above, a single segmented node on tmpfs,
driven by enough concurrent connections, sustains **~3,000–5,300 items/s ingest** and **>9,000 items/s
claim+finalize** per queue — both above the 2,777.78/s floor. The old `LocalObjectLog` file-per-command path
(~1,821/s tmpfs, ~30/s overlay) is gone.

## Finding 3 — at 8 CO-LOCATED owners on 12 cores the bars are only marginally met (the open item)

Representative steady-state run (default config: 1 queue/owner, 8 conns/queue, pipe 1000, **4 worker
threads/node**, seg latency 1 ms, 12,000 items/queue; smoke row in
`docs/perf/evidence/tp002-e2-multinode-smoke.jsonl`):

| owners | ingest agg /s | worst ingest/q | claim+final agg /s | worst claim+final/q |
|---|---|---|---|---|
| 2 | 6,378 | 3,191 | 48,437 | 24,220 |
| 4 | 10,080 | 2,521 | 51,731 | 12,940 |
| 8 | 21,896 | **2,741** | 103,205 | 12,904 |

- (1) ingest non-decreasing 2→4→8: **PASS**.
- (2) 8/2 ingest multiple: **3.43×** (bar 3.5×) — **just under**.
- (3) worst per-queue ≥ 2,778/s: ingest 2,521 (4-owner straggler) / 2,741 (8-owner) — **just under**;
  claim+finalize 12,904 — far over.
- (4) one-owner-per-queue: **PASS** (56 cross-node `-ERR no such queue` confirmations at 8 owners).

This run-to-run swings right at the bars (other default runs measured ratio 2.66–3.58 and 8-owner worst
ingest 2,113–3,005). The numbers are coin-flip at the floor edge — **not the reproducible margin a release
row should carry.**

### Root cause — an ingest-vs-claim+finalize worker-thread tension on a shared 12-core box

The two paths want OPPOSITE thread budgets, and 8 owner containers + the load driver overcommit 12 cores:

| worker threads/node | 8-owner worst INGEST/q | 8-owner worst CLAIM+FINALIZE/q | verdict |
|---|---|---|---|
| **2** | **3,307** (✓) | **1,904** (✗) | ingest passes, drain starves |
| **3** | 2,421 (✗) | 11,502 (✓) | drain passes, ingest under |
| **4** | 2,113–3,005 (≈) | 12,904 (✓) | drain passes, ingest at the edge |

- **Ingest** co-buffers and seals on the latency cap; its bottleneck is the per-queue coordinator async
  mutex, so it is *fastest with FEW worker threads* (less lock contention) and degrades as threads rise.
- **Claim/finalize** is a read-modify-write that **must force a seal immediately** (the next claim has to see
  the prior claim applied to the projection before it selects candidates — a correctness requirement, not
  just durability), so it *needs MORE worker threads* and starves at W=2.

No single `PQUEUE_WORKER_THREADS` setting clears BOTH per-queue floors at 8 owners: the box's sustained
ingest ceiling at W≥3 is ~17–22k/s (≈2,100–2,750/queue, at/just under floor), while at W=2 the force-sealed
claim path collapses to ~1,900/queue. The architecture scales (ingest is near-linear 2→8 in every regime;
one-owner-per-queue holds), and single-node throughput is well over floor — the wall is **8 servers + driver
on one 12-core host**, not the backend.

## Completion path
- Re-run on a host with more cores, or with the 8 owner nodes on **separate machines** and the driver on its
  own host (the real ADR-008 topology) — removes the co-location contention; the per-queue floors then hold
  with the single-node margins from Finding 2. On a pass the harness emits the `evidence_tier=release` E2
  rows automatically.
- OR a backend follow-up (separate bead): shrink the ingest per-queue coordinator critical section (move the
  SQLite batch apply out from under the coord mutex) so ingest no longer degrades with worker-thread count,
  letting one W setting satisfy both paths on a 12-core box.

No release row was emitted and no evidence was faked. The smoke row records the honest measured ceiling.
