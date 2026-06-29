# TP-002 E2 — multi-node object_log_sqlite_projection: harness + measured ceiling

**Beads:** `pqueue-b5af53fb` (containerized live multi-node E2 harness) and `pqueue-a983b5e2` (run the
provisioned multi-node E2 evidence). **Status: BOTH OPEN** — the harness is delivered and correct and the
single-node backend is now fast, but the release bars are only **marginally and not reproducibly** met at 8
co-located owners on this 12-core box (a hardware/co-location ceiling, characterized below). The hypothesized
backend follow-up (decoupling the SQLite apply from the coord mutex) was implemented and measured to
**regress** ingest — see **Finding 4**; it was not adopted, and the ceiling is confirmed to be the shared box
(CPU), not the backend's locking. **Date:** 2026-06-29.

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

## Finding 4 — the "decouple the apply from the coord mutex" follow-up was tried and REGRESSES ingest (2026-06-29)

The Completion-path hypothesis below ("move the SQLite batch apply out from under the coord mutex so ingest
no longer degrades with worker-thread count") was **implemented and measured at 8 owners across multiple
runs**. It does **not** help — it **regresses** the binding metric (8-owner per-queue ingest). The decouple
acked pushes at the durable SEAL boundary (eventual-apply: the apply is a derived view) and ran
`apply_committed_batch` on a per-queue ordered applier (FIFO mpsc, applies in strict commit order, claim
drains it via a barrier before selecting so it still observes prior pushes / no double-claim) on the blocking
pool, off the enqueue critical section. Correctness held (single-node still drained 12,000/12,000 with
claim+finalize ~23k–32k/q). But the throughput went the wrong way:

| variant @ 8 owners | ingest agg /s | worst ingest/q | 8/2 ratio |
|---|---|---|---|
| **HEAD (apply under coord mutex)**, W=2 | 16,251–22,616 | 2,028–2,827 | 2.71–3.57 |
| **HEAD**, W=4 | 20,937 | 2,608 | 3.26 |
| **decoupled apply**, W=1 (small batch) | 14,782 | 1,848 | 2.30 |
| **decoupled apply**, W=2 (spawn_blocking) | 15,759 | 1,968 | 2.64 |
| **decoupled apply**, W=1/W=2 (1 MB batch) | 15,605 / 12,469 | 1,907 / 1,559 | 3.98 / 3.17 |

**Why the model was wrong.** The doc's earlier model ("ingest bottleneck is the per-queue coord async mutex;
fastest with few threads") implied removing the apply from that mutex would lift ingest. It does the
opposite, because the binding resource at 8 co-located owners is **CPU, not lock-hold time**: 8 server
containers + a ~64-thread load driver already saturate the 12 cores. Apply-under-the-mutex provides natural
**backpressure** (a push acks only after its apply, rate-limiting the driver to the box's true capacity); the
decouple removes that backpressure and adds applier/blocking-pool threads, so more work is in flight on an
already-saturated box → context-switch thrash → **lower** aggregate ingest. This is the SAME conclusion the
earlier single-queue decouple attempt reached (it too was reverted because the box, not the lock, was the
ceiling). **The decouple was therefore not adopted.** It has now been measured-and-rejected twice; it is not
the path forward.

A secondary correction from the re-measurement: on current HEAD at **W=2** the force-sealed
**claim+finalize** path is comfortably over floor (worst ~10,900–12,500/q, not the ~1,900/q in the Finding 3
table — that row predates a claim-path fix). So at W=2 the ingest-vs-claim "tension" is gone; the **only**
binding constraint is 8-owner ingest, which coin-flips around the 2,778/q floor (median ~2,300–2,400/q, no
reproducible margin). This narrows — but does not move — the ceiling: it is a raw per-queue ingest CPU wall.

### Bug found + fixed during the re-measurement: concurrent cold-start epoch double-acquire

`lease_decide_acquire` is intentionally non-idempotent (it bumps `assignment_epoch` on **every** call). The
server's `ensure_epoch` had no per-queue serialization on the cold-start (`Unassigned`) path, so two
concurrent first-writes to an unowned queue could **each** acquire — double-bumping the epoch and fencing the
laggard with `-ERR pqueue epoch_stale`. Rare under the slow apply-under-mutex path (the first push finishes
its acquire before the second resolves), it surfaces reliably under any faster ingest path. **Fix
(`crates/pqueue-server/src/lib.rs`):** a per-queue acquire gate serializes the cold-start acquisition (taken
ONLY on the unowned path — the hot already-owned path stays lock-free); a concurrent first-writer that loses
the gate re-resolves and reuses the winner's session. Whole workspace test suite green.

## Completion path
- Re-run on a host with more cores, or with the 8 owner nodes on **separate machines** and the driver on its
  own host (the real ADR-008 topology) — removes the co-location contention; the per-queue floors then hold
  with the single-node margins from Finding 2. On a pass the harness emits the `evidence_tier=release` E2
  rows automatically. **This is the real path** (the wall is the shared box, not the backend).
- A backend CPU-EFFICIENCY follow-up (separate bead), if more headroom is wanted on a co-located box: cut the
  **per-item CPU of a seal** (the segment is serialized with `serde_json`; a faster codec would lower the
  seal cost that caps per-queue ingest). NOTE: a concurrency RESTRUCTURE (decoupling the apply) is explicitly
  NOT this path — see Finding 4; it has been measured-and-rejected.

No release row was emitted and no evidence was faked. The smoke row records the honest measured ceiling.
