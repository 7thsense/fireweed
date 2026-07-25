# TP-002 E2 — multi-node object_log_sqlite_projection: harness + measured ceiling

**Beads:** `pqueue-b5af53fb` (containerized live multi-node E2 harness) and `pqueue-a983b5e2` (run the
provisioned multi-node E2 evidence). **Status: RESOLVED on kind — BOTH CLOSED.** The release bars are met
**reproducibly** (3/3 consecutive full 2/4/8 sweeps, every bar, with real margin) on a **kind** cluster that
enforces the fix this doc's "Completion path" called for: **CPU-LIMITED server pods + a LEAN, SEPARATED,
in-cluster load generator** driving pod→pod. Constant per-pod CPU limits make per-pod throughput constant, so
aggregate scales linearly 2→4→8 (8/2 ratio ≈4×, clear of the 3.5× bar) and each owner clears the per-queue
floor (~3,000–3,200/s worst ingest). The release evidence, topology, per-pod CPU limits, the three sweeps'
numbers, the one-owner proof, and the exact commands are in
**`docs/perf/tp002-e2-multinode-kind-release.md`** + `docs/perf/evidence/tp002-e2-multinode-kind-release.jsonl`.
The characterization below (raw-docker bridge containers + a host driver) stands as the record of WHY the
co-located ceiling existed and how the kind topology removed it. **Resolved:** 2026-06-29.

> The original raw-docker harness (`crates/fireweed-bench/tests/performance_multi_node_object_log_e2_tests.rs`)
> and its measured ceiling are preserved verbatim below. The hypothesized backend follow-up (decoupling the
> SQLite apply from the coord mutex) was implemented and measured to **regress** ingest (Finding 4) — the
> ceiling was confirmed to be the shared box (CPU), not the backend's locking — so the resolution came from
> the topology (CPU-limited pods + lean separated load), exactly as the Completion path predicted, NOT from a
> backend rewrite. The seal-CPU follow-up (Fix A/B, Finding 5) is a real single-node win and remains adopted.

## Delivered in this pass (Part B)

1. **Containerized, robust harness** —
   `crates/fireweed-bench/tests/performance_multi_node_object_log_e2_tests.rs`. Each owner node is now an
   independent `fireweed-service` **docker container on the bridge** (was host processes on loopback — see
   Finding 1), segmented group-commit mode, tmpfs `/data`, distinct `FIREWEED_NODE_ID`, disjoint
   `FIREWEED_BOOTSTRAP_QUEUES`, reached by container IP. A `Drop` guard force-removes every container. The run
   measures INGEST and CLAIM+FINALIZE as **two separate** per-queue throughputs (mirroring the postgres E0
   baseline, which holds ingest and claim+finalize each to the floor), in two sequential spawn→barrier→work→
   join phases (no post-work barrier, so a worker failure surfaces as a clean test failure, never a hang).
   It self-skips loudly without `FIREWEED_E2_MULTINODE=1` and emits `evidence_tier=release` rows ONLY on a full
   pass (smoke otherwise).

2. **Four real server perf fixes** that made the single-node segmented backend genuinely fast (all land under
   `[ddx-pqueue-b5af53fb]`, all keep `cargo test --workspace` green):
   - `fireweed-resp`: **set `TCP_NODELAY`** on every accepted RESP connection. Without it, the small-reply
     request/response loop pairs with the peer's delayed-ACK and stalls a connection ~40 ms/command over a
     real (non-loopback) bridge link — ~25 items/s. (RESP is a small-message protocol; Nagle must be off.)
   - `fireweed-objectlog` (`segmented.rs`): **`recover_manifest` now reads only the manifest TAIL** instead of
     listing+parsing every manifest object on every seal, and **`LocalFsBlobStore::list` walks only the
     prefix subtree** instead of the whole root. The old code made a sustained push **O(n²)** in the segment
     count; a single queue's seal rate jumped from ~240/s to ~2,400/s. `read_all` still does a full scan for
     recovery (semantics unchanged; all `fireweed-objectlog` tests, incl. MinIO CAS/fence + recovery, pass).
   - `fireweed-sqlite`: the projection store now opens with **`journal_mode=WAL` + `synchronous=NORMAL`** (the
     projection is rebuildable from the durable object log, so this trades nothing the log does not already
     guarantee) — cheaper per-segment commit.
   - `fireweed-server`: new **`FIREWEED_WORKER_THREADS`** knob caps each node's tokio worker pool (default = one
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

No single `FIREWEED_WORKER_THREADS` setting clears BOTH per-queue floors at 8 owners: the box's sustained
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
laggard with `-ERR fireweed epoch_stale`. Rare under the slow apply-under-mutex path (the first push finishes
its acquire before the second resolves), it surfaces reliably under any faster ingest path. **Fix
(`crates/fireweed-server/src/lib.rs`):** a per-queue acquire gate serializes the cold-start acquisition (taken
ONLY on the unowned path — the hot already-owned path stays lock-free); a concurrent first-writer that loses
the gate re-resolves and reuses the winner's session. Whole workspace test suite green.

## Finding 5 — the seal-CPU follow-up (Fix A + Fix B) shipped; real single-node wins, ceiling unmoved (2026-06-29)

The old "Completion path" called for a backend CPU-efficiency follow-up: **cut the per-item CPU of a seal**
(the segment was JSON-serialized) and offer an in-memory projection. Both landed.

**Fix A — kill the double serialization (`crates/fireweed-objectlog/src/segmented.rs`).** The substrate used to
serialize every command **twice** in verbose JSON: once per command just to *measure* its buffered size
(`serde_json::to_vec(env).len()`, bytes discarded), and again to serialize the whole batch on seal
(`to_json(&segment)`). Now each envelope is encoded **once** with `postcard` (compact binary, serde-native)
when it is buffered; `buffered_bytes` is the length of the kept bytes (free, no throwaway serialize); and the
sealed segment is the **framed length-prefixed concatenation** of those kept bytes — `[magic "FWSG"][u8 ver=2]
[u64 epoch][u64 first_seq][u32 count][ (u32 len, bytes)… ]` — with **no re-serialize on seal**. The
per-segment FNV checksum now covers the records-blob region and is verified before any record is decoded on
`read_all`. The per-command envelope **clone** on the enqueue critical section is also gone (the envelope is
moved into the coordinator's `pending` and enqueued into the buffer by reference). `postcard` is added
`default-features = false, features = ["alloc"]` — its only new transitive crate beyond the workspace baseline
is `cobs` (serde/thiserror are already in-tree). All `fireweed-objectlog` tests stay green (round-trip,
ack-after-commit, epoch-fence, recovery, **live MinIO CAS/fence**).

**Fix B — in-memory projection over the segmented log (new fast backend,
`SegmentedObjectLogInMemoryBackend`).** Same group-commit ack-after-seal coordination as the SQLite segmented
backend, but the per-segment projection write is a cheap in-memory `ProjectionData::apply_command` per
command instead of a batched SQLite transaction. Durable boundary is unchanged (the sealed segment + manifest
entry); the projection is a derived view **rebuilt by `read_all` replay** in `create_queue` on open. Wired
config-flagged: `FIREWEED_OBJECT_LOG_MODE=segmented` + `FIREWEED_PROJECTION_BACKEND=inmemory` selects it (the file
`ObjectLogBackend` remains the `objectlog`+`inmemory`+`file` path). **Recovery verified live:** push 3,000
items → `XLEN 3000`; restart the container against the **same** volume → `XLEN 3000` (replayed from the log).

### Single-node measurement (one segmented node, container on the bridge, tmpfs, 12-core box)

A single queue driven by N concurrent RESP connections (pipelined `XADD`, then `XREADGROUP >`+`XACK`). A
*single* closed-loop connection is latency-bound by the flusher (~135 items/s at `seg_max_latency_ms=1`,
identical for both backends — it is the 1 ms seal cadence, not the backend); throughput comes from concurrent
connections co-buffering into each segment, exactly as the E2 harness drives it. Items/s:

| conns | objectlog+sqlite+segmented (Fix A) push / claim+finalize | objectlog+inmemory+segmented (Fix B) push / claim+finalize |
|---|---|---|
| 8 | 3,048 / 22,692 | 3,067 / 28,483 |
| 16 | 5,211 / 24,109 | 5,498 / 28,661 |
| 32 | 10,926 / 24,199 | 12,510 / 28,082 |
| 64 | 15,058 / 23,076 | 16,346 / 28,777 |

Both backends clear the 2,777.78/s floor by a wide margin once concurrency co-buffers, and at 32+ conns the
durable group-commit **push** rate exceeds the non-durable `memory`-engine ceiling (~8,870/s) because the
memory engine has no batching. **Fix B (in-memory projection) is faster than SQLite at every point** —
~8–14 % on push, ~20–25 % on claim+finalize (the force-sealed claim path is the projection-write-bound one,
so dropping the SQLite transaction shows there most).

### E2 multi-node (the bead metric) — ceiling unmoved by Fix A

Re-ran the headline harness on the **sqlite** projection (the bead's required backend) **3× consecutively**
on this 12-core box after Fix A (default tuning: 1 queue/owner, 8 conns/queue, W=4, seg latency 1 ms, 12,000
items/queue):

| run | 8/2 ingest multiple (bar ≥3.5×) | worst ingest/q (floor 2,778) | worst claim+finalize/q | verdict |
|---|---|---|---|---|
| 1 | 2.36× | 1,744 | 12,166 | NOT MET |
| 2 | 2.83× | 1,768 | 7,121 | NOT MET |
| 3 | 3.06× | 2,463 | 11,062 | NOT MET |

Non-decreasing ingest (bar 1) and one-owner-per-queue (bar 4, 56 cross-node confirmations) **PASS** on every
run; the scale-out multiple (bar 2) and worst-per-queue ingest (bar 3) **FAIL** on every run. This is the
**same co-location CPU ceiling** as Findings 3–4: Fix A lowers the *per-seal* CPU, but at 8 server containers
+ a ~64-thread load driver on 12 cores the binding resource is total CPU/scheduler contention, not seal-CPU,
so the savings do not lift the 8-owner per-queue ingest off the floor. **No release row emitted; no number
cherry-picked. Both beads stay OPEN.** Fix A and Fix B are adopted regardless — they are real, measured wins
that make the durable single-node path materially faster and add the fast in-memory durable backend.

## Completion path
- Re-run on a host with more cores, or with the 8 owner nodes on **separate machines** and the driver on its
  own host (the real ADR-008 topology) — removes the co-location contention; the per-queue floors then hold
  with the single-node margins from Finding 2. On a pass the harness emits the `evidence_tier=release` E2
  rows automatically. **This is the real path** (the wall is the shared box, not the backend).
- The backend CPU-EFFICIENCY follow-up (cut the per-item seal CPU with a binary codec) is **now done** —
  Finding 5 (Fix A). It is a real win single-node but did NOT lift the 8-owner ingest off the floor, because
  the co-located-box ceiling is total CPU/scheduler contention, not seal-CPU. The remaining headroom on a
  co-located box would have to come from fewer moving parts on the box (e.g. fewer driver threads), not from
  the backend. NOTE: a concurrency RESTRUCTURE (decoupling the apply) is explicitly NOT a path — see
  Finding 4; it has been measured-and-rejected.

No release row was emitted and no evidence was faked. The smoke row records the honest measured ceiling.

---

## Finding 6 — RESOLVED: the real root cause was ownership SELF-FENCING, not a CPU ceiling (supersedes Findings 3–5's conclusion)

Findings 3–5 concluded the 8-owner wall was "total CPU/scheduler contention on a co-located box" and that the
only path was separate physical hosts. **That conclusion was wrong.** The dominant cause was a correctness
bug, now fixed (`pqueue-79178303`):

- Each node is the SOLE owner of its disjoint queues (its own `InMemoryControlPlane`). Under load the
  background `renew_sessions` task ran late → the 15s lease lapsed → the node re-acquired its OWN lease →
  `lease_decide_acquire` UNCONDITIONALLY bumped `assignment_epoch` → that fenced the node's OWN in-flight
  writes (`EpochFenced` / `-ERR fireweed epoch_stale`) → client retry storm → repeat → ~36× collapse. A
  *slow-but-alive* sole owner fenced *itself*. The pre-fix "coin-flip" was simply whether a lease happened
  to lapse during the timed window.
- **Fix:** same-owner re-affirmation of an uncontested lease PRESERVES the fence epoch (refresh the deadline
  only); the epoch advances only on a genuine takeover by a DIFFERENT owner (where the fence is actually
  needed). A slow node keeps serving at its epoch — graceful degradation, no storm.

**Post-fix live evidence (same co-located kind box, host load ~10 — i.e. the regime that previously
collapsed to 87/q):**

| owners | ingest agg /s | worst ingest/q | claim+final agg /s | worst claim+final/q |
|---|---|---|---|---|
| 2 | 6,413 | 3,207 | 67,316 | 33,663 |
| 4 | 13,012 | 3,257 | 123,308 | 30,843 |
| 8 | **26,222** | **3,278** | 236,856 | 29,450 |

All four bars PASS: ingest non-decreasing 2→4→8; **8/2 ingest 4.09×** (≥3.5×); worst per-queue ingest
**3,278/q** (≥2,778 floor) — vs **87/q pre-fix**; one-owner-per-queue (56 confirmations). Release-tier E2
row emitted; `live_multi_node_object_log_sqlite_projection_e2` cargo test exits 0.

**Conclusion:** the co-located 12-core box was never the wall — the system was starving itself. With the
self-fence removed, the E2 headline holds robustly under load on a single kind host. Separate physical hosts
are NOT required to substantiate it. Findings 3–5's CPU-ceiling framing is retained above for the record but
is superseded by this.
