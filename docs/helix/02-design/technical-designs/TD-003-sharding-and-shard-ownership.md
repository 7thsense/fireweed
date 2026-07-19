---
ddx:
  id: td-sharding-and-shard-ownership
  depends_on:
    - td-storage-architecture-backend-contracts
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - prd
    - concerns
  review:
    self_hash: bbb831efc281b902cc54122b99e39ea67da87dd2db8be0a8c144064d54c2ec17
    deps:
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
      concerns: 73756937e564b8120ca99407bacbd1fa67a06c6021a822c2cb321f7c9d95056e
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
      td-storage-architecture-backend-contracts: 53b17202dcf527948da8d8508639ba6077197c7fd2df1e9888833ca69a9f9f2f
    reviewed_at: "2026-07-19T02:12:30Z"
---

# Technical Design: TD-003 Queue Ownership and Fencing

**Contract**: API-001 | **ADR**: ADR-001, ADR-002, ADR-004, ADR-008, ADR-009 | **Scope**: queue-to-owner assignment, single-writer ownership, epoch fencing, reassignment, drain, recovery, the in-process library owner-runtime and its cached-epoch data-plane fence

## Scope

This technical design defines how a horizontally scaled pqueue deployment
assigns ownership of whole queues to single-writer owners without an external
coordinator, and how a queue's local progress bound is preserved on its one
owner. Per ADR-008, **the queue is the unit of sharding**: a whole queue is
owned by exactly one node at a time, and horizontal scale is achieved by
distributing *queues* across nodes — there is no intra-queue sharding, no
cross-shard claim fan-out, and no cross-shard progress aggregation. This design
is backend-neutral: it constrains every backend profile in TD-001, and TD-002
(`postgres_native`) and TD-004 (`object_log_sqlite_projection`) inherit it.

In scope:

- Deterministic queue-to-owner assignment computed from `ControlPlaneStore`
  state, with no node-to-node discovery or consensus, including the
  `target_owner` vs `active_owner` distinction during reassignment.
- Storage-backed queue leases owned by the `ControlPlaneStore`, including
  renewal, expiry, and monotonic `assignment_epoch` allocation.
- The single authoritative fencing rule: `assignment_epoch` is allocated in the
  control plane and is durably fenced into the durable log before the new lease
  is usable; `LogStore.append_batch` rejects any epoch that is not the current
  control-plane epoch for the queue.
- Reassignment: changing *which owner* holds a queue (owner failure, scale
  up/down, operator action).
- Graceful drain of an owned queue before reassignment.
- Recovery of an owned queue from the latest snapshot plus log tail.
- The per-queue progress bound: `oldest_eligible_age_ms` and
  `progress_bound_risk_count` are **local** properties of the queue on its one
  owner, plus the owner-liveness guard that makes FR-12 enforceable.
- Conformance scenarios: stale-epoch reject (including the
  post-epoch-advance/pre-new-segment window), reassignment, drain, owner
  liveness, stalled-queue visibility.

Out of scope:

- The claim algorithm and eligibility predicate (API-001 Eligibility
  Precedence; TD-001 `ClaimPlan`). Claims are single-owner-local; there is no
  cross-owner claim scheduling.
- Group co-residency placement rules and the client-visible granularity axes
  (ADR-004). Under the queue-as-shard-unit model a group is co-resident on the
  queue's single owner **by construction**; `group_key` is an
  ordering/compatibility concern only, never a placement key.
- The per-group summary projection's row-maintenance and gate-flip lag model
  (TD-002 / TD-004).
- Exact Postgres DDL for control-plane tables (TD-002) and object-log
  manifest/segment shapes (TD-004).
- Operator APIs to trigger reassign/drain (P1 operator contract).
- Cross-tenant or cross-queue placement policy and capacity-based bin-packing
  (P1).
- The no-Postgres / object-store `ControlPlaneStore` implementation — committed
  direction per ADR-008 §4 (the object log provides per-queue multi-node fencing
  and coordination), but designed and reviewed separately; this design specs only
  the pluggable seam, see "Control-Plane Pluggability".

## Technical Approach

**Strategy**: pqueue achieves horizontal scale by distributing whole queues
across owner nodes and giving exactly one worker authority over each queue at a
time (ADR-008). Authority is not negotiated between nodes; it is *read* from the
control plane and *enforced* at the durable log via a monotonic epoch fence.
This keeps the data plane horizontally scalable across the queue population
(ADR-001 decision drivers) while keeping the only coordination point a low-rate,
transactional control plane (concerns.md `deployment-topology` override). A
producer that needs more than one owner's throughput for a logical stream
partitions it across multiple queues at the application layer (ADR-008).

**Key decisions**:

- **One assignment, one mechanism.** Queue-to-owner assignment is deterministic
  from `ControlPlaneStore` state plus the live owner set. It is a pure function
  of control-plane state and requires no node-to-node discovery. There is no
  separate item-to-shard assignment: an item belongs to its queue, and the queue
  has exactly one owner.
- **Storage-backed lease, not a lock service.** Each `(tenant_id, queue_id)` has
  at most one *active owner lease* recorded in the `ControlPlaneStore`. The lease
  carries a monotonically increasing `assignment_epoch`. A worker may append to a
  queue's `LogStore` only while it holds a non-expired lease for the current
  epoch.
- **Epoch fences the log, and the epoch is durably advanced before the new lease
  is usable.** Correctness does not depend on lease-clock accuracy. The
  `assignment_epoch` is allocated in the control plane on `acquire_queue_lease`;
  before the acquiring owner may serve claims, the new epoch is durably fenced
  into the queue's durable log (see "Single Authoritative Fencing Rule").
  Thereafter only the holder of the current `assignment_epoch` can append; the
  backend rejects any epoch that is not current. The lease is a
  *liveness/assignment* mechanism; the epoch is the *safety* mechanism.
- **The progress bound is a local per-queue property.** The progress bound
  (FR-9/FR-12) is queue-wide and is computed entirely on the queue's one owner
  from its own projection (D1). `oldest_eligible_age_ms` and
  `progress_bound_risk_count` are local values; there is no cross-shard
  aggregation, k-way merge, or per-group progress invariant in the engine.
  Queue-global *enforcement* is the conjunction of (i) the owner's claim planner
  honoring the bound for the queue's items (TD-001) and (ii) the queue having a
  live owner (the owner-liveness guard below).
- **No external coordinator.** Assignment, leases, and epochs live in the
  control plane. pqueue runs no membership, election, or consensus protocol.

**Trade-offs**:

- We gain single-writer-per-queue safety with a familiar transactional store, but
  the control plane must stay available for ownership changes (assignment is read
  from the control plane; the existing fallback in TD-001 — reject mutations with
  retryable commit errors — applies).
- We gain deterministic placement (cheap routing, no rebalance chatter), and
  because the owned unit is the whole queue, a claim never fans out across owners
  — it is single-hop and stall-free (ADR-008).
- We accept that a single queue cannot exceed one owner's throughput; this is
  mitigated by app-level multi-queue fan-out and is acceptable because the
  per-queue E0 floor (≥10M items/hr) is met by a single owner with batching
  (ADR-008).

## Queue Ownership and Placement

### Item-to-queue (trivial)

An item belongs to the queue named in its push. There is no intra-queue
placement function: the queue has exactly one owner, so every item of a queue —
and every member of a `group_key` within it — is co-resident on that owner **by
construction** (ADR-008). `group_key` is an ordering/compatibility key only and
MUST NOT be a client-visible or physical placement key (ADR-004). This is what
makes `whole_group` (reachable via `compatibility.group_batching`) and
`whole_cohort` (reachable via `cohort_policy`) claims owner-local and atomic
without any co-residency flag.

> **Internal storage partitioning (non-normative for ownership).** A relational
> backend MAY *physically* hash-partition its item table — e.g. Postgres
> declarative partitioning by `hash(tenant_id, queue_id) % N`, `N` default 16 —
> purely for vacuum/index-size isolation (TD-002). That partition is an internal
> **storage** detail: it is **not** an ownership, routing, or client-visible unit,
> and it does not bound how many nodes the queue population spreads across.

### Queue-to-owner (deterministic, control-plane-driven)

| Rule | Normative text |
|------|----------------|
| Owner set source | The set of candidate owner workers is registered in the `ControlPlaneStore` (`pqueue_workers` or equivalent, see Data Model) with a heartbeat. pqueue MUST NOT discover workers peer-to-peer. The **live owner set** is the set of registered owners whose `heartbeat_at + heartbeat_ttl_ms > now()`. |
| Assignment function | The control plane MUST compute a deterministic **target owner** for each queue from `((tenant_id, queue_id), live_owner_set)` (e.g. rendezvous / highest-random-weight hashing) so that adding or removing one owner moves only an `O(queues / owners)` fraction of queues. The function MUST be a pure function of `((tenant_id, queue_id), live_owner_set)`. |
| Target vs active owner | The function's output is the **target owner**. The **active owner** is whoever currently holds the non-expired lease in the authority record. These MAY differ transiently (a new target is selected but the previous owner's lease has not yet expired or drained). Safety never depends on them agreeing; see Queue Lease Lifecycle and the "Single Authoritative Fencing Rule". |
| Authority record | For each queue the control plane MUST record at most one active owner lease: `(active_owner_id, assignment_epoch, lease_expires_at, state, target_owner_id)`. |
| Epoch monotonicity | `assignment_epoch` MUST increase strictly each time ownership of a queue changes (new owner, reclaim after expiry, or forced reassignment). It MUST NOT decrease or repeat for a queue. |

## Queue Lease Lifecycle

The `ControlPlaneStore` owns queue leases. The following operations are part of
the `ControlPlaneStore` capability (see API / Interface Design) and are
transactional. Throughout, `active_owner` is the lease holder recorded in the
authority record; `target_owner` is the deterministic assignment-function output.

| State | Meaning | Allowed transitions |
|-------|---------|---------------------|
| `unassigned` | No live active owner. | -> `assigned` via `acquire_queue_lease`. |
| `assigned` | An active owner holds a non-expired lease for the current epoch. | -> `assigned` (renew, same epoch); -> `draining` (graceful handoff via `begin_drain` when `target_owner != active_owner`); -> `unassigned` (lease expiry reclaim, new epoch on next acquire). |
| `draining` | Active owner is finishing in-flight work; not accepting new claims; a `target_owner` is recorded. | -> `unassigned` when drain completes or deadline passes. |

**What `resolve_queue_owner` returns by state.** `resolve_queue_owner(queue)`
returns the deterministic `target_owner` plus the current `active_owner`,
`assignment_epoch`, and `state`. Callers interpret it as:

- `unassigned`: the `target_owner` SHOULD call `acquire_queue_lease`.
- `assigned` and `target_owner == active_owner`: the active owner renews; others
  do nothing.
- `assigned` and `target_owner != active_owner`: a reassignment is desired; the
  control plane (or operator) initiates `begin_drain`. The target owner MUST NOT
  acquire until the lease is released/expired.
- `draining`: the active owner is handing off; the recorded `target_owner` waits
  for `unassigned`, then acquires a strictly greater epoch.

**Heartbeat / expiry rules.** Owner liveness is governed by
`OwnerRegistration.heartbeat_at + heartbeat_ttl_ms`. Lease liveness is governed
by `lease_expires_at`. An owner whose heartbeat has expired is removed from the
live owner set (changing future `target_owner` computations) but its lease is
reclaimed only via `lease_expires_at` (so safety is governed by the lease+epoch,
not the heartbeat). `lease_ttl_ms` SHOULD be `>= heartbeat_ttl_ms`.

**Acquisition.** A worker that the deterministic assignment function selects as
`target_owner` for a queue MUST call `acquire_queue_lease(queue, owner_id)`. The
control plane MUST, in one transaction:

1. Reject the acquire if an active (`assigned`/`draining`) non-expired lease is
   held by a different `active_owner` (return current `active_owner` + epoch +
   `state`).
2. Otherwise allocate a strictly greater `assignment_epoch`, set
   `state=assigned`, `active_owner_id=owner_id`, `target_owner_id=owner_id`, and
   `lease_expires_at = now() + lease_ttl_ms`.

After a successful acquire, the new owner MUST durably fence the new epoch into
the queue's log before serving claims (see "Single Authoritative Fencing Rule").

**Renewal.** The owner MUST call
`renew_queue_lease(queue, owner_id, expected_epoch)` before `lease_expires_at`.
Renewal MUST NOT change `assignment_epoch`. A renewal whose `expected_epoch` does
not match the stored epoch, or whose `owner_id` is not the `active_owner`, MUST
fail with `queue-epoch-stale`; the worker MUST stop appending and re-resolve
assignment.

**Expiry / reclaim.** If a lease is not renewed before `lease_expires_at`, the
queue is reclaimable. The next `acquire_queue_lease` (by the deterministically
selected `target_owner`) allocates a **new, strictly greater**
`assignment_epoch`, which fences the previous owner's appends.

**Single Authoritative Fencing Rule (MUST — closes the stale-writer window).**
There is exactly one fencing authority: the control-plane `assignment_epoch`. To
prevent a stale epoch-`E` writer from appending after epoch `E+1` is acquired but
before any `E+1` segment exists, both of the following MUST hold:

1. **Durable fence before use.** On `acquire_queue_lease`, before the new owner
   serves any claim or appends any data segment, it MUST durably record the new
   epoch in the queue's durable log such that the log's recorded current epoch
   becomes `E+1`. For `postgres_native` this is the `assignment_epoch` column on
   the queue-owner row updated in the same acquire transaction (the append
   transaction validates against it). For `object_log_sqlite_projection` (TD-004)
   the new owner MUST commit an **epoch-fence manifest entry** (a zero-or-control
   segment carrying `assignment_epoch = E+1`) via the manifest CAS *before*
   committing any data segment, so the manifest's recorded current epoch advances
   to `E+1` at handoff time, not lazily on first data write.
2. **Reject non-current epoch.**
   `LogStore.append_batch(queue, expected_epoch, ...)` MUST reject any append
   whose `expected_epoch` is not equal to the log's current recorded epoch (not
   merely `<=`). The TD-004 manifest CAS MUST therefore compare against the
   manifest's recorded current epoch (which step 1 has already advanced), and
   MUST reject a writer whose `expected_epoch` is not that current epoch — an
   epoch-`E` writer is rejected the instant `E+1` is fenced, regardless of
   whether an `E+1` *data* segment exists yet.

Therefore at most one writer can ever append to a queue at a given epoch, and a
superseded writer is fenced at handoff, not at first conflicting data write.
Lease TTL governs *liveness* (how fast a dead owner is replaced), never *safety*.

**Lease TTL guidance (SHOULD).** `lease_ttl_ms` SHOULD be configured well above
the worker's renewal interval and above worst-case GC/scheduling pauses, so a
healthy owner is never spuriously reclaimed; correctness does not depend on this,
only the rate of unnecessary reassignment.

**Note on credential strength (deferred).** v1 fencing uses the
`assignment_epoch` as the sole append credential, which is sufficient for trusted
service-internal owners. A later hardening MAY add an unguessable lease
incarnation token alongside the epoch to defend against owner spoofing in
less-trusted deployments. This is recorded as a future option, not a v1
requirement.

## In-Process Library Owner-Runtime (ADR-009)

This design is written in terms of an abstract "owner worker." Per ADR-007 there
are **two** driving adapters that realize an owner-runtime over the *same* engine
coordination: the RESP server (`pqueue-resp`) and the **in-process Rust library**
(`pqueue`, `Pqueue`). ADR-009 makes both first-class owners — **neither is exempt
from resolve + fence**, and coordination is enforced in the engine *below the
ports*, not in either adapter. The rules below constrain the library realization
specifically (closing the gap where the library delegated straight to the
data-plane ports without acquiring a lease); the RESP server realization is
unchanged from the rest of this design.

| Rule | Normative text |
|------|----------------|
| Library is an owner | A `Pqueue` handle MUST carry an `OwnerId` and a `ControlPlaneStore`, resolve ownership, and operate under an acquired, fenced lease for every queue-addressed op — identically to the RESP server. It MUST NOT append to a queue it has not acquired-and-fenced. A single embedded sole-owner deployment is the degenerate case: constant ownership and a constant (always-current) epoch, so single-instance behavior is unchanged. |
| Cached acquire-time epoch (MUST) | The `expected_epoch` carried on every data-plane append (`PushPort`/`ClaimPort`/`FinalizePort` -> `append_batch`) MUST be the epoch the owner **cached at `acquire_queue_lease`** (`OwnedSession.fence_epoch`), NOT a value re-read from the control plane / current log epoch at append time. Re-reading the current epoch defeats the fence (a superseded owner would read the new epoch and pass) and is therefore forbidden. The fence MUST be evaluated **at commit time inside the append's atomic unit of work**, so an owner superseded after it resolved but before it commits is rejected `queue-epoch-stale` mid-operation (no resolve->commit TOCTOU). |
| Single durable epoch (MUST) | For the cached-epoch fence to bind, the control-plane `assignment_epoch` and the storage append-fence epoch MUST be **one durable value advanced atomically at acquire** — already specified as the same token (Data Model) and bound in the `postgres_native` acquire transaction (Backend Profile Bindings). An implementation that keeps two separately-advanced counters does NOT satisfy this rule. |
| Data-path fail-closed (MUST) | Lease liveness MUST fail closed on the **data path**, not only the control path. If a library owner stalls (host GC pause) past `lease_expires_at` and a peer reclaims the queue at a greater epoch, the stalled owner's next append MUST be rejected by the cached-epoch fence regardless of whether its renew loop has run. The cached session is advisory for liveness; the append fence is the safety authority. |
| Target-affinity (MUST) | The library policy layer MUST restrict `acquire_queue_lease` to the queue's deterministic `target_owner` (Queue-to-owner) and MUST NOT acquire a queue a live peer is the target for. A queue held by a different live owner yields an owned-elsewhere resolution (rendered `-MOVED` by RESP, an `OwnedElsewhere` value by the library); the library MUST NOT contend by acquiring a *live* lease (online handoff is `begin_drain`). The reference in-memory control plane's *cooperative* acquire (admits any live owner) is a reference-impl simplification; target-affinity is the normative requirement **both** adapters MUST apply so they cannot thrash a queue against each other. After a renew/acquire timeout the owner MUST **re-resolve**, never blindly retry the non-idempotent acquire. |
| Bounded per-node coordination (MUST) | A library process owning many queues MUST keep renew/heartbeat and ownership state bounded per node — a single bounded renew/heartbeat driver, never one task/connection per queue (Queue density). |

**Multi-instance shared-store competition (library).** Multiple `Pqueue` instances
sharing one durable backend, competing for per-queue leases, is the library
realization of this design. It is correct **only** on a backend that presents the
single atomic acquire->fence epoch above — `postgres_native` once that binding
holds. The reference in-memory control plane (per-process, resets on restart) and a
backend with no shared durable control plane (sqlite-local) are **single-process
only**. `object_log_sqlite_projection` is **single-owner only** until the
manifest-CAS acquire→fence (Control-Plane Pluggability) lands and per-entry epochs
are recorded — committed build direction per ADR-008 §4: the object log is the
intended per-queue multi-node fencing/coordination substrate. A `Pqueue` constructed for multi-owner operation MUST runtime-refuse
a backend that does not present the atomic acquire->fence capability.

## Graceful Drain

Drain is the cooperative path used by reassignment and rolling deploys so claimed
work is not orphaned and progress is not interrupted. Drain is initiated when
`target_owner != active_owner` (a reassignment is desired) and the active owner
is still live.

| Step | Normative text |
|------|----------------|
| 1. Enter drain | The control plane (or operator action) sets the queue lease `state=draining` for the current epoch and records `target_owner_id`. The active owner observes this on its next renew. |
| 2. Stop new claims | While `draining`, the active owner MUST stop serving `BatchClaim` for that queue. Pushes, updates, renewals, and finalizations MAY continue so in-flight leases can be completed. |
| 3. Quiesce | The active owner SHOULD allow active leases to be finalized or to approach expiry up to a bounded `drain_deadline_ms`. It MUST NOT forcibly cancel in-flight worker leases. |
| 4. Hand off | When in-flight work is quiesced or the deadline passes, the active owner stops appending and releases the lease (`release_queue_lease(queue, owner_id, expected_epoch)`), setting `state=unassigned`. |
| 5. Reacquire | The recorded `target_owner` calls `acquire_queue_lease`, gets a strictly greater epoch, durably fences it (see Single Authoritative Fencing Rule), recovers from snapshot + log tail (see Recovery), and resumes claims. |

Drain MUST be safe even if it is interrupted: if the draining owner dies
mid-drain, lease expiry + epoch fencing (see Queue Lease Lifecycle) still
guarantees single-writer safety; the new owner simply recovers and may redeliver
leases that were not finalized (at-least-once, FR-28).

**Progress during drain (MUST).** A draining queue's items still accrue
progress-bound age (they are eligible work that is temporarily not being claimed).
A slow handoff is itself a progress-bound risk: the owner-liveness guard (see
Per-Queue Progress Bound) MUST treat a queue with eligible work that is draining
or unowned past its oldest-eligible item's remaining budget as a violation.
`drain_deadline_ms` SHOULD be set below `progress_bound_ms` for the queue.

## Reassignment

Reassignment changes *which owner* holds a queue; the item set and the queue's
identity are unchanged. Triggered by owner failure (heartbeat/lease expiry),
owner set change (scale up/down changing `target_owner`), or operator action. The
deterministic queue-to-owner function recomputes the `target_owner`; when it
differs from the live `active_owner`, handoff uses graceful drain (online) or
lease expiry + epoch fence (on failure). No item data moves; the new owner
recovers the same queue from snapshot + log tail.

Because the whole queue moves as a unit, reassignment can never split a live
cohort or group across owners — co-residency holds by construction (ADR-008), so
`whole_group`/`whole_cohort` atomicity (G1/G6) is preserved with no special
rebalance rule. There is **no resharding**: the queue is the unit of sharding, so
there is no `shard_count` to change and no item-redistribution migration. A
producer that outgrows one owner's throughput creates additional queues at the
application layer (ADR-008).

## Recovery

When an owner acquires a queue (cold start, reassignment, or restart), it MUST
rebuild authoritative queue state before serving claims, using the CQRS recovery
contract (ADR-001):

| Step | Normative text |
|------|----------------|
| 1. Resolve + fence epoch | Acquire the queue lease, read the current `assignment_epoch`, and durably fence it into the log (see Single Authoritative Fencing Rule) before any data append. |
| 2. Load snapshot | Read the latest `SnapshotStore` snapshot for the queue (TD-001 `latest_snapshot`); it carries a committed `CommandPosition`. For `postgres_native`, the DB-resident projection already holds acknowledged state at its persisted applied-high-water, so the snapshot-load step starts from that position (per ADR-013 the projection is a rebuildable cache, not authoritative; see Backend Profile Bindings). |
| 3. Replay tail | Read `LogStore` commands from the snapshot position forward (TD-001 `read_from`) and apply them to the projection (`apply_committed`), bounded by the retention/snapshot window (ADR-001 bounded replay). |
| 4. Materialize leases | Reconstruct active leases and lease-expiry state; expired leases become eligible again (FR-26) and MUST preserve progress-bound age (FR-11). |
| 5. Resume | Begin serving claims under the current epoch. All appends carry `expected_epoch`. |

Recovery MUST be idempotent: replaying already-applied commands MUST NOT
double-mutate (commands carry monotonic per-queue positions, TD-001). A new
epoch never rewinds the log; it only fences who may extend it.

**Post-fence acceleration boundary.** Ownership recovery MAY optimize required immutable tail reads only
after the durable fence and before serving. It MUST NOT trust a warmed mutable authority head, skip the
persisted projection high-water, or retain payload segments after hydration. Background recovery requires a
node-global bounded dispatcher, an owner generation distinct from the durable epoch, and cooperative
cancellation on acquire unwind, drain, or ownership loss. The current runtime does not provide those
prerequisites. SP-06 rejected a cache after finding content-addressed manifest candidates identifiable but
worth only 8.97% to 11.69% projected p95 improvement; a deliberately unapplied tail also read each unique
segment exactly once. A future proposal must target bounded-parallel required
tail recovery or constant-time authoritative-head access, not cache already-applied payloads. The in-memory
comparator's genesis replay does not authorize a production SQLite cache.

## Per-Queue Progress Bound

The progress bound is **queue-global and computed locally** on the queue's one
owner (D1; FR-9/FR-12 unchanged). There is exactly one `oldest_eligible_age_ms`
and one `progress_bound_risk_count` per queue. Because the whole queue lives on one
owner, both are **local** values read from the owner's own projection in a single
read — there is no cross-owner aggregation, k-way merge, or sum. The per-group
summary rows below are a **storage layout** of one queue's state on its owner, not a
per-group progress invariant: there is **no** per-group or per-shard progress
invariant in the engine, and per-group fairness is a routing concern served by
`DiscoverActiveScopes` (G4), not an engine guarantee.

| Rule | Normative text |
|------|----------------|
| Source | The owner maintains oldest-eligible age in the per-group summary projection (`pqueue_group_summary`, keyed `(tenant_id, queue_id, group_key)`, maintained transactionally with item mutations). `oldest_eligible_at` per row is authoritative and exact; eligible *counts* MAY be lagged/approximate (per the projection consistency model). |
| Queue oldest-eligible | `metrics.oldest_eligible_age_ms` (API-001) MUST equal `now() - min(oldest_eligible_at)` over the queue's own summary rows on its owner — a single local read. The per-group rows store one queue's state; this min imposes no per-group invariant. |
| Queue risk count | `metrics.progress_bound_risk_count` MUST be the count of eligible items whose eligible age is near `progress_bound_ms`. This MAY be approximate when documented (API-001 already allows approximate counts); the oldest-eligible age MUST be authoritative. |
| Read cost | The value MUST be served from the maintained summary projection: a bounded rank-index read on the owner, never a full-table scan of `pqueue_items`. |
| Read semantics | The read carries the summary `as_of` watermark so callers can reason about staleness; the `oldest_eligible_age_ms` is "exact as of `as_of`". |
| Progress enforcement (state vs owner) | TD-003 supplies the per-queue oldest-eligible state and the owner-liveness guard. The decision of how the owner orders claim capacity to keep the bound is the TD-001 claim planner's responsibility: the planner MUST honor the queue-global `progress_bound_ms` (claim a near-violation item before the bound via the queue's progress-protection window — TD-002 claim shape). |
| Owner-liveness guard (MUST) | The control plane MUST treat "a queue with eligible work has no live owner for longer than its oldest-eligible item's remaining budget against `progress_bound_ms`" as a progress-bound risk. The reassignment path (target-owner recompute + acquire) is the mechanism that restores a live owner; TD-003 requires this guard to exist and to be observable (FR-41). |
| Stalled-queue detection | If a queue has no live owner (lease expired, not yet reacquired) or is unreadable, its eligible items still accrue age. Monitoring (FR-41) and `DiscoverActiveScopes` MUST surface the last committed oldest-eligible age (with its `as_of`) so the violation is observable. A queue whose owner is dead for longer than `progress_bound_ms` is a progress-bound violation and MUST be observable. |
| Recurring participation (G5) | A recurring item participates in the queue's oldest-eligible computation exactly like any other item; re-arm only changes when it next becomes eligible. There is no recurring-specific aggregation. |

`DiscoverActiveScopes` (API-001, G4) returns scopes ranked by
`oldest_eligible_age_ms` descending by reading the owner's per-group summary rank
index. Because each queue has one owner, the ranking is a local top-N over the
owner's summary rows; there is no cross-owner merge. Results expose the summary
`as_of` so a caller can reason about lag.

## Control-Plane Pluggability

`ControlPlaneStore` (membership + leases + epoch allocation) is a **pluggable
capability** (ADR-008). The default and only v1-settled implementation is
Postgres (transactional acquire/renew/epoch allocation); ADR-001's bar — "Postgres
is preferred; a backend-specific control plane may be supported later but must
justify" — holds and is met. The no-Postgres / object-store implementation (S3
conditional-PUT lease + heartbeat membership + epoch CAS), enabling a pure
object-log + local-projection deployment, is **committed direction** (ADR-008 §4,
product-owner decision 2026-07-05: the object log provides multi-node fencing and
coordination at the per-queue level). The S3-CAS multi-object acquire→fence
atomicity design must still be proven before it is specified as settled — that
proof is sequenced build work, not an open question. This design specs only the
pluggable **seam**; the object-store implementation gets its own fresh-eyes
review when it lands.

**Seam contract (what any `ControlPlaneStore` implementation MUST provide).** The
seam is substrate-neutral; an implementation is admissible only if it upholds these
invariants — which the Postgres implementation obtains for free from a single
serializable transaction, and which the committed object-store implementation MUST
prove out before it is specified:

| Invariant | Requirement |
|-----------|-------------|
| Single active lease | At most one `active_owner` lease per `(tenant_id, queue_id)` at any instant. Concurrent `acquire_queue_lease` calls MUST linearize: at most one succeeds against a given prior epoch. |
| Monotonic epoch allocation | `assignment_epoch` is allocated strictly increasing per queue and never repeats or decreases, even across acquire races and reclaims (Epoch monotonicity, above). |
| Atomic acquire→fence ordering | The acquired epoch MUST become durable and binding on the log **before** the new owner serves any claim or appends any data segment (Single Authoritative Fencing Rule step 1). On a non-transactional substrate this multi-object ordering (lease record + log/manifest epoch) is the hard part the spike must establish. |
| Bounded staleness on resolve | `resolve_queue_owner` MAY return a stale `active_owner`/`state`, but a stale result MUST be *safe*: acting on it can only fail closed (the fenced append rejects a deposed owner), never produce two live writers. |
| Fail-closed unavailability | When the control plane is unreachable, existing owners keep serving under live leases and new acquisitions/renewals fail with a retryable error (TD-001 control-plane fallback); no append proceeds on an unconfirmed epoch. |

The trait below is the seam; backend DDL/CAS mechanics live in TD-002 (Postgres)
and the object-store control-plane design (TD-004 territory).

## API / Interface Design

TD-003 extends the `ControlPlaneStore` capability (TD-001) with queue-ownership
operations. The hot-path `LogStore.append_batch(expected_epoch)` fencing token is
unchanged from TD-001; its acceptance rule is tightened to "equals current
epoch" per the Single Authoritative Fencing Rule. Shapes are normative for
intent, not final syntax. `QueueKey` is `(tenant_id, queue_id)` — the owned unit.

```rust
pub struct QueueLease {
    pub queue: QueueKey,          // (tenant_id, queue_id)
    pub active_owner_id: OwnerId,
    pub target_owner_id: OwnerId,
    pub assignment_epoch: u64,
    pub state: QueueLeaseState,   // Unassigned | Assigned | Draining
    pub lease_expires_at: Timestamp,
}

pub struct QueueOwnerResolution {
    pub queue: QueueKey,
    pub target_owner_id: OwnerId,             // deterministic assignment output; always present
    pub active_owner_id: Option<OwnerId>,     // None when state == Unassigned
    pub assignment_epoch: Option<u64>,        // None when no lease has ever been granted
    pub state: QueueLeaseState,               // Unassigned | Assigned | Draining
    pub lease_expires_at: Option<Timestamp>,  // None when Unassigned
}

#[async_trait]
pub trait ControlPlaneStore { // additions to the TD-001 trait
    async fn register_owner(
        &self,
        owner_id: &OwnerId,
        heartbeat_ttl_ms: u64,
    ) -> Result<(), ControlPlaneError>;

    /// Deterministic target owner for a queue given the live owner set,
    /// plus the current active owner / epoch / state (target vs active may differ).
    async fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
    ) -> Result<QueueOwnerResolution, ControlPlaneError>;

    /// Acquire/reclaim; allocates a strictly greater epoch on ownership change.
    /// Caller MUST durably fence the new epoch into the log before serving claims.
    async fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner_id: &OwnerId,
        lease_ttl_ms: u64,
    ) -> Result<QueueLease, ControlPlaneError>;

    /// Renew without changing the epoch; fails `QueueEpochStale` on mismatch
    /// or when owner_id is not the active owner.
    async fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner_id: &OwnerId,
        expected_epoch: u64,
        lease_ttl_ms: u64,
    ) -> Result<QueueLease, ControlPlaneError>;

    /// Set state=draining and record target_owner_id for the current epoch.
    async fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner_id: &OwnerId,
    ) -> Result<QueueLease, ControlPlaneError>;

    async fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner_id: &OwnerId,
        expected_epoch: u64,
    ) -> Result<(), ControlPlaneError>;
}
```

| Operation | Maps to |
|-----------|---------|
| `register_owner` / heartbeat | Live owner set for deterministic queue-to-owner assignment. No peer discovery. |
| `resolve_queue_owner` | Deterministic target-owner function (rendezvous hashing) over live owners; returns target + active + epoch + state. |
| `acquire_queue_lease` | Lease acquisition; allocates new epoch; caller then durably fences it. |
| `renew_queue_lease` | Lease renewal; epoch-fenced. |
| `begin_drain` | Graceful Drain step 1; records `target_owner`. |
| `release_queue_lease` | Graceful Drain step 4. |
| (unchanged shape, tightened rule) `LogStore.append_batch(expected_epoch)` | Safety fence; rejects any epoch that is not the current epoch (TD-001/TD-002/TD-004). |

## Data Model Changes

TD-003 defines logical records; backend DDL belongs in TD-002 (Postgres) /
TD-004 (object-log control plane is still Postgres per ADR-001 in v1). The
`QueueAssignment` record extends TD-001; a worker registry is added.

```text
QueueAssignment {            // one row per owned queue
  tenant_id, queue_id,
  backend_profile,
  assignment_epoch,          // monotonic per queue (TD-001); durably fenced into the log on acquire
  active_owner_id,           // current lease holder; null when unassigned
  target_owner_id,           // deterministic assignment-function target; may differ during reassignment
  state,                     // unassigned | assigned | draining
  lease_expires_at,          // owner lease deadline
}

OwnerRegistration {
  owner_id,
  heartbeat_at,
  heartbeat_ttl_ms,
  labels                     // optional capacity/zone hints (P1 use)
}
```

The `assignment_epoch` here is the SAME token already threaded through
`CommandPosition.backend_epoch` and `pqueue_commands.assignment_epoch`
(TD-001/TD-002). TD-003 only specifies *how it advances* (ownership change),
*who allocates it* (the control plane on `acquire_queue_lease`), and *when it
becomes binding on the log* (durably fenced before the new lease is usable, see
Single Authoritative Fencing Rule).

## Security

- **Authorization**: owner registration and lease operations are control-plane
  operations; the service principal MUST be authorized for the deployment's
  control plane. Per-queue tenant authorization (ADR-002) still gates every
  data-plane append.
- **Tenant isolation**: queue leases are keyed by `(tenant_id, queue_id)`; a
  worker holding a lease for one tenant's queue MUST NOT thereby gain access to
  another tenant's queues (ADR-002).
- **Threats**:
  - *Zombie writer*: an owner that paused past its lease and resumed — mitigated
    by epoch fencing on `append_batch` (the core safety property), and
    specifically by the Single Authoritative Fencing Rule which fences the old
    epoch at handoff, not at first conflicting data write.
  - *Owner spoofing*: a worker claiming a queue it was not assigned —
    `acquire_queue_lease` rejects when an active lease is held by another owner;
    deterministic assignment plus single-active-lease bound the blast radius.
    Epoch-only credentials are sufficient for trusted internals; an unguessable
    lease incarnation token is a deferred hardening option (see Queue Lease
    Lifecycle note).
  - *Split brain*: two workers both believing they own a queue — permitted
    transiently for liveness, made safe by single-epoch append.

## Performance

- **Hot path unaffected**: appends carry the epoch the owner already holds; no
  per-append control-plane round trip. Ownership is cached and refreshed on
  renew interval. The durable epoch fence happens once per acquire, not per
  append.
- **Control-plane rate**: lease renew is O(owned queues / renew interval), a
  low-rate background load on the control plane (ADR-001 "low-rate control plane").
- **Progress read**: a bounded rank-index probe on the owner per metrics/discovery
  call, served from the maintained per-group summary index, not item scans. No
  cross-owner merge.
- **Reassignment cost**: recovery time per queue is bounded by snapshot +
  log-tail replay (ADR-001 bounded replay; TD-002 in-place projection makes this
  near-zero for `postgres_native`).

### Queue density: many queues owned per node (>=1000 active queues)

The PRD queue-density target (>=1000 concurrently active queues per node, ideally
a single node) means one node owns the leases for many queues at once. Ownership
and its background work MUST therefore be bounded per node, not per queue:

- **Lease renewal is batched per node.** A node renews all of its owned queue
  leases in bounded batched control-plane writes on the renew interval (one
  multi-row update / small number of statements), NOT one renew task or
  connection per queue. Control-plane rate stays O(owned queues / renew interval)
  with a small constant, and the durable-epoch fence is still once per acquire.
- **One assignment poll per node.** A node learns its assignment set from the
  control plane in a single bounded query, not per-queue subscriptions.
- **Progress aggregation is shared and bounded.** The oldest-eligible monitor
  (and the G4 discovery read path) runs as a bounded shared per-node job over the
  maintained per-group summary index, with work proportional to active queues
  scanned per pass and a bounded cadence — never one monitor loop per queue.
- **Owned-queue state is bounded.** Per-queue in-memory ownership state is small
  and capped; per-queue projection handles (e.g. SQLite databases under TD-004)
  are opened lazily and bounded by an LRU cap rather than held open per owned
  queue indefinitely.

Aggregate single-node throughput remains bounded by the node; density requires
that the 1000th owned active queue costs only bounded incremental
ownership/background resource and still meets its progress bound. Validated by
`queue_density_single_node_tests` (TP-002 E2).

## Backend Profile Bindings

| Profile | Queue lease store | Append fence | Recovery |
|---------|-------------------|--------------|----------|
| `postgres_native` (TD-002) | Postgres queue-owner row, transactional acquire/renew | `assignment_epoch` updated on the queue-owner row in the acquire transaction; the data-plane append transaction validates `expected_epoch == current assignment_epoch` (TD-002 stale-epoch reject). The acquire transaction IS the durable fence. | Normal reconnect: the DB-resident projection already holds acknowledged state at its persisted applied-high-water; recovery reads the current epoch and resumes. Per ADR-013 the projection is a rebuildable cache, not authoritative: it MUST also be reconstructable by replaying the persisted command log from genesis or a snapshot (migration tracked). |
| `object_log_sqlite_projection` (TD-004, D4b) | Postgres `ControlPlaneStore` in v1 (the object-store control plane — the object log providing per-queue fencing/coordination via manifest CAS — is committed direction, ADR-008 §4, acquire→fence proof pending) | On acquire, the new owner MUST commit an epoch-fence manifest entry advancing the manifest's recorded current epoch to `E+1` via CAS BEFORE any data segment; thereafter manifest commit MUST reject any `expected_epoch` not equal to the manifest's recorded current epoch. | Recovery = latest SQLite snapshot from object storage + replay sealed segments after the snapshot position (ADR-001 S3/Object-Log section). |

Both committed v1 profiles MUST pass the TD-003 conformance scenarios (see
Testing).

## Testing

TD-003 is not satisfied until these scenarios pass for every backend profile.

| Scenario | Required evidence |
|----------|-------------------|
| Stale-epoch reject | A writer holding epoch E appends after the queue is reassigned to epoch E+1; the append MUST fail (`queue-epoch-stale`) and MUST NOT mutate state. |
| Stale writer after epoch advance, before new data segment | Epoch E+1 is acquired and the epoch fence is committed, but NO E+1 data segment exists yet. An epoch-E writer's `append_batch`/manifest commit MUST be rejected immediately (it is not the current epoch), proving the fence binds at handoff, not at first conflicting data write. |
| Single writer under contention | Two owners transiently believe they own a queue; only the current-epoch holder's appends commit; no duplicate/lost commands. |
| Reassignment recovery | Kill the owner; after lease expiry a new owner acquires a greater epoch, durably fences it, recovers from snapshot + log tail, and reproduces queue state exactly. |
| Handoff read profile | With a dedicated SP-04 recorder, 200 post-fence/pre-serve handoffs at each governed queue item count and scripted latency report clean and one-unapplied-tail arms separately. Clean recovery records 20,300 immutable / 20,100 avoidable / 20,099 repeated GETs; the tail arm records 40,600 / 40,400 / 39,999 respectively, including exactly 200 unique required segment GETs. These item-count arms do not establish active-queue density. |
| Target vs active owner during rolling deploy | A new `target_owner` is selected while the previous `active_owner` lease is still live; `resolve_queue_owner` reports both; the target does not acquire until drain/expiry; no double-writer window. |
| Graceful drain | `begin_drain` stops new claims, lets in-flight leases finalize within `drain_deadline_ms`, releases the lease; the new owner resumes with no orphaned leases and no progress-bound violation. |
| Interrupted drain | Draining owner dies mid-drain; lease expiry + epoch fence still yield single-writer safety; unfinalized leases redeliver (at-least-once, FR-28). |
| Per-queue progress | A queue with skewed group load reports `oldest_eligible_age_ms` = `now() - min(oldest_eligible_at)` over its summary rows; an item near violation is claimed before `progress_bound_ms`. |
| Owner-local discovery | `DiscoverActiveScopes` ranks the queue's scopes by oldest-eligible from the owner's summary index; the result exposes the summary `as_of`. |
| Group co-residency by construction | All items of one `group_key` are owned by the queue's single owner; `whole_group`/`whole_cohort` claims (G1/G6) are owner-local and atomic with no co-residency flag. |
| Stalled-queue visibility | A queue left unowned past `progress_bound_ms` is surfaced as a progress-bound violation in metrics and `DiscoverActiveScopes`. |
| Queue density (>=1000 active queues/node) | A single node owns the leases for >=1000 concurrently active queues; lease renewal stays O(owned queues / interval) via batched per-node writes (not per-queue tasks/connections), background sweeps/aggregation run as bounded shared per-node jobs, every active queue meets its progress bound, any one queue can reach the per-queue floor, and there is no cross-queue degradation as the active-queue count grows to 1000 (`queue_density_single_node_tests`, TP-002 E2). |
| In-process library owner fenced at commit (ADR-009) | A `Pqueue` owner holding cached epoch E continues to append (push/claim/finalize) after a peer acquired E+1; the append MUST fail `queue-epoch-stale` **at commit**, MUST NOT mutate state, and MUST use the cached epoch (NOT a re-read of current). A sole-owner `Pqueue` is never spuriously fenced. |
| Library data-path fail-closed on stall (ADR-009) | A `Pqueue` owner whose lease expired during a simulated stall, after a peer reclaimed the queue, has its next append fenced **regardless** of whether its renew loop has run (the cached-epoch fence, not the renew loop, is the authority). |
| Multi-instance target-affinity, no thrash (ADR-009) | Two `Pqueue` instances over a shared `postgres_native` store: only the deterministic `target_owner` acquires; requests at a non-target return `OwnedElsewhere` (no contended acquire of a live lease); the `assignment_epoch` does not ping-pong; a superseded instance is fenced; ownership migrates to a new target only on expiry/drain. A multi-owner `Pqueue` on a non-atomic-acquire backend is runtime-refused. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Stale writer after epoch advance but before new data segment | M | H | Single Authoritative Fencing Rule: epoch durably fenced into the log at acquire; append rejects any non-current epoch; dedicated conformance row. |
| Control-plane unavailability blocks ownership changes | M | M | Existing owners keep serving under live leases; new acquisitions fail closed (TD-001 control-plane fallback); appends never proceed on a stale epoch. |
| Lease TTL too low causes ownership churn | M | M | TTL >> renew interval and GC pause; safety is epoch-based so churn only costs recovery, not correctness. |
| Versioned authority history makes handoff metadata reads grow with queue lifetime | H | H | Treat the head as mutable authority that cannot be cached. Design a separately reviewed constant-time conditional-head primitive before adding handoff warmup; TP-002 records physical request amplification. |
| Slow/indefinite drain hides a progress-bound violation | M | H | Owner-liveness guard counts draining/unowned queues; `drain_deadline_ms < progress_bound_ms`; stalled-queue conformance test. |
| Single queue mistaken for unbounded scale | M | M | A queue cannot exceed one owner's throughput (ADR-008); horizontal scale is cross-queue, evidence-gated (TP-002 E2 cross-queue scale-out); the per-queue E0 floor is met by one owner. |

## Review Checklist

- [x] In-process library is a first-class owner-runtime (ADR-009): resolves +
      fences like the RESP server; data-plane append uses the cached acquire-time
      epoch checked at commit; data-path fail-closed; target-affinity; multi-owner
      is postgres-only and runtime-refused elsewhere (object-log single-owner).
- [x] No external coordinator / consensus (ADR-001, concerns.md override).
- [x] Storage-backed queue leases owned by the pluggable `ControlPlaneStore`
      (Postgres default; object-store deferred, ADR-008).
- [x] Single authoritative fencing rule: epoch durably fenced before new lease
      usable; append rejects any non-current epoch (no stale-writer window).
- [x] Deterministic queue-to-owner assignment (HRW over `((tenant,queue),
      live_owner_set)`); target vs active owner defined.
- [x] Queue is the unit of sharding (ADR-008): no `shard_count`, no item-to-shard
      placement, no resharding; group co-residency holds by construction.
- [x] Reassignment changes owner only; whole queue moves as a unit so no
      cohort/group split is possible.
- [x] Graceful drain bounded below `progress_bound_ms`.
- [x] Recovery from snapshot + log tail, idempotent replay.
- [x] Progress bound is a local per-queue property (D1); no cross-shard
      aggregation/merge; enforcement owner (TD-001 planner) + owner-liveness guard
      (TD-003) named.
- [x] Oldest-eligible served from the per-group summary keyed
      `(tenant, queue, group_key)`, a local rank-index probe on the owner,
      surfaced via `DiscoverActiveScopes` (G4); no cross-owner merge.
- [x] Recurring items participate with no special handling (G5).
- [x] Conformance scenarios cover stale-epoch reject, post-advance/pre-segment
      fence, reassignment, drain, target-vs-active, per-queue progress, stalled
      queue, co-residency-by-construction.
