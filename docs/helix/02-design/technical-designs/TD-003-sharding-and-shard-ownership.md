---
ddx:
  id: td-sharding-and-shard-ownership
  depends_on:
    - td-storage-architecture-backend-contracts
    - adr-cqrs-log-projection-storage-model
    - prd
    - concerns
  review:
    self_hash: f962d0f302d06d256b30abad82b1da033df39b89630763b8be3a3954bc502aa7
    deps:
      adr-cqrs-log-projection-storage-model: 709f701130b5bd00666a1abeef4fb104555a623d39b9fec1fdb9b3167789de10
      concerns: 122b700fbf6049b7fa177b99efa27c5fce011775767d682458a0e2872981fb54
      prd: 382115039de93226b051a09e719c7e1c50f12563d96c1ba85ef142c0ae5d0ce0
      td-storage-architecture-backend-contracts: 5980a5612e178fc0828f567f21efaafd9d49cf7e62b2d8655bf7b9ef32e97d8d
    reviewed_at: "2026-06-20T19:01:18Z"
---

# Technical Design: TD-003 Sharding and Shard Ownership

**Contract**: API-001 | **ADR**: ADR-001, ADR-002, ADR-004 | **Scope**: shard assignment, ownership, fencing, rebalance, drain, recovery, cross-shard progress

## Scope

This technical design defines how a horizontally scaled pqueue queue is split
into shards, how a single writer owns each shard without an external
coordinator, and how the single queue-global progress bound is computed across
shards. It is backend-neutral: it constrains every backend profile in TD-001,
and TD-002 (`postgres_native`) and TD-004 (`object_log_sqlite_projection`)
inherit it.

In scope:

- Deterministic item-to-shard assignment and the relationship to group
  co-residency (ADR-004).
- Deterministic shard-to-owner assignment computed from `ControlPlaneStore`
  state, with no node-to-node discovery or consensus, including the
  `target_owner` vs `active_owner` distinction during reassignment.
- Storage-backed shard leases owned by the Postgres `ControlPlaneStore`,
  including renewal, expiry, and monotonic `assignment_epoch` allocation.
- The single authoritative fencing rule: `assignment_epoch` is allocated in the
  control plane and is durably fenced into the durable log before the new lease
  is usable; `LogStore.append_batch` rejects any epoch that is not the current
  control-plane epoch for the shard.
- Rebalance: changing `shard_count` (resharding) and changing shard ownership
  (reassignment), and the difference between them.
- Graceful drain of an owned shard before reassignment.
- Recovery of an owned shard from the latest snapshot plus log tail.
- Cross-shard queue-global progress: a single `oldest_eligible_age_ms` and
  `progress_bound_risk_count` per queue, computed across shards, the global
  owner-liveness guard that makes FR-12 enforceable, and how claimers/monitors
  find the worst oldest-eligible across shards (tie-in with
  `DiscoverActiveScopes`, G4).
- Conformance scenarios: stale-epoch reject (including the
  post-epoch-advance/pre-new-segment window), reassignment, drain, cross-shard
  progress, stalled-shard visibility.

Out of scope:

- The claim algorithm and eligibility predicate (API-001 Eligibility
  Precedence; TD-001 `ClaimPlan`).
- The cross-shard claim-capacity scheduling decision (which shard receives claim
  throughput to satisfy the queue-global bound). TD-003 defines the *state and
  liveness requirements* the planner must honor; the *enforcement/redirection
  algorithm* is owned by TD-001 (see "Cross-Shard Queue-Global Progress").
- Group co-residency placement rules and the four client-visible granularity
  axes (ADR-004).
- The per-group summary projection's row-maintenance and gate-flip lag model
  (TD-002 / TD-004); TD-003 only consumes its cross-shard aggregation guarantee.
- Exact Postgres DDL for shard tables (TD-002) and object-log manifest/segment
  shapes (TD-004).
- Operator APIs to trigger reshard/reassign/drain (P1 operator contract).
- Cross-tenant or cross-queue placement policy and capacity-based bin-packing
  (P1).

## Technical Approach

**Strategy**: pqueue achieves horizontal scale by partitioning each queue into a
fixed number of shards (`shard_count`, ADR-004) and giving exactly one worker
authority over each shard at a time. Authority is not negotiated between nodes;
it is *read* from the Postgres control plane and *enforced* at the durable log
via a monotonic epoch fence. This keeps the data plane horizontally scalable
(ADR-001 decision drivers) while keeping the only coordination point a low-rate,
transactional control plane (concerns.md `deployment-topology` override).

**Key decisions**:

- **Two distinct assignments, one mechanism.** Item-to-shard assignment is
  deterministic from item identity (ADR-004): non-group-aware items hash on
  `client_item_key`; group-aware items hash on `group_key` so a group is
  co-resident on one shard (D2). Shard-to-owner assignment is deterministic from
  `ControlPlaneStore` shard rows plus the live owner set. Both are pure functions
  of control-plane state; neither requires nodes to discover each other.
- **Storage-backed lease, not a lock service.** Each
  `(tenant_id, queue_id, shard_id)` has at most one *active owner lease* recorded
  in the Postgres `ControlPlaneStore`. The lease carries a monotonically
  increasing `assignment_epoch`. A worker may append to a shard's `LogStore`
  only while it holds a non-expired lease for the current epoch.
- **Epoch fences the log, and the epoch is durably advanced before the new lease
  is usable.** Correctness does not depend on lease-clock accuracy. The
  `assignment_epoch` is allocated in Postgres on `acquire_shard_lease`; before
  the acquiring owner may serve claims, the new epoch is durably fenced into the
  shard's durable log (see "Single Authoritative Fencing Rule"). Thereafter only
  the holder of the current `assignment_epoch` can append; the backend rejects
  any epoch that is not current. The lease is a *liveness/assignment* mechanism;
  the epoch is the *safety* mechanism.
- **Queue-global progress is an aggregation plus a global liveness guard, not a
  per-shard contract.** The progress bound (FR-9/FR-12) is queue-wide (D1). Each
  shard maintains its own oldest-eligible age in the per-group summary
  projection; the queue-global value is the max over shards. There is no
  per-shard or per-group progress invariant in the engine. Queue-global
  *enforcement* is the conjunction of (i) each shard's planner honoring the
  queue-global bound for its own items and (ii) every shard having a live owner;
  the cross-shard claim-capacity decision is owned by TD-001's planner (see
  "Cross-Shard Queue-Global Progress").
- **No external coordinator.** Assignment, leases, and epochs live in Postgres.
  pqueue runs no membership, election, or consensus protocol.

**Trade-offs**:

- We gain single-writer-per-shard safety with a familiar transactional store, but
  the control plane must stay available for ownership changes (assignment is read
  from Postgres; the existing fallback in TD-001 — reject mutations with
  retryable commit errors — applies).
- We gain deterministic placement (cheap routing, no rebalance chatter), but
  changing `shard_count` is a heavier operation (resharding, see Rebalance).
- We gain queue-global progress correctness across shards, but cross-shard
  `oldest_eligible_age_ms` requires one rank-index read per shard plus a merge
  (bounded by `shard_count`, served from the maintained per-group summary
  projection, not a scan).

## Shard Identity and Placement

### Item-to-shard (deterministic, ADR-004)

| Rule | Normative text |
|------|----------------|
| Shard count | A queue's `shard_count` is fixed at `CreateQueue` (API-001 `CreateQueue.shard_count`, surfaced into TD-001 `QueueDefinition.shard_count`). It MUST be >= 1. `shard_count=1` is the single-shard reference profile and MUST behave identically to a degenerate multi-shard queue. See "Shard count in the contract" below for who sets it. |
| Group-aware placement | When `group_co_residency=true` (API-001, D2), an item's shard MUST be `hash(group_key) mod shard_count`. All items sharing a `group_key` MUST be co-resident on exactly one shard (ADR-004). This is what makes `whole_group` (reachable via `compatibility.group_batching`) and `whole_cohort` (reachable via `cohort_policy`) claims shard-local and atomic. |
| Non-group placement | When `group_co_residency=false`, an item's shard MUST be `hash(client_item_key) mod shard_count`. |
| Hash function | `hash` MUST be a stable, documented, non-cryptographic hash with uniform distribution; it MUST NOT change for a queue after creation (changing it is equivalent to resharding, see Rebalance). |
| Visibility | `shard_id` MUST NOT be a client-visible ordering or progress key (ADR-004). It is a physical routing/capacity unit only. |

### Shard count in the contract (who sets `shard_count`)

| Rule | Normative text |
|------|----------------|
| Client-supplied, policy-bounded | `shard_count` is an **optional client field on `CreateQueue`** (API-001). When omitted it defaults to `1`. When supplied it MUST be `>= 1` and MUST be `<= deployment_max_shard_count` (a service/deployment policy bound); a request above the bound MUST be rejected with `invalid-request`. |
| Operator/service override | A deployment MAY pin or override `shard_count` by policy (e.g. force `1` on a single-node deployment, or derive a default from queue class). When policy overrides a client value, the stored `QueueDefinition.shard_count` is authoritative and `CreateQueue.response` MUST echo the effective value. |
| D4 v1 path | The D4(a) multi-shard path is requested by a client passing `shard_count = N > 1` on `CreateQueue` (subject to the policy bound). No separate "enable sharding" flag exists; `shard_count > 1` *is* the request. |
| Immutability | `shard_count` is immutable after `CreateQueue` in v1 (see Rebalance). An idempotent `CreateQueue` with a different `shard_count` is an incompatible definition and MUST fail per API-001 idempotent-create rules. |

### Shard-to-owner (deterministic, control-plane-driven)

| Rule | Normative text |
|------|----------------|
| Owner set source | The set of candidate owner workers is registered in the `ControlPlaneStore` (`pqueue_workers` or equivalent, see Data Model) with a heartbeat. pqueue MUST NOT discover workers peer-to-peer. The **live owner set** is the set of registered owners whose `heartbeat_at + heartbeat_ttl_ms > now()`. |
| Assignment function | The control plane MUST compute a deterministic **target owner** for each shard from `(shard_id, live_owner_set)` (e.g. rendezvous/highest-random-weight hashing) so that adding or removing one owner moves only an `O(shard_count / owners)` fraction of shards. The function MUST be a pure function of `(shard_id, live_owner_set)`. |
| Target vs active owner | The function's output is the **target owner**. The **active owner** is whoever currently holds the non-expired lease in the authority record. These MAY differ transiently (a new target is selected but the previous owner's lease has not yet expired or drained). Safety never depends on them agreeing; see Shard Lease Lifecycle and the "Single Authoritative Fencing Rule". |
| Authority record | For each shard the control plane MUST record at most one active owner lease: `(active_owner_id, assignment_epoch, lease_expires_at, state, target_owner_id)`. |
| Epoch monotonicity | `assignment_epoch` MUST increase strictly each time ownership of a shard changes (new owner, reclaim after expiry, or forced reassignment). It MUST NOT decrease or repeat for a shard. |

## Shard Lease Lifecycle

The `ControlPlaneStore` owns shard leases. The following operations are added to
the `ControlPlaneStore` capability (see API / Interface Design) and are
transactional. Throughout, `active_owner` is the lease holder recorded in the
authority record; `target_owner` is the deterministic assignment-function output.

| State | Meaning | Allowed transitions |
|-------|---------|---------------------|
| `unassigned` | No live active owner. | -> `assigned` via `acquire_shard_lease`. |
| `assigned` | An active owner holds a non-expired lease for the current epoch. | -> `assigned` (renew, same epoch); -> `draining` (graceful handoff when `target_owner != active_owner`); -> `unassigned` (lease expiry reclaim, new epoch on next acquire). |
| `draining` | Active owner is finishing in-flight work; not accepting new claims; a `target_owner` is recorded. | -> `unassigned` when drain completes or deadline passes. |

**What `resolve_shard_owner` returns by state.** `resolve_shard_owner(shard)`
returns the deterministic `target_owner` plus the current `active_owner`,
`assignment_epoch`, and `state`. Callers interpret it as:

- `unassigned`: the `target_owner` SHOULD call `acquire_shard_lease`.
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
`target_owner` for a shard MUST call `acquire_shard_lease(shard, owner_id)`. The
control plane MUST, in one transaction:

1. Reject the acquire if an active (`assigned`/`draining`) non-expired lease is
   held by a different `active_owner` (return current `active_owner` + epoch +
   `state`).
2. Otherwise allocate a strictly greater `assignment_epoch`, set
   `state=assigned`, `active_owner_id=owner_id`, `target_owner_id=owner_id`, and
   `lease_expires_at = now() + lease_ttl_ms`.

After a successful acquire, the new owner MUST durably fence the new epoch into
the shard's log before serving claims (see "Single Authoritative Fencing Rule").

**Renewal.** The owner MUST call
`renew_shard_lease(shard, owner_id, expected_epoch)` before `lease_expires_at`.
Renewal MUST NOT change `assignment_epoch`. A renewal whose `expected_epoch` does
not match the stored epoch, or whose `owner_id` is not the `active_owner`, MUST
fail with `shard-epoch-stale`; the worker MUST stop appending and re-resolve
assignment.

**Expiry / reclaim.** If a lease is not renewed before `lease_expires_at`, the
shard is reclaimable. The next `acquire_shard_lease` (by the deterministically
selected `target_owner`) allocates a **new, strictly greater**
`assignment_epoch`, which fences the previous owner's appends.

**Single Authoritative Fencing Rule (MUST — closes the stale-writer window).**
There is exactly one fencing authority: the control-plane `assignment_epoch`. To
prevent a stale epoch-`E` writer from appending after epoch `E+1` is acquired but
before any `E+1` segment exists, both of the following MUST hold:

1. **Durable fence before use.** On `acquire_shard_lease`, before the new owner
   serves any claim or appends any data segment, it MUST durably record the new
   epoch in the shard's durable log such that the log's recorded current epoch
   becomes `E+1`. For `postgres_native` this is the `assignment_epoch` column on
   the shard row updated in the same acquire transaction (the append transaction
   validates against it). For `object_log_sqlite_projection` (TD-004) the new
   owner MUST commit an **epoch-fence manifest entry** (a zero-or-control segment
   carrying `assignment_epoch = E+1`) via the manifest CAS *before* committing any
   data segment, so the manifest's recorded current epoch advances to `E+1` at
   handoff time, not lazily on first data write.
2. **Reject non-current epoch.**
   `LogStore.append_batch(shard, expected_epoch, ...)` MUST reject any append
   whose `expected_epoch` is not equal to the log's current recorded epoch (not
   merely `<=`). The TD-004 manifest CAS MUST therefore compare against the
   manifest's recorded current epoch (which step 1 has already advanced), and
   MUST reject a writer whose `expected_epoch` is not that current epoch — an
   epoch-`E` writer is rejected the instant `E+1` is fenced, regardless of
   whether an `E+1` *data* segment exists yet.

Therefore at most one writer can ever append to a shard at a given epoch, and a
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

## Graceful Drain

Drain is the cooperative path used by rebalance and rolling deploys so claimed
work is not orphaned and progress is not interrupted. Drain is initiated when
`target_owner != active_owner` (a reassignment is desired) and the active owner
is still live.

| Step | Normative text |
|------|----------------|
| 1. Enter drain | The control plane (or operator action) sets the shard lease `state=draining` for the current epoch and records `target_owner_id`. The active owner observes this on its next renew. |
| 2. Stop new claims | While `draining`, the active owner MUST stop serving `BatchClaim` for that shard. Pushes, updates, renewals, and finalizations MAY continue so in-flight leases can be completed. |
| 3. Quiesce | The active owner SHOULD allow active leases to be finalized or to approach expiry up to a bounded `drain_deadline_ms`. It MUST NOT forcibly cancel in-flight worker leases. |
| 4. Hand off | When in-flight work is quiesced or the deadline passes, the active owner stops appending and releases the lease (`release_shard_lease(shard, owner_id, expected_epoch)`), setting `state=unassigned`. |
| 5. Reacquire | The recorded `target_owner` calls `acquire_shard_lease`, gets a strictly greater epoch, durably fences it (see Single Authoritative Fencing Rule), recovers from snapshot + log tail (see Recovery), and resumes claims. |

Drain MUST be safe even if it is interrupted: if the draining owner dies
mid-drain, lease expiry + epoch fencing (see Shard Lease Lifecycle) still
guarantees single-writer safety; the new owner simply recovers and may redeliver
leases that were not finalized (at-least-once, FR-28).

**Progress during drain (MUST).** A draining shard's items still accrue
progress-bound age (they are eligible work that is temporarily not being claimed
on that shard). The cross-shard progress aggregation (see Cross-Shard
Queue-Global Progress) MUST continue to count a draining shard's oldest-eligible
age so the queue-global bound is not silently violated by a slow handoff.
`drain_deadline_ms` SHOULD be set below `progress_bound_ms` for the queue.

## Rebalance

pqueue distinguishes two operations. Both are control-plane events; neither
requires consensus.

### Reassignment (cheap, online)

Reassignment changes *which owner* holds a shard; `shard_count` and
item-to-shard mapping are unchanged. Triggered by owner failure (heartbeat/lease
expiry), owner set change (scale up/down changing `target_owner`), or operator
action. The deterministic shard-to-owner function recomputes the `target_owner`;
when it differs from the live `active_owner`, handoff uses graceful drain
(online) or lease expiry + epoch fence (on failure). No item data moves; the new
owner recovers the same shard from snapshot + log tail.

**Rebalance must not split a live cohort or group (MUST, G6).** Because cohorts
and groups inherit group co-residency (`shard = hash(group_key) mod
shard_count`, D2), all members of a `group_key` are co-resident on one shard. A
reassignment MUST move the whole shard — and therefore the whole cohort/group —
to the new owner as a unit; it MUST NOT split a live cohort's (or group's)
`group_key` across shards. Reassignment does not change item-to-shard mapping,
so this holds by construction; resharding (below), which does change the mapping,
is what could split a cohort and is therefore gated.

### Resharding (heavier, gated)

Resharding changes `shard_count`, which changes `hash(...) mod shard_count` and
therefore moves items between shards.

| Rule | Normative text |
|------|----------------|
| v1 default | `shard_count` is fixed at `CreateQueue` and is **immutable in v1** unless a later migration design (operator contract) defines a safe split/merge. |
| Why gated | Changing `shard_count` re-partitions group co-residency; a group must atomically move to its new shard or `whole_group`/`whole_cohort` atomicity breaks. This requires a migration command sequence (drain affected shards, copy group state under a fence, advance epoch) that is a P1 operator contract, not a hot-path operation. |
| Evidence tie-in | Online resharding under load is the only mechanism that proves *unbounded* horizontal scale-out; it is gated on the scale-substantiation evidence (TP-002, E0-E3). v1 commits to *pre-created multi-shard placement* (fixed `shard_count > 1` spread across owners), which is sufficient to defend D4(a) horizontal claim distribution without live resharding. |

**v1 commitment (MUST):** A queue created with `shard_count = N > 1` MUST
distribute its N shards across the available owners via the deterministic
shard-to-owner function, run independent single-writer claim/append per shard,
and aggregate progress queue-globally. This is the substantiated multi-shard
claim path (D4a). Live resharding (changing N) is deferred to a migration
contract.

## Recovery

When an owner acquires a shard (cold start, reassignment, or restart), it MUST
rebuild authoritative shard state before serving claims, using the CQRS recovery
contract (ADR-001):

| Step | Normative text |
|------|----------------|
| 1. Resolve + fence epoch | Acquire the shard lease, read the current `assignment_epoch`, and durably fence it into the log (see Single Authoritative Fencing Rule) before any data append. |
| 2. Load snapshot | Read the latest `SnapshotStore` snapshot for the shard (TD-001 `latest_snapshot`); it carries a committed `CommandPosition`. For `postgres_native`, the projection is authoritative in-place and snapshot load is a no-op (TD-002 / see Backend Profile Bindings). |
| 3. Replay tail | Read `LogStore` commands from the snapshot position forward (TD-001 `read_from`) and apply them to the projection (`apply_committed`), bounded by the retention/snapshot window (ADR-001 bounded replay). |
| 4. Materialize leases | Reconstruct active leases and lease-expiry state; expired leases become eligible again (FR-26) and MUST preserve progress-bound age (FR-11). |
| 5. Resume | Begin serving claims under the current epoch. All appends carry `expected_epoch`. |

Recovery MUST be idempotent: replaying already-applied commands MUST NOT
double-mutate (commands carry monotonic per-shard positions, TD-001). A new
epoch never rewinds the log; it only fences who may extend it.

## Cross-Shard Queue-Global Progress

The progress bound is **queue-global** (D1; FR-9/FR-12 unchanged). There is
exactly one `oldest_eligible_age_ms` and one `progress_bound_risk_count` per
queue, regardless of `shard_count`. There is **no** per-group or per-shard
progress invariant in the engine; per-group fairness is a routing concern served
by `DiscoverActiveScopes` (G4), not an engine guarantee.

### Aggregation contract

| Rule | Normative text |
|------|----------------|
| Per-shard input | Each shard maintains its oldest-eligible age in the per-group summary projection (`pqueue_group_summary`, keyed `(tenant_id, queue_id, shard_id, group_key)`, maintained transactionally with item mutations). `oldest_eligible_at` per row is authoritative and exact; eligible *counts* MAY be lagged/approximate (per the projection consistency model). |
| Queue-global oldest-eligible | `metrics.oldest_eligible_age_ms` (API-001) for a multi-shard queue MUST equal `now() - min(oldest_eligible_at)` across all of the queue's shards' summary rows. With group co-residency each group lives on one shard, so each group's row is already the cross-shard minimum for that group; the queue-global value is `max` of per-group ages = `now() - min` of per-group `oldest_eligible_at`. |
| Queue-global risk count | `metrics.progress_bound_risk_count` MUST be the sum across shards of eligible items whose eligible age is near `progress_bound_ms`. This MAY be approximate when documented (API-001 already allows approximate counts); the oldest-eligible age MUST be authoritative. |
| Read cost | The aggregation MUST be served from the maintained summary projection: **one rank-index read per shard (top-of-rank by oldest-eligible) plus a merge**, never a full-table scan of `pqueue_items`. Cost is `O(shard_count)` index probes, not one summary row per shard (a shard has one summary row per active group). |
| Read semantics across shards | Each per-shard read carries the shard's summary `as_of` watermark. The queue-global aggregate's `as_of` MUST be the **minimum** (oldest) `as_of` over the shards read, so callers can reason about staleness. The queue-global `oldest_eligible_age_ms` is "exact as of `min(as_of)`": exactness of the *age* per shard is guaranteed by the transactional summary maintenance, and the aggregate is exact with respect to the state each shard had committed as of its own watermark. A read MUST NOT silently drop a shard; an unreadable/unowned shard MUST be surfaced (see Stalled-shard detection). |
| Progress enforcement (state vs owner) | TD-003 supplies per-shard oldest-eligible state and the global owner-liveness guard. The decision of which shard receives claim capacity to keep the queue-global bound is the TD-001 claim planner's responsibility. TD-003 requires only: (i) each shard's planner, when serving that shard, MUST honor the queue-global `progress_bound_ms` for that shard's items (claim a near-violation item before the bound via the shard's progress-protection window — TD-002 claim shape); and (ii) every shard MUST have a live owner. Because each item lives on exactly one shard, queue-global compliance is the conjunction of per-shard compliance plus the global owner-liveness guard below. |
| Global owner-liveness guard (MUST) | The control plane MUST treat "a shard with eligible work has no live owner for longer than its oldest-eligible item's remaining budget against `progress_bound_ms`" as a queue-global progress-bound risk. The reassignment path (target-owner recompute + acquire) is the mechanism that restores a live owner; TD-003 requires this guard to exist and to be observable (FR-41). It does not specify the claim planner's intra-shard ordering, which is TD-001's. |
| Stalled-shard detection | If a shard has no live owner (lease expired, not yet reacquired) or is unreadable, its eligible items still accrue age. The cross-shard aggregation MUST include unowned/draining shards' oldest-eligible age (from the last committed summary, with its `as_of`) so monitoring (FR-41) and `DiscoverActiveScopes` surface the violation. A shard whose owner is dead for longer than `progress_bound_ms` is a progress-bound violation and MUST be observable. |
| Recurring participation (G5) | A recurring item participates in the single cross-shard queue-global oldest-eligible aggregation exactly like any other item; re-arm only changes when it next becomes eligible, never which shard owns it (co-residency, D2). There is no recurring-specific aggregation. |

### Finding the worst oldest-eligible across shards (claimer / monitor tie-in, G4)

A claimer or monitor that needs the worst oldest-eligible scope across shards
MUST use `DiscoverActiveScopes` (API-001, G4), which returns scopes ranked by
`oldest_eligible_age_ms` descending and aggregates across shards by reading each
shard's per-group summary rank index and merging queue-global by
`(queue_id, group_key)` (min oldest-eligible, summed counts) before top-N (G4
"Discovery Shard Aggregation"). TD-003 adds no new client operation for this; it
constrains the projection so that:

- A group spanning multiple shards is impossible under group co-residency (D2),
  so each `(shard_id, group_key)` row maps to exactly one shard and the
  queue-global merge by `group_key` is a no-op collision-wise.
- For non-group-aware queues, the queue-level descriptor's
  `oldest_eligible_age_ms` MUST be the cross-shard minimum `oldest_eligible_at`
  over the queue's shards (G4 shard-aggregation merge before top-N).
- `DiscoverActiveScopes` results MUST expose `as_of` = `min(as_of)` over the
  shards read (G4), so a caller can reason about summary lag and partial
  convergence.

## API / Interface Design

TD-003 extends the `ControlPlaneStore` capability (TD-001) with shard-ownership
operations. The hot-path `LogStore.append_batch(expected_epoch)` fencing token is
unchanged from TD-001; its acceptance rule is tightened to "equals current
epoch" per the Single Authoritative Fencing Rule. Shapes are normative for
intent, not final syntax.

```rust
pub struct ShardLease {
    pub shard: ShardKey,
    pub active_owner_id: OwnerId,
    pub target_owner_id: OwnerId,
    pub assignment_epoch: u64,
    pub state: ShardLeaseState, // Unassigned | Assigned | Draining
    pub lease_expires_at: Timestamp,
}

#[async_trait]
pub trait ControlPlaneStore { // additions to the TD-001 trait
    async fn register_owner(
        &self,
        owner_id: &OwnerId,
        heartbeat_ttl_ms: u64,
    ) -> Result<(), ControlPlaneError>;

    /// Deterministic target owner for a shard given the live owner set,
    /// plus the current active owner / epoch / state (target vs active may differ).
    async fn resolve_shard_owner(
        &self,
        shard: &ShardKey,
    ) -> Result<ShardOwnerResolution, ControlPlaneError>;

    /// Acquire/reclaim; allocates a strictly greater epoch on ownership change.
    /// Caller MUST durably fence the new epoch into the log before serving claims.
    async fn acquire_shard_lease(
        &self,
        shard: &ShardKey,
        owner_id: &OwnerId,
        lease_ttl_ms: u64,
    ) -> Result<ShardLease, ControlPlaneError>;

    /// Renew without changing the epoch; fails `ShardEpochStale` on mismatch
    /// or when owner_id is not the active owner.
    async fn renew_shard_lease(
        &self,
        shard: &ShardKey,
        owner_id: &OwnerId,
        expected_epoch: u64,
        lease_ttl_ms: u64,
    ) -> Result<ShardLease, ControlPlaneError>;

    /// Set state=draining and record target_owner_id for the current epoch.
    async fn begin_drain(
        &self,
        shard: &ShardKey,
        expected_epoch: u64,
        target_owner_id: &OwnerId,
    ) -> Result<ShardLease, ControlPlaneError>;

    async fn release_shard_lease(
        &self,
        shard: &ShardKey,
        owner_id: &OwnerId,
        expected_epoch: u64,
    ) -> Result<(), ControlPlaneError>;
}
```

| Operation | Maps to |
|-----------|---------|
| `register_owner` / heartbeat | Live owner set for deterministic shard-to-owner assignment. No peer discovery. |
| `resolve_shard_owner` | Deterministic target-owner function (rendezvous hashing) over live owners; returns target + active + epoch + state. |
| `acquire_shard_lease` | Lease acquisition; allocates new epoch; caller then durably fences it. |
| `renew_shard_lease` | Lease renewal; epoch-fenced. |
| `begin_drain` | Graceful Drain step 1; records `target_owner`. |
| `release_shard_lease` | Graceful Drain step 4. |
| (unchanged shape, tightened rule) `LogStore.append_batch(expected_epoch)` | Safety fence; rejects any epoch that is not the current epoch (TD-001/TD-002/TD-004). |

## Data Model Changes

TD-003 defines logical records; backend DDL belongs in TD-002 (Postgres) /
TD-004 (object-log control plane is still Postgres per ADR-001). The
`ShardAssignment` record in TD-001 is extended; a worker registry is added.

```text
ShardAssignment {            // extends TD-001 ShardAssignment
  tenant_id, queue_id, shard_id,
  backend_profile,
  assignment_epoch,          // monotonic per shard (TD-001); durably fenced into the log on acquire
  active_owner_id,           // current lease holder; null when unassigned
  target_owner_id,           // deterministic assignment-function target; may differ during reassignment
  state,                     // unassigned | assigned | draining
  lease_expires_at,          // owner lease deadline
  placement                  // control-plane routing metadata (TD-002)
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
*who allocates it* (the control plane on `acquire_shard_lease`), and *when it
becomes binding on the log* (durably fenced before the new lease is usable, see
Single Authoritative Fencing Rule).

## Security

- **Authorization**: owner registration and lease operations are control-plane
  operations; the service principal MUST be authorized for the deployment's
  control plane. Per-queue tenant authorization (ADR-002) still gates every
  data-plane append.
- **Tenant isolation**: shard leases are keyed by
  `(tenant_id, queue_id, shard_id)`; a worker holding a lease for one tenant's
  shard MUST NOT thereby gain access to another tenant's shards (ADR-002).
- **Threats**:
  - *Zombie writer*: an owner that paused past its lease and resumed — mitigated
    by epoch fencing on `append_batch` (the core safety property), and
    specifically by the Single Authoritative Fencing Rule which fences the old
    epoch at handoff, not at first conflicting data write.
  - *Owner spoofing*: a worker claiming a shard it was not assigned —
    `acquire_shard_lease` rejects when an active lease is held by another owner;
    deterministic assignment plus single-active-lease bound the blast radius.
    Epoch-only credentials are sufficient for trusted internals; an unguessable
    lease incarnation token is a deferred hardening option (see Shard Lease
    Lifecycle note).
  - *Split brain*: two workers both believing they own a shard — permitted
    transiently for liveness, made safe by single-epoch append.

## Performance

- **Hot path unaffected**: appends carry the epoch the owner already holds; no
  per-append control-plane round trip. Ownership is cached and refreshed on
  renew interval. The durable epoch fence happens once per acquire, not per
  append.
- **Control-plane rate**: lease renew is O(owned shards / renew interval), a
  low-rate background load on Postgres (ADR-001 "low-rate control plane").
- **Cross-shard progress read**: one rank-index probe per shard plus a merge
  (O(`shard_count`) probes) per metrics/discovery call, served from the
  maintained per-group summary index, not item scans.
- **Reassignment cost**: recovery time per shard is bounded by snapshot +
  log-tail replay (ADR-001 bounded replay; TD-002 in-place projection makes this
  near-zero for `postgres_native`).

### Queue density: many shards/queues owned per node (>=1000 active queues)

The PRD queue-density target (>=1000 concurrently active queues per node, ideally
a single node) means one node owns the shard leases for many queues at once
(>=1000 queues x `shard_count`). Ownership and its background work MUST therefore
be bounded per node, not per shard:

- **Lease renewal is batched per node.** A node renews all of its owned shard
  leases in bounded batched control-plane writes on the renew interval (one
  multi-row update / small number of statements), NOT one renew task or
  connection per shard. Control-plane rate stays O(owned shards / renew interval)
  with a small constant, and the durable-epoch fence is still once per acquire.
- **One assignment poll per node.** A node learns its assignment set from the
  control plane in a single bounded query, not per-queue subscriptions.
- **Cross-shard progress aggregation is shared and bounded.** The aggregation /
  oldest-eligible monitor (and the G4 discovery read path) runs as a bounded
  shared per-node job over the maintained per-group summary index, with work
  proportional to active shards scanned per pass and a bounded cadence — never
  one monitor loop per queue or per shard.
- **Owned-shard state is bounded.** Per-shard in-memory ownership state is small
  and capped; per-shard projection handles (e.g. SQLite databases under TD-004)
  are opened lazily and bounded by an LRU cap rather than held open per owned
  shard indefinitely.

Aggregate single-node throughput remains bounded by the node; density requires
that the 1000th owned active queue costs only bounded incremental
ownership/background resource and still meets its progress bound. Validated by
`queue_density_single_node_tests` (TP-002 E2).

## Backend Profile Bindings

| Profile | Shard lease store | Append fence | Recovery |
|---------|-------------------|--------------|----------|
| `postgres_native` (TD-002) | Postgres `pqueue_shards` row, transactional acquire/renew | `assignment_epoch` updated on the shard row in the acquire transaction; the data-plane append transaction validates `expected_epoch == current assignment_epoch` (TD-002 stale-epoch reject). The acquire transaction IS the durable fence. | Projection is in-place authoritative; recovery = read current epoch (no replay needed beyond crash recovery of the DB). |
| `object_log_sqlite_projection` (TD-004, D4b) | Postgres `ControlPlaneStore` (ADR-001 keeps control plane in Postgres for all profiles) | On acquire, the new owner MUST commit an epoch-fence manifest entry advancing the manifest's recorded current epoch to `E+1` via CAS BEFORE any data segment; thereafter manifest commit MUST reject any `expected_epoch` not equal to the manifest's recorded current epoch. | Recovery = latest SQLite snapshot from object storage + replay sealed segments after the snapshot position (ADR-001 S3/Object-Log section). |

Both committed v1 profiles MUST pass the TD-003 conformance scenarios (see
Testing).

## Testing

TD-003 is not satisfied until these scenarios pass for every backend profile
that claims multi-shard support.

| Scenario | Required evidence |
|----------|-------------------|
| Stale-epoch reject | A writer holding epoch E appends after the shard is reassigned to epoch E+1; the append MUST fail (`shard-epoch-stale`) and MUST NOT mutate state. |
| Stale writer after epoch advance, before new data segment | Epoch E+1 is acquired and the epoch fence is committed, but NO E+1 data segment exists yet. An epoch-E writer's `append_batch`/manifest commit MUST be rejected immediately (it is not the current epoch), proving the fence binds at handoff, not at first conflicting data write. |
| Single writer under contention | Two owners transiently believe they own a shard; only the current-epoch holder's appends commit; no duplicate/lost commands. |
| Reassignment recovery | Kill the owner; after lease expiry a new owner acquires a greater epoch, durably fences it, recovers from snapshot + log tail, and reproduces shard state exactly. |
| Target vs active owner during rolling deploy | A new `target_owner` is selected while the previous `active_owner` lease is still live; `resolve_shard_owner` reports both; the target does not acquire until drain/expiry; no double-writer window. |
| Graceful drain | `begin_drain` stops new claims, lets in-flight leases finalize within `drain_deadline_ms`, releases the lease; the new owner resumes with no orphaned leases and no progress-bound violation. |
| Interrupted drain | Draining owner dies mid-drain; lease expiry + epoch fence still yield single-writer safety; unfinalized leases redeliver (at-least-once, FR-28). |
| Cross-shard progress | A multi-shard queue with skewed load reports queue-global `oldest_eligible_age_ms` = max over shards (= `now() - min(oldest_eligible_at)`); an item near violation on any shard is claimed before `progress_bound_ms`. |
| Cross-shard as-of | A read while one shard's summary lags reports the aggregate `as_of` = min over shards and does not drop the lagging shard. |
| Stalled-shard visibility | A shard left unowned past `progress_bound_ms` is surfaced as a progress-bound violation in metrics and `DiscoverActiveScopes`. |
| Group co-residency invariance | All items of one `group_key` resolve to one shard; `whole_group`/`whole_cohort` claims (G1/G6) are shard-local and atomic. |
| Rebalance does not split a live cohort | Reassign a shard holding a live cohort's `group_key`; the whole cohort moves to the new owner as a unit; no member is split across shards (G6). |
| Reshard immutability (v1) | Attempting to change `shard_count` on an existing queue is rejected pending the migration contract. |
| Queue density (>=1000 active queues/node) | A single node owns the shards for >=1000 concurrently active queues; lease renewal stays O(owned shards / interval) via batched per-node writes (not per-shard tasks/connections), background sweeps/aggregation run as bounded shared per-node jobs, every active queue meets its progress bound, any one queue can reach the per-queue floor, and there is no cross-queue degradation as the active-queue count grows to 1000 (`queue_density_single_node_tests`, TP-002 E2). |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Stale writer after epoch advance but before new data segment | M | H | Single Authoritative Fencing Rule: epoch durably fenced into the log at acquire; append rejects any non-current epoch; dedicated conformance row. |
| Control-plane unavailability blocks ownership changes | M | M | Existing owners keep serving under live leases; new acquisitions fail closed (TD-001 control-plane fallback); appends never proceed on a stale epoch. |
| Lease TTL too low causes ownership churn | M | M | TTL >> renew interval and GC pause; safety is epoch-based so churn only costs recovery, not correctness. |
| Slow/indefinite drain hides a progress-bound violation | M | H | Aggregation counts draining shards; `drain_deadline_ms < progress_bound_ms`; stalled-shard conformance test; global owner-liveness guard. |
| Resharding misperceived as online in v1 | M | M | `shard_count` immutable in v1; resharding explicitly deferred to a migration contract and the scale-substantiation evidence (TP-002). |
| Multi-shard misread as the unbounded scale claim | M | H | Magnitude beyond one DB stays evidence-gated (TP-002, E0-E3); TD-003 commits the *mechanism* (fixed-N placement) that preserves the per-queue floor (E0) for every queue at any scale, not unbounded items/hr. |

## Review Checklist

- [x] No external coordinator / consensus (ADR-001, concerns.md override).
- [x] Storage-backed leases owned by Postgres `ControlPlaneStore`.
- [x] Single authoritative fencing rule: epoch durably fenced before new lease
      usable; append rejects any non-current epoch (no stale-writer window).
- [x] Deterministic item-to-shard (ADR-004 co-residency) and shard-to-owner
      assignment; target vs active owner defined.
- [x] `shard_count` is a client `CreateQueue` field (policy-bounded), `> 1`
      requests the D4 v1 multi-shard path.
- [x] Rebalance distinguishes online reassignment from gated resharding;
      reassignment must not split a live cohort/group.
- [x] Graceful drain bounded below `progress_bound_ms`.
- [x] Recovery from snapshot + log tail, idempotent replay.
- [x] Cross-shard progress is queue-global (D1); no per-group/per-shard
      invariant; enforcement owner (TD-001 planner) + global liveness guard
      (TD-003) named.
- [x] Oldest-eligible aggregation served from the unified per-group summary
      keyed `(tenant, queue, shard, group_key)`, one rank-index probe per shard +
      merge, `as_of` = min over shards, surfaced via `DiscoverActiveScopes` (G4).
- [x] Recurring items participate in cross-shard aggregation with no special
      handling (G5).
- [x] Conformance scenarios cover stale-epoch reject, post-advance/pre-segment
      fence, reassignment, drain, target-vs-active, cross-shard progress, as-of,
      stalled shard, cohort-not-split.
