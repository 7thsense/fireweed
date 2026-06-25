---
ddx:
  id: td-durability-reclaim-and-durable-state
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-queue-as-shard-unit-and-projection-families
    - td-storage-architecture-backend-contracts
    - api-native-client-interface
    - api-operator-repair-contract
  status: draft
---

# Technical Design

**TD ID**: TD-007
**Title**: Durability classes, ReclaimDriver, UpsertPort, and durable engine state
**Status**: draft
**Related**: ADR-007, TD-001 (backend contracts), TD-006 (RESP surface),
`docs/helix/04-build/hexagonal-migration-plan.md` (§2)

## Purpose

Specify the engine-side contracts the hexagonal cutover depends on, so backends and the RESP/library
adapters can be built against fixed semantics:

1. the **two durability classes** and which guarantees the engine may rely on;
2. the **unit-of-work / claim / upsert** atomicity model (`Backend`, `ClaimPort`, `UpsertPort`);
3. the **ReclaimDriver** (timed lifecycle transitions);
4. the **durable-state** design for logic migrated off the in-memory HTTP service
   (idempotency, lease fences, queue pause, operator-operation store, `command_position`);
5. **`client_item_key` uniqueness** routing.

The queue is the unit of sharding (ADR-008): a whole queue is owned by exactly one node, so all engine
state below is **per-`(tenant, queue)`** on that single owner. Horizontal scale is cross-queue
(distributing queues across owners), never intra-queue sharding.

## 1. Durability classes

Every driven adapter declares one class. The engine relies only on the **weakest** guarantee a given
backend declares.

| Class | Backends | Append+apply | Guarantee the engine may assume |
|---|---|---|---|
| **Atomic** | `memory` (lock), `sqlite`, `postgres` (one txn) | committed together | post-commit projection is globally consistent; Invariant 1 & 2 hold strictly |
| **Eventual-apply** | `objectlog` | log commit acks; projection applies within a bounded window | **self-read-after-write only** (an operation observes its own committed effect); priority order and no-double-claim are "over applied state, eventual"; **upsert unavailable** |

Rationale: object-log mode (TD-004) cannot wrap an S3 manifest commit and a SQLite projection apply
in one transaction. Rather than pretend a single seam unifies all backends, the engine branches on
the declared class. A backend MUST NOT declare `atomic` unless append and apply commit in one
transaction observable to the claim path.

## 2. Unit of work, claim, upsert

**2.1 `Backend::write`.** The atomic seam is a closure unit of work:
`write(|log: &mut dyn LogWriter, proj: &mut dyn ProjectionWriter| -> Result<R>) -> Result<R>`.
On atomic backends the closure body's log append and projection apply commit together (one lock / one
SQL txn). On eventual-apply backends `write` provides self-RAW: the closure's own appended commands
are visible to its own subsequent reads, but global consistency is not promised after return.

**2.2 `ClaimPort`.** The engine is the single *logical* claim authority, but a backend MAY implement
claim atomically (postgres `FOR UPDATE SKIP LOCKED` CTE) behind `ClaimPort::claim(req) -> Claimed`.
The port contract: a claimed item transitions to `leased` and enters the consumer's PEL **in the same
unit of work** as candidate selection — no item is selected-but-not-leased.

**2.3 `UpsertPort::replace_if_pending`.** Implements RESP `XADD`-with-colliding-`client_item_key`
(TD-006 §3 / Invariant 2):
- Executes **in the same unit of work / under the same item lock as claim**, so upsert and claim on
  one item **mutually exclude** (no TOCTOU). On atomic backends one serializes before the other.
- If the colliding item is **pending**: atomically supersede the old entry id and append the new one;
  return the new monotonic id. The old id thereafter reads as deleted (`XRANGE`→nil); `XLEN` nets
  unchanged.
- If the colliding item is **claimed (leased, non-terminal)**: reject with `-ERR pqueue invalid`
  (lifecycle transition not allowed on in-flight work). If the colliding item is **terminal**: reject
  with `-ERR pqueue terminal`. Never desync a PEL entry. (Mirrored verbatim in TD-006 §3 `XADD`.)
- On **eventual-apply** backends: **not implemented** — the engine returns `-ERR pqueue unavailable`
  for a colliding-key `XADD`, because the claim reads a lagging projection and the upsert↔claim race
  cannot be closed. (Absent `client_item_key` ⇒ plain append on all backends.)
- A later `XACK`/`XCLAIM` of a **superseded** old id returns `-ERR pqueue superseded` — never a silent
  `nil` (preserves at-least-once "no silent drop"; TD-006 §3).

## 3. ReclaimDriver

Redis evaluates lease idle-time lazily inside `XCLAIM`/`XAUTOCLAIM`, so a quiet stream needs no timer.
pqueue models several lifecycle transitions as commands that **something must fire**; without a driver,
an item on a queue with no client traffic never transitions and orphans. The `ReclaimDriver` fires:

| Transition | Trigger | Command |
|---|---|---|
| Lease expiry → reclaim | `now > lease_expires_at` | `LeaseExpired` (item → pending, attempt charged) |
| Cohort timeout | `now > cohort_deadline` = `min(cohort_created_at, first_eligible_at) + completion_bound_ms` (API-001 cohort-expiry rule; `first_eligible_at` = when the first member became claim-eligible) | `CohortExpired` (members → terminal `failed`) |
| Delay / recurrence promotion | `now > not_before` (incl. re-armed recurring items) | promote to eligible |
| Progress-bound metering | eligible age > `progress_bound_ms` | **launch = meter-only:** emit a `progress_bound_breach` metric/event; **no lifecycle transition** (D2 resolved — escalation/auto-action is post-launch) |

**Placement (hexagonal).** The transition *logic* is domain (`pqueue-engine`); the *clock/driving* is
the composition root's: `pqueue-server` (and an async library embedding) spawns a periodic task; a
**synchronous** library embedding drives it via an explicit `engine.tick(now) -> TickReport` entry
point. `tick(now)` is **idempotent** (re-running with the same/earlier `now` makes no further
transitions) and **serializes against claim** (a reclaim and a concurrent claim of the same item
mutually exclude via the same unit of work). The driver processes due transitions in bounded batches.

DoD: an item is reclaimed/expired with **zero** intervening client commands on its queue.

## 4. Durable engine state (migrated off in-memory service `Mutex`)

The HTTP service held these in `Arc<Mutex<QueueAdminState>>`. The engine makes each **log-backed**:
written as a command, materialized in the projection, and reconstructable by replay. The queue is the
unit of sharding (ADR-008), so all keys below are per-`(tenant, queue)` on the queue's single owner.

| State | Command(s) | Projection representation | Retention / compaction | Replay reconstruction |
|---|---|---|---|---|
| **Idempotency cache** (`request_id`→outcome; operator replay→409 fingerprint) | stamped on each mutating command | `request_id → {fingerprint, outcome, expires_at}` | bounded by `request_id_retention_ms`; compact expired on apply | replay re-derives from retained window |
| **Lease fences** | `FenceLease`/`UnfenceLease` | per-item `lease_generation`; stale gen ⇒ `XACK`→`stale_lease` | `UnfenceLease` + compaction once item terminal | replay rebuilds current generation |
| **Queue pause** | `PauseQueue`/`ResumeQueue` | `queue_admin_paused` flag | latest wins; no growth | last command wins |
| **Operator-operation store** (API-002 async ops) — **library/operator-only; built in Phase 2 (§4a migration), NOT Phase 1** | `OpStarted`/`OpProgress`/`OpFinished`/`OpCanceled` | `operation_id → {state ∈ {accepted,running,succeeded,partial,failed,canceled}, progress{matched,affected,failed,batches_total,batches_complete,updated_at}, errors[]}` **and** the `request_id → operation_id` idempotency anchor (replay of same `request_id` returns same `operation_id`; different body ⇒ `request-id-conflict`) | bounded retention after terminal | replay rebuilds the full API-002 async shape (full normative schema = API-002 §Asynchronous Operation Model; a large selector runs in bounded batches on the queue's single owner; RESP never touches this store) |
| **`command_position`** (item_version source) | every committed command advances it | monotonic per queue | none (counter) | **high-water mark persisted in the projection/SnapshotStore, NOT recomputed by counting a possibly-compacted log** — so replay after retention/compaction is monotonic and `item_version` never regresses |

Each is covered by a **replay-reconstruction conformance test**: build state via commands, drop the
projection, replay the log, assert identical state. Idempotency and lease fences are per-`(tenant,
queue)` on the queue's single owner; there is no cross-shard fence to coordinate (ADR-008).

## 5. `client_item_key` uniqueness

Uniqueness is enforced per `(tenant, queue)` and indexed in the projection so `UpsertPort` and dedup
are O(1) lookups in the claim unit of work. Because the queue is owned by exactly one node (ADR-008),
the index is owner-local and the check is trivially atomic with claim/upsert — there is no
key→shard routing to keep it local, and no multi-shard uniqueness concern.

## 6. Conformance requirements (durability DoD)

- **Reclaim-no-traffic:** an item is reclaimed/expired with zero client commands on its queue (§3).
- **Upsert↔claim exclusion:** concurrent `replace_if_pending` and claim on one item never both
  succeed; superseded-id `XACK` returns `-ERR pqueue superseded`.
- **Class guarantees:** atomic backends assert strict Invariant 1 & 2; eventual-apply asserts the
  weaker "over applied state, eventual" and returns `-ERR pqueue unavailable` for colliding-key `XADD`.
- **Durable-state replay:** each §4 row reconstructs identically after projection drop + log replay.
- **No-stub behavioral:** every port method (`ClaimPort`, `UpsertPort`, `Backend::write`,
  `ReclaimDriver`/`tick`) has a test that fails if the impl returns a default/no-op.

## 7. Open decisions
- **D1.** `tick` batch size / fairness across queues under a single driver — tune in Phase 1.
- **D2.** ~~progress_bound escalates vs meters~~ — **RESOLVED: meter-only at launch** (emit
  `progress_bound_breach`, no lifecycle transition); auto-escalation is post-launch (§3).
- **D3.** Idempotency-cache compaction cadence (apply-time vs periodic) — measure in Phase 3.
