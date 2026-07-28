---
ddx:
  id: td-durability-reclaim-and-durable-state
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-queue-as-shard-unit-and-projection-families
    - adr-log-single-source-of-truth
    - orthogonal-storage-matrix-brief
    - td-storage-architecture-backend-contracts
    - api-native-client-interface
    - api-operator-repair-contract
  status: draft
  review:
    self_hash: 120af49601b28d4388b5b394d729eb75d23c8c6a93d92104a2ff06c9a405c1b4
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-hexagonal-architecture-and-two-interfaces: 02e04b32110f57e05ea80a7b6ce642cba655866e14302db6a8b0d1de0f62d012
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T00:01:28Z"
---

# Technical Design

**TD ID**: TD-007
**Title**: Durability classes, ReclaimDriver, UpsertPort, and durable engine state
**Status**: draft
**Related**: ADR-007, ADR-013, TD-001 (backend contracts), TD-006 (RESP surface),
`orthogonal-storage-matrix-brief`,
`docs/helix/04-build/hexagonal-migration-plan.md` (§2)

## Purpose

Specify the engine-side contracts the hexagonal cutover depends on, so backends and the RESP/library
adapters can be built against fixed semantics:

1. the **persistence durability classes (Class A / Class B)** and the **internal append/apply
   classes (atomic / log-then-apply)** that implement them across the 5×3 matrix;
2. the **unit-of-work / claim / upsert** atomicity model (`Backend`, `ClaimPort`, `UpsertPort`);
3. the **ReclaimDriver** (timed lifecycle transitions);
4. the **durable-state** design for logic migrated off the in-memory HTTP service
   (idempotency, lease fences, queue pause, operator-operation store, `command_position`);
5. **`client_item_key` uniqueness** routing.

The queue is the unit of sharding (ADR-008): a whole queue is owned by exactly one node, so all engine
state below is **per-`(tenant, queue)`** on that single owner. Horizontal scale is cross-queue
(distributing queues across owners), never intra-queue sharding.

Public storage remains axes, not profile SKUs: log ∈
{`memory`, `sqlite`, `postgres`, `filesystem`, `s3`} × projection ∈
{`memory`, `sqlite`, `postgres`} (TD-001, matrix brief). Hybrid and turso are not public
projection axis values.

## 1. Durability classes

Two orthogonal notions of “class” apply. Do not collapse them.

### 1.1 Persistence durability class (ADR-013 / matrix brief) — client contract

Every matrix cell remains `LogStore × ProjectionStore` with append → apply → acknowledge.
**Persistence guarantees** differ by log axis:

| Class | Logs | Authority after restart | Client contract |
|---|---|---|---|
| **A — Durable log** | `sqlite`, `postgres`, `filesystem`, `s3` | Log is system of record; projection is rebuildable cache | Success ⇒ durable on log and visible in serving projection; recovery via high-water + tail replay; `request_id` resolves ambiguity across crash; branch / read-as-of / change-record-from-log require Class A |
| **B — Memory log** | `memory` | In-process log for ordering while alive; **after process death only projection remains** | Success ⇒ visible in projection; durable **iff** projection is durable (`sqlite` / `postgres`); no log rebuild, branch, read-as-of, or change-record-from-log |

Class B is a weaker **persistence envelope**, not a second architecture and not “no LogStore.”
The memory log remains a real `LogStore` for in-process ordering and fencing; it is simply not a
durable system of record across process death. Class B MUST be explicitly selectable
(`log=memory` via `StorageConfig` / composition axes). Silent null-log / absent-log composition is
forbidden. Docs and conformance MUST NOT claim Class A guarantees for Class B cells.

Matrix reminder (same as TD-001 / brief):

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|---|---|---|---|
| `memory` | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A |

### 1.2 Append/apply class — engine mechanics

Every driven adapter also declares an **append/apply** class. This describes internal commit
mechanics only. It does not replace Class A / Class B and does not create a second external API
beyond the persistence envelope of the selected log.

API-001 still requires success-visible and rejection-no-effect semantics for every selectable cell.
Unknown-outcome `request_id` replay is required on Class A; on Class B it holds for the live
process and for projection-retained outcomes when the projection is durable.

| Append/apply class | Typical compositions | Append+apply | Guarantee the engine may assume |
|---|---|---|---|
| **Atomic** | unified txn cells such as `sqlite`×`sqlite`, `postgres`×`postgres`; in-process `memory` lock compositions | committed together | post-commit projection is globally consistent with the log append for that unit of work; Invariant 1 & 2 hold strictly for the live process |
| **Log-then-apply** | object-log peers `filesystem` / `s3` (any public projection) | manifest (or durable log) commit makes acknowledgement eligible; projection/response barrier completes before success returns | the engine must route every public success through the operation's own visibility barrier; unrelated concurrent effects may have bounded apply lag, but no caller-observable success may be invisible or duplicated |

Rationale: object-log mode (TD-004) cannot wrap a `filesystem`/`s3` manifest commit and a local
projection apply in one transaction. The engine therefore branches on the declared append/apply
class to implement the correct response barrier and failure handling. A backend MUST NOT declare
`atomic` unless append and apply commit in one transaction observable to the claim path.

**Class A + log-then-apply** (`filesystem`/`s3` logs): ADR-013 Class A rules stand — durable log
is SoT; response barrier; crash/`request_id` recovery from log.

**Class A + atomic** (e.g. Postgres or SQLite unified txn): same Class A client contract inside one
transaction.

**Class B + any append/apply class** (`memory` log): in-process ordering and fencing; projection
visibility barrier for the live process; cross-restart durability only if the projection is
durable; no Class A log-rebuild claims.

## 2. Unit of work, claim, upsert

**2.1 `Backend::write`.** The atomic seam is a closure unit of work:
`write(|log: &mut dyn LogWriter, proj: &mut dyn ProjectionWriter| -> Result<R>) -> Result<R>`.
On atomic backends the closure body's log append and projection apply commit together (one lock / one
SQL txn). On log-then-apply backends `write` does not return public success until the command is
durable and the operation's own accepted effects are visible or reconstructable from committed state.

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
- If the colliding item is **claimed (leased, non-terminal)**: reject with `-ERR fireweed invalid`
  (lifecycle transition not allowed on in-flight work). If the colliding item is **terminal**: reject
  with `-ERR fireweed terminal`. Never desync a PEL entry. (Mirrored verbatim in TD-006 §3 `XADD`.)
- On **log-then-apply** backends whose serving projection may lag the validation projection:
  colliding-key replacement is unavailable and the engine returns `-ERR fireweed unavailable` for a
  colliding-key `XADD`.
- On object-log Class A cells (`filesystem` / `s3` log) that apply a durable
  projection image with a synchronously current serving view, colliding-key `XADD`,
  `update_fields`, and `reschedule` MAY be admitted only after TD-004 proves the
  deterministic apply-time re-validation + ack-after-apply closure. Pre-commit
  validation still reads the hot serving projection, but the definitive acceptance
  check runs again in the same committed apply transaction that makes success
  visible; a command that loses the claim race fails closed and replay re-derives
  the same rejection. Hybrid apply strategies are not public projection axis values.
- (Absent `client_item_key` ⇒ plain append on all backends.)
- A later `XACK`/`XCLAIM` of a **superseded** old id returns `-ERR fireweed superseded` — never a silent
  `nil` (preserves at-least-once "no silent drop"; TD-006 §3).

## 3. ReclaimDriver

Redis evaluates lease idle-time lazily inside `XCLAIM`/`XAUTOCLAIM`, so a quiet stream needs no timer.
fireweed models several lifecycle transitions as commands that **something must fire**; without a driver,
an item on a queue with no client traffic never transitions and orphans. The `ReclaimDriver` fires:

| Transition | Trigger | Command |
|---|---|---|
| Lease expiry → reclaim | `now > lease_expires_at` | `LeaseExpired` (item → pending, attempt charged) |
| Cohort timeout | `now > cohort_deadline` = `min(cohort_created_at, first_eligible_at) + completion_bound_ms` (API-001 cohort-expiry rule; `first_eligible_at` = when the first member became claim-eligible) | `CohortExpired` (members → terminal `failed`) |
| Delay / recurrence promotion | `now > not_before` (incl. re-armed recurring items) | promote to eligible |
| Progress-bound metering | eligible age > `progress_bound_ms` | **launch = meter-only:** emit a `progress_bound_breach` metric/event; **no lifecycle transition** (D2 resolved — escalation/auto-action is post-launch) |

**Placement (hexagonal).** The transition *logic* is domain (`fireweed-engine`); the *clock/driving* is
the composition root's: `fireweed-server` (and an async library embedding) spawns a periodic task; a
**synchronous** library embedding drives it via an explicit `engine.tick(now) -> TickReport` entry
point. `tick(now)` is **idempotent** (re-running with the same/earlier `now` makes no further
transitions) and **serializes against claim** (a reclaim and a concurrent claim of the same item
mutually exclude via the same unit of work). The driver processes due transitions in bounded batches.

DoD: an item is reclaimed/expired with **zero** intervening client commands on its queue.

## 4. Durable engine state (migrated off in-memory service `Mutex`)

The HTTP service held these in `Arc<Mutex<QueueAdminState>>`. The engine makes each **command-backed**:
written as a command, materialized in the projection, and — under **Class A** — reconstructable by
log replay. Under **Class B** (`memory` log), state is reconstructable only while the process (and,
if durable, the projection) retain it; there is no durable log rebuild. The queue is the
unit of sharding (ADR-008), so all keys below are per-`(tenant, queue)` on the queue's single owner.

| State | Command(s) | Projection representation | Retention / compaction | Replay reconstruction |
|---|---|---|---|---|
| **Idempotency cache** (`request_id`→outcome; operator replay→409 fingerprint) | stamped on each mutating command | `request_id → {fingerprint, outcome, expires_at}` | bounded by `request_id_retention_ms`; compact expired on apply | replay re-derives from retained window |
| **Lease fences** | `FenceLease`/`UnfenceLease` | per-item `lease_generation`; stale gen ⇒ `XACK`→`stale_lease` | `UnfenceLease` + compaction once item terminal | replay rebuilds current generation |
| **Queue pause** | `PauseQueue`/`ResumeQueue` | `queue_admin_paused` flag | latest wins; no growth | last command wins |
| **Operator-operation store** (API-002 async ops) — **library/operator-only; built in Phase 2 (§4a migration), NOT Phase 1** | `OpStarted`/`OpProgress`/`OpFinished`/`OpCanceled` | `operation_id → {state ∈ {accepted,running,succeeded,partial,failed,canceled}, progress{matched,affected,failed,batches_total,batches_complete,updated_at}, errors[]}` **and** the `request_id → operation_id` idempotency anchor (replay of same `request_id` returns same `operation_id`; different body ⇒ `request-id-conflict`) | bounded retention after terminal | replay rebuilds the full API-002 async shape (full normative schema = API-002 §Asynchronous Operation Model; a large selector runs in bounded batches on the queue's single owner; RESP never touches this store) |
| **`command_position`** (item_version source) | every committed command advances it | monotonic per queue | none (counter) | **high-water mark persisted in the projection/SnapshotStore, NOT recomputed by counting a possibly-compacted log** — so replay after retention/compaction is monotonic and `item_version` never regresses |

Each Class A cell is covered by a **replay-reconstruction conformance test**: build state via
commands, drop the projection, replay the log, assert identical state. Class B cells instead prove
projection-only reopen (when the projection is durable) and MUST NOT claim log-rebuild parity.
Idempotency and lease fences are per-`(tenant, queue)` on the queue's single owner; there is no
cross-shard fence to coordinate (ADR-008).

## 5. `client_item_key` uniqueness

Uniqueness is enforced per `(tenant, queue)` and indexed in the projection so `UpsertPort` and dedup
are O(1) lookups in the claim unit of work. Because the queue is owned by exactly one node (ADR-008),
the index is owner-local and the check is trivially atomic with claim/upsert — there is no
key→shard routing to keep it local, and no multi-shard uniqueness concern.

## 6. Conformance requirements (durability DoD)

- **Reclaim-no-traffic:** an item is reclaimed/expired with zero client commands on its queue (§3).
- **Upsert↔claim exclusion:** concurrent `replace_if_pending` and claim on one item never both
  succeed; superseded-id `XACK` returns `-ERR fireweed superseded`.
- **Persistence class guarantees (Class A / Class B):** every matrix cell exposes the inherent
  API-001 surface permitted for its durability class. Class A cells prove durable-log SoT,
  high-water + tail recovery, and `request_id` crash ambiguity resolution. Class B (`memory`
  log) cells prove projection visibility for the live process, projection-only reopen when the
  projection is durable, and **no** false log-rebuild / branch / read-as-of claims.
- **Append/apply class guarantees:** atomic and log-then-apply compositions satisfy the external
  transaction contract for their persistence class. Log-then-apply additionally proves its response
  barrier, crash-point matrix, `request_id` replay behavior (Class A), and the
  replacement/update/reschedule race-closure scenarios in TD-004. Storage selection never turns a
  supported operation into `Unavailable` solely because of axis choice within a supported cell.
- **Mutable-write race closure:** every log-then-apply composition that admits mutable writes MUST
  prove that `replace_if_pending`, `update_fields`, and `reschedule` against a pending item can race
  a concurrent claim under group commit without both succeeding; the closure mechanism is
  deterministic apply-time re-validation with ack-after-apply, so the winner's effect is visible on
  the response path and the loser fails closed.
- **Durable-state replay (Class A):** each §4 row reconstructs identically after projection drop +
  log replay. **Class B:** projection-only reopen tests where applicable; no log-replay requirement.
- **No-stub behavioral:** every port method (`ClaimPort`, `UpsertPort`, `Backend::write`,
  `ReclaimDriver`/`tick`) has a test that fails if the impl returns a default/no-op.

## 7. Open decisions
- **D1.** `tick` batch size / fairness across queues under a single driver — tune in Phase 1.
- **D2.** ~~progress_bound escalates vs meters~~ — **RESOLVED: meter-only at launch** (emit
  `progress_bound_breach`, no lifecycle transition); auto-escalation is post-launch (§3).
- **D3.** Idempotency-cache compaction cadence (apply-time vs periodic) — measure in Phase 3.
