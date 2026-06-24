---
ddx:
  id: td-resp-wire-adapter
  depends_on:
    - api-native-client-interface
    - api-operator-repair-contract
    - adr-cqrs-log-projection-storage-model
    - adr-granularity-mapping-and-claim-domain
  status: draft
  review:
    self_hash: ca22dc211e4bc9226ba212fee6e03c57589371ec73499d86f540b1ea65395b6f
    deps:
      adr-cqrs-log-projection-storage-model: 709f701130b5bd00666a1abeef4fb104555a623d39b9fec1fdb9b3167789de10
      adr-granularity-mapping-and-claim-domain: ba2d4c26c9fcaa4470ea65b61eff20cf382b6bba9e261cbd453f13122bfbc7c8
      api-native-client-interface: 6b76e5c4c37c91d40e8d5229d9eeae516f71385aa06e856fb41a4a19ee5856e8
      api-operator-repair-contract: 65ec2e36500a6c404ae53af1a65da26fcdcc0a07e0ef1578bae30ec94f2be6e6
    reviewed_at: "2026-06-23T22:06:42Z"
---

# Technical Design

**TD ID**: TD-006
**Title**: RESP wire adapter — stock Redis Streams worker surface plus Rust library control surface
**Status**: draft (v3, refolded against hexagonal migration plan v4)
**Related**: API-001, API-002, ADR-007, TD-007, `docs/helix/04-build/hexagonal-migration-plan.md`

## Purpose

Define the launch RESP surface for pqueue after the hexagonal cutover.

The launch decision is intentionally narrower than earlier drafts:

- RESP is a pqueue-native server that speaks a **stock Redis Streams-compatible worker subset**.
- The Rust library is the full-power interface for pqueue-specific control, inspection, filtered claim,
  gates, cohorts, rich finalize dispositions, and operator repair.
- No `PQ*` command vocabulary is required for launch. Any custom RESP command, including a possible
  `PQFIN`, is post-launch and must be justified by new evidence.

This TD exists to prevent accidental half-interfaces: every API-001/API-002 operation is classified
as `RESP-stock`, `library-only-intentional`, or `n/a`, and the stock Redis-compatible behavior is
specified enough for conformance tests.

## 1. Implementation Model

pqueue is a **native server that speaks RESP**, not Redis and not a Redis module.

Consequences:

- pqueue owns command semantics for the subset it implements.
- pqueue owns authentication and authorization for RESP connections.
- RESP compatibility means stock Redis Streams clients can run the unfiltered worker hot path without
  custom commands.
- Redis clients must treat returned batches as opaque work sets; pqueue delivery order is priority
  order, not stream-id order.

## 2. Container Entry Contract

RESP entries are flat field/value pairs. pqueue reserves fields used by API-001 and returns additional
reserved fields in claim replies. Non-reserved fields are opaque payload.

| Reserved field | Direction | Meaning |
|---|---|---|
| `client_item_key` | request | Caller-provided logical key for pending-item replacement and audit. |
| `priority` | request | Ordering key interpreted by the queue's priority model. |
| `group_key` | request | Optional co-residency and per-group ordering key. |
| `cohort_id` | request | Optional cohort identity. Whole-cohort semantics are library-only. |
| `not_before` | request | Earliest eligibility timestamp. |
| `max_attempts` | request | Retry bound override. |
| `gate_keys` | request | JSON array of dynamic gate keys. Gate mutation is library-only. |
| `metadata` | request | JSON object for predicates and audit. |
| `payload` | request | Opaque application payload. |
| `item_version` | reply | Server-assigned monotonic item version. |
| `lease_expires_at` | reply | Server-computed lease expiry timestamp. |
| `attempt_count` | reply | Delivery count (claims handed to a worker); a timed reclaim does not charge. |

The RESP entry id is the wire `item_id`. `client_item_key` is not the entry id; it is the caller's
logical replacement/idempotency key.

## 3. RESP Stock Command Surface

### `XADD`

Adds a pending item.

Rules:

- If `client_item_key` is absent, the call always appends.
- If `client_item_key` collides with a pending item on an atomic backend, pqueue performs atomic
  pending-item replacement: old id is superseded, a new monotonic id is returned, and `XLEN` nets
  unchanged.
- If the key collides with **claimed (leased, non-terminal)** work, the call returns
  `-ERR pqueue invalid` (no lifecycle transition on in-flight work). If it collides with **terminal**
  work, the call returns `-ERR pqueue terminal`. (Mapping pinned in TD-007 §2.3.)
- On eventual-apply backends, replacement is unavailable and returns `-ERR pqueue unavailable`.

### `XREADGROUP ... STREAMS <queue> >`

Claims eligible, previously undelivered work for a consumer group.

Rules:

- Delivery is priority-ordered over the backend's declared consistency class.
- Claim tracking is per item. There is no single `last-delivered-id` cursor that determines future
  eligibility.
- Returned entries include pqueue reserved reply fields. Stock clients ignore fields they do not use.
- Filtered claim, whole-cohort claim, explicit gate fences, and request-id claim replay are library-only.

### `XACK`

Completes claimed work.

Rules:

- `XACK` succeeds only for work still owned by the caller's group/consumer lease generation.
- If an operator/library repair action has stale-fenced the lease, `XACK` returns
  `-ERR pqueue stale_lease`.
- If the id was superseded by pending-item replacement, `XACK` returns `-ERR pqueue superseded`.
- A `0` count must not be used to hide stale or superseded lease failures.

### `XCLAIM` and `XAUTOCLAIM`

Reclaim expired work.

Rules:

- Reclaim pagination is entry-id ordered, matching the cursor shape of Redis Streams.
- Priority governs delivery through `XREADGROUP`, not cursor pagination through `XAUTOCLAIM`.
- `attempt_count` = the number of times the item was **delivered** (handed to a worker via a claim). A
  timed reclaim (`ReclaimDriver`/`XAUTOCLAIM` returning an expired lease to pending) is NOT a delivery and
  does **not** charge; the subsequent re-delivery charges the one attempt. So a reclaim+redeliver cycle
  bumps `attempt_count` by exactly one.
- Same-consumer `XCLAIM` is treated as a lease renew and does not charge an attempt.
- Cross-consumer `XCLAIM` reclaims ownership to the new consumer and charges one attempt (the re-lease is
  the delivery).
- The engine-owned `ReclaimDriver` is still required so quiet queues make lease/progress transitions
  without depending on client-driven `XAUTOCLAIM` traffic.

### `XPENDING`, `XLEN`, `XINFO`, `XDEL`

Supported for stock inspection and basic deletion.

Rules:

- Rich metrics, lifecycle state, operator inspection, and force purge are library-only.
- `XDEL` of active or terminal work follows the engine's lifecycle rules; invalid state returns
  `-ERR pqueue invalid` rather than silently violating delivery invariants.

## 4. Library-Only Surface

These API-001/API-002 capabilities are intentionally not exposed over launch RESP:

- filtered claim (`same_group_key`, exact `group_key`, metadata predicates);
- whole-group and whole-cohort lifecycle handles beyond default item-mode delivery;
- explicit lease duration renewals;
- finalize dispositions other than `complete`: `fail`, `retry`, `release`, `rearm`;
- queue create/configure, mutable priority updates, gate mutation, pause/resume;
- rich metrics, active scopes, queue admin state, item inspection, operation list/status/cancel;
- operator repair, redrive, force purge, archive, and retention operations;
- request-id replay semantics for commands where stock Redis has no request-id slot.

The library is not a fallback for an incomplete RESP contract. It is the designed full-power control
interface. The RESP launch contract is the stock worker hot path.

## 5. Capability Matrix

| Operation | RESP stock | Rust library |
|---|---:|---:|
| Push append | pass (`XADD`) | pass |
| Pending-item replacement | pass on atomic backends (`XADD` with `client_item_key`) | pass |
| Claim item, unfiltered | pass (`XREADGROUP >`) | pass |
| Claim filtered by group or metadata | library-only-intentional | pass |
| Claim whole group or whole cohort | library-only-intentional | pass |
| Renew lease with explicit duration | library-only-intentional | pass |
| Complete | pass (`XACK`) | pass |
| Fail/retry/release/rearm | library-only-intentional | pass |
| Reclaim expired | pass (`XCLAIM`/`XAUTOCLAIM`) | pass |
| Pending/depth inspection | pass (`XPENDING`/`XLEN`/`XINFO`) | pass |
| Rich metrics and active scopes | library-only-intentional | pass |
| Basic delete | pass (`XDEL`) | pass |
| Force purge | library-only-intentional | pass |
| Queue create/configure | library-only-intentional | pass |
| Gates and pause/resume | library-only-intentional | pass |
| Operator repair/redrive/archive/retention | library-only-intentional | pass |
| Operation status/cancel/list | library-only-intentional | pass |

No launch operation is exposed through custom RESP commands.

## 6. Redis Divergences

1. **Priority delivery, not stream-id delivery.** `XREADGROUP >` returns the highest-priority eligible
   items, so `XINFO GROUPS last-delivered-id` is not a useful work-progress high-water mark.
2. **Per-item delivery tracking.** Low-id, low-priority work is not orphaned by a high-id claim.
3. **`XAUTOCLAIM` cursor order remains entry-id order.** Reclaim pagination is cursor-faithful; it is
   not priority-ordered.
4. **Fenced `XACK`.** A stale lease returns `-ERR pqueue stale_lease`; it does not silently return `0`.
5. **Superseded ids are explicit failures.** `XACK`/`XCLAIM` of a superseded id returns
   `-ERR pqueue superseded`.
6. **Eventual-apply backends have weaker ordering guarantees.** Priority order is over applied state,
   and pending-item replacement returns `-ERR pqueue unavailable`.

## 7. Canonical Errors

Conformance tests assert these exact error prefixes:

| Condition | Error |
|---|---|
| stale operator-fenced lease | `-ERR pqueue stale_lease` |
| superseded entry id | `-ERR pqueue superseded` |
| unsupported on backend durability class | `-ERR pqueue unavailable` |
| terminal lifecycle state | `-ERR pqueue terminal` |
| invalid command or lifecycle transition | `-ERR pqueue invalid` |
| authorization failure | `-NOPERM` |

## 8. Required Conformance Tests

Run each applicable test against the RESP adapter with at least one pinned off-the-shelf Redis client.

- **Drain and reconcile**: produce mixed priorities, drain through `XREADGROUP >`, assert delivered set
  equals produced set, each item once, with no low-priority orphaning.
- **Pending replacement**: re-`XADD` a pending `client_item_key`, assert new id, old id superseded,
  and stable logical depth.
- **Claim collision**: re-`XADD` a claimed `client_item_key`, assert rejection.
- **Superseded ack**: `XACK` an old superseded id, assert `-ERR pqueue superseded`.
- **Cursor reclaim**: page `XAUTOCLAIM` from `0-0` until completion, assert the whole PEL is covered.
- **Crash recovery**: kill a consumer after claim, reclaim after expiry, assert no lost or double work.
- **Fence**: stale a lease through the library/operator surface, then assert stock `XACK` returns
  `-ERR pqueue stale_lease`.
- **Intra-group exclusion**: two consumers concurrently call `XREADGROUP >`; assert no item is claimed
  twice.
- **Quiet-queue reclaim driver**: without intervening client commands on the queue, assert expired
  work is reclaimed or progressed by `ReclaimDriver`.

## 9. Post-Launch Decisions

These are not part of the launch surface:

- whether a single custom `PQFIN` command is worth adding for atomic rich finalize over RESP;
- whether a read-only custom metrics command is useful enough to duplicate library inspection;
- whether Redis ACL category compatibility should be broadened beyond pqueue's launch authorization
  model.

Any post-launch custom command must update this TD, the capability matrix, and the conformance suite
before implementation.
