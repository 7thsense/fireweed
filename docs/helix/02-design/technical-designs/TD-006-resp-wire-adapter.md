---
ddx:
  id: td-resp-wire-adapter
  depends_on:
    - api-native-client-interface
    - api-operator-repair-contract
    - adr-cqrs-log-projection-storage-model
    - adr-granularity-mapping-and-claim-domain
    - adr-queue-as-shard-unit-and-projection-families
    - td-sharding-and-shard-ownership
  status: draft
  review:
    self_hash: d33d11d4e7e087384828e3ca3289d4f0b7bb6aefd88a4245ddb7f441f0706bc6
    deps:
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
      api-native-client-interface: 852a753af558d8b8a21e4a86e87915b14c030fefcb4a27473bcbb08cfe044580
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
    reviewed_at: "2026-07-07T06:16:09Z"
---

# Technical Design

**TD ID**: TD-006
**Title**: RESP wire adapter — stock Redis Streams worker surface plus Rust library control surface
**Status**: draft (v3, refolded against hexagonal migration plan v4)
**Related**: API-001, API-002, ADR-007, ADR-008, TD-003 (queue ownership/routing), TD-007, `docs/helix/04-build/hexagonal-migration-plan.md`

## Purpose

Define the launch RESP surface for pqueue after the hexagonal cutover.

The launch decision is intentionally narrower than earlier drafts, but no longer
stock-only:

- RESP is a pqueue-native server that speaks a **stock Redis Streams-compatible worker subset**.
- RESP also exposes a small **pqueue-native live item read subset** (`PQ.MGET`, `PQ.HGETALL`,
  `PQ.HMGET`) so clients can use pqueue as hot structured storage while work is still in queue
  ownership.
- The Rust library is the full-power interface for pqueue-specific control, inspection, filtered claim,
  gates, cohorts, rich finalize dispositions, and operator repair.
- Custom RESP mutation commands remain out of scope for launch; finalize variants beyond `XACK`, gate
  mutation, repair, and queue administration stay library-only.

This TD exists to prevent accidental half-interfaces: every API-001/API-002 operation is classified
as `RESP-stock`, `RESP-pqueue-native-read`, `library-only-intentional`, or `n/a`, and both the stock
Redis-compatible behavior and the pqueue-native live read behavior are specified enough for
conformance tests.

## 1. Implementation Model

pqueue is a **native server that speaks RESP**, not Redis and not a Redis module.

Consequences:

- pqueue owns command semantics for the subset it implements.
- pqueue owns authentication and authorization for RESP connections.
- RESP compatibility means stock Redis Streams clients can run the unfiltered worker hot path without
  custom commands; applications that need live hot-item reads use the explicit `PQ.*` command names.
- Redis clients must treat returned batches as opaque work sets; pqueue delivery order is priority
  order, not stream-id order.

## 1A. Queue Routing and Ownership (normative)

Per ADR-008 the queue is the unit of sharding: each queue `(tenant_id, queue_id)` is owned by exactly
one node at a time (TD-003). Horizontal scale is **cross-queue** — many queues distributed across owner
nodes — so a queue-addressed worker command is a single hop to that queue's owner: no scatter-gather on
the wire. The RESP surface presents Redis Cluster's slot/redirect vocabulary so stock cluster-aware
clients route unmodified.

**Scope.** This section governs **queue-addressed** commands — those naming one `(tenant_id, queue_id)`,
i.e. every §3 stock command. Tenant-spanning operations (e.g. tenant-wide active-scope discovery) are
NOT queue-addressed and are out of scope for the RESP worker surface (library-only, §4).

### Authority and redirect

| Element | Rule |
|---|---|
| Single owner of record | The authoritative current owner of a queue is the `active_owner` recorded in the TD-003 control-plane authority record (returned by `resolve_queue_owner`), NOT a value each node recomputes independently. The deterministic HRW/rendezvous placement function (TD-003) is how the control plane *selects* a target owner; the wire redirect always names the recorded `active_owner`. Routing therefore converges on one source of truth and cannot enter a persistent two-node `-MOVED` loop from divergent membership views (a divergence is a TD-003 liveness concern resolved by the authority record, not a routing-safety problem). |
| Serve only under a live current-epoch lease | A node MUST serve a queue-addressed command only while it holds a live TD-003 lease for the queue at the current epoch. A node that does not — never owned it, or was deposed and has learned so via a failed renew (`queue-epoch-stale`, TD-003) — MUST redirect, not serve. |
| MOVED on miss | A node that will not serve a queue it does not currently own MUST reply `-MOVED <slot> <owner-endpoint>` naming the recorded `active_owner`. A stock cluster-aware client updates its routing table and retries against the owner (the standard Redis Cluster client loop). **Authorization is checked first**: a principal not authorized for the queue receives `-NOPERM` (§7) and never a `-MOVED`, so a redirect never leaks a queue's existence or placement across a tenant boundary. |
| Slot mapping (stock-client compatibility) | pqueue presents the Redis Cluster 16384-slot space so stock clients bootstrap and parse redirects unmodified: `slot = crc16(routing_key) % 16384`, `routing_key = "{" + tenant_id + "/" + queue_id + "}"` (a Redis hash-tag, so the client's own key→slot computation matches). A `CLUSTER SLOTS` / `CLUSTER SHARDS` response advertises the current slot→owner view for bootstrap. The slot is a **routing hint only**: ownership is per-queue, so two queues sharing a slot MAY have different owners; the per-queue `-MOVED` corrects the client, which updates its table. Because a worker drains one queue, redirect churn is one-time per queue, not per command. |

### Staleness safety (what the fence does and does not cover)

The owner-of-record + redirect model tolerates a stale client routing table or a briefly-stale node
view **without distributed consensus**. The window in which a deposed node still believes it owns a
queue is bounded by the lease/renew interval (TD-003): on its next renew it learns it is deposed and
begins redirecting. Within that window:

| Command class | On a deposed/stale owner, within the renew window |
|---|---|
| **Durable writes** (`XADD`; the `append_batch` of any mutating command) | **Cannot corrupt state.** The TD-003 Single Authoritative Fencing Rule rejects an append whose `expected_epoch` is not the current control-plane epoch, the instant the epoch advances — so a misrouted write is rejected and the client retries against the current owner. `client_item_key` makes the `XADD` retry converge. |
| **Claims / delivery** (`XREADGROUP >`; cross-consumer `XCLAIM`) | **May redundantly deliver before commit, but cannot durably double-claim.** On an atomic backend (TD-007) select+append commit together, so a deposed owner's claim is fenced atomically and hands out nothing. On a log-then-apply backend (`objectlog`) any tentative delivery before the claim command commits is non-authoritative: if the `BatchClaim` append is fenced, **no durable lease is created**, and the worker's later `XACK`/finalize on the deposed owner is epoch-fenced -> `-ERR pqueue stale_lease` (§3). In `objectlog/hybrid-strict` and `objectlog/hybrid-async`, the hot projection is current on the success path, so the claim/upsert race can be closed by the backend's own response barrier; stock `XREADGROUP` still carries no `request_id`, so convergence on the stock path rests on the fence + at-least-once + the `stale_lease` ack rejection, while library/native paths use `request_id` replay (§4). |
| **Pure reads** (`XLEN`, `XINFO`, `XPENDING`, `XRANGE`) | **Bounded-stale, never authoritative.** A read has no `append_batch` and is not epoch-fenced; a deposed owner that has not yet failed a renew MAY serve a read from its frozen local projection. Such reads are best-effort and bounded-stale by the lease/renew interval and any documented unrelated-operation apply budget. A client needing an authoritative read MUST reach the current owner (follow the redirect). The read guarantee is "bounded staleness," not "fresh-or-fenced"; a node MUST still redirect once it has learned it is not the owner. |

### Reassignment (drain) on the wire

During a TD-003 `draining` handoff the queue still has exactly one lease-holding owner (the draining
owner); the incoming `target_owner` MUST NOT acquire until the queue reaches `unassigned` (TD-003). So
no second node can serve the queue during drain, and an `-ASK`-style "try this one query at the other
node" does NOT apply (the target cannot serve yet). Instead the command set splits:

- **In-flight commands stay on the draining owner**: `XACK`, same-consumer `XCLAIM` (lease renew),
  `XAUTOCLAIM` of the caller's own PEL, and `BatchRenewLeases` continue to be served so in-flight leases
  finalize (TD-003 drain MUST NOT cancel in-flight worker leases). A worker is never redirected
  mid-lease.
- **New claims are not started**: `XREADGROUP >` and cross-consumer `XCLAIM` (a new delivery) MUST NOT
  be served by the draining owner; it returns a retryable `-ERR pqueue unavailable` until handoff
  completes. Once the new owner has acquired (queue `assigned` to the target), the normal MOVED-on-miss
  path redirects new claims to it.

This section adds no custom RESP command and re-specifies no TD-003 mechanism: placement, lease, epoch
fence, and the `unassigned`/`assigned`/`draining` states are TD-003's. TD-006 states only the wire
behavior — owner-of-record redirect, slot mapping for stock clients, the staleness-safety envelope, and
the drain command split.

## 2. Container Entry Contract

RESP entries are flat field/value pairs. pqueue reserves fields used by API-001 and returns additional
reserved fields in claim replies and `PQ.*` reads. Non-reserved `XADD` field/value pairs are stored as
the item's structured field map.

| Reserved field | Direction | Meaning |
|---|---|---|
| `client_item_key` | request | Caller-provided logical key for pending-item replacement and audit. |
| `priority` | request | Ordering key interpreted by the queue's priority model. |
| `group_key` | request | Optional per-group ordering/compatibility key (co-resident on the queue's owner by construction, ADR-008). |
| `cohort_id` | request | Optional cohort identity. Whole-cohort semantics are library-only. |
| `not_before` | request | Earliest eligibility timestamp. |
| `max_attempts` | request | Retry bound override. |
| `gate_keys` | request | JSON array of dynamic gate keys. Gate mutation is library-only. |
| `metadata` | request | JSON object for predicates and audit. |
| `payload` | request | Opaque application payload. |
| `item_id` | reply/read | Server-assigned item id. Read-only; cannot be supplied as a user field. |
| `item_version` | reply | Server-assigned monotonic item version. |
| `lifecycle_state` | reply/read | `Pending` or `Leased` for live reads. |
| `lease_expires_at` | reply | Server-computed lease expiry timestamp. |
| `attempt_count` | reply/read | Delivery count (claims handed to a worker); a timed reclaim does not charge. |

The RESP entry id is the wire `item_id`. `client_item_key` is not the entry id; it is the caller's
logical replacement/idempotency key.

`XADD` MUST reject non-reserved user fields whose names collide with read-only/system fields returned by
`PQ.*` (`item_id`, `item_version`, `lifecycle_state`, `attempt_count`, `group_key`, `not_before`) or
with parsed request fields (`client_item_key`, `priority`, `payload`). User field names MUST be UTF-8.

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
- On pure lagging-projection log-then-apply backends, replacement is unavailable and returns
  `-ERR pqueue unavailable`.
- On `objectlog/hybrid-strict` and `objectlog/hybrid-async`, replacement MAY be enabled only after
  TD-004 proves deterministic apply-time re-validation with ack-after-apply. Until that proof lands,
  they also return `-ERR pqueue unavailable`.
- Non-reserved field/value pairs are stored as structured item fields. `payload` is stored separately as
  the existing opaque payload slot. Claims and `PQ.*` reads return both.

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
  bumps `attempt_count` by exactly one. This is the same single counter the API-001 claimed-item shape
  returns as `attempt_count`: `retry_policy.max_attempts` bounds it, and a (library-only) `rearm` resets
  it to 0 for the next cycle.
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

## 3A. pqueue-native Live Item Reads

These commands intentionally emulate useful Redis hash read shapes without making pqueue a Redis hash
keyspace. They read **live pqueue items** addressed by `client_item_key` within one queue. "Live" means
the item is still queue-owned active work: `Pending` or `Leased`, not terminal, not purged, and not
superseded. Leased items are returned because they are still in queue ownership until finalization.

### `PQ.MGET <queue> <client_item_key> [client_item_key ...]`

Returns an array aligned with the requested keys. Each live item is rendered as
`[item_id, [field, value, ...]]`, the same entry-shaped body used by `XREADGROUP`; a missing,
terminal, purged, or superseded key returns nil in that array position.

### `PQ.HGETALL <queue> <client_item_key>`

Returns a flat `[field, value, ...]` array for one live item. A non-live key returns an empty array,
matching Redis `HGETALL`'s missing-key shape. Returned fields include pqueue system fields
(`item_id`, `client_item_key`, `item_version`, `lifecycle_state`, `priority`, `attempt_count`,
`payload` when present, `group_key`/`not_before` when present) followed by user fields.

### `PQ.HMGET <queue> <client_item_key> <field> [field ...]`

Returns an array aligned with requested field names. Missing fields, or a non-live item, return nil in
the corresponding position. System fields and user fields are addressable by name.

## 4. Library-Only Surface

These API-001/API-002 capabilities are intentionally not exposed over launch RESP:

- filtered claim (`same_group_key`, exact `group_key`, metadata predicates);
- whole-group and whole-cohort lifecycle handles beyond default item-mode delivery;
- explicit lease duration renewals;
- finalize dispositions other than `complete`: `fail`, `retry`, `release`, `rearm`;
- queue create/configure, mutable priority updates, gate mutation, pause/resume;
- rich metrics, active scopes, queue admin state beyond live item reads, operation list/status/cancel;
- operator repair, redrive, force purge, archive, and retention operations;
- request-id replay semantics for commands where stock Redis has no request-id slot.

The library is not a fallback for an incomplete RESP contract. It is the designed full-power control
interface. The RESP launch contract is the stock worker hot path.

## 5. Capability Matrix

| Operation | RESP stock | Rust library |
|---|---:|---:|
| Push append | pass (`XADD`) | pass |
| Pending-item replacement | pass on atomic backends (`XADD` with `client_item_key`); pass on `objectlog/hybrid-strict` and `objectlog/hybrid-async` once TD-004 proves deterministic apply-time re-validation with ack-after-apply | pass |
| Claim item, unfiltered | pass (`XREADGROUP >`) | pass |
| Claim filtered by group or metadata | library-only-intentional | pass |
| Claim whole group or whole cohort | library-only-intentional | pass |
| Renew lease with explicit duration | library-only-intentional | pass |
| Complete | pass (`XACK`) | pass |
| Fail/retry/release/rearm | library-only-intentional | pass |
| Reclaim expired | pass (`XCLAIM`/`XAUTOCLAIM`) | pass |
| Pending/depth inspection | pass (`XPENDING`/`XLEN`/`XINFO`) | pass |
| Live item multi-get by `client_item_key` | pass (`PQ.MGET`) | pass |
| Live item field reads | pass (`PQ.HGETALL`/`PQ.HMGET`) | pass |
| Rich metrics and active scopes | library-only-intentional | pass |
| Basic delete | pass (`XDEL`) | pass |
| Force purge | library-only-intentional | pass |
| Queue create/configure | library-only-intentional | pass |
| Gates and pause/resume | library-only-intentional | pass |
| Operator repair/redrive/archive/retention | library-only-intentional | pass |
| Operation status/cancel/list | library-only-intentional | pass |

Launch custom RESP commands are read-only and scoped to live item access.

## 6. Redis Divergences

1. **Priority delivery, not stream-id delivery.** `XREADGROUP >` returns the highest-priority eligible
   items, so `XINFO GROUPS last-delivered-id` is not a useful work-progress high-water mark.
2. **Per-item delivery tracking.** Low-id, low-priority work is not orphaned by a high-id claim.
3. **`XAUTOCLAIM` cursor order remains entry-id order.** Reclaim pagination is cursor-faithful; it is
   not priority-ordered.
4. **Fenced `XACK`.** A stale lease returns `-ERR pqueue stale_lease`; it does not silently return `0`.
5. **Superseded ids are explicit failures.** `XACK`/`XCLAIM` of a superseded id returns
   `-ERR pqueue superseded`.
6. **Log-then-apply backends preserve the same durable queue contract through a response barrier.**
   Pure lagging-projection profiles still return `-ERR pqueue unavailable` for pending-item
   replacement; `objectlog/hybrid-strict` and `objectlog/hybrid-async` lift that ban only after they
   prove deterministic apply-time re-validation with ack-after-apply in TD-004.
7. **`PQ.H*` commands are pqueue live-item reads, not Redis hashes.** They emulate Redis hash read
   response shapes over pqueue's structured item fields. They do not create independent hash keys, and
   data disappears from the live view when the queue item completes, fails, is purged, or is superseded.

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
