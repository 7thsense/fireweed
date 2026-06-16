---
ddx:
  id: adr-granularity-mapping-and-claim-domain
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
  review:
    self_hash: ba2d4c26c9fcaa4470ea65b61eff20cf382b6bba9e261cbd453f13122bfbc7c8
    deps:
      api-native-client-interface: f90b0c65a65c4b088b9b04cb28ca0d5b0d174acf7cdfc326bcd859d79c7d1762
      concerns: 122b700fbf6049b7fa177b99efa27c5fce011775767d682458a0e2872981fb54
      prd: 382115039de93226b051a09e719c7e1c50f12563d96c1ba85ef142c0ae5d0ce0
    reviewed_at: "2026-06-16T17:42:59Z"
---

# ADR-004: Granularity Mapping and Claim Domain

## Context

API-001 defines `tenant_id`, `queue_id`, `group_key`, and `metadata`; ADR-002
adds a physical `shard_id`. The first validation workload (Seventh Sense) nests
account, job, connector, and campaign concepts, claims work in
scheduled-timestamp order, and physically hash-shards on a job-like key. The
native model never mapped that nesting onto pqueue's axes, never stated at what
domain claim result order is guaranteed, never stated the placement precondition
under which per-group order is achievable, and never stated whether `shard_id`
is client-visible. Several downstream gap designs (group-cardinality claim,
atomic cohort claim, active-scope discovery, recurring/perpetual items) depend
on a single, shared answer.

## Decision

### Client-visible granularity axes

The client-visible granularity axes are exactly four:

1. **`tenant_id`** — isolation / authorization / control-plane-routing boundary
   (ADR-002).
2. **`queue_id`** — configuration + ordering-policy + progress-bound + metrics
   boundary (FR-1). One queue hosts many groups.
3. **`group_key`** — logical ordering / compatibility partition WITHIN a queue.
   It is the unit of per-group ordering and of group-aware claim compatibility.
   It carries NO progress-bound meaning of its own (progress is queue-global,
   see below).
4. **`metadata`** — caller-defined eligibility-gate inputs and observability
   dimensions (FR-16, FR-17, FR-45).

### `shard_id` is never client-visible

`shard_id` is a physical routing/capacity unit owned by the control plane
(ADR-002, TD-001). It **MUST NOT** appear in any client request, response,
ordering rule, progress metric, or discovery descriptor, and **MUST NOT** be a
client-visible ordering or progress scope. Result order and progress
guarantees are defined entirely in terms of `queue_id` and `group_key`.

### Progress scope is queue-global

Every queue has exactly ONE progress bound, enforced queue-globally over all
eligible items across all shards (FR-9, FR-12; cross-shard aggregation owned by
the shard-ownership technical design, TD-003). `group_key` is **not** a progress
scope: the engine does not owe a per-group non-starvation invariant, and there
is **no per-group progress metric**. Per-group fairness, where a worker fleet
needs it, is achieved by routing workers to groups with eligible work via
`DiscoverActiveScopes` (API-001, g4), not by an engine invariant. No field named
`claim_scope` exists; `group_key` selection is a claim-compatibility and ordering
concern only. A worker that supplies an explicit caller filter (`group_key`,
`metadata_equals`) MUST be returned only items inside that filter; the
queue-global progress bound never causes the engine to return an item outside a
caller's declared filter.

### Effective claim domain and deterministic-in-domain ordering

The **effective claim domain** of a `BatchClaim` is the candidate set remaining
after applying (a) the queue's eligibility predicate (the single canonical
**Eligibility Precedence** order defined in API-001, authored by g2) and (b) the
request's caller compatibility filters (`group_key`, `same_group_key`,
`metadata_equals`) and any active claim-unit mode (g1 `group_batching`, g6
`whole_cohort`). Claim result order **MUST** be deterministic within the
effective claim domain under the queue's `ordering_mode`, `priority_model`, and
queue-global progress contract.

**Per-group order is a placement-enabled property.** When the effective claim
domain is a single `group_key` **on a queue with `group_co_residency=true`**,
that group's items are co-resident on one shard, the single-shard candidate set
covers the whole group, and the claim result order over that domain **MUST** be
the exact per-group priority order. When a `group_key` filter is applied on a
queue with `group_co_residency=false`, the filter is a valid claim-domain
restriction but pqueue does **NOT** promise per-group total order across shards
(older same-group items MAY live on another shard); the result order is the
queue's ordering mode over the matching items observed, deterministic per shard
and merged per the cross-shard merge rule (TD-003), not a per-group total order.
Ordering ACROSS distinct `group_key`s in one response is unspecified except as a
claim-unit mode's own contract defines it (g1 defines its representative
ordering; g6 returns one cohort).

### Group co-residency (placement capability)

A queue MAY declare `group_co_residency=true` at creation (API-001 CreateQueue;
default false; immutable after creation). When true:

- Shard placement is a deterministic function of `group_key`:
  `shard_id = hash(group_key) mod shard_count`. All items sharing a `group_key`
  are co-resident on exactly one shard.
- Every pushed item **MUST** carry `group_key`; an item without `group_key`
  **MUST** be rejected per item with `invalid`.
- Because a group is shard-local, whole-group (g1 `group_batching`) and
  whole-cohort (g6 `whole_cohort`) claims are shard-local and atomic without
  cross-shard coordination, and a single-`group_key` claim returns exact
  per-group order.

When `group_co_residency=false` (default), `group_key` is still a valid
ordering/compatibility restriction filter, but items of one `group_key` MAY be
spread across shards; claim modes that require whole-group atomicity (g1
`group_batching`, g6 `whole_cohort`) are then invalid and **MUST** be rejected
with `invalid-request`, and a bare `group_key` filter is honored only with the
weaker (non-per-group-total-order) guarantee above. `group_co_residency` is a
PLACEMENT capability only; it carries no progress meaning (progress remains
queue-global).

`group_co_residency` is part of a queue's stable configuration identity:
idempotent `CreateQueue` MUST treat a differing `group_co_residency` value as an
incompatible definition and reject it as a conflict (API-001 idempotent-create
rules); it participates in the queue's configuration hash and is immutable after
creation.

### Recurrence vs cohort exclusivity

Because a recurring item is a never-terminal singleton and a cohort is a
fixed-size, complete-then-claim `whole_cohort` claim unit (g6),
`recurrence.mode=recurring` and a cohort-enabled topology are mutually exclusive
on the same queue. A queue is EITHER cohort-enabled (keyed by `callback_id`,
supports `whole_cohort`, `oneshot`) OR a non-cohort scheduled-action queue
(which MAY be `recurring`, keyed by `job_id`). There is no "cohort-enabled
recurring" topology. CreateQueue MUST reject a queue that sets both
`recurrence.mode=recurring` and `cohort_policy.enabled=true` with envelope
`invalid-request`.

### Per-group summary projection (single canonical projection)

There is exactly ONE per-group summary projection, **`pqueue_group_summary`**,
keyed by **`(tenant_id, queue_id, shard_id, group_key)`**. It is the single
source of truth for (a) g1 `group_batching` oldest-group selection, (b) g4
`DiscoverActiveScopes` group-granularity ranking, and (c) per-group
observability. Per group it holds the authoritative oldest-eligible timestamp
`oldest_eligible_at`, an exact selection-oriented **representative claim key**
(`rep_progress_guard_sort`, `rep_priority_sort`, `rep_created_at`, `rep_item_id`)
used to rank groups for whole-group selection and discovery, and
eligible/at-risk counts (MAY be lagged/approximate). It is maintained in the same
transaction as item mutations that change a group's eligible set, using the same
Eligibility Precedence predicate. The authoritative fields (`oldest_eligible_at`
and `rep_*`) are kept **exact-on-read under the gate predicate** (see the
gate-flip consistency model below and TD-002); only the counts may lag. The DDL
and full consistency model are owned by TD-002. Tenant-wide queue-granularity
discovery is a rollup over this same projection (queue-level
`min(oldest_eligible_at)`), not a second table. There is no
`pqueue_active_scope_summary` table.

### Gate-flip / per-group exact-on-read model (consistency)

A queue-scoped gate flip (g2 `SetGates`) changes which items are eligible
WITHOUT writing any item row and WITHOUT synchronously rewriting every affected
group's summary row. To keep per-group selection correct, the authoritative
per-group oldest-eligible value is **exact-on-read**: the summary row stores the
group's representative item/key computed under item-level eligibility, and the
read path (g1 selection, g4 discovery, metrics) joins the candidate group's gate
keys to the current gate state and, if the representative item is gate-blocked at
read time, advances to the group's next item that is open under all gate keys —
rather than trusting a possibly-stale summary row. This is the same exact-on-read
contract g2 defines for `oldest_eligible_age`. Because a group becomes
gate-blocked as a unit only when a gate key it carries is blocked, this join is
O(blocked keys), not O(items). Eligible counts MAY lag a flip and converge by a
bounded background recompute (TD-002). This is how a reader distinguishes
"whole group blocked" (all of the group's representatives are gate-blocked, so
the group is excluded) from "oldest item blocked" (advance to the next open item
in the group).

### Granularity mapping (normative reference table)

| Domain concept | pqueue axis | Rationale |
|----------------|-------------|-----------|
| Tenant / account class / deployment boundary | `tenant_id` | Auth + storage isolation (ADR-002). |
| Logical work stream with its own ordering policy & progress bound | `queue_id` | Config + ordering + metrics boundary (FR-1). |
| Per-key FIFO/priority ordering & compatibility unit | `group_key` | Ordering/compatibility partition; physical shard derives from it iff `group_co_residency`; per-group order requires `group_co_residency`. |
| Application states / filters (paused, connector, campaign, account) | `metadata` | Eligibility gates + observability (FR-45, FR-17). |
| Physical partition / capacity unit | `shard_id` | API-invisible; never a client ordering/progress scope. |

### Per-queue `group_key` topology

`group_key` is the queue's single placement and ordering partition; a queue
chooses ONE topology at creation:

| Queue topology | `group_key` is | `metadata` carries | Use |
|----------------|----------------|--------------------|-----|
| Per-job scheduled-action queue (non-cohort) | `job_id` | `account_id`, `connector`, `campaign_id`, `callback_id` | Per-job ordering; no cohort atomicity needed. |
| Cohort-enabled queue (`cohort_policy.enabled=true`) | `callback_id` | `job_id`, `account_id`, `connector`, `campaign_id` | Atomic complete-cohort claim keyed by `batch_checksum`/`callback_id`. |
| Recurring non-cohort scheduled-action queue (`recurrence.mode=recurring`) | `job_id` (resp. `connector_id`) | `account_id`, `connector`, `campaign_id`, `callback_id` | Per-job/connector recurring tick; the recurrence key IS the `group_key`. Recurring singletons are non-cohort (g6); a recurring item MUST NOT be a cohort member. |

Because `group_key` owns BOTH placement (group co-residency) and the cohort
identity, a single queue MUST pick one topology. If a single queue genuinely
requires both per-job ordering AND callback-cohort atomicity at once, that is an
explicit Seventh Sense confirmation item: `metadata` cannot substitute for a
lost placement/ordering/atomicity key, so the workload would need either two
queues (one keyed by `job_id`, one by `callback_id`) or a future composite-key
design. v1 does not provide a composite `group_key`.

### Seventh Sense topology (per-queue, resolves the group_key question)

Seventh Sense has distinct queue shapes, each with its OWN `group_key` topology:

| Queue shape | `group_key` keyed by | Other identifiers | Why |
|-------------|----------------------|-------------------|-----|
| **Cohort-enabled callback queue** (callback-batched sends, `CallbackActionScheduleType.cohorts(n)`) | `callback_id` (cohort identity) | `job_id`, `account_id`, `connector`, `campaign_id` move to `metadata` | Whole-cohort atomicity (g6) batches by callback; the placement/atomicity key MUST be the cohort identity. |
| **Non-cohort scheduled-action queue** (per-job scheduled actions) | `job_id` | `account_id`, `connector`, `campaign_id` are `metadata` | Per-job ordering + group-aware claim by job; no cohort. |
| **Recurring scheduled-action queue** (per-job/connector recurring tick) | `job_id` (resp. `connector_id`) | `account_id`, `connector`, `campaign_id`, `callback_id` are `metadata` | Per-job/connector recurring ordering; non-cohort; the recurrence key IS the `group_key`. |

In all shapes the scheduled timestamp maps to `priority` (timestamp ascending),
and `group_co_residency=true` is set so the chosen `group_key` is shard-local
(required for per-group order and whole-group/whole-cohort atomicity, and so a
recurring singleton stays on one shard for its lifetime — re-arm never relocates
it). Progress remains queue-global in all shapes; per-job or per-callback
fairness, where needed, is a `DiscoverActiveScopes` routing concern (g4), not an
engine invariant (D1). Discovery is topology-agnostic: it reports whatever
`group_key` the queue's topology defines and supports group granularity on BOTH
`group_co_residency` modes (co-residency affects placement and atomic claim
modes, not whether discovery can rank a queue's groups); group descriptors carry
no per-group progress guarantee.

**Seventh Sense confirmation item.** If a single physical queue genuinely needs
BOTH per-`job_id` ordering (or per-`job_id` recurrence) AND callback-cohort
atomicity at the same time, that is NOT expressible by moving one key to
`metadata` (metadata cannot carry a placement/atomicity key). Such a queue MUST
be split into two queues (one keyed by `job_id`, one by `callback_id`) OR the
migration MUST confirm which single key is authoritative. This is flagged for
Seventh Sense confirmation before the migration design; it is NOT assumed
resolved here.

## Consequences

Positive: one queue hosts many groups without queue-cardinality explosion;
per-group order is enforceable shard-locally under co-residency; whole-group and
whole-cohort claims need no cross-shard coordination; `shard_id` stays a pure
physical concern; there is exactly one per-group summary projection with a
well-defined exact-on-read consistency model.

Negative: per-group total order and whole-group/whole-cohort atomicity require
`group_co_residency=true`, which makes shard placement a function of `group_key`,
so a single very large group cannot be split across shards (accepted for v1; a
group is the ordering/atomicity unit). On non-co-resident queues a `group_key`
filter restricts but does not totally order across shards. Queues that need
cross-group metadata-spanning claims must NOT use whole-group/whole-cohort modes.

### Open topology decisions

A recurring singleton's placement key is its `group_key`; with
`group_co_residency=true` the singleton is fixed to one shard for its lifetime
and re-arm never relocates it. If a single workload genuinely needs BOTH
per-`job_id` recurrence ordering AND callback-cohort atomicity, those are two
distinct queues (one non-cohort recurring queue keyed by `job_id`, one
cohort-enabled queue keyed by `callback_id`); a single queue cannot carry both
axes because `group_key` can encode only one of them and metadata cannot replace
a lost placement/ordering/atomicity key. That case MUST be confirmed with
Seventh Sense before migration and is recorded as an explicit confirmation item —
it is NOT silently resolved by moving one axis to metadata.

## Status

Proposed.
