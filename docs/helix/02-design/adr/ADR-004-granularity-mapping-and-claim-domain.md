---
ddx:
  id: adr-granularity-mapping-and-claim-domain
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
  review:
    self_hash: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
    deps:
      api-native-client-interface: c70eba23875d1b9592ea70e5a28b472f936fc0238dba17a0c5cb7773a94c297f
      concerns: 7e3b81e376f75f71691f55ac1ca4d9599eddcfe6eefe70f614c366c132e07992
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
    reviewed_at: "2026-07-06T00:56:00Z"
---

# ADR-004: Granularity Mapping and Claim Domain

## Context

API-001 defines `tenant_id`, `queue_id`, `group_key`, and `metadata`; physical
placement is an internal storage concern (ADR-008: the queue is the unit of
sharding; any item-table partition is client-invisible). The first validation
workload (Seventh Sense) nests account, job, connector, and campaign concepts,
claims work in scheduled-timestamp order, and physically hash-partitions on a
job-like key. The native model never mapped that nesting onto pqueue's axes, never
stated at what domain claim result order is guaranteed, never stated the placement
precondition under which per-group order is achievable, and never stated whether
physical placement is client-visible. Several downstream gap designs (group-cardinality claim,
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

### Physical placement is never client-visible

A queue's physical placement — its single owning node and any internal storage
partitioning of that node's item table (ADR-008) — is owned by the control plane
(ADR-002, TD-001/TD-003). It **MUST NOT** appear in any client request, response,
ordering rule, progress metric, or discovery descriptor, and **MUST NOT** be a
client-visible ordering or progress scope. Result order and progress
guarantees are defined entirely in terms of `queue_id` and `group_key`.

### Progress scope is queue-local

Every queue has exactly ONE progress bound, enforced over all of that queue's
eligible items on its single owner (FR-9, FR-12; ADR-008 — the queue is the unit
of sharding, so there is no cross-shard aggregation). `group_key` is **not** a
progress scope: the engine does not owe a per-group non-starvation invariant, and there
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

**Per-group order is unconditional.** Because the queue is the unit of sharding
(ADR-008) — a whole queue is owned by one node, with no intra-queue sharding —
every `group_key`'s items are co-resident on that one owner **by construction**.
When the effective claim domain is a single `group_key`, the candidate set covers
the whole group and the claim result order over that domain **MUST** be the exact
per-group priority order. Ordering ACROSS distinct `group_key`s in one response is
unspecified except as a claim-unit mode's own contract defines it (g1 defines its
representative ordering; g6 returns one cohort).

### Group co-residency is automatic (ADR-008)

Co-residency is **no longer a queue option**. Under ADR-008 the queue is the unit
of sharding, so all items of a `group_key` are co-resident on the queue's single
owner **by construction**. The `group_co_residency` field is therefore **removed
from the contract** (API-001 CreateQueue) and **from the configuration-identity
hash**. Its former consequences become **unconditional queue properties**:

- Whole-group (g1 `group_batching`) and whole-cohort (g6 `whole_cohort`) claims
  are always shard-local and atomic — there is no cross-shard coordination to
  avoid, so no `group_co_residency=true` precondition is needed.
- A single-`group_key` claim always returns exact per-group order.
- The "every pushed item MUST carry `group_key`" rule, where it applies, is gated
  by the queue's group/cohort configuration (`cohort_policy.enabled` or
  `compatibility.group_batching`) alone — never by a `group_co_residency` flag.

`group_key` is an **ordering/compatibility** key only; it carries **no placement
or progress meaning** (placement is per-queue, ADR-008; the progress bound is
queue-local). The former `group_co_residency=false` mode — a `group_key` filter
honored with a weaker, non-per-group-total-order, cross-shard-merged guarantee —
is **retracted together with intra-queue sharding** (ADR-008; PRD FR-13). There
is no cross-shard merge.

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
keyed by **`(tenant_id, queue_id, group_key)`** (owner-local; one row per group —
ADR-008). It is the single
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
| Per-key FIFO/priority ordering & compatibility unit | `group_key` | Ordering/compatibility partition only; never a placement key (ADR-008); per-group order is unconditional (the queue is its own shard). |
| Application states / filters (paused, connector, campaign, account) | `metadata` | Eligibility gates + observability (FR-45, FR-17). |
| Physical placement / capacity unit | queue owner + internal storage partition | API-invisible (ADR-008); the queue's owner and any internal storage partitioning are physical-only, never a client ordering/progress scope. |

### Per-queue `group_key` topology

`group_key` is the queue's ordering/compatibility partition (not a placement key;
placement is per-queue, ADR-008); a queue chooses ONE topology at creation:

| Queue topology | `group_key` is | `metadata` carries | Use |
|----------------|----------------|--------------------|-----|
| Per-job scheduled-action queue (non-cohort) | `job_id` | `account_id`, `connector`, `campaign_id`, `callback_id` | Per-job ordering; no cohort atomicity needed. |
| Cohort-enabled queue (`cohort_policy.enabled=true`) | `callback_id` | `job_id`, `account_id`, `connector`, `campaign_id` | Atomic complete-cohort claim keyed by `batch_checksum`/`callback_id`. |
| Recurring non-cohort scheduled-action queue (`recurrence.mode=recurring`) | `job_id` (resp. `connector_id`) | `account_id`, `connector`, `campaign_id`, `callback_id` | Per-job/connector recurring tick; the recurrence key IS the `group_key`. Recurring singletons are non-cohort (g6); a recurring item MUST NOT be a cohort member. |

Because `group_key` owns BOTH the ordering/compatibility unit and the cohort
identity, a single queue MUST pick one topology. If a single queue genuinely
requires both per-job ordering AND callback-cohort atomicity at once, that is an
explicit Seventh Sense confirmation item: `metadata` cannot substitute for a
lost ordering/atomicity key, so the workload would need either two queues (one
keyed by `job_id`, one by `callback_id`) or a future composite-key design. v1
does not provide a composite `group_key`.

### Seventh Sense topology (per-queue, resolves the group_key question)

Seventh Sense has distinct queue shapes, each with its OWN `group_key` topology:

| Queue shape | `group_key` keyed by | Other identifiers | Why |
|-------------|----------------------|-------------------|-----|
| **Cohort-enabled callback queue** (callback-batched sends, `CallbackActionScheduleType.cohorts(n)`) | `callback_id` (cohort identity) | `job_id`, `account_id`, `connector`, `campaign_id` move to `metadata` | Whole-cohort atomicity (g6) batches by callback; the placement/atomicity key MUST be the cohort identity. |
| **Non-cohort scheduled-action queue** (per-job scheduled actions) | `job_id` | `account_id`, `connector`, `campaign_id` are `metadata` | Per-job ordering + group-aware claim by job; no cohort. |
| **Recurring scheduled-action queue** (per-job/connector recurring tick) | `job_id` (resp. `connector_id`) | `account_id`, `connector`, `campaign_id`, `callback_id` are `metadata` | Per-job/connector recurring ordering; non-cohort; the recurrence key IS the `group_key`. |

In all shapes the scheduled timestamp maps to `priority` (timestamp ascending).
Per-group order and whole-group/whole-cohort atomicity hold by construction (the
queue is the unit of sharding, ADR-008 — a group is always on the queue's single
owner), and a recurring singleton stays on that owner for its lifetime — re-arm
never relocates it. Progress remains queue-local in all shapes; per-job or
per-callback fairness, where needed, is a `DiscoverActiveScopes` routing concern
(g4), not an engine invariant (D1). Discovery is topology-agnostic: it reports
whatever `group_key` the queue's topology defines and supports group granularity
regardless (placement is per-queue, not a `group_key` function); group descriptors carry
no per-group progress guarantee.

**Seventh Sense confirmation item.** If a single physical queue genuinely needs
BOTH per-`job_id` ordering (or per-`job_id` recurrence) AND callback-cohort
atomicity at the same time, that is NOT expressible by moving one key to
`metadata` (metadata cannot carry an ordering/atomicity key). Such a queue MUST
be split into two queues (one keyed by `job_id`, one by `callback_id`) OR the
migration MUST confirm which single key is authoritative. This is flagged for
Seventh Sense confirmation before the migration design; it is NOT assumed
resolved here.

## Consequences

Positive: one queue hosts many groups without queue-cardinality explosion;
per-group order is enforceable locally and unconditionally (the queue is its own
shard); whole-group and whole-cohort claims need no cross-shard coordination by
construction; queue placement stays a pure physical concern (ADR-008); there is
exactly one per-group summary projection with a well-defined exact-on-read
consistency model.

Negative: a single very large queue cannot be split across owners — it lives on
one node, and therefore so does any single large group within it (accepted for
v1; scale by partitioning the workload across multiple queues, ADR-008 / PRD
FR-13). Queues that need cross-group metadata-spanning claims must NOT use
whole-group/whole-cohort modes.

### Open topology decisions

A recurring singleton's `group_key` is its ordering key; the singleton lives on
the queue's single owner for its lifetime and re-arm never relocates it. If a
single workload genuinely needs BOTH per-`job_id` recurrence ordering AND
callback-cohort atomicity, those are two distinct queues (one non-cohort
recurring queue keyed by `job_id`, one cohort-enabled queue keyed by
`callback_id`); a single queue cannot carry both axes because `group_key` can
encode only one of them and metadata cannot replace a lost ordering/atomicity
key. That case MUST be confirmed with
Seventh Sense before migration and is recorded as an explicit confirmation item —
it is NOT silently resolved by moving one axis to metadata.

## Status

Accepted (status updated 2026-07-05; this ADR is load-bearing for the accepted ADR-008 cascade and is
implemented in code — treating it as still "proposed" was stale metadata).
