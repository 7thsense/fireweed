---
ddx:
  id: api-workload-integration-profiles
  depends_on:
    - prd
    - api-native-client-interface
    - api-operator-repair-contract
  review:
    self_hash: dc9e97f201ac546ad838d120811aa790826aee707b6870c4396c2f97d1ba81a8
    deps:
      api-native-client-interface: 6b76e5c4c37c91d40e8d5229d9eeae516f71385aa06e856fb41a4a19ee5856e8
      api-operator-repair-contract: 65ec2e36500a6c404ae53af1a65da26fcdcc0a07e0ef1578bae30ec94f2be6e6
      prd: 382115039de93226b051a09e719c7e1c50f12563d96c1ba85ef142c0ae5d0ce0
    reviewed_at: "2026-06-22T18:57:24Z"
---

# Contract

**Contract ID**: API-003
**Type**: integration guidance / adapter contract
**Version**: v1
**Status**: complete
**Related**: API-001, API-002, PRD

## Purpose

This contract defines **Application Workload Integration Profiles**: prescriptive
guidance for how an application delivery engine should use the native pqueue
primitives defined in API-001 (and the operator surface in API-002) to run a
real workload. It is **not** a new queue primitive and adds **no** new engine
behavior, fields, options, or semantics. Every obligation here resolves to an
existing API-001/API-002 operation.

A profile is normative for the **adapter author** (the integrator wiring an
application onto pqueue) and advisory for everyone else. It must be executable by
an adapter author who has no access to any external chat, ticket, or
product-specific design doc. Where this contract says an adapter "MUST" do
something, it constrains the adapter, not the pqueue engine.

## Scope and Boundaries

In scope:

- How to configure a queue for a workload shape.
- The division of responsibility between the producing side, the claiming
  (worker) side, and the operator surface.
- How to map application delivery results onto pqueue's existing finalize
  outcomes.
- How to use dynamic gates and `not_before` for caller-driven pacing.
- Where archive/retention responsibilities live.

Explicitly out of scope (these remain caller-owned application concerns and MUST
NOT be pushed into pqueue):

- Provider/transport-specific APIs (no SES-, 7snx-, or Seventh-Sense-specific
  surface).
- Downstream API rate limiting or quota enforcement (see the non-goal below).
- Delivery state machines, sender readiness, suppression decisions, provider
  result normalization, and provider idempotency keys.
- Any change to API-002 archive/retention behavior beyond referencing it.

Domain neutrality: profiles MAY use generic illustrative nouns such as *tenant*,
*sender*, *domain*, *provider*, or *batch compatibility key*, but every such noun
is **opaque, caller-owned** data carried in `metadata`, `group_key`, or
`gate_keys`. pqueue never interprets these values (API-001 Common Types).

## Preserved Non-Goal: Downstream Rate Limits

pqueue does **not** enforce downstream API rate limits and applies no rate
admission to claims. A workload paces downstream calls itself, using the native
levers (PRD non-goal; API-001 "Caller-driven downstream pacing"):

- `max_items` and claim cadence (how many, how often a worker claims),
- `not_before` (defer an item until a wall-clock time),
- `group_key` / group selection (claim one compatibility partition at a time),
- retries (`retry` finalize with a caller-computed backoff `not_before`),
- caller-owned **dynamic gates** (`SetGates`) to pause/resume a scope.

A profile MUST restate this boundary and MUST NOT describe pqueue as throttling,
shaping, or admitting traffic by downstream capacity.

---

## Profile: Scheduled Batch Delivery

This first profile covers a **scheduled batch delivery** workload generically: an
application enqueues units of work that become due at a scheduled time, and a
fleet of workers claims due work, calls some downstream provider, and reports the
result. It is intentionally provider-neutral.

### Queue Creation Recommendations

The adapter SHOULD create one queue per logical delivery stream with:

- `priority_model`: `timestamp` ascending, so due time orders work
  (earliest-due first). Use `priority` to carry the scheduled time.
- `ordering_mode`: `bounded_relaxed` for throughput, or `strict` when strict
  per-domain order is required. Order is always evaluated over the effective
  claim domain (API-001 Batch Claim).
- `progress_bound_ms`: set to the workload's freshness SLA. Progress is
  **queue-global**; the profile MUST NOT assume a per-group or per-domain SLA
  (ADR-004; PRD FR-9/FR-12).
- `group_co_residency`: `true` **only if** the workload claims whole
  compatibility batches (e.g. "all work for one *sender*+*provider* together")
  via `compatibility.group_batching`; in that case also set
  `max_eligible_group_size`. Otherwise leave it `false` and treat `group_key` as
  an optional opaque routing/ordering hint. `group_key` carries no progress
  meaning either way.
- `eligibility_policy.gate_keys`: `dynamic` if the workload needs to pause a
  scope (a sender, a provider, a tenant) without deleting work; `none`
  otherwise.
- `retry_policy`: a `max_attempts` consistent with the application's
  redelivery policy.

The example nouns above (*sender*, *provider*, *domain*) are illustrative
caller-owned keys, not pqueue concepts.

### Producer Obligations

The producing side (the scheduler/ingest) MUST:

1. Enqueue each unit with `BatchPush`, using a stable `client_item_key` so
   duplicate submissions converge by key (API-001 idempotency). The producer
   MUST NOT rely on pqueue to dedup by payload content.
2. Carry the scheduled due time in `priority` (timestamp model) and, when work
   must not be claimable before a wall-clock instant, also set `not_before`.
   `not_before` and `priority` are distinct (API-001 Common Types).
3. Put any caller-owned routing/compatibility values (tenant, sender, domain,
   provider, campaign) in `metadata` and, when batch compatibility is needed, in
   `group_key`. These are opaque to pqueue.
4. Declare `gate_keys` on items only when the queue is `gate_keys = dynamic`,
   to make those items pausable as a scope.
5. Reschedule still-pending work with `BatchUpdate` (priority / `not_before` /
   metadata) rather than re-pushing; `BatchUpdate` applies to pending items only.

### Worker Obligations

The claiming side (the delivery workers) MUST:

1. Claim due work with `BatchClaim`, choosing `max_items` and claim cadence to
   pace downstream calls (this is the workload's rate control, not pqueue's).
2. Treat the claimed item using the **Claimed Item Response Shape** (API-001):
   correlate via `client_item_key`, read the `payload`, apply caller `metadata`,
   and use `lease_token` + `lease_expires_at` to bound processing time.
3. Renew with `BatchRenewLeases` before `lease_expires_at` for long downstream
   calls; never exceed the lease without renewing.
4. For whole-batch workloads, claim with `compatibility.group_batching` (whole
   eligible groups, atomic per group) and process each returned group as a unit;
   for atomic cohorts, use `compatibility.whole_cohort` and finalize through the
   shared `cohort_lease_token`.
5. Finalize every claimed item exactly once via `BatchFinalize` (see mapping).

### Finalize Outcome Mapping

A profile MUST map application delivery results onto the **existing five**
finalize outcomes only (API-001 `BatchFinalize`); it MUST NOT invent new
outcomes, delivery states, or provider-result semantics:

| Application result | Finalize outcome | Notes |
|--------------------|------------------|-------|
| Delivered / accepted by provider | `complete` | Terminal success. |
| Permanent rejection (won't succeed on retry) | `fail` | Terminal failure; carry a caller-defined `failure_code` if used. |
| Transient failure (provider 5xx, timeout, throttled-by-provider) | `retry` | Set a caller-computed backoff via `not_before`; pqueue does not compute backoff and does not interpret provider status. |
| Worker cannot process now but item should stay claimable for others | `release` | Returns the item to eligible without consuming an attempt beyond policy. |
| Recurring/periodic unit that should re-arm for its next cycle | `rearm` | Only on queues with `recurrence` configured; re-arms without terminating. |

"Throttled by the provider" maps to `retry` with a caller-chosen `not_before` —
it is an application backoff decision, never a pqueue rate-admission decision.

### Dynamic Gate Usage

When the queue is `eligibility_policy.gate_keys = dynamic`, the adapter MAY pause
and resume a scope (e.g. all work for one sender or provider) by blocking and
unblocking a caller-owned gate key via the operator/control `SetGates` operation
(API-001 Eligibility Precedence). While a gate key is `blocked`, items carrying
it are ineligible for claim and do not accrue progress-bound age beyond policy.
This is a pause switch, **not** a rate limiter: it is binary (blocked/unblocked)
and carries no per-second/per-quota semantics. For paced throttling, use
`not_before` and claim cadence instead.

### Archive and Retention Boundaries

Lifecycle cleanup is owned by the operator surface, not by this profile:

- Bounded, per-key removal of a specific item uses native `PurgeItems`
  (API-001).
- Bulk archival and retention — `ArchiveItems`, `RunRetention`, and selector
  `PurgeQueueItems` — are **privileged operator operations defined in API-002**.
  A workload adapter MUST NOT re-implement archive/retention semantics; it
  references API-002 and lets operators run retention within policy.

This contract does not change any API-002 behavior; it only points to it.

---

## Profile: Embedded Engine Integration

This profile covers **embedded mode** (ADR-006): a host application links
`pqueue-core` + `pqueue-storage` in-process and drives the engine directly,
rather than calling the API-001 client operations over HTTP/SDK. It is the mode a
same-process delivery host uses for in-process latency and control. The surface
it binds (`pqueue-core`; `pqueue-storage::{traits, commands, types}`) is the
public, versioned embedding contract declared in ADR-006.

### Backend selection (durability is mandatory)

The embedder constructs a backend and MUST use a **durable** one in production —
`postgres_native`, `object_log_sqlite_projection`, or standalone `sqlite`
(ADR-006). The in-memory backend (`pqueue_storage::memory`) is dev/test only: it
has no durable ack boundary and loses all enqueued/leased/finalized state on
restart. An embedded host MUST NOT back production delivery work with it.

### Driving the engine

The embedder maps each workload action onto a native `QueueCommand` and drives
the same log → projection → claim loop the service would:

1. **Create queue** via `ControlPlaneStore::create_queue` with a validated
   `QueueDefinition` (queue-creation recommendations from the Scheduled Batch
   Delivery profile apply unchanged).
2. **Enqueue / update / finalize**: build the matching `QueueCommand`
   (`BatchPush` / `BatchUpdate` / `BatchFinalize`, etc.) in a `CommandEnvelope`,
   `LogStore::append_batch` it to obtain a durable `CommandPosition`, then
   `ProjectionStore::apply_committed` that committed page. Append is the durable
   ack boundary; an embedder MUST treat work acknowledged only after
   `append_batch` returns on a durable backend.
3. **Claim** via `ProjectionStore::batch_claim` (`ClaimRequest`), which applies
   the single Eligibility Precedence and returns the claimed-item set (API-001
   "Claimed Item Response Shape").

Producer/worker obligations, the **finalize-outcome mapping** (the five outcomes
only), the **dynamic-gate** usage, and the **downstream-rate non-goal** are
identical to the Scheduled Batch Delivery profile above — this profile changes
only *where the boundary is bound* (storage traits in-process vs API-001
operations), not the semantics. The embedder MUST NOT reinterpret command or
finalize semantics; it constructs native commands and lets the engine apply them.

### Conformance

An embedded delivery adapter MUST pass pqueue's published **embedder delivery
adapter conformance** suite (ADR-006 §5): push/claim/finalize, duplicate
convergence by `client_item_key`, retry/expired-lease re-pending, and
terminal-failure semantics through the embedded surface. A host's own adapter
conformance test maps to this suite rather than redefining the guarantees.

---

## Precedence and Compatibility

- This contract is **subordinate** to API-001 and API-002: if any guidance here
  appears to conflict with an API-001/API-002 normative rule, the API-001/API-002
  rule governs and this profile is in error.
- A profile never introduces a field, option, outcome, or operation absent from
  API-001/API-002. New workload shapes are added as **new profiles** under this
  contract, never as new engine primitives.

## Validation Checklist

- [x] Profile is provider-neutral; all domain nouns are opaque caller-owned
  `metadata`/`group_key`/`gate_keys` (no SES/7snx/Seventh-Sense surface).
- [x] Preserves the downstream-rate non-goal; pacing is caller-driven via
  `max_items`, claim cadence, `not_before`, group selection, retries, and gates.
- [x] Finalize mapping uses only the five existing outcomes
  (`complete`/`fail`/`retry`/`release`/`rearm`).
- [x] Producer/worker obligations reference only existing API-001 operations.
- [x] Archive/retention defers to API-002; no behavior change.
- [x] Executable by an adapter author with no external context.
