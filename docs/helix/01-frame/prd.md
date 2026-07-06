---
ddx:
  id: prd
  review:
    self_hash: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
    deps: {}
    reviewed_at: "2026-07-06T14:59:49Z"
kind: product
---

# Product Requirements Document

## Summary

pqueue is a durable priority queue engine for applications that need high-volume,
ordered, recoverable work execution. A queue defines its priority model,
ordering mode, progress bound, eligibility rules, and batching constraints.
Clients push and update items idempotently, workers claim eligible items under a
lease, and claimed items are finalized as complete, failed, retryable, or
released.

pqueue is also the transaction mapping layer for this centralized state-machine
workflow. The native interface is batch-centric, and every storage profile MUST
present the same external mutation contract: a successful response means the
mutation is durable and visible through subsequent reads/claims, a rejected
mutation has no durable effect, and an interrupted or timed-out mutation can be
resolved through `request_id` replay without duplicating state transitions.
Storage choices may change latency, cost, scale envelope, and recovery time; they
MUST NOT change transaction integrity.

The product is general-purpose and may become open source. Seventh Sense is the
first validation workload: several delivery, action, job, and connector queues
need timestamp-ordered execution, mutable schedules, batch processing,
idempotent writes, group-aware claims, and horizontal scale beyond a single
database at 10M-item queue sizes. The per-queue throughput floor is at least
10M items/hr, sustained for any queue at any deployment scale (see "Scale
Substantiation").

The top success measures are throughput, latency, and correctness. Every queue
sustains at least 10M items/hr with sub-second p95/p99 for core batch
operations; a horizontally distributed deployment spreads write and claim load by
placing queues across independent nodes to exceed any single deployment's ceiling
and to preserve that per-queue floor for every queue as the number of queues and
total load grow. A
single deployment supports at least 1000 concurrently active queues (single node
as the target host) with no cross-queue degradation, while still claiming every
eligible item before its queue-global progress bound.
Each measure references a recorded evidence artifact (see "Scale
Substantiation").

The primary high-scale value profile is local memory or SQLite serving
projections backed by a durable object log, giving Redis-level hot-path behavior
with object-store durability and queue count bounded by cluster capacity rather
than by one database. A Postgres log backend remains a lower-latency option with
different scaling and operational parameters, but it is not allowed to define a
different client contract.

## Problem and Goals

### Problem

High-volume async systems commonly split priority ordering, delayed execution,
idempotent ingest, retry, leases, batching, and observability across message
brokers, sorted sets, database tables, and worker-specific code. That creates
duplicated queue implementations and makes it hard to prove work is not lost,
claimed twice, delayed indefinitely, or processed in batches that downstream
APIs can accept.

Seventh Sense has this problem today across `jobs_scheduled_actions`,
`actions_scheduled`, `actions_queue`, `jobs_queue`, `connectors_queue`,
connector event chunks, and Marketo enrichment queues. The specific tables
differ, but they point to the same product need: a general durable priority
queue with timestamp ordering as a first-class validation case.

### Goals

1. Applications can define durable priority queues without hardcoding domain
   concepts into the queue engine.
2. Workers can claim the highest-priority eligible work, or bounded-relaxed work
   when scale requires it, without starving eligible items.
3. Producers and schedulers can write and update items idempotently at the
   single-deployment write rate in Success Metrics, and beyond it by distributing
   work across multiple queues.
4. Downstream API workers can claim, update, and finalize compatible batches
   efficiently.
5. Seventh Sense can replace or consolidate its scheduled delivery/action queues
   without losing timestamp scheduling, lifecycle safety, or operational
   visibility.
6. Operators can configure a commit-latency bound for durable-log profiles and
   understand the resulting tradeoff between latency, batch density, and backing
   store request cost.
7. Callers can depend on one transaction contract across all supported
   implementation combinations without knowing whether pqueue uses memory,
   SQLite, Postgres, or an object log internally.

### Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| Per-queue throughput floor | Every queue sustains at least 10M accepted items/hr (ingest and claim/finalize) under representative batch + idempotent-duplicate load | Single-deployment benchmark per the Tier-1 evidence record (TP-002 E1) against the per-queue throughput floor (TP-002 E0) |
| Throughput preserved at any scale | The ≥10M items/hr per-queue floor holds for any queue regardless of queue count or total deployment load; horizontal scale beyond a single deployment is achieved by distributing queues across nodes | Multi-queue scale-out + concurrency benchmark per the Tier-2 evidence record (TP-002 E2) |
| Queue density | At least 1000 concurrently active queues are supported, with a single node as the target host: every active queue meets its progress bound, there is no cross-queue degradation as the active-queue count grows to 1000, and any single queue can still reach the per-queue throughput floor when it is the hot queue. Aggregate single-node throughput is bounded by the node (not 1000× the floor); multi-node provides aggregate headroom | Multi-queue density benchmark per the Tier-2 evidence record (TP-002 E2) |
| Hot queue scale | At least 10M items resident in a single active queue (including terminal retained rows per retention policy) remain claimable and observable with sub-second p95/p99 on its single owning deployment | Benchmark per TP-002 E1 (single deployment) |
| Core operation latency | Sub-second p95 and p99 for batch push, batch update, batch claim, and batch finalize | Benchmark harness under representative Seventh Sense and synthetic workloads |
| External transaction integrity | 100% of supported implementation combinations satisfy the same success/error/unknown-outcome contract under retries, process crashes, projection rebuilds, and log replay | Backend conformance and fault-injection matrix per TP-003 |
| Commit latency and cost dial | Durable-log profiles publish latency, throughput, and object-store request-cost curves for the configured commit-latency bound | Object-log latency/cost matrix per TP-002 E3 |
| Progress bound compliance | 100% of eligible items claimed before their configured progress bound is exceeded | Queue metrics plus adversarial tests with skewed priority and group distributions |
| Claim safety | Zero concurrent active leases for the same item | Concurrency stress test with worker crashes and lease expiry |

### Scale Substantiation

Every scale claim in this PRD references an **evidence record** that names the
deployment shape, the workload envelope, and the design/test artifact plus
benchmark that substantiates it. This PRD names no storage backend, shard
mechanism, or query: those belong in the governing design and test documents.

- **Single-deployment envelope** - write/throughput/latency/correctness on one
  storage deployment, validated against the per-queue throughput floor (TP-002
  evidence record E0: at least 10M items/hr per queue) by the Tier-1 benchmark
  (E1).
- **Horizontal envelope** - the queue population distributed across nodes exceeds
  any single deployment's aggregate write/claim ceiling, and the ≥10M items/hr
  per-queue floor is preserved for every queue as the number of queues and total
  load grow (no cross-queue degradation), while still claiming every eligible item
  before its per-queue progress bound, validated by the multi-queue scale-out
  benchmark (E2). The queue-ownership and second-backend mechanisms that deliver
  this live in the design artifacts the technical context references.

Both envelopes are v1 commitments. A scale claim that cannot reference its
evidence record is not publishable.

### Non-Goals

- pqueue v1 will not hardcode Seventh Sense job, action, connector, quota,
  paused, suppressed, or campaign concepts into the core item model.
- pqueue v1 will not be a full workflow engine like Temporal.
- pqueue v1 will not require strict global priority ordering for every queue.
- pqueue v1 will not implement AMQP, Kafka, or SQS compatibility as the core
  data model. A Kafka producer wire adapter (ApiVersions/Metadata/Produce only,
  mapped to pqueue enqueue semantics) is in scope as P2 (ADR-005); consumer-side
  Kafka APIs are permanently out of scope.
- pqueue v1 will not prescribe a storage engine or shard implementation in the
  PRD.
- pqueue v1 does not enforce downstream API rate limits or quotas. Pacing work
  to a downstream system's accepted rate (for example a per-account,
  per-connector, or per-day send ceiling) is the responsibility of the calling
  worker. pqueue exposes claim-pacing controls (claim batch size, claim cadence,
  per-item `not_before`, and group selection) that a caller uses to pace itself;
  pqueue itself performs no rate admission and applies no downstream-rate token
  bucket.

## Users and Scope

### Primary Persona: Queue Platform Engineer

**Role**: Engineer operating pqueue for one or more applications

**Goals**: Define queues, scale throughput, preserve durability, observe
backlogs, and keep noisy workloads isolated.

**Pain Points**: Existing queue behavior is split across brokers, tables, and
worker code; correctness and latency are hard to reason about at 10M-item scale.

### Secondary Persona: Worker/Application Engineer

**Role**: Engineer producing and consuming queued work

**Goals**: Push items idempotently, update priority and metadata, claim
compatible batches, and finalize outcomes without designing a custom queue.

**Pain Points**: Downstream APIs require batches by account, connector, job,
campaign, domain, or other compatibility keys; FIFO queues do not provide that
shape directly.

### Validation Persona: Seventh Sense Delivery Engineer

**Role**: Engineer migrating Seventh Sense scheduled delivery and action queues

**Goals**: Preserve timestamp-prioritized delivery, mutable scheduling, retry,
pause/suppression gates, and job/account/connector observability.

**Pain Points**: Current scheduled and queue-like systems repeat similar
priority, retry, claim, and state logic with different table shapes.

## Requirements

### Must Have (P0)

1. General queue namespaces with isolation, routing, metrics, and independent
   scale behavior.
2. Queue-defined priority models, including timestamp ascending as a first-class
   model and at least one non-timestamp model.
3. Strict and bounded-relaxed ordering modes with mandatory progress guarantees.
4. Priority and eligibility as separate concepts.
5. Idempotent batch push and batch update, including priority updates before an
   item is terminal.
6. Batch claim and group-aware batch claim.
7. Durable claim leases with at-least-once execution and single active lease per
   item.
8. Batch finalize for complete, failed, retry, and release outcomes with
   per-item results.
9. Opaque payload and metadata with metadata-driven eligibility gates.
10. Observability for queue depth, lifecycle counts, leases, retries, oldest
    eligible age, and progress-bound risk.
11. Performance at 10M-item hot queue scale with sub-second p95/p99 for core
    batch operations under representative load: every queue sustains at least
    10M items/hr (the per-queue floor) on its single owning deployment, with that
    floor preserved for every queue at any deployment scale (horizontal scale is
    achieved by distributing queues across nodes) while still preserving the
    per-queue progress bound.
    Substantiated by the recorded scale evidence (see "Scale Substantiation").
12. Active-scope discovery (native service mode): a tenant-scoped read operation
    (`DiscoverActiveScopes`, API-001) returns the queues, and group keys within a
    queue, that currently have eligible work, ranked by oldest-eligible age -
    tenant-scoped top-N across queues when no queue is named, and queue-global
    when one queue is named - so a worker fleet can route
    claims for per-group fairness. Per-group fairness is a routing concern served
    by this operation, not an engine progress invariant; the engine guarantees
    only the single queue-global progress bound (FR-9/FR-12). Discovery is
    advisory for reservation - `BatchClaim` remains the authoritative selection
    path. Compatibility adapters MAY omit it and MUST document the omission.
13. The queue is the unit of sharding: each queue is owned by a single node.
    Write and claim load scale beyond one storage deployment by distributing
    queues across nodes; a producer needing more than one owner's throughput for
    a logical stream partitions it across multiple queues at the application
    layer. Single-active-lease, deterministic claim ordering, and the per-queue
    progress bound are preserved per queue.
14. Queue density: a single deployment supports at least 1000 concurrently active
    queues, with a single node as the target host, with no cross-queue
    degradation - every active queue meets its progress bound and any queue can
    still reach the per-queue throughput floor when it is the hot queue. This
    requires per-queue background work (lease-expiry sweeps, progress monitoring,
    summary recompute, recurring rearm, idempotency/retention GC) to be
    multiplexed onto bounded shared per-node resources, never one task, loop, or
    connection per queue.
    Aggregate single-node throughput is bounded by the node (not 1000x the
    per-queue floor); multi-node deployment provides aggregate headroom.
15. Backend-independent transaction contract: every supported implementation
    combination MUST preserve the same external semantics for batch mutation
    success, structured rejection, unknown retry resolution, idempotency replay,
    read-your-write visibility, claim exclusivity, and recovery from durable
    state. No caller may need backend-specific write, flush, replay, or repair
    choreography to preserve state-machine integrity.
16. Durable-log profiles MUST expose an operator-configurable commit-latency
    bound that controls group-commit cadence. Lower bounds reduce mutation
    latency and increase object-store/log request cost; higher bounds increase
    batch density and latency. The bound is a performance/cost dial only and
    MUST NOT weaken transaction integrity.

### Should Have (P1)

1. SQS-shaped API adapter for familiar send, receive, delete, visibility, delay,
   and batch semantics. The adapter cannot represent mutable priority or
   schedule updates and must remain secondary to the native pqueue API.
2. pqueue deployment-level rate limits, quotas, and tenant capacity controls
   (protecting the pqueue deployment from noisy tenants - not enforcing callers'
   downstream API limits, which are a permanent non-goal).
3. Dead-letter, redrive, and retention policies configurable per queue.
4. Operational repair actions for pause, unpause, reschedule, retry, fail,
   complete, and purge by queue scope.

### Nice to Have (P2)

1. Additional compatibility adapters, such as BullMQ-style or Faktory-style
   client APIs.
2. A hosted dashboard for queue inspection, repair, and trend analysis.
3. Optional bounded-relaxed ordering-quality metrics such as rank error.
4. Kafka producer wire adapter: ApiVersions/Metadata/Produce over heimq-wire,
   mapping Produce records to pqueue enqueue semantics. Consumer-side Kafka APIs
   are permanently out of scope (ADR-005).

## Functional Requirements

### Subsystem: Queue Definition

- **FR-1** - A queue is an isolated namespace with a stable identifier used for
  routing, scaling, metrics, and operational control.
- **FR-2** - A queue declares its priority model at creation, including priority
  value type, ordering direction, and deterministic tie-breaker.
- **FR-3** - Timestamp ascending is a first-class priority model.
- **FR-4** - At least one non-timestamp priority model is supported to validate
  that pqueue is not timestamp-only.
- **FR-5** - A queue declares its ordering mode at creation: strict or
  bounded-relaxed.
- **FR-6** - Queue priority model and ordering mode are immutable after creation
  unless a later migration design explicitly supports changing them.

### Subsystem: Priority and Progress

- **FR-7** - Strict queues claim eligible items according to priority key plus
  deterministic tie-breaker, computed over the claim's effective domain (the
  candidate set after the queue eligibility predicate and any caller
  group/metadata filters); when that domain is a single `group_key`, the order is
  exact per-group priority order (every group is co-resident on the queue's owner
  by construction — the queue is the unit of sharding).
  The progress bound (FR-9, FR-12) remains queue-global regardless of group
  filtering; group filtering does not create a per-group progress metric and
  never causes the engine to return items outside the caller's declared filter.
- **FR-8** - Bounded-relaxed queues may claim eligible items out of strict
  priority order to improve throughput.
- **FR-9** - Every queue declares a mandatory progress bound measured from the
  moment an item becomes eligible.
- **FR-10** - Ineligible items, including future-scheduled, gated, leased, or
  retry-backoff items, do not accrue progress-bound age while ineligible.
- **FR-11** - Lease expiry returns an item to eligibility without resetting its
  progress-bound clock.
- **FR-12** - The queue must claim eligible items before their progress bound is
  exceeded, regardless of priority relaxation or group-aware batching.
- **FR-13** - Ordering-quality bounds such as maximum rank error may be exposed,
  but they do not replace the mandatory progress bound.

### Subsystem: Eligibility and Metadata

- **FR-14** - Claims return the highest-priority eligible items under the
  queue's ordering and progress contract.
- **FR-15** - Eligibility is determined by lifecycle state, lease state,
  not-before timing, retry timing, and queue-defined metadata gates.
- **FR-16** - Items carry opaque caller-defined payload and metadata.
- **FR-17** - Metadata gates can prevent an otherwise high-priority item from
  being claimed, such as a paused or disabled domain state in the caller's
  application.

### Subsystem: Idempotent Ingest and Mutation

- **FR-18** - Clients can push one or more items idempotently using a
  caller-supplied logical item key.
- **FR-19** - Duplicate pushes for the same logical item key converge on one
  logical item according to documented conflict rules.
- **FR-20** - Clients can batch update priority, not-before timing, payload
  references, and metadata for non-terminal items.
- **FR-21** - Batch push and update return per-item results for accepted,
  duplicate, conflicted, rejected, and failed items.
- **FR-22** - The queue defines an idempotency retention window so deduplication
  state is bounded.

### Subsystem: Claim Leases and Lifecycle

- **FR-23** - Every accepted item is observable in exactly one lifecycle state:
  pending, in process, retry, complete, or failed, or an equivalent model that
  preserves those observable states. Recurring items (FR-49) cycle between
  pending and in-process indefinitely and reach a terminal state only on explicit
  terminal finalize or out-of-band purge; `recurrence.until` stops re-arming but
  does not by itself change lifecycle state.
- **FR-24** - A claim creates a lease that makes the item unavailable to other
  workers until finalized, released, or expired.
- **FR-25** - No item may have more than one active lease at a time.
- **FR-26** - If a worker does not finalize before lease expiry, the item becomes
  eligible for redelivery according to queue policy.
- **FR-27** - Accepted items, priority, metadata, lease state, and lifecycle
  state survive process and node restart.
- **FR-28** - The delivery guarantee is at-least-once execution with a single
  active lease; consumers remain responsible for idempotent side effects.

### Subsystem: Batch and Group Operations

- **FR-29** - Workers can claim up to a bounded number of eligible items in one
  batch.
- **FR-30** - Batch claim returns items in the queue's ordering mode and
  deterministic result order.
- **FR-31** - Group-aware batch claim can restrict results to items sharing a
  caller-defined compatibility key or metadata predicate.
- **FR-31a** - A group-aware batch claim can bound the number of distinct
  compatibility groups returned in one claim and return all currently-eligible
  items for each selected group as a whole unit, so workers can build batches
  sized by downstream group-cardinality limits (for example an API that accepts
  up to N distinct entities per call) rather than only by total item count. This
  mode requires the queue to be created with group co-residency and a per-group
  size cap (`max_eligible_group_size`).
- **FR-32** - Group-aware batch claim, including bounded multi-group
  (group-batching) claims, must not permanently favor one group or violate the
  queue's single queue-global progress bound for other eligible groups;
  multi-group selection orders groups by the queue's claim ordering and that
  progress bound, and skips only groups that are contended by another active
  claim. Per-group fairness across many groups is achieved by routing workers via
  active-scope discovery, not by a per-group engine invariant. There is no
  per-group progress metric.
- **FR-32a** - A queue MAY enable cohort claims (`cohort_policy`), in which a set
  of items sharing a `group_key` forms one cohort with a declared complete size.
  Cohort support is opt-in and requires group co-residency placement; it MUST NOT
  change semantics for queues that do not enable it.
- **FR-32b** - When cohort claims are enabled, no cohort member may be claimed by
  any claim unit until the cohort is complete AND every member is individually
  eligible; a whole-cohort claim MUST lease the entire cohort atomically under one
  lease or lease none of it. Cohort members are never individually claimable.
- **FR-32c** - A cohort that does not become complete-and-claim-eligible within
  its `completion_bound_ms` MUST be expired (all members terminal `failed`,
  `cohort-incomplete`); a partial cohort MUST NOT execute in v1. The queue-global
  progress bound (FR-9/FR-12) is unchanged; `completion_bound_ms` is a
  cohort-lifecycle timeout that `CreateQueue` MUST enforce to be
  `<= progress_bound_ms`, which preserves FR-12 for withheld eligible members.
- **FR-33** - Workers can batch finalize leased items as complete, failed,
  retryable with optional delay, or released.
- **FR-34** - Batch finalize returns per-item results, including stale lease,
  already terminal, validation failure, and success outcomes.
- **FR-35** - Queue or deployment configuration exposes maximum batch sizes and
  claim limits.

### Subsystem: Retry, Failure, and Retention

- **FR-36** - Retry outcome supports retry count, retry metadata, and not-before
  timing.
- **FR-37** - Queue policy defines when retryable items become terminal failed
  items.
- **FR-49** - A queue may be declared `recurring`; an item may be re-armed
  (returned to pending with a new caller-supplied eligibility time and optional
  priority) an unbounded number of times without becoming terminal failed.
- **FR-50** - Re-arm does not count against retry exhaustion (FR-37) and MUST
  reset the per-cycle transient-retry counter; only transient `retry` finalizes
  within a cycle are bounded by `max_attempts`.
- **FR-51** - A recurring item is a single logical item per logical key
  (singleton), reusing idempotent push convergence; pqueue does not create one
  item per cycle.
- **FR-52** - A recurring item terminates only via explicit terminal finalize,
  `recurrence.until` followed by terminal finalize/purge, or out-of-band
  `PurgeItems`. Idle backoff scheduling is caller-owned in v1.
- **FR-53** - Recurring inventory MUST be observable (idle recurring count vs
  active recurring count) and MUST NOT be counted as retry backlog or failures.
- **FR-54** - A re-armed recurring item is subject to the same queue-global
  progress bound (FR-9/FR-12) as any other eligible item once its eligibility
  time passes; recurrence introduces no per-group progress bound and no second
  eligibility rule.
- **FR-38** - Terminal complete and failed outcomes are durably recorded with
  lifecycle state, finalization metadata, failure code when present, and final
  command position.
- **FR-39** - Queue policy defines bounded retention for terminal item records
  and idempotency records so storage growth is controlled.

### Subsystem: Observability and Operations

- **FR-40** - The queue exposes counts by lifecycle state per queue.
- **FR-41** - The queue exposes oldest eligible age, current worst progress-bound
  risk, active leases, retry backlog, and terminal failure counts.
- **FR-42** - The queue exposes throughput and latency metrics for push, update,
  claim, finalize, retry, and lease expiry.
- **FR-43** - Queue isolation includes noisy-neighbor protection: one queue's
  load or backlog cannot prevent another queue from making progress within its
  configured limits.
- **FR-48** - In native service mode, pqueue exposes a tenant-scoped discovery
  read that enumerates the queues, and group keys within a queue, that currently
  have eligible work for the principal, ranked by oldest-eligible age. With no
  `queue_id` it ranks authorized queues (tenant-scoped top-N across queues); with
  one `queue_id` it ranks that queue's group keys. Discovery MUST use the same eligibility predicate as
  `BatchClaim` (the API-001 Eligibility Precedence subsection) and MUST be
  gate-current at read time. Discovery is the mechanism by which a worker fleet
  achieves per-group fairness; per-group fairness is NOT an engine progress
  invariant (the engine guarantees only the queue-global progress bound,
  FR-9/FR-12). Discovery results are advisory for reservation and MAY be
  approximate when documented; they MUST NOT lease, mutate, reserve, or guarantee
  that a subsequent claim succeeds. Discovery MUST only enumerate queues the
  principal is authorized to read, and MUST bound enumeration via pagination or a
  documented per-tenant queue ceiling.

### Subsystem: Seventh Sense Validation

- **FR-44** - pqueue can represent Seventh Sense scheduled delivery/action work
  using timestamp-ascending priority without embedding Seventh Sense-specific
  states in the core lifecycle.
- **FR-45** - Seventh Sense-specific pause, suppression, account, connector, job,
  and campaign controls are represented as eligibility predicates per the API-001
  Eligibility Precedence definition: static per-item conditions use
  `eligibility_policy.metadata_blockers`, and scope-level conditions that block or
  reopen many items at once (for example an account or connector disabled) use
  dynamic gate keys via `SetGates`. Downstream delivery-pacing and quota
  constraints are **not** core queue features and are **not** eligibility
  conditions: callers pace claims using claim batch size, claim cadence,
  `not_before`, and group selection (see Non-Goals). pqueue does not model a
  downstream rate or quota as a lifecycle state or claim-time admission control.
- **FR-46** - pqueue can support the existing Seventh Sense need to ingest work
  quickly and update scheduled time later.
- **FR-47** - pqueue can claim batches compatible with downstream Seventh Sense
  API constraints such as account, connector, job, campaign, or external batch
  key.
- **FR-47a** - pqueue can represent Seventh Sense work by mapping its nesting
  onto the four client-visible axes (ADR-004): `tenant_id` for the
  account/isolation boundary, `queue_id` per logical stream, `group_key` for the
  per-queue ordering/atomicity key (`job_id` for non-cohort scheduled-action
  queues; `callback_id` for cohort-enabled callback queues — co-resident on the
  queue's owner by construction, so per-group order and cohort atomicity hold), and
  `account`, `connector`, `campaign` as `metadata`. The scheduled timestamp maps
  to timestamp-ascending `priority`. Progress is enforced queue-globally; per-job
  or per-callback fairness is a worker-routing concern via active-scope discovery.
- **FR-47b** - pqueue can claim batches bounded by a downstream per-call entity
  limit, such as the Marketo lead-enrichment constraint of up to 300 distinct
  leads per API call, returning all currently-eligible tasks for the selected
  leads as whole groups in one atomic claim.
- **FR-47c** - pqueue can represent the Seventh Sense `actions_scheduled`
  `batch_checksum` / `callback_id` "execute the complete batch together"
  requirement as an opt-in cohort claim, without embedding `callback_id`
  semantics in the core lifecycle.
- **FR-55** - pqueue can represent Seventh Sense `jobs_queue` and
  `connectors_queue` singleton recurring rows (one per job/connector key,
  re-armed via next-processing time, never terminal until the underlying entity
  is removed via `PurgeItems`) using recurring-queue primitives, without
  embedding job/connector concepts in the core model.

## Acceptance Test Sketches

| Requirement | Scenario | Input | Expected Output |
|-------------|----------|-------|-----------------|
| FR-2, FR-3 | Create timestamp queue | Queue definition with timestamp ascending priority | Queue accepts timestamp priorities and orders eligible claims by timestamp plus tie-breaker |
| FR-7 | Strict ordering | Three eligible items with priorities 3, 1, 2 | Claim returns priority 1 before 2 before 3 |
| FR-9, FR-12 | Progress bound | Relaxed queue with one eligible item repeatedly bypassed by newer higher-priority work | Item is claimed before configured progress bound is exceeded |
| FR-18, FR-19 | Idempotent push | Same logical item key pushed twice in a batch retry | One logical item exists; response reports duplicate/converged result |
| FR-20 | Mutable priority | Item ingested without final schedule, then updated with timestamp priority | Item becomes claimable according to updated priority and eligibility |
| FR-24, FR-26 | Lease recovery | Worker claims item and crashes before finalize | Item is invisible during lease and eligible again after lease expiry |
| FR-29, FR-31 | Group-aware batch claim | Eligible items across groups A and B, worker requests group-compatible batch | Claim returns compatible items from one allowed group without breaking progress bound |
| FR-33, FR-34 | Batch finalize | Batch includes valid lease, stale lease, and already terminal item | Response reports per-item success or reason |
| FR-43 | Queue isolation | One queue receives sustained 10M-item backlog while another has small eligible backlog | Small queue continues to meet claim latency and progress bounds |
| FR-44, FR-46 | Seventh Sense validation | Delivery item created before optimized send time, then scheduled later | Item is accepted, updated, and claimed according to timestamp priority |
| API-001 | Request ID conflict | Same `request_id` is reused with a different batch body | Request fails with `request-id-conflict` |
| API-001 | Optimistic update conflict | Batch update includes stale `expected_item_version` | Item result reports `conflict` without mutating item |
| API-001 | Leased update conflict | Client attempts `BatchUpdate` on a leased item | Item result reports `conflict`; worker must renew or finalize |
| API-001 | Claim retry idempotency | Client retries `BatchClaim` with same `request_id` while leases are active | Response returns the same claimed set and lease tokens |
| API-001 | Tenant spoofing rejection | HTTP principal authorized for tenant A calls route for tenant B | Request fails as forbidden or not found |
| API-001 | SQS adapter limitation | Client attempts to update priority through SQS-shaped adapter | Adapter rejects or documents unsupported operation; native `BatchUpdate` is required |
| FR-48 | Active-scope ranking across queues | Three queues authorized to caller: A empty, B oldest-eligible 9s, C oldest-eligible 30s | Discovery (no `queue_id`) returns C then B (oldest first); A omitted |
| FR-48 | Queue-global group ranking | One queue; group `g_old` (oldest-eligible 40s) and group `g_new` (oldest-eligible 5s) | Group discovery returns `g_old` before `g_new` in true queue-global oldest-first order |
| FR-48 | Auth filtering | Tenant has queues B (authorized) and D (not authorized), both with eligible work | Discovery returns B only; D never appears and presence is not leaked |
| FR-48 | Gate-current advance (not just exclude) | Group `g` has oldest item `i1` whose gate key is `blocked` and a next item `i2` (eligible, age 12s) | Discovery reports `g` with `oldest_eligible_age_ms` = 12s (`i1` skipped, `g` NOT omitted); blocking all of `g`'s items omits `g` |
| FR-48 | Eligibility parity under cohort | Incomplete cohort exists | Discovery does not report the cohort group as eligible until complete |
| FR-48 | Eligibility parity under rearm | Recurring item is idle (disarmed) | Discovery omits it; after rearm it appears |
| FR-48, FR-47 | Discover-then-claim fairness | Worker discovers oldest active group, then `BatchClaim` that `group_key` | Claim returns items from the reported group; a group drained after discovery returns empty without error |
| FR-31a, FR-47b | Group-cardinality claim | Eligible items across 1,000 co-resident groups; claim with `group_batching.max_groups=300` | Claim returns all eligible items for the 300 highest-claim-order wholly-available groups, total <= max_items, no group partially returned |
| FR-45, Non-Goals | No downstream rate enforcement | Queue with many eligible items for one `group_key`; worker claims with `max_items=25` then pauses between calls | pqueue applies no rate-based throttling: each `BatchClaim` returns up to 25 items subject only to normal eligibility and `max_items` (a short or empty batch is still valid per API-001), never withholding work for a downstream-rate reason; pacing is determined entirely by the worker's `max_items` and call cadence |
| FR-49, FR-50 | Perpetual re-arm | Recurring item claimed and `rearm`-finalized far more than `max_attempts` times | Never terminal; stays in pending/leased cycle |
| FR-50 | Per-cycle retry budget | Within one cycle, `retry` up to `max_attempts` then once more | Item terminal `failed` for that cycle; a prior `rearm` had reset the counter |
| FR-51, FR-55 | Singleton recurring | Same `client_item_key` pushed/claimed/re-armed across many cycles | Exactly one logical item; `item_version` increments per `rearm` |
| FR-52 | until stops scheduling | `rearm` after `recurrence.until` | Per-item `terminal`; lifecycle unchanged until terminal finalize/purge |
| FR-52 | Purge teardown | `PurgeItems` for a recurring key (and with `force` while leased) | Per-item `purged`; row removed; terminal command position + tombstone recorded |
| FR-52 | Purge replay + late finalize | Duplicate purge `request_id`; then a finalize for the purged item | Duplicate returns recorded `purged`/`not_found` (no re-delete); late finalize returns `not_found` |
| FR-53 | Idle metrics | Idle re-armed recurring item | Counted in `recurring_pending`; not in `retry_backlog` or `failed`; not contributing to `oldest_eligible_age_ms` |
| FR-54 | Progress parity | Re-armed item whose `not_before` has passed, repeatedly bypassed | Claimed before queue-global `progress_bound_ms` is exceeded (measured from `max(commit_time, not_before)`) |
| API-001 | Rearm on oneshot | `rearm` against a `oneshot` queue | Per-item `invalid`; item unchanged |
| API-001 | Rearm replay | Duplicate `request_id` for a `rearm` | Same recorded `not_before`/`eligible_since`/priority/version returned; no recompute |
| API-001 | Eligible-instant determinism | `rearm` with `not_before` in the past vs future | `eligible_since = max(commit_time, not_before)` in both cases; replay returns the same instant |

## Technical Context

This PRD records product requirements only. Storage engine, shard strategy,
indexing, protocol, and deployment topology belong in later technical design.

Reference systems and interfaces to study:

- Meta FOQS for distributed priority queues with priority, delay, leases,
  ack/nack, metadata, TTL, and massive backlog scale:
  <https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/>
- Amazon SQS for visibility timeout, at-least-once delivery, batch receive, and
  FIFO group semantics:
  <https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html>
- PGMQ for an open-source SQS-like Postgres queue interface:
  <https://github.com/pgmq/pgmq>
- BullMQ, Faktory, and Asynq for developer-facing job queue APIs, retries,
  delayed work, and observability:
  <https://bullmq.io/>,
  <https://github.com/contribsys/faktory>,
  <https://github.com/hibiken/asynq>
- Research on relaxed concurrent priority queues, especially MultiQueue and
  k-LSM, for ordering-quality and throughput tradeoffs:
  <https://arxiv.org/abs/2107.01350>,
  <https://arxiv.org/abs/1503.05698>

## Constraints, Assumptions, Dependencies

### Constraints

- **Business**: The first validation workload is Seventh Sense scheduled
  delivery/action execution.
- **Technical**: pqueue must support durable claims and batch operations without
  requiring exact global ordering for every queue.
- **Technical**: Downstream API rate/quota enforcement is out of the pqueue
  engine. The claim path MUST NOT contain a downstream-rate admission stage;
  callers pace via claim batch size, claim cadence, `not_before`, and group
  selection.
- **Product**: Seventh Sense-specific concepts must remain outside the core
  model unless they are necessary for generic durable priority queue semantics.

### Assumptions

- Queue priority type, ordering direction, ordering mode, and progress bound can
  be fixed at queue creation for v1.
- At-least-once execution with single active lease is the correct durability
  contract; exactly-once side effects are caller-owned.
- Batch and group-aware operations are required for the first useful release.
- Timestamp priority uses producer-supplied scheduled time; producers are
  responsible for the business meaning of that timestamp.

### Dependencies

- Seventh Sense production workload data for realistic load profiles, group
  distributions, priority skew, and downstream API batch constraints.
- The committed v1 technical designs for storage backends (including the second,
  higher-scale backend) and queue ownership/assignment/fencing/rebalance across
  nodes. These substantiate the horizontal envelope in Success Metrics.
- API-001 for native pqueue operations. SQS-shaped compatibility remains a
  later adapter, not the native contract.
- A later operator/retention contract for P1 redrive, purge, archive, and
  administrative repair APIs.

## Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Strict priority creates a scalability bottleneck | High | High | Support bounded-relaxed ordering with mandatory progress bounds |
| Relaxed ordering misses business scheduling expectations | Medium | High | Require per-queue progress bounds and validate timestamp queues against Seventh Sense scheduling SLA |
| Group-aware claims starve other groups | Medium | High | Make progress bounds override group preference |
| Idempotency state grows without bound | Medium | Medium | Require a documented idempotency retention window |
| Generic model becomes too Seventh Sense-specific | Medium | High | Keep Seventh Sense states in metadata and validation, not core lifecycle |
| Performance tests use unrealistic uniform workloads | High | Medium | Include skewed priority, future-scheduled, leased, retry, and group-heavy test profiles |

## Resolved API Decisions

- v1 progress-bound metrics use eligible age: `oldest_eligible_age_ms` and
  `progress_bound_risk_count` count or estimate eligible items near
  `progress_bound_ms`. Bypass count and rank-error metrics remain optional
  later ordering-quality metrics.
- Retry is represented as `pending` with retry metadata and `not_before`, while
  remaining observable as retry.
- Native batch operations are best-effort with per-item results. `CreateQueue`
  and each returned claim lease are atomic. Two opt-in claim modes are
  additionally all-or-nothing: the whole-eligible-group claim mode
  (`group_batching`) leases each selected group as an all-or-nothing unit, and
  the whole-cohort claim mode (`whole_cohort=true` on cohort-enabled queues)
  leases one complete cohort under a shared cohort lease, all-or-nothing. These
  are the explicit all-or-nothing claim modes v1 supports for group-aware claims;
  no other v1 all-or-nothing batch mode is required.
- The first client surface is the native pqueue API defined in API-001. An
  SQS-shaped adapter is P1 compatibility work and cannot represent mutable
  priority or schedule updates.
- v1 scale is committed in two envelopes - single-deployment (validated against
  the per-queue throughput floor of at least 10M items/hr per queue) and
  horizontal cross-queue scale-out (which preserves that floor for every queue at
  any scale) - each referencing a recorded evidence artifact (see "Scale
  Substantiation"). The PRD states product envelopes only; storage backend and
  placement mechanism live in the governing design/test artifacts.
- Downstream API rate/quota enforcement is a non-goal of the pqueue engine, not a
  deferred feature. Callers pace claim output using `max_items`, claim cadence,
  `not_before`, and group selection (`compatibility.group_key` / `same_group_key`
  / `metadata_equals` / `group_batching.max_groups`). The `rate_limited` item
  status and the rate-limit error row are reserved exclusively for pqueue's own
  deployment/tenant capacity controls (P1), not downstream-system pacing.
- Recurring (never-terminal) items are a first-class queue mode. A `recurring`
  queue accepts a `rearm` finalize outcome that returns the item to pending with
  a caller-supplied `not_before` (effective eligible instant
  `max(commit_time, not_before)`), does not count against `max_attempts`, and
  resets the per-cycle retry counter. A re-armed item obeys the single
  queue-global progress bound once eligible; recurrence adds no per-group progress
  bound. Engine-maintained idle backoff is out of scope for v1 (caller owns
  backoff math). Termination is via terminal finalize, `recurrence.until` + drain,
  or targeted in-band `PurgeItems` (distinct from the P1 operator purge contract).
- The Seventh Sense `jobs_queue`/`connectors_queue` poll-cursor pattern (one
  durable row per pollable resource, claimed oldest-`last_processed_at` first) is
  expressible as a timestamp-ascending queue where `priority = last_processed_at`,
  `not_before = next_processing_at`, and `group_key = type` only once recurring
  rearm and dynamic gates land; without those, the rows would exhaust retries, go
  terminal, and mis-gate. `DiscoverActiveScopes` is the separate operation for
  choosing which queue/group to service; it is the per-group fairness mechanism,
  not a claim path. Per-group fairness is achieved by routing via discovery, not
  by a per-group engine progress invariant.

## External Validation Inputs

- Seventh Sense operators must confirm (1) the production scheduling SLA used to
  choose `progress_bound_ms` per queue, and (2) whether any single queue requires
  BOTH per-`job_id` ordering AND callback-cohort atomicity simultaneously. Per
  ADR-004, a single `group_key` is the ordering/atomicity key; a queue needing
  both keys must be split (one keyed by `job_id`, one by `callback_id`) because
  metadata cannot carry a placement/atomicity key. This confirmation gates the
  migration topology but does NOT block generic pqueue implementation, since
  `group_key` topology and `progress_bound_ms` are per-queue configuration.

## Success Criteria

pqueue is successful when a general-purpose queue can be created with a
configured priority model, accepts and updates work idempotently at production
scale, claims eligible work in strict or bounded-relaxed order without
starvation, survives worker failure through leases, and supports group-aware
batch execution. For Seventh Sense, success means scheduled delivery/action work
can move to pqueue without losing timestamp scheduling correctness, operational
visibility, or throughput.
