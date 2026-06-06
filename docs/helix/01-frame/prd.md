---
ddx:
  id: prd
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

The product is general-purpose and may become open source. Seventh Sense is the
first validation workload: several delivery, action, job, and connector queues
need timestamp-ordered execution, mutable schedules, batch processing,
idempotent writes, group-aware claims, and horizontal scale at 10M-item queue
sizes.

The top success measures are throughput, latency, and correctness: millions of
writes per hour per deployment, sub-second p95 and p99 for core batch
operations under representative load, and no eligible item starved beyond its
configured progress bound.

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
3. Producers and schedulers can write and update items idempotently at millions
   of writes per hour.
4. Downstream API workers can claim, update, and finalize compatible batches
   efficiently.
5. Seventh Sense can replace or consolidate its scheduled delivery/action queues
   without losing timestamp scheduling, lifecycle safety, or operational
   visibility.

### Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| Write throughput | Millions of accepted item writes per hour per deployment | Load test with batch push and idempotent duplicate writes |
| Hot queue scale | At least 10M items in a single active queue | Load test with mixed eligible, future, leased, retry, and terminal items |
| Core operation latency | Sub-second p95 and p99 for batch push, batch update, batch claim, and batch finalize | Benchmark harness under representative Seventh Sense and synthetic workloads |
| Progress bound compliance | 100% of eligible items claimed before their configured progress bound is exceeded | Queue metrics plus adversarial tests with skewed priority and group distributions |
| Claim safety | Zero concurrent active leases for the same item | Concurrency stress test with worker crashes and lease expiry |

### Non-Goals

- pqueue v1 will not hardcode Seventh Sense job, action, connector, quota,
  paused, suppressed, or campaign concepts into the core item model.
- pqueue v1 will not be a full workflow engine like Temporal.
- pqueue v1 will not require strict global priority ordering for every queue.
- pqueue v1 will not implement AMQP, Kafka, or SQS compatibility as the core
  data model.
- pqueue v1 will not prescribe a storage engine or shard implementation in the
  PRD.

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
11. Performance at 10M-item hot queue scale with millions of writes per hour and
    sub-second p95/p99 core operation latency under representative load.

### Should Have (P1)

1. SQS-shaped API adapter for familiar send, receive, delete, visibility, delay,
   and batch semantics.
2. Queue-level rate limits, quotas, and tenant capacity controls.
3. Dead-letter, redrive, and retention policies configurable per queue.
4. Operational repair actions for pause, unpause, reschedule, retry, fail,
   complete, and purge by queue scope.
5. Active-queue discovery for workers that need to find queues with eligible
   work.

### Nice to Have (P2)

1. Additional compatibility adapters, such as BullMQ-style or Faktory-style
   client APIs.
2. A hosted dashboard for queue inspection, repair, and trend analysis.
3. Optional bounded-relaxed ordering-quality metrics such as rank error.

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
  deterministic tie-breaker.
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
  preserves those observable states.
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
- **FR-32** - Group-aware batch claim must not permanently favor one group or
  violate the queue's progress bound for other eligible groups.
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
- **FR-38** - Terminal failed items are inspectable and optionally redrivable by
  authorized operators or clients.
- **FR-39** - Terminal complete and failed items can be retained, archived, or
  purged according to queue policy.

### Subsystem: Observability and Operations

- **FR-40** - The queue exposes counts by lifecycle state per queue.
- **FR-41** - The queue exposes oldest eligible age, current worst progress-bound
  risk, active leases, retry backlog, and terminal failure counts.
- **FR-42** - The queue exposes throughput and latency metrics for push, update,
  claim, finalize, retry, and lease expiry.
- **FR-43** - Queue isolation includes noisy-neighbor protection: one queue's
  load or backlog cannot prevent another queue from making progress within its
  configured limits.

### Subsystem: Seventh Sense Validation

- **FR-44** - pqueue can represent Seventh Sense scheduled delivery/action work
  using timestamp-ascending priority without embedding Seventh Sense-specific
  states in the core lifecycle.
- **FR-45** - Seventh Sense-specific pause, suppression, quota, account,
  connector, job, and campaign controls are represented as metadata and
  eligibility gates.
- **FR-46** - pqueue can support the existing Seventh Sense need to ingest work
  quickly and update scheduled time later.
- **FR-47** - pqueue can claim batches compatible with downstream Seventh Sense
  API constraints such as account, connector, job, campaign, or external batch
  key.

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
- A later technical design for storage, shard ownership, indexes, and lease
  recovery.
- A later API design for native pqueue operations and any SQS-shaped adapter.

## Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Strict priority creates a scalability bottleneck | High | High | Support bounded-relaxed ordering with mandatory progress bounds |
| Relaxed ordering misses business scheduling expectations | Medium | High | Require per-queue progress bounds and validate timestamp queues against Seventh Sense scheduling SLA |
| Group-aware claims starve other groups | Medium | High | Make progress bounds override group preference |
| Idempotency state grows without bound | Medium | Medium | Require a documented idempotency retention window |
| Generic model becomes too Seventh Sense-specific | Medium | High | Keep Seventh Sense states in metadata and validation, not core lifecycle |
| Performance tests use unrealistic uniform workloads | High | Medium | Include skewed priority, future-scheduled, leased, retry, and group-heavy test profiles |

## Open Questions

- [ ] What exact progress-bound metric should v1 expose: maximum eligible age,
  maximum bypass count, maximum delay, or a combination? - blocks ordering-mode
  feature spec, ask product and technical design reviewers.
- [ ] Should retry be represented internally as a distinct lifecycle state or as
  pending with retry metadata while remaining observable as retry? - blocks data
  model design, ask technical design reviewers.
- [ ] Are batch operations best-effort with per-item results, or are any
  operations required to be all-or-nothing? - blocks API design, ask product and
  implementation reviewers.
- [ ] What is the first SQS-compatible surface, if any? - blocks compatibility
  adapter planning, ask product.
- [ ] What Seventh Sense scheduling SLA should timestamp queues use for progress
  bound validation? - blocks validation plan, ask Seventh Sense operators.

## Success Criteria

pqueue is successful when a general-purpose queue can be created with a
configured priority model, accepts and updates work idempotently at production
scale, claims eligible work in strict or bounded-relaxed order without
starvation, survives worker failure through leases, and supports group-aware
batch execution. For Seventh Sense, success means scheduled delivery/action work
can move to pqueue without losing timestamp scheduling correctness, operational
visibility, or throughput.
