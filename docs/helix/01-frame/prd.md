---
ddx:
  id: prd
  review:
    self_hash: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
    deps: {}
    reviewed_at: "2026-08-04T04:50:53Z"
kind: product
---

# Product Requirements Document

## Summary

fireweed is a durable priority queue engine for applications that need
high-volume, ordered, recoverable work execution. A queue declares its stages,
typed fields and indexes when it is created. Clients add and update records
idempotently by key. Workers claim the records of one stage in rank order under
a lease, and they settle claimed records by writing them: a stage move, a new
rank, or a delete.

The contract a consumer depends on is
[API-006](../02-design/contracts/API-006-fireweed-kernel-contract.md). It is
the eight operations of the in-process `Store` trait (`define_queue`, `write`,
`claim`, `read`, `stats`, `scan`, `aggregate` and `delete_queue`), plus a
separate `Operator` trait for on-call repair.
[ADR-025](../02-design/adr/ADR-025-kernel-records-contract.md) records the
decisions behind it. Clause ids in parentheses in this PRD (for example C4)
are API-006 clauses.

The interface is batch-centric, and its guarantees do not depend on the
implementation:
- A successful mutation is durable (DUR1).
- A rejected mutation has no effect (E1).
- An interrupted or timed-out mutation is resolved by resending it with the
  same `request_id`, or by `read`, without duplicating state transitions (E2,
  RQ1–RQ4).
- A claim observes every acknowledged write (V2). A short or empty claim is a
  successful poll (C5), and C5a bounds how long a candidate can be missed.
- `read` and `scan` are fresh (V3).

Latency, cost and recovery time MUST NOT weaken these guarantees.

The product is general-purpose and may become open source. Seventh Sense is the
first validation workload: several delivery, action, job, and connector queues
need timestamp-ordered execution, mutable schedules, batch processing,
idempotent writes, group-aware claims, and horizontal scale beyond a single
database at 10M-record queue sizes. Scale qualification is portable across
hosts: it gates on exact outcomes, monotonic progress, bounded resources, and
same-run degradation as load and queue count increase (see "Scale
Substantiation").

The top success measures are correctness, progress under load, resource bounds,
and measured capacity. A horizontally distributed deployment spreads write and
claim load by placing queues across independent nodes without losing work,
duplicating claims, or violating any queue's progress. A single deployment
exercises at least 1000 concurrently active queues while a designated hot queue
and all cold queues make monotonic progress through bounded shared pools.
Throughput and latency distributions are reported with the exact topology and
resource envelope; they are capacity observations, not host-independent pass
bars. Each measure references a recorded evidence artifact (see "Scale
Substantiation").

### Implementation

The one production implementation of `Store` is an S3 object log with a Turso
projection
([ADR-024](../02-design/adr/ADR-024-s3-turso-public-cell.md)).
[TD-017](../02-design/technical-designs/TD-017-kernel-projection-storage-layout.md)
designs the Turso layout, and
[TD-018](../02-design/technical-designs/TD-018-partitioned-object-log.md) the
partitioned object log.

- The object log is the durable authority. A write is acknowledged once it is
  durable in the log.
- The Turso projection is derived. It serves claims and reads, and it is
  rebuilt from the log through projection control on the implementation's
  admin handle (API-006 §7).
- The control plane maps each queue to one partition, which fixes the queue's
  node and log residency. Queues are never rebalanced across partitions
  (ADR-025 §5).
- There are no storage selectors: the S3×Turso constructor takes its own
  configuration. Filesystem publication inside `fireweed-objectlog` tests the
  log protocol without MinIO; it is not a supported deployment.

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
2. Workers can claim the lowest-ranked claimable records of a stage, in rank
   order, at a cost logarithmic in queue size plus the records returned
   (API-006 C9).
3. Producers and schedulers can write and update records idempotently at the
   single-deployment write rate in Success Metrics, and beyond it by
   distributing work across multiple queues.
4. Downstream API workers can claim, update, and settle compatible batches
   efficiently.
5. Seventh Sense can replace or consolidate its scheduled delivery and action
   queues without losing timestamp scheduling, lease safety, or operational
   visibility.
6. Operators can configure the object log's commit-latency bound and
   understand the resulting tradeoff between latency, batch density, and
   object-store request cost.
7. Callers depend on one contract (API-006) with no backend-specific
   choreography: success is durable (DUR1), a claim observes every acknowledged
   write (V2), and `read` and `scan` are fresh (V3).

### Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| Single-queue progress under load | A 10M-resident queue completes the declared ingest and claim/settle operation mix with exact counts, no duplicate claims, monotonically advancing progress, and bounded queues/connections/tasks under ordinary concurrent host load | Portable Tier-1 evidence record (TP-002 E1), with throughput reported as topology-bound capacity |
| Behavior preserved at scale | Adding queues, nodes, and concurrent load preserves exact outcomes and every queue's claim order and progress (API-006 C4, C5a); same-run loaded results remain within the declared degradation envelope of interleaved controls | Multi-queue scale-out + concurrency benchmark per the Tier-2 evidence record (TP-002 E2) |
| Queue density | At least 1000 cold queues plus one designated hot queue are active concurrently; every queue makes progress, the hot and cold phases complete exactly, and shared workers/connections/tasks remain bounded rather than growing per queue | Multi-queue density benchmark per the Tier-2 evidence record (TP-002 E2) |
| Hot queue scale | At least 10M records resident in one active queue remain writable, claimable, observable, and exactly recoverable under the declared topology | Benchmark per TP-002 E1 and E3 |
| Core operation capacity | Throughput and p50/p95/p99 for batch insert, update, claim, and settle are published with workload, host, topology, and resource limits; pass/fail uses correctness, progress, bounded resources, and same-run degradation rather than absolute speed | Benchmark harness under representative Seventh Sense and synthetic workloads |
| External transaction integrity | The S3×Turso implementation passes the API-006 conformance suite (CS1–CS7), including its success, error and unknown-outcome semantics (DUR1, E1, E2, RQ1–RQ4), and recovers from process loss by rebuilding the projection from the object log. A short or empty claim is a successful poll (C5), not a lost mutation | Conformance suite plus fault injection per TP-003 |
| Commit latency and cost dial | The object log publishes latency, throughput, and object-store request-cost curves for its configured commit-latency bound | Object-log latency/cost matrix per TP-002 E3 |
| Claim order and progress | 0 claim-order inversions against `(rank, insertion sequence)` within a stage, and a claim on a quiet queue returns its first candidates (API-006 C4, C5a) | Contract suite plus adversarial tests with skewed rank and group distributions |
| Claim safety | Zero records attached to two live claims | Concurrency stress test with worker crashes and lease expiry |

### Scale Substantiation

Every scale claim in this PRD references an **evidence record** that names the
deployment shape, the workload envelope, and the design/test artifact plus
benchmark that substantiates it. Beyond naming the one production
implementation and its queue-to-partition placement, this PRD names no storage
layout or query: those belong in the governing design and test documents.

- **Single-deployment envelope** - write/throughput/latency/correctness on one
  storage deployment, validated by TP-002 E0's portable contract and the Tier-1
  benchmark (E1). Absolute throughput and latency describe the declared
  topology's capacity only.
- **Horizontal envelope** - as queue population and node count increase, exact
  outcomes, fencing, bounded resources, and per-queue progress are preserved;
  interleaved same-run controls quantify degradation without assuming a host's
  speed. The multi-queue scale-out benchmark (E2) validates this envelope. The
  placement mechanism lives in ADR-025 §5 and TD-018.

Both envelopes are v1 commitments. A scale claim that cannot reference its
evidence record is not publishable.

### Non-Goals

- fireweed v1 will not hardcode Seventh Sense job, action, connector, quota,
  paused, suppressed, or campaign concepts into the core record model.
- fireweed v1 will not be a full workflow engine like Temporal.
- fireweed v1 will not trade claim order for throughput. Every claim is in
  strict rank order within its stage (API-006 C4).
- fireweed v1 will not implement AMQP, Kafka, or SQS compatibility as the core
  data model. The contract is in-process; a network transport over `Store` is
  later work under its own contract (API-006 §8).
- fireweed v1 has no record expiry or retention machinery. Records live until
  the consumer deletes them or closes their queue, and bulk retention is the
  consumer's (API-006 §8).
- fireweed v1 does not enforce downstream API rate limits or quotas. Pacing work
  to a downstream system's accepted rate (for example a per-account,
  per-connector, or per-day send ceiling) is the responsibility of the calling
  worker. fireweed exposes claim-pacing controls (claim batch size, claim
  cadence, a future `rank`, and whole-group claims) that a caller uses to pace
  itself; fireweed itself performs no rate admission and applies no
  downstream-rate token bucket.

## Users and Scope

### Primary Persona: Queue Platform Engineer

**Role**: Engineer operating fireweed for one or more applications

**Goals**: Define queues, scale throughput, preserve durability, observe
backlogs, and keep noisy workloads isolated.

**Pain Points**: Existing queue behavior is split across brokers, tables, and
worker code; correctness and latency are hard to reason about at 10M-record
scale.

### Secondary Persona: Worker/Application Engineer

**Role**: Engineer producing and consuming queued work

**Goals**: Add and update records idempotently, change their rank and fields,
claim compatible batches, and settle them without designing a custom queue.

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

- **P0-1.** General queue namespaces with isolation, routing, metrics, and
  independent scale behavior. Queues are cheap: a consumer uses one queue per
  job type.
- **P0-2.** One caller-controlled `rank` (`i64`) per record orders claims
  within a stage. Timestamps (the default rank is the insertion time in µs) and
  non-timestamp priorities are both ranks (API-006 §2).
- **P0-3.** Strict rank ordering with a progress guarantee (C4, C5a), at a
  claim cost of O(log n + k + b) for n records, k returned and b skipped for
  Blocked gates (C9).
- **P0-4.** Rank and claimability as separate concepts: a record is claimable
  by its stage, lease state, `rank <= horizon`, and gates (C1).
- **P0-5.** Idempotent, atomic write units: insert, upsert, sparse update,
  stage move, settle, delete, claim extension, gate ops and a queue-metadata
  CAS, with exact replay by `request_id` (API-006 §4.2).
- **P0-6.** Batch claim, always within one declared stage: the next records in
  rank order, whole groups, or named keys.
- **P0-7.** Durable claim leases with at-least-once execution and at most one
  live claim per record.
- **P0-8.** Settling claimed records by writes that carry the claim token
  (stage move, new rank, release, delete), with an outcome per unit.
- **P0-9.** Declared, typed fields and indexes, fixed at queue creation, with
  field projection on `claim` and `read`; queue-level gates; and
  consumer-owned queue metadata.
- **P0-10.** Observability for counts by stage and lease state, pause state,
  ready groups and the oldest ready time (`stats`), and counts over declared
  indexes (`aggregate`).
- **P0-11.** Performance at 10M-record hot queue scale under representative
  concurrent load: core batch operations complete with exact outcomes,
  monotonic progress, no duplicate claims, and bounded shared resources.
  Horizontal scale is achieved by distributing queues across nodes while
  preserving each queue's claim order and progress (API-006 C4, C5a). Absolute
  throughput and latency are reported for the declared topology as capacity
  evidence, not used as portable release thresholds. Substantiated by the
  recorded scale evidence (see "Scale Substantiation").
- **P0-13.** The queue is the unit of placement. The control plane maps each
  queue to one partition, and each partition is owned by one node. Write and
  claim load scale beyond one node by distributing queues across partitions
  and nodes; a producer needing more than one partition's throughput for a
  logical stream splits it across multiple queues at the application layer.
  Claim exclusivity, claim order, and progress (C4, C5a, C6) hold per queue.
- **P0-14.** Queue density: a single deployment exercises at least 1000 cold
  queues plus one hot queue on one node. Every queue makes progress, exact
  stage counts reconcile, and loaded behavior stays within its declared
  same-run degradation envelope. Per-queue background work (lease reclaim,
  count maintenance, request-id GC) is multiplexed onto bounded shared
  per-node resources, never one task, loop, or connection per queue. Aggregate
  single-node throughput is reported for the declared node; multi-node
  deployment provides aggregate headroom.
- **P0-15.** The API-006 contract holds on the S3×Turso implementation.
  Mutation success is durable (DUR1). Structured rejection has no effect (E1).
  Unknown outcomes resolve by `request_id` or by `read` (E2, RQ1–RQ4).
  Idempotent replay and claim exclusivity hold. A claim observes every
  acknowledged write (V2); a short or empty claim is a successful poll, not a
  command failure (C5), bounded by C5a; `read` and `scan` are fresh (V3).
  Recovery rebuilds the Turso projection from the object log through
  projection control on the admin handle.
- **P0-16.** The object log MUST expose an operator-configurable
  commit-latency bound that controls group-commit cadence. Lower bounds reduce
  mutation latency and increase object-store request cost; higher bounds
  increase batch density and latency. The bound is a performance and cost dial
  only and MUST NOT weaken transaction integrity.

### Should Have (P1)

- **P1-1.** A network transport over `Store`, under its own contract and
  adding no semantics, so non-Rust consumers such as Seventh Sense's Scala
  services can use fireweed.
- **P1-2.** fireweed deployment-level rate limits, quotas, and tenant capacity
  controls (protecting the fireweed deployment from noisy tenants - not
  enforcing callers' downstream API limits, which are a permanent non-goal).
- **P1-4.** Operational repair: pause and resume are the `"*"` gate (W9);
  reschedule, retry, move, complete and purge are ordinary writes by key; ending
  a lease without its token is `Operator::force_clear`, reached only from the
  admin handle (FR-58).

### Nice to Have (P2)

- **P2-1.** Compatibility adapters, such as SQS-, BullMQ- or Faktory-style
  client APIs, over the network transport (P1-1).
- **P2-2.** A hosted dashboard for queue inspection, repair, and trend
  analysis.

## Functional Requirements

Clause ids in parentheses (for example C4) are API-006 clauses.

### Subsystem: Queue Definition

- **FR-1** - A queue is an isolated namespace with a stable identifier,
  `(tenant, queue)`, used for routing, scaling, metrics, and operational
  control. Queues are cheap: a consumer uses one queue per job type. No
  operation and no atomic unit spans queues (A4, ISO1).
- **FR-2** - Every record carries one caller-controlled `rank` (`i64`). A claim
  takes the records of its stage in ascending rank, and equal ranks go in
  insertion order (ORD1, ORD2). A queue declares no priority model, value type,
  direction, or tie-breaker.
- **FR-3** - Timestamp ordering is first-class. The default rank is the
  insertion time in µs, which gives FIFO, and a caller-supplied time in µs as
  the rank gives timestamp-ascending order. A rank in the future schedules the
  record: it is not claimable by `Next` or `WholeGroups` until the claim's
  horizon (by default, now) reaches it (C1).
- **FR-4** - Non-timestamp priorities are ranks too. A priority bucket is an
  offset added to the rank: negative to run sooner, positive to run later
  (API-006 §6.5).
- **FR-6** - A queue's definition is declared when the queue is created: an
  ordered list of 1..=32 stages (default `[INITIAL, COMPLETED]`; the first is
  the insert default), up to 256 typed fields, and up to 32 indexes. The
  definition is immutable: `define_queue` is create-or-verify, a different
  definition is a `DefinitionConflict`, and changing any part of it means a new
  queue (D1–D6, §5.1).
- **FR-56** - A queue carries consumer-owned metadata: a map of at most 64 KiB
  with a version, changed only by a metadata op inside a write unit (a sparse
  patch, optionally compare-and-swap on the version), read fresh together with
  the definition, and never interpreted by the engine (W13, R4, §5.2).
- **FR-57** - A queue can be closed with `delete_queue`, even while it holds
  records and live claims. `archive: true` archives it: its records stay in
  durable storage but out of reach of every op. A full delete is allowed only
  when the instance's delete policy is `AllowFull` (the default is
  `ArchiveOnly`). Closing is atomic and durable, every lease token of the queue
  dies, and closed is final: every later op on the queue, `define_queue`
  included, fails with `QueueClosed`, except a repeated `delete_queue`, which
  reports `already_closed` (DQ1–DQ5).

### Subsystem: Priority and Progress

- **FR-7** - A claim names one declared stage and returns the claimable records
  of that stage in `(rank, insertion sequence)` order, or the named records of a
  `Keys` claim (C1, C4). Records of other stages, leased records, and records
  ranked beyond the horizon are never visited (C9). On a quiet queue, a claim
  returns the first `min(max_records, candidates)` candidates in claim order
  (C5a).
- **FR-10** - A ready record has waited since it last became Free in its
  current stage: at insert, on a stage move while Free, on release, on
  force-clear, or on lease reclaim. `stats.groups.oldest_ready_since` reports
  the earliest such time among a stage's ready grouped records. Rank is never
  read as a time, so a priority offset does not shift it (S2).
- **FR-11** - Lease expiry frees every record still attached to the expired
  claim within Δ (at most 5 s). Each record keeps its stage, rank, and insertion
  sequence, so it returns to its place in claim order, and its `attempts` count
  is kept (V4, ORD1).

### Subsystem: Eligibility and Fields

- **FR-14** - A claim returns the first candidates of its stage in claim order.
  A record is a candidate when it is in the claim's stage, Free, of
  `rank <= horizon`, and carries no Blocked gate, while the queue is not paused
  (C1, C5).
- **FR-15** - Claimability depends only on stage, lease state, rank against the
  claim horizon, and gates (C1). There is no separate not-before time, retry
  timer, or lifecycle state: a delay or a retry backoff is a rank in the
  future.
- **FR-16** - Records carry declared, typed fields (`Int`, `Float`, `Bool`,
  `Time`, `Str`, `Bytes`), at most 256 per queue, declared with their indexes
  when the queue is created. A `Str` or `Bytes` value is at most 64 KiB and an
  encoded record at most 4 MiB, and every value within those bounds behaves the
  same: there is no "large" flag and no separate payload. A queue declares up
  to 32 indexes of 1..=8 keys each, over fields of any declared type (`Str` and
  `Bytes` included) and the built-ins stage, rank and group; the encoded index
  key is at most 1024 B. Fields are the only per-record data (F1–F3, §5.1).
- **FR-59** - `claim` and `read` take an optional list of declared field names
  and return only those fields; with no list they return every field.
  Projection changes only the returned fields: which records a claim takes, and
  what it does to them, are the same. A projected claim or read never reads the
  stored value of a field it does not return, except where that value shares
  storage with a returned field, so a claim that names no fields reads no field
  storage and is not limited by large values it does not need (F4).
- **FR-17** - Gates can keep an otherwise claimable record unclaimed. A record
  carries up to 8 gate keys, and a gate's queue-level state (Open or Blocked) is
  set by gate ops inside write units, atomically with the unit and without
  changing any record. The reserved gate `"*"` pauses the queue (W9). A static
  per-record block is a gate or a stage.

### Subsystem: Idempotent Ingest and Mutation

- **FR-18** - Records are addressed by a key that the upstream system owns:
  opaque bytes, 1..=256 B, unique within the queue across every stage. A large
  natural key is hashed upstream (§3).
- **FR-19** - Writes to one key converge on one record. An insert with
  `Expect::Absent` reports `Exists` when the key is present, and a write with
  `Expect::Any` upserts (W3, W5). A resend with the same `request_id` returns
  the recorded outcome without re-evaluation (RQ2).
- **FR-20** - A write changes any record in any stage by key, with a sparse
  patch of its stage, rank, gates, and fields, optionally guarded by its
  version (W3, W5, W8). A leased record is written only by its claim holder,
  with the token (W4). A write request is a list of atomic units; a unit may
  also carry claim extensions and gate ops, and one unit of the request may
  carry the queue-metadata op (W2, W13, W14). Records are selected for a write
  by key, after a `read` or `scan` where needed.
- **FR-21** - A write returns one outcome per unit: the per-write outcomes of an
  applied unit, or the failing write and its reason for a rejected unit, which
  has no effect (W2).
- **FR-22** - `request_id` is optional on `write`, `claim` and
  `Operator::force_clear`; `define_queue` and `delete_queue` take none because
  they are naturally idempotent (RQ1). Replay is kept for the request-id
  retention R, which is at least 24 h. R is a contract bound, not a per-queue
  setting (V5, §6.8). An id-less write that fails with `Unavailable` is
  resolved by `read` of what it touched (RQ4).

### Subsystem: Claim Leases and Lifecycle

- **FR-23** - Every record is in exactly one of its queue's declared stages and
  is either Free or Leased. The caller owns the stage: an insert lands in the
  first stage unless it names another, and any write may move a record to any
  declared stage. No stage is special: there is no terminal flag and no engine
  Completed or Failed state. A record lives until the consumer deletes it or
  closes its queue (§3).
- **FR-24** - A claim leases the batch it returns under one token and one
  expiry. No other claim returns a leased record, and lease-free writes to it
  are rejected, until its holder releases or deletes it, the claim expires, or
  an operator force-clears it. `Extend` renews the whole claim. A claim has no
  holder identity: whoever presents the token is the holder (C3, L1–L4).
- **FR-25** - No record may be attached to more than one live claim (C6).
- **FR-26** - If the holder neither settles nor extends before the claim's
  expiry, every record still attached to the claim becomes claimable again
  within Δ. There is no queue redelivery policy (V4).
- **FR-27** - Every acknowledged effect survives process and node restart:
  records with their stage, rank, gates, and fields; claims and leases; gate
  state; queue metadata; and queue closure (DUR1).
- **FR-28** - The delivery guarantee is at-least-once execution with a single
  live claim; consumers remain responsible for idempotent side effects.
  `attempts` counts the claims of a record and is never reset (L6).

### Subsystem: Batch and Group Operations

- **FR-29** - Workers can claim up to 1000 records in one claim. The response
  is at most 16 MiB and always holds at least the first candidate (C7, §6.8). A
  claim resent with the same `request_id` returns the same records and token
  while the claim is live and every record it returned is still attached (C8).
- **FR-30** - A claim returns records in a deterministic order:
  `(rank, insertion sequence)` for `Next` and within each group of
  `WholeGroups`, and request order for `Keys` (C4).
- **FR-31** - A claim selects the next records of one stage (`Next`), whole
  groups (`WholeGroups`), or named keys (`Keys`). No claim orders by a declared
  index: records found through an index are claimed by key after a `scan`, a
  kind of work is a stage, and a compatibility key is a group or an indexed
  field. A `Keys` claim reports every key it did not claim, with the reason
  (C1, C2, C4).
- **FR-31a** - A `WholeGroups` claim bounds the number of distinct groups it
  returns (`max_groups`) and returns every candidate of each group it takes, so
  workers can size batches by a downstream per-call entity limit. It stops
  before a group that would exceed `max_records` or the response limit; an
  oversized first group comes back as a prefix flagged `partial_group`. Inserts
  are never rejected for group size, and there is no per-group size cap (C4,
  C7, W10).
- **FR-32** - `WholeGroups` orders groups by their first candidate in claim
  order and takes only groups with no leased member in any stage (C4, C6a). A
  consumer that needs one holder per group claims the group's stages only with
  `WholeGroups`. There is no per-group fairness mechanism beyond rank order.
- **FR-33** - Workers settle claimed records by writing them with the claim's
  token: `Release(t)` with a stage move (finish or fail), a new rank (retry with
  backoff), or a `Delete`. `Held(t)` patches a record and keeps its lease.
  Per-transition history travels in the settling write as fields (W4, W5, W7).
- **FR-34** - A rejected write unit reports the failing write and its reason,
  including a stale or wrong token (`StaleLease`), a record leased by another
  holder (`Leased`), a version mismatch, and a validation failure (W2, W4).
- **FR-35** - Batch sizes and claim limits are contract constants, not queue or
  deployment configuration (§6.8).

### Subsystem: Retry, Failure, and Retention

- **FR-36** - The engine counts `attempts` (+1 per claim, never reset). A retry
  is a settling write with a future rank (the backoff) and its retry metadata
  in fields. A consumer compares `attempts` with its own limit and moves an
  exhausted record to a stage it names for failures (W5, §3).
- **FR-49** - A record can recur without limit: its holder settles each cycle
  with `Release(t)` and a new rank (the next run time), in the same stage or
  another. Recurrence needs no queue mode and no special op.
- **FR-51** - A recurring record is one record per key; re-ranking it never
  creates another record.
- **FR-52** - A recurring record ends when the consumer deletes it (W7) or moves
  it to a stage that nothing claims. Idle backoff scheduling is caller-owned.
- **FR-53** - Idle (future-ranked) and ready records can be told apart: `stats`
  counts records by stage and lease state, and `aggregate` over a declared index
  keyed on the stage and `Rank` buckets them by rank (S1, Q4).
- **FR-54** - A re-ranked record is claimed in rank order, like any other
  record, once its rank is at or below the claim's horizon. Recurrence adds no
  second rule.

### Subsystem: Observability and Operations

- **FR-40** - The queue exposes Free and Leased counts for each declared stage,
  and whether it is paused (S1).
- **FR-41** - The queue exposes, for one stage, a bounded count of groups with a
  ready record and the oldest ready time (S2), and counts over any declared
  index range, by index key, time bucket, value range, or lease state (Q4). A
  failure stage's size is a stage count.
- **FR-42** - Implementations emit throughput and latency telemetry for write,
  claim, and lease reclaim. It is outside the `Store` contract, which exposes no
  backend introspection (API-006 §8).
- **FR-43** - Queue isolation includes noisy-neighbor protection: one queue's
  load or backlog cannot prevent another queue from making progress within its
  configured limits.
- **FR-58** - Ending a lease without its token is `Operator::force_clear`, in a
  separate trait reached only from the admin handle; `Store` has no such op.
  Normal workflows never use it as a side channel, and CI bans the `Operator`
  trait and the admin handle in Snorri, the workload and loadgen crates, and
  the contract suite (OP1–OP4).
- **FR-60** - Records can be inspected without claiming them. `read` loads up
  to 1000 records by key in any stage and lease state, and, when asked, the
  queue's definition and metadata. `scan` lists records through a declared
  index in index order, with an opaque cursor that neither duplicates nor skips
  records unchanged during paging. Both observe every acknowledged write and
  never return lease tokens (R1, R3, R4, Q1–Q3, V3).

### Subsystem: Seventh Sense Validation

- **FR-44** - fireweed can represent Seventh Sense scheduled delivery and action
  work with the scheduled time in µs as the rank, one queue per job type, and
  consumer-named stages for its steps, without Seventh Sense-specific states in
  the engine. API-003 maps each Seventh Sense queue onto the contract.
- **FR-45** - Seventh Sense pause, suppression, account, connector, job, and
  campaign controls are gates: a status change is one gate op that blocks or
  reopens every record carrying that gate at once, without rewriting records
  (W9). Downstream delivery-pacing and quota constraints are **not** core queue
  features and are **not** claimability conditions: callers pace claims using
  `max_records`, claim cadence, a future rank, and whole-group claims (see
  Non-Goals). fireweed does not model a downstream rate or quota as a stage or
  as claim-time admission control.
- **FR-46** - fireweed can ingest work quickly and set or change its scheduled
  time later: a lease-free write by key sets a new rank or moves the record to
  another stage (W5).
- **FR-47** - fireweed can claim batches compatible with downstream Seventh
  Sense API constraints: one queue per job type, stages for each kind of work,
  whole groups by a group key such as a lead or a job, and, for records found
  by a declared field such as account, connector, or campaign, a `scan`
  followed by a `Keys` claim (C1, C2, C4).
- **FR-47a** - fireweed can represent Seventh Sense's nesting on API-006's
  axes: `tenant` for the account and isolation boundary; one queue per job
  type; stages for that type's steps; `group` as the whole-group axis (for
  example `job_id`, or the lead for Marketo enrichment); declared fields for
  account, connector, and campaign; and gates for account and connector status.
  Claims are in rank order within a stage; per-job fairness is not an engine
  concept.
- **FR-47b** - fireweed can claim batches bounded by a downstream per-call
  entity limit, such as the Marketo lead-enrichment limit of 300 distinct leads
  per API call, with `WholeGroups { max_groups: 300 }`: every currently
  claimable task of each selected lead, as whole groups, in one atomic claim
  (C3, C4).
- **FR-47c** - fireweed can execute a Seventh Sense `actions_scheduled`
  callback batch (`batch_checksum` / `callback_id`) together: each batch is
  inserted as one group in one atomic write unit, claimed with `WholeGroups`,
  and checked against its expected count by the caller, with no `callback_id`
  semantics in the engine.
- **FR-55** - fireweed can represent Seventh Sense `jobs_queue` and
  `connectors_queue` singleton recurring rows: one record per job or connector
  key in its type's queue, re-ranked on settle with the next processing time
  (`{Release(t), Put{rank}}`), and removed with a `Delete` when the underlying
  entity is removed, without job or connector concepts in the engine.

## Acceptance Test Sketches

The governing acceptance scenarios are API-006 §9 (CS6) and TP-003.

| Requirement | Scenario | Input | Expected Output |
|-------------|----------|-------|-----------------|
| FR-2, FR-3 | Default FIFO | Three inserts with no rank into a queue with the default stages | A `Next` claim of `INITIAL` returns them in insertion order |
| FR-3, FR-15 | Scheduled record | An insert ranked 60 s in the future | A `Next` claim skips it until the horizon reaches its rank; a `Keys` claim takes it at once |
| FR-4, FR-7 | Rank order | Three candidates of one stage ranked 3, 1 and 2 | The claim returns rank 1, then 2, then 3 |
| FR-6 | Immutable definition | `define_queue` twice with one definition, then with one more field | `created: true`, then `created: false`, then `DefinitionConflict` carrying the stored definition |
| FR-56 | Metadata CAS in a unit | A unit with two record writes and a metadata op expecting version 3 while the version is 4 | The unit is rejected at `Metadata` with `MetadataVersionMismatch { current: 4 }`; neither record changes |
| FR-57 | Close a busy queue | `delete_queue { archive: true }` on a queue with records and a live claim | Success; every later op except `delete_queue` fails with `QueueClosed`, also after restart; the claim's token is dead; a repeat returns `already_closed: true` |
| FR-57 | Full delete under the default policy | `delete_queue { archive: false }` on an `ArchiveOnly` instance | `NotPermitted`, with no effect |
| FR-10, FR-41 | Oldest ready time | A grouped record ranked with a −1 h priority offset, released at time t | `stats.groups.oldest_ready_since` is t, not its rank |
| FR-11, FR-26 | Lease recovery | A worker claims three records and crashes | No claim returns them while the lease is live; within Δ after `until` they are claimable again in their places, with `attempts` kept |
| FR-14, FR-17 | Gates and pause | A candidate carries gate `acct:7`; `acct:7` is Blocked, then Open; then `"*"` is Blocked | Not claimable while `acct:7` is Blocked; claimable after it opens; nothing is claimable while `"*"` is Blocked, and `stats.paused` is true |
| FR-16 | Declared fields | A write with an undeclared field; a write with a `Str` value for an `Int` field | Both `Invalid`, with no effect |
| FR-16 | Large values | A 64 KiB `Bytes` value | Round-trips through `write`, `claim` and `read`, and after restart |
| FR-16 | Binary index key | An index keyed on a `Bytes` field; a write whose encoded index key exceeds 1024 B | `scan` returns hits in bytewise key order; the oversized write is `Invalid` |
| FR-59 | Field projection | Records of about 4 MiB each, claimed with an empty field list and `max_records` 10 | Up to 10 records come back with no fields, leased, with `attempts` and version bumped as for an unprojected claim; an unprojected claim stops at the 16 MiB response limit |
| FR-18, FR-19 | Idempotent insert | One key inserted twice with `Expect::Absent` | One record; the second write reports `Exists { version }` |
| FR-19, FR-22 | Request-id replay | A write resent with the same `request_id` and body, then with a different body | The recorded response with `replayed: true`, then `RequestIdConflict` |
| FR-20 | Optimistic update | A write with `Expect::Version` of a stale version | `VersionMismatch { current }`; the unit has no effect |
| FR-20, FR-24 | Leased record | A lease-free write to a claimed record, then the holder's write with `Held(t)` | `Leased`; the holder's write applies and the record stays leased |
| FR-21 | Independent units | A request of two units, the second with a failing write | The first unit applies; the second is rejected at its failing write, with no effect |
| FR-25 | Claim exclusivity | Concurrent claims of one stage | No record is attached to two live claims |
| FR-28 | Attempt count | Claim, release, claim again | `attempts` is 2 |
| FR-29, FR-30 | Batch order | 1,500 candidates; a claim with `max_records` 1000 | 1000 records in `(rank, insertion sequence)` order |
| FR-31a, FR-47b | Group-cardinality claim | Candidates across 1,000 groups; `WholeGroups { max_groups: 300 }` | Up to 300 whole groups, in the order of each group's first candidate, within `max_records`; no group is split unless the first alone exceeds the limits (`partial_group`) |
| FR-32 | Group exclusivity across stages | Group g has a leased record in stage `a`; a `WholeGroups` claim of stage `b` | g is not returned until its record in `a` is Free |
| FR-33, FR-34 | Settle | Units that release claimed records with a stage move, one carrying the token of an expired claim | The live-token units apply; the expired one is rejected with `StaleLease` |
| FR-36 | Retry with backoff | `{Release(t), Put{rank: now + 30 s, fields: {last_error}}}` | The record is claimable again after 30 s, with `attempts` kept and the field stored |
| FR-49, FR-51, FR-55 | Recurring singleton | One key claimed and settled with `{Release(t), Put{rank: next}}` over many cycles | One record; its version increases every cycle and `attempts` counts every claim |
| FR-52 | End a recurring record | `{Release(t), Delete}` | `Deleted { existed: true }`; the key is free at once |
| FR-53 | Idle versus ready | `aggregate` over an index keyed on stage and rank, with `eq` on the stage and `Ranges` on rank split at now | Future-ranked and ready records are counted apart |
| FR-40 | Counts by stage | Records in three stages, some leased | `stats` reports Free and Leased counts for every declared stage, in declaration order |
| FR-43 | Queue isolation | One queue holds a 10M-record backlog; another holds a small one | Claims on the small queue keep making progress |
| FR-45, Non-Goals | No downstream rate enforcement | Many candidates; a worker claims with `max_records` 25 and pauses between calls | Each claim returns up to 25 records by the normal candidate rules; fireweed never withholds work for a downstream-rate reason |
| FR-44, FR-46 | Seventh Sense scheduling | A delivery record inserted before its send time is known, then re-ranked to the send time | It is claimed only once the horizon reaches the send time |
| FR-47c | Callback batch | A batch inserted as one group in one unit; a `WholeGroups { max_groups: 1 }` claim | The whole batch comes back in one claim, and the caller checks its count |
| FR-58 | Force-clear | `Operator::force_clear` on a leased record | The record is Free with a new version and `cleared: true`; the claim's other records stay attached; `Store` has no op that does this |
| FR-60 | Fresh inspection | A write, then `read` of its key and a `scan` over an index it changes | Both observe the write |

## Technical Context

This PRD records product requirements. Storage layout, placement, indexing,
and deployment topology belong in the technical designs (TD-017, TD-018).

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

## Constraints, Assumptions, Dependencies

### Constraints

- **Business**: The first validation workload is Seventh Sense scheduled
  delivery/action execution.
- **Technical**: fireweed must support durable claims and batch operations with
  strict rank order within a stage (API-006 C4), and no ordering across stages
  or queues.
- **Technical**: Downstream API rate/quota enforcement is out of the fireweed
  engine. The claim path MUST NOT contain a downstream-rate admission stage;
  callers pace via claim batch size, claim cadence, a future `rank`, and
  whole-group claims.
- **Product**: Seventh Sense-specific concepts must remain outside the core
  model unless they are necessary for generic durable priority queue semantics.

### Assumptions

- The queue definition (stages, fields, and indexes) can be fixed at queue
  creation; changing it means a new queue (API-006 §5.1).
- At-least-once execution with a single live claim is the correct durability
  contract; exactly-once side effects are caller-owned.
- Batch and group-aware operations are required for the first useful release.
- A timestamp rank uses a producer-supplied scheduled time in µs; producers are
  responsible for the business meaning of that timestamp.

### Dependencies

- Seventh Sense production workload data for realistic load profiles, group
  distributions, rank skew, and downstream API batch constraints.
- API-006 for the contract. A network transport is later work over `Store`,
  not part of the contract.
- The S3×Turso technical designs (TD-017 for the Turso projection layout,
  TD-018 for the partitioned object log) and mapped queue placement with no
  rebalancing (ADR-025 §5). These substantiate the horizontal envelope in
  Success Metrics.

## Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Strict rank order creates a scalability bottleneck | High | High | Keep every claim a stage-bound index walk of O(log n + k + b) (API-006 C9), held by the TP-003 claim-path performance gates |
| Large field values shrink claim batches or slow claims | Medium | High | Field projection (F4) and TD-017's lean claim row keep field bytes off the claim path |
| Group-aware claims starve other groups | Medium | High | `WholeGroups` orders groups by their first candidate in rank order (API-006 C4) |
| Idempotency state grows without bound | Medium | Medium | Request ids are optional and kept for R (at least 24 h, V5); an id-less request resolves by `read` (RQ4) |
| Generic model becomes too Seventh Sense-specific | Medium | High | Keep Seventh Sense states in consumer-named stages and declared fields, not engine states |
| Performance tests use unrealistic uniform workloads | High | Medium | Include skewed-rank, future-ranked, leased, retried, and group-heavy test profiles |

## Resolved Product Decisions

- The first client surface is API-006, in-process in Rust. A network transport
  over `Store` follows under its own contract (P1-1).
- Atomicity: each write unit is atomic, and the units of one request are
  evaluated independently (W2). A claim and all its leases are atomic (C3).
  `WholeGroups` returns whole groups, or a flagged prefix of an oversized
  first group (C4). Nothing is atomic across queues (A4).
- v1 scale is committed in two envelopes - single-deployment and horizontal
  cross-queue scale-out. Both preserve exact outcomes, per-queue progress, and
  bounded shared resources under load, and each references a recorded
  evidence artifact (see "Scale Substantiation").
- Downstream API rate/quota enforcement is a non-goal of the fireweed engine,
  not a deferred feature. Callers pace claim output with `max_records`, claim
  cadence, a future rank, and `WholeGroups { max_groups }`. fireweed's own
  deployment and tenant capacity controls are P1-2.
- Recurring work is an ordinary record, re-ranked by its settling write. There
  is no recurring queue mode.
- The Seventh Sense `jobs_queue`/`connectors_queue` poll-cursor pattern (one
  durable row per pollable resource, claimed by its next processing time) is
  one record per resource in its type's queue, ranked by its next processing
  time and re-ranked on settle. Choosing which queue to service next is
  caller-side routing, informed by `stats`.

## Success Criteria

fireweed is successful when a general-purpose queue can be created with
declared stages, fields, and indexes, accepts and updates work idempotently at
production scale, claims claimable work in rank order without starvation,
survives worker failure through leases, and supports group-aware batch
execution. For Seventh Sense, success means scheduled delivery/action work can
move to fireweed without losing timestamp scheduling correctness, operational
visibility, or throughput.
