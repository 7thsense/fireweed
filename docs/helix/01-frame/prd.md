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

## Public cell (ADR-024)

ADR-024 supersedes the 2026-09-17 storage-selector amendment for public
selectors. The public product is one cell: an object-log published to S3, a
Turso projection, and `AsyncProjection`. There is no public storage matrix.
Filesystem publication stays inside `fireweed-objectlog` as the local adapter
of that log. Turso `:memory:` is the non-durable form of the same projection.
The separate in-memory map is not a product projection. Class A durability is
the object log. Turso is rebuildable through `projection_control` and is not
the command log.

The 2026-09-17 amendment is historical. It does not define current selectors.
Historical DDx IDs, requirement IDs, artifact names, and original measurements
retain their identity. SQLite selectors stay retired and are not a differential
reference. See [the kernel contract](../02-design/contracts/API-006-fireweed-kernel-contract.md)
and [storage authority manifest](../04-build/storage-authority-manifest.json).

## Kernel contract (API-006)

[API-006](../02-design/contracts/API-006-fireweed-kernel-contract.md) is the
contract a consumer depends on. It supersedes API-001 to API-005 (owner
decision, 2026-09-23 design discussion), and the requirements below follow it:

- A queue declares an ordered list of stages, typed fields and indexes when it
  is created, and that definition is immutable. Queue metadata is mutable,
  owned by the consumer and never interpreted.
- A record has an upstream-owned key, one stage, a `rank` (`i64`) and declared
  fields. A claim walks one stage in rank order under a per-claim lease; a write
  addresses records by key. No stage is special: there are no terminal states.
- The contract is in-process. There is no remote API, expiry or retention
  machinery, cohort, discovery, side record, progress bound or relaxed ordering.

Requirement ids are stable. A requirement the contract removed is marked
**Retired (API-006)** with its reason, and its id is never reused. New
requirements take the next free id (FR-56 onward).

## Summary

fireweed is a durable priority queue engine for applications that need high-volume,
ordered, recoverable work execution. A queue declares its stages, typed fields
and indexes. Clients add and update records idempotently by key, workers claim
the records of one stage in rank order under a lease, and they settle claimed
records by writing them: a stage move, a new rank, or a delete.

fireweed is also the transaction mapping layer for this centralized state-machine
workflow. The native interface is batch-centric, and its semantics are API-006's.
A successful mutation is durable (DUR1). A claim observes every acknowledged
write (V2); a short or empty claim is a successful poll, not a failure of the
mutation (C5), and C5a bounds how long a candidate can be missed. `read` and
`scan` are fresh (V3). A rejected mutation has no committed effect (E1). An
interrupted or timed-out mutation can be resolved through `request_id`, or by
`read`, without duplicating state transitions (E2, RQ1–RQ4). Implementation
note (ADR-024): the public storage cell is S3 object-log × Turso projection
with `AsyncProjection`; Class A durability is the object log, and Turso rebuilds
through projection control on the admin handle and is not the command log.
Latency, cost, and recovery time MUST NOT silently change state-machine
integrity.

The product is general-purpose and may become open source. Seventh Sense is the
first validation workload: several delivery, action, job, and connector queues
need timestamp-ordered execution, mutable schedules, batch processing,
idempotent writes, group-aware claims, and horizontal scale beyond a single
database at 10M-item queue sizes. Scale qualification is portable across hosts:
it gates on exact outcomes, monotonic progress, bounded resources, and same-run
degradation as load and queue count increase (see "Scale Substantiation").

The top success measures are correctness, progress under load, resource bounds,
and measured capacity. A horizontally distributed deployment spreads write and
claim load by placing queues across independent nodes without losing work,
duplicating claims, or violating queue-global progress. A single deployment
exercises at least 1000 concurrently active queues while a designated hot queue
and all cold queues make monotonic progress through bounded shared pools.
Throughput and latency distributions are reported with the exact topology and
resource envelope; they are capacity observations, not host-independent pass bars.
Each measure references a recorded evidence artifact (see "Scale
Substantiation").

The public durable cell is S3 object-log × Turso projection (ADR-024): object-store
durability and a rebuildable serving projection. It is the product, not one row
of a larger matrix. Retired log and projection selectors are not a roadmap.

### Storage product boundary

The public product is one cell: S3 object log × Turso projection (ADR-024).
`ResponseBarrier` has only `AsyncProjection`. Turso means the embedded/local
Turso 0.7 adapter in ordinary WAL mode. It is not a default among other
projections, and it is not a SQLite differential reference. Public selectors
do not include `memory`, `sqlite`, `postgres`, or `filesystem` logs, `memory`
or `postgres` projections, `objectlog`, `inmemory`, Hybrid aliases, or `Strict`.

Class A durability is the object log. The log remains authoritative after
restart and Turso recovers through `projection_control` (high-water plus tail
replay). There is no public Class B cell. The control plane is not a storage
cell and not a mandatory PostgreSQL dependency. Public composition is native
async.

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
3. Producers and schedulers can write and update items idempotently at the
   single-deployment write rate in Success Metrics, and beyond it by distributing
   work across multiple queues.
4. Downstream API workers can claim, update, and finalize compatible batches
   efficiently.
5. Seventh Sense can replace or consolidate its scheduled delivery/action queues
   without losing timestamp scheduling, lifecycle safety, or operational
   visibility.
6. Operators can configure a commit-latency bound for applicable Class A logs and
   understand the resulting tradeoff between latency, batch density, and backing
   store request cost.
7. Callers can depend on one transaction contract (API-006) without
   backend-specific choreography: success is durable (DUR1), a claim observes
   every acknowledged write (V2), and `read` and `scan` are fresh (V3). The
   public cell (S3 object-log × Turso, ADR-024) implements it.

### Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| Single-queue progress under load | A 10M-resident queue completes the declared ingest and claim/finalize operation mix with exact counts, no duplicate claims, monotonically advancing cursors, and bounded queues/connections/tasks under ordinary concurrent host load | Portable Tier-1 evidence record (TP-002 E1), with throughput reported as topology-bound capacity |
| Behavior preserved at scale | Adding queues, owners, and concurrent load preserves exact outcomes and every queue's claim order and progress (API-006 C4, C5a); same-run loaded results remain within the declared degradation envelope of interleaved controls | Multi-queue scale-out + concurrency benchmark per the Tier-2 evidence record (TP-002 E2) |
| Queue density | At least 1000 cold queues plus one designated hot queue are active concurrently; all queues become progress-eligible, the hot and cold phases complete exactly, and shared workers/connections/tasks remain bounded rather than growing per queue | Multi-queue density benchmark per the Tier-2 evidence record (TP-002 E2) |
| Hot queue scale | At least 10M items resident in one active queue remain writable, claimable, observable, and exactly recoverable under the declared topology | Benchmark per TP-002 E1 and E3 |
| Core operation capacity | Throughput and p50/p95/p99 for batch push, update, claim, and finalize are published with workload, host, topology, and resource limits; pass/fail uses correctness, progress, bounded resources, and same-run degradation rather than absolute speed | Benchmark harness under representative Seventh Sense and synthetic workloads |
| External transaction integrity | The public cell (s3 × turso, ADR-024) satisfies API-006's success, error and unknown-outcome semantics (DUR1, E1, E2, RQ1–RQ4) and proves crash recovery from the object log. A short or empty claim is a successful poll (C5), not a lost mutation. This row does not record a 10M-resident or 1000-queue pass | Cell conformance plus fault injection per TP-003 on that cell |
| Commit latency and cost dial | Applicable Class A logs publish latency, throughput, and object-store request-cost curves for the configured commit-latency bound | Object-log latency/cost matrix per TP-002 E3 |
| Claim order and progress | 0 claim-order inversions against `(rank, insertion sequence)` within a stage, and a claim on a quiet queue returns its first candidates (API-006 C4, C5a). Replaces the retired progress-bound metric | Contract suite plus adversarial tests with skewed rank and group distributions |
| Claim safety | Zero concurrent active leases for the same item | Concurrency stress test with worker crashes and lease expiry |

### Scale Substantiation

Every scale claim in this PRD references an **evidence record** that names the
deployment shape, the workload envelope, and the design/test artifact plus
benchmark that substantiates it. This PRD names no storage backend, shard
mechanism, or query: those belong in the governing design and test documents.

- **Single-deployment envelope** - write/throughput/latency/correctness on one
  storage deployment, validated by TP-002 E0's portable contract and the Tier-1
  benchmark (E1). Absolute throughput and latency describe the declared
  topology's capacity only.
- **Horizontal envelope** - as queue population and owner count increase, exact
  outcomes, fencing, bounded resources, and per-queue progress are preserved;
  interleaved same-run controls quantify degradation without assuming a host's
  speed. The multi-queue scale-out benchmark (E2) validates this envelope. The
  queue-ownership and second-backend mechanisms live in the design artifacts the
  technical context references.

Both envelopes are v1 commitments. A scale claim that cannot reference its
evidence record is not publishable.

### Non-Goals

- fireweed v1 will not hardcode Seventh Sense job, action, connector, quota,
  paused, suppressed, or campaign concepts into the core item model.
- fireweed v1 will not be a full workflow engine like Temporal.
- fireweed v1 will not offer relaxed ordering. Every claim is in strict rank
  order within its stage (API-006 C4); the earlier bounded-relaxed option is
  retired.
- fireweed v1 will not implement AMQP, Kafka, or SQS compatibility as the core
  data model. It has no remote API or wire adapter: the RESP adapter is
  deleted, and a network front may be rebuilt later as a pure transport over
  `Store` (API-006 §8). The Kafka producer wire adapter (ADR-005) was already
  retired by ADR-007.
- fireweed v1 will not prescribe a storage engine or shard implementation in the
  PRD.
- fireweed v1 does not enforce downstream API rate limits or quotas. Pacing work
  to a downstream system's accepted rate (for example a per-account,
  per-connector, or per-day send ceiling) is the responsibility of the calling
  worker. fireweed exposes claim-pacing controls (claim batch size, claim cadence,
  a future `rank`, and whole-group claims) that a caller uses to pace itself;
  fireweed itself performs no rate admission and applies no downstream-rate token
  bucket.

## Users and Scope

### Primary Persona: Queue Platform Engineer

**Role**: Engineer operating fireweed for one or more applications

**Goals**: Define queues, scale throughput, preserve durability, observe
backlogs, and keep noisy workloads isolated.

**Pain Points**: Existing queue behavior is split across brokers, tables, and
worker code; correctness and latency are hard to reason about at 10M-record scale.

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

1. General queue namespaces with isolation, routing, metrics, and independent
   scale behavior.
2. One caller-controlled `rank` (`i64`) per record orders claims within a
   stage. Timestamps (the default rank is the insertion time in µs) and
   non-timestamp priorities are both ranks (API-006 §2).
3. Strict rank ordering, with a progress guarantee (API-006 C4, C5a) and a
   claim cost of O(log n + k + b) for n records, k returned and b skipped for
   gates (C9). Bounded-relaxed ordering and per-queue progress bounds are
   retired.
4. Rank and claimability as separate concepts: claimability is stage, lease,
   `rank <= horizon`, and gates (C1).
5. Idempotent, atomic write units: insert, upsert, sparse update, stage move,
   settle, delete, claim extension, gate ops and a queue-metadata CAS, with
   exact replay by `request_id` (API-006 §4.2). The selector-resolving operator
   mutation is retired: selection is `scan` or `read`, and safety is the
   version check.
6. Batch claim, always within one declared stage, including whole-group
   claims.
7. Durable claim leases with at-least-once execution and single active lease per
   record.
8. Settling claimed records by writes carrying the claim token (stage move, new
   rank, release, delete), with per-write outcomes. There are no engine
   finalize outcomes.
9. Declared, typed fields and indexes (fixed at queue creation), queue-level
   gates, and consumer-owned queue metadata. There is no separate payload and
   no side record.
10. Observability for counts by stage and lease state, pause state, ready
    groups and the oldest ready time (`stats`), and counts over declared
    indexes (`aggregate`).
11. Performance at 10M-item hot queue scale under representative concurrent
    load: core batch operations complete with exact outcomes, monotonic progress,
    no duplicate claims, and bounded shared resources. Horizontal scale is
    achieved by distributing queues across nodes while preserving each queue's
    claim order and progress (API-006 C4, C5a). Absolute throughput and latency
    are reported for the declared topology as capacity evidence, not used as
    portable release thresholds.
    Substantiated by the recorded scale evidence (see "Scale Substantiation").
12. Retired (API-006): active-scope discovery is deleted (FR-48). Its one real
    use, a batching threshold, is the bounded `stats.groups` read (S2).
13. The queue is the unit of sharding: each queue is owned by a single node.
    Write and claim load scale beyond one storage deployment by distributing
    queues across nodes; a producer needing more than one owner's throughput for
    a logical stream partitions it across multiple queues at the application
    layer. Single-active-lease, deterministic claim ordering, and progress
    (API-006 C5a) are preserved per queue.
14. Queue density: a single deployment exercises at least 1000 cold queues plus
    one hot queue on one node. Every queue becomes progress-eligible, exact
    stage counts reconcile, and loaded behavior stays within its declared
    same-run degradation envelope. This
    requires per-queue background work (lease-expiry sweeps, summary recompute,
    request-id GC) to be
    multiplexed onto bounded shared per-node resources, never one task, loop, or
    connection per queue.
    Aggregate single-node throughput is reported for the declared node;
    multi-node deployment provides aggregate headroom.
15. The API-006 transaction contract holds on the public cell (ADR-024). Batch
    mutation success is durable (DUR1). Structured rejection has no committed
    effect (E1). Unknown outcomes resolve by `request_id` or by `read` (E2,
    RQ1–RQ4). Idempotency replay and claim exclusivity hold. A claim observes
    every acknowledged write (V2); a short or empty claim is a successful poll,
    not a command failure (C5), bounded by C5a; `read` and `scan` are fresh
    (V3). Implementation note: the cell is S3 object-log × Turso projection
    with `AsyncProjection`, and recovery replays the object log through
    projection control on the admin handle. No caller needs a second cell or a
    `Strict` barrier.
16. Applicable Class A log configurations MUST expose an
    operator-configurable commit-latency
    bound that controls group-commit cadence. Lower bounds reduce mutation
    latency and increase object-store/log request cost; higher bounds increase
    batch density and latency. The bound is a performance/cost dial only and
    MUST NOT weaken transaction integrity.

### Should Have (P1)

1. Deferred (API-006): an SQS-shaped API adapter. The contract has no remote
   API; any adapter is rebuilt later as a pure transport over `Store`, under its
   own contract, and stays secondary to it.
2. fireweed deployment-level rate limits, quotas, and tenant capacity controls
   (protecting the fireweed deployment from noisy tenants - not enforcing callers'
   downstream API limits, which are a permanent non-goal).
3. Retired (API-006): per-queue dead-letter, redrive and retention policies. A
   dead letter is a stage the consumer names, redrive is a stage move, and
   retention is a consumer concern.
4. Operational repair: pause and resume are the `"*"` gate (W9); reschedule,
   retry, fail, complete and purge are ordinary writes; ending a lease without
   its token is `Operator::force_clear`, reached only from the admin handle
   (FR-58).

### Nice to Have (P2)

1. Deferred (API-006): additional compatibility adapters, such as BullMQ-style
   or Faktory-style client APIs, wait for a transport over `Store`.
2. A hosted dashboard for queue inspection, repair, and trend analysis.
3. Retired (API-006): bounded-relaxed ordering-quality metrics. There is no
   relaxed ordering.
4. Retired: the Kafka producer wire adapter (ADR-005, superseded by ADR-007).
   API-006 has no remote API either. Consumer-side Kafka APIs remain
   permanently out of scope.

## Functional Requirements

Requirement ids are stable. **Retired (API-006)** marks a requirement the
kernel contract removed; its id is never reused. Clause ids in parentheses (for
example C4) are API-006 clauses.

### Subsystem: Queue Definition

- **FR-1** - A queue is an isolated namespace with a stable identifier,
  `(tenant, queue)`, used for routing, scaling, metrics, and operational
  control. Queues are cheap: a consumer uses one queue per job type. No
  operation and no atomic unit spans queues (A4).
- **FR-2** - Every record carries one caller-controlled `rank` (`i64`). A claim
  takes the records of its stage in ascending rank, and equal ranks go in
  insertion order (ORD1, ORD2). A queue declares no priority model, value type,
  direction, or tie-breaker.
- **FR-3** - Timestamp ordering is first-class. The default rank is the
  insertion time in µs, which gives FIFO, and a caller-supplied time in µs as
  the rank gives timestamp-ascending order. A rank in the future schedules the
  record: it is not claimable until the claim's horizon (by default, now)
  reaches it (C1).
- **FR-4** - Non-timestamp priorities are ranks too. A priority bucket is an
  offset added to the rank: negative to run sooner, positive to run later
  (API-006 §6.5).
- **FR-5** - Retired (API-006): there is no per-queue ordering mode. Every claim
  is in strict rank order within its stage (C4). Bounded-relaxed ordering is
  removed because rank ordering is the product's core and must stay simple and
  fast (owner decision, 2026-09-23 design discussion).
- **FR-6** - A queue's definition is declared when the queue is created: an
  ordered list of 1..=32 stages (default `[INITIAL, COMPLETED]`; the first is
  the insert default), up to 256 typed fields, and up to 32 indexes. The
  definition is immutable: `define_queue` is create-or-verify, a different
  definition is a conflict, and changing any part of it means a new queue
  (D1–D6, §5.1).
- **FR-56** - A queue carries consumer-owned metadata: a map of at most 64 KiB
  with a version, changed only by a metadata op inside a write unit (a sparse
  patch, optionally compare-and-swap on the version), read fresh together with
  the definition, and never interpreted by the engine (W13, R4, §5.2).
- **FR-57** - A queue can be closed with `delete_queue`, even while it holds
  records and live claims. `archive: true` archives it; a full delete is
  allowed only when the instance's delete policy is `AllowFull` (the default is
  `ArchiveOnly`). Closing is atomic and durable, every lease token of the queue
  dies, and closed is final: every later op on the queue, `define_queue`
  included, fails with `QueueClosed`, except a repeated `delete_queue`, which
  reports `already_closed` (DQ1–DQ5).

### Subsystem: Priority and Progress

- **FR-7** - A claim names one declared stage and returns the claimable records
  of that stage in `(rank, insertion sequence)` order, or the named records of a
  `Keys` claim (C1, C4). No claim orders by an index. Records of other stages,
  leased records, and records ranked beyond the horizon are never visited (C9).
- **FR-8** - Retired (API-006): bounded-relaxed ordering is removed (see FR-5).
- **FR-9** - Retired (API-006): there is no per-queue progress bound. Strict
  rank order replaces it: the lowest-ranked claimable record of a stage is the
  first candidate of the next claim of that stage, and a claim on a quiet queue
  returns the first `min(max_records, candidates)` candidates (C5a).
- **FR-10** - A record's ready age counts from when it became claimable,
  `max(rank, the time it last became Free in its stage)`. A record ranked in the
  future, or leased, is not ready (S2).
- **FR-11** - Lease expiry frees every record still attached to the expired
  claim within Δ (at most 5 s). Each record keeps its stage, rank, and insertion
  sequence, so it returns to its place in claim order, and its `attempts` count
  is kept (V4, ORD1).
- **FR-12** - Retired (API-006): there is no progress bound to enforce (see
  FR-9). The contract suite tests claim order and progress instead (C4, C5a).
- **FR-13** - Retired (API-006): without relaxed ordering there is no rank-error
  bound (see FR-5).

### Subsystem: Eligibility and Metadata

- **FR-14** - A claim returns the first candidates of its stage in claim order.
  A record is a candidate when it is in the claim's stage, Free, of
  `rank <= horizon`, and carries no Blocked gate, while the queue is not paused
  (C1, C5).
- **FR-15** - Claimability depends only on stage, lease state, rank against the
  claim horizon, and gates (C1). There is no separate not-before time, retry
  timer, or lifecycle state; a delay or a retry backoff is a rank in the future.
- **FR-16** - Records carry declared, typed fields (`Int`, `Float`, `Bool`,
  `Time`, `Str`, `Bytes`), declared with their indexes when the queue is
  created. A value is at most 64 KiB and a record at most 4 MiB. Every value
  within those bounds behaves the same: there is no "large" flag and no
  separate payload. Fields are the only per-record data; there are no side
  records (F1–F3, §5.1).
- **FR-17** - Gates can keep an otherwise claimable record unclaimed. A record
  carries up to 8 gate keys, and a gate's queue-level state (Open or Blocked) is
  set by gate ops inside write units, atomically with the unit and without
  changing any record. The reserved gate `"*"` pauses the queue (W9). Metadata
  blockers are removed: a static per-record condition is a gate or a stage.

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
  carry the queue-metadata op (W2, W13, W14). The selector-resolving operator mutation is retired: selection is
  `scan` or `read`, and safety is the version check.
- **FR-21** - A write returns one outcome per unit: the per-write outcomes of an
  applied unit, or the failing write and its reason for a rejected unit, which
  has no effect (W2).
- **FR-22** - `request_id` is optional on `write`, `claim` and
  `Operator::force_clear`; `define_queue` and `delete_queue` take none because
  they are naturally idempotent (RQ1). Replay is kept for the request-id
  retention R, which is at least 24 h. R is a contract bound, not a per-queue
  setting (V5, §6.8).

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
  an operator force-clears it (C3, L1–L4).
- **FR-25** - No record may have more than one active lease at a time (C6).
- **FR-26** - If the holder neither settles nor extends before the claim's
  expiry, every record still attached to the claim becomes claimable again
  within Δ. There is no queue redelivery policy (V4).
- **FR-27** - Every acknowledged effect survives process and node restart:
  records with their stage, rank, gates, and fields; claims and leases; gate
  state; queue metadata; and queue closure (DUR1).
- **FR-28** - The delivery guarantee is at-least-once execution with a single
  active lease; consumers remain responsible for idempotent side effects.
  `attempts` counts the claims of a record and is never reset (L6).

### Subsystem: Batch and Group Operations

- **FR-29** - Workers can claim up to 1000 records in one claim. The response
  is at most 16 MiB and always holds at least the first candidate (C7, §6.8).
- **FR-30** - A claim returns records in a deterministic order:
  `(rank, insertion sequence)` for `Next` and within each group of
  `WholeGroups`, and request order for `Keys` (C4).
- **FR-31** - A claim narrows its results by stage (one stage per claim) or by
  whole groups (`WholeGroups`). Records found through a declared index are
  claimed by key: a `scan`, then a `Keys` claim (C2). Single-group claims,
  index-ordered claims and metadata-predicate filters are removed: a record kind
  is a stage, and a compatibility key is a group or an indexed field (C1, C4).
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
- **FR-32a** - Retired (API-006): cohorts are deleted (owner decision,
  2026-09-23 design discussion). The use case is served by inserting the whole
  group in one atomic write unit, claiming it with `WholeGroups`, and checking
  the group's count in the caller.
- **FR-32b** - Retired (API-006): cohorts are deleted (see FR-32a).
- **FR-32c** - Retired (API-006): cohorts are deleted, so there is no cohort
  completion bound or expiry (see FR-32a).
- **FR-33** - Workers settle claimed records by writing them with the claim's
  token: `Release(t)` with a stage move (finish or fail), a new rank (retry with
  backoff), or a `Delete`. `Held(t)` patches a record and keeps its lease. There
  are no engine finalize outcomes (W4, W5, W7).
- **FR-34** - A rejected write unit reports the failing write and its reason,
  including a stale or wrong token (`StaleLease`), a record leased by another
  holder (`Leased`), a version mismatch, and a validation failure (W2, W4).
  There is no "already terminal" outcome.
- **FR-35** - Batch sizes and claim limits are contract constants, not queue or
  deployment configuration (§6.8).

### Subsystem: Retry, Failure, and Retention

- **FR-36** - The engine counts `attempts` (+1 per claim, never reset). Retry
  metadata is fields written by the settling write, and a retry delay is a rank
  in the future (W5, §3).
- **FR-37** - Retired (API-006): there is no queue retry policy and no engine
  failure state. A consumer compares `attempts` with its own limit and moves an
  exhausted record to a stage it names for failures.
- **FR-49** - A record can recur without limit: its holder settles each cycle
  with `Release(t)` and a new rank (the next run time), in the same stage or
  another. There is no recurring queue mode and no re-arm op.
- **FR-50** - Retired (API-006): there is no engine retry exhaustion for a
  re-rank to count against, and `attempts` is never reset. A consumer that
  needs a per-cycle retry budget stores a base count in a field and compares
  `attempts` minus that base.
- **FR-51** - A recurring record is one record per key; re-ranking it never
  creates another record.
- **FR-52** - A recurring record ends when the consumer deletes it (W7) or moves
  it to a stage that nothing claims. There is no `recurrence.until`. Idle
  backoff scheduling is caller-owned.
- **FR-53** - Idle (future-ranked) and ready records can be told apart: `stats`
  counts records by stage and lease state, and `aggregate` over a declared index
  keyed on the stage and `Rank` buckets them by rank (S1, Q4). There is no retry
  backlog or failure count to confuse them with.
- **FR-54** - A re-ranked record is claimed in rank order, like any other
  record, once its rank is at or below the claim's horizon. Recurrence adds no
  second rule.
- **FR-38** - Retired (API-006): there are no terminal outcomes or finalization
  records. A settling write is durable like any write (DUR1), a settled record
  stays in a consumer-named stage, and per-transition history travels in its
  fields (W5). No command position is exposed (§6.7).
- **FR-39** - Retired (API-006): retention is a consumer concern. Records never
  expire; the consumer deletes them (W7) or closes their queue (FR-57), and
  request ids are kept at least R (FR-22). A future delete-by-filter op may
  serve bulk retention.

### Subsystem: Observability and Operations

- **FR-40** - The queue exposes free and leased counts for each declared stage,
  and whether it is paused (S1).
- **FR-41** - The queue exposes a bounded ready-groups count and the oldest
  ready time for a stage (S2), and counts over any declared index range, by
  index key, time bucket, value range, or lease state (Q4). Progress-bound
  risk, retry backlog, and terminal failure counts are removed with the
  concepts they measured; a failure stage's size is a stage count.
- **FR-42** - Implementations emit throughput and latency telemetry for write,
  claim, and lease expiry. It is outside the `Store` contract, which exposes no
  backend introspection (API-006 §8).
- **FR-43** - Queue isolation includes noisy-neighbor protection: one queue's
  load or backlog cannot prevent another queue from making progress within its
  configured limits.
- **FR-48** - Retired (API-006): active-scope discovery is deleted. Only tests
  called it; its one real use, Seventh Sense's batching threshold, is the
  bounded `stats.groups` read (S2).
- **FR-58** - Ending a lease without its token is `Operator::force_clear`, in a
  separate trait reached only from the admin handle; `Store` has no such op.
  Normal workflows never use it as a side channel, and CI bans the `Operator`
  trait and the admin handle in Snorri, the workload and loadgen crates, and
  the contract suite (OP1–OP4).

### Subsystem: Seventh Sense Validation

- **FR-44** - fireweed can represent Seventh Sense scheduled delivery and action
  work with the scheduled time in µs as the rank, one queue per job type, and
  consumer-named stages for its steps, without Seventh Sense-specific states in
  the engine.
- **FR-45** - Seventh Sense pause, suppression, account, connector, job, and
  campaign controls are gates: a status change is one gate op that blocks or
  reopens every record carrying that gate at once, without rewriting records
  (W9). There are no metadata blockers. Downstream delivery-pacing and quota
  constraints are **not** core queue features and are **not** claimability
  conditions: callers pace claims using `max_records`, claim cadence, a future
  rank, and whole-group claims (see Non-Goals). fireweed does not model a
  downstream rate or quota as a stage or as claim-time admission control.
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
- **FR-47b** - fireweed can claim batches bounded by a downstream per-call entity
  limit, such as the Marketo lead-enrichment limit of 300 distinct leads per API
  call, with `WholeGroups { max_groups: 300 }`: every currently claimable task
  of each selected lead, as whole groups, in one atomic claim (C3, C4).
- **FR-47c** - fireweed can execute a Seventh Sense `actions_scheduled`
  callback batch (`batch_checksum` / `callback_id`) together: each batch is
  inserted as one group in one atomic write unit, claimed with `WholeGroups`,
  and checked against its expected count by the caller, with no `callback_id`
  semantics in the engine. Cohorts are deleted (API-006 §8); this rewording
  replaces the earlier cohort claim (ADR-025 KC-12, draft D14).
- **FR-55** - fireweed can represent Seventh Sense `jobs_queue` and
  `connectors_queue` singleton recurring rows: one record per job or connector
  key in its type's queue, re-ranked on settle with the next processing time
  (`{Release(t), Put{rank}}`), and removed with a `Delete` when the underlying
  entity is removed, without job or connector concepts in the engine.

## Acceptance Test Sketches

These sketches predate API-006 and use API-001 vocabulary. Rows for retired
requirements (FR-5, FR-8, FR-9, FR-12, FR-13, FR-32a to FR-32c, FR-37, FR-38,
FR-39, FR-48, FR-50) are retired with them, and FR-47c's cohort rows give
way to its reworded requirement. The rows that cite API-001
map to API-006 clauses: request-id conflict to RQ2, optimistic update conflict
to W3 (`VersionMismatch`), leased update conflict to W4 (`Leased`), and claim
retry idempotency to C8. The tenant-spoofing and SQS-adapter rows are retired
with the remote API, and the `rearm` rows with the recurring mode. The other
rows keep their intent under the rewritten requirements. The governing
acceptance scenarios are API-006 §9 (CS6) and TP-003.

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
| FR-45, Non-Goals | No downstream rate enforcement | Queue with many eligible items for one `group_key`; worker claims with `max_items=25` then pauses between calls | fireweed applies no rate-based throttling: each `BatchClaim` returns up to 25 items subject only to normal eligibility and `max_items` (a short or empty batch is still valid per API-001), never withholding work for a downstream-rate reason; pacing is determined entirely by the worker's `max_items` and call cadence |
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

This PRD records product requirements, including the closed public storage axes
and durability classes. Storage algorithms, shard strategy, indexing, protocol,
and deployment topology belong in later technical design.

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
- At-least-once execution with single active lease is the correct durability
  contract; exactly-once side effects are caller-owned.
- Batch and group-aware operations are required for the first useful release.
- A timestamp rank uses a producer-supplied scheduled time in µs; producers are
  responsible for the business meaning of that timestamp.

### Dependencies

- Seventh Sense production workload data for realistic load profiles, group
  distributions, priority skew, and downstream API batch constraints.
- The S3×Turso technical designs (TD-017 for the Turso projection layout,
  TD-018 for the partitioned object log) and mapped queue placement with no
  rebalancing (ADR-025 OD-13). These substantiate the horizontal envelope in
  Success Metrics.
- API-006 for the kernel contract. Any remote or compatibility adapter is a
  later transport over `Store`, not the contract.
- Retired: a separate operator/retention contract. Repair is
  `Operator::force_clear` plus ordinary writes (API-006 §7), whole queues close
  with `delete_queue`, and retention is a consumer concern.

## Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Strict priority creates a scalability bottleneck | High | High | Keep every claim a stage-bound index walk of O(log n + k + b) (API-006 C9), held by the TP-003 claim-path performance gates |
| Relaxed ordering misses business scheduling expectations | n/a | n/a | Retired (API-006): there is no relaxed ordering |
| Group-aware claims starve other groups | Medium | High | `WholeGroups` orders groups by their first candidate in rank order (API-006 C4) |
| Idempotency state grows without bound | Medium | Medium | Require a documented idempotency retention window |
| Generic model becomes too Seventh Sense-specific | Medium | High | Keep Seventh Sense states in consumer-named stages and declared fields, not engine states |
| Performance tests use unrealistic uniform workloads | High | Medium | Include skewed priority, future-scheduled, leased, retry, and group-heavy test profiles |

## Resolved API Decisions

API-006 supersedes these decisions where they conflict. Retired: the
progress-bound metrics (there is no progress bound), retry as `pending` with
`not_before` (a retry is a new rank), the whole-cohort claim mode (cohorts are
deleted; `WholeGroups` is the remaining all-or-nothing group claim), the
SQS-shaped adapter as P1 work (no remote API), recurring items as a queue mode
(a recurring record is re-ranked on settle), and discovery as the fairness
mechanism (deleted). Pacing uses `max_records`, claim cadence, a future rank,
and `WholeGroups`, not `not_before` and `compatibility` filters. The first
client surface is API-006, not API-001.

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
- The first client surface is the native fireweed API defined in API-001. An
  SQS-shaped adapter is P1 compatibility work and cannot represent mutable
  priority or schedule updates.
- v1 scale is committed in two envelopes - single-deployment and horizontal
  cross-queue scale-out. Both preserve exact outcomes, queue-global progress,
  and bounded shared resources under load, and each references a recorded
  evidence artifact (see "Scale
  Substantiation"). The PRD states product envelopes only; storage backend and
  placement mechanism live in the governing design/test artifacts.
- Downstream API rate/quota enforcement is a non-goal of the fireweed engine, not a
  deferred feature. Callers pace claim output using `max_items`, claim cadence,
  `not_before`, and group selection (`compatibility.group_key` / `same_group_key`
  / `metadata_equals` / `group_batching.max_groups`). The `rate_limited` item
  status and the rate-limit error row are reserved exclusively for fireweed's own
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
  migration topology but does NOT block generic fireweed implementation, since
  `group_key` topology and `progress_bound_ms` are per-queue configuration.
  Under API-006 neither question applies: there is no progress bound, cohorts
  are deleted, and a queue's whole-group axis is its `group`.

## Success Criteria

fireweed is successful when a general-purpose queue can be created with
declared stages, fields, and indexes, accepts and updates work idempotently at
production scale, claims claimable work in rank order without starvation,
survives worker failure through leases, and supports group-aware
batch execution. For Seventh Sense, success means scheduled delivery/action work
can move to fireweed without losing timestamp scheduling correctness, operational
visibility, or throughput.
