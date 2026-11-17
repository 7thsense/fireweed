---
ddx:
  id: product-vision
  review:
    self_hash: d70aaff09b5d5f59211e5ef3ae9156ee30776e95bce7a70398978e83e39d39e8
    deps: {}
    reviewed_at: "2026-07-20T00:01:19Z"
---

# Product Vision

## Mission Statement

fireweed is a batch-centric state-machine queue engine for applications that need
ordered, recoverable work execution at scale. One simple claim-group/commit-group interface works on PostgreSQL and S3/object-log with Turso. Durable acknowledgement, atomic workflow state and next work, native filtered priority claims, fresh successful reads and recoverable retries are mandatory on both. Seventh Sense is the first validation workload.

## Positioning

For engineers building high-volume scheduling and execution systems, fireweed is a
durable queue that orders eligible work by a queue-defined priority model and
maps that workflow onto the right backing store without exposing the storage
protocol to callers. Unlike FIFO queues with scheduler logic layered around them,
fireweed makes priority, eligibility, claim leases, retries, final state, and
transaction integrity part of the queue contract.

## Vision

When fireweed succeeds, applications have one dependable primitive for accepting,
ordering, claiming, retrying, and completing work.

**North Star**: Every accepted item is executed according to its queue's
priority, progress, and selected durability-class guarantees, with no lost work
inside that boundary, no concurrent execution of the same claim, and an
explicit final state.

## Queue product boundary

[ADR-027](../02-design/adr/ADR-027-data-plane-interface-is-the-implementation-boundary.md) and [API-008](../02-design/contracts/API-008-data-plane-dsl.md) define Fireweed's interface. A consumer declares its queues and tables in a schema document and changes them with programs: lists of typed statements that run as one transaction. An executor creates real tables and indexes from the schema and runs each statement as plain SQL. PostgreSQL 18 is first and production-supported; a local engine (SQLite, and Turso with an S3 log) follows and passes the same conformance documents. One program can claim a filtered batch in due-time order, settle it, update workflow state and insert next work atomically. Counts and state reads are indexed selects and declared reads. Consumers never write SQL, manage storage transactions or filter a broadly claimed queue.

Success acknowledges durable effects, and every later program sees them. A request id replays its recorded response; a request issued before the space's horizon is refused rather than run twice. No executor exposes its database session to consumers.

The [Architecture](../02-design/architecture.md) owns module boundaries. Its configuration, diagnostics, durable-record and Snorri contracts define the construction and integration details. Helix v0.15.4 concerns apply to this library: encapsulation, typed configuration, caller-owned telemetry/logging, and precisely scoped formal analysis. Passing historical models or legacy facade tests does not qualify the new interface.

## User Experience

Engineers create a queue with a priority model, push or update work
idempotently, claim compatible batches of eligible items, and record outcomes.

## Target Market

| Attribute | Description |
|-----------|-------------|
| Who | Engineers building durable, high-volume async work systems |
| Pain | FIFO queues and ad hoc scheduler tables do not model priority, eligibility, leases, batching, and retries as one contract |
| Current Solution | Message brokers, sorted sets, database tables, and worker-specific retry logic |
| Why They Switch | Priority-aware execution, durable lifecycle state, group-aware batching, and horizontal scale beyond a single database belong in the queue primitive, on infrastructure that infrastructure teams already operate. Horizontal scale is a v1 commitment substantiated by portable TP-002 evidence: queue-global progress, correctness, bounded shared resources, and same-run behavior as queues, owners, and load increase. Machine-specific capacity is published for declared topologies, not used as a universal release gate. |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| Configurable priority ordering | Queues can model timestamp, numeric, score, or other ordered work without changing worker code |
| Bounded progress guarantees | Relaxed priority ordering can scale without starving eligible work |
| Durable execution lifecycle | Work remains recoverable across worker and process failures |
| Batch and group-aware claims | Workers can efficiently satisfy downstream API batch constraints |
| Atomic queue and workflow integrity | Commit claimed work, workflow state and next work together, with durable acknowledgement and exact replay on both backends |
| Tunable durability economics | Operators can configure the object-log segment size target and maximum flush latency to trade batching delay against request cost and batch density |
| One interface, two durable implementations | PostgreSQL is the baseline; S3/object-log with Turso must pass the same guarantees and comparable settled benchmarks |

## Success Definition

| Criterion | Definition |
|-----------|------------|
| Priority correctness | Claims follow the queue's configured priority and progress contract |
| Durable execution safety | No accepted item is lost or concurrently held by multiple active claims |
| Transaction contract | The public cell satisfies one mutation, rejection, and request-replay contract. Class A success survives on the object log. Claim polls applied rows under `AsyncProjection` |
| Scale readiness | Hot queues with 10M resident items remain writable, claimable, observable, and exactly recoverable under ordinary concurrent load. Horizontal deployments distribute **queues across independent owner nodes** while preserving queue-global progress, claim safety, and bounded shared resources. A node exercises at least 1000 concurrently active queues without lost or duplicate work. Same-run baseline/load comparisons detect material degradation; absolute rates and latency percentiles are capacity evidence tied to the declared host and topology, never portable release bars. Substantiated by TP-002 E1 single-deployment, E2 cross-queue and density, and E3 object-log evidence. |
| Seventh Sense validation | Timestamp-ascending delivery queues meet Seventh Sense scheduling, idempotency, batch, and latency requirements |

## Why Now

Seventh Sense needs a shared queue backbone for several scheduled and
queue-like systems, but the underlying problem is general. Defining fireweed as a
general durable priority queue now prevents Seventh Sense-specific table and
worker assumptions from becoming the core product contract.
