---
ddx:
  id: product-vision
  review:
    self_hash: d70aaff09b5d5f59211e5ef3ae9156ee30776e95bce7a70398978e83e39d39e8
    deps: {}
    reviewed_at: "2026-07-19T23:13:10Z"
---

# Product Vision

## Mission Statement

pqueue is a batch-centric state-machine queue engine for applications that need
ordered, recoverable work execution at scale. It provides one external
transaction contract across local memory, SQLite, Postgres, and object-log
deployment profiles: accepted mutations are durable and visible, rejected
mutations have no durable effect, and ambiguous retries are resolved by
idempotency keys rather than by caller-side storage choreography. Seventh Sense
is the first validation use case: timestamp-ordered delivery work with
idempotent writes, durable claims, batch execution, and no lost work.

## Positioning

For engineers building high-volume scheduling and execution systems, pqueue is a
durable queue that orders eligible work by a queue-defined priority model and
maps that workflow onto the right backing store without exposing the storage
protocol to callers. Unlike FIFO queues with scheduler logic layered around them,
pqueue makes priority, eligibility, claim leases, retries, final state, and
transaction integrity part of the queue contract.

## Vision

When pqueue succeeds, applications have one dependable primitive for accepting,
ordering, claiming, retrying, and completing work.

**North Star**: Every accepted item is durably executed according to its queue's
priority and progress guarantees, with no lost work, no concurrent execution of
the same claim, and an explicit final state.

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
| Backend-independent transaction integrity | Callers see the same commit, visibility, idempotency, and recovery guarantees regardless of storage profile |
| Tunable durability economics | Operators can choose a minimum/maximum commit latency bound that trades mutation latency against object-log request cost and batch density |
| Redis-hot, S3-durable profile | Local memory or SQLite projections can serve hot queue operations while an object log provides durable replay and cluster-scale queue count |

## Success Definition

| Criterion | Definition |
|-----------|------------|
| Priority correctness | Claims follow the queue's configured priority and progress contract |
| Durable execution safety | No accepted item is lost or concurrently held by multiple active claims |
| Transaction contract | Every implementation profile satisfies the same externally visible mutation contract: success means durable and visible, rejection means no committed effect, and unknown outcomes are resolvable by `request_id` without duplicate state-machine transitions |
| Scale readiness | Hot queues with 10M resident items remain writable, claimable, observable, and exactly recoverable under ordinary concurrent load. Horizontal deployments distribute **queues across independent owner nodes** while preserving queue-global progress, claim safety, and bounded shared resources. A node exercises at least 1000 concurrently active queues without lost or duplicate work. Same-run baseline/load comparisons detect material degradation; absolute rates and latency percentiles are capacity evidence tied to the declared host and topology, never portable release bars. Substantiated by TP-002 E1 single-deployment, E2 cross-queue and density, and E3 object-log evidence. |
| Seventh Sense validation | Timestamp-ascending delivery queues meet Seventh Sense scheduling, idempotency, batch, and latency requirements |

## Why Now

Seventh Sense needs a shared queue backbone for several scheduled and
queue-like systems, but the underlying problem is general. Defining pqueue as a
general durable priority queue now prevents Seventh Sense-specific table and
worker assumptions from becoming the core product contract.
