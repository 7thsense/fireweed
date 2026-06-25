---
ddx:
  id: product-vision
  review:
    self_hash: d5543b685e3f48406e03429eda380418943be1c1b152483fe349ef466d5dfaa1
    deps: {}
    reviewed_at: "2026-06-25T04:21:18Z"
---

# Product Vision

## Mission Statement

pqueue is a durable priority queue engine for applications that need ordered,
recoverable work execution at scale. Seventh Sense is the first validation use
case: timestamp-ordered delivery work with idempotent writes, durable claims,
batch execution, and no lost work.

## Positioning

For engineers building high-volume scheduling and execution systems, pqueue is a
durable queue that orders eligible work by a queue-defined priority model. Unlike
FIFO queues with scheduler logic layered around them, pqueue makes priority,
eligibility, claim leases, retries, and final state part of the queue contract.

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
| Why They Switch | Priority-aware execution, durable lifecycle state, group-aware batching, and horizontal scale beyond a single database belong in the queue primitive, on infrastructure that infrastructure teams already operate. Horizontal scale is a v1 commitment substantiated by recorded benchmark evidence (TP-002 E2 cross-queue scale-out and E3 object-log profile prove the per-queue floor of ≥10M items/hr holds for every queue at any scale, E0), not an aspiration. |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| Configurable priority ordering | Queues can model timestamp, numeric, score, or other ordered work without changing worker code |
| Bounded progress guarantees | Relaxed priority ordering can scale without starving eligible work |
| Durable execution lifecycle | Work remains recoverable across worker and process failures |
| Batch and group-aware claims | Workers can efficiently satisfy downstream API batch constraints |

## Success Definition

| Criterion | Definition |
|-----------|------------|
| Priority correctness | Claims follow the queue's configured priority and progress contract |
| Durable execution safety | No accepted item is lost or concurrently held by multiple active claims |
| Scale readiness | Every queue sustains at least 10M items/hr (the per-queue floor, E0), and that floor holds for any queue at any deployment scale: hot queues with millions of items stay writable, claimable, and observable under production load, scaling horizontally beyond a single database by distributing **queues across independent owner nodes** (the queue is the unit of sharding) while preserving each queue's queue-global progress guarantee and the per-queue floor for every queue. A single node supports at least 1000 concurrently active queues (queue density) with no cross-queue degradation. Substantiated by the recorded scale evidence the PRD and design/test artifacts reference (TP-002 E1 single-deployment, E2 cross-queue + multi-queue density scale-out, E3 object-log profile) |
| Seventh Sense validation | Timestamp-ascending delivery queues meet Seventh Sense scheduling, idempotency, batch, and latency requirements |

## Why Now

Seventh Sense needs a shared queue backbone for several scheduled and
queue-like systems, but the underlying problem is general. Defining pqueue as a
general durable priority queue now prevents Seventh Sense-specific table and
worker assumptions from becoming the core product contract.
