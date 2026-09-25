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

fireweed is a batch-centric durable queue engine for applications that need
ordered, recoverable work execution at scale. It gives one transaction
contract (API-006): a successful write is durable, a rejected write has no
effect, and an unknown outcome is resolved by request identity or by reading
what it touched, never by caller-side storage choreography. Seventh Sense is
the first validation use case: time-ordered delivery work with idempotent
writes, durable claims, batch execution, and no lost work.

## Positioning

For engineers building high-volume scheduling and execution systems, fireweed
is a durable queue whose records are claimed in rank order within declared
stages, without exposing its storage to callers. Unlike FIFO queues with
scheduler logic layered around them, fireweed makes rank, claimability, claim
leases, retries, and transaction integrity part of the queue contract.

## Vision

When fireweed succeeds, applications have one dependable primitive for adding,
ordering, claiming, retrying, and settling work.

**North Star**: Every accepted record is claimed in rank order within its
stage, with no lost work and no concurrent execution of the same claim, and it
stays in the stage its consumer chose until the consumer moves or deletes it.

### Implementation

ADR-024 fixes the one production implementation of the contract: an S3 object
log with a Turso projection.

- The object log (`fireweed-objectlog`) is the durable authority. Its
  filesystem publication adapter tests the log protocol without an S3
  endpoint; it is not a supported deployment.
- The Turso projection is derived. It serves claims and reads, and it is
  rebuilt from the log through projection control on the implementation's
  admin handle.
- The control plane maps each queue to one partition. It is not a storage
  tier.

The contract never names any of this. A second implementation would implement
the `Store` trait directly and pass the same conformance suite.

## User Experience

Engineers declare a queue's stages, fields, and indexes; add and update
records idempotently by key; claim batches of one stage in rank order, as
whole groups, or by key; and settle each claimed record with a write that
moves, re-ranks, or deletes it.

## Target Market

| Attribute | Description |
|-----------|-------------|
| Who | Engineers building durable, high-volume async work systems |
| Pain | FIFO queues and ad hoc scheduler tables do not model order, schedule, leases, batching, and retries as one contract |
| Current Solution | Message brokers, sorted sets, database tables, and worker-specific retry logic |
| Why They Switch | Rank-ordered execution, durable lease state, group-aware batching, and horizontal scale beyond a single database belong in the queue primitive, on infrastructure that infrastructure teams already operate. Horizontal scale is a v1 commitment substantiated by portable TP-002 evidence: per-queue progress, correctness, bounded shared resources, and same-run behavior as queues, nodes, and load increase. Machine-specific capacity is published for declared topologies, not used as a universal release gate. |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| One rank for order and schedule | Timestamps, numeric priorities, priority offsets, and future schedules are all one `i64` rank, without changing worker code |
| Strict order at logarithmic cost | Every claim takes its stage's lowest-ranked claimable records at a cost logarithmic in queue size, with a progress guarantee, so order is never traded for throughput |
| Durable execution | Work survives worker and process failures; an expired lease returns records to their place in claim order |
| Batch and group-aware claims | Workers meet downstream batch constraints with batch claims, whole-group claims, and claims by key |
| Transaction integrity | Success is durable, rejection has no effect, request-id replay is exact, and a claim observes every acknowledged write. An empty claim is a successful poll, not a lost mutation |
| Tunable durability economics | Operators set the object log's commit-latency bound to trade write latency against request cost and batch density |
| One production implementation | S3 object-log durability with a rebuildable Turso projection (ADR-024) |

## Success Definition

| Criterion | Definition |
|-----------|------------|
| Order correctness | Claims follow rank order within a stage and the progress rule (API-006 C4, C5a) |
| Durable execution safety | No accepted record is lost or held by two live claims |
| Transaction contract | The S3×Turso implementation passes the API-006 conformance suite: durable success, effect-free rejection, exact request-id replay, and claims that observe every acknowledged write |
| Scale readiness | Hot queues with 10M resident records remain writable, claimable, observable, and exactly recoverable under ordinary concurrent load. Horizontal deployments distribute **queues across independent nodes** while preserving per-queue progress, claim safety, and bounded shared resources. A node exercises at least 1000 concurrently active queues without lost or duplicate work. Same-run baseline/load comparisons detect material degradation; absolute rates and latency percentiles are capacity evidence tied to the declared host and topology, never portable release bars. Substantiated by TP-002 E1 single-deployment, E2 cross-queue and density, and E3 object-log evidence. |
| Seventh Sense validation | Delivery queues ranked by scheduled time meet Seventh Sense scheduling, idempotency, batch, and latency requirements |

## Why Now

Seventh Sense needs a shared queue backbone for several scheduled and
queue-like systems, but the underlying problem is general. Defining fireweed as a
general durable priority queue now prevents Seventh Sense-specific table and
worker assumptions from becoming the core product contract.
