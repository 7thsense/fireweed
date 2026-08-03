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
ordered, recoverable work execution at scale. It provides one external
transaction contract across an interchangeable log-by-projection storage
matrix: accepted mutations are visible at the selected response barrier,
rejected mutations have no committed effect, and ambiguous retries are resolved
by request identity rather than by caller-side storage choreography. Durability
after process death follows the selected log class instead of being overstated
as uniform. Seventh Sense is the first validation use case: timestamp-ordered
delivery work with idempotent writes, durable claims, batch execution, and no
lost work.

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

### Storage product law

The public storage product is exactly five log backends (`memory`, `sqlite`,
`postgres`, `filesystem`, `s3`) crossed with three projections (`memory`,
`sqlite`, `postgres`): 15 supported cells assembled through one typed
composition model. The control plane is a separate optional topology choice,
not a mandatory PostgreSQL tier or a bundled storage product. Public product paths
use native-async composition; a blocking store may be isolated behind a bounded
adapter actor without changing that public execution model.

Logs define the cross-process durability class. `sqlite`, `postgres`,
`filesystem`, and `s3` logs are Class A: the durable log is authoritative and a
projection can be rebuilt by high-water plus tail replay. The `memory` log is
Class B: after process death only a durable projection can remain, so the
product makes no log-rebuild, branch, read-as-of, or log-derived change-record
claim for those three cells. Filesystem and S3 are peer implementations of the
same object-log protocol; Postgres is first-class on both public axes.

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
| Composition-independent transaction integrity | All 15 cells preserve commit, visibility, rejection, and idempotency semantics; restart recovery follows the cell's explicit Class A or Class B boundary |
| Tunable durability economics | Operators can choose a minimum/maximum commit latency bound that trades mutation latency against object-log request cost and batch density |
| Independent serving and durability choices | Operators select log durability independently from memory, SQLite, or Postgres serving projections without adopting a separate product profile |

## Success Definition

| Criterion | Definition |
|-----------|------------|
| Priority correctness | Claims follow the queue's configured priority and progress contract |
| Durable execution safety | No accepted item is lost or concurrently held by multiple active claims |
| Transaction contract | Every supported cell satisfies the same mutation, visibility, rejection, and request-replay contract; Class A success survives through the durable log, while Class B persistence is limited to the selected projection |
| Scale readiness | Hot queues with 10M resident items remain writable, claimable, observable, and exactly recoverable under ordinary concurrent load. Horizontal deployments distribute **queues across independent owner nodes** while preserving queue-global progress, claim safety, and bounded shared resources. A node exercises at least 1000 concurrently active queues without lost or duplicate work. Same-run baseline/load comparisons detect material degradation; absolute rates and latency percentiles are capacity evidence tied to the declared host and topology, never portable release bars. Substantiated by TP-002 E1 single-deployment, E2 cross-queue and density, and E3 object-log evidence. |
| Seventh Sense validation | Timestamp-ascending delivery queues meet Seventh Sense scheduling, idempotency, batch, and latency requirements |

## Why Now

Seventh Sense needs a shared queue backbone for several scheduled and
queue-like systems, but the underlying problem is general. Defining fireweed as a
general durable priority queue now prevents Seventh Sense-specific table and
worker assumptions from becoming the core product contract.
