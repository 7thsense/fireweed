---
ddx:
  id: guide-choosing-fireweed
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: discover-meta-asynchronous-computing-learnings}
    - {kind: informed_by, to: api-fireweed-kernel-contract}
---

# Choosing Fireweed Instead of a Stream

Use Fireweed when the application owns durable work state that changes per
record: its rank, its stage, its lease, and its fields. Use an immutable
sequential stream when consumers only need ordered append records, offset
progress, replay, and fan-out over the same record sequence.

This guide applies the product boundary in the [PRD](../prd.md), the
[kernel contract](../../02-design/contracts/API-006-fireweed-kernel-contract.md)
and the local
[Meta Async discovery note](../../00-discover/meta-asynchronous-computing-learnings.md).
That discovery note is evidence for the primitive selection boundary; it is not
a compatibility or performance claim about FOQS or Fireweed.

## Decision Table

| Need | Choose Fireweed when | Choose a stream when |
|---|---|---|
| Mutable or arbitrary order | Workers must claim the lowest-ranked claimable record of a stage, and producers may change a record's rank at any time before it is settled. | Record order is append order or partition order, and changing order would mean writing another event for consumers to interpret. |
| Scheduling | A record ranked in the future is skipped by claims until the claim horizon reaches its rank, and then takes its place in claim order. | Consumers can read every record in sequence and decide locally whether to defer action. |
| Leases | Work must be claimed by one holder at a time, come back after lease expiry, and be settled by a write: a stage move, a new rank, or a delete. | Consumers keep their own processing state, and the storage primitive only needs to expose records and committed offsets. |
| Per-record delayed retry | A failed record needs its own backoff and attempt count, and re-enters claim order at its new rank. | Retry is a consumer concern, often expressed by writing a new event, seeking, or handling a side channel. |
| Groups | Claims must take compatible work whole, by account, connector, job, lead, or another group key, or by keys found through a declared index. | A partition key or topic layout already provides the needed grouping, and consumers do not need per-record claim selection. |
| Immutable sequential batches | Batches are selected by stage, rank, gates, and groups rather than by contiguous log position. | Consumers should process contiguous records in append order or partition order. |
| Offsets | Worker progress is the stage and lease state of individual records, not a durable consumer offset in an immutable log. | Each consumer or group advances an offset through a record sequence. |
| Replay | Fireweed rebuilds its own serving state from its durable log; application workers do not consume that log. | Consumers are expected to replay historical records as part of normal application behavior. |
| Broadcast consumption | One record is settled once by one holder, unless the producer writes separate records per recipient. | Multiple independent consumer groups should each observe the same record stream. |

## Use Fireweed

- Scheduled delivery or action execution where the scheduled time is a rank
  that may change, and idempotent writes, leases, and per-record delayed retry
  are part of the work state.
- Connector, enrichment, or API work that must be claimed as whole groups
  (account, connector, job, lead) so downstream batches are compatible.
- Recovery-sensitive work where a worker crash must let its lease expire and
  return the same records to their place in claim order, never to two live
  holders.
- A mutable backlog where operators or callers reschedule, retry, move, or
  delete individual records by key.

## Do Not Use Fireweed

- Event distribution where every subscriber should observe every event. Use a
  stream or pub/sub system with independent consumer progress.
- Audit logs, analytics ingestion, or CDC pipelines where immutable append
  order, offsets, retention, and replay are the main contract.
- Workloads where consumers can process sequential batches and do not need
  arbitrary order, per-record leases, or per-record delayed retry.
- Transport or scheduler problems such as downstream API rate tokens,
  downstream quotas, worker-runtime placement, or load balancing. Keep those
  policies in the caller or an adjacent scheduler/router layer; Fireweed's own
  deployment and tenant capacity controls are a separate product concern.

## Change Log Versus Stream Consumption

Fireweed has a durable log internally. Every acknowledged write, claim, and
queue close is recorded in its S3 object log, and its serving projection is
rebuilt from that log. The log is not the worker consumption model, and the
contract exposes no log position or history: the only change-feed promise is
that a record's `version` strictly increases (API-006 §6.7).

Workers consume Fireweed by claiming records under leases and settling them
with writes. They do not advance offsets through Fireweed's internal log, and
they cannot replay it as an application event stream. If the application
contract is "every consumer group reads the same ordered history," use a
stream and keep Fireweed for the mutable work queues that need leased
execution.
