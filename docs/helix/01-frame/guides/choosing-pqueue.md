---
ddx:
  id: guide-choosing-pqueue
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: discover-meta-asynchronous-computing-learnings}
---

# Choosing pqueue Instead of a Stream

Use pqueue when the application owns durable work state whose eligibility,
priority, lease, retry, and completion state can change per item. Use an
immutable sequential stream when consumers only need ordered append records,
offset progress, replay, and fan-out over the same record sequence.

This guide applies the product boundary in the [PRD](../prd.md) and the local
[Meta Async discovery note](../../00-discover/meta-asynchronous-computing-learnings.md).
That discovery note is evidence for the primitive selection boundary; it is not
a compatibility or performance claim about FOQS or pqueue.

## Decision Table

| Need | Choose pqueue when | Choose a stream when |
|---|---|---|
| Mutable or arbitrary priority | Workers must claim the highest-priority eligible item, and producers may update priority before terminal completion. | Record order is append order or partition order, and changing priority would mean writing another event for consumers to interpret. |
| `not_before` scheduling | Each item can become eligible at a future timestamp, and workers should skip ineligible items without advancing past them forever. | Consumers can read every record in sequence and decide locally whether to defer action. |
| Leases | Work must be claimed by one active worker at a time, recovered after lease expiry, and finalized with complete, retry, fail, or release semantics. | Consumers keep their own processing state, and the storage primitive only needs to expose records and committed offsets. |
| Item-level delayed retry | A failed item needs its own retry delay, attempt state, and later re-entry into claim order. | Retry is a consumer concern, often expressed by writing a new event, seeking, or handling a side channel. |
| Groups and cohorts | Claims must batch compatible work by account, connector, job, campaign, domain, or another grouping key while still preserving queue progress. | A partition key or topic layout already provides the needed cohort ordering, and consumers do not need item-level claim selection. |
| Immutable sequential batches | Batches are selected by queue eligibility, priority, and group compatibility rather than by contiguous log position. | Consumers should process contiguous records in append order or partition order. |
| Offsets | Worker progress is lease/finalize state on individual items, not a durable consumer offset in an immutable log. | Each consumer or group advances an offset through a record sequence. |
| Replay | Operational recovery replays pqueue's durable state changes to rebuild queue state; application workers should not use that log as their consumption API. | Consumers are expected to replay historical records as part of normal application behavior. |
| Broadcast consumption | One work item should be completed once by one worker, unless the producer intentionally enqueues separate items per recipient. | Multiple independent consumer groups should each observe the same record stream. |

## Use pqueue

- Scheduled delivery or action execution where `not_before`, priority updates,
  idempotent writes, leases, and delayed item retry are part of the work state.
- Connector, enrichment, or API work that must be claimed in account,
  connector, job, or campaign cohorts so downstream batches are compatible.
- Recovery-sensitive work where a worker crash must leave a lease to expire and
  return the same item to eligible claim order without duplicate active owners.
- A mutable backlog where operators or callers need to reschedule, retry,
  release, fail, or complete individual items.

## Do Not Use pqueue

- Event distribution where every subscriber should observe every event. Use a
  stream or pub/sub system with independent consumer progress.
- Audit logs, analytics ingestion, or CDC pipelines where immutable append
  order, offsets, retention, and replay are the main contract.
- Workloads where consumers can process sequential batches and do not need
  arbitrary priority, per-item leases, or item-level delayed retry.
- Transport or scheduler problems such as downstream rate tokens, worker-runtime
  placement, load balancing, or quota enforcement. Keep those policies in the
  caller or adjacent scheduler/router layer.

## Change Log Versus Stream Consumption

pqueue has a durable change log internally because supported storage profiles
must recover and rebuild queue state without losing committed mutations. That
log records queue state transitions such as push, update, claim, finalize, and
repair operations. It is not the worker consumption model.

Workers consume pqueue by claiming eligible items under leases and finalizing
those items. They do not advance offsets through pqueue's internal log, and
they should not depend on replaying that log as an application event stream. If
the application contract is "every consumer group reads the same ordered
history," use a stream and keep pqueue for the mutable work queues that need
leased execution.
