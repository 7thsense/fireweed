---
ddx:
  id: discover-foqs-scaling-distributed-priority-queue
  type: resource-summary
  links:
    - {kind: informs, to: prd}
---

# FOQS: Scaling a Distributed Priority Queue

## Source

- Publisher: Meta Engineering
- Published: 2021-02-22
- URL: <https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/>
- Accessed: 2026-07-22

## Summary

Meta describes FOQS as an internal, persistent, multitenant distributed priority queue built on sharded
MySQL. The post covers its five-operation Thrift interface, namespace/topic/item model, priority and delay
ordering, leases and redelivery, active-topic discovery, pull-based consumption, and demand-sensitive
prefetching.

## Relevant Findings

- The public interface is intentionally small: `Enqueue`, `Dequeue`, `Ack`, `Nack`, and
  `GetActiveTopics`.
- A dequeue request accepts multiple `(topic, count)` pairs and returns at most the requested count for
  each topic. This gives consumers one fan-in operation without claiming cross-topic atomicity.
- Items separate integer priority from `deliver_after`, carry immutable payload plus mutable metadata, and
  use leases for redelivery. `Nack` can delay redelivery and update metadata.
- Topics are cheap, dynamic logical priority queues. Consumers discover only topics with ready work rather
  than polling every possible topic.
- Pull consumption keeps downstream pacing with consumers. FOQS adds a routing layer to solve discovery
  while avoiding consumer-overload policy in the queue.
- Prefetch demand follows dequeue request rate. The post describes strict ordering as future work, so its
  architecture is evidence for scalable priority delivery rather than strict global priority order.

## HELIX Usage

This resource informs the PRD's reference-system analysis and future review of
`API-001-native-client-interface.md`. Use it to evaluate API ergonomics, active-scope discovery,
multi-queue worker fan-in, delayed retry, and the boundary between queue storage and worker pacing.

## Authority Boundary

FOQS is an internal Meta system described at architecture level; the post is not a released protocol,
compatibility target, or correctness specification. It does not override pqueue's batch result semantics,
mutable priority, idempotency, progress bounds, group/cohort claims, backend-independent transaction
contract, or queue-defined eligibility. Any interface change belongs in API-001 after PRD framing.

## Review Checklist

- [x] Source URL and access date are present
- [x] Summary is concise and source-faithful
- [x] Findings are relevant to pqueue discovery
- [x] HELIX usage is specific
- [x] Boundary prevents over-applying the source
