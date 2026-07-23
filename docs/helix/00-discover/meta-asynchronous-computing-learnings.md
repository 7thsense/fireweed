---
ddx:
  id: discover-meta-asynchronous-computing-learnings
  type: resource-summary
  links:
    - {kind: informs, to: prd}
---

# Asynchronous Computing at Meta: Overview and Learnings

## Source

- Publisher: Meta Engineering
- Published: 2023-01-31
- URL: <https://engineering.fb.com/2023/01/31/production-engineering/meta-asynchronous-computing/>
- Accessed: 2026-07-22

## Summary

Meta describes separating asynchronous workload storage and ingestion from transport, flow control, and
compute execution after a central dispatcher accumulated too many responsibilities. It also compares
priority queues with streams and identifies leases, arbitrary ordering, and granular delayed retry as the
queue's flexibility advantages and lifecycle/index cost as their price.

## Relevant Findings

- The overloaded dispatcher combined queue lifecycle, dequeue-rate adaptation, prioritization, rate and
  quota management, downstream protection, load balancing, and worker-runtime management.
- Meta separated source-specific reading from a data-source-agnostic scheduler responsible for execution
  flow control and compute policy.
- Rate limiting, quota management, downstream protection, and cross-region load balancing belong to the
  transport/scheduler layer in the revised architecture, not to the queue storage primitive.
- Priority queues suit workloads needing arbitrary ordering, leases, per-item access, and granular delayed
  retry. Streams are cheaper when immutable sequential batches suffice.
- Supporting external sources directly avoided copying every workload into FOQS merely to use the Async
  transport layer.

## HELIX Usage

This resource informs pqueue's product boundary and integration guidance. Use it to preserve the PRD's
non-goal for downstream rate enforcement, to keep worker execution policy out of the queue engine, and to
explain when a stream is a better primitive than pqueue.

## Authority Boundary

The post describes Meta's wider Async platform, not a requirement that pqueue build a scheduler, worker
runtime, push transport, or multi-source ingestion framework. It supports a clean extension boundary:
pqueue owns durable ordered work state; callers or adjacent routing components own compute admission and
downstream policy.

## Review Checklist

- [x] Source URL and access date are present
- [x] Summary is concise and source-faithful
- [x] Findings are relevant to pqueue discovery
- [x] HELIX usage is specific
- [x] Boundary prevents over-applying the source
