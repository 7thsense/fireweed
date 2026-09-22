---
ddx:
  id: feat-observability-and-operations
  depends_on:
    - prd
kind: feature
---

# FEAT: Observability and Operations

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Behavior

The queue exposes counts by lifecycle state (FR-40).
It exposes oldest eligible age, progress-bound risk, active leases, retry backlog, and terminal failures (FR-41).
It exposes throughput and latency for push, update, claim, finalize, retry, and lease expiry (FR-42).
One queue's backlog cannot stop another queue from making progress inside its limits (FR-43).
Discovery lists queues and group keys with eligible work without leasing or mutating them (FR-48).

Covers FR-40, FR-41, FR-42, FR-43, and FR-48.
