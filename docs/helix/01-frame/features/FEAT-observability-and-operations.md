---
ddx:
  id: feat-observability-and-operations
  depends_on:
    - prd
kind: feature
---

# FEAT: Observability and Operations

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

The queue exposes free and leased counts per stage and its pause state (FR-40).
It exposes the ready groups and oldest ready time of a stage, and counts over declared indexes (FR-41).
Implementations emit throughput and latency telemetry outside the store contract (FR-42).
One queue's backlog cannot stop another queue from making progress inside its limits (FR-43).
Retired: discovery (FR-48). The bounded ready-groups read serves its one use.
Ending a lease without its token is an operator action, reached only from the admin handle and banned in consumer crates (FR-58).

Covers FR-40, FR-41, FR-42, FR-43, FR-48, and FR-58.
