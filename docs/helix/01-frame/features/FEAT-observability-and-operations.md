---
ddx:
  id: feat-observability-and-operations
  depends_on:
    - prd
kind: feature
---

# FEAT: Observability and Operations

## Behavior

- The queue exposes Free and Leased counts per stage and its pause state (FR-40).
- It exposes the ready groups and oldest ready time of a stage, and counts over declared indexes (FR-41).
- Implementations emit throughput and latency telemetry outside the store contract (FR-42).
- One queue's backlog cannot stop another queue from making progress inside its limits (FR-43).
- Ending a lease without its token is an operator action, reached only from the admin handle and banned in consumer crates (FR-58).
- Records are inspected without claiming them: by key with `read`, and through declared indexes with `scan`, both fresh (FR-60).

Covers FR-40 through FR-43, FR-58 and FR-60.
