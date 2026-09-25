---
ddx:
  id: us-observability-and-operations
  depends_on:
    - feat-observability-and-operations
    - prd
kind: user-story
---

# US: Observability and Operations

As a queue platform engineer, I see each queue's backlog, inspect its records, and repair it without special powers on the data plane.

## Acceptance

- `stats` reports Free and Leased counts for every declared stage, and whether the queue is paused (FR-40).
- `stats.groups` reports, for one stage, up to `limit` groups with a ready record and the oldest ready time (FR-41).
- `aggregate` counts the records of a declared index range by index key, time bucket, value range or lease state (FR-41).
- Throughput and latency for write, claim and lease reclaim are emitted as telemetry outside the contract (FR-42).
- One queue's backlog does not stop another queue from making progress (FR-43).
- `Operator::force_clear` detaches a leased record without its token. It is reachable only from the admin handle, and CI rejects its use in Snorri, the workload and loadgen crates, and the contract suite (FR-58).
- `read` and `scan` observe every acknowledged write, and `scan` pages through a declared index without duplicating or skipping records that are unchanged during paging (FR-60).
