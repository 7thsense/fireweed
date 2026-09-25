---
ddx:
  id: us-observability-and-operations
  depends_on:
    - feat-observability-and-operations
    - prd
kind: user-story
---

# US: Observability and Operations

> **Acceptance superseded in part by API-006** (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`).
> These lines predate API-006. Where one conflicts with the Behavior section of [the parent feature](../features/FEAT-observability-and-operations.md),
> or cites a requirement the PRD marks **Retired (API-006)**, the feature and API-006 govern. The public-cell
> sentence is implementation wording (ADR-024).

As a queue caller on the public cell, I can rely on observability and operations without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

The queue exposes counts by lifecycle state (FR-40).
It exposes oldest eligible age, progress-bound risk, active leases, retry backlog, and terminal failures (FR-41).
It exposes throughput and latency for push, update, claim, finalize, retry, and lease expiry (FR-42).
One queue's backlog cannot stop another queue from making progress inside its limits (FR-43).
Discovery lists queues and group keys with eligible work without leasing or mutating them (FR-48).
