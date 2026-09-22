---
ddx:
  id: us-batch-and-group-operations
  depends_on:
    - feat-batch-and-group-operations
    - prd
kind: user-story
---

# US: Batch and Group Operations

As a queue caller on the public cell, I can rely on batch and group operations without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

Workers claim up to a bounded number of eligible items in one call (FR-29).
Batch claim follows the queue ordering mode (FR-30).
Group-aware claim can restrict results to one shared group key (FR-31).
That claim can bound the number of distinct groups (FR-31a).
Group-aware batch claim is atomic for the returned set (FR-32).
A queue may enable cohort claims (FR-32a).
Cohort members are not claimed by a non-cohort claim (FR-32b).
A cohort that misses its completion bound expires instead of blocking the progress bound (FR-32c).
Workers batch-finalize leased items as complete, failed, retry, or release (FR-33).
Batch finalize returns per-item results, including stale lease (FR-34).
Configuration exposes maximum batch sizes (FR-35).
A second claim with the same request id returns the same leased set while those leases are active.
