---
ddx:
  id: feat-batch-and-group-operations
  depends_on:
    - prd
kind: feature
---

# FEAT: Batch and Group Operations

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Behavior

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

Covers FR-29 through FR-35, including FR-31a and FR-32a through FR-32c.
