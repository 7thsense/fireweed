---
ddx:
  id: feat-eligibility-and-metadata
  depends_on:
    - prd
kind: feature
---

# FEAT: Eligibility and Metadata

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Behavior

A claim returns the highest-priority eligible items under the queue ordering (FR-14).
Eligibility follows lifecycle, lease, schedule, and gate state (FR-15).
Items carry opaque payload and metadata (FR-16).
Metadata gates can keep an otherwise high-priority item unclaimed (FR-17).

Covers FR-14 through FR-17.
