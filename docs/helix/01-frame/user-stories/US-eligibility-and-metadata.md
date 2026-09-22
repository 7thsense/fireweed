---
ddx:
  id: us-eligibility-and-metadata
  depends_on:
    - feat-eligibility-and-metadata
    - prd
kind: user-story
---

# US: Eligibility and Metadata

As a queue caller on the public cell, I can rely on eligibility and metadata without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

A claim returns the highest-priority eligible items under the queue ordering (FR-14).
Eligibility follows lifecycle, lease, schedule, and gate state (FR-15).
Items carry opaque payload and metadata (FR-16).
Metadata gates can keep an otherwise high-priority item unclaimed (FR-17).
