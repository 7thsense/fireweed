---
ddx:
  id: us-eligibility-and-metadata
  depends_on:
    - feat-eligibility-and-metadata
    - prd
kind: user-story
---

# US: Eligibility and Metadata

> **Acceptance superseded in part by API-006** (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`).
> These lines predate API-006. Where one conflicts with the Behavior section of [the parent feature](../features/FEAT-eligibility-and-metadata.md),
> or cites a requirement the PRD marks **Retired (API-006)**, the feature and API-006 govern. The public-cell
> sentence is implementation wording (ADR-024).

As a queue caller on the public cell, I can rely on eligibility and metadata without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

A claim returns the highest-priority eligible items under the queue ordering (FR-14).
Eligibility follows lifecycle, lease, schedule, and gate state (FR-15).
Items carry opaque payload and metadata (FR-16).
Metadata gates can keep an otherwise high-priority item unclaimed (FR-17).
