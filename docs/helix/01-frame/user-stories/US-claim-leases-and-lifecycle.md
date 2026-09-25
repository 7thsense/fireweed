---
ddx:
  id: us-claim-leases-and-lifecycle
  depends_on:
    - feat-claim-leases-and-lifecycle
    - prd
kind: user-story
---

# US: Claim Leases and Lifecycle

> **Acceptance superseded in part by API-006** (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`).
> These lines predate API-006. Where one conflicts with the Behavior section of [the parent feature](../features/FEAT-claim-leases-and-lifecycle.md),
> or cites a requirement the PRD marks **Retired (API-006)**, the feature and API-006 govern. The public-cell
> sentence is implementation wording (ADR-024).

As a queue caller on the public cell, I can rely on claim leases and lifecycle without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

Every accepted item is in exactly one lifecycle state (FR-23).
A claim creates a lease that hides the item from other workers (FR-24).
No item has more than one active lease (FR-25).
If the worker does not finalize before expiry, the item becomes eligible again (FR-26).
Accepted items, priority, metadata, lease state, and lifecycle survive restart from the object log (FR-27).
Delivery is at-least-once with a single active lease (FR-28).
