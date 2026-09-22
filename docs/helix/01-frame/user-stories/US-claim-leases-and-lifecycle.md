---
ddx:
  id: us-claim-leases-and-lifecycle
  depends_on:
    - feat-claim-leases-and-lifecycle
    - prd
kind: user-story
---

# US: Claim Leases and Lifecycle

As a queue caller on the public cell, I can rely on claim leases and lifecycle without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

Every accepted item is in exactly one lifecycle state (FR-23).
A claim creates a lease that hides the item from other workers (FR-24).
No item has more than one active lease (FR-25).
If the worker does not finalize before expiry, the item becomes eligible again (FR-26).
Accepted items, priority, metadata, lease state, and lifecycle survive restart from the object log (FR-27).
Delivery is at-least-once with a single active lease (FR-28).
