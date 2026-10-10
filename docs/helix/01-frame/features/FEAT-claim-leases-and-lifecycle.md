---
ddx:
  id: feat-claim-leases-and-lifecycle
  depends_on:
    - prd
kind: feature
---

# FEAT: Claim Leases and Lifecycle

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Behavior

Every accepted item is in exactly one lifecycle state (FR-23).
A claim creates a lease that hides the item from other workers (FR-24).
No item has more than one active lease (FR-25).
If the worker does not finalize before expiry, the item becomes eligible again (FR-26).
Accepted items, priority, metadata, lease state, and lifecycle survive restart from the object log (FR-27).
Delivery is at-least-once with a single active lease (FR-28).

Covers FR-23 through FR-28.

## Formal analysis obligations

[CONTRACT-006](../../02-design/contracts/CONTRACT-006-formal-queue-semantics.md) defines
formal safety and progress vocabulary; [TP-006](../../03-test/TP-006-formal-analysis.md)
defines bounded model and independent implementation evidence requirements.
The current public-cell and PRD requirements retain authority. Conditional
eventual progress does not discharge the numeric queue-global progress bound.
