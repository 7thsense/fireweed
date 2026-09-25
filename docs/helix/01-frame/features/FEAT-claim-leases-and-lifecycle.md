---
ddx:
  id: feat-claim-leases-and-lifecycle
  depends_on:
    - prd
kind: feature
---

# FEAT: Claim Leases and Lifecycle

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

Every record is in exactly one declared stage and is Free or Leased. No stage is terminal (FR-23).
A claim leases its batch under one token; other claims skip those records and lease-free writes to them are rejected (FR-24).
No record has more than one active lease (FR-25).
If the holder neither settles nor extends before expiry, the records become claimable again within Δ (FR-26).
Every acknowledged effect survives restart (FR-27).
Delivery is at-least-once with a single active lease, and `attempts` counts claims (FR-28).

Covers FR-23 through FR-28.
