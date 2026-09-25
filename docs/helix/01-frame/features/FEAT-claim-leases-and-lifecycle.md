---
ddx:
  id: feat-claim-leases-and-lifecycle
  depends_on:
    - prd
kind: feature
---

# FEAT: Claim Leases and Lifecycle

## Behavior

- Every record is in exactly one declared stage and is Free or Leased. Any write may move it to any declared stage, and no stage is terminal (FR-23).
- A claim leases its batch under one token and one expiry; other claims skip those records and lease-free writes to them are rejected. The token is the only authority: a claim has no holder identity (FR-24).
- No record is attached to more than one live claim (FR-25).
- If the holder neither settles nor extends before expiry, the records become claimable again within Δ (FR-26).
- Every acknowledged effect survives restart (FR-27).
- Delivery is at-least-once with a single live claim, and `attempts` counts claims (FR-28).

Covers FR-23 through FR-28.
