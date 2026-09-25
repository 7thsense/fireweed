---
ddx:
  id: us-claim-leases-and-lifecycle
  depends_on:
    - feat-claim-leases-and-lifecycle
    - prd
kind: user-story
---

# US: Claim Leases and Lifecycle

As a worker engineer, I hold claimed records exclusively until I settle them, and a crashed worker's records come back.

## Acceptance

- Every record is in exactly one declared stage, and any write may move it to any declared stage; no stage is terminal (FR-23).
- A claim returns one token and one expiry for its whole batch. Until the holder releases or deletes the records, or the claim expires, no other claim returns them and lease-free writes to them are rejected with `Leased` (FR-24).
- `Extend` renews every record still attached to the claim, and `Release` detaches one record while the others stay leased (FR-24).
- No record is ever attached to two live claims (FR-25).
- If the holder neither settles nor extends before expiry, every record still attached becomes claimable within Δ, and the token then gets `StaleLease` (FR-26).
- Records, claims, gates, metadata and queue closure survive a restart (FR-27).
- `attempts` rises by one per claim and is never reset (FR-28).
