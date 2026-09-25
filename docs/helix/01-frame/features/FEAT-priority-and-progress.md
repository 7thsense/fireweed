---
ddx:
  id: feat-priority-and-progress
  depends_on:
    - prd
kind: feature
---

# FEAT: Priority and Progress

## Behavior

- A claim takes one stage's claimable records in (rank, insertion sequence) order and never visits other stages, leased records, or records ranked beyond the horizon. On a quiet queue it returns the first candidates in that order (FR-7).
- A ready record has waited since it last became Free in its stage. The oldest ready time is read from that moment, never from the rank (FR-10).
- Lease expiry frees the claim's records within Δ, and each keeps its rank and its place in claim order (FR-11).

Covers FR-7, FR-10 and FR-11.
