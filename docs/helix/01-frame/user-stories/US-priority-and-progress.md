---
ddx:
  id: us-priority-and-progress
  depends_on:
    - feat-priority-and-progress
    - prd
kind: user-story
---

# US: Priority and Progress

As a worker engineer, I claim a stage's work strictly in rank order, and no ready record waits forever.

## Acceptance

- A claim of a stage returns candidates in `(rank, insertion sequence)` order; equal ranks come in insertion order (FR-7).
- A claim never returns records of another stage, leased records, or records ranked beyond the horizon (FR-7).
- On a quiet queue, a claim returns the first `min(max_records, candidates)` candidates in claim order (FR-7).
- `stats.groups.oldest_ready_since` reports when the longest-waiting ready record last became Free. A record released at time t reports t, whatever its rank (FR-10).
- When a claim expires, within Δ its records are claimable again with their stage, rank, insertion sequence and `attempts` unchanged (FR-11).
