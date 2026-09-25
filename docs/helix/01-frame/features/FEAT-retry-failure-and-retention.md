---
ddx:
  id: feat-retry-failure-and-retention
  depends_on:
    - prd
kind: feature
---

# FEAT: Retry, Failure, and Retention

## Behavior

- The engine counts attempts. A retry is a settling write with a future rank and its retry metadata in fields, and a consumer moves an exhausted record to its own failure stage (FR-36).
- A record recurs by being re-ranked when it is settled (FR-49).
- A recurring record is one record per key (FR-51).
- A recurring record ends when it is deleted or moved to a stage nothing claims (FR-52).
- Idle and ready records are told apart by stage counts and rank aggregates (FR-53).
- A re-ranked record is claimed in rank order like any other (FR-54).
- Records never expire. Retention is the consumer's: it deletes records or closes their queue (FR-23, FR-57).

Covers FR-36, FR-49, and FR-51 through FR-54.
