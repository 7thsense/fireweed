---
ddx:
  id: feat-batch-and-group-operations
  depends_on:
    - prd
kind: feature
---

# FEAT: Batch and Group Operations

## Behavior

- Workers claim up to 1000 records in one claim, and a claim resent with the same request id returns the same records and token while the claim is live (FR-29).
- A claim returns records in (rank, insertion sequence) order, or in request order for a claim by key (FR-30).
- A claim takes the next records of one stage, whole groups, or named keys. Records found through a declared index are claimed by key (FR-31).
- A whole-group claim bounds the number of distinct groups and returns each group whole (FR-31a).
- It takes only groups with no leased member in any stage, ordered by each group's first candidate (FR-32).
- Workers settle claimed records by writing them with the claim token: a release with a stage move or a new rank, or a delete (FR-33).
- A rejected write reports why, including a stale lease and a record leased by another holder (FR-34).
- Batch sizes and claim limits are contract constants (FR-35).

Covers FR-29 through FR-35, including FR-31a.
