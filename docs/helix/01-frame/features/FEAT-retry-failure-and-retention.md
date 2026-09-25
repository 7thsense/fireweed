---
ddx:
  id: feat-retry-failure-and-retention
  depends_on:
    - prd
kind: feature
---

# FEAT: Retry, Failure, and Retention

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

The engine counts attempts. Retry metadata is fields, and a retry delay is a future rank (FR-36).
Retired: queue retry policy (FR-37). A consumer moves an exhausted record to its own failure stage.
Retired: terminal outcome records (FR-38) and retention policy (FR-39). Records live until the consumer deletes them or closes their queue.
A record recurs by being re-ranked when it is settled (FR-49).
Retired: re-arm budget resets (FR-50). `attempts` is never reset.
A recurring record is one record per key (FR-51).
A recurring record ends when it is deleted or moved to a stage nothing claims (FR-52).
Idle and ready records are told apart by stage counts and rank aggregates (FR-53).
A re-ranked record is claimed in rank order like any other (FR-54).

Covers FR-36, FR-37, FR-38, FR-39, and FR-49 through FR-54.
