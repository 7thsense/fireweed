---
ddx:
  id: us-retry-failure-and-retention
  depends_on:
    - feat-retry-failure-and-retention
    - prd
kind: user-story
---

# US: Retry, Failure, and Retention

As a queue caller on the public cell, I can rely on retry, failure, and retention without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

Retry carries retry count, retry metadata, and not-before (FR-36).
Queue policy defines when retryable items become terminal failed (FR-37).
Terminal complete and failed outcomes are durable (FR-38).
Retention of terminal and idempotency records is bounded (FR-39).
A queue may be recurring and an item may be re-armed (FR-49).
Re-arm does not count as retry exhaustion (FR-50).
A recurring item is one logical item per logical key (FR-51).
A recurring item ends only by explicit terminal finalize or purge (FR-52).
Recurring inventory is observable separately from live work (FR-53).
A re-armed item uses the same queue-global progress bound once it is eligible (FR-54).
