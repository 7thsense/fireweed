---
ddx:
  id: us-retry-failure-and-retention
  depends_on:
    - feat-retry-failure-and-retention
    - prd
kind: user-story
---

# US: Retry, Failure, and Retention

As a worker engineer, I retry, fail and repeat work with ordinary writes, and I remove records when I no longer need them.

## Acceptance

- A retry is `{Release(t), Put{rank}}` with a future rank. The record is claimable again once the horizon reaches that rank, with `attempts` kept (FR-36).
- A worker that sees `attempts` above its own limit moves the record to a failure stage the queue declares (FR-36).
- A recurring record settled with `{Release(t), Put{rank: next run}}` stays one record per key across any number of cycles, and its version increases every cycle (FR-49, FR-51).
- A recurring record ends when it is deleted or moved to a stage nothing claims (FR-52).
- Idle (future-ranked) and ready records are counted apart with `stats` and an `aggregate` over rank (FR-53).
- A re-ranked record is claimed in rank order like any other record once its rank is at or below the horizon (FR-54).
- A record stays until the consumer deletes it or closes its queue; nothing expires it (FR-23, FR-57).
