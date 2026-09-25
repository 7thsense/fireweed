---
ddx:
  id: us-queue-definition
  depends_on:
    - feat-queue-definition
    - prd
kind: user-story
---

# US: Queue Definition

As a queue platform engineer, I declare a queue's stages, fields and indexes once, and every write and claim on that queue is checked against that definition for the queue's whole life.

## Acceptance

- `define_queue` on an absent queue creates it with its normalized definition and empty metadata at version 0, and reports `created: true` (FR-1, FR-6).
- The same definition again reports `created: false`. A different definition fails with `DefinitionConflict`, carries the stored definition, and changes nothing (FR-6).
- A queue defined with no stages has the stages `[INITIAL, COMPLETED]`, and an insert that names no stage lands in `INITIAL` (FR-6).
- An insert with no rank gets the insertion time in µs, so records claim in FIFO order. A caller-supplied time in µs orders by that time, and a future time is not claimable until the claim horizon reaches it (FR-2, FR-3).
- A priority offset added to the rank moves a record ahead of, or behind, records of the same time (FR-4).
- A metadata op inside a write unit applies atomically with the unit's record writes, and a stale expected version rejects the whole unit (FR-56).
- `read` with `queue: true` returns the definition and the current metadata with its version (FR-56).
- `delete_queue` closes a queue that holds records and a live claim. Afterwards every op except `delete_queue` fails with `QueueClosed`, including after a restart, and a full delete under `ArchiveOnly` fails with `NotPermitted` (FR-57).
