---
ddx:
  id: feat-queue-definition
  depends_on:
    - prd
kind: feature
---

# FEAT: Queue Definition

## Behavior

- A caller creates an isolated queue, addressed by tenant and queue name. Queues are cheap: one queue per job type (FR-1).
- Every record has one caller-controlled rank, and a claim takes a stage's records in (rank, insertion sequence) order (FR-2).
- The default rank is the insertion time in µs, and a future rank schedules the record (FR-3). A non-timestamp priority is a rank or a rank offset (FR-4).
- The definition (stages, fields and indexes) is declared at creation and is immutable. A different definition for the same queue is a conflict (FR-6).
- A queue carries consumer-owned, versioned metadata, changed only inside a write unit and never interpreted (FR-56).
- A queue is closed by archiving it, or by fully deleting it where policy allows, even while it holds records and live claims. Closed is final (FR-57).

Covers FR-1 through FR-4, FR-6, FR-56 and FR-57.
