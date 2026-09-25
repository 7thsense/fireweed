---
ddx:
  id: feat-queue-definition
  depends_on:
    - prd
kind: feature
---

# FEAT: Queue Definition

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

A caller creates an isolated queue (FR-1). Queues are cheap: one queue per job type.
Every record has one caller-controlled rank, and a claim takes a stage's records in (rank, insertion sequence) order (FR-2).
The default rank is the insertion time in µs, and a future rank schedules the record (FR-3). A non-timestamp priority is a rank or a rank offset (FR-4).
Retired: the queue ordering mode (FR-5). Every claim is in strict rank order.
The definition (stages, fields, and indexes) is declared at creation and is immutable (FR-6).
A queue carries consumer-owned, versioned metadata, changed only inside a write unit and never interpreted (FR-56).
A queue can be closed by archiving it, or by fully deleting it where policy allows, and closed is final (FR-57).

Covers FR-1 through FR-6, FR-56, and FR-57.
