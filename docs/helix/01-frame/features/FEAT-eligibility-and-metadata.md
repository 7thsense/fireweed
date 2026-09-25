---
ddx:
  id: feat-eligibility-and-metadata
  depends_on:
    - prd
kind: feature
---

# FEAT: Eligibility and Metadata

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

A claim returns the first candidates of its stage: Free, ranked at or below the horizon, with no Blocked gate, while the queue is not paused (FR-14).
Claimability depends only on stage, lease state, rank against the horizon, and gates (FR-15).
Records carry declared, typed fields, each value at most 64 KiB, with no payload slot and no side records (FR-16).
Gates keep an otherwise claimable record unclaimed, and the reserved gate `"*"` pauses the queue (FR-17).

Covers FR-14 through FR-17.
