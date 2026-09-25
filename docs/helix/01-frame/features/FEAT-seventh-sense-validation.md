---
ddx:
  id: feat-seventh-sense-validation
  depends_on:
    - prd
kind: feature
---

# FEAT: Seventh Sense Validation

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

Scheduled work uses its scheduled time as its rank, one queue per job type, and consumer-named stages, with no Seventh Sense states in the engine (FR-44).
Pause, suppression, account, connector, job, and campaign controls are gates, not a downstream quota (FR-45).
Work can be ingested quickly and re-ranked or moved later (FR-46).
Claim batches match downstream constraints through per-type queues, stages, whole groups, and claims by key of records found through a declared field (FR-47).
Nesting maps to tenant, queue, stage, group, declared fields, and gates (FR-47a).
A claim can return whole groups within a downstream per-call entity limit (FR-47b).
A complete callback batch executes together: an atomic group insert, a whole-group claim, and a caller count check, with no cohort in the engine (FR-47c).
Singleton recurring job and connector rows are re-ranked on settle and deleted with their entity (FR-55).

Covers FR-44 through FR-47, FR-47a, FR-47b, FR-47c, and FR-55.
