---
ddx:
  id: feat-batch-and-group-operations
  depends_on:
    - prd
kind: feature
---

# FEAT: Batch and Group Operations

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

Workers claim up to 1000 records in one claim (FR-29).
A claim returns records in (rank, insertion sequence) order, or in request order for a claim by key (FR-30).
A claim narrows its results by stage or by whole groups; records found through a declared index are claimed by key. Single-group and index-ordered claims are removed (FR-31).
A whole-group claim bounds the number of distinct groups and returns each group whole (FR-31a).
It takes only groups with no leased member in any stage, ordered by each group's first candidate (FR-32).
Retired: cohorts (FR-32a through FR-32c). An atomic group insert, a whole-group claim, and a caller count check serve the use case.
Workers settle claimed records by writing them with the claim token: a release with a stage move or a new rank, or a delete (FR-33).
A rejected write reports why, including a stale lease and a record leased by another holder (FR-34).
Batch sizes and claim limits are contract constants (FR-35).
A second claim with the same request id returns the same records and token while the claim is live and every record is still attached.

Covers FR-29 through FR-35, including FR-31a and FR-32a through FR-32c.
