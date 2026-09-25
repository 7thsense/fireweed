---
ddx:
  id: feat-priority-and-progress
  depends_on:
    - prd
kind: feature
---

# FEAT: Priority and Progress

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

A claim takes one stage's claimable records in (rank, insertion sequence) order and never visits other stages, leased records, or records ranked beyond the horizon (FR-7).
Retired: bounded-relaxed ordering (FR-8), the per-queue progress bound (FR-9, FR-12), and rank-error bounds (FR-13). Strict rank order and the quiet-queue progress rule replace them.
A record's ready age counts from the later of its rank and the time it last became Free in its stage (FR-10).
Lease expiry frees the claim's records within Δ, and each keeps its rank and its place in claim order (FR-11).

Covers FR-7 through FR-13.
