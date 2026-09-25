---
ddx:
  id: feat-idempotent-ingest-and-mutation
  depends_on:
    - prd
kind: feature
---

# FEAT: Idempotent Ingest and Mutation

Implementation note (ADR-024): the public cell is one cell, an S3 object log and a Turso projection with `AsyncProjection`, and other selectors fail closed before I/O. The behavior below is API-006's and does not depend on the cell.

## Behavior

Records are addressed by an upstream-owned opaque key, unique within the queue (FR-18).
Writes to one key converge on one record, and a resend with the same request id replays the recorded outcome (FR-19).
A write patches any record by key (stage, rank, gates, and fields) with an optional version check; only the claim holder writes a leased record (FR-20).
A write returns one outcome per atomic unit (FR-21).
Request ids are kept for at least 24 h, a contract bound rather than a queue setting (FR-22).

Covers FR-18 through FR-22.
