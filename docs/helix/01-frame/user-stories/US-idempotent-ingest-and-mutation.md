---
ddx:
  id: us-idempotent-ingest-and-mutation
  depends_on:
    - feat-idempotent-ingest-and-mutation
    - prd
kind: user-story
---

# US: Idempotent Ingest and Mutation

As a queue caller on the public cell, I can rely on idempotent ingest and mutation without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

Clients push one or more items idempotently with a request id (FR-18).
Duplicate pushes for the same logical item key converge on one item (FR-19).
Clients can batch-update priority, not-before, payload, and metadata (FR-20).
Batch push and update return per-item results (FR-21).
The queue defines an idempotency retention window (FR-22).
