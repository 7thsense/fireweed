---
ddx:
  id: us-queue-definition
  depends_on:
    - feat-queue-definition
    - prd
kind: user-story
---

# US: Queue Definition

As a queue caller on the public cell, I can rely on queue definition without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

A caller creates an isolated queue (FR-1) with a declared priority model (FR-2).
Timestamp ascending is a first-class model (FR-3), and at least one non-timestamp model is supported (FR-4).
The queue declares strict or bounded-relaxed ordering at creation (FR-5).
Priority model and ordering mode do not change after creation (FR-6).
