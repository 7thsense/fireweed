---
ddx:
  id: feat-queue-definition
  depends_on:
    - prd
kind: feature
---

# FEAT: Queue Definition

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Behavior

A caller creates an isolated queue (FR-1) with a declared priority model (FR-2).
Timestamp ascending is a first-class model (FR-3), and at least one non-timestamp model is supported (FR-4).
The queue declares strict or bounded-relaxed ordering at creation (FR-5).
Priority model and ordering mode do not change after creation (FR-6).

Covers FR-1 through FR-6.
