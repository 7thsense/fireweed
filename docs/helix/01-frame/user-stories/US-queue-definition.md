---
ddx:
  id: us-queue-definition
  depends_on:
    - feat-queue-definition
    - prd
kind: user-story
---

# US: Queue Definition

> **Acceptance superseded in part by API-006** (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`).
> These lines predate API-006. Where one conflicts with the Behavior section of [the parent feature](../features/FEAT-queue-definition.md),
> or cites a requirement the PRD marks **Retired (API-006)**, the feature and API-006 govern. The public-cell
> sentence is implementation wording (ADR-024).

As a queue caller on the public cell, I can rely on queue definition without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

A caller creates an isolated queue (FR-1) with a declared priority model (FR-2).
Timestamp ascending is a first-class model (FR-3), and at least one non-timestamp model is supported (FR-4).
The queue declares strict or bounded-relaxed ordering at creation (FR-5).
Priority model and ordering mode do not change after creation (FR-6).
