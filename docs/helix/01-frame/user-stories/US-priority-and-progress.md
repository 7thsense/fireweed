---
ddx:
  id: us-priority-and-progress
  depends_on:
    - feat-priority-and-progress
    - prd
kind: user-story
---

# US: Priority and Progress

> **Acceptance superseded in part by API-006** (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`).
> These lines predate API-006. Where one conflicts with the Behavior section of [the parent feature](../features/FEAT-priority-and-progress.md),
> or cites a requirement the PRD marks **Retired (API-006)**, the feature and API-006 govern. The public-cell
> sentence is implementation wording (ADR-024).

As a queue caller on the public cell, I can rely on priority and progress without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

Strict queues claim by priority key plus the tie-breaker (FR-7).
Bounded-relaxed queues may claim within the declared rank error (FR-8).
Every queue has one queue-global progress bound (FR-9).
Ineligible time does not count toward that bound (FR-10).
Lease expiry restores eligibility without resetting eligible age (FR-11).
Eligible items are claimed before the progress bound expires (FR-12).
Ordering-quality bounds such as maximum rank error may be exposed without replacing the progress bound (FR-13).
