---
ddx:
  id: us-priority-and-progress
  depends_on:
    - feat-priority-and-progress
    - prd
kind: user-story
---

# US: Priority and Progress

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
