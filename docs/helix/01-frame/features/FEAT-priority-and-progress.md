---
ddx:
  id: feat-priority-and-progress
  depends_on:
    - prd
kind: feature
---

# FEAT: Priority and Progress

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Behavior

Strict queues claim by priority key plus the tie-breaker (FR-7).
Bounded-relaxed queues may claim within the declared rank error (FR-8).
Every queue has one queue-global progress bound (FR-9).
Ineligible time does not count toward that bound (FR-10).
Lease expiry restores eligibility without resetting eligible age (FR-11).
Eligible items are claimed before the progress bound expires (FR-12).
Ordering-quality bounds such as maximum rank error may be exposed without replacing the progress bound (FR-13).

Covers FR-7 through FR-13.
