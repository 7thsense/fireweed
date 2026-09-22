---
ddx:
  id: feat-seventh-sense-validation
  depends_on:
    - prd
kind: feature
---

# FEAT: Seventh Sense Validation

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Behavior

Scheduled delivery uses timestamp-ascending priority without Seventh Sense states in the core lifecycle (FR-44).
Pause, suppression, account, connector, job, and campaign controls are eligibility predicates, not a downstream quota (FR-45).
Work can be ingested quickly and rescheduled later (FR-46).
Claim batches can match downstream account, connector, job, campaign, or external batch keys (FR-47).
Nesting maps to tenant, queue, group key, and metadata (FR-47a).
A claim can return whole groups within a downstream per-call entity limit (FR-47b).
The complete-batch callback requirement is an opt-in cohort claim (FR-47c).
Singleton recurring job and connector rows re-arm until purge, without those concepts in the core model (FR-55).

Covers FR-44 through FR-47, FR-47a, FR-47b, FR-47c, and FR-55.
