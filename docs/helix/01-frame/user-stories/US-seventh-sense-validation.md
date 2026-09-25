---
ddx:
  id: us-seventh-sense-validation
  depends_on:
    - feat-seventh-sense-validation
    - prd
kind: user-story
---

# US: Seventh Sense Validation

> **Acceptance superseded in part by API-006** (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`).
> These lines predate API-006. Where one conflicts with the Behavior section of [the parent feature](../features/FEAT-seventh-sense-validation.md),
> or cites a requirement the PRD marks **Retired (API-006)**, the feature and API-006 govern. The public-cell
> sentence is implementation wording (ADR-024).

As a queue caller on the public cell, I can rely on seventh sense validation without choosing another storage cell.

The public cell is one cell: an S3 object log, a Turso projection, and AsyncProjection. Other selectors fail closed before I/O.

## Acceptance

Scheduled delivery uses timestamp-ascending priority without Seventh Sense states in the core lifecycle (FR-44).
Pause, suppression, account, connector, job, and campaign controls are eligibility predicates, not a downstream quota (FR-45).
Work can be ingested quickly and rescheduled later (FR-46).
Claim batches can match downstream account, connector, job, campaign, or external batch keys (FR-47).
Nesting maps to tenant, queue, group key, and metadata (FR-47a).
A claim can return whole groups within a downstream per-call entity limit (FR-47b).
The complete-batch callback requirement is an opt-in cohort claim (FR-47c).
Singleton recurring job and connector rows re-arm until purge, without those concepts in the core model (FR-55).
