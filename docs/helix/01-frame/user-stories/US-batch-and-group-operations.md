---
ddx:
  id: us-batch-and-group-operations
  depends_on:
    - feat-batch-and-group-operations
    - prd
kind: user-story
---

# US: Batch and Group Operations

As a worker engineer, I claim and settle batches that fit a downstream API's batch constraints.

## Acceptance

- A claim returns up to `max_records` records (at most 1000) in a response of at most 16 MiB, and at least the first candidate when one exists (FR-29).
- A claim resent with its `request_id` returns the same records and token while the claim is live and all its records are still attached (FR-29).
- `Next`, and each group of `WholeGroups`, return records in `(rank, insertion sequence)` order. `Keys` returns them in request order and reports every key it did not claim with a reason (FR-30, FR-31).
- `WholeGroups { max_groups }` returns up to `max_groups` groups, each with all its candidates; an oversized first group comes back as a prefix flagged `partial_group` (FR-31a).
- `WholeGroups` never returns a group with a leased record in any stage (FR-32).
- A settling write with `Release(t)` moves, re-ranks or deletes a claimed record, and `Held(t)` patches it and keeps it leased (FR-33).
- A unit with a stale or wrong token is rejected with `StaleLease`, and a lease-free write to a leased record with `Leased` (FR-34).
- Batch sizes and claim limits are contract constants (FR-35).
