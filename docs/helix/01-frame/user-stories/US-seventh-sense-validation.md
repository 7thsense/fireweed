---
ddx:
  id: us-seventh-sense-validation
  depends_on:
    - feat-seventh-sense-validation
    - prd
kind: user-story
---

# US: Seventh Sense Validation

As a Seventh Sense delivery engineer, I move scheduled delivery, action, job and connector work onto fireweed without Seventh Sense states in the engine.

## Acceptance

- Scheduled work uses its scheduled time in µs as its rank, one queue per job type, with consumer-named stages (FR-44).
- An account, connector, job or campaign status change is one gate op that blocks or reopens every record carrying the gate. There is no downstream rate admission (FR-45).
- Work is inserted before its send time is known, then re-ranked or moved later by key (FR-46).
- Batches match downstream constraints through per-type queues, stages, whole groups, and a `scan` followed by a `Keys` claim for records found by a declared field (FR-47).
- Seventh Sense's nesting maps to tenant, queue, stage, group, declared fields and gates (FR-47a).
- `WholeGroups { max_groups: 300 }` returns every claimable task of up to 300 leads in one claim (FR-47b).
- A callback batch inserted as one group in one unit comes back whole in one `WholeGroups` claim, and the caller checks its count (FR-47c).
- A job or connector row is one record per key, re-ranked with its next processing time on settle and deleted with its entity (FR-55).
