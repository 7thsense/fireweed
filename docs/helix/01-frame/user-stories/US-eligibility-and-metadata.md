---
ddx:
  id: us-eligibility-and-metadata
  depends_on:
    - feat-eligibility-and-metadata
    - prd
kind: user-story
---

# US: Eligibility and Fields

As a worker engineer, I store typed data on each record and control when records become claimable, so a claim returns exactly the work that is ready and only the data the worker needs.

## Acceptance

- A record is a candidate only when it is in the claim's stage, Free, ranked at or below the horizon, and carries no Blocked gate, while `"*"` is Open (FR-14, FR-15).
- Blocking a gate makes every record that carries it unclaimable at once, without changing any record; opening it makes them claimable again. Blocking `"*"` pauses the queue (FR-17).
- A write that names an undeclared field, or gives a value of the wrong type, is `Invalid` and records nothing (FR-16).
- A 64 KiB `Str` or `Bytes` value round-trips through `write`, `claim` and `read`, and after a restart (FR-16).
- An index may be keyed on a `Str` or `Bytes` field, and a write whose encoded index key exceeds 1024 B is `Invalid` (FR-16).
- `claim` and `read` with a field list return only the named fields. With an empty list they return no fields, while the claim leases the same records and increments `attempts` as it would without the list. An undeclared or repeated name is `Invalid` (FR-59).
