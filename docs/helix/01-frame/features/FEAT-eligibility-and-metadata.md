---
ddx:
  id: feat-eligibility-and-metadata
  depends_on:
    - prd
kind: feature
---

# FEAT: Eligibility and Fields

## Behavior

- A claim returns the first candidates of its stage: Free, ranked at or below the horizon, with no Blocked gate, while the queue is not paused (FR-14).
- Claimability depends only on stage, lease state, rank against the horizon, and gates (FR-15).
- Records carry declared, typed fields, each `Str` or `Bytes` value at most 64 KiB, with no payload slot and no side records. Declared indexes may key on fields of any type (FR-16).
- `claim` and `read` return only the fields they name, and never read the stored values of fields they do not return (FR-59).
- Gates keep an otherwise claimable record unclaimed, and the reserved gate `"*"` pauses the queue (FR-17).

Covers FR-14 through FR-17, and FR-59.
