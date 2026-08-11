# Decision: commit batch coalescing shape predicate

**Status:** accepted (2026-08-11)  
**Related:** fireweed-6bfe48ca, fireweed-4ba1dfd7, epic fireweed-110c25bc / fireweed-51cc51ae  
**Product model:** ADR-012 log × projection — product bars are **not** on unified `open_sqlite_relational`.

## Incident

A bulk-apply coalescing change on **unified** `SqliteRelationalBackend::commit_transition`
landed on main near `ed311dff`. Snorri same-day ladder (sqlite composition, 10k members,
claim-batch 500):

| workers | tps (landing) | tps (v0.31.2) |
|--------:|--------------:|--------------:|
| 1 | 988 | 2,069 |
| 4 | 2,247 | 3,395 |
| 8 | 1,540 | 3,692 |

`durable_queue_commit` wall: **124.2 s** vs **37.2 s** at v0.31.2.

## Mechanism

Coalescing **all** lifecycle pushes / finalizes / sides across N entries into O(1) apply
units inverted under snorri's **shape** (high typed-index count, entity documents, ~2.3 KB
payloads, 500-entry batches). Lean multi-index gates (≤8 indexes, tiny payloads) did not
predict this (fireweed-346a8d9b closed on lean evidence).

**Product path** (log-replay): single-hop projection validate + coalesced **envelopes** on
`AsyncLogReplayBackend` is a different optimization surface (fewer async hops / log records)
and is gated on **memory×memory** and **sqlite×memory** product cells.

## Shape predicate (any future coalescing on apply path)

Must measure / gate against **all** of:

| Dimension | Snorri-critical value |
|-----------|----------------------|
| Typed indexes | ≥ 19 (including unique) |
| Lifecycle entity documents | present |
| Payload bytes | ~2300+ |
| Entries / commit | 500–512 |

## Re-landing precondition (two-part)

1. Product-cell snorri-shaped gate green: `sqlite_commit_batch_linearity` including
   memory×memory and sqlite×memory absolute floors.
2. External snorri worker ladder **w=8 ≥ 3,692 tps** on the candidate revision, recorded
   in the handoff RESULT (not fabricated locally).

## Unified path

`open_sqlite_relational` remains for sole-owner discovery convenience. It is **non-orthogonal**
and **non-product** for durability and release absolute floors.
