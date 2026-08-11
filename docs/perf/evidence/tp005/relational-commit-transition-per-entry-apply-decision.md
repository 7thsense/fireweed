# Decision record: relational `commit_transition` stays per-entry apply

Lineage: fireweed-38213c74 / fireweed-6e651ac5 / fireweed-ca9c45a0 / fireweed-346a8d9b (closed the
bulk-apply coalescing) → fireweed-6bfe48ca (snorri regression, HOLD, revert c6c411ff) →
fireweed-4ba1dfd7 (this record — pins the invariant and the re-landing precondition).

Governing evidence: `docs/perf/evidence/tp005/commit-amortization-latest.md:102-142`. Restored code:
`crates/fireweed-sqlite/src/relational/backend.rs`, `commit_transition` (the `for entry in entries`
loop).

## 1. What regressed

Snorri's same-day external worker ladder at main `ed311dff` (post bulk-apply-coalescing, pre-revert),
sqlite, 10k members, claim-batch 500, delivery assertions green (this was a pure performance
regression, not a correctness one):

| workers | tps (landing, bulk-apply) | tps (v0.31.2, per-entry) | ratio |
|---:|---:|---:|---:|
| 1 | 988 | 2,069 | 0.48× |
| 4 | 2,247 | 3,395 | 0.66× |
| 8 | 1,540 | 3,692 | 0.42× |

`durable_queue_commit` wall time: **124.2 s** (landing, bulk-apply) vs **37.2 s** (v0.31.2,
per-entry) — a 3.3× inflation.

Reference numbers to beat on re-landing: v0.31.2 baseline **3,692 tps at w=8**, 0.93 ms/entry
in-commit.

## 2. The mechanism

fireweed-6e651ac5 restructured relational `commit_transition` from N bounded, fixed-shape apply
units — one `WriteSideRecords`, one `AdvanceInstanceFence`, one `Push`, one `Finalize` per commit
entry, each a small statement eligible for `prepare_cached` reuse across entries — into O(1) command
groups sized by the **whole commit body**:

- every entry's `side_records` were drained into one shared `Vec` and applied as a single
  `WriteSideRecordsCommand` spanning the entire batch;
- every entry's `instance_fence` was staged into a hand-rolled multi-row upsert built with
  `format!("... VALUES {values} ...")` per 64-row chunk and executed with plain `tx.execute`, **not**
  `prepare_cached` — a remainder-sized chunk (batch size not a multiple of 64) pays a fresh
  parse+plan every commit instead of reusing a cached statement;
- every entry's `lifecycle_items` were merged into one `Push` command spanning the whole batch,
  with typed-index uniqueness bookkeeping (`maintain_typed_indexes_on_insert`) run once over the
  merged set instead of once per small unit;
- `Finalize` was likewise merged into one command for the whole batch.

At small entries/commit or few/no typed indexes (the shapes `fireweed-346a8d9b` measured — see
§3), the fixed per-entry overhead this coalescing removed (repeated `apply_command_sql` call
overhead, repeated small-statement dispatch) outweighs the cost of staging all entries into shared
`Vec`s before flush. At snorri's shape — 19 typed indexes, entity documents (~2.3 KB JSON per
item), ~2.3 KB payload bytes per item, 500 entries/commit — the balance inverts: the amount of data
staged in memory before a single flush point scales with entries-per-commit (not O(1)), typed-index
uniqueness bookkeeping runs over a 500-item merged set instead of being pipelined in small units,
and the non-cached fence-upsert SQL text is rebuilt on every commit. `durable_queue_commit` cost
rose with batch size instead of amortizing — the inverted-batching signature the local synthetic
probe (`sqlite_commit_batch_size_sweep_repro_table`, unasserted at the time) printed but did not
fail on, and that only snorri's real worker ladder caught.

## 3. Why the `fireweed-346a8d9b` lean-index closure evidence did not transfer

`fireweed-346a8d9b`'s charter was to prove commit amortization holds under snorri-like typed-index
counts before closing the coalescing work. Its closing test
(`sqlite_commit_amortizes_with_multi_typed_indexes`,
`crates/fireweed/tests/sqlite_commit_batch_linearity.rs`) measured:

- **8** typed indexes (`N_INDEXES = 8`), not 19;
- lifecycle items built from small string fields only (`f0..f7`, each a short `format!` string) —
  **no** JSON entity-document-sized payload;
- **no** `payload` bytes attached to the pushed/lifecycle items at all (default empty payload).

Snorri's real commit shape combines three axes at once that `346a8d9b` never combined: **19**
typed indexes (not 8), entity documents present (not absent), and ~2.3 KB payload bytes per item
(not empty) — all at the same 500-entries/commit batch size that inflated `durable_queue_commit`.
`346a8d9b`'s ratio-only amortization result (ratio ≤1.05, 64→512 entries/commit) genuinely held at
its own shape; it did not bound the combined cost of the three axes snorri's workload multiplies
together. The closure evidence was real but did not generalize, which is exactly what
`docs/perf/evidence/tp005/commit-amortization-latest.md`'s "Snorri-shaped regression gate"
section (fireweed-d8ceee81) now guards against with an *asserted* (not print-only) snorri-shaped
probe.

## 4. Shape predicate and re-landing precondition

**Shape predicate.** A commit shape is "snorri-shaped" (bulk-apply coalescing MUST NOT be
re-attempted, or must be proven safe against it first) when it combines:

| axis | snorri value | `346a8d9b` lean value |
|---|---:|---:|
| typed index count | 19 (1 unique) | 8 (1 unique) |
| entity documents present | yes | yes (small: 8 short string fields, no size-bearing payload) |
| payload bytes per item | ~2.3 KB | 0 (no payload attached) |
| entries per commit | 500 | 64 / 512 (ratio-only, no 500-entry combined-axis case) |

A future coalescing attempt is safe to consider only after it is measured at **all four axes
simultaneously** at or above these values — index count ≥19, entity documents present, payload
bytes ≥~2.3 KB, entries/commit ≥500 — not at any one axis in isolation.

**Re-landing precondition (two-part, both required):**

1. The snorri-shaped regression gate stays green:
   `sqlite_commit_snorri_shaped_ladder_probe` / `assert_snorri_amortizes`
   (`crates/fireweed/tests/sqlite_commit_batch_linearity.rs`) — asserted ratio ≤1.05 at 500 and 512
   entries/commit vs 64, on `open_sqlite_relational`, at the shape in §4 — AND the per-entry
   behavioral invariant in `crates/fireweed-sqlite/tests/relational_commit.rs`
   (`relational_commit_transition_applies_each_entry_independently`,
   `relational_commit_transition_per_entry_outcomes_are_order_stable`) is updated deliberately to
   describe the new applied-command shape (not silently left passing against stale expectations).
2. **AND** a fresh external snorri worker ladder, run from the candidate revision, measures
   **≥3,692 tps at w=8** (the v0.31.2 per-entry baseline) — a local/synthetic probe alone is not
   sufficient evidence; §1 and §3 are the record of a local-probe-shaped closure that did not
   transfer to the real workload once already.

Until both hold, `commit_transition` on `SqliteRelationalBackend` keeps the per-entry apply loop
restored by c6c411ff: one `WriteSideRecords`, one `AdvanceInstanceFence`, and one `Push` per commit
entry. `Finalize` alone stays coalesced into one envelope per commit body
(`00f3bd8b`, evidenced safe at the snorri shape — see
`docs/perf/evidence/tp005/commit-amortization-latest.md:167-177`); it is a flat item-state update
with no per-entry typed-index or entity-document cost, so it is not part of the mechanism in §2 and
is not gated by this precondition.
