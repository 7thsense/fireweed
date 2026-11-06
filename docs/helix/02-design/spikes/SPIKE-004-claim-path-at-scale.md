---
ddx:
  id: spike-claim-path-at-scale
  status: draft
---

# Technical Spike: Claim-Path Lease Representation at Scale

**Spike ID**: SPIKE-004 | **Lead**: DDx queue steward | **Time Budget**: bounded runs | **Status**: Draft

## Objective

**Technical question**: the owner's round-4 questions on the claim path —

> 4. I have NFC what this means but we need to stay fast with 10m or more
> records. Why not create a "claims" table with any metadata (such as the
> timeout) and then a indexed, nullable "claim-id" field on the queue
> records?

Under the kernel contract's new record model (draft §1, §7.2), `stage` is a
first-class record attribute with **no terminal flag on the row**: a
finished record stays an ordinary row in its terminal stage (e.g.
`COMPLETED`), remains in the stage-prefixed claim-order index, and is
"always Free" (draft §1 line 74). Today's lease representation is lease
columns on the item row (`lease_token_hash`, `lease_expires_at`,
`crates/fireweed-relational/src/schema.rs:37-38`) plus a separate
`fireweed_lease_bearers` table for bearer tokens (`schema.rs:318-326`).
Claim order is `(rank asc, insertion sequence asc)` within a stage (draft
§2.3 C4), where `rank` defaults to 0 and the caller may set it to
epoch-µs for timestamp priority.

Goals:
- build a realistic `items` table at 10M+ rows, most of it resting in a
  terminal stage (`COMPLETED`) per the new no-terminal-flag model, and
  measure the claim path's cost under two lease representations;
- compare **L1** (today's lease columns, ported to the stage-prefixed claim
  index the contract requires) against **L2** (the owner's proposal: a
  separate `claims` table plus a nullable, indexed `claim_id` on the item
  row);
- measure ingest, claim, finish, lease-extend and expiry-sweep cost, DB and
  index size, and the cost of a claim query that forgets to bind `stage`;
- recommend a lease representation and state what keeps the claim path
  O(log n) at 10M+ records.

Out of scope (tracked elsewhere, not measured here): record-key physical
layout (SPIKE-002), `fields`/index storage width (SPIKE-003); whether
`rank` should default to `now` instead of `0` (owner's round-4 question 3,
a semantics decision, not a storage-layout question); RESP/streams
protocol scope (owner's round-4 question 9).

## Scope (setup)

**Hardware** — VERIFIED (measured on this host):
- `nproc`: 32
- `free -g`: 94 GiB total, ~87 GiB available, 24 GiB swap (3 GiB in use)
- `uname -r`: `6.6.87.2-microsoft-standard-WSL2` — WSL2 VM, not bare-metal.
- Load average at the start of the run: `12.60 12.16 13.28` (1-slot heavier
  loaded than idle: shared, concurrently-loaded machine — see Limitations).
- root filesystem: `ext4` on `/dev/sdd`, 4.0 TB, ~3.5 TB free.

**Software** — VERIFIED:
- Vendored Turso `0.7.2` at `vendor/turso`/`vendor/turso_core`, the exact
  pinned version `crates/fireweed-turso/src/local.rs:54`
  (`TURSO_SUPPORTED_VERSION`) requires.
- Standalone Cargo project (own empty `[workspace]` table, not a workspace
  member) at `spikes/claim-scale/`, with path dependencies mirroring
  `crates/fireweed-turso/Cargo.toml:26,28` exactly (same shape as
  `spikes/key-storage/Cargo.toml`): `turso` and `turso_core` pinned to
  `=0.7.2`, `default-features = false`, `turso_core` with
  `features = ["fs"]`. Built `--release` with `CARGO_TARGET_DIR` pointed at
  the shared spike-target scratch directory named in the task.
- Rust: `cargo 1.97.1`, `rustc 1.97.1`.
- Turso 0.7.2 has no `dbstat` virtual table (re-verified on this checkout:
  `grep -rli dbstat vendor/turso_core/ vendor/turso/` finds only a comment
  referencing upstream SQLite's `dbstat.c`, no registered vtab — the same
  finding SPIKE-002 made). Per-index cost is measured by file-size delta
  against a matched baseline schema that omits the claim-order index.

**Connection configuration** — VERIFIED, copied verbatim from
`spikes/key-storage/src/main.rs` `configure_connection`, itself mirroring
`crates/fireweed-turso/src/local.rs:3407-3444` under the profile production
actually opens (`TursoConfig::local(path).with_log_backed_projection()`,
`crates/fireweed/src/turso_compose.rs:136`): `page_size=4096`,
`journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-131072` (128 MiB),
`temp_store=MEMORY`, `wal_autocheckpoint=0` (no auto-checkpoint under load,
log-backed writers recover from the object-log after a crash), `busy_timeout=5s`.

**Representative row** — the task's explicit shape, not the full 27-column
`fireweed_items` hot-row set (that was SPIKE-002's concern): `queue_no
INTEGER`, `seq INTEGER` (insertion order / PK component), `key BLOB` (16
bytes, upstream-id-shaped) under the unique index `(queue_no, key)`,
`stage INTEGER`, `rank INTEGER` (monotonic per row, not all-equal, so the
claim-order B-tree is exercised realistically), `fields TEXT` (~300 bytes,
a single JSON string field), `version INTEGER`. See `spikes/claim-scale/src/main.rs`.

## Method

**Two lease representations**, both carrying the contract's stage-prefixed
claim-order index `(queue_no, stage, rank, seq)` (draft §7.2's
`fireweed_items_pending_eligible_order_idx`, simplified to this spike's
leaner schema):

| | L1 "lease columns" (today's shape, ported) | L2 "claims table" (owner's proposal) |
|---|---|---|
| Item row | `lease_token_hash BLOB`, `lease_expires_at INTEGER` | `claim_id INTEGER` (nullable) |
| Claim-order index | `(queue_no, stage, rank, seq) WHERE lease_expires_at IS NULL` | `(queue_no, stage, rank, seq) WHERE claim_id IS NULL` |
| Expiry-sweep index | `(queue_no, lease_expires_at) WHERE lease_expires_at IS NOT NULL` | `claims(expires_at)` |
| Claim metadata | none (token hash inline) | `claims(claim_id PK, token_hash, holder, expires_at, record_count)` |
| Finish/extend-by-claim | addressed by row identity (already known from the claim) | `items(claim_id) WHERE claim_id IS NOT NULL` for the sweep's bulk clear |

Full DDL: `spikes/claim-scale/src/main.rs` `schema_ddl`.

**Population**: one queue (`queue_no=1`). 10M-row run: 9,500,000 rows
ingested directly into `COMPLETED` (stage ordinal 1) and 500,000 into
`INITIAL` (stage ordinal 0), in 1000-row `Immediate` transactions. 1M-row
variant: the same 95%/5% ratio (950,000 / 50,000). `rank` is assigned
monotonically per row (`base_ts + seq`), not a constant, so the claim-order
B-tree holds a realistic spread of keys rather than one degenerate value.

**Operations measured**, each a sustained loop, `spikes/claim-scale/src/main.rs` `run()`:
- **ingest**: 1000-row transactions (both stages).
- **claim**: select the first 100 free `INITIAL` records by rank and lease
  them, one `BEGIN IMMEDIATE` transaction per claim (select + per-row
  lease). 3 trials x 100 reps, claims/s and p50/p99 latency per trial.
- **finish**: release and move the just-claimed 100 records to `COMPLETED`.
  Same trial/rep shape as claim.
- **extend**: one 1000-record claim, extended repeatedly (3 trials x 20
  reps); L1 updates 1000 item rows per extend, L2 updates 1 `claims` row.
- **expiry sweep**: reclaim 10,000 expired leases, staged as 100 claims of
  100 records each (matching the claim op's own batch shape) with
  `expires_at` set 1s in the past, then swept. 3 trials. Reports discovery
  time (the scan that finds expired work) separately from total sweep time.
- **DB size and per-index size**: main-file bytes after `PRAGMA
  wal_checkpoint(TRUNCATE)`, post-ingest; per-index cost via
  `size_baseline` (identical schema minus the claim-order index only, at
  1M scale, delta against the full-schema 1M run).
- **stage-blind claim**: the same claim-order select with the `stage=?`
  predicate dropped, to show what a claim path that forgets to bind stage
  actually costs, diagnosed with `EXPLAIN QUERY PLAN` (`spike explain`
  subcommand) and timed for real at both scales.

One untimed warm-up claim+finish runs before the timed claim/finish trials
begin: the first write after the ingest checkpoint pays a one-off
page-cache/WAL-reopen cost unrelated to the layout under test (measured in
a calibration run: it inflated trial-1 p99 by ~200x versus every other
rep in that trial — `spikes/claim-scale/src/main.rs` comment at the
warm-up call site).

Driver: `spikes/claim-scale/run.sh`. Raw output: `spikes/claim-scale/results/*.jsonl`
(one JSON line per phase/trial) and `*.log` (stderr progress).

**Two methodology defects found and fixed before any of the numbers below
were trustworthy** (both MEASURED via `/proc/<pid>/io` and `EXPLAIN QUERY
PLAN`, not guessed — summarized again as Finding 0 below):

1. **Unbounded WAL growth under `wal_autocheckpoint=0`.** The initial harness
   checkpointed once after ingest, then ran the whole sustained
   claim/finish/extend/sweep loop with no further checkpoint — the log-backed
   profile disables SQLite's own auto-checkpoint (SPIKE-002 already noted
   this), and nothing else was bounding the WAL. A 300-claim + 300-finish
   probe at a trivial 10,000-row scale never finished in 270s of 98%-CPU work
   before being killed; `/proc/<pid>/io` showed 157.6 GB read and 174.9 GB
   written (16.9 GB physical) for ~300 KB of logical row changes. Production
   does not run this way: `crates/fireweed-turso/src/projection.rs:127-176`
   (`maybe_passive_checkpoint_log_backed`) issues `PRAGMA
   wal_checkpoint(PASSIVE)` after every apply batch once the WAL exceeds
   `LOG_BACKED_CHECKPOINT_TRY_BYTES = 400 MiB`, exactly the "process-wide
   Passive cap" `local.rs:3403` describes. The harness now mirrors this
   (`maybe_passive_checkpoint()`, same 400 MiB threshold, called after every
   write transaction — ingest batch, claim, finish, extend, sweep). The same
   probe workload then completed in 28.9s with claim and finish latency flat
   across all 15 repeated trials.
2. **Two `UPDATE`s silently picked the wrong index.** `EXPLAIN QUERY PLAN` on
   every hot statement (not just `SELECT`s — SPIKE-002 Finding 0 only checked
   those) found: the `finish` `UPDATE` (`WHERE queue_no=? AND seq=?`, the
   full primary key) used `items_key_idx` and bound only `queue_no=?` instead
   of the PK autoindex `sqlite_autoindex_items_1`, on both layouts, even
   though the structurally-identical `claim` lease-set `UPDATE` correctly
   picked the PK unhinted; and L2's sweep `UPDATE items SET claim_id=NULL
   WHERE claim_id=?1` did a full `SCAN items` instead of using
   `items_claim_id_idx`. Fixed with explicit `INDEXED BY` on every per-row
   `UPDATE` (`sqlite_autoindex_items_1` for by-PK updates,
   `items_claim_id_idx` for the sweep's by-claim clear). The latter also
   needed the partial index's condition restated in the `WHERE` clause
   (`AND claim_id IS NOT NULL`) — Turso's partial-index eligibility check
   does not treat `claim_id=?` as statically proving non-null the way it
   would a literal, so `INDEXED BY` alone raised "no query solution" until
   the redundant clause was added. **Consequence for production**: every
   claim-path `UPDATE`, not only its `SELECT`s, needs the same explicit-hint
   discipline SPIKE-002 already established for reads.

**A third defect, found later, at 10M-row scale, after the two above were
already fixed and Findings 1-5 already measured** (Finding 6's original L2 @
10M run): L2's `finish` throughput collapsed to ~62-69 finishes/s (p99
103.6-126.2 ms) against 679.3 finishes/s at 1M and L1's 577.2 finishes/s at
10M — a regression this document did not originally record. Diagnosed with a
new `spike diag <layout> <db_path> <n_reps>` subcommand (opens an
already-ingested db directly, no re-ingest, and runs instrumented
claim/finish reps with per-statement timing and WAL/checkpoint visibility)
against the persisted 10M-row db. `EXPLAIN QUERY PLAN` on every finish
statement, re-run at the real 10M scale rather than the small calibration db
`spike explain` normally builds, showed correct index choice throughout
(`DELETE FROM claims WHERE claim_id=?1` → `SEARCH claims USING INTEGER
PRIMARY KEY`; the finish `UPDATE` already forced `sqlite_autoindex_items_1`
per defect 2 above); WAL-checkpoint interaction was directly ruled out
(`will_checkpoint=false` and the checkpoint pragma itself took 2-4 µs on
every flagged slow rep — the WAL stayed under 50 MB, nowhere near the 400
MiB threshold); the claims-row `DELETE` was consistently sub-150µs even in
transactions where the item-row `UPDATE` took 60-120 ms. A controlled
ablation (raising `cache_size` from production's 128 MiB to 1 GiB for the
same replay, same host window) eliminated the stalls entirely (44/300 → 0/300
write transactions affected), isolating the mechanism to **page-cache
pressure**: L2's schema has two more B-tree structures (`claims` +
`claims_expires_idx`) than L1's one (`items_lease_expiry_idx`), all
competing for the same fixed 128 MiB `cache_size` against the
~9.5M-11M-entry `items_claim_idx` partial index, which does not fit that
budget on *either* layout at 10M rows (Finding 7 measures it at 25.16 MiB at
1M rows alone) — L2's extra structures evict more of that index's hot
upper-tree pages than L1's leaner schema does. On top of this, a genuine
implementation defect was found and fixed: `do_finish`'s L2 branch looped
100 individual by-PK `UPDATE`s instead of one bulk `UPDATE ... WHERE
claim_id=?` via `items_claim_id_idx` — the same grouping key `do_sweep`'s own
clear-by-claim_id statement already uses (defect 2 above). Fixing `do_finish`
to match `do_sweep`'s pattern cut the same ablation's affected-transaction
rate from 44/300 to 11/300 at production's real 128 MiB `cache_size` — a
real, worthwhile reduction, but not a complete fix: the residual cache
pressure is a genuine property of the claims-table design at 10M+-row scale,
not a bug, and is carried into the Recommendation below as an explicit
caveat. Full diagnostic detail: `spikes/claim-scale/results/finding-l2-finish-regression.txt`.
Finding 6 reports the corrected, re-measured numbers.

## Findings

Hardware note: load average climbed from `12.60` (start) to `~29-31` (out of
32 cores) partway through the sweep — other sessions on this shared host ran
concurrent `cargo`/`rustc`/linker work (VERIFIED via `ps aux`, e.g. a
`cargo check --workspace` and several `rustc`/`ld.mold`/`clang` processes
building an unrelated worktree). Absolute throughput numbers below should be
read with that in mind; every layout comparison is still same-host,
same-window, so the *relative* comparison (L1 vs L2) is unaffected. 3
repeats per cell, median and spread `(max−min)/avg` reported, the same
variance methodology SPIKE-002 Finding 4 used.

### Finding 0 (methodology, gates every number below): see the two defects in Method

Summarized here for the record: (1) sustained small write-transactions under
`wal_autocheckpoint=0` with no periodic checkpoint amplify I/O catastrophically
(157.6 GB read / 174.9 GB written for ~300 KB of logical changes, MEASURED);
fixed by mirroring production's 400 MiB Passive-checkpoint cap
(`projection.rs:127-176`). (2) Two `UPDATE`s (`finish` on both layouts, L2's
sweep clear) silently picked the wrong index and would have scaled O(table
size) per call; fixed with explicit `INDEXED BY`, mirroring SPIKE-002
Finding 0/1's discipline extended from `SELECT` to `UPDATE`. Every number
below is measured with both fixes in place.

### Finding 1: claim and finish throughput — comparable across layouts, MEASURED at 1M rows

| Metric | L1 (lease columns) | L2 (claims table) |
|---|---:|---:|
| claim, median claims/s (3 trials) | 517.5 | 626.2 |
| claim, spread | 30.6% | 24.4% |
| claim, p50 latency (median trial) | 1,666.7 µs | 1,564.4 µs |
| claim, p99 latency (median trial) | 3,451.8 µs | 1,969.3 µs |
| finish, median finishes/s (3 trials) | 535.9 | 679.3 |
| finish, spread | 34.8% | 9.7% |
| finish, p50 latency (median trial) | 1,676.7 µs | 1,453.2 µs |

Both layouts claim/finish 100-record batches at comparable, sub-2ms p50
latency — L2 measured 10-25% faster on both operations at this scale and
noise level, but the spreads (25-35% on L1) mean this gap is not a strong
signal at 1M rows; see Finding 6 for 10M. Neither claim nor finish touches a
claim-count-dependent structure differently between layouts at this batch
size (L1: 100 lease-column writes; L2: 1 claims-row insert + 100 claim_id
writes), so comparable cost is the expected result, not a surprise.

### Finding 2: lease extension — L2 is ~80x faster, MEASURED, matches the O(1) vs O(n) prediction exactly

| Metric | L1 (update 1000 item rows) | L2 (update 1 claims row) |
|---|---:|---:|
| median extends/s (3 trials) | 90.9 | 7,345.2 |
| spread | 12.7% | 6.5% |
| p50 latency (median trial) | 10,789.2 µs | 132.1 µs |
| ratio | 1x | **81x** |

This is the sharpest, cleanest measured difference in the whole sweep — a
1000-record claim's lease extension is one row write either way in the
owner's design, versus 1000 row writes in today's shape, and the measured
81x throughput ratio (82x by latency) is close to the naive prediction
(L1 does exactly 1000x the row-writes of L2 for this operation; latency
ratio is sub-1000x because L1's per-row writes are cheap individually and
batch inside one transaction, but the gap is still an order of magnitude).

### Finding 3: expiry sweep — L2's discovery step is ~75x faster, total sweep ~30-40% faster, MEASURED

| Metric | L1 (scan item rows) | L2 (scan claims rows) |
|---|---:|---:|
| discovery, median (3 trials) | 9.92 ms | 0.13 ms |
| discovery ratio | 1x | **76x** |
| total sweep, median (3 trials) | 147.87 ms | 104.27 ms |
| total sweep, spread | 6.8% | 8.7% |
| total ratio | 1x | 1.4x |
| rows reclaimed per sweep | 10,000 | 10,000 (via 100 claim rows) |

The sweep was staged as 100 claims of 100 records each (matching the spike's
own claim batch size), so L2's discovery scans 100 `claims` rows against 1
that L1 scans 10,000 `items` rows — a 100:1 ratio in candidate-set size that
measures out to a 76x wall-clock difference in the discovery step alone.
Total sweep time favors L2 by a smaller 1.4x because clearing the found work
still costs both layouts one write per affected item row (10,000 either
way) — L2's advantage is entirely in *finding* expired work cheaply, not in
clearing it, which is exactly the owner's stated goal ("stay fast with 10m+
records" is a *discovery* cost concern once most of a queue's records are
terminal, §1 "always Free").

### Finding 4: DB size — effectively identical, MEASURED

| | L1 | L2 |
|---|---:|---:|
| post-ingest-checkpoint main file | 443,392,000 B | 443,396,096 B |
| final (post-operations) main file | 443,396,096 B | 443,396,096 B |
| difference | — | **+4,096 B** (one page) |

At 1M rows (950,000 `COMPLETED` + 50,000 `INITIAL`), L1's two nullable lease
columns on every row cost almost exactly the same as L2's one nullable
`claim_id` column plus the separate `claims` table (which stays tiny: at
most a few hundred live rows at any moment, since claims are deleted on
finish/sweep). The claims table's own overhead is noise at this scale — see
Finding 7 for the isolated per-index/per-table delta.

### Finding 5: a stage-blind claim query costs ~5,000-6,500x more than the stage-bound one, MEASURED, same mechanism on both layouts

| | L1 | L2 |
|---|---:|---:|
| stage-bound `SELECT`, p50 | 80.8 µs | 62.2 µs |
| stage-blind `SELECT` (no `stage=?`), p50 | 457,412.4 µs (457 ms) | 406,186.9 µs (406 ms) |
| slowdown | **5,662x** | **6,530x** |

`EXPLAIN QUERY PLAN` (via `spike explain`, both layouts) shows why: with
`stage` unbound, Turso's planner still picks the claim-order partial index
for the `queue_no=?` prefix, but the plan adds `USE SORTER FOR ORDER BY` —
without a `stage` equality term, the index's physical order (`stage, rank,
seq`) cannot satisfy a plain `ORDER BY rank, seq` across stage boundaries, so
the engine must materialize and sort every free row of the queue (at 1M
rows, ~989,900-995,000 of them — nearly the whole table, since terminal-stage
rows are "always Free" too, draft §1) before it can return the first 100.
Forcing `INDEXED BY` on the blind query does not help (verified, `spike
explain` output) — the sorter is unavoidable once `stage` drops out of the
`WHERE` clause. This is independent of the lease-representation choice: both
layouts share the same `(queue_no, stage, rank, seq)` claim-order index
shape, and both pay the identical mechanism.

### Finding 6: at 10M rows, throughput does not degrade once the finish defect is fixed — MEASURED, this is the direct answer to "stay fast at 10m+"

L2 @ 10M below is the **corrected** re-measurement, after the `do_finish`
bulk-update-by-`claim_id` fix (Method addendum above). Both L1 @ 10M and the
first (broken) L2 @ 10M attempt were measured on this shared host with load
average 12.6 → ~29-31 (Scope's Hardware note); this corrected L2 @ 10M run
was re-measured in a calmer window (load average 5.08 → 6.41 throughout,
`spikes/claim-scale/results/l2-10m.jsonl`, `l2-10m.log`) specifically
because the first attempt's numbers were contaminated by *this spike's own*
concurrent 100M-row bulk-build process (Finding 8) — declining
throughput trial-to-trial that tracked the bulk-build's load ramp, not a
property of L2 itself. The `ingest rate` row is therefore not a precise
apples-to-apples figure against L1's (different load window) and should be
read directionally only; every other row (claim/finish/extend/sweep,
DB size, stage-bound/blind `SELECT`) is a same-mechanism, same-shape
comparison unaffected by that caveat.

| Metric | L1 @ 1M | L1 @ 10M | Δ | L2 @ 1M | L2 @ 10M | Δ |
|---|---:|---:|---:|---:|---:|---:|
| claim, median claims/s | 517.5 | 596.4 | +15% | 626.2 | 649.0 | +3.7% |
| finish, median finishes/s | 535.9 | 577.2 | +8% | 679.3 | 695.4 | +2.4% |
| extend, median extends/s | 90.9 | 87.6 | −4% | 7,345.2 | 6,201.2 | −15.6% |
| sweep, median total_ms | 147.87 | 147.15 | ~0% | 104.27 | 98.27 | −5.8% |
| sweep, median discovery_ms | 9.92 | 10.93 | +10% | 0.13 | 0.13 | ~0% |
| ingest rate, rows/s (different load windows — directional only) | 11,476 | 11,551 | ~0% | 14,721 | 16,358 | +11% |
| post-checkpoint DB size | 443,392,000 B | 4,445,007,872 B | **10.024x** | 443,396,096 B | 4,445,007,872 B | **10.024x** |
| stage-bound `SELECT` p50 | 80.8 µs | 358.3 µs (1 rep) | +343% | 62.2 µs | 291.7 µs (1 rep) | +369% |
| stage-blind `SELECT` p50 | 457,412.4 µs | 4,943,937.6 µs (1 rep) | **10.8x** | 406,186.9 µs | 4,105,957.5 µs (1 rep) | **10.1x** |

At 10x the row count, **claim, finish, extend and sweep throughput on both
layouts are flat to within measurement noise** (L1: −4% to +15%; L2: −15.6%
to +3.7%, all within or near the ~2-24% spreads this scale's trials show) —
none of the four core operations shows the growth-with-table-size signature
that would indicate an O(table size) component, on *either* layout, once the
finish defect above is fixed. **DB size scales identically and linearly on
both layouts** (10.024x for 10x the rows, exactly matching — expected, since
Finding 4/7 already established the two schemas cost the same on disk).
**The stage-blind query's cost scales with row count on both layouts**
(10.1-10.8x for 10x rows, Finding 5's O(n)-sort mechanism, confirmed at the
target scale on both). **L2 remains faster than L1 on every operation at
10M**, not just at 1M (Finding 1-3): finish +20.5% (695.4 vs 577.2/s), sweep
total −33.2% (98.27 vs 147.15 ms), sweep discovery −98.8% (0.13 vs 10.93 ms,
the ~84x ratio Finding 3 found at 1M holds at 10M), extend still ~71x
(6,201.2 vs 87.6/s). L2's extend `Δ` of −15.6% is the largest single-layout
change in the table, but 6,201/s is still 71x L1's 10M figure and within the
18% spread this cell's own 3 trials show — not read as a scaling signal.

### Finding 7: per-index / per-table cost of the claims-table design (size-baseline delta), MEASURED

`size_baseline` builds the identical 1M-row schema minus only the claim-order
partial index (`items_claim_idx`), isolating two numbers per layout: the
claim-order index's own on-disk cost (delta against the matching full-schema
1M run, Finding 1's `l1-1m`/`l2-1m`), and the layout-specific extra
structures' cost at rest (the baseline-vs-baseline delta, since both
baselines share the identical `items` + `items_key_idx` and differ only in
L1's `items_lease_expiry_idx` vs L2's `items_claim_id_idx` + `claims` +
`claims_expires_idx`).

| | L1 | L2 |
|---|---:|---:|
| baseline main file (no claim-order index) | 417,005,568 B | 417,009,664 B |
| full-schema main file (with claim-order index, Finding 1) | 443,392,000 B | 443,396,096 B |
| **claim-order index (`items_claim_idx`) cost** | **26,386,432 B (25.16 MiB)** | **26,386,432 B (25.16 MiB)** |
| layout-specific extra structures, baseline-vs-baseline delta | — | **+4,096 B (1 page)** |

The claim-order index costs **exactly the same** 26,386,432 bytes on both
layouts — expected, since it is the identical `(queue_no, stage, rank, seq)`
key shape over the identical 1M rows regardless of which lease
representation is chosen. At 25.16 MiB against a ~443 MiB total database,
it is the single largest secondary structure in the schema (~5.95% of the
1M-row file), which foreshadows Finding 8's cache-pressure result: at 10M+
rows this index no longer fits the connection's 128 MiB `cache_size`
budget on either layout (Finding 6/Interpretation).

The **layout-specific** extra structures — L1's single
`items_lease_expiry_idx` versus L2's `items_claim_id_idx` + `claims` +
`claims_expires_idx` (three B-trees instead of one) — cost only 1 extra
page (4,096 B) at this snapshot, because `size_baseline` measures
immediately after ingest, before any claim/finish/sweep cycle has run: all
three of L2's extra structures are still empty (no row has ever been
claimed), so their footprint is just their root pages. This confirms
Finding 4's "DB size is a wash" holds even in isolation — L2's *design*
does not cost more pages at rest, only more distinct B-trees that must
share the same fixed cache budget once claim/finish traffic starts
populating them (Finding 6/Interpretation, Finding 8).

### Finding 8: the owner's target shape — 100 queues, 100M rows, one Turso file — MEASURED

The owner's stated target ("we could easily have 100 1m record queues alive
at once") means many queues sharing one Turso file, not one queue at 100M
rows. This finding builds that shape directly: 100 queues, ~1M rows each
(950,000 `COMPLETED` + 50,000 `INITIAL` per queue, the same 95/5 split as
every other run), 100,000,000 rows total, L2 layout (the Recommendation),
one file. New tooling: `spike bulk_build` (fastest-possible load, not an
ingest benchmark), `spike finding8` (single-writer claim/finish/extend/sweep
round-robin over a selected subset of queues, same trial shape as `run()` so
the numbers compare directly against Findings 1-6), `spike multi_claim` (N
concurrent claimers, shared-connection vs separate-connection modes). All
three subcommands are in `spikes/claim-scale/src/main.rs`; raw output in
`spikes/claim-scale/results/multi100-l2-build.jsonl`, `finding8-l2.jsonl`,
`multi-claim-l2-large.jsonl`.

**Build time — MEASURED, exceeded the ~90-minute target, root cause identified.**
Per the task's own fallback, 100M rows were attempted first (large per-queue
transactions, `generate_series`-driven `INSERT..SELECT` instead of
per-row Rust-side binding, indexes built after the load — confirmed
`generate_series` is a built-in table-valued function on this vendored
Turso, no extension loading needed). It completed, but took **120.6 minutes
total**, not ~90:

| Phase | Time | Rate / note |
|---|---:|---|
| bulk load (100M rows, `INSERT..SELECT generate_series`) | 28.05 min | 59,418 rows/s |
| `CREATE UNIQUE INDEX items_key_idx` | **84.65 min** | dominant cost — 70% of total build |
| `CREATE INDEX items_claim_idx` (claim-order, partial) | 6.78 min | |
| `CREATE INDEX items_claim_id_idx` (partial) | 0.50 min | |
| `claims` table + `claims_expires_idx` | ~0 min | table empty at build time |
| final checkpoint (`TRUNCATE`) | 0.58 min | |
| **total** | **120.55 min** | final main file 45,093,863,424 B (45.09 GB) |

The bulk load itself (28 min for 100M rows) is well inside budget, and so is
building every *non-unique* index (6.78 + 0.50 + ~0 ≈ 7.3 min combined for
the other three structures over the same 100M rows). `CREATE UNIQUE INDEX
items_key_idx` alone blew the budget, taking **12.5x longer** than the
non-unique `items_claim_idx` built over the identical row set in the same
run. Two candidate mechanisms, not fully isolated from each other in this
spike (INFERRED, not separately ablated — time did not allow a controlled
unique-vs-non-unique-same-key comparison): (a) uniqueness verification
itself, and (b) `items_key_idx`'s key (`queue_no, key` — `key` a 16-byte
`randomblob`) has **no correlation with insertion/physical order**, unlike
`items_claim_idx`'s key (`queue_no, stage, rank, seq` — `rank` assigned
roughly monotonically per queue at ingest), so building it means inserting
100M B-tree entries in effectively random key order rather than the
near-sorted order the other indexes get "for free" from the ingest scheme.
(b) is the more likely dominant factor given the 12.5x gap significantly
exceeds what uniqueness-checking overhead alone typically costs in B-tree
engines, but this spike did not isolate it. **Practical consequence for
production**: bulk-populating or backfilling a large existing table should
avoid building a `UNIQUE` index (or any index on an order-uncorrelated key)
as a bulk `CREATE INDEX` step after load — either build it incrementally
during ingest (incurring the per-row cost throughout, but bounded and
predictable) or budget for this cost as a distinct, dominant phase.
Per the task's own contingency, since 100M *did* complete (just over
budget), it was used for the rest of Finding 8 rather than falling back to
20M — the fallback was not needed, only flagged.

**Claim/finish/extend/sweep, round-robin over 20 of the 100 queues, single
writer — MEASURED** (`spike finding8`, load average 6.7-7.5 throughout, no
mid-cell jump):

| Metric | L2 @ 10M (1 queue, corrected Finding 6) | L2 @ 100M (100 queues, 20 touched) | Δ |
|---|---:|---:|---:|
| claim, median claims/s (3 trials) | 649.0 | 351.6 | **−45.8%** |
| claim, p99 latency (worst trial) | 1,806.9 µs | 94,559.5 µs | **52x** |
| finish, median finishes/s (3 trials) | 695.4 | 746.3 | +7.3% |
| finish, p99 latency (worst trial) | 1,674.2 µs | 1,800.9 µs | +7.6% |
| extend, median extends/s (3 trials) | 6,201.2 | 8,805.1 | +42.0% |
| sweep, median total_ms (3 trials) | 98.27 | 98.92 | +0.7% |
| sweep, median discovery_ms (3 trials) | 0.13 | 0.12 | ~0% |
| DB size, start → end of trials | — | 45,093,863,424 → 45,093,900,288 B | +9 pages |

**Finish, extend and sweep hold flat from 10M/1-queue to 100M/100-queues,
20 touched** — the finish fix (Method addendum) generalizes to the target
shape; extend and sweep's O(claims)-not-O(records) design (Findings 2-3)
generalizes too, unaffected by how many *other* queues' rows share the file.
**Claim does not hold flat**: median throughput dropped 45.8% and two of the
three trials show a severe p99 tail (53.6-94.6 ms, the worst trial's p99 is
52x the 10M single-queue figure) — the same page-cache-pressure mechanism
from the finish regression (Method addendum, `finding-l2-finish-regression.txt`),
now visible on `do_claim` instead: at 100M rows the shared `items_claim_idx`
partial index is roughly 10x larger again, and `do_claim`'s per-row
lease/`claim_id`-assignment loop has no pre-existing grouping key to bulk by
(unlike finish, which can bulk-update by the `claim_id` a claim's rows
already share) — there is no equivalent fix available within this design.
This is a real, MEASURED risk for "stay fast at 100M+", carried into the
Recommendation below, not papered over by the finish fix.

**Does claim cost depend on total rows in the file, or only the
queue/stage prefix? Both, in different senses — MEASURED plus INFERRED
mechanism.** `EXPLAIN QUERY PLAN` (Finding 5, `explain-query-plans.txt`)
shows the stage-bound claim `SELECT` compiles to `SEARCH items USING INDEX
items_claim_idx (queue_no=? AND stage=?)` — an indexed seek to the target
queue's own contiguous region of the shared B-tree, then a bounded walk of
exactly the batch size. **Algorithmically**, this seek is O(log of total
index size) — a slowly-growing term (log₂(100M) ≈ 26.6 vs log₂(10M) ≈ 23.3,
about 14% deeper) — and the walk touches only that queue's own entries, not
other queues' rows as a structural matter: the *plan* does not change
between a 10M single-queue file and a 100M/100-queue file, and other
queues' row counts are not part of the seek or walk cost algebraically.
**Practically**, under production's fixed 128 MiB `cache_size` shared by
the *whole file*, once total data exceeds that budget (Finding 7: the
claim-order index alone is 25.16 MiB at 1M rows on one queue; at 100M rows
across 100 queues it is roughly 100x that, far past 128 MiB regardless of
which queue is being served), a claim against queue A can be slowed by
whatever evicted queue A's own hot pages from cache — including other
queues' traffic. This is exactly the 45.8%-median / 52x-p99 result above:
not a change in Big-O, but a change in cache-hit rate driven by total file
size, because 128 MiB has to serve the whole file's working set, not just
one queue's.

**Concurrency: 1 writer vs N concurrent claimers, shared vs separate
connections — MEASURED** (`spike multi_claim`, `multi-claim-l2-large.jsonl`,
load average 6.7-8.5 throughout):

| Mode | N workers | claims/s | p50 | p99 |
|---|---:|---:|---:|---:|
| shared (1 `Mutex<Connection>`) | 1 | 166.1 | 2,555.5 µs | 90,684.1 µs |
| shared | 8 | 272.3 | 23,478.6 µs | 124,482.3 µs |
| shared | 32 | 217.7 | 103,407.8 µs | 591,581.6 µs |
| separate (N raw connections) | 1 | 317.9 | 2,639.3 µs | 5,597.5 µs |
| separate | 8 | 267.4 | 2,612.7 µs | 184,126.7 µs |
| separate | 32 | — | — | **crashed: `Busy("database is locked")`** |

**How writes serialize, stated plainly**: every write to a Turso/SQLite WAL
file goes through exactly one physical writer lock, regardless of how many
connections or app-level workers exist — this spike's `shared` mode makes
that explicit by routing every worker's transaction through one
`Arc<tokio::sync::Mutex<Connection>>`, the same shape production's own
`writer: &Mutex<Connection>` parameter already uses
(`crates/fireweed-turso/src/projection.rs` `run_passive_checkpoint`,
Method). Under `shared`, aggregate throughput does **not** scale with N
(166→272→218 claims/s across N=1/8/32, noisy but flat, not linear in N —
confirming no write parallelism is available no matter how many workers
queue up), and p50 latency grows roughly with queue depth (2.6 → 23.5 → 103
ms as N grows 1→8→32) — a predictable, if slower, queueing cost, and it
**never fails**: every worker's claim eventually succeeds. Under `separate`
(each worker opens its own connection, no app-level coordination, relying
on Turso's own file lock and the harness's `busy_timeout=5s`), throughput
is similarly flat to slightly worse (318→267 claims/s, N=1→8) with a p99
tail that grows *faster* than `shared`'s (5.6 → 184 ms, N=1→8) because a
losing connection's wait is a blocking retry against the lock rather than
an ordered queue slot — and at N=32 this crossed the 5-second
`busy_timeout` and the worker's transaction failed outright with
`Busy("database is locked")` (first observed in a smaller N=32 sample,
`multi-claim-l2.jsonl`) — a correctness failure, not just a slowdown.
**Consequence for production**: the existing `Mutex<Connection>` pattern is
not a style choice — it is what prevents `SQLITE_BUSY` failures from ever
reaching a caller at realistic claimer concurrency; bypassing it (multiple
independent writer connections to the same file) trades a predictable
queueing cost for a real reliability cliff once concurrency is high enough
to exceed the busy-timeout budget.

### Finding 9: lean claim-path row at 100M — MEASURED, removes Finding 8's claim degradation

Follow-up to Finding 8's unresolved claim-latency risk, prompted directly by
SPIKE-006's Recommendation: SPIKE-006
(`docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md`)
recommends a claim-path row that "carries only `key, stage, rank, claim_id,
group, gates, version`... no field bytes at all," with fields split into a
separate table, and found that its own layout C — the only one shaped this
way — was immune to a 128 MiB vs 1 GiB cache-size ablation ("no measurable
benefit from 1 GiB") specifically because "C's claim-path row and its
field-storage tables are structurally decoupled, so claim churn cannot evict
field data from cache and vice versa." Finding 8's degraded claim path was
carrying ~300 B of `fields` bytes on every row of the same B-tree the claim
`SELECT`/`UPDATE` touches. This finding tests whether narrowing L2's item
row the same way — no `fields` column, a separate `fields` table holding
the identical ~300 B payload — closes that gap at the owner's actual 100M
target shape, using the same `finding8` harness and trial shape as Finding
8 so the numbers compare directly.

**Methodology check, MEASURED (code read): SPIKE-004's claim path already
had both of SPIKE-006's required disciplines.** SPIKE-006's own Method
section reports that its harness needed two fixes — every multi-statement
op wrapped in one explicit transaction, and per-row statements prepared
once and reused across the loop — "until both fixes... this spike's own
claim path was 300-500x slower than its corrected form." SPIKE-006
explicitly credits this spike's `do_claim` as the shape it should have
matched from the start ("the same per-operation transaction shape
`spikes/claim-scale`'s `do_claim` already used"; "the exact
prepare-once/execute-many shape `spikes/claim-scale`'s `do_claim` already
used"). Re-reading `do_claim` (`spikes/claim-scale/src/main.rs:479-554`)
confirms this directly: the whole select-then-lease sequence runs inside
one `conn.transaction_with_behavior(TransactionBehavior::Immediate)` (line
493), and each per-row lease statement — L1's lease-column `UPDATE` (line
507, executed in a loop at 514-522) and L2's `claim_id` `UPDATE` (line 542,
executed in a loop at 545-546) — is prepared once outside its loop and
reused via `.execute()` per row, not re-prepared per row. `do_claim_q`,
`do_finish`, `do_extend` and `do_sweep` follow the identical shape. **Both
disciplines were already present from Finding 0's methodology fixes; no
change was needed for Finding 9.**

**Build: lean L2, 100 queues x 1M rows (95%/5% split), `items_key_idx`
maintained during ingest — MEASURED, 56.8% faster than Finding 8's fat
build, and avoids its dominant 84.65-minute post-load UNIQUE-index cost.**
Per the task's instruction to avoid Finding 8's dominant post-load
`CREATE UNIQUE INDEX items_key_idx` cost where practical, this build
creates `items_key_idx` *before* the load (empty-table DDL, near-instant)
so it is maintained incrementally as part of each per-queue
`INSERT..SELECT generate_series` statement, instead of as a separate bulk
step after — the same "index on an order-uncorrelated key" identified as
Finding 8's dominant cost, now paid incrementally throughout ingest instead
of in one 84.65-minute step. `items_claim_idx` (order-correlated with
`rank`, already fast in Finding 8) and the rest still build after the load,
unchanged from Finding 8's shape. Small-scale validation (a 2-queue,
2M-row probe) confirmed the maintained-index overhead was reasonable
(~20s/queue against Finding 8's ~16.8s/queue unindexed-load rate) before
committing to the full 100-queue build (`spike bulk_build l2 ... lean`,
`spikes/claim-scale/results/multi100-l2-lean-build.jsonl`).

| Phase | Fat (Finding 8) | Lean (Finding 9) |
|---|---:|---:|
| bulk load (100M rows) | 28.05 min (`items_key_idx` not yet built) | 42.03 min (`items_key_idx` maintained during ingest) |
| `CREATE UNIQUE INDEX items_key_idx` (post-load) | **84.65 min** | **0 min (built before load, folded into bulk load above)** |
| `items_claim_idx` (post-load) | 6.78 min | 8.81 min |
| `items_claim_id_idx` (post-load) | 0.50 min | 0.92 min |
| `claims` table + `claims_expires_idx` | ~0 min | ~0 min |
| final checkpoint | 0.58 min | 0.31 min |
| **total** | **120.55 min** | **52.06 min** |
| final main file size | 45,093,863,424 B (45.09 GB) | 47,911,657,472 B (47.91 GB) |

Total build time drops from 120.55 to 52.06 minutes (**56.8% faster**,
**68.5 minutes saved**) and lands well inside the original ~90-minute
target Finding 8 missed. The lean bulk-load phase itself runs at 39,659
rows/s against Finding 8's 59,418 rows/s unindexed rate; INFERRED (rate
comparison only, not separately isolated by an ablation run) that roughly
14 of the 42.03 minutes is the `items_key_idx`-maintenance cost folded into
that phase, the rest matching Finding 8's own two-table (items + a new
`fields` table, same shape as the recommendation) write cost. Final DB size
is **not** smaller (47.91 GB vs 45.09 GB, +6.25%) — moving `fields` to a
separate table does not reduce total bytes stored, it relocates them; the
point of the lean row is a narrower `items` table (the B-tree the claim
path's `SELECT`/`UPDATE` actually touches), not a smaller file. See
Limitations for what this build did not isolate.

**Claim/finish/extend/sweep, round-robin over 20 of the 100 queues, single
writer, 128 MiB cache — MEASURED**, calm host window (load average
descended 21.07 -> 11.40 while waiting for concurrent unrelated host
activity to clear, then held 11.29-11.40 (1-min) / 15.13-15.22 (5-min)
flat across both this cell and the 1 GiB ablation cell below, no mid-cell
jump; an initial attempt taken while the host was still busy with this
spike's own preceding bulk-build plus unrelated concurrent `cargo`/`rustc`
work from other sessions was discarded and re-measured in the calmer
window once available, the same reason Finding 6 re-measured its own L2 @
10M numbers — `spikes/claim-scale/results/finding8-l2-lean.jsonl`):

| Metric | Lean L2 @ 100M (20 touched) |
|---|---:|
| claim, median claims/s (3 trials) | 620.9 |
| claim, spread | 2.2% |
| claim, p99 latency (worst trial) | 2,395.1 µs |
| finish, median finishes/s (3 trials) | 708.5 |
| finish, spread | 14.1% |
| finish, p99 latency (worst trial) | 2,041.4 µs |
| extend, median extends/s (3 trials) | 9,905.2 |
| extend, spread | 10.4% |
| sweep, median total_ms (3 trials) | 106.56 |
| sweep, median discovery_ms (3 trials) | 0.12 |

**Cache-size ablation (lean, 128 MiB vs 1 GiB) — MEASURED**, same calm
window, back-to-back, same 20 queues
(`spikes/claim-scale/results/finding8-l2-lean-1gib.jsonl`):

| Metric | 128 MiB | 1 GiB | Δ |
|---|---:|---:|---:|
| claim, median claims/s | 620.9 | 565.7 | −8.9% |
| finish, median finishes/s | 708.5 | 668.7 | −5.6% |
| extend, median extends/s | 9,905.2 | 5,607.1 | **−43.4%** |
| sweep, median total_ms | 106.56 | 105.65 | −0.9% |

Claim, finish and sweep are flat to within this harness's normal
trial-to-trial noise band (Findings 1/6 already show 2-35% spreads at this
rep count) — no measurable benefit from 1 GiB, the same qualitative result
SPIKE-006 Finding 8 reports for its own layout C. **Extend is the one
exception**: a real, tight-spread (1.2%, not noise) 43.4% slowdown at 1
GiB, reproducible across all 3 trials on the same queues in the same
order as the 128 MiB cell. MEASURED, mechanism not isolated — INFERRED
candidate explanation: extend's absolute latency is tiny (a single
`claims`-row `UPDATE`, ~90-170 µs total), so a fixed per-statement pager
overhead that scales with the *configured* `cache_size` (a larger page-hash
table to probe on every page fetch/release, independent of how many pages
are actually resident) would be amortized to noise inside claim/finish's
much longer 100-row transactions but would dominate extend's single-row
one. Not confirmed by a dedicated ablation of pager internals — flagged for
follow-up, not load-bearing for the Recommendation below (128 MiB is
already the production default; this is not a case *for* 1 GiB).

**Does a lean claim-path row remove Finding 8's claim degradation at 100M?
Yes — MEASURED, stated plainly.**

| Metric | L2 @ 10M (1 queue, Finding 6) | Fat L2 @ 100M (20 touched, Finding 8) | Lean L2 @ 100M (20 touched, Finding 9) | Lean Δ vs 10M | Lean Δ vs Fat @100M |
|---|---:|---:|---:|---:|---:|
| claim, median claims/s | 649.0 | 351.6 | 620.9 | **−4.3%** | **+76.6%** |
| claim, p99 latency | 1,806.9 µs | 94,559.5 µs | 2,395.1 µs | 1.33x higher | **39.5x lower (faster)** |
| finish, median finishes/s | 695.4 | 746.3 | 708.5 | +1.9% | −5.1% |
| finish, p99 latency | — | 1,800.9 µs | 2,041.4 µs | — | +13.4% |
| extend, median extends/s | 6,201.2 | 8,805.1 | 9,905.2 | +59.7% | +12.5% |
| sweep, median total_ms | 98.27 | 98.92 | 106.56 | +8.4% | +7.7% |
| sweep, median discovery_ms | 0.13 | 0.12 | 0.12 | ~0% | 0% |

Finding 8's claim degradation — median throughput down 45.8% and p99 up to
52x from the clean 10M/1-queue baseline — is the one number this finding
targeted, and it is the one number that moves: lean's 100M/20-touched claim
throughput (620.9/s) sits within 4.3% of the 10M/1-queue baseline (649.0/s,
well inside this harness's normal single-digit-to-teens spread band) and
39.5x faster in the tail than the fat layout's own 100M measurement, itself
landing within 1.33x of the 10M baseline's own p99 — the "flat from 10M to
100M" result Finding 8 wanted but did not get on the fat layout. Finish,
extend and sweep — already flat under the fat layout (Finding 8's own
`do_finish` fix) — remain flat under lean too, exactly as expected: nothing
about narrowing the row's field bytes changes an operation whose cost was
already bounded by claim count rather than record width. **The mechanism
matches SPIKE-006's own prediction exactly**: `items_claim_idx` (the shared
`(queue_no, stage, rank, seq)` partial index) is byte-identical in shape
between the fat and lean layouts — indexing only `queue_no, stage, rank,
seq`, never `fields` — so its own on-disk size is unaffected by this
change (Finding 7 already established the claim-order index costs the
same on both L1 and L2 regardless of lease representation, for the same
reason: neither indexes `fields`). What changes is the `items` *table*
itself, which `do_claim`'s per-row `UPDATE items INDEXED BY
sqlite_autoindex_items_1 SET claim_id=? WHERE queue_no=? AND seq=?`
directly reads and writes: at ~64 bytes/row instead of ~364 bytes/row (5.7x
narrower — INFERRED from the schema, `spikes/claim-scale/src/main.rs`
`schema_ddl_lean` vs `schema_ddl`, not separately measured as an isolated
byte count), roughly 5.7x more of that table's rows fit in the same 128 MiB
budget, so a claim's random touches across the shared table are far less
likely to evict — or be evicted by — another queue's pages. This is the
same "claim-path row and field-storage tables are structurally decoupled,
so claim churn cannot evict field data from cache and vice versa" mechanism
SPIKE-006 Finding 8 reports for its own layout C, now confirmed at the
owner's actual multi-queue target shape rather than SPIKE-006's own
single-file, fewer-queues sweep.

**The 100M claim target is met.** Under the lean L2 layout, claim, finish,
extend and sweep are all flat from 10M/1-queue to 100M/100-queues (20
touched) to within this harness's ordinary measurement noise — the
degradation Finding 8 found and could not fix within the fat-row design is
gone under the lean row, not merely reduced.

## Interpretation

1. **The owner's claims-table proposal (question 4) is a clean win on every
   operation that scales with claim *count* rather than claim *record
   count*: extend (~81x) and the sweep's discovery step (~76x)**, both
   MEASURED, both matching the design's own O(1)-vs-O(n) logic exactly (one
   `claims` row per claim vs. one row write per claimed *record*). These are
   also the two operations most directly implicated by "stay fast with 10m+
   records": a queue running for a long time accumulates many claims over
   its life, and a metadata table sized by claim count (bounded by
   concurrency x lease duration, not by total record count) is the
   mechanism that keeps them cheap at 10M+ resting records.
2. **Claim and finish throughput are not meaningfully different between
   layouts** (Finding 1) — both are dominated by the same per-record cost
   (an indexed row write, once per claimed record, times batch size),
   which every design pays regardless of where the lease state lives.
3. **DB size is a wash** (Finding 4): the claims table's own footprint is
   noise next to 1M+ item rows, because live claims are always a small
   fraction of resting records (bounded by concurrency, not scale).
4. **Every claim path — on both layouts — must bind `stage`, or pay
   thousands-x** (Finding 5). This is not a lease-representation question;
   it is a property of the shared `(queue_no, stage, rank, seq)` claim-order
   index shape the contract already specifies (draft §7.2), and it holds
   regardless of which lease design is chosen.
5. **Finding 6 (10M rows) is the direct answer to "we need to stay fast with
   10m or more records" — but only after fixing a real L2-specific defect
   this document did not originally record.** L2's `finish` throughput
   collapsed ~10x at 10M rows in the first measurement attempt; root-caused
   to page-cache pressure from L2's extra B-trees (`claims`,
   `claims_expires_idx`) competing with the large `items_claim_idx` under
   production's fixed 128 MiB `cache_size`, compounded by a genuine
   implementation defect (`do_finish` not using the `claim_id` grouping key
   the way `do_sweep` already does). After the fix, claim/finish/extend/sweep
   are flat 1M→10M on both layouts and L2 remains faster than L1 on every
   operation. The underlying cache-pressure mechanism is not eliminated,
   only reduced — see point 7.
6. **Finding 7 confirms the claim-order index, not the lease design, drives
   DB size**: `items_claim_idx` costs the identical 25.16 MiB on both
   layouts at 1M rows (~5.95% of the file), while the two layouts' own
   extra structures cost only 1 page apart when empty. This is also the
   first hint of Finding 7/8's cache-pressure story: at 25 MiB per 1M rows,
   the claim-order index alone exceeds the 128 MiB `cache_size` budget
   somewhere in the 10-25M-row range even for a *single* queue, well before
   the owner's 100M-row target.
7. **Finding 8, at the owner's actual target shape (100 queues, 100M rows,
   one file), confirms finish/extend/sweep generalize cleanly but surfaces a
   real, MEASURED residual risk on `claim`**: median claim throughput
   dropped 45.8% (100M/20-queues-touched vs the clean 10M/1-queue point) with
   p99 latency spikes up to 52x, driven by the same cache-pressure mechanism
   as point 5's finish regression — but `claim` has no equivalent fix,
   because unlike `finish` (which can bulk-update by the `claim_id` its rows
   already share), a claim's rows have no pre-existing grouping key before
   the claim is assigned. Algorithmically the claim path is still O(log
   total index size) for the seek plus O(batch) for the walk — confirmed by
   `EXPLAIN QUERY PLAN` compiling to the same indexed-seek plan at every
   scale measured — but under a *fixed* cache budget shared by the whole
   file, "stay fast at 100M+" is not free: it depends on total file size,
   not only on the queue being served. Finding 8 also shows L2's sweep is
   *global* (one call reclaims expired claims across every queue in the
   file, regardless of how many), while L1's sweep is forced
   queue-by-queue (one call per queue) by its `(queue_no,
   lease_expires_at)`-keyed expiry index — a further, structural point in
   L2's favor at the owner's multi-queue target shape, independent of the
   claim-latency risk above.
8. **Finding 8's concurrency test shows writes fully serialize regardless of
   how many concurrent claimers exist**, and that this is a *requirement*,
   not a style choice: routing all writes through one
   `Mutex<Connection>` (production's own `writer: &Mutex<Connection>`
   pattern) never fails, only queues (p50 latency grows with concurrency);
   bypassing it with independent connections relying on Turso's own file
   lock is measurably worse under load and fails outright
   (`Busy("database is locked")`) once concurrency exceeds the
   `busy_timeout` budget.

## Recommendation

**Adopt the owner's claims-table design (L2), with a lean claim-path row
carrying no `fields` bytes** for the S3×Turso lease representation. This
holds at 1M, 10M and the owner's actual 100M/100-queue target shape
(Findings 1-9). Finding 8 first surfaced a real, measured claim-latency
degradation at 100M+ under a fat (fields-inline) item row; Finding 9 traced
it to page-cache pressure from those inline field bytes and resolved it by
dropping `fields` from the item row (below) — the claim path's item row
must be lean from the start, not fat-then-optimized-later.

**1. Lease representation.** A `claims(claim_id INTEGER PRIMARY KEY,
token_hash BLOB NOT NULL, holder TEXT, expires_at INTEGER NOT NULL,
record_count INTEGER NOT NULL)` table, plus a nullable, indexed `claim_id`
on the item row in place of today's `lease_token_hash`/`lease_expires_at`
columns. It is never worse than today's lease-column shape on claim/finish
(Finding 1) or DB size (Finding 4/7 — the claims table and its index cost 1
extra page over L1's single expiry index when empty, noise against the
25.16 MiB claim-order index either layout must carry). It is dramatically
better on extend (~81x, Finding 2) and the sweep's discovery step (~76x at
1M, ~84x still holding at 10M, Finding 3/6) because both become O(claims)
not O(claimed records) — the direct fix for "stay fast at 10m+ records".
At the owner's 100-queue target shape, L2's sweep is additionally *global*
(one call reclaims expired claims across every queue in the file) where
L1's is forced queue-by-queue by its `(queue_no, lease_expires_at)` key
(Finding 8) — a further, structural advantage once many queues share one
file.

**2. Index set.** Four structures on top of the base table + its
`(queue_no, key)` unique index: (a) the stage-prefixed claim-order partial
index `(queue_no, stage, rank, seq) WHERE claim_id IS NULL` the contract
already specifies (draft §7.2) — an indexed range seek on `(queue_no,
stage)` followed by an ordered walk of exactly the claimed batch size, no
sorter, confirmed at every scale measured (Finding 5/6/8); (b) `claim_id
INTEGER, WHERE claim_id IS NOT NULL` on the item row, serving finish's
bulk clear and any by-claim lookup; (c) `claims(expires_at)` serving the
global sweep; (d) the claims table's own PK. Building large indexes whose
key is *not* correlated with insertion order (like a random-blob unique
key) as a bulk step after a large load is expensive — `CREATE UNIQUE INDEX
items_key_idx` over 100M rows took 84.65 minutes, 12.5x longer than the
non-unique, insertion-order-correlated `items_claim_idx` over the identical
rows (Finding 8) — relevant to migration/backfill planning, not to the
steady-state claim path itself.

**3. Required statement forms, MEASURED as necessary, expected to carry
over to production's actual schema:**
- `INDEXED BY` forced on every hot statement, `SELECT` *and* `UPDATE` alike
  (Finding 0/Method) — the planner does not reliably choose the right index
  on its own, on this Turso version, for either statement kind.
- The sweep's by-`claim_id` clear needs the partial index's `IS NOT NULL`
  condition restated explicitly in the `WHERE` clause alongside `INDEXED
  BY` (Finding 0) — Turso's partial-index eligibility check does not infer
  it from a bound parameter.
- **`finish` must bulk-update by `claim_id`** (`UPDATE items INDEXED BY
  items_claim_id_idx SET stage=?, claim_id=NULL, ... WHERE claim_id=? AND
  claim_id IS NOT NULL`), mirroring the sweep's own pattern, *not* loop a
  by-PK `UPDATE` once per claimed row — this is the fix for the finish
  regression found in this spike (Method addendum, Finding 6) and cut the
  same page-cache-pressure ablation's affected-transaction rate from 14.7%
  to 3.7% at production's real `cache_size`. Every row a `finish` call
  touches shares one `claim_id` by construction, so this statement form is
  always available and should be the shipped shape, not an optimization to
  revisit later.
- Every claim-path query must bind `stage`, or pay 5,000-6,500x at 1M rows
  and 10.1-10.8x more on top of that at 10M (Finding 5/6) — not a
  lease-representation question, a property of the shared claim-order index
  shape, applies to both layouts.

**4. Checkpoint policy.** Keep the production `maybe_passive_checkpoint_log_backed`
mechanism (already present, `projection.rs:127-176`, `PRAGMA
wal_checkpoint(PASSIVE)` once the WAL exceeds 400 MiB) exercised on the
claim path's own connection — this spike's initial, unbounded-WAL harness
reproduced a >50,000x I/O amplification without it (Finding 0/Method), and
that mechanism, not the lease design, is what makes the *entire* claim path
tractable under sustained write load at any scale. Separately, and
specific to concurrent access: route every write through one
`Mutex<Connection>` (production's existing pattern) rather than multiple
independent connections to the same file — writes serialize either way
(exactly one physical WAL writer lock exists), but the `Mutex` pattern
degrades to a predictable queueing cost under concurrency and never fails,
where independent connections relying on Turso's own file lock degrade
faster and fail outright (`Busy("database is locked")`) once concurrent
claimers exceed the `busy_timeout` budget (Finding 8, MEASURED: shared mode
never errored at N=32 concurrent claimers; separate mode crashed at N=32).

**What threatened "fast at 100M+" — resolved by Finding 9's lean
claim-path row, MEASURED.** Finding 8 found `claim` throughput was not
flat from 10M/1-queue to 100M/100-queues (20 touched): median dropped
45.8%, with p99 spikes up to 52x, driven by page-cache pressure from the
fat item row — ~300 B of `fields` bytes inline on every row of the same
`items` table `do_claim`'s per-row `UPDATE` reads and writes, crowding out
the claim-order index and the rest of the schema under the fixed 128 MiB
`cache_size` budget. **Finding 9 resolves this**: dropping `fields` from
the item row entirely (SPIKE-006's own recommended layout, applied to this
spike's L2 claims-table design) and moving it to a separate table that the
claim path never touches narrows the `items` table roughly 5.7x
(~364 B/row to ~64 B/row), and claim throughput at 100M/20-touched returns
to within 4.3% of the clean 10M/1-queue baseline (620.9 vs 649.0 claims/s)
with p99 within 1.33x of that baseline (down from 52x) — flat, not merely
improved, and the 100M claim target is met. This was not available to the
fat-row L2 design Finding 8 measured; it requires the claim-path row
itself to carry no field bytes, exactly as SPIKE-006 independently
recommends. **Practical consequence**: production should ship the lean
row (`queue_no, seq, key, stage, rank, version, claim_id` — no `fields`)
from the start, not add it later as an optimization — a fat-row L2
deployment at the owner's 100-queue/100M-row target would re-encounter
Finding 8's degradation. Caveats carried forward, not resolved by Finding
9: only 20 of the 100 queues were exercised per trial, not all 100; L1 was
not re-measured with a lean row (out of scope — L1 is not the
Recommendation, and Finding 6/7 already showed L1 pays the identical
claim-order-index cache pressure fat L2 did); and Finding 9's own cache-size
ablation surfaced one unexplained wrinkle — `extend` ran 43.4% slower at 1
GiB than at 128 MiB under the lean layout, mechanism not isolated — which
does not argue against 128 MiB (already production's default, and already
sufficient for claim/finish/sweep under the lean row).

## Limitations

- Shared, concurrently-loaded WSL2 host (load average `12.6` → `~29-31` of 32
  cores partway through — Scope/Findings). Absolute latency/throughput
  numbers should be read as order-of-magnitude, relative-ranking evidence
  between the two layouts measured in the same window, not precise SLA
  figures — consistent with SPIKE-002's own variance disclosure.
- Standalone-project plain local-file IO, not fireweed's private
  `RebuildableIo` wrapper (same limitation SPIKE-002 disclosed, INFERRED not
  to change the layout-to-layout *comparison* since both layouts run through
  the identical IO path).
- `claim`/`finish` batch size fixed at 100 and the extend/sweep sizes fixed
  at the task's specified 1,000/10,000 — this spike does not sweep batch
  size, so a conclusion at a very different batch size is not directly
  measured here.
- The stage-blind query (Finding 5) was measured at `FW_SPIKE_BLIND_REPS=3`
  for 1M rows and reduced to 1 rep at 10M scale (`run.sh`) because a single
  rep already costs hundreds of ms to seconds; median/spread at 10M scale is
  therefore a single measurement, not a 3-repeat median, and should be read
  accordingly.
- `attempts`/`version`/gate/group mechanics from the kernel contract are not
  modeled in this spike's leaner schema (the task specified exactly
  `queue_no, key, stage, rank, fields, version`) — this spike isolates the
  claim-order-index and lease-representation question the owner asked about,
  not the full record shape SPIKE-002/SPIKE-003 already cover.
- The two methodology defects (Finding 0) were found and fixed *during* this
  spike's own development, not inherited from a previously-validated
  harness; they are disclosed in full (Method) precisely because they would
  otherwise have invalidated every number in this document. A third,
  L2-specific defect (the finish regression) was found the same way, later,
  at 10M-row scale, after Findings 1-5 were already measured — disclosed in
  full in the Method addendum and `results/finding-l2-finish-regression.txt`.
- The corrected L2 @ 10M re-measurement (Finding 6) was run in a calmer host
  window (load average 5.1-6.4) than L1 @ 10M's original run (load average
  12.6→~29-31) — a direct consequence of re-running it *after* diagnosing
  the regression, once this spike's own 100M-row bulk-build (Finding 8) had
  released the host. Every claim/finish/extend/sweep/DB-size/`SELECT`
  comparison in Finding 6 is a same-mechanism, same-shape comparison
  unaffected by this (the *relative* L1-vs-L2 comparison still holds); the
  `ingest rate` row is flagged directional-only for this reason and is not
  load-bearing for any Recommendation claim.
- Finding 8's 100M-row dataset was built once (L2 layout only, ~120.6
  minutes) — L1 at the same 100-queue/100M-row shape was not built and
  measured, per the task's own "L1 as the control if time allows" allowance
  and this spike's overall time budget; Finding 8's Interpretation points
  are therefore INFERRED to apply to L1 too (L1 shares the identical,
  equally-oversized `items_claim_idx`, Finding 6/7), not separately
  MEASURED at 100M scale.
- Finding 8's `multi_claim` concurrency matrix used `ops_per_worker` = 30
  (first pass) and 100 (second, reported pass) per cell — smaller sample
  sizes than the 100-rep, 3-trial methodology Findings 1-6 use, because
  each `(mode, N)` cell is its own short-lived process; the qualitative
  conclusions (writes serialize, `shared` never fails, `separate` degrades
  and then fails at N=32) are consistent across both passes and a `Busy`
  crash is a binary, not a percentile, so this is not read as materially
  weaker evidence for *that* claim, but the exact claims/s and percentile
  figures in Finding 8's concurrency table carry more sampling noise than
  Findings 1-6's.
- The 100M-row bulk build's `CREATE UNIQUE INDEX` cost (84.65 minutes, 12.5x
  the non-unique index over the same rows, Finding 8) was not decomposed
  into "uniqueness checking" vs. "random key order" — both are plausible
  contributors and this spike did not run the controlled unique-vs-non-unique
  same-key ablation that would isolate them; the finding and its production
  consequence (avoid bulk-building such indexes after a large load) hold
  either way, but the *mechanism* attribution is INFERRED, not MEASURED.
- `spike claim`'s per-row lease/`claim_id`-assignment loop was not tested
  with a bulk `IN (...)`-list `UPDATE` as an alternative to the current
  per-row-by-PK form — a possible partial mitigation for Finding 8's
  claim-latency risk at 100M+, mentioned in the Recommendation as a
  direction for separate follow-up, not measured in this spike; Finding 9
  resolved the underlying degradation a different way (a lean row) without
  needing this.
- **Finding 9's lean-layout db was built once** (L2 only, ~52 min), the
  same "build once, measure against it" scope Finding 8 itself used for the
  fat build — no repeat build to check build-time variance. Its `finding8`
  measurement cells (128 MiB, 1 GiB) were each run 3 trials as usual, but an
  initial attempt at 128 MiB was discarded and re-measured after waiting for
  a calmer host window (load descended from ~21 to ~11 over about 5.5
  minutes of unrelated concurrent `cargo`/`rustc` activity on this shared
  host clearing) — the same contamination-and-recheck pattern Finding 6
  already disclosed for its own re-measurement; the discarded attempt is
  kept in `results/finding8-l2-lean-contaminated.jsonl` for reference, not
  used in Finding 9's reported numbers.
- **Only 20 of the lean db's 100 queues were exercised**, the same subset
  size Finding 8 used, not all 100 — Finding 9's Interpretation is
  MEASURED for that touched subset and INFERRED (not separately measured)
  to generalize to a full 100/100 sweep, the same disclosed gap Finding 8
  already carried.
- **L1 was not re-measured with a lean row.** Finding 9 targets the
  Recommendation (L2) specifically, since SPIKE-006's lean-row rationale
  (decoupling claim-path state from field bytes) is orthogonal to which
  lease representation is chosen; L1's own 100M/20-touched claim number
  (Finding 8) was not re-measured under a lean row and is INFERRED, not
  MEASURED, to improve similarly.
- **Finding 9's 128-MiB-vs-1-GiB `extend` result (43.4% slower at 1 GiB)
  was not isolated to a mechanism** — flagged as a real, tight-spread,
  reproducible MEASUREMENT with an INFERRED (pager-overhead) candidate
  explanation only; no dedicated ablation of Turso's pager internals was
  run to confirm it.
- **The 5.7x item-row-width narrowing figure (~364 B to ~64 B/row) is
  INFERRED from the schema definitions** (`schema_ddl` vs `schema_ddl_lean`,
  column list only), not measured as an isolated per-row byte count on
  disk (Turso has no `dbstat`, the same limitation Finding 7's own
  index-only-cost isolation already worked around differently, via a
  baseline-delta build this finding did not repeat for the item table
  specifically).

## Source Ledger

| Source | Claim used | Confidence |
|---|---|---|
| `docs/helix/00-discover/external-interface-kernel-contract-draft.md` §1 (record model, lease, rank), §2.3 (`claim`, C4 order), §7.2 (claim-order index, stage storage) | contract shape this spike ports | VERIFIED (read) |
| owner round-4 question 4 (2026-09-23) | the claims-table proposal under test | VERIFIED (read, this task's relayed request) |
| `crates/fireweed-relational/src/schema.rs:37-38,318-326` | today's lease-column shape and `fireweed_lease_bearers` | VERIFIED (read) |
| `crates/fireweed-turso/src/local.rs:3407-3444,3403` | `configure_connection` pragma profile, the "process-wide Passive cap" comment | VERIFIED (read, reused verbatim from `spikes/key-storage`) |
| `crates/fireweed-turso/src/projection.rs:100-176,3987,4039` | `maybe_passive_checkpoint_log_backed`, `LOG_BACKED_CHECKPOINT_TRY_BYTES`, its per-apply-batch call sites | VERIFIED (read, root-caused Finding 0's WAL-growth defect) |
| `spikes/key-storage/src/main.rs` | connection-config template, dbstat-absence precedent, `INDEXED BY` discipline (SPIKE-002 Finding 0/1) | VERIFIED (read, own prior spike) |
| `spikes/claim-scale/src/main.rs` (whole file) | this spike's harness, schema, operations, the `maybe_passive_checkpoint`/`INDEXED BY` fixes, the `do_finish` bulk-by-`claim_id` fix, the `diag`/`bulk_build`/`finding8`/`multi_claim` tooling, and Finding 9's `schema_ddl_lean`/`FIELDS_TABLE_DDL`/`FW_SPIKE_CACHE_SIZE` additions | VERIFIED (own code, own runs) |
| `spikes/claim-scale/results/*.jsonl` | all measured figures in Findings 0-9 | VERIFIED (measured, this run) |
| `spikes/claim-scale/results/finding-wal-growth.txt` | Finding 0's WAL-growth defect repro | VERIFIED (measured, this run) |
| `spikes/claim-scale/results/finding-l2-finish-regression.txt` | Finding 6's finish regression: diagnosis, ablations (cache_size, bulk-update), and the `spike diag` tooling that produced them | VERIFIED (measured, this run) |
| `EXPLAIN QUERY PLAN` output, `spike explain`/`spike diag` subcommands | Finding 0's two wrong-index defects, Finding 5's sorter mechanism, Finding 6's at-real-scale plan verification | VERIFIED (measured, this run) |
| `/proc/<pid>/io` counters | Finding 0's WAL-growth I/O-amplification figures | VERIFIED (measured, this run) |
| `crates/fireweed-turso/src/projection.rs` `run_passive_checkpoint`'s `writer: &Mutex<Connection>` parameter | production's own single-writer-connection pattern, mirrored by `multi_claim`'s "shared" mode | VERIFIED (read) |
| `docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md` Method (transaction/prepared-statement rules, "300-500x slower" quote), Recommendation 1 (layout C's claim-path row shape) and Finding 8 (cache-size ablation, "structurally decoupled" mechanism) | Finding 9's premise (a lean claim-path row) and the methodology-check cross-reference for SPIKE-004's own `do_claim` | VERIFIED (read) |

## Artifacts

- `spikes/claim-scale/` — standalone Cargo project (`Cargo.toml`, `src/main.rs`, `run.sh`), now with `diag`, `bulk_build`, `finding8` and `multi_claim` subcommands alongside the original `run`/`size_baseline`/`explain`, plus Finding 9's `fat|lean` row-layout option on `bulk_build`/`finding8` and the `FW_SPIKE_CACHE_SIZE` ablation override.
- `spikes/claim-scale/results/*.jsonl` — raw per-phase measurements: one file
  per (layout, scale) run, the two size baselines, the 100M-row bulk-build
  (`multi100-l2-build.jsonl`), the 20-queue round-robin
  (`finding8-l2.jsonl`), the concurrency matrix
  (`multi-claim-l2.jsonl`, `multi-claim-l2-large.jsonl`), and Finding 9's
  lean-layout build (`multi100-l2-lean-build.jsonl`) and 20-queue
  round-robin at 128 MiB and 1 GiB (`finding8-l2-lean.jsonl`,
  `finding8-l2-lean-1gib.jsonl`; the discarded, contaminated first attempt
  at 128 MiB is kept as `finding8-l2-lean-contaminated.jsonl` for
  reference, not used in the reported numbers).
- `spikes/claim-scale/results/*.log` — stderr progress logs, one per run above.
- `spikes/claim-scale/results/finding-wal-growth.txt` — the WAL-growth defect's
  repro and `/proc/<pid>/io` evidence, in full.
- `spikes/claim-scale/results/finding-l2-finish-regression.txt` — the finish
  regression's diagnosis, ruled-out causes, ablations and fix, in full.
- `spikes/claim-scale/results/explain-query-plans.txt` — full `EXPLAIN QUERY
  PLAN` output for every hot statement on both layouts (`spike explain`),
  including the two wrong-index defects, the stage-blind sorter, and the
  as-shipped bulk finish statement.
- No production code was modified; no contract types were changed.
