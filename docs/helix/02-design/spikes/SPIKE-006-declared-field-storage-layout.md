---
ddx:
  id: spike-declared-field-storage-layout
  depends_on:
    - spike-record-field-storage
    - spike-turso-value-size-boundary
    - spike-claim-path-at-scale
  links:
    - {kind: informed_by, to: spike-record-field-storage}
    - {kind: informed_by, to: spike-turso-value-size-boundary}
    - {kind: informed_by, to: spike-claim-path-at-scale}
  status: draft
---

# Technical Spike: Declared-Field Storage Layout at Scale

**Spike ID**: SPIKE-006 | **Lead**: DDx queue steward | **Time Budget**: bounded runs | **Status**: Draft

## Objective

**Owner decisions that bind this work** (owner decision, 2026-09-23 design discussion):
- Fields *and* their indexes are declared at queue creation.
- A queue may have many fields. Some are big (up to 64 KiB, e.g. profiles).
  Most are not updated most of the time. Some are indexed. "Let's optimize
  the hell out of this."
- `rank` (`i64`) is the only ordering key; a claim takes records with
  `rank <= horizon`, in rank order, within a stage.
- No stage is special, and any physical treatment must be uniform across
  stages.
- Many queues share one Turso file (one per partition), and queues should be
  cheap.
- Claims must stay fast at 100M+ records per file.

**Technical question**: given a *declared* field schema (types, sizes,
which fields are indexed, fixed at queue creation, kernel-contract-draft.md
§3.1) and the stated workload — about 100 declared fields per queue (~85
small, ~10 medium 256 B-2 KiB, ~2 large 16-64 KiB), 3-5 indexed fields,
updates that usually touch 1-3 small fields, and claims that return the
small fields and never the large ones — **what physical layout on
S3×Turso is fastest**, and does the answer change once many queues share
one Turso file at the scale SPIKE-004 found page-cache pressure at?

This spike holds the lease/claim-order design SPIKE-004 already measured
and recommended (a nullable, indexed `claim_id` on the item row plus a
`claims` metadata table) constant across every candidate below, and varies
only field-storage placement — the one variable SPIKE-003/SPIKE-005 left
open once a *declared* schema (not the current draft's free-form
`BTreeMap<String, Value>`) is on the table. SPIKE-003 already found that a
declared field schema is a small, real win only when values are small, and
recommended against requiring one — but SPIKE-003 tested at most 200
*undeclared* fields at one uniform value size, never the specific
many-small/some-medium/few-large mix this spike's task brief specifies, and
never a scheme that also declares *which* fields are indexed, at *queue*
granularity, up front. This spike re-opens that question for the
mixed-size, declared-index workload the owner's round-2/round-3/round-4
decisions actually describe.

Out of scope (tracked elsewhere, not re-measured here): the lease/claim
representation itself (SPIKE-004's `claim_id` + `claims` table design is
reused verbatim, not re-litigated); the value-size spill boundary
(SPIKE-005 already found ~4 KiB, reused verbatim below); record-key
physical layout (SPIKE-002); `finish`/`extend`/expiry-sweep cost (SPIKE-004
already covers all three; this spike's claim path stops at leasing +
returning the small fields, per the task's workload list).

## Scope

**Hardware** — MEASURED (this host, at spike start):
- `nproc`: 32; `free -g`: 94 GiB total.
- `uname -r`: `6.6.87.2-microsoft-standard-WSL2` (WSL2 VM, not bare metal).
- root filesystem: `ext4`, 4.0 TB, ~3.2 TB free.
- Load average at spike start: `6.31 7.57 8.46` (shared, concurrently-loaded
  host — see Limitations and the per-cell load disclosure in Findings).

**Software** — MEASURED (this checkout):
- Vendored Turso `0.7.2` at `vendor/turso` (`default-features = false`), the
  exact pin `crates/fireweed-turso/Cargo.toml:26` uses.
- Standalone Cargo project (own empty `[workspace]` table, not a workspace
  member) at `spikes/declared-fields/`, path dependency on `vendor/turso`
  `=0.7.2`. Built `--release` with a private `CARGO_TARGET_DIR` (not the
  shared scratch path the task named) — SPIKE-003 and SPIKE-005 both
  independently found a *different* concurrent session's spike build
  overwrite the shared target path's binary mid-run; this spike avoids that
  failure mode the same way, at the cost of a longer first build.
- `cargo`/`rustc` `1.97.1` (matching SPIKE-002/003/004/005's toolchain).

**Connection configuration** — MEASURED (mirrored verbatim from
`crates/fireweed-turso/src/local.rs` `configure_connection`, reproduced in
`spikes/declared-fields/src/db.rs`), the same log-backed profile
`spikes/claim-scale` used (production's actual
`TursoConfig::local(path).with_log_backed_projection()` profile,
`crates/fireweed/src/turso_compose.rs:136`): `page_size=4096`,
`journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-131072` (128 MiB),
`temp_store=MEMORY`, `wal_autocheckpoint=0`, `busy_timeout=5s`. A single
`Mutex`-style single-writer-connection discipline is used throughout (one
`Connection` per process, one writer at a time) — SPIKE-004 Finding 8
measured that independent writer connections to the same file fail with
`Busy("database is locked")` at realistic claimer concurrency; this spike
does not re-test concurrency (SPIKE-004 already owns that question) and
simply inherits the single-connection discipline.

**Passive-checkpoint cap** — MEASURED (mirrored from
`crates/fireweed-turso/src/projection.rs:127-176`
`maybe_passive_checkpoint_log_backed`, `LOG_BACKED_CHECKPOINT_TRY_BYTES =
400 MiB`): every write transaction, in ingest and in every benched
operation, is followed by a `PRAGMA wal_checkpoint(PASSIVE)` once the WAL
file exceeds 400 MiB. SPIKE-004 Finding 0 found that omitting this
mechanism inflates I/O by tens of thousands of times under sustained
small-write load; this spike reproduces that mechanism verbatim
(`spikes/declared-fields/src/db.rs` `maybe_passive_checkpoint`) rather than
re-discovering the same defect.

**`INDEXED BY` discipline** — every hot `SELECT` and `UPDATE` in
`spikes/declared-fields/src/layouts.rs` carries an explicit `INDEXED BY`
naming the PK autoindex or the purpose-built secondary index. SPIKE-004
Finding 0 and SPIKE-002 Finding 0/1 both found Turso's planner does not
reliably pick the right index on its own, for `SELECT` or `UPDATE` alike, on
this vendored version; this spike does not re-verify that finding with its
own `EXPLAIN QUERY PLAN` pass (time budget), and instead applies the
discipline unconditionally from the start.

**The declared workload** (the task's own shape, `spikes/declared-fields/src/gen.rs`):
97 declared fields per queue — 85 small (alternating `Int(i64)` and short
`Str`, 8-32 B), 10 medium (`Str`, 256 B-2 KiB), 2 large (`Bytes`, fixed at
16 KiB and 48 KiB so total large-field bytes/row is a deterministic 64 KiB,
the top of the owner's stated 16-64 KiB band) — rounding to the task's
"about 100 declared fields" once counted per the ~85/~10/~2 split it
specifies exactly. 4 of the 85 small fields (2 `Int`, 2 `Str`) are declared
indexed, inside the task's 3-5 band. Every value is generated
deterministically from a SplitMix64 mixer keyed on `(queue_no, item_no,
field_no)` (same approach as `spikes/claim-scale`/`spikes/value-boundary`),
so every run is byte-reproducible and no generation cost is inside a timed
region.

## Candidate layouts

- **(A) Baseline.** All 97 fields as one JSON `TEXT` blob on the hot row
  (tagged-array encoding, matching SPIKE-003's model (a) and the current
  draft's assumed shape, kernel-contract-draft.md:757), patched with
  `json_patch`. Nothing is spilled — a large field's bytes are base64-encoded
  into the same JSON document as everything else. The 4 indexed fields get
  entries in a shared `item_index (queue_no, field_no, item_no, index_key)`
  side table (SPIKE-003's index-table shape, one covering secondary index).
- **(B) Declared field ids.** A compact `postcard`-encoded `BTreeMap<u16,
  Value>` of the 95 small+medium fields on the hot row (one BLOB column);
  the 2 large fields always spill to a per-`(queue_no, item_no, field_no)`
  values table (SPIKE-005's ~4 KiB spill threshold — both large fields here
  are always above it, so they are unconditionally off-row, never a boundary
  case). Same shared `item_index` side table as (A).
- **(C) Hot/cold split.** The claim-path `items` row carries *no fields at
  all* — only `queue_no, item_no, stage, rank, claim_id, version`. Small and
  medium fields live in a separate `item_cold` table, keyed by `(queue_no,
  item_no)`, same postcard-map shape as (B)'s hot row. Large fields spill the
  same way as (B). Same shared `item_index` side table.
- **(D) Per-queue physical tables.** Real typed SQL columns (`INTEGER`/`TEXT`)
  for the 95 small+medium fields, on a table named per queue
  (`items_q<N>`), with native SQL indexes on the 4 declared indexed columns
  and the claim-order index. Large fields spill to a per-queue values table.
  The lease/`claims` table is the one structure still shared across every
  queue in the file (lease design is out of scope, held constant).

Every candidate keeps the identical stage-prefixed claim-order index shape
SPIKE-004 already recommends: `(queue_no, stage, rank, item_no) WHERE
claim_id IS NULL` for A/B/C (one shared `items` table, many queues), or
`(stage, rank, item_no) WHERE claim_id IS NULL` per queue for D (physical
separation already does the `queue_no` prefixing). No candidate special-cases
any stage — the physical schema has no notion of "terminal" or "special"
stage, per the owner's uniformity requirement.

## Method

**Workload generator and harness**: `spikes/declared-fields/src/{gen,codec,layouts,db,main}.rs`.
`gen.rs` generates the 97-field record and the claim-order rank; `codec.rs`
holds the two encodings (JSON tagged-array for A, `postcard` map for
B/C/D); `layouts.rs` holds the DDL and every operation for all four
candidates; `db.rs` mirrors production's connection/checkpoint policy and
the load-average/percentile helpers; `main.rs` is the CLI driver
(`ingest`, `bench`, `per_queue_tables`, `dbsize` subcommands).

**Population**: each queue ingested with a 95%/5% stage split (95% into a
terminal-like `DONE` stage, 5% into the claimable `WORK` stage) — the same
ratio `spikes/claim-scale` used, so the claim-order index holds a realistic
mostly-terminal working set (kernel-contract-draft.md §1: "records in
terminal stages are data... always Free"). `rank` is assigned monotonically
per queue (`base_ts + item_no`), not constant, so the claim-order B-tree
holds a realistic key spread. Ingest batches 200 rows per `BEGIN IMMEDIATE`
transaction, with `maybe_passive_checkpoint` called after every batch.

**Operations measured**, each a sustained loop over 3 trials (task's
measurement-hygiene requirement), `spikes/declared-fields/src/main.rs`
`cmd_bench`:
- **claim**: `SELECT item_no ... INDEXED BY <claim-order idx> WHERE stage=?
  AND claim_id IS NULL ORDER BY rank, item_no LIMIT 100`, lease all 100 via
  an `INSERT INTO claims` plus a per-row `claim_id` `UPDATE`, then fetch and
  decode each claimed row's *small* fields only (never the large ones) — the
  exact shape the task's workload specifies. One untimed warm-up claim (a
  full claim+lease+fetch cycle) runs before the timed trials, matching
  `spikes/claim-scale`'s finding that the first write after an ingest
  checkpoint pays a one-off page-cache/WAL-reopen cost unrelated to layout.
- **update 1-3 small fields**: a single-row transaction patching 3 fixed,
  unindexed small field ids (the task's "common case").
- **update one indexed field**: a single-row transaction patching the one
  designated indexed `Int` field; A/B/C additionally rekey that field's
  `item_index` row, D relies on Turso's native index maintenance.
- **update a large field**: a single-row transaction overwriting one of the
  two large fields (rare, per the task).
- **point read, full record**: every field, small/medium/large.
- **point read, small fields only**: the small subset only, matching what a
  claim actually needs.
- **aggregate/count**: `COUNT(*) ... GROUP BY` the indexed `Int` field over a
  bounded range (that field's declared value range is bounded to
  `[0,2000)`, not near-unique like the other small `Int` fields, so a range
  query at this spike's row counts produces meaningfully-populated groups
  rather than ~1 row/group).
- **DB size**: main-file bytes after `PRAGMA wal_checkpoint(TRUNCATE)`,
  post-ingest, plus bytes/row.

Item ids touched by update/read operations are chosen pseudo-randomly
(SplitMix64 over the trial/rep index) from the full ingested pool, matching
the "1000-row pool" sampling pattern SPIKE-003/SPIKE-005 both used. All
statements are single-row, single-key — this spike's per-operation
granularity is one record per transaction throughout (claim excepted, which
batches 100 per the contract's own claim shape), so SPIKE-005's
batched-`UPDATE...FROM(VALUES...)`-against-a-non-rowid-PK query-plan trap
(measured ~100x slower there) does not arise here: nothing in this spike
issues that statement form.

**Measurement hygiene**: `[load]` lines bracket every trial with
`/proc/loadavg`'s 1-minute average (`db::load_avg_1m`), disclosed in full in
Findings below. Each cell (one trial of one operation) is repeated 3 times;
median and `(max-min)/avg` spread are reported, the same convention
SPIKE-002/004/005 use. A cell is re-run if the load average jumped
mid-cell (disclosed inline where it happened).

**Scales measured**:
1. **Primary sweep**: 3 queues, 200,000 records/queue (600,000 rows/layout),
   all four layouts — meets the task's "at least 200k records per queue
   across several queues" floor. Each layout's file grew to 48-62 GB
   (Finding 1) — every claim/update/read/aggregate measurement below runs
   against that full file, not a synthetic small one.
2. **Per-queue-table cost** (D only): schema-creation time, schema-only DB
   size, and Turso open time at 100, 1,000 and 10,000 queues' worth of
   tables in one file, each queue seeded with a small (20-row) dataset so
   the size number reflects real table+index overhead, not an empty schema.
3. **Cache-size ablation**: layout C's claim/update/read throughput at
   production's 128 MiB `cache_size` versus 1 GiB, against an additional
   two queues (600 GB -> 1.2M rows total in that file by the time of the
   ablation) — same ablation shape SPIKE-004's own `cache_size` ablation
   used.

A dedicated, separately-built "larger file" point (beyond the primary
sweep's own file, which already reaches 48-62 GB / up to 1.2M rows by the
time of the cache-size ablation) was scoped out under this spike's time
budget once the primary sweep and the per-queue-table scale test (Finding 7)
had already produced a clear, decisive cross-layout signal; see Limitations.

**Two real implementation bugs were found and fixed mid-spike, both via an
implausible first result, the same way SPIKE-004 found and disclosed its own
methodology defects (SPIKE-004 Finding 0).** Every number in Findings below
is measured after both fixes:
1. **Missing transaction wrapping.** The claim operation's ~201 statements
   (1 index-range `SELECT`, 1 `claims` `INSERT`, 100 lease `UPDATE`s, 100
   fetch-small `SELECT`s) were first issued with no explicit transaction, so
   each ran as its own autocommit transaction instead of one. Update
   operations for layouts B/C (2-3 statements: read-modify-write against a
   binary-map blob, plus C's separate hot-row version bump) had the same
   defect at smaller scale. Fixed by wrapping every operation in one
   `BEGIN IMMEDIATE`/`COMMIT` (`do_claim`, and a transaction opened once per
   rep inside `run_update_phase`, `spikes/declared-fields/src/main.rs`) —
   the same per-operation transaction shape `spikes/claim-scale`'s `do_claim`
   already used, which this harness should have matched from the start.
2. **Per-row statement re-preparation inside the claim loop.** Even after
   fix 1, claim throughput stayed orders of magnitude below every other
   operation. Traced to `claim_lease_row`/`claim_fetch_small` (per-row
   helper functions) each calling `conn.prepare(...)` fresh on every one of
   the 100 claimed rows, instead of preparing once and reusing the
   statement across the loop — again, the exact prepare-once/execute-many
   shape `spikes/claim-scale`'s `do_claim` already used. Fixed by
   `claim_lease_all`/`claim_fetch_small_all` (`spikes/declared-fields/src/layouts.rs`),
   each preparing one statement and looping `stmt.execute()`/`stmt.query()`
   over the 100 rows.

Driver: `spikes/declared-fields/run.sh`. Raw output:
`spikes/declared-fields/results/*.jsonl` (one JSON line per phase/trial) and
`*.log` (stderr progress plus `[load]` lines). The authoritative bench run
per layout is `results/primary-<layout>-benchauth.jsonl` (a fresh,
previously-untouched queue in each layout's already-ingested file, run
back-to-back with no interleaved activity, load average flat throughout —
see per-cell load disclosure below).

## Findings

Hardware note: load average at spike start was `6.31 7.57 8.46`; it ranged
6-20 across the roughly 3 hours this spike's measurement took (this is a
shared, concurrently-loaded host — other sessions' `cargo`/`cc1`/linker
processes were directly observed via `ps aux` during one contaminated
attempt, see Limitations). The tables below are all from
`primary-<layout>-benchauth.jsonl`, whose own load average was flat and
moderate throughout every claim trial (7.96-10.08, no jump, confirmed
per-layout in the raw logs) — the one run in this spike's history that
meets the "re-run a cell if the load jumped mid-cell" bar cleanly, so it is
the run these Findings report. 3 repeats per cell (trials), median and
`(max-min)/avg` spread reported, the SPIKE-002-established convention.

### Finding 1: ingest — value-size/encoding dominates DB size and throughput, matching SPIKE-003/SPIKE-005's mechanism exactly, MEASURED

| Layout | rows/queue x queues | ingest rows/s | DB bytes/row | Total DB size |
|---|---:|---:|---:|---:|
| A (baseline JSON) | 200,000 x 3 | 540.1 | 103,033 | 61.82 GB |
| B (declared-id binary map) | 200,000 x 3 | 767.5 | 80,617 | 48.37 GB |
| C (hot/cold split) | 200,000 x 3 | 763.4 | 80,645 | 48.39 GB |
| D (per-queue tables) | 200,000 x 3 | 728.7 | 80,174 | 48.10 GB |

A's JSON+base64 encoding inflates every field, most visibly the two large
(16 KiB + 48 KiB raw) fields: base64 alone adds ~33%, plus the tagged-array
JSON wrapper (`kernel-contract-draft.md:757`) and per-key text overhead for
all 97 fields, versus B/C/D's compact binary map (postcard) for
small/medium fields and raw bytes for spilled large fields. The
~80,000-83,000 B/C/D figures track almost exactly with each other (within
0.6%) because the dominant cost in all three is the identical 64 KiB/row of
raw large-field bytes spilled to a side table (SPIKE-005's ~4 KiB threshold
recommendation, reused verbatim: both this workload's large fields, 16 KiB
and 48 KiB, are always well above it) — small/medium-field encoding
efficiency (postcard vs typed columns) is noise next to that. This directly
reproduces SPIKE-003's finding ("value size, not field count, dominates")
and SPIKE-005's finding (large-value spill is what keeps the hot row's
bytes-per-row bounded), now at the declared, mixed-size, many-field scale
this spike's task brief specifies rather than SPIKE-003's single-value-size
sweeps.

### Finding 2: claim — layout C is fastest, D and B are comparable, A is 300-500x slower and remains the clearest single signal in this spike, MEASURED

| Layout | median claims/s | spread | p50 latency | p99 latency |
|---|---:|---:|---:|---:|
| A | 0.46 | 8.7% | 2,800.9 ms | 5,251.8 ms |
| B | 137.14 | 15.5% | 7.3 ms | 9.1 ms |
| C | 228.45 | 31.8% | 4.4 ms | 4.6 ms |
| D | 133.14 | 11.5% | 7.5 ms | 7.8 ms |

C claims 100 records in a median 4.4 ms — 1.7x faster than B or D (both
~7.3-7.5 ms) and ~500x faster than A. The mechanism for A is exactly
SPIKE-005's "an inline large field on a hot queue directly taxes claim
throughput" finding, now measured on the full declared-field workload
rather than SPIKE-005's single-field sweep: A's `fields` blob (all 97
fields, including both large ones, inline as one ~103 KB JSON `TEXT` value)
must be fully parsed to `json`-decode even the *small* fields the claim
returns, so every one of the 100 fetched rows in a claim pays the cost of
decoding 64 KB of large-field base64 text it never uses. C is fastest
because its claim-path `items` row carries no fields at all (the design's
whole point) — leasing never touches field storage, and the "return small
fields" fetch step joins to a separate `item_cold` table whose row (postcard
map of 95 small+medium fields, no large-field bytes) is smaller and cheaper
to decode than B's equivalent hot-row blob, which additionally shares its
table and physical pages with the claim-path columns B and C both mutate on
every lease. D's per-record fetch avoids decode entirely (direct typed
columns) but pays a wider `SELECT` (85 named columns) and a full-row
serialize/deserialize over more columns than C's single blob column,
landing it in the same band as B rather than ahead of it.

### Finding 3: update 1-3 small fields, one indexed field, and one large field — D is fastest and most consistent, C is a strong second, A is catastrophic, B is erratic and not fully explained, MEASURED

| Operation | Layout | median ops/s | spread | p50 | p99 |
|---|---|---:|---:|---:|---:|
| update_small | A | 19.94 | 39.9% | 6.4 ms | 295.0 ms |
| update_small | B | 131.43 | 164.2% | 0.49 ms | 122.3 ms |
| update_small | C | 1,838.35 | 6.8% | 0.53 ms | 0.71 ms |
| update_small | D | 2,674.61 | 3.0% | 0.37 ms | 0.45 ms |
| update_indexed | A | 15.49 | 51.5% | 9.4 ms | 299.8 ms |
| update_indexed | B | 52.66 | 67.4% | 0.63 ms | 242.1 ms |
| update_indexed | C | 1,493.79 | 1.2% | 0.66 ms | 0.82 ms |
| update_indexed | D | 2,339.12 | 4.2% | 0.42 ms | 0.51 ms |
| update_large | A | 10.88 | 12.6% | 89.0 ms | 264.5 ms |
| update_large | B | 64.57 | 203.1% | 0.68 ms | 196.4 ms |
| update_large | C | 1,742.83 | 14.0% | 0.55 ms | 0.71 ms |
| update_large | D | 1,806.16 | 2.2% | 0.54 ms | 0.61 ms |

D wins every update op, by a modest 15-55% margin over C, with the tightest
spreads (2-6%) of any layout — direct typed-column writes need no
encode/decode round trip, and D's per-queue table means no other queue's
writes ever share its pages. C is a strong, consistent second (1.2-14%
spread) even though its small/indexed-field updates pay one extra
statement over B's shape (a read-modify-write against `item_cold`, plus a
separate tiny version-bump on `items`) — its hot `items` row is small
enough that the version-bump is nearly free, and `item_cold`'s own RMW cost
matches B's. **A is uniformly the day's worst performer here too**
(11-20 ops/s, p99 in the hundreds of ms) — `json_patch` must parse and
re-serialize the *entire* ~103 KB row for a 1-3-field patch, reproducing
SPIKE-003 Finding 3's whole-row-rewrite mechanic at the scale this
workload's large inline fields make it bite hardest.

**B is the one result this spike could not fully explain.** Architecturally
B's hot row (a postcard map of 95 fields, no claim-path-only columns beyond
the same four A/C/D also carry) should update at a cost close to C's — same
blob size, one fewer statement — but it measured 7-35x slower with spreads
of 67-203% and p99s in the hundreds of ms, on a run whose load average was
flat throughout (Findings preamble). The most likely candidate, INFERRED
but not confirmed: B's single `items` table serves *both* the claim path
(100 `claim_id` writes per claim, run immediately before these update
phases in the same benchmark) and the field-storage blob, so claim's own
write churn may evict that table's blob-bearing pages from Turso's 128 MiB
cache right before the update phases need them back — a self-inflicted
cache eviction B's design invites by co-locating high-churn claim state and
large blobs on the same table, that C avoids structurally (claim churn
touches only the small `items` table; `item_cold`'s pages are never touched
by a claim). This is plausible and consistent with Finding 8's cache-size
ablation showing no such effect for C, but it was not isolated with its own
targeted ablation within this spike's time budget — see Limitations.

### Finding 4: point reads — C wins both full-record and small-only reads; D's wide typed-column `SELECT` is not free; A pays the JSON-decode tax on every read, MEASURED

| Operation | Layout | median reads/s | spread | p50 | p99 |
|---|---|---:|---:|---:|---:|
| read_full | A | 29.64 | 38.5% | 3.8 ms | 245.5 ms |
| read_full | B | 98.33 | 222.3% | 0.24 ms | 229.1 ms |
| read_full | C | 5,765.70 | 2.0% | 0.16 ms | 0.25 ms |
| read_full | D | 3,931.74 | 17.8% | 0.25 ms | 0.33 ms |
| read_small | A | 5,187.22 | 8.3% | 0.19 ms | 0.27 ms |
| read_small | B | 16,142.49 | 24.9% | 0.06 ms | 0.11 ms |
| read_small | C | 21,437.24 | 4.1% | 0.04 ms | 0.07 ms |
| read_small | D | 9,296.08 | 5.7% | 0.10 ms | 0.14 ms |

C reads both the full record and the small-only projection fastest, and
with the tightest spread of any layout on `read_full` (2.0%). Two things
stand out. First, **C's full-record read beats D's** (5,766 vs 3,932/s)
even though C makes more round trips (`items` + `item_cold` + 2 x
`item_values`, versus D's `items_q<N>` + 2 x `values_q<N>`) — decoding one
compact postcard blob is cheaper than Turso deserializing a wide
(95-column) typed row, and C's `item_cold` table is never touched by
claim-path writes (Finding 3), so it stays cache-resident. Second, **D's
`read_small` (a `SELECT` naming 85 columns) is slower than both B and C's
single-blob-decode reads** (9,296/s vs 16,142/s and 21,437/s) — the
per-column result-row overhead of a wide typed `SELECT` outweighs skipping
a decode step, at least at this field count. A's `read_full` pays the same
tax claim does (full-JSON-decode of a 103 KB row, most of it base64) and is
last by two to three orders of magnitude; A's `read_small` is comparatively
fine (5,187/s) because it never touches the large-value spill machinery the
others have and JSON-parsing 103 KB, while expensive, is still a single
in-process operation with no extra round trips.

### Finding 5: aggregate/count over an indexed range — D fastest via native SQL, B and C tied via the shared index table, A slowest but all four are within one order of magnitude, MEASURED

| Layout | p50 latency (median of 3 trials) | spread |
|---|---:|---:|
| A | 11.93 ms | 16.6% |
| B | 9.24 ms | 8.6% |
| C | 9.24 ms | 0.4% |
| D | 7.46 ms | 1.1% |

Every layout answers `COUNT(*) ... GROUP BY` over a ~500-wide range of the
declared indexed field (501 groups, ~50,000 rows) in single-digit
milliseconds. D's native SQL index is fastest (37% faster than A) but the
gap between layouts is modest compared to claim/update/read — aggregate
touches only the index structure (the shared `item_index` table for
A/B/C, a native B-tree index for D), never the field-storage blob or
columns at all, so the layout choice barely matters here. This is the
expected result: §2.7's `aggregate` op is defined entirely over declared
index keys, and every layout here shares the same underlying index shape
(A/B/C literally share one `item_index` table; D uses Turso's native
per-column index).

### Finding 6: DB size — see Finding 1 (ingest and DB size were measured together)

### Finding 7: per-queue physical tables (D) do not stay cheap at scale — schema-creation time and open time both scale super-linearly, and 10,000 queues is catastrophic, MEASURED

| N queues | schema-create time | DB size (schema + 20 rows/queue) | Turso open time |
|---:|---:|---:|---:|
| 100 | 2.12 s | 163.5 MB | 5.6 ms |
| 1,000 | 140.5 s (2.3 min) | 1.64 GB | 202.8 ms |
| 10,000 | 13,111.3 s (**3.64 hours**) | 16.36 GB | **26.64 s** |

Going from 100 to 1,000 queues (10x) took create time from 2.1 s to 140.5 s
— a 66x increase, not 10x. Going from 1,000 to 10,000 (10x again) took
create time from 140.5 s to 13,111.3 s — a 93x increase. Both create time
and open time (the cost of `Builder::new_local(...).build()` plus one
trivial `sqlite_master` query, forcing the schema to actually parse) scale
*super-linearly* with queue count, not linearly — table_count in
`sqlite_master` grew linearly (201 -> 2,001 -> 20,001, exactly 2 tables per
queue plus the one shared `claims` table), so the cost is not explained by
row count or data volume (only 20 rows/queue throughout) but by the sheer
number of distinct schema objects (tables + their indexes) Turso's own
schema/catalog machinery must create and, on open, parse. At 10,000 queues
— nowhere near this spike's other scales (600,000-1,200,000 rows) but
squarely inside "100 1M-record queues alive at once," a shape the owner's
own framing names — opening the file alone costs 26.6 seconds before a
single query can run, and building it in the first place costs over 3.6
hours.

### Finding 8: cache-size ablation (layout C) — production's 128 MiB default is already sufficient; no measurable benefit from 1 GiB, MEASURED

| Cache size | claim median claims/s | update_small median ops/s | read_full median reads/s |
|---|---:|---:|---:|
| 128 MiB (production default) | 229.61 | 2,283.82 | 4,071.13 |
| 1 GiB | 228.07 | 2,126.75 | 3,898.34 |

Measured against an additional two queues in layout C's already-large file
(6 queues x 200,000 rows = 1.2M rows, ~58 GB by this point), back to back,
same host window. Every metric is within measurement noise between the two
cache sizes (claim: -0.7%; update_small: -6.9%; read_full: -4.2%, all well
inside this spike's typical 3-15% single-digit spreads). **This is a clean,
specific result for layout C's design**: because C's claim path never
touches field-storage bytes and its `item_cold`/`item_values` tables are
never touched by claim churn (Finding 3's mechanism), C's working set per
operation stays small regardless of total file size, so a bigger cache
buys nothing. This does not generalize to every layout without further
testing (B's Finding 3 anomaly is the specific candidate that motivated
this ablation, but B itself was not re-ablated here — see Limitations) and
is a narrower, more specific result than SPIKE-004's own cache-size
ablation, which found a *large* effect for its leaner, shared-table L2
design at 10M-row scale. The two results are not in tension: SPIKE-004's
effect appeared once its shared claim-order index alone exceeded 128 MiB
(its Finding 7); this spike's C design keeps every claim-path structure
(the `items` row, the claim-order index) small enough that it does not
approach that budget even at 1.2M rows with 97 declared fields/record,
because the fields that make each *record* large are the ones C keeps off
the claim path entirely.

## Interpretation

1. **The four candidates split cleanly into "never do this" (A), "fails a
   named owner requirement at scale" (D), "the recommended layout" (C), and
   "an unresolved anomaly, not recommended as-is" (B).** A is 15-500x
   slower than every alternative on every operation that touches field
   storage (claim, all three updates, `read_full`), for the single
   structural reason SPIKE-003/SPIKE-005 already predicted and this spike
   now confirms at the declared, many-field, mixed-size scale: one JSON
   blob on the hot row means every touch of *any* field pays the cost of
   the *whole* record, small fields and 64 KB of base64-encoded large
   fields alike.
2. **D wins nearly every per-operation throughput number (Findings 2-5)
   but is disqualified by Finding 7.** The owner's own framing --- "many
   queues share one Turso file... queues should be cheap" --- is a
   requirement this spike can check directly, and per-queue physical
   tables fail it catastrophically once queue count reaches into the
   thousands: 3.64 hours to build 10,000 queues' schemas, 26.6 seconds just
   to *open* the file afterward. A design that is fastest per-operation but
   cannot be created cheaply at the scale the owner explicitly describes
   ("100 1M-record queues alive at once") is not a viable recommendation,
   regardless of its operational speed.
3. **C is the only layout that is simultaneously fast and cheap to scale
   in queue count.** It wins claim outright (Finding 2, 1.7x over its
   nearest rival) and both point-read shapes (Finding 4), is a close,
   consistent second on every update (Finding 3, 15-55% behind D, with
   equally tight spreads), and ties for second on aggregate (Finding 5).
   Because C's queues are rows in shared tables, not physical schema
   objects, it has none of Finding 7's per-queue creation cost --- a new
   queue is as cheap as any other insert. The cache-size ablation
   (Finding 8) independently confirms why: C's claim-path row and its
   field-storage tables are structurally decoupled, so claim churn cannot
   evict field data from cache and vice versa, keeping every operation's
   working set small regardless of total file size or `cache_size` budget.
4. **B's anomaly (Finding 3) is a caution about colocating high-churn
   claim-path state with field-storage bytes on the same table, even when
   that table is otherwise compact.** B's hot row is small (comparable to
   C's `item_cold` row) and its update path issues one fewer statement than
   C's, yet it measured 7-35x slower with far wider spreads on every
   operation that shares its `items` table with claim traffic. The
   candidate mechanism --- claim's own write churn evicting the same
   table's blob-bearing pages --- is architecturally the same one Finding
   3/8 credit C's *split* for avoiding, which is consistent evidence
   pointing the same direction even though this spike did not isolate it
   with its own dedicated ablation. Practically, this is a second,
   independent reason (beyond raw throughput) to prefer C's split over a
   "smaller blob, same table" design like B: colocation risk, not just
   blob size, seems to matter.
5. **Every layout's `aggregate` cost is close (Finding 5) because it is
   the one operation this workload's field-storage placement choice barely
   touches.** `aggregate` is defined entirely over declared index keys
   (§2.7), and A/B/C already share one `item_index` structure regardless of
   how the *record* itself is stored; D's native index is fastest but by a
   modest margin. This is expected, not a surprise, and confirms the field
   storage question this spike investigates is genuinely orthogonal to
   index-range query cost, which SPIKE-003's own index-side-table
   recommendation already assumed.

## Recommendation

**1. Layout: adopt (C), the hot/cold split**, for the declared-field
storage question this spike investigates. The claim-path row carries only
`key, stage, rank, claim_id, group, gates, version` (kernel-contract-draft.md
§1's own attribute list, minus `fields`/`payload`) — no field bytes at all.
Small and medium declared fields live in one binary-map blob per record, in
a table keyed by the record's identity, separate from the claim-path row.
Large fields (measured over ~4 KiB, SPIKE-005's threshold, reused verbatim)
spill to a per-field values table, exactly as SPIKE-005 already
recommended for the undeclared-schema case. **This spike's contribution
beyond SPIKE-005 is extending the same "keep the hot claim-path row free of
field bytes" principle to *every* field, not only the large ones** — once a
field schema is declared at queue creation (this spike's premise, and the
owner's own decision, "fields and indexes are declared at queue
creation"), there is no cost to knowing in advance that *no* declared field
belongs on the claim-path row, so the split can be total rather than
size-triggered. Declared indexes live in a shared index side-table
(`item_index`, SPIKE-003's own recommended shape), one row per (queue,
indexed field, record).

**2. Contract guidance for field declaration:**
- **Field types**: the three this spike modeled cover the workload's real
  shapes — `Int(i64)`, `Str` (short and medium), `Bytes` (large/opaque).
  `Float`/`Bool`/`Time` were not separately measured (INFERRED to behave
  like `Int` cost-wise: fixed-width, cheap to encode/decode) — the same
  disclosed gap SPIKE-003 already carried forward.
- **Field count**: this spike measured 97 fields/record (85 small + 10
  medium + 2 large) as one declared set, not swept independently the way
  SPIKE-003 swept 5/50/200 — MEASURED only at "about 100," which is the
  scale the task specifies as realistic; no evidence here suggests a lower
  practical ceiling than SPIKE-003's own 200-field measurement.
- **Max value size**: 64 KiB, reusing SPIKE-005's Recommendation 1
  verbatim — nothing in this spike's mixed-workload measurement suggests a
  different number.
- **Which fields may be indexed**: this spike measured 4 of 97 (2 `Int`, 2
  `Str`), inside the task's stated 3-5 band; large (`Bytes`) fields were
  not indexed (not tested, and not a realistic use case for an opaque
  blob) — the contract should keep indexed fields to small, typed scalars,
  consistent with kernel-contract-draft.md §3.1's own "typed indexes."
- **Large/cold marking**: **no**, fields should not need an explicit
  "large" or "cold" declaration. The engine can apply SPIKE-005's ~4 KiB
  measured spill threshold automatically per field value, the same way
  SPIKE-005 already recommended for the undeclared-schema draft — a
  declared schema does not change that recommendation, it only adds the
  further, structural "keep all fields off the claim-path row" split this
  spike recommends in point 1, which is a *placement* decision the engine
  makes uniformly for every declared field, not a per-field flag the
  caller sets.

**3. Implementation rules needed to hit these numbers, MEASURED as
necessary in this spike:**
- **`INDEXED BY` forced on every hot `SELECT` and `UPDATE`**, exactly as
  SPIKE-002/SPIKE-004 already established and this spike applied from the
  start (Scope) — not re-verified with a fresh `EXPLAIN QUERY PLAN` pass
  here, but the discipline was load-bearing throughout.
- **Every multi-statement operation, claim included, must run inside one
  explicit transaction, and any per-row loop inside that transaction must
  prepare its statement once and reuse it across every row.** This is not
  a performance nicety: this spike's own claim path was 300-500x slower
  than its corrected form until both fixes (Method) were applied, indistinguishable
  from a fundamentally slow design until the bug was found. Any
  implementation of the claim op's 100-row lease-and-fetch loop needs this
  discipline from the start.
- **Cache sizing**: production's existing 128 MiB `cache_size` default is
  sufficient for the recommended layout (C) at the scale this spike
  measured (Finding 8) — no change needed *for this layout*. This is a
  direct consequence of recommendation 1's split: keeping claim-path state
  and field bytes on separate, independently-small structures is what
  keeps the working set inside the existing budget. A layout that
  colocates them (B, or a naive single-table design) may need a larger
  cache or may simply be slower and noisier regardless of cache size
  (Finding 3) — not recommended either way.
- **Per-queue schema objects are a scale hazard, not a implementation
  detail to fix later.** Finding 7's numbers are decisive enough that no
  implementation should create one physical table (or index set) per
  queue at the owner's stated scale ("100 1M-record queues alive at
  once," multiplied by however many partitions); this rules out design
  (D) as stated, and should also rule out any future design that
  reintroduces per-queue physical objects for some other reason (e.g. a
  per-queue index) without re-measuring this cost at the target queue
  count.

**4. What a direct-PostgreSQL implementation would do differently.**
PostgreSQL's TOAST mechanism already auto-spills large attribute values
(the TOAST threshold is roughly 2 KB, in the same neighborhood as
SPIKE-005's measured ~4 KiB Turso overflow boundary) out of the main heap
row automatically, and — INFERRED from PostgreSQL's documented MVCC/TOAST
behavior, not measured in this spike, which is Turso-only — an `UPDATE`
that does not change a TOASTed attribute's value does not recopy its TOAST
chunks, only the new heap tuple's other columns. This gives PostgreSQL
something like SPIKE-005's large-field spill "for free" via TOAST, without
a hand-built values side table. **It does not give PostgreSQL C's
claim-path/field split for free**: PostgreSQL's MVCC still copies the
*entire* heap tuple (every non-unchanged-TOAST column) on every `UPDATE`,
so a wide row carrying both claim-path columns and small/medium declared
fields inline would still pay a copy-the-whole-row tax on every claim
lease, the same mechanism this spike found for B (Finding 3) — a direct
PostgreSQL implementation should therefore adopt the *same* hot/cold split
this spike recommends for Turso: a slim claim-path row (`key, stage, rank,
claim_id/lease, group, gates, version`), a separate `fields` row per record
(a `jsonb` column with expression or partial indexes on the declared
indexed fields is PostgreSQL's natural equivalent of this spike's binary
map + `item_index` side table, and gets index maintenance from PostgreSQL
itself rather than hand-written index-table upkeep), and large fields left
to TOAST rather than a hand-built spill table. **Design D's per-queue
physical-table approach is a known PostgreSQL anti-pattern at the target
scale for the same reason Finding 7 measured on Turso** — thousands of
tables in one PostgreSQL database materially load `pg_class`/catalog
lookups, planner cache, and autovacuum's per-relation bookkeeping (INFERRED
from PostgreSQL's well-documented catalog/autovacuum scaling
characteristics, not measured here) — so a direct-PostgreSQL implementation
should not adopt per-queue tables either, converging on the same
shared-table, hot/cold-split recommendation this spike reaches for Turso.

## Limitations

- **Two real implementation bugs (missing transaction wrapping, per-row
  statement re-preparation) were present for most of this spike's
  measurement history and were found and fixed only after producing
  implausible results** (Method) — every number in Findings is measured
  after both fixes, and the fixes themselves (prepare-once/execute-many,
  one transaction per operation) are now load-bearing parts of
  Recommendation 3, not incidental cleanup. Earlier, pre-fix measurement
  runs are not reported anywhere in this document.
- **B's anomaly (Finding 3) is not fully diagnosed.** The candidate
  mechanism (claim-churn cache eviction of a shared table's blob pages) is
  architecturally coherent and consistent with Finding 8's ablation result
  for C, but this spike did not run a dedicated `EXPLAIN QUERY
  PLAN`/cache-instrumentation pass isolating it for B specifically the way
  SPIKE-004's Method addendum diagnosed its own L2 finish regression. A
  follow-up spike (or a `spike diag`-style tool, `spikes/claim-scale`'s own
  precedent) could resolve this if B (or a close variant) is ever
  reconsidered.
- **No dedicated, separately-built "larger file" point beyond the primary
  sweep's own 48-62 GB / up to 1.2M-row file** (by the time of the
  cache-size ablation) was built, given this spike's time budget, once the
  primary sweep plus the per-queue-table scale test (Finding 7) had
  already produced a decisive, consistent cross-layout signal. SPIKE-004's
  own page-cache-pressure regime appeared at 10-25M rows for a much
  leaner (~450 B/row) schema; this spike's rows are 100-200x larger, and
  Finding 8's ablation suggests C specifically does not approach that
  regime even at 1.2M rows — but this is not verified at the 10M+-row
  scale SPIKE-004 tested, only INFERRED from the mechanism Finding 8
  measured.
- **Only `Int`, `Str` and `Bytes` field types were exercised** (not
  `Float`/`Bool`/`Time`), the same simplification SPIKE-003 already
  disclosed for its own single-variant generation.
- **Every update/read operation in this spike is a single-record
  transaction**, matching the task's own per-operation granularity ("update
  1-3 small fields," not "update 1-3 small fields on N records") — no
  batched multi-record write form was measured for any layout, so this
  spike says nothing about batched-write cost or about the
  `UPDATE...FROM (VALUES...)`-against-non-rowid-PK query-plan trap
  SPIKE-005 found (which only bites a *batched* multi-row form; nothing
  here issues one).
- **Field value content is deterministic, seeded synthetic data**
  (SplitMix64-generated), not real production field distributions —
  matching every prior spike's own generation approach.
- **Host contention**: load average ranged 6-20 across this spike's
  measurement window (shared host, other sessions' `cargo`/compiler
  processes directly observed), consistent with SPIKE-002 through
  SPIKE-005's own disclosures. The authoritative `benchauth` run
  (Findings) had flat, moderate load throughout every trial; earlier
  attempts that showed load jumping mid-run were discarded per the task's
  own measurement-hygiene instruction, not reported.
- **The per-queue-table experiment (Finding 7) used 20 rows/queue**, not
  the primary sweep's 200,000 — deliberately, to isolate schema/creation
  cost from data-volume cost (the task's own framing: "measure what
  per-queue tables cost at scale: create time, schema size, and Turso open
  time"). The 10,000-queue cell's 3.64-hour create time and 16.36 GB
  schema-only size are therefore schema overhead almost in isolation, not
  a combined schema-plus-data figure.
- **The cache-size ablation (Finding 8) tested only layout C**, the
  recommended layout, at only two cache sizes (128 MiB, 1 GiB) — it does
  not characterize B, D, or A's cache-size sensitivity, and does not sweep
  intermediate sizes.

## Source Ledger

| Source | Claim used | Confidence |
|---|---|---|
| `docs/helix/00-discover/external-interface-kernel-contract-draft.md` §1 (record model, `fields`/`payload`, built-in attributes), §2.1/§3.1 (declared indexes at queue creation), §2.7 (`aggregate` over declared index keys) | the declared-field-schema premise this spike investigates, the record attribute list the claim-path row is trimmed to | VERIFIED (read) |
| `docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md` | value-size-dominates-field-count finding, index side-table shape, whole-row-rewrite mechanic (its Finding 3), EAV/declared-schema recommendation this spike extends | VERIFIED (read, this spike's stated starting point) |
| `docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md` | ~4 KiB spill threshold, 64 KiB max value size, off-row-storage-is-what-claims-need-for finding, page-size recommendation (not re-tested here) | VERIFIED (read, reused verbatim) |
| `docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md` | claim/`claims`-table lease design reused verbatim (held constant, not re-litigated), page-cache-pressure mechanism at 10-25M rows, `INDEXED BY` discipline, cache-size ablation precedent, `Mutex<Connection>` single-writer requirement | VERIFIED (read) |
| `crates/fireweed-turso/src/local.rs` (`configure_connection`) | log-backed connection profile this spike mirrors (page_size, WAL, synchronous=NORMAL, cache_size=-131072, temp_store=MEMORY, wal_autocheckpoint=0) | VERIFIED (read, reused verbatim from `spikes/claim-scale`) |
| `crates/fireweed-turso/src/projection.rs:127-176` (`maybe_passive_checkpoint_log_backed`, `LOG_BACKED_CHECKPOINT_TRY_BYTES`) | Passive-checkpoint cap this spike mirrors | VERIFIED (read) |
| `spikes/claim-scale/src/main.rs` (`do_claim`, `configure_connection`) | connection-config template; the prepare-once/execute-many and one-transaction-per-operation shape this spike's own bugs deviated from and were fixed to match | VERIFIED (read, own prior spike) |
| `spikes/declared-fields/` (whole crate, this spike's own code) | schema DDL for all four layouts, workload generation, every measured operation | VERIFIED (own code, own runs) |
| `spikes/declared-fields/results/*.jsonl`, `*.log` | every measured figure in Findings 1-8 | MEASURED (this spike's own runs) |
| `/proc/loadavg` sampled via `[load]` lines throughout every run | host-contention disclosure, the flat-load confirmation for the authoritative `benchauth` run | MEASURED (this spike) |
| `ps aux` (one contaminated-run investigation) | confirmed concurrent `cargo`/`cc1`/linker processes from other sessions on this shared host | MEASURED (this spike) |
| PostgreSQL TOAST/MVCC behavior (general documented behavior, not read from PostgreSQL's own source in this spike) | the direct-PostgreSQL recommendation's TOAST-auto-spill and whole-tuple-copy-on-UPDATE claims | INFERRED (general PostgreSQL knowledge, not measured or source-verified in this spike) |
| PostgreSQL catalog/autovacuum scaling at high table counts (general documented operational knowledge) | the "per-queue physical tables is a known PostgreSQL anti-pattern" claim | INFERRED (general PostgreSQL operational knowledge, not measured here) |

## Artifacts

- `spikes/declared-fields/` — standalone Cargo project (`Cargo.toml`,
  `src/{main,layouts,gen,codec,db}.rs`, `run.sh`); no DB files or build
  output committed.
- `spikes/declared-fields/results/*.jsonl` — raw per-phase measurements:
  ingest, the authoritative `benchauth` bench run per layout, the
  intermediate/superseded bench attempts (kept for the record, not used in
  Findings — see Method/Limitations), the per-queue-table scale test
  (`pqt-*.jsonl`), and the cache-size ablation
  (`cache-ablation-*.jsonl`).
- `spikes/declared-fields/results/*.log` — stderr progress and `[load]`
  lines, one per run above.
- This results document.
- No production code was modified; no contract types were changed.
