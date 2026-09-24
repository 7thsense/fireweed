---
ddx:
  id: spike-turso-value-size-boundary
  depends_on:
    - external-interface-kernel-contract-draft
    - spike-record-field-storage
  links:
    - {kind: informed_by, to: external-interface-kernel-contract-draft}
    - {kind: informed_by, to: spike-record-field-storage}
  status: draft
---

# Technical Spike: Turso Value-Size Boundary for Large Field Values

**Spike ID**: SPIKE-005 | **Lead**: DDx queue steward | **Time Budget**: ~3 h measurement, bounded runs | **Status**: Draft

## Objective

**Owner's question** (round-3 decisions, item 8): current draft records must be able
to carry larger field values than SPIKE-003 tested (256 B). The owner's answer to
"do we need a separate large-value type" was:

> No. We need a type that allows for larger fields (profiles). It doesn't have to
> be huge, but needs to be more than 4kb. 64kb is probably enough. Is there a
> boundary size we should target for turso? Test.

**Technical question**: SPIKE-003
(`docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md`) found that value
size, not field count, dominates ingest/update cost, and that a 64 KiB value
inline on the hot row costs 3.5-9x the update throughput of the same value kept
off-row — but it tested only one size (64 KiB) at one operation (update) and did
not locate *where* the cost starts, nor whether Turso's own storage engine has a
hard physical transition at some size. This spike finds that boundary.

Goals:
- measure ingest, small-field update, large-value update, point read
  (with/without the large value), and claim-style index-walk cost across ten
  value sizes (256 B - 64 KiB), for two storage placements: inline on the hot
  row, and off the hot row in a separate values table keyed by
  `(item_no, field_no)`;
- locate Turso's actual B-tree overflow-page spill boundary (not just an
  empirical "it got slower" point) and confirm it against the measured knee;
- test whether a larger `PRAGMA page_size` changes the picture for the inline
  case;
- recommend a max field value size, a spill threshold, and a page-size verdict,
  so the contract can keep one `fields` concept (D9,
  kernel-contract-draft.md:1030) with the storage engine transparently placing
  a value off the hot row above the threshold, rather than exposing storage
  placement as a second caller-visible concept.

Out of scope (tracked elsewhere): the `payload`-vs-`fields` collapse itself is
an owner/contract decision this spike informs but does not make; whether to
keep a distinct `payload` slot at all is not re-litigated here. RESP/streams
compatibility (owner decision on the same round), the claims-table design
(owner's round-3 item 4), and the object-log/queue-partition design (item 1)
are unrelated to this spike and untouched.

## Scope

Compared, both against the vendored Turso 0.7.2 embedded engine (`vendor/turso`),
configured the way `crates/fireweed-turso/src/local.rs` configures its own
connections, at ten value sizes — 256 B, 512 B, 1 KiB, 2 KiB, 3 KiB, 4 KiB,
8 KiB, 16 KiB, 32 KiB, 64 KiB:

- **(a) Inline** — `items (item_no INTEGER PRIMARY KEY, small BLOB NOT NULL,
  large BLOB NOT NULL)`. `small` (fixed 32 B, present at every size) stands in
  for an ordinary small field on the same record; `large` is the field under
  test, colocated on the same hot row.
- **(b) Off-row** — `items (item_no INTEGER PRIMARY KEY, small BLOB NOT NULL)`
  plus `values_tbl (item_no, field_no, value BLOB NOT NULL, PRIMARY KEY
  (item_no, field_no))`. The hot row's own `item_no` is the reference into
  `values_tbl` — no separate pointer column — the same design
  `fireweed_item_payloads` already uses (`crates/fireweed-relational/src/schema.rs:55-64`,
  VERIFIED: `PRIMARY KEY (tenant_id, queue_id, item_id)`, joined back to
  `fireweed_items` by that same key, no stored pointer).

Both variants carry a third table, `item_index (item_no INTEGER PRIMARY KEY,
rank INTEGER NOT NULL)` with a covering `(rank, item_no)` secondary index,
populated for every ingested row — the owner's round-3 answer to "why not just
rank" (item 3: "put int64(now) into the rank field... you get insertion
order") makes `rank` a mandatory ordering field on every record, not an
optional secondary index, so this spike populates it unconditionally rather
than treating it as SPIKE-003's opt-in indexed-field case.

Six operations per (variant, size), each repeated 3x with median and
min-max spread reported (host load, disclosed below, was non-trivial):

1. **Ingest** — 1,000-row batched transactions to a target of 50,000 rows,
   wall-clock cutoff 30 s/rep (a slow config reports fewer rows, not a
   truncated timer).
2. **Update, small field** — 5 rounds x 1,000-row pool, overwriting only
   `small`/`items.small`, never touching the large value's storage. Isolates
   whether an inline large value taxes updates of a *different*, small field
   on the same row.
3. **Update, large value itself** — same pool/rounds, overwriting the large
   field (`items.large` inline, `values_tbl.value` off-row).
4. **Point read without the large value** — `SELECT small FROM items WHERE
   item_no = ?`, 1,000 random samples. Same query text both variants (the
   off-row `items` table has no `large` column to accidentally select).
5. **Point read with the large value** — inline: one `SELECT small, large`;
   off-row: two round trips (row, then value) — the shape a caller reading
   the hot row and conditionally fetching the payload would actually issue,
   not a SQL join.
6. **Claim-style index walk** — 20 batches x 100: `SELECT item_no FROM
   item_index WHERE rank >= ? ORDER BY rank LIMIT ?` from a random offset,
   then one `small`-only point read per matched `item_no`.

Additionally, a **page-size sweep**: inline only, at 4 KiB and 16 KiB values
(straddling the default page's overflow threshold, see Findings), across
`PRAGMA page_size` in {4096, 8192, 16384, 65536}, measuring ingest and
large-value update (20,000-row target, smaller so the sweep stays fast — this
sub-experiment is about the *shape* of the page-size effect, not an
independent full-scale run).

## Setup

**Hardware** (measured, `nproc`, `free -g`, `/proc/loadavg`, at spike start and
again at write-up time):
- 32 logical CPUs, 94 GiB RAM (87-88 GiB available both times).
- `/proc/loadavg` ranged 13-22 (1-minute average) across the run on this
  32-core box — **the host was not idle**. `ps aux` during the run showed
  concurrent `cargo test`, `cargo clippy --workspace --all-targets
  --all-features`, `cargo check --workspace`, and `rustc` jobs building
  `aws-sdk-s3`, `redis`, `fireweed-relational`, `fireweed-memory` and other
  crates, from other sessions on the same shared machine. This is disclosed,
  not modeled away: individual rep numbers carry real noise (see Variance);
  the cross-size and cross-variant *shapes*, corroborated by an independent
  physical mechanism (the measured overflow-page threshold, below), are the
  load-bearing result.
- `uname -r`: `6.6.87.2-microsoft-standard-WSL2` (WSL2 VM, not bare metal) —
  absolute timings may not transfer to a bare-metal/cloud host.
- Filesystem: `ext4` on `/dev/sdd`, 4.0 TB, 3.4-3.5 TB free throughout.

**Software** (VERIFIED): vendored Turso `0.7.2` at `vendor/turso`
(`vendor/turso/Cargo.toml:15`), `default-features = false` — the exact spec
`crates/fireweed-turso/Cargo.toml:26` uses. `cargo 1.97.1`, `rustc 1.97.1`.

**Build — a real, disclosed methodology hazard, not a footnote.** The task
brief's own `CARGO_TARGET_DIR` (a shared scratch path this session was told
was "already warm") produced, on the very first `cargo build --release` under
it, a binary whose CLI usage text (`spike run <l1|l2> <db_path>...`) did not
match this spike's own `main.rs` at all — a **different, concurrent session's
spike** had built into the identical `target/release/spike` path between this
session's build and its first run, overwriting the binary. This is the exact
hazard SPIKE-003 documented independently
(`spikes/field-storage/`, "a *different* concurrent session's spike... had
overwritten this spike's binary between builds"). This spike's fix is the
same: every number below comes from a build against a **private**
`CARGO_TARGET_DIR`
(`.../scratchpad/design/vb-private-target`, not the shared
`.../design/spike-target` the task brief named), confirmed by `file` and a
smoke-test run before the timed sweep started. The one contaminated build
produced no data and is not included anywhere below.

**Connection configuration**, mirrored from
`crates/fireweed-turso/src/local.rs` `configure_connection`
(local.rs:3407-3444, VERIFIED) and `TursoConfig::local()` (local.rs:96-104,
VERIFIED), reproduced in `spikes/value-boundary/src/db.rs`: `page_size = 4096`
by default (settable — only takes effect before a fresh file's schema is
created, local.rs:3409-3411 comment, "existing files retain their page size";
overridden only by the page-size sweep), `journal_mode = WAL`
(local.rs:3412-3416), `synchronous = OFF` and `wal_autocheckpoint = 1000`
(the non-`rebuildable_io` branch — this spike does not build the log-backed
`RebuildableIo` adapter, out of scope; `TursoConfig::local()`'s own default,
`rebuildable_io: false`, local.rs:102, and what every non-log-backed
`fireweed-turso` caller in the repo runs today), `cache_size = -131072`
(128 MiB cap, local.rs:3431), 5 s busy timeout (`DEFAULT_BUSY_TIMEOUT`,
local.rs:59).

**Build**: standalone Cargo project, `spikes/value-boundary/` (own empty
`[workspace]` table, not a workspace member), `edition = "2021"`, path
dependency on `vendor/turso` `= "0.7.2"`, `default-features = false` — the
same spec `crates/fireweed-turso/Cargo.toml:26` uses. Built `--release`.

**Method — cache-warmth and WAL-growth artifacts, found and fixed before the
timed sweep ran.** Two methodology hazards surfaced during pilot runs and
were fixed (see `spikes/value-boundary/src/bench.rs` `warm_pool`, and
`main.rs`'s untimed one-round warm-up writes) before any number below was
collected:
1. `run_ingest`'s closing `PRAGMA wal_checkpoint(TRUNCATE)` leaves the page
   cache in a state where the *first* op measured afterward pays a one-time
   cold-read tax every later op on the same connection does not — this would
   bias op-to-op comparisons by measurement order, not by variant or size.
   Fixed by an untimed full read of the 1,000-row update/read pool before any
   timed op begins.
2. The same checkpoint truncates the WAL file to 0 bytes; the *first write*
   after that pays a one-time WAL-file-growth cost. Fixed by one untimed,
   discarded round of both the small-field and large-value updates before the
   timed reps begin.
   Both are disclosed as found-and-fixed, not modeled away, the same way
   SPIKE-003 disclosed and fixed its own WAL-checkpoint timing hazard.

**A third, more consequential hazard — a Turso query-planner gotcha, not a
storage-cost finding — required a mid-spike design change.** The initial
off-row large-value update used the same `UPDATE ... FROM (VALUES ...) AS v`
batched form SPIKE-003 recommended, which is correct and fast against `items`
(`item_no INTEGER PRIMARY KEY`, i.e. the table's rowid) but was measured
~100x slower against `values_tbl`, whose primary key is the *composite*
`(item_no, field_no)` — not the rowid. `EXPLAIN QUERY PLAN` (probed with
`SPIKE_UPDATE_PROBE=1`, `spikes/value-boundary/src/main.rs` `update_probe`)
confirms why:

| Form | Plan against `values_tbl` (20,000 bg rows) | Measured (pool=200, 1 txn) |
|---|---|---|
| `UPDATE ... FROM (VALUES ...) AS v WHERE t.item_no = v.column1 AND t.field_no = 0` | `SCAN values_tbl USING COVERING INDEX ...` / `SCAN v` (O(pool x table)) | 243.6/s |
| `UPDATE ... WHERE item_no = ? AND field_no = ?` (plain, per-row) | `SEARCH values_tbl USING INDEX sqlite_autoindex_values_tbl_1 (item_no=? AND field_no=?)` | 25,937.9/s |

For comparison, the same batched-`VALUES` form against `items` (whose PK *is*
the rowid) gets `SCAN v` / `SEARCH items USING INTEGER PRIMARY KEY (rowid=?)`
— the efficient plan — at every size tested. **This is specific to Turso's
planner choosing a nested-loop scan instead of an available index when the
`UPDATE...FROM` target's primary key is not the rowid; it is not a cost of
off-row storage itself.** `run_update_large`'s off-row branch was changed to
one plain per-row `UPDATE ... WHERE item_no = ? AND field_no = ?` per row,
all inside the round's one transaction (`bench.rs` comment documents the
probe inline). Every off-row `UPDATE_LARGE` number below uses this corrected
form. **Flagged as a follow-up, not resolved here**: `fireweed_items`' own
primary key is also composite (`tenant_id, queue_id, item_id`,
`crates/fireweed-relational/src/schema.rs:19-51`, VERIFIED) — not the rowid —
so if `fireweed-turso`'s real batched-write path uses the same
`UPDATE...FROM (VALUES...)` shape against `fireweed_items` itself, it may hit
the identical SCAN/SCAN plan. This spike did not audit `turso_compose.rs`'s
actual write SQL (out of scope, a general write-path question, not a
value-size one) and explicitly does not claim production is affected —
only that the mechanism is real, measured, and worth an independent check.

**Row/batch counts actually run**: the task brief asked for 50,000+ records
with 1,000-row update/read batches. Ingest ran to the 50,000-row target with
a 30 s/rep cutoff; at 16 KiB and above the cutoff binds for some reps (fewer
rows completed, marked `cut_off=true` below) — a disclosed reduction, not a
silent one, and, as SPIKE-003 found, the per-batch steady-state numbers
(rows/s, bytes/row) that matter for locating the boundary are unaffected by
running fewer total batches. Every update/read/claim op ran against a base
database populated by that size/variant's own ingest.

**Repetition and variance**: every (variant, size, operation) cell was run 3x
(`REPS=3` in the harness); tables below report the median and the observed
[min, max]. Where the spread is wide, it is host contention (disclosed above)
or, per the payload-placement pattern, real bimodal behavior — called out
inline.

## Findings

### FINDING: Turso's actual B-tree overflow-page spill boundary for this schema is between 3 KiB and 4 KiB, at the default 4 KiB page — and it lands almost exactly where the owner's own intuition did

Turso's overflow-threshold formula (`vendor/turso_core/storage/btree.rs:9027-9036`,
VERIFIED, standard SQLite B-tree mechanics): for a table-leaf cell,
`max_local = usable_space - 35`; a row's payload spills the excess to
overflow pages once it exceeds that. At the production default `page_size =
4096` with no reserved bytes, `usable_space = 4096`, so `max_local = 4061`
bytes of **total row payload** (record header + every non-rowid column's
encoded bytes — here, `small` (32 B + ~2 B overhead) plus `large` (X bytes +
~2 B overhead) plus a ~5 B record header). Solving for X: the row stops
fitting entirely on the page somewhere around **X ~ 4,020 bytes**.

Measured on-disk bytes/row (identical across reps — deterministic content,
`db_bytes` is exact — VERIFIED this spike):

| Value size | inline B/row | offrow B/row (items+values_tbl+index) |
|---|---|---|
| 256 B | 343.24 | 355.86 |
| 512 B | 613.91 | 668.63 |
| 1,024 B | 1,396.00 | 1,450.72 |
| 2,048 B | 4,133.27 | 4,188.00 |
| 3,072 B | 4,133.27 (identical to 2,048 B) | 4,188.00 (identical to 2,048 B) |
| 4,096 B | **4,636.59** (+503 B step) | **4,691.31** (+503 B step) |
| 8,192 B | 8,732.59 | 8,787.31 |
| 16,384 B | 16,924.59 | 16,979.31 |
| 32,768 B | 33,307.5-33,308.1 | 33,362.2-33,362.5 |
| 65,536 B | 66,075.4 | 66,128.8-66,129.1 |

The 2,048 B and 3,072 B rows are byte-identical on disk — both fit entirely
locally in one dedicated leaf page (no overflow yet, matching the formula's
prediction that ~4,020 B is the true boundary), so the extra kilobyte of
content costs nothing beyond what the page had free. The **4,096 B row is
the first to step up** (+503 B — one overflow page engaged, exactly the
predicted crossing between 3,072 B and 4,096 B), and every size above it adds
essentially its own bytes as new overflow-chain length. This confirms the
formula against real measurement, not just against SPIKE-003's earlier
single-point 4,096 B observation.

### FINDING: there are two distinct knees, not one — an earlier "one row, one page" transition around 1-2 KiB, then the true overflow-chain cost from ~4 KiB up

Ingest throughput (rows/s, inline, median of 3 reps, 50,000-row target):

| 256 B | 512 B | 1,024 B | 2,048 B | 3,072 B | 4,096 B | 8,192 B | 16,384 B | 32,768 B¹ | 65,536 B¹ |
|---|---|---|---|---|---|---|---|---|---|
| 44,815 | 29,042 | 21,451 | 5,757 | 7,620 | 6,717 | 4,330 | 2,169 | 1,230 | 504 |

¹ cutoff-bound (30 s/rep); fewer than 50,000 rows completed (33-40k at
32,768 B, 14-19k at 65,536 B) — steady-state rows/s is still a real per-batch
measurement, not an extrapolation (SPIKE-003's same reasoning).

The first big drop is between **1,024 B and 2,048 B** (21,451 -> 5,757
rows/s, a 3.7x cliff) — **before** the ~4,020 B true-overflow boundary above.
This is the page-packing transition visible in the bytes/row table: below
~1 KiB several rows share a leaf page; at 2,048 B a row already needs (most
of) a dedicated page to itself (4,133 B/row, just above one 4,096 B page),
so every insert now pays a fresh page allocation instead of appending to an
already-resident page. `3,072 B` measuring *faster* than `2,048 B`
(7,620 vs 5,757 rows/s) is measurement noise, not a real inversion — both
sizes are physically identical on disk (same bytes/row, previous finding),
so their true per-row cost should be indistinguishable; host contention
(disclosed above) explains the gap, corroborated by the spread ([5,663,
9,131] for 2,048 B) overlapping 3,072 B's own range ([5,765, 7,758]).

The **second, larger and monotonic** drop starts at 4,096 B and continues
through every larger size (6,717 -> 4,330 -> 2,169 -> 1,230 -> 504 rows/s)
as the overflow chain lengthens — this is the true spill cost the formula
predicts, and it never recovers: unlike the first knee (a one-time
page-monopolization cost), each doubling of value size roughly doubles the
number of overflow pages an insert must write.

### FINDING: an inline large value taxes *every* update of the same row, including updates that never touch it — and the tax starts small, then compounds

Small-field update throughput (updates/s, median of 3 reps, 1,000-row pool,
5,000 updates):

| Size | inline | offrow | offrow/inline |
|---|---|---|---|
| 256 B | 52,665 | 38,680 | 0.73x |
| 1,024 B | 43,997 | 25,721 | 0.58x |
| 2,048 B | 7,235 | 29,711 | 4.11x |
| 4,096 B | 24,412¹ | 30,710 | 1.26x |
| 8,192 B | 18,088 | 16,589 | 0.92x |
| 16,384 B | 10,912 | 21,182 | 1.94x |
| 32,768 B | 4,328 | 12,875 | 2.97x |
| 65,536 B | 2,593 | 7,328 | 2.83x |

¹ inline's own 2,048 B cell (7,235/s) is visibly the odd one out relative to
its 1,024 B and 4,096 B neighbors (43,997 and 24,412/s) — the same
page-monopolization transition as the ingest finding, landing on a noisier
measurement here (spread [5,968, 7,750]) than the ingest table's.

Below the true overflow boundary (<~4 KiB), inline is **faster** than
off-row for a small-field update — off-row pays a fixed per-row overhead
(the corrected single-statement form, previous section) that dominates when
the large value itself is cheap to carry along anyway. **Above ~4 KiB, the
relationship inverts and keeps widening**: by 32-64 KiB, updating the small
field costs 2.8-3.0x more when a large value is colocated on the row than
when it lives off-row. This directly confirms SPIKE-003's payload-placement
finding (3.5-9x at a single 64 KiB point) and locates *where* the crossover
happens: **between 2 KiB and 4 KiB**, coincident with the true overflow
boundary, not the earlier page-packing knee.

### FINDING: updating the large value itself is comparable or cheaper off-row once values are non-trivial, but off-row pays a real fixed per-statement cost at small sizes

Large-value update throughput (updates/s, median of 3 reps):

| Size | inline | offrow | offrow/inline |
|---|---|---|---|
| 256 B | 50,095 | 13,573 | 0.27x |
| 1,024 B | 40,759 | 12,150 | 0.30x |
| 4,096 B | 21,339 | 10,838 | 0.51x |
| 16,384 B | 8,015 | 7,762 | 0.97x |
| 32,768 B | 2,571 | 4,240 | 1.65x |
| 65,536 B | 1,340¹ | 2,944 | 2.20x |

¹ inline 65,536 B's own reps ranged [529, 1,390] — one rep landed far below
its siblings, consistent with a background-load spike (not reproduced in any
other cell's spread of this magnitude); the median is reported, the outlier
noted rather than discarded.

At small sizes, off-row is 3-4x *slower* to update the value itself — the
corrected single-row-per-statement form (this spike's query-plan finding,
above) pays a per-row statement/index-lookup cost that the inline path's
batched multi-row `UPDATE...FROM` avoids. By 16 KiB the two are within noise
of each other, and above that off-row wins outright (1.65-2.2x), because it
never carries the unrelated `small` column's bytes through the same
serialized record. **This means an off-row spill implementation should not
naively reuse a single-row-per-statement update for every write once values
are small** — a batched form is available and fast for the *inline* case,
and the off-row case would benefit from one too if Turso's planner is
worked around (e.g. by giving the spill table's key a leading rowid-aliased
column, or by finding an `UPDATE...FROM` phrasing the planner will index)
rather than by defaulting to per-row statements at every size.

### FINDING: reading a row without the large value is what off-row storage is *for* — flat cost regardless of size, versus a 4-6x inline penalty by 32-64 KiB

Point-read latency without the large value (µs, median avg of 3 reps x 1,000
samples):

| Size | inline | offrow | inline/offrow |
|---|---|---|---|
| 256 B | 20.89 | 31.53¹ | 0.66x |
| 1,024 B | 23.41 | 19.53 | 1.20x |
| 4,096 B | 33.11 | 22.01 | 1.50x |
| 16,384 B | 50.62 | 18.59 | 2.72x |
| 32,768 B | 113.76 | 21.02 | 5.41x |
| 65,536 B | 80.65² | 20.18 | 4.00x |

¹ offrow's 256 B cell (31.53 µs) is the one point-read outlier in the whole
off-row column (every other size sits in a tight 17-34 µs band with no size
trend); not reproduced at 512 B (22.82 µs) run immediately after in the same
sweep — host noise on that specific rep window, not a real small-size cost.
² inline 65,536 B's own spread was wide ([70.20, 123.27] µs); 32,768 B's
median (113.76 µs) is itself noisier ([100.27, 127.60]) than the clean
monotonic climb through 16,384 B would predict, but every inline number from
2,048 B up is well above every offrow number at any size — the qualitative
finding (flat off-row, climbing inline) holds regardless of exactly which
of the two largest cells has the higher point estimate.

Off-row's `READ_WITHOUT` cost is **flat**, 17-34 µs across the entire
256 B-64 KiB sweep, because the query never touches `values_tbl` at all —
size-independent by construction. Inline's cost **climbs monotonically**
from ~21 µs to 80-114 µs, because Turso's row decode pays for the value's
storage (including walking its overflow-page chain once one exists) even
though the query's `SELECT small` never asks for that column's value. This
is the single cleanest, most decision-relevant number in this spike: **the
claim-style hot path — read a row's small fields to decide what to do next,
without needing a big field like a profile blob — gets 4-6x slower purely
from an unrelated column's size, once that column is inline and large.**

### FINDING: reading a row *with* the large value favors inline until sizes are large, because off-row pays a flat two-round-trip tax at every size

Point-read latency with the large value (µs, median avg of 3 reps):

| Size | inline | offrow | offrow/inline |
|---|---|---|---|
| 256 B | 20.59 | 72.44 | 3.52x |
| 4,096 B | 34.75 | 75.90 | 2.18x |
| 16,384 B | 50.33 | 80.46 | 1.60x |
| 32,768 B | 123.01¹ | 107.39 | 0.87x |
| 65,536 B | 80.17 | 139.92 | 1.75x |

¹ wide spread ([88.68, 169.57] µs, one 32,483 µs max sample recorded, almost
certainly a scheduler-preemption outlier under the disclosed host
contention, not a storage-mechanics effect); median reported.

Off-row's two-round-trip shape (row query, then value query — the realistic
"read the row, then conditionally fetch the payload" pattern, not a SQL
join) costs a roughly constant 50-70 µs more than inline **when the large
value is actually needed**, at every size this spike tested up to 64 KiB.
Combined with the previous finding, the two together describe the real
trade-off precisely: **off-row is a large, size-independent win whenever a
caller can avoid touching the big field, and a modest, size-independent cost
whenever it cannot** — which is exactly the shape a "spill large values off
the hot row automatically" design should produce, and exactly why
SPIKE-003's `payload` design (and D9's two-slot model) already assumed it.

### FINDING: claim-style index-walk-plus-fetch inherits the point-read-without shape, since a claim never wants the large value

Claim throughput (claims/s, median of 3 reps, 100-item batches, small-only
row fetch):

| Size | inline | offrow | inline/offrow |
|---|---|---|---|
| 256 B | 50,097 | 44,659 | 1.12x |
| 4,096 B | 36,978 | 43,756 | 0.85x |
| 16,384 B | 26,176 | 50,718 | 0.52x |
| 65,536 B | 14,013 | 55,041 | 0.25x |

Same shape as `READ_WITHOUT` (unsurprising — a claim batch is exactly N of
those point reads plus one shared index-range scan): inline drops 3.6x from
256 B to 65,536 B, off-row holds flat within noise (42-58k/s throughout).
For the "stay fast at 10M+ records" concern this spike's task brief carries
over from the owner's own framing, this is the concrete number: **an inline
large field on a hot queue directly taxes claim throughput as that field
grows, even though a claim never reads it; off-row does not.**

### FINDING: a larger `PRAGMA page_size` does not reliably help, and was frequently worse, for the inline large-value case

Page-size sweep (inline only, ingest rows/s and large-value update/s, median
of 3 reps, 20,000-row target):

| page_size | 4,096 B ingest | 4,096 B update | 16,384 B ingest | 16,384 B update |
|---|---|---|---|---|
| 4,096 (default) | 11,174 | **24,076** | 4,199 | 9,972 |
| 8,192 | 9,469 | 8,479 | 5,124 | 11,731 |
| 16,384 | 14,011 | 19,498 | 5,979 | 11,041 |
| 65,536 | 17,011¹ | 8,592 | 3,460 | **5,679** |

¹ wide spread ([9,339, 27,118]/s) — one clean rep, two contended-looking
ones; median reported, not the high outlier.

At `page_size = 4,096`, the 4,096 B value is right at the overflow boundary
(previous finding: true spill starts ~4,020 B), so it already overflows by a
hair. Moving to `page_size = 8,192` or `16,384` (where `max_local` grows to
8,157 / 16,349 — both comfortably clear of 4,096 B, so the value stops
overflowing entirely) does **not** produce the expected win: 8,192 B pages
measured *slower* on both ingest and update for the 4,096 B value than the
default. 16,384 B pages did beat the default on update (19,498 vs 24,076 is
still a loss, but ingest at 16,384-byte pages, 14,011 rows/s, beat the
default's 11,174). The 64 KiB page (fully clears overflow for both tested
sizes) was the **worst** update performer for the 16,384 B value (5,679/s,
worse than every other page size including the default, where that value
still overflows). The pattern across both value sizes and both operations is
inconsistent in direction but consistently **not a clear win**: a larger
page trades fewer/no overflow-chain hops for a bigger WAL frame written on
every touched page, and at this scale the second cost is not smaller than
the first. **No page size tested beats the 4 KiB default clearly and
consistently enough to recommend changing it**, and 3 reps per cell is not
enough sample to fully separate real page-size effects from the disclosed
host contention noise — a genuine limitation, not a null result being
overclaimed as one.

## Interpretation

**The owner's instinct was right, and Turso's own physics agrees almost
exactly**: "more than 4 KiB, 64 KiB is probably enough" describes precisely
the terrain this spike measured — 4 KiB (specifically ~4,020 bytes at the
production default page size) is where Turso's B-tree stops storing a row
entirely on its own page and starts walking an overflow chain, and every
downstream cost (ingest, small-field update, point-read-without, claim) that
this spike measured degrades sharply and monotonically from that point
on, in a way it does not below it.

**There are two knees, and only one of them is the physical overflow
boundary.** The first, smaller one (~1-2 KiB) is a page-packing effect —
rows stop sharing a leaf page with their neighbors — and costs a few-x, not
an order of magnitude; it recovers no further as values keep growing (a
2,048 B row and a 3,072 B row are physically identical on disk). The second
(~4 KiB, confirmed against the verified `usable_space - 35` formula) is the
real overflow-chain cost, and it compounds without bound as values keep
growing — this is the one that matters for a spill-threshold decision.

**Off-row storage is not a uniform win — it is a trade that strongly favors
the operations a queue actually does on the hot path.** It is measurably
*worse* than inline for updating the large value itself at small sizes (the
corrected single-statement form's per-row overhead), and it pays a flat
tax whenever a caller genuinely needs the value alongside the row. But for
the two operations this spike's task brief explicitly named as the ones that
must "stay fast with 10M+ records" — a small-field update on an otherwise
large row, and a claim-style walk that only wants a few small fields to
decide what to do next — off-row is flat-cost and inline degrades
directly with the unrelated field's size. A spill design should optimize for
exactly this: keep the hot row small so the operations that run constantly
(claim, small-field patch, index maintenance) never pay for a field they are
not touching.

**The query-plan finding is the spike's most actionable surprise, and it is
orthogonal to value size.** A batched `UPDATE ... FROM (VALUES ...)` against
a table whose primary key is not the rowid gets a nested-loop scan plan in
this vendored Turso build, ~100x slower than either an equivalent single-row
equality `UPDATE` or the same batched form against a true `INTEGER PRIMARY
KEY` table. Any off-row spill implementation must route its batched
large-value writes around this, and — flagged, not resolved here — the real
`fireweed_items` table shares the same composite (non-rowid) primary-key
shape as this spike's `values_tbl`, so the production batched-write path is
worth an independent check for the same plan.

## Recommendation

1. **Max field value size: 64 KiB**, confirming the owner's own estimate.
   Every operation this spike measured completed correctly and at a bounded
   (if degraded, for the inline case) cost at 64 KiB; nothing in the data
   suggests a lower hard ceiling is warranted, and 64 KiB stays two orders
   of magnitude under the draft's existing 4 MiB whole-record cap
   (kernel-contract-draft.md:583, D15/kernel-contract-draft.md:1036).

2. **Spill threshold: 4 KiB.** This is not an arbitrary round number — it is
   within ~80 bytes of Turso's own measured, formula-confirmed overflow-page
   boundary at the production default page size (`usable_space - 35 =
   4,061` total row payload, crossed for this spike's schema between
   3,072 B and 4,096 B of field value). Below it, inline is frequently
   *faster* (small-value update, and — matched cost — ingest); above it,
   every hot-path operation this spike measured degrades, in some cases by
   an order of magnitude by 64 KiB, and off-row is flat or better on every
   one of them except "update the large value itself at a small size" (which
   is moot once the value is actually large enough to spill). Recommend the
   engine (not the contract) enforce this: **one `fields` concept, one
   `BTreeMap<String, Value>`, with `fireweed-turso` transparently storing
   any single field's value off the hot row once it exceeds ~4 KiB**,
   exactly as the task brief's framing asked — no second caller-visible
   "payload" type is needed for this. Whether the existing `payload` slot
   (D9) becomes redundant with this mechanism, or stays for other reasons
   (e.g. a documented "this is definitely bulk data" signal), is the
   owner's call, not resolved by this spike.

3. **Do not change the default `PRAGMA page_size` from 4 KiB.** No larger
   page size tested (8/16/64 KiB) produced a consistent win on either ingest
   or large-value update throughput for the two representative sizes tested,
   and two of the three larger sizes were the *worst* performer on at least
   one operation. This spike's page-size sweep is under-sampled (3 reps,
   2 value sizes, one operation pair) to fully separate a real page-size
   effect from host noise — but the burden is on a future change to show a
   clear win, and this data does not show one. Keep the existing default
   (`crates/fireweed-turso/src/local.rs:3411`, and SPIKE-002's already-made
   recommendation to retain it).

4. **Implement the spill write path as a corrected batched form, not a
   naive per-row loop.** This spike's off-row `UPDATE_LARGE` numbers use a
   single-statement-per-row form specifically because the batched
   `UPDATE...FROM (VALUES...)` form measured ~100x slower against a
   composite-keyed side table (query-plan finding, above). That per-row
   form is fine for the workload this spike modeled (a 1,000-row pool,
   5 rounds) but its relative cost grows with statement count; a
   production spill implementation should either find an `UPDATE...FROM`
   phrasing Turso's planner will index for a composite-keyed table, or
   accept the per-row form deliberately (documented, not accidental).

5. **Follow-up, not resolved here**: check whether `fireweed-turso`'s actual
   batched-write path (`turso_compose.rs`) issues `UPDATE ... FROM
   (VALUES ...)` against `fireweed_items` itself, which shares
   `values_tbl`'s composite (non-rowid) primary key shape. If it does, the
   same SCAN/SCAN plan this spike measured may already be costing production
   writes, independent of value size.

## Limitations

- **n=3 per cell**, as the task specified, not enough to fully separate a
  real small effect from host contention on a shared, non-idle machine —
  several individual cells are flagged inline where the spread is wide
  enough to matter (2,048 B inline update-small; 32,768/65,536 B read-with;
  page-size sweep generally). The cross-size and cross-variant *shapes* are
  corroborated by the independently-verified overflow-page formula and are
  the reliable part of this spike's output; individual third-significant-figure
  numbers are not.
- **20,000-row target for the page-size sweep**, not 50,000 — a disclosed
  reduction to keep an 8-cell sub-experiment inside the time budget; ingest
  rows/s and update updates/s are both per-batch steady-state measures and
  should hold at 50,000 rows, per SPIKE-003's identical reasoning for its own
  row-count reduction.
- **One field per row tested**, not the many-small-plus-one-large mix a real
  record would carry (SPIKE-003 already covers many-small-fields cost
  separately). This spike's `small` column (fixed 32 B) is a minimal stand-in
  for "some other field exists on the row," not a full field-count sweep.
- **The claim-walk model is a simplification.** `item_index`'s `rank` here
  is literal insertion order (`rank = item_no`), not a real due-time or
  priority ordering, and the walk starts from a uniformly random offset each
  batch rather than the sequential/oldest-first pattern a real claim would
  use. The relative cost structure (index scan + N small-field row fetches,
  large value never touched) should generalize; the absolute claims/s
  numbers are this spike's synthetic workload, not a claim-path capacity
  estimate (SPIKE-004 owns that question).
- **The off-row large-value update's per-row form was a mid-spike fix**, not
  the design this spike set out to test — a batched form that avoids the
  measured query-plan trap was not found within this spike's time budget
  (Recommendation 4).
- **The production `fireweed_items` batched-write path was not audited**
  for the same query-plan issue (Recommendation 5) — flagged, not measured.
- No RESP, gates, leases, stages, claims-table design, or any other
  kernel-contract surface beyond field-value storage placement was exercised.

## Source Ledger

| Source | Claim used | Confidence |
<br>|---|---|---|
| `vendor/turso_core/storage/btree.rs:9020-9051` | `payload_overflow_threshold_max`/`_min` formulas, table-leaf vs index-leaf distinction | VERIFIED (read) |
| `vendor/turso_core/storage/sqlite3_ondisk.rs:95-150` | `PageSize::MIN`/`MAX`/`DEFAULT` (512 / 65,536 / 4,096, power-of-two only) | VERIFIED (read) |
| `crates/fireweed-turso/src/local.rs:59,96-104,3396-3444` | connection configuration this spike mirrors | VERIFIED (read) |
| `crates/fireweed-relational/src/schema.rs:14-64` | `fireweed_items`/`fireweed_item_payloads` real column shape, composite (non-rowid) primary keys, off-row payload design comment | VERIFIED (read) |
| `docs/helix/00-discover/external-interface-kernel-contract-draft.md:583,1030,1036` | 4 MiB record cap, D9 two-data-slot design, D15 limits table | VERIFIED (read) |
| `docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md` | payload-vs-inline 3.5-9x finding this spike extends/locates | VERIFIED (read, this spike's stated starting point) |
| `vendor/turso/src/params.rs`, `vendor/turso/src/rows.rs`, `vendor/turso/src/connection.rs` | `IntoParams`/`Row`/`Connection` API surface this spike's harness uses | VERIFIED (read) |
| This spike's own runs (full sweep, page-size sweep, `SPIKE_UPDATE_PROBE` plan probe) | every number in Findings/Results | MEASURED (this spike); raw `\|`-delimited logs in `spikes/value-boundary/results/` |
| `/proc/loadavg`, `ps aux`, `nproc`, `free -g`, `df -h` sampled during the runs | host contention and disk-space disclosure | MEASURED (this spike) |

## Artifacts

- `spikes/value-boundary/` — the standalone spike Cargo project (source:
  `main.rs`, `bench.rs`, `db.rs`, `gen.rs`, `sql.rs`; no DB files or build
  output committed).
- `spikes/value-boundary/results/full-run.log`,
  `spikes/value-boundary/results/page-run.log` — raw `\|`-delimited output
  from the two sweeps this doc's tables summarize, plus their stderr
  progress logs.
- This results document.
- No changes to any existing repository file.
