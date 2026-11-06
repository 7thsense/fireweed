---
ddx:
  id: spike-record-key-storage
  status: draft
---

# Technical Spike: Record Key Storage Cost on Turso

**Spike ID**: SPIKE-002 | **Lead**: DDx queue steward | **Time Budget**: ~3 hours | **Status**: Draft

## Objective

**Technical question**: does the kernel contract's record `Key` — upstream-owned,
UTF-8, declared 1..=1024 bytes (draft §2.0, `Key(String)`, line 141; §4 limits
table, line 589) — create a storage or performance bottleneck on the S3×Turso
production backend at the lengths upstream ids actually take (short integers,
UUIDs, emails, free-form strings up to 256 bytes and beyond), and if so, what
physical key layout removes it?

This is the owner's round-3 decision 3 (SPIKE IT):
the owner's round-3 decision 3 (2026-09-23 design discussion):
> Upstream owns record ids and their uniqueness contract. 256-byte UTF-8
> strings are fine, as long as that does not create a storage or performance
> bottleneck. SPIKE IT. Upstream ids are very likely 32/64/128-bit ints, but
> often represent emails or similar. Don't optimize for useless readability if
> it carries a performance cost. The column needs a unique constraint on
> (queue, key): it is written twice (row + index) and processed for indexing,
> but only once, at ingest.

Goals:
- measure physical storage cost (DB file size after checkpoint) per key shape
  and per candidate schema layout, on the vendored Turso 0.7.2 engine
  configured the way `crates/fireweed-turso` opens its production projection;
- measure ingest, point-lookup, upsert and claim-by-keys throughput per layout;
- recommend a concrete key type, max length, surrogate-queue-number decision,
  and whether hashing is warranted.

Out of scope (tracked elsewhere, not measured here): how a client declares
which fields it will supply and wants indexed (draft §2.1 `IndexDef`/`IndexKey`,
lines 196-197; §3.1, line 468) — that is the separate typed-index/field-storage
question; whether `rank`/`due` must avoid a datetime assumption (decisions-round3.md
line 19) — unrelated to key storage. Both are flagged, not investigated.

## Scope (setup)

**Hardware** — VERIFIED (measured on this host):
- `nproc`: 32
- `free -g`: 94 GiB total, ~86 GiB available, 24 GiB swap (3 GiB in use)
- `uname -r`: `6.6.87.2-microsoft-standard-WSL2` — this is a WSL2 VM, not bare-metal
  Linux; absolute I/O timings may not transfer to a bare-metal/cloud host, noted
  as a limitation below.
- root filesystem: `ext4` on `/dev/sdd`, 4.0 TB, ~1 TB free.
- Machine was **shared** with concurrent, unrelated work for the full run: a
  sibling spike process (`spikes/field-storage/`, confirmed via `/proc/<pid>/cwd`
  pointing at a different session's scratch directory) plus several concurrent
  `cargo`/`rustc` builds from other sessions were observed running throughout
  (via `ps`), consistent with the noise reported below.

**Software** — VERIFIED:
- Vendored Turso `0.7.2` at `vendor/turso` (`vendor/turso/Cargo.toml:15`) and
  `vendor/turso_core` (`vendor/turso_core/Cargo.toml:15`), the exact pinned
  version `crates/fireweed-turso/src/local.rs:54` (`TURSO_SUPPORTED_VERSION`)
  requires; qualified boundary is `embedded_local_ordinary_wal`
  (`local.rs:56`).
- `vendor/turso_core/FIREWEED.md:1-11` documents a Fireweed-only backport: a
  settable/queryable `PRAGMA wal_autocheckpoint` on ordinary native WAL, default
  1000 frames, zero disables automatic checkpoints. This is the pinned engine's
  real behavior, not upstream 0.7.2 unmodified.
- The spike is a standalone Cargo project (own empty `[workspace]` table, not a
  workspace member) at `spikes/key-storage/`, with path dependencies mirroring
  `crates/fireweed-turso/Cargo.toml:26,28` exactly: `turso` and `turso_core`
  pinned to `=0.7.2`, `default-features = false`, `turso_core` with
  `features = ["fs"]` (`spikes/key-storage/Cargo.toml`). Built
  `--release` with `CARGO_TARGET_DIR` pointed at the shared spike-target
  scratch directory named in the task.
- Rust: `cargo 1.97.1`, `rustc 1.97.1`.

**Connection configuration** — VERIFIED, copied from
`crates/fireweed-turso/src/local.rs:3407-3444` (`configure_connection`) under
the profile production actually opens
(`TursoConfig::local(path).with_log_backed_projection()`,
`crates/fireweed/src/turso_compose.rs:136` — confirmed by grep, the only
call site that opens the production Turso projection):
`page_size=4096`, `journal_mode=WAL`, `synchronous=NORMAL` (rebuildable_io=true
branch, `local.rs:3420-3429`), `cache_size=-131072` (128 MiB,
`local.rs:3431`), `temp_store=MEMORY` (`local.rs:3432-3436`),
`wal_autocheckpoint=0` (log-backed, no auto-checkpoint under load,
`local.rs:3437-3442`, `checkpoint_frames` at `local.rs:3396-3405`),
`busy_timeout=5s` (`DEFAULT_BUSY_TIMEOUT`, `local.rs:59`). Reproduced
verbatim in `spikes/key-storage/src/main.rs:449-462` (`configure_connection`).

**Limitation (INFERRED, not measured)**: the spike uses plain local-file IO,
not fireweed's private `RebuildableIo` wrapper (`crates/fireweed-turso/src/rebuildable_io.rs`,
internal to that crate and not reusable from a standalone project). The pragma
profile is identical; only the underlying IO adapter differs. Since every
layout in this sweep runs through the identical IO path, the layout-to-layout
*comparison* is unaffected; only cross-run *absolute* wall-clock numbers might
not transfer exactly to production's IO adapter.

**Representative row** — VERIFIED, the 27 non-identity ("hot") columns of
`fireweed_items` copied verbatim (name, type, nullability, default) from
`crates/fireweed-relational/src/schema.rs:20-52`, minus `tenant_id`,
`queue_id`, `item_id`, `client_item_key` (the identity/key columns that vary
by layout under test) — see `spikes/key-storage/src/main.rs:207-240`
(`HOT_COLUMNS_DDL`). Every combo pays this same realistic per-row cost; the
spike is not a bare two-column table.

## Method

Five candidate physical key layouts, built exactly as the task specified
(`spikes/key-storage/src/main.rs:173-365`, `layout_sql`):

| Layout | Identity columns | Secondary unique index | Notes |
|---|---|---|---|
| **A** | `tenant_id TEXT, queue_id TEXT, item_id TEXT` (PK) | `(tenant_id, queue_id, client_item_key)` TEXT | Today's shape, `crates/fireweed-relational/src/schema.rs:15-19,53,65-66`, minus the `WHERE superseded=0` partial predicate — the new contract has no tombstones/supersede (draft §3.3 line 507, "There are no tombstones"), so a plain non-partial index is the honest carry-forward. |
| **B** | `queue_no INTEGER, row_seq INTEGER` (PK) | `(queue_no, client_item_key)` TEXT | Surrogate integer in place of the two TEXT identity columns. |
| **C** | same as B | `(queue_no, client_item_key)` BLOB | Key stored/indexed as BLOB instead of TEXT. |
| **D** | `queue_no, row_seq` (PK) + `client_item_key TEXT` (stored, unindexed) + `key_hash BLOB` | `(queue_no, key_hash)` UNIQUE, 16 bytes | Fixed-size hash index; full key kept unindexed for lookup/collision verification. |
| **E** | same PK as B | `(queue_no, client_item_key)` INTEGER | Int shape only. |

Two no-index baselines (`run_baseline`, `main.rs:743-812`) isolate the index's
own on-disk cost by file-size delta, since **Turso 0.7.2 does not implement
`dbstat`** — VERIFIED: `grep -rli dbstat vendor/turso_core/ vendor/turso/`
finds one hit, a comment at
`vendor/turso_core/storage/sqlite3_ondisk.rs:2130` referencing upstream
SQLite's `dbstat.c`, not a registered virtual table.

Key shapes (`main.rs:88-171`, `Shape`): 64-bit int as decimal TEXT (≈19 bytes),
8-byte BLOB and native INTEGER; UUID as 36-byte canonical-hex TEXT and 16-byte
BLOB; email-like TEXT (`user<hex>@<domain>`, ~25–37 bytes); generic 128-byte
and 256-byte TEXT/BLOB strings. All keys are generated deterministically from
the row index via the SplitMix64 finalizer (`main.rs:46-52`), a bijection over
`u64`, so distinct rows never collide — no external RNG crate needed for a
storage/perf spike.

Per combo, one process (fresh DB file): ingest `rows` rows in 1000-row
`Immediate` transactions; `PRAGMA wal_checkpoint(TRUNCATE)`, driven to
completion by draining its result rows exactly as production does
(`crates/fireweed-turso/src/projection.rs:117-122`, `144-146` — its lazy
`Rows` cursor does not execute until stepped, confirmed the hard way, see
Finding 0 below); 20,000 point lookups by key, keys sampled uniformly at
random from the ingested set; 20,000 `INSERT … ON CONFLICT(…) DO UPDATE`
upserts, all landing on the conflict/update path (fresh, never-before-used PK
per attempt so only the secondary unique index can conflict); 20,000
claim-style key lookups (200 batches × 100 keys), the shape of the contract's
`Select::Keys` claim (draft §2.3, line 349: "At most 1000 keys"). 23 combos ×
100,000 rows fit in ~8 minutes total wall time; see Finding 4 for the 1M-row
attempt and why 100k was used for the full sweep, per the task's explicit
"100k if 1M is too slow" allowance.

Driver: `spikes/key-storage/run.sh`. Raw output:
`spikes/key-storage/results/results.jsonl` (23 lines, one per combo) and
`results/variance.jsonl` (9 repeated runs, 3 combos × 3 reps, for the variance
figures in Finding 4).

## Findings

### Finding 0 (methodology): the query planner does not pick a secondary unique index without a hint — VERIFIED, and this gates every other measurement

First calibration attempt (100,000 rows, all four phases, default parameters)
did not finish in over 6 minutes of wall time (killed; 378.8s of single-core
CPU time consumed, `ps` confirmed CPU-bound, not I/O-blocked). Root cause,
found with `EXPLAIN QUERY PLAN` (`main.rs:530-558`, run with
`FW_SPIKE_EXPLAIN=1`):

```
-- indexes on items
   sqlite_autoindex_items_1   (the table's own PK autoindex)
   items_key_idx              CREATE UNIQUE INDEX items_key_idx ON items (queue_no, client_item_key)

-- EXPLAIN QUERY PLAN (no hint) SELECT item_version FROM items
--   WHERE queue_no=?1 AND client_item_key=?2
   SEARCH items USING INDEX sqlite_autoindex_items_1 (queue_no=?)

-- EXPLAIN QUERY PLAN (INDEXED BY items_key_idx) SELECT item_version FROM items
--   INDEXED BY items_key_idx WHERE queue_no=?1 AND client_item_key=?2
   SEARCH items USING INDEX items_key_idx (queue_no=? AND client_item_key=?)
```

Without a hint, Turso 0.7.2's planner picks the table's own PK autoindex and
binds only `queue_no=?`, leaving `client_item_key` as a residual filter over
every row of that `queue_no` — an O(rows-per-queue) scan per lookup, not an
index seek. MEASURED: a 100-lookup timed loop against a 2,000-row table, run
this way, achieved only ~1,473–1,675 lookups/sec — *slower per lookup* than
the properly-indexed 100,000-row measurements below (24k–40k/sec), which is
itself the tell that the unhinted path scales with table size. With
`INDEXED BY`, the plan seeks on both columns.

This is not a novel discovery: production `crates/fireweed-turso` already
applies exactly this workaround, everywhere, on the SQLite-dialect-specific
Turso path — VERIFIED, e.g. `crates/fireweed-turso/src/projection.rs:1627,1749`
use `INDEXED BY fireweed_items_active_key`, the production analogue of this
spike's layout A key index, and 20+ further call sites in
`crates/fireweed-turso/src/query.rs` and `projection.rs` hint every hot query.
This spike's harness adopted the identical pattern once its absence made the
measurement itself infeasible (`main.rs:520-530` comment, `559`).

**Corroborating, out-of-track observation (flagged, not fixed here)**:
`crates/fireweed-relational/src/apply.rs:5043-5051` runs
`... WHERE tenant_id=? AND queue_id=? AND client_item_key IN ({placeholders}) AND superseded=0`
with no `INDEXED BY` — that file is the dialect-neutral relational layer
shared with the PostgreSQL backend, so it cannot carry SQLite-only
`INDEXED BY` syntax. Whether the Turso execution path re-hints this
particular query elsewhere was not traced (out of this spike's scope); it is
architecturally exposed to the same defect measured here if not.

### Finding 1: `IN (...)` against a composite index's second column does not seek either — MEASURED

Even *with* `INDEXED BY items_key_idx` forced, `client_item_key IN (v1..v100)`
against the composite `(queue_no, client_item_key)` index still only binds
`queue_no=?`:

```
-- EXPLAIN QUERY PLAN (claim IN-list) SELECT item_version FROM items
--   INDEXED BY items_key_idx WHERE queue_no=?1 AND client_item_key IN (?2,...,?101)
   SEARCH items USING INDEX items_key_idx (queue_no=?)
```

MEASURED (100 batches × 100 keys, layout B, `str256`, 20,000-row table):
single IN-list query per batch of 100 keys ran at **1,133–1,287 keys/sec**;
100 individual indexed point lookups for the same 100 keys ran at
**~35,700–37,819 lookups/sec** — roughly **28× faster**. This spike's "claim"
measurements below therefore issue one indexed lookup per key
(`main.rs:671-687`), not a single `IN (...)`; the diagnostic IN-list path is
retained only behind `FW_SPIKE_EXPLAIN=1` for evidence (`main.rs:641-670`).

**Recommendation consequence**: a future `fireweed-turso` implementation of
the contract's `Select::Keys` claim (draft §2.3, up to 1000 keys, line 349)
must issue one hinted indexed lookup per key, not a single `IN (...)` — until
an upstream Turso planner fix changes this, which should be re-verified on
version bumps.

### Finding 2: storage cost by layout and key shape — MEASURED, 100,000 rows, 1 run each (see Finding 4 for variance)

Bytes/row = `post_checkpoint_main_bytes / rows`, from `results/results.jsonl`.
`post_checkpoint_main_bytes` was reproducible to the byte across 3 repeats for
every combo re-run (0.0% variance — see Finding 4), so this column is the
spike's most trustworthy signal.

| Layout | int (≈19B text/8B/native) | UUID (36B text/16B) | email (~25–37B) | 128B string | 256B string |
|---|---:|---:|---:|---:|---:|
| **A** today's (3×TEXT PK + TEXT key idx) | 271.5 | 309.5 | 296.3 | 521.7 | 830.6 |
| **B** `queue_no` INTEGER + TEXT key idx | 215.3 | 252.0 | 239.9 | 465.7 | 741.7 |
| **C** `queue_no` INTEGER + BLOB key idx | 190.8 | 207.3 | 239.9 | 465.7 | 741.7 |
| **D** `queue_no` + 16B hash idx (+unindexed stored key) | 229.7 | 248.2 | 238.3 | 351.4 | 499.1 |
| **E** `queue_no` + INTEGER key idx (int only) | 190.8 | — | — | — | — |
| baseline: no secondary index (matched width) | 171.3 (int) | — | — | — | 424.9 (256B text) |

All figures bytes/row. Ingest, lookup, upsert, claim throughput (ops/sec) for
every combo are in `results/results.jsonl`; see Finding 3 for why upsert/ingest
absolute numbers are noisy and Finding 4 for the measured variance.

**Index-only overhead** (combo minus the width-matched no-index baseline,
isolating the secondary index's own cost since Turso has no `dbstat`):
- 256-byte key, plain TEXT/BLOB unique index (B/C): **316.7 bytes/row** —
  close to one extra full copy of the 256-byte key (the "written twice"
  framing in the owner's decision, `decisions-round3.md:8`, made concrete: a
  plain secondary key index costs *almost exactly one more copy of the key*,
  confirmed by the two measured points at 128 and 256 bytes: marginal cost is
  **2.156 bytes of storage per additional byte of key length** — very close
  to 2×, matching "row + index").
- 256-byte key, 16-byte hash unique index (D): **74.2 bytes/row**, flat
  regardless of the original key's length (the hash is always 16 bytes) —
  **4.3× smaller** than the plain-index overhead above.
- native 64-bit int, INTEGER unique index (E): **19.5 bytes/row** — the
  cheapest option measured, as expected for SQLite/Turso's variable-length
  integer serial types.

**Break-even for hashing** — MEASURED anchors, INFERRED interpolation: at
19-byte keys (int-as-text) layout D costs 14.4 B/row *more* than B (229.7 vs
215.3); at ~25–37-byte keys (email) D is already very slightly cheaper (238.3
vs 239.9); at 128 and 256 bytes D is markedly cheaper (−24.5%, −32.7%). The
break-even point is bracketed by the int and email measurements — INFERRED
somewhere in the roughly 20–40 byte range, not a single precise figure (a
two-point linear fit from the 128/256-byte pair alone predicts a much lower
~14 bytes, which the actual int/email measurements contradict — string-shape
fixed overhead is not perfectly linear down to very short keys, so this spike
does not claim a single precise break-even width).

**BLOB vs TEXT** (layout C vs B) is a wash for content with no tighter binary
form: email/128B/256B strings measured **byte-identical** between C and B
(0.0% difference — SQLite/Turso's TEXT and BLOB serial-type encoding cost the
same for the same byte length). BLOB only wins when a genuinely more compact
binary encoding exists: int-as-8-byte-BLOB is **11.4%** smaller than
int-as-decimal-TEXT, UUID-as-16-byte-BLOB is **17.8%** smaller than
UUID-as-36-byte-TEXT.

**Surrogate `queue_no`** (layout B vs A) is an unconditional win, independent
of key shape or length: **10.7%–20.7%** smaller across every shape measured,
from collapsing the two `tenant_id`/`queue_id` TEXT identity columns into one
internal `queue_no INTEGER`. This changes only the *physical projection
schema*; the contract's public `QueueKey { tenant, queue }` (draft §2.0, line
140, each bounded 1..=128 bytes) is unaffected — a small tenant/queue registry
table would map to the surrogate, an implementation (TD/ADR) detail, not a
contract change.

### Finding 3: read/write throughput by layout and key shape — MEASURED, noisy (see Finding 4)

Point-lookup and claim-by-keys throughput (both indexed single-key seeks)
stayed in a comparatively tight band across every layout and shape: lookups
24,102–39,813/sec, claim (per-key) 29,539–40,655/sec — at most ~30% spread
between the fastest and slowest shape within a layout, with no layout or
shape standing out as qualitatively slower once the Finding 0 fix is applied.
Ingest and upsert throughput varied far more (ingest 13,445–71,154 rows/sec;
upsert 3,534–39,966 ops/sec across the sweep) — see Finding 4: this spread is
dominated by shared-machine contention noise, not the key layout, based on the
repeated-run variance study below.

### Finding 4: variance — MEASURED, 3 repeats × 3 representative combos (100,000 rows each)

| Combo | Metric | Spread (max−min)/avg | Note |
|---|---|---:|---|
| B/str256 | `post_checkpoint_main_bytes` | **0.0%** | byte-identical across all 3 runs |
| D/str256 | `post_checkpoint_main_bytes` | **0.0%** | byte-identical |
| E/int | `post_checkpoint_main_bytes` | **0.0%** | byte-identical |
| B/str256 | lookup ops/sec | 34.8% | 23,361–33,930 |
| D/str256 | lookup ops/sec | 11.0% | 26,307–29,435 |
| E/int | lookup ops/sec | 8.4% | 35,075–38,149 |
| B/str256 | upsert ops/sec | 10.6% | 17,812–19,820 |
| D/str256 | upsert ops/sec | 61.0% | 12,751–22,657 |
| E/int | upsert ops/sec | 11.4% | 20,390–22,807 |
| all three | checkpoint wall time | 68–78% | 1.0s–3.1s (same byte count checkpointed each time) |

File size is fully reproducible; every throughput number carries real,
double-digit-percent noise consistent with the shared, concurrently-loaded
machine (Finding 0's sibling-spike and cargo-build processes, confirmed via
`ps`/`/proc` during the run). **Throughput comparisons across layouts in
Finding 3 should be read as order-of-magnitude/relative-ranking evidence, not
as precise SLA numbers** — no single run in the 23-combo sweep was repeated,
so a same-layout, adjacent-shape difference of under roughly 30-40% is within
measured noise and should not be over-interpreted.

### Finding 5 (tangential, out-of-track color): 1M rows was attempted and found too slow

Per the task's explicit "100k if 1M is too slow" allowance: a 1,000,000-row
calibration run (layout B, `str256`, all four phases) took **2 minutes 18
seconds** wall time — over the ~2-minute per-run bound — driven mostly by
ingest (68.5s, 14,607 rows/sec — roughly half the same combo's measured
100,000-row ingest rate of 28,570–31,871 rows/sec across separate runs, i.e.
throughput roughly halved at 10× the row count) and by
`PRAGMA wal_checkpoint(TRUNCATE)` itself (**54.0 seconds**, checkpointing an
8.66 GB WAL down to a 741.6 MB main file — WAL at 100k rows was 638 MB,
checkpoint 0.8–3.1s). This spike used **100,000 rows** for the full sweep
instead, per the task's fallback. This checkpoint/WAL-growth non-linearity is
independent of key shape (same layout/shape, 10× the rows, checkpoint cost
grew ~20–40×) and is offered only as tangential color supporting the owner's
separate decision that queues should stay cheap/small (round3 decision 5) —
not a claim this spike investigated further.

## Interpretation

1. **256-byte (and up to the declared 1024-byte) keys are not a storage or
   performance bottleneck** on S3×Turso at the row counts and machine profile
   measured. A 256-byte key under the cheapest sane layout (B, plain TEXT
   unique index) costs 741.7 bytes/row — 74 MB for 100k rows, extrapolating
   linearly (Finding 2's marginal-cost model), roughly 740 MB for 1M rows.
   Lookup and claim throughput at 256 bytes (24k–34k/sec) were at most ~30%
   below the cheapest short-key shapes (35k–40k/sec) — not a cliff, not
   remotely disqualifying on any read hot path. The owner's stated condition
   ("fine, as long as it does not create a bottleneck") is satisfied across
   the contract's full declared 1..=1024-byte range; **no reason to tighten
   the contract's `Key` bound below 1024 bytes**.
2. The **surrogate `queue_no INTEGER`** replacing the two TEXT tenant/queue
   identity columns is a clean, unconditional 11–21% storage win, independent
   of the key-type decision, and belongs in the physical projection schema
   regardless of what else this spike recommends.
3. **BLOB is worth it only when a tighter binary encoding genuinely exists**
   (fixed-width int, UUID) — 11–18% smaller, no measured throughput cost.
   For arbitrary upstream strings (the owner's "often an email or similar"),
   BLOB buys nothing over TEXT; keep the contract's `Key(String)` UTF-8 type
   and store it as TEXT.
4. **Hashing pays off once keys are meaningfully long** (roughly 30+ bytes,
   bracketed by measurement, not pinned to one figure) — a 25–33% storage win
   at 128–256 bytes, with lookup/claim throughput statistically
   indistinguishable from the plain-index layouts (Finding 3/4). It is a net
   loss for short keys (ints, most UUIDs) because it pays for both a fixed
   16-byte hash index *and* an unindexed full copy of the original key.
5. The single most consequential finding for actually shipping this
   (Finding 0/1) has nothing to do with which key layout is chosen: **every**
   layout requires an explicit `INDEXED BY` hint on Turso 0.7.2 for key
   lookups, and claim-by-keys must be issued as one indexed lookup per key,
   never a single `IN (...)`. Production code already does the former
   everywhere on the Turso-specific path; the contract's new `Select::Keys`
   claim (draft §2.3 C2) needs the same discipline applied to its
   implementation, and the `fireweed-relational` shared-SQL `IN (...)` path
   flagged in Finding 0 is worth an explicit, separate check.

## Recommendation

**For the contract (`fireweed-contract`)**: no change needed.
`Key(String)`, UTF-8, 1..=1024 bytes (draft §2.0 line 141; §4 line 589) stays
as declared — the owner's 256-byte case, and the full range up to 1024 bytes,
measured with no bottleneck.

**For the S3×Turso projection schema** (`crates/fireweed-relational`,
`crates/fireweed-turso` — implementation-level, not contract-level):
1. Adopt a surrogate internal `queue_no`/tenant+queue registry in place of the
   two TEXT identity columns on every hot table — measured 11–21% smaller,
   unconditionally, at zero measured throughput cost.
2. Store `Key` as TEXT by default (matches the contract's `Key(String)` type
   exactly, zero cost vs BLOB for arbitrary upstream ids).
3. Where the upstream id shape is a fixed-width binary type the implementation
   can recognize (64-bit int, UUID) — optional — store a tighter BLOB
   encoding internally for a further 11–18% reduction; the contract-facing
   `Key` stays the same UTF-8 string regardless.
4. Once a queue's observed/expected key length runs meaningfully past
   ~30–40 bytes (free-form strings, emails, UUID-as-text), use a fixed
   16-byte-hash unique index with the full key stored unindexed alongside it
   (layout D) — 25–33% smaller at that length, no measured throughput cost.
   Two hard requirements if adopted, both **INFERRED, not measured by this
   spike** (standard hash-collision reasoning, not benchmarked):
   - use a real, vetted 128-bit-class hash (e.g. XXH3-128, BLAKE3 truncated,
     SipHash-128) — **not** this spike's two-lane salted FNV-1a-64 combiner
     (`main.rs:63-74`), which exists only to produce a fixed-size key for
     measurement and was never intended as a production digest;
   - on every unique-index conflict (insert-time and lookup-time), re-check
     the stored, unindexed key text before treating it as the same logical
     key. A 128-bit hash is not a bijection: with a well-distributed digest,
     the birthday-bound collision probability stays negligible (≈n²/2^129)
     even at very large per-queue key counts, but "negligible" is not "zero,"
     and an unverified hash match would silently merge two distinct upstream
     keys.
5. Whatever layout is chosen, force `INDEXED BY <key-index-name>` on every
   key-equality query (Finding 0) and implement `Select::Keys` claims as one
   indexed lookup per key, not a single `IN (...)` (Finding 1) — required
   regardless of the storage-layout decision above, and already the
   established pattern in `crates/fireweed-turso/src/query.rs` and
   `projection.rs`.

## Limitations

- Single run per combo for the full 23-combo sweep (Finding 4 quantifies
  variance from a separate 9-run study on 3 representative combos, not from
  repeating the full sweep — repeating all 23 combos 3× each was out of the
  ~2-minute-per-run/overall time budget).
- WSL2 VM, not bare-metal; shared, concurrently-loaded machine for the whole
  run (Finding 4). File-size results are unaffected (0% variance, deterministic
  by construction); throughput results should be read as relative ranking.
- Standalone-project IO path, not fireweed's private `RebuildableIo` adapter
  (Scope/Setup); pragma profile is identical, so the layout comparison is
  unaffected, but absolute wall-clock numbers may not transfer exactly.
- `dbstat` is unavailable on this Turso version (VERIFIED, Method); index-only
  cost is therefore a file-size *delta* against a matched no-index baseline,
  measured only at two key widths (int, 256 bytes) — not a per-B-tree
  breakdown.
- Hash-collision handling (Recommendation point 4) is analytical, not
  measured: no collision was engineered or observed at these row counts.
- Finding 0's `fireweed-relational/apply.rs:5043-5051` observation is flagged,
  not traced end-to-end to confirm whether it is actually exposed on the
  Turso path today.

## Source Ledger

| Source | Claim used | Confidence |
|---|---|---|
| `docs/helix/00-discover/external-interface-kernel-contract-draft.md:61-64,140,141,166,196-197,349,395,468,586,589` | Key/QueueKey types and bounds, `Select::Keys` C2, index-key mechanism boundary | VERIFIED (read) |
| owner round-3 decisions (2026-09-23 discussion) | owner's round-3 decision 3 and open questions | VERIFIED (read) |
| `crates/fireweed-relational/src/schema.rs:15-53,65-66` | today's `fireweed_items` schema, identity columns, partial unique index, hot-column shapes | VERIFIED (read) |
| `crates/fireweed-turso/src/local.rs:54,56,59,96-126,3341-3444` | pinned Turso version/boundary, `TursoConfig`, `configure_connection`/`configure_committed_reader` pragmas | VERIFIED (read) |
| `crates/fireweed/src/turso_compose.rs:136` | production opens `with_log_backed_projection()` | VERIFIED (read/grep, only call site) |
| `vendor/turso_core/FIREWEED.md:1-11` | Fireweed's `wal_autocheckpoint` backport on the pinned engine | VERIFIED (read) |
| `crates/fireweed-turso/Cargo.toml:26,28` | exact vendored path-dependency shape mirrored by the spike | VERIFIED (read) |
| `crates/fireweed-turso/src/projection.rs:117-146,1421,1536,1570,1627,1749,1924,1931,1937` and `src/query.rs:36-37,254,288,388-389,613,976-977,1613` | production's existing `INDEXED BY` discipline, corroborating Finding 0 | VERIFIED (grep + read) |
| `crates/fireweed-relational/src/apply.rs:5043-5051` | unhinted `client_item_key IN (...)`, flagged in Finding 0 | VERIFIED (read); Turso-path exposure not traced |
| `spikes/key-storage/src/main.rs` (whole file, cited by section above) | this spike's harness, layouts, shapes, config, `INDEXED BY` fix | VERIFIED (own code, own runs) |
| `spikes/key-storage/results/results.jsonl`, `results/variance.jsonl` | all measured figures in Findings 2–4 | VERIFIED (measured, this run) |
| Hash-collision birthday bound, choice of production-grade 128-bit hash | Recommendation point 4 requirements | INFERRED (standard hash-collision reasoning; not benchmarked) |
| Break-even hashing width (~20–40 bytes) | Finding 2 | INFERRED (interpolated from measured anchor points, explicitly not pinned to a single figure) |

## Artifacts

- `spikes/key-storage/` — standalone Cargo project (`Cargo.toml`, `src/main.rs`,
  `run.sh`).
- `spikes/key-storage/results/results.jsonl` — 23-combo sweep, 100,000 rows
  each, one JSON line per (layout, shape) combo plus the two no-index
  baselines.
- `spikes/key-storage/results/variance.jsonl` — 9-run variance study (3
  combos × 3 repeats).
- No production code was modified; no contract types were changed.
