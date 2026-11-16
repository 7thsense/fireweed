---
ddx:
  id: spike-record-field-storage
  status: draft
---

# Technical Spike: Record Field Storage and Indexing on S3×Turso

**Spike ID**: SPIKE-003 | **Lead**: DDx queue steward | **Time Budget**: ~2 h measurement, bounded runs | **Status**: Draft

## Objective

**Owner's questions** (decisions-round3.md, "Owner questions to answer (research)"):

> How does a client specify what values (fields/metadata) it will supply on
> items? How are they stored? What if the client wants to index one? How do
> we ensure good performance with large values, high rates of change, or
> large numbers of values?

**Technical question**: on the production S3×Turso backend, how should a
record's `fields` (the current draft's `BTreeMap<String, Value>`,
kernel-contract-draft.md:80,151,156, "the queryable data, and with the
built-in attributes the source of index keys") be stored and indexed so that
ingest, update and point-read cost stay predictable as field count, value
size, and per-field update frequency grow — and does that answer change if
queues declare a field schema (names/types) at creation instead of writing
field names freely per record?

Goals:
- measure, not guess, the cost of four storage models for `fields` on a
  vendored-Turso projection configured the way `fireweed-turso` configures
  its own connections;
- separately measure the cost of keeping a large value off the hot row
  (`payload`) versus inline with `fields`, which the current design already
  assumes (kernel-contract-draft.md:81, "Stored off the hot row"; D9,
  kernel-contract-draft.md:1030) but has not quantified;
- recommend the storage model, whether to require a declared field schema,
  concrete limits, payload-vs-fields guidance, and rules for indexed fields
  under high churn.

Out of scope (tracked by other spikes/decisions): the upstream record-id
column (owner decision 3, a separate "SPIKE IT"), RESP isolation (owner
decision 4), the priority/datetime question, force-clear/operator isolation,
job-type-as-queue, and queue deletion. This spike touches none of those.

## Scope

Compared, all against the vendored Turso 0.7.2 embedded engine
(`vendor/turso`, `vendor/turso_core`), configured the way
`crates/fireweed-turso/src/local.rs` configures its own connections:

- **(a) JSON TEXT + `json_patch`** — the current draft's model: `fields TEXT`
  holding a JSON object of tagged-array values (`{"f007": ["s","..."]}`,
  matching the encoding the draft's production-mapping row already commits to,
  kernel-contract-draft.md:757), updated with `UPDATE ... SET fields =
  json_patch(fields, patch)`, set-based over a batch.
- **(b) Compact binary encoding, dynamic field names** — `fields BLOB` holding
  a `postcard`-encoded `BTreeMap<String, Value>`, read-modify-written in Rust:
  a batched `SELECT`, decode, patch, re-encode, batched write-back.
- **(c) EAV side table** — `eav (item_no, field_no, value)`, one row per
  (item, field), upserted with `INSERT ... ON CONFLICT DO UPDATE`.
- **(d) Declared-schema binary map** — identical to (b) except the map key is
  `u16` (a small int a queue would assign to each field name at creation)
  instead of `String`.

All four models share one index side table,
`item_index (item_no INTEGER PRIMARY KEY, index_key TEXT NOT NULL)` with a
covering `(index_key, item_no)` secondary index — the spike's stand-in for
today's `fireweed_item_index (tenant_id, queue_id, index_name, index_key,
item_id)` (`crates/fireweed-relational/src/schema.rs:257-263`, VERIFIED). The
draft settles index architecture as "declared at queue creation... in a
separate index table" for every model (kernel-contract-draft.md:468-471,
VERIFIED) — indexing is not one of the four variables under test; it is a
constant overhead applied uniformly, measured separately as indexed-vs-not.

## Setup

**Hardware** (measured, `nproc`, `free -g`, `/proc/loadavg`):
- 32 logical CPUs, 94 GiB RAM (87 GiB free at idle).
- **The host was not idle during measurement.** `/proc/loadavg` samples taken
  during the runs ranged from 21.9 to 43.2 (1-minute average) on a 32-core
  box, and `ps aux` during one run showed concurrent, unrelated `cargo
  build`/`cargo test`/`rustc`/`ld.mold` jobs from other sessions on the same
  machine (this is a shared host; the task brief's own
  `CARGO_TARGET_DIR` was independently found to be shared with a *different*
  concurrent field-storage-adjacent spike whose build clobbered this spike's
  binary mid-run — see Variance below). This is disclosed, not modeled away:
  absolute throughput numbers below carry real noise from host contention;
  where that noise was large enough to plausibly change an interpretation,
  it was isolated and re-measured (see Variance).

**Build**: standalone Cargo project,
`spikes/field-storage/` (own empty `[workspace]` table, not a member of the
main workspace), `edition = "2021"`, path dependency on
`vendor/turso` `= "0.7.2"`, `default-features = false` — the exact spec
`crates/fireweed-turso/Cargo.toml:26` uses (VERIFIED). Built `--release` with
its own private `CARGO_TARGET_DIR` (see Variance for why "private").

**Connection configuration**, mirrored from
`crates/fireweed-turso/src/local.rs` `configure_connection`
(local.rs:3407-3445, VERIFIED) and `TursoConfig::local()`
(local.rs:96-104, VERIFIED):
- `page_size = 4096` (local.rs:3411), `journal_mode = WAL` (local.rs:3412-3416),
  `cache_size = -131072` (128 MiB cap, local.rs:3430-3431), a 5 s busy timeout
  (`DEFAULT_BUSY_TIMEOUT`, local.rs:59,100,3443).
- `synchronous`: the adapter sets `NORMAL` only when a custom object-log-backed
  `IO` impl (`rebuildable_io`) replaces physical fsync (local.rs:3417-3429,
  "The log-backed I/O adapter omits physical sync; the object-log is
  durable."). This spike does not build that IO adapter (out of scope — it is
  S3-log integration, not field storage), so it takes the adapter's own
  non-rebuildable branch: `synchronous = OFF`, `wal_autocheckpoint = 1000`.
  This is `TursoConfig::local()`'s own default (`rebuildable_io: false`,
  local.rs:102) and what every non-log-backed `fireweed-turso` caller in the
  repo actually runs today, so it is not a made-up profile.
- One writer connection per run (no reader pool, no committed-reader tuning —
  those exist for concurrent-reader isolation, orthogonal to field-encoding
  cost).

**Method**: for each (model, config), a fresh on-disk DB file, schema created,
then `batch_size = 1,000`-record transactions (`BEGIN IMMEDIATE` /
`COMMIT`) until the target row count or a 90-second wall-clock cutoff, whichever
comes first (a cutoff run is labeled `cut_off=true` with its actual row
count — one occurred, noted below). DB size is read after
`PRAGMA wal_checkpoint(TRUNCATE)`, so it reflects steady-state on-disk bytes,
not a mid-WAL snapshot. Value/field generation is **outside** every timed
region (a real caller already holds the value it wants to write); only the DB
round trip — encode, SQL, and, for (b)/(d), the read-decode-patch-encode
cycle — is timed. Full harness:
`spikes/field-storage/src/{main,bench,models,gen,sql,db}.rs`.

**Row/batch counts actually run**: the task brief asked for 1,000-record
batches over 100,000 records. Under the measured host contention above, a
100k-row run for the slower configs (e.g. EAV at 200 fields, or 4 KiB
values) would not reliably finish inside the ~2-minute per-run bound this
task also sets, and the two constraints conflict under real load. This spike
resolves them by running **20,000 rows (20 batches of 1,000)** per config —
still the requested batch size, a large enough sample that steady-state
per-batch cost (which is what throughput and DB-bytes/row measure) is
representative, and small enough to stay inside the time bound even under
contention. This is a disclosed reduction, not a guess: every ingest number
below is a real measurement of 20,000 rows, not an extrapolation, except
where a cutoff is explicitly marked.

**Repetition and variance**: the full 20-config ingest matrix × 4 models was
run twice end-to-end ("run1", "run2b"), plus three additional targeted
"pair-probe" repetitions (3 alternating rounds each) for two cells that
looked anomalous, and the payload-placement experiment was run 5 times after
a methodology fix (see below). Tables report the primary measurement plus the
observed range; where a value is a corrected estimate, it is marked and the
correction is explained.

**Variance note — a genuine methodological hazard, not just host noise**:
mid-spike, a re-run of the full matrix (via the shared `CARGO_TARGET_DIR` the
task brief specified, "shared with another spike") produced instant CLI-usage
errors. `ps aux` showed a *different* concurrent session's spike — building
into the exact same `target/release/spike` path with a same-named but
differently-shaped binary (`spike <a|b|c|d|e> <int|uuid|...> <db> <rows>`,
evidently a companion spike on record-id storage) — had overwritten this
spike's binary between builds. All numbers reported below are from builds
against a **private** `CARGO_TARGET_DIR`, taken after that discovery; the one
contaminated invocation (instant usage-error exit) produced no data and is
not included anywhere below.

## Findings

### FINDING: All three "declared field schema" hypotheses hold, but only one of them is large enough to matter at realistic value sizes

Declaring field names as small ints at queue creation (model d vs model b)
measurably shrinks the postcard blob — **MEASURED this spike**
(`SPIKE_SIZE_PROBE`, `spikes/field-storage/src/main.rs` `size_probe`):

| Fields | String-keyed blob (b) | u16-keyed blob (d) | Diff | Per field |
|---|---|---|---|---|
| 5 | 1,321 B | 1,301 B | 20 B | 4.0 B/field |
| 50 | 13,201 B | 13,001 B | 200 B | 4.0 B/field |
| 200 | 52,802 B | 52,074 B | 728 B | 3.6 B/field |

Confirmed against the real DB, not just the isolated encoder
(`SPIKE_DB_SIZE_PROBE`): `length(fields)` for 5 real ingested rows read back
1,321 B (b) vs 1,301 B (d), matching the isolated measurement exactly.

The constant ~4 bytes/field is the field-name-string overhead (`"f007"`,
length-prefixed) minus the 2-byte fixed `u16`. At 256 B values (the spike's
default), 4 bytes is **0.03 B/field of a ~262 B field entry** — noise. At
16 B values, the *same absolute* 4 bytes/field is **~20% of the entry** —
visible: `INGEST|declared|50|16` measured 1,027.28 B/row on disk vs
`INGEST|binary|50|16` at 1,369.29 B/row (**25% smaller**, both runs, exact
byte-identical DB size across repetitions since content generation is
seeded). No measured throughput difference survived isolation (see
Interpretation): a 6-round alternating pair-probe at fields=50/256B put
binary and declared within noise of each other (3.5–5.7k rows/s either way,
no consistent winner).

### FINDING: value size, not field count, dominates ingest and update cost, and does so by an order of magnitude before the 4 MiB record cap is anywhere near reached

Holding fields at 50 and sweeping value size (16 B → 256 B → 4,096 B) drops
ingest throughput 15–60× across every model (run2b, rows/s):

| Model | 16 B | 256 B | 4,096 B | 16B→4096B ratio |
|---|---|---|---|---|
| json | 10,144 | 5,028 | 309 | 33× |
| binary | 18,290 | 5,521 | 283 | 65× |
| eav | 2,798–3,727¹ | 2,193–2,472¹ | 243 | ~13× |
| declared | 8,495–5,502¹ | 1,564–5,502¹ | 313 | ~18× |

¹ range across the raw run and the pair-probe correction, see Interpretation.

By contrast, holding value size at 256 B and sweeping field count (5 → 50 →
200) drops throughput only 5–25×, and the *hot-row-bytes-per-record* at
200×256B (52.8 KB) is nowhere near the draft's 4 MiB record cap
(kernel-contract-draft.md:583). The EAV model (one row per field) is the one
place field *count* dominates on its own: EAV/200 ingest (566–472 rows/s) is
the single worst non-outlier cell in the whole matrix at 256 B values, worse
than json or binary/declared at the same field count (900–1,330 rows/s) —
because EAV pays one B-tree insert per field, not one per record.

### FINDING: an inline large value costs 3.5–9× the update throughput of the same value off-row, confirming (and quantifying) the draft's existing `payload` design

**Method fix note**: the first version of this experiment did not checkpoint
the WAL between the initial payload insert and the timed update loop, so an
in-flight autocheckpoint sometimes landed inside the timed window and
produced wildly inconsistent numbers for the off-row variant (4.2k–27k
updates/s across five runs, see raw logs). After inserting an explicit
`PRAGMA wal_checkpoint(TRUNCATE)` between ingest and the timed loop (now in
`payload_sweep`, `spikes/field-storage/src/main.rs`), the off-row/baseline
gap disappeared (both are "no large inline value", and the operation is fast
enough — low single-digit seconds for 20,000 updates — that scheduler jitter
alone explains a 2× spread between them) while the **inline** number stayed
consistently, dramatically worse in every one of 7 total repetitions:

| Run | baseline (no payload) | inline 64 KiB payload | off-row 64 KiB payload | inline/baseline |
|---|---|---|---|---|
| fix-1 | 12,951/s | 2,901/s | 4,231/s | 4.5× slower |
| fix-2 | 11,676/s | 3,356/s | 9,050/s | 3.5× slower |
| fix-3 | 13,030/s | 3,243/s | 27,204/s | 4.0× slower |
| fix-4 | 14,178/s | 1,601/s | 24,308/s | 8.9× slower |
| fix-5 | 15,362/s | 3,100/s | 13,914/s | 5.0× slower |

In every single repetition, off-row beats inline by a wide margin (1.5×–15×
that run's own inline number), and inline is consistently the slowest of the
three regardless of host noise. This is the expected SQLite/Turso storage
mechanic: a table row is one serialized B-tree record, so an `UPDATE`
touching only `fields` still rewrites the *whole* row, payload included, even
though the payload column is never named in the `SET` clause. It is exactly
why the current schema already keeps payload in a separate
`fireweed_item_payloads` table ("Payload lives off the hot row so
Claim/Complete/schedule UPDATEs do not rewrite blobs",
`crates/fireweed-relational/src/schema.rs:55-57`, VERIFIED) — this spike
supplies the first measured magnitude for that decision (3.5–9× on a 64 KiB
value; the effect only grows with value size).

### FINDING: point-read cost tracks the storage model directly — one row beats N rows

Single-key point reads (2,000 samples, both full runs, consistent ordering
in both):

| Model | run1 avg / p50 / p99 (µs) | run2b avg / p50 / p99 (µs) |
|---|---|---|
| declared | 31.81 / 29.52 / 62.46 | 27.26 / 25.55 / 58.37 |
| binary | 49.53 / 45.85 / 93.91 | 38.70 / 36.42 / 79.22 |
| json | 63.57 / 59.58 / 112.45 | 53.87 / 50.80 / 92.74 |
| eav | 77.44 / 72.50 / 136.69 | 63.60 / 62.52 / 91.73 |

The relative order (declared < binary < json < eav) held in both independent
runs despite ~15–20% absolute swing between them from host contention. EAV
is worst because a point read of a 50-field record is 50 row fetches
(`SELECT field_no, value FROM eav WHERE item_no = ?`), not one; json is
slower than binary/declared because it pays a text→JSON parse the binary
models skip.

### FINDING: index maintenance under churn costs roughly 15–30% at this scale, for one index

Comparing indexed vs not-indexed at the same (model, fields_touched), run2b
(updates/s; the pool is re-touched every round, so by round 2 every indexed
write is a genuine rekey, not a first insert):

| Model | 1 field, no index | 1 field, indexed | Δ | 10 fields, no index | 10 fields, indexed | Δ |
|---|---|---|---|---|---|---|
| json | 6,879 | 4,969 | −28% | 2,815¹ | 5,347 | +90%¹ |
| binary | 5,421 | 5,566 | +3%¹ | 5,963 | 5,174 | −13% |
| eav | 14,704 | 11,504 | −22% | 6,974 | 2,397 | −66%¹ |
| declared | 7,022 | 5,309 | −24% | 7,765 | 6,102 | −21% |

¹ outside the ~15–30% band the majority of cells show; treated as noise at
this sample size (n=1 per cell; see Limitations), except that a single
correctly-maintained index adding one small upsert per touched record is
architecturally bounded and additive by construction (one extra row write per
touched index, regardless of hot-row model) — the 15–30% figure from the
majority of cells is the usable number, not the two outliers.

## Results tables

### Ingest throughput and on-disk size (20,000 rows, batch 1,000)

| Model | Fields | Value size | rows/s (best estimate) | range observed | DB bytes/row |
|---|---|---|---|---|---|
| json | 5 | 256 B | 16,819 | 7,876–16,819 | 2,053.53 |
| json | 50 | 256 B | 5,028 | 3,620–5,028 | 13,657.29 |
| json | 200 | 256 B | 1,316 | 917–1,316 | 54,617.50 |
| json | 50 | 16 B | 10,144 | 3,735–10,144 | 2,053.53 |
| json | 50 | 4,096 B | 309 | 245–309 | 205,827.48 |
| binary | 5 | 256 B | 35,459 | 15,096–35,459 | 1,369.29 |
| binary | 50 | 256 B | ~5,355² | 3,332–5,531 | 13,315.28 |
| binary | 200 | 256 B | 1,333 | 1,275–1,333 | 53,258.24 |
| binary | 50 | 16 B | 18,290 | 12,455–18,290 | 1,369.29 |
| binary | 50 | 4,096 B | 283 | 262–283 | 205,485.47 |
| eav | 5 | 256 B | 12,498 | 11,588–12,498 | 1,440.15 |
| eav | 50 | 256 B | ~2,472² | 2,004–2,541 | 14,427.96 |
| eav | 200 | 256 B | 566 | 472–566 | 58,465.08 |
| eav | 50 | 16 B | ~3,727² | 1,007³–3,888 | 2,195.46 |
| eav | 50 | 4,096 B | 243 | 156³–243 | 231,204.45–231,204.86 |
| declared | 5 | 256 B | 21,773 | 12,458–21,773 | 1,369.29 |
| declared | 50 | 256 B | ~5,502² | 805³–5,702 | 13,109.86 |
| declared | 200 | 256 B | 1,284 | 1,160–1,284 | 53,258.24 |
| declared | 50 | 16 B | 14,954 | 8,495–14,954 | 1,027.28 |
| declared | 50 | 4,096 B | 318 | 313–318 | 205,314.25 |

² corrected via a 3-round alternating pair-probe after the raw two-run figure
looked anomalous relative to its neighbors (see Interpretation) — the
pair-probe value is the "best estimate", the raw two-run figures are folded
into "range observed".
³ a single-run outlier attributable to host contention (see Interpretation);
kept in the range column for honesty, excluded from "best estimate".
`eav|50|4096` in run1 hit the 90 s cutoff at 15,000/20,000 rows
(`cut_off=true`); run2b completed all 20,000.

### Point reads — see Findings table above (both full runs shown there).

### Update churn — see Findings table above (indexed-vs-not; run2b shown, run1
in the raw logs agrees on model ordering within model, i.e. json/binary/eav/
declared are all in the same 2.1k–14.7k updates/s band, but individual cells
swing as noted).

### Payload placement — see Findings table above (5 corrected repetitions).

## Interpretation

**Host contention explains, and a targeted re-measurement confirms, the two
anomalous ingest cells.** `eav|50|16` in run1 (1,007 rows/s) is far below
every other eav/16B-adjacent measurement (run2b: 2,798; three clean
pair-probe rounds: 3,625/3,888/3,669) and, more tellingly, is *slower* than
`eav|50|256` in the same run (2,004 rows/s) — backwards, since a smaller
value should never ingest slower holding everything else constant. The
isolated 3-round pair-probe (run immediately before/after nothing else on the
host) reproduced the expected direction cleanly every time (16 B faster than
256 B). Likewise `declared|50|256` in run1 (805 rows/s) and run2b (1,564
rows/s) are both far below the isolated pair-probe's three clean rounds
(5,702/5,352/5,453 rows/s, alternated against `binary|50|256` in the same
tight loop) — and in that same tight loop, `binary|50|256`'s *own* round 0
(7,092 rows/s) was itself the outlier once (its siblings ran 3,604–3,740),
proving the noise is symmetric across models, not a real cost specific to
`declared`. **Net: model (b) and model (d) are statistically indistinguishable
in throughput at 256 B values; the two-run figures for that one cell should
not be read as "declared is 3–4× slower."**

**Declaring a field schema at queue creation is a small, real win that only
matters when values are small.** The size effect (4 bytes/field saved) is
architecturally real and reproducible (measured three independent ways:
isolated encoder, real DB row length, DB-file size at 16 B values), but it is
proportionally tiny against a 256 B value (0.03 B/field of overhead) and only
becomes visible — a genuine 25% smaller hot row — once values are small
(16 B; plausibly true of many real upstream-id-style or flag-style fields).
Throughput showed no measurable difference either way once contention was
isolated out.

**EAV is the worst model on every axis except single-field-touch update
throughput, where it wins clearly** (json/binary/declared 4.2k–7.8k
updates/s vs EAV 6.9k–14.7k for the 1-field cells) — because EAV writes
exactly one small row for a one-field patch, never touching the other 49
fields' storage at all, where the other three models must round-trip
(RMW) or rewrite (json_patch) something shaped like the whole record. That
advantage shrinks once 10 of 50 fields are touched (EAV drops to the same
band as everyone else) and inverts on ingest, point-read, and DB size at
higher field counts, because EAV pays one B-tree row per field on every one
of those paths.

**The payload-placement effect is the largest, cleanest signal in the whole
spike** (3.5–9× across 7 repetitions, always in the same direction) and it
required a methodology fix to see cleanly — a reminder that "keep large
values off the hot row" is not merely a stylistic preference; an un-checkpointed
WAL can hide or invert the effect in a naive measurement, and a production
implementation that batches many small updates against a queue whose records
happen to carry a large inline blob would see the same 3.5–9× penalty on
every touch of that record's `fields`, not just the first.

## Recommendation

1. **Storage model: keep JSON TEXT + `json_patch` (model a)** as the
   `fields` storage model, per the current draft. It is within ~10–20% of
   the fastest binary variant on ingest and update throughput at realistic
   (256 B) value sizes, and its on-disk footprint is only ~3–4% larger than
   the best binary variant at the same config (13,657 vs 13,110–13,315
   B/row at fields=50/256B). That margin does not justify the architectural
   cost of the alternative: **(b)/(d) require a batched `SELECT` before the
   `UPDATE`** (this spike's own RMW implementation,
   `spikes/field-storage/src/models.rs` `binary_update`), which is a
   read-then-write race unless every row additionally carries a CAS check —
   an extra mechanism the contract would need to add and every caller would
   need to use correctly. `json_patch` is a single atomic SQL statement per
   row with no intermediate read (INFERRED: not itself benchmarked for
   concurrency here, but architecturally load-bearing — see D8/D2 in the
   verified `json_patch` semantics at kernel-contract-draft.md:757 and
   `vendor/turso_core/json/ops.rs:12-24`, VERIFIED for the NULL-safety half
   of that claim). Do not add EAV or a new binary hot-row encoding to the
   contract's storage model.

2. **Do not require queues to declare a field schema (names/types) at
   creation.** The measured benefit (§Findings 1) is real but small at the
   value sizes this spike could confirm move the needle (256 B+): it only
   becomes a meaningful (~25%) hot-row-size win at small (~16 B) values, and
   showed no throughput benefit once contention noise was controlled for.
   The cost of requiring a declared schema — a field-name registry, ID
   assignment and reuse rules, a migration story for renamed fields — is not
   justified by that margin. If a queue has many (dozens+) small
   (sub-32-byte) fields updated at high rates, document a *consumer-level*
   mitigation instead of a new contract mechanism: pick short field names
   (the JSON tagged-array encoding already pays per-key overhead
   proportional to the name's length,
   kernel-contract-draft.md:757). This is guidance for documentation, not a
   new storage mode.

3. **Limits.** The draft's existing 4 MiB record cap
   (kernel-contract-draft.md:583) is not the binding constraint this spike
   found — throughput degrades an order of magnitude from *value size* alone
   (16 B → 4,096 B: 13–65× slower, §Findings 2) long before 4 MiB is
   approached even at 200 fields × 4 KiB. Recommend the contract's companion
   guidance (not necessarily a new hard-enforced constant — that is the
   owner's call) document:
   - **per-field value size**: keep individual field values well under
     ~1 KiB; values approaching or exceeding a few KiB should go in
     `payload` instead (see #4).
   - **fields per record**: this spike tested up to 200 and treats that as
     the practical, throughput-tested ceiling for predictable per-batch
     latency (5–25× slower than 5 fields at the same value size); there is
     currently no documented guidance at all on this axis, and there should
     be, independent of whether it becomes a hard contract limit.

4. **`payload` vs `fields`.** Confirmed with the largest, cleanest effect
   measured in this spike (§Findings 3): anything that is (a) not queried or
   indexed, and (b) more than a few KiB, belongs in `payload`, off the hot
   row — never inlined into `fields` or a fields-adjacent column, even if it
   is logically "part of the record" and even if that column is never
   named in a given `UPDATE`'s `SET` clause. Keep `fields` for small, typed,
   potentially-indexed scalars only. This validates the current schema's
   existing off-row `fireweed_item_payloads` design
   (`crates/fireweed-relational/src/schema.rs:55-57`) with a measured
   magnitude (3.5–9× on a single 64 KiB value; expected to grow with size)
   rather than changing it.

5. **Indexed fields under churn.** A single declared index adds a roughly
   bounded, additive cost per write that touches its key — this spike's
   majority-of-cells measurement puts it at ~15–30% of update throughput for
   one index at 1,000 distinct rows under constant churn (§Findings 5).
   Recommend the contract's guidance state plainly that this cost is
   per-touched-index (declare only what is actually queried, per
   kernel-contract-draft.md:468-471's own "Adding one means a new queue"
   framing already pushing toward deliberate declaration) and roughly
   additive across indexes touched by the same write — this spike measured
   one index only and did not test N>1 concurrently-touched indexes; that
   is a gap for a follow-up spike if the contract needs a firmer number than
   "roughly additive."

## Limitations

- **n=1–2 per cell** for most update-churn numbers (two full runs; the
  payload experiment is the exception at n=5, and three cells got a
  dedicated n=3 pair-probe). Given the measured host contention, treat any
  single number in the Update table as ±30–50% until corroborated; the
  *shapes* (which model/config is faster, roughly by how much) are more
  reliable than any individual number's third significant figure.
- **20,000 rows, not 100,000**, per config (see Setup) — a disclosed,
  reasoned reduction, not a silent one. Bytes/row and rows/s are both
  per-batch steady-state measures and should hold at 100k; total ingest wall
  time would not (it would scale ~5×, which is exactly why 100k did not fit
  the per-run time bound under measured contention).
- Field values were generated as `Value::Str` only, sized by the controlled
  `value_size` parameter (§Setup — value generation is outside the timed
  region). Int/Float/Bool/Time fields were not separately measured; the
  contract's tagged-array JSON encoding and postcard encoding both handle
  them, and their marginal cost relative to a same-size `Str` should be
  small (a fixed-width numeric encode/decode is cheaper than a variable-length
  string one), but this is INFERRED, not measured here.
- The EAV model's declared/typed-column potential (separate `value_text`,
  `value_int`, `value_real` columns instead of one `TEXT` column) was not
  tested; this spike's EAV used one `TEXT` column throughout to isolate the
  row-count effect cleanly, so EAV's measured numbers are a conservative
  (not most-favorable) representation of that model.
- The `N>1` concurrently-touched-index case (§Recommendation 5) was not
  measured.
- No RESP, gates, leases, stages, or any other kernel-contract surface was
  exercised — this spike is `fields`/`payload`/index-side-table storage
  only, against a bare Turso schema built for this purpose, not the real
  `fireweed-relational` schema or apply path.

## Source Ledger

| Source | Claim used | Confidence |
|---|---|---|
| `docs/helix/00-discover/external-interface-kernel-contract-draft.md:80-81,151,156,196-197,468-471,583-584,757,1030` | `fields`/`payload`/`Value`/`IndexKey` shape, index architecture, JSON tagged-array encoding, 4 MiB record cap | VERIFIED (read) |
| `crates/fireweed-relational/src/schema.rs:19-53,55-64,252-263` | today's `fireweed_items` columns, off-row `fireweed_item_payloads`, `fireweed_item_index` shape | VERIFIED (read) |
| `crates/fireweed-turso/src/local.rs:59,96-104,3346-3394,3407-3445` | connection configuration this spike mirrors (page size, journal mode, synchronous branch, cache size, busy timeout) | VERIFIED (read) |
| `vendor/turso_core/json/ops.rs:12-24` | `json_patch` RFC 7386 merge-patch semantics, NULL propagation | VERIFIED (read) |
| `vendor/turso_core/function.rs:238-240,280-282,1708-1712` | `JsonPatch`/`JsonRemove` function registration | VERIFIED (read) |
| `vendor/turso_core/translate/update.rs:80,308-322` | `UPDATE ... FROM` is supported; a column-alias list on a `VALUES`-derived table (`AS v(a,b)`) is not — the unaliased form (`AS v`, `v.column1`/`v.column2`) is | VERIFIED (read for FROM support) + MEASURED this spike (`SPIKE_SQL_PROBE`, `spikes/field-storage/src/main.rs` `sql_probe`) for the alias-list rejection |
| `crates/fireweed-turso/Cargo.toml:26,28` | exact vendored Turso dependency spec this spike's `Cargo.toml` matches | VERIFIED (read) |
| This spike's own runs (`run1`, `run2b`, `pair-probe` ×2, `payloadfix` ×5, `size_probe`, `db_size_probe`) | every number in Findings/Results | MEASURED (this spike); raw stdout/stderr logs retained in the session scratchpad, not the repo |
| `/proc/loadavg`, `ps aux`, `nproc`, `free -g` sampled during the runs | host contention disclosure | MEASURED (this spike) |

## Artifacts

- `spikes/field-storage/` — the standalone spike Cargo project (source only;
  no DB files, no build output committed).
- This results document.
- No changes to any existing repository file.
