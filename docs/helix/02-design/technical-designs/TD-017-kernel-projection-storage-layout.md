---
ddx:
  id: td-kernel-projection-storage-layout
  type: technical-design
  depends_on:
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - spike-record-key-storage
    - spike-record-field-storage
    - spike-claim-path-at-scale
    - spike-turso-value-size-boundary
    - spike-declared-field-storage-layout
    - adr-log-single-source-of-truth
    - adr-turso-derived-projection
    - adr-s3-turso-public-cell
    - td-object-log-turso-projection
    - td-unified-packed-mutation-path
    - td-partitioned-object-log
  links:
    - {kind: informed_by, to: api-fireweed-kernel-contract}
    - {kind: informed_by, to: adr-kernel-records-contract}
    - {kind: informed_by, to: spike-record-key-storage}
    - {kind: informed_by, to: spike-record-field-storage}
    - {kind: informed_by, to: spike-claim-path-at-scale}
    - {kind: informed_by, to: spike-turso-value-size-boundary}
    - {kind: informed_by, to: spike-declared-field-storage-layout}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: td-unified-packed-mutation-path}
    - {kind: informed_by, to: td-partitioned-object-log}
  status: accepted
---

# Technical Design: TD-017 Kernel projection storage layout

**Status**: Accepted
**Contract**: API-006 | **ADR**: ADR-013, ADR-016, ADR-024, ADR-025 | **TD**: TD-010, TD-016, TD-018 | **Evidence**: SPIKE-002 to SPIKE-006
**Scope**: the Turso projection schema and SQL statement forms that implement API-006 on S3×Turso

## Scope

API-006 leaves the physical design to the implementation (its §8). This design fixes it for S3×Turso: how the Turso projection stores records, claims, fields, declared indexes and queues, and which statement forms touch them.

In scope:
- the projection tables, their columns and their indexes;
- the statement forms that every hot path must use;
- the connection policy: WAL, checkpoint, cache size and the single writer;
- the performance checks, including the ones that verify API-006's cost rules (C9, the S2 cost bound and F4's projection read rule), mapped to the TP-003 gates that own their pass bars;
- the known risks.

Out of scope:
- The write pipeline: generations, the overlay, log-first claims, routing, the order of append and apply, and how ops are packed into vectors. TD-016 owns those mechanics. This design assumes only that each packed vector applies in one transaction (§11, rule 6), and that a claim's records and response stay within API-006's limits (C7, §6.8).
- The object log, partition assignment, snapshots, compaction, log splitting and full-delete garbage collection. TD-018 designs them, and its designs are proposed and measurement-gated. The placement model they build on (mapped, one partition per queue, no rebalancing) is decided (ADR-025 §5).
- API-006 semantics. If this design seems to disagree with API-006, API-006 wins.
- A second implementation. ADR-025 commits to none; §15 only notes what would carry over.

Fireweed is pre-production, so this layout has no data migration (API-006 §8).

## Evidence

| Spike | Path | Used for |
|---|---|---|
| SPIKE-002 | `docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md` | key representation, `INDEXED BY`, per-key lookups |
| SPIKE-003 | `docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md` | off-row large values, index maintenance cost, declared field ids |
| SPIKE-004 | `docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md` | claims table, claim index, stage binding, bulk finish, checkpoint cap, single writer, 100M records |
| SPIKE-005 | `docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md` | spill threshold, page size, the `UPDATE … FROM (VALUES …)` trap |
| SPIKE-006 | `docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md` | hot/cold split, binary field map, shared index table, per-queue tables, cache ablation |

Every number carries a label:
- **MEASURED (SPIKE-00N Fk)**: from that spike's finding k. The spikes ran on a shared WSL2 host with 1-minute load averages between 5 and 43 on 32 cores (SPIKE-003 saw the highest). Relative comparisons inside one spike are the load-bearing signal; absolute numbers are directional.
- **MEASURED (evidence N)**: an `ss_phased_capacity` run recorded under `docs/perf/evidence/ss-phased/N/`.
- **INFERRED**: derived by arithmetic or reasoning from measurements, code or documentation, and not measured.

Configuration values (page size, cache size, thresholds, API-006 limits) are settings, not measurements. Each one cites its source.

## Technical approach

**Strategy.** Keep the claim path narrow. The row that a claim walks and writes carries only identity, stage, rank, lease state and a few counters. Everything else a record has (its fields, its large values, its index entries) lives in separate tables keyed by the same `(queue_no, seq)` identity. Claim churn and field traffic then never share pages.

**Key decisions.**
- One projection file per partition, with many queues per file. Each queue is an integer `queue_no`. There are no per-queue tables.
- A lean `items` row with no field bytes.
- Leases in a `claims` table, with a nullable `claim_id` on the item row.
- One stage-prefixed claim index, partial on `claim_id IS NULL`.
- Keys stored as opaque `BLOB`, unique per queue, with the unique index maintained on every insert.
- Fields in a cold binary map keyed by declared field id. Any value over 4 KiB spills to a values table.
- Claims and reads fetch only the fields their projection names; an empty projection reads no field storage (§6.3).
- Declared indexes in one shared table of ESF keys.
- The definition and the metadata in their own per-queue rows, off the small registry row.
- Every hot statement names its index. Key lookups are one statement per key. A whole claim is finished in bulk by `claim_id`. Each change runs in one explicit transaction and reuses its prepared statements.
- One writer connection per file behind a `Mutex`, and a Passive checkpoint cap on the WAL.

**Trade-offs.**
- Gain: claim cost does not depend on how many fields a record has or how large they are, beyond the fields the claim returns; extend and expiry discovery cost O(claims), not O(records); a new queue is a few inserted rows.
- Lose: a full record read touches up to three tables; a field update rewrites the cold map and also bumps the version on the hot row, one statement more than a colocated layout; descending index scans need a second B-tree.
- Rejected:
  - fields on the hot row (SPIKE-006 layouts A and B, §2);
  - per-queue tables (SPIKE-006 layout D, §1);
  - lease columns on the item row (SPIKE-004 L1, §3);
  - engine-side key hashing (§5);
  - a page size above 4 KiB (§12).

## 1. File and queue model

- The control plane assigns each queue to one partition, and queues are never moved between partitions (ADR-025 §5). Each partition has one Turso projection file, so many queues share a file; that is this design's choice, shared with TD-018, which designs the partitions and their log. Writer parallelism comes from more partition files per node, not from more writers per file (§12).
- The object log is the authority (ADR-013). The file is derived, and it is rebuilt from the log through projection control on the admin handle (TD-010; API-006 §7). Every value a client observes or sends back, or that a later check compares, comes from the log: the insertion sequence, versions, and for each claim its id and the hash of its token (never the token itself). A rebuild therefore reproduces them, and live tokens stay valid across a rebuild or a handoff. File-local surrogates (`queue_no`, group ids, gate ids) never leave the file, and a rebuild need not reproduce them.
- Per-queue physical tables are ruled out. With 100, 1,000 and 10,000 queues, schema creation took 2.12 s, 140.5 s and 13,111.3 s (3.64 h), and opening the file took 5.6 ms, 202.8 ms and 26.64 s (MEASURED, SPIKE-006 F7). In this layout a new queue is a few inserted rows.

## 2. The `items` table: the claim-path row

```sql
CREATE TABLE items (
    queue_no   INTEGER NOT NULL,  -- queue surrogate (§5)
    seq        INTEGER NOT NULL,  -- insertion sequence (ORD1); high part of the opaque Version
    key        BLOB    NOT NULL,  -- API-006 Key: opaque bytes, 1..=256 B
    stage      INTEGER NOT NULL,  -- position of the stage in the declared list
    rank       INTEGER NOT NULL,
    ver        INTEGER NOT NULL,  -- per-incarnation counter; low part of the opaque Version
    claim_id   INTEGER,           -- NULL = Free (§3)
    attempts   INTEGER NOT NULL,
    free_since INTEGER NOT NULL,  -- µs; used only by stats S2 (§9)
    grp        INTEGER,           -- interned group id; NULL = ungrouped (§9)
    gates      BLOB,              -- at most 8 interned gate ids; NULL = none (§9)
    PRIMARY KEY (queue_no, seq)
);
```

| Column | Rule |
|---|---|
| `seq` | Assigned by the queue's sequencer and carried in the log, in the order ORD1 requires. It never changes, stage moves included. A re-insert after delete gets a new, higher one. |
| `key` | The bytes the caller sent, compared bytewise (§5). |
| `stage` | The declaration position, so stage values sort as API-006 §4.0 requires. |
| `rank` | Set by the caller. An insert that sets none gets the insertion time in µs on the implementation `Clock`. |
| `ver` | 1 at insert. +1 on every applied write, every claim of the record and every force-clear that detaches it. Extend and reclaim leave it unchanged (API-006 §3). |
| `claim_id` | Attached to a claim iff not NULL (§3). |
| `attempts` | 0 at insert, +1 per claim, never reset. |
| `free_since` | Set at insert, on a stage move of a Free record, on release, on force-clear, and on reclaim (to the expired `until`), as API-006 §3 defines. |
| `grp`, `gates` | File-local ids (§9). `grp` is fixed after insert (W8). |

**Version.** API-006's `Version` is opaque, ordered per key only (its §4.0). This layout encodes it from `(seq, ver)`: it strictly increases across delete and re-insert because `seq` does (INFERRED design). Versions of different keys therefore happen to order by insertion; the contract gives that order no meaning, and nothing may depend on it.

**Why no field bytes.** A 100-record claim on a 97-field workload took a median 4.4 ms when the claim row held no fields (layout C), 7.3 ms with the field map on the claim row (layout B), and 2,800.9 ms with JSON fields on it (layout A): 228.45, 137.14 and 0.46 claims/s (MEASURED, SPIKE-006 F2). An inline 64 KiB value cut a claim-style walk from 55,041 to 14,013 claims/s (MEASURED, SPIKE-005) and cost 3.5–9× on updates (MEASURED, SPIKE-003).

**Row width.** SPIKE-004 measured a claim row of `queue_no`, `seq`, `key`, `stage`, `rank`, a version and `claim_id`, plus about 300 B of fields, which this row drops. SPIKE-006's layout C carried the same columns without `key` and without fields. This row adds `attempts`, `free_since`, `grp` and `gates`: small integers and a list of at most 8 ids, INFERRED to add under 30 B per row. Not measured.

## 3. Claims and the `claim_id` model

```sql
CREATE TABLE claims (
    queue_no     INTEGER NOT NULL,
    claim_id     INTEGER NOT NULL,     -- unique per queue, from the log (below)
    token_hash   BLOB    NOT NULL,     -- carried in the claim's log entry
    expires_at   INTEGER NOT NULL,     -- the claim's `until`, µs
    record_count INTEGER NOT NULL,     -- records still attached
    PRIMARY KEY (queue_no, claim_id)
);
CREATE INDEX claims_expires_idx ON claims (expires_at);
CREATE INDEX items_claim_id_idx ON items (queue_no, claim_id) WHERE claim_id IS NOT NULL;
```

This is SPIKE-004's L2 design (ADR-025 §2). The table is keyed by `(queue_no, claim_id)`, so claim ids need only be unique per queue, a token cannot act on another queue (API-006 ISO1), and `delete_queue` finds a queue's claims by key prefix. SPIKE-004 keyed `claims` by an `INTEGER PRIMARY KEY`; the composite key is INFERRED to seek at the same cost. A claim has no holder identity (API-006 L1), so the table has no holder column.

- **One row per claim.** A claim that returns at least one record inserts one row (C3). A claim that returns nothing inserts none.
- **Claim id and token.** `claim_id` is unique per queue and derived from the queue's own log stream: the offset of the append that carries the claim in that queue's stream, and the claim's ordinal within that append. TD-018 keeps per-queue dense offsets, and its partition-wide `commit_seq` is not used, so two queues in one file can share a `claim_id` without colliding. The token is `claim_id` followed by at least 128 random bits. The claim's log entry carries `token_hash`, a hash of the whole token, and never the token; the table stores the same hash. A rebuild restores every `claims` row, live tokens included, from those log entries. A token check is a primary-key seek on `(queue_no, claim_id)`, with `queue_no` taken from the request's queue, and a constant-time hash compare (INFERRED design).
- **Liveness (L2).** A claim is live iff its row exists and `expires_at > now`. `record_count` counts the attached records. The transaction that detaches the last one deletes the row.
- **Attachment.** A record is attached to claim c iff its `(queue_no, claim_id)` is c's key. `Held(t)` and `Release(t)` require that and a live claim, else `StaleLease`.
- **Extend (W14).** One `UPDATE claims INDEXED BY sqlite_autoindex_claims_1 SET expires_at = ? WHERE queue_no = ? AND claim_id = ?`. No item row and no version changes. W2a's rule (an extend sees the request's earlier detaches of the claim's records) is checked against `record_count` after those detaches are planned.
- **Release and Delete** detach one record and decrement `record_count`.
- **Reclaim (V4).** A sweep walks `claims_expires_idx` up to now, clears each expired claim's records by `(queue_no, claim_id)` (§11, rule 5) with `free_since = expires_at` and no version change, and deletes the row. The sweep covers every queue in the file in one pass (MEASURED, SPIKE-004 F8).
- **Claim replay (C8).** Records only ever leave a claim, never join one. So "every returned record is still attached" is the O(1) test `record_count` = the number of records the claim returned (INFERRED).

Evidence:
- Extending a 1,000-record claim: 7,345.2 extends/s against 90.9 with lease columns on the item rows, 81× (MEASURED, SPIKE-004 F2); 6,201.2/s at 10M records (F6); 8,805.1/s at 100M (F8).
- Expiry discovery: 0.13 ms against 9.92 ms, 76×; the whole sweep of 10,000 records took 104.27 ms against 147.87 ms (MEASURED, SPIKE-004 F3).
- Claim and finish cost the same on both designs (MEASURED, SPIKE-004 F1), and the file size differs by one 4,096 B page (MEASURED, SPIKE-004 F4, F7).

## 4. The claim index

```sql
CREATE INDEX items_claim_idx ON items (queue_no, stage, rank, seq) WHERE claim_id IS NULL;
```

- It holds every Free record of every stage. API-006 has no terminal stage, so a settled record stays in this index while it is Free, and the stage prefix keeps claims of other stages from visiting it. SPIKE-004 measured exactly this shape, with 95% of the rows resting in a stage nothing claims.
- **A `Next` claim** seeks `(queue_no, stage)` and walks `rank <= horizon` in `(rank, seq)` order. The plan is `SEARCH items USING INDEX items_claim_idx (queue_no=? AND stage=? …)`, with no sorter (MEASURED, SPIKE-004 F5, F6, F8).
- **C9.** The stage prefix excludes other stages, the partial predicate excludes Leased records, and the rank bound excludes `rank > horizon`, so none of them is visited. When the queue has no Blocked gate (the usual case), the walk reads the index only. When some gate is Blocked, each candidate's `gates` is read from its row, and a skipped candidate costs one row read. The cost is O(log n + k + b). A paused queue returns before the walk (INFERRED; SPIKE-004 did not model gates).
- **Bind the stage.** Without `stage = ?`, the same query sorts every Free record of the queue: 406,186.9 µs against 62.2 µs at p50 (6,530×) at 1M records, and 4,105,957.5 µs at 10M (MEASURED, SPIKE-004 F5, F6).
- **Size.** 26,386,432 B (25.16 MiB) per 1M Free records, about 26.4 B per record (MEASURED, SPIKE-004 F7). That is about 2.5 GiB at 100M records (INFERRED). This index is what outgrows the cache first (§12, risk 1).
- **Scale.** 626.2 claims/s at 1M records and 649.0 at 10M, flat (MEASURED, SPIKE-004 F1, F6). At 100 queues × 1M records in one file, 620.9 claims/s with this layout's lean row, against 351.6 with field bytes on the claim row (MEASURED, SPIKE-004 F8, F9; risk 1).

## 5. Keys

```sql
CREATE UNIQUE INDEX items_key_idx ON items (queue_no, key);
```

- **Queue surrogate.** An integer `queue_no` replaces the `(tenant, queue)` text columns on every row: 10.7–20.7% smaller rows for every key shape (MEASURED, SPIKE-002 F2). The registry maps names to numbers (§8).
- **`BLOB` key.** API-006 keys are opaque bytes, stored verbatim. `BLOB` and `TEXT` cost the same for the same bytes. A 64-bit integer as an 8-byte `BLOB` is 11.4% smaller than its decimal text, and a UUID as 16 bytes is 17.8% smaller than its 36-byte text (MEASURED, SPIKE-002 F2). Upstream gets that saving by sending compact binary keys.
- **No engine-side hashing.** API-006 caps keys at 256 B, and upstream hashes larger natural keys (ADR-025 §2). SPIKE-002's hash layout would save 32.7% at 256 B (741.7 against 499.1 B per row), but it needs a collision re-check on every conflict. Key cost is linear, 2.156 B of storage per extra key byte, with no cliff (MEASURED, SPIKE-002 F2).
- **Maintained during ingest.** The unique index exists before the first insert and is never dropped for a bulk load, a backfill or a projection rebuild. Built after the load, `CREATE UNIQUE INDEX` over 100M rows took 84.65 of the build's 120.55 minutes, 12.5× the time of the non-unique claim index over the same rows (MEASURED, SPIKE-004 F8). Why it is slower (random key order or the uniqueness check) was not isolated (INFERRED). Maintained during ingest instead, the lean 100M build took 52.06 minutes against 120.55 (MEASURED, SPIKE-004 F9).
- **Lookups.** One statement per key:

  ```sql
  SELECT seq, stage, rank, ver, claim_id, attempts, grp, gates
    FROM items INDEXED BY items_key_idx
   WHERE queue_no = ?1 AND key = ?2;
  ```
  Without the hint, the planner binds only `queue_no` and scans the queue: 1,473–1,675 lookups/s on 2,000 rows, against 24,102–39,813/s hinted on 100,000 rows (MEASURED, SPIKE-002 F0, F3). `key IN (…)` binds only `queue_no` even with the hint: 1,133–1,287 keys/s against 35,700–37,819/s one key at a time, about 28× (MEASURED, SPIKE-002 F1). This applies to `write`, `read` and `Keys` claims, each up to 1,000 keys.

## 6. Fields

### 6.1 The cold field map

```sql
CREATE TABLE item_fields (
    queue_no INTEGER NOT NULL,
    seq      INTEGER NOT NULL,
    map      BLOB    NOT NULL,  -- binary map: field id -> inline value or spill marker
    PRIMARY KEY (queue_no, seq)
);
```

- **Encoding.** A compact binary (postcard) map from a `u16` field id to either the value inline or a spill marker `(length)`. The field id is the field's position in the normalized, name-sorted field list (API-006 D1). It is fixed for the queue's life because the definition is immutable. A declared id costs about 4 B per field less than a name key (MEASURED, SPIKE-003).
- **Not JSON.** JSON fields (SPIKE-006 layout A) were 15–500× slower on every operation that touches fields (MEASURED, SPIKE-006 F2–F4).
- A record with no fields has no row.
- **Many medium fields.** The map row may exceed one page. SPIKE-006's cold row held 95 small and medium fields, 10 of them 256 B–2 KiB, and that layout still read full records fastest: 5,765.70/s at 0.16 ms p50 (MEASURED, SPIKE-006 F4). That the row exceeded one page is INFERRED from its size.
- **Updates.** A field update reads the map by primary key, patches it and writes it back, and bumps `ver` on the hot row. Single-record updates of this layout ran at 1,838.35/s for 3 small fields, 1,493.79/s for an indexed field and 1,742.83/s for a large field (MEASURED, SPIKE-006 F3).
- **W8 record size.** The 4 MiB bound covers the key, the map and the spilled lengths. The map records each spilled length, so the check reads no spilled value (INFERRED).
- **Types.** `Int`, `Str` and `Bytes` were measured. `Float`, `Bool` and `Time` are fixed-width and INFERRED to cost no more than `Int` (SPIKE-006).

### 6.2 Per-field spill

```sql
CREATE TABLE item_values (
    queue_no INTEGER NOT NULL,
    seq      INTEGER NOT NULL,
    field_id INTEGER NOT NULL,
    value    BLOB    NOT NULL,
    PRIMARY KEY (queue_no, seq, field_id)
);
```

- **Rule.** A `Str` or `Bytes` value whose encoding is over 4,096 B is stored here, and the map holds its marker. A value at or under 4,096 B stays in the map. The rule applies to each value on every write, so a value that shrinks moves back into the map. The threshold is an internal constant; changing it changes nothing a caller can observe (API-006 F2).
- **Why 4 KiB.** At the 4 KiB page, a table-leaf cell spills once its payload exceeds usable size − 35 = 4,061 B (the formula in `vendor/turso_core/storage/btree.rs`, read by SPIKE-005). Row size stepped up by 503 B between a 3,072 B and a 4,096 B value (MEASURED, SPIKE-005).
  - At or under the threshold, inline is faster: off-row updates of the value itself ran at 0.27–0.51× the inline rate from 256 B to 4 KiB.
  - Above it, off-row wins: 1.65–2.20× from 32 to 64 KiB.
  - Reading a row without the value stays flat off-row, at 17–34 µs, against 80–114 µs inline at 32–64 KiB (all MEASURED, SPIKE-005).
- **Writes.** One prepared upsert per spilled value, and one delete when a value is removed or shrinks below the threshold:

  ```sql
  INSERT INTO item_values (queue_no, seq, field_id, value) VALUES (?1, ?2, ?3, ?4)
      ON CONFLICT (queue_no, seq, field_id) DO UPDATE SET value = excluded.value;
  DELETE FROM item_values INDEXED BY sqlite_autoindex_item_values_1
   WHERE queue_no = ?1 AND seq = ?2 AND field_id = ?3;
  ```
  Never use `UPDATE … FROM (VALUES …)` on this table (§11, rule 7).
- **Reads.** A claim or read fetches only the spilled values its projection returns (§6.3). Each spilled value read costs about 50–70 µs more than an inline one (MEASURED, SPIKE-005; risk 3).
- **Bound.** Every measured operation completed at bounded cost with 64 KiB values, the API-006 maximum (MEASURED, SPIKE-005).

### 6.3 Field projection

`claim` and `read` return only the fields their `fields` list names (API-006 F4). Under this hot/cold layout, projection limits the cold-field fetch to the requested fields:

| `fields` | Work per returned record |
|---|---|
| `None` | Read the `item_fields` row and every spilled value: `SELECT field_id, value FROM item_values INDEXED BY sqlite_autoindex_item_values_1 WHERE queue_no = ?1 AND seq = ?2`. |
| `Some(names)`, not empty | Read the `item_fields` row and decode only the named entries. For each named field whose entry is a spill marker, one primary-key seek: `… WHERE queue_no = ?1 AND seq = ?2 AND field_id = ?3`. Unrequested spilled values are never read. |
| `Some([])` | Nothing beyond the hot row. A claim reads and writes `items`, `claims` and the counters; a read reads `items` alone. Neither touches `item_fields` or `item_values`. |

- **Shared storage.** Inline values (4,096 B or less each) share the map row, so a non-empty projection reads that row whole. API-006 F4 allows this: a value that shares storage with a returned field may be read. Every value over 4,096 B is in its own `item_values` row and is read only when requested.
- **Cost.** Layout C's claim, which read each record's map row and returned its small fields, took a median 4.4 ms per 100-record claim (MEASURED, SPIKE-006 F2). An empty projection skips the map read, so it costs less (INFERRED). A 100-record claim that also returns 2 spilled values per record adds about 10–14 ms (INFERRED from SPIKE-005's 50–70 µs per spilled read).
- **Response size.** The 16 MiB response cap (C7) counts the projected records, so a claim that leaves large fields out is not shortened by them.
- **Writes do not project.** A write reads the map whenever it patches fields, for the W8 size check and index rekeys (§6.1, §7).

## 7. Declared indexes: the shared index table

```sql
CREATE TABLE item_index (
    queue_no INTEGER NOT NULL,
    idx      INTEGER NOT NULL,  -- position of the index in the normalized definition
    esf_key  BLOB    NOT NULL,  -- ESF key tuple, at most 1024 B (W1, W8)
    seq      INTEGER NOT NULL,
    PRIMARY KEY (queue_no, idx, esf_key, seq)
);
CREATE INDEX item_index_desc_idx ON item_index (queue_no, idx, esf_key DESC, seq ASC);
```

- **One table.** It serves every declared index of every queue in the file (the SPIKE-003 and SPIKE-006 shape), with no per-queue objects (§1).
- **Key.** `esf_key` is the `fireweed-contract` ESF encoding of the index's keys in declared order: field values of any declared type (`Str` and `Bytes` included), the stage by its declaration position, the rank, and the group key. A record has an entry iff every key has a value (Q1).
- **Maintenance.**
  - An insert adds the record's entries, and a delete removes them.
  - A write that changes any key of an index (a field, the stage or the rank; the group is fixed) deletes the old entry and inserts the new one. The old key comes from stored values the write has already read: the hot row and the map.
  - Claims, releases, extends and reclaims change no index key, so the claim path never touches this table.
- **Cost.** One touched index cost about 15–30% of update throughput (MEASURED, SPIKE-003, one index, 1–2 runs per cell). Layout C updated an indexed field at 1,493.79/s against 1,838.35/s for unindexed fields (MEASURED, SPIKE-006 F3).
- **Scans (Q2, ORD3).** An ascending scan walks the primary-key autoindex. A descending scan walks `item_index_desc_idx`. Both give `seq` ascending among equal keys. The cursor is `(esf_key, seq)`. A hit joins `items` by `(queue_no, seq)` for the key, stage and lease.
- **`aggregate`** walks the range and groups by decoded key components. `Dim::Lease` reads `items.claim_id`. Layout C counted about 50,000 records in 501 groups in 9.24 ms p50 (MEASURED, SPIKE-006 F5).
- **Second tree.** The descending index doubles the write cost of each entry (INFERRED). The ascending/descending pair is today's `fireweed_item_index` shape (`crates/fireweed-relational/src/schema.rs`); see risk 4.
- **Index-cell overflow.** An index cell spills above ((usable − 12) × 64 / 255) − 23 bytes (`vendor/turso_core/storage/btree.rs`), which is 1,002 B at a 4 KiB page (INFERRED arithmetic). An ESF key near the 1024 B limit therefore overflows. Keys of numbers, short strings and binary ids stay far below it, so the overflow is a tail cost of long `Str` or `Bytes` keys (INFERRED).

## 8. Queue definition, metadata and lifecycle

```sql
CREATE TABLE queues (
    queue_no      INTEGER PRIMARY KEY,
    tenant        TEXT    NOT NULL,
    queue         TEXT    NOT NULL,
    state         INTEGER NOT NULL,            -- 0 open, 1 archived, 2 deleted
    paused        INTEGER NOT NULL DEFAULT 0,  -- the "*" gate
    blocked_gates INTEGER NOT NULL DEFAULT 0   -- Blocked gate keys other than "*"
);
CREATE UNIQUE INDEX queues_name_idx ON queues (tenant, queue);
CREATE TABLE queue_defs (queue_no INTEGER PRIMARY KEY, definition BLOB NOT NULL);
CREATE TABLE queue_meta (queue_no INTEGER PRIMARY KEY, version INTEGER NOT NULL, metadata BLOB NOT NULL);
CREATE TABLE apply_cursor (queue_no INTEGER PRIMARY KEY, next_offset INTEGER NOT NULL);
CREATE TABLE schema_meta (version INTEGER NOT NULL);
```

- **Apply cursor and schema version.** Every apply transaction advances `apply_cursor.next_offset` for each queue it applies, in the same transaction (ADR-013, TD-010). `schema_meta` holds the file's one schema version, which the adapter verifies on open (TD-010).
- **Three rows per queue.** The registry row stays small. The definition (up to 256 fields and 32 indexes) and the metadata (up to 64 KiB) get rows of their own, so gate ops and state changes do not rewrite them. An inline large value taxes updates of the other columns in its row: 2.8–3.0× at 32–64 KiB (MEASURED, SPIKE-005).
- **Definition.** The canonical encoding of the normalized `QueueDef`, so D2 is a byte comparison. On first use, the implementation parses it into an in-memory schema per `queue_no`: stage ordinals, field ids and types, and index ids and keys. The definition is immutable, so the cache is never invalidated; closing the queue drops it.
- **Metadata.** The canonical encoding of the map, and its version. A W13 compare-and-swap reads the version and writes both in the unit's apply transaction. `read{queue: true}` reads `queue_defs` and `queue_meta`.
- **`define_queue`.** For an absent queue, one transaction inserts the `queues`, `queue_defs` and `queue_meta` rows (version 0, empty map) and one `stage_counts` row per stage (§9). For an open queue, it compares definitions (D2, D3). For a closed queue, it fails with `QueueClosed` (D6).
- **`delete_queue`.**
  - The apply sets `state`. Every op checks the cached state first, so from that transaction on every op other than `delete_queue` fails with `QueueClosed`, and every token of the queue is dead (DQ2–DQ5).
  - A background task then removes the queue's rows from every table in bounded batches. Each batch is its own transaction under the checkpoint cap.
  - The `queues` row is kept, so `queue_no` is never reused (a closed key is retired for good, DQ3) and leftover rows stay unreachable.
  - Archive and full delete differ only in the log: an archive keeps the queue's log and drops its projection (ADR-025 §2).
  - The purge cost is not measured (INFERRED), and full-delete garbage collection of the log is pending measurement.

## 9. Groups, gates, counts and request ids

The spikes did not model these structures: SPIKE-004 left gates, groups and attempts out of scope. Everything in this section is INFERRED, and the §13 capacity runs (P1–P4, P2p–P4r) measure it.

```sql
CREATE TABLE groups (
    queue_no  INTEGER NOT NULL,
    grp       INTEGER NOT NULL,
    group_key TEXT    NOT NULL,
    members   INTEGER NOT NULL,            -- records of the group, in any stage; the row goes at 0
    leased    INTEGER NOT NULL DEFAULT 0,  -- attached records of the group, in any stage (C6a)
    PRIMARY KEY (queue_no, grp)
);
CREATE UNIQUE INDEX groups_key_idx ON groups (queue_no, group_key);
CREATE INDEX items_group_idx ON items (queue_no, grp, stage, rank, seq)
    WHERE claim_id IS NULL AND grp IS NOT NULL;
CREATE TABLE group_heads (
    queue_no  INTEGER NOT NULL,
    stage     INTEGER NOT NULL,
    grp       INTEGER NOT NULL,
    head_rank INTEGER NOT NULL,  -- first Free record of the group in the stage
    head_seq  INTEGER NOT NULL,
    PRIMARY KEY (queue_no, stage, grp)
);
CREATE INDEX group_heads_order_idx ON group_heads (queue_no, stage, head_rank, head_seq);
CREATE INDEX items_ready_idx ON items (queue_no, stage, free_since, rank)
    WHERE claim_id IS NULL AND grp IS NOT NULL;

CREATE TABLE gates (
    queue_no INTEGER NOT NULL,
    gate_id  INTEGER NOT NULL,
    gate_key TEXT    NOT NULL,
    blocked  INTEGER NOT NULL DEFAULT 0,
    carriers INTEGER NOT NULL DEFAULT 0,  -- records carrying the gate; the row goes when 0 and Open
    PRIMARY KEY (queue_no, gate_id)
);
CREATE UNIQUE INDEX gates_key_idx ON gates (queue_no, gate_key);

CREATE TABLE stage_counts (
    queue_no INTEGER NOT NULL,
    stage    INTEGER NOT NULL,
    free     INTEGER NOT NULL,
    leased   INTEGER NOT NULL,
    PRIMARY KEY (queue_no, stage)
);

CREATE TABLE requests (
    queue_no     INTEGER NOT NULL,
    request_id   BLOB    NOT NULL,
    fingerprint  BLOB    NOT NULL,
    outcome      BLOB    NOT NULL,
    retain_until INTEGER NOT NULL,
    PRIMARY KEY (queue_no, request_id)
);
CREATE INDEX requests_retain_idx ON requests (retain_until);
```

- **Groups.**
  - Group keys are interned per queue. The first insert of a group creates its row; `members` counts the group's records in every stage and changes only on insert and delete, because a record's group is fixed (W8). The delete that takes `members` to 0 deletes the row, so a group that is used once and then emptied (for example, one Snorri workflow instance) leaves nothing behind. A later insert of the same key interns it again, possibly under another id.
  - `leased` changes on claim, release, delete, force-clear and reclaim.
  - A `WholeGroups` claim walks `group_heads_order_idx` for `(queue_no, stage)` while `head_rank <= horizon`, skips groups with `leased > 0` (C6a), and reads each group's candidates from `items_group_idx`, dropping gated ones.
  - A group whose head is gated has its first candidate later than its head. The walk keeps fetched groups in a heap ordered by first candidate and emits a group only once that candidate precedes the next unvisited head, so groups come out in C4 order.
- **Group heads.** A row exists iff the group has a Free record in the stage. Each apply batch recomputes the rows of the `(stage, group)` pairs it touched, set-based, through `items_group_idx`. That costs O(members of the pair) (risk 5).
- **`stats.groups` (S2).** Both values are exact:
  - `ready_groups` counts `group_heads_order_idx` entries with `head_rank <= horizon`, up to `limit`: O(log n + `limit`). The head holds the group's lowest Free rank in the stage, so a group has a ready record iff its head does.
  - `oldest_ready_since` walks `items_ready_idx` for `(queue_no, stage)` in `free_since` order and returns the first entry with `rank <= horizon`, or none when the walk ends. `rank` is an index column, so a skipped entry costs one index entry and no row read: O(log n + s), where s is the grouped Free records of the stage ranked beyond the horizon, as API-006 S2 bounds it. Rank is compared only with the horizon, never used as a time.

  ```sql
  SELECT free_since FROM items INDEXED BY items_ready_idx
   WHERE queue_no = ?1 AND stage = ?2 AND claim_id IS NULL AND grp IS NOT NULL AND rank <= ?3
   ORDER BY free_since
   LIMIT 1;
  ```
- **Ready index.** `items_ready_idx` holds only grouped Free records, so claims and releases of ungrouped records never touch it. For grouped records it adds one index write to each claim, release, reclaim, stage move and rank change (risk 5).
- **Gates.** Gate keys are interned per queue, and the hot row holds their ids. `queues.blocked_gates` and `queues.paused` let a claim skip gate checks when nothing is Blocked. Each connection keeps the set of Blocked ids per queue in memory, refreshed at apply. A gate op (W9) upserts a `gates` row and adjusts the counter; it changes no record.
  - `carriers` counts the records that carry the gate. Insert, delete and every gate edit adjust it (at most 8 gates per record).
  - A `gates` row is deleted when its gate is Open and no record carries it: by the gate op that opens it with `carriers` at 0, or by the write that drops the last carrier of an Open gate. A Blocked gate that no record carries keeps its row, because API-006 W9 must remember that it is Blocked.
- **Stage counts (S1).** Every apply transaction updates the rows of the stages whose free or leased counts it changed: inserts, deletes, claims, releases, reclaims, stage moves and force-clears (risk 5).
- **Request ids (RQ1–RQ4).**
  - The scope `(queue, request_id)` is shared by `write`, `claim` and `force_clear`.
  - The row is written in the same apply transaction as the effects. The log envelope carries the outcome too, so a rebuild restores it.
  - A claim's outcome records `claim_id`, the returned `seq` values, `skipped` and `partial_group`, and a replay renders the records from current state.
  - Rows are swept after `retain_until` (R ≥ 24 h) in bounded batches.

## 10. Operations against the layout

The pipeline (TD-016) decides which connection and which transaction run each statement. Every statement below names its index (§11). Every apply transaction in the table (each `write` unit, `claim`, `define_queue`, `delete_queue`, reclaim batch and `force_clear`) also advances `apply_cursor` for the queue (§8).

| Op | Projection work |
|---|---|
| `define_queue` | §8. |
| `write` Put, absent key | Insert `items` (the first stage unless the patch names one; `rank` = now in µs; `ver` 1; `attempts` 0; `free_since` = now). Insert `item_fields` if the record has fields, `item_values` for each spilled value, and `item_index` for each index whose keys are all present. Update `stage_counts`, the group (its row and `members`), each carried gate's `carriers`, and `group_heads`. |
| `write` Put, existing record | Key lookup (§5). A field patch reads the map, which W8 needs anyway. Update the hot row by primary key (`ver` + 1; stage, rank and gates as patched; `claim_id` per the lease op; `free_since` = now when a Free record changes stage). Write the map, upsert or delete spills, and rekey changed indexes. Update counts, gate `carriers` for a gate edit, and group heads. |
| `write` with `Release(t)` | As Put, plus `claim_id = NULL`, `free_since` = now, `claims.record_count` − 1 (the row is deleted at 0) and `groups.leased` − 1. Use the bulk form (§11, rule 5) when the request releases the whole claim. |
| `write` Delete | Delete the hot row, the map, the spills and the index entries. Detach and decrement as Release when the record is attached. Decrement `groups.members` and each carried gate's `carriers`, and delete rows that reach their removal rule (§9). |
| Extend | `UPDATE claims INDEXED BY sqlite_autoindex_claims_1 SET expires_at = ? WHERE queue_no = ? AND claim_id = ?` (a primary-key seek). |
| Gate op | Upsert `gates`, or delete the row when it opens a gate no record carries; adjust `queues.blocked_gates` or `queues.paused`. |
| Metadata op | Read and write `queue_meta`. |
| `claim` `Next` | The candidate walk (§4). Then, per `seq`, one prepared lease `UPDATE` by primary key; insert `claims`; update `groups.leased`, `stage_counts` and `group_heads`; fetch the hot rows, then only the maps and spilled values the projection needs (§6.3). |
| `claim` `WholeGroups` | The §9 walk, then lease as `Next`. |
| `claim` `Keys` | One `items_key_idx` lookup per key. Classify as `NotFound`, `OtherStage`, `Leased` or `Blocked`, then lease as `Next`. |
| `read` | One key lookup per key, then only the maps and spilled values the projection needs (§6.3). `queue: true` adds `queue_defs` and `queue_meta`. |
| `stats` | The queue's `stage_counts` rows, `queues.paused`, and the §9 reads of `group_heads_order_idx` and `items_ready_idx`. |
| `scan` | The `item_index` range on the primary-key autoindex or on `item_index_desc_idx`; each hit joins `items`. |
| `aggregate` | The `item_index` range, grouped by decoded key components; `Dim::Lease` joins `items.claim_id`. |
| `delete_queue` | §8. |
| Reclaim sweep (V4) | `claims_expires_idx` up to now. For each claim: count by stage and group through `items_claim_id_idx`, clear by `(queue_no, claim_id)` with `free_since = expires_at` and no `ver` change, update counts, `groups.leased` and `group_heads`, and delete the claim row. |
| `force_clear` | Per key: detach (`claim_id = NULL`, `ver` + 1, `free_since` = now) and decrement the claim. Then apply the optional Put or Delete as `write` does. |

The claim candidate walk and lease statements:

```sql
SELECT seq FROM items INDEXED BY items_claim_idx
 WHERE queue_no = ?1 AND stage = ?2 AND claim_id IS NULL AND rank <= ?3
 ORDER BY rank, seq
 LIMIT ?4;

UPDATE items INDEXED BY sqlite_autoindex_items_1
   SET claim_id = ?3, attempts = attempts + 1, ver = ver + 1
 WHERE queue_no = ?1 AND seq = ?2;
```

## 11. Required statement forms

These rules are not tuning. Each one was necessary to get the measured numbers, and breaking it turns an O(log n) path into an O(n) one or worse.

1. **Name the index on every hot statement:** `SELECT`, `UPDATE` and `DELETE` alike. Turso 0.7.2's planner does not reliably pick it. Unhinted, a key lookup bound only `queue_no` (SPIKE-002 F0), a finish `UPDATE` by primary key used `items_key_idx` and bound only `queue_no`, and the sweep's clear by `claim_id` scanned the whole table (MEASURED, SPIKE-004 F0). Primary-key autoindexes are named `sqlite_autoindex_<table>_1`, the name SPIKE-004 used for `items`; `claims` now has one too (§3). A seek on an `INTEGER PRIMARY KEY` (`queues`, `queue_defs`, `queue_meta`) needs no hint (SPIKE-004's plan for its rowid-keyed `claims`: `SEARCH claims USING INTEGER PRIMARY KEY`).
2. **Restate the partial-index predicate.** `INDEXED BY items_claim_id_idx` needs `AND claim_id IS NOT NULL` in the `WHERE` clause, or Turso reports "no query solution" (MEASURED, SPIKE-004 F0). The claim walk states `claim_id IS NULL` for the same reason.
3. **Bind the stage in every claim-path statement.** A stage-blind claim was 5,662–6,530× slower at 1M records and a further 10.1–10.8× slower at 10M (MEASURED, SPIKE-004 F5, F6).
4. **One statement per key, never `IN (…)`.** An `IN` list on the second column of a composite index binds only the first column: about 28× slower (MEASURED, SPIKE-002 F1). The same holds for the per-`seq` leases of a claim.
5. **Finish a whole claim in bulk by `claim_id`.** When a request releases every record attached to a claim with the same hot-row patch (for example, the same stage move), one statement clears them all:

   ```sql
   UPDATE items INDEXED BY items_claim_id_idx
      SET claim_id = NULL, stage = ?3, ver = ver + 1, free_since = ?4
    WHERE queue_no = ?1 AND claim_id = ?2 AND claim_id IS NOT NULL;
   DELETE FROM claims INDEXED BY sqlite_autoindex_claims_1
    WHERE queue_no = ?1 AND claim_id = ?2;
   ```
   Each record's own work (its map, index rekeys, counts and group heads) stays per record. Otherwise, release by primary key, one prepared statement per record. The bulk form cut the transactions stalled by cache pressure at 10M records from 44 of 300 to 11 of 300 at the 128 MiB cache, and finish then held flat: 695.4 finishes/s at 10M and 746.3 at 100M (MEASURED, SPIKE-004 Method, F6, F8).
6. **One explicit transaction per change, with prepared statements reused inside it.** SPIKE-006's claim path was 300–500× slower than its corrected form until every operation ran in one `BEGIN IMMEDIATE … COMMIT` and each per-row loop prepared its statement once (MEASURED, SPIKE-006 Method). In production, the unit is one apply transaction per packed vector (TD-016), never one autocommit per statement.
7. **No `UPDATE … FROM (VALUES …)` against a composite-key table.** Turso plans it as a nested scan: 243.6 updates/s against 25,937.9/s with per-row statements, about 100× (MEASURED, SPIKE-005). `items`, `item_fields`, `item_values` and `item_index` all have composite primary keys.
8. **Test the plans.** A unit test runs `EXPLAIN QUERY PLAN` on every hot statement and asserts the named index, no `SCAN` of `items` and no sorter on the claim walk. SPIKE-002 and SPIKE-004 made this check by hand. It must be re-run on every Turso version bump (SPIKE-002 F1).

## 12. Connection policy

| Setting | Value | Source |
|---|---|---|
| `page_size` | 4096 | `configure_connection`, `crates/fireweed-turso/src/local.rs` |
| `journal_mode` | `WAL` | same |
| `synchronous` | `NORMAL` on log-backed writers; the object log is the durable copy | same |
| `wal_autocheckpoint` | 0 on log-backed writers | same |
| Passive checkpoint | after every apply batch once the WAL exceeds 400 MiB | `maybe_passive_checkpoint_log_backed`, `LOG_BACKED_CHECKPOINT_TRY_BYTES`, `crates/fireweed-turso/src/projection.rs` |
| `cache_size` | 128 MiB for the writer and serving readers; 4 MiB for pooled readers | `local.rs` (`SERVING_READER_CACHE_KIB`, `POOLED_READER_CACHE_KIB`) |
| `temp_store` | `MEMORY` | `local.rs` |
| `busy_timeout` | 5 s | `local.rs` |

- **The checkpoint cap is mandatory.** Without it, a 300-claim, 300-finish probe on 10,000 rows did not finish in 270 s and read 157.6 GB and wrote 174.9 GB for about 300 KB of logical change. With it, the probe finished in 28.9 s (MEASURED, SPIKE-004 F0). A large WAL also makes checkpoints slow: truncating an 8.66 GB WAL took 54.0 s (MEASURED, SPIKE-002 F5).
- **Keep the 4 KiB page.** No larger page (8, 16 or 64 KiB) won consistently, and two were the worst on at least one operation (MEASURED, SPIKE-005). The spill threshold (§6.2) is tied to this page size.
- **Cache.**
  - Layout C gained nothing from 1 GiB over 128 MiB at 1.2M records: 229.61 against 228.07 claims/s (MEASURED, SPIKE-006 F8).
  - With field bytes on the claim row, 1 GiB removed every finish stall at 10M records (44 of 300 transactions to 0), and at 100 queues × 1M records claims fell 45.8% at 128 MiB (MEASURED, SPIKE-004 Method, F8).
  - The claim index needs about 26.4 B per Free record, so a file's claim indexes pass 128 MiB at about 5.1M Free records (INFERRED arithmetic). A claim touches only the index's interior pages and the leaves it walks, not the whole index.
  - The default stays 128 MiB. With the lean row, the G-100M shape meets AC-PERF-6's proposed envelope at 128 MiB, and 1 GiB brought no gain on claim, finish or sweep (MEASURED, SPIKE-004 F9; risk 1).
- **One writer per file.** Every write goes through one `Mutex<Connection>`.
  - Through that mutex, 1, 8 and 32 concurrent claimers got 166.1, 272.3 and 217.7 claims/s, with p50 2.6, 23.5 and 103.4 ms, and never failed.
  - With a separate connection per claimer, 32 claimers failed with `Busy("database is locked")` (MEASURED, SPIKE-004 F8).
  - Throughput does not grow with writers per file. It grows with files per node (§1). Reads use separate serving connections (TD-010).

## 13. Performance gates

TP-003 §3.12 owns every pass bar. This section maps each check of this layout to the TP-003 gate that decides it, and lists the capacity runs that are reported but never gate. API-006 leaves three cost rules to performance gates (its CS2): C9, the S2 cost bound, and F4's rule that a projection reads no unreturned field value.

| Check | Shape | TP-003 gate | Evidence |
|---|---|---|---|
| Plans | §11, rule 8, on the production schema: every hot statement uses its named index, with no `SCAN items` and no sorter on the claim walk; the `stats.groups` reads seek `group_heads_order_idx`, visiting at most `limit` groups, and `items_ready_idx` with no sorter; a claim with `fields: Some([])` reads no `item_fields` or `item_values` row, and one with a projection reads only the named spilled values (§6.3) | AC-PERF-1, AC-PERF-4 | The failure shape is SPIKE-004 F5's `USE SORTER FOR ORDER BY`. |
| Claims table | Extend and expiry discovery cost O(claims); a whole claim is finished in bulk by claim (§3, §11 rule 5) | AC-PERF-3 | SPIKE-004 F2, F3, F6 |
| Schema | The lean `items` row; per-field spill over 4,096 B; the 4 KiB page; the `queue_no` surrogate and the `BLOB` key; no per-queue schema object | AC-PERF-5, AC-PERF-8 | SPIKE-002, SPIKE-005, SPIKE-006 F2, F7 |
| 1M to 10M | `Next` claim and bulk finish on one queue at 1M and 10M records, interleaved in one run | AC-PERF-2 | 626.2 and 649.0 claims/s (MEASURED, SPIKE-004 F1, F6) |
| G-100M | One file; 100 queues × 1M records (950,000 in a stage nothing claims, 50,000 in the claimed stage); this layout; the §12 settings; one writer; 100-record `Next` claims, bulk finish, extend and sweep, round-robin over 20 queues; 3 trials × 100 repetitions (SPIKE-004's `finding8` harness); against a 10M single-queue control in the same host window | AC-PERF-6 | 10M single queue: 649.0 claims/s, p99 1,806.9 µs; 100M with field bytes on the claim row: 351.6 claims/s, p99 94,559.5 µs, a fail (MEASURED, SPIKE-004 F6, F8); 100M with this layout's lean claim row: 620.9 claims/s (95.7% of the control), p99 2,395 µs (1.33×), inside the proposed envelope (MEASURED, SPIKE-004 F9). AC-PERF-6 carries the proposed envelope (median ≥ 90% and worst-trial p99 ≤ 2× the control), INFERRED from SPIKE-004's 2–24% trial spreads. |
| Queues per file | `define_queue` and file open at 100, 1,000 and 10,000 queues in one file | AC-PERF-8 | Per-queue tables took 13,111.3 s to create and 26.64 s to open at 10,000 queues (MEASURED, SPIKE-006 F7). In this layout a queue is a few rows (§1, §8). |

- G-100M also records build time and file size. For reference, the 100M build with fields on the claim row took 120.55 minutes and produced a 45,093,863,424 B file (MEASURED, SPIKE-004 F8). These are not pass criteria.

**Capacity runs.** These are reported with their topology and never gate (TP-003 §3.12; TP-005 owns the capacity matrix). Whether any becomes a same-run gate is TP-003's call.
- **P1–P4.** `ss_phased_capacity`, N = 10,000, on kernel shapes: declared fields, ordinary stages and `rank`. The reference run was on `filesystem--turso`, which is not the ADR-024 public cell: 14,493 / 18,843 / 13,367 / 17,086 per s (MEASURED, evidence `1788659385`). They are a regression reference for this layout, not a product bar.
  - P2's profile is 1,024 B (`PROFILE_BYTES`, `crates/fireweed/tests/support/ss_capacity.rs`). Ported to a declared `Bytes` field, it stays in the cold map, so P1–P4 do not exercise the spill.
- **P2p, P3p, P4p and P4r**, on a Snorri-shaped queue:
  - 22 declared indexes, two of them keyed on `Stage`;
  - Snorri's large state and audit values as declared fields, so the ones over 4 KiB spill;
  - P2p mixes lease-free stage moves between ordinary stages into the updates;
  - P4p is a claim, then a settle that moves the record to another ordinary stage with `Release(t)` and a field patch, rekeying the `Stage`-keyed indexes;
  - P4r is a claim, then a settle that sets a new rank with `Release(t)`;
  - claims run both unprojected and with a projection that leaves out the large fields (§6.3).
- **32 queues per file.** The 32-shard workflow qualification, run as 32 queues in one partition file.
- **Snorri transition bench.** `WholeGroups` claims on the transition stage, the hydrating instance read, the instance record's `Expect::Version` write, and settles that move stages with history in fields, sized with Snorri's measured audit and state bytes.

## 14. Risks

1. **Cache pressure on the 100M claim.**
   - MEASURED: with about 300 B of fields on the claim row, 100 queues × 1M records in one file cut claims 45.8% (649.0 to 351.6 claims/s) and raised p99 52× (1,806.9 to 94,559.5 µs) against 10M records in one queue. Finish, extend and sweep held flat (SPIKE-004 F8).
   - MEASURED: with this design's lean claim row, the same shape runs claims at 620.9/s (within 4.3% of the 10M control) with p99 2,395 µs (1.33× the control, 39.5× below the fat row). Finish, extend and sweep stay flat, and a 1 GiB cache brings no gain on claim, finish or sweep (SPIKE-004 F9). The lean row removes table-page pressure; the claim index is no narrower, and at this scale that is enough.
   - MEASURED, mechanism not isolated: `extend` ran 43.4% slower at a 1 GiB cache than at 128 MiB (SPIKE-004 F9). It is one more reason to keep the 128 MiB default (§12).
   - The spike is not the product: TP-003 AC-PERF-6 runs the G-100M shape (§13) on the product implementation.
   - If the product misses the envelope: a larger writer cache for many-queue files, or fewer Free records per file (more partitions per node). Leasing a claim's rows with one `IN (…)` `UPDATE` does not help: `IN` on a composite key's second column does not seek (SPIKE-002 F1).
2. **SPIKE-006's layout B anomaly.**
   - MEASURED: layout B put the field map on the claim row, and its updates ran 7–35× slower than layout C's on a run with flat load. `update_small` ran at 131.43/s against 1,838.35/s, with 67–203% spreads and p99s of 122–242 ms (SPIKE-006 F3).
   - INFERRED: the proposed mechanism, claim churn evicting the blob pages of the shared table before the updates needed them, was not isolated.
   - Why it matters here: (a) C's claim advantage over B (1.7×) is explained, but its update advantage is not; (b) it is plausibly the same mechanism as risk 1, where field bytes also sat on the claim row; (c) so nothing field-shaped goes back onto `items` without a new measurement. A `spike diag`-style isolation is the follow-up if a B-like layout is ever reconsidered.
3. **Unprojected claims read every spilled value.**
   - A claim or read with `fields: None` fetches every spilled value of every returned record. Each costs about 50–70 µs more to read than an inline one (MEASURED, SPIKE-005). A 100-record claim with 2 spilled values per record adds about 10–14 ms to layout C's measured 4.4 ms (INFERRED), and C7's 16 MiB response cap can shorten the batch.
   - Mitigation: a consumer that does not need its large fields on the claim path names the fields it needs (API-006 F4, §6.3). The P2p–P4r capacity runs measure both shapes (§13).
4. **The descending index tree** doubles `item_index` write cost (INFERRED). P2p and P3p measure it. An alternative is one ascending tree with descending pages that re-sort ties by `seq`, but ties can be large (an index on `Stage` alone), so it is not proposed.
5. **Unmeasured structures (§9):** gate filtering; group heads, whose recomputation costs O(members) of each touched `(stage, group)` pair; stage counts, updated in every apply transaction; `items_ready_idx`, one more index write per claim and release of a grouped record; the request table; and the `members` and `carriers` counters. The §13 capacity runs measure them.
6. **Host noise.** Every spike ran on a shared WSL2 host with 1-minute load averages of 5–43. The gates re-measure on the release host, and absolute spike numbers are not SLAs.
7. **`delete_queue` purge.** Removing a large queue's rows costs writer time on the file. The background batches bound it per transaction, but the total is not measured.

## 15. A second implementation (non-normative)

ADR-025 commits to no second implementation, so this design specifies none. If a direct PostgreSQL implementation is ever built, it would implement `Store` against `fireweed-contract` only and pass the same suite. Most of this layout's logical shape would carry over: a lean claim row, a claims table, a stage-prefixed partial claim index, a per-field spill table beside one binary field map (PostgreSQL's TOAST rewrites a whole changed column, so the spill table would still be needed), and one shared index table with no per-queue tables. Its concurrency, group-exclusivity and counter designs would be its own, and none of it has been measured. Any such design must keep `stats` reads unlagged (API-006 V3).
