---
ddx:
  id: adr-per-queue-secondary-indexes
  depends_on:
    - adr-queue-as-shard-unit-and-projection-families
    - adr-cqrs-log-projection-storage-model
  review:
    self_hash: cd412536c22371beb00f53e7d439cbabed2de5f357c1cf2b8659b9ab38f4c055
    deps:
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
    reviewed_at: "2026-07-06T14:59:49Z"
---

# ADR-010: Per-Queue Projection-Maintained Secondary Indexes over Configured Item Fields

- Status: Accepted; **declaration shape superseded by ADR-011** (the untyped `IndexSpec { name, fields, unique }` and raw length-prefixed byte keys are replaced by axon-esf typed `IndexDef`/`CompoundIndexDef` and the canonical `axon_esf::index_key` encoder). The maintain-on-apply mechanism, per-queue scope, pre-commit uniqueness validation, `IndexQueryPort`, and phasing in this ADR stand.
- Date: 2026-06-27
- Supersedes/relates: ADR-008 (queue as shard unit + two projection families), ADR-001 (CQRS log/projection), ADR-009 (encapsulated library surface), CONTRACT-007 (queue schema — referenced for index *shape* only)
- Driver: consumer "cayce" needs to look up queued items by a configured field value without scanning, and to enforce uniqueness on a configured field, in tests that run on the in-memory backend.

## 1. Context

pqueue items already carry a structured field map `fields: BTreeMap<String, Bytes>` on every record
(`crates/pqueue-projection/src/lib.rs:47`, surfaced through `LiveItemView` at
`crates/pqueue-engine/src/port.rs:121`, and mutable in place via `update_fields` / FAC-1). Today the only
content-addressable lookup is by `client_item_key` (`ProjectionData::by_key`,
`crates/pqueue-projection/src/lib.rs:272`, `lookup_by_key` at `:674`). There is no way to ask "which item
has `fields["external_id"] == X`" except a full scan, and no way to *enforce* that at most one live item
carries a given field value.

The projection already maintains exactly one derived index — the priority-ordered eligibility set `elig`
(`eligible: BTreeSet<EligKey>`, `crates/pqueue-projection/src/lib.rs:274`) — using a **maintain-on-apply**
pattern: every mutating arm of `apply_command` (`:346`) keeps `elig` in sync inside the same unit of work
(`insert_pending` at `:291`, `transition` at `:320`, the `ReplacePending`/`PurgeItems` arms at `:470`/`:547`).
This ADR adds **secondary indexes over configured fields** that follow the *same* pattern, so every backend
that shares `ProjectionData` gets them for free, read-after-write, with no new I/O surface.

### Scope guardrails (authoritative, deliberately narrow)

- **Per-queue only.** An index belongs to one queue; there is no cross-queue lookup. This is a natural fit:
  one queue == one owner == one `ProjectionData` (ADR-008).
- **Over configured item FIELDS.** Index keys are built from one or more named entries of the item `fields`
  map. pqueue stays **domain-agnostic** — indexes are generic over field *names* and opaque *bytes* values.
  No tenant/job/run or any cayce concept is modeled.
- **Unique AND non-unique.** Unique = a push/upsert/update that would create a duplicate key fails atomically
  with `EngineError::Conflict` (`crates/pqueue-engine/src/error.rs`), committing nothing. Non-unique =
  lookup returns all matching items.
- A lookup returns enough to identify items: `(client_item_key, item_id, item_version)`.
- Maintained atomically on **push, upsert (`replace_if_pending`), `update_fields`, purge**; **read-after-write
  consistent** for atomic backends, including immediately after `update_fields`.
- Available in the **in-memory backend** (cayce's tests query it without production storage), not only SQL.

## 2. Decision

Add a per-queue, declaration-driven set of secondary indexes maintained by the projection state machine.

1. **Declaration** lives on `QueueDefinition` as a list of `IndexSpec { name, fields, unique }`, validated at
   `create_queue`.
2. **In-memory maintenance** lives in `ProjectionData` as a name-keyed set of composite-key maps, maintained
   in the same `apply_command` arms that already maintain `elig`, with a **pre-commit uniqueness validator**
   mirroring `update_fields_validate` (`crates/pqueue-projection/src/lib.rs:714`).
3. **Query** is a new read port returning `(client_item_key, item_id, item_version)` for exact composite-key
   match (unique-get and non-unique-lookup), surfaced as facade methods on `Pqueue`.
4. **Relational parity** (later phase) realizes the same indexes as a side index table with a partial unique
   constraint, returning identical results; proven by shared conformance scenarios run across both families.

---

## 3. Declaration model (`QueueDefinition`)

Add a new field to `QueueDefinition` (`crates/pqueue-core/src/domain.rs:555`) and to the construction in
`CreateQueue::validate` (`:580`-`:781`):

```rust
// pqueue-core/src/domain.rs (sketch — NOT written by this ADR)
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexSpec {
    /// Unique index name within the queue (the lookup handle).
    pub name: String,
    /// Ordered list of field names whose values compose the key. Order is significant.
    pub fields: Vec<String>,
    /// true => at most one live item may carry a given composite key (atomic Conflict on violation).
    pub unique: bool,
}

pub struct QueueDefinition {
    // ...existing fields...
    /// Per-queue secondary indexes over configured item fields. Empty (default) = no indexes,
    /// behaviour byte-identical to today.
    #[serde(default)]
    pub secondary_indexes: Vec<IndexSpec>,
}
```

`#[serde(default)]` keeps existing persisted definitions and the wire compatible (an absent key
deserializes to the empty vec). `CreateQueue` (the request, `:537`) gains the same field; the rest of the
construction is a straight copy-through.

**Validation at `create_queue`** (a new block in `CreateQueue::validate`, alongside the existing
`CreateQueueError::invalid_request` checks):
- `name` non-empty and **unique** across the queue's `secondary_indexes` (duplicate name → `invalid_request`).
- `fields` non-empty; each field name non-empty; no duplicate field name within one spec.
- A practical cap (e.g. `<= 8` indexes/queue, `<= 4` fields/index) → `invalid_request` if exceeded (cheap
  guard against pathological maintenance cost; exact numbers are an open decision, §9).
- **No referential check** that the field names will appear on pushed items — fields are dynamic per item,
  so an index simply does not cover items that lack its fields (see missing-field semantics next).

**Missing-field semantics (RECOMMENDED: sparse / skip-indexing).** If an item does not contain *every*
field named by a spec, the item is **not indexed by that index** (the entry is omitted entirely). Rationale:
(a) it matches conventional partial/sparse secondary-index behaviour, (b) it avoids a flood of spurious
unique collisions where many items share the "all-absent" key, (c) `fields` is a sparse `BTreeMap` so
absence is the normal case. The alternative (index-as-empty-bytes) is recorded as an open decision (§9).

---

## 4. In-memory structures (`ProjectionData`)

### 4.1 Composite-key encoding

A composite key is the deterministic, **length-prefixed** concatenation of each configured field's raw
bytes, in the spec's field order:

```
key = for each field name in spec.fields:  be_u32(value.len()) || value_bytes
```

Length-prefixing makes the encoding **unambiguous** (no separator can collide with field content — bytes are
arbitrary), order-sensitive (matching `fields` order), and total/`Ord` as a `Vec<u8>`. Field *values* are
hashed/compared as **raw bytes** — no normalization, casing, or trimming (pqueue is domain-agnostic; a
consumer that wants case-insensitivity normalizes before writing the field). Exact-match only; this encoding
intentionally does **not** support prefix/range scans in v1 (deferred, §8).

### 4.2 State

```rust
// pqueue-projection/src/lib.rs (sketch)
type CompositeKey = Vec<u8>;

enum SecondaryIndex {
    Unique(BTreeMap<CompositeKey, ItemId>),
    NonUnique(BTreeMap<CompositeKey, BTreeSet<ItemId>>),
}

pub struct ProjectionData {
    // ...existing: items, by_key, eligible, next_seq, priority_model, paused...
    /// Per-queue secondary indexes, keyed by IndexSpec.name. Built once from the queue's specs.
    indexes: BTreeMap<String, SecondaryIndex>,
    /// The specs themselves (field lists), needed to recompute keys from a record's fields.
    index_specs: Vec<IndexSpec>,
}
```

- Unique: `key -> item_id` (one live item).
- Non-unique: `key -> BTreeSet<ItemId>` (all matching items; `BTreeSet` gives deterministic, monotonic-id
  ordering for the returned list — `ItemId` is `Ord`, `crates/pqueue-core/src/domain.rs:109`).

### 4.3 Construction (signature change + ripple)

`ProjectionData::new` (`crates/pqueue-projection/src/lib.rs:280`) gains the specs:

```rust
pub fn new(priority_model: PriorityModel, index_specs: Vec<IndexSpec>) -> Self
```

Every construction site passes `definition.secondary_indexes.clone()` — all four log-replay backends already
hold `definition` at the call:
- `crates/pqueue-memory/src/lib.rs:608` (create) — and any rebuild path,
- `crates/pqueue-postgres/src/lib.rs:233` and `:862`,
- `crates/pqueue-objectlog/src/lib.rs:269` and `:750`,
- `crates/pqueue-sqlite/src/lib.rs:234` and `:838`.

Because the index specs come from the **`QueueDefinition`** (durable in the control plane / `queues`
table), not from the command log, a **log-replay rebuild** reconstructs the indexes correctly: the backend
fetches the definition, constructs `ProjectionData::new(model, specs)`, then replays commands — each replayed
Push/ReplacePending/UpdateFields/Purge re-runs the same maintenance, rebuilding the index deterministically.
No new log command and no snapshot-format change is required for the in-memory family.

### 4.4 Key-computation helper

```rust
// Returns (index_name, composite_key) for every index this record currently belongs to (sparse skip).
fn index_keys(&self, rec: &ItemRecord) -> Vec<(&str, CompositeKey)>
```

For each spec, gather the named field values from `rec.fields`; if any is missing, **skip** that index for
this record (§3 sparse rule); else emit `(name, encode(values))`.

---

## 5. Maintenance hooks (which `apply_command` arm touches which index)

All maintenance happens inside `apply_command` (`crates/pqueue-projection/src/lib.rs:346`), in the same UoW
as the existing `elig` updates, so it is atomic and read-after-write with the command's other effects. Two
private helpers added next to `insert_pending`/`transition`:

```rust
fn index_insert(&mut self, rec: &ItemRecord);   // add this record's keys to every covering index
fn index_remove(&mut self, rec: &ItemRecord);    // remove this record's keys from every covering index
```

| Arm (current line) | Index maintenance |
|---|---|
| `Push` → `insert_pending` (`:291`, `:350`) | `index_insert(rec)` after the record is built. |
| `ReplacePending` (`:470`) | `index_remove(superseded_rec)` when it is marked `superseded`, then `index_insert(replacement)` (the existing arm already removes the superseded item from `elig` and inserts the replacement via `insert_pending`). |
| `UpdateFields` (`:405`) | **Delta recompute:** capture `index_keys(rec)` *before* applying the field ops, apply the ops (existing `:414`-`:423`), recompute `index_keys(rec)` *after*; remove keys that left, insert keys that arrived. Keys unchanged by the delta are untouched. This is the read-after-write requirement called out for FAC-1. |
| `PurgeItems` (`:547`) | `index_remove(rec)` before the record is dropped from `items` (the arm already removes it from `by_key`/`elig`). |

**Terminal lifecycle transitions** (`Finalize`/`LeaseExpired`/`CohortExpired`, `:432`/`:484`/`:498`):
**no index change in v1** (RECOMMENDED). Field content does not change on a lifecycle transition, so the key
content is stable; an item stays indexed until it is **purged**. This matches cayce's explicit maintenance
list (push/upsert/update/purge) and keeps the unique invariant meaning "one *un-purged* item per key". The
consequence — a Completed item still occupies its unique key until purge — is deliberate and is recorded as
an open decision (§9), together with the OPTIONAL extension of allowing a native attribute (e.g. lifecycle
state) in a key, which *would* make terminal transitions move the key.

### 5.1 Unique-conflict: validate before commit (no rollback)

The module INVARIANT (`crates/pqueue-projection/src/lib.rs:17`-`:20`) is that `apply_command` is infallible —
`commit` (`:248`) appends to the log *before* applying and never rolls back, so any command that can be
rejected MUST be pre-validated. Unique-index conflict is exactly such a case. Add validators mirroring
`update_fields_validate` (`:714`), called by the backend ports *before* `commit_locked`:

```rust
// Reject (Conflict) if inserting these items would collide on any unique index — with an existing entry
// OR with another item in the same batch. Mutates nothing.
pub fn push_index_validate(&self, items: &[PushItem]) -> EngineResult<()>;

// Reject (Conflict) if applying field_ops to item_id would land on a unique key already held by a
// DIFFERENT item. Mutates nothing. (Run in addition to the existing update_fields_validate.)
pub fn update_fields_index_validate(
    &self, item_id: &ItemId, field_ops: &BTreeMap<String, Option<Bytes>>,
) -> EngineResult<()>;

// Reject (Conflict) if the replacement's unique keys collide with any item OTHER than superseded_item_id.
pub fn replace_pending_index_validate(
    &self, superseded_item_id: &ItemId, replacement: &PushItem,
) -> EngineResult<()>;
```

Backends wire these next to their existing pre-validation, e.g.:
- `MemoryBackend::push` (`crates/pqueue-memory/src/lib.rs:293`) and `replace_if_pending` (`:204`) call the
  matching validator before `commit_locked` (`:154`).
- `MemoryBackend::update_fields` (`:459`) already calls `update_fields_validate` at `:474`; it adds a call to
  `update_fields_index_validate` in the same guarded block.
- `Push`/`Upsert` collide-in-batch is handled inside `push_index_validate` (check the batch against itself
  and the existing index). `Purge` needs no validator (removal cannot violate uniqueness).

On `Err(EngineError::Conflict)` nothing is appended and nothing is applied — the same "structured rejection,
no divergence" contract as finalize/renew.

---

## 6. Query port + facade method

A new read port (sibling of `ProjectionRead`, `crates/pqueue-engine/src/port.rs:142`):

```rust
/// One hit from a secondary-index lookup — enough to identify and re-read the item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHit {
    pub client_item_key: ClientItemKey,
    pub item_id: ItemId,
    pub item_version: u64,
}

pub trait IndexQueryPort: Send + Sync {
    /// Exact composite-key get on a UNIQUE index. `Ok(None)` if no item holds the key.
    /// `EngineError::Invalid` if `index_name` is not a unique index on this queue.
    fn index_get_unique(
        &self, shard: &QueueKey, index_name: &str, key_values: &[Bytes],
    ) -> impl Future<Output = EngineResult<Option<IndexHit>>> + Send;

    /// Exact composite-key lookup on a NON-UNIQUE (or unique) index. Returns all matching items,
    /// ordered by item_id ascending (insertion/monotonic order). Empty if none.
    fn index_lookup(
        &self, shard: &QueueKey, index_name: &str, key_values: &[Bytes],
    ) -> impl Future<Output = EngineResult<Vec<IndexHit>>> + Send;
}
```

`key_values.len()` must equal `spec.fields.len()` (else `EngineError::Invalid`); the port encodes them with
the §4.1 rule and probes the map. Backing methods on `ProjectionData` (read side, near `lookup_by_key` at
`crates/pqueue-projection/src/lib.rs:674`):

```rust
pub fn index_get_unique(&self, index_name: &str, key_values: &[Bytes]) -> EngineResult<Option<IndexHit>>;
pub fn index_lookup(&self, index_name: &str, key_values: &[Bytes]) -> EngineResult<Vec<IndexHit>>;
```

Each resolves `ItemId`s through `self.items` to read `client_item_key` + `item_version`, so a hit always
carries the *current* version (read-after-write).

**Facade** on `Pqueue<B>` (`crates/pqueue/src/lib.rs`, near `live_item` at `:519`), adding `IndexQueryPort`
to the `LibBackend` bound (`:37`) and `ConformanceCore`:

```rust
pub async fn query_index_unique(
    &self, queue: &QueueKey, index_name: &str, key_values: Vec<Bytes>,
) -> EngineResult<Option<IndexHit>>;

pub async fn query_index(
    &self, queue: &QueueKey, index_name: &str, key_values: Vec<Bytes>,
) -> EngineResult<Vec<IndexHit>>;
```

These are pure reads (no epoch/fence — like `peek`/`live_item` at `:513`/`:519`).

---

## 7. Relational (SQL) realization — later phase

The relational family (`crates/pqueue-sqlite/src/relational.rs`, postgres analogue) has **no** shared
`ProjectionData`; the `pqueue_items` table *is* the projection and `apply_command_sql`
(`crates/pqueue-sqlite/src/relational.rs:610`) is the apply. Realize the indexes as a **side index table**
maintained in the same transaction:

```sql
CREATE TABLE IF NOT EXISTS pqueue_item_indexes (
    tenant_id TEXT NOT NULL,
    queue_id  TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_key  BLOB NOT NULL,          -- §4.1 length-prefixed composite encoding (identical bytes to in-memory)
    item_id    TEXT NOT NULL,
    client_item_key TEXT NOT NULL,
    item_version    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, index_name, item_id)
);
-- UNIQUE indexes get a partial unique constraint so a duplicate key violates atomically:
CREATE UNIQUE INDEX IF NOT EXISTS pqueue_item_indexes_uniq
    ON pqueue_item_indexes (tenant_id, queue_id, index_name, index_key)
    WHERE /* index_name is one of the queue's UNIQUE specs */;
```

Maintenance is added to the same `apply_command_sql` arms that already touch `pqueue_items`:
- `Push` / `ReplacePending` insert (`insert_item` at `:397`) → INSERT the covering rows; ReplacePending
  DELETEs the superseded item's rows first.
- `UpdateFields` (`:687`-`:740`) already read-merge-writes the `fields` JSON; in the same statement-group it
  DELETEs this item's index rows and re-INSERTs from the merged fields (delta or full-recompute-for-item).
- `PurgeItems` (`:904`) DELETEs this item's index rows.

**Uniqueness:** SQL gives the atomic guarantee *for free* — a violating INSERT raises a constraint error that
**rolls back the whole transaction**, which the backend maps to `EngineError::Conflict` (so nothing commits,
matching the in-memory pre-validate contract). Optionally also pre-`SELECT` to return `Conflict` without
relying on the driver error mapping; either way the observable result is identical.

**Lookups:** `index_get_unique` / `index_lookup` become `SELECT client_item_key, item_id, item_version FROM
pqueue_item_indexes WHERE tenant=? AND queue=? AND index_name=? AND index_key=? ORDER BY item_id`. Read-
after-write holds within the transaction.

The encoding bytes (§4.1) are **shared** between families so a key computed in memory and a key stored in SQL
are byte-identical — this is what lets the conformance suite assert the two families return the same hits.

---

## 8. Conformance strategy

Add shared scenarios in `crates/pqueue-conformance/src/scenarios.rs` (alongside `update_fields_merges_and_cas`
at `:53`), gated behind a new `ConformanceCore` bound `+ IndexQueryPort` (`crates/pqueue-conformance/src/lib.rs:74`),
and register them in `core_suite!` (`:268`). Each scenario builds a queue whose `QueueDefinition` declares an
index (a new `index_bearing_queue()` helper variant of the existing `shard()` def), so the SAME scenario runs
on the in-memory family (`conformance_suite!`) and — in the later phase — the relational family
(`core_suite!(@atomic)` for `SqliteRelationalBackend`), proving they AGREE:

1. `index_unique_get_after_push` — push two items with distinct unique-field values; `query_index_unique`
   returns each by its value; a missing value returns `None`.
2. `index_nonunique_lookup_returns_all` — push N items sharing a non-unique field value; `query_index`
   returns all N in item_id order; items lacking the field are absent (sparse rule).
3. `index_updated_after_update_fields` — push, then `update_fields` to change the indexed field; the OLD key
   no longer resolves, the NEW key resolves to the item with its bumped `item_version` (read-after-write).
4. `index_removed_after_purge` — push then `purge`; the key resolves to nothing.
5. `index_unique_conflict_rejected_atomically` — push an item on a unique key, then push/upsert/update a
   second item onto the same key → `Err(EngineError::Conflict)`, and assert the second item was NOT created /
   the first is unchanged (nothing committed).
6. `index_replace_pending_moves_key` — upsert (`replace_if_pending`) the same `client_item_key` with a new
   indexed value; the old value stops resolving, the new value resolves to the replacement id.

Phase 1 wires scenarios 1-6 against the in-memory backend(s). The relational family adopts the same scenarios
in Phase 2 (§7), which is the cross-family AGREE proof. A `log_replay_suite!` add-on (optional) can assert the
indexes rebuild correctly after a reopen/replay (§4.3).

---

## 9. Phasing

**Phase 1 — unblock cayce (in-memory only).**
- `IndexSpec` + `secondary_indexes` on `CreateQueue`/`QueueDefinition` + `create_queue` validation (§3).
- `ProjectionData` index maps + `ProjectionData::new` signature change + the four construction-site updates
  (§4.3); maintenance in Push/ReplacePending/UpdateFields/Purge (§5); the three pre-commit uniqueness
  validators (§5.1) wired into the memory backend ports.
- `IndexQueryPort` + `ProjectionData` read methods + facade `query_index*` (§6) for the memory backend.
- In-memory conformance scenarios 1-6 (§8).

**Phase 2 — relational parity.**
- `pqueue_item_indexes` side table + maintenance in `apply_command_sql` arms + lookups for sqlite and
  postgres relational backends (§7); same scenarios run cross-family (§8).

**Deferred (explicitly out of scope until asked):** prefix/range lookups (exact-match only in v1); native
attributes (lifecycle state, group_key) as key components (OPTIONAL, §9 open decision); terminal-transition
index removal (v1 keeps until purge); per-index TTL/retention; index introspection/metrics; objectlog
eventual-apply index queries beyond what log replay already yields.

---

## 10. Open decisions for the user

1. **Missing-field semantics.** RECOMMENDED: sparse/skip-indexing (item not in the index unless it has all the
   spec's fields). Alternative: index-as-empty-bytes. Skip avoids spurious unique collisions on absent fields.
2. **Terminal-transition maintenance.** RECOMMENDED: keep an item indexed until it is **purged** (lifecycle
   transitions don't change field content; matches cayce's push/upsert/update/purge list). Consequence: a
   Completed/Failed item still holds its unique key until purge. Alternative: drop from indexes on terminal
   transition (then a new item may reuse a unique key while the old one is merely complete-not-purged).
3. **Key encoding.** RECOMMENDED: length-prefixed `be_u32(len)||bytes` per field, raw bytes, exact-match only.
   Confirm no normalization (case/trim) is wanted in the engine (kept domain-agnostic — caller normalizes).
4. **Native attributes in keys (OPTIONAL).** Allow a key component to reference a record attribute (e.g.
   `lifecycle_state`, `group_key`) and not only a `fields` entry? Deferred by default; if accepted, it
   re-opens decision #2 because such a key *would* move on a transition.
5. **Caps.** Confirm the per-queue index count cap and per-index field-count cap (suggested 8 / 4), and any
   max composite-key byte length.
6. **Non-unique lookup ordering.** RECOMMENDED: item_id ascending (monotonic = insertion order). Confirm no
   priority/eligibility ordering is expected from a content lookup.
7. **Definition change after create.** v1 treats `secondary_indexes` as immutable post-`create_queue` (an
   incompatible re-create is a `QueueDefinitionConflict`, consistent with the existing definition-conflict
   rule). Confirm online add/drop of an index is out of scope for now.
