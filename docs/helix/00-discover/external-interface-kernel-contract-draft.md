---
ddx:
  id: external-interface-kernel-contract-draft
  depends_on:
    - external-interface-contract-research
  status: draft
---

# Fireweed kernel contract: synthesized draft (API-006 candidate)

Base: design A (records-literal). Grafted from the other designs:
- From B: execution lanes, token encoding, perf gates, `GateEdit`.
- From C: `Keys` skip reasons, `Expect::Exists`, gate ops inside units, and the Snorri call-site mapping.

Every blocker and major defect the judges raised is resolved in §11. A later verification pass (three lenses: citations, consistency, compliance; 51 findings) and the lead engineer's binding adjudication of it are applied throughout, with one row per finding in §12; a check pass follows in §13.

The owner's round-2 decisions (2026-09-23, §9.1) are applied throughout, and §14 summarizes what they changed. A round-2 verification pass (three lenses: consistency, production, coverage; 36 findings) is applied too, with one row per finding at the end of §14. Where the decisions conflict with round 1, round 2 wins:
- **Stages.** Every record is in exactly one stage that its queue declares. Terminal stages replace Completed and Failed. The only engine-owned state is the lease: Free or Leased.
- **No expiry.** Records live until the consumer deletes them. Retention is a consumer concern.
- **Definition and metadata.** The queue definition (stages and indexes) is immutable. Queue metadata is mutable, owned by the consumer and never interpreted by the engine.
- **Deletions.** Cohorts and the RESP adapter are gone. Fireweed is pre-production, so there is no existing data to keep and no replay compatibility.
- **Co-evolution.** Snorri adapts to the contract: groups for workflow instances, stages for record kinds.

Round-1 owner decisions that still stand:
- Per-transition metadata (history, audit, acks) is carried by the update or settle command and stored on the items themselves. Queue-level configuration is queue metadata (§3.2), not an item.
- There is no Idle, Parked or `Due::Never` state, no `record_kind` convention and no side store.

Two round-1 rules are superseded. "There is no never-claimable record" becomes: records in terminal stages are data and are never claimable. "`due: None` means claim-by-key only" becomes: `due: None` is removed (§10 O-3).

---

## 0. Summary

| Op | Facet | One-line purpose |
|---|---|---|
| `define_queue` | kernel | Create-or-verify a queue's immutable definition: its stages (each terminal or not) and its typed indexes. |
| `write` | kernel | The only mutation. It covers insert, upsert, sparse update, stage moves (finish and redrive included), CAS, settle, extend, force-clear, delete, gate flips and queue-metadata CAS. Writes are grouped into atomic units. `request_id` is optional. |
| `claim` | kernel | Lease records of one non-terminal stage exclusively: the next by rank, whole groups, named keys, or an index range. |
| `read` | kernel | Point reads by key, in any stage, plus the queue's definition and metadata. Observes every acknowledged write; never exposes tokens. |
| `stats` | kernel | Counts by stage × lease state, the pause flag, and a bounded, un-gated ready-group threshold read for one stage. |
| `scan` | Query | Page through records in a declared index. Observes every acknowledged write. |
| `aggregate` | Query | Count the records in an index range, grouped by index keys (fields or built-ins such as the stage), time buckets, value ranges or lease state. |

- **Public operations: 7, all mandatory for every implementation.**
  - Round 2 changed what the ops carry, not how many there are. Stages, queue metadata and the definition read ride existing ops, and `Finish` is gone.
  - The verification pass narrowed the ops without changing their number: `LeaseOp::Hold` and `read(Prefix)` are gone, the write `request_id` is optional, `read` and `scan` are fresh (V3), and Δ bounds only the time-driven transition (V4).
- **Today: 100 public entry points**, plus `pub mod turso_compose`:
  - 74 `Fireweed` methods and 4 `ProjectionControl` methods (`crates/fireweed/src/facade.rs:1020-1731`)
  - 10 `operator_*` methods (`operator.rs:887-1035`)
  - 12 free functions (`crates/fireweed/src/lib.rs:307, 5319-6358`)
- **Everything else becomes one of:**
  - the `fireweed::operator` helper module, built only on these 7 ops;
  - the S3×Turso constructor plus a `TursoAdmin` handle;
  - deleted.

---

## 1. Record model

**Identity.**
- A record is addressed by `(QueueKey { tenant, queue }, key)`.
- `Key` is UTF-8, 1..=1024 bytes, and compared bytewise.
- At most one record exists per key, in any stage.
- `item_id` leaves the contract. S3×Turso keeps it as its private row key: every row already carries `client_item_key TEXT NOT NULL` under the unique index `fireweed_items_active_key` (`crates/fireweed-relational/src/schema.rs:19,65-66`).

**Attributes.**

| Attribute | Type | Written by | Rule |
|---|---|---|---|
| `key` | `Key` | caller, insert | Identity. Immutable. |
| `version` | `Version { incarnation: u64, seq: u64 }` (`Ord`) | implementation | Opaque; compared for equality in CAS. Strictly increases on every applied change to the record: a write (stage moves included), a claim, a release or an extend. It is strictly greater than every version the key held before, including across delete and re-insert. Lease reclaim, gate ops and queue-metadata ops do not change it. |
| `stage` | `Stage` | caller: set on insert, changed by any write | One of the queue's declared stages (§3). Every record is in exactly one. The engine enforces no transition graph: a write may move a record between any two declared stages, including out of a terminal stage. |
| `stage_since` | `Timestamp` | implementation | When the record entered its current stage. Set to now on insert and by every write that changes `stage`. A write that names the current stage again does not change it. |
| `lease` | `Free \| Leased` | claim, write, reclaim | The only engine-owned state. A record in a terminal stage is always Free (W1, C1). |
| `leased_until` | `Option<Timestamp>` | implementation | `Some` iff the record is Leased. The token is never readable. |
| `due` | `Timestamp` | caller | The record is claimable by scheduled selects of its stage once `due <= due_by`. An insert that leaves it unset gets now (W5). It has no effect while the record is in a terminal stage. |
| `rank` | `i64` | caller | Ascending claim order within a stage; default 0. For timestamp priority use epoch-µs; for descending order, negate the value. |
| `group` | `Option<GroupKey>` | caller, insert only | The whole-group claim axis. `WholeGroups` skips a group with a Leased member in any stage (C4, C6a); `Next`, `Keys` and `Where` ignore group leases. |
| `gates` | `BTreeSet<GateKey>`, at most 8 | caller | The record is unclaimable while any carried gate is blocked. |
| `fields` | `BTreeMap<String, Value>` | caller, patched per key | Typed scalars: `Str \| Int(i64) \| Float(f64, finite) \| Bool \| Time \| Bytes`. The queryable data, and with the built-in attributes the source of index keys (§3). |
| `payload` | `Option<Bytes>` | caller, whole replace | Opaque. Stored off the hot row. |
| `attempts` | `u32` | implementation | +1 per claim. Never reset (S3×Turso never emits `Finalize{Rearm}`, §7.2). A consumer that limits attempts across a redrive keeps its own base in `fields` (§5 `redrive`). |

- **Removed in round 2:** `state` (Pending, Leased, Completed, Failed), `expires_at` and `finished_at`. `stage` plus `lease` replace `state`, and `stage_since` replaces `finished_at`.
- **Derived attributes, not returned:**
  - `pending_since` is set to now on insert, on `Release`, on `Clear` of a Leased record, and by every write that changes `stage`. On lease reclaim it is set to `leased_until`. No other write changes it.
  - For a Free record in a non-terminal stage, `ready_since = max(due, pending_since)`. Records in terminal stages have no `ready_since` and are never counted by S2.
- `Timestamp` is UTC epoch microseconds (`i64`), so PostgreSQL `timestamptz` round-trips exactly.

**Lifecycle.** A record moves on two independent axes. The stage belongs to the caller and the lease belongs to the engine.
```
STAGE (caller-owned: any move between declared stages is one write)

  insert: Put{stage: a}
        │
        ▼
     [ a ] ──── Put{stage: b} ────► [ b ] ──── Put{stage: done} ────► [[ done ]]
       ▲                                                                   │
       └─────────────────── Put{stage: a}   (redrive) ─────────────────────┘

     [ s ]    non-terminal stage: work. Claimable by claims that name s.
     [[ s ]]  terminal stage: data. Never claimable, always Free.

LEASE (engine-owned: exists only in a non-terminal stage)

     FREE ── claim(stage = the record's stage) ──► LEASED ── {Extend(t,d), Put} ──► LEASED (same token)
       ▲                                              │
       ├───────── {Release(t) | Clear, Put} ◄──────────┤
       └───── lease reclaim, within Δ of leased_until ┘

  finish   {Release(t), Put{stage: <terminal>}}        one write: the lease ends and the record moves
  redrive  {None | Clear, Put{stage: <non-terminal>}}  one write, out of a terminal stage
  delete   {…, Delete} from any stage (a Leased record needs Release(t) or Clear) ──► absent; the key is free at once
```

**What is not in the model.**
- No Completed or Failed state and no `Finish` op. Finishing is a write that moves the record to a terminal stage, usually with `Release(t)`.
- No expiry: no `expires_at`, no retention setting and no key tombstone. A record lives until the consumer deletes it. A future `delete-by-filter` operation could serve bulk retention; it is not part of this contract.
- No `due: None` (claim-by-key only). A record that is only ever read by key belongs in a terminal stage. Work that is only ever claimed by key belongs in a stage that only `Keys` claims name (§10 O-3).
- No Idle, Parked, `Due::Never` or `record_kind`, no side store, and no transition graph.
- **Records in terminal stages are data; records in non-terminal stages are work.** This replaces the round-1 rule that "a record nothing claims is a mapping defect". Snorri's instance items and open awaits are data in terminal stages (§8.1).

---

## 2. Operations

All types live in the new I/O-free crate `fireweed-contract`. The crate holds:
- the DTOs, the trait and the errors;
- the shared types below, including the `Clock` trait (§4);
- the constants (§4);
- validation;
- `fingerprint`;
- the axon-esf index-key encoder;
- the order comparators.

The trait is object-safe. `fireweed::Fireweed` wraps `Arc<dyn Store>` and delegates 1:1.

### 2.0 Shared types
```rust
pub struct QueueKey { pub tenant: String, pub queue: String } // each 1..=128 bytes of non-blank UTF-8 (today's TenantId/QueueId, now bounded; §4)
pub struct Key(String);            // UTF-8, 1..=1024 B, compared bytewise
pub struct Stage(String);          // ^[A-Za-z0-9._:-]{1,128}$; must be one of the queue's declared stages
pub struct GroupKey(String);       // ^[A-Za-z0-9._:-]{1,256}$
pub struct GateKey(String);        // ^[A-Za-z0-9._:-]{1,256}$; "*" is reserved (W9)
pub struct RequestId(String);      // 1..=128 B
pub struct LeaseToken(Bytes);      // opaque and self-describing (§4 Leases); never readable from a Record
pub struct Cursor(Bytes);          // opaque keyset over (encoded index key, insertion seq) (Q2)
#[derive(PartialEq, Eq, PartialOrd, Ord)] pub struct Version { pub incarnation: u64, pub seq: u64 }
pub type Timestamp = i64;          // UTC epoch µs
pub enum Lease { Free, Leased }
pub enum Value { Str(String), Int(i64), Float(f64 /* finite */), Bool(bool), Time(Timestamp), Bytes(Bytes) }
pub enum Attr { Field(String), Stage, Group, Due, Rank, StageSince } // what an index key, range or aggregate dimension names
pub struct Record {
    pub key: Key, pub version: Version, pub stage: Stage, pub stage_since: Timestamp,
    pub lease: Lease, pub leased_until: Option<Timestamp>, pub due: Timestamp, pub rank: i64,
    pub group: Option<GroupKey>, pub gates: BTreeSet<GateKey>, pub fields: BTreeMap<String, Value>,
    pub payload: Option<Bytes>, pub attempts: u32,
}
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
    fn sleep_until(&self, t: Timestamp) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}
```
- `Value` order: within a variant, the natural order (`Str` and `Bytes` bytewise, `Float` numeric); across variants, the declaration order above. It matches the ESF encoded order for values of one declared type. Every place that compares values (`IndexRange` bounds, `aggregate` group tuples) compares values of one declared type, because range keys, `Dim` attributes and `Ranges.attr` must be index keys (Q1, Q4). In group tuples, `None` sorts first.
- Built-in attributes have fixed types: `Stage` and `Group` are `Value::Str`, `Due` and `StageSince` are `Value::Time`, and `Rank` is `Value::Int`. `Stage` values name a stage but compare by declaration position, not bytewise, because the index stores the stage's ordinal (§3).
- `Record` carries every §1 attribute except the token and the derived `pending_since`/`ready_since`.

```rust
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;
pub trait Store: Send + Sync + 'static {
    fn define_queue<'a>(&'a self, q: &'a QueueKey, def: QueueDef) -> BoxFuture<'a, Defined>;
    fn write<'a>(&'a self, q: &'a QueueKey, req: WriteRequest) -> BoxFuture<'a, WriteResponse>;
    fn claim<'a>(&'a self, q: &'a QueueKey, req: ClaimRequest) -> BoxFuture<'a, ClaimResponse>;
    fn read<'a>(&'a self, q: &'a QueueKey, req: ReadRequest) -> BoxFuture<'a, ReadResponse>;
    fn stats<'a>(&'a self, q: &'a QueueKey, req: StatsRequest) -> BoxFuture<'a, Stats>;
    fn scan<'a>(&'a self, q: &'a QueueKey, req: ScanRequest) -> BoxFuture<'a, ScanPage>;             // Query
    fn aggregate<'a>(&'a self, q: &'a QueueKey, req: AggregateRequest) -> BoxFuture<'a, AggregateResponse>; // Query
}
pub enum Error {
    QueueNotFound, DefinitionConflict { existing: QueueDef }, Invalid(String), TooLarge,
    RequestIdConflict, RequestExpired, Busy { retry_after: Duration }, Unavailable(String),
}
```

**Envelope error classes.**
- Definitely not applied: `QueueNotFound`, `DefinitionConflict`, `Invalid`, `TooLarge`, `RequestIdConflict`, `RequestExpired`, `Busy`.
- Outcome unknown: `Unavailable` only. Resend with the same `request_id` to resolve it; for an id-less write, `read(Keys)` the keys it touched (D8), and `read{queue: true}` if it carried a metadata op.
- Envelope-rejected requests record nothing.

### 2.1 `define_queue`
Why it exists: the stage set and the indexes are fixed per queue before any record exists (§3).

```rust
pub struct QueueDef { pub stages: Vec<StageDef>, pub indexes: Vec<IndexDef> }
pub struct StageDef { pub name: Stage, pub terminal: bool }
pub struct IndexDef { pub name: String, pub keys: Vec<IndexKey> }
pub enum IndexKey { Field { name: String, ty: IndexType }, Stage, Group, Due, Rank, StageSince } // IndexType = Str|Int|Float|Time|Bool (axon-esf)
pub struct Defined { pub created: bool }
```

- **D1.** An absent queue MUST be created atomically and durably, with empty queue metadata at version 0 (W13), returning `created: true`.
- **D2.** An identical definition MUST return `created: false` with no effect. Identical means equal after sorting `indexes` by name. The order of `stages` and the key order inside an index are significant.
- **D3.** A different definition MUST fail with `DefinitionConflict { existing }` and have no effect. There is no alter or delete: the definition is immutable (§3.1 says why). Queue metadata is not part of the definition; `define_queue` neither sets nor compares it.
- **D4.** The definition MUST be validated:
  - 1..=32 stages with unique names, at least one of them non-terminal;
  - at most 32 indexes, with unique names of 1..=128 bytes;
  - 1..=8 keys per index, each built-in key at most once per index;
  - field names of 1..=128 bytes;
  - a field name has one `IndexType` across all indexes.
- **D5.** Every other op on an undefined queue MUST fail with `QueueNotFound`.
- Idempotency: the operation is naturally idempotent and takes no `request_id`.

### 2.2 `write`
Why it exists: it is the single mutation verb. A settle is a write that carries a lease sub-op, and the settle's patch, stage move included, is stored on the settled item.

```rust
pub struct WriteRequest { pub request_id: Option<RequestId>, pub units: Vec<Unit> }
pub struct Unit { pub writes: Vec<RecordWrite>, pub gates: Vec<(GateKey, GateState)>,
                  pub metadata: Option<MetadataOp> }                                        // atomic
pub enum GateState { Open, Blocked }
pub struct MetadataOp { pub expect: MetaExpect, pub patch: BTreeMap<String, Option<Value>> } // Some=set, None=remove
pub enum MetaExpect { Any, Version(u64) }
pub struct RecordWrite { pub key: Key, pub expect: Expect, pub lease: LeaseOp, pub op: Op }
pub enum Expect { Any, Absent, Exists, Version(Version) }
pub enum LeaseOp { None, Extend(LeaseToken, Duration), Release(LeaseToken), Clear }
pub enum Op { Put(Patch), Delete }
#[derive(Default)] pub struct Patch {
    pub stage: Change<Stage>, pub due: Change<Timestamp>, pub rank: Change<i64>, pub group: Change<Option<GroupKey>>,
    pub gates: GateEdit, pub clear_fields: bool, pub fields: BTreeMap<String, Option<Value>>, // Some=set, None=remove
    pub payload: Change<Option<Bytes>>,
}
#[derive(Default)] pub enum Change<T> { #[default] Keep, Set(T) }
#[derive(Default)] pub enum GateEdit { #[default] Keep, Replace(BTreeSet<GateKey>), Edit { add: BTreeSet<GateKey>, remove: BTreeSet<GateKey> } }
pub struct WriteResponse { pub units: Vec<UnitOutcome>, pub replayed: bool }
pub enum UnitOutcome { Applied { writes: Vec<WriteOutcome>, metadata_version: Option<u64> },
                       Rejected { at: At, reason: Rejection } }
pub enum At { Write(u32), Metadata }
pub enum WriteOutcome { Put { version: Version, created: bool }, Deleted { existed: bool } }
pub enum Rejection { Exists { version: Version }, NotFound, VersionMismatch { current: Version },
                     Leased, StaleLease, MetadataVersionMismatch { current: u64 }, Invalid(String) }
```

Round 2 removed `Op::Finish`, `Outcome`, `WriteOutcome::Finished`, `Patch.expires_at` and `Rejection::Terminal`. Records in terminal stages are writable.

- **W1. Envelope validation happens before any effect.** Failures are `Invalid` or `TooLarge`, and nothing is recorded. Checked:
  - the limits in §4;
  - key, gate, group, stage and field-name syntax;
  - a patch that names a stage the queue does not declare;
  - a key appearing more than once anywhere in the request;
  - a gate key appearing more than once in the request's gate ops;
  - more than one metadata op in the request;
  - the reserved gate `"*"` appearing in any record's gates (it is legal only in `Unit.gates`);
  - `Extend` combined with `Delete`, or with a patch that names a terminal stage (a Leased record is never in a terminal stage);
  - an `Extend` duration outside 1 ms..=1 h;
  - an indexed field whose value type differs from its declaration;
  - an indexed field value whose encoding alone makes an index key exceed the §4 limit (a key that exceeds it only together with stored values is a per-record `Invalid`, W8);
  - a metadata entry name outside 1..=128 bytes, or a metadata patch whose encoding alone exceeds 64 KiB;
  - a request with no units, a unit with no writes, no gate ops and no metadata op, or a `GateEdit::Edit` whose `add` and `remove` sets overlap.

  A unit with gate ops or a metadata op and no writes is legal (the pause helper, §5; Snorri's manifest bootstrap, §8.1).
- **W2. Units apply in request order.**
  - Each unit is atomic: all its writes, gate ops and metadata op apply, or none do.
  - A rejected unit reports where it failed (`At::Write(i)` for the first failing write, or `At::Metadata`) and the reason, and has no effect on other units.
  - Keys are unique per request and there is at most one metadata op per request, so units are independent: each unit's preconditions see the state as of the request.
  - The applied units of one request become durable together.
- **W2a. Evaluation order.** Each write is checked in this order, and the first failure is reported:
  1. `Expect` (W3).
  2. Absent key: `Put` gives `StaleLease` if the lease op is `Extend` or `Release`. Otherwise it inserts (W5), or gives `Invalid` if its patch does not set `stage`. `Delete` (with `Expect` `Any` or `Absent`) gives `Deleted { existed: false }` for every lease op.
  3. Lease op (W4): `Leased` or `StaleLease`.
  4. Patch validity (W8): `Invalid`.

  Once every write of the unit passes, the unit's metadata op is checked (W13): `MetadataVersionMismatch` or `Invalid`. Round 2 removed the terminal-record step, because records in terminal stages are writable.
- **W3. `Expect`:**
  - `Any`: no check.
  - `Absent`: the record must not exist, else `Exists { version }`.
  - `Exists`: else `NotFound`.
  - `Version(v)`: else `NotFound` or `VersionMismatch { current }`.
- **W4. `LeaseOp`:**
  - `None`: the caller holds no lease. If the record is Leased, the write is rejected with `Leased`.
  - `Extend(t, d)` and `Release(t)`: the record MUST be Leased with token `t` and `leased_until > now` by the implementation `Clock`, else `StaleLease`.
    - `Extend` sets `leased_until = now + d` (d in 1 ms..=1 h, checked in W1) and keeps the same token. A holder patches while keeping its lease with `{Extend(t, d), Put}`.
    - `Release` ends the lease.
  - `Clear`: ends any lease without a token (force). The former holder's token becomes invalid. `Clear` on a Free record is a no-op and never causes a rejection.
- **W5. `Put(patch)`:**
  - On an absent key: requires `Any` or `Absent`, a lease op of `None` or `Clear` (W2a), and a patch that sets `stage` (else a per-record `Invalid`). The record is inserted Free with defaults (`due: now`, `rank: 0`, no group, no gates, empty fields, no payload, `attempts: 0`, `stage_since = pending_since = now`), then the patch is applied. Any declared stage may be named, terminal included, so a record can be written straight into a terminal stage as data.
  - On an existing record, in any stage: only the `Set` attributes change.
    - With `Release`, or `Clear` on a Leased record, the lease ends and the record is Free.
    - With `Extend` it stays Leased; its patch cannot name a terminal stage (W1). A move to another non-terminal stage keeps the lease, the token and `leased_until`, so the record is not a candidate of its new stage until the lease ends (C1, C6).
    - With `None`, or `Clear` on a Free record, it stays Free.
  - **Stage moves.** `stage: Set(s)`, where s differs from the current stage, moves the record to s: `stage_since = now` and `pending_since = now`. Any declared stage may follow any other. The insertion sequence is kept (§4 Ordering).
  - Finishing is a stage move: `{Release(t), Put{stage: Set(<terminal>)}}` by the holder, `{None, Put{stage: Set(<terminal>)}}` on a Free record, or `{Clear, Put{stage: Set(<terminal>)}}` by force. Redrive is `{None | Clear, Put{stage: Set(<non-terminal>)}}` on a record in a terminal stage.
- **W6. Retired (round 2).** It defined `Finish`. Finishing is now a `Put` that moves the record to a terminal stage (W5). Terminal records are writable, so the `Terminal` rejection is gone.
- **W7. `Delete`:**
  - Removes the record and any lease, in any stage.
  - A Leased record requires `Release(t)` or `Clear`; `None` gives `Leased` (W4).
  - An absent key with `Expect` `Any` or `Absent` gives `Deleted { existed: false }` for every lease op.
- **W8. Patch semantics:**
  - `stage` must name a declared stage (W1).
  - `clear_fields` empties the field map before `fields` is applied. Field keys the patch does not name are untouched.
  - `GateEdit` is resolved against the stored set. A result with more than 8 gates is a per-record `Invalid`.
  - Setting `group` on an existing record to a different value is a per-record `Invalid`.
  - `payload` is replaced whole.
  - A result whose encoded record (key, fields and payload) exceeds 4 MiB, or whose encoded key in any index exceeds 1024 B (§4), is a per-record `Invalid`.
  - A change to `stage` (which also sets `stage_since`, W5), `due`, `rank` or a field rekeys every index that has any changed attribute, `StageSince` included, as a key (§3.1, §7.5 risk 10).
  - The group, gate-count, record-size and index-key checks above use stored values, so an implementation decides them before the acknowledgement from the stored record (§7.2 `write` update row).
- **W9. Gate ops:**
  - A gate op sets the queue-level state of a gate key. `"*"` is the queue pause gate, implicitly carried by every record.
  - A gate never set is Open. An Open gate holds no storage.
  - Gate ops change no record, no version and no metadata.
  - They apply atomically with the unit's record writes and metadata op.
- **W10.** Inserts MUST NOT be rejected because of group size.
- **W11.** Success MUST mean durable (§4).
- **W12.** A claim issued after the acknowledgement MUST observe the write (V2), and so MUST a `read` or `scan` (V3), including a `read{queue: true}` of the metadata (R4).
- **W13. Queue metadata op** (round 2, §3.2):
  - Queue metadata is a map of `String → Value` with a `u64` version. A new queue has an empty map at version 0 (D1).
  - `MetaExpect::Version(v)` requires the current version to be v, else `MetadataVersionMismatch { current }`. `MetaExpect::Any` skips the check.
  - The patch is sparse: `Some(value)` sets an entry, `None` removes it, and entries it does not name are untouched.
  - An applied op increments the version by 1 and reports the new version in `metadata_version`.
  - A result whose encoding exceeds 64 KiB (§4) rejects the unit with `Invalid`.
  - The engine never interprets metadata: no op's behavior depends on it, and a metadata op changes no record, no record version and no gate.
  - It applies atomically with the unit's record writes and gate ops.
- Errors: `QueueNotFound`, `Invalid`, `TooLarge`, `RequestIdConflict`, `Busy`, `Unavailable`.
- Idempotency (§9 D3):
  - `request_id` is optional.
  - With an id, the same id and fingerprint within R returns the recorded `WriteResponse` with `replayed: true`, without re-evaluation. A different fingerprint returns `RequestIdConflict`.
  - Without an id, an `Unavailable` outcome is resolved by `read(Keys)` of the touched keys, and by `read{queue: true}` for a metadata op (D8). A resent id-less request is evaluated again, so an insert that did apply comes back as `Exists`, and a metadata CAS that did apply comes back as `MetadataVersionMismatch`.

### 2.3 `claim`
Why it exists: it is the only op that selects rather than addresses, and the only op that creates leases.

```rust
pub struct ClaimRequest { pub request_id: Option<RequestId>, pub stage: Stage, pub select: Select,
                          pub max_records: u32, pub lease: Duration, pub due_by: Option<Timestamp> }
pub enum Select { Next, WholeGroups { max_groups: u32 }, Keys(Vec<Key>),
                  Where { range: IndexRange, descending: bool } }
pub struct ClaimResponse { pub claimed: Vec<Claimed>, pub skipped: Vec<(Key, Skip)>,
                           pub partial_group: Option<GroupKey>, pub replayed: bool }
pub struct Claimed { pub record: Record, pub lease: LeaseToken }
pub enum Skip { NotFound, Terminal, OtherStage(Stage), Leased, Blocked, NotAttempted }
```

- **C1. One stage per claim.** Every claim names one stage, which MUST be declared and non-terminal, else `Invalid`. Only records in that stage are candidates, so records in terminal stages are never claimable. Candidates for scheduled selects (`Next`, `WholeGroups`, `Where`) are records that satisfy all of the following:
  - in the claim's stage;
  - Free;
  - `due <= due_by` (default: implementation now);
  - no carried gate blocked;
  - `"*"` not blocked.
- **C2. `Keys(ks)`:**
  - At most 1000 keys, deduplicated in first-occurrence order. `max_records` MUST be at least the number of distinct keys, otherwise `Invalid`.
  - It ignores `due`, but respects the stage, gates, pause and leases.
  - Every distinct key is reported, either claimed or in `skipped` with a reason. C5's omission allowance does not apply to `Keys`: every key is classified exactly.
  - Skip reasons: `NotFound` (no record), `Terminal` (the record is in a terminal stage), `OtherStage(s)` (the record is in another non-terminal stage s), `Leased`, and `Blocked` (a gate or pause). When several apply, the first of `NotFound`, `Terminal`, `OtherStage`, `Leased`, `Blocked` is reported.
  - If C7's byte limit stops the claim, each remaining key is reported as `NotAttempted`.
- **C3. Each claimed record becomes Leased,** with:
  - a fresh unguessable token;
  - `leased_until = now + lease` (1 ms..=1 h);
  - `attempts + 1`;
  - a new version.
  All leases in one response are durable together before the response returns.
- **C4. Order** (always within the claim's stage):
  - `Next`: `(rank asc, insertion sequence asc)`. The insertion sequence is the §4 total order, fixed at insert and never changed.
  - `WholeGroups { n }`:
    - Considers groups with no Leased member **in any stage**, ordered by their first candidate. Group exclusivity therefore spans stages: while any record of a group is Leased, no `WholeGroups` claim of any stage returns that group (C6a, §9 D18). `Next`, `Keys` and `Where` do not check group leases.
    - Returns all candidates of each group. Omission (C5) applies only to whole groups: a returned group contains every candidate member, or `partial_group` is set.
    - Stops before a group that would exceed `max_records` or the response byte limit (C7).
    - If the first group alone exceeds `max_records` or the response byte limit, it returns that group's longest prefix that fits, in C4 order, and sets `partial_group`.
    - Ungrouped records are never selected.
  - `Where`: index order (descending if requested), then insertion sequence ascending. Only records of the claim's stage are returned, whatever the index's keys.
  - `Keys`: request order.
- **C5. Result guarantees:**
  - Results MUST be in the C4 order.
  - Every returned record MUST have been a candidate under all acknowledged writes and gate changes (V2).
  - A claim MAY omit candidates, for example candidates under concurrent mutation or not yet reclaimed. `WholeGroups` may omit only whole groups (C4); `Keys` never omits (C2). A short or empty result is a successful poll.
- **C5a. Progress.** If no write, claim or gate op on the queue has been in flight during the last Δ and no candidate is inside a V4 window, a claim MUST return `min(max_records, number of candidates)` records, subject to the C4 group rules and C7. Those records MUST be the first candidates in C4 order.
- **C6.** A record MUST never have two live leases.
- **C6a. Group exclusivity.** A `WholeGroups` claim MUST NOT return a group any record of which is Leased, by any claim of any stage, at the claim's linearization point. `Next`, `Keys` and `Where` do not check group leases, so a consumer that relies on one holder per group claims those stages only with `WholeGroups` (§8.1).
- **C7.** The response is at most 16 MiB. The byte limit never empties a result: if at least one candidate exists, a claim that stops at the limit returns at least the first (a record is at most 4 MiB, §4). The claim never splits a group except in the `partial_group` case (C4).
- **C8. Idempotency:** `request_id` is optional.
  - When present, a replay within R with the same fingerprint returns the same records and tokens, rendered from current state, if every lease is still held by that claim. `skipped` and `partial_group` are returned as recorded. A lease is held iff the record is Leased with that token and `leased_until > now`. Otherwise the replay returns `RequestExpired`.
  - A different fingerprint returns `RequestIdConflict`.
  - Without an id, a lost response leaks the leases until they lapse.
- Errors:
  - `Invalid`: a stage that is undeclared or terminal, `max_records` of 0 or over 1000, a lease out of bounds, an undeclared index or a bad range shape, more than 1000 keys, or `Keys` with `max_records` below the number of distinct keys.
  - Also `QueueNotFound`, `RequestIdConflict`, `RequestExpired`, `Busy`, `Unavailable`.

### 2.4 `read`
Why it exists: point loads replace `live_item(s)`, `claimed` and `side_record(s)`, and the queue read replaces `queue_definition` and serves metadata CAS (read the version, then write with `MetaExpect::Version`). There is no key-prefix listing; a consumer lists records through a declared index with `scan` (§6, §8.2).

```rust
pub struct ReadRequest { pub keys: Vec<Key>, pub queue: bool }
pub struct ReadResponse { pub records: Vec<Option<Record>>, pub queue: Option<QueueState> }
pub struct QueueState { pub definition: QueueDef, pub metadata: BTreeMap<String, Value>, pub metadata_version: u64 }
```

- **R1.** At most 1000 keys. The response has one positional `Option<Record>` per key, in any stage and lease state.
- **R2.** Removed in the verification pass. `read(Prefix)` served only Snorri's instance listing, which is now `scan` over a Snorri-declared index, and `retained_items`, which has no consumer.
- **R3.** A read observes every write, gate change and metadata op acknowledged before it was issued (V3). It never mutates and never returns tokens.
- **R4.** `queue: true` also returns the queue's definition and its metadata with the metadata version. A request with no keys and `queue: false` is `Invalid`. (New in round 2.)
- Errors: `QueueNotFound`, `Invalid`, `TooLarge` (a response over 16 MiB), `Unavailable`.

### 2.5 `stats`
Why it exists: backlog counts by stage, and Seventh Sense's `hasNextTasks` threshold, which must be a bounded read and never a claim.

```rust
pub struct StatsRequest { pub groups: Option<GroupStatsRequest> }
pub struct GroupStatsRequest { pub stage: Stage, pub limit: u32 /* <= 10_000 */, pub due_by: Option<Timestamp> /* default: now */ }
pub struct Stats { pub stages: Vec<StageCount>, pub paused: bool, pub groups: Option<GroupStats> }
pub struct StageCount { pub stage: Stage, pub free: u64, pub leased: u64 }
pub struct GroupStats { pub ready_groups: u32, pub oldest_ready_since: Option<Timestamp> }
```

- **S1.** `stages` has one entry per declared stage, in declaration order. It counts every record that has not been deleted, by stage and lease state; `leased` is always 0 for a terminal stage. `paused` means `"*"` is blocked. The values observe every acknowledged write (V3) and reflect lease reclaim within Δ (V4).
- **S2.** `groups.stage` MUST be a declared, non-terminal stage, else `Invalid`. `ready_groups = min(limit, number of distinct groups that have a Free record in that stage with ready_since <= due_by)`.
  - `due_by` defaults to the implementation's now.
  - Ungrouped records are not counted.
  - `oldest_ready_since` is the minimum `ready_since` over those records.
  - Both ignore gates, pause and Leased members. They count intrinsic readiness only, so they can overcount the groups `WholeGroups` can claim (D13).
  - The cost MUST be bounded by `limit`.
- **S3.** `stats` never mutates.

### 2.6 `scan` (Query)
```rust
pub struct IndexRange { pub index: String, pub eq: Vec<Value>, pub lower: Bound<Value>, pub upper: Bound<Value> }
pub struct ScanRequest { pub range: IndexRange, pub descending: bool, pub limit: u32, pub cursor: Option<Cursor> }
pub struct ScanPage { pub hits: Vec<Hit>, pub cursor: Option<Cursor> }
pub struct Hit { pub key: Key, pub stage: Stage, pub lease: Lease, pub values: Vec<Value> }
```

- **Q1.** The index MUST be declared.
  - `eq` binds a leading prefix of the index keys.
  - `lower` and `upper` bound the next key.
  - Values for built-in keys use the §2.0 types; a `Stage` value that names an undeclared stage is `Invalid`.
  - A record is in an index iff it has a value for every key: every field key with its declared type, and a group for a `Group` key. `Stage`, `Due`, `Rank` and `StageSince` always have a value. Membership holds in every stage and lease state.
- **Q2.** Hits come in encoded-index-key order (asc or desc), then insertion sequence asc.
  - `limit` is at most 1000.
  - The cursor is opaque, a keyset over `(index key, insertion seq)`.
  - Paging MUST NOT duplicate or skip records that are unchanged during paging.
- **Q3.** A page observes every write and gate change acknowledged before it was requested (V3).

### 2.7 `aggregate` (Query)
```rust
pub struct AggregateRequest { pub range: IndexRange, pub by: GroupBy, pub max_groups: u32 /* <= 10_000 */ }
pub enum GroupBy { Dims(Vec<Dim>), Ranges { attr: Attr, bounds: Vec<Value> } }
pub enum Dim { Attr(Attr), TimeBucket { attr: Attr, width_ms: u64 }, Lease }  // epoch-aligned buckets
pub struct AggregateResponse { pub groups: Vec<(Vec<Option<Value>>, u64)>, pub truncated: bool }
```

- **Q4.** Groups are ordered ascending by group tuple (§2.0 `Value` order, `None` first, stages by declaration position), and the first `max_groups` are returned.
  - `truncated` is set when more exist.
  - The `IndexRange` follows §2.6 Q1. Every `Ranges.bounds` value for `Attr::Stage` MUST name a declared stage, else `Invalid`.
  - Every `Dim::Attr`, every `TimeBucket` attribute and `Ranges.attr` MUST be a key of the range's index, otherwise `Invalid`. `TimeBucket` needs a `Time` key: a `Time` field, `Due` or `StageSince`.
  - `Dim::Lease` is the one dimension that is not an index key. It is read from the record, and its values are `Value::Str("free" | "leased")`. It replaces round 1's `GroupBy::State`: counting a range by stage and lease is `Dims([Attr(Stage), Lease])` over an index with a `Stage` key.
  - `Ranges` returns buckets `[b_i, b_{i+1})` plus a null bucket. Values outside `[b_0, b_last)`, and records without the attribute, fall in the null bucket. `Ranges` is used alone.
- **Q5.** The result observes every acknowledged write (V3) but MAY be torn under concurrent writes.

---

## 3. Queue definition and queue metadata

### 3.1 The definition

`QueueDef` has exactly two parts, and the engine interprets both:
- **`stages`:** an ordered set of 1..=32 stages, each flagged terminal or not, with at least one non-terminal.
  - Every record is in exactly one stage. An insert names it, and any write may change it (W5).
  - Every claim names one non-terminal stage (C1). Records in terminal stages are never claimable.
  - There is no transition graph. Any move between declared stages is allowed, including out of a terminal stage, so a redrive is one atomic write.
  - `stats` counts by stage and lease (S1), `aggregate` groups by stage, and indexes may key on it.
- **`indexes`:** typed indexes whose keys are record fields or the built-in attributes `Stage`, `Group`, `Due`, `Rank` and `StageSince`.
  - Built-in keys keep aggregates over large queues on the index. `Stage` is the main one. For example, an index `(Stage, StageSince)` counts or pages one stage's records by age, which is what a consumer's own retention job needs now that there is no expiry.
  - Lease state, `leased_until` and `attempts` cannot be index keys. They change on every claim and on reclaim, so indexing them would put index maintenance on the claim path and in the reclaim sweep. `stats` already counts by lease, and `Dim::Lease` reads it from the record (Q4).
  - Indexes are declared at queue creation. Adding one means a new queue (§10 O-6 covers the related add-stage question).

**Why the definition is immutable.** `define_queue` is create-or-verify, and a different definition is a `DefinitionConflict` (D3). Four things depend on that:
1. **Stranded records.** Removing or renaming a stage would leave records in a stage that no longer exists: no claim could name it, and no write could validate against it. Flipping a stage's terminal flag would leave Leased records in a terminal stage, or turn data into claimable work.
2. **Physical structures.** Implementations store the stage as its ordinal and build claim structures on it. S3×Turso prefixes the claim-order index with it (§7.2), and PostgreSQL copies the terminal flag onto the row so its claim index can be partial (§7.3). Index entries are materialized per record. Changing any of these means rewriting every record's rows while claims run.
3. **Writer validation.** W1 checks stage names and indexed field types against the definition at the envelope. A definition that could change between that check and the commit would let an acknowledged write become invalid.
4. **Log-replay interpretation.** S3×Turso rebuilds its projection by replaying the command log. The stage ordinals, terminal flags and index keys in old commands must mean what they meant when they were written, and a single immutable definition guarantees that.

Changing the stage set means a new queue. A future explicit migration op may append stages: appending keeps every ordinal, terminal flag and existing record valid. It is not part of this contract (§10 O-6).

### 3.2 Queue metadata

Queue metadata is mutable configuration that the consumer owns and the engine never interprets:
- a map of `String → Value` with a `u64` version, empty at version 0 when the queue is created (D1);
- changed only by a metadata op inside a write unit, atomically with the unit's record writes and gate ops (W13);
- compare-and-swap on the version (`MetaExpect::Version`), with a sparse patch;
- read fresh through `read{queue: true}` (R4); there is no separate op;
- bounded at 64 KiB encoded per queue, with entry names of 1..=128 bytes.

The bound keeps it configuration, such as Snorri's shard manifest (§8.1). It cannot become a key-value store, or side records under another name. No op's behavior depends on metadata, and a metadata op changes no record, no record version and no gate.

### 3.3 What left today's `QueueDefinition`

Everything in today's `QueueDefinition` is now a stage or index declaration, a contract constant (§4), or removed. Today's lifecycle states (Pending, Leased, Complete, Failed) are fixed by the engine; stages are declared by the consumer, and only the lease stays with the engine.

| Dropped from today's `QueueDefinition` (`crates/fireweed-core/src/domain.rs:925-963`) | Where it went |
|---|---|
| `tenant_id`, `queue_id` | Now the `QueueKey` argument. |
| `priority_model` (all kinds), `tie_breaker` | Replaced by `rank: i64` ascending plus insertion order, within a stage. |
| `ordering_mode`, `max_rank_error`, `progress_bound_ms` | Removed. Only Strict ordering is used (Snorri `lib.rs:16617-16620`). |
| `eligibility_policy.gate_keys`, `max_gate_keys_per_item`, `max_gates_per_request` | Gates are always on. `max_gates_per_request` (distinct item gate keys per push, `async_composed.rs:1726-1731`; Snorri uses 10,000, snorri `lib.rs:16624`) is removed. Per-record gates are bounded at 8, and `Unit.gates` ops at 1000 per request, a new and separate bound. B's proposed `gates: bool` flag is unnecessary: the claim already skips the anti-join when no gate is blocked (`crates/fireweed-turso/src/projection.rs:1997-2012`). Always-on gates cost one queue-prefix probe per claim (`projection.rs:1989-1996`), which `GateKeyPolicy::None` skips today; the P3/P4 gates measure it. |
| `eligibility_policy.metadata_blockers` | Replaced by a caller gate. |
| `cohort_policy` | Removed (owner, D-B). A cohort was a set of items pushed with a declared `cohort_size`, claimed only as a whole once complete, settled together, and expired if still incomplete after `completion_bound_ms` (`CohortPolicy`, `crates/fireweed-core/src/domain.rs:642-653`). |
| `recurrence` | Removed. Re-arm is `{Release(t), Put{due}}`. S3×Turso maps it to `Finalize{Retry}` or to a held `UpdateFields` plus `Finalize{Release}`, never `Finalize{Rearm}`, so `attempts` is never reset (§7.2). |
| `retry_policy.max_attempts` | Removed. `attempts` is exposed; a consumer moves an exhausted record (`attempts − attempts_base` over its limit, §5 `redrive`) to its own failure stage. |
| `request_id_retention_ms` | Constant R ≥ 24 h. |
| `client_item_key_retention_ms` | Removed. There are no tombstones: a key is free again as soon as its record is deleted, and a record guards its key for as long as the consumer keeps it. |
| `terminal_retention_ms` | Removed. There is no expiry (owner, D-A); records live until the consumer deletes them. |
| `max_lease_duration_ms` | Constant: 1 h. |
| `max_push_batch_size`, `max_claim_batch_size` | Constant: 1000. |
| `max_eligible_group_size` | Removed. W10 means inserts are never rejected, and C4/C7 bound a group by `max_records` and the response limit. The S3×Turso adapter sets it to `None` (§7.2). |
| `secondary_indexes`, `entity_schema` | Removed. |
| `typed_indexes` (with `unique`) | Now `indexes`, without `unique` and with built-in keys allowed. The key is the only uniqueness mechanism (D7). Snorri's production `by_run_target_key` moves into its item key (S11). |
| `emit_change_records` | Removed. S9 deletes `crates/fireweed-server/src/change_record_sink.rs` and its config plumbing: it serialized internal envelopes and had no consumer. |

---

## 4. Cross-cutting semantics

**Durability.**
- An `Ok` from `define_queue`, `write` or `claim` means every applied effect survives process loss.
  - S3×Turso: the object-log append is durable.
  - PostgreSQL: `COMMIT`.
- A projection apply is never part of the acknowledgement.

**Visibility (one model for every implementation).**
- **V1.** Write preconditions (`Expect`, lease token, lease state) are linearizable per record against the latest committed state. The metadata version (W13) is linearizable per queue.
- **V2.** A claim observes every write (stage moves included) and gate change acknowledged before it was issued. Omission is still allowed (C5), within C5a. The 7S scheduler's `findByJobId → deleteByIds → findByJobId` loop, now a claim loop on the `queued` stage, depends on it (§8.3).
- **V3.** `read` (records and queue metadata) and `scan` observe every write, gate change and metadata op acknowledged before the call was issued. `aggregate` and `stats` observe them too but MAY be torn under concurrent writes.
  - This costs neither backend anything. S3×Turso already waits for log coverage before every read: `side_record`, `side_records_by_prefix` and `explain_commit` (`crates/fireweed/src/turso_compose.rs:2035-2054`), and `range_scan`, `grouped_aggregate`, `metrics_by_query` and `declared_bucket_segment` (`:2310-2334`), all through `wait_request_entry_coverage` (`:3567-3579`). PostgreSQL reads committed state.
  - The consumer mappings depend on it: Snorri's scan-then-write run mutations, its input-ack lookup (`scan(by_consumed_event)`), its await reads by key and its manifest read (§8.1).
- **V4. The time-driven transition happens within Δ after its deadline.** There is exactly one:
  1. **Lease reclaim.** A record past `leased_until` becomes Free, with `pending_since = leased_until` (§1). Until then an implementation MAY still report and treat it as Leased. Token ops (`Extend`/`Release`) MUST be rejected as soon as `leased_until <= now`.
  2. *Retired (round 2): record expiry. There is no expiry (§1).*
  - Δ is measured on the injected `Clock`. Lease reclaim is scheduled on it with `Clock::sleep_until`, so advancing a manual clock wakes it. A claim that observes `Clock::now() − last_sweep ≥ Δ` MUST wake the sweep and MAY proceed without waiting for it (C5 already permits omitting records not yet reclaimed). This keeps a queue recoverable under a stepped logical clock with no ambient clock read (Snorri, `lib.rs:7936-7946`), and keeps reclaim work off the claim's latency.
- **V5. Request-id retention** is not a Δ transition: a request id MAY be forgotten after R.
- **Δ.**
  - Δ bounds only lease reclaim (V4). Reads are never lagged by Δ (V3).
  - One bound per implementation instance, fixed at construction and documented.
  - It MUST be at most 5 s. The S3×Turso default is 1 s, the same as the server reclaim tick (`crates/fireweed-server/src/env_config.rs:673`).
  - `fireweed-contract` defines `trait Clock { fn now(); fn sleep_until(t) }` (§2.0). Lease reclaim, including the tick, is scheduled on it, and Δ is measured on it.
  - The contract suite takes `(constructor, reopen, Δ, Clock, fault hook)`:
    - `reopen` restarts the implementation over the same durable state, to test W11, §2.1 D1 and C3 ("survives process loss");
    - the fault hook makes the next acknowledgement fail with `Unavailable` after commit, to test unknown-outcome resolution by resend and by `read`;
    - a manual `Clock` drives V4.
  - It runs every scenario twice: directly, and through a torn-read adversary that serves `aggregate` and `stats` from a mix of pre- and post-write state while writes are concurrent. `read` and `scan` get no lag wrapper.
  - There are no per-backend clauses and no backend knobs.

**Atomicity.**
- One `Unit`: all of its record writes, gate ops and its metadata op, within one queue.
- One claim: all of its leases.
- Nothing is atomic across queues.

**Request-id replay.**
- Scope is `(queue, request_id)`, shared across `write` and `claim`. `request_id` is 1..=128 bytes.
- `fireweed_contract::fingerprint(op kind, body)` is a pure, versioned canonical hash shared by every implementation.
- The request record is written in the same atomic step as the effects.
- R ≥ 24 h (V5).
- `write` and `claim` MAY carry an id (§9 D3). An id-less write resolves `Unavailable` by `read` of what it touched (D8). Snorri's transitions carry ids.

**Ordering.**
- Claims: as in C4, within the claim's stage.
- `scan` and `Where`: index key (descending if requested), then insertion sequence ascending.
- The insertion sequence is a per-queue total order on inserts. Within one request it follows unit order, then write order within the unit. If insert A is acknowledged before insert B is issued, `seq(A) < seq(B)`.
- It is assigned at insert and never changed by writes (stage moves included), release or reclaim. A re-insert after delete gets a new sequence.
- Within a stage, equal-rank records therefore claim in insertion order, not in the order they entered the stage. A consumer that wants stage-entry order sets `rank` on the move, for example to the move time in µs.

**Leases.**
- There is one token per claimed record, opaque and self-describing: the implementation can locate the row from the token alone.
- `Extend` keeps the token.
- Fencing is by token plus `leased_until`. A holder never needs the version.
- A Leased record is always in a non-terminal stage (C1, W1).
- Delivery is at-least-once with at most one live lease.

**Limits (contract constants).**

| Item | Bound |
|---|---|
| Record writes per request (summed over units) | 1000 (TD-012:51) |
| Gate ops per request (`Unit.gates`, summed over units) | 1000 |
| Metadata ops per request | 1 |
| Canonical encoded request | 16 MiB |
| Record (key, fields and payload, encoded) | 4 MiB; a write that would exceed it is a per-record `Invalid` (W8) |
| Encoded index key (ESF, all keys of one index) | 1024 B; `Invalid` (W1, W8). PostgreSQL btree entries fail above about 2704 B. With tenant, queue and index names bounded at 128 B each, an `index_entries (queue, idx, esf_key, seq)` entry stays under about 2.4 KB, so both backends accept the same writes. |
| Response (claim, read, scan) | 16 MiB. A short page is allowed, except for `read`, which returns `TooLarge`, and a claim always returns at least its first candidate (C7). |
| `claim.max_records`, `read` keys, `scan.limit` | 1000 |
| `aggregate.max_groups`, `stats.groups.limit` | 10,000 |
| Gates per record | 8 |
| Key | 1..=1024 B |
| Group and gate key | `^[A-Za-z0-9._:-]{1,256}$`; `"*"` is reserved |
| Stage name | `^[A-Za-z0-9._:-]{1,128}$` |
| Stages per queue | 1..=32, at least one non-terminal (§2.1 D4) |
| Tenant, queue and index name | 1..=128 B (`QueueKey`, `IndexDef.name`; D4) |
| Field name | 1..=128 B; one `IndexType` per name across indexes (§2.1 D4) |
| Indexes per queue | 32, with 1..=8 keys each; each built-in key at most once per index |
| Queue metadata | 64 KiB encoded per queue; entry names 1..=128 B (W13) |
| Lease and `Extend` duration | 1 ms..=1 h |
| R (replay retention) | ≥ 24 h |
| Δ | ≤ 5 s |

**Change-feed hook.**
- The only reserved invariant: `version` is strictly increasing per key, including across delete and re-insert.
- S3×Turso keeps its per-queue command sequence (`relational_cursor.next_seq`, `last_command_sequence`; `schema.rs:40,96-100`) internal.
- No position type, history or ordering promise is exposed.

---

## 5. Facets: claim versus write, Query and Operator

**Claim versus write, in plain English.**
- A **claim** walks the records of one stage and takes exclusive, crash-safe batches of them. You do not name the records: the claim finds the next ones in order and leases them. The state change is the cursor. A claimed record is Leased, so the next claim skips it. When you settle it, usually by moving it to another stage, it leaves this stage's walk for good. If you crash, the lease lapses and the records come back.
- A **write** addresses records by key. Use it without a lease for records you already know: an external event that names its record, an upsert, a correction, a stage move decided from outside. Use it with the lease token to settle what you claimed.
- Rule of thumb: if you are asking "what is next?", claim. If you already know the key, write. There is one `write` op for both uses.

**Query.** Mandatory for every implementation; it is part of `Store`.
- In plain English: when you define a queue you name typed indexes over record fields and built-in attributes, for example `(tenant, run, scheduled_at)` or `(Stage, StageSince)`. After that you can:
  - page through the records in a range of that index (`scan`);
  - count them by any index key (the stage included), time bucket, value range or lease state (`aggregate`);
  - claim in index order instead of rank order (`Select::Where`), still within one stage.
- Every query is a declared-index range, never a table scan.
- Why mandatory: Snorri uses `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `metrics_by_query` and `claim_by_query` on operational paths. A capability flag would be backend introspection.

**Operator.** A helper module, `fireweed::operator`, built only on the 7 ops. It is not an obligation on any backend.
- In plain English: it is what an on-call person does. Pause or resume a queue, look at a record, free a stuck lease, reschedule, move work to another stage (finish it, fail it, or send it back to be redone), and delete records. Each can be done one record at a time, or in bulk over an index range page by page, for example over `(Stage, StageSince)` to redrive or purge one stage's oldest records.
- Mapping:

| Helper | Kernel composition |
|---|---|
| `pause`, `resume`, `is_paused` | `write{units: [Unit{writes: [], gates: [("*", Blocked \| Open)], metadata: None}]}` (a gate-only unit is legal, W1); `stats.paused` |
| `inspect` | `read(Keys)` (no tokens) |
| `force_release(due)` | `{Exists, Clear, Put{due}}`. `Exists` keeps a deleted key from coming back with default values. |
| `reschedule` | `{Version(v), None, Put{due, rank}}`. `Leased` is surfaced, not overridden. |
| `move(stage)`, `force_finish(stage)` | `{Exists, Clear, Put{stage: Set(s)}}`. `force_finish` names a terminal stage. `Clear` is a no-op on a record whose lease was already reclaimed (W4), so the write gives the same result whichever way an implementation sees the record inside the V4.1 window. `Exists` keeps a deleted key from being re-inserted. |
| `redrive(stage)` | `{Version(v), None, Put{stage: Set(s), fields: {"attempts_base": Some(Int(a))}}}` from a terminal stage back to a non-terminal one, with v and a (the record's `attempts`) from `inspect` or a scan-then-read. One atomic write; the version keeps a concurrent change from being overwritten. `attempts` is never reset (§1), unlike today's `RetryCountMode::Reset` default (`crates/fireweed-engine/src/operator.rs:280-285`); a consumer that exhausts on attempts compares `attempts − attempts_base`. |
| `purge` | `{Any, Clear, Delete}` |

- `redrive` is restored in round 2 (§9 D11). The verification pass removed it because its Delete-then-insert could lose the record in a crash. Terminal records are now writable, so it is one atomic stage move.
- No archive, no async operation registry and no audit store: none has a consumer.

**Discovery: deleted.**
- It listed which groups have ready work so an external router could spread workers across groups.
- It is removed because only tests call it. Its one real use, Seventh Sense's `hasNextTasks` batching threshold, is served by the bounded `stats.groups` read (S2).

**Coordination and ownership: out of the contract.**
- `ownership`, `renew_owned`, epochs, `reassign` and `claim_across_queues` are S3×Turso deployment plumbing.
- `claim_across_queues` is deleted (owner decision). The rest move to `TursoAdmin`.
- A fenced owner surfaces as `Unavailable`.

**Mandatory for every implementation:**
- all 7 ops;
- the §4 constants and error classes;
- an injectable `fireweed_contract::Clock` on its constructor, on which lease reclaim is scheduled and Δ is measured;
- a documented Δ of at most 5 s;
- for the contract suite: a `reopen` over the same durable state and a post-commit `Unavailable` fault hook (test builds only);
- the pure `fireweed-contract` encoders (fingerprint, ESF), used verbatim.

Nothing else is mandatory.

---

## 6. Disposition of every current public method and free function

| Current | New home | Reason |
|---|---|---|
| `create_queue`, `ensure_queue` | `define_queue` | Create-or-verify is one idempotent op. The definition now declares the stages. |
| `queue_definition` | `read{queue: true}` (R4) | The queue read returns the definition and the metadata. `DefinitionConflict` still returns the stored definition. |
| `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id` | `write` (`Absent`, `Put{stage, …}`) | Duplicates become a per-unit `Exists`, not a whole-request `Conflict`. Every insert names its stage. |
| `upsert` | `write` (`{Any, None, Put{stage: Set(s), clear_fields: true, fields: <all>, payload: Set, due: Set, rank: Set, group: Set(g), gates: Replace}}`) | Replaces the item's attributes, as today's supersede does (`crates/fireweed/src/turso_parity.rs:138-189`); `created` reports Inserted vs Replaced. A Leased record gives `Leased`, as today's `Invalid("collision with claimed item")`. A record in a terminal stage is replaced and moved to `s`, where today it gives `Terminal`; a caller that must refuse that reads first and writes with `Version(v)` (§10 O-5). A group change is `Invalid` (W8). |
| `batch_update` | `write` (`Exists`/`Version`, `Put`) | Updates Free records in any stage; a Leased record gives `Leased` (D2). |
| `reschedule` | `write` (`Version`, `None`, `Put{due, rank}`) | A leased record gives `Leased` (D2, §10 O-2). |
| `mutate_items`, `bounded_mutation` | `scan`/`read` + `write` (`Version`, gate edits, stage moves, `Clear`) | Selection is caller logic; CAS guarantees safety. |
| `set_gates` | `write` (`Unit.gates`) | Gates flip atomically with record patches. |
| `purge` | `write` (`Delete`; `Clear` = force) | |
| `claim`, `claim_with`, `claim_response_with`, `claim_at`, `claim_response_at` | `claim` (`stage`; `Next`/`WholeGroups`, `due_by`) | One request type and one response type. `claim_with(metadata_equals)` filters that pick a record kind, such as Snorri's `record_kind`, become the claim's stage. The single-group claim (`ClaimCompatibility.group_key`, the previous draft's `Select::Group`) is deleted: no Snorri or 7S mapping uses it, and it would lease members of a group another holder claimed with `WholeGroups` (§9 D26). |
| `claim_by_query`, `claim_by_query_at` | `claim` (`Where`) | |
| `claim_by_item_ids` | `claim` (`Keys`, with skip reasons) | Keys are the identity (API-001:612-613 reasons kept, plus `OtherStage`). |
| `claim_across_queues` | Deleted | The owner called it unnecessary. |
| `ack`, `complete`, `fail` | `write` (`Release(t)`, `Put{stage: <terminal>}`) | Now token-validated. Complete and fail are moves to the consumer's terminal stages, for example `done` and `failed`. |
| `nack`, `release`, `retry`, `retry_after`, `nack_retry_after` | `write` (`Release(t)`, `Put{due}`) | `attempts` is never reset (§7.2 maps these to `Finalize{Release}` or `Finalize{Retry}`). |
| `rearm`, `rearm_at`, `rearm_after` | `write` (`Release(t)`, `Put{due: next}`) | Recurrence is caller logic. Never `Finalize{Rearm}`, which resets the attempt count. |
| `renew` | `write` (`Extend(t, d)`) | The token stays stable. |
| `reassign` | Deleted | Use `Clear`, then `claim Keys`. |
| `commit`, `commit_multi_claim` | `write` (multi-write units) | Side records and lifecycle items become item writes in the unit; the fence becomes group exclusivity (C6a, §8.1). |
| `explain_commit` | Deleted | Use request replay, `read(Keys)` of the keys the unit touched, or a consumer index such as Snorri's `by_tx_request` (D8, §8.2). |
| `commit_capabilities`, `hot_projection_capabilities` | Deleted | No introspection. |
| `side_record`, `side_records`, `live_item`, `live_items`, `claimed` | `read(Keys)` | |
| `side_records_by_prefix` | `scan` over a consumer-declared index | Snorri's instance listing scans `by_stage_key` with `eq=["instance"]` (§8.2). There is no `read(Prefix)`. |
| `retained_items` | Deleted | No consumer. |
| `peek` | Deleted | Use `scan`/`read`. |
| `metrics` | `stats` | Counts by stage and lease instead of four fixed states. |
| `metrics_by_query` | `aggregate(Dims[Attr(Stage), Lease])` | Over an index with a `Stage` key. |
| `range_scan` | `scan` | |
| `grouped_aggregate` | `aggregate(Dims)` | |
| `declared_bucket_segment` | `aggregate(Ranges)` | |
| `query_index`, `query_index_unique`, `query_index_typed`, `query_index_unique_typed` | `scan` (full `eq`) | |
| `discover`, `discover_active_scopes`, `discover_active_scopes_stamped`, free fn `select_active_scope_from_prefix` | Deleted | §5 |
| `reclaim_expired`, `reclaim_expired_at` | Deleted | The constructor-owned tick does this on the injected `Clock`; a claim that sees Δ elapsed wakes it and never reclaims inline (V4.1). |
| `ownership`, `renew_owned` | `TursoAdmin` | Deployment plumbing. |
| `projection_control`, `ProjectionControl::{capabilities, verify, delete, rebuild}` | `TursoAdmin::projection_control()` | AGENTS.md rule updated (slice S3). |
| `current_position` | Deleted | No consumer (Snorri does not call it). Deleting it keeps a log-position type off the public surface. |
| `snapshot_now`, `latest_snapshot_info` | `TursoAdmin` | Log and projection concepts. |
| `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `compact_log_behind_snapshot`, `last_rebuild_stats` | Deleted | Stubs: always `Unavailable` or `None` (`facade.rs:1379-1446`). |
| `operator_pause`, `operator_resume`, `operator_admin_state`, `operator_inspect_item`, `operator_repair`, `operator_purge`, `operator_redrive` | `fireweed::operator` helpers | §5. `operator_redrive` becomes the `redrive` stage move (round 2, D11). |
| `operator_archive`, `operator_get_operation`, `operator_audit_records` | Deleted | No consumer. |
| Cohort surface: the `CohortPolicy`, `CohortId` and `CohortOnIncomplete` re-exports (`crates/fireweed/src/lib.rs:80`), the `cohort_size` push attribute (`:3363-3364`), and `whole_cohort` claims with their `cohort_lease_token` (`:4133-4155`) | Deleted (owner, D-B) | See the `cohort_policy` row in §3.3 for what a cohort was. FR-47c is served by an atomic group insert, a `WholeGroups` claim and a caller count check (§8.3). |
| The RESP front: `crates/fireweed-resp` (about 5.8k lines), bound by `fireweed-server` (`crates/fireweed-server/src/lib.rs:3273-3283`) | Deleted (owner, D-D) | It bound engine ports directly and used consumer names as lease tokens (API-001:638-645). §10 O-4 covers the missing network front. |
| free fn `open`, `open_async` | `fireweed::s3turso::open(S3TursoConfig, Arc<dyn Clock>) -> (Fireweed, TursoAdmin)` | One blessed cell; it owns the tick. |
| free fn `open_product` | `fireweed-test-support` | MinIO fixture (`lib.rs:5981`). |
| free fn `open_objectlog` | Deleted | Retired cell. |
| free fns `open_objectlog_postgres`, `open_objectlog_postgres_async`, `open_postgres`, `open_postgres_async`, `open_postgres_runtime`, `open_postgres_runtime_async`, `open_postgres_coordinated` | Deleted | Port-level PostgreSQL backends are retired; `fireweed-pg` implements `Store`. |
| `pub mod turso_compose` | Private | Callers never name a backend. |

---

## 7. Execution mapping

### 7.1 Corrections to the fact sheet and the judges (verified)

| Claim | Code says |
|---|---|
| "Claim = Class S txn + claim outbox" | Claim is log-first and packed. "New serving writes no `fireweed_claim_outbox` rows" (`crates/fireweed/src/turso_compose.rs:3395`); the table is drain-only (`schema.rs:297-299`). **The production judge is right.** |
| "TD-016: 8 inflight, 20 ms linger" | Per generation: 8 requests, 8,192 items, 32 MiB. At most 4 generations and 16 requests per queue (`crates/fireweed-engine/src/claim_batch.rs:22-30`). The linger is 10 ms (`turso_compose.rs:276`). **Confirmed.** |
| "Keep terminal states + the existing reaper" (C, production judge) | S3×Turso never reaps terminal rows today. `reap_terminal_items_sql` (`apply.rs:2873`) has no caller. `ProjectionStore::reap_terminal_items` defaults to a no-op (`fireweed-engine/src/compose.rs:1316-1327`). The Turso `ReclaimDriver::tick` only reclaims leases and expires cohorts (`turso_compose.rs:2259-2275`). **The contract now has no expiry (round 2), so no reaper is needed.** |
| "Group size is validated at claim only today" (coverage judge) | It is enforced at push: API-001:262-268 and `validate_push` (`fireweed-engine/src/async_composed.rs:655-676,1737`). The resolution rule (never reject inserts) is still adopted; this is a behavior change. |
| "Snorri's commit gets ~12k/s" | Snorri's commit runs on the Parity lane (`turso_compose.rs:2553-2568`). The 12.1k/11.8k workflows/s figure is addressed `mutate_items` with no auxiliary records and no indexes (`docs/helix/04-build/workflow-capacity-review.md:8-14,95-104`). The addressed fast gate excludes typed indexes and gate changes (`turso_compose.rs:5055-5062`). |
| "`fields` is a postcard blob" (C) | `fields` is `TEXT` JSON (`schema.rs:29`): a map of byte arrays (`fireweed-relational/src/codec.rs:31-46`). The typed index source is the separate `index_fields` postcard BLOB (`schema.rs:32-34`). |
| "Mandatory claim `request_id` adds a new durable row" | A claim that carries an id already writes a durable receipt: the envelope's `request_outcome: RequestOutcome::BatchClaim{…}`, set only when `request_id` is present (`turso_compose.rs:4486-4500`), read back on replay by `replay_recorded_batch_claim` (`:5400-5420`), plus the in-process map (`:3096-3098`). Optional ids keep today's cost. |
| "The group summary has an `oldest_eligible_at` index" | Only in the retired PG port (`fireweed-postgres/src/relational.rs:695`). The Turso schema has none, and the summary lags time-only `not_before` crossings (`schema.rs:127-136`; `apply.rs:3718-3760` filters `not_before<=now`). |
| "The Turso group summary models intrinsic eligibility" (this draft, first pass) | The recompute is gate-filtered: the Turso `relect_group_summaries` always appends `GATE_ANTI_JOIN` (`fireweed-turso/src/projection.rs:607-626`), and the relational one appends it whenever a gate is blocked (`apply.rs:3727-3731`). Only pause is unmodeled (`schema.rs:134-135`). It is also stale by design: "Exact at mutation time except item Claim/Complete and uniform-priority BatchUpdate" (`apply.rs:3710-3712`). It cannot serve S2 (§7.2 `stats`, D13). |
| "The set-based `UpdateFields` path excludes CAS and gates" (this draft, first pass) | Two tiers exist. The operation-shaped API-001 fast path (`api001_update_shape`, `apply.rs:4458-4466`; `try_apply_operation_shaped_api001_batch`, `:4570-4590`) requires a uniform shape with no gates, CAS or field_ops, and P2/P3 ride it. The general set-based path (`update_fields_batch_is_set_based`, `:4436-4440`; `apply_update_fields_batch_sql`, `:4950-5330`) excludes only field_ops and entity documents; it already does CAS (skip on mismatch, `:5132-5137`), whole-set gate replace (`:5288-5322`), Leased rows when not api001 (`:5129`) and a batched read-back of `fields` (`:5070-5118`). |
| "Reads may lag acknowledged writes by Δ" (this draft, first pass) | Every S3×Turso read already waits for log coverage (`turso_compose.rs:2035-2054, 2310-2334`; `wait_request_entry_coverage`, `:3567-3579`). V3 now requires it. |
| "Snorri's `by_run_target_key` is only used by tests" (this draft, first pass) | It is declared `unique=true` in Snorri's production queue definition (snorri `lib.rs:16865-16874`), enforced on every push, and called load-bearing by Snorri's capability map (`docs/helix/02-design/fireweed-seam-capability-map.md:147`). |

### 7.2 S3×Turso per operation

Every change below is marked **[S]** (a simplification) or **[B]** (a new burden).

**Stage storage** (the base for every row below):
- `fireweed_items` gains `stage INTEGER NOT NULL`, the stage's ordinal in the immutable definition, and `stage_since INTEGER NOT NULL`, which replaces `terminal_at` (`crates/fireweed-relational/src/schema.rs:43`).
- The internal `lifecycle_state` keeps three of its values, derived from stage and lease: `Pending` is Free in a non-terminal stage, `Leased` is Leased, and `Complete` is Free in a terminal stage. `Failed` is no longer produced. This is the cheapest faithful mapping. A move into a terminal stage is today's Complete transition plus a stage write, so it rides the packed `Finalize` lane (settle row below), and terminal rows leave the pending indexes exactly as Complete rows do today.
- The claim-order index becomes `fireweed_items_pending_eligible_order_idx (tenant_id, queue_id, stage, priority_sort, created_seq, item_id, not_before, eligible_since) WHERE lifecycle_state = 'Pending' AND superseded = 0`. Today it is the same index without `stage` and with `cohort_size` (`schema.rs:82-85`). A claim binds its stage as the third key, so it walks only its own stage's rows, and a stage move rekeys the row as a priority change does.
- The group-head index `fireweed_items_pending_group_nonnull_idx` (`schema.rs:88-90`, today `(tenant_id, queue_id, group_key, priority_sort, created_seq, item_id)`) becomes `(tenant_id, queue_id, group_key, stage, priority_sort, created_seq, item_id) WHERE lifecycle_state='Pending' AND superseded=0 AND group_key IS NOT NULL`. Stage goes after `group_key`, not after `queue_id`: today's only user, the group-head reseek in summary maintenance (`apply.rs:3572-3580`), binds `group_key` with `INDEXED BY`, and a stage-first order would turn every reseek into a walk of the queue's whole prefix. A `WholeGroups` claim binds `(group_key, stage)` and walks in claim order; a stage-blind reseek walks only that group's Pending rows, and it is off the Turso claim path (live apply of a leased Claim skips it, `apply.rs:3453-3456`). `fireweed_group_summary` stays keyed by group (PK `(tenant_id, queue_id, group_key)`, `schema.rs:120-133`) as a prefilter; it counts Pending members in every stage, so it is a superset of the groups that have candidates in the claimed stage.
- Stats counters: today these are four `resident_*` columns on the `queues` row (`crates/fireweed-turso/src/metrics.rs:11-12`), maintained in the apply transaction by `MetricsDelta` from before/after counts (`:257-277`). They become rows of a new `fireweed_stage_counts (tenant, queue, stage, free, leased)` table, and `counts()` groups by `(stage, lifecycle_state)`. Today's `resident_complete` becomes the `free` count of a terminal stage. Three stage-blind mechanisms change with it (S4.1, S6, S7):
  - `MetricsDelta::capture` skips `UpdateFields` and `UpdateFieldsBatch` (`metrics.rs:225-233`), so lease-free stage moves would never reach the counters. It scopes only the entries that set a stage; every other update keeps `continue`, so P2/P3 add no count reads.
  - The fresh after-plans assume every push lands Pending (`FreshPush([n,0,0,0])`, `metrics.rs:132-137`) and every claim goes Pending → Leased. They derive per-stage deltas from the stage on each pushed item (an insert may land in a terminal stage) and on each claim, which is why `ClaimCommand` gains `stage` (S4.1).
  - The `stats` read folds the unapplied log tail in memory (`turso_compose.rs:3750-3858`; `fold_lifecycle_metrics` and `fold_membership_metrics`, `:5835-5990`) from retained changes that carry no stage (`RetainedLifecycleChange::Claim(Vec<ItemId>)` and `RetainedMembershipChange::Push(Vec<(ItemId, ClientItemKey)>)`, `crates/fireweed-objectlog/src/async_projection_apply.rs:25-28, 62-65`). `Push` carries each item's (stage, terminal) and `Claim` carries the stage, and the three folds become per-stage. A lease-free stage move in the tail is either retained with its old and new stage or makes the read fall back to today's coverage wait (`turso_compose.rs:3852-3857`); it never folds as a no-op. `stats.groups` reads its structure only after a coverage wait.

**Routing** is a pure function of request shape and applies to the whole request:
- **Route P, the packed Update generation.** Taken when every unit has exactly one write, no gate ops and no metadata op, and every write is `{Put, None}` or `{Put, Release(t)}`. A finish (`{Put{stage: <terminal>}, Release(t)}`) and a lease-free stage move are both route P.
  - For `{Any, None, Put}` the shape does not say insert or update: that depends on the snapshot, and one request (a 7S callbacks batch, for example) mixes new and existing keys. Today each generation work item is one kind (`Push`, `BatchUpdate` or `Finalize`; `validate`, `async_composed.rs:575-600`). **[B]** A new `Write` generation work kind resolves each write against the snapshot (absent key → `Push` item, existing key → `UpdateFields`, settle → `Finalize` plus a held `UpdateFields`) and emits all envelopes in one append with per-unit outcomes. The push key snapshot selects only `client_item_key` (`crates/fireweed-turso/src/local.rs:3697-3719`); it also loads `item_id`, `item_version`, `lifecycle_state` and `stage`, which `Exists { version }` needs, and the overlay keeps key → (item_id, version) for keys inserted in unapplied generations (S4, S6).
- **Route X, the existing exclusive commit/parity path.** Everything else: multi-write units, gate ops, metadata ops, `Extend`, `Clear` and `Delete`.
- An envelope-kind parity test asserts that pure-insert, pure-update and pure-finish requests emit exactly today's `Push`, `UpdateFieldsBatch` and `Finalize` kinds (plus the request outcome when an id is present).

| Op / shape | Existing fast path | Internal change |
|---|---|---|
| `write` insert (P) | TD-016 `Push` member; the snapshot loads the request's keys (`turso_compose.rs:4221-4260`) | [B] Per-record `Exists` outcomes instead of a whole-request `Conflict` (`async_composed.rs:647-648`). [S] Delete the push-time group-size rejection (`:655-676,1737`) and the ungrouped-item rejection (`:1718-1722`; see the queue mapping below). [B] A `RequestOutcome::Write` row only on pushes that carry an id. Id-less pushes, as P1 runs today (`crates/fireweed/tests/ss_phased_capacity.rs:152-164`), keep today's cost. Use a compact outcome encoding (risk 1). [S] `created_seq = base_seq + offset` in request order (`apply.rs:2212-2223`) already gives the §4 insertion order when units are flattened in unit order, then write order. [B] Each pushed item carries its stage ordinal and `stage_since = now`. An insert into a terminal stage is written as a `Complete` row, so it never enters the pending indexes. |
| `write` update (P) | `UpdateFieldsBatch`. Two tiers exist. The operation-shaped API-001 fast path (`apply.rs:4458-4466,4570-4590`) requires a uniform shape with no gates, CAS or field_ops; P2/P3 ride it. The general set-based path (`apply.rs:4436-4440,4950-5330`) excludes only field_ops and entity documents. It already supports CAS (skip on mismatch, `:5132-5137`), whole-set gate replace (`:5288-5322`), Leased rows when not api001 (`:5129`) and a batched read-back of `fields` (`:5070-5118`). | [B] Field patches are a NULL-safe merge on either tier: `fields = CASE WHEN v.patch IS NULL THEN fields ELSE json_patch(fields, v.patch) END`, or rows without a field patch bind `'{}'`. Vendored `json_patch` returns SQL NULL when either argument is NULL (`vendor/turso_core/json/ops.rs:23-25`) and `fields` is `TEXT NOT NULL` (`schema.rs:29`), so a bare `json_patch` would poison a mixed pack. Merge-patch `null` removes a key (vendored `json_patch`/`json_remove`, `vendor/turso_core/function.rs:238-240,1708-1712`). Values are encoded as JSON arrays (`["i",5]`, `["s","x"]`), never objects, so the merge never recurses. [B] **Tier routing within P.** A lease-free `Expect::Any\|Exists` write whose patch sets any of `due`, `rank`, `payload`, whole `fields` (`clear_fields: true`) or a sparse field patch, and touches no indexed field (`clear_fields: true` touches every indexed field), no gate and no stage, stays on the operation-shaped tier. That tier already replaces `payload`, `metadata` and whole `fields` (`api001_update_shape`, `apply.rs:4458-4480`; the whole replace, `:4636-4652`); a sparse patch there is the NULL-safe `json_patch` above, in SQL, and needs no read-back because no index key changes. It still needs one uniform shape per batch. Today's P2 replaces `payload` and `metadata` (`crates/fireweed/tests/ss_phased_capacity.rs:193-205`), and D9 folds `metadata` into `fields`, so the ported P2 is `{Exists, None, Put{payload: Set(Some(profile)), fields: {"phase": Some(Str("needs_schedule"))}}}` on this tier, and gate 1 runs on that shape. Writes that touch an indexed field, a gate or a stage, or carry `Expect::Version`, go to the general tier, which costs them the fast path; P2p/P3p measure it. [B] **Terminal-stage rows are writable on every tier.** Every `UpdateFields` apply path filters on a live lifecycle and silently applies nothing to a row that does not match: the single arm (`apply.rs:5567-5737`; `lifecycle_state IN ('Pending','Leased')` at `:5577` and `:5651`), the general tier's read-back (`:5080`, `:5087`, which then skips the missing row) and the operation-shaped tier (`lifecycle_state='Pending'` at `:4694`, `:4755`, `:4816`, `:4851`); in memory, `apply_update_fields` debug-asserts a non-terminal item (`fireweed-projection/src/lib.rs:2316-2319`). Live and rebuilt projections skip the same rows, so a live == rebuild proof would not catch the acknowledged-but-unapplied write. Every path accepts `Complete` rows: the single arm and the general tier take `('Pending','Leased','Complete')`, the operation-shaped tier `('Pending','Complete')` (never Leased, D2). A patch on a terminal-stage row bumps `item_version`, rekeys and keeps `Complete`, and no write whose target may be `Complete` is routed to a tier whose predicate excludes it. The planner accepts rows in terminal stages; today `plan_batch_update` rejects them with `Terminal` (`crates/fireweed-engine/src/compose.rs:2009-2012`). [B] **Stage moves:** `UpdateFields` gains `set_stage`, applied on the general tier. The apply sets `stage`, sets `stage_since` and `eligible_since` to now when the stage changes (§1), and derives `lifecycle_state` from the target stage's terminal flag and the lease: a Free row becomes `Pending` (non-terminal) or `Complete` (terminal); a Leased row, i.e. a held `UpdateFields` from `{Extend(t,d), Put{stage: Set(b)}}` whose target W1 makes non-terminal, stays `Leased` with its token and `lease_expires_at` unchanged, so it never enters stage b's claim-order index. `Pending` to `Complete` is a lease-free finish. A terminal-to-terminal move keeps `Complete` and sets `stage` and `stage_since`. `Complete` to `Pending` is a redrive, which has no apply arm today. [B] **W8 before the acknowledgement.** The group, gate-count, record-size and index-key checks need stored values, and the planner snapshot has none of them: `BatchUpdateSnapshotItem` holds `{ item_id, client_item_key, state, item_version, fenced, superseded }` (`compose.rs:1500-1507`), and `validate_batch_update` plans against it (`async_composed.rs:692-712`). The snapshot adds `stage`, `group_key` and `gate_keys`, plus a new `record_bytes` column (encoded key + fields + payload length) that apply maintains. Only for a write whose patch touches an indexed key does it also load the stored values of that index's other keys (or `length(index_key)` of the affected `fireweed_item_index` rows). P2p/P3p measure this read. [B] `GateEdit::Edit` needs set-based add/remove (`DELETE … IN`, `INSERT … ON CONFLICT DO NOTHING`) beside today's whole-set replace. [B] The general tier accepts Leased rows, so the planner rejects `Leased` for lease-`None` writes (see the acked-update paragraph below). [B] Every kernel `UpdateFields` emission uses the no-reset semantics of the batch path: `eligible_since` changes only per §1 (a stage change sets it to now). Today the non-api001 path resets it to `max(not_before, now)` (`apply.rs:5169-5172`; in memory, `fireweed-projection/src/lib.rs:2437-2442`), so the result would otherwise depend on the tier. [B] Index rekey: for rows whose patch touched an index key (a field or a built-in), the existing batched read-back of `fields`, ESF keys computed in Rust, then batched `DELETE`/`INSERT` on `fireweed_item_index`. Today rekey runs only on entity replacement and per row (`apply.rs:5704-5735`). |
| `write` settle `{Release(t), Put}` (P) | Packed `Finalize` handles Complete/Fail only. Retry/Release/Rearm use the singleton path with a coverage wait (`turso_compose.rs:4773-4825`; `async_composed.rs:769-790`). `Finalize{Release}` ignores `not_before` (`apply.rs:5814,5834`). Only Retry (backoff, `:5836-5840`) and Rearm carry a due time, and Rearm resets `retry_count` (`:5851,5876-5882`; mandated by API-001:258). | [B] Mapping, never `Finalize{Rearm}`, because `attempts` is never reset (§1): `{Release(t), Put{}}` → `Finalize{Release}`. `{Release(t), Put{due}}` with no other attribute → `Finalize{Retry, not_before}`, which preserves `retry_count` and, with `max_attempts = u32::MAX`, never exhausts. `{Release(t), Put{stage: Set(s)}}` with no other attribute → `Finalize{Complete}` when s is terminal and `Finalize{Release}` when it is not, carrying s. **This is how a finish rides packed `Finalize`:** internally it is today's Complete (`apply.rs:5802-5803`). `FinalizeOutcome` gains a `stage` field, and `FINALIZE_SET` (`apply.rs:5849-5853`) also sets `stage` and `stage_since`, bucketed by target stage as it is bucketed by state today. [B] Finalize rekeys `Stage`-, `StageSince`- and `Due`-keyed indexes (a finish changes the first two; `Finalize{Retry, not_before}` changes `Due`). Today the Finalize arm reads only `retry_count, max_attempts` and never touches `fireweed_item_index` (`apply.rs:5763-5935`). For an index whose leading keys are the changed built-ins, the fixed-width prefix is replaced in SQL (`index_key = ?prefix \|\| substr(index_key, ?w+1)`) with no read; any other position reads `fields` back in one batched SELECT per Finalize batch (risk 10). `Finalize{Fail}` is never emitted: a failure stage is just another terminal stage. Any other `{Release(t), Put{…}}` → a held `UpdateFieldsBatch` on the general tier with the rest of the patch, followed by the `Finalize` above. The general tier already accepts Leased rows when not api001, so there is no new apply mode; only planner and overlay support is new. Both commands land in one packed vector and one writer transaction. A contract-suite scenario asserts that `attempts` is unchanged across a release and re-schedule. [S] These join the packed member. The singleton path exists to read attempt counts and lease versions, and because an in-flight claim may still be Pending in Turso (`turso_compose.rs:4814-4816`). Kernel queues never exhaust retries, and the [B] overlay-token check below covers in-flight claims. [B] Token validation, which packed finalize skips today (`async_composed.rs:769-827`): check the overlay's remembered in-flight tokens, plus one batched `lease_token_hash, lease_expires_at` read by item id for applied claims, plus `leased_until > now`. The token encodes `(batch token, item_id)`, so no key lookup is needed. [B] `eligible_since` follows §1's `pending_since` rules: `Finalize{Release}`, a stage change and `Clear` of a Leased record set it to now, and `Finalize{Retry}` sets it to `max(not_before, now)`. Today Release never updates it (`FINALIZE_SET`, `apply.rs:5849-5853`) and Retry sets it to `not_before` (`:5901-5909`). |
| `write` multi-write unit, gate ops, `Extend`, `Clear`, `Delete` (X) | `commit_transition` parity path (`turso_compose.rs:2553-2568`; `turso_parity.rs:545-600`) | [B] Generalize `prepare_commit_transition` (`commit_surface.rs`) from a claim plus fence to N keyed preconditions, emitting `Push` / `UpdateFields` (held or plain) / `Finalize` / `RenewLease` / `PurgeItems` / `SetGates` (`"*"` maps to `PauseQueue`/`ResumeQueue`) / `SetQueueMetadata` (next row) in one atomic append. [B] `Clear` on a Leased record emits a `Finalize{Release}` without a token check, or `Finalize{Complete}` carrying the stage when the patch names a terminal stage, followed by `UpdateFields` for the rest of the patch; `Clear` on a Free record emits nothing. It never emits `MutateItems{invalidate}`, a full-row replacement (`ResolvedItemValues` carries payload, fields, metadata, gates and index fields, `crates/fireweed-engine/src/command.rs:259-276`) that would read the whole stored row, payload included, and write it to the log. [S] Stop emitting `WriteSideRecords`/`AdvanceInstanceFence`, and drop `fireweed_side_records`/`fireweed_instance_fences` (`schema.rs:240-251`). Snorri stays on this path. Its transition unit writes the instance item, which lives in a terminal stage, with no extra claim (§8.1, O-1). |
| `write` with a metadata op (X) | — | [B] A new `SetQueueMetadata { expected_version, patch }` command, planned under the route-X exclusive fence against `queues.metadata_version` and `metadata`, read after the coverage wait (`turso_parity.rs:563-581` takes `selection_fence.acquire_exclusive`, then `wait_until_covers`, before the operation runs), so no generation overlay is involved. It is applied to new `metadata BLOB` and `metadata_version INTEGER` columns on the `queues` row (`schema.rs:9-14`), in the same writer transaction as the unit's other commands; the apply does not re-check the version. Metadata ops are rare and at most 64 KiB, so route X costs nothing that matters. |
| `claim Next` | Packed item claim | [S] Token = `(batch token, item_id)`; Version = `(item_id, item_version)`. [B] The claim binds its stage in the stage-prefixed claim-order index (above). [B] **Per-member stage and `due_by` in the packed realize.** Today one SELECT serves every claim member of a generation: `claim_members` holds `(eligibility_at, max_items, token, expiry)` and no stage (`turso_compose.rs:4416-4450`), the microbatch uses the latest `now` across members and splits rows by position (`fireweed-turso/src/local.rs:803-833`), and any filtered claim is sent to the exclusive path because "packed realize does not yet honor that filter" (`turso_compose.rs:4575-4581`). Every kernel claim names a stage and a `due_by`, so as-is members would receive each other's rows and rows due after their own `due_by` (C1), or every claim would take the exclusive path (P4). `claim_members` carries the claim's stage and `due_by`; the microbatch partitions a generation's members by (stage, `due_by`), each partition runs one ordered scan on the stage-prefixed claim-order index binding `stage=?` and its own `due_by`, and the generation's exclude set is shared across partitions. A kernel claim never takes the exclusive path because of its stage (S6, S7). [B] Always-on gates cost one queue-prefix probe per claim (`fireweed-turso/src/projection.rs:1989-1996`), which `GateKeyPolicy::None` skips today; P3/P4 measure it. [B] A claim that sees Δ elapsed signals the reclaim tick and never reclaims inline (V4.1, Lease reclaim row). [B] The acked-update exclusion (below). |
| `claim WholeGroups` | Exclusive grouped path | [B] Candidates are filtered by stage: `select_group_batching` adds `e.stage=?` and `i.stage=?` (`fireweed-turso/src/projection.rs:800-884`) and walks the group-head index by `(group_key, stage)` (above). `fireweed_group_summary` stays keyed by group as a prefilter, so no summary path is rekeyed on a stage move; the claim already derives each group's first candidate from the item rows, not from the summary's `rep_*` columns. [S] `select_same_group` (`projection.rs:886-932`) goes with `Select::Group` (§9 D26). [S] The "no Leased member" test (`NOT EXISTS … lifecycle_state='Leased'`, `crates/fireweed-turso/src/projection.rs:829-831`) is unchanged, because group exclusivity spans stages (C6a, §9 D18). S3×Turso's grouped path runs under the exclusive selection fence (`turso_compose.rs:4618`); S2's C6a scenario, which S7 must pass, covers a concurrent packed `Next` claim that leases a group member in another stage. [B] Return the first group truncated to its longest prefix within `max_records` and the response byte limit, with `partial_group` (C4), because pushes no longer cap group size. [S] Delete the `max_eligible_group_size` requirement and the `BatchTooLarge` check (`crates/fireweed-engine/src/claim_validation.rs:77-84`); C4 truncates through `partial_group` instead. [S] Drop the cohort branches, including the `cohort_size IS NULL` predicates in `select_group_batching` (`projection.rs:800-831`). |
| `claim Keys` | `claim_by_item_ids` parity | [B] Key-to-id resolution through `fireweed_items_active_key`; drop the `not_before` check for this selector; skip reasons with the C2 precedence `NotFound`, `Terminal`, `OtherStage`, `Leased`, `Blocked`, plus `NotAttempted` at the byte limit; `Invalid` when `max_records` is below the key count. [B] Extend the `RequestOutcome::BatchClaim` receipt (today `item_ids`, `lease_token`, `worker_id`) with `skipped` and `partial_group`, so C8 replays them as recorded. |
| `claim Where` | `claim_by_query` parity | [B] Add the stage predicate on the joined item row. An index whose first key is `Stage` serves it without a filter, so a consumer that claims by index on a mixed-stage queue declares one. Hits map to keys. The insertion-sequence tie-break is ascending in both directions. |
| `read` | `committed_live_items` after the coverage wait | [S] Unchanged: V3 keeps the coverage wait. There is no prefix read. [B] `read{queue: true}` returns the `queues` row's definition and metadata columns after the same coverage wait. |
| `stats` | Maintained counters (`fireweed-turso/src/projection.rs:1690-1698`; `metrics.rs:11-12, 257-277`) | [B] Counts by stage and lease from `fireweed_stage_counts` (above). [B] Groups: a separate, un-gated structure keyed by `(stage, group)`, maintained on every change to the Free membership of a non-terminal stage (insert, Claim, Finalize, LeaseExpired, Release, stage moves, `not_before`/rank updates, Purge). For example, a per-(stage, group) `earliest_ready_at` column updated outside the gated recompute, plus the index `(tenant, queue, stage, earliest_ready_at)`; `ready_groups = SELECT count(*) FROM (… WHERE stage = ? AND earliest_ready_at <= ? LIMIT n)`. Maintenance is one set-based recompute per apply batch over the distinct (stage, group) pairs the batch touched, computing `MIN(max(not_before, eligible_since))` over the pair's Pending rows through the group-head index, never one statement per item. `earliest_ready_at` is a minimum, so a claim of a group's earliest ready record needs that reseek, not a decrement. This brings back the per-group apply work that today's summary deliberately skips after Claim/Complete (`apply.rs:3710-3712`; `apply_one_claim_sql`, `apply.rs:3845-3941`, does no group work), on grouped items such as the phased-capacity ones (risk 13). The existing `fireweed_group_summary` recompute cannot serve S2: it anti-joins blocked item gates and is deliberately stale after Claim/Complete (`apply.rs:3703-3712`; `fireweed-turso/src/projection.rs:607-626`), so a group whose ready members were all claimed would keep stale values without bound, and 7S's `hasNextTasks` would fire permanently. |
| Versions | `item_version` +1 in every apply arm | [B] `LeaseExpired` stops bumping `item_version` (`apply.rs:6073-6084`, plus the in-memory arm). This is safe: `commit_validate` fences on state, token and expiry first (`fireweed-projection/src/lib.rs:4484-4510`). |
| Lease reclaim | `ReclaimDriver::tick` exists but embedded use is host-driven (`crates/fireweed/src/lib.rs:5065-5090`); the server runs it every 1 s (`fireweed-server/src/lib.rs:3435-3460`) | [B] The constructor owns a tick task at interval Δ, scheduled with `Clock::sleep_until`, so advancing a manual `Clock` wakes it; this keeps Snorri's quiet queues recoverable under its stepped lease clock. A claim that sees `Clock::now − last_sweep ≥ Δ` only signals the tick task and never reclaims inline (V4.1): on S3×Turso a sweep is a coverage wait, a planning read and a durable append (`turso_compose.rs:2244-2254`; `fireweed-engine/src/async_composed.rs:1211-1250`), and the source-audit assertion that "item Claim must not wait apply before return" (`turso_compose.rs:1089-1092`) stays. The tick's retry sleeps use wall time today (`Instant::now()` and `tokio::time::sleep`, `crates/fireweed-objectlog/src/reclaim_tick.rs:482-489`) and move onto the `Clock` (S7). Reclaim sets `eligible_since = leased_until` (§1). There is no expiry sweep (round 2). |
| `scan`, `aggregate` | `fireweed-turso/src/query.rs` generators, after the coverage wait (`turso_compose.rs:2310-2334`) | [S] Hits carry `key`, `stage` and `lease`; `bounded_mutation` and `query_index*` are deleted. [B] Index keys derive from `fields` and, for built-in keys, from row columns: the `stage` ordinal, `group_key`, `not_before` (the due time), `priority_sort` and `stage_since`. A write or settle that changes one rekeys the index rows that name it, so a finish rekeys every `Stage`- or `StageSince`-keyed index and a `Finalize{Retry}` every `Due`-keyed one (settle row, risk 10). Claims change no indexable attribute, so the claim path never touches `fireweed_item_index`. The `index_fields` blob is retired. [B] `Dim::Lease` reads `lifecycle_state` from the joined item row. [B] Reject encoded index keys over 1024 B (W1/W8); nothing checks today. |

**Queue definition mapping (adapter only):**
- the stages from the definition: the ordinal is the declaration position, and the terminal flags drive the `Pending`/`Complete` mapping above. Today's `QueueDefinition` has no stages (`crates/fireweed-core/src/domain.rs:925-963`), and its `QueueIndex` wraps axon's field-only `IndexDeclaration` (`domain.rs:838-844`), which cannot express the `Stage`, `Group`, `Due`, `Rank` or `StageSince` keys. S4.1 adds `stages: Vec<StageDef { name, terminal }>` and a built-in-key form of index declaration to it; both ride `CreateQueueCommand { definition }` (`fireweed-engine/src/command.rs:211-213`), so replay rebuilds the ordinals, terminal flags and built-in keys (§3.1 point 4);
- Int64-ascending priority over `rank`, Strict ordering. Every kernel insert carries an explicit `not_before` (the due time) and `priority` (the rank), so the FIFO rowid claim scan, which needs both to be absent (`is_fifo_claim_scan_item`, `fireweed-relational/src/query.rs:21-26`) and walks `rowid` from one stage-blind floor per shard (`fireweed-turso/src/local.rs:751-770`; `NOT INDEXED … ORDER BY rowid`, `projection.rs:2020-2045`), never activates for kernel queues; S9 deletes it. The phased-capacity pushes already set both (`ss_phased_capacity.rs:151-158`), so gate 1 does not ride it today;
- gates `Dynamic`/8, `max_gates_per_request = None` (§4 bounds `Unit.gates` instead);
- no cohort;
- the default `recurrence`: the adapter never emits `Finalize{Rearm}` (settle row above);
- `max_attempts = u32::MAX`;
- `terminal_retention_ms`: any value. It is inert, since S3×Turso never reaps terminal rows (§7.1), and S9 removes the setting;
- `client_item_key_retention_ms = 0`;
- `max_eligible_group_size = None`. With `Some`, `validate_push_shape` rejects every ungrouped item (`crates/fireweed-engine/src/async_composed.rs:1718-1722`, pinned by the unit test at `:4684-4721`), and `validate_claim_compatibility` requires the setting and returns `BatchTooLarge` for any group-batching claim whose `max_items` is below it (`claim_validation.rs:77-84`). [S] S4 deletes both, next to the push-time group-size rejection (`:655-676,1737`);
- `typed_indexes` from `indexes`; built-in keys are new ([B], `scan` row).

**The acked-update-vs-claim skip** (`docs/helix/04-build/one-projection-cleanup.md:139-144`) is incompatible with W2/W11, because `Applied` must mean applied. Sharing the per-queue sequencer does not make it unreachable today, so closing it is **[B] work in S4/S7**, not an assertion:
- Within a generation, all works are validated first (`validate_inert_mutation_generation_folding`, `turso_compose.rs:4302-4303`) and claims are realized afterwards (`realize_accepted_claims`, `:4310`). The claim exclude set is only `leased_ids ∪ terminal_ids` (`claim_exclude_ids`, `:281-291`), so an update accepted as Pending in the same generation can still be claimed, including one that moves the record out of the claimed stage, and if the claim envelope precedes it, the update's apply skips.
- `validate_batch_update` plans against `batch_items` and never consults the overlay's `leased_ids` (`async_composed.rs:692-745`), so an update can be accepted for an item claimed in an earlier, unapplied generation.
- Item claims never wait for apply (source-audit assertion, `turso_compose.rs:1089-1092`), so a claim can lease a row under its pre-update stage, `due` or gates, which violates V2/C5 even when no skip occurs.

Required changes: `claim_exclude_ids` includes ids with accepted-but-unapplied updates (current and in-flight generations); `validate_batch_update` rejects ids in the overlay's `leased_ids` with `Leased`; gate ops acknowledged through route X are visible to later packed claims. Proof: a planner test that interleaves `Put{due: future}`, `Put{stage: other}` and `Claim` in one generation and across two unapplied generations. The cost is inside the P2/P4 gates (risk 8). The exclusion MUST clear within Δ of acknowledgement (apply lag ≤ Δ); otherwise the claim waits for coverage of the excluded ids, so C5a holds. The apply-time skip then stays as an asserted invariant with a counter.

**Perf gates.** Every S3×Turso slice must pass:
1. `ss_phased_capacity` at ≥ evidence `1788659385` minus 5%, with an exact final residual. P1 pushes carry no id; P3/P4 include the always-on gate probe and P2/P4 the acked-update exclusion. P4's completion is a finish into a terminal stage (`Finalize{Complete}` with a stage), and every phase maintains the per-stage counters. P1, P3 and P4 also maintain the `stats.groups` structure, since the phased-capacity items are grouped (risk 13). P2 runs the ported shape `{Exists, None, Put{payload, fields: {phase}}}` on the operation-shaped tier (§7.2 `write` update row):
   - P1 ≥ 13,768/s
   - P2 ≥ 17,901/s
   - P3 ≥ 12,699/s
   - P4 ≥ 16,232 items/s
2. New P2p, P3p and P4p runs on a Snorri-shaped queue with 22 typed indexes (§8.2), two of them keyed on `Stage`. They include the planner snapshot's W8 read (stored group, gates, `record_bytes` and co-key values, §7.2 `write` update row). The patch touches 1 indexed field and 5 unindexed fields, sized with Snorri's measured transition bytes; Snorri's large blobs (`next_state`, `audit`) ride `payload`, not `fields` (D9, §8.1), and the P4p payload uses their measured size. P2p mixes in lease-free stage moves, including moves out of a terminal stage. P4p is claim plus a finish (`{Release(t), Put{stage: <terminal>, …}}`) with a patch, which rekeys the `Stage`-keyed indexes; P4r is claim plus `Release{due}` with a patch. Each must be ≥ today's `batch_update`/`mutate_items` baseline on the same queue minus 5%. The baseline is measured before S4 lands.
3. The 32-shard workflow qualification: worst shard ≥ 9,500/s and total ≥ 11,220/s.
4. A Snorri-shaped transition bench ≥ today's `commit` on Parity minus 5%. It includes the `WholeGroups` claim on `transition`, the hydrating instance read, the instance item's `Version` write, the `by_tx_request` index row, and Snorri's measured audit/state bytes in the settle patch and payload.
5. *Retired (round 2).* P5 and P5s measured the expiry sweep, and there is no expiry.

### 7.3 Direct PostgreSQL (`fireweed-pg`, depends only on `fireweed-contract`)

| Op | Execution |
|---|---|
| Schema | `records(queue, key COLLATE "C", seq bigint, ver int, stage smallint, terminal bool, stage_since, due, rank, grp, fields jsonb, payload bytea, attempts, lease_token_hash, leased_until, pending_since, PK(queue,key))`. `stage` is the ordinal, and `terminal` is copied from the immutable definition (§3.1), which lets the claim index `(queue, stage, rank, seq) WHERE NOT terminal` be partial. Also `record_gates`, `blocked_gates`, `index_entries(queue, idx, esf_key bytea, seq, key)` (`esf_key` ≤ 1024 B fits a btree entry), `group_summary(queue, stage, grp, earliest_ready_at)` with an index on `(queue, stage, earliest_ready_at)`, `stage_counts(queue, stage, free_delta, leased_delta)`, `requests(queue, request_id, fingerprint, response, retain_until)` and `queues(def, metadata bytea, metadata_version bigint)`. `Version = (seq, ver)`: `seq` is the incarnation and the tie-break. A record is Leased iff `leased_until > $now`. |
| Insertion sequence | `seq` comes from a per-queue sequence: reserve a block of n values and assign `seq = base + ordinality` over the request's inserts in unit order, then write order (`unnest(…) WITH ORDINALITY`). A plain `bigserial` filled by `INSERT … SELECT FROM unnest` does not guarantee array order (§4 Ordering). |
| `define_queue` | `INSERT … ON CONFLICT DO NOTHING RETURNING`, then compare definitions (§2.1 D2). |
| `write` | One transaction per request: when an id is present, check `requests` for replay; `SELECT … FOR UPDATE` on all keys of the request `ORDER BY key` (W1 makes this one lock set); if a unit carries a metadata op, then `SELECT … FOR UPDATE` on the `queues` row (records before the queue row in every transaction, so no lock cycle); evaluate units in memory (W2a, W13); apply accepted units set-based (`INSERT … SELECT FROM unnest … WITH ORDINALITY`, `UPDATE … FROM unnest` with `fields = (fields - $rm) \|\| $set`, `DELETE`); maintain `record_gates`, `index_entries`, `blocked_gates`, `group_summary` and `stage_counts`; insert the `requests` row when an id is present; `COMMIT` is the ack. |
| `claim` | Scheduled selects: one CTE over the partial claim index, `WHERE stage=$s AND due<=$by AND (leased_until IS NULL OR leased_until<=$now) AND NOT gates-blocked … ORDER BY rank, seq LIMIT n FOR UPDATE SKIP LOCKED`, then `UPDATE … RETURNING`. Reclaim is a predicate: every read, count and sweep treats `leased_until <= $now` as Free (`$now` is bound from the injected `Clock::now()`, never SQL `now()`), with `pending_since = leased_until`. Δ is the sweep interval (≤ 5 s). `WholeGroups`: lock group heads with `SKIP LOCKED`, skip a group with a live lease in any stage, and drop any group not fully locked (whole-group omission, C4). `Keys`: a blocking `FOR UPDATE` on at most 1000 keys, so classification by stage and lease is exact (C2). `Where`: join `index_entries`, filtered by stage. **Group exclusivity (C6a).** Under READ COMMITTED a `WholeGroups` claim on stage B can check "no live lease in any stage" against a snapshot taken before a concurrent claim on stage A commits its lease on another member, and B never tries to lock A's row, so row locks alone would let both claims hold the group. Every claim that leases a grouped record therefore takes `pg_advisory_xact_lock(hash(queue, grp))` (`WholeGroups`: `pg_try_advisory_xact_lock`, skipping the group on failure), then re-checks, in a new statement, that no record of the group has a live lease. |
| `read`, `stats`, `scan`, `aggregate` | Plain SELECTs on committed state, so V3 holds by construction; `read{queue: true}` reads the `queues` row. `stats` sums `stage_counts`: each transaction appends `(queue, stage, Δfree, Δleased)` rows and the Δ sweep folds them, so counting is not O(records) and writers never contend on one counter row. `stats.groups` reads `group_summary` over the `(queue, stage, earliest_ready_at)` index, mirroring §7.2; the summary is maintained in the same transaction as every Free-membership change. A `SELECT DISTINCT grp … LIMIT n` would walk every group when few are ready, which breaks S2's cost bound. The ESF encoder comes from `fireweed-contract`. |
| Time | Every Δ on the injected `Clock`: a bounded `DELETE` sweep for `requests`; a sweep that materializes lapsed leases (`leased_until = NULL`, `pending_since = leased_until`) and folds them into `group_summary` and `stage_counts`; and the `stage_counts` fold. There is no record-expiry sweep. |

### 7.4 Implementation slices (separate worktrees, disjoint files)

| # | Goal | Files owned | Proof | LOC | Deps |
|---|---|---|---|---|---|
| S0 | Split `turso_compose.rs` into lane modules (a pure move) and stub new modules | `crates/fireweed/src/turso_compose.rs`, `turso_parity.rs` → `crates/fireweed/src/turso/{mod,generation,claim,finalize,parity,ports,tick}.rs`; `mod` lines in `lib.rs`; empty `store_turso.rs` | Builds; the source-audit test (`turso_compose.rs:7384-7430`) moved; P1–P4 unchanged | ±7.5k moved | — |
| S1 | Contract crate | new `crates/fireweed-contract/**` | Unit tests for W1/D4/C-shape validation (stage names, terminal flags, metadata size), fingerprint stability, `Version` and `Value` ordering (stages by declaration position), ESF round-trip for field and built-in keys and the 1024 B index-key bound; the §2.0 types and `Clock` trait; a clause-ID manifest test (every public item cites a clause, and retired IDs stay retired) | ~1.7k | — |
| S2 | Black-box suite | new `crates/fireweed-contract-suite/**` | `run_all(ctor, reopen, Δ, clock, fault_hook)`; one scenario per clause, including W2a precedence, C2 skip reasons (`Terminal`, `OtherStage`, `NotAttempted`), C5a progress, §4 insertion order across stage moves, `attempts` unchanged across a release and re-schedule, a finish-then-redrive round trip, a write by key to a record in a terminal stage (payload patch, fields patch, terminal-to-terminal move) whose new values `read` returns, including after `reopen`, a held `{Extend, Put{stage: Set(b)}}` that keeps the record Leased so a concurrent `claim(stage: b)` returns nothing until release, `WholeGroups` exclusivity across stages (C6a, including against a concurrent `Next` claim of another stage that leases a group member), a metadata CAS in one unit with record writes (and its rejection rolling the unit back), `read{queue}` freshness, claim → stats → finish → stats by stage, and release → `stats.oldest_ready_since`; a torn-read adversary for `aggregate` and `stats`; deep scenarios from `fireweed-conformance` (replay, fencing, ordering, unknown outcome through the fault hook, durability through `reopen`) | ~3.6k | S1 |
| S3 | Docs | new `docs/helix/02-design/contracts/API-006-kernel-contract.md`; supersession headers on API-001/002/004/005; PRD FR-45/47a/47c/48/55 (FR-47c: cohorts deleted, D-B); TD-016 constants; TD-010 claim note; `AGENTS.md` rule → `TursoAdmin::projection_control()` | Clause IDs match S1; `ddx doc stale` clean | ~0.8k doc lines | S1 |
| S4.1 | Command and definition types (lands first) | `crates/fireweed-engine/src/command.rs`, `crates/fireweed-core/src/domain.rs` | `stage` on pushed items, `stage` on `ClaimCommand`, `set_stage` on `UpdateFields`, `stage` on `FinalizeOutcome`, `field_patch`, gate add/remove, a no-reset flag for `eligible_since`, and a new `SetQueueMetadata` command; `QueueDefinition.stages: Vec<StageDef { name, terminal }>` and a built-in-key form of index declaration (or reserved pseudo-fields resolved from row columns), both carried by `CreateQueue` so replay rebuilds ordinals, terminal flags and built-in keys. No serde-default compatibility: nothing durable exists yet (D-C) | ~0.3k | — |
| S4 | Planner and overlay | `fireweed-engine/src/{async_composed.rs, commit_surface.rs, claim_validation.rs, finalize_validation.rs, compose.rs}` | Planner tests for W2a–W13; packed Release/Retry/Complete-with-stage, never Rearm or Fail; stage validation, and terminal rows writable (the `compose.rs:2009-2012` rejection goes); the `Write` generation work kind (absent key → `Push` item, existing key → `UpdateFields`, settle → `Finalize` plus a held `UpdateFields`, all in one append with per-unit outcomes) and the overlay's key → (item_id, version) map for keys inserted in unapplied generations; the planner snapshot's `stage`, `group_key`, `gate_keys` and `record_bytes`, plus stored co-key values only for writes that touch an indexed key, so W8 is decided before the acknowledgement; the token fence rejects stale, expired and foreign tokens; per-record `Exists`; N-key transition; deletion of the ungrouped-item rejection (`async_composed.rs:1718-1722`), the push-time group-size rejection and the `claim_validation.rs:77-84` requirement and `BatchTooLarge`; `validate_batch_update` rejects overlay-leased ids; the interleaved update/claim planner test (one generation and two unapplied generations) | ~2k | S4.1 |
| S5 | Apply, schema and in-memory parity | `fireweed-relational/src/{apply.rs, schema.rs, codec.rs, sql.rs}`, `fireweed-projection/src/lib.rs` | `stage` and `stage_since` columns (replacing `terminal_at`); the stage-prefixed claim-order index and the group-head index `(tenant_id, queue_id, group_key, stage, priority_sort, created_seq, item_id)`; `lifecycle_state` derived from stage and lease, including the redrive arm (`Complete` → `Pending`) and a held stage move that keeps a Leased row Leased; every `UpdateFields` path (the single arm, `apply.rs:5567-5737`; the general set-based tier, `:4950-5330`; the operation-shaped tier, `:4570-4870`) and the in-memory `apply_update_fields` accept `Complete` rows: a patch on a terminal-stage row bumps `item_version`, rekeys and keeps `Complete`, and a terminal-to-terminal move sets `stage` and `stage_since`; `FINALIZE_SET` with stage; Finalize rekeys `Stage`-, `StageSince`- and `Due`-keyed indexes (a SQL prefix replace for leading built-in keys, otherwise one batched `fields` read per Finalize batch); `fireweed_stage_counts`; `queues.metadata`/`metadata_version`; a `record_bytes` column maintained by apply; NULL-safe `json_patch` field patches on both tiers; gate add/remove; no-reset `eligible_since`; batched rekey, built-in keys included; `LeaseExpired` without version bump and with `eligible_since = leased_until`; Release, Clear and stage moves set `eligible_since = now` and Retry sets `max(not_before, now)`; the un-gated per-(stage, group) `earliest_ready_at` structure, recomputed set-based once per apply batch over the touched (stage, group) pairs; live == rebuild via `projection_control()`, including writes to terminal-stage records | ~2k | S4.1 |
| S6 | Turso projection reads | `fireweed-turso/src/{projection.rs, query.rs, local.rs, tx.rs, metrics.rs}` | `Keys` claim with stage reasons and `NotAttempted`; stage-filtered `Next`, `WholeGroups` (`e.stage=?`/`i.stage=?` in `select_group_batching`) and `Where`; the packed claim microbatch partitioned by (stage, `due_by`), one ordered scan per partition with a shared exclude set; token batch read; the planner snapshot's `stage`, `group_key`, `gate_keys`, `record_bytes` and co-key loads, and the push key snapshot's `item_id`, `item_version`, `lifecycle_state` and `stage`; per-stage counters (`counts()` by `(stage, lifecycle_state)`; `MetricsDelta::capture` scopes only `UpdateFields`/`UpdateFieldsBatch` entries that set a stage; per-stage fresh after-plans) and stats groups `LIMIT` over the un-gated index (tests: claim, stats, finish, stats; and with an unapplied tail that holds an insert into a terminal stage, a claim and a lease-free finish, `stats` equals `stats` after coverage); `read{queue}`; scan hits with key, stage and lease; built-in index keys; the 1024 B index-key check | ~1.6k | S5 (schema) |
| S7 | S3×Turso `Store` adapter | `crates/fireweed/src/turso/**`, `store_turso.rs`, `crates/fireweed-objectlog/src/{async_projection_apply.rs, reclaim_tick.rs}`, new `crates/fireweed/tests/kernel_capacity.rs` | S2 green; envelope-kind parity test; all §7.2 perf gates; `claim_members` carries stage and `due_by`, and a test in which one generation carries claims on two stages with two `due_by` values gives each member only rows of its stage with `due <= due_by`, with no kernel claim on the exclusive path; `RetainedMembershipChange::Push` carries (stage, terminal) and `RetainedLifecycleChange::Claim` the stage, and the three `stats` tail folds are per-stage; the tick drives V4.1 within Δ on the injected `Clock`, its retry sleeps move onto the `Clock`, and a claim only signals it (under a stepped manual `Clock` a quiet queue recovers its leases within Δ, and no claim waits on coverage); `claim_exclude_ids` covers accepted-but-unapplied updates, and route-X gate and metadata ops are visible to later packed claims and reads; two concurrent route-X metadata CAS writes with the same expected version, exactly one of which applies | ~3.1k | S0, S1, S4, S5, S6 |
| S8 | Facade and helpers | `crates/fireweed/src/{lib.rs, facade.rs, operator.rs}` | Only `Store`, builders, `operator`, `s3turso::open` and `TursoAdmin` are public; S2 passes through the facade | +0.8k / −6k | S7 |
| S9 | Deletions | cohort code (D-B), side-record, fence and `ReplacePending` code in engine, relational, turso and projection; `crates/fireweed-resp` and the RESP wiring in `fireweed-server` (D-D, O-4); discovery; `claim_across_queues`; the operator plane except the helpers; `retained_items`; `current_position`; `crates/fireweed-server/src/change_record_sink.rs` and its config plumbing (it serialized internal envelopes and had no consumer); retired configs, including `terminal_retention_ms` and `client_item_key_retention_ms`; the unemitted `FinalizeKind::{Fail, Rearm}` arms; the FIFO rowid claim scan (`is_fifo_claim_scan_item`, the per-shard claim-scan hints and the `NOT INDEXED … ORDER BY rowid` path, §7.2 queue mapping); `select_same_group` (`Select::Group`, §9 D26); `crates/fireweed-postgres` port backends | Workspace builds; S2 green; perf gates re-run | −21k+ | S8 |
| S10 | In-repo port | `crates/fireweed/tests/**` (39 files, ~18.7k lines), `fireweed-workload`, `fireweed-bench`, `fireweed-server` (including deleting `postgres_native.rs`) | `ss_*` and workflow qualification on the kernel; server green | ±8k | S8 |
| S11 | Snorri migration | snorri `crates/snorri-fireweed/src/lib.rs` | Snorri acceptance on the new pin; transition bench; the §8.1 stages; instance groups with `WholeGroups` replacing the fence and the four conflict-retry loops, with `transition` and `state_store` claimed only by `WholeGroups`; the facade commit, handler timeouts and dispatch outcomes mapped as in §8.1; the instance item in a terminal stage (O-1); awaits read by key with a version CAS, routed by a read on every shard queue, and the re-await rule; the split cancelled stages and `await.dead`; the shard manifest in queue metadata; gate-membership pause and suppress (§8.2); `by_tx_request`; the upsert mapping; the `by_run_target_key` key-space change (D7) | ~2.5k edits | S8 released |
| S12 | `fireweed-pg` (future) | new `crates/fireweed-pg/**` | S2 green with the same torn-read adversary | ~4k | S1, S2 |

**Critical path:** S0 → S4.1 → (S4 ∥ S5 → S6) → S7 (S2 green plus gates) → S8 → S10 → release → S11. S1, S2 and S3 run in parallel from the start. S9 follows S8.

### 7.5 Risks

1. **Write ids.** They are optional (§9 D3), so id-less P1 pushes and P4 settles keep today's cost and resolve `Unavailable` by `read(Keys)` (D8). Writes that carry an id, such as Snorri's transitions, add a request row and a `RequestOutcome`. Mitigation: a compact binary outcome. Gate: 4.
2. **Token validation and held `UpdateFields` + `Finalize` fusion.** Adds one batched read per finalize member. Gate: P4/P4p.
3. **22-index rekey on patch, on the general set-based tier.** Patches that touch an indexed field, a stage or a gate, and `Expect::Version` writes, leave the operation-shaped fast path; unindexed field, payload, `due` and `rank` patches stay on it (§7.2). The W8 snapshot read (stored group, gates, `record_bytes`, and co-key values for patches that touch an indexed key) sits before every such acknowledgement. If P2p/P3p fail, stop and escalate. There is no silent fallback.
4. *Retired (round 2): the expiry sweep. There is no expiry.*
5. *Retired (round 2): the `due: None` sentinel rows. `due: None` is removed (O-3).*
6. **`partial_group` changes FR-47b for oversized groups** (§9 D5).
7. **Snorri pin v0.31.21 predates `main`.** The migration is a rewrite plus a key-space audit, including the `by_run_target_key` key change (D7).
8. **Acked-update-vs-claim exclusion.** It adds overlay bookkeeping to every packed claim and update, stage moves included (§7.2). Gate: P2/P4.
9. *Retired (round 2): the per-batch instance claim. The instance item is data in a terminal stage (O-1).*
10. **Stage-keyed index rekey on finish.** A settle that changes the stage rewrites every index entry keyed on `Stage` or `StageSince`, and a `Finalize{Retry, not_before}` every entry keyed on `Due`. That puts index writes on the packed `Finalize` path for queues that declare such indexes, as Snorri's two lifecycle indexes do. Today the Finalize arm neither reads `fields` nor touches `fireweed_item_index` (`apply.rs:5763-5935`). Leading built-in keys are rewritten in SQL with no read; a composite key whose changed built-in is not leading, such as `(Field, Stage)`, needs one batched `fields` read per Finalize batch. Gate: P4p.
11. **Per-stage counters.** `fireweed_stage_counts` replaces four columns on one row with one row per touched stage, updated in every apply transaction. Gates: P1–P4.
12. **7S's hot Marketo completion is `{Release(t), Delete}` on route X** (§8.3). With no expiry, a finish into a terminal stage would keep every finished task, so the mapping deletes, as 7S does today. Measure it on the Marketo shape before the 7S cutover. If route X is too slow, add a packed `{Release(t), Delete}` member (`Finalize{Release}` and `PurgeItems` in one vector).
13. **`stats.groups` maintenance on the hot apply paths.** The un-gated per-(stage, group) structure is maintained on insert, Claim, Finalize, LeaseExpired and due/rank updates for every grouped non-terminal stage, work that today's summary deliberately skips after Claim/Complete (`apply.rs:3710-3712`). `earliest_ready_at` is a minimum, so a claim needs a reseek, done set-based once per apply batch over the touched (stage, group) pairs (§7.2 `stats` row). The phased-capacity items are grouped, in `max(n/100, 50)` interleaved groups (`crates/fireweed/tests/support/ss_capacity.rs:447-450`), so one 1,000-item claim can touch 1,000 groups. Gates: P1, P3, P4.

---

## 8. Snorri and Seventh Sense migration

### 8.1 Snorri: stages, instance groups and where the side-record data goes

Snorri adapts to the kernel (owner, D-K). Groups are workflow instances, stages are its record kinds, per-transition history rides the settled inputs, awaits are read by key, and the shard manifest is queue metadata.

**Stages** (a proposal; S11 finalizes the names). Today Snorri's record kinds are values of its `record_kind` field (`transition`, `effect`, `await`, `result` and `state_store_item`; snorri `lib.rs:149-151, 208, 215`), and claims filter on it. As stages, a claim names its kind and the filters go.

| Stage | Terminal | Holds | Used by |
|---|---|---|---|
| `transition` | no | Transition inputs: start inputs and the continuations of deliveries, timers, effect results, handler timeouts and dispatch outcomes. `group` = instance id; `rank` = `scheduled_at` µs. | `claim(transition, WholeGroups{n}, due_by)`: one holder per instance (C6a, §9 D18). It is claimed only with `WholeGroups`; any other select on `transition` bypasses per-instance exclusivity. |
| `effect` | no | Effect dispatch items. | `claim(effect, Next \| Where)`; dispatch settles each into `effect.done`. |
| `timer` | no | Timer awaits (today awaits with `await_type == "timer"`, fired by `fire_due_durable_timers`, `lib.rs:6515`), with `due` = fire time. | `claim(timer, Next)`. |
| `state_store` | no | State-store facade items, `group` = instance id. Today every facade claim filters on `record_kind = "state_store_item"` to keep them from the transition claimer (`lib.rs:209-215`). | `claim(state_store, WholeGroups{n}, due_by)` only, as for `transition`. |
| `transition.done` | yes | Settled inputs carrying per-transition history. | `scan` and `read`: acks, recovery, audit hydration. |
| `effect.done`, `state_store.done` | yes | Dispatched effects, finished facade items. | Counted by `stats`. |
| `transition.cancelled`, `effect.cancelled`, `state_store.cancelled` | yes | Cancelled work, one stage per kind, so `count_work`, which counts transition items only (`transition_metrics_request`, `lib.rs:16602-16610`; `canceled_items: count.failed`, `:9125`), counts cancelled transitions only. | Counted by `stats`. |
| `await` | yes | Open external-event and effect awaits. | Read by key on delivery, on every shard queue (below). |
| `await.done` | yes | Consumed awaits and fired timers. | Read by key: `AlreadyConsumed`. |
| `await.dead` | yes | Effect awaits whose handler exhausted its attempts (today `MirrorAwaitStatus::DeadLettered`, `lib.rs:1103`, or `FinalizeKind::Fail` on an exhausted outbox, `:5095`). | Read by key. |
| `result` | yes | Effect results (today `record_kind = "result"`, written by `effect_result_commit_entry`, `lib.rs:5663-5688`). | Read by key. |
| `instance`, `instance.closed` | yes | The instance item, before and after the final transition. | Read by key; written by transition and facade-commit units (O-1). |

**Claim rule.** Every claim that names `transition` or `state_store` MUST be `claim(<stage>, WholeGroups{n}, due_by)`, with `rank = scheduled_at` µs so groups order as today's `scheduled_at` claim does. C6a gives group exclusivity only among `WholeGroups` claims: a `Next`, `Where` or `Keys` claim on those stages would lease an instance's inputs while another holder has the instance. Today's native transition claim is `claim_by_query_at` on `by_record_kind_scheduled_at` with a `claim_at` fallback (`lib.rs:9335-9380`); it becomes `claim(transition, WholeGroups{n}, due_by)`.

Only `transition` inputs and `state_store` items carry the instance group. Timers, effects and awaits keep the instance id in `fields` (`workflow_instance_id`, `lib.rs:140`) and have no group. A leased timer or effect therefore never holds the instance, and group exclusivity, which spans stages (D18), serializes exactly the instance's transitions and facade commits. In this mapping effect dispatch and timer firing change no instance state. If S11 keeps dispatch state in the instance projection, dispatch claims `WholeGroups{effect}` and effect items carry the instance group.

**Shard routing.** Await, timer and result items live in the emitting instance's shard queue: the delivery unit writes `await:{k}` and a grouped continuation atomically, and nothing is atomic across queues (§4). External await keys carry no instance id (`external_event_await_key` = `external_event:{len}:{type}:{correlation}`, snorri-core `lib.rs:878-885`), so a delivery resolves the shard with `read(Keys["await:{k}"])` on every shard queue in the manifest roster, issued in parallel. Found in `await`: deliver in that queue. Found in `await.done`: `AlreadyConsumed`. Absent in every shard: `NoSuchAwait`. If any shard returns `Unavailable`, the delivery fails and never reports `NoSuchAwait`. A key open in more than one shard is ambiguous; the delivery fails rather than choosing, and S11 decides whether such keys must be instance-scoped. Effect results route the same way by effect key. Today's resolver reads a route from the default queue (`durable_await_route`, `lib.rs:4017-4041`) and falls back to a scan of every roster queue (`lib.rs:4164-4237`), so the fan-out is not new; the `by_record_kind_key_instance` index it scans exists only for that routing and is dropped (§8.2).

**Where today's side-record data goes** (all written in the same unit):

| Today (Snorri `lib.rs:13265-13361, 13524-13770`) | Carried by | Written as |
|---|---|---|
| Instance projection `snorri:v1:instance:{id}` + fence `snorri:v1:projection-fence:{id}` (`expected=rev-1 → next=rev`) | **The instance item** `snorri:v1:instance:{id}` in terminal stage `instance`: data, never claimable. The projection lives in `payload`, and the field `idempotency_key: {id}` puts it in `by_stage_key` for the instance listing (§8.2). In normal operation only transition and facade-commit units write it. Both run under a `WholeGroups` claim of the instance's group (on `transition` or `state_store`), which gives one holder per instance across stages, so those writes are serialized without a lease or a fence. Handler timeouts and dispatch outcomes never write it (below). The unit's `Expect::Version(v)`, with v from the hydrating read that a transition needs anyway, fails only against an out-of-band writer (a migration, a recovery or an operator cancel). This removes the fence and the four projection-conflict retry loops (`EXTERNAL_EVENT_`, `TIMER_FIRE_`, `EFFECT_RESULT_` and `EFFECT_DISPATCH_CONFLICT_RETRY_BUDGET`, `lib.rs:160, 170, 179, 191`). | The start input's unit: `{Absent, None, Put{stage: Set(instance), payload}}`. Later transitions: `{Version(v), None, Put{payload: Set(projection)}}`. The final transition also sets `stage: Set(instance.closed)`. A one-shot workflow, whose first transition is also its last, inserts straight into `instance.closed`. |
| Transition metadata `snorri:workflow:transition:{rid}` | **Settled input item** fields `tx_request_id`, `instance_id`, `state_seq`, plus index `by_tx_request (tx_request_id)` | The settle: `{Any, Release(t), Put{stage: Set(transition.done), fields, payload}}` |
| Input ack `snorri:workflow:input:{event}` (point-read by `durable_input_ack`, `lib.rs:7127-7143`) | Settled input item field `consumed_event_id`, plus index `by_consumed_event` | The settle. The ack lookup becomes `scan(by_consumed_event, eq=[event], limit 1)`, which observes every acknowledged settle (V3), so delivery status still comes from durable state. |
| `state:{inst}:{rid}`, `audit:{inst}:{rev}:{rid}`, `audit-digest:…` | Settled input item: `audit_digest` in `fields`; `next_state` and `audit`, KB-sized blobs, in the off-row `payload` (the settle replaces it whole with `{next_state, audit}`; nothing reads the input's original payload after settle, and Snorri folds it into the blob if it ever needs it; D9 keeps blobs off the hot row). Index `by_instance_revision (instance_id, state_seq)` for hydration. | The settle |
| Await index `snorri:v1:await-index:{k}` | **The await item** `await:{k}` in terminal stage `await` of the emitting instance's shard queue (field `instance_id`; the queue is found by the shard-routing read above, so the item carries no `queue_id`): data, read by key. A delivery consumes it with a version CAS that moves it to `await.done`, so a replayed delivery still classifies as `AlreadyConsumed` and an unknown key as `NoSuchAwait` (snorri-core `lib.rs:897-909`). | In the emitting transition, after its hydrating read of `await:{k}` in its own shard: absent → `{Absent, None, Put{stage: Set(await), fields}}`; in `await.done` (a key awaited again after consumption) → `{Version(v), None, Put{stage: Set(await), fields}}`; already in `await` → the transition rejects the program before writing. A bare `Absent` must never reject the whole transition unit. Consumption below. |
| Timer awaits | **The timer item** `timer:{k}` in `timer`, with `due` = fire time | `{Absent, None, Put{stage: Set(timer), due: Set(fire_at), fields}}` in the emitting transition |
| Effects and continuations (`lifecycle_items`) | Effect items in `effect`; continuations in `transition`, with the instance group | `{Absent, None, Put{stage, due, rank, fields}}` |
| Effect results | **The result item** `result:{…}` in `result` | Inserted by the result unit (below) |
| Replay markers (bootstrap, migration, recovery) | Deleted | Request-id replay, as for `STATE_STORE_COMMIT_KEY_PREFIX` |
| Migration and recovery audit markers | Fields on the instance item | `{Version(v), None, Put{fields}}` in the migration or recovery unit |
| Shard manifest `snorri:v1:workflow-shard-manifest` (`lib.rs:3244-3245`), a side record on shard 0 | **Queue metadata** of shard 0's queue, entry `snorri.shard_manifest` (§3.2) | Bootstrap: a unit with only a metadata op, `{expect: Version(0), patch: {snorri.shard_manifest: Some(Bytes(manifest))}}`. A losing concurrent opener gets `MetadataVersionMismatch` and reads the winner's manifest. Open: `read{keys: [], queue: true}`. This replaces the bootstrap retry loop (`WORKFLOW_SHARD_MANIFEST_BOOTSTRAP_MAX_ATTEMPTS`, `lib.rs:133, 3448`). |
| State-store commit record `STATE_STORE_COMMIT_KEY_PREFIX` | Deleted | Request-id replay covers it |

**The transition** (today a claim, then a finalize, then 6+ side records, then a fence). The write carries the transition's `request_id` (D3):
```
claim(transition, WholeGroups{n}, due_by) → the inputs of up to n instances, tokens t_in; one holder per instance
read(Keys[instance keys, emitted await keys]) → projections and versions v, await states   (the hydration a transition needs anyway)
Unit { gates: [], metadata: None, writes: [
  { input_key,                 Any,        Release(t_in), Put(Patch{ stage: Set("transition.done"),
                                                            fields: {tx_request_id, instance_id, state_seq,
                                                                     consumed_event_id, audit_digest},
                                                            payload: Set({next_state, audit}) }) },
  { "snorri:v1:instance:{id}", Version(v), None,          Put(Patch{ payload: Set(projection) }) },   // start: Absent, stage "instance"
  { "effect:{k}:attempt:1",    Absent,     None,          Put(Patch{ stage: Set("effect"), due: Set(t), rank, fields{…} }) },
  { "await:{k}",               Absent | Version(v_k), None, Put(Patch{ stage: Set("await"), fields{instance_id, …} }) } ] }   // re-await rule above
```

**A delivery, a timer or an effect result** records an event and never writes the instance item:
```
read(Keys["await:{k}"]) on every shard queue → in "await" of queue Q with version v; in "await.done" → AlreadyConsumed; absent everywhere → NoSuchAwait
Unit (in Q) { writes: [
  { "await:{k}",       Version(v), None, Put(Patch{ stage: Set("await.done"), fields{consumed_event_id} }) },
  { continuation_key,  Absent,     None, Put(Patch{ stage: Set("transition"), group: Set(Some(instance)),
                                                    due: Set(now), rank: Set(scheduled_at_µs), payload }) } ] }
```
- Two deliveries of one await race on `Version(v)`. One wins, and the other re-reads and gets `AlreadyConsumed`.
- A timer is claimed (`claim(timer, Next)`) and settled the same way: `{Release(t), Put{stage: Set("await.done")}}` plus the continuation.
- An effect result also inserts `result:{…}` in stage `result`.

**The other paths that write the instance projection today** (each fenced today, and each covered by the fence removal only as mapped here):
- **Facade commits.** `state_store_commit` (`lib.rs:8049-8150`) writes the instance projection under `instance_fence` for facade inputs claimed with `claim_by_query_at` on `record_kind = state_store_item` (`lib.rs:7926-7970`). `state_store` items carry `group = instance` and are claimed only with `claim(state_store, WholeGroups{n})`. The facade commit writes the instance item with `Version(v)` in the same unit as its settles, as the transition does. Group exclusivity spans stages, so a facade holder and a transition holder never hold one instance at once.
- **Handler timeouts and dispatch outcomes** (`record_durable_handler_timeout`, `lib.rs:5038-5112`, which claims the await by key, bumps the outbox attempts, re-issues or fails the effect and loops on `ProjectionConflict`; `record_durable_dispatch_outcome`, `lib.rs:5154-5200`, a throwaway push-claim-commit that writes the outbox) never write the instance item. Each one CAS-updates the await (`{Version(v), None, Put{fields{attempts}}}`, routed as above), inserts the next `effect:{k}:attempt:{n+1}` or moves the await to `await.dead`, and inserts a continuation into `transition` (group = instance) that carries the outbox change for the instance's next transition to apply.

With these, only transition and facade-commit units write the instance item, which is what O-1 and the retry-loop row in §8.2 rely on.

**Net rows per transition:** 0 new rows for metadata, instead of 6+ side-record upserts. The input row is patched in place and the instance item is rewritten in place. There is no fence row and no extra claim.

**Retention.** There is no expiry (owner, D-A). Settled inputs, consumed awaits, results and instance items stay until Snorri deletes them. That matches today's footprint: the side records are never deleted today (the await index, `lib.rs:1305-1320`), and S3×Turso never reaps terminal rows (§7.1).

### 8.2 Snorri call sites

| Snorri today | New |
|---|---|
| `create_queue` | `define_queue` with the §8.1 stages and 22 indexes (of 32). They are today's 19 production `IndexDef`s over `fields` (`scheduled_action_typed_indexes`, `lib.rs:16647-16902`; without `unique`), with `record_kind` replaced by the `Stage` key in its two lifecycle indexes (`by_record_kind_key`, `by_record_kind_scheduled_at`, `lib.rs:136-137`, renamed `by_stage_key` and `by_stage_scheduled_at`), plus the §8.1 indexes `by_tx_request`, `by_consumed_event` and `by_instance_revision`. The SS1a variant's `by_record_kind_key_instance` (`lib.rs:148`; added only by the operator-activated `scheduled_action_typed_indexes_with_instance`, `:16903`) is dropped: only the await resolvers scan it (`lib.rs:4164-4237`), and await reads by key replace them (§8.1 shard routing). The previous draft counted it among the 19 and named three `Stage`-keyed indexes; there are two. `metadata`, `index_fields` and `entity` fold into `fields`. There is no retention setting. |
| `push`, `push_with_request_id`, `push_batch_with_request_id` (10,000/batch) | `write` insert units naming their stage, chunked to at most 1000 per request, with request ids `{rid}:{chunk}` |
| `upsert` / `create_work_items` (`lib.rs:7682-7706`, "Create or replace", Inserted/Replaced) | `{Any, None, Put{stage: Set(s), clear_fields: true, fields: <all>, payload: Set, due: Set, rank: Set, group: Set(g), gates: Replace}}`, which replaces the item as today's supersede does (`crates/fireweed/src/turso_parity.rs:138-189`); a bare `Put` would leave stale fields behind. `created` gives Inserted vs Replaced. A `Leased` rejection surfaces as today's error, or S11 adds a Skipped status (`WorkflowItemCreateStatus` has only Inserted and Replaced, `lib.rs:1546-1551`). A record in a terminal stage is replaced and moved to `s`, where today it is refused as `Terminal`; if Snorri must keep refusing, it reads first and writes with `Version(v)` (§10 O-5). A group change is `Invalid` (W8). |
| `claim`, `claim_at` | For `effect` and `timer`: `claim(stage, Next, due_by)`, looped for more than 1000 (never for `transition` or `state_store`, §8.1 claim rule) |
| `claim_with(metadata_equals)` intents; push → `claim_with` → `commit` dances (bootstrap, migration, manifest) | The `record_kind` filter becomes the claim's stage. Replay markers become request-id replay. A migration or recovery is one unit that CAS-writes the instance item (`{Version(v), None, Put{fields: audit}}`). The manifest bootstrap is one metadata CAS (§8.1). The marker, purge and filtered-claim steps disappear. |
| `claim_by_query(_at)` | For `effect` and `timer`: `claim(stage, Where, due_by)`. The native transition claim (`claim_by_query_at` on `by_record_kind_scheduled_at` with a `claim_at` fallback, `lib.rs:9335-9380`) and the facade claim (`lib.rs:7926-7970`) become `claim(transition \| state_store, WholeGroups{n}, due_by)` (§8.1). |
| Key-range claim plus release of extras | `claim(stage, Keys)` with skip reasons |
| `commit`, `commit_multi_claim` | As in §8.1: `claim(transition, WholeGroups)`, the hydrating instance read, then one unit per instance with several `Release` settles and the instance write |
| Projection-conflict retry loops (four budgets of 8, `lib.rs:160, 170, 179, 191`) | Deleted: group exclusivity serializes transitions and facade commits, handler timeouts and dispatch outcomes write only their await and a continuation, and deliveries CAS their await (§8.1) |
| `ack`, `nack(Release)` | `{Release(t), Put{stage: Set(<done stage>)}}` (for example `state_store.done`), `{Release(t), Put{}}` (`Finalize{Release}`) |
| `reschedule` (`lib.rs:11226-11260`, lease-preserving) | `{Version, None, Put{due}}`. `Leased` is counted as `conflicted`, which Snorri already reports (§10 O-2). |
| `mutate_items` live mutations | Keep today's gate-membership scheme, which the clauses already support. `snorri.paused`, `snorri.suppressed`, `snorri.failed` and each per-rule gate are blocked once, with unit gate ops. PauseRun = `scan(by_run_status, eq=[tenant, run])`, then per item `{Exists, None, Put{gates: Edit{add: {snorri.paused}}}}` (`scan` hits carry no version; a gate `Edit` is commutative, so no CAS is needed); ResumeRun removes the gate. SuppressRule adds the rule's gate to the items it selects (rules are created after their items, `lib.rs:14439-14460`). Lifecycle classification reads `Record.gates` (`lib.rs:14629-14690`; ResumeRun selection, `:14356-14376`). Run and rule mutations are paged and not atomic, unlike today's all-or-nothing `SelectFirst`; a Leased item reports `Leased` and is retried (§10 O-2). CancelRun/CancelItems: `scan` by run, then `{Exists, None, Put{stage: Set(<kind>.cancelled)}}`, with the kind from the hit's stage (`transition.cancelled`, `effect.cancelled` or `state_store.cancelled`), or `{Exists, Clear, Put{stage: Set(<kind>.cancelled)}}` for leased items, which is valid even if the lease was reclaimed in between (W4, W5). DeleteNotStarted: `read(Keys)` for the version, then `{Version(v), None, Delete}`. |
| `purge(force)` | `{Any, Clear, Delete}` |
| `side_record`, `live_item`, `claimed` | `read(Keys)`, or the ack scan (§8.1) |
| `side_records_by_prefix` (instance listing, audit hydration) | Instance listing: `scan(by_stage_key, eq=["instance"])` (and `"instance.closed"` for finished ones). Audit hydration: `scan(by_instance_revision, eq=[inst])`, then `read(Keys)` for the payloads. |
| `explain_commit`; `recover_mirrored_transition_in_queue(queue_id, rid)` (`lib.rs:11893-11916`) | `scan(by_tx_request, eq=[rid], limit 1)`, then `read(Keys)` of the input item (in `transition.done`, with `tx_request_id`). The input key `{request_id}:{run_id}:{item_kind}` cannot be derived from `rid`, and a fresh handle lacks the original request body for a resend. |
| Shard manifest read and bootstrap (`lib.rs:3244-3245, 3448`) | `read{keys: [], queue: true}`; bootstrap by metadata CAS (§8.1) |
| `metrics` (`lib.rs:8265-8270`) | `stats`: free and leased per stage. `count_work` stays transition-only: pending = free(`transition`), leased = leased(`transition`), complete = free(`transition.done`), failed = free(`transition.cancelled`). |
| `metrics_by_query`, `grouped_aggregate`, `declared_bucket_segment`, `range_scan` | `aggregate(Dims[Attr(Stage), Lease])`, `aggregate(Dims)`, `aggregate(Ranges)`, `scan` |
| `reclaim_expired_at(lease_time)` (`lib.rs:7936-7946`) | Deleted. Snorri injects its lease clock as the constructor `Clock` (D12). The reclaim tick sleeps on that `Clock`, so advancing it wakes the tick, and a claim that sees Δ elapsed signals it (V4). That keeps quiet queues recoverable without an ambient clock read, as the cited comment requires. |
| `commit_capabilities`, `hot_projection_capabilities` | Deleted |
| `projection_control` | `TursoAdmin` |
| Unique `by_run_target_key` (production, `lib.rs:16865-16874`) | The item key must carry the target identity, replacing today's `client_item_key` `{request_id}:{run_id}:{item_kind}` (`lib.rs:15238`), whose request-id redelivery dedup (`:14225`) must survive. S11 picks one key that preserves both properties, for example `(tenant, run, kind, version, target)`, with a proof that request-id redelivery yields the same target (D7). |

### 8.3 Seventh Sense

With stages, Seventh Sense's job types and its Marketo and callback pipelines become stages of a few queues instead of one queue each.

| Trait method | New |
|---|---|
| `TJobsQueue.create` / `TConnectorsQueue.create` (conflict-ignore on `(jobId, type)`, `QuillJobsQueue.scala:29-35`, and on `(connectorId, type)`, `QuillConnectorsQueue.scala:29-31`) | One `jobs` queue whose non-terminal stages are the job types, and one `connectors` queue whose stages are the connector types. The key is `{jobId}:{type}` on `jobs` and `{connectorId}:{type}` on `connectors`, and the stage is the type. `{Absent, None, Put{stage: Set(type), due: Set(now), rank: Set(now_µs), gates: {acct:a, conn:c}, fields}}`; `Exists` means ignore. `rank` = now matches `last_processed_at DEFAULT now()` (7S `interfaces/graphile/db/postgres/dump.sql:919-926`), so a new job does not jump ahead of processed ones. Create units carry no gate ops. A type added in a later release needs a new stage (§10 O-6). **Trade-off.** Types as stages share one queue's caps (8 requests per claim generation, at most 4 generations and 16 requests per queue, `crates/fireweed-engine/src/claim_batch.rs:22-30`) and one object-log shard, where per-type queues each had their own pipeline. One queue per type stays an allowed mapping, and it also sidesteps O-6. Before the 7S cutover, measure concurrent `handleNextJob` across all types on one `jobs` queue. |
| Account and connector status | The only writer of gate state. A status change is one gate op on the `jobs` queue and one on the `connectors` queue (gate state is per queue, W9), plus one per interim single-type queue (§10 O-6); with types as stages this replaces the per-type fan-out. A new connector's gate is set `Blocked` when the connector is created, before any of its jobs are inserted. A blind gate op inside a create unit could overwrite a concurrent status change, so create units never carry one. |
| `handleNextJob(type, asOf, timeout)` / `handleNextConnector(type, asOf, timeout)` (`TJobsQueue.scala:20`, `TConnectorsQueue.scala:14`) | `claim(stage: type, Next, max 1, lease=timeout, due_by=asOf)`, then the effect, then `{Any, Release(t), Put{due: Set(nextProcessingAt.getOrElse(asOf)), rank: Set(asOf_µs), fields{inactivityCount}}}` or `{Release(t), Delete}`. In 7S a NULL next time means due now (`.filter(_.nextProcessingAt.forall(_ <= lift(asOf)))`, `QuillJobsQueue.scala:77`, `QuillConnectorsQueue.scala:82`); `State.active()` and `inactive(Seq.empty)` yield it (`JobQueueItem.scala:29-30`; `MarketoEventLeadEnrichmentWorker.scala:94`). |
| JobContentLoader legacy type | Today it claims `type.legacy + type` together (`QuillJobsQueue.scala:76`). A claim names one stage, so there is no legacy stage. At cutover each legacy row is written as `{jobId}:job-content-loader` in stage `job-content-loader` with `Absent` (an existing new-type row wins), as 7S migration 000100 already renames `callback-job-content-loader` rows (`interfaces/graphile/db/postgres/migrations/committed/000100.sql:6-9`). |
| `deleteByJobIds` / `deleteByConnectorIds` | `{Any, Clear, Delete}` on `{jobId}:{type}` (on `jobs`) or `{connectorId}:{type}` (on `connectors`) for every declared type, and on each interim single-type queue (§10 O-6). The stage set is fixed, so the keys are enumerable without a scan. |
| Connector and job deletion (Postgres cascades them to the Marketo and callback tables: `actions_scheduled_job_id_fkey … ON DELETE CASCADE`, 7S `dump.sql:3973`; both Marketo queues' `connector_id` foreign keys, `:4173`, `:4181`) | Connector deletion pages `scan` over a `(Stage)` index of `marketo:{c}` and deletes each hit with `{Any, None, Delete}`; job deletion does the same for `callbacks:{job}`. A `Leased` hit is retried after its lease ends, since only the holder writes a Leased record (the owner's round-3 answer R3-2, §14 round-2 verification). The emptied queue's definition, metadata and counter rows stay, because the contract has no queue deletion (§10 O-7). |
| Marketo `create` (one txn with a connector row) | One queue per connector, `marketo:{c}`, created with `define_queue` before its first write (§2.1 D5), with two non-terminal stages: `lead` (lead enrichment) and `email` (email enrichment). First the connector job `{Absent}` on the connectors queue, then lead tasks `{Absent, None, Put{stage: Set(lead), due: Set(now), rank: Set(ctime_µs), group: Set(Some("lead:{leadId}")), fields}}`. These are two requests; a crash between them is harmless. Group keys carry a `lead:` or `email:` prefix because group exclusivity spans stages (C6a, §9 D18) and lead and email ids are both Marketo integers in one queue's group namespace. |
| `takeNextTasks(asOf, N)` | `claim(stage: lead, WholeGroups{N}, due_by=asOf)` |
| `hasNextTasks(asOf, N, maxWait)` | `stats{groups: {stage: lead, limit: N, due_by: asOf}}`, then `ready_groups >= N \|\| asOf - oldest_ready_since > maxWait`. Retried tasks count from their ready time, not their ctime. |
| `updateRetryInfo` | `{Any, Release(t), Put{due: Set(nextRetryAt.getOrElse(now)), fields{retryCount}}}` (the same NULL-means-now rule, `QuillMarketoEventLeadEnrichmentQueue`) |
| Marketo `delete` (the hot completion; successful and retry-exhausted tasks, `MarketoEventLeadEnrichmentWorker.scala:107-110`) | `{Release(t), Delete}`, which is what 7S does today. Records live until deleted (D-A), so a move to a terminal stage would keep every finished task. It runs on route X (risk 12). |
| `TMarketoEventEmailEnrichmentQueue` (`create`, `takeNextTasks`, `updateRetryInfo`, `delete`; a live worker) | Stage `email` of the same per-connector queue, grouped by email. 7S takes one email per batch: the query picks the email with the oldest ready task, then up to `maxTasks` of its tasks (`QuillMarketoEventEmailEnrichmentQueue.scala:56-68`), and the worker enriches the whole batch with the first task's email (`MarketoEventEmailEnrichmentWorker.scala:139`), so a batch that mixed emails would corrupt event data. Create: `{Absent, None, Put{stage: Set(email), due: Set(now), rank: Set(ctime_µs), group: Set(Some("email:{emailId}")), fields}}`. `takeNextTasks(asOf, maxTasks)` = `claim(stage: email, WholeGroups{max_groups: 1}, max_records = maxTasks, due_by = asOf)`: the first ready email's tasks, or a prefix with `partial_group` when that email has more than `maxTasks`, as today's one email then `take(maxTasks)`. Retry and delete work as for leads. |
| Callbacks queued `createOrUpdate` | One `callbacks:{job}` queue per job, with non-terminal stages `queued` and `scheduled` and terminal stages `succeeded` and `failed`. A queued action and the scheduled action it becomes share one id (`CallbackActionQueued.scala:27-34`, `CallbackActionScheduled.scala:17-23`), so they are one record that moves between stages. `createOrUpdate` = `{Any, None, Put{stage: Set(queued), due: Set(now), rank: Set(0), group: Set(Some(callbackId)), fields}}`; the group is set at the first insert, since it cannot change later (W8). `rank: Set(0)` and `due: Set(now)` are the queued defaults, so a record moved back from `scheduled` regains insertion order instead of keeping its scheduled `rank = state_ordinal·10^17 + executed_µs`, and is claimable at once instead of at its scheduled `due`. A `Leased` rejection (the scheduler holds the record) is retried. It is a sparse `Put` without `clear_fields` and without a `ctime` field, so a conflict keeps the record's insertion order, as today's `onConflictUpdate` keeps `ctime`. Ids are deduplicated per request (W1 rejects a repeated key, which would make a whole Kafka batch `Invalid`). An update of an action that is already past `queued` moves it back to `queued`; today the same update adds a second, queued row beside the scheduled one. |
| Callbacks `findByJobId(jobId, max)` | `claim(stage: queued, Next, max_records = max)`: the records are leased for the scheduler, in insertion order. The claim is V2-fresh, so the scheduler's `findByJobId → deleteByIds → findByJobId` loop (`CallbackJobSchedulerWorker.scala:46-73`) never sees what it already moved. |
| The scheduler's `scheduledActionCreator.create` then `deleteByIds` (`CallbackJobSchedulerWorker.scala:72-75`) | One settle per action: `{Release(t), Put{stage: Set(scheduled), due, rank, fields{state, state_ordinal, retryCount, scheduled, executed, batchChecksum, single, stats_type, …}}}`. `rank = state_ordinal·10^17 + executed_µs`, so Scheduled comes before Failed, then by `executed`. Batch members get a common `due = min(executed)`. Today's create-then-delete becomes one atomic stage move, so a crash between the two steps no longer leaves both rows. Actions that failed scheduling are released with `{Release(t), Put{}}`. |
| `findReadyToExecuteByJobId` (`QuillCallbackScheduledActionsPersistence.scala:55-96`) | Batches: `claim(stage: scheduled, WholeGroups{1})`. The batch is complete iff `claimed.len() == batchChecksum`, as `isFullBatch` does (`CallbackActionScheduled.scala:37-40`). A single forms a one-member group with no `batchChecksum`, which reproduces today's "first ready action is a single" branch. Otherwise, release all and claim singles: `claim(stage: scheduled, Where{by_single (Stage, single, state_ordinal, executed), eq=[Str("scheduled"), Bool(true)]}, max_records = maxSize)`. `by_single` leads with `Stage`, as §7.2 requires for index claims on a mixed-stage queue, so the walk skips the succeeded and failed singles that stay until `deleteExpiredByJobId`. No counter record and no gate. Results settle with the token, writing the fields 7S's `complete` sets (`callbackActionState`, `completed`, and `retryCount + 1` on Failed; `CallbackActionScheduled.scala:51-63`): `{Release(t), Put{stage: Set(succeeded), fields{state: Success, state_ordinal: 3, completed}}}`; to retry, `{Release(t), Put{due: Set(executed), rank: Set(4·10^17 + executed_µs), fields{state: Failed, state_ordinal: 4, retryCount, completed}}}`, so a retried action sorts after every Scheduled one in `WholeGroups` and `by_single` order, as 7S's `(callbackActionStateId, executed)` order does (`QuillCallbackScheduledActionsPersistence.scala:79`; Scheduled = 1, Failed = 4, `CallbackActionState.scala`); once `retryCount ≥ maxRetries`, `{Release(t), Put{stage: Set(failed), fields{state: Failed, state_ordinal: 4, retryCount, completed}}}`, so `calculateStats`, which groups by `state`, counts it as Failed. |
| `calculateStats` | `aggregate(Dims[TimeBucket(scheduled, 3_600_000), Attr(state), Attr(isEnrolledUsingOpenRateFilter), Attr(engagementClass), Attr(personalizationClass)])` over `by_stats (stats_type, scheduled, state, isEnrolledUsingOpenRateFilter, engagementClass, personalizationClass)` with `eq=[true]`. The queue is per job, so the index needs no `jobId`. The dimensions are index keys, as §2.7 Q4 requires. Q1 leaves out of an index any record that lacks a key, and 7S's dimensions are optional (`card.flatMap(_.field1(…))`, `QuillCallbackScheduledActionsPersistence.scala:111-118`; `Option[String]` in `ScheduledActionBucketResponse`, `:141-147`), which today's SQL groups under NULL. So every scheduled record writes all `by_stats` keys, with `Str("")` as the sentinel for an absent card attribute, which 7S maps back to `None`, and writes `stats_type` as true (type empty or in `typesForStats`) or false. `withAllTypes = true` (JobStatsService) is the same aggregate with empty `eq`, over the whole index. |
| `deleteExpiredByJobId(jobId, asOf)` (deletes rows with `scheduled <= asOf`, `QuillCallbackScheduledActionsPersistence.scala:128-137`) | 7S's own retention, kept as caller logic: `scan(by_scheduled (scheduled), upper ≤ asOf)` page by page; skip hits in stage `queued` (a re-queued action keeps its old `scheduled` field, and today's queued row is a separate row that survives); `read(Keys)` the rest and delete each with `{Version(v), None, Delete}`. The version keeps a concurrent re-queue from being deleted, and a `Leased` record (held by the executor) is left for a later run instead of losing its lease. A future `delete-by-filter` op (D-A) would make it one call. |

**FR coverage:**
- FR-44: `rank` is the timestamp.
- FR-45: gates, including caller gates for static blocks.
- FR-46: insert now, `Put{due, rank}` later.
- FR-47 and FR-47a: tenant, queue, stage, `group`, `fields`.
- FR-47b: `WholeGroups`, with `partial_group` for oversized groups.
- FR-47c: cohorts are deleted (owner, D-B). An atomic group insert, a `WholeGroups{1}` claim and a caller count check (`batchChecksum`) serve it. Where members trickle in over several units, as 7S's do, the count check at claim covers the gap.
- FR-55: `{Release(t), Put{due: next}}` forever (never `Rearm`, so `attempts` keeps counting), `Delete` to remove.

---

## 9. Decisions

### 9.1 Owner decisions, round 2 (binding)

| # | Decision | Applied in |
|---|---|---|
| D-A | Retention is a consumer concern. No expiry machinery: no `expires_at`, no `terminal_retention`, no expiry sweep. Records live until the consumer deletes them. A future `delete-by-filter` op is noted, not specified. | §1, §3.3, §4 V4, §7, §8 |
| D-B | Cohorts are deleted. FR-47c is served by an atomic group insert, a `WholeGroups` claim and a caller count check. | §3.3, §6, §8.3 |
| D-C | Pre-production: no existing durable data, no import or replay compatibility; old command kinds need not decode. | §7.4 S4.1, S9 |
| D-D | The RESP adapter is dropped. | §6, §7.4 S9, §10 O-4 |
| D-E | Index definitions are required at queue creation, and indexes may use built-in attributes; stage is the main one. | §2.1, §3.1 |
| D-F | The queue definition (stages with terminal flags, and indexes) is immutable and create-or-verify. | §2.1 D3, §3.1 |
| D-G | Queue metadata: a versioned `Value` map owned by the consumer, never interpreted, with CAS and sparse patches, written inside write units, read fresh, and bounded (64 KiB). Snorri's shard manifest lives there. | §2.2 W13, §2.4 R4, §3.2, §8.1 |
| D-H | Stage is a first-class record attribute: at most 32 declared stages, each terminal or not; one stage per record; any move allowed; each claim names one non-terminal stage; stats count by stage and lease; aggregates and indexes may use stage; S3×Turso stores it as a column that prefixes the claim-order index. | §1, §2, §3.1, §7.2 |
| D-I | Terminal stages replace Completed/Failed. The lease (Free or Leased) is the only engine-owned state. No `Finish` op and no `Terminal` write rejection; `stage_since` replaces `finished_at`. | §1, §2.2 |
| D-J | A claim walks a stage and takes exclusive, crash-safe batches: the state change is the cursor. A write by key with no lease serves records the caller already knows. One `write` op. | §5 |
| D-K | Fireweed and Snorri co-evolve. Snorri uses instance groups with `WholeGroups` (replacing the fence), history on settled inputs, awaits read by key, and the manifest in queue metadata; its record kinds become stages. | §8.1, §8.2 |

### 9.2 Decisions made on the owner's behalf

Each is overturnable. IDs D1–D17 are kept from earlier rounds; D17 is retired and D18–D26 are new.

| # | Decision | Rationale (overturnable) |
|---|---|---|
| D1 | Revised (round 2). Terminal stages replace terminal states (D-I), and there is no expiry (D-A). On S3×Turso a record in a terminal stage is stored as today's `Complete` lifecycle row, so a finish still rides the packed `Finalize` (P4). | Lowest production cost: the internal lifecycle keeps three live values, and the claim-order index stays partial on `Pending`, now prefixed by stage. One time-driven transition remains (V4.1; request-id retention is a MAY, V5), against today's lease, cohort, tombstone and request transitions. Snorri's completed/failed counts survive as the free counts of its terminal stages. |
| D2 | A non-holder `Put` on a Leased record is rejected per record with `Leased`. Only the token holder patches, through the lease sub-op. | The operation-shaped packed update is pending-only (`apply.rs:5129`; API-001:347; `one-projection-cleanup.md:139-144,333`), and the planner must enforce the same for the general tier, which accepts Leased rows (§7.2). Allowing non-holder writes needs an overlay rewrite and reopens the lost-update race. Snorri's cost is O-2. |
| D3 | Claim and write `request_id` are both optional. Id-less writes resolve `Unavailable` by `read` (D8); an id gives replay. Snorri's transitions carry ids. | Keeps the durable receipt off the hot paths: a claim with an id writes it (`turso_compose.rs:4486-4500`; replay read at `:5400-5420`), and P1 pushes carry none today (`ss_phased_capacity.rs:152-164`). Revised in the verification pass, which found the old "writes need unknown-outcome resolution" rationale contradicted D8. |
| D4 | `Version = (incarnation, seq)`: `item_id` and `item_version` +1 on Turso; `seq` and `ver` on PostgreSQL. Lease reclaim does not change it. | No position-derived versions. Avoids ABA across re-insert at zero cost (`ItemId` is `epoch<<40 \| node<<32 \| counter`, `domain.rs:116-130`). |
| D5 | Remove `max_group_size`. Never reject inserts; an oversized first group, by record count or by response bytes, is claimed partially with `partial_group`. | The Marketo `create` has no per-lead cap. The alternatives are wedging the queue (`TooLarge`) or silent starvation. |
| D6 | Gate ops and the queue-metadata op live inside write units; pause is the reserved gate `"*"`. There is no `set_gates`, `pause` or metadata op. | Snorri flips gates atomically with item patches, and its manifest bootstrap is one metadata CAS. One mechanism for pause, gates and metadata. |
| D7 | Remove `unique` indexes. The key is the only uniqueness. | One identity mechanism. The only consumer is Snorri's production `by_run_target_key` unique constraint (`lib.rs:16865-16874`), which Snorri treats as load-bearing. Snorri must make the item key encode `(tenant, run, kind, version, target)` for those items, replacing its current `client_item_key` dedup (`{request_id}:{run_id}:{item_kind}`, `lib.rs:15238`; redelivery rationale at `:14225`). S11 picks one key that preserves both properties and proves that request-id redelivery yields the same target. |
| D8 | No `read(Outcome)` and no `explain_commit`. | Replay-by-resend (writes with an id) plus a `read` of the touched items and, for a metadata op, of the queue (every write) resolves every unknown outcome. Snorri locates a transition by `scan(by_tx_request)` (§8.2). |
| D9 | Two data slots: typed `fields` (sparse, hot row, indexed) and opaque `payload` (whole, off-row). | `payload` stays off the hot row so Claim/Complete do not rewrite blobs (`schema.rs:55-57`). `metadata`, `entity` and `index_fields` collapse into `fields`. Snorri's KB-sized `next_state` and `audit` go in `payload` (§8.1). |
| D10 | The operator is a helper module. Discovery, coordination and multi-queue claim leave the contract, and the RESP adapter is deleted (D-D). | No consumers; the owner could not define the operator. |
| D11 | Revised (round 2): the `redrive` helper is restored as one atomic stage move out of a terminal stage. | The verification pass deleted it because its Delete-then-insert was not atomic. Terminal records are now writable, so it is one write (§5). |
| D12 | The injected `Clock` governs leases and Δ. Snorri's per-request lease clock becomes the constructor `Clock`; host-driven reclaim goes away. | One semantics. The constructor-owned tick, scheduled on the `Clock` and woken by any claim that sees Δ elapsed (V4), replaces `reclaim_expired_at` and keeps quiet queues recoverable under a stepped clock. Revised in the round-2 verification: a claim never runs the sweep inline, because on S3×Turso a sweep is a coverage wait plus a durable append, and item claims never wait for apply (`turso_compose.rs:1089-1092`). |
| D13 | `stats.groups` counts one non-terminal stage and ignores gates, pause and Leased members, so it can overcount `WholeGroups`-claimable groups. | The existing summary recompute anti-joins blocked item gates and is deliberately stale after Claim/Complete (`apply.rs:3703-3712`; `fireweed-turso/src/projection.rs:607-626`), so it cannot serve S2. S2 uses a separate un-gated per-(stage, group) structure (§7.2); a gate-aware count would need an anti-join per group. |
| D14 | Retire FR-48 (discovery). Reword FR-45 (no `metadata_blockers`), FR-47a (no fairness via discovery) and FR-47c (cohorts deleted: atomic group insert, `WholeGroups` and a caller count check, D-B). | No consumers. |
| D15 | Per-request limits of 1000 rows and 16 MiB, key ≤ 1024 bytes, record ≤ 4 MiB, encoded index key ≤ 1024 bytes, lease ≤ 1 h, at most 32 stages, queue metadata ≤ 64 KiB. | TD-012 row and byte admission. Snorri's 10,000-row batches are chunked. The record bound keeps a claim from wedging on an unreturnable record (C7); the index-key bound keeps PostgreSQL btree entries legal. The stage bound keeps the ordinal small and `stats` bounded; the metadata bound is D-G's. |
| D16 | `read` and `scan` are fresh; `aggregate` and `stats` may be torn (V3). Δ bounds only the time-driven transition. | S3×Turso already waits for coverage on every read (`turso_compose.rs:2035-2054, 2310-2334, 3567-3579`) and PostgreSQL is fresh by construction, so a stale-read allowance bought nothing, while Snorri's scan-then-write run mutations, input-ack lookup and await reads depend on freshness. |
| D17 | *Retired (round 2).* It made `Finish` with `expires_at: Set(None)` retain a record until `Delete`. | There is no `Finish` and no expiry: every record is retained until `Delete`. |
| D18 | `WholeGroups` group exclusivity spans stages: a group with a Leased member in any stage is skipped. It is a MUST (C6a). `Next`, `Keys` and `Where` do not check it. | It is today's predicate, unchanged (`fireweed-turso/src/projection.rs:829-831`). Every consumer that claims its grouped stages only with `WholeGroups` gets one holder per group across the queue, which is the fence replacement Snorri needs (D-K). A consumer that wants per-stage concurrency leaves the other stages' records ungrouped, as Snorri's timers and effects are (§8.1). |
| D19 | The built-in index keys are `Stage`, `Group`, `Due`, `Rank` and `StageSince`. Lease state, `leased_until` and `attempts` are not indexable; `Dim::Lease` reads the lease from the record. | Built-ins that change on claim or reclaim would put index maintenance on the claim path and in the reclaim sweep. `stats` already counts by stage and lease. |
| D20 | `due` is a `Timestamp`, and `due: None` is removed (§10 O-3). | Every by-key-only use has a stage form: data read by key lives in a terminal stage, and work claimed only by key lives in a stage that only `Keys` claims name. This removes the `not_before = i64::MAX` sentinel rows from the claim index (old risk 5) and the `due: None → Some` rule for `pending_since`, and `ready_since` is always defined. |
| D21 | A Leased record is never in a terminal stage: W1 rejects `Extend` with a patch that names a terminal stage. | It keeps "terminal means never claimable and always Free" true by construction, so C6 and the reclaim sweep never see terminal rows. |
| D22 | A definition has 1..=32 stages, at least one non-terminal. An insert must name its stage, and any declared stage is allowed, terminal included. | A queue with no claimable stage is a table, which the contract is not. An insert straight into a terminal stage is how a consumer writes data such as Snorri's instance item (§8.1). |
| D23 | Queue metadata is `String → Value`, versioned by a `u64` from 0, one op per request, read through `read{queue: true}`. | One op per request keeps units independent (W2), as unique keys do. `read` is the fresh read (V3), while `stats` may be torn, so CAS belongs on `read`. |
| D24 | `Keys` skip precedence is `NotFound`, `Terminal`, `OtherStage`, `Leased`, `Blocked`. | The most specific structural reason comes first, then the transient ones, so a caller can tell "wrong key" from "try again later". |
| D25 | A stage move keeps the insertion sequence and sets `stage_since` and `pending_since` to now. | The insertion sequence is fixed at insert (§4). Resetting `pending_since` makes S2's `oldest_ready_since` measure readiness within the stage. A consumer that wants stage-entry order sets `rank` on the move. |
| D26 | `Select::Group(g)` is removed; `Select` is `Next`, `WholeGroups`, `Keys` and `Where`. | No Snorri or 7S mapping uses a single-group claim, and it leased members of a group that another holder had claimed with `WholeGroups`, a hole in the exclusivity that D-K relies on. S9 deletes `select_same_group`. |

---

## 10. Open questions for the owner

The owner answered the previous round's questions (D-A–D-K). Old Q2 (retention), Q4 (cohorts), Q5 (existing data), Q6 (RESP) and Q7 (adding indexes) are settled. O-1–O-3 remain; O-4–O-6 are new consequences of the round-2 decisions, and O-7 comes from the round-2 verification.

1. **O-1. Snorri's instance item.** Is an item that is only ever written and read, and never claimed, acceptable?
   - Recommendation: yes, because an item is an item. The instance item `snorri:v1:instance:{id}` lives in terminal stage `instance`, so it is never claimable by construction. It is written in the same unit that settles the instance's `transition` inputs, or its `state_store` facade inputs. The `WholeGroups` claims on those two stages serialize those units, because group exclusivity spans stages (C6a), so no lease or fence is needed. Handler timeouts and dispatch outcomes write only their await and a continuation (§8.1). The unit's `Expect::Version(v)`, with v from the hydrating read a transition needs anyway, guards against out-of-band writers (migration, recovery, operator cancel) (§8.1).
   - Cost: no extra op per batch. The hydrating read is today's projection load, and the payload is rewritten once per transition, as today's side record is.
   - Alternative: keep the instance item in a non-terminal stage and `claim(Keys)` it per transition batch, with its lease as the lock (the round-1 design). Cost: one extra claim per batch on the parity lane (old risk 9), a lease and token per instance, and a second exclusion mechanism beside the group.
   - Rule change: the round-1 rule "a record nothing claims is a mapping defect" is replaced by "records in terminal stages are data; records in non-terminal stages are work" (§1).
   - *Recommendation: the terminal-stage instance item.*
2. **O-2. Rescheduling a record while it is claimed** (old Q3; Snorri's lease-preserving reschedule, `lib.rs:11237-11250`). Costs of each option:
   - (a) *Reject with `Leased`, counted as `conflicted`.* Zero engine cost. A reschedule that races a worker is lost until the caller retries.
   - (b) *Force: `Clear` + `Put{due}`.* Fences the worker, wasting its work and causing redelivery.
   - (c) *Allow schedule-only non-holder writes.* Relaxes the pending-only guard on the packed path, requires overlay tracking of claim versions, and rewrites API-001:347. Production-blocker risk.
   - *Recommendation: (a).*
3. **O-3. `due: None`.** The draft removes it (§9 D20): `due` is a `Timestamp`, and an insert that omits it gets now.
   - Each round-1 use has a stage form. Data read by key (Snorri's instance items and open awaits) lives in a terminal stage. Work claimed only by key lives in a stage that only `Keys` claims name, and scheduled claims of other stages never see it.
   - Cost of keeping it: `not_before = i64::MAX` sentinel rows in the claim-order index (old risk 5), a second way to say "claim by key only", and a special `pending_since` rule.
   - Cost of removing it: a consumer that wants by-key-only records inside a stage that also holds scheduled work must split the stage.
   - *Recommendation: confirm the removal.*
4. **O-4. No network front after RESP (new).** RESP was the only client protocol. `fireweed-server` binds it (`crates/fireweed-server/src/lib.rs:3273-3283`) and runs the reclaim tick beside it. With RESP dropped (D-D), the server has nothing to serve, and a consumer outside Rust, such as Seventh Sense (Scala), has no way to call the `Store`.
   - (a) Delete the server's front with RESP, and keep the crate as the composition root for a later front.
   - (b) Specify a replacement front over `Store` (for example gRPC) as a separate contract before the 7S cutover.
   - *Recommendation: (a) now, and (b) as its own contract when the 7S cutover is scheduled. The transport is outside this kernel contract.*
5. **O-5. A stage precondition for lease-free writes (new).** Removing `Rejection::Terminal` (D-I) removed the only check that stopped a lease-free write from reviving finished work. An upsert of a record in a terminal stage now replaces it and moves it (§6). Snorri's `create_work_items` refuses that today (§8.2), and a stage-conditional event ("move to b only if the record is still in a") needs the same check.
   - Today's mechanism: `read`, then `Expect::Version(v)`. One extra read, and a retry when anything else changed the record.
   - Option: `Expect::Stage(s)`, with a `WrongStage { current }` rejection. One more `Expect` variant and one more rejection for every implementation.
   - *Recommendation: defer. Read plus `Version` covers both consumers; add `Expect::Stage` if a hot path needs it.*
6. **O-6. Adding stages (new).** Seventh Sense's job types become stages of one queue (§8.3), and 7S adds job types in ordinary releases. The definition is immutable (D-F), so until the append-only add-stage migration op (§3.1) exists, a new type needs a new queue.
   - *Recommendation: specify the append-only add-stage op before the 7S cutover. Until then, 7S declares every known type at creation, and a new type gets its own queue.* Each interim single-type queue needs its own status gate op and its own `deleteByJobIds`/`deleteByConnectorIds` writes (§8.3). One queue per type also stays an allowed mapping on its own merits (§8.3 trade-off).
7. **O-7. Queue deletion for per-entity queues (new, round-2 verification).** 7S maps one Marketo queue per connector and one callbacks queue per job (§8.3). Postgres cascades connector and job deletion to those rows today; the kernel mapping deletes the records page by page, but D3 has no queue delete, so every deleted job leaves a `callbacks:{job}` definition, metadata and per-stage counter rows behind forever.
   - (a) Add a queue drop that succeeds only on a queue with no records, and is otherwise `Invalid`.
   - (b) Accept the catalog leak.
   - (c) Map callbacks to one queue with the job id in the key and indexes, trading the per-job queue's isolation for a bounded catalog.
   - *Recommendation: (a) before the 7S cutover. It does not touch definition immutability (D-F), since a dropped queue has no records to strand.*

---

## 11. Judge defects: resolutions

> History. §11–§13 record the rounds before the owner's round-2 decisions, and their clause, question and decision references use the numbering of their time (for example §10 Q1–Q7, W6, D17 and `Finish`). §14 lists what round 2 changed.

| Lens | Design | Sev | Defect | Resolution |
|---|---|---|---|---|
| all | A/B/C | blocker/major | Idle / Parked / never-claimed side-record replacements | Removed. Data goes on settled input, instance, await and continuation items in the same unit (§8.1). The instance item is a work item claimed by key per transition batch; Q1 confirms its cost and lists the one remaining store-only candidate, the shard manifest. |
| coverage | all | major | Per-instance projection vs on-item storage not argued | §8.1 plus Q1, with costs. |
| coverage | A | major | Cohorts deleted without a linearizable completion | Q4 option 2: `WholeGroups{1}` returns a batch whole, and completeness is `claimed.len() == batchChecksum` at claim, with no counter item and no gate; option 1 is costed. |
| coverage/production | A, B | major | Insert rejected on group size (W10/P8) | W10 forbids it; D5. The judges' "claim-time today" premise is wrong (§7.1), but the resolution is adopted. S4 also deletes the ungrouped-item rejection and the claim-time `BatchTooLarge` check (§7.2). |
| coverage | B | major | No lease-free atomic multi-record write | Units need no lease (W2). |
| coverage | B, C | major | Leased reschedule dropped | Resolution rule applied: D2; Snorri mapping plus Q3 with costs. |
| coverage | B | major | No `unique` index | Deviation: D7. Snorri's production `by_run_target_key` (`lib.rs:16865-16874`) moves into its item key (S11). |
| coverage/prod/pg/scope | C | major | `hasNextTasks` as claim-then-requeue | Bounded read: `stats.groups` (S2) over a new un-gated `earliest_ready_at` structure (§7.2). |
| production | A | blocker | W11: non-holder writes to leased records | D2 / W4 `None` gives `Leased`. |
| production | A | blocker | W9: position-derived versions | D4: `item_version` +1 kept; incarnation from `item_id`. |
| production | A | major | Mandatory claim `request_id` | D3: optional. Write ids are optional too. |
| production | A, B, C | major | Sparse fields not on the set-based path; typed rekey | §7.2: NULL-safe `json_patch` on the `fields` TEXT column, on the general set-based tier; batched rekey; 22-index P2p/P3p gates. |
| production | A | major | Complete becomes writable Idle, with a new unnamed purge tick | D1 keeps terminal states. The tick is explicit in S7, with gate P5. |
| production | B | major | `due_groups` is O(groups) | `LIMIT n` over a new un-gated per-group index (S2, §7.2). |
| production/pg | B, C | major | Expired leases "eligible on read" with no ticker | V4 within Δ on the injected `Clock`; the constructor owns the tick, and every claim runs the reclaim sweep once Δ has passed (S7). |
| production | C | major | Settle-with-patch on the exclusive lane | P route: a held `UpdateFieldsBatch` on the general set-based tier plus `Finalize{Release\|Retry\|Complete\|Fail}` in one member, never `Rearm`; P4p/P4r gates. |
| production | C | major | Fields in the postcard blob | Fields in `TEXT` JSON (`schema.rs:29`); `index_fields` retired. |
| postgres | A | major | Lease-expiry version effect unspecified | D4: expiry never changes the version; Turso `LeaseExpired` no longer bumps it. |
| postgres | A | major | Token "MAY" be rejected after expiry | W4: MUST reject once `leased_until <= now`. |
| postgres | A | major | Serializable exact-prefix claims vs SKIP LOCKED | C5: ordered results, MAY omit (whole groups only for `WholeGroups`, never for `Keys`); C5a requires progress when the queue is quiescent; V1 is per record. |
| postgres | B | major | "At least one" result; "Postgres: S = 0" | Dropped as per-backend clauses; one Δ for time-driven transitions and one torn-read adversary (§4). C5a and C7 now give both backends the same progress rules. |
| postgres | B | major | Undefined `eligible_since` | `pending_since` and `ready_since` defined in §1 (tightened in the verification pass). |
| postgres | B, C | major | Total-order or internal-sequence feed hook | Only per-key increasing `version`. |
| postgres | B | major | Purge reserves keys, dropping 7S re-creates | No tombstones (§1). |
| postgres | C | major | Cohort deadline is an unbounded time transition | Q4 recommends deletion; if kept, it is stated "within Δ". |
| scope | A | major | No docs, test-port or AGENTS.md slice | S3, S10. |
| scope | B | major | False cite `ItemState::Idle` (`domain.rs:1453`) | No Idle state exists or is added. |
| scope | B | major | Parallel slices share `turso_compose.rs` | S0 splits it first. |
| scope | B | major | Three mutation verbs, 9 knobs, `fields_equal` | One `write`; two `QueueDef` settings (assuming Q4 option 2); claims only by declared index or key. |
| scope | C | major | Undefined DTOs | All types are defined in §2; the shared types (`Record`, `State`, `Value` order, `Cursor`, `LeaseToken`, `RequestId`, `QueueKey`, `Clock`) are in §2.0. |
| scope | C | major | Duplicate mechanisms (Outcome read, pause vs gate, `Group(None)`) | D8; pause is `"*"`; `Group(None)` dropped. |

**Minor defects adopted:**
- `created` flag (coverage A)
- `Keys` skip reasons (A)
- `GateEdit` add/remove (A)
- key ≤ 1024 bytes (A)
- `Expect::Exists` (scope A)
- `Filter::In` dropped (pg A)
- ordered aggregate truncation (pg A)
- `TimeBucket.width_ms` (pg A)
- defined no-op delete outcome (pg A)
- no deletion version output (scope A)
- `Busy` classed as not-applied (pg B)
- no TD-011 wording in the PostgreSQL mapping (pg B)
- stable token on `Extend` (coverage C)
- oversized-group signal via `partial_group` (coverage C)
- claim replay returns `RequestExpired` once a lease has lapsed (scope C)
- self-describing tokens (pg C)
- blocking lock for PostgreSQL `Keys` (pg C)
- `due` is an explicit timestamp, with no `Due::Now` (pg C)
- one queue per Marketo connector (coverage A)
- `findByJobId` becomes `scan` + `read` (coverage B/C)
- `Version` stored beside `projection_revision` (coverage B); superseded in the verification pass, since the instance item's lease is now the lock (§8.1)

**Minor defects rejected, with evidence:**
- B's `gates: bool` flag: the anti-join is already skipped when no gate is blocked (`fireweed-turso/src/projection.rs:1997-2012`). Always-on gates cost one queue-prefix probe per claim (`:1989-1996`), which the P3/P4 gates measure.
- Scope's "perf gate must be the exact evidence number": the binding rule sets −5% as the noise margin.

---

## 12. Verification-pass revisions

Three verifier lenses checked this draft against the code and the consumers: citations (11 findings), consistency (22) and compliance (18). Every fix was adopted unless the lead engineer's adjudication overrode it; "adj. N" names the adjudication item. None was rejected. Clause IDs are unchanged; new clauses are W2a, C5a, V5, D16 and D17, and R2 and D11 are marked removed. "§2.1 D*n*" is a `define_queue` clause and "§9 D*n*" a decision; "§2.7 Q4" is an `aggregate` clause and "§10 Q*n*" an owner question.

| # | Lens | Section | Sev | Resolution |
|---|---|---|---|---|
| 1 | citations | §7.2 update and settle rows | major | Adopted. The two `UpdateFields` tiers are described with their real exclusions (§7.1, §7.2). Field patches go on the general tier; CAS and gate edits route there and lose the fast path (P2p/P3p measure it); "new held mode" deleted. |
| 2 | citations | §7.2 `stats`; §9 D13 | major | Adopted. D13 rationale rewritten. `stats.groups` uses a separate un-gated structure maintained on every Pending-membership change. Claim → stats → complete → stats test added to S6 and S2. |
| 3 | citations | §7.2 queue mapping | major | Adopted. `max_eligible_group_size = None`; S4 deletes `async_composed.rs:1718-1722` and the `claim_validation.rs:77-84` requirement and `BatchTooLarge`. |
| 4 | citations | §7.2 settle row and queue mapping | major | Adopted. `{Release, Put{due}}` → `Finalize{Retry}` or held `UpdateFields` + `Finalize{Release}`; never `Rearm`; `recurrence = Recurring` removed (§3, §6, §7.2); `attempts` scenario in S2. |
| 5 | citations | §7.2 acked-update-vs-claim | major | Adopted. Recast as [B] work in S4/S7 with the three required changes, the interleaving planner test and the P2/P4 cost (risk 8). |
| 6 | citations | §9 D7; §11 | major | Adopted. D7 cites Snorri's production unique index; the §11 row no longer says "tests"; the key change is in S11, §8.2 and risk 7. |
| 7 | citations | §3 gates row; §11 rejected minors | minor | Adopted. The `claim.rs:99-113` citation is dropped; the always-on probe cost is stated in §3, §7.2 and gate 1. |
| 8 | citations | §7.1; §9 D3 | minor | Adopted. The receipt write is cited at `turso_compose.rs:4486-4500`, the replay read at `:5400-5420`. |
| 9 | citations | §7.2 settle row | minor | Adopted. Rationale reworded; the comment is cited at its current lines, `turso_compose.rs:4814-4816`. |
| 10 | citations | §7.2 update row | minor | Adopted. NULL-safe `CASE WHEN v.patch IS NULL …`, or bind `'{}'`. |
| 11 | citations | §3 gates row | minor | Adopted. `max_gates_per_request` described as distinct item gate keys per push (Snorri: 10,000) and removed; the adapter maps it to `None`. |
| 12 | consistency | §2.2 W4, W6 | major | Adopted. W6: a record that is not Leased requires `None` or `Clear`. W4: `Clear` on a record that is not Leased is a no-op. `force_finish` and Snorri's CancelRun are now valid in the V4.1 window. |
| 13 | consistency | §2.2 W2a; §2.3 C2 | major | Adopted as W2a, without `Hold` (adj. 4). C2 gains the skip precedence `NotFound`, `Terminal`, `Leased`, `Blocked`. |
| 14 | consistency | §2.3 C2 | major | Adopted. `max_records` ≥ distinct keys, else `Invalid`; exact classification; `Skip::NotAttempted`. |
| 15 | consistency | §2.3 C5 | major | Adopted. C5a (progress) added; `WholeGroups` may omit only whole groups (C4, C5). |
| 16 | consistency | §4 Ordering | major | Adopted. Insertion sequence is a per-queue total order; §7.2 insert row and §7.3 assign it by unit order, then write order. |
| 17 | consistency | §1; §2.5 S2 | major | Adopted. `pending_since` rules replaced; `due = None` is never counted; §7.2 requires no-reset `UpdateFields` emission and reclaim at `leased_until`. |
| 18 | consistency | §2.5 S1 | major | Adopted. Counts cover every undeleted record. |
| 19 | consistency | §1 Expiry; §2.2 W6 | major | Adopted, with the `Set(None)` meaning from #36 (adj. 5): `Set(Some(t))` → t; `Keep` → `finished_at + terminal_retention`, replacing any stored value; `Set(None)` retains until `Delete` (not `Invalid`). |
| 20 | consistency | §4 Limits; C4; C7 | major | Adopted. Record ≤ 4 MiB (W8); C7 first-candidate rule; `partial_group` also at the byte limit. |
| 21 | consistency | §4 Limits; W1 | major | Adopted. Encoded index key ≤ 1024 B. W1 rejects a value that alone exceeds it; W8 rejects per record a key that exceeds it together with stored fields, since a sparse patch cannot be checked at the envelope. |
| 22 | consistency | §4 Δ; §2.0; §7.4 S2 | major | Adopted. `Clock` trait in `fireweed-contract` (§2.0); suite inputs `(constructor, reopen, Δ, Clock, fault hook)`. |
| 23 | consistency | §2.3 C4 | minor | Adopted. |
| 24 | consistency | §2.2 W1, W9 | minor | Adopted. Empty-request rules and overlapping `GateEdit` in W1; unset gate is Open in W9. |
| 25 | consistency | §2.1 D2, D4 | minor | Adopted. |
| 26 | consistency | §4 V4 | minor | Adopted. V4.2 uses the later of `expires_at` and the end of the lease; request-id retention moved to V5. |
| 27 | consistency | §5 operator helpers | minor | `force_release` adopted as `{Exists, Clear, Put{due}}`. The `redrive` part is moot: the helper is removed (adj. 4, #48). |
| 28 | consistency | §2.5 S2; §9 D13 | minor | Adopted. |
| 29 | consistency | §2.2 W4 | minor | Adopted. |
| 30 | consistency | §2.3 C8 | minor | Adopted. |
| 31 | consistency | §2 types; §2.7 Q4; §11 | minor | Adopted. §2.0 defines `Record`, `State`, `Value` order, `Cursor`, `LeaseToken`, `RequestId`, `QueueKey` and `Clock`; `Ranges` null bucket; `State` keys; `Dim` fields must be index fields. Because of that rule, the 7S `by_stats` index carries the stats dimensions (§8.3). |
| 32 | consistency | §4 Δ vs §7.3 | minor | Adopted. PostgreSQL treats lapsed leases as Pending everywhere; Δ is its sweep interval, and the sweep also materializes lapsed leases into `group_summary`. |
| 33 | consistency | §0; §3; §4 V4 | minor | Adopted. |
| 34 | compliance | §0, §1, §8.1, §8.2, §10 Q1/Q4, §11 | blocker | Adopted per adj. 1. The instance item is a work item claimed by key, and its lease is the instance lock. Q1 is a confirmation with the cost. Replay markers → request-id replay; migration/recovery audit → the instance item's settle patch; the shard manifest is listed under Q1. Q4's counter item is replaced by the #38 mapping. |
| 35 | compliance | §4 V3; §2.4–§2.7; §8 | major | Adopted per adj. 2. V3 is fresh for `read`/`scan` and torn-allowed for `aggregate`/`stats`; Δ applies only to V4; the lag wrapper is replaced by a torn-read adversary; no "weaker model" wording remains. Recorded as §9 D16. |
| 36 | compliance | §1; §2.2 W6; §8.1 await row; §10 Q2 | major | Adopted. `Finish` with `Set(None)` retains until `Delete` (§9 D17); consumed awaits use it. |
| 37 | compliance | §8.3; §2.2 W5 | major | Adopted. NULL next time → `asOf`/now; create `rank = now_µs`; W5 insert default `due: Some(now)`. |
| 38 | compliance | §8.3 callbacks; §10 Q4; C4 | major | Adopted. Two small additions: `by_single` also carries `state_ordinal` to keep 7S's Scheduled-before-Failed order, and `by_stats` carries the dimensions (#31). |
| 39 | compliance | §8.2 `mutate_items` | major | Adopted. Global gates plus per-item gate membership; paged, not atomic. |
| 40 | compliance | §8.1; §8.2 | major | Adopted. `by_tx_request` index; recovery is `scan` then `read(Keys)`; counted in gate 4. |
| 41 | compliance | §3; §8.1; §8.2 | major | Adopted. Snorri `terminal_retention` = 24 h; P5s variant (22 indexes, 24 h, advanced `Clock`). |
| 42 | compliance | §6 upsert; §8.2 upsert | minor | Adopted. |
| 43 | compliance | §8.3 | minor | Adopted. Gate fan-out from the status path only; legacy JobContentLoader migration; `define_queue` per connector; `asOf`-based `hasNextTasks`; EmailEnrichment mapped; sparse callback `Put`, `by_job`, per-request dedupe. |
| 44 | compliance | §2.2; §4; §9 D3; §7.5 risk 1 | minor | Adopted per adj. 3. The write `request_id` is optional; id-less writes resolve by `read(Keys)` (D8). |
| 45 | compliance | §2.2 `Hold`; §2.4; §6 | minor | Adopted per adj. 4. `Hold` dropped (`Extend` + `Put` covers holder patches) and removed from route P; `read` is `Keys` only; `side_records_by_prefix` → `scan`; `retained_items` deleted. |
| 46 | compliance | §6; §3; §7.4 S9 | minor | Adopted per adj. 4. `current_position` deleted, neither moved to `TursoAdmin` nor to test support; `change_record_sink.rs` and its config plumbing added to S9. |
| 47 | compliance | §9 D7 | minor | Adopted with #6. |
| 48 | compliance | §5; §9 D11 | minor | Adopted per adj. 4. `redrive` removed from the helpers; D11 marked removed. |
| 49 | compliance | §7.3 `stats` | minor | Adopted. |
| 50 | compliance | §9 D12; §4 V4; §8.2 | minor | Adopted. |
| 51 | compliance | §8.1; §7.2 gates 2 and 4 | minor | Adopted. P4p and gate 4 are sized with Snorri's measured bytes; `next_state` and `audit` go in the input item's `payload`. |

## 13. Check-pass revisions

A second reviewer checked the §12 revisions and found 18 new text-level problems (N1–N18). All were fixed:

| # | Sev | Problem | Fix |
|---|---|---|---|
| N1 | major | C5a contradicted the S3×Turso acked-update exclusion | C5a quiescence is "no op in flight during the last Δ". §7.2: the exclusion clears within Δ, otherwise the claim waits for coverage. |
| N2 | major | `pending_since` = now on Release had no S3×Turso implementation | §7.2 settle row, S5 and S2: Release/Clear set `eligible_since = now`, Retry sets `max(not_before, now)`. |
| N3 | major | 7S queued callbacks were scan-consumed (never claimed) | `findByJobId` = `claim(Where{by_job})`; `deleteByIds` = `{Release(t), Delete}`; `createOrUpdate` retries on `Leased`. |
| N4 | major | "Static blocks" via `due: None` reintroduced parking | FR-45 and the `metadata_blockers` row: caller gates only. |
| N5 | major | Finish with `Keep` overwrote the callback create-time expiry | Callback settles carry an explicit `expires_at`. |
| N8 | major | One-shot Snorri workflows could not follow the instance row | §8.1: only the start input inserts. Q1(c) options, recommending a W2a insert-finished branch. |
| N6 | minor | The `DISTINCT … LIMIT` alternative broke S2's cost bound | Removed. |
| N7 | minor | The PostgreSQL btree budget ignored unbounded names | Tenant, queue and index names are 1..=128 B (§4, D4, `QueueKey`). |
| N9 | minor | The V3/D16 rationale cited the removed CAS reload loop | Now cites the scan-then-write mutations and the ack lookup. |
| N10 | minor | Scan-driven writes used `Version(v)` from version-less hits | `Exists` plus a commutative gate `Edit`; `read(Keys)` before a versioned delete. |
| N11 | minor | 19 vs 22 indexes | 22 throughout. |
| N12 | minor | C8 replays `skipped`/`partial_group`, which the receipt does not store | [B]: extend the `BatchClaim` receipt. |
| N13 | minor | The PostgreSQL claim SQL used `now()` | Bind `$now` from `Clock::now()`. |
| N14 | minor | `Ranges.field` was not required to be an index field | Q4 requires it. |
| N15 | minor | The input settle discarded the original payload silently | Stated explicitly as intended. |
| N16 | minor | Q2 omitted instance-item expiry and outstanding awaits | Q2 bullets added; the final transition deletes outstanding awaits. |
| N17 | minor | §3 vs Q4 wording of the cohort option | Aligned to a `cohort_size` insert attribute. |
| N18 | minor | The upsert mappings dropped `group` | `group: Set(g)` added. |

---

## 14. Round-2 revisions

The owner answered the previous draft's §10 questions and settled the stage model in the discussion that followed (§9.1, D-A–D-K). This round applies those decisions. §11–§13 are unchanged history.

| Section | What changed | Why |
|---|---|---|
| Preamble, §0 | Round-2 summary; op purposes restated for stages and metadata; still 7 ops, now without the conditional cohort count. | D-A–D-K |
| §1 | `stage` plus `lease` replace `state`; `stage_since` replaces `finished_at`; `expires_at`, the Expiry subsection and `due: None` are gone; a new two-axis lifecycle diagram; "terminal-stage records are data, non-terminal records are work" replaces "a record nothing claims is a mapping defect". | D-A, D-H, D-I, O-1, O-3 |
| §2 | New `Stage`, `Lease` and `Attr` types; `QueueDef` declares stages, and indexes take built-in keys; `Op::Finish`, `Outcome`, `WriteOutcome::Finished`, `Patch.expires_at` and `Rejection::Terminal` removed; `Patch.stage` added; a metadata op in units (W13); every claim names a stage (C1), with `Skip::Terminal` and `Skip::OtherStage` for `Keys` (C2); `WholeGroups` exclusivity spans stages (C4); `read{queue}` returns the definition and metadata (R4); `stats` counts by stage and lease, and its group threshold names a stage; `Dim::Lease` replaces `GroupBy::State`. | D-E–D-J |
| §3 | Split into the definition (stages and indexes, with the reasons it is immutable) and queue metadata; the retention rows are removed and the cohort row is settled. | D-A, D-B, D-E–D-G |
| §4 | V4 has one time-driven transition; metadata joins V1, V3 and atomicity; stage and metadata limits; an ordering note for stage moves. | D-A, D-G, D-H |
| §5 | A plain-English "claim versus write" with the owner's rule of thumb; operator helpers are stage moves; `redrive` restored. | D-I, D-J |
| §6 | `queue_definition` → `read{queue}`; finish and fail become stage moves; `operator_redrive` kept as a helper; the cohort surface and the RESP front deleted. | D-B, D-D, D-F, D-G |
| §7 | Stage column and stage-prefixed claim-order and group-head indexes; a finish rides `Finalize{Complete}` carrying the stage (a terminal stage is today's `Complete` row internally); a redrive apply arm; per-stage counters; a `SetQueueMetadata` command; built-in index keys; the expiry sweep, perf gate 5 (P5/P5s) and risks 4, 5 and 9 retired; risks 10–12 added; slices and the PostgreSQL mapping updated. | D-A, D-C, D-H, D-I |
| §8 | Snorri: stages for record kinds, `WholeGroups` over instance groups instead of the fence and its four retry loops, the instance item in a terminal stage, awaits read by key with a version CAS, the manifest in queue metadata, and no retention settings. Seventh Sense: job types, the Marketo pipeline and the callback pipeline become stages; no `expires_at`. | D-A, D-K, O-1 |
| §9 | Owner decisions listed (§9.1); D1, D6, D8, D10–D16 revised; D17 retired; D18–D25 added. | — |
| §10 | Reduced to O-1–O-3, plus three new questions (O-4–O-6) that follow from the round-2 decisions. | Owner answers |

**Identifier changes.**
- Retired, not reused: W6 (`Finish`), V4.2 (record expiry), §9 D17, risks 4, 5 and 9, and perf gate 5.
- New: W13, R4, §9 D18–D25, and O-4–O-6.
- Questions are renamed from Q1–Q7 to O-1–O-6: old Q1 (instance lock) became O-1 and old Q3 (leased reschedule) became O-2; old Q2, Q4, Q5, Q6 and Q7 are answered.
- Every surviving clause keeps its ID.
- Round-2 verification: new C6a, §9 D26, O-7 and risk 13; `Select::Group` is removed.

### Round-2 verification

Three verifier lenses checked the round-2 draft against the owner's decisions, the code and the consumers: consistency (9 findings), production (15) and coverage (12). Every code and consumer claim a resolution relies on was re-read in this worktree, in Snorri (`crates/snorri-fireweed/src/lib.rs`) and in Seventh Sense. Where two findings touched one passage they were merged, choosing the option most faithful to the owner's decisions and to production cost. The owner's round-3 answers (R3-1 and R3-2 resolved, R3-3 and R3-4 pending) are folded in by a later pass; here they only decide conflicts, and one fix changed because of R3-2: once a record is claimed, only its holder writes it until the lease ends. No finding was rejected outright.

| # | Lens | Section | Sev | Resolution |
|---|---|---|---|---|
| V1 | consistency | §7.2 `write` update row; §2.2 W5 | major | Adopted, first option. The apply derives `lifecycle_state` from the target stage and the lease, so a held `{Extend, Put{stage: Set(b)}}` keeps the row Leased with its token and expiry, and W5 says so. The simpler alternative, forbidding a stage change with `Extend`, is not taken: D-H lets any write change the stage. The proof goes to S2 (a concurrent `claim(stage: b)` returns nothing until release) and S5 (the apply), not S4, whose planner tests cannot run a claim. |
| V2 | consistency | §5 `redrive`; §3.3; §1 `attempts` | major | Adopted. `redrive` writes `attempts_base`, consumers compare `attempts − attempts_base`, and the change from today's `RetryCountMode::Reset` default (`operator.rs:280-285`, verified) is stated. |
| V3 | consistency | §1 `group`; §8.1 `transition`; §8.2 claim rows | minor | Adopted, merged with V28 (the claim rule) and V36 (no `Group` select). |
| V4 | consistency | §2.2 W8 | minor | Adopted. |
| V5 | consistency | §2.7 Q4 | minor | Adopted. |
| V6 | consistency | Preamble | minor | Adopted. |
| V7 | consistency | §4 V2, V3 | minor | Adopted. |
| V8 | consistency | §8.3 callbacks rows | minor | (1) Superseded by V29's fuller settle, which also writes `state_ordinal`. (2) Adopted, plus `due: Set(now)`: a record moved back to `queued` would otherwise keep its scheduled `due` and stay invisible to `findByJobId`'s claim until then. (3) Adopted: `by_single (Stage, single, state_ordinal, executed)`. |
| V9 | consistency | §8.3 jobs and connectors; §10 O-6 | minor | Adopted, merged with V33 and V24. |
| V10 | production | §7.2 `write` update row; S2, S5 | major | Adopted; every cited predicate verified. S2 covers the write, the read and `reopen`. The `projection_control()` rebuild half goes to S5's live == rebuild proof, because S2 is backend-agnostic and `projection_control` belongs to S3×Turso's `TursoAdmin`. |
| V11 | production | §7.2 `claim Next`; S6, S7 | major | Adopted. The single-SELECT realize and the exclusive fallback were verified (`turso_compose.rs:4416-4450, 4575-4581`; `local.rs:803-833`). |
| V12 | production | §7.2 `stats`; risk 13; gate 1; S5 | major | Adopted. Risk 13 states the cost shape: interleaved groups mean one 1,000-item claim can touch 1,000 groups. |
| V13 | production | §7.2 stage storage; S4.1, S6, S7 | major | Adopted. One detail is written as a requirement rather than asserted: a lease-free stage move in the unapplied tail is either retained with its stages or makes `stats` fall back to the coverage wait (`turso_compose.rs:3852-3857`). |
| V14 | production | §4 V4; §7.2 Lease reclaim; S7 | major | Adopted. A claim only signals the tick, the tick and its retry sleeps run on the `Clock`, and the source-audit assertion stays. §6, §8.2 and §9 D12 follow. |
| V15 | production | §7.2 stage storage and `claim WholeGroups`; S5, S6 | major | Adopted, the recommended option: the group-head index becomes `(…, group_key, stage, …)`, `fireweed_group_summary` stays keyed by group as a prefilter, and `select_group_batching` adds stage predicates. The alternative, a summary keyed by stage, is not taken: it rekeys the summary on every stage move on the apply path, and the claim already ranks groups from item rows, not from the summary (`projection.rs:800-884`, verified). A stage-blind reseek walks one group's Pending rows, off the Turso claim path. |
| V16 | production | §7.2 `write` update row; S4, S5, S6; gate 2; risk 3 | major | Adopted. |
| V17 | production | §7.2 `write` update row; gate 1; risk 3 | major | Adopted. The ported P2 is `{Exists, None, Put{payload, fields: {phase}}}` on the operation-shaped tier. With V10 that tier accepts `Complete` rows but never Leased ones (D2). |
| V18 | production | §7.2 queue mapping; S4.1 | minor | Adopted. |
| V19 | production | §7.2 route X | minor | Adopted. It becomes moot if the owner accepts the pending round-3 recommendation (R3-3) to drop `Clear`. |
| V20 | production | §7.2 metadata row; S4, S7 | minor | Adopted. |
| V21 | production | §7.2 queue mapping; S9 | minor | Adopted, first option: explicit `not_before` and `priority` on every kernel insert, and S9 deletes the FIFO scan. Verified that gate 1 does not ride the FIFO scan today: the phased-capacity pushes set both (`ss_phased_capacity.rs:151-158`). |
| V22 | production | §7.2 routing; S4, S6 | minor | Adopted. |
| V23 | production | §7.2 settle row; risk 10; S5 | minor | Adopted. |
| V24 | production | §8.3 jobs row | minor | Adopted, merged with V9. |
| V25 | coverage | §8.3 Marketo email and `create` | major | Adopted. The one-email batch and the worker's `head` were verified in 7S. |
| V26 | coverage | §8.1 shard routing, await row, delivery; §8.2 `create_queue` | major | Adopted. It corrects one premise: `by_record_kind_key_instance` is not among Snorri's 19 production indexes but only in the operator-activated SS1a variant (`lib.rs:16903`). So the kernel queue keeps 22 indexes, two of them `Stage`-keyed, not three as the previous draft said (gate 2, risk 10 and §8.2 corrected). Added: a key open in two shards fails the delivery as ambiguous, and S11 decides the key scope. |
| V27 | coverage | §8.1 other write paths and stage table; §8.2; §10 O-1 | major | Adopted. The facade commit, handler timeouts and dispatch outcomes are all mapped, so the fence-removal claims need no qualification. `await.dead` added. |
| V28 | coverage | §8.1 claim rule; §8.2 claim rows | major | Adopted, merged with V3. |
| V29 | coverage | §8.3 `calculateStats` and settles | major | Adopted. |
| V30 | coverage | §8.3 retry settle | minor | Adopted. |
| V31 | coverage | §8.3 callbacks | minor | Adopted. The "outside a claim" row is deleted: its only caller, `CallbackScheduledActionCreator.create/update`, runs inside the scheduler's and the executor's claims (verified). `deleteExpiredByJobId` takes the first option: skip `queued`, `read`, then `{Version(v), None, Delete}`. |
| V32 | coverage | §8.3 connector and job deletion; §10 O-7 | minor | Adopted with one change. Deletion uses `{Any, None, Delete}` and retries `Leased` hits instead of `{Any, Clear, Delete}`, because the owner's round-3 answer R3-2 makes the holder the only writer of a Leased record. O-7 added. |
| V33 | coverage | §8.3 jobs and connectors | minor | Adopted, merged with V9. |
| V34 | coverage | §2.3 C6a; §7.3 `claim` | minor | Adopted. |
| V35 | coverage | §8.1 stage table; §8.2 metrics and CancelRun | minor | Adopted. |
| V36 | coverage | §2.3 `Select`; §0; §6; §7.2; §9 D26 | minor | Adopted. |
