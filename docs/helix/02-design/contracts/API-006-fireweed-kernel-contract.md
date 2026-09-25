---
ddx:
  id: api-fireweed-kernel-contract
  depends_on:
    - external-interface-kernel-contract-draft
    - spike-record-key-storage
    - spike-record-field-storage
    - spike-claim-path-at-scale
    - spike-turso-value-size-boundary
    - spike-declared-field-storage-layout
  status: accepted
---

# API-006: Fireweed kernel contract

**Type**: normative kernel contract (Rust, in-process, transport-neutral)
**Version**: v1
**Status**: accepted
**Related**: API-001, API-002, API-003, API-004 and API-005 (this contract supersedes all five as of 2026-09-24); ADR-025 (the decision that adopts this contract, with its decision log); `external-interface-kernel-contract-draft` (design history and execution mapping); SPIKE-002 to SPIKE-006

## 1. Purpose and scope

This contract defines what Fireweed promises a consumer: the record model, the eight operations of the data-plane `Store` trait with their errors and idempotency, the queue definition, and the rules for durability, visibility, atomicity, ordering and leases that hold across all of them. Every implementation meets it exactly and passes one black-box conformance suite (§9).

- **In scope:** the types of the `fireweed-contract` crate, the `Store` trait, the separate `Operator` trait, the contract constants, and the inputs of the conformance suite.
- **Out of scope:** how an implementation stores, logs, places, snapshots or compacts data; the remote API; and everything listed in §8. The S3×Turso execution design lives in the technical designs and in the draft's execution mapping, not here.
- **Consumers adapt.** The contract is not bent to fit any consumer's current code. Snorri and Seventh Sense migrate to it (owner decision, 2026-09-23 design discussion).

All types live in the I/O-free crate `fireweed-contract`: the DTOs, the `Store` and `Operator` traits, the errors, the `Clock` trait, the constants (§6.8), validation, `fingerprint`, the canonical encoder that every byte limit is measured with (LIM1), the ESF index-key encoder and the order comparators. Every implementation uses the encoders verbatim. `Store` is object-safe, and the public `fireweed::Fireweed` wraps `Arc<dyn Store>` and delegates 1:1.

MUST, MUST NOT and MAY are used as in RFC 2119. Each clause has a stable id (for example W4) that the conformance suite cites. Retired ids (W6, R2) are never reused.

## 2. Overview

**Queues.** A queue is an independent, named collection of records, addressed by `(tenant, queue)`. Queues are cheap: a consumer uses one queue per job type. Each queue has an immutable *definition* (its stages, fields and indexes) and a small mutable *metadata* map that belongs to the consumer. A queue lives until it is *closed*, by archiving or deleting it.

**Records.** A record is addressed by a key that the upstream system chooses and keeps unique within the queue. The whole model is: add records (insert or upsert), claim them for work, update them, repeat. A record lives until the consumer deletes it or closes its queue; records never expire.

**Stages.** Every record is in exactly one of its queue's declared stages. An insert lands in the first stage unless it names another, and any write may move a record to any declared stage. No stage is special: there is no terminal flag and no Completed or Failed state. A stage that nothing claims is effectively "done", but the engine neither knows nor cares.

**Rank.** One `i64` per record orders claims within a stage and decides when the record becomes claimable. A claim takes records whose rank is at most the claim's *horizon* (by default, now in µs), lowest rank first; equal ranks go in insertion order. The default rank is the insertion time in µs, which gives FIFO. A future rank schedules a record, and a priority bucket is an offset added to the rank. Rank ordering is the product's core: it is simple, and it is fast (C9).

**Claims versus writes.**
- A **claim** walks one stage and takes an exclusive, crash-safe batch. You do not name records: the claim finds the next ones in rank order and leases them all under one token. The state change is the cursor. A claimed record is locked, so the next claim skips it. When you settle it, usually by moving it to another stage, it leaves this stage's walk. If you crash, the lease lapses and the records come back.
- A **write** addresses records by key. Without a token, it serves records you already know: an upsert, an external event that names its record, a correction, a stage move decided elsewhere. With the claim's token, it settles what you claimed. While a record is claimed only the holder writes it, until the holder releases it or the lease expires.
- Rule of thumb: if you are asking "what is next?", claim. If you already know the key, write.

**Fields.** Fields are declared when the queue is created, each with a type. A queue may declare many fields; values may be large (up to 64 KiB, for example a profile); most are rarely updated. Some fields are indexed, and the indexes are declared at creation too. Indexes serve `scan` and `aggregate`; claims are always in rank order (§8). The engine places values itself: there is no "large" flag and no separate payload.

**Gates and pause.** A record may carry up to 8 gate keys. While any of them is Blocked, the record is not claimable. The reserved gate `"*"` pauses the whole queue.

## 3. Record model

| Attribute | Type | Set by | Rule |
|---|---|---|---|
| `key` | `Key` | caller, at insert | Identity. Opaque bytes, 1..=256 B, compared bytewise, immutable. At most one record per key in a queue, in any stage. The upstream system owns keys and their uniqueness; a large natural key is hashed upstream. |
| `stage` | `Stage` | caller | One of the queue's declared stages. An insert that names none lands in the first declared stage. Any write may move the record to any declared stage. There is no transition graph. |
| `rank` | `i64` | caller | Claim order within a stage and the horizon test (C1). An insert that sets none gets the insertion time in µs on the implementation `Clock`. |
| `group` | `Option<GroupKey>` | caller, at insert only | The whole-group claim axis (C4, C6a). Immutable after insert (W8). |
| `gates` | `BTreeSet<GateKey>`, at most 8 | caller | The record is unclaimable while any carried gate is Blocked (W9). |
| `fields` | `BTreeMap<FieldName, Value>` | caller, sparse patch | Declared fields only, each of its declared type (F1–F3). |
| `version` | `Version` | engine | Strictly increases on every applied write to the record, every claim of it and every force-clear that detaches it. It never repeats for a key, including across delete and re-insert. Extend and lease reclaim do not change it. |
| `lease` | `Free \| Leased { until }` | engine | `Leased` iff the record is attached to a live claim (L2). The token is never readable. |
| `attempts` | `u32` | engine | +1 per claim of the record. Never reset; a re-insert after delete starts at 0. |

Internal attributes, never returned:
- the **insertion sequence** (ORD1), the tie-break after rank;
- **`free_since`**, the time the record last became a Free member of its current stage: at insert, on a stage move of a Free record, on release, on force-clear, and on reclaim (set to the expired `until`). Only S2 uses it, through `ready_since = max(rank, free_since)`.

`Timestamp` is UTC epoch microseconds (`i64`).

**Lifecycle.** A record moves on two independent axes. The stage belongs to the caller and the lease belongs to the engine.
```
STAGE (caller-owned; any move between declared stages is one write)

  insert ──► [ s0 ] ── Put{stage: s1} ──► [ s1 ] ── Put{stage: s2} ──► [ s2 ]
               ▲                                                         │
               └────────────────────── Put{stage: s0} ───────────────────┘
  s0 is the first declared stage and the insert default. No stage is terminal.

LEASE (engine-owned; one claim leases a batch under one token t)

  FREE ── claim(stage) ──► LEASED (attached to claim t)
   ▲                         │  write {Held(t), Put}        stays attached
   │                         │  Extend(t, d) in a unit      the whole claim is renewed
   ├── write {Release(t), Put | Delete} ◄──┤
   ├── claim t expires (within Δ) ◄────────┤
   └── Operator force-clear ◄──────────────┘

  delete: {None, Delete} on a Free record, or {Release(t), Delete} on a held one. The key is free at once.
```

## 4. The `Store` trait

### 4.0 Shared types, trait and errors

```rust
pub struct QueueKey { pub tenant: String, pub queue: String } // each 1..=128 B of non-blank UTF-8
pub struct Key(Bytes);             // opaque, 1..=256 B, compared bytewise
pub struct Stage(String);          // ^[A-Za-z0-9._:-]{1,128}$
pub struct FieldName(String);      // 1..=128 B of UTF-8
pub struct GroupKey(String);       // ^[A-Za-z0-9._:-]{1,256}$
pub struct GateKey(String);        // ^[A-Za-z0-9._:-]{1,256}$; "*" is reserved (W9)
pub struct RequestId(String);      // 1..=128 B
pub struct LeaseToken(Bytes);      // opaque, one per claim (L1); never readable from a Record
pub struct Cursor(Bytes);          // opaque keyset over (encoded index key, insertion sequence)
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)] pub struct Version(/* private */); // opaque; ordered per key only
pub type Timestamp = i64;          // UTC epoch µs
pub enum Lease { Free, Leased { until: Timestamp } }
pub enum FieldType { Int, Float, Bool, Time, Str, Bytes }
pub enum Value { Int(i64), Float(f64 /* finite */), Bool(bool), Time(Timestamp), Str(String), Bytes(Bytes) }
pub enum Attr { Field(FieldName), Stage, Rank, Group } // an index key, range key or aggregate dimension
pub struct Record {
    pub key: Key, pub version: Version, pub stage: Stage, pub rank: i64, pub group: Option<GroupKey>,
    pub gates: BTreeSet<GateKey>, pub fields: BTreeMap<FieldName, Value>, pub lease: Lease, pub attempts: u32,
}
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
    fn sleep_until(&self, t: Timestamp) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}
```
- `Version` is opaque. Its only meaning is its order among the versions of one key (§3, §6.7). The order between versions of different keys means nothing, and a consumer MUST NOT depend on it.
- `Value` order: within a type, the natural order (`Str` and `Bytes` bytewise, `Float` numeric); across types, declaration order. It matches the ESF encoded order. Every comparison the contract makes is between values of one declared type. In aggregate group tuples, `None` sorts first.
- A `Float` is finite, and `-0.0` is normalized to `+0.0` wherever a `Value` enters the contract (W1), so equal numbers are one value in storage, in `eq` bounds and in aggregate groups.
- Built-in attributes have fixed value types: `Stage` and `Group` are `Value::Str`, and `Rank` is `Value::Int`. Stage values compare by declaration position, not bytewise.

```rust
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;
pub trait Store: Send + Sync + 'static {
    fn define_queue<'a>(&'a self, q: &'a QueueKey, def: QueueDef) -> BoxFuture<'a, Defined>;
    fn write<'a>(&'a self, q: &'a QueueKey, req: WriteRequest) -> BoxFuture<'a, WriteResponse>;
    fn claim<'a>(&'a self, q: &'a QueueKey, req: ClaimRequest) -> BoxFuture<'a, ClaimResponse>;
    fn read<'a>(&'a self, q: &'a QueueKey, req: ReadRequest) -> BoxFuture<'a, ReadResponse>;
    fn stats<'a>(&'a self, q: &'a QueueKey, req: StatsRequest) -> BoxFuture<'a, Stats>;
    fn scan<'a>(&'a self, q: &'a QueueKey, req: ScanRequest) -> BoxFuture<'a, ScanPage>;
    fn aggregate<'a>(&'a self, q: &'a QueueKey, req: AggregateRequest) -> BoxFuture<'a, AggregateResponse>;
    fn delete_queue<'a>(&'a self, q: &'a QueueKey, req: DeleteQueueRequest) -> BoxFuture<'a, QueueDeleted>;
}
pub enum Error {
    QueueNotFound, QueueClosed, DefinitionConflict { existing: QueueDef }, Invalid(String), TooLarge,
    NotPermitted, RequestIdConflict, RequestExpired, Busy { retry_after: Duration }, Unavailable(String),
}
```

**Error classes.**
- **E1.** Definitely not applied: `QueueNotFound`, `QueueClosed`, `DefinitionConflict`, `Invalid`, `TooLarge`, `NotPermitted`, `RequestIdConflict`, `RequestExpired` and `Busy`. A request rejected with one of them records nothing.
- **E2.** Outcome unknown: `Unavailable` only. Resolve it by resending with the same `request_id` (RQ2), or, for an id-less request, by `read` of what it touched (RQ4).
- **E3.** Every op MAY return `Busy` or `Unavailable`. `TooLarge` is only for a request over 16 MiB encoded, or a `read` response over 16 MiB, both measured as LIM1 states; every other limit violation is `Invalid`.

### 4.1 `define_queue`

Why it exists: the stages, fields and indexes are fixed per queue before any record exists (§5).

```rust
pub struct QueueDef { pub stages: Vec<Stage>, pub fields: Vec<FieldDef>, pub indexes: Vec<IndexDef> }
pub struct FieldDef { pub name: FieldName, pub ty: FieldType }
pub struct IndexDef { pub name: String, pub keys: Vec<Attr> }
pub struct Defined { pub created: bool }
```

- **D1.** An absent queue MUST be created atomically and durably, with its normalized definition and empty metadata at version 0 (W13), returning `created: true`. Normalization: empty `stages` becomes `[INITIAL, COMPLETED]`; `fields` and `indexes` are sorted by name.
- **D2.** A definition that equals the stored one after normalization MUST return `created: false` with no effect. The order of `stages` and the order of keys within an index are significant.
- **D3.** A different definition MUST fail with `DefinitionConflict { existing }`, carrying the stored normalized definition, and have no effect. There is no alter: the definition is immutable (§5.1). Queue metadata is not part of the definition, and `define_queue` neither sets nor compares it.
- **D4.** The definition MUST be validated, else `Invalid`:
  - `stages`: empty (the default list), or 1..=32 unique names;
  - `fields`: at most 256, with unique names of 1..=128 B;
  - `indexes`: at most 32, with unique names of 1..=128 B; 1..=8 keys each; no key twice within one index; every `Attr::Field` names a declared field.
- **D5.** Every other op on a queue that was never defined MUST fail with `QueueNotFound`.
- **D6.** `define_queue` on a closed queue MUST fail with `QueueClosed` (DQ3).
- Errors: `DefinitionConflict`, `Invalid`, `QueueClosed`, and E3.
- Idempotency: naturally idempotent; it takes no `request_id`.

### 4.2 `write`

Why it exists: it is the single mutation verb. Insert, upsert, sparse update, stage moves, CAS, settle, claim renewal, delete, gate flips and the queue-metadata CAS are all writes, grouped into atomic units.

```rust
pub struct WriteRequest { pub request_id: Option<RequestId>, pub units: Vec<Unit> }
pub struct Unit {                                                   // atomic
    pub writes: Vec<RecordWrite>, pub extend: Vec<Extend>,
    pub gates: Vec<(GateKey, GateState)>, pub metadata: Option<MetadataOp>,
}
pub struct RecordWrite { pub key: Key, pub expect: Expect, pub lease: LeaseOp, pub op: Op }
pub enum Expect { Any, Absent, Exists, Version(Version) }
pub enum LeaseOp { None, Held(LeaseToken), Release(LeaseToken) }
pub enum Op { Put(Patch), Delete }
#[derive(Default)] pub struct Patch {
    pub stage: Option<Stage>, pub rank: Option<i64>, pub group: Option<GroupKey>, // None = keep
    pub gates: GateEdit, pub clear_fields: bool,
    pub fields: BTreeMap<FieldName, Option<Value>>,                   // Some = set, None = remove
}
#[derive(Default)] pub enum GateEdit { #[default] Keep, Replace(BTreeSet<GateKey>),
                                       Edit { add: BTreeSet<GateKey>, remove: BTreeSet<GateKey> } }
pub struct Extend { pub token: LeaseToken, pub lease: Duration }
pub enum GateState { Open, Blocked }
pub struct MetadataOp { pub expect: MetaExpect, pub patch: BTreeMap<String, Option<Value>> } // Some = set, None = remove
pub enum MetaExpect { Any, Version(u64) }
pub struct WriteResponse { pub units: Vec<UnitOutcome>, pub replayed: bool }
pub enum UnitOutcome {
    Applied { writes: Vec<WriteOutcome>, extended: Vec<Timestamp>, metadata_version: Option<u64> },
    Rejected { at: At, reason: Rejection },
}
pub enum At { Write(u32), Extend(u32), Metadata }
pub enum WriteOutcome { Put { version: Version, created: bool }, Deleted { existed: bool } }
pub enum Rejection { Exists { version: Version }, NotFound, VersionMismatch { current: Version },
                     Leased, StaleLease, MetadataVersionMismatch { current: u64 }, Invalid(String) }
```

- **W1. Envelope validation happens before any effect.** A failure is `Invalid` (or `TooLarge`, E3) and records nothing. Checked:
  - the §6.8 limits, with every byte bound measured as LIM1 states;
  - key, stage, group, gate and field-name syntax;
  - a patch that names an undeclared stage or an undeclared field, or a field value whose type differs from its declaration, or a non-finite `Float`, or a `Str` or `Bytes` value over 64 KiB (F1, F2). A `-0.0` is not an error: it is normalized to `+0.0` in every field value, metadata value and range bound;
  - a key that appears more than once anywhere in the request, a gate key more than once in the request's gate ops, or a token more than once in the request's extends;
  - more than one metadata op in the request;
  - the reserved gate `"*"` in any record's gates (it is legal only in `Unit.gates`);
  - `Delete` combined with `Held`;
  - an `Extend` duration outside 1 ms..=1 h;
  - an indexed field value whose ESF encoding alone makes an index key exceed 1024 B (a key that exceeds it only together with stored values is a per-record `Invalid`, W8);
  - a metadata entry name outside 1..=128 B, or a metadata patch whose canonical encoding (LIM1) alone exceeds 64 KiB;
  - a request with no units; a unit with no writes, no extends, no gate ops and no metadata op; a `GateEdit::Edit` whose `add` and `remove` overlap.

  A unit with only extends, gate ops or a metadata op is legal: a heartbeat, a pause, a manifest bootstrap.
- **W2. Units.**
  - Each unit is atomic: all its record writes, extends, gate ops and metadata op apply, or none do.
  - Keys are unique per request, each token is extended at most once per request, and there is at most one metadata op per request. So every unit's preconditions are evaluated against the state as of the request, and the accepted units then apply in request order. The one exception is an extend: it also sees the detaches of its claim's records made earlier in the same request (W2a).
  - A rejected unit reports where it failed (`At::Write(i)` for the first failing write, `At::Extend(i)`, or `At::Metadata`) and why, and has no effect on the other units.
  - The applied units of one request become durable together.
- **W2a. Evaluation order.** Each write is checked in this order, and the first failure is reported:
  1. `Expect` (W3).
  2. Absent key: `Put` gives `StaleLease` under `Held` or `Release`, and otherwise inserts (W5). `Delete` gives `Deleted { existed: false }` under any lease op.
  3. Lease op (W4): `Leased` or `StaleLease`.
  4. Patch validity (W8): `Invalid`.

  Once every write of the unit passes, its extends are checked (W14), then its metadata op (W13). An `Extend(t)` is checked against the state after the detaches (`Release(t)`, and `Delete` under `Release(t)`) of t's records made by the earlier applied units of the request and by its own unit's writes. If those detaches leave no record attached to t, the unit is rejected with `StaleLease` at `At::Extend(i)`. A detach in a later unit does not affect it: the extend applies, and the later unit may then end the claim (L4).
- **W3. `Expect`:** `Any` checks nothing. `Absent` requires no record, else `Exists { version }`. `Exists` requires a record, else `NotFound`. `Version(v)` requires the record at version v, else `NotFound` or `VersionMismatch { current }`.
- **W4. `LeaseOp`.** While a record is Leased, only its holder writes it (L3).
  - `None`: the caller holds no lease. If the record is Leased, the write is rejected with `Leased`.
  - `Held(t)` and `Release(t)`: the record MUST be attached to claim t, and t MUST be live by the implementation `Clock` (L2), else `StaleLease`. `Held` keeps the record attached, so a holder patches it and keeps its lease. `Release` detaches it: it is Free after the write, and the claim's other records stay attached.
- **W5. `Put(patch)`:**
  - On an absent key (`Expect` `Any` or `Absent`, lease op `None`): the record is inserted Free with the defaults (the first declared stage, `rank` = now in µs, no group, no gates, no fields, `attempts: 0`) and a new insertion sequence, then the patch is applied.
  - On an existing record, in any stage: only what the patch names changes. Under `Release` the record ends Free, under `Held` it stays attached, and under `None` it stays Free.
  - **Stage moves.** `stage: Some(s)`, with s different from the current stage, moves the record to s. Any declared stage may follow any other, and the insertion sequence is kept (ORD1). A held move (`{Held(t), Put{stage: s}}`) keeps the record Leased under t, so it is not a candidate of s until it is Free.
  - **Settling is a write.** Finishing, failing and retrying are stage moves or rank changes, usually with `Release(t)`; per-transition history travels in the same patch as fields. No stage is special.
- **W6.** Retired.
- **W7. `Delete`:** removes the record, in any stage, and detaches it from its claim. A Leased record requires `Release(t)`; `None` gives `Leased` (W4). An absent key with `Expect` `Any` or `Absent` gives `Deleted { existed: false }`. The key is free at once; a re-insert gets a version greater than any the key held (§3), a new insertion sequence and `attempts: 0`.
- **W8. Patch semantics:**
  - `clear_fields` empties the field map before `fields` applies. Fields the patch does not name are untouched; `None` removes a field.
  - `GateEdit` resolves against the stored set. A result with more than 8 gates is a per-record `Invalid`.
  - `group` is set only at insert. On an existing record, a `group` that differs from the stored one (a record with no group included) is a per-record `Invalid`.
  - A result whose encoded record (key and fields, LIM1) exceeds 4 MiB, or whose ESF-encoded key in any index exceeds 1024 B, is a per-record `Invalid`.
  - These checks use stored values, and MUST be decided before the acknowledgement.
- **W9. Gate ops:**
  - A gate op sets the queue-level state of a gate key. `"*"` is the queue pause gate, implicitly carried by every record.
  - A gate never set is Open.
  - Gate ops change no record, no version and no metadata, and apply atomically with the rest of their unit.
- **W10.** An insert MUST NOT be rejected because of its group's size.
- **W11.** Success MUST mean durable (DUR1).
- **W12.** A claim, `read` or `scan` issued after the acknowledgement MUST observe the write, its gate ops and its metadata op (V2, V3).
- **W13. Queue metadata op** (§5.2):
  - Metadata is a `String → Value` map with a `u64` version; a new queue has an empty map at version 0 (D1).
  - `MetaExpect::Version(v)` requires the current version to be v, else `MetadataVersionMismatch { current }`. `MetaExpect::Any` skips the check.
  - The patch is sparse: `Some` sets an entry, `None` removes it, and unnamed entries are untouched.
  - An applied op increments the version by 1 and reports it in `metadata_version`.
  - A result whose canonical encoding (LIM1) exceeds 64 KiB rejects the unit with `Invalid`.
  - The engine never interprets metadata: no op's behavior depends on it, and a metadata op changes no record, no version and no gate.
- **W14. `Extend { token: t, lease: d }`:**
  - Claim t MUST be live (L2) after the detaches that W2a names, else `StaleLease` at `At::Extend(i)`.
  - It sets the claim's `until` to now + d, so every record still attached to t reports the new `until`. The token is unchanged, and no record's version changes.
  - The applied unit reports each new `until` in `extended`, in request order.
- Errors: `QueueNotFound`, `QueueClosed`, `Invalid`, `TooLarge`, `RequestIdConflict`, and E3.
- Idempotency: `request_id` is optional (RQ1–RQ4). With an id, a resend within R with the same fingerprint returns the recorded response with `replayed: true`, without re-evaluation. Without one, a resend is evaluated again, so an insert that did apply comes back as `Exists` and a metadata CAS that did apply as `MetadataVersionMismatch`.

### 4.3 `claim`

Why it exists: it is the only op that selects rather than addresses, and the only op that creates leases.

```rust
pub struct ClaimRequest { pub request_id: Option<RequestId>, pub stage: Stage, pub select: Select,
                          pub max_records: u32, pub lease: Duration, pub horizon: Option<i64> /* default: now µs */ }
pub enum Select { Next, WholeGroups { max_groups: u32 }, Keys(Vec<Key>) }
pub struct ClaimResponse { pub lease: Option<ClaimLease>, pub records: Vec<Record>,
                           pub skipped: Vec<(Key, Skip)>, pub partial_group: Option<GroupKey>, pub replayed: bool }
pub struct ClaimLease { pub token: LeaseToken, pub until: Timestamp }
pub enum Skip { NotFound, OtherStage(Stage), Leased, Blocked, NotAttempted }
```

- **C1. One stage per claim.** A claim names one declared stage, else `Invalid`. Any declared stage may be claimed. Candidates for `Next` and `WholeGroups` are the records that are:
  - in the claim's stage;
  - Free;
  - of `rank <= horizon` (default: now in µs on the implementation `Clock`);
  - carrying no Blocked gate, while `"*"` is not Blocked.
- **C2. `Keys(ks)`:**
  - At most 1000 keys, deduplicated in first-occurrence order. `max_records` MUST be at least the number of distinct keys, else `Invalid`.
  - It ignores the horizon but respects the stage, gates, pause and leases.
  - Every distinct key is reported exactly: claimed, or in `skipped` with a reason. C5's omission allowance does not apply.
  - Skip reasons, in precedence order: `NotFound`, `OtherStage(s)` (the record is in stage s), `Leased`, `Blocked` (a gate or pause).
  - If C7's byte limit stops the claim, each remaining key is `NotAttempted`.
- **C3. One claim per response.** A claim that returns at least one record creates one claim: a fresh unguessable token and `until` = now + `lease` (1 ms..=1 h), returned in `lease`. Each returned record is attached to it, becomes Leased, gets `attempts + 1` and a new version. The claim and all its leases are durable before the response returns. A claim that returns no record creates nothing and returns `lease: None`.
- **C4. Order,** always within the claim's stage:
  - `Next`: `(rank asc, insertion sequence asc)`.
  - `WholeGroups { max_groups }`:
    - It considers groups with no Leased member **in any stage**, ordered by their first candidate, and returns up to `max_groups` of them. `max_groups` MUST be at least 1, else `Invalid`.
    - It returns all candidates of each group it returns, in `Next` order. Omission (C5) applies only to whole groups.
    - It stops before a group that would exceed `max_records` or the response byte limit (C7). If the first group alone exceeds either, it returns that group's longest prefix that fits and sets `partial_group`.
    - Ungrouped records are never selected.
  - `Keys`: request order.
- **C5. Result guarantees.** Results MUST be in C4 order, and every returned record MUST have been a candidate under all acknowledged writes and gate ops (V2). A claim MAY omit candidates, for example ones under concurrent mutation or not yet reclaimed; `WholeGroups` omits only whole groups, and `Keys` never omits. A short or empty result is a successful poll.
- **C5a. Progress.** If no write, claim or gate op on the queue has been in flight during the last Δ, and no record of the claim's stage is attached to a claim whose `until` passed within the last Δ (V4), a claim MUST return `min(max_records, number of candidates)` records, subject to the C4 group rules and C7, and they MUST be the first candidates in C4 order.
- **C6.** A record MUST never be attached to two live claims.
- **C6a. Group exclusivity.** A `WholeGroups` claim MUST NOT return a group any record of which is Leased, by any claim of any stage, at its linearization point. `Next` and `Keys` do not check group leases, so a consumer that relies on one holder per group claims those stages only with `WholeGroups`.
- **C7.** The response is at most 16 MiB, measured as LIM1 states. The byte limit never empties a result: if a candidate exists, a claim that stops at the limit returns at least the first one (a record is at most 4 MiB). A claim never splits a group except in the `partial_group` case.
- **C8. Idempotency.** `request_id` is optional.
  - A resend within R with the same fingerprint returns the same records and token, rendered from current state, if the claim is still live and every record it returned is still attached to it; `skipped` and `partial_group` are returned as recorded. Otherwise it returns `RequestExpired`.
  - A different fingerprint returns `RequestIdConflict`.
  - Without an id, a lost response leaks its claim until the lease expires.
- **C9. Cost.** A `Next` claim MUST cost O(log n + k + b), where n is the number of records in the queue, k the number returned and b the number of candidates skipped for Blocked gates. Records of other stages, Leased records and records with `rank > horizon` MUST NOT be visited. This clause is verified by the implementation's performance gates, not by the black-box suite.
- Errors: `Invalid` (an undeclared stage; `max_records` of 0 or over 1000; `max_groups` of 0; a lease out of bounds; more than 1000 keys; `Keys` with `max_records` below the number of distinct keys), `QueueNotFound`, `QueueClosed`, `RequestIdConflict`, `RequestExpired`, and E3.

### 4.4 `read`

Why it exists: point loads by key, in any stage, and the queue's definition and metadata (the read half of a metadata CAS). There is no key-prefix listing; list records through a declared index with `scan`.

```rust
pub struct ReadRequest { pub keys: Vec<Key>, pub queue: bool }
pub struct ReadResponse { pub records: Vec<Option<Record>>, pub queue: Option<QueueState> }
pub struct QueueState { pub definition: QueueDef, pub metadata: BTreeMap<String, Value>, pub metadata_version: u64 }
```

- **R1.** At most 1000 keys. The response has one positional `Option<Record>` per requested key, in any stage and lease state.
- **R2.** Retired.
- **R3.** A read observes every write, gate op and metadata op acknowledged before it was issued (V3). It never mutates and never returns tokens.
- **R4.** `queue: true` also returns the normalized definition and the metadata with its version. A request with no keys and `queue: false` is `Invalid`.
- Errors: `QueueNotFound`, `QueueClosed`, `Invalid`, `TooLarge` (a response over 16 MiB), and E3.

### 4.5 `stats`

Why it exists: backlog counts by stage, and a bounded "enough ready groups?" threshold read that is never a claim.

```rust
pub struct StatsRequest { pub groups: Option<GroupStatsRequest> }
pub struct GroupStatsRequest { pub stage: Stage, pub limit: u32 /* <= 10_000 */, pub horizon: Option<i64> /* default: now µs; never before now */ }
pub struct Stats { pub stages: Vec<StageCount>, pub paused: bool, pub groups: Option<GroupStats> }
pub struct StageCount { pub stage: Stage, pub free: u64, pub leased: u64 }
pub struct GroupStats { pub ready_groups: u32, pub oldest_ready_since: Option<Timestamp> }
```

- **S1.** `stages` has one entry per declared stage, in declaration order, counting every record by stage and lease state. `paused` means `"*"` is Blocked. The values observe every acknowledged write but MAY be torn (V3), and reflect lease reclaim within Δ (V4).
- **S2.** `groups.stage` MUST be a declared stage, and `groups.horizon`, when given, MUST NOT be earlier than now on the implementation `Clock`; either violation is `Invalid`. A caller that wants now omits the horizon.
  - `ready_groups = min(limit, number of distinct groups that have a Free record in that stage with rank <= horizon)`. Ungrouped records are not counted.
  - `oldest_ready_since` is the minimum `ready_since = max(rank, free_since)` over those records (§3).
  - Both ignore gates, pause and Leased members, so they can overcount the groups a `WholeGroups` claim can take.
  - The cost MUST be O(log n + `limit`), where n is the number of records in the queue. Like C9, this cost bound is verified by the implementation's performance gates, not by the black-box suite (CS2).
- **S3.** `stats` never mutates.
- Errors: `QueueNotFound`, `QueueClosed`, `Invalid`, and E3.

### 4.6 `scan`

```rust
pub struct IndexRange { pub index: String, pub eq: Vec<Value>, pub lower: Bound<Value>, pub upper: Bound<Value> }
pub struct ScanRequest { pub range: IndexRange, pub descending: bool, pub limit: u32, pub cursor: Option<Cursor> }
pub struct ScanPage { pub hits: Vec<Hit>, pub cursor: Option<Cursor> }
pub struct Hit { pub key: Key, pub stage: Stage, pub lease: Lease, pub values: Vec<Value> }
```

- **Q1.** The index MUST be declared, else `Invalid`.
  - `eq` binds a leading prefix of the index keys; `lower` and `upper` bound the next key.
  - Values have the key's type: the declared type for a field, and the §4.0 types for built-ins. A `Stage` value that names an undeclared stage is `Invalid`.
  - A record is in an index iff it has a value for every key: each field key present, and a group for a `Group` key. `Stage` and `Rank` always have a value. Membership holds in every stage and lease state.
- **Q2.** Hits come in encoded-index-key order (ascending or descending), then insertion sequence ascending. `limit` is 1..=1000. The cursor is opaque. Paging MUST NOT duplicate or skip records that are unchanged during paging.
- **Q3.** A page observes every write and gate op acknowledged before it was requested (V3).
- Errors: `QueueNotFound`, `QueueClosed`, `Invalid`, and E3.

### 4.7 `aggregate`

```rust
pub struct AggregateRequest { pub range: IndexRange, pub by: GroupBy, pub max_groups: u32 /* <= 10_000 */ }
pub enum GroupBy { Dims(Vec<Dim>), Ranges { attr: Attr, bounds: Vec<Value> } }
pub enum Dim { Attr(Attr), TimeBucket { attr: Attr, width_ms: u64 }, Lease } // epoch-aligned buckets
pub struct AggregateResponse { pub groups: Vec<(Vec<Option<Value>>, u64)>, pub truncated: bool }
```

- **Q4.** Counts the records in the range (Q1), grouped:
  - Groups are ordered ascending by group tuple (§4.0 order, `None` first, stages by declaration position). The first `max_groups` are returned, and `truncated` is set when more exist.
  - Every `Dim::Attr`, every `TimeBucket` attribute and `Ranges.attr` MUST be a key of the range's index, else `Invalid`. `TimeBucket` needs a `Time` field.
  - `Dim::Lease` is the one dimension that is not an index key. It is read from the record, as `Value::Str("free" | "leased")`.
  - `Ranges` returns buckets `[b_i, b_{i+1})` plus a null bucket for values outside `[b_0, b_last)`. It is used alone. `bounds` MUST hold at least 2 values of the attribute's type in strictly ascending order, and every `Ranges` bound for `Stage` MUST name a declared stage, else `Invalid`.
  - A `TimeBucket` `width_ms` of 0 is `Invalid`.
  - An empty `Dims` counts the whole range as one group whose tuple is empty.
- **Q5.** The result observes every acknowledged write but MAY be torn under concurrent writes (V3).
- Errors: `QueueNotFound`, `QueueClosed`, `Invalid`, and E3.

### 4.8 `delete_queue`

Why it exists: queues are cheap and per job type, so they must be removable, even when not empty.

```rust
pub struct DeleteQueueRequest { pub archive: bool }
pub struct QueueDeleted { pub already_closed: bool }
pub enum DeletePolicy { ArchiveOnly /* default */, AllowFull } // fixed at construction
```

- **DQ1.** On an open queue, `archive: true` archives it: its records stay in durable storage but are no longer reachable through any op. `archive: false` deletes it fully, and only if the instance's `DeletePolicy` is `AllowFull`. Under `ArchiveOnly`, `archive: false` fails with `NotPermitted` and has no effect, whatever the queue's state: open, archived or deleted. The policy is a construction input, `ArchiveOnly` by default.
- **DQ2.** It succeeds on a non-empty queue, including one with Leased records. It is atomic and durable (DUR1): every record, claim, gate and metadata entry of the queue leaves the data plane together, and every outstanding token is dead.
- **DQ3.** After success the queue is **closed**, and closed is final. Every op on it other than `delete_queue` (DQ5), `define_queue` included, fails with `QueueClosed`. There is no read-only state and no reopen through `Store`; retrieving an archive is outside this contract.
- **DQ4.** Every op on the queue is linearized before or after the close. An op other than `delete_queue` that is linearized after it fails with `QueueClosed`, and every write acknowledged before it is part of the archive.
- **DQ5.** On a closed queue, `delete_queue` returns `already_closed: true` with no further effect, with two exceptions: a full delete (`archive: false`) under `ArchiveOnly` fails with `NotPermitted` (DQ1), and a full delete of an archived queue under `AllowFull` also discards the archive. When storage is reclaimed after a full delete is implementation-defined; the reclamation design is pending measurement, and it changes nothing observable through `Store`.
- Errors: `QueueNotFound` (never defined), `NotPermitted`, and E3.
- Idempotency: naturally idempotent (DQ5); it takes no `request_id`.

## 5. Queue definition and queue metadata

### 5.1 The definition

`QueueDef` has three parts, and the engine interprets all three:
- **`stages`:** an ordered list of 1..=32 names. The first is the insert default. With no list given, the stages are `[INITIAL, COMPLETED]`. Every record is in exactly one stage, every claim names one (C1), `stats` counts by stage (S1), and indexes and aggregates may key on it. No stage is special.
- **`fields`:** at most 256 typed fields. The types are `Int` (i64), `Float` (finite f64), `Bool`, `Time` (µs), `Str` (UTF-8) and `Bytes`.
- **`indexes`:** at most 32 typed indexes, each over 1..=8 keys drawn from the declared fields and the built-ins `Stage`, `Rank` and `Group`. Indexed values are meant to be small scalars: an index's encoded key is at most 1024 B (W1, W8). Lease state, `until`, `attempts` and `version` cannot be index keys: they change on every claim and reclaim, and `stats` and `Dim::Lease` already read the lease.

Field rules:
- **F1.** A record carries only declared fields, each with a value of its declared type. A write that names an undeclared field, or gives a value of another type, is `Invalid` (W1).
- **F2.** A `Str` or `Bytes` value is at most 64 KiB, and an encoded record (key and fields) at most 4 MiB, both measured as LIM1 states. Every value within these bounds behaves the same in every op: there is no caller-visible "large" flag, no separate payload, and no size-dependent semantics. Where values are physically placed is the implementation's choice.
- **F3.** A record may lack any declared field. `None` in a patch, or `clear_fields`, makes a field absent, and an absent field keeps the record out of every index keyed on it (Q1).

Fields are the only per-record data. Metadata for a transition (history, audit, acknowledgements) travels in the write that settles the record and is stored in its fields. There are no side records.

**Why the definition is immutable.** `define_queue` is create-or-verify, and a different definition is a `DefinitionConflict` (D3). Stage names, field types and index keys are interpreted by every write (W1), every claim and every replay. Changing them would strand records in a removed stage, make acknowledged writes invalid after the fact, and rewrite every record's index entries while claims run. Changing any part of the definition means a new queue.

### 5.2 Queue metadata

Queue metadata is mutable configuration that the consumer owns and the engine never interprets:
- a map of `String → Value` with a `u64` version, empty at version 0 when the queue is created (D1);
- changed only by a metadata op inside a write unit, atomically with the unit's other effects, by compare-and-swap on the version with a sparse patch (W13);
- read fresh through `read{queue: true}` (R4);
- at most 64 KiB encoded (LIM1), with entry names of 1..=128 B.

The bound keeps it configuration, such as a consumer's shard manifest. It is not a key-value store, and not side records under another name.

## 6. Cross-cutting semantics

### 6.1 Durability
- **DUR1.** An `Ok` from `define_queue`, `write`, `claim`, `delete_queue` or an `Operator` op means every effect it reports survives process loss: after a `reopen` over the same durable state, every read observes it. The reads (`read`, `stats`, `scan`, `aggregate`) never mutate.

### 6.2 Visibility
- **V1.** Write preconditions (`Expect`, token, claim liveness and lease state) are linearizable per record against the latest committed state. The metadata version (W13) is linearizable per queue, and claim liveness per claim.
- **V2.** A claim observes every write (stage and rank changes included), extend and gate op acknowledged before it was issued. It may still omit candidates (C5), within C5a.
- **V3.** `read` and `scan` are fresh: they observe every write, gate op and metadata op acknowledged before they were issued. `stats` and `aggregate` observe them too but MAY be torn under concurrent writes.
- **V4. Lease reclaim is the only time-driven transition.**
  - When a claim's `until` passes, the claim ends, and within Δ every record still attached to it becomes Free. Until then an implementation MAY still report and treat those records as Leased.
  - Token use (`Held`, `Release`, `Extend`) MUST be rejected with `StaleLease` as soon as `until <= now`.
  - Reclaim is scheduled on the injected `Clock` with `sleep_until`, so advancing a manual clock wakes it. A claim that observes `Clock::now() − last sweep ≥ Δ` MUST wake the sweep and MAY proceed without waiting for it.
- **V5.** A request id MAY be forgotten after the request-id retention R. That is not a Δ transition.
- **Δ** bounds only lease reclaim; reads are never lagged by Δ. Each implementation instance has one Δ, fixed at construction, documented, and at most 5 s, measured on its injected `Clock`.

### 6.3 Atomicity and isolation
- **A1.** A unit (W2): all its record writes, extends, gate ops and its metadata op, in one queue.
- **A2.** A claim (C3): the claim and all its leases.
- **A3.** A `delete_queue` (DQ2): the whole queue.
- **A4.** Nothing is atomic across queues, and no op spans queues.
- **ISO1. Queue isolation.** An op on one queue MUST NOT observe or change any other queue's records, claims, gates, metadata or definition. A token is valid only on the queue whose claim created it: `Held(t)`, `Release(t)` or `Extend(t)` on another queue gets `StaleLease`.

### 6.4 Request-id replay
- **RQ1.** The scope is `(queue, request_id)`, shared by `write`, `claim` and `Operator::force_clear`. A `request_id` is 1..=128 B and always optional.
- **RQ2.** A request's fingerprint is `fireweed_contract::fingerprint(op kind, body)`, a pure, versioned canonical hash shared by every implementation. A resend within R with the same id and fingerprint returns the recorded outcome with `replayed: true` and is not re-evaluated (claims: C8). A different fingerprint returns `RequestIdConflict`.
- **RQ3.** The request record is written in the same atomic step as the effects.
- **RQ4.** An id-less request is evaluated again on every resend. An id-less write that failed with `Unavailable` is resolved by `read` of the keys it touched, and `read{queue: true}` if it carried a metadata op.

### 6.5 Ordering
- **ORD1. Insertion sequence.** A per-queue total order on inserts. Within one request it follows unit order, then write order within the unit. If insert A is acknowledged before insert B is issued, `seq(A) < seq(B)`. It is assigned at insert and never changed by writes (stage moves included), claims, releases or reclaim. A re-insert after delete gets a new one.
- **ORD2. Claim order** is `(rank, insertion sequence)` within the claim's stage (C4). Equal ranks therefore claim in insertion order, not in the order records entered the stage. A consumer that wants stage-entry order sets `rank` on the move.
- **ORD3.** `scan` orders by encoded index key (descending if requested), then insertion sequence ascending. No claim orders by an index.

Rank conventions (non-normative): the default is the insertion time in µs; a scheduled record's rank is its not-before time; a priority bucket is an offset added to the rank, negative to run sooner and positive to run later. The engine interprets rank only through ordering and the horizon test.

### 6.6 Leases and claims
- **L1.** A claim is the unit of leasing: one token, one expiry `until`, and the set of records attached to it. The token is opaque and unguessable, returned only by the `claim` that created it (and its replays), and never readable through `read`, `scan`, `stats` or `aggregate`.
- **L2.** A claim is **live** while `until > now` and at least one record is attached to it. A record is Leased iff it is attached to a live claim, subject to the V4 window.
- **L3.** While a record is Leased, only its holder writes it (`Held` or `Release` with the token), no lease-free write applies to it (W4), and no claim returns it (C1, C2). `Operator::force_clear` (§7) is the only exception, and it is not part of `Store`.
- **L4.** `Extend` renews the whole claim (W14). `Release` and `Delete` detach one record, and the claim's other records stay attached. A claim ends when its `until` passes or its last record is detached.
- **L5.** Fencing is by token and `until`. A holder never needs the record version.
- **L6.** Delivery is at-least-once with at most one live claim per record (C6); `attempts` counts claims.

### 6.7 Change feed
The only reserved invariant is that `version` strictly increases per key, including across delete and re-insert. No log position, history or ordering promise is exposed.

### 6.8 Limits (contract constants)

| Item | Bound |
|---|---|
| Record writes per request (summed over units) | 1000 |
| Extends per request; gate ops per request (each summed over units) | 1000 each |
| Metadata ops per request | 1 |
| Canonical encoded request (LIM1) | 16 MiB (`TooLarge`) |
| Key | opaque bytes, 1..=256 B |
| Declared fields per queue | 256 |
| `Str` or `Bytes` field value (content bytes, LIM1) | 64 KiB |
| Encoded record (key and fields, LIM1) | 4 MiB; exceeding it is a per-record `Invalid` (W8) |
| Indexes per queue | 32, with 1..=8 keys each, no key twice within one index |
| Encoded index key (ESF, all keys of one index) | 1024 B (W1, W8) |
| Response (claim, read, scan; LIM1) | 16 MiB. A short page is allowed, except for `read` (`TooLarge`); a claim always returns its first candidate (C7). |
| `claim.max_records`, `Keys`, `read` keys, `scan.limit` | 1000 |
| `aggregate.max_groups`, `stats.groups.limit` | 10,000 |
| Gates per record | 8 |
| Group and gate key | `^[A-Za-z0-9._:-]{1,256}$`; `"*"` is reserved |
| Stage name | `^[A-Za-z0-9._:-]{1,128}$` |
| Stages per queue | 1..=32; default `[INITIAL, COMPLETED]` |
| Tenant, queue, index and field name | 1..=128 B |
| Queue metadata (LIM1) | 64 KiB encoded; entry names 1..=128 B |
| Lease and `Extend` duration | 1 ms..=1 h |
| `request_id` | 1..=128 B |
| R (request-id retention) | at least 24 h |
| Δ | at most 5 s |

- **LIM1. Byte measures.** Every byte bound in this contract is measured with `fireweed-contract`'s encoders, never with an implementation's storage size, so every implementation accepts and rejects at the same boundary:
  - `fireweed_contract::canonical_len` of the canonical encoding measures a request (E3), a record's key and fields (W8, F2), the queue metadata map and a metadata patch (W1, W13, §5.2), and a response (C7, R1);
  - a `Str` value is measured by its UTF-8 bytes, and a `Bytes` value by its bytes (F2);
  - `fireweed_contract::esf_index_key` measures an index key (W1, W8).

The key, value and field bounds rest on measurement (non-normative evidence; the physical design is not part of this contract):
- Key storage cost is linear in key length, with no cliff up to 1024 B (`docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md`).
- Values up to 64 KiB work at bounded cost on every measured operation (`docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md`).
- Up to 200 fields, and a mixed workload of 97 declared fields with 4 indexed, were measured (`docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md`, `docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md`).
- The per-claim token follows the claims-table lease design, which makes extend about 80× and expiry discovery about 75× faster (`docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md`).

## 7. The `Operator` trait and the admin handle

Force-clear ends a lease without its token. It exists only for on-call repair, in a trait of its own that no data-plane handle reaches (owner decision, 2026-09-23 design discussion).

```rust
pub trait Operator: Send + Sync + 'static {
    fn force_clear<'a>(&'a self, q: &'a QueueKey, req: ForceClearRequest) -> BoxFuture<'a, ForceClearResponse>;
}
pub struct ForceClearRequest { pub request_id: Option<RequestId>, pub records: Vec<ForceClear> }
pub struct ForceClear { pub key: Key, pub expect: Expect, pub then: Option<Op> } // then: a lease-free Put or Delete
pub struct ForceClearResponse { pub outcomes: Vec<ForceOutcome>, pub replayed: bool }
pub enum ForceOutcome { Applied { cleared: bool, write: Option<WriteOutcome> }, Rejected(Rejection) }
```

- **OP1. Separation.** `Operator` is a separate trait, obtained only from the admin handle. `Store` has no op that ends a lease without its token, and no `Store` value converts into an `Operator`.
- **OP2. Effect.** Each entry is its own atomic unit:
  1. `expect` is checked (W3).
  2. If the record is Leased, it is detached from its claim without the token, with a new version, and `cleared` is true. The claim's other records stay attached, and the token stays valid for them.
  3. `then`, if present, applies as a lease-free write (W5, W7, W8).

  A failing entry reports `Rejected` and has no effect; the other entries are unaffected.
- **OP3.** Envelope validation (at most 1000 entries, unique keys), durability, visibility and request-id replay are those of `write` (W1, DUR1, V1–V3, RQ1–RQ4).
- **OP4. Usage rule.** Normal workflows MUST NOT use force-clear as a side channel. CI bans the `Operator` trait and the admin handle in Snorri, in the workload and loadgen crates, and in the contract suite.

**Admin handle.** Each implementation's constructor returns its `Store` together with an admin handle; for S3×Turso it is `TursoAdmin`. The admin handle carries the `Operator`, projection control and queue placement. Everything on it other than the `Operator` is implementation-specific deployment control, outside this contract. No `Store` semantics depend on it, and no consumer data path needs it.

**Construction inputs** that the contract fixes: the injected `Clock`, the documented Δ, and the `DeletePolicy`. Every other constructor input is implementation-specific.

## 8. What the contract deliberately excludes

- **A remote API.** The contract is in-process. The RESP adapter is deleted; a network front may be rebuilt later as a pure transport over `Store`, under its own contract.
- **Cohorts.** Deleted. An atomic group insert, a `WholeGroups` claim and a caller count check serve the use case.
- **Retention and expiry.** There is no `expires_at`, retention setting, reaper or key tombstone. Records live until the consumer deletes them or closes their queue. A future delete-by-filter op may serve bulk retention; it is not part of this contract.
- **`due`.** There is no not-before attribute and no "claimable only by key" state. `rank` carries both order and schedule (C1).
- **Index-ordered claims.** `rank` is the only claim order (C4). There is no claim in declared-index order: a consumer finds records through `scan` and claims them with `Keys`, or gives each kind of work its own stage or group.
- **Terminal stages.** There is no terminal flag, no Completed or Failed state, no `Finish` op and no transition graph.
- **Side records and payloads.** There is no side store, no separate payload slot and no per-record metadata outside the declared fields.
- **Backend introspection.** There are no capability flags, backend identity, log or projection positions, and no commit explanations. The same `Store` behaves the same on every implementation.
- **Other surfaces.** Unique secondary indexes (the key is the only uniqueness), discovery, cross-queue claims or atomicity, single-group claims, and adding stages, fields or indexes to an existing queue.
- **Physical design.** Storage layout, partition placement, snapshots, compaction, log splitting and garbage collection. None is observable through `Store`.
- **Compatibility.** Fireweed is pre-production: there is no import of existing data and no replay compatibility with earlier command formats.

## 9. Conformance

- **CS1. One suite.** `fireweed-contract-suite` runs in-process against `Arc<dyn Store>` through `run_all(constructor, reopen, Δ, clock, fault_hook)`:
  - `constructor(DeletePolicy)` builds a fresh instance over empty durable state, on the suite's manual `Clock`;
  - `reopen` restarts the instance over the same durable state, to test DUR1, D1, C3 and DQ2;
  - `Δ` is the instance's documented bound;
  - `clock` is a manual `Clock` that drives V4, the default rank and the default horizon;
  - `fault_hook` makes the next acknowledgement fail with `Unavailable` after commit, to test resolution by resend and by `read` (E2, RQ2, RQ4). It exists only in test builds.
- **CS2.** The suite has at least one scenario per MUST clause of §4 and §6, each citing its clause id. Two cost clauses are left to performance gates: C9 and the S2 cost bound.
- **CS3.** Every scenario runs twice: directly, and through a torn-read adversary that serves `stats` and `aggregate` from a mix of pre- and post-write state while writes are concurrent. `read` and `scan` get no lag wrapper.
- **CS4.** There are no per-backend clauses, backend knobs or capability probes. Every implementation passes or fails the same suite.
- **CS5.** The suite never links the `Operator` trait or the admin handle (OP4). OP1–OP3 are tested by a separate operator suite that takes the admin handle.
- **CS6.** The scenarios include at least:
  - W2a precedence; the C2 skip reasons, including `OtherStage` and `NotAttempted`; C5a progress;
  - the default stages and the insert default; a move between any two stages, back to the first included; ORD1 across stage moves;
  - the horizon: a record with a future rank is not claimable by `Next` until the horizon reaches it, yet is claimable by `Keys`;
  - the per-claim token: one token per response; `Extend` renews every attached record; `Release` detaches one record while the others stay attached; expiry frees all attached records within Δ; a stale token gets `StaleLease`; a lease-free write to a held record gets `Leased`; a token used on another queue gets `StaleLease` (ISO1);
  - W2a extends: a unit that releases some of a claim's records and extends the claim renews the rest; a unit, or a later unit, that releases the claim's last record and extends it is rejected with `StaleLease` at `At::Extend(i)`;
  - a held stage move that keeps the record Leased, so a concurrent claim of the new stage returns nothing until release;
  - `WholeGroups` exclusivity across stages (C6a), including against a concurrent `Next` claim of another stage;
  - `attempts` unchanged across a release and a reschedule;
  - F1–F3: undeclared field and wrong type rejected; 64 KiB `Str` and `Bytes` values round-trip through `write`, `claim` and `read`, and after `reopen`; 256 B non-UTF-8 keys;
  - a metadata CAS in one unit with record writes, and its rejection rolling the unit back; `read{queue}` freshness;
  - a claim, `stats`, a stage move, then `stats` again; release, then `stats.oldest_ready_since`;
  - `delete_queue` under both policies, on a non-empty queue with a live claim: `NotPermitted` under `ArchiveOnly` for a full delete, on an open queue and again on an archived one; `QueueClosed` for every other op afterwards, including after `reopen`; `already_closed` on repeat;
  - `stats.groups` with a horizon earlier than now is `Invalid`; `-0.0` round-trips as `+0.0`; a `Ranges` with unsorted bounds and a zero `TimeBucket` width are `Invalid`, and an empty `Dims` returns one total group.
- **CS7. Mandatory for every implementation:**
  - all eight `Store` ops, with no optional facet;
  - the §6.8 constants and the error classes;
  - an injectable `fireweed_contract::Clock` on its constructor, on which lease reclaim is scheduled and Δ is measured;
  - a documented Δ of at most 5 s;
  - a `DeletePolicy` construction input, `ArchiveOnly` by default;
  - an admin handle that provides the `Operator`;
  - `reopen` and the post-commit fault hook, in test builds;
  - the pure `fireweed-contract` encoders (fingerprint, `canonical_len`, ESF), used verbatim.

  Nothing else is mandatory.
