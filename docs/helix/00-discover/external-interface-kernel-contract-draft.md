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

Every blocker and major defect the judges raised is resolved in §11. A later verification pass (three lenses: citations, consistency, compliance; 51 findings) and the lead engineer's binding adjudication of it are applied throughout, with one row per finding in §12. The owner's binding decision is applied throughout:
- Metadata is carried by the update or settle command and stored on the items themselves.
- There is no Idle, Parked or `Due::Never` state.
- There is no never-claimable record class, no `record_kind` convention and no side store.
- `due: None` means claim-by-key only. Every such record is claimed by its owner; there is no store-only record (§1, §8.1).

---

## 0. Summary

| Op | Facet | One-line purpose |
|---|---|---|
| `define_queue` | kernel | Create-or-verify a queue's immutable definition: typed indexes and the default retention for finished records. |
| `write` | kernel | The only mutation. It covers insert, upsert, sparse update, CAS, settle (release/finish), extend, force-clear, delete and gate flips. Writes are grouped into atomic units. `request_id` is optional. |
| `claim` | kernel | Lease records exclusively: the next by rank, one group, whole groups, named keys, or an index range. |
| `read` | kernel | Point reads by key, all states. Observes every acknowledged write; never exposes tokens. |
| `stats` | kernel | Counts by state, the pause flag, and a bounded, un-gated ready-group threshold read. |
| `scan` | Query | Page through records in a declared typed index. Observes every acknowledged write. |
| `aggregate` | Query | Count the records in an index range, grouped by fields, time buckets, value ranges or state. |

- **Public operations: 7, all mandatory for every implementation.**
  - The count assumes §10 Q4 option 2 (cohorts deleted). Option 1 adds `Select::Cohort`, a `cohort_size` insert attribute and a third time-driven transition, but no new op.
  - The verification pass narrowed the ops without changing their number: `LeaseOp::Hold` and `read(Prefix)` are gone, the write `request_id` is optional, `read` and `scan` are fresh (V3), and Δ bounds only the time-driven transitions (V4).
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
- At most one record exists per key, in any state.
- `item_id` leaves the contract. S3×Turso keeps it as its private row key: every row already carries `client_item_key TEXT NOT NULL` under the unique index `fireweed_items_active_key` (`crates/fireweed-relational/src/schema.rs:19,65-66`).

**Attributes.**

| Attribute | Type | Written by | Rule |
|---|---|---|---|
| `key` | `Key` | caller, insert | Identity. Immutable. |
| `version` | `Version { incarnation: u64, seq: u64 }` (`Ord`) | implementation | Opaque; compared for equality in CAS. Strictly increases on every applied change to the record: a write, a claim, a settle or an extend. It is strictly greater than every version the key held before, including across delete and re-insert. Lease expiry and gate changes do not change it. |
| `state` | `Pending \| Leased \| Completed \| Failed` | claim / write | Completed and Failed are terminal. |
| `due` | `Option<Timestamp>` | caller | `Some(t)`: scheduled. The record is claimable by scheduled selects once `t <= due_by`. `None`: claim-by-key only. The record is claimable only by `Select::Keys`, and its owner claims it that way (§8.1). This is not a state. An insert that leaves `due` unset gets `Some(now)` (W5). |
| `rank` | `i64` | caller | Ascending claim order; default 0. For timestamp priority use epoch-µs; for descending order, negate the value. |
| `group` | `Option<GroupKey>` | caller, insert only | Claim scope and whole-group axis. |
| `gates` | `BTreeSet<GateKey>`, at most 8 | caller | The record is unclaimable while any carried gate is blocked. |
| `fields` | `BTreeMap<String, Value>` | caller, patched per key | Typed scalars: `Str \| Int(i64) \| Float(f64, finite) \| Bool \| Time \| Bytes`. The only queryable data and the source of index keys. |
| `payload` | `Option<Bytes>` | caller, whole replace | Opaque. Stored off the hot row. |
| `expires_at` | `Option<Timestamp>` | caller; defaulted on finish | Earliest time the implementation may delete the record. `None` on a finished record means it is retained until `Delete` (W6). |
| `attempts` | `u32` | implementation | +1 per claim. Never reset (S3×Turso never emits `Finalize{Rearm}`, §7.2). |
| `leased_until` | `Option<Timestamp>` | implementation | `Some` iff the record is `Leased`. The token is never readable. |
| `finished_at` | `Option<Timestamp>` | implementation | Set when the record enters Completed or Failed. |

- **Derived attributes, not returned:**
  - `pending_since` is set to now on insert, on `Release`, and on `Clear` of a Leased record. On lease reclaim it is set to `leased_until`. A write whose patch changes `due` from `None` to `Some` sets `pending_since = now`. No other write changes it.
  - For `due = Some(t)`, `ready_since = max(t, pending_since)`. A record with `due = None` has no `ready_since` and is never counted by S2.
- `Timestamp` is UTC epoch microseconds (`i64`), so PostgreSQL `timestamptz` round-trips exactly.

**Lifecycle.**
```
 write Put (absent key) ──► PENDING ◄── write {None|Clear, Put}: sparse patch, stays Pending
                             │   ▲
       claim (C1/C2)         │   │ write {Release(t)|Clear, Put}   lease reclaim within Δ of leased_until
                             ▼   │
                           LEASED ── write {Extend(t,d), Put} ──► LEASED (same token)
                             │
   write {Release(t)|Clear, Finish(o)}          write {None|Clear, Finish(o)} (from PENDING)
                             ▼                              │
                  COMPLETED | FAILED  ◄─────────────────────┘   (read-only; Put -> Terminal)
 any state ── write Delete (Leased needs Release(t)|Clear) ──► absent (key free immediately)
 expires_at set and passed, not Leased ── deleted within Δ (V4.2) ──► absent (no tombstone)
```

**Expiry.**
- Any record may carry `expires_at`.
- `Finish` sets it (W6):
  - patch `expires_at: Set(Some(t))` gives `t`;
  - `Keep` gives `finished_at + def.terminal_retention`, replacing any stored value;
  - `Set(None)` leaves none, and the finished record is retained until an explicit `Delete`.
- A record with `expires_at` is deleted within Δ after the later of `expires_at` and the time it stopped being Leased (V4.2). Until then it exists, is readable and is counted (S1), and `Expect::Absent` fails on it.
- After deletion the key is free. There are no key-retention tombstones.
- This is the only retention mechanism. It replaces:
  - terminal retention;
  - `client_item_key_retention_ms` (a consumer with a longer dedup window sets `terminal_retention` to it, §8.2);
  - cohort expiry;
  - the indefinite lifetime of side records (indefinite retention is now an explicit `Set(None)` on `Finish`).

**What is not in the model.**
- No Idle, Parked, `Due::Never` or `record_kind`.
- No never-claimable class, and no side store.
- `due: None` means claim-by-key only. Every such record is an ordinary Pending item that its owner claims with `Select::Keys` (C2): Snorri's instance items, claimed per transition batch, and its awaits, claimed on delivery (§8.1).
- There is no store-only record. A consumer record that nothing ever claims is a mapping defect, not a contract state. The one candidate left, Snorri's shard manifest, is listed in §10 Q1.

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
pub struct GroupKey(String);       // ^[A-Za-z0-9._:-]{1,256}$
pub struct GateKey(String);        // ^[A-Za-z0-9._:-]{1,256}$; "*" is reserved (W9)
pub struct RequestId(String);      // 1..=128 B
pub struct LeaseToken(Bytes);      // opaque and self-describing (§4 Leases); never readable from a Record
pub struct Cursor(Bytes);          // opaque keyset over (encoded index key, insertion seq) (Q2)
#[derive(PartialEq, Eq, PartialOrd, Ord)] pub struct Version { pub incarnation: u64, pub seq: u64 }
pub type Timestamp = i64;          // UTC epoch µs
pub enum State { Pending, Leased, Completed, Failed }
pub enum Value { Str(String), Int(i64), Float(f64 /* finite */), Bool(bool), Time(Timestamp), Bytes(Bytes) }
pub struct Record {
    pub key: Key, pub version: Version, pub state: State, pub due: Option<Timestamp>, pub rank: i64,
    pub group: Option<GroupKey>, pub gates: BTreeSet<GateKey>, pub fields: BTreeMap<String, Value>,
    pub payload: Option<Bytes>, pub expires_at: Option<Timestamp>, pub attempts: u32,
    pub leased_until: Option<Timestamp>, pub finished_at: Option<Timestamp>,
}
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
    fn sleep_until(&self, t: Timestamp) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}
```
- `Value` order: within a variant, the natural order (`Str` and `Bytes` bytewise, `Float` numeric); across variants, the declaration order above. It matches the ESF encoded order for values of one declared type. Every place that compares values (`IndexRange` bounds, `aggregate` group tuples) compares values of one declared type, because `Dim` and range fields must be index fields (Q1, Q4). In group tuples, `None` sorts first.
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
- Outcome unknown: `Unavailable` only. Resend with the same `request_id` to resolve it; for an id-less write, `read(Keys)` the keys it touched (D8).
- Envelope-rejected requests record nothing.

### 2.1 `define_queue`
Why it exists: indexes and the retention default are fixed per queue before any record exists.

```rust
pub struct QueueDef { pub indexes: Vec<IndexDef>, pub terminal_retention: Duration }
pub struct IndexDef { pub name: String, pub fields: Vec<(String, IndexType)> } // IndexType = Str|Int|Float|Time|Bool (axon-esf)
pub struct Defined { pub created: bool }
```

- **D1.** An absent queue MUST be created atomically and durably, returning `created: true`.
- **D2.** An identical definition MUST return `created: false` with no effect. Identical means equal after sorting `indexes` by name; the field order inside an index is significant.
- **D3.** A different definition MUST fail with `DefinitionConflict { existing }` and have no effect. There is no alter or delete.
- **D4.** The definition MUST be validated:
  - at most 32 indexes, with unique names of 1..=128 bytes;
  - 1..=8 fields per index;
  - field names of 1..=128 bytes;
  - a field name has one `IndexType` across all indexes;
  - `terminal_retention` in 1 s..=400 d, in whole milliseconds.
- **D5.** Every other op on an undefined queue MUST fail with `QueueNotFound`.
- Idempotency: the operation is naturally idempotent and takes no `request_id`.

### 2.2 `write`
Why it exists: it is the single mutation verb. A settle is a write that carries a lease sub-op, and the settle's patch is stored on the settled item.

```rust
pub struct WriteRequest { pub request_id: Option<RequestId>, pub units: Vec<Unit> }
pub struct Unit { pub writes: Vec<RecordWrite>, pub gates: Vec<(GateKey, GateState)> } // atomic
pub enum GateState { Open, Blocked }
pub struct RecordWrite { pub key: Key, pub expect: Expect, pub lease: LeaseOp, pub op: Op }
pub enum Expect { Any, Absent, Exists, Version(Version) }
pub enum LeaseOp { None, Extend(LeaseToken, Duration), Release(LeaseToken), Clear }
pub enum Op { Put(Patch), Finish(Outcome, Patch), Delete }
pub enum Outcome { Completed, Failed }
#[derive(Default)] pub struct Patch {
    pub due: Change<Option<Timestamp>>, pub rank: Change<i64>, pub group: Change<Option<GroupKey>>,
    pub gates: GateEdit, pub clear_fields: bool, pub fields: BTreeMap<String, Option<Value>>, // Some=set, None=remove
    pub payload: Change<Option<Bytes>>, pub expires_at: Change<Option<Timestamp>>,
}
#[derive(Default)] pub enum Change<T> { #[default] Keep, Set(T) }
#[derive(Default)] pub enum GateEdit { #[default] Keep, Replace(BTreeSet<GateKey>), Edit { add: BTreeSet<GateKey>, remove: BTreeSet<GateKey> } }
pub struct WriteResponse { pub units: Vec<UnitOutcome>, pub replayed: bool }
pub enum UnitOutcome { Applied(Vec<WriteOutcome>), Rejected { write: u32, reason: Rejection } }
pub enum WriteOutcome { Put { version: Version, created: bool }, Finished { version: Version }, Deleted { existed: bool } }
pub enum Rejection { Exists { version: Version }, NotFound, VersionMismatch { current: Version },
                     Leased, StaleLease, Terminal, Invalid(String) }
```

- **W1. Envelope validation happens before any effect.** Failures are `Invalid` or `TooLarge`, and nothing is recorded. Checked:
  - the limits in §4;
  - key, gate, group and field-name syntax;
  - a key appearing more than once anywhere in the request;
  - a gate key appearing more than once in the request's gate ops;
  - the reserved gate `"*"` appearing in any record's gates (it is legal only in `Unit.gates`);
  - `Extend` combined with `Finish` or `Delete`;
  - an `Extend` duration outside 1 ms..=1 h;
  - an indexed field whose value type differs from its declaration;
  - an indexed field value whose encoding alone makes an index key exceed the §4 limit (a key that exceeds it only together with stored fields is a per-record `Invalid`, W8);
  - a request with no units, a unit with no writes and no gate ops, or a `GateEdit::Edit` whose `add` and `remove` sets overlap.

  A unit with gate ops and no writes is legal (the pause helper, §5).
- **W2. Units apply in request order.**
  - Each unit is atomic: all its writes and gate ops apply, or none do.
  - A rejected unit reports the first failing write index and the reason, and has no effect on other units.
  - Keys are unique per request, so units are independent: each unit's preconditions see the state as of the request.
  - The applied units of one request become durable together.
- **W2a. Evaluation order.** Each write is checked in this order, and the first failure is reported:
  1. `Expect` (W3).
  2. Absent key: `Put` gives `StaleLease` if the lease op is `Extend` or `Release`, and otherwise inserts (W5). `Finish` gives `NotFound`. `Delete` (with `Expect` `Any` or `Absent`) gives `Deleted { existed: false }` for every lease op.
  3. Terminal record: `Put` or `Finish` gives `Terminal`. `Delete` requires `None` or `Clear`, otherwise `StaleLease`.
  4. Lease op (W4): `Leased` or `StaleLease`.
  5. Patch validity (W8): `Invalid`.
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
  - `Clear`: ends any lease without a token (force). The former holder's token becomes invalid. `Clear` on a record that is not Leased is a no-op and never causes a rejection.
- **W5. `Put(patch)`:**
  - On an absent key: requires `Any` or `Absent` and a lease op of `None` or `Clear` (W2a). The record is inserted as Pending with defaults (`due: Some(now)`, `rank: 0`, no group, no gates, empty fields, no payload, no `expires_at`, `attempts: 0`, `pending_since = now`), then the patch is applied. An unscheduled insert is an explicit `due: Set(None)`.
  - On an existing record: only the `Set` attributes change.
    - With `Release`, or `Clear` on a Leased record, the record becomes Pending.
    - With `Extend` it stays Leased.
    - With `None`, or `Clear` on a Pending record, it stays Pending.
  - On a terminal record: rejected with `Terminal`.
- **W6. `Finish(outcome, patch)`:**
  - The record must exist (`NotFound`) and not be terminal (`Terminal`).
  - A Leased record requires `Release(t)` or `Clear`. A record that is not Leased requires `None` or `Clear`.
  - The patch is applied, then the state becomes the outcome, and `finished_at = now`.
  - After `Finish`, `expires_at` is:
    - `t` if the patch has `expires_at: Set(Some(t))`;
    - `finished_at + def.terminal_retention` with `Keep`, replacing any stored value;
    - none with `Set(None)`: the record is retained until an explicit `Delete`.
- **W7. `Delete`:**
  - Removes the record and any lease.
  - A Leased record requires `Release(t)` or `Clear`. A terminal record requires `None` or `Clear` (W2a).
  - An absent key with `Expect` `Any` or `Absent` gives `Deleted { existed: false }` for every lease op.
- **W8. Patch semantics:**
  - `clear_fields` empties the field map before `fields` is applied. Field keys the patch does not name are untouched.
  - `GateEdit` is resolved against the stored set. A result with more than 8 gates is a per-record `Invalid`.
  - Setting `group` on an existing record to a different value is a per-record `Invalid`.
  - `payload` is replaced whole.
  - A result whose encoded record (key, fields and payload) exceeds 4 MiB, or whose encoded key in any index exceeds 1024 B (§4), is a per-record `Invalid`.
- **W9. Gate ops:**
  - A gate op sets the queue-level state of a gate key. `"*"` is the queue pause gate, implicitly carried by every record.
  - A gate never set is Open. An Open gate holds no storage.
  - Gate ops change no record and no version.
  - They apply atomically with the unit's record writes.
- **W10.** Inserts MUST NOT be rejected because of group size.
- **W11.** Success MUST mean durable (§4).
- **W12.** A claim issued after the acknowledgement MUST observe the write (V2), and so MUST a `read` or `scan` (V3).
- Errors: `QueueNotFound`, `Invalid`, `TooLarge`, `RequestIdConflict`, `Busy`, `Unavailable`.
- Idempotency (§9 D3):
  - `request_id` is optional.
  - With an id, the same id and fingerprint within R returns the recorded `WriteResponse` with `replayed: true`, without re-evaluation. A different fingerprint returns `RequestIdConflict`.
  - Without an id, an `Unavailable` outcome is resolved by `read(Keys)` of the touched keys (D8). A resent id-less request is evaluated again, so an insert that did apply comes back as `Exists`.

### 2.3 `claim`
Why it exists: it is the only op that selects rather than addresses, and the only op that creates leases.

```rust
pub struct ClaimRequest { pub request_id: Option<RequestId>, pub select: Select, pub max_records: u32,
                          pub lease: Duration, pub due_by: Option<Timestamp> }
pub enum Select { Next, Group(GroupKey), WholeGroups { max_groups: u32 }, Keys(Vec<Key>),
                  Where { range: IndexRange, descending: bool } }
pub struct ClaimResponse { pub claimed: Vec<Claimed>, pub skipped: Vec<(Key, Skip)>,
                           pub partial_group: Option<GroupKey>, pub replayed: bool }
pub struct Claimed { pub record: Record, pub lease: LeaseToken }
pub enum Skip { NotFound, Leased, Blocked, Terminal, NotAttempted }
```

- **C1. Candidates for scheduled selects** (`Next`, `Group`, `WholeGroups`, `Where`) are records that satisfy all of the following:
  - Pending;
  - `due = Some(t)` with `t <= due_by` (default: implementation now);
  - no carried gate blocked;
  - `"*"` not blocked.
- **C2. `Keys(ks)`:**
  - At most 1000 keys, deduplicated in first-occurrence order. `max_records` MUST be at least the number of distinct keys, otherwise `Invalid`.
  - It ignores `due`, including `None`, but respects gates, pause and leases.
  - Every distinct key is reported, either claimed or in `skipped` with a reason. C5's omission allowance does not apply to `Keys`: every key is classified exactly. `Blocked` covers both gate and pause.
  - When several skip reasons apply, the first of `NotFound`, `Terminal`, `Leased`, `Blocked` is reported.
  - If C7's byte limit stops the claim, each remaining key is reported as `NotAttempted`.
- **C3. Each claimed record becomes Leased,** with:
  - a fresh unguessable token;
  - `leased_until = now + lease` (1 ms..=1 h);
  - `attempts + 1`;
  - a new version.
  All leases in one response are durable together before the response returns.
- **C4. Order:**
  - `Next`: `(rank asc, insertion sequence asc)`. The insertion sequence is the §4 total order, fixed at insert and never changed.
  - `Group(g)`: the same order, within g.
  - `WholeGroups { n }`:
    - Considers groups with no Leased member, ordered by their first candidate.
    - Returns all candidates of each group. Omission (C5) applies only to whole groups: a returned group contains every candidate member, or `partial_group` is set.
    - Stops before a group that would exceed `max_records` or the response byte limit (C7).
    - If the first group alone exceeds `max_records` or the response byte limit, it returns that group's longest prefix that fits, in C4 order, and sets `partial_group`.
    - Ungrouped records are never selected.
  - `Where`: index order (descending if requested), then insertion sequence ascending.
  - `Keys`: request order.
- **C5. Result guarantees:**
  - Results MUST be in the C4 order.
  - Every returned record MUST have been a candidate under all acknowledged writes and gate changes (V2).
  - A claim MAY omit candidates, for example candidates under concurrent mutation or not yet reclaimed. `WholeGroups` may omit only whole groups (C4); `Keys` never omits (C2). A short or empty result is a successful poll.
- **C5a. Progress.** If no write, claim or gate op on the queue has been in flight during the last Δ and no candidate is inside a V4 window, a claim MUST return `min(max_records, number of candidates)` records, subject to the C4 group rules and C7. Those records MUST be the first candidates in C4 order.
- **C6.** A record MUST never have two unexpired leases.
- **C7.** The response is at most 16 MiB. The byte limit never empties a result: if at least one candidate exists, a claim that stops at the limit returns at least the first (a record is at most 4 MiB, §4). The claim never splits a group except in the `partial_group` case (C4).
- **C8. Idempotency:** `request_id` is optional.
  - When present, a replay within R with the same fingerprint returns the same records and tokens, rendered from current state, if every lease is still held by that claim. `skipped` and `partial_group` are returned as recorded. A lease is held iff the record is Leased with that token and `leased_until > now`. Otherwise the replay returns `RequestExpired`.
  - A different fingerprint returns `RequestIdConflict`.
  - Without an id, a lost response leaks the leases until they expire.
- Errors:
  - `Invalid`: `max_records` of 0 or over 1000, a lease out of bounds, an undeclared index or a bad range shape, more than 1000 keys, or `Keys` with `max_records` below the number of distinct keys.
  - Also `QueueNotFound`, `RequestIdConflict`, `RequestExpired`, `Busy`, `Unavailable`.

### 2.4 `read`
Why it exists: point loads replace `live_item(s)`, `claimed` and `side_record(s)`. There is no key-prefix listing; a consumer lists records through a declared index with `scan` (§6, §8.2).

```rust
pub struct ReadRequest { pub keys: Vec<Key> }
pub struct ReadResponse { pub records: Vec<Option<Record>> }
```

- **R1.** At most 1000 keys. The response has one positional `Option<Record>` per key, in any state.
- **R2.** Removed in the verification pass. `read(Prefix)` served only Snorri's instance listing, which is now `scan` over a Snorri-declared index, and `retained_items`, which has no consumer.
- **R3.** A read observes every write and gate change acknowledged before it was issued (V3). It never mutates and never returns tokens.
- Errors: `QueueNotFound`, `Invalid`, `TooLarge` (a response over 16 MiB), `Unavailable`.

### 2.5 `stats`
Why it exists: backlog counts, and Seventh Sense's `hasNextTasks` threshold, which must be a bounded read and never a claim.

```rust
pub struct StatsRequest { pub groups: Option<GroupStatsRequest> }
pub struct GroupStatsRequest { pub limit: u32 /* <= 10_000 */, pub due_by: Option<Timestamp> /* default: now */ }
pub struct Stats { pub pending: u64, pub leased: u64, pub completed: u64, pub failed: u64,
                   pub paused: bool, pub groups: Option<GroupStats> }
pub struct GroupStats { pub ready_groups: u32, pub oldest_ready_since: Option<Timestamp> }
```

- **S1.** Counts cover every record that has not been deleted, by state. A record past `expires_at` is counted until it is deleted (V4.2). `paused` means `"*"` is blocked. The values observe every acknowledged write (V3) and reflect the time-driven transitions within Δ (V4).
- **S2.** `ready_groups = min(limit, number of distinct groups that have a Pending record with ready_since <= due_by)`.
  - `due_by` defaults to the implementation's now.
  - Ungrouped records are not counted. Records with `due = None` have no `ready_since` (§1) and are not counted.
  - `oldest_ready_since` is the minimum `ready_since` over those records.
  - Both ignore gates, pause and Leased members. They count intrinsic readiness only, so they can overcount the groups `WholeGroups` can claim (D13).
  - The cost MUST be bounded by `limit`.
- **S3.** `stats` never mutates.

### 2.6 `scan` (Query)
```rust
pub struct IndexRange { pub index: String, pub eq: Vec<Value>, pub lower: Bound<Value>, pub upper: Bound<Value> }
pub struct ScanRequest { pub range: IndexRange, pub descending: bool, pub limit: u32, pub cursor: Option<Cursor> }
pub struct ScanPage { pub hits: Vec<Hit>, pub cursor: Option<Cursor> }
pub struct Hit { pub key: Key, pub state: State, pub values: Vec<Value> }
```

- **Q1.** The index MUST be declared.
  - `eq` binds a leading prefix of the index fields.
  - `lower` and `upper` bound the next field.
  - A record is in an index iff it has every index field with the declared type, in any state.
- **Q2.** Hits come in encoded-index-key order (asc or desc), then insertion sequence asc.
  - `limit` is at most 1000.
  - The cursor is opaque, a keyset over `(index key, insertion seq)`.
  - Paging MUST NOT duplicate or skip records that are unchanged during paging.
- **Q3.** A page observes every write and gate change acknowledged before it was requested (V3).

### 2.7 `aggregate` (Query)
```rust
pub struct AggregateRequest { pub range: IndexRange, pub by: GroupBy, pub max_groups: u32 /* <= 10_000 */ }
pub enum GroupBy { Fields(Vec<Dim>), Ranges { field: String, bounds: Vec<Value> }, State }
pub enum Dim { Field(String), TimeBucket { field: String, width_ms: u64 } }  // epoch-aligned buckets
pub struct AggregateResponse { pub groups: Vec<(Vec<Option<Value>>, u64)>, pub truncated: bool }
```

- **Q4.** Groups are ordered ascending by group tuple (§2.0 `Value` order, `None` first), and the first `max_groups` are returned.
  - `truncated` is set when more exist.
  - `Dim` fields and `Ranges.field` MUST be fields of the range's index, otherwise `Invalid`.
  - `Ranges` returns buckets `[b_i, b_{i+1})` plus a null bucket. Values outside `[b_0, b_last)`, and records without the field, fall in the null bucket.
  - `State` group keys are `Value::Str("pending" | "leased" | "completed" | "failed")`.
  - `Ranges` and `State` are used alone.
- **Q5.** The result observes every acknowledged write (V3) but MAY be torn under concurrent writes.

---

## 3. Queue definition

`QueueDef` has exactly two settings:
- `indexes`: typed indexes over `fields`.
- `terminal_retention`: the default `expires_at` offset applied at `Finish`.

This assumes §10 Q4 option 2. Option 1 adds a `cohort_size` insert attribute, `Select::Cohort` and the cohort completion deadline (a third V4 transition).

Everything else is either a contract constant (§4) or removed.

| Dropped from today's `QueueDefinition` (`crates/fireweed-core/src/domain.rs:925-963`) | Where it went |
|---|---|
| `tenant_id`, `queue_id` | Now the `QueueKey` argument. |
| `priority_model` (all kinds), `tie_breaker` | Replaced by `rank: i64` ascending plus insertion order. |
| `ordering_mode`, `max_rank_error`, `progress_bound_ms` | Removed. Only Strict ordering is used (Snorri `lib.rs:16617-16620`). |
| `eligibility_policy.gate_keys`, `max_gate_keys_per_item`, `max_gates_per_request` | Gates are always on. `max_gates_per_request` (distinct item gate keys per push, `async_composed.rs:1726-1731`; Snorri uses 10,000, snorri `lib.rs:16624`) is removed. Per-record gates are bounded at 8, and `Unit.gates` ops at 1000 per request, a new and separate bound. B's proposed `gates: bool` flag is unnecessary: the claim already skips the anti-join when no gate is blocked (`crates/fireweed-turso/src/projection.rs:1997-2012`). Always-on gates cost one queue-prefix probe per claim (`projection.rs:1989-1996`), which `GateKeyPolicy::None` skips today; the P3/P4 gates measure it. |
| `eligibility_policy.metadata_blockers` | Replaced by a caller gate. |
| `cohort_policy` | Removed if the owner accepts Q4. |
| `recurrence` | Removed. Re-arm is `{Release(t), Put{due}}`. S3×Turso maps it to `Finalize{Retry}` or to a held `UpdateFields` plus `Finalize{Release}`, never `Finalize{Rearm}`, so `attempts` is never reset (§7.2). |
| `retry_policy.max_attempts` | Removed. `attempts` is exposed. |
| `request_id_retention_ms` | Constant R ≥ 24 h. |
| `client_item_key_retention_ms` | Removed. There are no tombstones; `expires_at` is the dedup window. A consumer whose dedup window is longer than its retention sets `terminal_retention` to the window, or sets `expires_at` on every `Finish` (Snorri: 24 h, §8.2). |
| `terminal_retention_ms` | Now `terminal_retention`. It is only a default; `expires_at` is per record. |
| `max_lease_duration_ms` | Constant: 1 h. |
| `max_push_batch_size`, `max_claim_batch_size` | Constant: 1000. |
| `max_eligible_group_size` | Removed. W10 means inserts are never rejected, and C4/C7 bound a group by `max_records` and the response limit. The S3×Turso adapter sets it to `None` (§7.2). |
| `secondary_indexes`, `entity_schema` | Removed. |
| `typed_indexes` (with `unique`) | Now `indexes`, without `unique`. The key is the only uniqueness mechanism (D7). Snorri's production `by_run_target_key` moves into its item key (S11). |
| `emit_change_records` | Removed. S9 deletes `crates/fireweed-server/src/change_record_sink.rs` and its config plumbing: it serialized internal envelopes and had no consumer. |

---

## 4. Cross-cutting semantics

**Durability.**
- An `Ok` from `define_queue`, `write` or `claim` means every applied effect survives process loss.
  - S3×Turso: the object-log append is durable.
  - PostgreSQL: `COMMIT`.
- A projection apply is never part of the acknowledgement.

**Visibility (one model for every implementation).**
- **V1.** Write preconditions (`Expect`, lease token, state) are linearizable per record against the latest committed state.
- **V2.** A claim observes every write and gate change acknowledged before it was issued. Omission is still allowed (C5), within C5a.
- **V3.** `read` and `scan` observe every write and gate change acknowledged before the call was issued. `aggregate` and `stats` observe them too but MAY be torn under concurrent writes.
  - This costs neither backend anything. S3×Turso already waits for log coverage before every read: `side_record`, `side_records_by_prefix` and `explain_commit` (`crates/fireweed/src/turso_compose.rs:2035-2054`), and `range_scan`, `grouped_aggregate`, `metrics_by_query` and `declared_bucket_segment` (`:2310-2334`), all through `wait_request_entry_coverage` (`:3567-3579`). PostgreSQL reads committed state.
  - The consumer mappings depend on it: Snorri's scan-then-write run mutations and its input-ack lookup (`scan(by_consumed_event)`) read durable state, and the 7S scheduler loops `findByJobId → deleteByIds → findByJobId` (§8).
- **V4. Time-driven transitions happen within Δ after their deadline.** There are exactly two (assuming §10 Q4 option 2; option 1 adds the cohort completion deadline):
  1. **Lease reclaim.** A record past `leased_until` becomes unleased and Pending, with `pending_since = leased_until` (§1). Until then an implementation MAY still report and treat it as Leased. Token ops (`Extend`/`Release`) MUST be rejected as soon as `leased_until <= now`.
  2. **Record expiry.** A record with `expires_at` set is deleted within Δ after the later of `expires_at` and the time it stopped being Leased.
  - Δ is measured on the injected `Clock`. Before serving a claim, an implementation MUST run the reclaim sweep when `Clock::now() − last_sweep ≥ Δ`. This keeps a queue recoverable under a stepped logical clock with no ambient clock read (Snorri, `lib.rs:7936-7946`).
- **V5. Request-id retention** is not a Δ transition: a request id MAY be forgotten after R.
- **Δ.**
  - Δ bounds only the time-driven transitions (V4). Reads are never lagged by Δ (V3).
  - One bound per implementation instance, fixed at construction and documented.
  - It MUST be at most 5 s. The S3×Turso default is 1 s, the same as the server reclaim tick (`crates/fireweed-server/src/env_config.rs:673`).
  - `fireweed-contract` defines `trait Clock { fn now(); fn sleep_until(t) }` (§2.0). Every time-driven transition, including the tick, is scheduled on it, and Δ is measured on it.
  - The contract suite takes `(constructor, reopen, Δ, Clock, fault hook)`:
    - `reopen` restarts the implementation over the same durable state, to test W11, §2.1 D1 and C3 ("survives process loss");
    - the fault hook makes the next acknowledgement fail with `Unavailable` after commit, to test unknown-outcome resolution by resend and by `read(Keys)`;
    - a manual `Clock` drives V4.
  - It runs every scenario twice: directly, and through a torn-read adversary that serves `aggregate` and `stats` from a mix of pre- and post-write state while writes are concurrent. `read` and `scan` get no lag wrapper.
  - There are no per-backend clauses and no backend knobs.

**Atomicity.**
- One `Unit`: all of its record writes and gate ops, within one queue.
- One claim: all of its leases.
- Nothing is atomic across queues.

**Request-id replay.**
- Scope is `(queue, request_id)`, shared across `write` and `claim`. `request_id` is 1..=128 bytes.
- `fireweed_contract::fingerprint(op kind, body)` is a pure, versioned canonical hash shared by every implementation.
- The request record is written in the same atomic step as the effects.
- R ≥ 24 h (V5).
- `write` and `claim` MAY carry an id (§9 D3). An id-less write resolves `Unavailable` by `read(Keys)` of the keys it touched (D8). Snorri's transitions carry ids.

**Ordering.**
- Claims: as in C4.
- `scan` and `Where`: index key (descending if requested), then insertion sequence ascending.
- The insertion sequence is a per-queue total order on inserts. Within one request it follows unit order, then write order within the unit. If insert A is acknowledged before insert B is issued, `seq(A) < seq(B)`.
- It is assigned at insert and never changed by writes, release or expiry. A re-insert after delete gets a new sequence.

**Leases.**
- There is one token per claimed record, opaque and self-describing: the implementation can locate the row from the token alone.
- `Extend` keeps the token.
- Fencing is by token plus expiry. A holder never needs the version.
- Delivery is at-least-once with at most one live lease.

**Limits (contract constants).**

| Item | Bound |
|---|---|
| Record writes per request (summed over units) | 1000 (TD-012:51) |
| Gate ops per request (`Unit.gates`, summed over units) | 1000 |
| Canonical encoded request | 16 MiB |
| Record (key, fields and payload, encoded) | 4 MiB; a write that would exceed it is a per-record `Invalid` (W8) |
| Encoded index key (ESF, all fields of one index) | 1024 B; `Invalid` (W1, W8). PostgreSQL btree entries fail above about 2704 B. With tenant, queue and index names bounded at 128 B each, an `index_entries (queue, idx, esf_key, seq)` entry stays under about 2.4 KB, so both backends accept the same writes. |
| Response (claim, read, scan) | 16 MiB. A short page is allowed, except for `read`, which returns `TooLarge`, and a claim always returns at least its first candidate (C7). |
| `claim.max_records`, `read` keys, `scan.limit` | 1000 |
| `aggregate.max_groups`, `stats.groups.limit` | 10,000 |
| Gates per record | 8 |
| Key | 1..=1024 B |
| Group and gate key | `^[A-Za-z0-9._:-]{1,256}$`; `"*"` is reserved |
| Tenant, queue and index name | 1..=128 B (`QueueKey`, `IndexDef.name`; D4) |
| Field name | 1..=128 B; one `IndexType` per name across indexes (§2.1 D4) |
| Indexes per queue | 32, with 1..=8 fields each |
| Lease and `Extend` duration | 1 ms..=1 h |
| `terminal_retention` | 1 s..=400 d, whole milliseconds |
| R (replay retention) | ≥ 24 h |
| Δ | ≤ 5 s |

**Change-feed hook.**
- The only reserved invariant: `version` is strictly increasing per key, including across delete and re-insert.
- S3×Turso keeps its per-queue command sequence (`relational_cursor.next_seq`, `last_command_sequence`; `schema.rs:40,96-100`) internal.
- No position type, history or ordering promise is exposed.

---

## 5. Facets: Query and Operator

**Query.** Mandatory for every implementation; it is part of `Store`.
- In plain English: when you define a queue you name typed indexes over record fields, such as `(tenant, run, scheduled_at)`. After that you can:
  - page through the records in a range of that index (`scan`);
  - count them by field value, time bucket, value range or state (`aggregate`);
  - claim in index order instead of rank order (`Select::Where`).
- Every query is a declared-index range, never a table scan.
- Why mandatory: Snorri uses `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `metrics_by_query` and `claim_by_query` on operational paths. A capability flag would be backend introspection.

**Operator.** A helper module, `fireweed::operator`, built only on the 7 ops. It is not an obligation on any backend.
- In plain English: it is what an on-call person does. Pause or resume a queue, look at a record, free a stuck lease, reschedule, finish or cancel work, and delete records. Each can be done one record at a time, or in bulk over an index range page by page.
- Mapping:

| Helper | Kernel composition |
|---|---|
| `pause`, `resume`, `is_paused` | `write{units: [Unit{writes: [], gates: [("*", Blocked \| Open)]}]}` (a gate-only unit is legal, W1); `stats.paused` |
| `inspect` | `read(Keys)` (no tokens) |
| `force_release(due)` | `{Exists, Clear, Put{due}}`. `Exists` keeps a deleted key from coming back with default values. |
| `reschedule` | `{Version(v), None, Put{due, rank}}`. `Leased` is surfaced, not overridden. |
| `force_finish` | `{Any, Clear, Finish(o)}`. `Clear` is a no-op on a record whose lease was already reclaimed (W4), so the write gives the same result whichever way an implementation sees the record inside the V4.1 window. |
| `purge` | `{Any, Clear, Delete}` |

- `redrive` is removed (verification pass): it had no consumer, and a crash between its `Delete` and its re-insert lost the Failed record unless the caller had persisted the copy (§9 D11).
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
- an injectable `fireweed_contract::Clock` on its constructor, on which every time-driven transition is scheduled and Δ is measured;
- a documented Δ of at most 5 s;
- for the contract suite: a `reopen` over the same durable state and a post-commit `Unavailable` fault hook (test builds only);
- the pure `fireweed-contract` encoders (fingerprint, ESF), used verbatim.

Nothing else is mandatory.

---

## 6. Disposition of every current public method and free function

| Current | New home | Reason |
|---|---|---|
| `create_queue`, `ensure_queue` | `define_queue` | Create-or-verify is one idempotent op. |
| `queue_definition` | Deleted | `DefinitionConflict` returns the stored definition. |
| `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id` | `write` (`Absent`, `Put`) | Duplicates become a per-unit `Exists`, not a whole-request `Conflict`. |
| `upsert` | `write` (`{Any, None, Put{clear_fields: true, fields: <all>, payload: Set, due: Set, rank: Set, group: Set(g), gates: Replace}}`) | Replaces the item's attributes, as today's supersede does (`crates/fireweed/src/turso_parity.rs:138-189`); `created` reports Inserted vs Replaced. A Leased record gives `Leased` and a terminal one `Terminal`, as today's `Invalid("collision with claimed item")` and `Terminal`. A group change is `Invalid` (W8). |
| `batch_update` | `write` (`Exists`/`Version`, `Put`) | The same pending-only semantics. |
| `reschedule` | `write` (`Version`, `None`, `Put{due, rank}`) | A leased record gives `Leased` (D2, Q3). |
| `mutate_items`, `bounded_mutation` | `scan`/`read` + `write` (`Version`, gate edits, `Clear`) | Selection is caller logic; CAS guarantees safety. |
| `set_gates` | `write` (`Unit.gates`) | Gates flip atomically with record patches. |
| `purge` | `write` (`Delete`; `Clear` = force) | |
| `claim`, `claim_with`, `claim_response_with`, `claim_at`, `claim_response_at` | `claim` (`Next`/`Group`/`WholeGroups`, `due_by`) | One request type and one response type. |
| `claim_by_query`, `claim_by_query_at` | `claim` (`Where`) | |
| `claim_by_item_ids` | `claim` (`Keys`, with skip reasons) | Keys are the identity (API-001:612-613 reasons kept). |
| `claim_across_queues` | Deleted | The owner called it unnecessary. |
| `ack`, `complete`, `fail` | `write` (`Release(t)`, `Finish`) | Now token-validated. |
| `nack`, `release`, `retry`, `retry_after`, `nack_retry_after` | `write` (`Release(t)`, `Put{due}`) | `attempts` is never reset (§7.2 maps these to `Finalize{Release}` or `Finalize{Retry}`). |
| `rearm`, `rearm_at`, `rearm_after` | `write` (`Release(t)`, `Put{due: next}`) | Recurrence is caller logic. Never `Finalize{Rearm}`, which resets the attempt count. |
| `renew` | `write` (`Extend(t, d)`) | The token stays stable. |
| `reassign` | Deleted | Use `Clear`, then `claim Keys`. |
| `commit`, `commit_multi_claim` | `write` (multi-write units) | Side records, fences and lifecycle items become item writes in the unit. |
| `explain_commit` | Deleted | Use request replay, `read(Keys)` of the keys the unit touched, or a consumer index such as Snorri's `by_tx_request` (D8, §8.2). |
| `commit_capabilities`, `hot_projection_capabilities` | Deleted | No introspection. |
| `side_record`, `side_records`, `live_item`, `live_items`, `claimed` | `read(Keys)` | |
| `side_records_by_prefix` | `scan` over a consumer-declared index | Snorri's instance listing uses `by_record_kind_key` (§8.2). There is no `read(Prefix)`. |
| `retained_items` | Deleted | No consumer. |
| `peek` | Deleted | Use `scan`/`read`. |
| `metrics` | `stats` | |
| `metrics_by_query` | `aggregate(State)` | |
| `range_scan` | `scan` | |
| `grouped_aggregate` | `aggregate(Fields)` | |
| `declared_bucket_segment` | `aggregate(Ranges)` | |
| `query_index`, `query_index_unique`, `query_index_typed`, `query_index_unique_typed` | `scan` (full `eq`) | |
| `discover`, `discover_active_scopes`, `discover_active_scopes_stamped`, free fn `select_active_scope_from_prefix` | Deleted | §5 |
| `reclaim_expired`, `reclaim_expired_at` | Deleted | The constructor-owned tick and the claim-time sweep do this (V4). |
| `ownership`, `renew_owned` | `TursoAdmin` | Deployment plumbing. |
| `projection_control`, `ProjectionControl::{capabilities, verify, delete, rebuild}` | `TursoAdmin::projection_control()` | AGENTS.md rule updated (slice S3). |
| `current_position` | Deleted | No consumer (Snorri does not call it). Deleting it keeps a log-position type off the public surface. |
| `snapshot_now`, `latest_snapshot_info` | `TursoAdmin` | Log and projection concepts. |
| `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `compact_log_behind_snapshot`, `last_rebuild_stats` | Deleted | Stubs: always `Unavailable` or `None` (`facade.rs:1379-1446`). |
| `operator_pause`, `operator_resume`, `operator_admin_state`, `operator_inspect_item`, `operator_repair`, `operator_purge` | `fireweed::operator` helpers | §5 |
| `operator_archive`, `operator_get_operation`, `operator_audit_records`, `operator_redrive` | Deleted | No consumer (`operator_redrive`: §5, D11). |
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
| "Keep terminal states + the existing reaper" (C, production judge) | S3×Turso never reaps terminal rows today. `reap_terminal_items_sql` (`apply.rs:2873`) has no caller. `ProjectionStore::reap_terminal_items` defaults to a no-op (`fireweed-engine/src/compose.rs:1316-1327`). The Turso `ReclaimDriver::tick` only reclaims leases and expires cohorts (`turso_compose.rs:2259-2275`). **Any expiry is new work.** |
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

**Routing** is a pure function of request shape and applies to the whole request:
- **Route P, the packed Update generation.** Taken when every unit has exactly one write and no gate ops, and every write is one of:
  - `{Put, None}`;
  - `{Put, Release(t)}`;
  - `{Finish, Release(t)}`.
- **Route X, the existing exclusive commit/parity path.** Everything else: multi-write units, gate ops, `Extend`, `Clear`, `Delete`, and `Finish` with `None`.
- An envelope-kind parity test asserts that pure-insert, pure-update and pure-complete requests emit exactly today's `Push`, `UpdateFieldsBatch` and `Finalize` kinds (plus the request outcome when an id is present).

| Op / shape | Existing fast path | Internal change |
|---|---|---|
| `write` insert (P) | TD-016 `Push` member; the snapshot loads the request's keys (`turso_compose.rs:4221-4260`) | [B] Per-record `Exists` outcomes instead of a whole-request `Conflict` (`async_composed.rs:647-648`). [S] Delete the push-time group-size rejection (`:655-676,1737`) and the ungrouped-item rejection (`:1718-1722`; see the queue mapping below). [B] A `RequestOutcome::Write` row only on pushes that carry an id. Id-less pushes, as P1 runs today (`crates/fireweed/tests/ss_phased_capacity.rs:152-164`), keep today's cost. Use a compact outcome encoding (risk 1). [S] `created_seq = base_seq + offset` in request order (`apply.rs:2212-2223`) already gives the §4 insertion order when units are flattened in unit order, then write order. |
| `write` update of a Pending record (P) | `UpdateFieldsBatch`. Two tiers exist. The operation-shaped API-001 fast path (`apply.rs:4458-4466,4570-4590`) requires a uniform shape with no gates, CAS or field_ops; P2/P3 ride it. The general set-based path (`apply.rs:4436-4440,4950-5330`) excludes only field_ops and entity documents. It already supports CAS (skip on mismatch, `:5132-5137`), whole-set gate replace (`:5288-5322`), Leased rows when not api001 (`:5129`) and a batched read-back of `fields` (`:5070-5118`). | [B] Field patches go on the **general** tier only: `fields = CASE WHEN v.patch IS NULL THEN fields ELSE json_patch(fields, v.patch) END`, or rows without a field patch bind `'{}'`. Vendored `json_patch` returns SQL NULL when either argument is NULL (`vendor/turso_core/json/ops.rs:23-25`) and `fields` is `TEXT NOT NULL` (`schema.rs:29`), so a bare `json_patch` would poison a mixed pack. Merge-patch `null` removes a key (vendored `json_patch`/`json_remove`, `vendor/turso_core/function.rs:238-240,1708-1712`). Values are encoded as JSON arrays (`["i",5]`, `["s","x"]`), never objects, so the merge never recurses. [B] Tier routing within P: writes that set only `due`/`rank` with `Expect` `Any` or `Exists` keep the operation-shaped tier. Writes with field patches, `Expect::Version` or gate edits go to the general tier, which costs them the fast path; P2p/P3p measure it. Adding CAS or gate edits to the operation-shaped tier instead would be new work and is not assumed. [B] `GateEdit::Edit` needs set-based add/remove (`DELETE … IN`, `INSERT … ON CONFLICT DO NOTHING`) beside today's whole-set replace. [B] The general tier accepts Leased rows, so the planner rejects `Leased` for lease-`None` writes (see the acked-update paragraph below). [B] Every kernel `UpdateFields` emission uses the no-reset semantics of the batch path: `eligible_since` changes only per §1 (`due` from `None` to `Some` sets it to now). Today the non-api001 path resets it to `max(not_before, now)` (`apply.rs:5169-5172`; in memory, `fireweed-projection/src/lib.rs:2437-2442`), so the result would otherwise depend on the tier. [B] Typed index rekey: for rows whose patch touched an indexed field, the existing batched read-back of `fields`, ESF keys computed in Rust, then batched `DELETE`/`INSERT` on `fireweed_item_index`. Today rekey runs only on entity replacement and per row (`apply.rs:5704-5735`). |
| `write` settle `{Release(t), Put}` and `{Release(t), Finish}` (P) | Packed `Finalize` handles Complete/Fail only. Retry/Release/Rearm use the singleton path with a coverage wait (`turso_compose.rs:4773-4825`; `async_composed.rs:769-790`). `Finalize{Release}` ignores `not_before` (`apply.rs:5814,5834`). Only Retry (backoff, `:5836-5840`) and Rearm carry a due time, and Rearm resets `retry_count` (`:5851,5876-5882`; mandated by API-001:258). | [B] Mapping, never `Finalize{Rearm}`, because `attempts` is never reset (§1): `{Release(t), Put{}}` → `Finalize{Release}`. `{Release(t), Put{due}}` with no other attribute → `Finalize{Retry, not_before}`, which preserves `retry_count` and, with `max_attempts = u32::MAX`, never exhausts. `{Release(t), Put{due, …}}` with other attributes → a held `UpdateFieldsBatch` on the general tier that sets `not_before` and the patch, followed by `Finalize{Release}`. `{Release(t), Finish(o, patch)}` → a held `UpdateFieldsBatch` when the patch is non-empty, followed by `Finalize{Complete\|Fail}`. The general tier already accepts Leased rows when not api001, so there is no new apply mode; only planner and overlay support is new. Both commands land in one packed vector and one writer transaction. A contract-suite scenario asserts that `attempts` is unchanged across a release and re-schedule. [S] These join the packed member. The singleton path exists to read attempt counts and lease versions, and because an in-flight claim may still be Pending in Turso (`turso_compose.rs:4814-4816`). Kernel queues never exhaust retries, and the [B] overlay-token check below covers in-flight claims. [B] Token validation, which packed finalize skips today (`async_composed.rs:769-827`): check the overlay's remembered in-flight tokens, plus one batched `lease_token_hash, lease_expires_at` read by item id for applied claims, plus `leased_until > now`. The token encodes `(batch token, item_id)`, so no key lookup is needed. [B] `eligible_since` follows §1's `pending_since` rules: `Finalize{Release}` and `Clear` of a Leased record set it to now, and `Finalize{Retry}` sets it to `max(not_before, now)`. Today Release never updates it (`FINALIZE_SET`, `apply.rs:5849-5853`) and Retry sets it to `not_before` (`:5901-5909`). |
| `write` multi-write unit, gate ops, `Extend`, `Clear`, `Delete`, `Finish` with `None` (X) | `commit_transition` parity path (`turso_compose.rs:2553-2568`; `turso_parity.rs:545-600`) | [B] Generalize `prepare_commit_transition` (`commit_surface.rs`) from a claim plus fence to N keyed preconditions, emitting `Push` / `UpdateFields` (held or plain) / `Finalize` / `RenewLease` / `PurgeItems` / `MutateItems{invalidate}` / `SetGates` (`"*"` maps to `PauseQueue`/`ResumeQueue`) in one atomic append. [S] Stop emitting `WriteSideRecords`/`AdvanceInstanceFence`, and drop `fireweed_side_records`/`fireweed_instance_fences` (`schema.rs:240-251`). Snorri stays on this path; its transition batch adds one `claim Keys` for the instance items (§8.1, Q1, gate 4). |
| `claim Next` | Packed item claim (unchanged) | [S] Token = `(batch token, item_id)`; Version = `(item_id, item_version)`. [B] Always-on gates cost one queue-prefix probe per claim (`fireweed-turso/src/projection.rs:1989-1996`), which `GateKeyPolicy::None` skips today; P3/P4 measure it. [B] The claim-time reclaim sweep (V4) and the acked-update exclusion (below). |
| `claim Group`, `WholeGroups` | Exclusive grouped path (unchanged) | [B] Return the first group truncated to its longest prefix within `max_records` and the response byte limit, with `partial_group` (C4), because pushes no longer cap group size. [S] Delete the `max_eligible_group_size` requirement and the `BatchTooLarge` check (`crates/fireweed-engine/src/claim_validation.rs:77-84`); C4 truncates through `partial_group` instead. [S] Drop the cohort branches (if Q4). |
| `claim Keys` | `claim_by_item_ids` parity | [B] Key-to-id resolution through `fireweed_items_active_key`; drop the `not_before` check for this selector; skip reasons with the C2 precedence `NotFound`, `Terminal`, `Leased`, `Blocked`, plus `NotAttempted` at the byte limit; `Invalid` when `max_records` is below the key count. [B] Extend the `RequestOutcome::BatchClaim` receipt (today `item_ids`, `lease_token`, `worker_id`) with `skipped` and `partial_group`, so C8 replays them as recorded. |
| `claim Where` | `claim_by_query` parity (unchanged) | Hits map to keys. The insertion-sequence tie-break is ascending in both directions. |
| Claim-by-key only (`due: None`) | — | [B] Stored as the `not_before = i64::MAX` sentinel. Claims ignore it; `Keys` ignores `not_before`. No new lifecycle value. |
| `read` | `committed_live_items` after the coverage wait | [S] Unchanged: V3 keeps the coverage wait. There is no prefix read. |
| `stats` | Maintained counters (`fireweed-turso/src/projection.rs:1690-1698`) | [B] Groups: a separate, un-gated structure maintained on every change to Pending membership (insert, Claim, Finalize, LeaseExpired, Release, `not_before`/rank updates, Purge). For example, a per-group `earliest_ready_at` column updated outside the gated recompute, plus the index `(tenant, queue, earliest_ready_at)`; `ready_groups = SELECT count(*) FROM (… WHERE earliest_ready_at <= ? LIMIT n)`. The existing `fireweed_group_summary` recompute cannot serve S2: it anti-joins blocked item gates and is deliberately stale after Claim/Complete (`apply.rs:3703-3712`; `fireweed-turso/src/projection.rs:607-626`), so a group whose ready members were all claimed would keep stale values without bound, and 7S's `hasNextTasks` would fire permanently. S1 counts include expired rows not yet swept. |
| Versions | `item_version` +1 in every apply arm | [B] `LeaseExpired` stops bumping `item_version` (`apply.rs:6073-6084`, plus the in-memory arm). This is safe: `commit_validate` fences on state, token and expiry first (`fireweed-projection/src/lib.rs:4484-4510`). |
| Lease reclaim and expiry | `ReclaimDriver::tick` exists but embedded use is host-driven (`crates/fireweed/src/lib.rs:5065-5090`); the server runs it every 1 s (`fireweed-server/src/lib.rs:3435-3460`) | [B] The constructor owns a tick task at interval Δ, scheduled with `Clock::sleep_until`. Every claim first runs the reclaim sweep when `Clock::now − last_sweep ≥ Δ` (V4), which keeps Snorri's quiet queues recoverable under its stepped lease clock. Reclaim sets `eligible_since = leased_until` (§1). The tick also runs a new expiry sweep: page rows with `expires_at <= now AND lifecycle_state <> 'Leased'`, append logged `PurgeItems` (deterministic replay) in generation-sized batches. It needs an `expires_at` column and a partial index; `Finalize` sets `expires_at` per W6; the internal `client_item_key_retention_ms = 0` means no tombstones. |
| `scan`, `aggregate` | `fireweed-turso/src/query.rs` generators, after the coverage wait (`turso_compose.rs:2310-2334`) | [S] Hits carry `key` and `state`; `bounded_mutation` and `query_index*` are deleted. [B] Index keys derive from `fields`. The `index_fields` blob is retired. [B] Reject encoded index keys over 1024 B (W1/W8); nothing checks today. |

**Queue definition mapping (adapter only):**
- Int64-ascending priority over `rank`, Strict ordering;
- gates `Dynamic`/8, `max_gates_per_request = None` (§4 bounds `Unit.gates` instead);
- no cohort;
- the default `recurrence`: the adapter never emits `Finalize{Rearm}` (settle row above);
- `max_attempts = u32::MAX`;
- `terminal_retention_ms` from the definition;
- `client_item_key_retention_ms = 0`;
- `max_eligible_group_size = None`. With `Some`, `validate_push_shape` rejects every ungrouped item (`crates/fireweed-engine/src/async_composed.rs:1718-1722`, pinned by the unit test at `:4684-4721`), and `validate_claim_compatibility` requires the setting and returns `BatchTooLarge` for any group-batching claim whose `max_items` is below it (`claim_validation.rs:77-84`). [S] S4 deletes both, next to the push-time group-size rejection (`:655-676,1737`);
- `typed_indexes` from `indexes`.

**The acked-update-vs-claim skip** (`docs/helix/04-build/one-projection-cleanup.md:139-144`) is incompatible with W2/W11, because `Applied` must mean applied. Sharing the per-queue sequencer does not make it unreachable today, so closing it is **[B] work in S4/S7**, not an assertion:
- Within a generation, all works are validated first (`validate_inert_mutation_generation_folding`, `turso_compose.rs:4302-4303`) and claims are realized afterwards (`realize_accepted_claims`, `:4310`). The claim exclude set is only `leased_ids ∪ terminal_ids` (`claim_exclude_ids`, `:281-291`), so an update accepted as Pending in the same generation can still be claimed, and if the claim envelope precedes it, the update's apply skips.
- `validate_batch_update` plans against `batch_items` and never consults the overlay's `leased_ids` (`async_composed.rs:692-745`), so an update can be accepted for an item claimed in an earlier, unapplied generation.
- Item claims never wait for apply (source-audit assertion, `turso_compose.rs:1089-1092`), so a claim can lease a row under its pre-update `due` or gates, which violates V2/C5 even when no skip occurs.

Required changes: `claim_exclude_ids` includes ids with accepted-but-unapplied updates (current and in-flight generations); `validate_batch_update` rejects ids in the overlay's `leased_ids` with `Leased`; gate ops acknowledged through route X are visible to later packed claims. Proof: a planner test that interleaves `Put{due: future}` and `Claim` in one generation and across two unapplied generations. The cost is inside the P2/P4 gates (risk 8). The exclusion MUST clear within Δ of acknowledgement (apply lag ≤ Δ); otherwise the claim waits for coverage of the excluded ids, so C5a holds. The apply-time skip then stays as an asserted invariant with a counter.

**Perf gates.** Every S3×Turso slice must pass:
1. `ss_phased_capacity` at ≥ evidence `1788659385` minus 5%, with an exact final residual. P1 pushes carry no id; P3/P4 include the always-on gate probe and P2/P4 the acked-update exclusion:
   - P1 ≥ 13,768/s
   - P2 ≥ 17,901/s
   - P3 ≥ 12,699/s
   - P4 ≥ 16,232 items/s
2. New P2p, P3p and P4p runs on a Snorri-shaped queue with 22 typed indexes (§8.2). The patch touches 1 indexed field and 5 unindexed fields, sized with Snorri's measured transition bytes; Snorri's large blobs (`next_state`, `audit`) ride `payload`, not `fields` (D9, §8.1), and the P4p payload uses their measured size. P4p is claim plus `Finish` with a patch, and P4r is claim plus `Release{due}` with a patch. Each must be ≥ today's `batch_update`/`mutate_items` baseline on the same queue minus 5%. The baseline is measured before S4 lands.
3. The 32-shard workflow qualification: worst shard ≥ 9,500/s and total ≥ 11,220/s.
4. A Snorri-shaped transition bench ≥ today's `commit` on Parity minus 5%. It includes the per-batch instance `claim Keys` (Q1), the `by_tx_request` index row, and Snorri's measured audit/state bytes in the settle patch and payload.
5. A new P5 steady state: ingest plus claim/finish plus expiry at 60 s retention for 10 min, with the residual bounded. Variant P5s: the same on the Snorri-shaped queue (22 indexes, §8.2) at 24 h retention, with the injected `Clock` advanced so the 24 h window turns over within the run.

### 7.3 Direct PostgreSQL (`fireweed-pg`, depends only on `fireweed-contract`)

| Op | Execution |
|---|---|
| Schema | `records(queue, key COLLATE "C", seq bigint, ver int, state, due, rank, grp, fields jsonb, payload bytea, expires_at, attempts, lease_token_hash, leased_until, pending_since, finished_at, PK(queue,key))`; `record_gates`, `blocked_gates`, `index_entries(queue, idx, esf_key bytea, seq, key)` (`esf_key` ≤ 1024 B fits a btree entry), `group_summary(queue, grp, earliest_ready_at)` with an index on `(queue, earliest_ready_at)`, `requests(queue, request_id, fingerprint, response, expires_at)`, `queues(def)`. `Version = (seq, ver)`: `seq` is the incarnation and the tie-break. |
| Insertion sequence | `seq` comes from a per-queue sequence: reserve a block of n values and assign `seq = base + ordinality` over the request's inserts in unit order, then write order (`unnest(…) WITH ORDINALITY`). A plain `bigserial` filled by `INSERT … SELECT FROM unnest` does not guarantee array order (§4 Ordering). |
| `define_queue` | `INSERT … ON CONFLICT DO NOTHING RETURNING`, then compare definitions (§2.1 D2). |
| `write` | One transaction per request: when an id is present, check `requests` for replay; `SELECT … FOR UPDATE` on all keys of the request `ORDER BY key` (W1 makes this one lock set); evaluate units in memory (W2a); apply accepted units set-based (`INSERT … SELECT FROM unnest … WITH ORDINALITY`, `UPDATE … FROM unnest` with `fields = (fields - $rm) \|\| $set`, `DELETE`); maintain `record_gates`, `index_entries`, `blocked_gates` and `group_summary`; insert the `requests` row when an id is present; `COMMIT` is the ack. |
| `claim` | Scheduled selects: one CTE, `WHERE ((state='pending' AND due<=$by) OR (state='leased' AND leased_until<=$now)) AND NOT gates-blocked … ORDER BY rank, seq LIMIT n FOR UPDATE SKIP LOCKED`, then `UPDATE … RETURNING`. Reclaim is a predicate: every read, count and sweep treats `state='leased' AND leased_until <= $now` as Pending (`$now` is bound from the injected `Clock::now()`, never SQL `now()`), with `pending_since = leased_until`. Δ is the sweep interval (≤ 5 s). `WholeGroups`: lock group heads with `SKIP LOCKED`, drop any group not fully locked (whole-group omission, C4). `Keys`: a blocking `FOR UPDATE` on at most 1000 keys, so classification is exact (C2). `Where`: join `index_entries`. |
| `read`, `stats`, `scan`, `aggregate` | Plain SELECTs on committed state, so V3 holds by construction. `stats.groups` reads `group_summary` over the `(queue, earliest_ready_at)` index, mirroring §7.2; the summary is maintained in the same transaction as every Pending-membership change. A `SELECT DISTINCT grp … LIMIT n` would walk every group when few are ready, which breaks S2's cost bound. The ESF encoder comes from `fireweed-contract`. |
| Time | Every Δ on the injected `Clock`: bounded `DELETE` sweeps for `expires_at` and `requests`, and a sweep that materializes lapsed leases (`state='pending'`, `pending_since = leased_until`) and folds them into `group_summary`. |

### 7.4 Implementation slices (separate worktrees, disjoint files)

| # | Goal | Files owned | Proof | LOC | Deps |
|---|---|---|---|---|---|
| S0 | Split `turso_compose.rs` into lane modules (a pure move) and stub new modules | `crates/fireweed/src/turso_compose.rs`, `turso_parity.rs` → `crates/fireweed/src/turso/{mod,generation,claim,finalize,parity,ports,tick}.rs`; `mod` lines in `lib.rs`; empty `store_turso.rs` | Builds; the source-audit test (`turso_compose.rs:7384-7430`) moved; P1–P4 unchanged | ±7.5k moved | — |
| S1 | Contract crate | new `crates/fireweed-contract/**` | Unit tests for W1/D4/C-shape validation, fingerprint stability, `Version` and `Value` ordering, ESF round-trip and the 1024 B index-key bound; the §2.0 types and `Clock` trait; a clause-ID manifest test (every public item cites a clause) | ~1.6k | — |
| S2 | Black-box suite | new `crates/fireweed-contract-suite/**` | `run_all(ctor, reopen, Δ, clock, fault_hook)`; one scenario per clause, including W2a precedence, C2 `NotAttempted`, C5a progress, §4 insertion order, `attempts` unchanged across a release and re-schedule, claim → stats → complete → stats, and release → `stats.oldest_ready_since`; a torn-read adversary for `aggregate` and `stats`; deep scenarios from `fireweed-conformance` (replay, fencing, ordering, unknown outcome through the fault hook, durability through `reopen`) | ~3.5k | S1 |
| S3 | Docs | new `docs/helix/02-design/contracts/API-006-kernel-contract.md`; supersession headers on API-001/002/004/005; PRD FR-45/47a/47c/48/55; TD-016 constants; TD-010 claim note; `AGENTS.md` rule → `TursoAdmin::projection_control()` | Clause IDs match S1; `ddx doc stale` clean | ~0.8k doc lines | S1 |
| S4.1 | Command types (lands first) | `crates/fireweed-engine/src/command.rs` | serde-default fields: `field_patch`, gate add/remove, `expires_at`, and a no-reset flag for `eligible_since`; old envelopes still decode | ~0.2k | — |
| S4 | Planner and overlay | `fireweed-engine/src/{async_composed.rs, commit_surface.rs, claim_validation.rs, finalize_validation.rs}` | Planner tests for W2a–W7; packed Release/Retry/Finish, never Rearm; the token fence rejects stale, expired and foreign tokens; per-record `Exists`; N-key transition; deletion of the ungrouped-item rejection (`async_composed.rs:1718-1722`), the push-time group-size rejection and the `claim_validation.rs:77-84` requirement and `BatchTooLarge`; `validate_batch_update` rejects overlay-leased ids; the interleaved update/claim planner test (one generation and two unapplied generations) | ~1.8k | S4.1 |
| S5 | Apply, schema and in-memory parity | `fireweed-relational/src/{apply.rs, schema.rs, codec.rs, sql.rs}`, `fireweed-projection/src/lib.rs` | NULL-safe `json_patch` field patches on the general set-based tier; gate add/remove; no-reset `eligible_since`; batched rekey; `expires_at`; `LeaseExpired` without version bump and with `eligible_since = leased_until`; Release/Clear set `eligible_since = now` and Retry sets `max(not_before, now)`; the un-gated `earliest_ready_at` structure maintained on every Pending-membership change; live == rebuild via `projection_control()` | ~1.5k | S4.1 |
| S6 | Turso projection reads | `fireweed-turso/src/{projection.rs, query.rs, local.rs, tx.rs}` | `Keys` claim with reasons and `NotAttempted`, token batch read, stats groups `LIMIT` over the un-gated index (test: claim, stats, complete, stats), expiry page, scan hits with keys, the 1024 B index-key check | ~1.2k | S5 (schema) |
| S7 | S3×Turso `Store` adapter | `crates/fireweed/src/turso/**`, `store_turso.rs`, new `crates/fireweed/tests/kernel_capacity.rs` | S2 green; envelope-kind parity test; all §7.2 perf gates; the tick and the claim-time sweep drive V4 within Δ on the injected `Clock`; `claim_exclude_ids` covers accepted-but-unapplied updates, and route-X gate ops are visible to packed claims | ~2.8k | S0, S1, S4, S5, S6 |
| S8 | Facade and helpers | `crates/fireweed/src/{lib.rs, facade.rs, operator.rs}` | Only `Store`, builders, `operator`, `s3turso::open` and `TursoAdmin` are public; S2 passes through the facade | +0.8k / −6k | S7 |
| S9 | Deletions | cohort, side-record, fence and `ReplacePending` code in engine, relational, turso and projection; discovery; `claim_across_queues`; operator plane (including `operator_redrive`); `retained_items`; `current_position`; `crates/fireweed-server/src/change_record_sink.rs` and its config plumbing (it serialized internal envelopes and had no consumer); retired configs; `crates/fireweed-postgres` port backends | Workspace builds; S2 green; perf gates re-run | −15k+ | S8, Q4, Q5 |
| S10 | In-repo port | `crates/fireweed/tests/**` (39 files, ~18.7k lines), `fireweed-workload`, `fireweed-bench`, `fireweed-server` (including deleting `postgres_native.rs`) | `ss_*` and workflow qualification on the kernel; server green | ±8k | S8 |
| S11 | Snorri migration | snorri `crates/snorri-fireweed/src/lib.rs` | Snorri acceptance on the new pin; transition bench; the instance item claimed by key (Q1); gate-membership pause and suppress (§8.2); `by_tx_request`; 24 h `terminal_retention`; the upsert mapping; the `by_run_target_key` key-space change (D7) | ~2k edits | S8 released |
| S12 | `fireweed-pg` (future) | new `crates/fireweed-pg/**` | S2 green with the same torn-read adversary | ~4k | S1, S2 |

**Critical path:** S0 → S4.1 → (S4 ∥ S5 → S6) → S7 (S2 green plus gates) → S8 → S10 → release → S11. S1, S2 and S3 run in parallel from the start. S9 follows S8.

### 7.5 Risks

1. **Write ids.** They are optional (§9 D3), so id-less P1 pushes and P4 settles keep today's cost and resolve `Unavailable` by `read(Keys)` (D8). Writes that carry an id, such as Snorri's transitions, add a request row and a `RequestOutcome`. Mitigation: a compact binary outcome. Gate: 4.
2. **Token validation and held `UpdateFields` + `Finalize` fusion.** Adds one batched read per finalize member. Gate: P4/P4p.
3. **22-index rekey on patch, on the general set-based tier.** Field patches, CAS and gate edits leave the operation-shaped fast path. If P2p/P3p fail, stop and escalate. There is no silent fallback.
4. **Expiry sweep.** It adds purge work that S3×Turso never did, because terminal rows never left before. Snorri's 24 h retention on a 22-index queue makes it hot-writer work. Gates: P5, P5s.
5. **Claim-by-key-only (`due: None`) sentinel rows sit in the pending index.** A `Next` claim walks those with low rank. Measure on Snorri's shape.
6. **`partial_group` changes FR-47b for oversized groups** (§9 D5).
7. **Snorri pin v0.31.21 predates `main`.** The migration is a rewrite plus a key-space audit, including the `by_run_target_key` key change (D7).
8. **Acked-update-vs-claim exclusion.** It adds overlay bookkeeping to every packed claim and update (§7.2). Gate: P2/P4.
9. **Instance-item claim per transition batch** (Q1). One extra `claim Keys` on the parity lane. Gate: 4.

---

## 8. Snorri and Seventh Sense migration

### 8.1 Where Snorri's side-record data goes (all written in the same unit)

| Today (Snorri `lib.rs:13265-13361, 13524-13770`) | Carried by | Written as | Expiry |
|---|---|---|---|
| Instance projection `snorri:v1:instance:{id}` + fence `snorri:v1:projection-fence:{id}` (`expected=rev-1 → next=rev`) | **Instance item** `snorri:v1:instance:{id}`: a work item claimed by key. Each transition batch issues `claim(Keys[instance keys of the claimed inputs])`. The claim is V2-fresh, and its lease (C6) is the instance lock, which replaces both the fence and the `VersionMismatch` reload loop (today's `ProjectionConflict` retries under a budget of 8, snorri `lib.rs:153-190`). The projection lives in `payload`. Fields `record_kind: "instance"` and `idempotency_key: {id}` put it in `by_record_kind_key` for the instance listing (§8.2). | First transition: `{Absent, None, Put{due: Set(None), payload}}`. A concurrent first transition gets `Exists` and retries through the claim. Later transitions: `{Any, Release(t_inst), Put{payload: Set(projection)}}`. Final transition: `{Any, Release(t_inst), Finish(Completed, Patch{payload})}`. A `Leased` skip on the instance means another batch holds the lock: release that input and retry it. Only the start input inserts the instance item; a `Terminal` or `NotFound` skip for an instance that has already started fails the input. A workflow whose first transition is also its last is Q1(c). | None while the instance is live. The final `Finish` gives `finished_at + terminal_retention` (24 h). (Q1 confirms the cost.) |
| Transition metadata `snorri:workflow:transition:{rid}` | **Settled input item** fields `tx_request_id`, `instance_id`, `state_seq`, plus index `by_tx_request (tx_request_id)` | The settle patch: `{Any, Release(t), Finish(Completed, patch)}` | The input item's `expires_at` (Q2) |
| Input ack `snorri:workflow:input:{event}` (point-read by `durable_input_ack`, `lib.rs:7127-7143`) | Settled input item field `consumed_event_id`, plus index `by_consumed_event` | The settle patch. The ack lookup becomes `scan(by_consumed_event, eq=[event], limit 1)`, which observes every acknowledged settle (V3), so delivery status still comes from durable state. | The input item's `expires_at`: at least Snorri's 24 h dedup window (Q2) |
| `state:{inst}:{rid}`, `audit:{inst}:{rev}:{rid}`, `audit-digest:…` | Settled input item: `audit_digest` in `fields`; `next_state` and `audit`, KB-sized blobs, in the off-row `payload` (the settle patch replaces it whole with `{next_state, audit}`; nothing reads the input's original payload after settle, and Snorri folds it into the blob if it ever needs it; D9 keeps blobs off the hot row). Index `by_instance_revision (instance_id, state_seq)` for hydration. | The settle patch | The input item's `expires_at` (Q2) |
| Await index `snorri:v1:await-index:{k}` | **The await item** `await:{k}` (fields `instance_id`, `queue_id`). `due: None` (claim-by-key only) or `due` = deadline; consumed by `claim Keys`. | `{Absent, None, Put}` in the emitting transition | None while pending. When consumed it is the settled input of that transition, finished with `expires_at: Set(None)`: it is retained until `Delete`, so a replayed delivery still classifies as `AlreadyConsumed` rather than `NoSuchAwait` (snorri-core `lib.rs:897-909`). (Q2) |
| Effects and continuations (`lifecycle_items`) | The effect and continuation items | `{Absent, None, Put{due, rank, fields}}` | `terminal_retention` (24 h, §8.2) after `Finish` |
| Replay markers (bootstrap, migration, recovery) | Deleted | Request-id replay, as for `STATE_STORE_COMMIT_KEY_PREFIX` | — |
| Migration and recovery audit markers | The claimed instance item's settle patch (`fields`) | `{Any, Release(t_inst), Put{fields}}` in the migration or recovery unit | The instance item's |
| Shard manifest `snorri:v1:workflow-shard-manifest` (`lib.rs:3244-3245`) | Open: §10 Q1 (move it out of Fireweed, or name it as the one config record) | — | — |
| State-store commit record `STATE_STORE_COMMIT_KEY_PREFIX` | Deleted | Request-id replay covers it | — |

**The transition** (today's `commit` is a claim, then a finalize, then 6+ side records, then a fence). The write carries the transition's `request_id` (D3):
```
claim(Next | Where)                         → input items and tokens t_in   (unchanged)
claim(Keys[instance keys of those inputs])  → instance tokens t_inst: the instance lock (one extra claim per batch, Q1)
Unit { gates: [], writes: [
  { input_key,                  Any,     Release(t_in),   Finish(Completed, Patch{ fields: {tx_request_id, instance_id,
                                                           state_seq, consumed_event_id, audit_digest},
                                                           payload: Set({next_state, audit}) }) },  // expires_at per Q2
  { "snorri:v1:instance:{id}",  Any,     Release(t_inst), Put(Patch{ payload: Set(projection) }) },
  { "effect:{k}:attempt:1",     Absent,  None,            Put(Patch{ due: Set(Some(t)), rank, fields{record_kind:"effect",…} }) },
  { "await:{k}",                Absent,  None,            Put(Patch{ due: Set(deadline_or_None), fields{instance_id, queue_id,…} }) } ] }
```

Snorri's `record_kind` is Snorri's own indexed field; the contract does not define it.

**Net rows per transition:** 0 new rows for metadata, instead of 6+ side-record upserts. The input row is patched in place. The instance item is claimed (one extra lease in the batch's `Keys` claim) and settled in place.

### 8.2 Snorri call sites

| Snorri today | New |
|---|---|
| `create_queue` | `define_queue`: today's 19 `IndexDef`s over `fields` (without `unique`), plus the §8.1 indexes `by_tx_request`, `by_consumed_event` and `by_instance_revision` (22 of 32). `terminal_retention` 24 h, matching Snorri's `client_item_key_retention_ms` (`lib.rs:16629-16631`): Snorri has `Finish` paths outside §8.1 (lifecycle commits at `lib.rs:3551, 4897, 5737, 6441, 6714, 7445, 7528, 7607`; state-store facade commits at `8148`), and at 60 s a facade retry of `create_work_items` or `push` with the same key after 60 s would insert duplicate work. S3×Turso never reaped these rows before, so this is new purge work on the hot writer (gate 5, P5s). `metadata`, `index_fields` and `entity` fold into `fields`. |
| `push`, `push_with_request_id`, `push_batch_with_request_id` (10,000/batch) | `write` insert units, chunked to at most 1000 per request, with request ids `{rid}:{chunk}` |
| `upsert` / `create_work_items` (`lib.rs:7682-7706`, "Create or replace", Inserted/Replaced) | `{Any, None, Put{clear_fields: true, fields: <all>, payload: Set, due: Set, rank: Set, group: Set(g), gates: Replace}}`, which replaces the item as today's supersede does (`crates/fireweed/src/turso_parity.rs:138-189`); a bare `Put` would leave stale fields behind. `created` gives Inserted vs Replaced. `Leased` and `Terminal` rejections surface as today's errors, or S11 adds a Skipped status (`WorkflowItemCreateStatus` has only Inserted and Replaced, `lib.rs:1546-1551`). A group change is `Invalid` (W8). |
| `claim`, `claim_at` | `claim(Next, due_by)`, looped for more than 1000 |
| `claim_with(metadata_equals)` intents; push → `claim_with` → `commit` dances (bootstrap, migration, manifest) | Replay markers become request-id replay. A migration or recovery is one unit that settles the claimed instance item (`claim Keys`, then `{Release(t), Put{fields: audit}}`). The shard-manifest bootstrap is §10 Q1. The marker, purge and filtered-claim steps disappear. |
| `claim_by_query(_at)` | `claim(Where, due_by)` |
| Key-range claim plus release of extras | `claim(Keys)` with skip reasons |
| `commit`, `commit_multi_claim` | As in §8.1: one `claim Keys` for the batch's instance items, then units with several `Release`/`Finish` writes each |
| `ack`, `nack(Release)` | `{Release(t), Finish(Completed)}`, `{Release(t), Put{}}` (`Finalize{Release}`) |
| `reschedule` (`lib.rs:11226-11260`, lease-preserving) | `{Version, None, Put{due}}`. `Leased` is counted as `conflicted`, which Snorri already reports (Q3). |
| `mutate_items` live mutations | Keep today's gate-membership scheme, which the clauses already support. `snorri.paused`, `snorri.suppressed`, `snorri.failed` and each per-rule gate are blocked once, with unit gate ops. PauseRun = `scan(by_run_status, eq=[tenant, run])`, then per item `{Exists, None, Put{gates: Edit{add: {snorri.paused}}}}` (`scan` hits carry no version; a gate `Edit` is commutative, so no CAS is needed); ResumeRun removes the gate. SuppressRule adds the rule's gate to the items it selects (rules are created after their items, `lib.rs:14439-14460`). Lifecycle classification reads `Record.gates` (`lib.rs:14629-14690`; ResumeRun selection, `:14356-14376`). Run and rule mutations are paged and not atomic, unlike today's all-or-nothing `SelectFirst`; a Leased item reports `Leased` and is retried (Q3). CancelRun/CancelItems: `scan` by run, then `{Exists, None, Finish(Failed)}`, or `{Any, Clear, Finish(Failed)}` for leased items, which is valid even if the lease was reclaimed in between (W4, W6). DeleteNotStarted: `read(Keys)` for the version, then `{Version(v), None, Delete}`. |
| `purge(force)` | `{Any, Clear, Delete}` |
| `side_record`, `live_item`, `claimed` | `read(Keys)`, or the ack scan (§8.1) |
| `side_records_by_prefix` (instance listing, audit hydration) | Instance listing: `scan(by_record_kind_key, eq=["instance"])`. Audit hydration: `scan(by_instance_revision, eq=[inst])`, then `read(Keys)` for the payloads. |
| `explain_commit`; `recover_mirrored_transition_in_queue(queue_id, rid)` (`lib.rs:11893-11916`) | `scan(by_tx_request, eq=[rid], limit 1)`, then `read(Keys)` of the input item (terminal plus `tx_request_id`). The input key `{request_id}:{run_id}:{item_kind}` cannot be derived from `rid`, and a fresh handle lacks the original request body for a resend. |
| `metrics` (`lib.rs:8265-8270`) | `stats`: pending, leased, completed, failed |
| `metrics_by_query`, `grouped_aggregate`, `declared_bucket_segment`, `range_scan` | `aggregate(State)`, `aggregate(Fields)`, `aggregate(Ranges)`, `scan` |
| `reclaim_expired_at(lease_time)` (`lib.rs:7936-7946`) | Deleted. Snorri injects its lease clock as the constructor `Clock` (D12). The claim-time sweep (V4: reclaim when `Clock::now − last_sweep ≥ Δ`) keeps quiet queues recoverable without an ambient clock read, as the cited comment requires. |
| `commit_capabilities`, `hot_projection_capabilities` | Deleted |
| `projection_control` | `TursoAdmin` |
| Unique `by_run_target_key` (production, `lib.rs:16865-16874`) | The item key must carry the target identity, replacing today's `client_item_key` `{request_id}:{run_id}:{item_kind}` (`lib.rs:15238`), whose request-id redelivery dedup (`:14225`) must survive. S11 picks one key that preserves both properties, for example `(tenant, run, kind, version, target)`, with a proof that request-id redelivery yields the same target (D7). |

### 8.3 Seventh Sense

| Trait method | New |
|---|---|
| `TJobsQueue`/`TConnectorsQueue.create` (conflict-ignore) | One queue per type. `{Absent, None, Put{due: Set(Some(now)), rank: Set(now_µs), gates: {acct:a, conn:c}, fields}}`; `Exists` means ignore. `rank` = now matches `last_processed_at DEFAULT now()` (7S `interfaces/graphile/db/postgres/dump.sql:919-926`), so a new job does not jump ahead of processed ones. Create units carry no gate ops. |
| Account and connector status | The only writer of gate state. A status change fans out one gate op to each type queue, as separate, non-atomic requests (gate state is per queue, W9). A new connector's gate is set `Blocked` when the connector is created, before any of its jobs are inserted. A blind gate op inside a create unit could overwrite a concurrent status change, so create units never carry one. |
| `handleNext(type, asOf, timeout)` | `claim(Next, max 1, lease=timeout, due_by=asOf)`, then the effect, then `{Any, Release(t), Put{due: Set(Some(nextProcessingAt.getOrElse(asOf))), rank: Set(asOf_µs), fields{inactivityCount}}}` or `{Release(t), Delete}`. In 7S a NULL next time means due now (`.filter(_.nextProcessingAt.forall(_ <= lift(asOf)))`, `QuillJobsQueue.scala:77`, `QuillConnectorsQueue.scala:82`); `State.active()` and `inactive(Seq.empty)` yield it (`JobQueueItem.scala:29-30`; `MarketoEventLeadEnrichmentWorker.scala:94`). |
| JobContentLoader legacy type | It claims the legacy type across types today (`QuillJobsQueue.scala:76`). The cross-queue claim is deleted, so the legacy rows are migrated into the per-type queue at cutover. |
| `deleteByIds` | `{Any, Clear, Delete}` |
| Marketo `create` (one txn with a connector row) | Queue per connector, `marketo-lead:{c}`, created with `define_queue` before its first write (§2.1 D5: every other op fails with `QueueNotFound`). First the connector job `{Absent}` on the connectors queue, then tasks `{Absent, Put{due: Set(Some(now)), group: leadId, rank: ctime_µs}}`. These are two requests; a crash between them is harmless. |
| `takeNextTasks(asOf, N)` | `claim(WholeGroups{N}, due_by=asOf)` |
| `hasNextTasks(asOf, N, maxWait)` | `stats{groups: {limit: N, due_by: asOf}}`, then `ready_groups >= N \|\| asOf - oldest_ready_since > maxWait`. Retried tasks count from their ready time, not their ctime. |
| `updateRetryInfo` | `{Any, Release(t), Put{due: Set(Some(nextRetryAt.getOrElse(now))), fields{retryCount}}}` (the same NULL-means-now rule, `QuillMarketoEventLeadEnrichmentQueue`) |
| Marketo `delete` (the hot completion) | `{Release(t), Finish(Completed)}`, which rides packed `Finalize`, with a short `terminal_retention`. Not `Delete`, which takes route X. |
| `TMarketoEventEmailEnrichmentQueue` (`create`, `takeNextTasks`, `updateRetryInfo`, `delete`; a live worker) | The lead-enrichment mapping without groups: its own per-connector queue, and `takeNextTasks` = `claim(Next, maxTasks, due_by = asOf)`. |
| Callbacks queued `createOrUpdate` | `{Any, None, Put{fields}}`. A `Leased` rejection (the scheduler holds the record) is retried. It is a sparse `Put` without `clear_fields` and without a `ctime` field, so a conflict keeps the record's insertion order, as today's `onConflictUpdate` keeps `ctime`. Ids are deduplicated per request (W1 rejects a repeated key, which would make a whole Kafka batch `Invalid`). |
| Callbacks `findByJobId(jobId, max)` | `claim(Where{by_job, eq=[jobId]}, max_records = max)`: the records are leased for the scheduler, in insertion order (C4 `Where` tie-break). The claim is V2-fresh, so the scheduler's `findByJobId → deleteByIds → findByJobId` loop (`CallbackJobSchedulerWorker.scala:46-73`) never sees its own deletes. No record is scan-consumed. |
| Callbacks `deleteByIds` | `{Release(t), Delete}` with the claim's tokens |
| `TCallbackScheduledActionsPersistence` (`TCallbackScheduledActionsPersistence.scala:9-25`): `createOrUpdate` | One scheduled-actions queue per job. Fields `{state, state_ordinal, retryCount, scheduled, batchChecksum, callbackId, single: Bool, stats_type: Bool}` plus the stats dimensions. `rank = state_ordinal·10^17 + executed_µs`, so Scheduled comes before Failed, then by `executed`. Batch members get `group = callbackId` and a common `due = min(executed)`; singles have no group. `expires_at = scheduled + scheduledStoreExpiration`. |
| `findReadyToExecuteByJobId` (`QuillCallbackScheduledActionsPersistence.scala:55-96`) | Batches: `claim(WholeGroups{1})`. The batch is complete iff `claimed.len() == batchChecksum`, as `isFullBatch` does (`CallbackActionScheduled.scala:37-40`); otherwise release all and claim singles. Singles: `claim(Where{by_single (single, state_ordinal, executed), eq=[true]}, max_records = maxSize)`. No counter record and no gate. Results settle with the token, and every `Finish` carries `Patch{expires_at: Set(Some(scheduled + scheduledStoreExpiration))}` so the create-time expiry survives W6: `{Release(t), Finish(Completed, …)}` or `{Release(t), Put{due, fields{retryCount}}}`, and `Finish(Failed)` once `retryCount ≥ maxRetries`. |
| `calculateStats` | `aggregate(Fields[TimeBucket(scheduled, 3_600_000), Field(state), Field(isEnrolledUsingOpenRateFilter), Field(engagementClass), Field(personalizationClass)])` over `by_stats (jobId, stats_type, scheduled, state, isEnrolledUsingOpenRateFilter, engagementClass, personalizationClass)` with `eq=[job, true]`. The dimensions are index fields, as §2.7 Q4 requires. |
| `deleteExpiredByJobId` | Deleted: `expires_at` (above) replaces it (`CallbackJobExecutorWorker.scala:102-112`). |

**FR coverage:**
- FR-44: `rank` is the timestamp.
- FR-45: gates, including caller gates for static blocks.
- FR-46: insert now, `Put{due, rank}` later.
- FR-47 and FR-47a: tenant, queue, `group`, `fields`.
- FR-47b: `WholeGroups`, with `partial_group` for oversized groups.
- FR-47c: Q4 option 2 (`WholeGroups{1}` plus the `batchChecksum` check at claim).
- FR-55: `{Release(t), Put{due: next}}` forever (never `Rearm`, so `attempts` keeps counting), `Delete` to remove.

---

## 9. Decisions made on the owner's behalf

| # | Decision | Rationale (overturnable) |
|---|---|---|
| D1 | Keep terminal states (Completed/Failed) plus per-record `expires_at`. No tombstones and no writable-idle "complete". | Lowest production cost: completion stays the packed `Finalize` (P4). Fewest time-driven transitions: 2 (V4; request-id retention is a MAY, V5), versus today's lease, cohort, tombstone and request transitions. Snorri's completed/failed counts survive. Expiry is new work in every option (§7.1). |
| D2 | A non-holder `Put` on a Leased record is rejected per record with `Leased`. Only the token holder patches, through the lease sub-op. | The operation-shaped packed update is pending-only (`apply.rs:5129`; API-001:347; `one-projection-cleanup.md:139-144,333`), and the planner must enforce the same for the general tier, which accepts Leased rows (§7.2). Allowing non-holder writes needs an overlay rewrite and reopens the lost-update race. Snorri's cost is Q3. |
| D3 | Claim and write `request_id` are both optional. Id-less writes resolve `Unavailable` by `read(Keys)` (D8); an id gives replay. Snorri's transitions carry ids. | Keeps the durable receipt off the hot paths: a claim with an id writes it (`turso_compose.rs:4486-4500`; replay read at `:5400-5420`), and P1 pushes carry none today (`ss_phased_capacity.rs:152-164`). Revised in the verification pass, which found the old "writes need unknown-outcome resolution" rationale contradicted D8. |
| D4 | `Version = (incarnation, seq)`: `item_id` and `item_version` +1 on Turso; `seq` and `ver` on PostgreSQL. Lease expiry does not change it. | No position-derived versions. Avoids ABA across re-insert at zero cost (`ItemId` is `epoch<<40 \| node<<32 \| counter`, `domain.rs:116-130`). |
| D5 | Remove `max_group_size`. Never reject inserts; an oversized first group, by record count or by response bytes, is claimed partially with `partial_group`. | The Marketo `create` has no per-lead cap. The alternatives are wedging the queue (`TooLarge`) or silent starvation. |
| D6 | Gate ops live inside write units; pause is the reserved gate `"*"`; there is no `set_gates` or `pause` op. | Snorri flips gates atomically with item patches. One mechanism for pause and gates. |
| D7 | Remove `unique` indexes. The key is the only uniqueness. | One identity mechanism. The only consumer is Snorri's production `by_run_target_key` unique constraint (`lib.rs:16865-16874`), which Snorri treats as load-bearing. Snorri must make the item key encode `(tenant, run, kind, version, target)` for those items, replacing its current `client_item_key` dedup (`{request_id}:{run_id}:{item_kind}`, `lib.rs:15238`; redelivery rationale at `:14225`). S11 picks one key that preserves both properties and proves that request-id redelivery yields the same target. |
| D8 | No `read(Outcome)` and no `explain_commit`. | Replay-by-resend (writes with an id) plus `read(Keys)` of the touched items (every write) resolves every unknown outcome. Snorri locates a transition by `scan(by_tx_request)` (§8.2). |
| D9 | Two data slots: typed `fields` (sparse, hot row, indexed) and opaque `payload` (whole, off-row). | `payload` stays off the hot row so Claim/Complete do not rewrite blobs (`schema.rs:55-57`). `metadata`, `entity` and `index_fields` collapse into `fields`. Snorri's KB-sized `next_state` and `audit` go in `payload` (§8.1). |
| D10 | The operator is a helper module; Discovery, coordination and multi-queue claim leave the contract. | No consumers; the owner could not define the operator. |
| D11 | Removed in the verification pass: the `redrive` helper is deleted. | No consumer, and its Delete-then-insert was not atomic. Atomic redrive would need a terminal-revive write path. |
| D12 | The injected `Clock` governs leases, expiry and Δ. Snorri's per-request lease clock becomes the constructor `Clock`; host-driven reclaim goes away. | One semantics. The constructor-owned tick plus the claim-time sweep (V4) replaces `reclaim_expired_at` and keeps quiet queues recoverable under a stepped clock. |
| D13 | `stats.groups` ignores gates, pause and Leased members; it can overcount `WholeGroups`-claimable groups. | The existing summary recompute anti-joins blocked item gates and is deliberately stale after Claim/Complete (`apply.rs:3703-3712`; `fireweed-turso/src/projection.rs:607-626`), so it cannot serve S2. S2 uses a separate un-gated structure (§7.2); a gate-aware count would need an anti-join per group. |
| D14 | Retire FR-48 (discovery) and reword FR-45 (no `metadata_blockers`) and FR-47a (no fairness via discovery). | No consumers. |
| D15 | Per-request limits of 1000 rows and 16 MiB, key ≤ 1024 bytes, record ≤ 4 MiB, encoded index key ≤ 1024 bytes, lease ≤ 1 h. | TD-012 row and byte admission. Snorri's 10,000-row batches are chunked. The record bound keeps a claim from wedging on an unreturnable record (C7); the index-key bound keeps PostgreSQL btree entries legal. |
| D16 | `read` and `scan` are fresh; `aggregate` and `stats` may be torn (V3). Δ bounds only time-driven transitions. | S3×Turso already waits for coverage on every read (`turso_compose.rs:2035-2054, 2310-2334, 3567-3579`) and PostgreSQL is fresh by construction, so a stale-read allowance bought nothing, while Snorri's scan-then-write run mutations and input-ack lookup depend on freshness. |
| D17 | `Finish` with `expires_at: Set(None)` retains the record until `Delete`; `Keep` gives `finished_at + terminal_retention`. | Snorri's consumed awaits must be retained (`AlreadyConsumed`, snorri-core `lib.rs:897-909`). Without this, indefinite retention could only be a Pending-unscheduled record, which is a parked record. |

---

## 10. Open questions for the owner

1. **Snorri instance lock (confirmation).** Confirm that Snorri's per-instance projection lives on the instance item `snorri:v1:instance:{id}`, a work item claimed by key, whose lease is the instance lock (§8.1). It replaces both the fence and the `VersionMismatch` reload loop, and it is the only design that keeps the no-store-only rule: an unscheduled item that nobody claims would be the side record and fence under new names.
   - Cost: one extra `claim(Keys)` per transition batch on the parity lane; gate 4 measures it. The instance payload is rewritten once per transition, as the side record is today.
   - Variant: rekey Snorri's per-run `workflow.entry` item to `snorri:v1:instance:{id}` and settle it with `{Release(t), Put}` (clearing its transition index fields) instead of `Finish`. It saves the extra key but ties the entry item's lifetime to the instance.
   - Replay markers become request-id replay, and migration and recovery audit ride the claimed instance item's settle patch (§8.1), so neither leaves a never-claimed record.
   - The one store-only record left is the shard manifest `snorri:v1:workflow-shard-manifest` (`lib.rs:3244-3245`), written once at bootstrap and read at open. Options: (a) move it out of Fireweed into Snorri's deployment configuration; (b) name it as the one sanctioned config record, an explicit exception to the rule.
   - (c) One-shot workflows, where the first transition is also the last: W1 forbids naming the instance key twice in one request, and `Finish` on an absent key is `NotFound`. Options: (i) a new W2a branch where `{Absent, None, Finish(o, patch)}` inserts the record already finished, which is one more rule for every implementation; (ii) no instance item for one-shot workflows, which is a Snorri-side special case.
   - *Recommendation: confirm the instance item; (a) for the manifest; (i) for one-shot workflows.*
2. **Retention of Snorri acks, audit and consumed awaits.** These live on terminal items. Today the side records live forever, and on S3×Turso so do the terminal rows.
   - Consumed awaits finish with `expires_at: Set(None)` and are retained until `Delete`, so a replayed delivery still classifies as `AlreadyConsumed` (snorri-core `lib.rs:897-909`). They accumulate, as today's await index does (Snorri never deletes it, `lib.rs:1305-1320`).
   - Input items: `Keep` gives `finished_at + 24 h` (`terminal_retention`, §8.2). A longer audit retention is an explicit `expires_at: Set(Some(finished_at + audit_retention))` on the settle, configured in Snorri (default 30 d).
   - The instance item itself now expires `terminal_retention` (24 h) after the final transition. Today the instance projection side record is kept forever. The final transition MUST also `Delete` any outstanding `due: None` await items of the instance, or they would never be claimed.
   - *Recommendation:* as above. Is indefinite audit a requirement? If so, input items also finish with `Set(None)`, and Snorri deletes them itself. Is indefinite retention of finished instance projections a requirement? If so, the final transition finishes the instance item with `Set(None)`.
3. **Snorri's lease-preserving reschedule** (`lib.rs:11237-11250`). Costs of each option:
   - (a) *Reject with `Leased`, counted as `conflicted`.* Zero engine cost. A reschedule that races a worker is lost until the caller retries.
   - (b) *Force: `Clear` + `Put{due}`.* Fences the worker, wasting its work and causing redelivery.
   - (c) *Allow schedule-only non-holder writes.* Relaxes the pending-only guard on the packed path, requires overlay tracking of claim versions, and rewrites API-001:347. Production-blocker risk.
   - *Recommendation: (a).*
4. **FR-47c cohorts.** Two options, with costs:
   - (1) *Keep an opt-in cohort*: `cohort_size` on insert, `Select::Cohort`, settle-together, and a completion deadline that turns Failed.
     - Costs: the `fireweed_cohorts` machinery stays; +1 time-driven transition (V4 becomes three); PostgreSQL needs a cohort table, a counter lock and a sweeper; 3 more contract clauses; §0 and §3 grow as noted there.
   - (2) *Delete cohorts.* Batch members carry `group = callbackId` and a common `due`, and `claim(WholeGroups{1})` returns a batch whole (C4 allows only whole-group omission). Completeness is checked at claim: `claimed.len() == batchChecksum`, as `isFullBatch` does (`CallbackActionScheduled.scala:37-40`); otherwise the caller releases all and claims singles, which is today's fallback in `findReadyToExecuteByJobId`. No counter item and no gate (§8.3).
     - Members do trickle in: queued actions go through Kafka, and the scheduler creates scheduled actions per `findByJobId(jobId, batchSize)` chunk (`CallbackJobSchedulerWorker.scala:47,72`), so a batch that straddles a chunk arrives in several units. The claim-time check handles that without a counter.
     - Costs: an FR-47c rewording; an incomplete batch at the head is leased and released on each poll until it completes (today it is re-read); a `by_single` index for the mixed singles/batch queue.
   - *Recommendation: (2).*
5. **Existing durable data.** Is a fresh store acceptable (a pre-1.0 break), or must the new build replay existing logs (`WriteSideRecords`, `AdvanceInstanceFence`, `ReplacePending`, cohort commands) and convert side records?
   - *Recommendation:* a fresh store, plus a one-shot Snorri export/import through old and new pins. This sets the size of S9.
6. **RESP adapter.** It binds engine ports and uses consumer names as lease tokens (API-001:638-645).
   - *Recommendation:* feature-gate it off at the break, then retire it or rebind it onto `Store` later.
7. **Adding an index to an existing queue** (Snorri SS1a).
   - *Recommendation:* defer. `define_queue` stays create-or-verify.

---

## 11. Judge defects: resolutions

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

