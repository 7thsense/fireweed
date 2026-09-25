---
ddx:
  id: adr-kernel-records-contract
  depends_on:
    - spike-record-key-storage
    - spike-record-field-storage
    - spike-claim-path-at-scale
    - spike-turso-value-size-boundary
    - spike-declared-field-storage-layout
  links:
    - {kind: informed_by, to: spike-record-key-storage}
    - {kind: informed_by, to: spike-record-field-storage}
    - {kind: informed_by, to: spike-claim-path-at-scale}
    - {kind: informed_by, to: spike-turso-value-size-boundary}
    - {kind: informed_by, to: spike-declared-field-storage-layout}
    - {kind: informed_by, to: adr-023-pre-release-fireweed-namespace-cutover}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
  status: accepted
---

# ADR-025: The external interface is the eight-operation kernel records contract

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-003, API-006, ADR-023, ADR-024, TD-017, TD-018, SPIKE-002 to SPIKE-006 | High; the storage consequences are measured (SPIKE-002 to SPIKE-006) |

## Context

**Today's external interface is not a contract.**
- The public surface has about 100 entry points: 74 `Fireweed` methods, 4 `ProjectionControl` methods, 10 `operator_*` methods and 12 free functions, plus `pub mod turso_compose`.
- Only about 15 of them are semantic core. The rest are aliases, stubs, test fixtures, backend introspection (`commit_capabilities`, `current_position`, the snapshot family), and retired configuration kept only so it can be rejected.
- Several methods have no normative text.
- Durability and visibility are stated in terms of the object log ("durable on the object log", "a claim polls applied rows") instead of as semantics.
- The deep conformance suite runs only against engine ports (`LibBackend`, a 21-trait bound that includes log and snapshot traits). The public-level suite has the right shape but only smoke depth, and it carries backend knobs.

**A shared internal seam bends.** `PostgresRelationalBackend` was plugged in at the engine-port seam, and the seam absorbed it: lowest-common-denominator defaults, capability flags exported as API, and dialect forks. The seam for a second implementation must be the external contract, not an internal port.

**Consumers need little.** Seventh Sense's queues reduce to four primitives: idempotent create or upsert; take (eligible, ordered, bounded, exclusive, optionally whole groups); settle; and delete by key. Snorri calls about 24 methods, at a pin (`v0.31.21`) older than `main`.

**The storage consequences are measured.** Five spikes measured keys, fields, value sizes, field layouts and the claim path on Turso up to 100M records (SPIKE-002 to SPIKE-006).

## Decision

Fireweed's external interface is the kernel records contract, API-006 (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`). API-006 is normative. This section states the decisions it rests on, each with its reason.

### 1. The kernel contract is the external interface

- **API-006 is the interface.** It is the eight operations of the object-safe `Store` trait (`define_queue`, `write`, `claim`, `read`, `stats`, `scan`, `aggregate` and `delete_queue`), their DTOs and errors, the contract constants, and the rules for durability, visibility, atomicity, ordering and leases. All of it lives in the I/O-free crate `fireweed-contract`, and `fireweed::Fireweed` wraps `Arc<dyn Store>` and delegates 1:1.
- **Every op is mandatory for every implementation.** There are no optional facets, capability flags, backend identity, log positions or commit explanations (API-006 §8). Query (`scan`, `aggregate`) is part of `Store`, because consumers use it on operational paths.
- **Nothing else is public.** Every other entry point becomes a `write`, `claim` or `read` composition, moves to the implementation's admin handle, or is deleted.
- **Hard pre-release cutover,** as ADR-023 did for names: no aliases, no compatibility layer, no import of existing data and no replay compatibility with earlier command formats. Fireweed is pre-production.
- **Semantics are stated as semantics.** Durability is DUR1. Visibility is V1–V5, with one documented Δ of at most 5 s that bounds only lease reclaim. The contract never names the object log.
  - S3 object log × Turso (ADR-024) is the one production implementation. TD-017 and TD-018 design it.
  - A second implementation, such as an isolated PostgreSQL unit, would implement `Store` directly and depend only on `fireweed-contract`, never on engine ports. This ADR commits to no second implementation.
- **One black-box conformance suite.** It runs in-process against `Arc<dyn Store>` (CS1–CS7), with no backend knobs. Every scenario runs twice: directly, and through a torn-read adversary for `stats` and `aggregate`. The cost rules (C9, the S2 cost bound, F4's read rule) are verified by performance gates.
- **The contract grows only by spec.** Every public item of `fireweed-contract` traces to a clause id, and clause ids are never reused. An addition that one implementation or one consumer needs, with no clause behind it, is rejected. A change to the model needs a new ADR.

### 2. The records model

1. **The model is: add records (insert or upsert), claim them for work, update them, repeat.** Reason: it covers what Snorri and Seventh Sense do with a queue, and nothing more is needed.
2. **The upstream system owns record keys:** opaque bytes, 1..=256 B, unique per queue. A larger natural key is hashed upstream. Reason: the caller already has the identity. Key cost is linear in length with no cliff (SPIKE-002), and engine-side hashing would need a collision re-check on every conflict.
3. **Stages are declared and none is special.** A queue declares an ordered list of 1..=32 stages; the first is the insert default, and `[INITIAL, COMPLETED]` is the default list. Any write may move a record to any declared stage. There is no terminal flag, no Completed or Failed state and no transition graph. Reason: a stage names a kind of work to claim. A stage that nothing claims is already "done" to the consumer, so the engine needs no lifecycle of its own.
4. **`rank: i64` is the single client-controlled ordering key.** Its default is the insertion time in µs. A claim takes the records of its stage with `rank <= horizon` (default now), in `(rank, insertion sequence)` order. A future rank schedules a record, and a priority bucket is an offset added to the rank. There is no `due`. Reason: rank ordering is the product's core, and one ordering key keeps a claim to one index seek (C9). `due` duplicated what rank and the horizon express.
5. **Claims are rank-ordered only.** A claim selects by `Next`, `WholeGroups` or `Keys`; no claim orders by a declared index. Claiming by query is a stage per kind of work, `WholeGroups`, or a `scan` followed by a `Keys` claim. Reason: a second claim order competes with rank, and a claim over an index that does not lead with the stage is stage-blind, which SPIKE-004 measured at 5,000× or more slower.
6. **A claim is one batch under one token and one `until`.** `Extend` renews the whole claim; `Release` or `Delete` detaches one record, and the claim's other records stay attached. A claim is live while `until > now` and at least one record is attached, and a claim that returns nothing creates none. Reason: the claims table (SPIKE-004 L2) makes extend about 80× and expiry discovery about 75× faster, because both cost O(claims) instead of O(claimed records).
7. **A claim has no holder identity.** `claim` takes none, and the claims table has no `holder` column. Whoever presents the token is the holder. Reason: the token is the only authority, and an identity with no semantics would be a label that nothing checks.
8. **Only the holder writes a claimed record,** with its token, until it releases the record or the lease expires. A lease-free write to a Leased record is rejected with `Leased`. Reason: exclusivity is unconditional, so no workflow can race a holder by key.
9. **`write` is the single mutation verb.** Insert, upsert, sparse update, stage moves, CAS on the version, settle, claim renewal, delete, gate flips and the metadata CAS are all writes, grouped into atomic units. `request_id` is optional. Reason: settling, patching, gate flips and a metadata CAS must commit together, and an id-less request resolves an unknown outcome by `read`, which keeps request records off the hot paths.
10. **The engine owns three record attributes:** `version` (opaque, strictly increasing per key across delete and re-insert, unchanged by extend and reclaim), the lease state, and `attempts` (+1 per claim, never reset). The insertion sequence and `free_since` stay internal. Reason: `version` gives CAS and the one change-feed hook without exposing a log position.
11. **Fields are declared at queue creation.** The types are `Int` (i64), `Float` (finite f64), `Bool`, `Time` (µs), `Str` (UTF-8) and `Bytes`. A queue has at most 256 fields; each `Str` or `Bytes` value is at most 64 KiB, and an encoded record at most 4 MiB. There is no "large" flag and no separate payload. Reason: value size, not field count, dominates cost (SPIKE-003), and the engine spills large values itself (SPIKE-005, SPIKE-006), so a caller never has to classify one. `Int`, `Str` and `Bytes` were measured; `Float`, `Bool` and `Time` are fixed-width and inferred to cost no more than `Int`.
12. **`claim` and `read` project fields.** Each takes an optional list of declared field names; `None` returns every field. The engine never reads the value of a field it does not return, unless that value shares storage with a returned one (F4). Reason: with 64 KiB values and the 16 MiB response cap, large fields would otherwise shrink claim batches and cost every claim a read of each spilled value (about 50–70 µs each, SPIKE-005). A claim with an empty list reads no field storage at all.
13. **Indexes are declared at creation, and any field type may be a key.** A queue has at most 32 indexes of 1..=8 keys each, drawn from fields of any declared type (`Str` and `Bytes` included) and the built-ins `Stage`, `Rank` and `Group`. The whole encoded (ESF) index key is at most 1024 B. Lease state, `until`, `attempts` and `version` are never keys. Reason: short strings and binary ids are ordinary lookup keys, and one bound on the encoded key covers every type. The lease attributes change on every claim, so indexing them would put index maintenance on the claim path.
14. **The key is the only uniqueness.** There are no unique secondary indexes. Reason: cross-record uniqueness needs a check before every commit; a consumer puts the identity into the key instead.
15. **The definition is immutable.** `define_queue` is create-or-verify over the normalized stages, fields and indexes. Reason: every write, claim and replay interprets the definition. Changing it would strand records in a removed stage, invalidate acknowledged writes and rekey indexes while claims run. A new definition means a new queue.
16. **Queue metadata is the only mutable queue state a consumer owns.** It is never interpreted, CAS-versioned, at most 64 KiB, and written inside write units. There are no side records: per-transition history travels in the write that settles a record and is stored in its fields. Reason: consumers need small configuration, such as Snorri's shard manifest, updated atomically with records. The bound keeps it configuration rather than a second store.
17. **One queue per job type, and queues are cheap.** Reason: shared-queue stage mappings and definition changes are never needed. Per-queue physical tables are therefore ruled out: SPIKE-006 measured 3.64 h to create 10,000 of them.
18. **`delete_queue` closes a queue, even a non-empty one.** It archives by default; a full delete needs `DeletePolicy::AllowFull`, a construction input that defaults to `ArchiveOnly`. Closed is final, and a closed queue key is never reused. Archive keeps the log and drops the projections. Reason: cheap per-entity queues must be removable. Retiring the key keeps a stale version from matching a record in a recreated queue. Full delete stays behind a policy until garbage collection is designed and measured.
19. **`stats` counts; it never claims.** It reports Free and Leased counts per stage, the pause state, and a bounded group-readiness read. `oldest_ready_since` is the earliest `free_since` among the ready records, and a `stats.groups` horizon is never earlier than now. Reason: rank may carry a priority offset, so it is never read as a time; `free_since` is when the record actually became Free. The readiness read answers "can a claim take enough groups now or by a later horizon", which a past horizon never asks.
20. **`read` and `scan` are fresh; `stats` and `aggregate` may be torn.** Lease reclaim is the only time-driven transition, bounded by Δ on an injected `Clock`. Reason: consumers' scan-then-write paths need fresh reads, and counting reads stay cheap.
21. **Outside the contract:** expiry and retention (records live until deleted or the queue closes; a future delete-by-filter may serve bulk retention), cohorts (an atomic group insert, a `WholeGroups` claim and a count check serve the use case), a remote API (the RESP adapter is deleted; a transport over `Store` may be built later under its own contract), and exposed history (only per-key `version` monotonicity is reserved). Reason: none of them is needed to add, claim and update records, and each would add backend-specific obligations.

### 3. The operator split

- **`Store` cannot end a lease without its token.** No `Store` value converts into anything that can (OP1).
- **Force-clear lives in a separate `Operator` trait,** reached only through the implementation's admin handle. Its one op, `force_clear`, detaches a record from its claim without the token, then optionally applies a lease-free `Put` or `Delete`. Each key is its own atomic unit (OP2, OP3).
- **The admin handle is not a consumer path.** For S3×Turso it is `TursoAdmin`, which carries the `Operator`, projection control and queue placement. Everything on it other than the `Operator` is implementation-specific deployment control. No `Store` semantics depend on it, and no consumer data path needs it.
- **CI enforces the split.** Normal workflows never use force-clear as a side channel. CI bans the `Operator` trait and the admin handle in Snorri, in the workload and loadgen crates, and in the contract suite (OP4). A separate operator suite tests OP1–OP3 (CS5).
- **Reason.** Force-clear is the one exception to "only the holder writes a claimed record", and keeping it off the data plane makes that rule unconditional for every consumer workflow. No other on-call action needs special power: pause and resume are the reserved gate `"*"`, inspection is `read`, and reschedule, move, requeue and purge are ordinary writes by key.

### 4. Consumers adapt

- **The contract is not bent to fit a consumer's current code.** Snorri and Seventh Sense adapt to it, and API-003 maps their workloads onto it.
- **Snorri:**
  - A workflow instance is a group. Per-instance exclusivity comes from claiming the instance's stages only with `WholeGroups`, whose exclusivity spans every stage (C6a).
  - Record kinds become stages, and each claim names its stage.
  - Per-transition history, acknowledgements and audit travel in the write that settles each input, and are stored as fields of the settled record.
  - The shard manifest lives in queue metadata, bootstrapped by one metadata CAS.
  - Snorri's lease clock is the constructor `Clock`.
  - The record key carries the target identity, since there are no unique secondary indexes.
- **Seventh Sense:** one queue per job type; `delete_queue` removes per-connector and per-job queues when their owner is deleted. It is written in Scala, so its cutover needs a transport over `Store`.
- **A consumer's need becomes a contract change only through a clause change** in API-006, plus an ADR when it changes the model. It is never a consumer-shaped method on `Fireweed`.

### 5. S3×Turso placement and storage

These decisions bind the S3×Turso implementation, not the contract; API-006 says nothing about physical design (§8). TD-017 (`docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`) designs the Turso projection, and TD-018 (`docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`) the partitioned object log.

**Placement.**
- There are no partitions inside a queue. The control plane assigns each queue one partition by mapping, not by hashing, and that partition fixes the queue's node and its object-log segment residency.
- Tenants are independent of partitions. Many queues share a partition's log stream and a single head or index document, instead of one index document per queue.
- Queues are never rebalanced across partitions.
- Reason: mapping lets the control plane pack and separate queues by measured load, and sharing a partition's log and head keeps queues cheap.

**Measure first.** Compaction, snapshots and truncation, splitting the log by partition range, and full-delete garbage collection are proposals, and none is committed before it is measured. The first measurement is today's per-queue commit cost on MinIO at 1, 10, 100 and 1,000 queues. Reason: performance comes first, and these designs trade durability and rebuild guarantees for cost.

**Storage evidence** (MEASURED unless marked):
- **Keys** (SPIKE-002): a per-queue integer surrogate instead of `(tenant TEXT, queue TEXT)` saves 11–21% per row; `BLOB` keys save 11–18% for integer and UUID keys; cost is linear in key length with no cliff up to 1024 B. Every key lookup forces `INDEXED BY` and looks up each key separately, never in one `IN (…)` list.
- **Values** (SPIKE-005): Turso's spill knee is between 3 and 4 KiB at the default 4 KiB page, so the page stays 4 KiB, values are capped at 64 KiB, and values over about 4 KiB spill off the row.
- **Fields** (SPIKE-003): value size, not field count, dominates cost. An inline large value costs 3.5–9× the update throughput of the same value stored off-row. Up to 200 fields were measured.
- **Layout** (SPIKE-006): the claim-path row carries no field bytes; fields live in a separate binary-map row, values over about 4 KiB spill per field, and declared indexes live in one shared index table. Per-queue physical tables scale super-linearly in schema creation and open time and are unusable at 10,000 queues.
- **Claim path** (SPIKE-004):
  - Stage-bound claims are O(log n) index seeks. Stage-blind queries are 5,000× or more slower, so every statement binds the stage.
  - The claims table makes extend about 80× and expiry discovery about 75× faster. A whole claim is finished in bulk by `claim_id`.
  - With the lean claim row, 100 queues × 1M records in one Turso file claim at 620.9 claims/s, within 4.3% of a 10M-record single-queue control, with p99 at 1.33× the control. With about 300 B of fields on the claim row, the same shape lost 46% of claim throughput and p99 rose 52×.
  - One writer per Turso file: independent writer connections fail with `SQLITE_BUSY` at N=32, so the `Mutex<Connection>` writer is mandatory. Writer parallelism comes from more partition files per node.
- **Transactions** (SPIKE-004, SPIKE-006): every multi-statement op runs in one explicit transaction and reuses its prepared statements; the Passive WAL checkpoint cap is mandatory; and the key index is maintained during ingest, because building it after a bulk load dominates build time.

## Alternatives

| Option | Evaluation |
|--------|------------|
| Keep the ~100-method facade and document it | Rejected. It is not a contract: aliases, stubs, backend leaks and retired selectors would each need a clause, an implementation and a test on every backend. |
| Seat a second implementation at the engine ports (`LibBackend`) | Rejected. The port already bent to fit PostgreSQL, and a shared mid-level abstraction grows to fit both designs. |
| A small kernel plus optional facets (Query, Discovery, Operator, side records), each all-or-nothing per implementation | Rejected. A capability flag is backend introspection, and consumers use Query on operational paths. All eight ops are mandatory, discovery and side records do not exist, and the operator is a separate trait, not a facet. |
| About 11 verbs (`push`, `settle`, `renew`, `update`, `set_gates`, `purge` and so on) | Rejected for one `write` with atomic units. Separate verbs cannot settle, patch, flip gates and CAS metadata atomically. |
| A `request_id` on every op | Rejected. Ids are optional: request records stay off the hot paths, and an id-less request resolves an unknown outcome by `read`. |
| Bounded-stale reads for every implementation, tested by an injected-lag adversary | Rejected. `read` and `scan` are fresh, because consumers' scan-then-write paths depend on it; only `stats` and `aggregate` may be torn. |
| Terminal stages | Rejected. No stage is special. |
| A `due` attribute beside `rank` | Rejected. Rank carries both order and schedule, and the claim horizon bounds which ranks are claimable. |
| One lease token per record, in lease columns on the item row | Rejected. A per-claim token makes extend and expiry O(claims) (SPIKE-004). |
| Typed fields plus an opaque whole-replace payload | Rejected. There is one declared-field concept, and large values spill automatically (SPIKE-005, SPIKE-006). |
| Claims and reads that always return every field | Rejected. Large fields would shrink claim batches and add a spilled-value read to every claim. |
| Side records or a side store for per-transition data | Rejected. History goes on the settled records, and configuration goes in queue metadata. |
| Operator helpers on the data plane, with a `Clear` lease op | Rejected. Force-clear is a separate `Operator` trait, reached from the admin handle. |
| A holder identity on claims, for observability | Rejected. The token is the only authority, and an unchecked label adds a column to every claim. |
| Unique secondary indexes | Rejected. The key is the only uniqueness. |
| A mutable definition, or an add-stage op | Rejected. The definition is immutable, and one queue per job type removes the need. |
| No queue deletion, or deleting only empty queues | Rejected. `delete_queue` works on a non-empty queue, archives by default, and deletes fully only under `AllowFull`. |
| Job types as stages of one shared queue | Rejected. One queue per job type, and queues are cheap. |
| Mutation history as an external obligation (change records, an outbox) | Rejected. Only per-key `version` monotonicity is reserved. |
| `explain_commit` or an outcome read | Rejected. An unknown outcome resolves by resending with the request id, or by `read`. |
| A remote API (RESP) in the contract | Rejected. A transport may be built later over `Store`, under its own contract. |
| Single-group claims; multi-queue claims | Rejected. A single-group claim breaks group exclusivity, and a multi-queue claim has no consumer. |
| Claims in declared-index order | Rejected. Rank is the single ordering key, and a claim over an index that does not lead with the stage is 5,000× or more slower (SPIKE-004). |
| Hashed placement, or partitions inside a queue | Rejected for production: mapped placement, one partition per queue, no rebalancing. Not a contract matter. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | One seam, the `Store` trait, and one suite with no backend knobs. Any implementation can be certified without sharing engine code. |
| Positive | Eight mandatory ops instead of about 100 entry points. The public surface exposes no backend type, capability flag, log position or retired selector. |
| Positive | Claims are stage-bound index seeks on a partial `(stage, rank, seq)` index: flat from 1M to 10M records in one queue, and within 4.3% of that at 100 queues × 1M records in one Turso file (SPIKE-004). Extend and expiry are O(claims). |
| Positive | Consumers get atomic multi-record units, gate flips and a metadata CAS in one write, and queue deletion for cheap per-entity queues. |
| Positive | Field projection keeps large values off the claim path: a claim reads and returns only the fields it names. |
| Negative | Breaking cut for Snorri and Seventh Sense. Snorri's migration is a rewrite from a pin older than `main`. |
| Negative | There is no network front until a transport over `Store` exists, so Seventh Sense (Scala) cannot cut over before then. |
| Negative | The definition is immutable: a new stage, field or index means a new queue. |
| Negative | Consumers own retention (no expiry) and uniqueness beyond the key (no unique indexes). |
| Negative | A lease-free write to a claimed record is rejected with `Leased`. A consumer that reschedules a claimed record retries after the lease ends, or settles it with the token. |
| Negative | One token per claim: an id-less claim whose response is lost strands its whole batch until the lease expires. |
| Negative | A claim that asks for large values may return fewer records than `max_records`, because of the 16 MiB response cap (C7). |
| Negative | `oldest_ready_since` costs O(log n + `limit` + s), where s counts grouped Free records ranked beyond the horizon, so a stage with many scheduled grouped records makes it slower (S2). |
| Neutral | S3 object log × Turso (ADR-024) is the one production implementation. The object log, projection control and placement are implementation internals behind `TursoAdmin`. |
| Neutral | The ESF order-preserving index-key encoder is part of `fireweed-contract`, and every implementation uses it verbatim. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| The product implementation misses the 100 queues × 1M records claim envelope that the spike met with the lean claim row | L | H | TP-003 AC-PERF-6 runs that shape on the product implementation (TD-017 §13). |
| Performance work starts on compaction, snapshots or GC before the commit cost is known | M | H | Measure per-queue commit cost on MinIO at 1, 10, 100 and 1,000 queues first (TD-018, Spike A). |
| The contract grows to fit one consumer or one backend | M | H | Spec-only growth: every public item cites a clause, and a clause-manifest test runs in `fireweed-contract`. |
| `Operator` is used as a side channel | L | H | OP4's CI ban and the separate operator suite (CS5). |
| Consumers rely on behavior only one implementation has, such as untorn `stats` | M | M | The torn-read adversary on every scenario (CS3), and no backend knobs (CS4). |
| The ESF encoder cannot yet express `Bytes` values or the `Stage`, `Rank` and `Group` built-ins | M | M | Extend the encoder in `fireweed-contract`, with ordering and round-trip tests: `Str` and `Bytes` order bytewise, stages order by declaration position, and a key is at most 1024 B. |
| Full delete and storage reclamation are unmeasured | M | M | `ArchiveOnly` is the default, `AllowFull` is opt-in, and reclamation timing is implementation-defined (DQ5). |
| Snorri's migration stalls on the mapping | H | M | Snorri adapts to the contract, and API-003 is the map. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| `fireweed-contract-suite` passes against S3×Turso through `run_all`, with at least one scenario per MUST clause, each run directly and through the torn-read adversary (CS1–CS3, CS6) | A backend knob, capability probe or per-backend clause enters the suite. |
| The `fireweed` public API is the eight `Store` ops plus the S3×Turso constructor and its admin handle | A backend type, capability flag, log position, retired selector or alias becomes reachable from a consumer. |
| CI rejects any use of `Operator` or the admin handle in Snorri, the workload and loadgen crates, and the contract suite | An allowlist entry or a workaround reaches force-clear from a data path. |
| Performance gates show a `Next` claim at O(log n + k + b) (C9) at 10M records, and at 100 queues × 1M records per Turso file; the S2 cost bound; and a projected claim that reads no unrequested spilled value (F4) | A statement that does not bind the stage, a claim that reads an unrequested spilled value, or a claim-path regression past the gate. |
| Snorri runs on the kernel with no side records, fences or `record_kind` filters | Snorri asks for a contract change that has no clause behind it. |
| `ddx doc validate`, `ddx doc audit`, `ddx doc stale` and `git diff --check` report no new graph defect | This ADR introduces a missing dependency, a duplicate id or a broken link. |

## Concern Impact

- `api-style`: the contract is typed and versioned in `fireweed-contract`, with stable clause ids that are never reused. Every input is validated at the envelope before any effect (W1), and there are no backend-shaped errors.
- `durable-priority-queue-semantics`:
  - Priority is `rank`, and eligibility is stage, horizon and gates, so priority stays separate from eligibility.
  - A record has at most one live claim, delivery is at-least-once, and request-id replay is held for at least R.
  - Order within a stage is strict `(rank, insertion sequence)`, and C5a is the progress rule; there is no queue progress bound.
- `testing`, `verification`: one black-box suite with no backend knobs, a torn-read adversary, and `reopen` plus a post-commit fault hook. The cost rules go to performance gates.
- `resilience`: an unknown outcome resolves by resend with the request id, or by `read` (E2, RQ4). Lease reclaim is bounded by Δ on an injected `Clock`.
- `deployment-topology`: placement is the control plane's, mapped rather than hashed (§5), and TD-018 designs it.

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/contracts/API-003-workload-integration-profiles.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
- `docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`
- `docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md`
- `docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md`
- `docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md`
- `docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md`
- `docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md`
- `docs/helix/02-design/adr/ADR-023-pre-release-fireweed-namespace-cutover.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
