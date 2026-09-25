---
ddx:
  id: adr-kernel-records-contract
  depends_on:
    - api-fireweed-kernel-contract
    - external-interface-contract-research
    - external-interface-kernel-contract-draft
    - spike-record-key-storage
    - spike-record-field-storage
    - spike-claim-path-at-scale
    - spike-turso-value-size-boundary
    - spike-declared-field-storage-layout
  links:
    - {kind: informed_by, to: api-fireweed-kernel-contract}
    - {kind: informed_by, to: external-interface-contract-research}
    - {kind: informed_by, to: external-interface-kernel-contract-draft}
    - {kind: informed_by, to: adr-023-pre-release-fireweed-namespace-cutover}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
    - {kind: supersedes, to: adr-per-queue-secondary-indexes}
    - {kind: supersedes, to: adr-queue-template-and-exact-ensure}
    - {kind: supersedes, to: adr-facade-lifecycle-and-routing-orchestration}
    - {kind: supersedes, to: adr-concrete-fireweed-facade-and-optional-controls}
    - {kind: supersedes, to: td-queue-history-change-records}
  status: accepted
---

# ADR-025: The external interface is the eight-operation kernel records contract

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner (model and binding decisions); lead engineer (defaults marked in the decision log) | API-006, ADR-009, ADR-010, ADR-011, ADR-014, ADR-018, ADR-019, ADR-022, ADR-023, ADR-024, TD-008, SPIKE-002 to SPIKE-006 | High for the model; medium for the lead-engineer defaults |

The decision is accepted, and so is the contract text it adopts, API-006. Every lead-engineer default and choice in the decision log stays open to owner override, and the entries marked **Open for the owner** need an owner answer.

## Context

**The external interface was not a contract.** The research audit (`docs/helix/00-discover/external-interface-contract-research.md`) found:
- About 100 public entry points: 74 `Fireweed` methods, 4 `ProjectionControl` methods, 10 `operator_*` methods and 12 free functions, plus `pub mod turso_compose` (`docs/helix/00-discover/external-interface-kernel-contract-draft.md` §0).
- Only about 15 of them are semantic core. The rest are aliases, stubs, test fixtures, backend introspection (`commit_capabilities`, `current_position`, the snapshot family), and retired configuration kept only so it can be rejected.
- API-001 and API-005 contradict the code and each other. Several methods have no normative text at all.
- Durability and visibility are stated in terms of the object log ("durable on the object log", "a claim polls applied rows") instead of as semantics.
- The deep conformance suite runs only against engine ports (`LibBackend`, a 21-trait bound that includes log and snapshot traits). The public-level suite has the right shape but only smoke depth, and it carries backend knobs.

**A shared internal seam bends.** `PostgresRelationalBackend` was plugged in at the engine-port seam, and the seam absorbed it. The result was lowest-common-denominator defaults, capability flags exported as API, and dialect forks. The lesson the research draws: the seam for a second implementation must be the external contract, not an internal port.

**Consumers need little.** Seventh Sense's queues reduce to four primitives: idempotent create or upsert; take (due, eligible, ordered, bounded, exclusive, optionally whole groups); settle; and delete by key. Snorri calls about 24 methods, at a pin (`v0.31.21`) older than `main`.

**The replacement was worked out in rounds.** A synthesized kernel draft (`external-interface-kernel-contract-draft`) went through judge, verification and check passes and two rounds of owner decisions. The owner then settled the record model in the 2026-09-23 design discussion: stages with no special stage, rank as the only ordering key, claims as leased batches, declared fields, queue deletion, and a separate operator interface. Five spikes measured the storage consequences on Turso (SPIKE-002 to SPIKE-006). API-006 (`docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`) is the resulting contract text. This ADR records the decision to adopt it, and why.

## Decision

### 1. The kernel contract replaces the facade

- **The external interface is API-006.** It is the eight operations of the object-safe `Store` trait: `define_queue`, `write`, `claim`, `read`, `stats`, `scan`, `aggregate` and `delete_queue`. It also covers their DTOs and errors, the contract constants, and the cross-cutting rules for durability, visibility, atomicity, ordering and leases.
  - All of these types live in the I/O-free crate `fireweed-contract`.
  - `fireweed::Fireweed` wraps `Arc<dyn Store>` and delegates 1:1.
- **Every op is mandatory for every implementation.** There are no optional facets, capability flags, backend identity, log positions or commit explanations (API-006 §8). Query (`scan`, `aggregate`) is part of `Store`, because consumers use it on operational paths.
- **The rest of today's surface is rewritten, moved or deleted.** Each of today's roughly 100 entry points becomes a `write`, `claim` or `read` composition, moves to the implementation's admin handle, or is deleted.
  - The draft's §6 disposition table is the per-method map. Read it with the later decisions applied: `Clear` becomes `Operator::force_clear`, `due` becomes `rank`, and terminal stages become ordinary stages.
- **Hard pre-release cutover.** This follows ADR-023's precedent: no aliases, no compatibility layer, no import of existing data and no replay compatibility with earlier command formats (owner decision, 2026-09-23 design discussion).
- **Semantics are stated as semantics.** Durability is DUR1. Visibility is V1–V5, with one documented Δ of at most 5 s that bounds only lease reclaim. The contract never names the object log.
  - S3 object log × Turso (ADR-024) is the one production implementation.
  - A second implementation, such as an isolated PostgreSQL unit, would implement `Store` directly and depend only on `fireweed-contract`, never on engine ports. This ADR commits to no second implementation.
- **One black-box conformance suite.** It runs in-process against `Arc<dyn Store>` (CS1–CS7), with no backend knobs. Every scenario runs twice: directly, and through a torn-read adversary for `stats` and `aggregate`.
- **The contract grows only by spec.**
  - Every public item of `fireweed-contract` traces to a clause id, and retired ids are never reused (API-006 §1).
  - An addition requested because one implementation or one consumer needs it, with no clause behind it, is rejected.
  - A change to the model needs a new ADR.

### 2. The records model

The whole model is: add records (insert or upsert), claim them for work, update them, repeat (owner decision, 2026-09-23 design discussion). API-006 §2–§6 is normative; this is the summary.

- **Queues.**
  - A queue is addressed by `(tenant, queue)`. Queues are cheap, and a consumer uses one queue per job type.
  - Each queue has an immutable **definition**: its ordered stages, declared fields and declared indexes. It is set by create-or-verify (D1–D3), and changing any part of it means a new queue.
  - Each queue also has a small mutable **metadata** map. The consumer owns it and the engine never interprets it. It is at most 64 KiB, versioned by compare-and-swap, and written inside write units (W13).
  - A queue lives until `delete_queue` closes it by archiving it, or by deleting it fully where policy allows (DQ1–DQ5).
- **Records.**
  - A record is addressed by a key that the upstream system owns: opaque bytes, 1..=256 B, unique per queue. A large natural key is hashed upstream.
  - A record lives until the consumer deletes it or closes its queue. There is no expiry.
  - The engine owns three attributes: `version` (strictly increasing per key, including across delete and re-insert), the lease state, and `attempts` (+1 per claim, never reset).
- **Stages.**
  - Every record is in exactly one of its queue's declared stages. The first declared stage is the insert default, and `[INITIAL, COMPLETED]` is the default list.
  - No stage is special: there is no terminal flag, no Completed or Failed state, and no transition graph. Any write may move a record to any declared stage.
  - A stage that nothing claims is "done" to the consumer. The engine neither knows nor cares.
- **Rank.**
  - `rank: i64` is the single client-controlled ordering key. Its default is the insertion time in µs.
  - A claim takes the records of its stage with `rank <= horizon` (default now), in `(rank, insertion sequence)` order. A future rank schedules a record, and a priority bucket is an offset added to the rank.
  - There is no `due`, no relaxed ordering mode and no queue progress bound. Order within a stage is strict, and C5a is the progress rule.
  - Rank ordering is the product's core differentiator. It is kept simple and fast: C9 bounds a `Next` claim at O(log n + k + b).
- **Claims versus writes.**
  - A claim walks one stage and takes an exclusive, crash-safe batch; the state change is the cursor. It selects by `Next`, `WholeGroups` or `Keys`. `Next` and `WholeGroups` take records in rank order; `Keys` takes the named records. No claim orders by an index (KC-13).
  - A claim that returns records creates one claim with one token and one `until`. `Extend` renews the whole claim. `Release` or `Delete` detaches one record, and the claim's other records stay attached.
  - While a record is claimed, only the holder writes it, with its token, until it releases the record or the lease expires. After that, anyone may claim the record or write it by key.
  - A write addresses records by key. Without a token, it serves records the caller already knows.
  - Delivery is at-least-once, with at most one live claim per record (L1–L6).
- **Writes.** `write` is the single mutation verb. Insert, upsert, sparse update, stage moves, CAS on the version, settle, claim renewal, delete, gate flips and the metadata CAS are all writes, grouped into atomic units. `request_id` is optional (W1–W14, RQ1–RQ4).
- **Declared fields.**
  - Fields and their indexes are declared at queue creation. The types are `Int`, `Float`, `Bool`, `Time`, `Str` and `Bytes`.
  - The limits: at most 256 fields; each `Str` or `Bytes` value at most 64 KiB; an encoded record at most 4 MiB; at most 32 indexes, with 1..=8 keys each drawn from fields and the built-ins `Stage`, `Rank` and `Group`; an encoded index key at most 1024 B.
  - There is no "large" flag and no separate payload. Where values are stored is the implementation's choice (F1–F3).
  - The key is the only uniqueness: there are no unique secondary indexes. Every query goes through a declared index.
- **No exposed history.** The only change-feed hook is that `version` increases per key. No log position, mutation history or ordering promise is exposed (API-006 §6.7). Per-transition history travels in the write that settles a record and is stored in that record's fields. There are no side records.

### 3. The operator split

- **`Store` cannot end a lease without its token.** No `Store` value converts into anything that can (OP1).
- **Force-clear lives in a separate `Operator` trait.** It is reached only through the implementation's admin handle (owner decision, 2026-09-23 design discussion). Its one op, `force_clear`, detaches a record from its claim without the token, then optionally applies a lease-free `Put` or `Delete`. Each key is its own atomic unit (OP2, OP3).
- **The admin handle is not a consumer path.** For S3×Turso it is `TursoAdmin`, and it carries the `Operator`, projection control and queue placement. Everything on it except the `Operator` is implementation-specific deployment control. No `Store` semantics depend on it, and no consumer data path needs it.
- **CI enforces the split.** Normal workflows never use force-clear as a side channel. CI bans the `Operator` trait and the admin handle in Snorri, in the workload and loadgen crates, and in the contract suite (OP4). A separate operator suite tests OP1–OP3 (CS5).
- **Why.**
  - Force-clear is the one exception to "only the holder writes a claimed record". Keeping it off the data plane makes that rule unconditional for every workflow a consumer writes.
  - The other on-call actions in the draft's operator helpers need no special power. Pause and resume are the reserved gate `"*"`; inspection is `read`; reschedule, move, requeue and purge are ordinary writes by key. Only ending someone else's lease needs force-clear.

### 4. Fireweed and Snorri co-evolve

- **The contract is not bent to fit Snorri's current code. Snorri adapts** (owner decision, 2026-09-23 design discussion). Seventh Sense migrates the same way (API-006 §1).
- **Snorri's mapping follows these principles:**
  - A workflow instance is a group. Per-instance exclusivity comes from claiming the instance's grouped stages only with `WholeGroups`, whose exclusivity spans every stage (C6a). That replaces the projection fence and its retry loops.
  - Record kinds become stages, and claims name the stage instead of filtering on a `record_kind` field.
  - Per-transition history, acknowledgements and audit travel in the write that settles each input, and are stored as fields of the settled record.
  - The shard manifest lives in queue metadata, bootstrapped by one metadata CAS.
  - Side records, the projection fence, replay markers and `reclaim_expired_at` go. Snorri's lease clock becomes the constructor `Clock`.
  - The unique `by_run_target_key` index goes: the record key must carry the target identity.
- **The draft's §8 is the working migration map, not a contract.** It was written with terminal stages and `due`. Where it names a terminal stage, read "a stage nothing claims"; where it sets `due`, set `rank`; where it claims with `Where`, use a stage per kind of work, `WholeGroups`, or a `scan` followed by a `Keys` claim (KC-13). Snorri's pin predates `main`, so its migration is a rewrite (draft §7.5 risk 7).
- **Seventh Sense.**
  - One queue per job type (owner decision, 2026-09-23 design discussion) replaces the draft's "job types as stages of one queue" mapping. It also removes the need for an add-stage migration op (draft §10 O-6).
  - `delete_queue` removes per-connector and per-job queues when their owner is deleted (draft §10 O-7).
  - Seventh Sense is Scala, so its cutover needs a transport over `Store` (below).
- **A consumer's need becomes a contract change only through a clause change** in API-006, plus an ADR when it changes the model. It is never a consumer-shaped method on `Fireweed`.

### 5. Recorded here, decided elsewhere

The following owner decisions and measurements bind the S3×Turso implementation, not the contract. API-006 says nothing about physical layout (§8). They are recorded here so the decision log is complete. The S3×Turso technical designs carry them: TD-017 (`docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`, the Turso projection layout) and TD-018 (`docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`, the partitioned object log and mapped placement).

**Object log and placement** (owner decisions, 2026-09-23 design discussion):
- There are no partitions inside a queue.
  - The control plane assigns each queue one partition by mapping, not by hashing. That partition fixes the queue's node and its object-log segment residency.
  - Tenants are independent of partitions. Many queues share a partition's log stream and a single head or index document, instead of one index document per queue.
  - Queues are never rebalanced across partitions.
- Compaction, snapshots and truncation, splitting the log by partition range, and full-delete garbage collection are proposed and must be measured before any commitment. The first step is measuring today's per-queue commit cost on MinIO at 1, 10, 100 and 1,000 queues.

**Performance evidence** (MEASURED unless marked):
- **Keys** (`docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md`):
  - A per-queue integer surrogate instead of `(tenant TEXT, queue TEXT)` saves 11–21% per row.
  - BLOB keys save 11–18% for integer and UUID keys.
  - Cost is linear in key length, with no cliff up to 1024 B.
  - Every key lookup must force `INDEXED BY` and look up each key separately, never in one `IN (…)` list.
- **Values** (`docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md`):
  - Turso's spill knee is between 3 and 4 KiB at the default 4 KiB page. Keep the 4 KiB page.
  - Cap values at 64 KiB, and spill values over about 4 KiB off the hot row.
- **Fields** (`docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md`):
  - Value size, not field count, dominates cost.
  - An inline large value costs 3.5–9× the update throughput of the same value stored off-row.
  - Up to 200 fields were measured.
- **Layout** (`docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md`):
  - Layout C: the hot claim-path row carries no field bytes. Fields live in a separate binary-map row, values over about 4 KiB spill per field automatically, and declared indexes live in a shared index table.
  - Per-queue physical tables (layout D) scale super-linearly in schema creation and open time, and are catastrophic at 10,000 queues. They would break "queues must be cheap".
- **Claim path** (`docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md`):
  - Stage-bound claims are O(log n) index seeks. Stage-blind queries are 5,000× or more slower, so every statement must bind the stage.
  - The claims table makes extend about 80× faster and expiry discovery about 75× faster. Finish must bulk-update by `claim_id`.
  - At 100 queues × 1M records in one Turso file, with field bytes on the claim row, claim throughput dropped 46% and p99 rose 52× from page-cache pressure. SPIKE-006's lean claim row is expected to fix this (INFERRED), and a follow-up is measuring it.
  - One writer per Turso file: independent writer connections fail with `SQLITE_BUSY` at N=32, so the `Mutex<Connection>` writer is mandatory. Writer parallelism comes from more partition files per node.
- **Transactions** (SPIKE-004, SPIKE-006):
  - Every multi-statement op runs in one explicit transaction, and reuses its prepared statements within it.
  - The Passive WAL checkpoint cap is mandatory.
  - A `UNIQUE` index built after a bulk load dominates build time, so the key index is maintained during ingest.

## Alternatives

| Option | Source | Evaluation |
|--------|--------|------------|
| Keep the ~100-method facade and document it | Research §2 | Rejected. It is not a contract: aliases, stubs, backend leaks and retired selectors would each need a clause, an implementation and a test on every backend. |
| Seat a second implementation at the engine ports (`LibBackend`) | Research §4 | Rejected. The port already bent to fit PostgreSQL. A shared mid-level abstraction grows to fit both designs. |
| A small kernel plus optional facets (Query, Discovery, Operator, Side records), each all-or-nothing per implementation | Research §5.2, §6 Q1 | Rejected. A capability flag is backend introspection, and consumers use Query on operational paths. All eight ops are mandatory. Discovery and side records are deleted. The operator is a separate trait, not a facet. |
| About 11 verbs: `push`, `settle`, `renew`, `update`, `set_gates`, `purge` and so on | Research §5.2 | Rejected for one `write` with atomic units. Separate verbs could not settle, patch, flip gates and CAS metadata atomically, and the judges flagged "three mutation verbs, 9 knobs" (draft §11). |
| A `request_id` on every op | Research §5.1 | Rejected. Ids are optional (draft §9.2 D3): the receipt stays off the hot paths, and an id-less request resolves an unknown outcome by `read`. |
| Bounded-stale reads for every implementation, tested by an injected-lag adversary | Research §5.3 | Rejected. `read` and `scan` are fresh; only `stats` and `aggregate` may be torn (draft D16). Consumers' scan-then-write paths depend on freshness. |
| Terminal stages (a terminal flag; terminal stages never claimable) | Draft round 2, D-I, D21, D22 | Rejected by the owner. No stage is special. |
| `due` beside `rank`, including `due: None` | Draft §1, D20, O-3 | Rejected by the owner. Rank is the only ordering key, and the claim horizon replaces `due_by`. |
| One lease token per record, in lease columns on the item row | Draft §4; SPIKE-004 L1 | Rejected. The claims table (SPIKE-004 L2) gives a per-claim token. Extend and expiry become O(claims), not O(claimed records). |
| Two data slots: typed `fields` plus an opaque whole-replace `payload` | Draft D9 | Rejected. There is one declared-field concept, and large values spill automatically (SPIKE-005, SPIKE-006). |
| Side records or a side store for per-transition data | Today's surface | Rejected by the owner. History goes on the settled records; configuration goes in queue metadata. |
| Operator helpers on the data plane, with a `Clear` lease op | Draft D10, §5 | Rejected by the owner. Force-clear is a separate `Operator` trait, reached from the admin handle. |
| Unique secondary indexes | ADR-010, ADR-011; draft D7 | Rejected. The key is the only uniqueness mechanism. |
| A mutable definition, or an append-only add-stage op | Draft §10 O-6 | Rejected. The definition is immutable, and one queue per job type removes the need. |
| No queue deletion, or dropping only empty queues | Draft §10 O-7 | Replaced by `delete_queue`: it works on a non-empty queue, archives by default, and allows a full delete only under `AllowFull`. |
| Job types as stages of one shared queue | Draft §8.3 | Rejected by the owner. One queue per job type, and queues must be cheap. |
| Mutation history as an external obligation (change records, an outbox) | Research §5.3; TD-008 | Rejected. Only per-key `version` monotonicity is reserved. |
| `explain_commit` or an outcome read | Draft D8 | Rejected. Resolve an unknown outcome by resending with the request id, or by `read`. |
| A remote API (RESP) in the contract | ADR-007, TD-006 | Deleted by the owner. It may be rebuilt later as a pure transport over `Store`, under its own contract. |
| Single-group claims; multi-queue claims | Draft D26; ADR-019 | Deleted. A single-group claim breaks group exclusivity, and a multi-queue claim has no consumer. |
| Claims in declared-index order (`Select::Where`) | Draft C4; API-004 claim by query | Removed in review (KC-13). Rank is the single client-controlled ordering key (OD-3), and a claim over an index that does not lead with the stage is stage-blind, which SPIKE-004 measured at 5,000× or more slower. |
| Hashed placement, or partitions inside a queue | ADR-008 (HRW); the object-log partition design input | Rejected by the owner for production: mapped placement, one partition per queue, no rebalancing. Not a contract matter. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | One seam, the `Store` trait, and one suite with no backend knobs. Any implementation can be certified without sharing engine code. |
| Positive | Eight mandatory ops instead of about 100 entry points. The public surface exposes no backend type, capability flag, log position or retired selector. |
| Positive | Claims are stage-bound index seeks on a partial `(stage, rank, seq)` order, with no throughput loss measured at 10M records. Extend and expiry are O(claims). |
| Positive | Consumers get atomic multi-record units, gate flips and metadata CAS in one write, and queue deletion for cheap per-entity queues. |
| Negative | Breaking cut for Snorri and Seventh Sense. Snorri's migration is a rewrite from a pin older than `main`. |
| Negative | There is no network front after RESP is deleted. Seventh Sense (Scala) cannot cut over until a transport over `Store` exists under its own contract. |
| Negative | The definition is immutable: a new stage, field or index means a new queue. |
| Negative | Consumers own retention (no expiry) and uniqueness beyond the key (no unique indexes). Snorri's `by_run_target_key` moves into its record key. |
| Negative | A lease-free write to a claimed record is rejected with `Leased`. Snorri's lease-preserving reschedule becomes a conflict to retry. |
| Negative | One token per claim: an id-less claim whose response is lost strands its whole batch until the lease expires. |
| Negative | Large values and the 16 MiB response cap mean a claim may return fewer records than asked, because there is no field projection (decision log KC-11). |
| Neutral | S3 object log × Turso (ADR-024) stays the one production implementation. The object log, projection control and placement become implementation internals behind `TursoAdmin`. |
| Neutral | The ESF order-preserving index-key encoder stays. `fireweed-contract` provides it, and every implementation uses it verbatim. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| The lean claim row does not recover the 46% throughput drop and 52× p99 at 100 queues × 1M records per Turso file | M | H | The fix is INFERRED from SPIKE-006. The follow-up measurement must confirm it before the S3×Turso design commits to that density. |
| Performance work starts on compaction, snapshots or GC before the commit cost is known | M | H | Owner decision: measure per-queue commit cost on MinIO at 1, 10, 100 and 1,000 queues first. |
| The contract grows to fit one consumer or one backend | M | H | Spec-only growth: every public item cites a clause, and a clause-manifest test runs in `fireweed-contract` (draft §7.4 S1). |
| `Operator` is used as a side channel | L | H | OP4's CI ban and the separate operator suite (CS5). |
| Consumers rely on behavior only one implementation has, such as untorn `stats` | M | M | The torn-read adversary on every scenario (CS3), and no backend knobs (CS4). |
| The ESF encoder cannot express `Bytes` values or the `Stage`, `Rank` and `Group` built-ins (axon's `IndexType` has none of them) | M | M | Extend the encoder in `fireweed-contract`, with ordering and round-trip tests: stages order by declaration position, and a key is at most 1024 B. |
| Full delete and storage reclamation are unmeasured | M | M | `ArchiveOnly` is the default, `AllowFull` is opt-in, and reclamation timing is implementation-defined (DQ5). |
| Snorri's migration stalls on the mapping | H | M | The co-evolution rule: Snorri adapts, and the draft's §8 is the working map. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| `fireweed-contract-suite` passes against S3×Turso through `run_all`, with at least one scenario per MUST clause, each run directly and through the torn-read adversary (CS1–CS3, CS6) | A backend knob, capability probe or per-backend clause enters the suite. |
| The `fireweed` public API is the eight `Store` ops plus the S3×Turso constructor and its admin handle | A backend type, capability flag, log position, retired selector or alias becomes reachable from a consumer. |
| CI rejects any use of `Operator` or the admin handle in Snorri, the workload and loadgen crates, and the contract suite | An allowlist entry or a workaround reaches force-clear from a data path. |
| Performance gates show a `Next` claim at O(log n + k + b) (C9) at 10M records, and at 100 queues × 1M records per Turso file with the lean claim row | A statement that does not bind the stage, or a claim-path regression past the gate. |
| Snorri runs on the kernel with no side records, fences or `record_kind` filters | Snorri asks for a contract change that has no clause behind it. |
| `ddx doc validate`, `ddx doc audit`, `ddx doc stale` and `git diff --check` report no new graph defect | This ADR introduces a missing dependency, a duplicate id or a broken link. |

## Decision log

**Owner decisions** (owner decision, 2026-09-23 design discussion). These are binding. Where the owner's decisions conflict, the later one wins. The last column names the earlier draft decision each one replaces or confirms (draft §9.1 D-A–D-K, §9.2 D1–D26, §10 O-1–O-7).

| # | Decision | Applied in | Replaces or confirms |
|---|----------|------------|----------------------|
| OD-1 | The model: add records (upsert), claim them for work, update them, repeat. The upstream system owns record keys: opaque bytes, 1..=256 B, unique per queue. Large natural keys are hashed upstream. | API-006 §2, §3, §4.0 | Replaces the draft's UTF-8 `Key` of 1..=1024 B |
| OD-2 | Stages are first-class. The definition declares an ordered stage list, and the first stage is the insert default. With no list given, the stages are `[INITIAL, COMPLETED]`. No stage is special: no terminal flag and no Completed or Failed state. Records are just records until the queue is closed. | §3, D1, §5.1 | Replaces D-H's terminal flags, D-I, D21 and D22; answers O-1 (a record nothing claims is just a record) |
| OD-3 | `due` is removed. `rank` (i64) is the single ordering key, defaulting to the insertion time in µs. A claim takes its stage's records with `rank <= horizon` (default now), in rank order, with ties broken by insertion sequence. Priority buckets are offsets added to rank. Rank ordering is the core differentiator and stays simple and exceptionally fast. | C1, C4, C9, ORD1–ORD2 | Replaces `due`, D20 and O-3 |
| OD-4 | A claim walks a stage in exclusive, crash-safe batches: the state change is the cursor. A lease-free write by key serves records the caller already knows. While claimed, a record is locked, and only the holder writes it, until release or lease expiry. After that, anyone may claim or write it. | C1–C6, W4, L3 | Confirms D-J and round-3 answer R3-2; settles O-2 as "reject with `Leased`" |
| OD-5 | Leases use a claims table (SPIKE-004 L2): `claims(claim_id, token_hash, holder, expires_at, record_count)`, a nullable indexed `claim_id` on records, and a claim-order index partial on `claim_id IS NULL`. So the token is per claim; extending renews the whole claim; releasing a record detaches it. | L1–L4, W14; implementation | Replaces the draft's per-record token |
| OD-6 | Force-clear exists only in a separate `Operator` interface, obtained from the admin handle and never from `Store`. CI bans it in Snorri, the workload and loadgen crates, and the contract suite. Normal workflows never use it as a side channel. | §7, OP1–OP4, CS5 | Replaces D10's helper module and the `Clear` lease op; settles R3-3 |
| OD-7 | Side records are gone. Per-item metadata travels in write commands and is stored on the items. Queue metadata is mutable, consumer-owned, never interpreted, CAS-versioned, at most 64 KiB, and writable inside write units. The definition (stages, declared fields, indexes) is immutable and create-or-verify. | W13, §5, D1–D3 | Confirms D-F and D-G; extends the definition to fields |
| OD-8 | Fields and their indexes are declared at queue creation. There may be many fields; some are large (up to 64 KiB), most are rarely updated, and some are indexed. Storage follows SPIKE-006 layout C with automatic spill, and there is no "large" flag or separate payload. Indexed fields are small typed scalars. The contract states only the declaration rules and limits. | F1–F3, §5.1, §6.8 | Replaces D9's `payload`; confirms D-E |
| OD-9 | Queues are deletable through `delete_queue` with an archive bit, even when non-empty. A server-side policy decides: `archive_only` (the default) or `allow_full`. Archive keeps the log and drops the projections. Full delete is proposed, pending a GC design and measurement. "Closed" means archived or deleted, and there is no read-only state. | §4.8, DQ1–DQ5 | Settles O-7 |
| OD-10 | One queue per job type. Queues must be cheap. | §2 | Replaces the draft's §8.3 job-types-as-stages mapping; removes O-6 |
| OD-11 | Cohorts are deleted. Retention is a consumer concern: there is no expiry machinery, though a future delete-by-filter is possible. There is no import or replay compatibility (pre-production). The remote API is left off: the RESP adapter is deleted, to be rebuilt later as a pure transport. The contract suite runs in-process against `Store`. | §8, CS1 | Confirms D-A, D-B, D-C and D-D; settles O-4 |
| OD-12 | Fireweed and Snorri co-evolve. The contract is never bent to fit Snorri's current code, and Snorri adapts: groups are workflow instances, with per-instance exclusivity from whole-group claims; record kinds become stages; per-transition history goes on settled items; the manifest goes in queue metadata. | §1; this ADR §4 | Confirms D-K |
| OD-13 | There are no partitions inside a queue. Each queue is assigned one partition, mapped by the control plane, not hashed, which fixes its node and object-log segment residency. Tenants are independent of partitions. Many queues share a partition's log stream and a single head or index document. There is no rebalancing. Production design, not contract. | This ADR §5; S3×Turso designs | Conflicts with ADR-008's HRW placement (see Supersession) |
| OD-14 | Compaction, snapshots and truncation, log splitting by partition range (for example 1–255 into 1–127 and 128–255), and full-delete GC are proposed and must be measured before commitment: "be very careful and measure first; get proper performance in place first". The first step is measuring per-queue commit cost on MinIO at 1, 10, 100 and 1,000 queues. | This ADR §5; DQ5 | — |

**Lead-engineer defaults from the spec-evolution brief.** The owner may override each one.

| # | Default | Applied in |
|---|---------|------------|
| LE-1 | Field types: `Int` (i64), `Float` (finite f64), `Bool`, `Time` (µs), `Str` (UTF-8) and `Bytes`. Only `Int`, `Str` and `Bytes` were measured; the rest are inferred to be as cheap. | §4.0, §5.1 |
| LE-2 | At most 256 declared fields per queue; each value at most 64 KiB; an encoded record at most 4 MiB. | F2, W8, §6.8 |
| LE-3 | At most 32 indexes per queue, each over 1..=8 declared small scalar fields or the built-ins `Stage`, `Rank` and `Group`; indexed values at most 1024 B encoded. API-006 applies the bound to the whole encoded index key (all keys of one index), which also bounds each value (KC-6). | D4, W1, W8, §5.1 |
| LE-4 | The lease token is per claim (it follows from OD-5). | L1, C3 |
| LE-5 | Engine-owned record attributes: `version` (strictly increasing per key, including across delete and re-insert), claim state, and `attempts` (+1 per claim, never reset). No `stage_since` in the contract; stats keep an internal `free_since` only. | §3 |
| LE-6 | Eight operations on `Store`: `define_queue`, `write`, `claim`, `read`, `stats`, `scan`, `aggregate`, `delete_queue`. `Operator` is a separate trait. The admin handle (`TursoAdmin`) carries projection control and placement. | §4, §7 |

**Choices made while writing API-006** (lead engineer). The owner may override each one.

| # | Choice | Applied in |
|---|--------|------------|
| KC-1 | Extend is claim-level: `Unit.extend: Vec<Extend>`, which changes no record version. `LeaseOp::Held(t)` lets a holder patch a record and keep it. `Store` has no `Clear`. | W4, W14 |
| KC-2 | A claim is live while `until > now` and at least one record is attached. Extending an empty claim gives `StaleLease`. A claim that returns nothing creates no claim (`lease: None`). | L2, C3 |
| KC-3 | A queue has 1..=32 stages, and an empty stage list means the default list. | D1, D4 |
| KC-4 | `horizon` replaces `due_by` in `claim` and in `stats.groups`. It applies to `Next` and `WholeGroups` claims; `Keys` ignores it. | C1, C2, S2 |
| KC-5 | **Open for the owner.** S2 keeps `oldest_ready_since`, computed from an internal `free_since` as `ready_since = max(rank, free_since)`. This reads rank as a µs time, which holds for the default rank and scheduled ranks but not for a rank carrying a priority offset. | S2, §3 |
| KC-6 | **Open for the owner.** The only built-in index keys are `Stage`, `Rank` and `Group`. `TimeBucket` applies only to `Time` fields. Any field type may be indexed, `Str` and `Bytes` included, within the 1024 B limit on the whole encoded index key. OD-8 says indexed fields are small typed scalars; the owner confirms whether short `Str` and `Bytes` values count, or whether index keys exclude `Bytes`. | §5.1, Q4 |
| KC-7 | `delete_queue` adds the errors `QueueClosed` and `NotPermitted`. A closed queue's key is retired for good, so a later `define_queue` fails with `QueueClosed`; that keeps a stale version from matching a record in a recreated queue. A repeat returns `already_closed`. A full delete of an archived queue is allowed under `AllowFull`. `DeletePolicy` is a construction input, and the suite's constructor takes it. | DQ1–DQ5, D6, CS1 |
| KC-8 | `Operator` has a single `force_clear`: each key is its own atomic unit, with an optional lease-free follow-up write. A separate operator suite tests it. | OP1–OP3, CS5 |
| KC-9 | Clause C9 carries "keep rank ordering exceptionally fast". Performance gates verify it; the black-box suite does not. | C9 |
| KC-10 | Smaller choices:<br>- A field value over 64 KiB is `Invalid`, not `TooLarge`.<br>- A request has at most 1000 extends, and each token appears at most once.<br>- Patch fields are `Option` (`None` = keep), so an upsert can no longer assert "this record has no group".<br>- A force-clear that detaches a record bumps its version.<br>- `Busy` is allowed on every op.<br>- A `scan` `limit` of 0 is `Invalid`. | W1, W8, OP2, E3, Q2 |
| KC-11 | **Open for the owner.** `claim` and `read` have no field projection. With 64 KiB values and the 16 MiB response cap, a claim may return fewer records than asked. | C7, §6.8 |
| KC-12 | Carried from the draft's §9.2, and still overridable:<br>- optional request ids on `write` and `claim` (D3);<br>- an opaque `Version`, ordered per key only and unchanged by lease reclaim (D4, reshaped by KC-16);<br>- no maximum group size, with `partial_group` for an oversized first group (D5);<br>- gate ops and the metadata op inside write units, and pause as gate `"*"` (D6);<br>- no unique indexes (D7);<br>- no `explain_commit` and no outcome read (D8);<br>- the injected `Clock` governs leases and Δ (D12);<br>- `stats.groups` ignores gates, pause and Leased members (D13);<br>- request limits: 1000 records and 16 MiB per request, leases up to 1 h, R ≥ 24 h, Δ ≤ 5 s (D15);<br>- `read` and `scan` fresh, `stats` and `aggregate` possibly torn (D16);<br>- `WholeGroups` exclusivity spans stages (D18);<br>- `Keys` skip precedence (D24, without `Terminal`);<br>- a stage move keeps the insertion sequence (D25);<br>- no single-group claim (D26);<br>- retiring FR-48 and rewording FR-45, FR-47a and FR-47c (D14). | §4, §6 |
| KC-13 | **Open for the owner.** Claims are rank-ordered only: the draft's `Select::Where` (claims in declared-index order, ascending or descending) is removed. It was a second client-controlled claim order, against OD-3, and it allowed claims over an index that does not lead with the stage, which SPIKE-004 measured at 5,000× or more slower and C9 did not bound. Claim by query becomes a `scan` followed by a `Keys` claim, or a stage per kind of work, or `WholeGroups`. Restoring an index-filtered claim needs a clause that binds the stage, keeps `(rank, insertion sequence)` order and carries a cost bound like C9. | C1, C4, ORD3, §8 |
| KC-14 | **Open for the owner.** `claim` takes no holder identity, so the `holder` column of OD-5's claims table is unused (TD-017 stores NULL). The alternative is an optional holder input on `ClaimRequest`, for observability only, with no semantics. | C3, OD-5 |
| KC-15 | `stats.groups.horizon`, when given, must not be earlier than now (else `Invalid`). That keeps `oldest_ready_since` exact at O(log n + `limit`). S2's cost bound, like C9, is verified by performance gates, not by the black-box suite. | S2, CS2 |
| KC-16 | Smaller choices made in review:<br>- An `Extend(t)` is checked after the detaches of t's records made by earlier applied units and by its own unit; if none remain attached, it is `StaleLease`.<br>- `Version` is opaque apart from its order per key.<br>- Every byte bound is measured with the `fireweed-contract` encoders (LIM1).<br>- A `Float` `-0.0` is normalized to `+0.0`.<br>- An empty `GroupBy::Dims` is one total group; a zero `TimeBucket` width and fewer than two, or unsorted, `Ranges` bounds are `Invalid`.<br>- A full delete under `ArchiveOnly` is `NotPermitted` in every queue state, and `delete_queue` is the one op a closed queue still answers.<br>- Queue isolation is its own clause (ISO1): a token is valid only on its own queue. | W2, W2a, W14, §4.0, LIM1, Q4, DQ1–DQ5, ISO1 |

## Supersession

- **Supersedes:**
  - **ADR-010**, per-queue secondary indexes. Its remaining parts go: unique indexes, the pre-commit uniqueness validator, `IndexQueryPort` and the `query_index*` methods, and in-memory-first phasing. Indexes are now declared in `QueueDef.indexes` over declared fields and `Stage`, `Rank` and `Group`; they have no `unique`, and are read through `scan` and `aggregate`. Maintaining indexes on apply is an S3×Turso implementation detail (SPIKE-006 layout C's shared index table).
  - **ADR-018**, queue templates and exact ensure. `define_queue` is create-or-verify with exact comparison of the normalized definition, and `DefinitionConflict { existing }` (D1–D3), so exact ensure is now the op itself. `QueueTemplate`, `QueueCreationPolicy`, `ensure_queue`, `EnsureQueueOutcome` and `EnsureQueueError` are deleted. The per-backend atomic-create prerequisites (B-010, B-013 to B-016) are moot, because D1 requires atomic, durable create of every implementation.
  - **ADR-019**, lifecycle and routing orchestration. The lifecycle aliases (`complete`, `retry`, `release`, `discover`) are deleted, because settling is a write. Discovery and its stamped accessor are deleted (the draft's §5; S2 serves the one real use). `claim_across_queues` and the dispersion selectors are deleted. Nothing in it survives.
  - **ADR-022**, the concrete Fireweed facade and optional controls. Its one principle, a concrete, backend-opaque `Fireweed` handle, carries forward: `Fireweed` wraps `Arc<dyn Store>` (API-006 §1). The rest is replaced:
    - The inherent native-method surface becomes the eight ops.
    - `projection_control()` moves from `Fireweed` to the admin handle (`TursoAdmin`).
    - The queue-scoped capability checks are deleted, since there is no introspection.
    - `batch_update` becomes `write`.
    - The `Embedded*` and role-named constructor families become one S3×Turso constructor.
  - **TD-008**, queue history through change records. `ChangeRecord` was already retired by ADR-014's note. What remains of TD-008 is removed:
    - `emit_change_records` on the definition, and the per-queue opt-out;
    - the terminal-retention default and the CL-6 reap and emission coupling (there are no terminal states and no expiry);
    - the Class A and Class B `ChangeRecordsRequireDurableLog` rule.

    Mutation history is not an external obligation (API-006 §6.7).
  - **API-001, API-002, API-003, API-004 and API-005**, through API-006, as of 2026-09-24 (API-006 header). Each carries a supersession header that says what survives and where it went.

  Each superseded ADR and TD above carries a matching supersession header and `status: superseded`.
- **Amends:**
  - **ADR-009**, engine-enforced coordination and the encapsulated library surface.
    - Decision 2 is superseded for the surface. The external surface is `fireweed-contract` (the object-safe `Store` and `Operator` traits and the DTOs) plus the `fireweed` wrapper and one constructor. The `dyn`-erasure ADR-009 rejected is now the design. The rule that engine ports and backends are not an external surface stands, but the publish topology must admit `fireweed-contract` as a published crate, or have `fireweed` re-export it whole.
    - Decision 7's `ItemId` leaves the contract. The record key is caller-owned, and S3×Turso keeps its item id as a private row key.
    - Decisions 1 and 3–6 (coordination below the ports, owner sessions, target affinity, epoch fencing, authorization delegated to the host) are no longer contract matters. They stay as S3×Turso internals, where a fenced owner surfaces as `Unavailable`. Target affinity is subject to OD-13's mapped placement. The RESP references in Decisions 1 and 6 are moot.
  - **ADR-011**, typed payload schemas and indexes through axon ESF.
    - Superseded: the JSON Schema `entity_schema` and its payload validation, which declared typed fields replace (F1–F3); axon's `IndexType`, `IndexDef`, `CompoundIndexDef` and `unique`; and the `QueueIndex` wrapper.
    - Standing: the ESF canonical, order-preserving index-key encoding. `fireweed-contract` now provides it, and every implementation uses it verbatim. It must be extended to `Bytes` values and to the built-in keys (Risks).
  - **ADR-014**, the embedded Fjord change-log surface. It is outside the external contract.
    - No change record, command envelope, log position or history is a `Store` obligation or part of conformance.
    - `emit_change_records` and the retention frontier tied to it leave the definition.
    - Whether `fireweed-server` keeps the embedded Fjord fetch of the command log is an S3×Turso deployment question that this ADR does not decide. If it is kept, it exposes internal command envelopes with no compatibility promise (OD-11).
  - **ADR-024**, the public cell is s3 log × turso projection.
    - It stands as the choice of the one production implementation of `Store`.
    - Amended: `projection_control` moves from `Fireweed` to `TursoAdmin`.
    - Amended: `StorageConfig` selectors, `ResponseBarrier`, `RETIRED_STORAGE_CELL` and the other retired configuration types leave the public surface, and the S3×Turso constructor takes its own configuration.
    - Amended: the contract states visibility as V1–V5 rather than `AsyncProjection`. "An empty claim is a poll" is C5. A claim must also observe every acknowledged write (V2), and `read` and `scan` are fresh (V3). That replaces ADR-024's "Callers cannot ask for read-your-writes" and its "claim polls applied rows".

  Each amended ADR carries an "Amended by ADR-025" note that points here.
- **Applies without change:** **ADR-023**, the pre-release namespace cutover. This ADR extends its hard-cutover rule from names to the interface: no aliases, no compatibility layer, no import or replay compatibility. No edit is needed beyond citing ADR-025. Its references to RESP extensions as a current surface become historical once RESP is deleted.
- **Also affected, to be reconciled separately.** Each now carries a short note that points here; its full rewrite is separate work:
  - **ADR-004.** Discovery, the queue progress bound and `metadata_equals` claim filters are gone, and stages and selects replace the claim domain. Its rule that physical placement is never client-visible stands (API-006 §8).
  - **ADR-007.** "Exactly two interfaces (RESP + Rust library)" becomes one in-process interface, with a transport possible later (OD-11).
  - **ADR-008, TD-003 and TD-015.** Their HRW placement conflicts with OD-13's mapped placement. "The queue is the unit of sharding" stands.
  - **The `durable-priority-queue-semantics` concern.** Its progress-bound practice is replaced by strict rank order plus C5a; `concerns.md` now says so.
  - **TD-016 and TD-010.** The write pipeline maps ops onto engine lifecycle states, terminal stages and cohort claims, and TD-010 caps a claim microbatch below API-006 C7 and §6.8. The op mapping without terminal stages, at API-006's limits, is not yet written.

## Concern Impact

- `api-style`: the contract is typed and versioned in `fireweed-contract`, with stable clause ids and retired ids never reused. Every input is validated at the envelope before any effect (W1), and there are no backend-shaped errors.
- `durable-priority-queue-semantics`:
  - Priority is `rank`, and eligibility is stage, horizon and gates, so priority stays separate from eligibility.
  - A record has at most one live claim, delivery is at-least-once, and request-id replay is held for at least R.
  - The queue progress bound is replaced by strict `(rank, seq)` order and C5a. The concern's practice text needs that update.
- `testing`, `verification`: one black-box suite with no backend knobs, a torn-read adversary, and `reopen` plus a post-commit fault hook. C9 goes to performance gates.
- `resilience`: an unknown outcome resolves by resend with the request id, or by `read` (E2, RQ4). Lease reclaim is bounded by Δ on an injected `Clock`.
- `deployment-topology`: placement is the control plane's, mapped rather than hashed (OD-13). It is recorded here and specified in TD-018.

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/00-discover/external-interface-contract-research.md`
- `docs/helix/00-discover/external-interface-kernel-contract-draft.md` (§6 disposition, §8 consumer mapping, §9 decisions, §10 open questions)
- `docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md`
- `docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md`
- `docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md`
- `docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md`
- `docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md`
- `docs/helix/02-design/adr/ADR-023-pre-release-fireweed-namespace-cutover.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
