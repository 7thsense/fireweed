---
ddx:
  id: adr-orthogonal-log-projection-composition
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
  review:
    self_hash: 5e35283d3ad0cc38c61d57aac7a63ce7c5fc8028bc8ff5f51a2bb4c28a1f13e6
    deps:
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-queue-as-shard-unit-and-projection-families: 64a7c7b0e2e5f4caa2c7d775b84c87a9a1e4484ae3df9dccbe3d145d22681a7e
    reviewed_at: "2026-08-04T04:50:53Z"
---

# Architecture Decision Record

**ADR ID**: ADR-012
**Title**: The backend is the orthogonal product `LogStore × ProjectionStore × optional ControlPlane`
**Status**: Accepted, superseded in part by ADR-015 (the synchronous `Backend::write(f)` and
`std::sync::Mutex<Inner<L, P>>` mechanism only). Product composition is now
native async. Shared orchestration is implemented by the engine's async
composition and log-replay products; inherently blocking stores may isolate a
whole transaction behind a bounded adapter actor. Remaining facade and evidence
closure tracks as beads.
**Related**: ADR-001 (CQRS log/projection), ADR-007 (hexagonal & two interfaces), ADR-008 (queue as
shard unit & two projection families — **superseded in part**, see below), ADR-009 (engine-enforced
coordination), TD-001 (backend contracts / conformance capability classes), TD-003 (ownership & epoch
fencing), TD-007 (durability), ADR-015 (full-async storage boundaries). Conformance harness:
`crates/fireweed-conformance`.

## Context

The original driven backends were **monoliths**. `MemoryBackend`, `SqliteBackend`, `PostgresBackend`,
and the two relational backends each bundle a specific command **log** with a specific
**projection** and re-implement *every* engine port over that pair. The orchestration logic (claim,
push, upsert, finalize, renew, reassign, purge, update-fields, reclaim, tick) is then duplicated almost
verbatim across crates: compare `fireweed-memory/src/lib.rs` and `fireweed-sqlite/src/lib.rs` — they differ
**only** in the log substrate (an in-memory `LogData` vs durable sqlite rows) and where the epoch lives.
Everything else — pre-validate against the projection, mint ids from `QueueCounters`, build the envelope,
append+apply atomically, render the result from the projection — is byte-for-byte identical.

That duplication is the root cost ADR-007 set out to remove ("one shared in-memory projection + swappable
log stores") but never finished: the projection state machine *was* extracted into `fireweed-projection`
(`ProjectionData`, `LogData`, `commit`), yet the **assembly** of log + projection into a backend was left
per-crate. ADR-008 then framed storage as **two projection families** (in-memory log-replay vs
DB-authoritative relational — a label ADR-013 has since retired; the relational projection is a
rebuildable cache) — a useful behavioral axis, but it described the families as distinct backend
*kinds* rather than as points in a composition.

## Decision

A backend is the **orthogonal product of three independent axes**:

```
Backend  =  LogStore  ×  ProjectionStore  ×  optional ControlPlane
```

assembled through one public composition model. Shared engine orchestration is
implemented by `AsyncComposedBackend` and the durable-log replay product, which
delegate to the selected log, projection, and optional control-plane ports.
Adapter-specific types may exist where I/O mechanics differ, but they do not
define a second public method surface or a per-pair product contract.

### The axes

| Axis | Responsibility | Options |
|---|---|---|
| **`LogStore`** | command ordering and the **epoch/fence authority** (co-located with the log, TD-003); Class A also owns durable replay, snapshots, and command high-water | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` |
| **`ProjectionStore`** | the materialized read model: full read/query/validation/apply and snapshot/recovery surface | `memory`, `sqlite`, `turso`, `postgres`; `turso` is the default |
| **`ControlPlane`** | optional queue definitions plus placement/membership/owner leases when the topology needs them | in-process, Postgres, or another separately qualified implementation |

The closed public set is the exact 5×4 product. Public `turso` means the
embedded/local Turso 0.7 adapter in ordinary WAL mode; remote, sync, and MVCC
modes are outside the decision boundary. SQLite remains a supported explicit
projection and the differential relational reference.

| Log \ Projection | `memory` | `sqlite` | `turso` (default) | `postgres` |
|---|---|---|---|---|
| `memory` | Class B | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A | Class A |

Class A logs are durable authorities and projections are rebuildable by
high-water plus tail replay. Class B's memory log is process-local; after
process death only a durable SQLite/Postgres projection may remain, and no
Class B cell claims log replay, branch, read-as-of, or log-derived history.

### Strict and asynchronous projection barriers

The public response-barrier values are `Strict` and `AsyncProjection`; they are
execution characteristics, not projection backends or product profiles.
`Strict` is required across all 20 cells. `AsyncProjection` is additionally
applicable to the eight filesystem/S3 object-log cells. The public log names are
`filesystem` and `s3`, and the public projection names are `memory`, `sqlite`,
`turso`, and `postgres`.

The remainder of this subsection preserves the design lineage under its former
internal `objectlog/hybrid-*` terminology. Those spellings are not accepted
public selectors. Read `objectlog` as the filesystem/S3 log family,
`hybrid-strict` as the SQLite projection under `Strict`, and `hybrid-async` as
the SQLite projection under `AsyncProjection`. This lineage does not restrict
the provider-neutral `AsyncProjection` contract to SQLite:

- `objectlog/hybrid-strict` is the synchronous hybrid contract. A successful
  response is legal only after manifest commit, durable SQLite projection apply,
  and hot in-memory apply/render for the operation's own result.
- `objectlog/hybrid-async` is the async-projection contract. A successful
  response is legal only after manifest commit plus synchronous in-memory
  apply/render for the operation's own result; SQLite projection apply MAY lag.
  SQLite lag is bounded and replayable, but it is not part of the success
  barrier.

Both modes compose the generic segmented object-log group commit runtime with a
`HybridProjectionStore`:

```
ComposedBackend<fireweed_objectlog::ObjectLog, HybridProjectionStore, InProcessControlPlane>
```

The hybrid projection is one `ProjectionStore` axis, not a new backend monolith.
Its read and validation surface is served from `InMemoryProjection`.
`objectlog/hybrid-strict` applies every committed batch to
`SqliteProjectionStore` first, then memory. `objectlog/hybrid-async` applies the
committed batch to memory on the response path and applies SQLite asynchronously
from the committed object log. In both modes a returned success means the
operation is manifest-committed and visible in the hot projection; in async mode
SQLite is a lagging recovery accelerator, not the acknowledged-command barrier.
The async SQLite worker consumes sealed object-log batches in monotonically
increasing batch sequence order, never applies batch N+1 before batch N, and
advances `sqlite_high_water` only after every command in the sealed batch has
been applied exactly once. Readers, claim selection, validation, metrics, and
response rendering observe memory, not the lagging SQLite projection.

For recovery, the local SQLite projection is a restart accelerator and
high-water source, never the command authority. The object log remains the
authority for acknowledged commands. `HybridProjectionStore` MUST hydrate a
complete `ProjectionImage` into memory before returning SQLite's recovery
high-water to `ComposedBackend::recover`; if image hydration fails or has not
run, recovery MUST fail closed or replay from genesis rather than serving an
empty hot projection. The `ProjectionImage` contract includes queue definition,
item lifecycle, leases, secondary indexes, side records, instance fences, queue
pause state, metrics, request-id replay records, and counters needed to resume
item-id allocation.

For `objectlog/hybrid-strict`, SQLite apply failure prevents success and recovery
replays the object-log tail; if SQLite commits and subsequent memory apply fails,
the projection MUST enter a poisoned state. For `objectlog/hybrid-async`, SQLite
apply failure after success is recorded as projection lag and retried from the
object log; memory apply failure before success prevents the response and leaves
the operation in unknown-outcome state for `request_id` replay. If async SQLite
lag cannot be replayed within its configured bound, the store fails closed for
recovery/high-water claims rather than treating SQLite as authoritative.
`sqlite_high_water` is a logical applied-command marker, not proof that object
log history is removable. SQLite WAL checkpoints, fsync mode, and page-cache
state are local durability implementation details; they may affect restart cost
on the same host, but they never authorize object-log trimming or replace the
manifest/snapshot retention rules.

Both hybrid modes MUST preserve replay-response idempotency for
committed-but-unreturned mutations. During recovery, committed commands with
`request_id` MUST repopulate the generic idempotency cache or an equivalent
durable replay record so a same-body retry returns the original result and a
different-body retry returns `request-id-conflict`. In `objectlog/hybrid-async`,
unknown-outcome handling is mandatory for every mutating command because success
can return before SQLite contains the replay record.

`objectlog/hybrid-async` release is blocked until the implementation proves one
combined lineage, idempotency, and retention frontier contract. The manifest
entry, segment sequence range, per-command `request_id` replay record,
in-memory projection image, SQLite `ProjectionImage`, and `sqlite_high_water`
MUST all describe the same committed command prefix before recovery or retention
can trust them. Async outcome retention MUST keep replayable `request_id`
results for every committed-but-unreturned or response-lost mutation through the
longer of API request-id retention and the active object-log recovery window.
Object-log expiry MUST use the minimum safe frontier across committed snapshots,
active manifest tail, request-id replay retention, client item-key retention,
and async SQLite lag; local SQLite high-water alone is never a retention
authority.

`objectlog/hybrid-async` MUST enter a poisoned SQLite state whenever async apply
cannot prove that SQLite represents a contiguous, trusted prefix of the object
log. Poison triggers include an apply gap or out-of-order sealed batch, a
`sqlite_high_water` value that does not match the manifest/segment command
prefix, a hydrated `ProjectionImage` that disagrees with the hot in-memory image
for the same frontier, checksum or segment replay failure while rebuilding the
SQLite projection, or repeated repair failure after the configured repair retry
budget. While poisoned, the object log and hot in-memory projection remain
authoritative for already acknowledged commands: readers, validation, response
rendering, idempotency replay, and request outcome decisions continue to use the
hot projection when it is present and still tied to a trusted object-log prefix.
SQLite is never a response barrier in `hybrid-async`; a poisoned SQLite
projection also cannot authorize retention, recovery high-water claims, replay
truncation, object-log expiry, or promotion of a local snapshot.

Repair MUST fail closed for any path that would rely on poisoned SQLite state.
The repair authority is the trusted object-log/snapshot frontier, selected from
the manifest, retained segments, committed snapshots, and replay-retention
requirements, not from the poisoned `sqlite_high_water`. Repair clears poison
only after rebuilding or replaying SQLite from that trusted frontier, verifying
checksums and segment continuity, hydrating a complete `ProjectionImage`, and
proving that the rebuilt SQLite image, `sqlite_high_water`, hot memory image,
request-id replay records, and manifest prefix describe the same committed
command prefix. If that proof fails, the store remains poisoned and recovery,
retention, and snapshot promotion stay fail closed.

Async apply debt is part of the contract, not an implementation detail.
`objectlog/hybrid-async` MUST publish bounded debt/backpressure metrics covering
oldest unapplied `batch_sequence`, pending logical batches, command and byte
debt, `sqlite_apply_lag_ms`, memory high-water, `sqlite_high_water`, configured
debt thresholds, and backpressure duration. When async debt exceeds the
configured budget, new mutating admission MUST fail closed or return typed
backpressure before acknowledging additional commands; recovery high-water,
snapshot promotion, and retention frontier advancement remain disabled until
ordered batching, lineage validation, and outcome retention are back within
budget.

### Robustness is a **checked invariant**, not a per-backend property

Any `L × P × C` is a backend the instant it type-checks, but it is only **correct** once it passes the
TD-001 conformance suite (`fireweed-conformance`). The suite is the contract; composition is the mechanism.
This ADR's Phase 1 proves the principle by re-expressing two existing monoliths as compositions and running
the *identical* shared suite against them.

### Where the epoch lives

The epoch is the **fence authority** and is **co-located with the `LogStore`** (`current_epoch` /
`acquire_epoch` / fenced `append`), because that is where both monoliths keep it: `MemoryBackend` in
`LogData.epoch`, `SqliteBackend` in the row store. `ComposedBackend`'s `impl ControlPlaneStore` therefore
**splits**: queue-definition methods delegate to `C`, while `current_epoch`/`acquire_epoch` delegate to `L`.
This is a deliberate refinement of ADR-008's "pluggable control plane" sketch: for a postgres-*native*
control plane that owns the epoch *transactionally*, the `LogStore` facet forwards its epoch methods into
the control plane's transaction (Phase 3+). The split keeps the common (memory/sqlite/objectlog) case honest
without a phantom epoch store.

### The atomic write seam (the crux): separate **and** unified transactional stores

> **Supersession note (ADR-015, 2026-07-18):** the atomicity requirements and separate/unified substrate
> distinction below remain governing history. The synchronous closure and standard-mutex realization are
> superseded. Typed backend-owned async commit operations and explicit whole-transaction adapters now
> realize this seam; TD-001 is normative for cancellation and suspension rules.

`Backend::write(f)` runs one unit of work: `f(&mut dyn LogWriter, &mut dyn ProjectionWriter)`, where the
closure appends commands and applies them, and the two effects commit **together**. There are two physical
realizations, and the composition must serve both **without forcing a phantom second write**:

1. **Separate-store path** (memory, sqlite-log-replay, objectlog, postgres-log). The log substrate and the
   projection substrate are **disjoint fields under one lock**. `ComposedBackend` owns
   `Mutex<Inner<L, P>>`; `write` destructures `Inner { log, projection, .. }` into two disjoint `&mut`
   faces and hands them to the closure. Atomicity = *one lock held for the whole UoW* (memory) or
   *durable-first ordering with an infallible, pre-validated in-memory apply* (sqlite). This is exactly the
   model both monoliths already use; `ComposedBackend` just makes it generic. **This path is implemented in
   this ADR's Phase 1.**

2. **Unified-transactional path** (sqlite-relational, postgres-relational / `postgres_native`). Here
   append+apply are **one DB transaction**: the command-log row and the projection mutation commit
   together. (As originally written this bullet called the relational projection "log-optional and
   DB-authoritative"; ADR-013 retired both properties — the log is mandatory and the projection is a
   rebuildable cache. What survives is the *mechanism*: one transaction, no separate two-phase log
   write.) The two-face closure must *not* be coerced into a
   split log write. The composition handles this by routing the UoW through a **single choke point**,
   `ComposedBackend::commit_locked(inner, shard, env, expected_epoch)`, which is the only place that
   sequences `epoch-resolve → fence → log.append → projection.apply`. For a unified store this choke point
   calls **one** transactional method that does append+apply atomically in one transaction (`append`
   reserves the synthetic `CommandPosition` / stages the command intent; `apply` performs the relational
   mutation; both target the same open transaction; `commit` flushes it). Because every orchestration port
   funnels through `commit_locked`, swapping the separate path for the unified path touches exactly one
   function.

   The disjoint-borrow obstacle (the closure wants `&mut log` **and** `&mut projection` simultaneously, but a
   unified store is one object / one transaction) is resolved by treating the log substrate of a unified
   store as a **disjoint logical facet of the same transaction** — a position counter / staged-command
   buffer that lives beside the projection rows in the one DB transaction. The two `&mut dyn` faces then
   borrow disjoint *parts* of the transaction wrapper, identically to how the separate path borrows disjoint
   *fields* of `Inner`. **No two-phase log write is introduced** — append and apply remain one
   transaction. (As originally written this said "no phantom log row is written: the `append` facet only
   mints the position"; ADR-013 supersedes that half — the log is mandatory and the relational family
   must be rebuildable from it, so the `append` facet MUST durably persist the command envelope as a real
   log row *inside the same transaction* as the projection mutation. What stands is that no separate,
   second-phase log write exists.)

   Proposed trait support (Phase 3, specified now so the shape is fixed): `LogStore` and `ProjectionStore`
   each expose the substrate behind `&mut self` write methods and `&self` reads, so a *single* type may
   implement **both** axes over one transaction (`impl LogStore + ProjectionStore for RelationalStore`).
   `ComposedBackend<RelationalStore, RelationalStore, RelationalControl>` then composes the relational
   backend with the log and projection facets being the *same* value, and `commit_locked` recognizes the
   unified case via a `LogStore::transaction_mode() -> TxnMode { Separate, Unified }` discriminator. This
   keeps the headline `ComposedBackend<L, P, C>` signature for both paths.

### Object-safety / zero-cost

`ComposedBackend` is **generic** over its axes (monomorphized, zero-cost) — the engine never needs
`dyn LogStore`. The two writer faces handed to the UoW closure remain `&mut dyn LogWriter` /
`&mut dyn ProjectionWriter` (object-safe, unchanged from the existing `Backend` port), so the conformance
`commit` helper and `append_at_epoch` keep working verbatim.

### Supersession of ADR-008

This supersedes ADR-008's framing of storage as **two distinct projection families**. The families are
retained as the **`ProjectionStore` axis** (in-memory vs relational) and as the TD-001 conformance
**capability classes** (core / log-replay / relational-reconnect), but they are no longer backend *kinds*:
they are one axis of a three-axis product, and "fused vs split" is precisely the `Separate`/`Unified`
write-seam distinction above. ADR-008's keystone decisions (queue as the unit of sharding; per-queue
ownership; epoch fencing) are unchanged.

## Historical phased rollout

The phases below record how the original synchronous design was introduced.
They are not current product status; the native-async implementation and open
evidence work are tracked by the storage-matrix completion brief.

- **Phase 0 — this ADR.** The model, the axis traits, the write-seam design (separate + unified). *Proposed.*
- **Phase 1 — this change.** The three traits + generic `ComposedBackend` (separate path) + the first axis
  impls (`MemoryLog`, `InMemoryProjection`, `SqliteLog`, `InProcessControlPlane`). Re-express the **memory**
  and **sqlite** backends as `ComposedBackend<…>` and run the shared TD-001 suite against both, **alongside**
  the still-present monoliths (nothing deleted yet).
- **Phase 2.** Delete `MemoryBackend` / `SqliteBackend` monoliths once the compositions are wired into the
  lib facade + server; re-point their tests at the compositions.
- **Phase 3.** The unified-transactional seam: `RelationalStore: LogStore + ProjectionStore`, re-express the
  sqlite-relational and postgres-relational backends as `ComposedBackend<…>` (one DB transaction).
- **Phase 4.** ObjectLog (segmented; S3/local) and Postgres log axes; the hybrid (in-mem + sqlite spill)
  projection axis.
- **Phase 5.** Remove the remaining monoliths; the engine ships only `ComposedBackend` + a library of axis
  impls.

## Consequences

- **+** Shared orchestration is centralized. A new storage implementation adds
  an axis implementation rather than a new public backend product, and must pass
  the common conformance suite before it is supported.
- **+** The "flat postgres" / relational case is a first-class member of the same `ComposedBackend`, not a
  special exception, because the write seam is designed for a unified transactional store from the start.
- **−** The axis traits are wide (the `ProjectionStore` surface mirrors the full projection read +
  validation API). This is intrinsic — the orchestration genuinely needs that surface — but it is a single
  trait definition, not N duplicated impls.
- **−** Until Phase 2/3/5 the monoliths and the compositions coexist; the compositions are the proving
  ground, the monoliths remain wired, so the gate stays green throughout.

## Decision note (2026-07-08, DDx B3.6): retain `SqliteRelationalBackend` after composed parity

**Decision.** `SqliteRelationalBackend` (the monolithic DB-authoritative sqlite relational backend,
`crates/fireweed-sqlite/src/relational.rs:4165`) is **retained**, not retired, at this time. This closes DDx
B3.6 ("retire or justify … after composed parity") on the **justify** branch.

**Why the original keep-reason is gone but retirement is still not clean.** ADR-012 kept the sqlite
relational monolith as the *sole owner* of the relational-class capabilities — non-item (cohort / whole-group
/ same-group) claim selection, per-group active-scope discovery, and operator gate state. DDx B0.2–B0.4
**ported those onto the composition** (`ComposedBackend<SqliteRelational, SqliteRelational,
InProcessControlPlane>`, aliased `ComposedSqliteRelationalBackend`, built by
`composed_sqlite_relational_in_memory()` / `composed_sqlite_relational(path)` at
`crates/fireweed-sqlite/src/relational.rs:9809-9829`), delegating to the relational-capable `ProjectionStore`
axis. So the *original* justification for the monolith no longer holds. Retirement is nonetheless declined
because it is **broad and would drop live coverage**, not a clean drop-in:

- **Only one production (non-test) construction site exists** — the benchmark harness
  `crates/fireweed-bench/src/main.rs:295` (`run_sqlite_relational`), which deliberately measures the
  DB-authoritative monolith as a *distinct backend family* from the composed sqlite-log path. Every other
  construction site is `#[cfg(test)]` / a `tests/` target.
- **Two conformance suites run against the monolith and are mirrored nowhere on the composed relational
  path:** `adr011_typed_conformance_suite!` and `claimed_item_shape_conformance_tests!(@whole_cohort …)`
  (`crates/fireweed-sqlite/tests/relational_conformance.rs:121-122`). The whole-cohort claim-*shape* arm is
  explicitly monolith-only today (`relational_conformance.rs:129`). Retiring the monolith would silently
  delete this coverage unless the suites are first re-homed onto `composed_sqlite_relational_in_memory()`.
- **The monolith is the DB-authoritative reference oracle for BQ-13 head-to-head cross-family parity**
  (`crates/fireweed-sqlite/tests/cross_family_parity.rs:20-22`, `scenarios::cross_family_core_parity`). It is
  an *independent* relational implementation (no log/projection composition machinery), which is precisely
  what makes it a trustworthy differential oracle against the in-memory family. Re-homing that role onto the
  composed relational backend would cross-check the composition against itself, weakening the differential.
- **Blast radius:** retirement means deleting the ~4,000-line struct plus its ~24 port impls
  (`relational.rs:5871`–`8101`) and migrating/re-homing ~5,000 lines across six test targets
  (`relational_conformance.rs`, `relational_commit.rs`, `relational_reconnect.rs`, `cross_family_parity.rs`,
  the `commit_transition_scenario_tests` in `fireweed-conformance/src/scenarios.rs`, and the in-file unit-test
  module `relational.rs:9931`+). That is a high-risk, broad change, not the low-risk deletion B3.6 targets.

**What composed parity DOES now cover** (so the monolith carries no *unique feature*, only unique *coverage /
oracle* duties): the full `core_suite!(@atomic)` at parity with the monolith
(`relational_conformance.rs:130-134`); rich cohort / whole-group / same-group claim **selection**,
active-scope **discovery**, and **gates** (`crates/fireweed-sqlite/tests/composed_relational_parity.rs`);
durable recovery-on-open (`composed_relational_reconnect.rs`, `durable_reconnect_suite!`); terminal reap
(`composed_relational_terminal_reap.rs`); and every orchestration port generically — including
`CommitTransitionPort`, `RecoveryReadPort`, `HotProjectionQueryPort`, `IndexQueryPort`,
`HistoricalProjectionRead`, and `ReclaimDriver` — implemented once on `ComposedBackend`
(`crates/fireweed-engine/src/compose.rs:3217`, `3430`, `3067`, `2910`, `2824`, `2639`).

**Tracked follow-up condition for eventual retirement (ADR-012 Phase 5, "remove the remaining monoliths").**
Retire `SqliteRelationalBackend` once ALL of the following hold, so no coverage or oracle guarantee is lost:
1. `adr011_typed_conformance_suite!` and `claimed_item_shape_conformance_tests!(@whole_cohort …)` pass
   against `composed_sqlite_relational_in_memory()` (mirror the two monolith-only suites onto the composed
   relational module in `relational_conformance.rs`).
2. BQ-13 head-to-head `cross_family_core_parity` runs with a relational representative whose independence
   from the composition is either preserved or explicitly accepted as no longer needed.
3. The `fireweed-bench` `run_sqlite_relational` shape is repointed at (or dropped in favor of) the composed
   relational constructor.
Then delete the `SqliteRelationalBackend` struct and its port impls, keeping the reusable free-function SQL
internals the composed `SqliteRelational` axis already depends on.
