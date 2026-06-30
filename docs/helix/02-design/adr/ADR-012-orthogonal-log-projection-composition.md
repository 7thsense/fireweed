# Architecture Decision Record

**ADR ID**: ADR-012
**Title**: The backend is the orthogonal product `LogStore × ProjectionStore × ControlPlane`, assembled by one generic `ComposedBackend`
**Status**: Proposed
**Related**: ADR-001 (CQRS log/projection), ADR-007 (hexagonal & two interfaces), ADR-008 (queue as
shard unit & two projection families — **superseded in part**, see below), ADR-009 (engine-enforced
coordination), TD-001 (backend contracts / conformance capability classes), TD-003 (ownership & epoch
fencing), TD-007 (durability). Conformance harness: `crates/pqueue-conformance`.

## Context

Every driven backend today is a **monolith**. `MemoryBackend`, `SqliteBackend`, `ObjectLogBackend`,
`PostgresBackend`, and the two relational backends each bundle a specific command **log** with a specific
**projection** and re-implement *every* engine port over that pair. The orchestration logic (claim,
push, upsert, finalize, renew, reassign, purge, update-fields, reclaim, tick) is then duplicated almost
verbatim across crates: compare `pqueue-memory/src/lib.rs` and `pqueue-sqlite/src/lib.rs` — they differ
**only** in the log substrate (an in-memory `LogData` vs durable sqlite rows) and where the epoch lives.
Everything else — pre-validate against the projection, mint ids from `QueueCounters`, build the envelope,
append+apply atomically, render the result from the projection — is byte-for-byte identical.

That duplication is the root cost ADR-007 set out to remove ("one shared in-memory projection + swappable
log stores") but never finished: the projection state machine *was* extracted into `pqueue-projection`
(`ProjectionData`, `LogData`, `commit`), yet the **assembly** of log + projection into a backend was left
per-crate. ADR-008 then framed storage as **two projection families** (in-memory log-replay vs
DB-authoritative relational) — a useful behavioral axis, but it described the families as distinct backend
*kinds* rather than as points in a composition.

## Decision

A backend is the **orthogonal product of three independent axes**:

```
Backend  =  LogStore  ×  ProjectionStore  ×  ControlPlane
```

assembled by exactly **one** generic struct, `ComposedBackend<L: LogStore, P: ProjectionStore, C: ControlPlane>`,
which implements every engine port by delegating to L / P / C. There is no per-substrate backend type and no
per-backend re-implementation of the orchestration ports — those live once, generically, on `ComposedBackend`.

### The axes

| Axis | Responsibility | Options |
|---|---|---|
| **`LogStore`** | the durable command log + the **epoch/fence authority** (co-located with the log, TD-003) + replay cursor (`read_from`) + snapshots + `command_position` high-water | Memory (in-proc `LogData`), Sqlite (durable rows), ObjectLog (segmented; S3 or local), Postgres |
| **`ProjectionStore`** | the materialized read model: the full `ProjectionRead` surface (`select_eligible`/`peek`/`pending`/`claimed_view`/`live_items`/`metrics`) + index queries + the pre-commit **validation** helpers + `apply(batch)` + snapshot/recovery | InMemory (`ProjectionData`), Sqlite, Postgres, Hybrid (in-mem + sqlite spill) |
| **`ControlPlane`** | queue **definitions** + placement (`create_queue`/`queue_definition`/`list_queues`) | InMemory, Postgres |

### Robustness is a **checked invariant**, not a per-backend property

Any `L × P × C` is a backend the instant it type-checks, but it is only **correct** once it passes the
TD-001 conformance suite (`pqueue-conformance`). The suite is the contract; composition is the mechanism.
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
   append+apply are **one DB transaction** and there is no separate command-log write at all (the relational
   projection is log-optional and DB-authoritative). The two-face closure must *not* be coerced into a
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
   *fields* of `Inner`. **No phantom log row is written**: for a DB-authoritative relational store the
   `append` facet only mints the position; the durable effect is the `apply`.

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

## Phased rollout

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

- **+** The orchestration logic exists once. A new backend is a new axis impl (a log, a projection, or a
  control plane), not a new monolith — and it inherits conformance for free.
- **+** The "flat postgres" / relational case is a first-class member of the same `ComposedBackend`, not a
  special exception, because the write seam is designed for a unified transactional store from the start.
- **−** The axis traits are wide (the `ProjectionStore` surface mirrors the full projection read +
  validation API). This is intrinsic — the orchestration genuinely needs that surface — but it is a single
  trait definition, not N duplicated impls.
- **−** Until Phase 2/3/5 the monoliths and the compositions coexist; the compositions are the proving
  ground, the monoliths remain wired, so the gate stays green throughout.
