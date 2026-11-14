---
ddx:
  id: adr-queue-interface-implementation-boundary
  depends_on:
    - api-native-client-interface
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-orthogonal-log-projection-composition
    - adr-log-single-source-of-truth
    - adr-concrete-fireweed-facade-and-optional-controls
    - adr-s3-turso-public-cell
    - td-postgres-native-reference-mode
  links:
    - {kind: informed_by, to: adr-hexagonal-architecture-and-two-interfaces}
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
  status: proposed
---

# ADR-025: The queue interface is the implementation boundary

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-11-13 | Proposed | Project owner | API-001, API-005, API-006, ADR-007, ADR-012, ADR-013, ADR-022, ADR-024 | Medium |

## Context

Fireweed is meant to be one optimized interface for priority work queues,
with several implementations behind it. A PostgreSQL implementation is the
reference: it is the simplest correct one and the one other implementations
are compared against.

The repository does not deliver that. The queue semantics are written down
in API-001, but no implementation can be written against them. Four
decisions put the boundary somewhere else.

1. **ADR-007 drew the hexagon around storage.** Driven adapters "implement
   the storage ports". The engine sits above them and owns the claim path,
   idempotency, fencing and validation. A second implementation therefore
   plugs in *below* the engine, not beside it.
2. **ADR-012 made every backend `LogStore × ProjectionStore × ControlPlane`.**
   That is the internal structure of one design (CQRS with a command log and
   a derived projection), stated as the shape every backend must have.
3. **ADR-013 made the command log mandatory.** "There is no architecture that
   drops `LogStore`." A relational implementation that keeps queue state in
   rows and claims with `FOR UPDATE SKIP LOCKED` is ruled out by definition.
4. **ADR-024 then reduced the product to one cell**, S3 object log × Turso
   projection, and API-005 bound the public Rust API to it.

The Postgres reference was designed once and then lost. TD-002 specified it
as "the reference correctness backend": queue state in relational tables and
claim as one `FOR UPDATE SKIP LOCKED` statement. The hexagonal migration
deleted that crate because it implemented the old storage traits, and
deferred a rebuild "fresh to the engine ports" as "durable command log +
projection rebuilt-from-log". The rebuild has the log shape the engine
requires, not TD-002's design.

The code follows those decisions.

| Observation | Evidence |
|-------------|----------|
| The trait set an implementer faces is one design's internals | `fireweed-engine` exports 53 public traits. Queue operations (`ClaimPort`, `PushPort`, `FinalizePort`) sit beside `LogStore`, `ProjectionStore`, `CommitStrategy`, `UnifiedAtomicCommitter`, `OwnedTaskDispatcher`, the async planners, `SnapshotStore` and `AsOfProjectionStore`. |
| There is no public trait to implement | The facade's backend bound, `LibBackend` (21 traits), is `pub(crate)` in `crates/fireweed/src/lib.rs`. |
| The facade owns implementation code | `crates/fireweed` depends on the memory, objectlog, postgres, projection and turso crates, and holds 7.8k lines of Turso composition in `turso_compose.rs`. |
| Implementations depend on each other | `fireweed-postgres` depends on `fireweed-objectlog` and `fireweed-projection`. `fireweed-turso` depends on `fireweed-relational`. |
| Postgres is forced into the log shape | `fireweed-postgres` (24.5k lines) carries `async_log_replay.rs`, `compose_log.rs` and `async_objectlog_postgres.rs`. |
| Conformance tests the log seam, not the queue | `run_conformance<B: ConformanceBackend>` requires `SnapshotStore + LogRead`, and tests build `CommandEnvelope`s by hand. An implementation without a command log cannot run it. |
| Internals leak into the public API | API-005 exposes `DurabilityClass`, `ResponseBarrier::AsyncProjection`, `projection_control`, snapshot and compaction methods, and "empty claim may be apply lag". |

The result is one implementation with a large, implementation-shaped API,
and no way to add a second without editing the facade crate.

## Decision

### 1. The boundary is the queue operation model

API-001 defines what a Fireweed queue does. That operation model, expressed
as Rust traits, is the boundary every implementation implements. The traits
and their request and response types live in one small crate,
`fireweed-interface` (working name), defined by API-006.

`fireweed-interface` depends on `fireweed-core` and general-purpose crates
(serde, bytes, futures) only. It contains no log, command, projection,
snapshot, epoch or storage type.

### 2. An implementation is a crate that implements the interface

An implementation crate implements the API-006 traits with whatever internal
design suits it. A command log, a derived projection, a relational schema or
an in-memory map are all valid internals. None of them appears in the
interface.

An implementation crate depends on `fireweed-interface`, `fireweed-core`,
its own drivers, and shared internal libraries it chooses to use. It does not
depend on another implementation crate. A shared library (for example the
current log and projection engine) is a dependency of the implementations
that use it. It is not part of the interface, and `fireweed-interface` never
depends on it.

### 3. Implementations differ only in declared guarantees

Every implementation gives the same answers to the same requests, with three
exceptions. An implementation declares each exception as a value in the
interface (API-006 `Guarantees`), and the conformance suite tests the
declared value.

| Guarantee | Values | Meaning |
|-----------|--------|---------|
| Durability | `Process`, `Durable` | Whether an acknowledged mutation survives a process crash. |
| New-work visibility | `Immediate`, `Bounded { max_ms }` | How soon a pushed, updated, released or re-armed item becomes claimable by a claim that starts after the mutation returned. |
| Sharing | `SingleProcess`, `MultiProcess` | Whether several processes may open the same storage at once without breaking leases or ordering. |

Safety effects are never relaxed. Once a mutation returns, no later claim
may return an item that a returned lease, finalize, purge or gate block has
removed from eligibility. Only the liveness effect, new work becoming
claimable, may lag, and only within the declared bound.

"Cell", "Class A/B", "response barrier" and "projection coverage" stop being
product terms. They remain internal vocabulary of the object-log
implementation.

### 4. Queue features are declared, not discovered by failure

Some queue features are optional: cohorts, group batching, recurrence,
dynamic gates, metadata blockers, typed payload schemas, secondary indexes,
and the priority kinds other than timestamp and int64. An implementation
lists the
features it supports in `Capabilities`. `create_queue` rejects a definition
that needs an unsupported feature with `feature-unsupported`, before creating
anything. A method on an existing queue never fails because the
implementation lacks a feature.

### 5. Operation families beyond the core are separate traits

The core trait covers the API-001 operations other than `MutateItems`, plus
lease reassignment. Larger families that
not every consumer needs are extension traits: item mutation (API-001
`MutateItems`), upsert, claimed-work commit with side records, secondary
index queries, projection queries (API-004), multi-queue claim, and operator
repair (API-002). An implementation implements the families it supports.
Implementing a family means implementing all of it.

A consumer that needs a set of families names it as a profile (API-003).
The Snorri profile is the first.

### 6. Maintenance and coordination belong to the implementation

Projection verify, delete and rebuild, snapshots, log compaction, ownership
renewal, expired-lease sweeping and command positions are mechanisms of
particular implementations. They are methods on the implementation's own
type, not on the interface or on `Fireweed`. An embedder that needs them
keeps a handle to the concrete implementation.

Time is the exception that crosses the boundary: implementations take an
injected `Clock` so conformance can control time.

### 7. One black-box conformance suite, with Postgres as the oracle

`fireweed-conformance` is rewritten against `fireweed-interface` only. It
takes an implementation, reads its `Guarantees` and `Capabilities`, and runs
every scenario that applies. Each implementation crate runs it from its own
tests.

The Postgres reference implementation is also the oracle. A differential
test applies the same generated operation sequence, under a manual clock,
to the reference and to the implementation under test, and compares
per-item results and final queue state. Comparisons cover only
deterministic behaviour: strict ordering, and outcomes after any declared
visibility bound has elapsed.

### 8. The Postgres reference is a direct relational design

The reference keeps queue state in tables, claims with
`SELECT … FOR UPDATE SKIP LOCKED`, runs each request in one transaction,
stores request-id responses and purge tombstones in tables, and gets
multi-process safety from the database. It declares `Durable`, `Immediate`
and `MultiProcess`. It is a new crate. The existing log-replay code in
`fireweed-postgres` is not the reference.

TD-002 is the starting design. Its schema, indexes, transaction flows,
group and cohort locking, and lease-expiry rules carry over. Two things
change. The implementation targets API-006, not the engine ports. Its
command table becomes optional audit, not a log the item tables must be
rebuildable from. The database's own durability and recovery are the
durability guarantee.

### 9. The facade and RESP adapter sit on the interface

`Fireweed` (API-005) becomes a thin, concrete wrapper over
`Arc<dyn QueueProvider>`, plus optional extension handles. It depends on
`fireweed-interface` only. The embedder builds an implementation from that
implementation's crate and passes it to `Fireweed::new`. `fireweed-resp`
depends on the core trait only, so the RESP server runs over any
implementation.

ADR-007's two driving interfaces (the Rust library and RESP) and ADR-022's
single concrete `Fireweed` type stand. What changes is what sits under them.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep ADR-024: one cell, storage ports as the seam | No rework; matches the shipped binary | Contradicts the product intent; no second implementation is possible without editing the facade; Postgres cannot be a natural reference | Rejected |
| Restore the 12-cell `Log × Projection` matrix | Re-uses existing adapters | Still forces every implementation through a command log; multiplies cells without defining a queue contract | Rejected |
| Make the engine's storage ports public and stable | Smallest code change | Publishes 53 implementation-shaped traits as the contract; locks every future implementation into CQRS | Rejected |
| **Queue operation model as the boundary, declared guarantees, black-box conformance, Postgres oracle** | Matches the intent; any design can implement it; correctness is defined independently of storage | Large migration; DTOs must move out of `fireweed-engine`; some current facade methods move off the public type | **Proposed** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | A new implementation needs only `fireweed-interface`, its drivers and the conformance suite. |
| Positive | Correctness is defined by observable behaviour and checked against a reference, not by matching one storage design. |
| Positive | The public API loses storage vocabulary: no cells, barriers, durability classes or projection coverage. |
| Positive | RESP and the facade work over every implementation. |
| Negative | Request and response types now in `fireweed-engine` and `fireweed` move into `fireweed-interface`. Downstream imports change. |
| Negative | Eighteen current `Fireweed` methods leave the public type, and the extension families move behind accessors (see API-006 "Mapping from the current facade"). Snorri must migrate. |
| Negative | The conformance suite is rewritten. The existing log-seam scenarios become internal tests of the object-log implementation. |
| Neutral | The object-log × Turso code survives as one implementation, with ADR-012, ADR-013, ADR-015, ADR-016 and ADR-017 as its internal design. |

### Documents this ADR changes on acceptance

- **ADR-024** is superseded. "One public cell" is replaced by "implementations
  of API-006".
- **ADR-007** is amended. The hexagon's driven side is the queue interface,
  not storage ports.
- **ADR-012, ADR-013, ADR-016, ADR-017** are rescoped to the object-log
  implementation's internal design. They stop being product law.
- **ADR-022** stands, except that `projection_control()` moves to the
  implementation type.
- **API-001** loses storage language (object log, `AsyncProjection`,
  projection-position fence, `PurgeItemsCommand`, `fireweed_group_summary`,
  observed projection frontier) and gains a `ReassignLeases` section. Its
  visibility rules are restated in terms of declared `Guarantees`.
- **API-004** keeps its semantics as the projection-query extension.
- **TD-002** is revised to target API-006, drop the engine-port and
  `LogStore`/`ProjectionStore` framing, and make the command table optional.
- **API-005** is rewritten as the binding of API-006 to `Fireweed`.
  `StorageConfig` leaves the facade. Each implementation crate owns its own
  configuration type.

### Migration order

1. Create `fireweed-interface` with the API-006 traits and types. Move the
   types from `fireweed-engine` and `fireweed`. No behaviour change.
2. Wrap the current object-log × Turso runtime as the first implementation
   of the core trait and the extensions it already supports.
3. Rewrite `fireweed-conformance` as the black-box suite. Run it against
   that implementation.
4. Build the Postgres reference implementation in a new crate. Add the
   differential test.
5. Move `Fireweed` and `fireweed-resp` onto `fireweed-interface` only. Move
   implementation-specific methods to the implementation type.
6. Add a dependency-direction test (below). Retire the `StorageConfig`
   matrix and `RETIRED_STORAGE_CELL`.

## Open questions for the deciders

1. **Does object-log × Turso stay a supported implementation?** Proposed:
   yes, declaring `Durable`, `Bounded { max_ms }` and `MultiProcess`.
2. **Is `Bounded` new-work visibility acceptable at all?** If no, every
   implementation must be `Immediate`, and the object-log implementation
   must wait for apply before returning. Proposed: allowed and declared.
3. **Is the Postgres reference supported in production, or only as the
   test oracle?** Proposed: supported in production.
4. **Which extension families make up the Snorri profile?** Proposed: item
   mutation, upsert, claimed-work commit, secondary index queries and
   projection queries.
5. **Names.** `fireweed-interface`, `QueueProvider` and the extension trait
   names are working names.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| The interface copies the current facade's 70 methods and stays implementation-shaped | M | H | API-006 groups the current methods into a small core and named families. Single-item and named-outcome wrappers (`push`, `ack`, `retry_after`) stay on `Fireweed` as helpers over the trait. The `_at` forms are removed because time comes from the injected `Clock`. |
| Bounded visibility leaks into safety behaviour | M | H | API-006 makes safety effects immediate for every implementation, and the conformance suite tests them under the maximum declared bound. |
| The Postgres reference grows its own extensions and becomes a second product shape | L | M | The reference implements only what API-006 defines. New behaviour goes into API-001 or an extension contract first. |
| Migration stalls halfway, leaving two boundaries | M | H | Each migration step lands with the conformance suite green on every implementation that exists at that point. Step 6's dependency test makes the end state checkable. |

## Validation

| Success metric | Review trigger |
|----------------|----------------|
| `cargo metadata` shows `fireweed-interface` depending on no `fireweed-*` crate except `fireweed-core` | A storage or engine crate enters its dependency tree. |
| `fireweed` and `fireweed-resp` depend on `fireweed-interface` and no implementation crate (except as dev-dependencies) | A facade or RESP change needs an implementation type. |
| No implementation crate depends on another implementation crate | A shared need is met by importing another implementation instead of a shared library. |
| The black-box conformance suite passes against every implementation, and the differential test passes against the Postgres reference | An implementation is released without passing both. |
| No public item in `fireweed-interface` names a log, command, projection, snapshot, epoch or cell | Such a type is proposed for the interface. |

## Concern Impact

- `resilience`: durability becomes a declared guarantee per implementation.
  Recovery mechanisms (log replay, projection rebuild, database recovery)
  are each implementation's business.
- `technology-radar`: storage engines are chosen per implementation. Adding
  one no longer changes the product contract.

## References

- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/contracts/API-006-queue-implementation-interface.md`
- `docs/helix/02-design/adr/ADR-007-hexagonal-architecture-and-two-interfaces.md`
- `docs/helix/02-design/adr/ADR-012-orthogonal-log-projection-composition.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
- `docs/helix/02-design/technical-designs/TD-002-postgres-native-reference-mode.md`
- `crates/fireweed/src/lib.rs` (`LibBackend`)
- `crates/fireweed-conformance/src/lib.rs` (`ConformanceBackend`)
