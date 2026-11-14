---
ddx:
  id: adr-queue-interface-implementation-boundary
  depends_on:
    - api-native-client-interface
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-queue-as-shard-unit-and-projection-families
    - adr-orthogonal-log-projection-composition
    - adr-log-single-source-of-truth
    - adr-concrete-fireweed-facade-and-optional-controls
    - adr-s3-turso-public-cell
    - td-postgres-native-reference-mode
  links:
    - {kind: informed_by, to: adr-hexagonal-architecture-and-two-interfaces}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
  status: proposed
---

# ADR-025: The queue interface is the implementation boundary

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-11-13 | Proposed | Project owner | API-001, API-002, API-003, API-004, API-005, API-006, ADR-007, ADR-008, ADR-012, ADR-013, ADR-022, ADR-024, TD-002, TD-006 | Medium |

Revision 2 (2026-11-13) records the owner's answers to seven decisions
raised in adversarial review, and corrects the defects that review found.

## Context

Fireweed is meant to be one optimized interface for priority work queues,
with several implementations behind it. A PostgreSQL implementation is the
reference: the simplest correct one, and the one other implementations are
compared against.

The repository does not deliver that. The queue semantics are written down
in API-001, but no implementation can be written against them. Five
decisions put the boundary somewhere else.

1. **ADR-007 drew the hexagon around storage.** Driven adapters "implement
   the storage ports". The engine above them owns the claim path,
   idempotency, fencing and validation. A second implementation plugs in
   *below* the engine, not beside it.
2. **ADR-008 made one owner node per queue, fenced by a log epoch, product
   law**, and API-001 justifies whole-group and whole-cohort atomicity by
   that owner. A database that gives the same atomicity through row locks
   does not fit the model.
3. **ADR-012 made every backend `LogStore × ProjectionStore × ControlPlane`.**
   That is the internal structure of one design, stated as the shape every
   backend must have.
4. **ADR-013 made the command log mandatory.** "There is no architecture that
   drops `LogStore`."
5. **ADR-024 then reduced the product to one cell**, S3 object log × Turso
   projection, and API-005 bound the public Rust API to it.

The contracts inherited those internals. API-001 requires
`applied_command_position` in the `SetGates` response and defines discovery
freshness as an "observed projection frontier". API-002 exposes
`last_command_position`. API-004 defines reads by locally applied projection
position. TD-006 requires an engine-owned `ReclaimDriver`.

TD-002 shows the same drift. It specified the Postgres reference with queue
state in relational tables and claim as one `FOR UPDATE SKIP LOCKED`
statement. It also required a command log written in the same transaction
("the log is never optional"), `LeaseExpired` commands, and epoch-fenced
sweepers. The hexagonal migration deleted the crate and deferred a rebuild
"fresh to the engine ports".

The code follows these decisions.

| Observation | Evidence |
|-------------|----------|
| The trait set an implementer faces is one design's internals | `fireweed-engine` exports 53 public traits. Queue operations (`ClaimPort`, `PushPort`, `FinalizePort`) sit beside `LogStore`, `ProjectionStore`, `CommitStrategy`, `UnifiedAtomicCommitter`, `OwnedTaskDispatcher`, the async planners, `SnapshotStore` and `AsOfProjectionStore`. |
| There is no public trait to implement | The facade's backend bound, `LibBackend` (21 traits), is `pub(crate)` in `crates/fireweed/src/lib.rs`. |
| The facade owns implementation code | `crates/fireweed` depends on the memory, objectlog, postgres, projection and turso crates, and holds 7.8k lines of Turso composition in `turso_compose.rs`. |
| Postgres is forced into another implementation's shape | `fireweed-postgres` (24.5k lines) depends on `fireweed-objectlog` and `fireweed-projection`, and carries `async_log_replay.rs`, `compose_log.rs` and `async_objectlog_postgres.rs`. |
| Conformance tests the log seam, not the queue | `run_conformance<B: ConformanceBackend>` requires `SnapshotStore + LogRead`, and tests build `CommandEnvelope`s by hand. |
| RESP is bound to engine ports | `RespBackend` requires `UpsertPort`, `ReclaimDriver`, `ControlPlaneStore`, `HotProjectionQueryPort` and `ProjectionRead` (`crates/fireweed-resp/src/lib.rs`). |
| Internals leak into the public API | API-005 exposes `DurabilityClass`, `ResponseBarrier::AsyncProjection`, `projection_control`, snapshot and compaction methods, and "empty claim may be apply lag". |

## Decision

### 1. The boundary is the queue operation model

API-001 defines what a Fireweed queue does. That operation model, as Rust
traits, is the boundary every implementation implements. The traits and
their request and response types live in one crate, `fireweed-interface`
(working name), defined by API-006.

`fireweed-interface` depends on `fireweed-core` and general-purpose crates
only. Its public items name no log, command, command position, projection,
snapshot, epoch, owner or storage cell.

The interface types are defined from API-001, not moved from the engine.
Where today's engine types are missing API-001 fields (for example
`QueueMetrics` has only lifecycle counts), the implementation adds them; the
interface does not shrink to fit.

### 2. An implementation is a crate that implements the interface

An implementation crate implements the API-006 traits with whatever internal
design suits it. It depends on `fireweed-interface`, `fireweed-core`, its own
drivers, and any shared internal library it chooses (for example the current
log and projection engine). It does not depend on another implementation
crate. `fireweed-interface` never depends on a shared internal library.

### 3. Implementations differ only in declared guarantees

Every implementation gives the same observable results for the same history,
with three exceptions that it declares in `Guarantees` and that conformance
tests.

| Guarantee | Values | Meaning |
|-----------|--------|---------|
| Durability | `Process`, `Durable` | Whether a returned mutation survives a crash and reopen. |
| New-work visibility | `Immediate`, `Bounded { max_ms }` | How soon a returned mutation's new-work effects become claimable and readable. `max_ms` is elapsed wall time, not `Clock` time. |
| Sharing | `SingleProcess`, `MultiProcess` | Whether several instances may serve the same storage at once. |

The ordering model is linearizable per queue. Every operation takes effect
at one point between its call and its return. A claim or read observes every
**safety effect** (a lease, finalize, purge, gate block, pause, or update
that removes eligibility) whose point precedes its own, for every
implementation. Only **new-work effects** (an item becoming claimable
because of a mutation) may lag, and only by the declared bound.

A `Bounded` implementation that cannot make a returned mutation visible
within `max_ms` fails closed: claims and reads on that queue return
`Unavailable` until it can. Time-driven effects are not subject to the
bound. Once an item is visible, its `not_before`, lease expiry and retry
backoff are evaluated against the clock at claim time.

### 4. Time is part of the contract (owner decisions 1 and 2)

**Observable state is a function of returned mutations and time.** Lease
expiry, cohort expiry, retention expiry and `not_before` take effect at their
due instant, evaluated against queue state as of that instant. Before an
operation at time `t` is served, the observable result equals processing
every transition due at or before `t` in due-instant order. An
implementation may realize this with sweepers, lazily on access, or both.
The contract does not care which, and no caller drives a tick.

API-001 changes to match. Lease expiry MUST NOT change `item_version` (it
says MAY today), so a lazy implementation and a sweeping one agree. Cohort
expiry is evaluated against the cohort's state at its deadline.

**One time authority per storage.** An implementation samples time for a
transition from one authority, after it has serialized that transition
against conflicting ones (for example after taking row locks). All instances
sharing storage use the same authority. The Postgres reference uses database
time in production. Under conformance, every implementation uses the
injected `Clock` as its authority.

Relative schedules (`retry after 30 s`) are resolved by the implementation
at the transition's time, so callers never compute absolute times from a
clock they do not own.

### 5. Queue features are declared, not discovered by failure

Optional queue features (cohorts, group batching, recurrence, dynamic gates,
metadata blockers, typed payload schemas, secondary indexes, and priority
kinds other than timestamp and int64) are listed in `Capabilities`.
`create_queue` rejects a definition that needs an unlisted feature with
`FeatureUnsupported`, before creating anything. An operation on an existing
queue never fails because the implementation lacks a feature.

### 6. A small core, with optional families (owner decisions 5 and 7)

The core trait is API-001's batch operations except `MutateItems`, plus
lease reassignment. Every implementation implements all of it.

Other operation families are extension traits. Implementing a family means
implementing all of it for every queue it applies to.

| Family | Source of semantics |
|--------|---------------------|
| `Upsert` | API-006 (TD-006 pending-item replacement) |
| `ItemMutation` | API-001 Atomic Item Mutation, now optional |
| `ClaimedWorkCommit` | API-006 (today only Rust doc comments) |
| `IndexQuery` | ADR-010, ADR-011 |
| `ProjectionQuery` | API-004 |
| `MultiQueueClaim` | API-006 |
| `StreamGroup` | API-006, TD-006: Redis Streams consumer-group leases, pending-entry listing, token-less acknowledgement |
| `OperatorRepair` | API-002, now optional |

A consumer states its needs as a profile: families, queue features, minimum
guarantees and minimum limits. RESP is a consumer with the `resp-worker`
profile (`Upsert` plus `StreamGroup`). Snorri has the `snorri` profile.

### 7. Leases are fenced by token (owner decision 6)

Core renew, reassign and finalize address a lease by `item_id` plus
`lease_token` (or cohort id plus cohort token), and report per-item
outcomes, as API-001 specifies. The current `Fireweed` helpers that
finalize by item id alone, all-or-nothing, are removed. Snorri migrates to
token-addressed, per-item calls.

`StreamGroup` is the one exception. Redis Streams lets any member of a
consumer group acknowledge an entry by id. That family provides token-less
acknowledgement and claim by worker name, with weaker fencing that API-006
states explicitly. It exists for RESP compatibility and is not a substitute
for the core path.

### 8. Single ownership becomes one implementation's design (owner decision 3)

ADR-008's one-owner-per-queue rule, epoch fencing and redirects become
internal design of the object-log implementation. The contract states the
atomicity it requires (whole groups and whole cohorts leased all-or-nothing,
no two active leases on one item) without saying how. The Postgres reference
gets it from transactions and row locks.

The object-log implementation declares `SingleProcess`. A second instance on
the same storage supersedes the first: every later operation on the first
fails with `InstanceSuperseded`. Multi-node object-log deployments with
redirects are outside the interface until that implementation can serve or
forward any queue from any instance. The RESP cluster is single-node today
(`crates/fireweed-resp/src/cluster.rs`), so nothing shipped is lost.

### 9. Maintenance belongs to the implementation

Projection verify, delete and rebuild; snapshots and log compaction;
ownership renewal; command positions: these are methods on the
implementation's own type. They are not on the interface or on `Fireweed`.
An embedder that needs them keeps a handle to the concrete type.

### 10. Black-box conformance, with Postgres as the oracle

`fireweed-conformance` is rewritten against `fireweed-interface` only. It
reads `Guarantees` and `Capabilities` and runs every applicable scenario,
including concurrent claims, crash and reopen in a killed child process for
`Durable`, peers in separate processes for `MultiProcess`, supersession for
`SingleProcess`, and fail-closed behaviour for `Bounded`.

A separate test-only crate runs a differential test: the same generated
sequential history, under a manual clock, against the Postgres reference
and the implementation under test, comparing outcomes and state. A
difference is a defect in one of the two, settled by API-001.

### 11. The Postgres reference is built from TD-002

The reference is a new crate. It keeps TD-002's queue-state tables, indexes,
`SKIP LOCKED` claim, and group and cohort locking. It drops the mandatory
command log, `LeaseExpired` commands, `assignment_epoch` fencing and
epoch-fenced sweepers, which ADR-013 and ADR-008 required. Time-driven
transitions follow §4. A command table may remain as audit. It declares
`Durable`, `Immediate` and `MultiProcess`, and is supported in production.

### 12. The facade and RESP sit on the interface

`Fireweed` (API-005) becomes a concrete wrapper over
`Arc<dyn QueueProvider>`, depending on `fireweed-interface` only. The
embedder builds an implementation from its crate and passes it to
`Fireweed::new`. `fireweed-resp` depends on `fireweed-interface` only and
requires the `resp-worker` profile. `fireweed-server` is the composition
root and is the one crate that names concrete implementations.

ADR-007's two driving interfaces and ADR-022's single concrete `Fireweed`
stand. What sits under them changes.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep ADR-024: one cell, storage ports as the seam | No rework | Contradicts the product intent; no second implementation without editing the facade | Rejected |
| Restore the 12-cell `Log × Projection` matrix | Re-uses existing adapters | Still forces every implementation through a command log; defines no queue contract | Rejected |
| Make the engine's storage ports public and stable | Smallest change | Publishes 53 implementation-shaped traits; locks future implementations into CQRS | Rejected |
| Every implementation `Immediate` | Simpler contract | Object-log must wait for apply before returning, which defeats its batching design | Rejected (owner decision 4) |
| Keep id-only, all-or-nothing finalize in the core | No Snorri migration | Weakens API-001's token fencing for every implementation | Rejected (owner decision 6) |
| **Queue operation model as the boundary, declared guarantees, time in the contract, black-box conformance, Postgres oracle** | Matches the intent; any design can implement it; correctness independent of storage | Large migration; contract and code changes listed below | **Proposed** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | A new implementation needs `fireweed-interface`, its drivers and the conformance suite, nothing else. |
| Positive | Correctness is defined by observable behaviour and checked against a reference. |
| Positive | The public API loses storage vocabulary. |
| Positive | Time-driven behaviour is deterministic under a manual clock, so expiry is testable. |
| Negative | Interface types are new. Downstream imports and some shapes change. |
| Negative | Snorri migrates from id-only, all-or-nothing finalize to token-addressed, per-item calls, and from `commit_capabilities` to the `ClaimedWorkCommit` family. |
| Negative | The object-log implementation must meet the safety-effect rule for every effect, not only gates, and must report missed visibility bounds as `Unavailable`. Neither is verified yet. |
| Negative | Multi-node object-log deployment is outside the interface until that implementation can serve any queue from any instance. |
| Neutral | Object-log × Turso survives as one implementation, with ADR-008, ADR-012, ADR-013, ADR-015, ADR-016 and ADR-017 as its internal design. |

### Documents this ADR changes on acceptance

| Document | Change |
|----------|--------|
| ADR-024 | Superseded. |
| ADR-007 | Amended: the driven side of the hexagon is the queue interface. |
| ADR-008 | Rescoped to the object-log implementation. Its atomicity outcomes move into API-001 as requirements stated without an owner. |
| ADR-012, ADR-013, ADR-016, ADR-017 | Rescoped to the object-log implementation. |
| ADR-022 | Stands, except `projection_control()` moves to the implementation type. |
| API-001 (major version) | Remove storage language (object log, `AsyncProjection`, projection-position fence, `PurgeItemsCommand`, `fireweed_group_summary`, observed projection frontier). Remove `SetGates.applied_command_position`; keep `gate_epoch`. Make `BatchClaimByItemIds` required core and drop its capability flag. Make `MutateItems` an optional family. Add `ReassignLeases`, item-id addressing for live reads, and the time-driven transition rule. Change lease expiry to MUST NOT change `item_version`. Give cohort renew and finalize the common status vocabulary. Restate whole-group and whole-cohort atomicity without an owner. Restate visibility, discovery `as_of` and metrics in terms of API-006 guarantees. |
| API-002 | Becomes the optional `OperatorRepair` family. Replace `last_command_position` and "durable terminal command position" with `item_version` and timestamps. Remove Class B text. |
| API-003 | Defines profiles in API-006 terms (families, features, guarantees, limits). |
| API-004 | Becomes the `ProjectionQuery` family. Restate the watermark rule as read-your-writes on the queue's linearization order. |
| API-005 | Rewritten as the binding of API-006 to `Fireweed`. `StorageConfig` leaves the facade. |
| TD-002 | Revised per §11. |
| TD-006 | Bound to the `resp-worker` profile. Line 252's required `ReclaimDriver` is replaced by the §4 time rule. |
| AGENTS.md | The projection-lifecycle test rule names the object-log implementation's own projection control, not `Fireweed::projection_control()`. |

### Migration order

Each step lands with the conformance suite green on every implementation
that exists at that point, and with the dependency test enforcing the
crates that exist.

1. Create `fireweed-interface` with the API-006 traits and types, defined
   from API-001. Add the dependency test for that crate.
2. Implement `QueueProvider` and the families it already supports over the
   current object-log runtime. Fill gaps the interface exposes (missing
   metrics fields, lease data in reads, the time rule).
3. Rewrite `fireweed-conformance` as the black-box suite. Run it against
   the object-log implementation.
4. Cut `Fireweed` and `fireweed-resp` over to the interface. Extend the
   dependency test to them. From here there is one boundary.
5. Build the in-memory implementation. It is the first second
   implementation and proves interchangeability cheaply.
6. Build the Postgres reference: core first, then families. Add the
   differential test.
7. Retire `StorageConfig` and `RETIRED_STORAGE_CELL`. Apply the document
   changes above.

## Remaining open questions

1. **Names.** `fireweed-interface`, `QueueProvider` and the family names are
   working names.
2. **Snorri profile.** Proposed: `Upsert`, `ItemMutation`,
   `ClaimedWorkCommit`, `IndexQuery`, `ProjectionQuery`; features
   `TypedPayloadSchema`, `SecondaryIndexes`, `DynamicGates`; `Durable`.
   Snorri's owners should confirm.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Object-log claims can return an item whose purge, pause or eligibility-removing update has committed but not applied | M | H | Conformance tests each safety effect under induced apply lag before step 4. If it fails, object-log fences those effects as API-001 already requires for gates. |
| The interface copies the facade's 84 methods and stays implementation-shaped | M | H | API-006 maps every method. Convenience forms stay on `Fireweed` as helpers over the trait. |
| The time rule is costly for a lazy implementation (cohort expiry against state at the deadline) | M | M | Sweeping at the due instant satisfies the rule. Laziness is allowed, not required. |
| The Postgres reference grows behaviour outside the contract | L | M | New behaviour goes into API-001, API-006 or a family contract first. |
| Migration stalls with two boundaries | M | H | Step 4 removes the old boundary before any new implementation is built. |

## Validation

| Success metric | Review trigger |
|----------------|----------------|
| `cargo metadata`: `fireweed-interface` depends on no `fireweed-*` crate except `fireweed-core` | A storage or engine crate enters its tree. |
| `fireweed`, `fireweed-resp` and `fireweed-conformance` depend on no implementation crate outside `[dev-dependencies]` | A facade or RESP change needs an implementation type. |
| No implementation crate depends on another | A shared need is met by importing an implementation. |
| Every implementation passes the black-box suite for what it declares; the Postgres reference passes the differential test against each | An implementation released without both. |
| Time-driven scenarios (lease expiry, cohort expiry, retention) pass under a manual clock with no tick | A test or caller needs to drive a tick. |

## Concern Impact

- `resilience`: durability is a declared guarantee per implementation.
  Recovery is each implementation's business.
- `technology-radar`: storage engines are chosen per implementation; adding
  one does not change the product contract.

## References

- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/contracts/API-006-queue-implementation-interface.md`
- `docs/helix/02-design/adr/ADR-007-hexagonal-architecture-and-two-interfaces.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/02-design/adr/ADR-012-orthogonal-log-projection-composition.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
- `docs/helix/02-design/technical-designs/TD-002-postgres-native-reference-mode.md`
- `docs/helix/02-design/technical-designs/TD-006-resp-wire-adapter.md`
- `crates/fireweed/src/lib.rs` (`LibBackend`), `crates/fireweed-conformance/src/lib.rs` (`ConformanceBackend`), `crates/fireweed-resp/src/lib.rs` (`RespBackend`)
