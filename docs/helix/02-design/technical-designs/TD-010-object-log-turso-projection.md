---
ddx:
  id: td-object-log-turso-projection
  depends_on:
    - adr-full-async-storage-boundaries
    - adr-async-commit-strategy-and-dispatch
    - adr-turso-derived-projection
    - adr-log-single-source-of-truth
    - td-storage-architecture-backend-contracts
    - td-s3-object-log-sqlite-projection-mode
    - api-native-client-interface
    - concerns
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: td-storage-architecture-backend-contracts}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: concerns}
  status: accepted
  review:
    self_hash: ef2026084d244dee4376217af4e3c5d9b9d80628edac3a8aeffc0876660d286f
    deps:
      adr-full-async-storage-boundaries: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
      adr-log-single-source-of-truth: 35052eb1b94371aa8abb8e8b348a21b459522c7d5feaba04b7146745a04bda62
      adr-turso-derived-projection: 76ec5fe8523c4fe831441229aa5f09f0bf966ac3849174764a7ba2c2d805f22a
      api-native-client-interface: 852a753af558d8b8a21e4a86e87915b14c030fefcb4a27473bcbb08cfe044580
      concerns: 73756937e564b8120ca99407bacbd1fa67a06c6021a822c2cb321f7c9d95056e
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
      td-storage-architecture-backend-contracts: f77d88cfdd2f4ad3c23d7f0310c5164eaecc57742f469cdc062accda44484a54
    reviewed_at: "2026-07-18T02:36:05Z"
---

# Technical Design: TD-010 Object-log + Turso Projection

**Contract**: API-001 | **ADR**: ADR-013, ADR-015, ADR-016, ADR-017 | **Scope**: native-async derived projection

## Scope

This design adds Turso Database as a local, rebuildable relational projection paired with the segmented
object log. It defines the production adapter, shared relational substrate, server profile, recovery,
failure behavior, and verification gates.

In scope:

- async storage-axis migration required to call a native-async projection without blocking;
- a driver-neutral relational schema/codec/query substrate shared by SQLite and Turso;
- `pqueue-turso`, pinned to Turso 0.7.0 in ordinary local WAL mode;
- feature-gated `objectlog/turso` server composition;
- full relational differential, cancellation, replay, reopen, and server conformance.

Out of scope:

- Turso as command-log authority, control plane, remote database, or embedded replica;
- replacing the standalone SQLite durable profile;
- experimental Turso MVCC, sync, FTS, or remote/cloud features;
- Niflheim changes and deferred quiet-host tests;
- a new broad GitHub Actions matrix dimension.

## Technical Approach

**Strategy**: complete the async storage boundary from ADR-015, extract driver-neutral relational facts,
then implement Turso directly against the async projection axis. Compose it only behind the object log,
whose manifest remains the acknowledged-command authority.

**Key decisions**:

- The engine owns typed async commit operations; no arbitrary async transaction closure is public.
- Async axes use shared receivers. `AsyncComposedBackend` takes a typed `SeparateReplayCommit` strategy
  for object-log profiles plus an injected runtime-neutral owned-task dispatcher; it never holds an
  axis-wide async mutex or infers append/apply sequencing from a durability enum.
- Shared SQL, schema, codecs, and typed rows live in `pqueue-relational`; Turso never depends on the
  SQLite adapter and the schema is not copied.
- `TursoProjectionStore` owns `turso::Database` plus a connection/transaction coordinator using
  `tokio::sync` primitives. The first implementation serializes writes to match the reference projection,
  while reads use independently configured connections when safe.
- Mutations transfer owned request data and an owned connection capability to ADR-015's bounded commit
  task before the driver transaction begins. Dropping the caller awaiter cannot cancel a started commit;
  shutdown drains started tasks and replay resolves any process-loss outcome.
- Every applied batch uses one immediate transaction. Projection rows, indexes, replay outcome, counters,
  and applied cursor commit together; an overlapping prefix is idempotent and a gap is rejected.
- Public server selection is feature-gated. Default builds and profiles remain unchanged.

**Trade-offs**: preserving relational SQL minimizes semantic porting risk, but Turso's pre-1.0 API and
build size require an exact pin and a focused validation lane.

## Component Changes

### Modified: async engine storage boundaries

- **Current state**: operation ports return futures, while storage axes and `Backend::write` are
  synchronous under a standard mutex.
- **Changes**: introduce native async axes, typed raw commit/fault controls, whole-transaction blocking
  adapters, explicit atomic-versus-replay commit strategies, injected owned-task dispatch, per-queue
  mutation gates, async recovery/inspection, and staged removal of the legacy sync seam.
- **Files**: `crates/pqueue-engine/src/port.rs`, `crates/pqueue-engine/src/compose.rs`,
  `crates/pqueue-engine/src/lib.rs`, conformance fault/scenario modules.

### New: driver-neutral relational substrate

- **Purpose**: one exact schema and one set of encodings/query constants for SQLite and Turso.
- **Interfaces**: pure owned values in; SQL text, bound values, and typed rows out.
- **Files**: `crates/pqueue-relational/src/{lib,schema,codec,row}.rs`.

The extraction must be behavior-preserving for SQLite before Turso consumes it. Driver transaction and
row APIs remain in their adapter crates.

### New: `pqueue-turso`

- **Purpose**: native-async local derived projection.
- **Interfaces**: async projection/read/recovery axes from TD-001.
- **Files**: `crates/pqueue-turso/src/{lib,config,apply,query,error}.rs` and `tests/**`.

`open` performs, consumes, and verifies individual settings for WAL, synchronous normal (`1`), and busy
timeout `5000`. It does not send the rusqlite PRAGMA batch through `execute_batch`, because the probe proved
that call can report failure after changing journal mode.

### Modified: segmented object-log composition

- Generalize the current object-log + SQLite projection backend over the async derived-projection
  contract while preserving its existing public alias.
- Add a feature-gated Turso alias/profile and await create, replay, apply, read, recovery, and shutdown.
- Ack only after object-log manifest commit and the profile's required Turso response barrier. A lost
  response is resolved from the durable log/request outcome.
- **Files**: `crates/pqueue-server/src/object_log_sqlite.rs`, server configuration and tests.

### Modified: blocking reference adapters

- Memory moves directly to async axes.
- SQLite, blocking Postgres, and filesystem/object-log work use explicit whole-transaction blocking
  wrappers below the async port.
- Existing composition-root `BlockingBackend` shims are removed after call-site parity.

## API/Interface Design

| Surface | Governing Contract | Story-Level Usage |
|---------|--------------------|-------------------|
| Queue operations and transaction outcomes | API-001 | No client-visible semantic change. |
| Backend profile selection | TD-001 / ADR-016 | Adds feature-gated `objectlog/turso`; unsupported builds fail configuration explicitly. |
| Storage traits | ADR-015 / TD-001 | Runtime-neutral `Send` futures; no Tokio type crosses into domain ports. |

The exact environment/config spellings are owned by the existing server configuration surface. The
implementation uses `PQUEUE_PROJECTION_BACKEND=turso` and `PQUEUE_TURSO_PROJECTION_PATH` only if those
names pass the server's existing compatibility and validation conventions.

## Data Model Changes

No logical schema change is intended. Turso executes the exact relational projection schema extracted
from the SQLite reference. The adapter-specific migration table records schema and adapter version. An
upgrade refuses a newer/unknown schema until the compatibility probe and migrations have passed.

## Integration Points

| From | To | Method | Data |
|------|----|--------|------|
| `AsyncComposedBackend` / segmented backend | `SeparateReplayCommit` + async storage axes | owned dispatched task | typed commands, positions, expected epoch, replay outcome |
| `pqueue-turso` | Turso 0.7 local database | async driver | relational schema, transactions, queries |
| object log | Turso projection | ordered replay/apply | sealed committed batches |
| server config | feature-gated composition | validated construction | projection kind and local path |

### External Dependencies

- `turso = "=0.7.0"`, `default-features = false`.
- Tokio synchronization/runtime support stays in adapter and composition crates, not domain contracts.
- If Turso is unavailable or poisoned, new mutations fail with the existing typed storage/backpressure
  error; recovery rebuilds from the trusted object-log frontier.

## Security

- Authentication and authorization remain in the driving adapter/host per ADR-002.
- Every relational key includes tenant and queue identity; differential tenant-isolation tests are P0.
- Local database permissions follow the SQLite projection posture; Turso never weakens object-log
  encryption or retention requirements.
- Malformed/corrupt local state cannot advance the trusted log frontier or authorize trimming.

## Performance

- No blocking driver call on a Tokio worker; a single-thread heartbeat must advance throughout DB work.
- No connection, task, or loop per queue. Connections and background apply are bounded shared resources.
- Turso must meet the existing sub-second operation targets before default enablement; initial production
  status does not claim improvement over SQLite.
- CI adds one focused/path-filtered Turso job, not a projection-by-kind matrix multiplication.

## Testing

- [ ] **Schema/config**: exact shared schema, partial indexes, individual PRAGMA trap/readbacks.
- [ ] **Atomicity**: lifecycle/lease/index/replay outcome plus cursor in one transaction; injected rollback.
- [ ] **Differential**: every `QueueCommand` arm and every projection-read output equals SQLite before and
  after reopen.
- [ ] **Recovery**: overlapping replay no-op, gap rejection, snapshot-tail counter restore, reset/rebuild,
  manifest-sealed-before-apply crash.
- [ ] **Cancellation**: before append, after staging, during commit, after durable eventual append, and
  cancelled lock waiter.
- [ ] **Dispatch/strategy**: atomic profiles cannot construct with separate append/apply; object-log uses
  `SeparateReplayCommit`; submitted tasks survive caller drop; a stalled queue does not stall another.
- [ ] **Runtime**: blocking-reference wrappers and Turso both keep a single-thread heartbeat alive.
- [ ] **Server**: create/push/claim/finalize/renew/reassign/read/reopen and feature-disabled config error.
- [ ] **Concurrency**: 16 disjoint writers and deterministic same-active-key conflict.
- [ ] **Security**: tenant isolation and corrupted-local-projection fail-closed behavior.

## Migration & Rollback

- **Backward compatibility**: client operations are unchanged; new profile is opt-in.
- **Data migration**: none from SQLite is required. A Turso projection rebuilds from the object log into a
  fresh file and becomes eligible only after frontier/image verification.
- **Feature toggle**: compile feature plus projection profile selection.
- **Rollback**: disable the profile, discard the local Turso file, and rebuild the SQLite projection from
  the same authoritative object log.

## Implementation Sequence

1. Add typed async commit and cancellation conformance; remove raw closure call sites.
2. Add shared-receiver async storage axes, typed commit strategies, an owned-task dispatcher, and migrate
   reference composition/memory.
3. Wrap blocking SQLite/object-log/Postgres transactions and remove composition-root blocking shims.
4. Extract the driver-neutral relational substrate with SQLite parity.
5. Implement and differentially test `pqueue-turso`.
6. Wire feature-gated object-log + Turso server profile and focused CI/render validation.
7. Remove the remaining legacy synchronous axes after repository-wide conformance passes.

**Prerequisites**: ADR-015 and ADR-016 accepted; exact Turso 0.7 probe preserved; ADR-013 response and
rebuild rules unchanged.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Async migration becomes a flag-day rewrite | M | H | Additive traits, explicit wrappers, dependency-ordered beads, removal last. |
| Relational extraction changes SQLite behavior | M | H | Land extraction with SQLite-only parity before Turso code. |
| Full command corpus exposes unsupported Turso behavior | M | H | Differential test blocks profile enablement; keep SQLite rollback. |
| Cancellation leaves waiter or transaction stranded | M | H | Owned commit and lock-wait cancellation tests. |
| New CI work is over-scaled | M | M | One focused job; no broad matrix expansion. |
