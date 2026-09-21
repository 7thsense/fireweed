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
    self_hash: d05ba185fd6e0be01855a97f39abbce04a486adf4a6f2c69f14e236befecc25f
    deps:
      adr-async-commit-strategy-and-dispatch: 6daa55d01fce58248b5b607c3015ed0600d23ff123912e2bc1fd63a484a8ab49
      adr-full-async-storage-boundaries: 0543121229a415143387307275263908017b43697ddac970d54d6d30a2c7ccaa
      adr-log-single-source-of-truth: c88063a069f43bd90f31e4875ad8b35fca9876de5b52cb777908d314d46abd1b
      adr-turso-derived-projection: b93a1a9c4ba242940b86878551dddd35f9aa4e399357417c620e66f5ab2a7b67
      api-native-client-interface: b99403ef55afffd134ac3ef1a71065c497558c94de379c2b257b119000a0f488
      concerns: d00e29334f99ed2fe3c9151bacb107255a3d7add89606949e409eb6614382d6c
      td-s3-object-log-sqlite-projection-mode: 7770bb133f4ace189bfc715e3be6472f894f7c62d52adfc051540fea97c6a4b2
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
    reviewed_at: "2026-08-04T04:50:53Z"
---

# Technical Design: TD-010 Default local Turso derived projection

## Public cell (ADR-024)

ADR-024 supersedes the 2026-09-17 storage-selector amendment for public
selectors. The public product is one cell: S3 object-log × Turso projection
(`s3 log × turso projection`). `ResponseBarrier` has only `AsyncProjection`.
The other eleven axis pairs and `Strict` are not a roadmap. Class A durability
is the object log. Turso is rebuildable through `projection_control` and is
not the command log.

The 2026-09-17 amendment is historical. It does not define current selectors.
Historical DDx IDs, requirement IDs, artifact names, and original measurements
retain their identity. SQLite selectors stay retired and are not a differential
reference.


**Contract**: API-001 | **ADR**: ADR-012, ADR-015, ADR-016, ADR-017 | **Scope**: supported native-async local projection

## Disposition

This design governs the public `fireweed-turso` projection on the one public
cell (ADR-024): S3 object-log × Turso. `ResponseBarrier` has only
`AsyncProjection`. Storage is not a combined profile and not a multi-cell matrix.

- The public log selector is `s3`.
- The public projection selector is `turso`.
- No `objectlog`, `inmemory`, `hybrid`, `memory`, `sqlite`, `postgres`, or
  `filesystem` selector is a public cell. Those pairs are not a roadmap.
- A qualifying distribution enables Turso. A build that omits the feature rejects
  `turso` selection as feature-unavailable before storage I/O; it never silently
  falls back to another projection.
- The adapter consumes the provider-neutral `EngineError` and `CommitRejection` vocabulary; it
  defines no Turso-specific public error, capability, or RESP token.
- Change-record and history capability come from the object log, never from
  Turso. Turso rebuilds through `projection_control`.

## Scope

This design defines Turso Database as the rebuildable local relational
projection for the S3 object log (ADR-024). It defines the adapter, recovery,
failure behavior, and validation gates for that cell. It does not add another
public log or projection.

In scope:

- async storage-axis migration required to call a native-async projection without blocking;
- the relational schema/codec/query substrate the Turso adapter uses;
- `fireweed-turso`, pinned to Turso 0.7.2 in ordinary local WAL mode;
- Turso composition with the S3 object log and `AsyncProjection`;
- cancellation, replay from the object log, reopen, rebuild through
  `projection_control`, and conformance on that cell. This scope does not
  claim a 10M-resident or 1000-queue pass.

Out of scope:

- Turso as command-log authority, control plane, remote database, or embedded replica;
- SQLite as a supported projection or as a differential reference that blocks Turso;
- any public cell other than s3 × turso, and any `Strict` barrier;
- experimental Turso MVCC, sync, FTS, or remote/cloud features;
- Niflheim changes;
- remote, sync, embedded-replica, or MVCC Turso support.

## Technical Approach

**Strategy**: complete the async storage boundary from ADR-015, extract driver-neutral relational facts,
then implement Turso directly against the async projection axis. Compose it
with the S3 object log (ADR-024). That log remains the command authority. The
Turso file is not the command log.

**Key decisions**:

- The engine owns typed async commit operations; no arbitrary async transaction closure is public.
- Async axes use shared receivers. `AsyncComposedBackend` takes a typed `SeparateReplayCommit` strategy
  for object-log profiles plus an injected runtime-neutral owned-task dispatcher; it never holds an
  axis-wide async mutex or infers append/apply sequencing from a durability enum.
- Shared SQL, schema, codecs, and typed rows live in `fireweed-relational`; Turso never depends on the
  SQLite adapter and the schema is not copied.
- `TursoProjectionStore` owns `turso::Database` plus a connection/transaction coordinator using
  `tokio::sync` primitives. The first implementation serializes writes to match the reference projection,
  while reads use independently configured connections when safe.
- Mutations transfer owned request data and an owned connection capability to ADR-015's bounded commit
  task before the driver transaction begins. Dropping the caller awaiter cannot cancel a started commit;
  shutdown drains started tasks and replay resolves any process-loss outcome.
- Every applied batch uses one immediate transaction. Projection rows, indexes, replay outcome, counters,
  and applied cursor commit together; an overlapping prefix is idempotent and a gap is rejected.
- Public server and facade selection use the canonical `turso` projection value.
  It is the default in typed config, service config, and deployment config. The
  feature gates adapter availability, not the public name; omitted support
  fails closed before I/O.

### Governing object-log serving protocol

For a durable object log with a Turso derived projection, the optimization unit
is a compatible vector of public requests, not the items inside one public
request. A driver may microbatch at most eight FIFO-compatible requests, 800
requested rows, 4 MiB of rendered response data, and 20 ms of linger. It keeps
one request identity, outcome vector, response, and lease token per public
request. Compatibility may never merge outcomes or relax the public batch
limit.

Item Claim is log-first Claim. Under one committed read snapshot, the elected
driver selects candidates and pre-materializes each bounded full response,
including fields, metadata, entity values, gates, schedules, and lease tokens.
It closes the snapshot before object-log I/O, appends the authoritative Claim
envelopes, and retains those responses until their positions settle in Turso.
After publication, response continuation may neither re-render from Turso nor
borrow a projection connection; a durable operation therefore cannot fail with
capacity backpressure while returning its retained result.

Compatible Push, BatchUpdate, Claim, and Complete envelopes remain distinct
inside one packed append. The apply worker consumes the sealed vector intact,
in log order, in one Turso writer transaction; it does not unpack the vector
into per-request transactions. Same-queue mutations use at most two FIFO
generations and sixteen requests, and queued generations retain request structs
rather than cloned payloads or pre-rendered bodies.

The new serving path never writes SQL-first leases or a new Claim outbox row.
The migration window keeps the legacy outbox schema and recovery-only drain for
at least one release so a lease committed before upgrade can still publish on
reopen. Once the log-first path and reopen test are qualified, new outbox writes
and the obsolete SQL-first serving path are removed separately.

**Trade-offs**: preserving relational SQL minimizes semantic porting risk, but Turso's pre-1.0 API and
build size require an exact pin and a focused validation lane.

## Component Changes

### Modified: async engine storage boundaries

- **Current state**: operation ports return futures, while storage axes and `Backend::write` are
  synchronous under a standard mutex.
- **Changes**: introduce native async axes, typed raw commit/fault controls, whole-transaction blocking
  adapters, explicit atomic-versus-replay commit strategies, injected owned-task dispatch, per-queue
  mutation gates, async recovery/inspection, and staged removal of the legacy sync seam.
- **Files**: `crates/fireweed-engine/src/port.rs`, `crates/fireweed-engine/src/compose.rs`,
  `crates/fireweed-engine/src/lib.rs`, conformance fault/scenario modules.

### New: driver-neutral relational substrate

- **Purpose**: one exact schema and one set of encodings/query constants for SQLite and Turso.
- **Interfaces**: pure owned values in; SQL text, bound values, and typed rows out.
- **Files**: `crates/fireweed-relational/src/{lib,schema,codec,row}.rs`.

The extraction must be behavior-preserving for SQLite before Turso consumes it. Driver transaction and
row APIs remain in their adapter crates.

### New: `fireweed-turso`

- **Purpose**: native-async local derived projection.
- **Interfaces**: async projection/read/recovery axes from TD-001.
- **Files**: `crates/fireweed-turso/src/{lib,config,apply,query,error}.rs` and `tests/**`.

`open` performs, consumes, and verifies individual settings for WAL, synchronous normal (`1`), and busy
timeout `5000`. It does not send the rusqlite PRAGMA batch through `execute_batch`, because the probe proved
that call can report failure after changing journal mode.

### Modified: public 5×4 composition

- Add Turso to the public projection enum and dispatch it through the common
  facade/server composition with every log; do not introduce a combined alias.
- Await create, replay, apply, read, recovery, and shutdown through the same
  public interfaces as the other projections.
- Under `Strict`, acknowledge only after the selected log's commit barrier and
  Turso apply. Under object-log `AsyncProjection`, use the provider-neutral
  ordered apply, poison, debt, and backpressure contract.
- **Files**: public facade and server configuration, common composition, adapter
  conformance, and focused Turso tests.

### Modified: blocking reference adapters

- Memory moves directly to async axes.
- SQLite, blocking Postgres, and filesystem/object-log work use explicit whole-transaction blocking
  wrappers below the async port.
- Existing composition-root `BlockingBackend` shims are removed after call-site parity.

## API/Interface Design

| Surface | Governing Contract | Story-Level Usage |
|---------|--------------------|-------------------|
| Queue operations and transaction outcomes | API-001 | No client-visible semantic change. |
| Projection selection | TD-001 / ADR-012 | `turso` is the canonical default projection; no combined Turso profile or alias exists. |
| Storage traits | ADR-015 / TD-001 | Runtime-neutral `Send` futures; no Tokio type crosses into domain ports. |

The public server configuration accepts
`FIREWEED_PROJECTION_BACKEND=turso` and selects it when that key is omitted.
`FIREWEED_TURSO_PROJECTION_PATH` is the public local database-path setting. A
feature-disabled build returns a typed availability error before I/O and never
maps Turso to SQLite or memory.

## Data Model Changes

No logical schema change is intended. Turso executes the exact relational projection schema extracted
from the SQLite reference. The adapter-specific migration table records schema and adapter version. An
upgrade refuses a newer/unknown schema until the compatibility probe and migrations have passed.

## Integration Points

| From | To | Method | Data |
|------|----|--------|------|
| `AsyncComposedBackend` / segmented backend | `SeparateReplayCommit` + async storage axes | owned dispatched task | typed commands, positions, expected epoch, replay outcome |
| `fireweed-turso` | Turso 0.7 local database | async driver | relational schema, transactions, queries |
| object log | Turso projection | ordered replay/apply | sealed committed batches |
| public typed/service config | feature-gated composition | validated construction | adapter path and common storage settings |

### External Dependencies

- `turso = "=0.7.2"`, `default-features = false`.
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
- Retained-response memory is bounded separately from page cache. Normal
  serving admits at most eight Claim drivers and twenty-four mutation
  generations with a combined 128 MiB retained-response ceiling; no queued
  generation clones payloads. After the shared reader is retired, the configured
  writer plus driver/outcome pools have a 224 MiB page-cache ceiling. These are
  structural ceilings, not substitutes for the behavioral M1/M2/M3 gates.
- Turso must preserve exact operation outcomes, monotonic progress, structural
  query bounds, and declared resource ceilings under public qualification;
  throughput and latency are compared with interleaved same-run SQLite controls
  and reported for every log composition with explicit workload and host bounds.
- CI retains one focused/path-filtered adapter job for the s3 × turso cell
  (ADR-024). It does not require a multi-cell matrix.

## Testing

- [ ] **Schema/config**: exact shared schema, partial indexes, individual PRAGMA trap/readbacks.
- [ ] **Atomicity**: lifecycle/lease/index/replay outcome plus cursor in one transaction; injected rollback.
- [ ] **Conformance**: every `QueueCommand` arm and projection-read output matches the expected public state before and
  after reopen. A SQLite twin is not the gate (ADR-016, ADR-024).
- [ ] **Recovery**: overlapping replay no-op, gap rejection, snapshot-tail counter restore, reset/rebuild,
  manifest-sealed-before-apply crash.
- [ ] **Cancellation**: before append, after staging, during commit, after durable eventual append, and
  cancelled lock waiter.
- [ ] **Dispatch/strategy**: atomic profiles cannot construct with separate append/apply; object-log uses
  `SeparateReplayCommit`; submitted tasks survive caller drop; a stalled queue does not stall another.
- [ ] **Runtime**: blocking-reference wrappers and Turso both keep a single-thread heartbeat alive.
- [ ] **Configuration**: feature-enabled builds select public `turso` explicitly
  and by default; feature-disabled builds reject it before I/O without fallback.
  Both paths cover create/push/claim/finalize/renew/reassign/read/reopen through
  public composition.
- [ ] **Concurrency**: 16 disjoint writers and deterministic same-active-key conflict.
- [ ] **Security**: tenant isolation and corrupted-local-projection fail-closed behavior.

## Migration & Rollback

- **Backward compatibility**: client operations are unchanged; `turso` is a
  projection-axis value, not a new profile or alias. Omitted projection settings
  now resolve to Turso.
- **Data migration**: none from SQLite is required. A Turso projection rebuilds from the object log into a
  fresh file and becomes eligible only after frontier/image verification.
- **Feature toggle**: a compile feature controls adapter availability. Default
  distributions enable it; minimal custom builds may omit it and fail closed.
- **Migration to default**: changing an omitted/default selection from another
  projection to Turso creates or opens the configured Turso file. Class A cells
  rebuild from the authoritative log before serving. Class B operators who need
  existing projection state must keep their prior projection explicit or
  perform an export/import outside this design; Fireweed does not pretend a
  memory log can rebuild lost history.
- **Rollback**: set the prior projection explicitly and, for Class A, rebuild it
  from the authoritative log. Preserve the Turso file until verification passes.
  Do not disable the feature while any deployment still relies on the default.

## Implementation Sequence

1. Add typed async commit and cancellation conformance; remove raw closure call sites.
2. Add shared-receiver async storage axes, typed commit strategies, an owned-task dispatcher, and migrate
   reference composition/memory.
3. Wrap blocking SQLite/object-log/Postgres transactions and remove composition-root blocking shims.
4. Extract the driver-neutral relational substrate with SQLite parity.
5. Implement and differentially test `fireweed-turso`.
6. Wire Turso to the S3 object log and `AsyncProjection` (ADR-024); verify
   feature-disabled fail-closed behavior.
7. Add focused adapter CI for that cell. Do not treat a multi-cell matrix as
   the qualification record, and do not claim a 10M or 1000-queue pass.
8. Remove the remaining legacy synchronous axes after repository-wide conformance passes.

**Prerequisites**: ADR-015 and ADR-016 accepted; exact Turso 0.7 probe preserved;
ADR-013 response and rebuild rules unchanged.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Async migration becomes a flag-day rewrite | M | H | Additive traits, explicit wrappers, dependency-ordered beads, removal last. |
| Relational extraction changes SQLite behavior | M | H | Land extraction with SQLite-only parity before Turso code. |
| Full command corpus exposes unsupported Turso behavior | M | H | Differential or common-conformance failure blocks default/public qualification and release. |
| Cancellation leaves waiter or transaction stranded | M | H | Owned commit and lock-wait cancellation tests. |
| New CI work is over-scaled | M | M | Cache the focused adapter job for the one public cell (ADR-024). |
