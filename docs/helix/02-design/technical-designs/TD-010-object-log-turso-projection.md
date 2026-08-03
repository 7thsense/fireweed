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
  status: superseded
  review:
    self_hash: 1e3623771c800e9d2c6874c19e94103d00c165f1afdd27ece4760fb43f6f7f69
    deps:
      adr-async-commit-strategy-and-dispatch: 61bf761b8f8b84581b174eb8f1c64a8893ede0dce9353707fb284f751fb82b5e
      adr-full-async-storage-boundaries: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
      adr-log-single-source-of-truth: 35052eb1b94371aa8abb8e8b348a21b459522c7d5feaba04b7146745a04bda62
      adr-turso-derived-projection: 76ec5fe8523c4fe831441229aa5f09f0bf966ac3849174764a7ba2c2d805f22a
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T00:01:28Z"
---

# Technical Design: TD-010 Internal Object-log + Turso Compatibility Projection

**Contract**: API-001 | **ADR**: ADR-012, ADR-015, ADR-016, ADR-017 | **Scope**: internal native-async compatibility projection

## Disposition

This design is superseded as a public server profile. It remains implementation guidance for the
internal, experimental `fireweed-turso` adapter and its focused compatibility tests only.

- Public log selectors are exactly `memory`, `sqlite`, `postgres`, `filesystem`, and `s3`.
- Public projection selectors are exactly `memory`, `sqlite`, and `postgres`.
- No `objectlog`, `inmemory`, `turso`, `hybrid`, or combined-profile alias is supported.
- Public selection of `turso` must return a configuration error in both feature-disabled and
  feature-enabled builds. Feature enablement exposes internal tests, not a public positive path.
- The internal adapter consumes the provider-neutral `EngineError` and `CommitRejection` vocabulary; it
  defines no Turso-specific public error, capability, or RESP token.
- While legacy programmatic projection variants remain, enabling change-record delivery on a Turso
  composition fails startup as retired legacy configuration; it does not create a public Turso history
  surface or reuse the Class B durability error.
- Historical `object-log + Turso` and `objectlog/turso` wording below describes design lineage only where
  it is not explicitly replaced by this disposition.

## Scope

This design retains Turso Database as a local, rebuildable relational projection used by internal
compatibility tests with the segmented object log. It defines the experimental adapter, shared
relational substrate, recovery, failure behavior, and validation gates without adding a public profile.

In scope:

- async storage-axis migration required to call a native-async projection without blocking;
- a driver-neutral relational schema/codec/query substrate shared by SQLite and Turso;
- `fireweed-turso`, pinned to Turso 0.7.0 in ordinary local WAL mode;
- feature-gated internal object-log/Turso composition;
- full relational differential, cancellation, replay, and reopen conformance.

Out of scope:

- Turso as command-log authority, control plane, remote database, or embedded replica;
- replacing the standalone SQLite durable profile;
- experimental Turso MVCC, sync, FTS, or remote/cloud features;
- Niflheim changes and deployment-capacity benchmarks;
- a new broad GitHub Actions matrix dimension.
- any public Turso selector, alias, server profile, or support commitment.

## Technical Approach

**Strategy**: complete the async storage boundary from ADR-015, extract driver-neutral relational facts,
then implement Turso directly against the async projection axis. Compose it only behind the object log,
whose manifest remains the acknowledged-command authority.

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
- Public server selection does not exist. The experimental feature gates adapter code and focused tests;
  public parsing rejects `turso` with the same typed configuration error in all builds.

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

### Internal: segmented object-log compatibility composition

- Exercise the segmented object log over the async derived-projection contract in internal tests without
  preserving or introducing a combined public alias.
- Add a feature-gated Turso test composition that awaits create, replay, apply, read, recovery, and
  shutdown without entering server configuration.
- Ack only after object-log manifest commit and the internal composition's Turso response barrier. A lost
  response is resolved from the durable log/request outcome.
- **Files**: internal composition and focused adapter tests; public server configuration is unchanged
  except for explicit rejection coverage.

### Modified: blocking reference adapters

- Memory moves directly to async axes.
- SQLite, blocking Postgres, and filesystem/object-log work use explicit whole-transaction blocking
  wrappers below the async port.
- Existing composition-root `BlockingBackend` shims are removed after call-site parity.

## API/Interface Design

| Surface | Governing Contract | Story-Level Usage |
|---------|--------------------|-------------------|
| Queue operations and transaction outcomes | API-001 | No client-visible semantic change. |
| Backend profile selection | TD-001 / ADR-012 | No Turso profile; `turso` fails public configuration explicitly in every build. |
| Storage traits | ADR-015 / TD-001 | Runtime-neutral `Send` futures; no Tokio type crosses into domain ports. |

The public server configuration must reject `FIREWEED_PROJECTION_BACKEND=turso`; it must not silently
map that value to another projection or condition acceptance on a compile feature.
`FIREWEED_TURSO_PROJECTION_PATH`, if retained at all, is internal test configuration and is not a public
operator contract.

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
| internal test config | feature-gated composition | validated construction | adapter path and test fixture |

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
- Turso must preserve exact operation outcomes, monotonic progress, structural
  query bounds, and declared resource ceilings under internal validation;
  throughput and latency are compared with interleaved same-run SQLite controls
  and reported as experimental evidence. No production status or public performance claim follows.
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
- [ ] **Configuration**: feature-disabled and feature-enabled builds both reject the public `turso`
  selector; internal tests cover create/push/claim/finalize/renew/reassign/read/reopen directly.
- [ ] **Concurrency**: 16 disjoint writers and deterministic same-active-key conflict.
- [ ] **Security**: tenant isolation and corrupted-local-projection fail-closed behavior.

## Migration & Rollback

- **Backward compatibility**: client operations are unchanged; there is no new public profile or alias.
- **Data migration**: none from SQLite is required. A Turso projection rebuilds from the object log into a
  fresh file and becomes eligible only after frontier/image verification.
- **Feature toggle**: compile feature for the internal adapter and validation lane only.
- **Rollback**: disable the experimental feature or discard its local Turso test state; supported public
  configurations are unaffected.

## Implementation Sequence

1. Add typed async commit and cancellation conformance; remove raw closure call sites.
2. Add shared-receiver async storage axes, typed commit strategies, an owned-task dispatcher, and migrate
   reference composition/memory.
3. Wrap blocking SQLite/object-log/Postgres transactions and remove composition-root blocking shims.
4. Extract the driver-neutral relational substrate with SQLite parity.
5. Implement and differentially test `fireweed-turso`.
6. Wire feature-gated internal object-log + Turso validation and verify public selector rejection in
   both feature modes.
7. Remove the remaining legacy synchronous axes after repository-wide conformance passes.

**Prerequisites**: ADR-015 accepted; ADR-016 retained as superseded experimental evidence; exact Turso
0.7 probe preserved; ADR-013 response and rebuild rules unchanged.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Async migration becomes a flag-day rewrite | M | H | Additive traits, explicit wrappers, dependency-ordered beads, removal last. |
| Relational extraction changes SQLite behavior | M | H | Land extraction with SQLite-only parity before Turso code. |
| Full command corpus exposes unsupported Turso behavior | M | H | Differential failure blocks the internal validation lane; public profiles are unaffected. |
| Cancellation leaves waiter or transaction stranded | M | H | Owned commit and lock-wait cancellation tests. |
| New CI work is over-scaled | M | M | One focused job; no broad matrix expansion. |
