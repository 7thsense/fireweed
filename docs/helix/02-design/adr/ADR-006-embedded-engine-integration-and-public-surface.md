---
ddx:
  id: adr-embedded-engine-integration-and-public-surface
  depends_on:
    - prd
    - api-native-client-interface
    - adr-rust-workspace-and-toolchain-policy
    - td-storage-architecture-backend-contracts
  review:
    self_hash: e06dc6a96cdcd7293b5ba67e9c17d387cd2bd51c14daef13287bdf62a9e3951e
    deps:
      adr-rust-workspace-and-toolchain-policy: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
      api-native-client-interface: 852a753af558d8b8a21e4a86e87915b14c030fefcb4a27473bcbb08cfe044580
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
      td-storage-architecture-backend-contracts: f77d88cfdd2f4ad3c23d7f0310c5164eaecc57742f469cdc062accda44484a54
    reviewed_at: "2026-07-18T02:26:58Z"
---

# ADR-006: Embedded Engine Integration and Public Crate Surface

## Status

Accepted, with two later amendments: the **client mode** half (API-001 over HTTP or the
`pqueue-client` SDK) was superseded by ADR-007's clean cutover — those crates are deleted; the client
faces are now RESP (TD-006) and the `pqueue` library. The **embedded mode** half stands, refined by
ADR-009: the embedding surface is the published `pqueue` facade crate (structurally enforced), not
direct `pqueue-core`/`pqueue-storage` trait access; ports are `#[doc(hidden)]` behind a documented
`internal` escape hatch.

ADR-016 adds Turso as a feature-gated, object-log-derived projection. It does not replace the standalone
SQLite durable backend or make a Turso projection authoritative; the ADR-006 durability requirement still
binds every embedded production profile.

## Context

A host application (the first being the 7snx managed-delivery engine) integrates
pqueue **as an embedded Rust library**: it path-depends on `pqueue-core` and
`pqueue-storage`, binds the storage traits (`LogStore`, `ProjectionStore`,
`ControlPlaneStore`), the command types (`pqueue_storage::commands`), and a
backend (today the in-memory one), and drives the engine loop itself
(`append_batch` → `apply_committed` → `batch_claim`).

This integration mode was not described or sanctioned by any prior contract, and
two prior decisions are in tension with it:

- API-001 names an embedded-library binding but only at the level of the native
  *operations*, and explicitly places storage adapter traits out of scope.
- TD-001 declares the storage trait surface **internal**, with API-001 as the
  sole external compatibility boundary.

So the surface a real embedder now depends on (`pqueue-core` +
`pqueue-storage::{traits, commands, memory, types}`) is de-facto public but
carries no stability guarantee, and an embedder can — as 7snx currently does —
run the **non-durable in-memory backend** in a production path, silently
forfeiting the TD-001 durable-ack guarantee (work is lost on restart).

This ADR resolves the tension by recognizing embedding as a first-class
integration mode with explicit guardrails, rather than leaving it undescribed.

## Decision

**1. There are two sanctioned integration modes.**

- **Client mode** — consume API-001 over HTTP or the `pqueue-client` SDK facade.
  API-001 remains the external compatibility boundary for this mode.
- **Embedded mode** — link `pqueue-core` + `pqueue-storage` and drive the
  storage traits in-process. API-003's "Embedded Engine Integration Profile"
  defines how; this ADR defines the surface and its stability.

**2. The embeddable surface is public and versioned.** The following become a
declared public, SemVer-stable surface (starting at the current `0.x`, with
breaking changes only on a minor bump pre-1.0 and a major bump post-1.0, plus a
one-minor deprecation window where feasible):

- `pqueue-core`: the domain types (ids, `QueueDefinition`, priority/lifecycle).
- `pqueue-storage`: `traits` (`LogStore`/`ProjectionStore`/`ControlPlaneStore`/
  `SnapshotStore`), `commands` (`CommandEnvelope`/`QueueCommand`/finalize kinds),
  and `types` (`QueueKey`/`CommandPosition`/`CommandChecksum`).

This supersedes TD-001's "storage contracts are internal" statement **for these
modules**: they are now an embedding contract, not just a backend-author one.
Modules not listed here remain internal.

**3. The in-memory backend is dev/test only and MUST NOT back production state.**
`pqueue_storage::memory` is the conformance/reference backend; it has no durable
ack boundary and loses all state on restart. An embedder running it in
production violates the TD-001 durable-ack guarantee. Production embedders MUST
use a durable backend.

**4. There are three durable backends.** A production deployment — embedded or
client — MUST use one of:

- `postgres_native` (incl. the managed Lakebase variant) — server Postgres.
- `object_log_sqlite_projection` — S3-compatible object-log authority + SQLite
  projection + Postgres control plane.
- `sqlite` — a standalone single-file durable backend (SQLite as the durable
  command-log + projection authority, no object-log/S3), for embedded hosts that
  want durability without a server or object store. Specified by its own TD; the
  durable boundary is WAL + fsync. (Engine: `rusqlite`/bundled SQLite for v1;
  evaluating a pure-Rust engine is tracked separately and not a blocker.)

**5. Embedded adapters MUST pass an embedder conformance suite.** pqueue
publishes an embedder-facing "delivery adapter conformance" suite (distinct from
the backend-author conformance of TD-001) asserting push/claim/finalize,
duplicate convergence, retry/expired-lease re-pending, and terminal-failure
semantics through the embedded surface. A host's adapter (e.g. 7snx's
`assert_delivery_queue_adapter_conformance`) maps to this suite.

## Alternatives Considered

### Keep the surface internal; require all consumers to use pqueue-client/API-001
Cleanest boundary, but contradicts a shipped reality: 7snx already embeds the
traits, and the client facade does not yet expose the in-process performance and
control an embedded host wants. Rejected as denial of the actual integration.

### Bless embedding but allow the memory backend in production
Lowest friction, but it ships a data-loss footgun: the memory backend has no
durable boundary. Rejected — embedding without a durability requirement is not a
safe contract.

### Replace SQLite with libsql or a pure-Rust engine for the embedded backend
This was out of scope for ADR-006 and remains so for the standalone `sqlite` authority, which uses
`rusqlite`. ADR-016 later selects Turso only as an additional rebuildable projection paired with the
durable object log; it does not revise this backend's authority or durability boundary.

## Consequences

- The embedding mode is documented and safe-by-contract: a durable backend is
  required, and the surface an embedder depends on is versioned.
- TD-001 and ADR-003 are amended: the listed `pqueue-core`/`pqueue-storage`
  modules are a public embedding contract; API-001 remains the boundary for
  client mode only.
- A new durable `sqlite` backend is required to give embedded hosts a
  server-free durable option; until it lands, embedded production hosts use
  `postgres_native` or `object_log_sqlite_projection`.
- 7snx must move its host runtime off the in-memory backend onto a durable
  backend; its adapter conformance test maps to the published embedder suite.
- pqueue takes on a SemVer obligation for the embedding surface; refactors of the
  storage traits/commands now require a deprecation path.
