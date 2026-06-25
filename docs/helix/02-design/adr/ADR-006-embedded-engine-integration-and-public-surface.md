---
ddx:
  id: adr-embedded-engine-integration-and-public-surface
  depends_on:
    - prd
    - api-native-client-interface
    - adr-rust-workspace-and-toolchain-policy
    - td-storage-architecture-backend-contracts
  review:
    self_hash: 6266b5ddd069b0a421dfba44333be9102c0fed225b8cd4e845637eb1d8f6309b
    deps:
      adr-rust-workspace-and-toolchain-policy: ab726c0cca517786afa9301ab8e15e525c664dfbcd011a2cf736e22993e2ef27
      api-native-client-interface: a97e014a176aa9e37a93fbab151c31ffb47aa8428c62e802c98fa3be0413426b
      prd: a910dd5fb95102767b4ddf81115569d39d85c7e082a40c62ce424dea73ca8533
      td-storage-architecture-backend-contracts: a0053226d680acddfc3b606ec106c47ffb09167374940dc8282607e46b8df96e
    reviewed_at: "2026-06-25T04:21:18Z"
---

# ADR-006: Embedded Engine Integration and Public Crate Surface

## Status

Accepted

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
Out of scope here. libsql is also C and offers no benefit over the bundled
SQLite already in use; a pure-Rust engine (redb/limbo) is a separate evaluation.
v1 of the `sqlite` backend uses `rusqlite`.

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
