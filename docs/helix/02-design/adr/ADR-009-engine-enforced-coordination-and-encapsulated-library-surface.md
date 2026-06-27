---
ddx:
  id: adr-engine-enforced-coordination-and-encapsulated-library-surface
  depends_on:
    - adr-embedded-engine-integration-and-public-surface
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-queue-as-shard-unit-and-projection-families
    - adr-auth-tenancy-and-storage-isolation
    - td-sharding-and-shard-ownership
  status: draft
  review:
    self_hash: f5795719c029efc047debaac97e0bfc86274b6f0c70b0b23c3df8c86bf519b68
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-embedded-engine-integration-and-public-surface: 6266b5ddd069b0a421dfba44333be9102c0fed225b8cd4e845637eb1d8f6309b
      adr-hexagonal-architecture-and-two-interfaces: 03851e92193304e7fddd7fe73abad5ef0ef20bb87b4316e1dcbfa42e5495cdc9
      adr-queue-as-shard-unit-and-projection-families: 77d1e2feb6a27e0a093564e3f07247cd8cc2c6fba6c3d20b5eeade568ba25964
      td-sharding-and-shard-ownership: 6bf3dcc75c94fefa35af4ed9f1859e76b76df3f171a89622fcb24888d92c93e4
    reviewed_at: "2026-06-27T19:02:57Z"
---

# Architecture Decision Record

**ADR ID**: ADR-009
**Title**: The engine enforces coordination below the ports; the library is the only encapsulated external surface
**Status**: draft
**Related**: ADR-002 (auth & deny-by-default), ADR-006 (embedded surface), ADR-007 (hexagonal & two
interfaces — this ADR *refines*, does not supersede), ADR-008 (queue as shard unit & pluggable control
plane), TD-003 (sharding & ownership), TD-006 (RESP `route`), TD-007 (durability & durable state),
`docs/helix/04-build/library-canonical-coordination-design.md` (the converged design this records)

## Context

ADR-007 ships two driving interfaces over one engine — the in-process Rust library (`pqueue` crate,
`Pqueue<B>`) and the RESP wire front (`pqueue-resp`) — as **co-equal driving adapters over the engine
ports**. ADR-008 makes the **queue** the unit of single-writer ownership with a control-plane lease and
an epoch fence (the Single Authoritative Fencing Rule). Reviewing the realized code against those
decisions surfaced three gaps that are individually small and collectively let an embedder silently
violate the ownership model:

1. **The library does not coordinate.** `Pqueue<B>` (`crates/pqueue/src/lib.rs`) holds
   `{ backend, clock, ids }`, has **no owner identity**, and **never** calls `resolve_queue_owner` /
   `acquire_and_fence`. Every method delegates straight to `backend.push/claim/finalize`. Its bound
   (`LibBackend`) already *requires* `ControlPlaneStore` — the capability is present and unused. Only the
   RESP `route` decision (`pqueue-resp/src/routing.rs`) assembles `authorize → resolve → serve/redirect`.

2. **The data plane never self-fences.** The real `PushPort`/`ClaimPort`/`FinalizePort` write paths
   **self-stamp the queue's current epoch** as `expected_epoch` (`ownership.rs:14-21`, `port.rs:33-34`,
   sqlite `append_durable`), so a superseded owner's claim is **not** rejected. Only the raw
   `LogWriter::append` seam checks a caller-supplied epoch (BQ-20), and no driving adapter routes through
   it. Worse, the control-plane lease epoch and the storage append-fence epoch are **two separate durable
   counters even on Postgres** (`postgres/src/control_plane.rs:16-19`, `port.rs:480-484`) — there is no
   atomic acquire→fence on any backend today.

3. **The ports are an open external surface.** `pub fn Pqueue::new(Arc<B>, clock)` hands `B` *in*, leaving
   a client holding an `Arc<B>: ClaimPort` it can call directly; no workspace crate sets `publish`, so a
   client can `cargo add pqueue-engine` and use `ClaimPort` with no friction. "Use the library" is a
   convention, not a constraint — the same reach-around hazard ADR-007 sought to remove, reintroduced at
   the client boundary.

The unifying question is **where** the ownership/fence model is enforced and **how** an external consumer
is constrained to it. This ADR decides both. It does not re-decide the ownership model (ADR-008/TD-003);
it locates its enforcement and seals the surface.

## Decision

1. **Coordination is an engine responsibility, enforced below the ports.** Ownership resolution and epoch
   fencing are *coordination* (a property of >1 instance sharing a queue), not *security*, and are
   independent of transport. They live in `pqueue-engine` (consistent with ADR-007: "the engine owns …
   fencing … validation") and are **invoked identically by every driving adapter**. Because the fence is
   enforced *below* the ports, neither adapter can skip it. This **refines ADR-007** (it names the engine,
   not either adapter, as the coordination locus) and does **not** make `pqueue-resp` depend on the
   `pqueue` crate; both adapters consume the same engine session.

2. **The published library is the only external surface to the engine.** A downstream crate reaches the
   engine **only** through `Pqueue`. The raw ports and concrete backends are not a usable external
   construction-or-call surface. Mechanisms (all required): (a) backend construction is via static,
   feature-gated, per-backend constructors — `Pqueue::open_postgres(cfg) -> Pqueue<impl LibBackend>` —
   that build the backend internally and return an **opaque** handle; (b) `Pqueue::new(Arc<B>, …)` becomes
   crate-private; (c) `pqueue` is the **only** `publish = true` crate — `pqueue-engine`, `pqueue-projection`,
   `pqueue-conformance`, and the adapter crates are `publish = false`; (d) port traits are `#[doc(hidden)]`
   with a doc-hidden marker supertrait (hygiene — true sealing is barred by the dependency-direction guard,
   and the load-bearing wall is (c)); (e) split guard tests assert the publish topology (manifest scan) and
   the absence of any public path to a port-bearing handle (`cargo-public-api`/`trybuild`). A single,
   feature-gated, loudly-documented `internal` escape hatch is permitted for custom adapters/tests:
   strong-by-default, not absolute, never `unsafe`. No `dyn`-erased backend (the ports are not object-safe);
   a single runtime `open(cfg)` switch is therefore out of scope.

3. **The library coordinates per op, with target-affinity.** `Pqueue` holds an `OwnerId` and a
   control-plane handle; per queue-addressed op it resolves ownership and operates under a cached
   `OwnedSession{lease_epoch, fence_epoch}`. Acquisition is restricted to the rendezvous `target_owner`
   (the engine's `acquire` stays cooperative; the **library policy layer** imposes target-affinity to
   prevent the epoch ping-pong livelock a free-contention default causes). A queue held by a different live
   owner yields an `OwnedElsewhere{owner, epoch}` **value** (the engine resolution that RESP renders as
   `-MOVED`); the library **never** contends by acquiring a live lease — online handoff is `begin_drain`
   (TD-003), not a contended acquire. A draining owner serves in-flight ops and refuses a new claim. A
   single, **bounded, per-node** renew/heartbeat driver (never one task per queue — ADR-002 density rule)
   keeps leases live; after a renew/acquire timeout the library **re-resolves**, it does not blindly retry
   the non-idempotent `acquire`.

4. **The fence is sourced from the cached session and checked at commit (and requires a single durable
   epoch).** The owner's cached `OwnedSession.fence_epoch` (captured at acquire, **never** re-read from
   `current_epoch` — re-reading makes the check a no-op) is threaded through the data-plane ports into the
   backend's atomic unit of work and checked **at commit time** (closing the resolve→commit TOCTOU). This
   makes lease liveness **fail closed on the data path**: a host stall that lets the lease expire and a
   peer reclaim it ⇒ the stalled instance's next append is `EpochFenced`, independent of its renew loop.
   This is only meaningful when the lease epoch and the storage append-fence epoch are **one durable
   value advanced atomically at acquire**; collapsing today's two counters is a prerequisite. This
   **reverses** the `port.rs:33-34` "in-process owners pass current epoch, never self-fence" contract.

5. **Multi-instance shared-store competition is a Postgres-only, conditional capability.** It is correct
   only on a backend that presents a single atomic acquire→fence epoch. **No backend provides this today.**
   memory (per-process control plane) and sqlite (no shared control plane) are single-process; object-log
   is `EventualApply` with a non-recoverable per-entry epoch and a non-atomic epoch object — **excluded**
   (single-owner only). Postgres becomes safe **once** the single durable epoch (Decision 4 prerequisite)
   and the data-plane fence threading land. The library **runtime-refuses** multi-owner construction on any
   backend that does not present the atomic acquire→fence capability.

6. **Authorization stays out of the library, bounded by ADR-002.** The library performs **no authn** and
   **no authz itself**: the embedding host owns its trust boundary. Per ADR-002 deny-by-default, a
   **multi-tenant** host MUST authorize (tenant, queue, op) before calling the library; the library neither
   relaxes nor substitutes for that. Trusted single-tenant embedders may pass a fixed principal (ADR-002
   embedded mode). RESP adds network authentication (`HELLO`/ACL) and the wire codec — and **only** those —
   on top of the same engine coordination.

## Consequences

- **New work this licenses (sequenced):** (i) collapse the control-plane lease epoch and the storage
  append-fence epoch into one durable atomically-advanced value on Postgres (the Decision-4 prerequisite,
  bead BQ-23 scope); (ii) thread the cached `fence_epoch` through `PushPort`/`ClaimPort`/`FinalizePort`,
  checked at commit (Decision 4); (iii) give `Pqueue` an owner identity, session, resolve ladder, drain
  split, and bounded renew driver (Decision 3); (iv) encapsulate the published surface (Decision 2).
  (i)+(ii) also unblock the deferred RESP server acquire-runtime (`pqueue-c33c367e`), which MUST apply the
  same target-affinity (Decision 3) so the two adapters cannot thrash a queue.
- **Contract text to rewrite:** `port.rs:33-34` and the `append_durable` BQ-20 notes (they prescribe the
  always-current self-stamp that Decision 4 reverses). Existing single-owner `Pqueue` constructions and
  tests are preserved via a degenerate sole-owner session (constant ownership, constant always-current
  epoch) so the change is behavior-preserving for the single-instance case.
- **Refines ADR-007** (engine, not "the library," is the coordination locus; "the library is the contract"
  means the encapsulated external *surface*, not that RESP stacks on the library). **Builds on ADR-008/
  TD-003** (re-scoped per-queue ownership is reused as-is). **Bounded by ADR-002** (authz delegation is
  valid only in embedded/trusted mode; multi-tenant hosts still authorize). **Amends ADR-006** by making
  the embedded public surface *enforced* (publish topology + private constructor) rather than conventional.
- **Honest scope:** multi-instance shared-store competition does not work today and is not claimed to; it
  is Postgres-only and gated on the single-durable-epoch + fence-threading work. Object-log multi-owner is
  out until a manifest-CAS epoch fence exists.
- **Trade-offs accepted:** `pqueue` becomes a thin composition root that fans into feature-gated adapter
  crates (an explicit dependency inversion, kept minimal by `optional` deps); the encapsulation wall is
  strong-by-default but a determined embedder can still path-depend on an internal crate (acceptable — the
  goal is preventing *accidental* reach-around, with a deliberate escape hatch for advanced use).

## Alternatives considered

- **RESP consumes the `pqueue` library crate (the literal "network consumes the Rust interface").**
  Rejected: it would couple the wire protocol to the library's ergonomic API and make one driving adapter
  depend on another, contradicting ADR-007's co-equal-adapter structure (which would itself need
  superseding). Putting coordination in the engine below the ports achieves the same anti-reach-around goal
  *more* strongly (the fence is unskippable for both adapters) without the coupling.
- **Leave coordination per-adapter (status quo: RESP coordinates, library doesn't).** Rejected: it is
  exactly the gap this ADR closes — an embedder gets an unfenced, non-resolving god-mode API while the
  network front is fenced, with nothing forcing the two to reconcile.
- **Enforce encapsulation by `dyn`-erased backend behind one `Pqueue::open(cfg)`.** Rejected as infeasible:
  `Backend::write<R,F>` is a generic method and the async ports return RPITIT, so the ports are not
  object-safe; runtime backend selection would require an erased-shim port redesign, deferred.
- **Document the library-vs-ports boundary as convention only.** Rejected: convention is what failed (the
  ports are publishable today). The boundary must be structural (publish topology + private constructor +
  guard tests) or it will erode again.
