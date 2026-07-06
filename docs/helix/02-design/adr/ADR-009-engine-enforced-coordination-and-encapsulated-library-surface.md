---
ddx:
  id: adr-engine-enforced-coordination-and-encapsulated-library-surface
  depends_on:
    - adr-embedded-engine-integration-and-public-surface
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-queue-as-shard-unit-and-projection-families
    - adr-auth-tenancy-and-storage-isolation
    - td-sharding-and-shard-ownership
  status: accepted
  review:
    self_hash: 36c73add90f1c464172040dd7c926608f49c5a263b2bf03d9dd03103d8a5b6c2
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-embedded-engine-integration-and-public-surface: e18689f92ad1070a9d3e96253f41b6d0a3fe67eb9b6eb80f5df07ac24e56c7cc
      adr-hexagonal-architecture-and-two-interfaces: 02e04b32110f57e05ea80a7b6ce642cba655866e14302db6a8b0d1de0f62d012
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
    reviewed_at: "2026-07-06T00:56:00Z"
---

# Architecture Decision Record

**ADR ID**: ADR-009
**Title**: The engine enforces coordination below the ports; the library is the only encapsulated external surface
**Status**: accepted (status updated 2026-07-05 — this ADR is cited as committed by the accepted
ADR-011 and is **partially realized**: the library facade, backend constructors, and ownership/session
code exist, but the Decision 2 publish topology is not yet enforced — no crate manifest sets
`publish = false` and no guard test asserts it — and the Decision 4/5 epoch-collapse and
fence-threading items remain sequenced build work. All are tracked as beads, not open design)
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

5. **Multi-instance shared-store competition requires a single atomic acquire→fence epoch — and the
   object log is the committed way to provide it per queue.** It is correct only on a backend that
   presents a single atomic acquire→fence epoch. **No backend provides this today.** memory (per-process
   control plane) and sqlite (no shared control plane) are single-process. Postgres becomes safe **once**
   the single durable epoch (Decision 4 prerequisite) and the data-plane fence threading land. For the
   object log, the design intent (product-owner decision, 2026-07-05) is that **the object log itself
   provides multi-node fencing and coordination at the per-queue level**: the manifest conditional-PUT
   series is already both the CAS and the epoch fence for appends (TD-004), and extending it to an atomic
   per-queue acquire→fence (epoch-fence manifest entry published before any data segment, per TD-003's
   Single Authoritative Fencing Rule) is sequenced build work, not an open question. Until that lands the
   object log remains single-owner, and the library **runtime-refuses** multi-owner construction on any
   backend that does not present the atomic acquire→fence capability.

6. **Authorization stays out of the library, bounded by ADR-002.** The library performs **no authn** and
   **no authz itself**: the embedding host owns its trust boundary. Per ADR-002 deny-by-default, a
   **multi-tenant** host MUST authorize (tenant, queue, op) before calling the library; the library neither
   relaxes nor substitutes for that. Trusted single-tenant embedders may pass a fixed principal (ADR-002
   embedded mode). RESP adds network authentication (`HELLO`/ACL) and the wire codec — and **only** those —
   on top of the same engine coordination.

7. **Structured `ItemId`, minted locally by the owning node** (recorded here for traceability — this
   decision previously lived only in the library-canonical-coordination design and the code). An
   `ItemId` packs `[epoch:24][node:8][counter:32]`: the owner-tenure epoch, a **configured** `node_id`
   (sourced by the deployment, e.g. Helm), and a per-tenure monotonic counter
   (`crates/pqueue-core/src/domain.rs` ItemId packing; counters in `QueueCounters`). Ids are minted
   locally by the queue's single owner with no coordination on the mint path — uniqueness follows from
   per-queue single-writer ownership (ADR-008) plus the epoch component across tenures. Ids are stable,
   orderable within a tenure, and never reused across epochs.

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
- **Honest scope:** multi-instance shared-store competition does not work today and is not claimed to;
  Postgres is gated on the single-durable-epoch + fence-threading work, and object-log multi-owner is out
  until the manifest-CAS acquire→fence lands — but the latter is committed direction (the object log is
  the intended per-queue multi-node fencing/coordination substrate), not a deferred maybe.
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
