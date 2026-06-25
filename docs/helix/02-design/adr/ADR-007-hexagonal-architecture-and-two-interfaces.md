---
ddx:
  id: adr-hexagonal-architecture-and-two-interfaces
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-embedded-engine-integration-and-public-surface
    - api-native-client-interface
    - api-operator-repair-contract
  status: draft
  review:
    self_hash: 03851e92193304e7fddd7fe73abad5ef0ef20bb87b4316e1dcbfa42e5495cdc9
    deps:
      adr-cqrs-log-projection-storage-model: 9a9570ebe2718bf637c73564018e3702bc4473bcbf5a6499b52b7e1937bd0b83
      adr-embedded-engine-integration-and-public-surface: 6266b5ddd069b0a421dfba44333be9102c0fed225b8cd4e845637eb1d8f6309b
      api-native-client-interface: a97e014a176aa9e37a93fbab151c31ffb47aa8428c62e802c98fa3be0413426b
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
    reviewed_at: "2026-06-25T04:21:18Z"
---

# Architecture Decision Record

**ADR ID**: ADR-007
**Title**: Hexagonal architecture and two interfaces (RESP + Rust library)
**Status**: draft (the "one shared projection" consequence superseded by ADR-008)
**Related**: ADR-001 (CQRS log/projection), ADR-006 (embedded surface), ADR-008 (queue-as-shard-unit &
two projection families — supersedes the one-shared-projection consequence below), API-001, API-002,
TD-006 (RESP surface), TD-007 (durability), `docs/helix/04-build/hexagonal-migration-plan.md`

## Context

pqueue began with an HTTP/REST native interface (API-001 HTTP binding), an HTTP client crate, and a
Kafka producer wire adapter (ADR-005), over a CQRS log+projection storage layer. As the product
focus narrowed to "pqueue as an embeddable priority work queue," three problems surfaced:

1. **Interface sprawl.** Three partial driving surfaces (HTTP, Kafka, SDK) each re-expressed the
   native operations, none was authoritative, and domain logic (auth, idempotency, lease fencing,
   queue pause) had accreted inside the HTTP service crate as in-memory state.
2. **Storage seam mismatch.** The embedded SQLite backend bypassed the `ProjectionStore` trait
   rather than composing with it, producing a "fused vs split" special case and a double-attempts
   hazard on the claim path.
3. **No client ecosystem.** The bespoke HTTP/JSON surface required hand-written SDKs for adoption.

This is pre-launch software with no external compatibility obligations, so a clean re-architecture
is preferable to incremental patching.

## Decision

Adopt **hexagonal architecture (ports and adapters)** with **exactly two driving interfaces**:

1. **A RESP wire adapter** — a pqueue-native server speaking a **stock Redis Streams-compatible
   worker subset** (produce/claim/ack/reclaim), so off-the-shelf Redis clients drive the hot path
   with no custom commands. Limited but contract-faithful ("pqueue-flavored Redis"); see TD-006.
2. **A Rust library** — the full-power interface for everything the RESP subset intentionally omits
   (filtered claim, gates, cohorts, rich finalize, mutable priority, operator repair).

The library is, by design, strictly more capable than the RESP surface; this asymmetry is recorded,
not accidental. A CLI is a library consumer, not a third interface.

Structure:
- **Domain** (`pqueue-core` types + `pqueue-engine` execution/ports) depends on nothing outward and
  defines all ports. The engine owns the single *logical* claim path, the migrated domain logic
  (auth/idempotency/fencing/pause/validation), and the `ReclaimDriver`.
- **Driven adapters** (`pqueue-memory`, `-sqlite`, `-postgres`, `-objectlog`) implement the storage
  ports; classified by durability class (TD-007).
- **Driving adapters** (`pqueue-resp`, `pqueue` library) and the **composition root**
  (`pqueue-server`) are the only places that name concrete types.
- A dependency-direction test forbids any domain→adapter edge.

The migration is a **clean cutover**: `pqueue-service`, `pqueue-client`, and `pqueue-kafka` are
deleted; no stubs, legacy fallbacks, or compatibility shims survive (see the migration plan).

## Consequences

**Positive:**
- One authoritative operation model behind two well-separated interfaces; domain logic lives in the
  domain, not in a transport crate.
- Off-the-shelf Redis clients (and `redis-cli`) drive the worker hot path; no SDK to maintain for the
  common case.
- Storage backends are driven adapters classified by durability class. (**Superseded in part by
  ADR-008:** the "fused vs split disappears / one shared projection" framing is retracted — there are
  **two projection families**, an in-memory log-replay projection and a relational / DB-resident
  projection, held identical by the conformance suite, not a single shared projection. A backend is a
  durability class *and* a projection family.)
- Modularity is mechanically enforced (dependency-direction test, behavioral no-stub conformance).

**Negative / accepted costs:**
- The RESP surface is deliberately limited; advanced capability requires the library (recorded
  asymmetry, capability matrix in TD-006).
- "pqueue-flavored Redis" diverges from literal Redis in named, documented ways (priority delivery
  order, upsert `XADD`, lease-generation-fenced `XACK`); semantic contracts hold but bit-identical
  behavior does not (TD-006 §3).
- The queue is the unit of sharding (ADR-008): a queue is owned by one node and horizontal scale is cross-queue (per-queue ownership + routing, TD-003/TD-006). There is no intra-queue/multi-shard coordination to build; the ports admit per-queue ownership and cross-queue distribution.
- A net-new RESP server, `ReclaimDriver`, and `UpsertPort` must be built, and durable state migrated
  off in-memory `Mutex` storage (TD-007).

## Alternatives considered

- **Keep HTTP/REST + SDKs.** Rejected: no client ecosystem, domain logic stranded in the transport
  crate, three partial surfaces.
- **RESP-framed custom protocol (`PQ*` commands as the primary surface).** Rejected: forfeits stock
  Redis client compatibility — the main reason to speak RESP at all.
- **Faithful Redis only (FIFO, no priority/upsert).** Rejected: discards pqueue's core value
  (priority, enforced uniqueness). The "semantic-contract fidelity" middle path keeps both, with
  documented flavor differences, and is validated by an off-the-shelf-client e2e suite.
- **Incremental in-place refactor.** Rejected: pre-launch, a clean cutover avoids a permanent
  legacy-compatibility tail.
