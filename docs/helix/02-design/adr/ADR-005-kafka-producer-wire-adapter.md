---
ddx:
  id: adr-kafka-producer-wire-adapter
  depends_on:
    - prd
  review:
    self_hash: b49b122239af43127faabd91747efc79cc3853555ffa9bfe4febb9d04f8bde32
    deps:
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
    reviewed_at: "2026-07-18T02:36:05Z"
---

# ADR-005: Kafka Producer Wire Adapter as P2 Compatibility Layer

## Status

**SUPERSEDED by [ADR-007](ADR-007-hexagonal-architecture-and-two-interfaces.md) (hexagonal migration,
Phase 6).** The Kafka producer wire adapter and its crate `pqueue-kafka` have been **DELETED**. The
clean-cutover architecture exposes exactly **two** interfaces over one CQRS engine — a RESP/Redis-Streams
wire front ([TD-006](../technical-designs/TD-006-resp-wire-adapter.md)) and a Rust library facade (the
`pqueue` crate) — and Kafka compatibility is not among them (ADR-007 §"Interface sprawl"). This ADR is
retained for historical context only; its decision is no longer in effect. See
[`hexagonal-migration-plan.md`](../../04-build/hexagonal-migration-plan.md).

**Scope note (2026-07-05)**: this ADR's "consumer-side Kafka APIs are permanently out of scope" verdict
applies to the **queue data plane** (Kafka committed-offset semantics do not compose with mutable
priority and progress bounds). It does not cover the **change log**: TD-008 requires a Kafka-protocol
consumer interface for the append-only, per-queue-ordered change-record stream, provided by fjord
embedded in pqueue-server (ADR-014, decided 2026-07-06). That surface has none of the data-plane
conflicts this ADR rejected.

_(Historical — as originally accepted 2026-06-16:)_ Accepted

## Context

pqueue's PRD (Non-Goals) states that pqueue will not implement AMQP, Kafka, or
SQS compatibility as the **core data model**. This correctly forecloses a design
where pqueue's items, priorities, and progress semantics would be shaped around
Kafka's partition/offset model.

However, IP-001 Slices 7-8 identify a concrete use case: pqueue should accept
inbound records from Kafka producers so that existing Kafka producer code can
enqueue work without modification. The heimq-wire + heimq-broker crates (IP-001)
provide a ready-made Kafka protocol engine. Building a thin adapter on top of
that engine requires only mapping Produce records onto pqueue's enqueue API —
the core data model remains pqueue-native.

## Decision

pqueue will implement a Kafka **producer** wire adapter as a P2 feature:

- **Wire protocol**: ApiVersions, Metadata, Produce requests handled by
  heimq-wire (no custom framing or handler registry).
- **Mapping**: each ProduceRequest record batch → one or more pqueue `enqueue`
  calls on the target queue identified by the Kafka topic name.
- **Consumer-side APIs are permanently out of scope**: Fetch, ListOffsets,
  OffsetFetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup will never be
  implemented. pqueue has its own native claim/fetch API; Kafka consumer
  semantics do not compose with mutable priority and progress bounds.
- **Scope boundary**: the adapter translates the wire representation only.
  pqueue's priority, scheduling, group selection, and idempotency rules apply
  after the enqueue call — the adapter cannot override them.

## Alternatives Considered

### Implement full Kafka compatibility (produce + consume)

Rejected. Consumer-side Kafka APIs (Fetch, consumer groups, offset commits)
conflict with pqueue's mutable priority and progress-bound model. A pqueue item
may be re-prioritized, delayed, or re-assigned after a claim, none of which maps
cleanly to committed-offset semantics. Implementing full compatibility would
require either lying to consumers or breaking pqueue invariants.

### Build a standalone adapter process

Rejected for P2 scope. An in-process adapter over heimq-wire is simpler, avoids
a network hop, and is consistent with how fjord wires its backends. A standalone
process remains an option for a future P3 deployment pattern.

### Use a Kafka adapter at the infrastructure level (e.g., Kafka Connect sink)

Viable as a deployment option but outside pqueue's scope. Does not enable
pqueue-native priority routing, group selection, or idempotency keys from
producer metadata.

## Consequences

- The PRD Non-Goals section is updated to clarify: core data model remains
  pqueue-native; the Kafka producer wire adapter is P2 (this ADR).
- A new bead will track the producer front-end implementation, gated behind
  the pqueue enqueue API being in place.
- Dependent: heimq-fef9406f (IP-001 Slices 7-8) adopts this adapter as the
  pqueue Kafka produce path conformance target.
