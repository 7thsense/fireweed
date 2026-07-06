---
ddx:
  id: adr-heimq-external-broker-change-log-consumer-surface
  depends_on:
    - adr-kafka-producer-wire-adapter
    - adr-auth-tenancy-and-storage-isolation
    - adr-log-single-source-of-truth
  status: accepted
  review:
    self_hash: 68dd5e8df6d5187c7abb5a1fac0add02ee49fab38badd9d37dc02bc7af6b805f
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-kafka-producer-wire-adapter: 43a41c225d87f7bd4ecad12b49012fad53dc10ecc8d44595e569aaaeae3cdd3a
      adr-log-single-source-of-truth: 66130c84cb8e5467f5192066a0446f527672dac2eea83f7eae70b66c1e3b724c
    reviewed_at: "2026-07-06T01:51:45Z"
---

# Architecture Decision Record

**ADR ID**: ADR-014
**Title**: Heimq provides the change-log Kafka consumer surface as an external broker
**Status**: Accepted
**Related**: TD-008 (queue history / change records), ADR-005 (Kafka producer adapter,
scope note only), ADR-002 (tenant isolation), ADR-013 (log is the source of truth).

## Context

TD-008 requires a Kafka-protocol consumer interface for the append-only,
per-queue-ordered change log. The product requirement is about the change-log
surface only; it does not reopen the queue data-plane decision that consumer-side
Kafka APIs are out of scope for the core queue model (ADR-005).

The two admissible shapes are:

- pqueue-as-broker: `pqueue-server` embeds Kafka metadata/fetch/group handling
  and serves change topics directly from the committed log tail.
- external broker: `pqueue` emits change records into a Kafka broker deployment
  that owns consumer groups, offsets, and fan-out.

The external-broker shape is the lower-risk fit for this repository. pqueue
already owns the committed-log tail, durable emission cursor, and per-queue
ordering. It does not need to become a Kafka consumer-group implementation in
order to satisfy the change-log requirement.

## Decision

pqueue will use **heimq as an external broker** for the change-log Kafka
consumer surface.

### Provider + shape

- **Provider**: heimq.
- **Shape**: external broker.
- **Surface**: pqueue emits change records to heimq; heimq serves Kafka clients
  over its own consumer-group and offset machinery.

### Offset to `CommandPosition` mapping

- Each emitted change record carries the originating `CommandPosition` in its
  payload or headers, including `backend_epoch` and `sequence`.
- Each `(tenant_id, queue_id)` change stream is a single ordered topic/partition
  so the broker offset is monotonic for that stream.
- Kafka offsets are broker-assigned append positions; `CommandPosition` remains
  the product's durable source identity.
- On failover, pqueue may re-emit the same logical record from a later durable
  cursor position, but the broker offset only advances. Consumers dedupe by the
  stable record identity, not by offset alone.

### Consumer-group and offset ownership

- Heimq owns consumer groups, committed offsets, and fan-out.
- pqueue owns only the source emission cursor for the committed log tail.
- No pqueue component stores Kafka consumer-group state or commits Kafka
  offsets.
- Topic names and ACL scopes are tenant-prefixed so a tenant can only bind to
  its own `(tenant_id, queue_id)` streams.

### Retention frontier

- pqueue's log and snapshot retention frontier remains authoritative for the
  source log.
- A source segment MAY expire only after the segment is covered by a committed
  snapshot and the durable emission cursor has advanced past the segment's
  terminal `CommandPosition`.
- Heimq retention MUST be configured independently so Kafka consumers can catch
  up within the pqueue retention window; pqueue does not rely on broker-side
  retention for its own source-log safety.

### CL-8 tenant authz

- Access to the Kafka surface MUST be tenant-scoped.
- Topic ACLs MUST be granted only for the tenant-prefixed topic namespace that
  corresponds to the caller's `(tenant_id, queue_id)` scope.
- The broker surface MUST not expose a tenant with any other tenant's change
  topics or consumer-group state.

## Rationale

- External broker keeps pqueue's responsibility limited to emitting a durable
  ordered stream.
- Heimq is already the in-tree Kafka wire-protocol dependency referenced by
  TD-008 and ADR-005, so the change-log surface aligns with existing protocol
  plumbing.
- Broker-owned groups and offsets avoid importing consumer-group semantics into
  pqueue's commit path or retention logic.

## Alternatives considered

### pqueue-as-broker

Rejected. It would force `pqueue-server` to own metadata/fetch/group handling,
offset storage, and fan-out. That adds a second durability model to the service
binary without improving the change-log contract.

### Fjord as the external broker

Rejected for this decision. Fjord remains a possible deployment family, but the
repository already anchors the Kafka protocol dependency around heimq-wire, so
heimq is the narrower and more direct fit for the change-log surface.

## Consequences

- TD-008 can now state a normative Kafka binding instead of an open question.
- Change-log consumers use stock Kafka clients against heimq.
- pqueue retains the append-only log/snapshot frontier and durable emission
  cursor as its source-retention boundary.
- ADR-005 remains unchanged in substance: consumer-side Kafka APIs are still out
  of scope for the queue data plane. This ADR only addresses the append-only
  change-log surface.
