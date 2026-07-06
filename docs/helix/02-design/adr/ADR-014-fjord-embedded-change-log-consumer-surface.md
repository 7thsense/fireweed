---
ddx:
  id: adr-fjord-embedded-change-log-consumer-surface
  depends_on:
    - adr-kafka-producer-wire-adapter
    - adr-auth-tenancy-and-storage-isolation
    - adr-log-single-source-of-truth
  review:
    self_hash: 1f3190f62b6dbccebd33dfd0f141e411e437bf041551f58e5ca61c0409a10e67
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-kafka-producer-wire-adapter: b49b122239af43127faabd91747efc79cc3853555ffa9bfe4febb9d04f8bde32
      adr-log-single-source-of-truth: 66130c84cb8e5467f5192066a0446f527672dac2eea83f7eae70b66c1e3b724c
    reviewed_at: "2026-07-06T17:31:18Z"
---

# Architecture Decision Record

**ADR ID**: ADR-014
**Title**: Fjord, embedded in pqueue, provides the change-log Kafka consumer surface
**Status**: Accepted (product-owner decision 2026-07-06; **supersedes the 2026-07-05 revision of this
ADR**, which chose heimq as an external broker — see "Superseded revision" below)
**Related**: TD-008 (queue history / change records), ADR-005 (Kafka producer adapter, scope note
only), ADR-002 (tenant isolation), ADR-013 (log is the source of truth), TD-009 (branches default
to change-record opt-out).

## Context

TD-008 requires a Kafka-protocol consumer interface for the append-only, per-queue-ordered change
log. The product requirement is about the change-log surface only; it does not reopen the queue
data-plane decision that consumer-side Kafka APIs are out of scope for the core queue model
(ADR-005).

The binding constraint (product owner, 2026-07-06): **pqueue must own an interface to the change
log.** Forcing operators to deploy an external Kafka queue is not acceptable as the required shape,
and an object-log-only pqueue with no Kafka component leaves no way for another system to consume
the change log at all. The consumer surface must exist in every pqueue deployment, out of the box.

Honest dependency state: pqueue currently has **no** Kafka-protocol dependency. `heimq-wire` left
the workspace when the `pqueue-kafka` producer adapter was deleted (ADR-005, superseded by ADR-007);
no workspace `Cargo.toml` references heimq or fjord today. Whatever provider is chosen is a new
dependency, not an existing anchor.

## Decision

pqueue **embeds fjord** — the sibling Kafka-protocol log system over object storage — to provide
the change-log Kafka consumer surface in-process. pqueue owns the interface; no external broker
deployment is required for another system to consume the change log.

### Provider + shape

- **Provider**: fjord, embedded as a component of `pqueue-server` behind an explicit seam.
- **Shape**: **pqueue produces to fjord; fjord does Kafka things.** Canonically (product owner,
  2026-07-06): *if* fjord change logs are active, there is one topic per pqueue queue; as changes
  are persisted to the projection (i.e., as commands commit under ADR-013's log-durable →
  projection-applied ordering), they are captured as change events on that topic; fjord allows
  consumer groups to consume those topics. The relationship is producer-only — the TD-008 emission
  task feeds committed change records through the delivery seam, and pqueue's responsibility ends
  there. Everything Kafka — metadata, fetch, consumer groups, committed offsets, topic state,
  fan-out — is fjord's concern, implemented and owned entirely inside the fjord component. No Kafka
  concept appears in pqueue's engine, projections, contracts, or vocabulary; pqueue owns the
  surface only in the deployment sense (it ships in-process, so it exists wherever pqueue runs and
  is activated by configuration). Disambiguation: "consumer group" on the RESP data plane is
  unrelated stock Redis-Streams wire vocabulary that pqueue accepts for client compatibility and
  never persists (TD-006); it has nothing to do with Kafka consumer groups.

### Boundary invariants (what "well-maintained boundaries" means)

1. **Feed-forward only.** Fjord consumes emitted change records through the same delivery seam as
   every other binding (`ChangeRecordSink` tail consumer, TD-008 CL-1..CL-8). It never reads pqueue
   internals — not the projection, not the command log, not the control plane.
2. **Never on the commit path.** CL-2 holds: pqueue's commit path neither blocks on, observes, nor
   fails because of fjord. Fjord unavailability degrades the Kafka surface only; queue correctness
   and the niflheim HTTP binding are unaffected.
3. **Separate storage namespace.** Fjord's topic/offset/consumer-group state lives in its own
   storage namespace (its own object-store prefix or volume), never intermixed with pqueue's log
   segments, manifests, or snapshots.
4. **Swappable at the seam.** A deployment that must publish to an external Kafka instead attaches
   a producer sink at the same seam — and the embedded fjord simply sits idle. The external option
   is a deployment choice, never the required shape.

### Offset ↦ `CommandPosition` mapping

- Each `(tenant_id, queue_id)` change stream is a **single-partition topic**, so the fjord-assigned
  offset is monotonic for that stream and per-queue order (CL-4) is preserved on the wire.
- Kafka offsets are broker-assigned append positions; `CommandPosition` remains the product's
  durable source identity and is carried on every record (headers, below).
- On failover, pqueue may re-emit records from the last durable emission cursor (CL-5). Re-emitted
  records appear at later offsets; the offset stream never regresses. Correctness for consumers
  rests on the per-record identity, not on offsets — see the consumer contract.

### Normative consumer contract

Two implementers must build the same consumer, so the record shape is pinned:

- **Record key** (Kafka message key): the per-record idempotency identity
  `"{item_id}:{backend_epoch}:{sequence}"`, with `item_id` empty for queue-scoped records. Combined
  with the topic's `(tenant_id, queue_id)` identity this equals TD-008's idempotency key; it is
  unique even when one `CommandPosition` fans out to N item records.
- **Headers**: `pq-tenant-id`, `pq-queue-id`, `pq-item-id` (absent for queue-scoped records),
  `pq-backend-epoch`, `pq-sequence`, `pq-command-kind`.
- **Payload**: the TD-008 `ChangeRecord` serialization.
- **Consumer obligations**: consume in offset order; deduplicate on the record key over a window at
  least as long as the worst-case emission outage + failover re-emission horizon (the same window
  TD-008 requires of niflheim); commit Kafka offsets only after the record's effect (or its dedupe
  decision) is durable on the consumer side. Committing offsets without dedupe state is not a
  conformant consumer.

### Retention frontier (scoped)

- pqueue's log and snapshot retention frontier remains authoritative for the source log.
- On a queue with `emit_change_records = true`, a source segment MAY expire only after (a) the
  segment is covered by a committed snapshot AND (b) the durable emission cursor has advanced past
  the segment's terminal `CommandPosition`.
- On a queue with `emit_change_records = false` (including TD-009 branches, which default to
  opt-out), **only condition (a) applies** — no emission cursor exists for the queue and segment
  expiry MUST NOT wait on one. Opting back in guarantees records from the opt-in position forward
  only (TD-008 CL-1), so re-enable never resurrects an expiry obligation for already-expired
  segments.
- Fjord-side topic retention is configured independently and bounds how far back a Kafka consumer
  can catch up; it never gates pqueue's source-log safety.

### CL-8 tenant authz

- Access to the embedded Kafka surface MUST be tenant-scoped: topic names are tenant-prefixed and
  ACLs grant only the caller's `(tenant_id, queue_id)` namespace.
- The surface MUST NOT expose any other tenant's change topics or consumer-group state, and MUST
  NOT leak topic existence across tenants (ADR-002 deny-by-default, no existence leak).

## Rationale

- **Ownership**: the requirement is an owned, always-present consumer surface. Only an embedded
  provider satisfies it; any external-broker shape makes the change log consumable only where an
  operator has deployed and wired a second system.
- **Fit**: fjord is Kafka-protocol over object storage — the same substrate family as pqueue's
  object log — so an embedded fjord adds no new storage service to a pure object-log deployment.
- **Scope discipline**: embedding fjord keeps Kafka consumer-group semantics out of pqueue's own
  code (they live behind the boundary), which is the substance of ADR-005's data-plane concern.

## Superseded revision

The 2026-07-05 revision of this ADR chose **heimq as an external broker**. It is superseded for two
reasons: (1) its rationale rested on a false premise — that heimq-wire was already an in-tree
dependency (it is not; it left with `pqueue-kafka`); (2) the external-broker shape fails the
ownership requirement — it cannot guarantee a consumer surface exists in every deployment. The
external-broker option survives as the idle-fjord deployment fallback described above, not as the
decision.

## Alternatives considered

### External broker (heimq or any managed Kafka)

Rejected as the required shape: it forces every deployment that wants change-log consumers to
operate a second system, and pqueue cannot guarantee the surface exists. Retained as an optional
deployment mode via a producer sink at the same seam (the embedded fjord idles).

### pqueue-as-broker, hand-rolled

Rejected: `pqueue-server` would have to implement Kafka metadata/fetch/consumer-group/offset
machinery itself, importing a second durability model into the service binary. Embedding fjord
provides that machinery behind a boundary instead.

## Consequences

- fjord becomes an embedded dependency of `pqueue-server` — git-pinned like `axon-esf` (ADR-011's
  no-path-deps rule applies; no path dependencies into sibling repos).
- The Kafka-binding implementation bead is re-scoped to: embed fjord behind the delivery seam, feed
  it from the emission task, enforce the boundary invariants, pin the record contract above, and
  prove CL-1..CL-8 on the embedded surface with a stock Kafka client.
- TD-008 states the fjord binding as normative; the niflheim HTTP binding is unchanged.
- ADR-005 remains unchanged in substance: consumer-side Kafka APIs are still out of scope for the
  queue data plane. This ADR addresses only the append-only change-log surface.
