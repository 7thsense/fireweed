---
ddx:
  id: adr-queue-as-shard-unit-and-projection-families
  depends_on:
    - prd
  status: accepted
  review:
    self_hash: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
    deps:
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
    reviewed_at: "2026-07-20T00:01:20Z"
---

# Architecture Decision Record

**ADR ID**: ADR-008
**Title**: The queue is the unit of sharding; two projection families; a pluggable control plane
**Status**: accepted
**Related**: PRD (FR-13), ADR-001 (CQRS log/projection), ADR-004 (granularity & claim domain),
ADR-007 (hexagonal & two interfaces), TD-001 (backend contracts), TD-002 (relational `postgres_native`),
TD-003 (sharding & ownership), TD-006 (RESP), TD-007 (durability), ADR-013 (single-source-of-truth
amendment to the projection-family authority claim),
`docs/helix/04-build/coordinated-log-relational-projection-plan.md`

## Context

The original storage model (ADR-001) and its technical designs specify **intra-queue sharding**: a queue is
split into `shard_count` shards, items are placed by `hash(group_key | client_item_key) mod shard_count`
(ADR-004 / TD-003), and a queue-global claim **fans out across all of a queue's shards and k-way-merges**
their candidates, with a queue-global progress bound aggregated across shards (TD-001/TD-003). That buys
*single-queue* horizontal scale (PRD FR-13; TP-002 E2: one queue over 8 shards ≥ 4× one deployment).

Two costs surfaced in review:

1. **Scatter-gather per claim.** A queue-global claim must probe the top of every shard — across owner nodes
   — and merge. That is expensive, stall-prone, and taxes priority specifically (cheap claims would require
   relaxing ordering). For a priority work queue this undercuts the core value at exactly the scale where it
   should hold up.
2. **ADR-007's projection premise contradicts the standing designs.** ADR-007 consolidated storage to "one
   shared in-memory projection + swappable log stores" and recorded that *"the 'fused vs split' special case
   disappears (a backend is just a durability class)."* But TD-002 still specifies the relational
   `fireweed_items` projection with an SQL `FOR UPDATE SKIP LOCKED` claim — a genuinely different projection.
   The code shipped the one-projection version (PHASE-7: `ShardId::ZERO`, an in-memory projection,
   `postgres_native` rebuilt as a log-store rather than the relational projection), leaving large queues
   RAM-bound to a single process and the multi-shard horizontal envelope unbuildable.

E0 requires a single owner to preserve exact outcomes, queue-global progress,
and bounded resources under load. "Many queues across many owners" provides
aggregate headroom while the at-least-1,001-active-queue density run exercises
one hot queue and at least 1,000 cold queues. The product owner has decided to trade single-queue
horizontal scale for single-hop, no-stall claims: **"Queue is the unit of sharding. If you want more
sharding, create more queues."**

## Decision

1. **The queue is the unit of sharding.** There is no intra-queue sharding. A whole queue is owned by exactly
   one node at a time, placed by a deterministic function of `(tenant_id, queue_id)` over the live owner set
   (HRW/rendezvous hashing, TD-003) — **per queue**, not per a fixed bucket count. Placement is
   **client-invisible** (ADR-004's "`shard_id` is never a client-visible ordering/progress key" holds).
   **`shard_count` is removed from the contract** (API-001 `CreateQueue`, the config-identity hash,
   idempotent-create). Horizontal scale is **cross-queue** — distributing queues across nodes; a producer
   needing more than one owner's throughput for a logical stream partitions it across multiple queues at the
   application layer.

   (A relational store MAY *physically* hash-partition its item table — e.g. Postgres declarative partitioning
   by `hash(tenant_id, queue_id) % N`, `N` default 16, power-of-2 — purely for vacuum/index-size isolation.
   That is an internal TD-002 **storage** optimization, **not** an ownership, routing, or client-visible
   unit, and it does not bound how many nodes the queue population spreads across.)

2. **Two projection families, one behavior and transaction contract.** The system supports (a) the **in-memory log-replay**
   projection (embedded / object-log) and (b) the **relational / DB-resident** projection (`fireweed_items` +
   SQL claim, sqlite/postgres). They share **behavior, not code**: the **conformance suite is the contract**
   that holds them identical. Partition principle: the **core** suite is all observable queue behavior
   *independent of durability substrate* — ordering, eligibility, claim atomicity, idempotency, lease/epoch
   fencing, success/error/unknown-outcome semantics, read-after-success visibility, and the per-queue progress bound — which **every** projection passes; the **log** suite is
   replay-from-log, snapshot+tail recovery, and segment/manifest commit, which only log-bearing backends run;
   a relational backend substitutes a **reconnect-after-crash durability** test for replay-from-log. This
   **retracts** ADR-007's "fused vs split disappears" premise (ADR-007:71).

3. **Ownership is per-queue single-writer with an epoch fence.** Retain TD-003's mechanism — a control-plane
   lease, deterministic HRW/rendezvous owner placement over the live owner set, and the **Single Authoritative
   Fencing Rule** (epoch allocated in the control plane, durably fenced into the log before the owner serves,
   append rejects any non-current epoch) — **re-scoped from per-`(queue,shard)` to per-`(tenant,queue)`**: the
   owned unit is the whole queue, and the lease / epoch / HRW key is `(tenant_id, queue_id)`. There is **no
   cross-shard machinery**: no fan-out/k-way-merge claim, no cross-shard progress aggregation, no resharding/
   cohort-split. The progress bound — both `oldest_eligible_age_ms` and `progress_bound_risk_count` — is a
   **local per-queue property**, not a cross-shard aggregate or sum.

4. **The control plane is pluggable, and the object log is the committed no-Postgres implementation.**
   `ControlPlaneStore` (membership + leases + epoch) is a capability with a Postgres implementation
   (default). The no-Postgres / object-store implementation (S3 conditional-PUT lease + heartbeat
   membership + epoch CAS), enabling a pure object-log + local-projection deployment, is **committed
   direction** (product-owner decision, 2026-07-05): the object log is intended to provide **multi-node
   fencing and coordination at the per-queue level**, building on the manifest conditional-PUT series
   that already serves as both CAS and epoch fence for appends (TD-004). The S3-CAS
   multi-object-acquire→fence-atomicity design must still be proven before the implementation ships (it
   has a real correctness cost the transactional Postgres path gets for free), but it is sequenced build
   work, not an open question. This loop specs the pluggable **seam**.

## Consequences

- **Removed across the cascade:** intra-queue item-to-shard placement (`hash(group_key) mod shard_count`),
  cross-shard claim fan-out/k-way-merge, cross-shard queue-global progress aggregation, resharding/cohort-
  split. `group_key` becomes an **ordering/compatibility** concern only (never placement).
  `group_co_residency` is **removed from the contract and the config-identity hash** (co-residency now holds
  by construction); the per-item `group_key`-required rule and `whole_group`/`whole_cohort` atomicity become
  **unconditional queue properties**, gated only by `cohort_policy.enabled` / `compatibility.group_batching`
  config, never by a `group_co_residency` flag. `metrics.oldest_eligible_age_ms` and
  `progress_bound_risk_count` are per-queue on its one owner (not cross-shard).
- **TP-002 E2 is reframed** from single-queue-N-shard to **cross-queue scale-out** —
  N queues across N owners, with exact work, logical progress, single ownership,
  fencing, and bounded shared resources preserved as owner and queue counts rise.
- **Gains:** claims are single-hop (no scatter-gather, no stalls); the relational projection removes the
  in-RAM ceiling; ownership/coordination collapses to one lease per queue — replacing the per-`(queue,shard)`
  lease — which is what makes the no-Postgres option tractable.
- **Trade-off accepted:** a single queue cannot exceed one owner's capacity — mitigated by app-level
  multi-queue fan-out. E0 qualifies behavior under load; measured rates are
  capacity evidence for the declared owner topology, not a portable contract.
- **Supersedes / amends:** ADR-004 (item-to-shard placement; `group_co_residency`), TD-003 (the cross-shard
  model), ADR-007 (retract the "fused vs split disappears" claim, ADR-007:71). It **amends ADR-001** — which
  already states "Postgres is preferred; a backend-specific control plane may be supported later but must
  justify" — by **adding a concrete pluggable `ControlPlaneStore` seam** (the object-store impl is the
  deferred candidate that must clear ADR-001's justification bar, not a removal of the bar) and by
  establishing the projection as a **family** with a behavior contract; ADR-001's `ControlPlaneStore`
  capability row (shard assignment / shard-owner leases) is re-scoped per-queue. The PRD was amended first
  (FR-13, FR-11/12, FR-48, Success Metrics) so the source of truth leads the cascade.

## Alternatives considered

- **Keep intra-queue sharding (the existing TD-003 model).** More capable — a single queue scales across
  shards — but pays the scatter-gather/stall cost on every queue-global claim, taxes priority, and mandates a
  transactional control plane. Rejected: consumer simplicity + no stalls + a no-Postgres option outweigh
  single-queue horizontal scale, which the floor math makes narrow.
- **Async-behind projection (log leads, projection trails).** Deferred and benchmark-gated; it only pays off
  when the log substrate is far cheaper than the projection AND any lag is hidden behind API-001's success
  barrier. Exposing read-after-success lag, delayed claim visibility, or backend-specific caller repair is not
  acceptable; it would break transaction integrity. (This bullet originally allowed a log-less relational
  default; ADR-013 retired that — the durable log is mandatory in every production deployment, so the v1
  relational mode is log + sync-projection.)
- **Routing via a separately-distributed owner map.** Unnecessary: owner placement is deterministic (HRW over
  the live owner set), so the map is *computable*, and a stale route is safe (the fenced append rejects a
  deposed owner) — so a lazy `MOVED`-style redirect-on-miss suffices.
