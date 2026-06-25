# Plan — The Queue Is the Unit of Sharding (cascade simplification + relational projection)

**Status:** plan (spec-evolution phase) — **GO**. Governs a `/loop` over the **design/spec cascade only**.
Code is a separate later phase.

**Branch:** `spec/coordinated-log-relational-projection`.

**Authority:** the keystone change — retiring *intra-queue* sharding from the **PRD** (FR-13, FR-48
multi-shard fixtures, the hot-queue/horizontal-envelope rows) — is a product-scope retraction. It was
**explicitly sanctioned by the product owner**: "Queue is the unit of sharding. If you want more sharding,
create more queues." Two adversarial review rounds shaped this plan; the only BLOCKER (the PRD retraction)
is now authorized, and the remaining conditions are baked in below.

## 0. The decision

**The queue is the unit of sharding.** A whole queue is owned by one node; there is no intra-queue
sharding. Horizontal scale = distributing **queues** across nodes (and app-level fan-out to more queues when
a producer needs more than one owner's throughput). This makes claims single-hop, kills scatter-gather and
the stalls it causes, and removes a large amount of cross-shard machinery. Consequence (accepted): the
"single queue distributed across shards exceeds one deployment" capability (PRD FR-13 / TP-002 E2) is
**retracted**; horizontal scale is re-expressed as **cross-queue**.

Note: the existing spec already *defaults* `shard_count` to 1 (one owner per queue). This change makes that
the *only* model — it removes the `shard_count > 1` opt-in and all the cross-shard machinery that hung off
it, so the cascade stops asserting a capability we no longer offer.

Two adjacent items ride along:
- **Two projection families** (in-memory log-replay + the relational `pqueue_items`/SQL-claim projection that
  TD-002 already specifies), held identical by the **conformance suite as contract**. This reconciles
  ADR-007's "one shared projection / fused-vs-split disappears" premise, which contradicts the standing TDs.
- **A pluggable `ControlPlaneStore` seam** (so a no-Postgres / object-store control plane is *possible*).
  Per review B3, the object-store impl has a real correctness cost (multi-object acquire→epoch→fence
  atomicity on eventually-consistent listing vs TD-004's locked fence rule). So this loop specs only the
  **pluggable seam**; the **object-store impl is deferred** to its own design + S3-CAS spike, not settled
  here.

## 1. Cascade-wide consequence (one model, stated everywhere)

- `shard_count` is **removed from the contract** (API-001 `CreateQueue`, the config-identity hash,
  idempotent-create). A queue maps to one owner via internal placement `hash(tenant_id, queue_id) % N`
  (N default 16, provisioning-time) — **client-invisible** (ADR-004's "shard_id never client-visible" holds).
- `group_co_residency` becomes a **no-op axis** (a queue is one shard, so groups/cohorts are co-resident by
  construction); `group_key` is **ordering/compatibility only**, never a placement key.
- **No cross-shard machinery anywhere**: delete cross-shard claim fan-out/k-way-merge, cross-shard
  queue-global progress aggregation, `SetGates.response.shards[]`, discovery shard-aggregation, resharding/
  cohort-split. Progress bound is a local per-queue property on its one owner.
- **Ownership keeps** TD-003's single-writer + Single Authoritative Fencing Rule + HRW placement + lease/
  drain/failover/recovery — re-scoped from per-`(queue,shard)` to **per-partition**.

## 2. Spec-update chunks (the `/loop` work-list)

Source-of-truth first; each independently committable; re-stamp affected `ddx:` hashes per §3.

- **C0 — PRD cleanup (keystone, sanctioned).** Retract intra-queue sharding: FR-13 (`prd.md:196-199`), the
  hot-queue-across-shards row (`:80`), the horizontal-envelope row (`:96`), FR-11/perf "horizontally across
  independent shards" (`:66,:183,:190`), the Tier-2 multi-shard framing (`:78`), and the FR-48 *multi-shard*
  acceptance fixtures (`:478` "One queue, 4 shards"; `:479` "Group spanning shards"). Restate: queue = unit
  of sharding; horizontal scale = queues across nodes; one queue = one owner; `group_key` = ordering only.
  Keep the cross-*queue* discovery fixtures (`:477,:480,:481`) — those still hold. *Acceptance:* the PRD no
  longer asserts within-queue sharding or cross-shard aggregation.
- **C1 — ADR-008 (new keystone).** "The queue is the unit of sharding." Decision + Alternatives (intra-queue
  sharding — retracted, cite PRD/E2) + Consequences. **Decides `shard_count`'s fate here**: removed from the
  contract. Records the two projection families (conformance-as-contract) and the pluggable control-plane
  seam (object-store impl deferred). Supersedes ADR-004 placement, TD-003 cross-shard, ADR-007
  one-projection, and amends ADR-001's control-plane stance.
- **C2 — ADR-004 amendment.** `group_key` ordering/compatibility only; remove `shard_id = hash(group_key)
  mod shard_count` placement; `group_co_residency` → no-op (state its fate explicitly); partition is internal
  physical placement, client-invisible.
- **C3 — ADR-001 + ADR-007 amendments.** ADR-001: *justify + add* a pluggable control plane (ADR-001 already
  says "Postgres preferred… backend control plane may be supported later but must justify" — keep that bar;
  the object-store impl is the deferred candidate that must clear it). Projection is a family. ADR-007:
  **retract** "the fused-vs-split special case disappears"; two families via conformance.
- **C4a — TD-003 simplify (placement + lease).** Partition-by-`(tenant,queue)`; one owner per partition;
  **keep** single-writer + Single Authoritative Fencing Rule + HRW + lease/drain/failover/recovery
  (re-scoped per-partition); **delete** intra-queue item-to-shard, the entire Cross-Shard Progress section
  (`TD-003:349-389`), resharding/cohort-split. Progress is local per-queue.
- **C4b — TD-003 control-plane pluggability (seam only).** `ControlPlaneStore` is pluggable; the
  object-store impl is recorded as a **deferred capability pending an S3-CAS spike** (review B3) — not specced
  as settled. Its own fresh-eyes review when it lands.
- **C5 — TD-001.** Rewrite the normative §"Multi-shard claim and cross-shard progress" (`TD-001:418-468`) —
  it is a section, not a line: remove fan-out/k-way-merge/claim-intent-plan/cross-shard-progress; claim is
  single-owner-local. Add the pluggable control-plane capability + the relational `ProjectionStore` variant +
  conformance-as-contract (core/log suites). Capability-class table.
- **C6 — TD-002.** Confirm the relational `pqueue_items` projection is per-queue single-owner; drop the
  `pqueue_group_summary` `shard_id` dimension (new key `(tenant,queue,group_key)`); remove cross-shard; keep
  the schema + claim CTE as source of truth (edit, don't replace); Postgres = one control-plane option;
  log-optional + idempotency.
- **C7 — API-001.** Enumerate and edit every cross-shard site: `CreateQueue.shard_count` (`:163`) + echo
  (`:165`) + config-identity hash (`:750`) + idempotent-create (`:176`); cross-shard claim routing/merge
  (`:435-444`); `SetGates.response.shards[]` (`:261`); discovery cross-shard aggregation (`:672-680`);
  `progress_bound_risk_count` "summed across shards" (`:621`). `oldest_eligible_age_ms` becomes per-queue.
- **C8 — API-002.** Remove the per-shard operator-command fan-out (`:82,:320` "split into per-shard commands…
  no global all-or-nothing across shards") — operator ops are now whole-queue.
- **C9 — TD-004 + TD-005 + TD-006 + TD-007.** Remove cross-shard references; per-partition model. TD-006:
  client routing `hash(tenant,queue) → owner` + `MOVED`-style redirect (redirect-on-miss safe via the fence),
  no scatter-gather on the wire. TD-007: durability/fence/recovery for the simplified model; the durability
  postures; `request_id` idempotency in claim timeout/failover.
- **C10 — TP-002.** **E2 reframed to cross-queue scale-out** (N queues across N owners, monotonic with owner
  count, per-queue floor preserved under density) — paired with the C0 PRD retraction; E1 = relational
  projection on one Postgres; E3 = object-log mode + recovery; note in-memory-only is not scale evidence.
  Keep E0–E3 IDs; remap. Retire `performance_multi_shard_scale_out_tests` as written.
- **C11 — TP-001 + TP-003.** Remove cross-shard/multi-shard test rows: TP-003 AC-SHARD-1/2, AC-DISC-2
  (spanning-shards), AC-E2E-5/6 ("multi-shard required"); TP-001 fan-out/cross-shard rows. Add
  conformance-as-contract suites + new surfaces (relational claim concurrency, ownership lease + fencing,
  failover recovery, routing redirect).
- **C12 — cascade re-stamp + consistency sweep.** `DesignSync` re-stamp all touched docs + dependents; sweep
  for any residual `shard_count > 1` / cross-shard / one-shared-projection assertion across the whole cascade
  (incl. ADR-005/006, TD-005, concerns.md); `build-progress.md` ledger entry + index pointer to ADR-008;
  `git status` clean.

## 3. Non-negotiables (loop invariants)

- **Doc-only.** No source changes. "Spec, not yet built" stated for future code. Don't regress PHASE-7's
  record of what is built.
- **Supersede, don't silently contradict** (review I2): edit each prior doc to *retract* the superseded
  claim; no doc may still assert intra-queue sharding, `shard_count > 1`, cross-shard progress, or one shared
  projection after its chunk.
- **Edit the rich TDs, don't replace** (review B1): reuse TD-002/TD-003's existing schema/CTE/fence rule; the
  change is *removal + re-scope + small additions*, not a thinner rewrite.
- **One home for fencing/ownership: TD-003.** Others reference it (review I1).
- **Complete supersession surface** (review B2): the chunk list above enumerates PRD, TD-001 §Multi-shard,
  API-001 (by line), API-002, TP-003 — verify nothing else asserts the retracted model in C12.
- **Object-store control plane is deferred, spike-gated** (review B3): this loop specs only the pluggable
  seam; the object-store impl is not settled without a real S3 `If-None-Match`/`If-Match` + multi-object
  acquire→fence atomicity spike.
- **Cascade hash discipline:** `DesignSync` re-stamp at the end of each chunk that changes a doc others
  depend on (C0–C5 especially), not deferred to C12 (review M2).
- **Review gate per chunk:** fresh-eyes adversarial review for C0/C1/C4a/C5/C6; documented self-review for
  amendments/sweeps. Record the verdict in the commit.
- **Commit per chunk** on the branch (Co-Authored-By).
- **Stop condition:** all chunks committed, cascade re-stamped + consistent, `git status` clean → end loop,
  `git log` the commits, summary. Stop earlier only on a genuine ambiguity needing the product owner.

## 4. Progress

- [x] C0 PRD cleanup · [x] C1 ADR-008 · [x] C2 ADR-004 · [x] C3 ADR-001+007 · [x] C4a TD-003 simplify
- [x] C4b TD-003 control-plane seam · [x] C5 TD-001 · [x] C6 TD-002 · [x] C7 API-001 · [x] C8 API-002
- [x] C9 TD-004/005/006/007 · [x] C10 TP-002 · [ ] C11 TP-001/003 · [ ] C12 re-stamp + sweep

> Code phase (separate, out of scope for this loop): M1 sqlite-relational projection (log-less,
> single-owner) → core conformance; M2 postgres relational + `SKIP LOCKED`; M3 per-partition ownership +
> fencing + Postgres control plane + `MOVED` routing; M4 benchmark (cross-queue). The no-Postgres
> object-store control plane is its own later design + spike.
