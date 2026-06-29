---
ddx:
  id: td-s3-object-log-sqlite-projection-mode
  depends_on:
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-sharding-and-shard-ownership
    - adr-queue-as-shard-unit-and-projection-families
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-granularity-mapping-and-claim-domain
    - adr-rust-workspace-and-toolchain-policy
    - prd
    - concerns
  review:
    self_hash: fde8c520a39579fd2c2e771a3f251d09714bb370db6e2eaf040c2d84e9e7dc0d
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: 9a9570ebe2718bf637c73564018e3702bc4473bcbf5a6499b52b7e1937bd0b83
      adr-granularity-mapping-and-claim-domain: f84d9bd6d3a8ab886c14f84afa45d189923e0cb7db32f57b700a9a0d8b1655b4
      adr-queue-as-shard-unit-and-projection-families: 77d1e2feb6a27e0a093564e3f07247cd8cc2c6fba6c3d20b5eeade568ba25964
      adr-rust-workspace-and-toolchain-policy: ab726c0cca517786afa9301ab8e15e525c664dfbcd011a2cf736e22993e2ef27
      api-native-client-interface: a97e014a176aa9e37a93fbab151c31ffb47aa8428c62e802c98fa3be0413426b
      concerns: 7e3b81e376f75f71691f55ac1ca4d9599eddcfe6eefe70f614c366c132e07992
      prd: a910dd5fb95102767b4ddf81115569d39d85c7e082a40c62ce424dea73ca8533
      td-postgres-native-reference-mode: ea91286ed9f810497a7da0dd05f962e0bfe2cb001acb682f3d7b10e1e69cdc64
      td-sharding-and-shard-ownership: 6bf3dcc75c94fefa35af4ed9f1859e76b76df3f171a89622fcb24888d92c93e4
      td-storage-architecture-backend-contracts: a0053226d680acddfc3b606ec106c47ffb09167374940dc8282607e46b8df96e
    reviewed_at: "2026-06-25T04:21:18Z"
---

# Technical Design: TD-004 S3 Object-Log + SQLite Projection Mode

**Contract**: API-001 | **ADR**: ADR-001, ADR-004, ADR-008 | **Depends on**: TD-001, TD-002, TD-003 | **Scope**: `object_log_sqlite_projection` backend

## Scope

This technical design defines the second committed v1 storage backend for pqueue:
`object_log_sqlite_projection`. In this mode an S3-compatible object store is the durable command
log (the ack boundary), a local SQLite database is the rebuildable operational projection, the same
object store holds periodic projection snapshots, and Postgres remains the control plane. Per ADR-008
the queue is the unit of sharding: a whole queue is owned by exactly one node, so the object log, the
manifest, and the SQLite projection are all **per-`(tenant, queue)`**, and there is no intra-queue
sharding or cross-shard command machinery.

This backend exists to substantiate pqueue's horizontal-scale and cost claims with a profile whose
durable-commit cost scales with *segments*, not with *commands* (see ADR-001 napkin cost comparison).
Horizontal scale is **cross-queue** (ADR-008): many queues across many owners. This is the
cost-optimized counterpart to the latency-optimized `postgres_native` reference mode (TD-002), and the
relational projection family's log-bearing member (the SQLite projection is rebuilt from the log).

In scope:

- Group-commit pipeline: per `tenant/queue` command buffering, segment sealing with checksums
  and monotonic command positions, segment write, manifest commit, ack, SQLite projection apply.
- The in-flight claim **reservation** model: how `BatchClaim` selects candidates, prevents duplicate
  local claims while a segment is pending, and rolls back on CAS/timeout/fence (see "Claim Reservation").
- Object layout for segments, manifests, and snapshots (logical; exact byte framing in implementation).
- Replay-response idempotency model (ack only after durable manifest commit) and the read-after-write
  / apply-ordering contract that keeps API-001 satisfied once a response returns.
- SQLite projection schema mapping from TD-001 logical projection records and TD-002 column semantics.
- Periodic SQLite snapshot to object storage at a committed log position.
- Bounded replay and recovery: snapshot + log-tail, with safe segment expiry.
- Manifest-commit epoch fencing **validated against the current control-plane epoch**, bound to the
  TD-003 queue-lease/epoch model.
- Object-store conditional-write (CAS) as a first-class required backend capability, with a defined
  fallback for stores that lack it.
- Cohort (`cohort_policy` / `whole_cohort`, G6) projection, shared-lease, expiry, and replay bindings
  in the SQLite projection.
- Commit-latency / cost tradeoff and how API-001 semantics still hold once a response returns.
- Conformance parity with `postgres_native` through the TD-001 shared suite.

Out of scope:

- Group placement: there is no `shard = hash(group_key) mod shard_count` rule and no `group_co_residency`
  CreateQueue capability (removed by ADR-008; `group_key` is ordering/compatibility only). A group's
  members are co-resident on the queue's single owner by construction.
- The per-queue progress / oldest-eligible computation contract (owned by TD-003; this backend supplies
  the local per-queue inputs on the owner).
- Queue-to-owner assignment, lease renewal, monotonic epoch allocation, reassignment, graceful drain,
  and recovery orchestration (owned by TD-003; this backend supplies the manifest-commit fencing
  enforcement point and the recovery read path).
- The definition of "eligible" / "active group" (owned by the API-001 "Eligibility Precedence"
  subsection; this backend implements it, it does not redefine it).
- The `gate_keys`/`SetGates` gate model and the O(1) gate-flip consistency contract (owned by API-001 /
  G2; this backend materializes it in SQLite, it does not introduce a second gate mechanism).
- `group_key` topology resolution (owned by ADR-004 / MF7; consumed here).
- Operator repair, purge, redrive, migration, and backend-migration APIs (P1).
- Exact Postgres control-plane DDL (TD-002 / TD-001 / TD-003).

## Technical Approach

`object_log_sqlite_projection` is a **replay-response** backend (TD-001 §"Durable Ack and Response
Replay"). The durable commit boundary is a committed manifest entry that names a sealed, checksummed
segment in object storage. No command is acknowledged before its segment's manifest entry is durably
committed. After commit, commands are applied to a local SQLite projection that serves claim planning,
lease state, idempotency lookup, and metrics. SQLite is rebuildable: snapshot + log tail reproduce
acknowledged state after node loss.

This backend deliberately trades acknowledgement latency for cost: durable-commit cost scales with the
number of sealed segments, not with the number of commands, so large client batches plus a configured
group-commit window yield the cost floor described in ADR-001. Small, latency-sensitive commits should
use `postgres_native` (TD-002) or a fast log backend instead.

It follows TD-001's capability boundaries unchanged:

- `ControlPlaneStore`: Postgres (queue defs, queue-owner assignment + `assignment_epoch`, backend
  profile). Identical to TD-002's control plane; not re-specified here. The control-plane seam is
  pluggable (ADR-008); the object-store control plane is a deferred, spike-gated capability, so in v1
  this backend still uses the Postgres control plane. TD-004 *reads* the current `assignment_epoch`
  from it on the manifest-commit path (see Epoch Fencing).
- `LogStore`: S3-compatible object log with group-commit sealed segments and a per-queue manifest.
- `ProjectionStore`: local SQLite, rebuildable, applied only from committed commands (relational
  projection family, log-bearing member).
- `SnapshotStore`: S3-compatible object storage holding SQLite snapshots at committed positions.

**Key decisions**

- **Manifest entry is the ack boundary.** A command is durable when, and only when, the manifest entry
  naming its segment is committed via a conditional (compare-and-set) object write.
- **Fencing is enforced against the current control-plane epoch.** The manifest commit is the
  enforcement point of TD-001 `append_batch(expected_epoch)`. The CAS guards the manifest tail; the
  epoch check validates against the epoch currently recorded in the Postgres control plane for the
  queue, not merely the highest epoch already in the manifest (see "Manifest Commit and Epoch Fencing").
  This `assignment_epoch` is the same `u64` queue epoch TD-003 allocates and threads through
  `CommandPosition.backend_epoch`.
- **SQLite is a projection, never an authority.** `apply_committed` is the only writer of committed
  state; in-flight claim reservations are a separate, non-authoritative bookkeeping table that holds no
  acknowledged state (see "Claim Reservation"). Acknowledged state survives via object-store segments +
  snapshots, never via local disk alone (ADR-001 Option 4 rejection).
- **Reject one-object-per-command.** Production configurations MUST seal multiple commands per segment;
  a 1-command-per-object configuration MUST be rejected at queue/backend configuration time. It remains
  available only behind an explicit development/test fallback flag.
- **Conditional object write (CAS) is a required capability.** If the configured object store cannot
  provide a conditional write primitive, the queue MUST either be rejected or run in the
  Postgres-manifest-pointer fallback mode (see "Object-Store Capability Requirements").

## Group-Commit Pipeline (normative)

This realizes the 8-step ADR-001 §"S3/Object-Log Commit Model" sequence. Each step's normative rules:

| Step | Rule |
|------|------|
| 1. Buffer | Commands MUST be buffered per `tenant/queue`. A buffer accumulates `CommandEnvelope`s (TD-001) in arrival order. Because the queue is the unit of sharding (ADR-008), every command for the queue — including every member of a `group_key` — lands in the one queue buffer on the queue's owner, so `whole_group` (G1 `compatibility.group_batching`) and `whole_cohort` (G6 `cohort_policy`) claims are owner-local by construction. |
| 2. Seal | A segment MUST seal when EITHER the buffered byte size reaches `segment_target_bytes` OR the oldest buffered command's age reaches `segment_max_latency_ms`, whichever comes first. Sealing assigns each command a monotonic per-queue `sequence` (TD-001 `CommandPosition.sequence`) contiguous with the prior segment, and computes a per-segment `checksum` plus per-command `checksum` (TD-001 `CommandEnvelope.checksum`). |
| 3. Write segment | The sealed, immutable segment MUST be written to object storage under a deterministic key (see "Object Layout") before any manifest commit references it. The write SHOULD use an idempotent PUT keyed by `(queue, first_sequence)` so retried writes do not create divergent objects. |
| 4. Commit manifest | A manifest entry naming the segment, its `[first_sequence, last_sequence]` range, its checksum, and the writer's `assignment_epoch` MUST be appended via a conditional write that succeeds only if (a) the manifest's tail still equals the writer's expected tail AND (b) the writer's `assignment_epoch` is the **current** epoch for the queue (see "Manifest Commit and Epoch Fencing"). A failed CAS MUST abort the commit, roll back the in-flight reservation, and the writer MUST treat itself as raced or fenced. |
| 5. Ack | Only after the manifest entry is durably committed MAY the commands in that segment be acknowledged to the caller. Acknowledgement returns a response derived from committed command state (replay-response model). |
| 6. Apply | After ack-eligibility, committed commands MUST be applied to the SQLite projection in `sequence` order, exactly once, idempotently keyed by `last_command_sequence` (no command at or below the projection's applied position is reapplied). The apply-vs-return ordering contract is in "Response / Apply Ordering". |
| 7. Snapshot | The writer MUST periodically snapshot the SQLite projection to object storage at a committed log position (see "Snapshots"). |
| 8. Expire | A log segment MAY be expired (deleted) only after a committed snapshot covers its entire `[first_sequence, last_sequence]` range AND the configured `log_recovery_window_ms` past that snapshot has elapsed (see "Retention and Expiry"). |

`rearm` (recurring items, G5) and in-band `PurgeItems` (G5) are ordinary commands in this pipeline: they
are buffered, sealed, manifest-committed, acked, and applied like any other mutating command, and gain
durability and replay parity for free. No special-case path is required (see "SQLite Projection").

### Object Layout (logical)

| Object | Key shape (logical) | Contents |
|--------|---------------------|----------|
| Segment | `t/{tenant}/q/{queue}/seg/{first_sequence:020}.seg` | Immutable framed `CommandEnvelope`s, per-command checksums, segment trailer checksum, `assignment_epoch`. |
| Manifest | `t/{tenant}/q/{queue}/manifest` | Ordered, append-only list of `{segment_key, first_sequence, last_sequence, segment_checksum, assignment_epoch, committed_at}`. Conditionally written (CAS). |
| Snapshot | `t/{tenant}/q/{queue}/snap/{snapshot_sequence:020}.sqlite` | SQLite projection image at `snapshot_sequence`, plus snapshot metadata `{snapshot_sequence, segment_range_covered, projection_schema_version, checksum}`. |

Object naming is implementation-refinable but MUST keep `tenant`, `queue` as the leading key
components (tenant isolation, TD-001 §Security) and MUST keep segment keys monotonically orderable by
first `sequence`.

> A relational backend MAY internally hash-partition large item tables for vacuum/index-size isolation
> (TD-002 `hash(tenant, queue) % N`); that is a storage detail of the relational projection, distinct
> from this object-log layout and never an ownership or routing unit (ADR-008).

## Object-Store Capability Requirements (normative)

| Element | Rule |
|---------|------|
| Conditional write | The object store MUST provide a conditional (compare-and-set) write usable for the manifest object — e.g., `If-Match`/ETag-conditional PUT, conditional-on-absence PUT for monotonic manifest objects, or an equivalent guaranteed atomic CAS. The accepted primitive(s) MUST be documented per supported store. |
| Unsupported CAS | If the configured store provides no usable conditional-write primitive, `CreateQueue`/backend configuration MUST either reject the queue with `invalid-request` OR the deployment MUST select the Postgres-manifest-pointer fallback mode. A store without CAS MUST NOT silently run plain manifest appends. |
| Postgres-manifest-pointer fallback | In fallback mode the manifest tail pointer and `assignment_epoch` check are committed in a Postgres `ControlPlaneStore` row (transactional CAS), while segments and snapshots remain in object storage. This preserves the single-writer fencing property when the object store cannot. The fallback's durable-commit cost is one small control-plane write per segment (still per-segment, not per-command). |
| Idempotent segment PUT | Segment writes MUST be idempotent under retry so a retried PUT after a network failure cannot create a divergent object at the same key. |

## Manifest Commit and Epoch Fencing (normative)

This backend's fencing primitive is the conditional manifest write, which is the *enforcement point* of
the TD-001 `LogStore.append_batch(queue, expected_epoch, …)` fence. It binds to the TD-003 queue
lease/epoch model: TD-003 owns assignment, lease renewal, monotonic `assignment_epoch` allocation,
reassignment, drain, and recovery; TD-004 owns how a stale writer is prevented from extending the
durable log. The TD-003 safety invariant — "regardless of lease-clock skew, an append whose
`expected_epoch` is not the current epoch for that queue MUST be rejected" — MUST hold for this backend.

| Element | Rule |
|---------|------|
| Epoch source | The `assignment_epoch` a writer commits with MUST be the epoch of its active TD-003 queue lease for `(tenant, queue)`. A writer with no current lease MUST NOT seal or commit segments. |
| Current-epoch validation (the core fix) | Manifest commit MUST succeed only if the writer's `assignment_epoch` equals the epoch **currently authoritative in the control plane** for that queue, not merely "≥ the highest epoch already in the manifest." A manifest CAS that checks only the manifest-recorded epoch is INSUFFICIENT: if the control plane advances the epoch (reassignment) before the new owner writes any manifest entry, an old-epoch writer's tail-matching CAS would otherwise still pass. The commit path MUST therefore validate against the current control-plane epoch. TWO conformant implementations are permitted, and a backend MUST use at least one: |
| — (a) epoch-on-commit check | The committing writer validates its `expected_epoch` against the current control-plane epoch as part of the commit (e.g., a guarded control-plane read/transaction that the manifest commit is conditioned on, or the Postgres-manifest-pointer fallback whose CAS row carries the current epoch). This makes "fence published in control plane → old writer cannot commit" immediate. |
| — (b) epoch fence published to manifest before handoff | Epoch advancement MUST publish a fence record into the queue's manifest (a manifest entry recording the new `assignment_epoch` and no segment, or a fence marker) BEFORE the new owner begins committing data segments, so any subsequent old-epoch CAS observes the higher epoch and fails. Under this implementation, the manifest-recorded-epoch check is sufficient *only because* advancement is guaranteed to fence the manifest first. Recovery (below) MUST perform this fence publication as its first manifest write. |
| Manifest tail CAS | In addition to the epoch validation, manifest commit MUST be conditional on the manifest tail still matching the writer's expected tail, so two writers at the same epoch (transient split-brain) cannot both extend the log from the same point. |
| Fenced writer | A writer whose commit fails because the current epoch has advanced (or a fence record now records a higher epoch) MUST treat itself as fenced: it MUST discard its in-flight buffer and roll back its in-flight claim reservations (see "Claim Reservation") without ack, and MUST NOT retry the commit under the old epoch. Unacked commands are re-driven by the new epoch holder on the normal replay path (caller retries by `request_id`). |
| Recovery read | A newly assigned epoch holder MUST (1) under implementation (b), publish its epoch fence to the manifest as its first write; (2) read the latest committed snapshot; (3) replay manifest segments with `sequence > snapshot_sequence`, validating per-segment and per-command checksums; before sealing any new data segment. This reproduces acknowledged state (TD-001 conformance: snapshot recovery). |
| No consensus | This mechanism MUST NOT introduce node discovery, leader election, or embedded consensus (ADR-001 / D4(c)). The object store's conditional write plus the Postgres-backed TD-003 lease/epoch are the only coordination primitives. |

## Claim Reservation (in-flight reservations before durable commit) (normative)

`BatchClaim` is a mutating command: its lease assignments are durable only after the claim command's
segment manifest entry commits (replay-response). But the SQLite claim transaction that *selects*
candidates runs at request time, before the group-commit window closes. This section defines how
candidates are reserved so the same item is not handed to two concurrent claims while the segment is
pending, without making SQLite an authority.

| Element | Rule |
|---------|------|
| Authority boundary | SQLite holds NO acknowledged lease until `apply_committed` runs (pipeline step 6). Reservations are a separate, non-authoritative, in-memory-or-local bookkeeping state (e.g., a `pending_reservations` table or in-process index) that records "these item rows are tentatively claimed by pending command `command_id` at epoch E." It is rebuilt from the in-flight buffer on restart and is never snapshotted. |
| Select + reserve atomically | A `BatchClaim` MUST, in one SQLite write transaction: (1) evaluate the API-001 Eligibility Precedence predicate and the unified `ClaimPlan` to select candidate items, (2) mark those items reserved against the new claim `command_id`, and (3) append the claim command to the pending segment buffer. Reserved items MUST be excluded from candidate selection by any concurrent claim, so no duplicate local claim can occur while the segment is pending. (SQLite serializes writers, making (1)–(3) atomic.) |
| Commit → promote | When the segment's manifest entry commits and `apply_committed` applies the claim command, the reservation is promoted to a durable lease (lease row written, reservation cleared). Only then is committed lease state exposed to reads/metrics. |
| Roll back on CAS/timeout/fence | If the segment's manifest CAS fails (fenced or raced), the commit deadline elapses (`commit-timeout`), or the writer is fenced, the writer MUST clear the reservations for that command so the items return to the candidate pool. No lease is created and no ack is returned; the caller retries by `request_id` (claim idempotency). |
| Crash before manifest | If the writer crashes after reserving but before the manifest commits, the reservation is lost (it was never durable) and the claim command is lost with the unacked buffer. The new epoch holder recovers from snapshot + committed log; the un-acked claim simply never happened, and the caller's retry by `request_id` either re-claims or returns `request-expired` per API-001 claim idempotency. |
| Reservation TTL | Reservations MUST have a bound tied to the commit deadline so a stuck writer cannot pin items indefinitely; a reservation older than the commit deadline MUST be reclaimable. |
| Idempotent re-claim | A retried claim `request_id` whose original command did commit MUST converge to the same lease set (replay-response, "Replay-Response Idempotency Model"); a retry whose original never committed is a fresh claim subject to current eligibility. |

## Response / Apply Ordering and Read-After-Write (normative)

The pipeline acks after manifest commit (step 5) but applies to SQLite after ack-eligibility (step 6).
This section states exactly what a caller may observe so API-001 holds once a response returns.

| Element | Rule |
|---------|------|
| Apply-before-return for the operation's own result | A mutating operation's response MUST reflect its own committed effect. The writer MUST apply the operation's committed command(s) to the SQLite projection (or otherwise reconstruct the response from the committed log) BEFORE returning success to the caller. Apply for an operation's own segment is therefore on the response path, not lazily deferred; "apply after ack-eligibility" (step 6) means after the manifest commit makes ack legal, and it completes before the response returns. |
| Self read-after-write | After a successful response, a caller's subsequent read/claim issued to the same queue owner MUST observe the just-returned effect (the command is applied). This matches `postgres_native` for the operation's own result. |
| Cross-operation lag bound | Other in-flight operations' effects that committed concurrently MAY not yet be applied when an unrelated read returns; such reads are at-least-as-new-as the last operation the reader itself acked. Apply lag between an unrelated command's commit and its visibility to a different reader is bounded by the apply pipeline and MUST NOT exceed the configured commit/apply budget. This is the same eventual-projection property `postgres_native` has for non-transactional read replicas, and it does not affect correctness: ordering, idempotency, and lease assignment are all derived from the committed log, not from read timing. |
| Recovery / new owner | After reassignment, the new owner serves reads only after it has replayed the committed log tail (Recovery), so a read served by the new owner reflects all acknowledged commands. There is no window in which an acknowledged command is invisible to the authoritative owner. |
| No FR-9/FR-12 weakening | Progress age accrues from `eligible_since`/`eligible_at`, which is set when a push command commits and applies; an item is not eligible until then. Commit batching delays *when an item becomes eligible*, not the *rate* at which eligible age accrues, so the queue-global progress bound is unaffected. |

## Replay-Response Idempotency Model (normative)

This backend uses TD-001's **replay-response** option (not transactional response). The rules below
specialize TD-001 §"Durable Ack and Response Replay" for object-log timing.

| Condition | Rule |
|-----------|------|
| Ack timing | A mutating operation MUST NOT return success before its segment's manifest entry is committed (pipeline step 5). Until then the operation is in-flight, not acknowledged. |
| Commit-but-unreturned | If the manifest commits but projection apply, response persistence, or response delivery fails, the operation is committed-but-unreturned. Retrying the same `request_id` MUST converge by locating the committed command (by `command_id`/`request_fingerprint` in the replayed log or projection) and returning the recorded or reconstructed response. |
| Request-id conflict | Retrying the same `request_id` with a different request fingerprint MUST fail with `request-id-conflict` (API-001). The fingerprint is carried in the committed `CommandEnvelope`, so this holds even if the projection has not yet applied the command. |
| Claim replay | `BatchClaim` retry MUST return the same claimed set while leases are active and MUST fail with `request-expired` once all returned leases are no longer active (API-001 claim idempotency). The claim command and its lease assignments are reproduced from the committed log; an un-committed (reserved-only) original claim is not a replay (see "Claim Reservation"). |
| Commit timeout | If the manifest commit cannot complete before the configured commit deadline, the operation MUST return envelope `commit-timeout` (or per-item `unavailable`), and its reservations MUST be rolled back; the caller retries with the same `request_id` and item keys, and accepted items converge (API-001). |

## Queue-Scoped Commands (single owner, normative)

Some commands are queue-scoped — the canonical case is `SetGates` (API-001 / G2). Because the queue is
owned by exactly one node (ADR-008), a queue-scoped command applies on that single owner: it is buffered,
sealed, and manifest-committed on the queue's one manifest, and acknowledged once that single manifest
entry commits. There is **no** cross-shard fan-out, multi-manifest convergence, or partial-visibility
window — a single manifest commit makes the gate flip durable and visible atomically. Idempotency holds
by `request_id`/fingerprint as for any other command; a fenced writer (epoch advanced mid-command) fails
its commit and the new owner re-drives on retry. The same applies to any other queue-scoped command
(e.g. an in-band `PurgeItems` span, G5).

## SQLite Projection (normative mapping)

The SQLite projection MUST represent the TD-001 logical projection records and MUST preserve the column
semantics defined in TD-002 (`pqueue_items`, `pqueue_request_idempotency`, `pqueue_item_key_retention`,
the gate-state table, the `pqueue_cohorts` projection, and the single per-group summary projection from
MF-PROJ). It is the same logical projection as TD-002, materialized in SQLite instead of Postgres — both
are members of the relational projection family (ADR-008), held identical by conformance. This is what
makes conformance parity possible.

Differences from TD-002 that are normative for SQLite:

| Element | Rule |
|---------|------|
| Authority | SQLite is rebuildable, NOT authoritative. `apply_committed` (TD-001 `ProjectionStore`) MUST be the only writer of committed item/lease/metric/gate/cohort rows, and MUST apply commands in `sequence` order exactly once. In-flight claim reservations (see "Claim Reservation") are the ONLY pre-commit state, are non-authoritative, and are never snapshotted. |
| Applied position | The projection MUST persist its highest applied `sequence` (`last_command_sequence`) so apply is idempotent across restarts and replay. |
| Claim transaction | `BatchClaim` candidate selection + reservation MUST occur atomically in a single SQLite write transaction (SQLite serializes writers), implementing the API-001 "Eligibility Precedence" predicate and the unified `ClaimPlan` (TD-001). Durable lease state appears only after `apply_committed` of the committed claim command. No second eligibility definition is introduced (MF4). |
| Group co-residency by construction | Because the queue is the unit of sharding (ADR-008), all items of a `group_key` are in the queue's one SQLite db on its owner, so `whole_group` (G1 `compatibility.group_batching`) and `whole_cohort` (G6 `cohort_policy`) selection are evaluated locally and atomically with no co-residency flag and no routing. `same_group_key` remains an item-level domain filter, NOT `whole_group N=1` (MF1); `whole_cohort` MUST NOT be combined with `same_group_key`/`group_key` (G6). |
| Recurring / rearm / purge | A recurring queue's `rearm` and in-band `PurgeItems` (G5) are ordinary committed commands applied by `apply_committed`: `rearm` releases lease state, sets `not_before` and `eligible_since = max(commit_time, not_before)`, resets per-cycle retry, and bumps `item_version` WITHOUT marking terminal; `PurgeItems` removes the item row (and, with `force`, the lease; a leased item without `force` returns `conflict`), writes a `client_item_key` tombstone, and recomputes the affected `pqueue_group_summary` row — all transcribed from TD-002 semantics, materialized in SQLite, and durable/replayable via the same pipeline. |
| Lease expiry | Lease expiry MUST append a `LeaseExpired` command to the object log before expired items become claimable again (TD-001 / TD-002 parity), preserving the progress-bound clock per FR-11. Lazy expiry in the claim path plus a bounded, epoch-fenced sweeper is the expected implementation. |
| Gate state | The queue's dynamic gate state (`gate_keys`/`SetGates`, G2) is materialized in SQLite as the gate-state table (TD-002 parity: `pqueue_gate_state` + the item→gate-key lookup). TD-004 introduces NO other gate mechanism. |

### Per-group summary projection (MF-PROJ, normative)

This backend maintains exactly ONE per-group summary projection, `pqueue_group_summary` (the reconciled
name from MF-PROJ; the prior `pqueue_active_scope_summary` is folded into it). It is maintained
transactionally with item mutations inside the same SQLite write transaction. Its consistency model and
gate-flip behavior are the canonical G2/MF-PROJ model; TD-004 references that model and does not
introduce another.

| Element | Rule |
|---------|------|
| Logical key | The logical projection grain is `(tenant_id, queue_id, group_key)` (group_key per ADR-004 topology, MF7): one row per `(queue, group_key)`. Because the queue lives on one owner (ADR-008), a group has exactly one summary row and per-group correctness is by construction; there is no cross-shard merge. |
| `oldest_eligible_at` | MUST be authoritative and exact-on-read, derived THROUGH the current gate predicate at read time (never trusted blindly from a possibly-stale row), because the queue's local progress bound (TD-003) depends on it. A gate-blocked item MUST NOT be reported as a group's oldest eligible item. |
| eligible counts | MAY be lagged/approximate; they MUST converge and MUST be documented as approximate where surfaced (API-001 metrics allow documented approximation). Counts MUST NOT be used for a correctness decision (e.g., whole-group/whole-cohort completeness), which re-derives membership under lock. |
| O(1) gate flip | A `SetGates` gate flip (G2) MUST NOT synchronously rewrite every affected group's summary row. The canonical mechanism is G2's `gate_keys`/`SetGates` with gate state and exact-on-read anti-join: `oldest_eligible_at` is derived through the gate state at read/claim time (an anti-join against blocked `gate_keys`), so a flip is O(keys flipped) and never O(items); affected groups' counts reconcile via a bounded background recompute scoped to groups sharing the flipped gate key. TD-004 materializes that model in SQLite; it introduces NO `metadata_blockers`-based or queue-generation gate-flip mechanism of its own. (Static `metadata_blockers` remain the separate, queue-definition-time eligibility condition of Eligibility Precedence; they are not the dynamic gate-flip mechanism.) |

### Cohort projection bindings (G6, normative)

This backend defends `whole_cohort` (G6 `cohort_policy`), not just Postgres/TD-002.

| Element | Rule |
|---------|------|
| SQLite cohort projection | The SQLite projection MUST materialize the same `pqueue_cohorts` logical record as TD-002, logical key `(tenant_id, queue_id, group_key)`, applied only from committed commands by `apply_committed` in `sequence` order. Because the queue is the unit of sharding (ADR-008), every command for a `group_key` lands in the queue's one SQLite db, so the cohort and all its members are local; `whole_cohort` claim, member exclusion, and expiry are evaluated in one SQLite write transaction (SQLite serializes writers, which is the lock unit standing in for the Postgres row lock). |
| Shared lease in SQLite | The cohort lease (`cohort_id`, `cohort_lease_token_hash`, lease expiry) is a projected record; renew/finalize/release/retry are applied as committed commands targeting `cohort_id`. No per-member lease rows are created for cohort members. The selected cohort row MUST be locked first and its completeness + per-member eligibility (Eligibility Precedence conditions 1–5) rechecked under that lock before leasing; a contended or under-lock-failing cohort is skipped, never partially leased (API-001 / G6). |
| Expiry under replay-response | `CohortExpired` MUST be a committed command appended to the object log (the ack boundary) before any member is marked terminal in the SQLite projection, mirroring TD-002. Replay of the log MUST reproduce the exact terminal / `cohort-incomplete` outcome and the `expire_command_pos` ordering. The cohort liveness bound is enforced `<= progress_bound_ms` at `CreateQueue` time (G6); there is no second progress scope. |
| Replay / recovery / snapshot | A snapshot at a committed `sequence` MUST include `pqueue_cohorts` rows (size, member_count, state, `cohort_created_at`, `first_eligible_at`, expire position, lease hash, `retention_until`); recovery = snapshot + log tail MUST reproduce: an in-flight `leased` cohort with its `cohort_lease_token`, a `forming` cohort's `member_count`, and a `terminal` cohort's `retention_until`. A replayed `whole_cohort` claim within the active lease MUST return the same member set + `cohort_lease_token` (idempotency parity with TD-002). |
| Membership / counts | Cohort eligible-age / counts are NOT duplicated in `pqueue_cohorts`; they come from the single `pqueue_group_summary` (MF-PROJ). Recurring items MUST NOT be `whole_cohort` members (recurrence and cohort topology are mutually exclusive, ADR-004 / G5). |

## Snapshots (normative)

| Element | Rule |
|---------|------|
| Trigger | A snapshot MUST be written when applied `sequence` advances by at least `snapshot_interval_commands` OR `snapshot_interval_ms` elapses since the last snapshot, whichever comes first. |
| Consistency | A snapshot MUST be a consistent SQLite image at a single applied `sequence` = `snapshot_sequence` (e.g., via SQLite online backup / `VACUUM INTO` against a read transaction). It MUST record the segment range it covers. In-flight reservations MUST NOT be included. |
| Durability | A snapshot MUST be durably written to object storage and checksummed before it may be used to authorize segment expiry. |
| Schema version | A snapshot MUST record `projection_schema_version`; recovery MUST reject or migrate a snapshot whose schema version it cannot apply, falling back to full log replay. |

## Retention and Expiry (normative)

| Element | Rule |
|---------|------|
| Segment expiry | A segment MAY be deleted only after a committed snapshot fully covers its `sequence` range AND `log_recovery_window_ms` has elapsed past that snapshot's `committed_at` (ADR-001 step 8). |
| Idempotency retention | Request-idempotency and item-key convergence records expire per `request_id_retention_ms` / `client_item_key_retention_ms` (API-001), enforced in the SQLite projection; expired records MUST NOT be required for any non-expired segment's replay. |
| Manifest retention | Manifest entries (including epoch fence records) for expired segments MAY be compacted, but the manifest MUST retain enough tail to validate the active recovery window and the monotonic `sequence`/epoch invariants. |
| Snapshot retention | At least the most recent committed snapshot whose recovery window has not expired MUST be retained at all times so recovery is always possible. |

## Configuration Validation (normative)

| Element | Rule |
|---------|------|
| Reject 1-object-per-command | A backend configuration that would seal one command per segment in production MUST be rejected at queue/backend configuration time with `invalid-request` (API-001). It is available only behind an explicit `dev_unsafe_one_command_segments` flag for tests; that flag MUST NOT be settable in a production deployment profile. |
| Reject missing CAS | If the configured object store lacks a usable conditional-write primitive and the deployment has not selected the Postgres-manifest-pointer fallback, queue/backend configuration MUST be rejected with `invalid-request` (see "Object-Store Capability Requirements"). |
| Window sanity | `segment_max_latency_ms` MUST be `> 0`; the effective claim/ack latency budget MUST be documented to callers because it bounds API-001 commit latency for this profile. |
| Snapshot vs recovery window | `log_recovery_window_ms` MUST be `>= snapshot_interval_ms` so an unexpired snapshot always exists before its covered segments can expire. |

## Commit-Latency / Cost Tradeoff (normative statement)

This profile's durable-commit cost scales with sealed-segment count, not command count (ADR-001 napkin
cost). The deliberate tradeoff: acknowledgement of a mutating operation MAY be delayed up to
`segment_max_latency_ms` (plus segment write + manifest commit time) so that many commands share one
durable object write. This is the lever that produces the S3 cost floor.

API-001 still holds **once a response returns**: the response is derived only from committed command
state (replay-response); per-item outcomes, ordering, idempotency, and lease semantics are identical to
`postgres_native`, and the operation's own effect is applied before its response returns (see "Response
/ Apply Ordering"). The client-visible differences are (1) higher and configurable acknowledgement
latency, and (2) the cross-operation apply-lag bound for *unrelated* concurrent operations' visibility to
*other* readers — both within the bounds API-001 §"Non-Normative Notes" and the Performance section
permit for object-log profiles. There is no weakening of FR-9/FR-12: the progress bound is computed from
`eligible_since`, which is unaffected by commit batching (an item is not eligible until its push command
is committed and applied, and progress age accrues from eligibility, FR-10).

## Security and Tenancy

- Every object key MUST begin with `tenant`, `queue` (TD-001 §Security; no cross-tenant key).
- The principal MUST be authorized against `tenant_id`/`queue_id` before any segment, manifest,
  snapshot, or projection access.
- Object storage MUST support encryption in transit and at rest via the provider; payload/metadata are
  caller data and MUST pass configured size limits.
- Lease tokens are stored only as hashes in the projection (TD-002 parity).
- Threats: a fenced/stale writer extending the log (mitigated by current-epoch validation on manifest
  commit, not manifest-recorded-epoch alone); segment tampering or truncation (mitigated by per-segment
  + per-command checksums validated on replay); cross-tenant access (mitigated by tenant-leading keys +
  authorization); replay/duplicate mutation (mitigated by `request_fingerprint` in the committed
  envelope + `request-id-conflict`).

## Performance

- Inherits PRD scale targets: 10M items in a hot queue, the per-queue throughput floor (TP-002 E0: >=10M items/hr per queue, preserved for every queue at any scale), and queue density (>=1000 concurrently active queues per node, TP-002 E2).
- **Queue density (>=1000 active queues/node).** A node hosts the SQLite projections for many queues at once. Per-queue SQLite databases MUST be opened lazily and bounded by an LRU (or equivalent) cap on open handles/memory, NOT held open per owned queue indefinitely; idle queues are closed and reopened on demand. Group-commit batching, the lease-expiry sweeper, snapshotting, and retention/expiry MUST run as bounded shared per-node jobs across many queues per pass, never one loop/task/connection per queue. A node MUST sustain >=1000 concurrently active queues with each meeting its progress bound and any one able to reach the per-queue floor; aggregate single-node throughput is bounded by the node (multi-node provides headroom). Validated by `queue_density_single_node_tests` (TP-002 E2).
- Acknowledgement latency intentionally includes group-commit time and is governed by
  `segment_max_latency_ms` + write + manifest-commit latency. p95/p99 ack targets for this profile are
  stated relative to the configured window, not the sub-second small-commit target of `postgres_native`.
- SQLite claim/lease/metric/gate reads MUST use indexes equivalent to TD-002's required indexes so a
  10M-item queue does not full-scan for claim, lease expiry, idempotency, gate anti-join, or progress
  metrics.
- Snapshot + log-tail replay time at 10M-item queue scale MUST be measured (Testing) and MUST bound
  recovery time; `snapshot_interval_*` is tuned against the measured replay rate.
- Telemetry overhead MUST be included in performance tests.

## Testing

### Completion Evidence (pre-ADR-008 build record)

> **Build-record note (ADR-008 simplification).** The completion evidence below records the
> `object_log_sqlite_projection` backend **as built** prior to the ADR-008 "queue is the unit of
> sharding" simplification — it used the earlier intra-queue-shard model (per-`(queue,shard)` manifests
> and cross-shard command convergence). The durable object-log substrate it validated — group-commit
> segments, manifest CAS, current-epoch fencing, in-flight claim reservation, snapshot + log-tail
> recovery, cohort/recurring replay — **carries forward unchanged** under the per-queue model; only the
> intra-queue-shard partitioning and the cross-shard command binding are retired as targets. Re-scoping
> the built code to the per-queue manifest/key layout is a later build phase (it is doc-only here). This
> record is preserved as the historical PHASE-7 build attestation, not as the current per-queue target.

As of 2026-06-16, the v1 `object_log_sqlite_projection` implementation is
complete for the committed pqueue backend profile and is validated against the
freestanding object-log abstraction plus SQLite projection. The durable
offset→location index is provided by object-log's own `ManifestSequencer`
(blob-persisted, rebuilt on reopen); `pqueue-objectlog` depends only on the
freestanding `object-log` crate and no longer on fjord's internal coordinator.
The validation boundary is:

- `cargo +1.92.0 test -p pqueue-service local_object_log_deployment_smoke_tests -- --ignored --nocapture`
  passes the local object-log deployment profile.
- `cargo +1.92.0 test -p pqueue-objectlog object_log_commit_recovery_tests -- --nocapture`
  passes group commit, replay, epoch fencing, object-store capability rejection,
  and Postgres manifest-pointer fallback checks.
- `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection PQUEUE_E2E_SCALE=smoke PQUEUE_E2E_SEED=1801 cargo +1.92.0 test -p pqueue-service --test product_workflows -- --ignored --nocapture`
  passes all nine product workflows and emits verification-ledger rows validated
  by `pqueue-verify-ledger --strict`.
- `bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3 --tp002-e0e1-source pqueue-7e2b3132 --tp002-e2-source pqueue-9afd88cc,pqueue-76d92a33 --tp002-e3-source pqueue-b1abd895,pqueue-472a09d4`
  passes from source-backed DDx evidence and regenerates the aggregate
  `product_validation_tests` ledger.

This proves the backend contract at the object-log layer used by pqueue.
Provider-specific hardening against a live cloud S3 endpoint remains a deployment
certification activity unless a future bead adds a concrete S3 adapter and
credentials-backed acceptance run. That future activity must not be cited as a
blocker for the current v1 profile unless the release claims provider-specific
S3 support rather than S3-compatible semantics through the freestanding
object-log.

### Required evidence surface (per-queue target)

The following cases define the required evidence surface:

- Group-commit ack boundary: a command is NOT acknowledged until its manifest entry commits; kill the
  writer after segment write but before manifest commit and prove the command is NOT acked and is safely
  re-driven by `request_id`.
- Reject 1-object-per-command: configuration is rejected outside the dev-unsafe flag.
- Object-store CAS capability: a store without conditional write is rejected OR runs the
  Postgres-manifest-pointer fallback; the fallback still enforces single-writer fencing.
- Current-epoch fencing: a writer holding epoch E whose queue was reassigned to E+1 in the control plane
  (WITHOUT the new owner having yet written a data manifest entry) MUST fail its manifest commit, discard
  its buffer, and roll back reservations; the new epoch holder reproduces acknowledged state. (This is
  the specific case manifest-recorded-epoch-only CAS would have wrongly allowed.)
- Duplicate-claim prevention during segment buffering: two concurrent `BatchClaim`s on overlapping
  candidates while the first claim's segment is pending MUST NOT both reserve the same item; CAS failure
  / timeout / fence rolls the reservation back and the item returns to the candidate pool; a writer crash
  before manifest leaves no durable lease; retry by `request_id` converges.
- Snapshot + log-tail recovery: restore latest snapshot, replay segments with `sequence > snapshot_sequence`,
  validate checksums, reproduce projection state (TD-001 conformance: snapshot recovery).
- Safe segment expiry: a segment is deletable only after a covering committed snapshot + recovery window;
  prove no expired segment is ever required for an in-window recovery.
- Replay-response idempotency: same `request_id` converges to the recorded/reconstructed response;
  different fingerprint → `request-id-conflict`; claim replay returns same lease set, then `request-expired`.
- Response / apply ordering: an operation's own committed effect is visible to the same caller's
  immediate follow-up read (self read-after-write); an unrelated reader's apply-lag for a concurrent
  command is bounded by the configured budget; a new owner serves reads only after replaying the log tail.
- Queue-scoped command (`SetGates`): a gate flip is committed on the queue's single manifest and is
  durable+visible atomically on its commit; a writer fenced mid-command (epoch advanced) fails its commit
  and the new owner re-drives by `request_id`. (No cross-shard partial-visibility case exists under the
  per-queue model.)
- Per-group summary (MF-PROJ): logical key is `(tenant_id, queue_id, group_key)`;
  `oldest_eligible_at` is exact-on-read through the gate anti-join; counts may lag and converge; a
  `SetGates` gate flip does not rewrite every group row yet leaves oldest-eligible authoritative (G2 model).
- Group co-residency by construction: `whole_group` (G1 `compatibility.group_batching`) and
  `whole_cohort` (G6 `cohort_policy`) and `same_group_key` resolve owner-locally and atomically;
  `same_group_key` behaves as an item-level domain filter, not `whole_group N=1` (MF1); `whole_cohort`
  rejects combination with `same_group_key`/`group_key` (G6).
- Cohort projection (G6): an atomic complete-cohort claim is never split or double-leased across a writer
  restart; a cohort member is never individually claimable while a sibling is non-terminal; `CohortExpired`
  precedes any claimability change in replay; a duplicate `client_item_key` push does not increment
  `member_count` after replay; `group_key` reuse after retention yields a new `cohort_id`; a replayed
  `whole_cohort` claim within the active lease returns the same member set + `cohort_lease_token`.
- Recurring / purge (G5): `rearm` and in-band `PurgeItems` are durable/replayable through the pipeline;
  rearm replay is deterministic with `eligible_since = max(commit_time, not_before)` and per-cycle retry
  reset; a `PurgeItems` purge is queue-local (one owner) and idempotent by `request_id`.
- Conformance parity: this backend passes the SAME TD-001 shared backend conformance suite as
  `postgres_native`, including the object-log–specific rows added to TD-001.
- **Scale/cost evidence (D4(d)):** the object-log scale/cost + recovery evidence record is TP-002 **E3**
  (object-log cost/ack + recovery), measured against the per-queue throughput floor TP-002 **E0**
  (>=10M items/hr per queue, preserved for every queue at any scale). E3 MUST report sustained items/hr
  at or above the E0 floor, ack-latency distribution at the configured window,
  durable-commit cost per million commands, and 10M-item recovery (snapshot + replay) time. A recurrence
  scale row (G5) runs under this profile as well. (Evidence-record IDs E0–E3 are owned by TP-002 / G8;
  TD-004 uses them, it does not mint new evidence IDs.)

## Migration & Rollback

- Backend profile is per-queue configuration (TD-001). A queue may be created on
  `object_log_sqlite_projection` after this backend passes the TD-001 conformance suite.
- Rollback: disable the profile for new queues; existing queues stay on their last known-good backend
  until a migration/repair design exists (P1). No in-place backend migration is defined here.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Ack latency unacceptable for some callers | M | M | Document the window as a first-class knob; steer latency-sensitive queues to `postgres_native`. |
| SQLite projection diverges from durable log | M | H | Apply only committed commands in `sequence` order with persisted applied position; reservations are non-authoritative; conformance replay + snapshot tests. |
| Object store lacks/weak conditional write | M | H | CAS is a required capability: reject the queue or run the Postgres-manifest-pointer fallback (current-epoch CAS in the control plane); document supported stores. |
| Stale-epoch writer commits after reassignment | M | H | Manifest commit validates against the CURRENT control-plane epoch (or epoch fence published to manifest before handoff), not manifest-recorded epoch alone; conformance test. |
| Snapshot/replay too slow at 10M items | M | M | Measure replay rate; tune `snapshot_interval_*`; bound recovery window. |
| Incomplete/gated cohort stalls atomic claim or recovery | M | H | Cohort row locked + rechecked under lock before leasing; `CohortExpired` linearized before terminal apply; liveness bound enforced `<= progress_bound_ms`; conformance replay test. |
| Segment expiry deletes state still needed for recovery | L | H | Gate expiry on committed-snapshot coverage + recovery window; conformance test. |

## Review Checklist

- [x] TD-001 traits map to object-log segments + SQLite projection + snapshot/replay + Postgres control plane (`pqueue-objectlog`, `pqueue-sqlite`, `local_object_log_deployment_smoke_tests`).
- [x] API-001 operations are preserved once a response returns; self read-after-write and bounded apply behavior are covered by product workflows and apply-before-return object-log tests.
- [x] Ack occurs only after durable manifest commit (replay-response); operation's own effect is applied before return (`object_log_commit_recovery_tests_reopens_from_object_log_blob`, request-id replay tests).
- [x] In-flight claim reservation prevents duplicate local claims; rollback/crash behavior is covered by object-log recovery and product crash-recovery workflows.
- [x] Manifest commit validates the current control-plane (queue) epoch, not only manifest-recorded epoch (`object_log_commit_recovery_tests_current_epoch_fences_stale_writers` and reopen-before-data-commit coverage).
- [x] Object-store conditional write is required; unsupported stores reject or use the Postgres manifest-pointer fallback (`object_log_commit_recovery_tests_rejects_missing_cas_without_fallback`, `object_log_commit_recovery_tests_postgres_manifest_pointer_fallback_keeps_epoch_fence`).
- [x] Queue-scoped commands (`SetGates`) commit on the queue's single manifest — durable+visible atomically, no cross-shard convergence (ADR-008). (Prior-build `storage_conformance_multi_shard_tests` are retired as a target; the single-owner gate path is covered by product workflow gate rows.)
- [x] Single per-group summary projection logical key `(tenant_id, queue_id, group_key)`; oldest-eligible is authoritative and counts may lag (`sqlite_projection_tests`, service metrics ground-truth tests).
- [x] Gate flips use the G2 `gate_keys`/`SetGates` model plus exact-on-read anti-join; no second gate mechanism (`service_gate_tests`, storage gate conformance).
- [x] Cohort (`pqueue_cohorts`) projection + shared lease + `CohortExpired`-before-terminal + replay parity are materialized in SQLite and covered by product callback cohort workflows.
- [x] One-object-per-command is rejected in production (`object_log_commit_recovery_tests_rejects_production_one_object_per_command_config`).
- [x] Group co-residency by construction makes `whole_group` / `whole_cohort` owner-local; `same_group_key` stays item-level (`product_workflow_marketo_group_batching_e2e`, `product_workflow_callback_cohort_e2e`).
- [x] Recurring `rearm` / in-band `PurgeItems` ride the pipeline as ordinary durable/replayable commands (`product_workflow_jobs_connectors_recurring_e2e`, recurrence/purge suites).
- [x] Eligibility uses the single API-001 Eligibility Precedence subsection (`core_eligibility_precedence_tests`, service/product workflow coverage).
- [x] Conformance parity with `postgres_native` is covered by the TD-001 shared suite and object-log product smoke matrix.
- [x] Scale/cost evidence uses TP-002 E3 vs E0 through source-backed release-gate beads (`pqueue-b1abd895`, `pqueue-472a09d4`, `pqueue-7e2b3132`); TD-004 mints no new evidence IDs.
