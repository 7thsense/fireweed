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
    self_hash: 8bacf6c79d2e3b82ee35cf5be4528818f720eed51bf9cfcbe200de48fc373caa
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
      adr-rust-workspace-and-toolchain-policy: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      concerns: 73756937e564b8120ca99407bacbd1fa67a06c6021a822c2cb321f7c9d95056e
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
      td-postgres-native-reference-mode: b58232f3c0b56c50bc1e5f01e13afc71ed1c333987498bbabc88c322f80b36e0
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
      td-storage-architecture-backend-contracts: 53b17202dcf527948da8d8508639ba6077197c7fd2df1e9888833ca69a9f9f2f
    reviewed_at: "2026-07-18T21:09:02Z"
---

# Technical Design: TD-004 S3 Object-Log + SQLite Projection Mode

**Contract**: API-001 | **ADR**: ADR-001, ADR-004, ADR-008 | **Depends on**: TD-001, TD-002, TD-003 | **Scope**: object-log local-projection backends

## Scope

This technical design defines the object-log local-projection profiles for
pqueue. In these modes an S3-compatible object store is the durable command log,
a local in-memory, SQLite, or hybrid projection serves hot queue operations, the
same object store holds periodic projection snapshots where configured, and
Postgres remains the control plane. `object_log_inmemory_projection` is the fast
local replay profile; `object_log_sqlite_projection` is the larger rebuildable
local-index profile; `object_log_hybrid_projection_strict` / runtime
`objectlog/hybrid-strict` is the SQLite-first plus hot-memory projection
profile; and `object_log_hybrid_projection_async` / runtime
`objectlog/hybrid-async` is the manifest-committed plus hot-memory
success-barrier profile whose SQLite projection may lag. Per ADR-008 the queue
is the unit of sharding:
a whole queue is owned by exactly one node, so the object log, the manifest, and
the local projection are all **per-`(tenant, queue)`**, and there is no
intra-queue sharding or cross-shard command machinery.

This backend exists to substantiate pqueue's horizontal-scale and cost claims with a profile whose
durable-commit cost scales with *segments*, not with *commands* (see ADR-001 napkin cost comparison).
Horizontal scale is **cross-queue** (ADR-008): many queues across many owners.
This is the cost-optimized counterpart to the latency-optimized
`postgres_native` reference mode (TD-002), and the profile that should deliver
Redis-level hot serving behavior with object-store durability when callers can
batch mutations. SQLite is the relational projection family's log-bearing
member; the in-memory projection is the log-replay serving member.

In scope:

- Group-commit pipeline: per `tenant/queue` command buffering, segment sealing with checksums
  and monotonic command positions, segment write, manifest commit, ack-eligibility, and local projection apply.
- The in-flight claim **reservation** model: how `BatchClaim` selects candidates, prevents duplicate
  local claims while a segment is pending, and rolls back on CAS/timeout/fence (see "Claim Reservation").
- Object layout for segments, manifests, and snapshots (logical; exact byte framing in implementation).
- Replay-response idempotency model (ack only after durable manifest commit) and the read-after-write
  / apply-ordering contract that keeps API-001 satisfied once a response returns.
- The backend-independent transaction contract: success is durable and visible,
  structured rejection has no committed effect for the rejected scope, and
  unknown outcomes resolve by `request_id`.
- SQLite projection schema mapping from TD-001 logical projection records and TD-002 column semantics.
- Hybrid projection semantics for `objectlog/hybrid-strict` and
  `objectlog/hybrid-async`: hot in-memory reads and validation, explicit success
  barriers, `ProjectionImage` hydration before returning SQLite high-water,
  strict-mode poison-on-memory-apply failure, async-mode projection lag, and
  durable request-id replay.
- Periodic SQLite snapshot to object storage at a committed log position.
- Bounded replay and recovery: snapshot + log-tail, with safe segment expiry.
- Manifest-commit epoch fencing validated against the epoch authoritative at the commit linearization point:
  guarded control-plane/CAS-row epoch for implementation (a), or storage-head epoch for implementation (b).
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

The object-log local-projection profile is a **replay-response** backend (TD-001 §"Durable Ack and Response
Replay"). The durable commit boundary is a committed manifest entry that names a sealed, checksummed
segment in object storage. No command is acknowledged before its segment's manifest entry is durably
committed. After commit, commands are applied to the local projection that serves claim planning,
lease state, idempotency lookup, and metrics. The projection is rebuildable:
snapshot + log tail reproduce acknowledged state after node loss.

This backend deliberately trades acknowledgement latency for cost: durable-commit cost scales with the
number of sealed segments, not with the number of commands, so large client
batches plus a configured group-commit window yield the cost floor described in
ADR-001. The operator-facing commit-latency bound is implemented by
`segment_max_latency_ms` and related size thresholds. Lower values reduce
mutation latency and increase object-store request cost; higher values improve
batch density and increase mutation latency. Small, latency-sensitive commits
should use `postgres_native` (TD-002) or a fast log backend instead.

It follows TD-001's capability boundaries unchanged:

- `ControlPlaneStore`: Postgres (queue defs, queue-owner assignment + `assignment_epoch`, backend
  profile). Identical to TD-002's control plane; not re-specified here. The control-plane seam is
  pluggable (ADR-008); the object-store control plane — the object log providing per-queue multi-node
  fencing and coordination via its manifest-CAS series — is committed direction (ADR-008 §4) whose
  acquire→fence atomicity proof has not yet landed, so in v1 this backend still uses the Postgres
  control plane. TD-004 *reads* the current `assignment_epoch`
  from it on the manifest-commit path (see Epoch Fencing).
- `LogStore`: S3-compatible object log with group-commit sealed segments and a per-queue manifest.
- `ProjectionStore`: local in-memory or SQLite, rebuildable, applied only from
  committed commands. In-memory is the log-replay serving family; SQLite is the
  relational projection family's log-bearing member.
- `SnapshotStore`: S3-compatible object storage holding SQLite snapshots at committed positions.

**Key decisions**

- **Manifest entry is the ack boundary.** A command is durable when, and only when, the manifest entry
  naming its segment is committed via a conditional (compare-and-set) object write. Success remains illegal
  until the operation's accepted effects are also visible through the local projection or equivalent
  committed response state.
- **Fencing is enforced at commit linearization.** The manifest commit is the enforcement point of TD-001
  `append_batch(expected_epoch)`. The CAS guards the manifest tail. Implementation (a) validates the guarded
  control-plane/CAS-row epoch; implementation (b) validates the storage-head epoch after CP has stopped new
  admission with non-serving `PendingFence`. This `assignment_epoch` is the same `u64` queue epoch TD-003
  allocates and threads through `CommandPosition.backend_epoch`.
- **SQLite is a projection, never an authority.** `apply_committed` is the only writer of committed
  state; in-flight claim reservations are a separate, non-authoritative bookkeeping table that holds no
  acknowledged state (see "Claim Reservation"). Acknowledged state survives via object-store segments +
  snapshots, never via local disk alone (ADR-001 Option 4 rejection).
- **Hybrid mode is explicit.** In `objectlog/hybrid-strict`,
  `HybridProjectionStore::apply` MUST call SQLite batch apply first and then
  apply the same positions and commands to `InMemoryProjection`. In
  `objectlog/hybrid-async`, manifest commit plus synchronous memory apply/render
  is the success barrier and SQLite apply may lag. All hot reads and pre-commit
  validation delegate to memory. Strict mode poisons on SQLite-commit then memory
  apply failure; async mode resolves pre-memory-render failures as
  unknown-outcome by `request_id` and retries lagging SQLite apply from the log.
- **Object log remains the authority.** Local SQLite under
  `objectlog/hybrid-strict` or `objectlog/hybrid-async` is a restart
  accelerator and durable projection image, not a command authority and not, by
  itself, permission to expire object-log segments.
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
| 1. Buffer | Commands MUST be buffered per `tenant/queue`. Before dispatch, the adapter serializes each `CommandEnvelope` exactly once, validates a conservative resident-peak charge, and acquires a non-cloneable byte permit covering the retained record and temporary seal-frame copy. Each independently admitted request reserves the fixed 25-byte frame overhead; co-batching MAY therefore overcharge the one merged frame but MUST never undercharge it. The same bytes move through coordinator and segment buffer in arrival order. Because the queue is the unit of sharding (ADR-008), every command for the queue — including every member of a `group_key` — lands in the one queue buffer on the queue's owner, so `whole_group` (G1 `compatibility.group_batching`) and `whole_cohort` (G6 `cohort_policy`) claims are owner-local by construction. |
| 2. Seal | A segment MUST seal when EITHER the buffered byte size reaches `segment_target_bytes` OR the oldest buffered command's age reaches `segment_max_latency_ms`, whichever comes first. Sealing assigns each command a monotonic per-queue `sequence` (TD-001 `CommandPosition.sequence`) contiguous with the prior segment, and computes a per-segment `checksum` plus per-command `checksum` (TD-001 `CommandEnvelope.checksum`). |
| 3. Write segment | The sealed, immutable segment MUST be written to object storage under a deterministic key (see "Object Layout") before any manifest commit references it. The write SHOULD use an idempotent PUT keyed by `(queue, first_sequence)` so retried writes do not create divergent objects. |
| 4. Commit manifest | A manifest entry naming the segment, its `[first_sequence, last_sequence]` range, its checksum, and the writer's `assignment_epoch` MUST be appended via a conditional write that succeeds only if (a) the manifest's tail still equals the writer's expected tail AND (b) the writer's `assignment_epoch` is the **current** epoch for the queue (see "Manifest Commit and Epoch Fencing"). A failed CAS MUST abort the commit, roll back the in-flight reservation, and the writer MUST treat itself as raced or fenced. |
| 5. Ack-eligibility | Only after the manifest entry is durably committed MAY the commands in that segment become eligible for acknowledgement. Manifest commit alone is not permission to return success before the operation's own visibility barrier is satisfied. |
| 6. Apply / response barrier | After ack-eligibility, committed commands MUST be applied to the local projection in `sequence` order, exactly once, idempotently keyed by `last_command_sequence` (no command at or below the projection's applied position is reapplied), or the operation response MUST otherwise be reconstructed from committed log state. The operation's own accepted effects MUST be externally visible before success returns. |
| 7. Snapshot | The writer MUST periodically snapshot the SQLite projection to object storage at a committed log position (see "Snapshots"). |
| 8. Expire | A log segment MAY be expired (deleted) only after a committed snapshot covers its entire `[first_sequence, last_sequence]` range AND the configured `log_recovery_window_ms` past that snapshot has elapsed (see "Retention and Expiry"). |

`rearm` (recurring items, G5) and in-band `PurgeItems` (G5) are ordinary commands in this pipeline: they
are buffered, sealed, manifest-committed, acked, and applied like any other mutating command, and gain
durability and replay parity for free. No special-case path is required (see "SQLite Projection").

### Buffered-byte admission (normative)

`objectlog_buffered_bytes_global` is a positive node-wide hard cap and MUST be at least
`segment_target_bytes`. An optional positive `objectlog_buffered_bytes_tenant` hard share MUST be no larger
than the global cap. Unconfigured tenants share otherwise-unused global capacity; strict partitioning is not
implied. `objectlog_buffered_bytes_queue_waiting` bounds admitted bytes parked behind one queue driver and
MUST be positive and no larger than the global cap. Request-count capacity and `segment_target_bytes` remain
independent controls.

One request whose peak charge exceeds its applicable hard cap is permanently rejected as
`EngineError::RequestTooLarge` / `invalid-request`. Temporary budget or queue-waiting exhaustion is
`EngineError::Backpressure` / retryable unavailable. The finite production policy rejects immediately on
exhaustion. `AsyncComposedBackend` raw submission MUST run its generic strategy preparation before queue
gating/dispatch. A service MAY select async waiting there and race the composed future with its runtime-owned
deadline; the direct replay-committer fallback uses finite `Reject` inside its owned task. No path holds a
queue gate while waiting. Cancellation deregisters
the waiter without charge. After acceptance, the permit follows the serialized records through repair, seal,
epoch/watermark fencing, manifest CAS success or loss, and projection apply. It releases only when the final
resident records/frame are gone. Shutdown closes waiters and drains accepted permits to zero.

Low-cardinality telemetry MUST expose configured global/tenant limits, current and peak charged bytes,
waiting requests, wait/reject counts, and adapter-measured wait duration. Tenant and queue IDs MUST NOT be
metric labels; tenant attribution is limited to rate-limited diagnostics. Every live object-log profile emits
these fields, including total/max wait nanoseconds and the queue cap, on its opt-in `[seg]` debug telemetry
line; the snapshot formatter is shared with focused visibility tests.

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
durable log. The TD-003 safety invariant is evaluated at the storage commit linearization point. A control-
plane `PendingFence(E+1)` is a non-serving reservation, not yet storage authority: an operation already
admitted under `Assigned(E)` MAY finish and linearize before the storage fence. New routing/admission is
disabled while pending, and every `E` commit attempt after the storage head advances to `E+1` MUST be rejected.

| Element | Rule |
|---------|------|
| Epoch source | New routing/admission requires a serving TD-003 `Assigned(E)` lease. Work already admitted under that lease carries `E` through commit; it MAY finish while CP records non-serving `PendingFence(E+1)`, but only before storage authority advances. A node MUST NOT admit new work from a pending or absent lease. |
| Current-epoch validation (the core fix) | Manifest commit MUST succeed only when the writer's `assignment_epoch` equals the epoch authoritative at that commit's linearization point. Under implementation (a), that is the guarded CP/CAS-row epoch. Under implementation (b), that is the manifest/storage head epoch; CP `PendingFence` alone is only a reservation. The new owner cannot serve until the storage fence, hydration, and CP confirmation finish. TWO conformant implementations are permitted, and a backend MUST use at least one: |
| — (a) epoch-on-commit check | The committing writer validates its `expected_epoch` against the serving control-plane/CAS-row epoch as part of the commit. Publishing the guarded epoch immediately prevents an older commit from linearizing afterward. |
| — (b) epoch fence published to manifest before serving handoff | CP first enters non-serving `PendingFence(E+1)`, stopping new old-owner admission. Storage then publishes `E+1` into the manifest head before the new owner hydrates or serves. An already-admitted `E` operation may linearize before that storage CAS; every `E` attempt after it loses to the higher head. Recovery MUST publish/confirm the storage fence before hydration and CP `Assigned(E+1)`. |
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
| Authority boundary | SQLite holds NO acknowledged lease until `apply_committed` runs (pipeline step 6). Reservations are a separate, non-authoritative, in-memory-or-local bookkeeping state (e.g., a `pending_reservations` table or in-process index) that records "these item rows are tentatively claimed by pending command `command_id` at epoch E." It is rebuilt from the in-flight buffer on restart and is never snapshotted. Mutable-write commands do not rely on this table for closure; they are revalidated against the hot projection and committed claim state during apply, in the same unit of work that releases success. |
| Select + reserve atomically | A `BatchClaim` MUST, in one SQLite write transaction: (1) evaluate the API-001 Eligibility Precedence predicate and the unified `ClaimPlan` to select candidate items, (2) mark those items reserved against the new claim `command_id`, and (3) append the claim command to the pending segment buffer. Reserved items MUST be excluded from candidate selection by any concurrent claim, so no duplicate local claim can occur while the segment is pending. (SQLite serializes writers, making (1)–(3) atomic.) |
| Commit → promote | When the segment's manifest entry commits and `apply_committed` applies the claim command, the reservation is promoted to a durable lease (lease row written, reservation cleared). Only then is committed lease state exposed to reads/metrics. |
| Roll back on CAS/timeout/fence | If the segment's manifest CAS fails (fenced or raced), the commit deadline elapses (`commit-timeout`), or the writer is fenced, the writer MUST clear the reservations for that command so the items return to the candidate pool. No lease is created and no ack is returned; the caller retries by `request_id` (claim idempotency). |
| Crash before manifest | If the writer crashes after reserving but before the manifest commits, the reservation is lost (it was never durable) and the claim command is lost with the unacked buffer. The new epoch holder recovers from snapshot + committed log; the un-acked claim simply never happened, and the caller's retry by `request_id` either re-claims or returns `request-expired` per API-001 claim idempotency. |
| Reservation TTL | Reservations MUST have a bound tied to the commit deadline so a stuck writer cannot pin items indefinitely; a reservation older than the commit deadline MUST be reclaimable. |
| Idempotent re-claim | A retried claim `request_id` whose original command did commit MUST converge to the same lease set (replay-response, "Replay-Response Idempotency Model"); a retry whose original never committed is a fresh claim subject to current eligibility. |

## Mutable-write race closure (normative)

`objectlog/hybrid-strict` and `objectlog/hybrid-async` MAY admit
`replace_if_pending`, `update_fields`, and `reschedule` only when the command's
apply phase performs deterministic re-validation against the current hot
projection in the same committed unit of work that makes the response visible.
The pre-commit hot read is a fast path, not the closure mechanism. The
deterministic apply step re-checks the target item after any concurrent claim
reservation has been materialized, so a command that loses the race fails
closed, never acks, and replays to the same rejection. This is the group-commit
buffered-window closure for mutable writes.

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

## Hybrid Mode Taxonomy, Success Barriers, and Poisoning (normative)

`objectlog/hybrid` is a taxonomy prefix, not a complete contract. Implementations
MUST select one of the named modes below and surface that mode in configuration,
telemetry, verification ledgers, and release notes.

| Mode | Success barrier | SQLite projection role | Failure semantics |
|------|-----------------|------------------------|-------------------|
| `objectlog/hybrid-strict` | Manifest commit + durable SQLite apply + synchronous memory apply/render for the operation's own result | SQLite is on the response path and is the owner-local restart accelerator/high-water source; the hot memory image is current before success returns | SQLite apply failure returns no success; SQLite commit followed by memory apply failure poisons the store; upsert/update/reschedule race closure is conformance-gated through deterministic apply-time re-validation |
| `objectlog/hybrid-async` | Manifest commit + synchronous memory apply/render for the operation's own result | SQLite is an asynchronous projection fed from the committed object log and MAY lag behind memory; the hot memory image is current before success returns | SQLite lag/failure after success is retried from the object log; memory apply/render failure before success produces an unknown-outcome retry path; upsert/update/reschedule race closure is conformance-gated through deterministic apply-time re-validation |

Both modes use the same manifest ack boundary and group-commit pipeline as the
other object-log profiles. The difference is which projection applies are inside
the success barrier.

For mutable item changes, the ban is profile-specific rather than universal:
`objectlog/hybrid-strict` and `objectlog/hybrid-async` MAY admit
`replace_if_pending`, `update_fields`, and `reschedule` because TD-004's
mutable-write closure runs deterministic apply-time re-validation in the same
ack-after-apply unit of work. Pure lagging-projection log-then-apply profiles
remain unable to close the same race and must keep the
`-ERR pqueue unavailable` behavior.

### `objectlog/hybrid-strict` apply path

`objectlog/hybrid-strict` has two ordered projection phases:

| Element | Rule |
|---------|------|
| SQLite-first apply | `HybridProjectionStore::apply` MUST durably apply the complete sealed batch to `SqliteProjectionStore::apply_committed_batch` before touching memory. Memory is never ahead of SQLite for an acknowledged command. |
| Memory apply | The same positions and command envelopes MUST then be applied to `InMemoryProjection` before the operation returns success. Reads, claim selection, metrics, secondary-index lookup, live-item lookup, and pre-commit validation MUST use the in-memory projection. Because the in-memory projection is current on the response path, mutable item changes (`replace_if_pending`, `update_fields`, and `reschedule`) MAY be admitted when the backend also proves the claim-race conformance cases below. |
| SQLite failure | If SQLite apply fails, no success response is returned. Recovery replays the object-log tail beyond the prior SQLite high-water. |
| Poisoned gap | If SQLite commits and the memory apply fails, the store MUST mark itself poisoned. The current operation returns `EngineError::Storage`; subsequent reads, validation, and writes fail closed with storage error; and the process must restart to hydrate memory from SQLite before serving. |
| No lazy divergence | A poisoned hybrid store MUST NOT continue serving with memory behind SQLite, even for read-only methods. |

### `objectlog/hybrid-async` apply path

`objectlog/hybrid-async` keeps the API success path hot without weakening the
durable-ack contract. Success is legal only after:

1. the command's object-log segment manifest entry is durably committed;
2. the command is synchronously applied to `InMemoryProjection`; and
3. the response is rendered from the in-memory projection or reconstructed from
   the committed command state.

SQLite apply MAY lag after success, but it MUST consume the committed object-log
sequence in order, exactly once, and persist its applied high-water. Reads, claim
selection, metrics, secondary-index lookup, live-item lookup, pre-commit
validation, and response rendering MUST use memory, not the lagging SQLite
projection. SQLite lag MUST be observable and bounded by the configured
`hybrid_async_sqlite_apply_lag` budget. If lag exceeds that bound or SQLite apply
cannot make progress, the implementation MUST fail recovery/high-water claims
closed and continue serving only when the object-log replay path can prove the
hot memory image is complete.

For `objectlog/hybrid-async`, a crash after manifest commit but before memory
apply/render is an unknown-outcome, not a success. Retrying the same
`request_id` resolves against the committed log: if the original command
committed, the retry returns or reconstructs that committed result; if no command
committed, the retry is a fresh attempt subject to current validation.

#### Async apply debt, backpressure, and poison thresholds (`objectlog/hybrid-async`, normative)

Async SQLite apply debt is a bounded runtime condition, not an unbounded
implementation detail. The implementation MUST expose the following metrics per
`(tenant_id, queue_id)` and MUST include max and p99 values in release evidence:

| Metric | Definition | Gate |
|--------|------------|------|
| `hybrid_async_sqlite_apply_lag` | Number of committed command sequences covered by the manifest and memory image but not yet covered by `sqlite_high_water`. | MUST remain `<= hybrid_async_sqlite_apply_lag_max_commands` and `<= hybrid_async_sqlite_apply_lag_max_ms` under steady load. |
| `hybrid_async_apply_debt_bytes` | Approximate retained object-log bytes that cannot be trimmed because async apply has not advanced `sqlite_high_water` or lineage validation is incomplete. | MUST remain below the configured per-queue debt budget; exceeding it enters backpressure. |
| `hybrid_async_apply_queue_depth` | Number of sealed segment batches waiting for async SQLite apply in `batch_sequence` order. | MUST remain bounded by the configured batch backlog budget. |
| `hybrid_async_oldest_unapplied_age_ms` | Age of the oldest committed command whose sequence is greater than `sqlite_high_water`. | MUST remain within the configured apply-lag time budget. |
| `hybrid_async_apply_retry_count` | Consecutive failed SQLite apply attempts for the same batch or command range. | MUST trip poison after the configured poison threshold. |

Backpressure MUST be applied before async apply debt can invalidate recovery or
retention assumptions:

| Condition | Required behavior |
|-----------|-------------------|
| Soft debt threshold | When `hybrid_async_sqlite_apply_lag`, `hybrid_async_apply_debt_bytes`, queue depth, or oldest-unapplied age crosses 75% of its configured limit, the backend MUST emit warning telemetry and prefer flushing/apply work over accepting more group-commit backlog. |
| Hard debt threshold | When any debt metric reaches its configured limit, the backend MUST fail new mutating operations for that queue with a retryable storage/backpressure error until ordered SQLite apply reduces debt below the clear threshold. Reads may continue only from memory while lineage proves memory completeness. |
| Recovery/high-water backpressure | While hard debt is active, `recovery_high_water` MUST NOT advertise the lagging `sqlite_high_water` as a safe replay skip point. Owner-local restart MUST replay from the last validated authoritative source. |
| Retention backpressure | Segment expiry MUST stop advancing whenever async apply debt is over budget, lineage validation is incomplete, or the SQLite worker is failed; local `sqlite_high_water` alone never authorizes deletion. |
| Clear threshold | Backpressure clears only after all debt metrics are below 50% of their configured limits and the SQLite worker has completed at least one ordered batch without retry. |

Poison is reserved for states that cannot be repaired by simply waiting for the
async worker:

| Poison threshold | Required behavior |
|------------------|-------------------|
| Repeated apply failure | If `hybrid_async_apply_retry_count` reaches the configured poison threshold for the same batch, the local projection MUST be marked poisoned; new reads, validation, writes, export, and high-water claims fail closed until repair or restart replays from an authoritative object-log source. |
| Non-contiguous apply | A gap, overlap, checksum mismatch, divergent `request_id` fingerprint, or attempt to advance `sqlite_high_water` past an unapplied sequence MUST poison the local projection. |
| Memory/SQLite divergence | Any mismatch between memory and SQLite over their shared validated prefix MUST poison the local projection, as defined by async lineage validation. |
| Repair failure | A repair pass that cannot reconstruct a contiguous prefix through manifest, segment, memory image, and SQLite image MUST leave the queue poisoned and MUST NOT advertise recovery readiness. |

#### Ordered batching and SQLite high-water (`objectlog/hybrid-async`, normative)

The async SQLite projection worker applies sealed object-log batches, not
opportunistic per-command fragments:

| Element | Rule |
|---------|------|
| Batch sequence | Each sealed segment batch has a monotonically increasing `batch_sequence` derived from the committed object-log order and covers a contiguous command `sequence` range. The SQLite worker MUST apply batches strictly in `batch_sequence` order; batch N+1 MUST NOT apply or advance visibility until batch N has fully applied. |
| Exactly-once replay | A batch is replayable from the object log and is idempotent against SQLite's persisted applied position. On restart, the worker resumes at `sqlite_high_water + 1`, skips commands `<= sqlite_high_water`, and applies every later committed command exactly once in `sequence` order. Partially applied batches MUST either roll back as a SQLite transaction or replay to the same final projection state before `sqlite_high_water` advances. |
| Memory versus SQLite readers | Hot reads, claim selection, pre-commit validation, response rendering, metrics, secondary-index lookup, and live-item lookup MUST read the synchronous in-memory projection. The lagging SQLite projection is visible only to recovery/export/diagnostic paths and MUST NOT answer API reads while its `sqlite_high_water` trails memory. |
| Logical high-water | `sqlite_high_water` is the highest logical command `sequence` whose effects are reflected in SQLite projection rows after a successful apply transaction. It is a recovery/export marker for the projection image; it is not an object-log durability marker and not a segment-retention authority. |
| WAL/fsync boundary | SQLite WAL frames, checkpoint state, page-cache contents, and fsync policy are local durability implementation details beneath the projection store. They MUST NOT be reported as `sqlite_high_water`, MUST NOT let recovery skip commands whose logical effects are not applied, and MUST NOT authorize object-log segment trimming. |
| Lag failure | If the worker cannot advance ordered batching within `hybrid_async_sqlite_apply_lag`, the store MUST keep serving from memory only when the object log proves memory completeness; recovery/high-water claims fail closed until ordered replay catches up or recovery replays from an earlier authoritative source. |

#### Async lineage validation (`objectlog/hybrid-async`, normative)

Before `objectlog/hybrid-async` may advertise a queue as recovered, compacted, or
release-ready, it MUST validate one lineage from durable log through both
projection images:

| Element | Rule |
|---------|------|
| Manifest to segment | Every active manifest entry MUST name an existing segment whose checksum, `[first_sequence, last_sequence]`, `batch_sequence`, and `assignment_epoch` match the manifest metadata. Missing, truncated, or checksum-mismatched segments fail recovery closed. |
| Segment to command sequence | Commands inside retained segments MUST be contiguous and monotonic by `sequence`; per-command checksums and `request_id`/`request_fingerprint` metadata MUST be validated before replay or idempotency cache reconstruction. Gaps, overlaps, or divergent fingerprints for the same `request_id` fail recovery closed. |
| Command sequence to memory | The hot in-memory projection MUST expose a `ProjectionImage` lineage marker containing the highest applied command `sequence` and the manifest tail it was built from. After success, memory MUST cover every acknowledged command through the operation's own sequence even when SQLite lags. |
| Command sequence to SQLite | SQLite `ProjectionImage` export MUST include `sqlite_high_water`, the covered batch sequence range, projection schema version, and source manifest tail. It is valid only if its applied command prefix is contiguous from the retained recovery base through `sqlite_high_water`. |
| Cross-image equality | For a shared prefix, memory and SQLite images MUST agree on queue definition, item lifecycle, leases, secondary indexes, side records, instance fences, pause/gate state, metrics, request-id replay records, client item-key retention records, and counters. A mismatch poisons the local projection and requires replay from an authoritative object-log source. |
| Release evidence | The release ledger for `objectlog/hybrid-async` MUST record the manifest tail, segment sequence ranges, applied `batch_sequence` values, `sqlite_high_water`, memory image high-water, idempotency replay record count, item-key retention record count, the computed retention frontier, async apply debt, replay debt, configured backpressure thresholds, and typed backpressure/poison state. |

#### Crash matrix and perf matrix (`objectlog/hybrid-async`, normative)

The release gate for `objectlog/hybrid-async` MUST include a crash matrix and
perf matrix that prove the async contract across manifest, memory, SQLite
high-water, repair, and release-lane load boundaries:

| Row | Scenario | Required proof |
|-----|----------|----------------|
| Crash before manifest commit | Kill the writer after segment write or local reservation but before manifest commit; prove no success was returned, no durable lease or mutation is visible, reservations are cleared, and retry by `request_id` is a fresh attempt unless a committed command exists. |
| Crash after manifest before memory render | Kill after manifest commit but before memory apply/render; prove retry observes an unknown-outcome, replays the committed command, returns/reconstructs the original response, and does not append a duplicate command for the same `request_id`. |
| Crash after memory render before response delivery | Kill after memory render but before response delivery; prove the command is committed, memory is reconstructed from log/SQLite plus tail replay, retry by `request_id` returns the original response, and no second lease/item transition is produced. |
| Crash during async SQLite apply | Kill while applying a sealed batch to SQLite; prove the partial batch rolls back or replays idempotently, `sqlite_high_water` does not advance past unapplied effects, and ordered batching resumes at `sqlite_high_water + 1`. |
| Crash after SQLite high-water advancement | Kill immediately after advancing `sqlite_high_water`; prove owner-local restart hydrates memory from the matching `ProjectionImage`, replays only commands beyond that high-water, and validates memory/SQLite equality over the shared prefix before serving. |
| Crash during repair | Kill during a repair pass; prove repair is restartable, leaves no advertised recovery readiness until lineage is complete, and keeps the queue poisoned/backpressured if manifest, segment, memory render, SQLite apply, or high-water evidence is incomplete. |
| Perf matrix under release-lane load | Run the release-lane scale/cost workload with async apply enabled; prove p50/p95/p99 ack latency, max/p99 `hybrid_async_sqlite_apply_lag`, `hybrid_async_apply_debt_bytes`, `hybrid_async_apply_queue_depth`, oldest unapplied `batch_sequence`, poison count, typed backpressure count/duration, segment batch density, object PUT count, recovery elapsed time, replayed tail length, and request-id replay convergence stay within the configured release gates. |

## Hybrid ProjectionImage Recovery (normative)

Hybrid recovery MUST avoid full-genesis replay on ordinary owner-local restart
without treating local SQLite as the command authority.

| Element | Rule |
|---------|------|
| Image seam | `pqueue-projection` MUST define a typed `ProjectionImage` export/import contract covering queue definition, item lifecycle, lease state, secondary indexes, side records, instance fences, queue paused state, metrics, request-id replay records, and item-id counters. Partial images that only load pending items are invalid. |
| SQLite export | `SqliteProjectionStore::export_projection_image(queue)` MUST read the durable SQLite projection at its current applied logical high-water (`sqlite_high_water`) into `ProjectionImage`. WAL checkpoint/fsync state is not part of the exported high-water contract. |
| Memory hydrate | `InMemoryProjection::hydrate_shard(definition, image)` MUST build memory to the exact same logical state before any hot read or validation method is served. |
| High-water barrier | `HybridProjectionStore::recovery_high_water` MUST return SQLite's high-water only after the in-memory shard has been hydrated from that image. If hydration fails, is incomplete, or has not run, it MUST return `None` or fail closed so `ComposedBackend::recover` replays from genesis rather than skipping log history. |
| Tail replay | After hydration, recovery replays only object-log commands beyond SQLite logical high-water through the normal hybrid apply path. For `objectlog/hybrid-async`, replay starts after the last ordered batch whose complete command range is covered by `sqlite_high_water`; any later committed commands are replayed from the object log even if local WAL or page-cache state contains partial effects. |

## Hybrid Snapshot Authority and Segment Retention (normative)

`objectlog/hybrid-strict` and `objectlog/hybrid-async` have two supported
recovery modes:

| Mode | Rule |
|------|------|
| Owner-local restart | With the local SQLite projection file present, hydrate memory from SQLite `ProjectionImage`, then replay only the object-log tail beyond SQLite high-water. |
| Disk loss / new owner without SQLite | Recreate SQLite and memory by replaying the retained object log from genesis, unless a separately committed object-store snapshot is present and validated. |

For the first hybrid release, segment expiry MUST NOT be based only on the local
SQLite file. Segments MAY be expired only under the existing committed
object-store snapshot plus recovery-window rule, or a later object-store
SQLite/ProjectionImage snapshot feature with its own recovery tests. A local
SQLite high-water alone is insufficient because local disk can be lost with the
owner.

## Replay-Response Idempotency Model (normative)

This backend uses TD-001's **replay-response** option (not transactional response). The rules below
specialize TD-001 §"Durable Ack and Response Replay" for object-log timing.

| Condition | Rule |
|-----------|------|
| Ack timing | A mutating operation MUST NOT return success before its segment's manifest entry is committed (pipeline step 5). Until then the operation is in-flight, not acknowledged. |
| Commit-but-unreturned / unknown-outcome | If the manifest commits but the mode-specific success barrier, response persistence, or response delivery fails, the operation is committed-but-unreturned and the caller observes an unknown-outcome. Retrying the same `request_id` MUST converge by locating the committed command (by `command_id`/`request_fingerprint` in the replayed log, memory projection, or durable replay record) and returning the recorded or reconstructed response. |
| Request-id conflict | Retrying the same `request_id` with a different request fingerprint MUST fail with `request-id-conflict` (API-001). The fingerprint is carried in the committed `CommandEnvelope`, so this holds even if the projection has not yet applied the command. |
| Claim replay | `BatchClaim` retry MUST return the same claimed set while leases are active and MUST fail with `request-expired` once all returned leases are no longer active (API-001 claim idempotency). The claim command and its lease assignments are reproduced from the committed log; an un-committed (reserved-only) original claim is not a replay (see "Claim Reservation"). |
| Commit timeout | If the manifest commit cannot complete before the configured commit deadline, the operation MUST return envelope `commit-timeout` (or per-item `unavailable`), and its reservations MUST be rolled back; the caller retries with the same `request_id` and item keys, and accepted items converge (API-001). |

For both `objectlog/hybrid-strict` and `objectlog/hybrid-async`, durable
request-id replay is mandatory for every mutating command, not only pushes. The
committed `CommandEnvelope::request_id` and command body MUST be enough to
reconstruct the request fingerprint and response after restart. Recovery MUST
either repopulate the generic idempotency cache from replayed committed commands
or persist equivalent request-id rows during apply. A same-body retry returns the
original result without a second append; a different-body retry returns
`request-id-conflict`. This is required for crashes after manifest commit, after
the mode-specific success barrier partially completes, and before response
delivery.

### Request-id coverage matrix (normative)

Every operation listed below MUST carry a stable `request_id` in the command
envelope, persist enough fingerprint/result material for replay-response
idempotency, and use the unknown-outcome rules above in both hybrid modes.

| Operation family | Covered mutations | Same `request_id` retry | Different fingerprint retry | Hybrid-async SQLite-lag note |
|------------------|-------------------|-------------------------|-----------------------------|------------------------------|
| Push | `BatchPush`, duplicate `client_item_key` convergence | Returns original item ids and accepted/rejected per-item results | `request-id-conflict` | SQLite may not yet contain `pqueue_request_idempotency`; replay MUST consult committed log or memory replay record |
| Claim | `BatchClaim` including group/cohort claims | Returns same active lease set, or `request-expired` after all leases are inactive | `request-id-conflict` | Reservation-only attempts are not committed; committed claims replay from log even if SQLite lags |
| Renew | `BatchRenewLeases` | Returns the same renewed lease expiry/version effects | `request-id-conflict` | Memory is authoritative for visible active lease state after success |
| Finalize | `BatchFinalize` success/failure terminal transitions | Returns the original terminal result and item versions | `request-id-conflict` | SQLite lag MUST NOT allow a finalized item to be claimed again from memory |
| Retry/release | retry scheduling, release/rearm of leased or recurring items | Returns the same retry/release/rearm result and next eligibility fields | `request-id-conflict` | `eligible_since`/`not_before` are read from memory until SQLite catches up |
| Update | `BatchUpdate`, priority/not-before/payload/metadata updates, `SetGates` queue-scoped updates | Returns the original update/gate result | `request-id-conflict` | Gate flips and secondary indexes are visible from memory before success |
| Purge | `PurgeItems` and bounded purge batches | Returns the original purged/not-found/conflict result and tombstone effects | `request-id-conflict` | Memory tombstones prevent resurrection while SQLite applies asynchronously |
| Operator-style mutations | repair, redrive, archive, pause/resume, cancel operation, retention/inspection mutations that change state | Returns the same `operation_id` or mutation result; committed batches are not rolled back by retry/cancel | `request-id-conflict` | Async operation progress records MUST replay from log/memory even before SQLite projection rows catch up |

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

Sequenced metadata uses two non-interchangeable typed protocols. The retention floor is
advance-then-delete: its epoch-fenced monotone manifest publication completes before segment deletion, and
the create-only manifest address remains occupied as a stale-writer collision fence. The deletion watermark
is delete-then-advance: every segment in the contiguous prefix is proven physically absent before an
append-only marker advances. `read_horizon.json` is a compatibility cache, never deletion authority.

Create-only head publication distinguishes applied, identical already-applied, precondition-lost, and
ambiguous outcomes. A failed response is resolved by rereading the exact authoritative address before retry
or use; successful publication adds no hot-path read. Partial-expiry visibility, contiguous-watermark
derivation, and reclamation-candidate selection share one pure floor/fence/data classifier. Branch-pin and
physical-absence checks remain explicit I/O. Candidate GC retains the horizon root, and reclamation never
frees an address needed by the authoritative head/candidate walk or create-only collision history.

Key and authority map:

| Metadata class | Typed identity | Durable key/address | Authority and retention |
|---|---|---|---|
| Authority head / epoch fence | `HeadVersion`, `AssignmentEpoch` | `authority_head/{version}.json` names an immutable `manifest_candidates/...` object | Versioned head is authoritative and retained; a candidate is visible only when named by the winning head. |
| Retention floor | `CommandSequence`, `ManifestIndex`, `AssignmentEpoch` | Floor fields in the winning authority candidate/head, or retained legacy `manifest_head/{index}.json` | Winning fenced publication is authoritative; its collision address is retained. |
| Deletion watermark | `ManifestIndex` | Append-only `manifest_head/{index}~watermark.json` | Marker history is authoritative and retained. `read_horizon.json` is a rebuildable compatibility cache only. |
| Reclaimed data | `ManifestIndex` | Segment object plus legacy `manifest/{index}.json` mirror | These addresses are `FreeAddress` targets after proof; the corresponding head/candidate collision history remains retained. |

State-transition matrix:

| Class | Required order | Serving/visibility rule | Idempotent terminal state |
|---|---|---|---|
| Authority head / epoch fence | CP `Assigned(E)` → CP `PendingFence(E+1)` → storage fence `E+1` → projection hydration → CP `Assigned(E+1)` | `PendingFence` never serves. An operation admitted under `E` may linearize before the storage fence; after the fence every `E` retry is rejected. | Winning head reread reports the exact epoch and CP confirm publishes that owner. |
| Retention floor | Read current floor → validate monotonicity → fenced create-only advance → classify eligible entries → physical expiry | No delete is eligible before the durable advance. Equal advance is a no-op; regression is invalid. | Durable floor is greater than or equal to the requested floor. |
| Deletion watermark | Delete segment → prove segment absent → delete freeable legacy mirror → prove retained head/authority lineage → publish append-only marker → refresh cache | Readers and seal/recovery consult marker authority, never the cache, for fencing or suppression. | Exact marker body exists; cache may lag without changing authority. |
| Reclaimed manifest address | Prove entry is behind the durable floor and not branch-pinned → reclaim freeable objects → preserve winning-chain root/collision address | A stale writer must still collide at every retained create-only address. | Freeable objects are absent and retained lineage remains readable. |

Failure and recovery table:

| Cut/failure | Required result | Recovery action |
|---|---|---|
| Crash after CP writes `PendingFence`, before storage fence | No new owner serves; at most the already-admitted old-epoch prefix can commit. | Reacquire/fence at the same or greater epoch, hydrate the complete prefix, then confirm. |
| Create-only response lost after durable effect | Do not blindly retry or report failure. | Exact-key reread with identical body yields `AppliedAfterAmbiguity`. |
| Exact-key reread is missing/fails, or contains a different body | Missing/failed stays typed `Ambiguous`; different body is `PreconditionLost`. | Caller fails closed or retries from refreshed authority; it must not delete or serve based on the attempted value. |
| Any segment delete or physical-absence proof fails | Deletion watermark does not advance. | Retry idempotent deletion and proof from the last authoritative marker. |
| Marker succeeds but cache refresh fails | Marker remains authoritative; cache lag cannot suppress recovery/seal. | Rebuild `read_horizon.json` from append-only marker history. |
| Cache contains a stale high value | It cannot fence a writer or hide unsealed/recoverable data. | Ignore it for authority decisions and derive the horizon from markers. |
| Candidate/head lineage required by recovery is missing | Fail closed. | Repair or restore retained authority history; never infer it from LIST or the cache. |

| Element | Rule |
|---------|------|
| Segment expiry | A segment MAY be deleted only after a committed snapshot fully covers its `sequence` range AND `log_recovery_window_ms` has elapsed past that snapshot's `committed_at` (ADR-001 step 8). |
| Hybrid local SQLite high-water | For `objectlog/hybrid-strict` and `objectlog/hybrid-async`, the local `sqlite_high_water` is a logical applied-command marker sufficient to skip historical log replay only on owner-local restart after `ProjectionImage` hydration succeeds and, for async mode, after ordered batching has caught up to the advertised high-water. It is NOT sufficient by itself to expire object-log segments, and SQLite WAL/fsync/checkpoint state never upgrades it into segment-expiry authority. |
| Idempotency retention | Request-idempotency and item-key convergence records expire per `request_id_retention_ms` / `client_item_key_retention_ms` (API-001), enforced in the SQLite projection; expired records MUST NOT be required for any non-expired segment's replay. |
| Manifest retention | Manifest entries (including epoch fence records) for expired segments MAY be compacted, but the manifest MUST retain enough tail to validate the active recovery window and the monotonic `sequence`/epoch invariants. |
| Snapshot retention | At least the most recent committed snapshot whose recovery window has not expired MUST be retained at all times so recovery is always possible. |

For `objectlog/hybrid-async`, segment, manifest, snapshot, idempotency, and
item-key expiry share one retention frontier. The implementation MUST compute
the deletion frontier as the minimum of:

1. the highest command sequence fully covered by a committed object-store
   snapshot whose `log_recovery_window_ms` has elapsed;
2. the oldest command sequence still required by the active manifest tail needed
   to validate monotonic sequence and epoch lineage;
3. the oldest command sequence that may still be needed to resolve an unexpired
   `request_id` replay, including committed-but-unreturned and response-lost
   async outcomes;
4. the oldest command sequence that may still be needed to enforce unexpired
   `client_item_key` convergence/tombstone retention;
5. for async mode, `sqlite_high_water - hybrid_async_sqlite_apply_lag` only when
   ordered SQLite apply is healthy, or no advancement at all while lag is
   over-budget, failed, or lineage validation is incomplete.

The object log MAY delete only segments whose entire sequence range is below
that minimum frontier and whose lineage has been validated through manifest,
segment, memory image, and SQLite image. Async outcome retention MUST keep the
material needed to return or reconstruct the original response for every
unexpired `request_id`; deleting the segment that contains the original command
is legal only after an equivalent durable replay record and any required
item-key tombstone survive beyond the same frontier. WAL checkpoint/fsync state,
page-cache contents, and local SQLite high-water never move this frontier by
themselves.

## Configuration Validation (normative)

| Element | Rule |
|---------|------|
| Reject 1-object-per-command | A backend configuration that would seal one command per segment in production MUST be rejected at queue/backend configuration time with `invalid-request` (API-001). It is available only behind an explicit `dev_unsafe_one_command_segments` flag for tests; that flag MUST NOT be settable in a production deployment profile. |
| Reject missing CAS | If the configured object store lacks a usable conditional-write primitive and the deployment has not selected the Postgres-manifest-pointer fallback, queue/backend configuration MUST be rejected with `invalid-request` (see "Object-Store Capability Requirements"). |
| Window sanity | `segment_max_latency_ms` MUST be `> 0`; it is the implementation of the profile's `max_commit_latency_ms` / commit-latency-bound knob. The effective claim/ack latency budget MUST be documented to callers because it bounds API-001 commit latency for this profile. |
| Snapshot vs recovery window | `log_recovery_window_ms` MUST be `>= snapshot_interval_ms` so an unexpired snapshot always exists before its covered segments can expire. |
| Hybrid pairing | `PQUEUE_PROJECTION_BACKEND=hybrid-strict` and `PQUEUE_PROJECTION_BACKEND=hybrid-async` are supported only with `PQUEUE_LOG_BACKEND=objectlog` until other pairings are intentionally implemented and tested. `memory/hybrid-*`, `sqlite/hybrid-*`, and `postgres/hybrid-*` MUST fail closed at startup. |
| Buffered-byte bounds | `PQUEUE_OBJECTLOG_BUFFERED_BYTES_GLOBAL` and `PQUEUE_OBJECTLOG_QUEUE_WAITING_BYTES` MUST be positive; the queue cap and optional `PQUEUE_OBJECTLOG_BUFFERED_BYTES_TENANT` MUST not exceed the global cap; and the segment target MUST not exceed the global cap. The composition root builds one node budget and injects it into every live object-log projection profile. |

## Runtime Wiring (normative)

`objectlog/hybrid-strict` and `objectlog/hybrid-async` MUST use the generic
object-log group-commit composition:

```
ComposedBackend<pqueue_objectlog::ObjectLog, HybridProjectionStore, InProcessControlPlane>
```

The server composition root MUST open the segmented object-log axis through the
same `ObjectLog::open_group_commit` path used by other object-log profiles, not a
third segmented backend monolith. Runtime wiring MUST plumb segment
configuration, recovery-tail configuration, debug segment/counter visibility,
and a bounded flusher task that calls `flush_tick` at
`group_commit_flush_interval_ms()`. The flusher and counters are profile
infrastructure; they MUST NOT change the transaction contract above.

## Commit-Latency / Cost Tradeoff (normative statement)

This profile's durable-commit cost scales with sealed-segment count, not command count (ADR-001 napkin
cost). The deliberate tradeoff: acknowledgement of a mutating operation MAY be delayed up to
the configured commit-latency bound, implemented as `segment_max_latency_ms`
(plus segment write + manifest commit + own-operation apply/response-barrier
time), so that many commands share one durable object write. This is the lever
that produces the S3 cost floor.

API-001 still holds **once a response returns**: the response is derived only from committed command
state (replay-response); per-item outcomes, ordering, idempotency, read-after-success visibility, and
lease semantics are identical to `postgres_native`, and the operation's own effect is applied before its
response returns (see "Response / Apply Ordering"). The client-visible differences are higher and
configurable acknowledgement latency plus different cost/recovery curves. There is no weakening of
FR-9/FR-12: the progress bound is computed from `eligible_since`, which is unaffected by commit batching
(an item is not eligible until its push command is committed and applied, and progress age accrues from
eligibility, FR-10).

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
- `objectlog/hybrid-strict` and `objectlog/hybrid-async` performance gates are
  concrete and release-blocking: push throughput and p50/p95/p99
  acknowledgement latency MUST be within 20% of `objectlog/inmemory` under the
  same segment settings; claim/finalize p95 latency MUST be within 20% of
  `objectlog/inmemory` for hot reads. Strict evidence reports SQLite apply
  amortization; async evidence reports max/p99 SQLite lag and request_id
  unknown-outcome replay convergence. Both modes report segment batch density,
  object PUT count, recovery elapsed time, replayed tail length, and maximum
  memory rehydrate time from SQLite `ProjectionImage`.
- `objectlog/hybrid-strict` and `objectlog/hybrid-async` recovery gates: smoke
  restart with 100k resident items and local SQLite present MUST complete hydrate
  plus tail replay in <= 5 seconds and replay <= 1,000 object-log commands;
  release-tier restart with 10M resident items and local SQLite present MUST
  complete in <= 60 seconds and replay <= max(10,000 commands, 0.1% of resident
  items); disk-loss recovery from retained object log MUST reconstruct exact
  metrics, indexes, leases, and request-id replay state with zero invariant
  violations.
- Telemetry overhead MUST be included in performance tests.

## Testing

### Completion Evidence (pre-ADR-008 build record)

> **Build-record note (ADR-008 simplification).** The completion evidence below records the
> `object_log_sqlite_projection` backend **as built** prior to the ADR-008 "queue is the unit of
> sharding" simplification — it used the earlier intra-queue-shard model (per-`(queue,shard)` manifests
> and cross-shard command convergence). The durable object-log substrate it validated — group-commit
> segments, manifest CAS, current-epoch fencing, in-flight claim reservation, snapshot + log-tail
> recovery, cohort/recurring replay — **carries forward unchanged** under the per-queue model; only the
> intra-queue-shard partitioning and the cross-shard command binding are retired as targets.
> **Update (2026-07-05): the re-scoping has since landed** — the codebase (workspace v0.8.0) is on the
> per-queue ownership model throughout (`QueueKey { tenant_id, queue_id }`, no `shard_count`, per-queue
> manifests in `pqueue-objectlog/src/segmented.rs`), so "later build phase" above is historical. This
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
- Hybrid poisoning: inject failure after SQLite commit and before memory apply;
  assert the operation returns storage failure, all subsequent reads,
  validation, and writes fail closed, and restart hydrates memory from SQLite
  before serving.
- Hybrid `ProjectionImage` hydration: export SQLite image, hydrate memory, then
  return SQLite high-water; include queue pause, metrics, secondary indexes,
  leases, side records, instance fences, counters, and request-id replay records.
- Hybrid request-id replay matrix: crash/retry around each mode's success
  barrier for push, claim, renew, finalize, retry/release, update, purge, and
  operator-style mutations; same-body retry returns the original result without
  append, different-body retry returns `request-id-conflict`.
- Mutable-write race closure: for profiles that admit mutable writes
  (`objectlog/hybrid-strict` and `objectlog/hybrid-async`), race
  `replace_if_pending`/`update_fields`/`reschedule` against a concurrent claim
  for the same pending item under group commit; exactly one path succeeds, the
  winner is visible on the response path, and the loser fails closed. The
  legacy `eventual_apply_suite` keeps asserting `upsert_is_unavailable` for the
  pure lagging-projection profiles, while this case covers the lifted profiles.
- Hybrid segment retention: prove local SQLite high-water alone never authorizes
  object-log segment expiry; compute the retention frontier as the minimum of
  committed snapshot coverage, active manifest tail, request-id retention,
  item-key retention, and async SQLite lag; disk-loss recovery succeeds from
  retained log.
- Hybrid async lineage validation: validate manifest entry -> segment checksum
  and sequence range -> command `request_id` fingerprints -> memory
  `ProjectionImage` -> SQLite `ProjectionImage`/`sqlite_high_water`; fail closed
  on gaps, overlaps, divergent fingerprints, or cross-image mismatches.
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
  (object-log latency/cost + recovery), measured against the per-queue throughput floor TP-002 **E0**
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
