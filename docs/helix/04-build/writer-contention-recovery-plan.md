---
ddx:
  id: build-writer-contention-recovery
  type: implementation-plan
  links:
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: tp-fireweed-performance-matrix}
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: concerns}
  review:
    round: 41
    claude: APPROVE
    codex: N/A
    folded: "docs/helix/04-build/reviews/writer-contention-round41/"
    disposition: "converged; round-41 shared-reader enumeration, WAL labels, concurrent memory-goal reconciliation, and cache-partition authority folded"
---

# Batch transactions, packed apply, and a continuous Seventh Sense pipeline

## Objective

Raise the one-queue Seventh Sense lifecycle to the governed T1/T2/T3 targets by
amortizing compatible selection, append, and apply work across bounded
microbatches without weakening log authority, exact public outcomes, bounded
admission, cancellation safety, or projection recovery.

## Scope and governing artifacts

This plan removes the per-request transaction boundary that dominates the
`filesystem--turso` Seventh Sense lifecycle. It preserves the public batch size
of 100 while microbatching compatible concurrent requests behind the facade.
The command log remains authoritative and Turso remains a derived serving
projection.

Governing authority, in order:

1. `prd` and API-001: batch-centric operations, deterministic ordering,
   single-active leases, and composition-independent outcomes.
2. ADR-013 and ADR-017: log authority, explicit separate append/apply commit,
   bounded admission, queue-local serialization, and drainable owned work.
3. TD-010: one shared Turso writer, independent reads, ordered idempotent apply,
   and no permanent task or connection per queue.
4. TP-005 and `ss-objectlog-turso-memory-goal.md`: public-facade evidence,
   exact reconciliation, and the N=100k T1/T2/T3 and M1/M2/M3 gates.

In scope: unfiltered item-level `BatchClaim`, `BatchFinalize(Complete)`, packed
object-log apply, the phased benchmark, and a bounded streaming lifecycle lane.
Grouped/cohort claims retain their current serialized implementation rather
than joining the microbatch, but every Pending-consuming Claim append joins the
new exclusive selection-fence domain. No public API or queue-definition field
changes.

Out of scope: `SKIP LOCKED` on Turso, a process-lifetime item planner map,
SQL-first leases, a claim outbox, larger public batches, multi-queue throughput
as a substitute for one-queue performance, or weakening the strict response
and projection-health contracts.

## Release reconciliation

The source-release baseline is annotated tag `v0.31.21` at current HEAD
`91f94ef1`, documented by `docs/releases/v0.31.21.md`. It is a source-preview
cut; GitHub's latest published Release is still `v0.31.9`, so no plan claim
depends on GitHub release metadata being current. This was rechecked on
2026-08-21: `git describe --tags --abbrev=0` returned `v0.31.21`, while
`gh release view` returned published Release `v0.31.9` from 2026-08-14.

For S-0, the normative adapter dependency is the executable pair on the
claimed v0.31.21 base: `crates/fireweed-turso/Cargo.toml` plus the root
`Cargo.lock`, both exact Turso 0.7.0. A nested benchmark lock or document-only
edit cannot change that pin. This worktree contains concurrent, uncommitted
TD-010/ADR-016/legal/benchmark-lock edits that describe 0.7.2 without changing
the adapter manifest or root lock, plus an `ss-objectlog-turso-memory-goal.md`
edit that claims an out-of-scope process-lifetime planner map is implemented
and labels older evidence latest. They are outside B-0 and are not authority
for its evidence or permission to amend this plan's explicit non-goals. S1's
existing `governing_microbatch_contract_present` scope must reconcile the
memory-goal planner-map and evidence claims without overwriting concurrent
work now. A 0.7.2 adoption must be one separate atomic change that
updates the adapter manifest/root lock and every governed dependency/legal
artifact, then reruns S-0 and supersedes its 0.7.0 evidence before merging.

Sixteen commits after `v0.31.20` through `0567e232`, followed by the
`v0.31.21` identity cut, landed three useful
precursors: ordered packed apply publication, thinner Class-S selection, and a
provisional lease pack. The provisional pack groups up to eight waiters behind
one `IMMEDIATE` commit and deletes claim outbox rows during live Claim apply.
It proves that transaction amortization matters, but it is not the plan's end
state:

- `LeasePackState` is global to a Turso adapter, not keyed by queue/epoch and
  compatibility;
- sealing invokes `class_s_claim` once per waiter inside the transaction, so it
  still performs per-request SELECT/UPDATE/bearer/outbox SQL;
- the pack reconstructs requests without all original request-id,
  fingerprint, claim-unit, and cohort fields;
- one waiter error rolls back every waiter, and the 20 ms linger is not yet
  justified by a latency/fill curve;
- it remains SQL-first and therefore retains the pre-PUT outbox/fencing
  recovery problem that blocked the earlier plan.
- its thin Class-S SELECT currently hardcodes empty `fields_json`,
  `metadata_json`, and `entity_document`, so evidence `1787274546` is not a
  contract-faithful public Claim baseline. Regression commit `5999aa77` is part
  of `v0.31.21`; S-1 and every later slice branch from `v0.31.21` and reverse
  that loss before measurement.

Treat this code as measured scaffolding to refine and then retire, not as proof
that S2–S6 are complete.

The S-0 probe against this release also invalidated one plan assumption before
pool implementation began. All twenty-four Deferred readers opened before a
live `IMMEDIATE` writer and preserved the committed `before` value while the
writer held an uncommitted `after` value and after it committed; a fresh reader
was intended to observe `after`, but that read accidentally used the writer
connection. However, Turso 0.7.0 returned no row for
`PRAGMA read_uncommitted` on every reader, and the standalone 0.7.2 probe did
the same. Turso also rejects the keyword value passed by the current adapter's
`pragma_update("query_only", "ON")`; both that error and the adjacent
`read_uncommitted` update are currently discarded. An exact diagnostic shows
the latter returns `Ok([])` but still has no readback row. S-0 therefore stopped as
designed. Its revised gate below proves committed isolation semantically,
requires readback only for supported settings, removes the unsupported
`read_uncommitted` configuration, and makes `query_only=1` configuration
failure visible. Round 37 then found that this first redesign proved
held-snapshot stability but not freshness when a pooled connection begins its
next snapshot, and that its single-row writer did not force shared WAL
activity. Round 38 required that spill evidence to use a named, pre-commit,
non-checkpoint instrument and caught an unmeasured 128 MiB→4 MiB serving-cache
cut. The current S-0 gate therefore uses an explicitly adversarial file-backed
writer, treats both pre-commit WAL-growth dispositions as valid when the
semantic isolation assertions pass, adds same-connection freshness under a
third live writer, preserves the serving cache through S0, and moves its
measured reduction into S3r. No serving pool or autocommit fallback is
authorized by this redesign.

## Current evidence and bottleneck

The latest committed N=10k diagnostic evidence at
`docs/perf/evidence/ss-phased/1787274546/summary.json` reports:

| Phase | Items/s |
| --- | ---: |
| P1 ingest | 31,780.1 |
| P2 enrich | 34,898.0 |
| P3 schedule | 49,911.6 |
| P4 claim + complete | 1,289.8 |

P4 still dominates the measured lifecycle. The provisional shared transaction
improved claim p50 from 492.9 ms to 211.2 ms and P4 from 913.3 to 1,289.8
ack-time items/s, while finalize p50 is 333.5 ms. Because this path drops public
Claim fields/metadata/entity, these rates diagnose transaction cost but cannot
serve as non-regression or T-gate baselines. S-1 restores fidelity and S0 records
the first valid baseline. T2 remains unscored: it is an
N=100k settled-rate gate, not comparable to this N=10k artifact. Each public
claim already uses set-based SQL for its own 100 items; the shared transaction
still runs that SQL sequence eight times. The inflight=8 harness submits
barriered waves and waits for all eight claims before refilling, so it is not a
continuous pipeline.

The optimization unit is not the 100 items already inside a public batch. It is
the set of compatible public requests that reach the same queue together.

## Shared constraints

- Keep one public response and one lease token per request. Rows from different
  requests never share a token, worker, replay identity, or result vector.
- A keyed coordinator may batch only the same tenant, queue, exact
  `expected_epoch` (`None` is never compatible with `Some(e)`), mutation kind,
  item-level claim class, serialized claim compatibility, lease duration, and
  eligibility lane. Ordinary claims (`eligibility_time=None`) share a lane while
  retaining each request's exact `now`; scheduled claims key on exact
  `Some(eligibility_time)` and never merge with ordinary claims. Tokens, workers,
  lease expiries, and operational times remain per request.
  Keeping `None` separate is deliberate even though dispatch can resolve it to
  the current epoch: a driver must never import one waiter's explicit fence into
  a sole-owner request or vice versa.
- Initial internal bounds are eight requests, 800 requested rows, the existing
  packed-append command/debt limit, a separate 4 MiB aggregate rendered-response
  limit, and no more than the current 20 ms linger. Hitting any bound dispatches
  immediately. One public request may exceed the aggregate response bound and
  then runs alone. The chosen linger must be backed by a same-SHA fill/latency
  curve. Empty keyed entries are reclaimed; there is no task or connection per
  queue.
- The mutation sequencer, rather than either ingress, owns its capacity. Across
  direct and live-KeyedQueueGate callers together, one queue retains at most
  sixteen requests in at most two FIFO generations. Each generation is bounded
  by eight requests, 800 items, the existing exact command/debt charge, and a
  4 MiB rendered-response estimate. Queued generations retain only fixed-size
  descriptors and `Arc`s to the request allocations the public futures already
  own; they never clone encoded payloads or pre-render responses. Only the one
  active generation renders, under its 4 MiB bound, and command/debt is reserved
  only after planning. Thus a legal large request runs alone under the same
  generation and public/command bounds without a second byte budget, cross-queue
  byte cliff, or oversize-loan scheduler. The two-generation choice is a latency
  and incremental-memory bound: one generation progresses and one hides the
  handoff; a third cannot increase same-queue throughput because validation and
  publication are ordered. The compatible request 17 (or whichever request
  first needs a third generation) is rejected before planning regardless of
  ingress. S0 records a 32-request
  same-queue baseline and S3s/S5 replay that
  closed cohort with explicit retries, so this deliberate admission tightening
  cannot hide behind the ordinary inflight-eight lane.
- The coordinator caps all queued and in-flight item-Claim callers, including
  duplicate attachments, at the same configured `max_queued_commits` used by
  the existing queue gate (1,024 in the Turso composition). Over-cap admission
  returns `EngineError::Backpressure { resource: "claim coordinator waiters" }`
  before selection or reservation; completion/cancellation releases one slot.
  This intentionally tightens the former gate's queued-only basis: attached
  response channels consume bounded memory even when they share durable work.
  Append-time admission is normative per product and reflects the real call
  graph. A prepare-phase gate that has been released before append is not an
  append admission:

  Every Pending-consuming Claim form also joins a queue-scoped `ClaimQueueTurn`
  after its append admission/gate and before a global Claim-driver slot. It
  admits one active and one queued driver per queue across item, group, and
  cohort compatibility keys; a third driver receives retryable
  `Backpressure { resource: "claim queue turn" }` before projection work. The
  active turn remains through append publication. Together with the mutation
  sequencer's one active generation, this leaves at most two fence contenders
  for a queue, so either contender can have only one legal fence predecessor.
  The global Claim-driver admission admits at most eight drivers into four
  slots, and the global shared-driver admission admits at most twenty-four
  generations into twelve slots; excess drivers fail capacity admission before
  joining the semaphore. Each admitted slot waiter therefore has at most one
  complete holder wave ahead of it.

  These driver limits are taken at driver ingress, not after accepting an
  unserviceable driver. ClaimCoordinator checks the eight-driver process budget
  only when creating a new compatibility bucket; compatible callers may still
  attach up to the independent 1,024-channel caller bound. Direct grouped/cohort
  Claim checks it at SelectionFenceAdmission, and a new mutation generation
  checks the twenty-four-driver shared budget before the sequencer retains the
  generation. The ninth Claim queue and twenty-fifth shared queue therefore get
  named capacity Backpressure without holding a queue turn. Above-cliff S3s/S5
  lanes require all admitted work to progress, the rejected request to have no
  durable effect, and fixed-cadence retry to complete every original ID. The
  caps deliberately match two waves over the sixteen driver connections: extra
  queued drivers cannot add I/O concurrency, while rejecting them bounds latency
  and retained response channels. ClaimQueueTurn uses the same one-progressing,
  one-handoff basis as mutation generations because queue-scoped `last_claim`
  forbids concurrent Claim progress across compatibility keys.

  | Site class | Admission then fence |
  | --- | --- |
  | Derived default item Claim append | ClaimCoordinator (1,024 active+queued callers process-wide) → ClaimQueueTurn (one active + one queued driver per queue) → ClaimDriverReadAdmission (four active + four queued process-wide) → driver pool → exclusive fence; never KeyedQueueGate. The caller ceiling is deliberate because every active caller retains an outcome channel; driver admissions separately bound resource predecessors. `claim_coordinator_rejects_1025_compatible_callers_within_eight_admitted_buckets` fixes caller behavior without colliding with the queue-9 driver cliff. |
  | Derived direct shared append without a live KeyedQueueGate permit, including Push, direct `BatchUpdate`, and prepared Retry/Release/Rearm Finalize | SelectionFenceAdmission queues at most 1,024 requests globally → mutation sequencer → SharedDriverReadAdmission → driver pool → shared fence. The sequencer shared by every ingress enforces the two-generation/16-request per-queue cap. Compatible same-kind requests co-seal up to eight requests/800 items/4 MiB; incompatible or complex mutations form singleton generations. The first request needing a third generation returns retryable Backpressure before planning. An earlier prepare gate is not held concurrently. |
  | Derived direct exclusive grouped/cohort Claim append | SelectionFenceAdmission → ClaimQueueTurn → ClaimDriverReadAdmission → driver pool → exclusive fence; it never joins a mutation generation. |
  | Derived append executed inside `AsyncComposedEngine::submit_operation` with its permit live, including reclaim/`LeaseExpired`, direct typed Renew, Purge, cohort renew/finalize, `commit_raw`, and Reassign | Existing KeyedQueueGate (at most sixteen active+queued per key and 1,024 queued globally) → the same capacity-owning mutation sequencer plus shared slot/pool when classified candidate-mutating, or ClaimQueueTurn plus Claim slot/pool when Pending-consuming, then classified fence if non-bypass. Bypass takes no sequencer/fence. S3i retry cadence re-enters this gate and meters gate wait separately. No SelectionFenceAdmission is added. |
  | Atomic Turso, including direct `BatchUpdate` and macro-generated operations | Native atomic writer/operation serialization; no derived coordinator, SelectionFenceAdmission, selection fence, or coverage wait is introduced. Existing KeyedQueueGate remains only where the current call path already takes it. |
  | Legacy outbox drain during reopen | `RecoveryOnly`: serving is blocked, replay is single-owner, and no serving admission/fence is taken. |

  S2 preserves KeyedQueueGate's global 1,024 queued-waiter cap and adds a
  sixteen-request active-plus-queued cap per key (one active, fifteen queued).
  A distinct active key owns execution rather than a queued response channel,
  so 1,025 independently active queues remain admitted, while request 17 for one
  active key and the 1,025th globally queued waiter are rejected. Empty keys are
  reclaimed. The last admitted same-key waiter has at most sixteen conservative
  predecessor responses because `submit_operation` retains the gate through
  apply/response. `keyed_gate_transitive_wait_ms` therefore has an injected hard
  bound of 16×540 s = 8,640 s. S0/S3s/S5 run a 32-request same-key mixed-command
  cohort with fixed-cadence retry; this is a safety/rejection ceiling, not the
  performance target. The per-key cliff uses distinct
  `QueueGateError::PerKeyFull` and normalizes to
  `Backpressure { resource: "keyed queue per-key waiters" }`; global request
  1,025 retains the existing global queue-full identity. Metrics and S3i retry
  routing never conflate them. Every derived append holds exactly one
  live append-time routing class; a public request may have used an earlier,
  non-overlapping prepare-phase gate. No total 3,072 composition bound is
  claimed because KeyedQueueGate active ownership remains per key.
  SelectionFenceAdmission uses the same queued-only accounting so 1,025
  distinct active queues remain admitted; only the 1,025th blocked fence waiter
  rejects. It is not acquired for bypass vectors.
  `derived_turso_admission_map_covers_every_append_site` is a source-audit/test
  gate asserted at the append site that takes the fence; it fails when a new
  derived append call site lacks one of these live classes. The no-wildcard
  command-disposition match remains the separate genuine compile-time guard. Every derived append
  holds exactly one append-time admission when non-bypass; bypass takes none.
  Prepare-phase gating is accounted separately and is never held simultaneously
  with SelectionFenceAdmission.
- A mutation generation elects one owned driver. After exact prior-frontier
  coverage, one committed snapshot validates its FIFO request vector against a
  deterministic in-generation overlay. The overlay applies each accepted
  request's identity, retention, unique-index, group-size, schedule, gate, and
  rendered-size effects before validating the next; a rejected request does not
  alter the overlay or neighboring outcomes. Accepted envelopes reserve exact
  debt and force-seal together in FIFO order. The next generation cannot plan
  until this generation publishes its mutation frontier. Thus same-queue Push
  and BatchUpdate still fill the ordinary packed append instead of paying one
  append/apply round trip per public request.
  `mutation_sequencer_capacity_rejections_total`,
  `mutation_sequencer_deadline_expiries_total`, per-request retry count, and
  original-request-to-success age are distinct metrics. A capacity-rejected
  request keeps no sequencer place and retries at the FIFO tail; the API promises
  bounded backpressure, not priority over newer accepted work. The closed-cohort
  overload gate nevertheless requires every one of its original request IDs to
  complete within the derived hard ceiling and reports maximum retry age, so
  starvation or livelock is visible.
- Candidate selection first reads only IDs, order fields, `not_before`, and
  encoded response lengths. Scheduled buckets can select once at their shared
  epoch. Ordinary buckets run at most one bounded reader SELECT per request in
  FIFO order against one committed snapshot, excluding already assigned IDs;
  this exactly simulates solo calls at their individual `now` values without
  creating writer transactions. A bucket dispatches only a whole-request prefix
  within the aggregate byte bound; a single oversized request uses the log-first
  one-request path without changing its public maximum or outcome.
- Command/debt reservation occurs after candidate selection but before append.
  This is safe because selection is read-only. Reservation failure causes no
  log or projection mutation.
- Requests join the coordinator before any ordering permit. The elected driver
  takes a new queue-scoped selection fence exactly once; this is not the
  existing exclusive, non-reentrant `KeyedQueueGate`. Never call nested
  `submit_operation` or `submit_commit` while holding it.
- Selection-fence membership is defined by exhaustive, no-wildcard
  `selection_fence_disposition(&QueueCommand) -> SelectionFenceDisposition`
  plus no-wildcard nested matches on `FinalizeKind`,
  `ResolvedItemMutationAction`, `ScheduleUpdate`, `PayloadUpdate`, and gate
  changes. New command or nested disposition variants fail compilation until
  classified. Candidate-affecting commands take the shared selection fence;
  every Pending-consuming Claim, including cohort and grouped modes, takes it
  exclusively. Proven leased-only and non-work commands bypass it. The exact
  classifications are normative:

  | Command | Disposition | Reason |
  | --- | --- | --- |
  | `Claim`, `CohortClaim` (all item/group/cohort modes) | exclusive | Selects and moves Pending rows; no two selection domains may overlap. |
  | `CreateQueue`, `Push`, `ReplacePending`, `UpdateFields`, `UpdateFieldsBatch` | shared | Establishes or changes queue state, candidate rows, ordering, schedule, gates, or rendered response size/content. `ScheduleUpdate::{Keep,Set}` and `PayloadUpdate::{Keep,Set}` are matched explicitly. |
  | `MutateItems::{Purge,Replace}` with every gate-change disposition | shared | Can remove, create, unblock, reschedule, reprioritize, or resize candidates. |
  | `LeaseExpired`, `CohortExpired`, `FenceLease`, `UnfenceLease`, `PauseQueue`, `ResumeQueue`, `PurgeItems`, `SetGates` | shared | Can change Pending membership, reclaimability, queue admission, or gate eligibility. |
  | `Finalize` with outcomes and `CohortFinalize` with `Retry`, `Release`, or `Rearm` | shared | Returns leased work to Pending or changes its next eligibility. A `Finalize` command takes the maximum disposition over every outcome: any Retry/Release/Rearm makes the whole command shared, even when mixed with Complete/Fail. |
  | `Finalize` with only `Complete`/`Fail`, and `CohortFinalize` with `Complete` or `Fail` | bypass | Moves only named leased rows to terminal state and cannot create a Pending candidate. Empty Finalize is invalid before classification. |
  | `RenewLease`, `CohortRenewLease`, `ReassignLease` | bypass | Changes only named leased rows without changing Pending membership. |
  | `WriteSideRecords`, `AdvanceInstanceFence` | bypass | Writes explicitly non-work state. |

  A sealed driver vector takes the maximum disposition across all of its
  commands (`bypass < shared < exclusive`); a bypass Complete packed with a
  mixed Complete+Retry Finalize therefore takes the shared fence for the whole
  append. Compatibility never downgrades this aggregation.

  The keyed fence uses Tokio's fair, write-preferring FIFO `RwLock`: once an
  exclusive waiter queues, later shared waiters cannot pass it. Every
  acquisition deadline follows one composition rule: start with the next power
  of two above 2× measured p99 and the stated floor, then raise it when necessary
  to cover the complete downstream hold of one legal predecessor plus 5 s
  scheduling slack; a required value above the reviewed hard cap blocks
  activation. The one-predecessor premise is structural, not statistical:
  ClaimQueueTurn and the mutation sequencer each grant one active queue turn;
  their admissions retain at most one queued turn; and the four-slot/eight-driver
  Claim plus twelve-slot/twenty-four-driver shared admissions permit at most one
  semaphore wave. With 5 s coverage/work, 30 s pre-position, and 30 s post-position
  maxima, a fence holder lasts at most 70 s, so the fence-acquisition hard cap is
  75 s. A Claim/shared slot holder can additionally spend 5 s borrowing the pool
  and 75 s acquiring the fence before 5 s coverage plus 5 s work, so both slot
  hard caps are 95 s. A mutation-sequencer predecessor can spend 5 s on
  precoverage, 95 s acquiring its slot, 5 s borrowing, 75 s acquiring the fence,
  5 s on delta coverage, 5 s on work, and 60 s in publication, so the
  mutation-sequencer and ClaimQueueTurn hard caps are each 255 s. Each turn
  admission enforces one active plus one queued turn, hence an admitted turn has
  at most one predecessor. Pool
  borrow remains 5 s and must be structurally immediate after one of the
  four-plus-twelve matching slots is held; qualifying lanes require p99 ≤100 ms
  and zero pool expiry. Drain/delta coverage retains its separate 5 s ceiling.
  This
  captures the active shared generations across queues; same-queue requests
  occupy one FIFO generation that can contain eight public requests. Expiry releases/retries and contributes to
  `claim_selection_fence_acquire_ms` and
  `claim_selection_fence_claim_starvation_total`. S3b lands this table and the fence machinery inert; no production append site
  changes exclusion domains before exact committed coverage exists. S5
  atomically activates all dispositions while cutting item Claim to log-first.
  S3g prepares grouped/cohort full-row response materialization and S3c activates
  it before the committed pools serve. S5 then takes SelectionFenceAdmission and
  the exclusive fence before grouped/cohort candidate selection, exactly waits
  both frontiers, materializes every bounded `ClaimedItem` vector on the already
  borrowed driver snapshot before append, retains the fence continuously through
  append publication, and advances queue-scoped `last_claim` with its own
  Claim/CohortClaim position. The append-publication hook releases its fence
  before apply; response completion exact-waits apply and sends retained data
  without `finish_rendered_claim`, `render_claimed`, or a pool borrow. Shared append sites use the same
  one-way admission/gate → mutation sequencer or ClaimQueueTurn → driver slot →
  pool → selection-fence order. No site class takes slot→turn/sequencer or fence before an
  earlier resource.
- Every shared holder records its last candidate-mutating position and releases
  after durable append and apply-unit publication (not apply commit). Every exclusive holder holds from
  caught-up candidate selection through append publication and records
  queue-scoped `last_claim` before release. It waits the prior Claim frontier
  before acquisition as an optimization, then re-reads and exactly waits both
  `last_claim` and the drained candidate frontier inside the fence. These waits
  never return merely because the apply queue is empty or not-yet-ready.
- Live exact coverage comes only from
  `AsyncProjectionApplyCoordinator::applied_high_water`, advanced after the
  apply transaction commits and before progress notification. Recovery seeds it
  once from the dedicated committed projection connection after authoritative
  tail equality. Every reservation has an RAII state: it may cancel only before
  append yields a durable position; after that transition, owned work must
  publish it and any drop/panic before publication immediately poisons the shard
  and wakes waiters. Process loss is resolved by authoritative recovery.
  With no applied high-water, a Ready entry is ineligible while any earlier
  same-shard Ready exists. At any high-water, a noncontiguous Ready entry waits
  while **any** same-shard reservation is outstanding; reservation deque index
  is never treated as log order. Only after no reservation remains and the
  bounded no-progress deadline expires does the coordinator poison a residual
  position gap. S3p uses two Fireweed-owned phases. A pre-position budget covers
  linger, `produce_lock` queueing, encode, and leader election; expiry cancels
  reservation and returns retryable Backpressure. Immediately before invoking
  `engine.produce`, the leader transitions every co-sealed waiter to the
  post-position phase; timeout/error during `engine.produce` or subsequent
  `advance_high_water`→`put_json` is ambiguous, poisons the shard, and never
  reuses positions. Followers have no independent timeout: they inherit the
  leader's typed result. Both phase defaults are 30 s in production/benchmarks.
  Under the packer mutex, the leader atomically checks/cancels the pre-position
  timer and marks the group post-position before entering `engine.produce`; a
  late pre-position timer cannot cancel a transitioned group.
  A reservation head watchdog uses their sum plus 1 s scheduling slack; it does
  not add `PACK_LINGER` twice. Its
  head-block watchdog cannot expire while that append is legally in flight.
  Once the append deadline has elapsed, expiry poisons rather than applying out
  of order. Record
  `apply_reserved_head_block_ms` and expiry count. Fence acquisition uses the
  publication-aware derived bound without poisoning an active bounded producer.
  Publication ceilings stop at apply-unit publication. Public response ceilings
  add a separately metered 30 s exact apply/high-water wait and 5 s retained-result
  delivery budget. Expiry after publication is durable ambiguity: poison/replay,
  never retryable capacity rejection. Thus one 505 s driver service has a 540 s
  response ceiling; a four-service closed cohort has 2,056.075 s; a 32-service
  same-KeyedQueueGate cohort with 31 predecessor responses and fixed re-admission
  intervals publishes in 31×540 + 505 + 1 + 0.775 = 17,246.775 s and responds
  in 17,281.775 s; an eight-round byte-split suffix
  request has 4,075 s; and the theoretical tail of 1,024
  compatible attached Claim callers (128 rounds of eight) has 64,675 s. S3m/S5
  record publication, apply wait, and retained-result delivery separately; the
  T2-derived cycle budget remains the performance stop long before these injected
  safety ceilings.
- Before exclusive acquisition, the driver first waits the current candidate
  frontier without blocking producers. Inside the fence it waits only the delta
  published by shared holders that drained during acquisition. Instrument
  selection-fence acquisition separately and
  `claim_selection_fence_hold_ms` by `wait`, `select`, `reserve`, and `append`.
  After acquiring the fence it re-reads and exactly waits both queue-scoped
  `last_claim` and the drained candidate frontier; the pre-fence wait is only an
  optimization. Post-S3c calibration slice S3m records the exact
  applied-high-water-only drain-wait p99 under shadow Claim-serialized load;
  S5 sets and records separate acquisition (75 s cap) and
  `CLAIM_SELECTION_DRAIN_WAIT_MAX_MS` (5 s cap) bounds above twice their p99.
  S3m also derives `CLAIM_SELECTION_WORK_MAX_MS` from 800-item/4 MiB
  select, full-row fields/metadata/entity/gate/schedule materialization,
  grouped/cohort shaping/invariant validation, and worst-case split/reserve/encode
  p99 with a 500 ms floor and 5 s
  cap. A required value beyond any cap blocks activation. On drain/work expiry
  it releases immediately, cancels any unused reservation, and returns the existing
  retryable backpressure error. A service call makes one pre-append attempt;
  caller retry is outside this protocol. Once PUT begins it is
  uninterruptible and timed separately; neither pre-append bound claims to abort
  object-store I/O.
- The coordinator permits only one elected driver per compatibility key, while
  `last_claim` is queue-scoped across all compatibility keys. Every Claim driver
  waits that queue frontier. Exact wait exits only on coverage, health/poison,
  cancellation/shutdown, or the existing bounded-debt failure path; unrelated
  queues have independent correctness state but share global apply latency.
- At open, epoch handoff, or poison recovery, serving remains blocked until the
  projection covers the authoritative tail; then queue frontiers initialize to
  covered/empty. Live updates use monotonic `max` within one backend epoch.
- The coordinator wraps dispatched work in its own active-driver registry; it
  does not trust `ObjectLogTaskDispatcher::drain()`. Caller cancellation drops
  only that caller's receiver. `close_and_drain` rejects queued/unsubmitted
  buckets and waits registered drivers through append/apply publication. The
  Turso product lifecycle calls it before backend teardown; process loss still
  resolves durable ambiguity by authoritative replay.
- Coordinator queues/frontiers are per queue, so unrelated queues remain
  correctness-independent, not latency-independent. Object PUT is serialized
  by the store-wide `produce_lock`, and one global apply worker/entry deque also
  serializes projection apply across queues. The plan claims no PUT/apply-level
  cross-queue latency independence. `cross_queue_append_wait_ms` and
  `cross_queue_apply_wait_ms` measure both limitations in the mixed controls.
  Committed selection/validation uses the fixed driver/outcome pools rather
  than one process-wide connection; each pool wait is metered separately. Every
  serving path that needs both a committed connection and a shared/exclusive
  selection fence uses the total order append admission/gate→mutation-generation
  sequencer when candidate-mutating or ClaimQueueTurn when Pending-consuming→
  driver-read slot (Claim four-slot or shared twelve-slot)→driver
  pool→fence→snapshot→release connection→metadata permit→produce lock.
  Any validation snapshot opened before `submit_operation` closes and returns
  its connection before gate acquisition; validation that must be protected by
  the append fence is moved inside that ordered region. The
  snapshot closes and the connection returns before object-log append while the
  fence remains held through durable publication. Bypass paths use the outcome
  pool without a fence or SelectionFenceAdmission, retaining KeyedQueueGate only
  when their existing path already holds it. No path may acquire an append gate
  or admission while holding a pooled connection, begin a pool borrow while
  holding a fence, or borrow either pool/take a fence while holding the metadata
  permit or produce lock. Driver-pool borrow has a fixed 5 s deadline and returns
  retryable `EngineError::Backpressure { resource: "committed driver read pool" }`;
  after a matching driver slot is held it must be structurally immediate with
  p99 ≤100 ms and zero expiry. The driver pool contains sixteen connections and
  the outcome pool eight, each
  with a 4 MiB configured page-cache ceiling. S-0 makes the shared serving
  reader query-only but preserves its 128 MiB cache through the S0 baseline.
  S3r then tests lowering that reader to 4 MiB under an explicit S0 before/after
  rate gate before constructing the pools. If the gate passes, the interim
  configured writer + shared reader + pools ceiling is 228 MiB. If it fails,
  retain 128 MiB and record 352 MiB through S3c; this legal result does not
  block pool construction. After S3c retires the shared reader either branch is
  224 MiB. S3r records RSS across pool construction and warm reads; these are
  configured ceilings, while M1/M2/M3 remain the behavioral memory gates.
  Recovery seeding uses one separately configured 4 MiB committed connection
  while serving admission is closed and closes it before the twenty-four serving
  readers become borrowable; recovery page-cache peak is 132 MiB and serving
  page-cache peak remains 224 MiB. Pre-materialized durable outcomes are heap,
  not page cache: slots release before append, so all eight admitted Claim drivers
  (including grouped/cohort) plus twenty-four admitted mutation generations may
  retain 4 MiB each, for a 128 MiB normal-response ceiling. A legal run-alone
  response replaces one 4 MiB lane and its exact bytes/public bound are recorded.
  No queued generation clones payload or pre-renders a response.
  Fence-taking
  operations use only the driver pool; pre-position replay/outcome lookup,
  observation, and bypass reads use only the outcome pool. Every operation that
  may append durable work fully materializes its bounded public outcome before
  append: the Claim driver bulk-reads and encodes its selected bodies under the
  4 MiB generation bound, while mutation/finalize outcomes are already determined
  by their validated envelopes. After publication, apply completion only releases
  the retained outcome to its waiter; it never borrows either pool and therefore
  cannot return capacity Backpressure for durable work.
  After request-entry high-water coverage and before a pre-position outcome-pool
  borrow, every such read takes an eight-slot OutcomeReadAdmission
  with eight active and eight queued readers. Reader 17 receives retryable
  `Backpressure { resource: "committed outcome read slots" }` before pool borrow.
  One outcome holder has a derived 5 s total connection-acquire-plus-work cap,
  inside which structural pool wait must remain ≤100 ms, so one queued wave plus
  5 s scheduling slack yields a 10 s admission-acquire
  cap. Once admitted, outcome-pool borrow must also have p99 ≤100 ms and zero
  expiry. Bypass appends take this outcome admission only when they actually read
  before append; no append gate/fence is added.
  `post_publication_response_never_borrows_committed_pool` is a source/fault gate:
  injecting reader-17 rejection after append is impossible, while failure of the
  retained-result send merely drops that waiter and leaves the durable outcome
  replayable. A four-slot ClaimDriverReadAdmission with an
  active-plus-queued capacity of eight, acquired after ClaimQueueTurn and before
  every Pending-consuming item/group/cohort Claim borrows the driver pool,
  reserves at least twelve driver connections for
  Push/Update/Retry/Purge validation under saturated Claim traffic. S3m derives
  its acquisition deadline from measured hold/wait p99 with a 500 ms floor and
  95 s hard ceiling; expiry is retryable
  `Backpressure { resource: "claim driver read slots" }`.
  A twelve-slot SharedDriverReadAdmission with an active-plus-queued capacity of
  twenty-four bounds all shared-fence validation
  borrowers before the driver pool, reserves their remaining twelve
  connections under four saturated Claims, and grants one active
  candidate-mutation generation per queue across both direct and KeyedQueueGate
  call sites. S3s derives its global-slot deadline with the same 500 ms floor
  and 95 s hard ceiling; expiry is retryable
  `Backpressure { resource: "shared driver read slots" }`. The global semaphore
  releases with the driver connection before append; only the per-queue
  generation sequencer remains live through publication. Before validation it
  exactly waits both the prior candidate-mutation frontier and queue-scoped
  `last_claim`, so identity/retention/unique-index/group-size validation observes
  every earlier mutation and every published Claim. Purge/Replace therefore
  cannot validate against a pre-Claim leased-state snapshot and turn a public
  Conflict into apply poison. Its pre-connection wait and in-fence delta
  wait each have separately derived bounds; expiry releases every resource and
  returns retryable Backpressure without an internal retry, and
  `shared_mutation_coverage_wait_ms`/expiry metrics. Every public observation
  waits its request-entry high-water before taking OutcomeReadAdmission and
  borrowing from the outcome pool;
  no outcome connection is held across apply progress. Push idempotency uses
  one short outcome-pool snapshot that closes before epoch/blob/counter work;
  validate-push and pause/intake then start one driver-pool snapshot after
  shared-fence acquisition on the connection borrowed earlier in the canonical
  order. No reader transaction spans
  object-log I/O. A fence holder never reacquires either pool. Connections return before
  uninterruptible append I/O. S3a's metadata-permit→produce-lock order is the
  suffix of this same graph, not a separate order.
- Derived apply owns the global apply-worker turn and Turso writer only after
  durable append publication; it never acquires append admission, KeyedQueueGate,
  either committed pool, the selection fence, metadata permit, or produce lock.
  A derived fence holder may wait for coordinator high-water that apply advances,
  but never acquires the writer or validates through a writer transaction.
  Atomic Turso writer transactions take no derived admission/fence. No writer
  holder may borrow a committed pool or acquire admission/fence. This one-way
  apply-worker→writer→commit→high-water-notify dependency closes the wait graph.
- No Turso writer or reader transaction spans object-log I/O.
- Append precedes projection mutation. A failure proved to precede position
  allocation cancels reservation and returns the existing error. Any error after
  `engine.produce` allocates a base offset—including `advance_high_water`—is
  ambiguous durable work: never cancel/reuse its reservation, poison/block the
  queue, and recover authoritatively. If apply fails after append, poison/block the queue
  under the existing recovery contract; replay from the authoritative log
  resolves the durable outcome.
- Apply the complete sealed command vector in log order with one writer
  acquisition and one `IMMEDIATE` transaction. Never split it by waiter before
  SQL apply. Consecutive compatible Claim or Finalize commands use set-based
  statements inside that transaction.
- When ordinary packer waiters co-seal, transfer/merge every participant
  reservation into the leader and re-charge the actual packed commands/debt;
  followers never cancel debt that the leader publishes.
- Every public call remains bounded and cancellation-safe. Dropping one waiter
  does not cancel a dispatched batch; it also does not lose the other waiters'
  outcomes. If its command is already durable, its lease stands and returns
  through ordinary expiry/reclaim even though the caller lost the token.
- Phase capacity and steady-state lifecycle capacity are separate evidence
  lanes. Neither may borrow unmeasured projection debt from a later phase.

## Work breakdown: implementation slices

| Slice | Change | Bounded files | Exit gate |
| --- | --- | --- | --- |
| S-1 | Restore Class-S public Claim response fidelity before benchmarking. | `crates/fireweed-relational/src/claim.rs`, `crates/fireweed-turso/src/local.rs`, `docs/perf/evidence/ss-phased/ladder.md` | `class_s_claim_preserves_fields_metadata_and_entity` fails today then proves object-log × Turso Claim returns the same public payload, fields, metadata, rehydrated entity, gate keys, and schedule values as the reference path, including an item whose gate key is present but currently satisfied and an entity stored only through index fields. `index_fields` remains an internal relational carrier used by `claimed_from_class_s` through `echo_entity_document`; `ClaimedItem` has no index-fields member. Put full-row mapping in one reusable bulk-render helper for S5. This slice restores `5999aa77`'s fields/metadata loss plus the pre-existing Class-S entity and satisfied-gate-key gaps; it retains the valid conditional gate anti-join, while S3c owns catch-up coverage and S7 owns the wave harness. Mark `1787274546` diagnostic/fidelity-reduced; record a fresh diagnostic row. |
| S-0 | Prove committed selection before choosing its implementation. | `tools/fireweed-turso-compat-probe/src/main.rs`, `crates/fireweed-turso/src/local.rs`, `docs/helix/00-discover/turso-0.7-compatibility-probe-results.md` | `committed_reader_pool_candidate_semantics_and_settings` runs file-backed in `fireweed-turso` and is authoritative at the executable adapter pin defined above. The copied standalone probe is corroborative, records both pins, and may never pin below the adapter. Add one production reader configure/verify helper with cache size and busy timeout independently parameterized. Serving preserves 128 MiB through S0 at the product timeout; pool candidates use 4 MiB/100 ms. Require exact supported readback for `journal_mode=wal`, `synchronous=0`, numeric `cache_size`, exact busy timeout, then numeric `query_only=1` last; failure hard-stops. Never issue `read_uncommitted`. Exact 0.7.0 evidence also asserts: `wal_autocheckpoint=0` setter succeeds but has no readback, so keep it non-fatal and make S3r's no-explicit-checkpoint source audit plus file-backed WAL liveness/bounds authoritative; `cache_spill=1` succeeds/readbacks `1`, with setter/readback failure recorded as `adversarial_spill_unavailable` rather than a program stop; query-only writes return `turso::Error::Error` containing `Cannot execute write statement in query_only mode`. Twenty-four candidates prove that typed rejection inside a snapshot and in autocommit after it closes. Configure the adversarial writer outside the reader helper with file-backed WAL, `synchronous=0`, cache `-4096`, the product busy timeout, attested `cache_spill=1`, and no `query_only`; it holds an uncommitted 800-row/4 MiB-class multi-page apply. Sample `-wal` length without explicit checkpoint immediately before the writer, strictly after updates but before commit, and after commit. Record growth with byte delta or `no_uncommitted_wal_growth_observed`; a non-monotonic delta or observed/undeterminable checkpoint during the window is `inconclusive-no-growth-observed`, while unavailable `cache_spill` is `adversarial_spill_unavailable`. The known unavailable `wal_autocheckpoint` readback alone does not force a label. WAL growth is diagnostic, not the semantic pass condition. Each reader's first SELECT must finish within 90 ms, return committed `before`, report neither `Busy` nor `BusySnapshot`, and contribute to live-writer/no-writer control maxima. The writer commits and each held snapshot remains `before`; a second writer opens and commits wholly inside that window and readers remain `before`. A third writer holds an uncommitted newest value while every reader closes, opens a second Deferred snapshot on the same connection, and sees the second writer's newest committed value, never the third writer's value. After the third commit, a separate configured non-writer connection must observe the third writer's value. Record/assert the exact 0.7.0 `read_uncommitted="ON"` disposition before removing the old call. Correct the file-backed live shared reader through the helper without changing its cache; propagate supported configuration failures and gate `serving_reader_is_query_only_committed_and_nonblocking` across validation including `server_update_snapshot`, `render_claimed`, `server_peek`, `server_pending`, `server_pending_page`, `server_pending_range`, `server_live_items`, `server_metrics`, and recovery reads at the product timeout, plus `push_preappend_and_durable_idempotency_are_native_async` and `finalize_dispositions_match_sqlite_for_terminal_retry_release_and_rearm`. S0 establishes the only rate baseline on this corrected behavior. This semantic gate is a hard prerequisite for S3r/S5 and reruns on adapter bumps. Any semantic or supported-readback failure stops for redesign/re-review; no multi-statement autocommit fallback is authorized. |
| S0 | Make evidence settlement-aware and establish fidelity-restored phased plus mixed same-SHA controls. | `crates/fireweed/tests/ss_phased_capacity.rs`, `crates/fireweed/tests/ss_mixed_overlap.rs`, `docs/perf/evidence/ss-phased/ladder.md` | Depends on S-1 and S-0. `ss_evidence_v4_measures_settlement_and_residual` fails before the slice then passes; schema v4 measures sampling/residuals and ack/settled/process wall. `ss_mixed_overlap_baseline` records far-future Push plus Claim/Complete rates, admitted-service p50/p95/p99, achieved fill and response-byte distribution under fidelity-restored realistic payloads, append/cross-queue/epoch-acquire/emission-cursor wait, current pack wait/WAL bytes, and N=10k starvation. Add a bounded observation cohort for `server_peek`, `server_pending`, `server_pending_page`, `server_pending_range`, `server_live_items`, and `server_metrics`, recording per-operation rate/p50/p95/p99 and exact response counts so S3r can measure a cache change. Separate one-queue closed cohorts cover 32 compatible mutations, four incompatible legal Claim compatibility keys, and 32 mixed commands routed through one KeyedQueueGate key. Each uses fixed 25 ms backoff after retryable Backpressure and records settled throughput, admission errors, retry count, admitted-service latency, original-request-to-success age, and completion of every original request ID. This SHA is the only rate non-regression baseline; it does not calibrate the later exact-wait fence. |
| S1 | Align TD-010 and TP-005 with compatible request microbatching, log-first claim, intact packed apply, and the two evidence lanes. | `TD-010-object-log-turso-projection.md`, `TP-005-fireweed-performance-matrix.md`, `ss-objectlog-turso-memory-goal.md` | Depends on S0. `governing_microbatch_contract_present` fails as an `rg` contract check on the parent SHA, then finds exact log-first, settled-T2, mixed-lane, and migration-window clauses; `ddx doc validate` passes. |
| S2 | Add bounded Claim and mutation coordinators plus inert selection admission/fence primitives alongside the provisional serving pack. | `crates/fireweed-engine/src/claim_batch.rs`, `crates/fireweed-engine/src/async_commit.rs`, `crates/fireweed-engine/src/lib.rs` | Depends on S1. ClaimCoordinator rejects active+queued caller 1,025 but checks the eight-driver budget only when creating a compatibility bucket. SelectionFenceAdmission caps direct shared admission at 1,024 queued requests; the common mutation sequencer accepts both direct and KeyedQueueGate ingresses, owns the eight/800/4 MiB generation bounds, retains zero-copy request references, and rejects whichever request first exceeds two generations/sixteen requests before planning. KeyedQueueGate retains its global queued-only cap, adds sixteen active+queued requests per key, and bypass skips selection admission/sequencing. Add inert ClaimQueueTurn with one active/one queued driver per queue, four-active/four-queued ClaimDriverReadAdmission, twelve-active/twelve-queued SharedDriverReadAdmission, and eight-active/eight-queued OutcomeReadAdmission; turn cap255 s, driver-slot cap95 s, and outcome-slot cap10 s are configurable. Global slots release with their connections; Claim/mutation queue turns can remain through publication. `*_close_releases_waiter_count`, cancellation, suffix re-drive, request-17 same-key rejection, cross-ingress/third-Claim-turn capacity, no-payload-clone accounting, at/above-cliff ingress behavior, two-wave bounds, and distinct-active-key tests prove accounting. Fence remains inert. |
| S2a | Carry append-time admission provenance to the derived commit site without activating it. | `crates/fireweed-engine/src/commit.rs`, `crates/fireweed-engine/src/async_composed.rs`, `crates/fireweed/src/turso_compose.rs` | Depends on S2. Add `AppendAdmissionClass` to the defaulted `RawCommitRequest` builder: generic object-log callers remain explicitly `NonDerived`, `submit_operation` marks `KeyedPermitLive`, a derived direct caller is `SelectionRequired` or `Bypass`, atomic is `AtomicNative`, and reopen is `RecoveryOnly`. Default item Claim carries its coordinator class on its dedicated append request. `append_admission_carrier_audits_derived_dispatch_and_commit_sites` is a source-audit/test gate, not a false whole-workspace compile gate; the exhaustive match at `ObjectLogTursoCommitter::commit_replayable` and the Class-S append proves the live derived class is observable. No production fence is taken until S5. |
| S2e | Preserve named capacity failures through strict commit and facade normalization. | `crates/fireweed-engine/src/error.rs`, `crates/fireweed/src/turso_compose.rs`, focused error tests | Depends on S2. `new_contention_resources_survive_commit_rejection_round_trip` adds `keyed queue per-key waiters`, `claim coordinator waiters`, `claim queue turn`, `claim driver read slots`, `shared driver read slots`, `committed outcome read slots`, `mutation sequencer capacity`, `mutation sequencer wait`, `selection fence waiters`, `committed driver read pool`, `committed outcome read pool`, and `projection coverage` to the `CommitRejection` resource whitelist; unknown resources still normalize to `bounded resource`. The public `map_claim`/`map_push`/`map_lifecycle` submit-error mapping preserves `QueueGateError::PerKeyFull` as retryable `keyed queue per-key waiters` and global queue-full as its distinct existing retryable resource instead of flattening either to `EngineError::Storage`. |
| S3a | Enforce global object-log lock order and permit-held metadata reads. | `crates/fireweed-objectlog/src/log_engine_store.rs`, its focused tests, `docs/perf/evidence/ss-phased/ladder.md` | Depends on S2. `objectlog_metadata_produce_lock_order_is_global` fails today then proves every produce path acquires metadata-permit→produce-lock, uses a permit-held high-water helper, and cannot invert under concurrent Complete/acquire-epoch/produce. It also asserts no metadata-permit or produce-lock holder can borrow a committed pool or acquire a selection fence, making these locks the terminal suffix of the S5 order. Record append, epoch-acquire, and emission-cursor wait; S0 non-regression/zero-hang passes. This slice can land and revert without changing pack scope or apply publication. |
| S3p | Make force-sealed packs exactly scoped, charged, budgeted, and owned through publication. | `crates/fireweed-objectlog/src/log_engine_store.rs`, `crates/fireweed-objectlog/src/async_projection_apply.rs`, `crates/fireweed/src/turso_compose.rs` | Depends on S3a. `driver_vector_force_seals_and_charges_own_group` proves scope, accounting, transfer, and owned publication. Normal followers transfer to the leader. Exact envelope bytes are authoritative debt. `PackedAppendError::{BeforePosition,PostPositionAmbiguous}` is the typed result broadcast to every co-sealed waiter. The pre-position deadline covers linger/lock/encode and returns retryable cancellation; the leader switches the group before calling `engine.produce`, after which produce timeout/error or periodic `advance_high_water`→`put_json` failure is ambiguous poison. Followers never independently time out or poison a live leader. `post_produce_high_water_failure_never_cancels_or_reuses_position` and `all_waiters_receive_same_typed_append_disposition` gate every branch. Injection may use 100 ms; a separate test asserts both 30 s defaults. S0 non-regression/zero-hang passes. |
| S3f | Apply post-position poison across every asynchronous object-log product. | `crates/fireweed-objectlog/src/async_product.rs`, `crates/fireweed-objectlog/src/async_product_sqlite.rs`, `crates/fireweed-postgres/src/async_objectlog_postgres.rs` | Depends on S3p. `all_async_products_preserve_ambiguous_durable_reservations` proves memory, sqlite, and postgres compositions never cancel after base-offset allocation and reopen/rebuild authoritatively. No public read assertion belongs here. |
| S3v | Align Turso and governed fault evidence with post-position poison. | `crates/fireweed/src/turso_compose.rs`, `crates/fireweed-objectlog/src/request_id_probe.rs`, `crates/fireweed-conformance/tests/e3_governed_transaction_evidence_matrix.rs` | Depends on S3f. `after_append_before_apply_poisons_then_recovers_authoritatively` updates Turso, request-id replay, and AC-TXN-4 expectations: no durable reservation cancels; withheld-success/reopen/rebuild outcomes remain authoritative. Read-side poison visibility is deferred explicitly to S3c. |
| S3r | Prepare committed Turso outcome pools and coherent helpers without switching serving reads. | `crates/fireweed-turso/src/projection.rs`, `crates/fireweed-turso/src/local.rs`, `crates/fireweed/src/turso_compose.rs` | Depends on S-0 and S2. First test lowering the still-live shared reader from 128 MiB to 4 MiB using the independently parameterized helper and a same-SHA before/after S0-harness delta: `render_claimed`, Claim/Complete, and the S0 `server_*` observation cohort must meet ordinary >=90% rate and <=125% p95/p99. Passing keeps 4 MiB and records a 228 MiB interim configured ceiling; failing restores 128 MiB, records 352 MiB through S3c, does not block pool construction, and flags a predicted pool-cache regression for S3s to profile/rederive inside the 224 MiB post-S3c envelope before activation. Then build inert sixteen-driver/eight-outcome pools through the helper: 4 MiB, numeric `query_only=1` last, `busy_timeout<=100 ms`, supported settings exact, and S-0-proven default committed isolation with no `read_uncommitted`; busy reads map to retryable pool Backpressure. After S3c either branch is 224 MiB; record RSS delta across construction/warm reads, with M1/M2/M3 authoritative. Every `projection.rs` pre-position outcome helper named by S3c is converted here to accept a borrowed OutcomeReadAdmission connection/Deferred snapshot after request-entry coverage, so S3c's activation is caller-side in `turso_compose.rs`/`local.rs`; post-publication response paths accept retained results and have no pool parameter. The existing serving reader remains active until S3c. `committed_pools_construct_isolated_connections` proves settings/ceiling and runs eight successive commit→snapshot rounds per pooled connection over a result set larger than 4 MiB, with a live uncommitted next writer during each snapshot; every returned row carries the current committed round number, so any stale page or dirty next-round value fails. `twenty_four_pooled_committed_readers_do_not_restore_47b1a223_wal_freeze` asserts liveness, bounded WAL bytes under known writes, no explicit checkpoint invocation by source audit/counter, and records observed automatic-checkpoint/WAL behavior without inferring it from the unavailable `wal_autocheckpoint` readback. A seventeen-reader lane proves readers 1–16 are bounded to two waves, reader 17 gets pre-borrow capacity Backpressure, and fixed-25 ms retry completes every ID within 31.05 s. `post_publication_response_never_borrows_committed_pool` fails before helper conversion then passes. S0 pooled-read latency/non-regression and zero-hang pass on this independently revertible preparatory slice; S3c remains blocked if S3s cannot rederive a failing cache trial within the 224 MiB envelope. |
| S3g | Prepare grouped/cohort Claim response materialization without switching serving. | `crates/fireweed-engine/src/async_composed.rs`, `crates/fireweed-turso/src/projection.rs`, `crates/fireweed/src/turso_compose.rs` | Depends on S2 and S3r. Add an inert prepared-result carrier and full-row grouped/cohort bulk helper. `grouped_cohort_claim_materializes_before_append` fails first, then a shadow path constructs exact payload/fields/metadata/entity/gate/schedule results on the driver snapshot; preserves `validate_rendered_claim` count/order and per-item token/expiry equality; applies cohort shaping by stripping per-item tokens and returning exact `cohort_lease_token`/`cohort_id`; and proves the post-apply continuation needs no projection handle. Existing serving remains unchanged until S3c atomically selects this carrier with committed pools. |
| S3b | Land exhaustive selection-fence and mutation-generation classifiers without activating either domain. | `crates/fireweed-engine/src/command.rs`, `crates/fireweed-engine/src/claim_batch.rs`, tests in those modules | Depends on S2 and S2a. No-wildcard classifiers assign every command/nested action both fence disposition and `MutationGenerationDisposition::{Compatible(kind),Singleton,NotCandidateMutating}`. Tests prove each non-bypass append has one admission, every candidate-mutating shared command joins the sequencer, compatible FIFO vectors take the overlay path, bypass/recovery do not, and no new command silently skips either domain. Both domains remain inert. |
| S3q | Split Push reads around object-log I/O and prepare FIFO mutation-generation validation. | `crates/fireweed-engine/src/async_push_planner.rs`, `crates/fireweed-engine/src/async_composed.rs`, `crates/fireweed/src/turso_compose.rs` | Depends on S3b and S3r. `push_projection_snapshots_do_not_span_objectlog_io` makes idempotency wait request-entry high-water before borrowing one outcome connection, then releases its snapshot before epoch/blob/counter work. The inert mutation-generation driver validates up to eight compatible Push or BatchUpdate requests in FIFO against one driver snapshot plus overlay, renders separate outcomes/envelopes, and releases global slot/connection before co-sealed append while retaining only the sequencer. Keyed/complex shared mutations join singleton generations. The sequencer—not SelectionFenceAdmission—admits at most two generations/16 requests per queue across both ingresses, retains only fixed descriptors plus zero-copy request references, and renders only the active generation. If the queued generation exceeds its S3s-derived sequencer deadline, every not-yet-planned member receives retryable `mutation sequencer wait` Backpressure with no durable effect, the generation is removed atomically, and a retry with the same request ID rejoins at the FIFO tail after the predecessor; it cannot overtake in-process work. Shared mutation pre-append deadline expiry performs no hidden internal retry: one service attempt aborts the generation and returns retryable Backpressure. Atomic products keep native behavior. Differential/fault tests preserve the named semantics, suffix re-drive, and packed fill. S3c activates generations/reads; S5 adds the fence. |
| S3i | Give callerless reclaim bounded retry ownership and per-queue isolation. | `crates/fireweed-objectlog/src/reclaim_tick.rs`, `crates/fireweed/src/turso_compose.rs`, focused reclaim tests | Depends on S3q. `reclaim_backpressure_isolated_and_retried_per_queue` fails today then replaces page-wide `?` propagation for retryable per-queue contention with one owned, deduplicated round-robin retry queue. It holds at most 1,024 queue IDs, retries at 10/20/40/80/160/320/640 ms then 1 s capped cadence, drains retries before fetching another 128-row page when full, and stops only on shutdown, epoch change, poison, or a non-retryable error. Each retry re-enters the sixteen-request per-key KeyedQueueGate before the mutation sequencer; gate wait, capacity rejection, and retry age are distinct metrics. It spawns no per-queue task. In a closed injected saturated-key lane, reclaim completes within 17×540 s + 1 s cadence + 1 s scheduling = 9,182 s. Another queue in the same page reclaims immediately and the saturated queue returns every expired lease to Pending; retry depth/age, ceiling, and page-isolation metrics reconcile. |
| S3s | Calibrate mutation-generation admission and committed pools before activation. | `crates/fireweed/tests/ss_mixed_overlap.rs`, `crates/fireweed-engine/src/claim_batch.rs`, `docs/perf/evidence/ss-phased/ladder.md` | Depends on S3q, S3i, S3r, and S3p. Ignored/opt-in `shadow_mutation_generation_calibration` reconstructs the S3c composition without switching serving. Independent capacity subtests establish exact cliffs: 32 compatible mutations on one queue, four incompatible Claim keys on one queue, 32 mixed same-KeyedQueueGate commands, nine Claim queues, twenty-five shared-generation queues, and seventeen outcome readers. They do not run concurrently when asserting the deterministic request 17, key 17, queue 9/25, or reader 17; realistic payloads instead report the observed first third-generation index, and a separate combined soak stays one below every cap. All use fixed 25 ms evidence-client backoff. Realistic Push/BatchUpdate, callerless reclaim re-entry through KeyedQueueGate, zero-copy request retention, and real packed publication are exercised. It records fill plus separate ingress-capacity rejection (including `keyed queue per-key waiters`), deadline expiry, retry count/age, keyed-gate/Claim-turn/sequencer/outcome-slot/driver-slot/pool/coverage distributions. Every original mutation, four-key Claim, and above-cliff driver ID publishes within 2,021.075 s and responds within 2,056.075 s; the serial 32-command same-key cohort publishes within 17,246.775 s and responds within 17,281.775 s; all seventeen outcome IDs complete within 31.05 s. Accepted work never stalls, rejected pre-position work has no durable effect, and overload settled rates remain ≥90% of their matching S0 cohorts; failure blocks S3c. Independently derive bounds with the structural composition rule: Claim/mutation turn floor500/cap255 s; shared/Claim slot floor500/cap95 s; OutcomeReadAdmission floor500/cap10 s; coverage/outcome work floor500/cap5 s; post-slot pool p99<=100 ms/zero expiry. The 75 s fence term is carried from legal composition but not measured while production dispositions are inert. Every pre-append service call makes one internal attempt; one service publishes within 505 s and responds within 540 s. If S3r flags predicted pool-cache regression, keep the fixed sixteen/eight connection counts but profile and repartition the 224 MiB post-S3c ceiling between the writer and the twenty-four pools, recording every configured cache, warm RSS, and rate/latency delta; failure to find a partition meeting S0 and memory gates blocks S3c. |
| S3c | Atomically activate committed outcome reads, mutation generations, and coordinator-authoritative exact coverage. | `crates/fireweed-objectlog/src/async_projection_apply.rs`, `crates/fireweed/src/turso_compose.rs`, `crates/fireweed-turso/src/local.rs` | Depends on S3r, S3g, S3q, S3i, S3s, S3p, S3f, and S3v. Seed high-water after tail equality, remove shortcuts, and switch all reads in one revert unit. Public reads wait request-entry high-water, then take OutcomeReadAdmission before outcome-pool borrow. Candidate mutations follow the canonical sequencer→shared-slot→pool order, pre-wait both the candidate-mutation and `last_claim` frontiers before slot/pool, bounded-wait only their delta after any later fence, validate one overlay snapshot, release slot/connection, and retain the sequencer through co-sealed publication. S3c activates mutation generations/SharedDriverReadAdmission, OutcomeReadAdmission, callerless retry routing, grouped/cohort pre-materialized results, and ClaimQueueTurn/ClaimDriverReadAdmission for grouped/cohort planning borrowers using S3s-derived settings; `finish_rendered_claim`/post-append `render_claimed` leave serving here. The provisional writer-transaction item Claim needs neither Claim resource until S5. Selection fence remains inert. Failure/non-regression breach reverts to prior serving. Atomic Turso does not wait; postgres is deferred. |
| S3m | Calibrate Claim-turn/slot and exact fence bounds after S3c and packed Claim apply. | `crates/fireweed/tests/ss_mixed_overlap.rs`, `crates/fireweed-engine/src/claim_batch.rs`, `docs/perf/evidence/ss-phased/ladder.md` | Depends on S3b, S3r, and S4. Ignored/opt-in `shadow_claim_drain_calibration_uses_exact_high_water` runs N=100k. Isolated capacity subtests cross nine Pending-consuming Claim queues and four incompatible same-queue Claim keys; a separate below-cap combined soak adds live mutations, oversubscribed observations, and apply traffic. On isolated non-serving shadow queues it takes the real selection fence even though production dispositions remain inert through S3c. Record Claim-turn, pre-fence coverage, Claim-slot, driver-pool, fence acquire/drain/delta-coverage, shared-fence starvation, select/reserve/encode including up to eight reservation-split rounds, end-to-end serialized Claim publication-plus-apply cycle, WAL/checkpoint, and achieved concurrency separately from S3s. Driver borrow after slot stays <=100 ms/zero expiry. Derive Claim-turn above 2×p99/composed hold with floor500/cap255 s; Claim-slot floor500/cap95 s; fence acquisition floor500/cap75 s; pre-fence/drain/delta coverage and full 800-item/4 MiB selection plus split/reserve/encode work floor500/cap5 s. One driver service publishes within 505 s/responds within 540 s; a byte-bound suffix request needing eight driver rounds publishes within 4,040 s and responds within 4,075 s in the realistic-payload lane. The necessary T2 diagnostic budget is `mean_claim_cycle_ms <= 1000 × achieved_items_per_claim_vector / 4000` (200 ms at fill 800); breach stops before S5 even when safety caps pass. Any required value above its cap also blocks S5. S5 re-derives on the activated fence path. |
| S4 | Coalesce packed Claim apply and distinguish authority-first commands before cutover. | `crates/fireweed-engine/src/command.rs`, `crates/fireweed-relational/src/apply.rs`, `crates/fireweed-turso/src/tx.rs` | Depends on S3c. `packed_authority_first_claim_matches_solo_model_in_one_transaction` fails today then proves one writer/`IMMEDIATE`, per-row token/worker/expiry, exact responses, and full moved-row count before token/bearer effects in ordinary and fused Claim+Complete. Fused grouped queues remove and re-elect group summaries via the same solo-path helper; foreign-token rows retain their token/index and cause authority-first poison. Legacy outbox Claim defaults retain recovery behavior. |
| S4b | Add object-log × Turso lifecycle ownership for coordinator drain. | `crates/fireweed/src/lib.rs`, `crates/fireweed/src/turso_compose.rs`, focused lifecycle tests | Depends on S2 and S3p. `dropping_objectlog_turso_drains_registered_driver` fails today then installs an `ObjectLogTursoLifecycle` handle. Sync `ProjectionLifecycle::shutdown` uses the flavor-safe object-log runtime bridge (never nested `block_on`) to close admission and await the coordinator registry through append/apply publication. |
| S5 | Atomically activate every selection-fence disposition while cutting the provisional SQL-first item lane to log-first microbatches. | `crates/fireweed-engine/src/claim_batch.rs`, `crates/fireweed-engine/src/async_composed.rs`, `crates/fireweed/src/turso_compose.rs`, `crates/fireweed-turso/src/local.rs`, `crates/fireweed-turso/src/projection.rs` | Depends on S-0, S2, S2a, S2e, S3a, S3p, S3f, S3v, S3q, S3i, S3s, S3b, S3c, S3r, S3m, S4, and S4b. This five-file unit extends the live ClaimQueueTurn/ClaimDriverReadAdmission to default item Claim and activates the selection fence; mutation generations, grouped/cohort pre-materialized results/turns/slots, OutcomeReadAdmission, and reclaim retry are already live. It proves item/group/cohort response continuation has no projection handle or post-publication pool borrow. The canonical turn/sequencer order is asserted for all non-bypass site classes. Independent activated capacity subtests run the four-key Claim, 32-request mutation, 32-request same-KeyedQueueGate, nine-Claim-queue, twenty-five-shared-queue, and seventeen-outcome-reader cliffs; deterministic zero-body request 17 and the realistic first third-generation index are recorded separately, while a below-cap combined soak runs callerless reclaim and realistic byte-split responses. It re-derives S3s/S3m thresholds with 255 s turn/sequencer, 95 s driver-slot, 10 s outcome-slot, 75 s fence-acquire, and 5 s coverage/outcome/work caps; any breach, pool expiry, shared-fence starvation, reclaim failure, post-publication pool borrow, or T2-derived Claim-cycle breach blocks release. Same-queue Push/BatchUpdate median generation fill remains 8. Four-key Claim/mutation/driver-overload IDs publish within 2,021.075 s and respond within 2,056.075 s; the 32-command same-key cohort publishes/responds within 17,246.775/17,281.775 s; byte-split Claim requests publish/respond within 4,040/4,075 s. Fixed 25 ms evidence-client backoff and named ingress capacity rejection remain distinct from deadline expiry. Every service call makes one internal attempt, publishes within 505 s, and responds within 540 s. Pre-position expiry is retryable/no-effect; post-position ambiguity poisons/replays. Comparable non-overload S0 mixed rates remain >=90% and admitted-service p95/p99 <=125%; overload lanes have separate settled-rate ≥90% and absolute retry-age gates. T2 remains absolute. |
| S6 | Coalesce compatible Complete requests and validate direct mixed-vector ordering. | relational apply, Turso transaction adapter, focused lifecycle tests | Depends on S3p, S4, and S5. `packed_complete_is_non_rejecting_and_mixed_vector_matches_model` fails today then proves expiry between validation/apply cannot poison neighbors, adjacent fusion/unfused direct relational vectors report statement counts, and both match the exact model. `mixed_finalize_vector_takes_maximum_fence_disposition` packs bypass-only Complete with Complete+Retry and proves the shared fence covers the whole vector. No object-log lane change is implied. |
| S7 | Replace wave barriers with continuous bounded replenishment and add a streaming lifecycle lane. | `ss_phased_capacity.rs`, new streaming harness or module, evidence schema tests | Depends on S0 and S2. `ss_streaming_continuously_replenishes_to_exact_n` fails before the slice then proves one completion immediately frees a slot, exact N—not an empty suffix—terminates the run, stage queues remain bounded, and exact N settles with no residual debt. |
| S8c | Remove the obsolete serving lane while preserving upgrade recovery. | obsolete SQL-first serving path, legacy outbox recovery tests, migration notes | Depends on S5 and S6. `preupgrade_claim_outbox_reopens_after_log_first_cutover` fails before cleanup then proves a pre-upgrade committed lease still drains after new outbox writes and the SQL-first serving path are removed. Retain legacy drain/schema for at least one migration release. This is rewritten bead `fireweed-ec528b80`. |
| S8q | Qualify recovery, scale, and memory on the final serving code. | fault/conformance tests, evidence artifacts, `ss-objectlog-turso-memory-goal.md` | Depends on S7 and S8c. The full fault matrix passes; N=100k T1/T2/T3 and M1/M2/M3 pass on one SHA with exact command/evidence/host records. This is terminal bead `fireweed-59eae996`; no cleanup clause belongs to it. |
| S9 | Deferred postgres object-log read-coverage parity. | `crates/fireweed-postgres/src/async_objectlog_postgres.rs`, focused postgres tests, follow-up notes | Depends on S3c for the proven pattern but is not on the Turso B8 critical path. `postgres_claimed_targets_waits_committed_coverage` tracks the known byte-identical uncovered helper and requires committed coherent validation before Renew/Reassign/Finalize. |

“Bounded files” names the intended ownership boundary, not permission to mix
unrelated cleanup into a slice. If a slice needs more than three implementation
files, split it before execution except for S5's reviewed five-file atomic
activation/revert unit.

S3c's grouped/cohort interim rule is explicit: before its committed snapshot it
exactly waits both queue-scoped `last_claim` and the candidate-mutation frontier,
then materializes the retained result. The selection fence remains inert until
S5, so a later concurrent shared mutation may still linearize during the legacy
prepare→append window; S3c accepts only outcomes matching one complete existing
solo ordering and does not claim the window closed. S5's continuous exclusive
fence is the acceptance gate that removes that window. No S3c/S3g result may
mix rows or shaping from two orderings.

## Claim microbatch protocol

The following protocol becomes live only in S5. Before that slice, S2/S3b
types and classification remain inert and the provisional serving behavior is
unchanged; S3c's exact-coverage repair is already live and independently safe.
S5 also replaces the legacy grouped/cohort split `prepare_claim` →
`commit_prepared` route with one SelectionFenceAdmission-first exclusive operation in
`async_composed.rs`, so its fence spans caught-up selection through durable
publication and it advances the same queue Claim frontier.

For an eligible keyed bucket, the elected driver performs this sequence:

1. Requests join the keyed coordinator; no request reads projection replay state
   yet. Duplicate request IDs with the same fingerprint attach to the queued or
   in-flight original outcome; a conflicting fingerprint fails that waiter.
   The elected driver is submitted to the owned dispatcher and no longer belongs
   to any caller future.
2. With its ClaimCoordinator append admission already held, the driver acquires
   the queue's one-active/one-queued ClaimQueueTurn under its 255 s bound and
   retains the active turn through append publication. Before taking the
   exclusive fence, it exactly waits for at most the derived 5 s
   precoverage bound until projection
   covers both the queue-scoped `last_claim` and the current candidate-mutation
   frontier. Producers may continue packing while this wait runs. It then
   acquires one of four Claim-driver read slots, then borrows one committed
   connection from the sixteen-connection driver pool; each
   fixed 5 s borrow deadline is outside the selection-fence hold budget. The outcome pool
   remains reserved for rendering, observations, and bypass reads. Every shared
   appender that also needs a
   committed read first holds its append admission or live KeyedQueueGate
   permit, then uses the same bounded pool-before-fence order; no connection
   holder may acquire an append gate/admission and no fence holder may begin a
   pool borrow. An expiry returns the connection and Claim-driver slot, releases
   the ClaimQueueTurn, and returns retryable Backpressure without an internal
   retry. Once borrowed, the maximum idle/used
   connection hold is the 75 s fence-acquisition ceiling plus 5 s drain ceiling
   plus 5 s derived work ceiling (85 s); the separate 5 s borrow deadline bounds
   admission to the pool, not the connection lease.
3. With the connection borrowed but no snapshot started, the driver acquires
   the new exclusive selection fence. This drains
   prior shared producer/retry/reclaim appenders and blocks later ones until the
   Claim vector has durable positions. The fence does not directly cover
   Complete or leased-only operations. An existing KeyedQueueGate holder waiting
   for a shared fence can transitively delay a later Complete behind that queue
   permit. With at most sixteen active+queued same-key requests, S5 records
   `keyed_gate_transitive_wait_ms` and requires zero expiry at that bound rather
   than claiming the serialization absent; the conservative injected bound is
   16×540 s = 8,640 s because the permit spans response completion.
4. Re-read queue-scoped `last_claim`, snapshot the drained candidate-mutation
   frontier, and exactly wait both uncovered deltas before replay or selection,
   subject to the S3m-derived 5 s drain-wait cap and one-attempt rule. The
   separately derived 800-item/4 MiB work bound, capped at 5 s, begins only
   after coverage. Confirm projection
   health. This serializes Claim selection behind the prior Claim apply, but one
   apply covers up to 800 items. This path bypasses the current
   `!has_ready`/empty-queue early return.
5. Start the committed snapshot only after in-fence coverage, then resolve request-id
   outcomes and fingerprint conflicts per request. Return replays immediately
   and exclude their maxima from the selection budget. Do not issue
   `read_uncommitted`; S-0 proves the required no-dirty-read/stable-snapshot
   semantics directly because Turso 0.7 does not expose an effective readback.
   The former shared reader is retired; every driver and outcome connection's
   supported effective values are asserted, and the semantic isolation probe
   is repeated after Turso version changes. Driver-pool
   wait is recorded as `committed_selection_connection_wait_ms`; outcome-pool
   wait is recorded separately. Use the
   S-0-proven short `TransactionBehavior::Deferred` snapshot. Close the snapshot
   and return the connection before append while retaining the fence. The
   snapshot is never a writer transaction; S-0 failure stops activation.
6. Scheduled buckets select once at their exact shared eligibility epoch and
   partition in FIFO order. Ordinary buckets execute up to eight bounded selects
   in FIFO, each at that request's exact `now` and excluding IDs already
   assigned. Every query preserves `(priority_sort, created_seq, item_id)` order
   and initially returns lightweight IDs, `not_before`, order fields, and encoded
   lengths.
7. Choose the largest whole-request prefix within the aggregate byte bound and
   bulk-read bodies only for that prefix. Suffix requests remain in the same
   FIFO bucket with their original request structs and exact `now`; batch
   completion atomically elects a new driver when the bucket is non-empty, and
   new arrivals join behind the suffix. The head is never bypassed. If it alone
   exceeds 4 MiB, it runs alone, so a sealed eight-request bucket drains in at
   most eight driver rounds. Materialize each selected request's final bounded
   `ClaimedItem` vector from these bodies now and retain it through apply; no
   post-publication SQL render is permitted. Assign a distinct token, request identity/outcome, and command
   identity to every
   non-empty request. Empty suffix requests receive empty results and no
   command. If selected, commit/close the deferred read snapshot here, before
   command/debt reservation or object-log I/O, then return the connection to the
   driver pool.
   The snapshot is opened only after in-fence coverage; no correctness claim
   depends on whether Turso pins it at `BEGIN` or at its first SELECT.
   A newly elected suffix round is owned, not detached from its waiters. If its
   driver-ingress/turn/slot/pool/fence attempt returns pre-position Backpressure,
   it resolves every still-attached suffix waiter with that retryable error and
   no durable effect; it does not spin internally. Evidence clients retry the
   original public request after the same fixed 25 ms cadence. The 4,040/4,075 s
   eight-round and 64,640/64,675 s deep-bucket ceilings assume admitted rounds
   with zero injected expiry; injected-rejection retry age is reported separately.
8. Encode the actual Claim envelopes and reserve their exact command/debt byte
   charge. If aggregate reservation cannot preserve solo outcomes, split the
   stable request prefix and retry down to one request. A per-request rejection
   releases the fence with no durable effect for that request. Immediately
   before append, stamp each expiry from the seal-time operational clock plus
   that request's original lease duration; selection eligibility remains the
   request's original `now` or explicit scheduled epoch.
9. Force-seal only this `(queue, resolved epoch, Claim lane)` with the vector.
   The new internal append accepts `Option<u64>` fence mode. Every object-log
   produce path—not only Claim—acquires locks in metadata-permit→produce-lock
   order. Under the permit, `Some(e)`
   re-reads and compares `e`; `None` reads and stamps the current epoch without
   fencing. A permit-held high-water helper avoids re-locking. Hold both through
   PUT publication.
   A stale explicit epoch yields no durable record. A single object may contain
   the separate request envelopes in request FIFO order.
10. In the owned task, store queue-scoped
    `last_claim = max(last_claim, final Claim position)` and publish the ordered
    apply unit through a drop-safe append+publish API. Then release the selection
    fence before waiting for apply. Later producers may append; the next driver
    cannot select until step 2 observes this Claim apply.
11. Apply the intact committed vector in one ordered Turso writer transaction.
   Exact-wait that request's Claim position, then release its pre-materialized
   response only after rows and lease bearers are visible. This step never calls
   `render_claimed` or borrows a committed pool; replay reads occurred before
   append and an apply failure poisons/rebuilds instead of synthesizing a result.
12. Resolve all attached waiters and reclaim the keyed entry when empty. A
    cancelled waiter loses its response, but owned work continues and its durable
    lease remains visible until expiry/reclaim. Shutdown rejects unsubmitted
    buckets, drains submitted driver publication/apply, and cannot leave an
    `ApplyPublish` follower waiting without a publisher.

The epoch check is authoritative for Fireweed's existing in-process queue-owner
contract and durable epoch metadata. This plan does not claim cross-process
object-store compare-and-swap where the provider lacks conditional publish.

This removes the SQL-commit-before-PUT failure window and therefore removes the
need for new `fireweed_claim_outbox` rows on the new path. Legacy drain and
schema remain for a migration release so pre-upgrade committed leases can still
be appended during reopen.

## Finalize and pipeline protocol

Complete uses the same coordinator bounds and compatibility rules. It validates
each request's lease identities before append, preserves one outcome vector per
request, packs adjacent Complete envelopes, and applies their disjoint item sets
in one writer transaction. A validation failure is reported for that request;
it must not cancel valid neighboring requests before dispatch. Once commands
are packed, log order is authoritative and apply does not reorder by command
kind.

The streaming lane uses bounded queues between these stages:

```text
ingest -> enrich -> schedule -> claim -> complete -> settle
```

Each stage forms public batches of 100. With depth 8, completion of any future
immediately admits the next batch; there is no `join_all` wave barrier. Queue
capacity is the backpressure mechanism. The phased lane retains its phase
barriers for attribution, but explicitly settles projection debt before each
next phase. Client concurrency is a load generator, not correctness authority;
the queue gate and coordinator enforce ordering and exclusivity.

## Issue decomposition

| Work item | Depends on | Standalone outcome |
| --- | --- | --- |
| B-1 restore Claim response fidelity | — | Class-S public results retain fields, metadata, and entity values. |
| B-0 committed selection probe | — | Proves committed Deferred snapshots; failure blocks implementation and requires redesign/re-review. |
| B0 truthful settled evidence | B-1, B-0 | Correct timing boundary and fidelity-restored same-SHA baseline. |
| B1 governing design/test alignment | B0 | Accepted microbatch and evidence contract. |
| B2 keyed coordinator | B1 | Reusable bounded request grouping, owned drivers, and queue frontiers land alongside the provisional pack. |
| B2a append-admission carrier | B2 | Every derived commit site can observe whether a live keyed permit, direct fence admission, coordinator, atomic-native path, or recovery owns append. |
| B2e named capacity errors | B2 | Strict commit normalization preserves the new bounded-resource names. |
| B3a global object-log lock order | B2 | Metadata/produce acquisition and permit-held high-water land independently of packing. |
| B3p exact sealed packing | B3a | Scoped force seal, exact reservation transfer, and owned publication land before fence waiters exist. |
| B3f all-product ambiguity handling | B3p | Memory/sqlite/postgres object-log products never cancel post-position ambiguity. |
| B3v governed fault validation | B3f | Turso, request-id probes, and AC-TXN-4 align with poison/recovery. |
| B3r committed-reader preparation | B-0, B2 | Bounded pools and coherent helpers pass full-count pragma, latency, memory, and WAL gates without switching serving reads. |
| B3g grouped/cohort retained results | B2, B3r | Grouped/cohort full-row outcomes materialize before append and post-apply continuation has no projection read. |
| B3b inert fence/generation classifiers | B2, B2a | Every command has explicit tested fence and mutation-generation membership before either domain activates. |
| B3q mutation-generation Push phases | B3b, B3r | Compatible FIFO Push/BatchUpdate planning coalesces without spanning object-log/counter work. |
| B3i callerless retry isolation | B3q | Reclaim owns bounded per-queue contention retries and one saturated queue cannot abort the rest of a page. |
| B3s mutation-generation calibration | B3q, B3i, B3r, B3p | Above-cliff shadow evidence derives sequencer, Claim/shared/outcome admission, coverage, and pool bounds before activation. |
| B3c committed reads plus exact waits | B3r, B3g, B3q, B3i, B3s, B3p, B3f, B3v | Serving reads, retained grouped/cohort outcomes, callerless retry, and mutation generations switch atomically with committed coordinator coverage before the fence activates. |
| B3m Claim/fence calibration | B3b, B3r, B4 | Post-repair packed-apply load derives Claim-slot and fence bounds before activation. |
| B4 one-transaction Claim apply | B3c | Packed Claim SQL is set-based and transaction-count bounded before serving cutover. |
| B4b Turso lifecycle drain | B2, B3p | Product teardown closes admission and drains registered drivers through apply publication. |
| B5 log-first Claim composition | B-0, B2, B2a, B2e, B3a, B3p, B3f, B3v, B3b, B3q, B3i, B3s, B3c, B3r, B3m, B4, B4b | Compatible requests become one authoritative packed Claim; all fence dispositions activate atomically and the provisional pack retires. |
| B6 one-transaction Complete apply | B3p, B4, B5 | Finalization does not restore per-request transaction cost. |
| B7 continuous streaming harness | B0, B2 | Lifecycle load is replenished continuously and remains bounded. |
| B8c serving-path cleanup | B5, B6 | New SQL-first/outbox writes stop while pre-upgrade drain/schema remain compatible. |
| B8 fault and capacity qualification | B7, B8c | Exact recovery plus T/M gates pass on the final serving code at one N=100k revision. |
| Bpg postgres read-coverage parity | B3c | Track the known postgres helper without expanding the Turso qualification path. |

B8 is the terminal validation bead. Closing implementation beads does not
constitute success.

## Validation plan

### Correctness and fault gates

- Two simultaneous eight-request claim microbatches over 2,000 items return
  pairwise-disjoint IDs in global queue order with one non-empty token per
  request.
- Mixed request maxima, a cancelled waiter, an empty suffix, and a stale epoch
  preserve the unaffected requests' exact ordered results.
- Scheduled-epoch and ordinary claims never share a bucket. `None` and
  `Some(expected_epoch)` never share a bucket; stale `Some(e)` fails at append
  even when a current-epoch request is queued concurrently.
- Eight ordinary calls using the production `SystemClock` fill one bucket and
  remain due-correct at their distinct `now` values. Concurrent ordinary and
  scheduled drivers on one queue still return disjoint IDs because both wait the
  queue-scoped Claim frontier.
- A replayed request returns its recorded outcome before candidate allocation;
  a conflicting fingerprint fails only that request.
- The microbatch driver's differential invalid-plan matrix preserves every
  applicable `validate_claim_plan`, commit-outcome, and rendered-result
  invariant while intentionally permitting coherent per-request replay
  metadata.
- The object-log × Turso Class-S response preserves non-empty fields, metadata,
  entity document, indexes, and schedule values before and after cutover.
- Crash/fault injection before append has no lease effect. Failure after durable
  append but before/during apply rebuilds to the same leases, bearers, counters,
  and high-water as uninterrupted execution.
- Duplicate apply and overlapping replay are no-ops. Inverse reserve/append
  order waits for the possible gap filler; a position gap poisons only after no
  reservation remains and the 500 ms no-progress deadline expires. Live state
  equals full rebuild after concurrent Update→Claim and Claim→Complete
  schedules.
- A new authority-first Claim poisons before token/bearer side effects if any
  named row is not Pending; a legacy outbox-drain Claim retains its migration
  semantics. The same count gate holds in fused Claim+Complete.
  Reclaim→Update→Claim, Pause→Claim, CohortFinalize(Retry)→Claim,
  SetGates-unblock→Claim, and Purge→Claim observe exact log order under the
  exhaustive selection-fence disposition.
- Cancelling the elected driver's caller before append, after append, after
  `last_claim`, after publication, and during apply never strands attached
  waiters or high-water. Shutdown rejects unsubmitted buckets and drains every
  submitted driver. Dropping the public object-log × Turso product exercises the
  same lifecycle hook without nested runtime blocking.
- Append success followed by publisher drop poisons and wakes the shard; it can
  never cancel a durable position or leave later exact waits stalled.
- Grouped/cohort Claim semantics remain on their existing implementation paths
  but acquire the exclusive selection fence. A concurrent CohortClaim and
  microbatched item Claim on one queue return disjoint IDs without poison.
  Renew and terminal finalize semantics retain their paths and bypass only as
  listed in the normative table; candidate-producing retry/reclaim append sites
  acquire the shared fence. The full conformance suite passes.
- `cargo test -p fireweed-relational -p fireweed-turso -p fireweed-objectlog -p fireweed`
  passes for the touched feature set; focused commands are written into each
  bead's acceptance criteria.

### Structural batching gates

With an injected clock and eight compatible queued requests:

- median microbatch fill is 8 in the zero-body deterministic saturation test;
  the fidelity-restored realistic-payload lane separately reports the achieved
  whole-request fill and response-byte-bound split rate;
- compatible same-queue Push and BatchUpdate each have median mutation-generation
  fill 8 under saturation; the realistic-payload lane reports generation fill,
  whole-request byte/item-bound split rate, the observed index of the first
  request requiring a third generation, and its deliberate per-queue capacity
  Backpressure separately from deadline expiry; the zero-body deterministic
  lane alone fixes that index at request 17;
- four incompatible Claim compatibility keys on one queue, nine Claim queues,
  twenty-five shared-generation queues, and seventeen outcome readers each cross
  their deliberate ingress cliff; accepted work progresses, rejected work has no
  durable effect, fixed-cadence retry completes every original ID, and capacity
  metrics remain separate from timeouts;
- queued mutation inputs are reference-counted without cloning payload bytes;
  only the active generation renders under 4 MiB and a legal oversized response
  takes the existing run-alone path;
- scheduled buckets use one candidate query for at most 800 rows; ordinary
  buckets use at most eight bounded selects under one committed snapshot;
- one packed append carries eight envelopes;
- one Claim apply and one Complete apply each acquire the writer once and begin
  one `IMMEDIATE` transaction;
- no writer/reader transaction spans object-log I/O;
- coordinator `applied_high_water` advances only after apply commit; a reserved
  batch delayed before publication reproduces `a7ba4320` without spinning or
  false coverage, then resolves when the RAII guard publishes/cancels;
- on a fresh shard, reservations created in the inverse of append/log-position
  order still apply the lower position first without poisoning; eligible
  selection, Claim render, live/update planning, public observation reads, and
  metrics use the S3c coverage rules and never the uncommitted cursor/empty-ready
  shortcuts;
- producer/update generations still fill the ordinary packer while different
  queues validate concurrently;
- every Claim form releases its exclusive fence after append publication, so
  apply/render and response wait can overlap the next lifecycle work;
- one saturated queue's reclaim Backpressure cannot abort another queue from the
  same page; the owned deduplicated retry queue eventually returns all expired
  leases to Pending without a per-queue task;
- exclusive in-fence `wait` stays under the S3m-derived drain bound and
  `select`/`reserve`/`encode` stays under the S3m-derived work bound; acquisition, wait,
  and work buckets are distinct, and bound-expiry plus Claim/shared starvation
  counters are zero in qualifying S3m/S5 lanes (deliberate capacity rejections
  are separate); injected expiry at each precoverage, turn/slot/fence acquisition,
  drain/delta, and work boundary cancels unused reservations and returns
  retryable backpressure in one attempt, and uninterruptible append wall is
  reported separately;
- one driver service publishes/responds within 505/540 s, each four-key closed
  cohort within 2,021.075/2,056.075 s, a 32-command same-key cohort within
  17,246.775/17,281.775 s, and a whole-request suffix needing eight byte-split
  rounds within 4,040/4,075 s; these are injected safety ceilings, while the
  T2-derived mean Claim cycle remains the performance stop;
- `apply_reserved_head_block_ms` remains below the pre-position plus
  post-position budgets and 1 s scheduling slack, with no added linger; its
  expiry/poison count is zero in S3c/S5, and an injected
  post-deadline reservation poisons instead of allowing an out-of-order apply;
- bumping the epoch after pack reservation but before seal yields `EpochFenced`
  and no durable command;
- every pack leader is owned through apply publication; cancelling a leader in
  Claim or Mutate lanes leaves no follower blocked and no reservation orphaned;
- cross-queue append and apply wait are both reported; independence claims are
  limited to state/correctness, not latency;
- batch, byte, linger, queue-wait, writer-wait/hold, append, apply, and settle
  counters reconcile to the request and item totals.

### Performance and memory gates

All comparisons use one SHA, declared quiet host, public facade, batch 100, and
three repetitions; report median and all raw samples.

1. N=10k smoke for `filesystem--memory` and `filesystem--turso`, inflight 1 and
   8, with exit status, process wall, ack wall, settled wall, and projection
   high-water/debt recorded.
2. N=100k phased qualification: P1 ≥ 8,000 items/s; P4 ≥ 4,000 settled items/s;
   exact N in every phase; pending=leased=0; command exits successfully.
3. N=100k streaming qualification: end-to-end settled rate and every stage's
   queue/service latency are reported. No numeric streaming SLA is invented in
   this plan; the first valid run becomes its revision-bound baseline. The
   one-shot 300 ms first-produce-apply delay remains, then apply is strict log
   order without command-class priority. A deterministic alternating
   produce/Claim/Complete test advances both stage counters to the tail; neither
   stage may stay full while high-water advances only the other class.
4. Diagnose the P4 N=100k/N=10k settled-rate ratio. A ratio below 50% triggers
   one profile-backed slice, not another inflight increase; it is diagnostic,
   not an additional B8 pass condition.
5. Run same-SHA N=100k `filesystem--memory` and `filesystem--turso` controls.
   M1 uses their RSS-delta ratio. M2 compares Turso N=10k with Turso N=100k.
   M3 uses Turso's post-P4 versus P2/P3 peak. Record RSS, HWM, DB/WAL bytes,
   object-log bytes, each pool's effective cache size/count, the writer cache,
   the transient seeding connection/lifetime, retained-response current/peak
   bytes (including grouped/cohort lanes and any run-alone response), and the
   224 MiB serving plus
   132 MiB recovery page-cache ceilings. If the memory control cannot complete, preserve the failed
   artifact and explicit OOM/abort disposition; M1 remains unscored, not passed.

Evidence is invalid if the JSON is written before projection settlement, the
command later hangs or exits non-zero, or a phase moves apply debt into the
next phase. The launcher records full process wall and exit status; the harness
records internal ack and settled intervals.

## Risks and rollbacks

| Risk | Detection | Rollback/mitigation |
| --- | --- | --- |
| Double linger harms latency | composed p50/p95/p99 and fill at coordinator 0/1/5/20 ms | Force-seal the driver vector so it never waits a second object-log linger; choose the smallest coordinator linger that fills batches. |
| Selection fence, global PUT, or global apply worker creates head-of-line blocking | queue/fence wait by cause plus `cross_queue_append_wait_ms` and `cross_queue_apply_wait_ms` | Key coordinator correctness state by queue, reclaim empty entries, and report store-wide PUT/apply serialization without claiming cross-queue latency independence. |
| Cross-request SQL changes outcomes | differential result/token/order tests | Keep separate envelopes and result vectors; disable microbatch path on any compatibility mismatch. |
| Append succeeds and apply fails | fault injection and reopen/rebuild | Log remains authority; poison serving; the new path never synthesizes an outbox append, while the legacy reopen drain remains during migration. |
| Packed apply reorders mixed commands | model/differential mixed-pack tests | Coalesce only consecutive compatible runs inside one transaction. |
| Shared-reader 4 MiB trial predicts pooled-read regression | S3r before/after Claim/Complete/render/observation rates or S3s warm-pool latency breaches | Retain the shared reader at 128 MiB through S3c, let S3r construct pools, and require S3s to profile/rederive pool cache sizing inside the 224 MiB post-S3c envelope; if it cannot, block S3c activation and keep prior serving. |
| Throughput gain is deferred debt | ack-versus-settled accounting and phase high-water | Reject the evidence and fix the boundary before further optimization. |
| T2 remains below 4k after transaction reduction | writer/SQL/PUT/apply spans and transaction counts | Stop after one profile-backed follow-up and report the bound. B8 remains open/T2 unscored unless the governing goal is explicitly amended and re-reviewed. |

Each implementation slice is independently revertible until its governing
fault tests pass. Do not delete the old claim path or schema in the same commit
that introduces the new path.

## Tracker transition after convergence

Do not dispatch the three open beads that describe the superseded SQL-first
protocol. After Claude convergence and before implementation:

- rewrite epic `fireweed-5ecf18f2` to this log-first S-1–S8q program;
- rewrite `fireweed-59eae996` as terminal B8 qualification;
- rewrite `fireweed-ec528b80` as the S8c serving-path/live-token cleanup while
  retaining the legacy outbox drain/schema migration window;
- preserve the three closed historical beads and their closing SHAs;
- file B-1, B-0, B0–B2, B2a, B2e, B3a, B3p, B3f, B3v, B3b, B3r, B3g, B3q, B3i, B3s, B3c, B3m, B4, B4b, B5–B7,
  and deferred Bpg with the exact
  named failing tests in the slice table, parent them to the rewritten epic,
  and wire the Issue decomposition graph.

All rewritten/new beads use
`spec-id: docs/helix/04-build/writer-contention-recovery-plan.md` and the labels
`area:turso`, `area:performance`, `kind:<type>`, and `activity:build`.

## Exit criteria

- S-1–S8q acceptance criteria pass and the terminal B8 bead records the exact
  commands, SHA, host, and evidence paths.
- Claude review has no BLOCKING findings and every WARNING is folded or
  explicitly accepted with rationale.
- `ddx doc validate` and relevant Rust tests pass.
- One same-SHA N=100k run satisfies T1/T2/T3 and M1/M2/M3; P4 is measured after
  projection settlement and the benchmark process exits successfully.
- Packed Claim and Complete each demonstrate one writer transaction for eight
  compatible 100-item requests under deterministic saturation.
- Live state equals authoritative-log rebuild across the required fault matrix.
- Superseded SQL-first/outbox writes are removed only after B5/B6 and their
  migration test pass; final B8 qualification runs on that cleaned serving code.

## Review checklist

- Does every optimization preserve public per-request results and queue order?
- Is log append always before derived projection mutation?
- Can any writer or reader transaction cover object-store I/O?
- Can an incompatible request enter a microbatch?
- Can cancellation, fencing, or one failed waiter corrupt neighboring outcomes?
- Does a packed object reach apply intact and commit once?
- Are throughput claims settled, same-SHA, and process-complete?
- Is success tied to B8 outcomes rather than child-bead closure?

## Open questions and handoff

The release-driven S-0 isolation-attestation redesign converged with Claude in
round 41. Its four remaining notes are folded: every shared-reader consumer is
named; WAL labels depend on observed behavior rather than unavailable readback;
the concurrent memory-goal planner-map/latest-evidence claims are deferred to
S1 reconciliation without modifying the user's file; and S3s may repartition
the fixed 224 MiB writer/pool cache envelope while keeping connection counts
fixed. Update B-0 to this exact gate, finish it, and resume the existing
dependency graph at B0. Any future adapter-pin change reopens S-0 first.
