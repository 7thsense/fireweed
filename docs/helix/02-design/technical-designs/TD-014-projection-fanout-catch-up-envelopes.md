---
ddx:
  id: td-projection-fanout-catch-up-envelopes
  depends_on:
    - product-vision
    - prd
    - adr-log-single-source-of-truth
    - adr-full-async-storage-boundaries
    - adr-orthogonal-log-projection-composition
    - td-storage-architecture-backend-contracts
    - td-s3-object-log-sqlite-projection-mode
    - td-postgres-native-reference-mode
    - td-sqlite-native-embedded-durable-mode
  links:
    - {kind: informed_by, to: product-vision}
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: td-storage-architecture-backend-contracts}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: informed_by, to: td-postgres-native-reference-mode}
    - {kind: informed_by, to: td-sqlite-native-embedded-durable-mode}
  status: proposed
---

# Technical Design: TD-014 Projection Fanout and Catch-Up Envelopes

**Status**: Proposed; operator review required.
**Contract**: API-001 | **ADR**: ADR-012, ADR-013, ADR-015

## Scope

This proposal defines measurable capacity and correctness envelopes for one
authoritative durable log feeding a serving projection, bounded secondary
projections, and recovery projections. It generalizes the existing
`objectlog/hybrid-async` debt and lineage rules without changing them.

In scope:

- projection roles and fanout count;
- apply lag, ordered catch-up, snapshot plus tail rebuild, and backpressure;
- retention/high-water coupling, failure isolation, and bounded resources;
- exact replay/rebuild oracles and capacity evidence under load.

Non-goals:

- making any projection authoritative over the durable log;
- allowing a lagging secondary to answer API-001 reads or claims;
- changing caller-visible success, request replay, lease, or progress semantics;
- adding projection implementations, configuration, or implementation beads.

## Governing Requirements

| Authority | Requirement preserved |
|---|---|
| Product vision | Durable queue behavior is backend-independent; storage changes capacity and recovery, not integrity. |
| PRD P0-10 | A successful mutation is durable and visible on the serving read/claim path. |
| PRD P0-11..14 | Queue ownership, fencing, and scale-out remain whole-queue and single-owner. |
| PRD P0-15 | Shared tasks, connections, pending work, and memory are bounded under load. |
| PRD P0-16 | Recovery and retained history remain governed and observable. |
| ADR-013 | The log is state-machine authority; projections are rebuildable derivatives. |
| ADR-015 | Owned work crosses async boundaries; no lock/transaction spans an uncontrolled await. |
| TD-001/002/004/005 | Atomic and log-plus-projection profiles retain their existing response barriers and recovery contracts. |

## Projection Roles

| Role | Required freshness | May serve API-001? | Failure effect |
|---|---|---|---|
| authoritative log | committed head | no direct mutation result without serving barrier | fail closed for new durable mutations |
| serving projection | synchronous through the operation's committed position | yes, only on authoritative owner | no success response until visible; poison fails serving closed |
| secondary projection | bounded debt behind committed head | no while behind; diagnostic/export use only | isolate, retry, then backpressure retention/mutation as debt requires |
| recovery projection | exact snapshot position plus contiguous tail | no until full lineage validation and owner fence | discard partial image and rebuild/replay |

One physical projection may fill serving and recovery roles only when its
lineage and high-water satisfy both. Role names do not create new authority.

## Decision

**Recommendation**: adopt bounded fanout with staged catch-up.

- Each queue has exactly one serving projection on its active owner.
- A composition may configure at most **two asynchronous secondary projection
  workers per queue** before a new reviewed profile is required.
- Each secondary has an independent ordered cursor, bounded command/byte/age
  debt, and one owned worker capability multiplexed through shared pools.
- The durable append and serving apply/render complete before success. Secondary
  enqueue/apply is never part of the caller's success barrier.
- Any secondary debt that threatens memory, replay, recovery, or retention
  enters deterministic soft then hard backpressure; it never weakens visibility.
- Catch-up uses snapshot-at-position plus contiguous log tail, then ordered live
  batches. A secondary becomes usable only after exact lineage validation.

The fanout value `2` is a proposed capacity ceiling for one queue, not a product
requirement to configure two secondaries.

## Alternatives

### A. Documentation-only no change

Keep the single serving plus optional SQLite secondary behavior in TD-004 and
document each future projection independently.

Benefits: no new abstraction or configuration.

Costs: no common fanout budget, debt accounting, failure isolation, or
retention rule; each adapter can invent incompatible overload behavior.

Verdict: acceptable if no second secondary is planned; rejected as the general
envelope because the next projection would reopen every invariant.

### B. Bounded fanout with staged catch-up — recommended

Use one role-neutral projection supervisor per queue with a fixed descriptor
set, shared worker pool, independent ordered cursors, and common debt policy.

Benefits: predictable resource bounds, isolated retries, one metrics vocabulary,
and exact recovery/retention coupling.

Costs: supervisor state and per-secondary cursors add complexity; hard debt can
backpressure mutations even while the serving path is healthy.

### C. Unbounded/dynamic staged catch-up

Allow projections to attach dynamically and replay from retained history.

Benefits: flexible analytics and migrations.

Costs: fanout multiplies serialization, memory, I/O, retained history, and
failure domains; an unbounded lagging consumer can prevent reclamation forever.

Verdict: rejected. Dynamic attachment requires a separately governed export or
change-record contract, not an unbounded internal projection.

## Metrics and Capacity Envelope

Every projection `p` exposes queue-scoped measurements:

| Metric | Definition | Required bound |
|---|---|---|
| `projection_committed_head` | authoritative log command position | monotonic |
| `projection_applied_head{projection=p}` | highest contiguous command reflected after transaction commit | monotonic; never exceeds committed head |
| `projection_lag_commands` | committed head minus applied head | `<= configured_max_commands` in steady state |
| `projection_lag_bytes` | retained canonical command bytes beyond applied head | `<= configured_max_bytes` |
| `projection_oldest_unapplied_age_ms` | logical/wall age of first unapplied committed command | `<= configured_max_age_ms` |
| `projection_pending_batches` | sealed ordered batches waiting/running/retrying | `<= configured_max_batches` |
| `projection_retry_count` | apply attempts after first failure | finite per policy; exhaustion poisons projection |
| `projection_snapshot_position` | validated snapshot command position | `<= applied_head` |
| `projection_replay_tail_commands` | commands required after selected snapshot | bounded by recovery policy |
| `projection_apply_memory_bytes` | owned queued plus active apply bytes | within per-projection and node budgets |
| `projection_worker_slots` | active shared-pool capabilities | fixed by node config, not queue count |

Evidence records configuration and observed maximums. Throughput, p50/p95/p99
apply latency, and recovery elapsed time are topology-specific capacity evidence,
not portable correctness gates.

## Resource Invariants

1. `serving_projection_count(queue) = 1` while a queue is serving.
2. `secondary_projection_count(queue) <= 2` for this proposed envelope.
3. No projection owns one OS thread, runtime, connection pool, or background
   loop per queue; work is multiplexed through bounded shared pools.
4. An apply batch owns a finite byte permit before it enters a queue and releases
   it on success, retry handoff, poison, cancellation, or shutdown.
5. Ordered apply permits batch `N+1` to execute speculatively only if its visible
   high-water cannot advance before complete batch `N`.
6. Snapshot creation, hydration, and tail replay have explicit command, byte,
   task, and elapsed-work budgets.
7. Fanout serializes a committed command at most once into canonical owned form;
   projection adapters may decode independently but may not retain caller borrows.
8. No byte-capacity wait or projection I/O occurs while holding the queue
   serialization lock or durable-log transaction.

## Commit and Response Ordering

For a mutation at position `H`:

```text
1. validate and admit owned request
2. commit authoritative durable log through H
3. apply/render serving projection through H
4. persist/resolve request_id outcome through the serving barrier
5. return success
6. enqueue bounded secondary work for H (may already have happened after step 2)
```

Step 6 never moves before step 3 as a condition for success. A secondary may
lag, retry, rebuild, or be poisoned while owner-local API-001 reads, claims, and
same-body replay continue from the valid serving projection. If debt threatens
the log retention/recovery envelope, backpressure blocks new admission rather
than acknowledging work that cannot remain recoverable.

## Catch-Up State Machine

```text
Detached
  -> SnapshotSelecting
  -> Hydrating(snapshot_position, lineage)
  -> Replaying(next_position..committed_head)
  -> Live(applied_head == observed committed head)
  -> Lagging(debt within bounds)
  -> Backpressured(soft | hard)
  -> Poisoned(failed_position, error_class)
  -> Rebuilding
```

- `Hydrating` accepts only a snapshot whose source log identity, queue identity,
  schema, checksum, and command position validate.
- `Replaying` applies a contiguous prefix. Gap, overlap, divergent request
  fingerprint, checksum mismatch, or out-of-order high-water poisons the image.
- `Live` is observational; new commits may immediately make it `Lagging`.
- `Poisoned` cannot advance high-water, retention, or claim authority. Repair
  selects a new validated snapshot or resumes from the last exact cursor.

## Overload and Backpressure

| State | Trigger | Required response |
|---|---|---|
| normal | all debt below 75% of configured limits | serve and catch up normally |
| soft debt | any command/byte/age/batch limit reaches 75% | prioritize catch-up, emit warning, reduce nonessential maintenance |
| hard debt | any configured limit reached | reject or defer new mutations with typed retryable backpressure; serving reads remain allowed if authoritative |
| poison | non-retryable apply/lineage failure or retry exhaustion | freeze cursor, fail secondary-dependent reads/exports, block retention; operator-visible recovery required |
| recovery debt | snapshot plus tail exceeds recovery budget | fail serving transition closed; choose newer snapshot or increase reviewed capacity |

Backpressure is queue/node storage protection. It is not downstream API pacing,
eligibility, or claim-rate admission.

## Retention and High-Water Coupling

No projection high-water alone authorizes durable-log deletion. The retention
frontier is the minimum position required by:

- authoritative manifest/log continuity;
- validated serving/recovery snapshot lineage;
- every configured non-detached secondary within the retained fanout set;
- `request_id` and item-key replay retention;
- branch/pin or other accepted history contracts;
- poison/backpressure state whose exact recovery source is not yet proven.

A secondary that exceeds its maximum retained debt must be detached through an
explicit operator action or force hard backpressure. Silent log trimming past a
lagging cursor is forbidden. Detach records the cursor and reason, then any
future reattach starts from a new validated snapshot rather than assuming the
old tail still exists.

## Failure Isolation

| Failure | Isolation behavior |
|---|---|
| one secondary transient apply error | retry only that projection; other cursors and serving path continue |
| secondary non-retryable error | poison that projection; freeze its cursor and retention contribution |
| serving apply/render failure after log commit | return no success; resolve unknown outcome through request replay after recovery |
| snapshot corruption | reject snapshot; do not change current projection; select another source or replay |
| process crash during apply | transaction rolls back or idempotent replay converges before high-water advances |
| shared-pool exhaustion | bounded queue/backpressure; no unbounded task spawn and no cross-queue starvation |
| projection schema incompatibility | detach/rebuild under explicit migration authority; never reinterpret bytes silently |

## Exact Replay and Rebuild Oracles

At a validated committed head `H`, a rebuilt projection must match the canonical
reference over:

- queue definition, pause/gates, counters, and ownership fence;
- lifecycle totals and each item state/version/schedule;
- active lease token hashes, expiry, worker, and epoch;
- priority/group/cohort order and eligibility summaries;
- fields, metadata, entity/index state, and recurring state;
- request-ID fingerprints, stored outcomes, and retention expiries;
- metrics with exact fields and documented approximate fields;
- applied position exactly `H` with no gap or overlap.

Pass bars are zero missing accepted commands, zero duplicate transitions, zero
simultaneous active leases, zero request-replay divergences, zero read-after-
success gaps on the serving path, and byte-identical canonical checksums where
the projection contract defines canonical encoding.

## Verification Matrix Before Acceptance

| Scenario | Required evidence |
|---|---|
| fanout 0, 1, 2 secondaries under identical seeded load | exact serving outcomes; per-projection resource/lag curves; bounded shared pools |
| delay batch N while N+1 is ready | no high-water skip; ordered eventual convergence |
| transient and permanent failure in one secondary | serving and unrelated secondary isolation; correct retry/poison state |
| crash during snapshot hydrate and tail replay | partial image never serves; exact restart/rebuild oracle |
| debt crosses soft and hard thresholds | typed telemetry/backpressure and deterministic recovery below hysteresis |
| retention attempts with one lagging/poisoned secondary | no deletion past complete minimum frontier evidence |
| serving failure after durable log commit | no success; same-body request replay resolves exactly once |
| unrelated hot/cold queues | stalled queue cannot consume unbounded shared resources or stop every queue |

All load comparisons use interleaved same-run controls and declared topology.
No result requires a quiet host or absolute host-speed threshold.

## Security

- Projection descriptors and lag metrics are tenant/queue authorized.
- Snapshot and replay inputs validate tenant, queue, source-log identity,
  checksum, schema, and command range before materialization.
- Error/poison telemetry records positions and classifications, never payloads,
  lease tokens, credentials, or replay bodies.
- A secondary cannot bypass serving-path authorization or expose stale data as
  authoritative.

## Rollback and Review

No rollout occurs while this artifact is proposed. If accepted, the first
implementation must preserve the current `objectlog/hybrid-async` one-secondary
behavior and prove the generic supervisor is behavior-equivalent before a
second secondary is enabled.

Rollback disables additional secondary descriptors, drains or explicitly
detaches their cursors, and retains the existing serving plus SQLite behavior.
It never trims required history until the recomputed frontier proves the
removed secondary no longer contributes.

**Proposed**. Operator review must accept the fanout ceiling, debt semantics,
detach policy, and retention coupling before contracts or implementation work
are created. This artifact creates no implementation beads.
