---
ddx:
  id: api-workload-integration-profiles
  depends_on:
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - prd
  status: accepted
  review:
    self_hash: 3206a0ad7896fa01deb790f1dca95bddab1cbe9d8f69a761cfb041a34498450e
    deps:
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
    reviewed_at: "2026-07-20T00:01:26Z"
---

# API-003: Workload integration profiles

**Type**: consumer integration profiles, normative for adapter authors
**Version**: v1
**Status**: accepted
**Related**: API-006 (the contract these profiles use); ADR-025 (why the contract has this shape); PRD Non-Goals, FR-44 to FR-47c and FR-55

## 1. Purpose and scope

This document says how a consumer runs its workload on the Fireweed kernel contract, API-006. It adds no operation, field, option or semantics: every rule resolves to API-006 clauses, which are cited by their ids (W5, C6a and so on). Where this document and API-006 disagree, API-006 governs.

- §2 holds the rules for every consumer.
- §3 maps Snorri and §4 maps Seventh Sense onto API-006. Consumers adapt to the contract; the contract is not bent to fit their current code (ADR-025 §4).

MUST, SHOULD and MAY here constrain the consumer's adapter, never the engine.

**Notation.** A record write is `{Expect, LeaseOp, Op}`, for example `{Any, Release(t), Put{stage: done}}`. A `Put` names only the patch fields it sets, and `Put{}` is an empty patch. `t` is the token of the claim that leased the record.

## 2. Rules for every consumer

**Queues and definitions**
- **P1.** A consumer uses one queue per job type. Queues are cheap, and nothing is atomic across them (A4).
- **P2.** The adapter calls `define_queue` with the full definition before the first write to a queue. A repeat returns `created: false` (D2), so calling it on every start is safe. A `DefinitionConflict` is a deployment error. A new stage, field or index needs a new queue (D3, API-006 §5.1).
- **P3.** Every per-record value is a declared field, of at most 64 KiB. Large values are ordinary fields: there is no payload slot and no size flag (F2).
- **P4.** When the entity that owns a queue is deleted, the adapter closes the queue with `delete_queue { archive: true }` (DQ1). Closed is final (DQ3), so a queue name built from an entity id requires that the id is never reused.

**Stages and rank**
- **P5.** A stage is either work that some claim walks, or a resting stage that nothing claims: finished, cancelled, or data read by key. The engine treats all stages alike.
- **P6.** `rank` is when a record may first be claimed, in µs: the insertion time by default, a scheduled time, or a retry time. A record is claimable only once `rank <= horizon` (C1). A priority bucket is therefore a bounded offset: negative to run sooner, positive to run later. An ordering class that must be claimable now gets its own stage, not a large offset.

**Claims**
- **P7.** The select follows what the worker needs (C2, C4):
  - `Next` for independent records, in rank order.
  - `WholeGroups` where one holder per group is required. A stage whose records rely on that exclusivity MUST be claimed only with `WholeGroups`, because `Next` and `Keys` do not check group leases (C6a).
  - `Keys` for records the caller has already identified.

  No claim orders by a declared index. Work selected by a query gets a stage per kind of work or a group, or is found with `scan` and claimed with `Keys`.
- **P8.** Claims and reads name the fields they use (F4), so large values stay off the claim path.
- **P9.** A short or empty claim is a successful poll (C5).

**Settling and writes by key**
- **P10.** Settling is a write under the claim's token (W5):
  - finish or fail: `{Any, Release(t), Put{stage: <resting stage>, fields}}`;
  - retry: `{Any, Release(t), Put{rank: <retry time>, fields}}`;
  - give back: `{Any, Release(t), Put{}}`;
  - remove: `{Any, Release(t), Delete}`.

  History, audit and acknowledgements for a transition travel in the same patch as fields.
- **P11.** Work that outlasts its lease renews the claim with an `Extend` before `until` (W14).
- **P12.** A write by key without a token applies only to a Free record (W4). A `Leased` rejection means a holder is working on the record: the adapter retries once the holder settles or the lease lapses, or leaves the change to the holder. A consumer never uses the `Operator` trait or the admin handle (OP4).
- **P13. Cancel without the operator.** Records that may be cancelled as a set carry a gate for that set. To cancel, the adapter:
  1. blocks the gate with one gate op in each queue that holds the set (W9), which stops every new claim of it at once;
  2. moves each Free record of the set to a resting stage by key;
  3. retries each `Leased` one until its holder settles or its lease lapses, and moves it only if it is still in a work stage.

  The blocked gate keeps a released record from being claimed again, so the retries converge. Once no record of the set is left in a work stage, the gate may be reopened.

**Pacing, retention and outcomes**
- **P14.** Fireweed applies no rate admission and enforces no downstream rate limit (PRD Non-Goals). A consumer paces itself with `max_records`, claim cadence, a future rank and whole-group claims. A gate is a binary pause switch, never a rate limiter.
- **P15.** Records never expire. A consumer that needs retention deletes records by key (W7) or closes the queue (DQ1).
- **P16.** A request holds at most 1000 record writes (API-006 §6.8); larger batches are split, each request with its own `request_id`. An `Unavailable` result is resolved by resending with the same `request_id`, or, for an id-less write, by a `read` of what it touched (RQ2, RQ4).
- **P17.** The embedding host passes its lease clock as the constructor `Clock`, and lease reclaim runs on it (V4). No consumer drives reclaim.

## 3. Snorri

Each shard of a Snorri deployment is one queue, and every shard queue has the same definition. The shard manifest lists them.

### 3.1 Model

- **A workflow instance is a group.** Transition inputs and state-store facade items carry `group` = instance id. Their stages are claimed only with `WholeGroups`, and group exclusivity spans stages (C6a), so one holder has an instance at a time across both. There is no projection fence.
- **Record kinds are stages.** Each claim names its kind; there is no `record_kind` filter.
- **Timers, effects and awaits have no group.** They keep the instance id in a field, so a leased timer or effect never holds the instance. Effect dispatch and timer firing change no instance state. If dispatch state ever moves into the instance projection, effect items carry the instance group and `effect` is claimed with `WholeGroups`.
- **The instance record is in a stage nothing claims.** In normal operation only transition and facade-commit units write it, and both run under the instance's group claim. A migration or recovery writes it with `Version(v)`.
- **Per-transition history is on the settled items.** Each settled input carries its transition's metadata, acknowledgement, next state and audit as fields.
- **Awaits are records read by key.**
- **The shard manifest is queue metadata.**

### 3.2 Definition

Stages, in this order (inserts name their stage):

| Stage | Holds | Claimed by |
|---|---|---|
| `transition` | Transition inputs: start inputs and the continuations of deliveries, timers, effect results, handler timeouts and dispatch outcomes. `group` = instance id; `rank` = `scheduled_at` µs. | `claim(transition, WholeGroups{n})` only |
| `state_store` | State-store facade items, `group` = instance id. | `claim(state_store, WholeGroups{n})` only |
| `effect` | Effect dispatch items. | `claim(effect, Next)` |
| `timer` | Timer awaits, `rank` = fire time µs. | `claim(timer, Next)` |
| `transition.done` | Settled inputs with their per-transition history. | nothing; `scan` and `read` |
| `effect.done`, `state_store.done` | Dispatched effects and finished facade items. | nothing; counted by `stats` |
| `transition.cancelled`, `effect.cancelled`, `state_store.cancelled` | Cancelled work, one stage per kind. | nothing; counted by `stats` |
| `await` | Open external-event and effect awaits. | nothing; read by key |
| `await.done` | Consumed awaits and fired timers. | nothing; read by key |
| `await.dead` | Effect awaits whose handler exhausted its attempts. | nothing; read by key |
| `result` | Effect results. | nothing; read by key |
| `instance`, `instance.closed` | The instance record, before and after the final transition. | nothing; read by key |

**Fields.** Snorri's `metadata`, `index_fields` and `entity` values become declared fields. The instance projection (`projection`) and a settled input's `next_state` and `audit` are `Bytes` fields of at most 64 KiB each. Claims and hydrating reads name the fields they need (P8).

**Indexes** (22 of 32; they serve `scan` and `aggregate` only):
- Snorri's 19 production indexes, without `unique`. The two lifecycle indexes key on `Stage` instead of `record_kind`: `by_stage_key` and `by_stage_scheduled_at`.
- `by_tx_request (tx_request_id)`, `by_consumed_event (consumed_event_id)` and `by_instance_revision (instance_id, state_seq)` over settled inputs.
- There is no `by_record_kind_key_instance`: await routing reads by key (§3.4).

**Keys.** There are no unique secondary indexes, so the key of a run-target item carries the target identity, for example `(tenant, run, kind, version, target)`. Request-id redelivery MUST yield the same key, which keeps the redelivery dedup that `client_item_key` gives Snorri.

### 3.3 Where Snorri's side records go

Everything below is written in the unit that produces it.

| Side record | On API-006 |
|---|---|
| Instance projection and its projection fence | The instance record `snorri:v1:instance:{id}` in `instance`, with the projection in a field. Its field `idempotency_key` puts it in `by_stage_key` for the instance listing. No fence (§3.4). |
| Transition metadata `snorri:workflow:transition:{rid}` | Fields `tx_request_id`, `instance_id` and `state_seq` on the settled input. |
| Input acknowledgement `snorri:workflow:input:{event}` | Field `consumed_event_id` on the settled input. The ack lookup is `scan(by_consumed_event, eq=[event], limit: 1)`, which observes every acknowledged settle (V3). |
| `state:…`, `audit:…`, `audit-digest:…` | Fields `next_state`, `audit` and `audit_digest` on the settled input. |
| Await index `snorri:v1:await-index:{k}` | The await record `await:{k}` in `await`, in the emitting instance's shard queue, with field `instance_id`. |
| Timer awaits | The timer record `timer:{k}` in `timer`. |
| Effects and continuations | Effect items in `effect`; continuations in `transition` with the instance group. |
| Effect results | The result record `result:{…}` in `result`. |
| Replay markers, state-store commit records | None: request-id replay covers them (RQ2). |
| Migration and recovery audit markers | Fields on the instance record, written by the migration or recovery unit with `Version(v)`. |
| Shard manifest `snorri:v1:workflow-shard-manifest` | Queue metadata of shard 0's queue, entry `snorri.shard_manifest` (API-006 §5.2). |

Records live until Snorri deletes them (P15).

### 3.4 Transitions, deliveries and facade commits

**A transition** is one unit per instance, carrying the transition's `request_id`:
```
claim(transition, WholeGroups{n})             → the inputs of up to n instances, token t
read{keys: [instance keys, emitted await keys]}  → projections with versions v, await states
Unit {
  { input_key,         Any,                   Release(t), Put{stage: transition.done,
                                                              fields: {tx_request_id, instance_id, state_seq,
                                                                       consumed_event_id, audit_digest, next_state, audit}} }
  { instance:{id},     Version(v),            None,       Put{fields: {projection}} }
  { effect:{k}:attempt:1, Absent,             None,       Put{stage: effect, rank, fields} }
  { await:{k},         Absent | Version(v_k), None,       Put{stage: await, fields: {instance_id, …}} }
}
```
- The unit settles every claimed input of the instance.
- The start input's unit inserts the instance record with `{Absent, None, Put{stage: instance, fields}}`. The final transition also moves it to `instance.closed`. A one-shot workflow inserts it straight into `instance.closed`.
- The group claim serializes an instance's transitions and facade commits, so `Version(v)` on the instance record fails only against an out-of-band writer, such as a migration or a recovery. There is no fence record and no projection-conflict retry loop.
- An emitted await key that is absent is inserted with `Absent`. One in `await.done` (awaited again) is moved back to `await` with `Version(v_k)`. One already in `await` makes the transition reject the program before writing.

**Shard routing.** Await, timer and result records live in the emitting instance's shard queue. External await keys carry no instance id, so a delivery finds its shard with `read{keys: ["await:{k}"]}` on every shard queue in the manifest, in parallel:
- found in `await` in queue Q: deliver in Q;
- found in `await.done`: `AlreadyConsumed`;
- absent in every shard: `NoSuchAwait`.

If any shard returns `Unavailable`, the delivery fails and never reports `NoSuchAwait`. A key open in more than one shard is ambiguous, and the delivery fails. Effect results route the same way by effect key.

**A delivery** never writes the instance record. It is one unit in Q:
```
{ await:{k},        Version(v), None, Put{stage: await.done, fields: {consumed_event_id}} }
{ continuation_key, Absent,     None, Put{stage: transition, group: instance, rank: scheduled_at µs, fields} }
```
Two deliveries of one await race on `Version(v)`; the loser re-reads and reports `AlreadyConsumed`.

- **Timers:** `claim(timer, Next)`, then `{Any, Release(t), Put{stage: await.done}}` plus the continuation.
- **Effect results:** as a delivery, plus `result:{…}` inserted into `result`.
- **Handler timeouts and dispatch outcomes** never write the instance record. Each CAS-updates its await with `{Version(v), None, Put{fields}}`, inserts the next `effect:{k}:attempt:{n+1}` or moves the await to `await.dead`, and inserts a continuation into `transition` that carries the outbox change for the instance's next transition.
- **Facade commits** (`state_store_commit`) claim `state_store` with `WholeGroups` and write the instance record with `Version(v)` in the same unit as their settles, as a transition does.

### 3.5 Live mutations

- **CancelRun** follows P13. Every work record of a run carries the run's gate `snorri.run:{run}` (a run id outside the gate-key syntax is hashed). The adapter blocks it in every shard queue, then pages `scan(by_run_status, eq=[tenant, run])` and moves each Free hit in a work stage with `{Exists, None, Put{stage: <kind>.cancelled}}`, taking `<kind>` from the hit's stage. A `Leased` hit is retried; a running effect is therefore cancelled after its holder settles or its lease lapses.
- **CancelItems:** the same move by key for each item, with `Leased` retried.
- **DeleteNotStarted:** `read{keys}`, then `{Version(v), None, Delete}` for each record that is Free with `attempts == 0`.
- **PauseRun, ResumeRun, SuppressRules, UnsuppressRule** use membership gates. `snorri.paused`, `snorri.suppressed`, `snorri.failed` and each per-rule gate are blocked once, with unit gate ops. PauseRun pages `scan(by_run_status, eq=[tenant, run])` and writes `{Exists, None, Put{gates: Edit{add: {snorri.paused}}}}` per hit; ResumeRun removes the gate. SuppressRules adds each rule's gate to the records the rule selects. A gate `Edit` commutes, so no version check is needed. Lifecycle classification reads `Record.gates`.
- **RescheduleItems, RecycleRule:** `{Exists, None, Put{rank: <new run time> µs}}`. A `Leased` record counts as conflicted.
- Run and rule mutations are paged and are not atomic across pages. A `Leased` record reports `Leased` and is retried (P12).

### 3.6 Call sites

| Snorri call | On API-006 |
|---|---|
| `create_queue` | `define_queue` with §3.2's stages, fields and indexes (P2). |
| `push`, `push_with_request_id`, `push_batch_with_request_id` | `write` insert units naming their stage, at most 1000 per request, with request ids `{rid}:{chunk}` (P16). |
| `upsert`, `create_work_items` | `{Any, None, Put{stage, rank, group, gates: Replace, clear_fields: true, fields}}`, which replaces the record whole; `created` tells Inserted from Replaced. The group must equal the stored one (W8). A `Leased` rejection is an error to the caller. To refuse replacing a record in a resting stage, the adapter reads first and writes with `Version(v)`. |
| `claim`, `claim_at` | `claim(effect \| timer, Next, horizon)` |
| `claim_by_query`, `claim_by_query_at` | `claim(transition \| state_store, WholeGroups{n})`; `claim(effect \| timer, Next)` |
| `claim_with(metadata_equals)`; push, claim and commit sequences for bootstrap, migration and the manifest | The stage replaces the filter. Request-id replay replaces replay markers. A migration or recovery is one unit that CAS-writes the instance record. The manifest bootstrap is one metadata CAS (below). |
| Key-range claim, then release of extras | `claim(stage, Keys)`, with its skip reasons (C2) |
| `commit`, `commit_multi_claim` | The transition unit (§3.4) |
| `ack`; `nack` with release | `{Any, Release(t), Put{stage: <kind>.done}}`; `{Any, Release(t), Put{}}` |
| `reschedule` | `{Exists, None, Put{rank}}`; `Leased` counts as conflicted |
| `mutate_items` | §3.5 |
| `purge(force)` | `{Any, None, Delete}`, retried on `Leased` (P12) |
| `side_record`, `live_item`, `claimed` | `read{keys}`, or the input-ack scan (§3.3) |
| `side_records_by_prefix` | Instance listing: `scan(by_stage_key, eq=[instance])`, and `eq=[instance.closed]` for finished instances. Audit hydration: `scan(by_instance_revision, eq=[inst])`, then `read{keys, fields: Some([next_state, audit])}`. |
| `explain_commit`, `recover_mirrored_transition_in_queue` | `scan(by_tx_request, eq=[rid], limit: 1)`, then `read{keys}` of the settled input in `transition.done` |
| Shard manifest read and bootstrap | Read: `read{keys: [], queue: true}`. Bootstrap: a unit with only the metadata op `{expect: Version(0), patch: {snorri.shard_manifest: Some(Bytes(manifest))}}`. A losing concurrent opener gets `MetadataVersionMismatch` and reads the winner's manifest. |
| `metrics` | `stats`. `count_work` counts transitions: pending = free(`transition`), leased = leased(`transition`), complete = free(`transition.done`), failed = free(`transition.cancelled`). |
| `metrics_by_query`, `grouped_aggregate`, `declared_bucket_segment`, `range_scan` | `aggregate(Dims[Attr(Stage), Lease])`, `aggregate(Dims)`, `aggregate(Ranges)`, `scan` |
| `reclaim_expired_at` | None: Snorri's lease clock is the constructor `Clock` (P17). |
| `commit_capabilities`, `hot_projection_capabilities`, `projection_control` | None. Projection control is on the admin handle, which Snorri never links (OP4). |

## 4. Seventh Sense

Seventh Sense is written in Scala, so it cuts over once a transport over `Store` exists; API-006 defines none (API-006 §8). All its queues share one tenant, because a job worker claims across every account of its job type and there are no cross-queue claims.

### 4.1 Queues

| Queue | One per | Key | Stages | Group | Gates |
|---|---|---|---|---|---|
| `jobs:{type}` | job type | `jobId` | `[ready]` | none | `acct:{a}`, `conn:{c}` |
| `connectors:{type}` | connector type | `connectorId` | `[ready]` | none | `acct:{a}`, `conn:{c}` |
| `marketo-lead:{c}` | connector | task id | `[ready]` | lead id | none |
| `marketo-email:{c}` | connector | task id | `[ready]` | email id | none |
| `callbacks:{job}` | job | action id | `[queued, scheduled.batch, scheduled.single, succeeded, failed]` | callback id | none |

Account and connector live in declared fields, and so does campaign where a queue carries it.

### 4.2 Jobs and connectors

| Seventh Sense call | On API-006 |
|---|---|
| `TJobsQueue.create`, `TConnectorsQueue.create` (ignore on conflict) | `{Absent, None, Put{gates: Replace({acct:a, conn:c}), fields}}` in the type's queue; `Exists` means ignore. The default rank, the insertion time, matches `last_processed_at DEFAULT now()`. |
| Account and connector status | The status writer is the only writer of gate state. A status change is one gate op in each job-type and connector-type queue (W9). A new connector's gate is set `Blocked` when the connector is created, before any of its jobs are inserted. Create units carry no gate op, so a create never overwrites a concurrent status change. |
| `handleNextJob(type, asOf, timeout)`, `handleNextConnector(type, asOf, timeout)` | `claim(ready, Next, max_records: 1, lease: timeout, horizon: asOf)`, then the effect, then `{Any, Release(t), Put{rank: nextProcessingAt.getOrElse(asOf) µs, fields: {inactivityCount}}}` or `{Any, Release(t), Delete}`. |
| JobContentLoader's legacy type | No legacy queue. At cutover each legacy row is written into the current type's queue with `Absent`, so an existing current-type row wins. |
| `deleteByJobIds`, `deleteByConnectorIds` | `{Any, None, Delete}` on the id in every type queue; the type set is fixed, so the keys are known without a scan. `Leased` is retried (P12). |

### 4.3 Marketo enrichment

| Seventh Sense call | On API-006 |
|---|---|
| Lead `create` (with a connector row) | `define_queue` of the connector's two queues, the connector record inserted with `Absent` in its connector-type queue, then each task `{Absent, None, Put{group: leadId, fields}}` in `marketo-lead:{c}`. These are separate requests, and a crash between them is harmless. |
| Lead `takeNextTasks(asOf, N)` | `claim(ready, WholeGroups{max_groups: N}, horizon: asOf)`: every claimable task of each of up to N leads, in one claim (FR-47b). |
| `hasNextTasks(asOf, N, maxWait)` | `stats{groups: {stage: ready, limit: N}}`, with `horizon: asOf` only when `asOf` is later than now (S2); then `ready_groups >= N \|\| now - oldest_ready_since > maxWait`. `oldest_ready_since` is when the longest-waiting ready task became Free, at its insert or its last release. |
| `updateRetryInfo` | `{Any, Release(t), Put{rank: nextRetryAt.getOrElse(now) µs, fields: {retryCount}}}` |
| `delete` (success, or retries exhausted) | `{Any, Release(t), Delete}` |
| Email enrichment (`create`, `takeNextTasks(asOf, maxTasks)`, `updateRetryInfo`, `delete`) | The same in `marketo-email:{c}`, grouped by email. `takeNextTasks` is `claim(ready, WholeGroups{max_groups: 1}, max_records: maxTasks, horizon: asOf)`: the first ready email's tasks, or a prefix with `partial_group` when that email has more than `maxTasks`. A batch never mixes emails, because the worker enriches the whole batch with one email. |
| Connector deletion | `delete_queue{archive: true}` on both Marketo queues, and `deleteByConnectorIds`. |

### 4.4 Callbacks

A queued action and the scheduled action it becomes share one id, so they are one record that moves between stages.

| Seventh Sense call | On API-006 |
|---|---|
| `createOrUpdate` | `{Any, None, Put{stage: queued, rank: 0, group: callbackId, fields}}`, sparse, without `clear_fields`. With `rank: 0` queued records claim in insertion order, and a record moved back from a scheduled stage regains its place. The group is set at the first insert (W8). Ids are deduplicated per request, since W1 rejects a repeated key. A `Leased` rejection (the scheduler holds the record) is retried. |
| `findByJobId(jobId, max)` | `claim(queued, Next, max_records: max)`. The claim is fresh (V2), so the scheduler's find, move, find loop never sees what it already moved. |
| Scheduler: create the scheduled action, then delete the queued one | One settle per action: `{Any, Release(t), Put{stage: scheduled.batch \| scheduled.single, rank: executed µs, fields: {state, retryCount, scheduled, executed, batchChecksum, single, stats_type, …}}}`. Batch members share `rank` = the earliest `executed`, so they become claimable together. An action that fails scheduling is released with `Put{}`. |
| `findReadyToExecuteByJobId` | Batches: `claim(scheduled.batch, WholeGroups{max_groups: 1})`. The batch is complete iff the claimed count equals `batchChecksum`, as `isFullBatch` checks; otherwise the executor releases it with the rank of its next check. Singles: `claim(scheduled.single, Next, max_records: maxSize)`. The executor claims both stages, and a batch and a set of singles are separate downstream calls. |
| `complete`: success | `{Any, Release(t), Put{stage: succeeded, fields: {state, completed}}}` |
| `complete`: failure with retries left | `{Any, Release(t), Put{rank: <retry time> µs, fields: {state, retryCount, completed}}}`, in its scheduled stage |
| `complete`: retries exhausted | `{Any, Release(t), Put{stage: failed, fields: {state, retryCount, completed}}}`, so `calculateStats`, which groups by `state`, counts it as Failed |
| `calculateStats` | `aggregate` over `by_stats (stats_type, scheduled, state, isEnrolledUsingOpenRateFilter, engagementClass, personalizationClass)` with `eq=[Bool(true)]` and `Dims[TimeBucket(scheduled, 3_600_000), Attr(state), Attr(isEnrolledUsingOpenRateFilter), Attr(engagementClass), Attr(personalizationClass)]`. `scheduled` is a `Time` field. A record is in an index only if it has every key (Q1), so every scheduled record writes all `by_stats` keys, with `Str("")` for an absent card attribute, which Seventh Sense reads back as `None`. `stats_type` is true when the type is empty or in `typesForStats`. `withAllTypes` is the same aggregate with an empty `eq`. |
| `deleteExpiredByJobId(jobId, asOf)` | Seventh Sense's own retention: page `scan(by_scheduled (scheduled), upper: Included(asOf))`, skip hits in `queued`, `read{keys}` the rest, and delete each with `{Version(v), None, Delete}`. The version check keeps a concurrent re-queue alive, and a `Leased` record is left for a later run. |
| Job deletion | `delete_queue{archive: true}` on `callbacks:{job}`, and `deleteByJobIds`. |

### 4.5 Requirement coverage

- **FR-44:** the rank is the scheduled time in µs; one queue per job type; consumer-named stages.
- **FR-45:** account and connector status are gates, flipped by one gate op per queue.
- **FR-46:** a record is inserted at once and re-ranked later with `Put{rank}`.
- **FR-47, FR-47a:** queues per job type, stages per step, `group` for whole-group claims, declared fields for account, connector and campaign; records found by a field are claimed with `scan` then `Keys`.
- **FR-47b:** `WholeGroups{max_groups: N}`, with `partial_group` for an oversized group.
- **FR-47c:** each callback batch is one group, claimed with `WholeGroups{max_groups: 1}` and checked against `batchChecksum`. Members that arrive in several units are covered by that count check.
- **FR-55:** each cycle settles with `{Release(t), Put{rank: next}}`, so `attempts` keeps counting; `Delete` removes the record.
