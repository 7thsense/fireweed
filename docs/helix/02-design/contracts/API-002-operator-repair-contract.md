---
ddx:
  id: api-operator-repair-contract
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-granularity-mapping-and-claim-domain
    - adr-queue-as-shard-unit-and-projection-families
    - td-storage-architecture-backend-contracts
    - td-sharding-and-shard-ownership
  review:
    self_hash: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
      api-native-client-interface: 852a753af558d8b8a21e4a86e87915b14c030fefcb4a27473bcbb08cfe044580
      concerns: 7e3b81e376f75f71691f55ac1ca4d9599eddcfe6eefe70f614c366c132e07992
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
      td-storage-architecture-backend-contracts: 430d0dc1f83fa62aeb19948efd2a84f5c31df7d15195e51c8296c93c711919f5
    reviewed_at: "2026-07-06T14:59:49Z"
---

# Contract

**Contract ID**: API-002
**Type**: operator / HTTP API / SDK
**Version**: v1
**Status**: complete
**Related**: PRD (P1 operator items), API-001, ADR-001, ADR-002, TD-001, TD-003

## Purpose

This contract defines the **operator repair surface** for pqueue: queue
pause/resume, item repair (reschedule, force retry/fail/complete/release, clear
lease), dead-letter redrive, bulk operator purge, archive, retention runs, and
operator inspection. It is the "P1 operator/retention contract for redrive,
purge, archive, and administrative repair" that API-001, ADR-001, ADR-002, TD-001,
and TD-002 defer to.

It is deliberately separate from the native client contract (API-001). API-001
operations are the steady-state data plane and MUST NOT carry destructive or
cross-lifecycle mutation rights. API-002 operations are privileged, deny-by-default
operator actions that MAY mutate leased and terminal items, and that therefore
require stronger authorization, idempotency, and auditing.

API-002 is transport-neutral; a Rust crate, HTTP API, or SDK MAY expose idiomatic
names but MUST preserve these operations, fields, lifecycle effects, per-item
outcomes, the async operation model, and the error rules.

## Scope and Boundaries

- In scope: queue admin state (pause/resume); item repair; redrive of terminal
  items; bulk operator purge by selector; archive; retention runs; operator
  inspection (get/list items, get/list operations); the asynchronous operation
  model for selector-scoped mutations.
- In scope: the durability, queue-epoch-fencing, idempotency, lease-fencing, and
  invariant-preservation rules these operations MUST obey.
- Out of scope: queue placement / ownership handoff, backend-profile change, and
  backend migration. Those are governed by the `admin:queue` permission (ADR-002)
  and the later migration design. API-002 references but does not define them.
- Out of scope: the native per-key/`item_id` `PurgeItems` recurring-teardown
  operation, which is **P0 and defined in API-001/TD-001** (`item:update` scope).
  API-002 `PurgeQueueItems` is the **bulk, selector-scoped** operator purge and
  MUST NOT be conflated with it.
- Owning system or team: pqueue operator/control surface.

## Relationship to API-001 and the engine invariants

Every API-002 mutation is a durable command (ADR-001 "repair or administrative
state transition") and preserves the engine's correctness invariants:

| Invariant | How API-002 preserves it |
|-----------|--------------------------|
| Single active lease (FR-25) | Any operator action on a leased item (`force_*`, `clear_lease`, purge, redrive) MUST fence the active lease: the lease token becomes stale and the worker's next renew/finalize MUST return `stale_lease`. No operator action creates a second active lease. |
| Queue-global progress bound (FR-9/FR-12) | An item returned to `pending` by repair/redrive becomes newly eligible; `eligible_since` is set to `max(commit_time, not_before)` — the single API-001 timing rule — so an item rescheduled into the future accrues no eligible age until its `not_before`. The one exception is `force_release` of an unexpired lease, which preserves the prior progress clock per FR-11. |
| Group co-residency and cohorts (ADR-004 / G6) | A queue is owned by one node (ADR-008), so a selector-scoped operation resolves on the queue's single owner and every `group_key`'s members — including a cohort's — are co-resident there by construction. Operator mutations against cohort members obey the **Cohort and group targeting** rule below: they expand to the whole cohort or are rejected; an operator action MUST NOT split a live cohort across claim units. |
| Durable ack (ADR-001 / TD-001) | An operator mutation is acknowledged only after its command(s) reach the backend durable boundary. A selector-scoped mutation runs on the queue's single owner under one queue epoch; a large match set is processed in bounded batches, each durably committed and queue-epoch fenced, and partial commit re-drives only the uncommitted batches and converges. There is no cross-owner split. |
| Idempotency | `request_id` deduplicates synchronous calls within `request_id_retention_ms`; for asynchronous operations the returned `operation_id` is the idempotency anchor (replay of the same `request_id` returns the same `operation_id`). |
| Tenant isolation (ADR-002) | Every operation is authorized against `tenant_id`/`queue_id` before reading or mutating any control-plane, log, projection, or snapshot state. |

### Cohort and group targeting (normative)

Operator mutations (`RepairItems`, `RedriveItems`, `PurgeQueueItems`,
`ArchiveItems`) against items that are members of a **live cohort** (G6) MUST NOT
act on a strict subset of a cohort:

- If `cohort_whole=true` (request field), an operation whose `item_refs`/`selector`
  matches any member of a live cohort MUST expand to operate on **all** members of
  that cohort atomically (one owner-local transaction, since cohort members are
  co-resident on the queue's single owner by construction).
- If `cohort_whole` is absent/false, an operation that would touch a strict subset
  of a live cohort's members MUST reject those members per item with
  `conflict` (reason `cohort-partial-target`) and MUST NOT mutate them.
- A terminal/expired cohort (no live shared lease) has no wholeness constraint; its
  former members are ordinary items.

This makes the cohort-wholeness invariant implementable and testable rather than
asserted. Non-cohort `group_key` groups have no wholeness constraint for operator
mutations (a group is an ordering/compatibility partition, not an atomic unit);
only cohorts are atomic.

### Selector-scoped safety guards (normative)

Every **selector-scoped** mutation (`RepairItems`, `RedriveItems`,
`PurgeQueueItems`, `ArchiveItems` — i.e. any operation invoked with a `selector`
rather than explicit `item_refs`) MUST honor the same blast-radius guards:

- `dry_run` MUST be supported and side-effect-free (returns matched count + sample,
  emits no command).
- `expected_match_count`, when present, MUST cause `match-count-mismatch` (no
  mutation) unless the matched count equals it.
- `max_affected` MUST be enforced; a selector matching more than `max_affected`
  MUST fail with `match-count-mismatch` unless `dry_run`. A deployment MAY impose a
  default `max_affected` cap that applies when the caller omits it.

`RunRetention` is not selector-scoped and cannot exceed the queue's configured
retention policy, so it does not take match guards; it MUST still be idempotent and
return the reclaimed counts.

## Authorization

API-002 operations are deny-by-default and use the operator permission set. This
contract extends the ADR-002 permission table:

| Permission | Applies to |
|------------|------------|
| `operator:inspect` | `GetItem`, `ListItems`, `GetQueueAdminState`, `GetOperation`, `ListOperations` (read-only) |
| `operator:repair` | `PauseQueue`, `ResumeQueue`, `RepairItems`, `RedriveItems`, `ArchiveItems`, `RunRetention`, `CancelOperation` |
| `operator:purge` | `PurgeQueueItems` (the most destructive operation; deployments MAY require a distinct grant) |
| `admin:queue` | queue placement / ownership handoff / backend migration (out of scope; see migration design) |

A principal MUST be authorized for the tenant, queue, and the specific operator
permission before the operation reads or mutates any state. `worker_id` is never
an operator principal. Operator mutations MUST emit an audit record
(`request_id`, `operation_id`, principal, selector fingerprint, affected counts)
without logging payloads by default.

## Common Types

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `tenant_id`, `queue_id` | string | yes | As API-001. MUST be authorized for the operator principal. | |
| `request_id` | string | yes | Envelope idempotency key; stable across retries; for async operations it MUST map to a single `operation_id`. | |
| `operation_id` | string | response | Server-assigned handle for an asynchronous operator operation. | Stable; the async idempotency anchor. |
| `selector` | object | one of `item_refs`/`selector` per op | Conjunctive predicate bounding the operation domain (below). MUST be tenant+queue scoped. | Drives bulk/async operations. |
| `item_refs[]` | array of `{item_id?, client_item_key?}` | one of `item_refs`/`selector` | Explicit item targets for small synchronous operations. | If both id and key present they MUST refer to the same item. |
| `dry_run` | boolean | no | If true, the operation MUST compute and return the matched count and a sample, and MUST NOT mutate state or emit commands. | Default false. |
| `expected_match_count` | integer | no | If present, the operation MUST fail with `match-count-mismatch` (no mutation) unless the matched count equals this value. | Runaway-blast guard. |
| `max_affected` | integer | no | Upper bound on items mutated; if the selector matches more, the operation MUST fail with `match-count-mismatch` unless `dry_run`. | Safety cap; deployment MAY enforce a lower default. |
| `audit_reason` | string | should | Free-text operator justification; stored in the operation/audit record. | Not interpreted. |

### Selector

| Field | Type | Rules |
|-------|------|-------|
| `lifecycle_states[]` | enum array | Subset of `pending`, `leased`, `complete`, `failed`. If absent, defaults are per-operation (e.g. redrive defaults to `failed`). |
| `metadata_equals` | object | Conjunctive key/value equality, same predicate shape as API-001 claim compatibility. |
| `group_key` | string | Restricts to one group (owner-local by construction). |
| `failure_code` | string | Matches terminal `failed` items with this `failure_code`. |
| `older_than_ms` | integer | Matches items whose `terminal_at` (terminal) or `created_at` (non-terminal) is older than now − value. |
| `not_before_before` | timestamp | Matches items whose `not_before` is at or before this instant. |
| `priority_range` | `{from?, to?}` | Matches items whose priority falls in the (tagged, model-typed) range. |

A selector MUST match at least one field. The empty selector is invalid
(`invalid-selector`) to prevent accidental whole-queue blasts; targeting a whole
queue requires an explicit `lifecycle_states` plus an acknowledgement field
(`confirm_whole_queue=true`).

## HTTP Route Shape

JSON request/response bodies. Operator routes are namespaced under `:operator`.

| Route | Operation | Permission |
|-------|-----------|------------|
| `POST /v1/tenants/{t}/queues/{q}/operator/queue:pause` | `PauseQueue` | `operator:repair` |
| `POST /v1/tenants/{t}/queues/{q}/operator/queue:resume` | `ResumeQueue` | `operator:repair` |
| `GET  /v1/tenants/{t}/queues/{q}/operator/state` | `GetQueueAdminState` | `operator:inspect` |
| `POST /v1/tenants/{t}/queues/{q}/operator/items:repair` | `RepairItems` | `operator:repair` |
| `POST /v1/tenants/{t}/queues/{q}/operator/items:redrive` | `RedriveItems` | `operator:repair` |
| `POST /v1/tenants/{t}/queues/{q}/operator/items:purge` | `PurgeQueueItems` | `operator:purge` |
| `POST /v1/tenants/{t}/queues/{q}/operator/items:archive` | `ArchiveItems` | `operator:repair` |
| `POST /v1/tenants/{t}/queues/{q}/operator/retention:run` | `RunRetention` | `operator:repair` |
| `GET  /v1/tenants/{t}/queues/{q}/operator/items/{item_id}` | `GetItem` | `operator:inspect` |
| `POST /v1/tenants/{t}/queues/{q}/operator/items:list` | `ListItems` | `operator:inspect` |
| `GET  /v1/tenants/{t}/queues/{q}/operator/operations/{operation_id}` | `GetOperation` | `operator:inspect` |
| `POST /v1/tenants/{t}/queues/{q}/operator/operations:list` | `ListOperations` | `operator:inspect` |
| `POST /v1/tenants/{t}/queues/{q}/operator/operations/{operation_id}:cancel` | `CancelOperation` | `operator:repair` |

## Asynchronous Operation Model

Selector-scoped mutations (`RepairItems`, `RedriveItems`, `PurgeQueueItems`,
`ArchiveItems`) MAY match large item counts on a queue. Because the queue is owned
by one node (ADR-008), they run on that single owner, processing the matched set
in bounded batches; they MUST support an asynchronous model:

| Element | Type / Shape | Required | Rules |
|---------|--------------|----------|-------|
| async accept | response | yes | A selector-scoped mutation returns `operation_id` and `state=accepted` once the operation is durably recorded; it then progresses asynchronously in bounded batches on the queue's owner. Small `item_refs`-scoped calls MAY complete synchronously and return per-item results inline. |
| `operation.state` | enum | yes | One of `accepted`, `running`, `succeeded`, `partial`, `failed`, `canceled`. `partial` means some batches committed and others failed and remain re-drivable. |
| `operation.progress` | object | yes | MUST include `matched`, `affected`, `failed`, and `updated_at` (and MAY include `batches_total`/`batches_complete`). Counts MAY be approximate while running but MUST be exact at a terminal `state`. |
| `operation.errors[]` | array | yes | Per-batch or per-item error detail for `partial`/`failed`. |
| idempotency | rule | yes | Replaying the create `request_id` MUST return the same `operation_id` and MUST NOT start a second operation. A different request body under the same `request_id` MUST fail with `request-id-conflict`. |
| retry/convergence | rule | yes | A `partial`/`failed` operation MUST be resumable: re-invoking it (same `operation_id` or same `request_id`) re-drives only the uncommitted batches and converges to the same end state. |
| `CancelOperation` | operation | yes | Best-effort: stops scheduling further batches; already-committed item mutations are durable and are not rolled back. Returns the operation in `canceled` with its progress. |

## Operations

### PauseQueue / ResumeQueue

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `PauseQueue` | operation | yes | MUST set a durable `queue_admin_paused` condition. This condition is an input to the **single Eligibility Precedence definition (API-001)** — it introduces no second eligibility rule: while the condition is set, every item is ineligible under that one definition, and the standard FR-10 age-accrual rule applies (no eligible age accrues while ineligible, exactly as for any other ineligibility input). `BatchClaim` MUST return an empty claim set with `queue_paused=true`; `BatchPush`/`BatchUpdate`/`BatchFinalize`/`BatchRenewLeases` MUST continue to succeed (pausing stops *handing out new work*, not in-flight completion). Idempotent. | Distinct from dynamic gates (G2): pause is one queue-wide admin condition, not a per-item metadata predicate; both are evaluated by the same Eligibility Precedence. |
| `ResumeQueue` | operation | yes | MUST clear `queue_admin_paused`; items become eligible again per the single Eligibility Precedence (their progress clocks resume under the normal FR-10/FR-11 rules). Idempotent. | |
| admin state durability | rule | yes | Pause/resume MUST be a durable control-plane state change; recovery preserves it. The `queue_admin_paused` condition is the only state pause adds; eligibility/age semantics remain the API-001 definition. | |

### RepairItems

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `RepairItems` | operation | yes | MUST apply one operator `action` to the targeted items (`item_refs` or `selector`). MAY act on `pending`, `leased`, and terminal items. | Stronger than API-001 `BatchUpdate`, which is pending-only. |
| `action` | enum | yes | One of `reschedule`, `force_retry`, `force_fail`, `force_complete`, `force_release`, `clear_lease`. | |
| `reschedule` | action | - | Sets `priority` and/or `not_before`; recomputes `eligible_since = max(commit_time, not_before)` (the single API-001 timing rule). Valid for `pending` and (with lease fence) `leased` items. | |
| `force_retry` | action | - | Returns the item to `pending` with `not_before` (required) and optional `priority`; sets `eligible_since = max(commit_time, not_before)`; MAY reset or increment `retry_count` per `retry_count_mode`; fences any active lease. | |
| `force_fail` | action | - | Sets terminal `failed` with operator `failure_code`; fences any active lease. | |
| `force_complete` | action | - | Sets terminal `complete`; fences any active lease. | Use with care; bypasses the worker. |
| `force_release` | action | - | Releases an active lease back to `pending`; preserves the progress-bound clock (FR-11). | |
| `clear_lease` | action | - | Invalidates the active lease without changing lifecycle (item returns to `pending` if it was `leased`); progress clock preserved. | Recovery from a wedged worker. |
| lease fence | rule | yes | Any `RepairItems` action on a `leased` item MUST invalidate the lease token (worker sees `stale_lease`). | |
| `item_version` | rule | yes | Each successful repair MUST increment `item_version`. | |
| result | per item | yes | `repaired`, `not_found`, `conflict`, `terminal` (when the action is invalid for the current state, e.g. `force_release` on a non-leased item), or `unavailable`. | |

### RedriveItems

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `RedriveItems` | operation | yes | MUST return terminal `failed` items (default `lifecycle_states=[failed]`) matching the selector to `pending`/eligible, for dead-letter recovery. | The DLQ redrive surface (PRD P1 #3). |
| `redrive.not_before` | timestamp | no | Next eligibility; default is immediate (now). | |
| `redrive.priority` | tagged scalar | no | Replaces priority; MUST match the queue priority model. | |
| `retry_count_mode` | enum | no | `reset` (default) sets `retry_count=0`; `preserve` keeps it; `increment` adds one. Affects subsequent retry-exhaustion. | |
| eligibility | rule | yes | A redriven item becomes newly eligible with `eligible_since = max(commit_time, redrive.not_before)` (the single API-001 timing rule); it accrued no eligible age while terminal. | |
| bound | rule | yes | MUST honor `max_affected`/`expected_match_count`; large spans run asynchronously. | |
| result | per item | yes | `redriven`, `not_found`, `conflict` (item not terminal-failed and not in selector lifecycle), or `unavailable`. | |

### PurgeQueueItems

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `PurgeQueueItems` | operation | yes | MUST delete items matching the `selector` (bulk operator purge), writing a tombstone and a durable terminal command position per removed item, honoring `client_item_key` retention for dedupe convergence. | Distinct from native per-key `PurgeItems` (API-001). |
| lease fence | rule | yes | Purging a `leased` item MUST fence the lease (worker sees `stale_lease`). | |
| retention | rule | yes | A purge MUST NOT delete command-log rows still required for replay/audit windows; only item/projection rows and (after retention) idempotency/tombstone state are removed (TD-002 retention rules). | |
| safety guards | rule | yes | MUST honor the **Selector-scoped safety guards** (`dry_run`, `expected_match_count`, `max_affected`); `dry_run` SHOULD be the recommended first step. | |
| convergence | rule | yes | A large purge runs on the queue's owner in bounded batches of `PurgeItemsCommand`s; partial commit re-drives the uncommitted batches and converges to the same per-item result. | |
| result | per item | yes | `purged`, `not_found` (already absent — idempotent), `conflict`, or `unavailable`. | |

### ArchiveItems

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `ArchiveItems` | operation | yes | MUST export items matching the `selector` (default terminal) to the configured archive sink, or mark them `retained`, before any subsequent purge. | Retention/audit. |
| `archive.sink` | reference | no | Names a configured archive destination; if absent, items are marked retained in place (exempt from automatic terminal retention deletion). | Sink configuration is deployment-defined, not part of this contract. |
| idempotency | rule | yes | Re-archiving an already-archived item is a no-op (`archived`); archive then purge is the supported teardown order. | |
| result | per item | yes | `archived`, `not_found`, or `unavailable`. | |

### RunRetention

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `RunRetention` | operation | yes | MUST trigger a bounded retention/compaction pass within the queue's configured policy (`terminal_retention_ms`, `request_id_retention_ms`, `client_item_key_retention_ms`). It MUST NOT delete beyond policy. | Convenience; retention also runs automatically. |
| result | summary | yes | Returns counts of expired terminal items, idempotency records, and tombstones reclaimed. | |

### Inspection: GetItem / ListItems / GetQueueAdminState

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `GetItem` | operation | yes | MUST return full operator-visible item state by `item_id` or `client_item_key`: lifecycle, priority, `not_before`, `eligible_since`, metadata, retry/cohort/recurrence state, lease summary (worker_id, `lease_expires_at`; never the token), `item_version`, `last_command_position`, timestamps. | Token MUST NOT be returned. |
| `ListItems` | operation | yes | MUST page items matching a `selector`, ordered by a caller-chosen key (`priority`, `eligible_since`, `created_at`, `terminal_at`); MUST return a stable `page_token`; page size bounded by deployment cap. | DLQ/operator browsing. |
| `GetQueueAdminState` | operation | yes | MUST return `paused` state, active operation summaries, retention status, and lifecycle counts (mirrors API-001 metrics plus operator state). | |

### GetOperation / ListOperations / CancelOperation

Defined by the Asynchronous Operation Model above. `GetOperation` MUST return the
full operation record; `ListOperations` pages operations by recency/state;
`CancelOperation` is best-effort and never rolls back committed item mutations.

## Error Semantics

Envelope errors SHOULD use RFC 9457 problem-details over HTTP; library bindings
map the same `code` values to typed errors.

| Condition | Error / Outcome | Retry | Recovery |
|-----------|-----------------|-------|----------|
| Principal lacks the operator permission | Envelope `operator-forbidden` | no | Obtain the required `operator:*` grant. |
| Missing/unauthorized tenant or queue | Envelope `queue-not-found` / `queue-forbidden` | no | Use an authorized queue. |
| Empty or malformed selector | Envelope `invalid-selector` | yes after fix | Provide at least one selector field; `confirm_whole_queue` for whole-queue scope. |
| Matched count ≠ `expected_match_count`, or > `max_affected` | Envelope `match-count-mismatch` (no mutation) | yes after re-scoping | Re-run with `dry_run` to inspect, then adjust the guard. |
| Reused `request_id`, different body | Envelope `request-id-conflict` | no | New `request_id` for different work. |
| Unknown `operation_id` | Envelope `operation-not-found` | no | List operations. |
| Backend cannot durably commit before timeout | Envelope `commit-timeout` or operation `partial` | yes (resumable) | Re-drive the operation; uncommitted batches converge. |
| Operator action invalid for an item's current state | Per-item `conflict` or `terminal` | maybe | Inspect item; choose a valid action. |
| Item already absent (purge) | Per-item `not_found` (idempotent success) | no | Treat as purged. |
| Acting on a leased item | Per-item action succeeds and the lease is fenced; the worker later sees `stale_lease` | n/a | Worker re-claims if the item becomes eligible. |

## Precedence and Compatibility

- API-001 remains the authoritative data-plane contract. API-002 is a privileged
  superset surface; it MUST NOT weaken API-001 semantics, and API-001 clients are
  unaffected by API-002's existence.
- Versioning: breaking changes require a new major contract version. v1 clients
  MAY ignore unknown response fields.
- Idempotency precedence: synchronous operator mutations dedupe by `request_id`
  within `request_id_retention_ms`; asynchronous operations dedupe by the
  `operation_id` the first `request_id` produced.
- Atomicity: `PauseQueue`/`ResumeQueue` are atomic. Operator commands run on the
  queue's single owner, each batch individually atomic and queue-epoch fenced; a
  large selector is processed best-effort in bounded batches with per-item results
  and resumable convergence — there is no global all-or-nothing across the whole
  selector.
- A `dry_run` MUST be free of side effects and MUST NOT emit commands.

## Examples

```json
{
  "operation": "RedriveItems",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "op_redrive_20260607_001",
  "selector": { "lifecycle_states": ["failed"], "failure_code": "downstream_5xx", "older_than_ms": 3600000 },
  "redrive": { "not_before": "2026-06-07T18:00:00Z" },
  "retry_count_mode": "reset",
  "max_affected": 500000,
  "dry_run": false,
  "audit_reason": "Marketo outage recovered; redrive 5xx failures"
}
```

```json
{
  "request_id": "op_redrive_20260607_001",
  "operation_id": "oper_01JX9Z...",
  "state": "accepted",
  "progress": { "matched": 412333, "affected": 0, "failed": 0, "batches_total": 413, "batches_complete": 0, "updated_at": "2026-06-07T17:42:10Z" }
}
```

```json
{
  "operation": "PurgeQueueItems",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "op_purge_20260607_002",
  "selector": { "lifecycle_states": ["complete"], "older_than_ms": 2592000000 },
  "dry_run": true
}
```

```json
{
  "request_id": "op_purge_20260607_002",
  "dry_run": true,
  "matched": 7421118,
  "sample": [{ "item_id": "itm_...", "lifecycle_state": "complete", "terminal_at": "2026-05-01T00:00:00Z" }]
}
```

## Non-Normative Notes

A hosted operator dashboard (PRD P2) is a client of this contract, not part of it.
The supported recurring-key teardown order is `recurrence.until` → drain →
`ArchiveItems` (optional) → native `PurgeItems` (per key) or operator
`PurgeQueueItems` (bulk). Queue placement / ownership handoff and backend
migration belong to `admin:queue` and the migration design; this contract
intentionally excludes them.

## Validation Checklist

- [x] Normative fields, operations, and rules are explicit.
- [x] Authorization is deny-by-default and operator-scoped.
- [x] Engine invariants (single active lease, queue-global progress, co-residency
  by construction, durable ack, idempotency, tenant isolation) are preserved by
  every mutation.
- [x] Destructive operations support `dry_run` and blast-radius guards.
- [x] The asynchronous, bounded-batch, resumable operation model is explicit.
- [x] Error handling is explicit, including per-item and envelope cases.
- [x] At least one executable test can be derived from each operation.
- [x] Bulk operator purge is not conflated with native per-key `PurgeItems`.
