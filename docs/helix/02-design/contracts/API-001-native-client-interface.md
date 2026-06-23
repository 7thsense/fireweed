---
ddx:
  id: api-native-client-interface
  depends_on:
    - prd
    - concerns
    - adr-cqrs-log-projection-storage-model
  review:
    self_hash: 6b76e5c4c37c91d40e8d5229d9eeae516f71385aa06e856fb41a4a19ee5856e8
    deps:
      adr-cqrs-log-projection-storage-model: 709f701130b5bd00666a1abeef4fb104555a623d39b9fec1fdb9b3167789de10
      concerns: 122b700fbf6049b7fa177b99efa27c5fce011775767d682458a0e2872981fb54
      prd: 382115039de93226b051a09e719c7e1c50f12563d96c1ba85ef142c0ae5d0ce0
    reviewed_at: "2026-06-20T19:00:41Z"
---

# Contract

**Contract ID**: API-001
**Type**: library / HTTP API / SDK
**Version**: v1
**Status**: complete
**Related**: PRD, ADR-001

## Purpose

This contract defines the native pqueue client interface for queue definition,
idempotent batch writes, mutable priority updates, batch claims, lease renewal,
and batch finalization.

The contract is transport-neutral. A Rust client, TypeScript client, HTTP API,
or embedded library binding may expose idiomatic names, but MUST preserve these
operations, fields, lifecycle semantics, per-item outcomes, and error rules.

The same native command model is exposed through three first-class surfaces:

- A Rust crate for embedded or same-process use.
- A stateless Rust service exposing an HTTP/JSON API for remote clients.
- Generated or hand-written SDKs that wrap the HTTP API and preserve batch-first
  semantics.

Compatibility adapters, such as an SQS-shaped API, are separate secondary
surfaces. They MUST NOT replace the native API because they cannot represent
mutable priority, mutable schedule, or pqueue's full batch/update semantics.

## Scope and Boundaries

- In scope: native client operations for queue creation, item write/update,
  claim, lease renewal, finalize, and basic queue metrics.
- In scope: request/response fields, required identifiers, lifecycle outcomes,
  idempotency behavior, lease semantics, and batch error behavior.
- In scope: first-class exposure surfaces and HTTP route shape.
- Out of scope: storage adapter traits, SQS-compatible adapter details, operator
  UI, authentication provider details, and exact generated SDK packaging.
- Owning system or team: pqueue core.

## Normative Surface

Use MUST, MUST NOT, MAY, and SHOULD intentionally. Every field, command,
message, endpoint, or payload element named here is part of the contract.

### Common Types

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `tenant_id` | string | yes for service mode | MUST identify the tenant/account boundary used for authorization, isolation, metrics, and control-plane routing. Embedded/local deployments MAY use a fixed default tenant. | Not necessarily equal to `queue_id`. |
| `queue_id` | string | yes | MUST be stable within `tenant_id`; MUST be used for routing, storage partitioning, and metrics. `queue_id` is required for every operation except `DiscoverActiveScopes`, where it is optional and selects group-granularity drill-down into a single queue. | Client-visible queue namespace. |
| `request_id` | string | yes for mutating operations | MUST be stable across retries of the same logical request; MUST be unique for different logical requests; MUST be returned in responses. | Envelope idempotency key and trace correlation ID. |
| `client_item_key` | string | yes for push | MUST identify the caller's logical item within a queue; MUST remain a durable secondary key for non-terminal lookup and for terminal lookup until item retention expires. | Duplicate pushes converge by this key. |
| `item_id` | string | response / update / finalize | MUST be server-assigned and stable for the accepted queue item. | Used after first accept. |
| `item_version` | integer | response / conditional update | MUST monotonically increase for each committed mutation of an item. | Used for optional optimistic concurrency. |
| `lease_token` | string | claim / renew / finalize | MUST be unguessable; MUST authorize lease renewal and finalization for one active lease. | Stale tokens fail per item. |
| `priority` | tagged scalar | yes when item should be orderable | MUST match the queue's declared priority model. | Timestamp queues use RFC 3339 UTC timestamps. |
| `not_before` | timestamp | no | If present, item MUST NOT be claimable before this timestamp. | Distinct from priority. |
| `payload` | opaque bytes or JSON value | no | MUST be stored and returned to claimers without pqueue interpreting application meaning. | Transport adapters define encoding. |
| `metadata` | JSON object / map | no | MUST be caller-defined and queryable only through supported predicates. | Used for gates, group keys, and observability dimensions. |
| `group_key` | string | no (yes when `group_co_residency=true`) | MAY identify a claim compatibility / ordering partition within a queue. When a claim's effective domain is a single `group_key` **on a `group_co_residency=true` queue**, claim result order is the exact per-group priority order (ADR-004). On a `group_co_residency=false` queue, `group_key` is a valid claim-domain restriction filter but does NOT promise per-group total order across shards. On a queue with `group_co_residency=true`, every item MUST carry `group_key` and all items sharing a `group_key` are co-resident on one shard. `group_key` carries no progress-bound meaning; progress is queue-global. | Examples: job, callback/cohort, account, connector, campaign. |
| `gate_keys` | array of strings | no | MAY declare zero or more opaque gate keys for the item. An item MUST be ineligible for claim while any of its gate keys is `blocked` in the queue's gate state (see Eligibility Precedence). pqueue MUST NOT interpret gate-key meaning. An item with no gate keys is never gate-blocked. Each key MUST match `^[A-Za-z0-9._:-]{1,256}$`; duplicates within one item MUST be collapsed to a set; the set size MUST NOT exceed the queue's `eligibility_policy.max_gate_keys_per_item`. Valid only when the queue's `eligibility_policy.gate_keys = dynamic`; otherwise the item fails per-item `invalid`. | Distinct from `group_key` (claim compatibility/co-residency, not eligibility) and from downstream rate pacing (not modeled by pqueue). Gate keys are opaque and independent of whichever `group_key` topology a queue uses (ADR-004). |
| `cohort_size` | integer | conditional | Required on every item of a queue with `cohort_policy.enabled=true`; MUST NOT be present otherwise (else per-item `invalid`). MUST be greater than 0. MUST be identical for every item sharing one `group_key`; a conflicting value on a later member MUST be rejected per item with `conflict`. Fixed at the first accepted member of the `group_key` and immutable thereafter. | Expected complete-cohort member count (analogue of `batch_checksum`). The cohort key is `group_key`; cohort identity = all items sharing a `group_key` on a cohort-enabled queue. |
| `lifecycle_state` | enum | response | MUST be one of `pending`, `leased`, `complete`, `failed`. Retry is represented as pending with retry metadata and `not_before`. A **recurring** item (see Queue Definition `recurrence`) cycles between `pending` and `leased` indefinitely and reaches `complete`/`failed` only on an explicit terminal finalize. After `recurrence.until` the item stops being re-armed but does **not** change lifecycle state until a terminal finalize occurs or the item is removed by `PurgeItems`. | Recurring items never auto-terminate. |
| `item_result.status` | enum | response | MUST be one of `accepted`, `updated`, `duplicate`, `claimed`, `renewed`, `completed`, `failed`, `retried`, `released`, `rearmed`, `purged`, `not_found`, `invalid`, `conflict`, `stale_lease`, `terminal`, `rate_limited`, `unavailable`. | Per-item outcome. `rearmed` is the per-item success status of a `rearm` finalize; `purged` is the per-item success status of a `PurgeItems` removal. `rate_limited` denotes a pqueue deployment/tenant capacity limit only (P1) — specifically the partial-batch case where pqueue accepts some items and declines others of one request under a capacity control; whole-request capacity rejection uses the envelope rate-limit error instead. `rate_limited` is never a downstream-API rate signal. |

### Tenant and Authorization Rules

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| HTTP principal | authenticated identity | yes for service mode | MUST be resolved before authorizing any route. The provider is intentionally outside this contract. | Examples: machine token, service account, user session. |
| Tenant binding | authorization rule | yes for service mode | `tenant_id` from the route MUST be authorized for the HTTP principal. Servers MAY infer a default tenant only when that inference is unambiguous and authorized. | Prevents route-level tenant spoofing. |
| Embedded tenant | configuration | yes for embedded mode | Embedded or local deployments MAY bind all operations to a configured default `tenant_id`. | Keeps local/library mode simple. |
| `worker_id` | observability identity | yes for claim | MUST NOT be treated as the authenticated principal. | Worker names are caller-supplied labels. |
| `DiscoverActiveScopes` permission | authorization rule | yes for service mode | The principal MUST hold `queue:read` for each queue a descriptor would expose. With no `queue_id` in the request, discovery MUST authorize per candidate queue and MUST include only queues for which authorization succeeds (mixed-authorization enumeration). Enumeration and per-queue auth fanout MUST be bounded by pagination or a documented per-tenant queue ceiling (see Active-Scope Discovery). | Tenant-wide route still authorizes per queue. |
| `DiscoverActiveScopes` not-found/forbidden | authorization rule | yes for service mode | When the request names a `queue_id` the principal is not authorized for, the server MUST return envelope `queue-forbidden` or `queue-not-found` without leaking existence. When no `queue_id` is named, unauthorized queues MUST be silently excluded, never reported. | Deny-by-default; no existence leak. |

### Exposure Surfaces

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| Rust embedded surface | crate API | yes | MUST expose the native operations as typed async Rust functions or traits. MUST NOT require the HTTP service for same-process deployments. | First local implementation surface. |
| HTTP service surface | HTTP/JSON API | yes | MUST expose the native operations over versioned `/v1` routes. MUST support stateless service containers behind a load balancer. | First remote implementation surface. |
| SDK surface | client library | should | SHOULD wrap the HTTP service without changing operation semantics, result ordering, or error codes. | Initial SDK targets are Rust and TypeScript unless later design changes this. |
| Compatibility adapter surface | adapter API | may | MAY expose SQS-shaped or other compatibility APIs. MUST document unsupported native semantics. | P1, not the native contract. |

### HTTP Route Shape

The HTTP binding MUST use JSON request and response bodies unless a later
transport contract explicitly defines another encoding.

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `POST /v1/tenants/{tenant_id}/queues` | HTTP operation | yes | MUST bind to `CreateQueue`. | Control-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/items:push` | HTTP operation | yes | MUST bind to `BatchPush`. | Data-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/items:update` | HTTP operation | yes | MUST bind to `BatchUpdate`. | Data-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/gates:set` | HTTP operation | yes | MUST bind to `SetGates`. | Eligibility-control-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/items:claim` | HTTP operation | yes | MUST bind to `BatchClaim`. | Data-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/items:purge` | HTTP operation | yes | MUST bind to `PurgeItems`. | Targeted in-band teardown route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/leases:renew` | HTTP operation | yes | MUST bind to `BatchRenewLeases`. | Data-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/items:finalize` | HTTP operation | yes | MUST bind to `BatchFinalize`. | Data-plane route. |
| `GET /v1/tenants/{tenant_id}/queues/{queue_id}/metrics` | HTTP operation | yes | MUST bind to `GetQueueMetrics`. | Observability route. |
| `POST /v1/tenants/{tenant_id}/scopes:discover` | HTTP operation | should | MUST bind to `DiscoverActiveScopes`. MUST be implemented (P0/MUST) in native service mode and MAY be omitted by compatibility adapters. Tenant-scoped; MAY accept an optional `queue_id` in the body to drill into one queue's groups. Read-only; no side effects; results aggregated across the queue's shards. MUST support pagination (`page_token`) or enforce a documented per-tenant queue ceiling. | First tenant-scoped (multi-queue) data-plane route. |

The HTTP binding MAY add transport headers for authentication, trace context,
content encoding, and idempotent retry metadata. Those headers MUST NOT change
the native operation semantics defined by this contract.

### Queue Definition

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `CreateQueue` | operation | yes | MUST create one queue definition atomically. | Control-plane operation. |
| `CreateQueue.queue_id` | string | yes | MUST be unique within `tenant_id`. | Idempotent create MAY return existing compatible definition. |
| `priority_model.kind` | enum | yes | MUST be one of `timestamp`, `int64`, `decimal`, `string`. | v1 MUST support `timestamp` and at least one non-timestamp kind. |
| `priority_model.direction` | enum | yes | MUST be `ascending` or `descending`. | Timestamp scheduled queues usually use `ascending`. |
| `priority_model.tie_breaker` | enum | yes | MUST define deterministic order for equal priority values. | v1 default SHOULD be `created_sequence`. |
| `ordering_mode` | enum | yes | MUST be `strict` or `bounded_relaxed`. | Determines claim ordering. |
| `group_co_residency` | boolean | no | Default false. **Immutable after creation.** If true, the queue MUST place items by `shard_id = hash(group_key) mod shard_count`, MUST require `group_key` on every pushed item, thereby co-locates each `group_key` on one shard, and thereby enables exact per-group claim order. Required by claim modes that need whole-group atomicity (`compatibility.group_batching`; `compatibility.whole_cohort`); those modes on a `group_co_residency=false` queue MUST be rejected `invalid-request`. Carries no progress meaning; the progress bound is always queue-global. | Placement capability, not a progress/claim-scope field. |
| `progress_bound_ms` | integer | yes | MUST be greater than 0. | Eligible items cannot be ignored beyond this bound. |
| `eligibility_policy.metadata_blockers` | object | no | If present, keys map to arrays of blocked JSON scalar values. An item whose metadata key equals any blocked value MUST be ineligible. Nested object and array equality are not part of v1. | Generic support for paused, suppressed, disabled, or quota-blocked states. |
| `eligibility_policy.gate_keys` | enum | no | MUST be one of `none`, `dynamic`. Default `none`. **Immutable after `CreateQueue`.** When `dynamic`, items MAY carry `gate_keys` and `SetGates` is permitted. When `none`, item `gate_keys` MUST be rejected per-item `invalid` and `SetGates` MUST fail the envelope with `gates-not-enabled`. | No in-place enable path because queue definitions are immutable. |
| `eligibility_policy.max_gate_keys_per_item` | integer | conditionally required | **MUST be present and > 0 whenever `gate_keys = dynamic`** (it has no meaning otherwise and MUST be absent/ignored when `gate_keys = none`). If omitted on a `dynamic` queue, `CreateQueue` MUST apply the deployment default `default_max_gate_keys_per_item` and MUST persist the effective value. Server MAY enforce a lower deployment cap; an item exceeding the effective value fails per-item `invalid`. | Bounds anti-join fan-in per item. |
| `eligibility_policy.max_gates_per_request` | integer | no | Per-queue override of the deployment cap on the number of canonical `{gate_key, state}` entries in one `SetGates` envelope. MUST be > 0 if present. If absent, the deployment default `default_max_gates_per_request` applies. | Normative home for the cap used by `SetGates`. |
| `cohort_policy.enabled` | boolean | no | Default false. If true, the queue MUST be created with the group co-residency placement capability (`group_co_residency=true`, ADR-004 / placement) so each `group_key` is co-resident on one shard; every pushed item MUST carry `group_key` and `cohort_size`. If a cohort-enabled queue is created without group co-residency, `CreateQueue` MUST fail with `queue-definition-conflict`. | Opt-in; queues without it are unaffected. Cohort key = `group_key`. Progress remains queue-global. |
| `cohort_policy.completion_bound_ms` | integer | conditional | Required when `cohort_policy.enabled=true`. MUST be greater than 0. **`CreateQueue` MUST reject `completion_bound_ms > progress_bound_ms` with `queue-definition-conflict`.** Bounds how long a cohort may remain not claim-eligible before it is expired per `on_incomplete`, measured per the formula in the cohort-expiry rules. | Cohort-lifecycle liveness timeout, NOT a progress scope. The hard `<= progress_bound_ms` check preserves FR-12 for withheld eligible members. |
| `cohort_policy.on_incomplete` | enum | conditional | Required when `cohort_policy.enabled=true`. v1 MUST be `expire_cohort`: at the cohort expiry deadline, all current members become terminal `failed` with failure code `cohort-incomplete` via `CohortExpired`. | `degrade_to_items` is reserved for a future minor and MUST NOT be accepted in v1. |
| `cohort_policy.max_cohort_size` | integer | conditional | Required when `cohort_policy.enabled=true`. MUST be greater than 0 and MUST be `<= max_claim_batch_size`. A `cohort_size` above this MUST be rejected per item with `invalid`. | Guarantees every complete cohort fits one claim. |
| `recurrence.mode` | enum | no | MUST be `oneshot` or `recurring`. Default MUST be `oneshot`. Immutable after creation. In `oneshot` mode a `rearm` finalize MUST be rejected per item with `invalid`. In `recurring` mode a `rearm` finalize MUST be accepted and MUST NOT count against `retry_policy.max_attempts`. | Generic perpetual-item primitive. |
| `recurrence.until` | timestamp | no | Valid only when `recurrence.mode = recurring`; on a `oneshot` queue creation MUST be rejected with envelope `invalid-request`. After this time the server MUST reject `rearm` per item with `terminal` and MUST NOT change the item's lifecycle state. The item remains in its current state until a terminal `complete`/`fail` finalize or out-of-band `PurgeItems` removal. | Stops re-arming; does NOT auto-complete. |
| `request_id_retention_ms` | integer | yes | MUST be greater than 0. | Bounds mutating request replay/deduplication. |
| `client_item_key_retention_ms` | integer | yes | MUST be greater than 0. | Bounds duplicate push convergence after terminal retention rules no longer keep the item addressable. |
| `max_lease_duration_ms` | integer | yes | MUST be greater than 0. | Caps claim and renew lease durations. |
| `retry_policy.max_attempts` | integer | yes | MUST be greater than 0. A `retry` finalize beyond this count MUST make the item terminal `failed`. The `retry` budget is **per recurring cycle**: a successful `rearm` MUST reset the item's transient-retry counter to 0. The `rearm` outcome (see Batch Finalize) MUST NOT count against `max_attempts` and MUST NOT cause terminal `failed`. `max_attempts` bounds only the transient-failure `retry` path within a single cycle. | Defines terminal retry exhaustion. |
| `max_push_batch_size` | integer | yes | MUST be greater than 0. | Server may enforce a lower deployment cap. |
| `max_claim_batch_size` | integer | yes | MUST be greater than 0. | Server may enforce a lower deployment cap. |
| `max_eligible_group_size` | integer | required when group batching is enabled | MUST be greater than 0 and MUST be `<= max_claim_batch_size`, so any single whole eligible group fits one claim. Bounds a group's non-terminal member count: `BatchPush` MUST fail per-item with `group-too-large` when accepting an item would push its `group_key`'s non-terminal member count over this value. Only meaningful on queues created with group co-residency (`group_co_residency=true`); ignored otherwise. | Required precondition for `compatibility.group_batching`. |
| `CreateQueue.shard_count` | integer | no | MUST be >= 1 if present; defaults to 1. Server MAY reject values above a deployment policy cap with `invalid-request`, and MAY override by policy. Fixed at create and immutable. `shard_count > 1` requests the horizontally sharded execution path. | Number of physical shards (ADR-004 / TD-003). `shard_id` is never client-visible. |
| `CreateQueue.response` | object | yes | MUST include the stored queue definition and `created` boolean. | `created=false` means compatible idempotent create. |
| `CreateQueue.response.shard_count` | integer | yes | MUST echo the effective stored `shard_count` (after any policy override). | Lets clients learn the effective shard count. |

`max_eligible_group_size` is enforced only at push: push, update, retry, lease
expiry, and gate reopen never increase a group's non-terminal member count, so
push is the sole growth point and the sole rejection point. Because
`max_eligible_group_size <= max_claim_batch_size`, every whole eligible group
always fits one `group_batching` claim.

`group_co_residency`, `shard_count`, `ordering_mode`, and `priority_model`
participate in the queue's stable configuration identity used for idempotent
create (see Precedence and Compatibility). A repeated `CreateQueue` with the same
`tenant_id`/`queue_id` but a differing `group_co_residency` or `shard_count`
value MUST be rejected as a definition conflict (`queue-definition-conflict`).

A cohort-enabled queue requires `group_co_residency=true`; a `recurring` queue
that carries `group_key` SHOULD enable `group_co_residency=true` so each
recurring singleton stays on one shard for its lifetime and re-arm never
relocates the item. `recurrence.mode=recurring` and `cohort_policy.enabled=true`
are mutually exclusive on the same queue (ADR-004); `CreateQueue` MUST reject a
queue that sets both with envelope `invalid-request`.

The deployment defines two required deployment defaults that back the per-queue
gate caps: `default_max_gate_keys_per_item` (integer > 0) and
`default_max_gates_per_request` (integer > 0). A queue that omits its own
`eligibility_policy.max_gate_keys_per_item` or
`eligibility_policy.max_gates_per_request` override inherits the corresponding
deployment default, so neither cap is ever undefined. `shard_count` is similarly
bounded by a deployment policy cap (`deployment_max_shard_count`).

### Batch Push

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchPush` | operation | yes | MUST accept one or more items and return one `item_result` per submitted item. | Best-effort per item. |
| `BatchPush.items[]` | array | yes | MUST NOT exceed queue or deployment max push batch size. | Empty batch is invalid. |
| `items[].client_item_key` | string | yes | MUST drive duplicate convergence within `client_item_key_retention_ms` and remain usable for lookup while the item is retained. | Required even when payload differs. |
| `items[].priority` | tagged scalar | yes | MUST match queue priority model. | Invalid values fail per item. |
| `items[].not_before` | timestamp | no | MUST make item ineligible until the timestamp. | `priority` still determines order once eligible. |
| `items[].payload` | opaque bytes or JSON value | no | MUST be stored as caller data. | May be omitted for pointer-only queues. |
| `items[].metadata` | JSON object / map | no | MUST be stored as caller metadata. | Size limits are deployment-defined. |
| `items[].gate_keys` | array of strings | no | MUST be stored as the item's gate-key set after validation (charset, length, dedup, cardinality cap per Common Types / Queue Definition). Empty or absent means no dynamic gate applies. Present when the queue is `gate_keys = none` MUST fail that item with `invalid`. | Dynamic eligibility gate keys. |
| `BatchPush.response.results[]` | array | yes | MUST preserve request item order. | Each result includes submitted `client_item_key`. |

Duplicate pushes with the same `client_item_key` MUST NOT mutate the existing
item. Clients MUST use `BatchUpdate` to change priority, `not_before`, payload,
or metadata after initial acceptance.

Successful first acceptance MUST create `item_version=1`. Duplicate pushes MUST
return the current `item_id` and `item_version` without incrementing
`item_version`.

### Batch Update

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchUpdate` | operation | yes | MUST update one or more pending, non-leased, non-terminal items and return one `item_result` per submitted update. | Best-effort per item. |
| `updates[].item_ref` | object | yes | MUST identify the target by `item_id` or `client_item_key`. | If both are present they MUST refer to the same item. |
| `updates[].expected_item_version` | integer | no | If present, the update MUST fail with per-item `conflict` unless the current item version matches. | Optimistic concurrency. |
| `updates[].priority` | tagged scalar | no | If present, MUST replace the current priority and match queue priority model. | Allows ingest before final schedule. |
| `updates[].not_before` | timestamp / null | no | If present, MUST replace or clear not-before eligibility. | `null` clears. |
| `updates[].payload` | opaque bytes or JSON value | no | If present, MUST replace payload. | Patch semantics are not v1. |
| `updates[].metadata` | JSON object / map | no | If present, MUST replace metadata. | Patch semantics are not v1. |
| `updates[].gate_keys` | array of strings / null | no | If present, MUST replace the item's gate-key set (full replacement, like `metadata`). `null` clears all gate keys. Same validation as push. **This is an eligibility-changing item mutation; see the `eligible_since` rule below.** | Gate-blocked items remain updatable: gating is eligibility, not a lease. |
| `BatchUpdate.response.results[]` | array | yes | MUST preserve request update order. | Terminal items fail per item. |

Updates to leased items MUST fail with per-item `conflict`. Workers that need to
change leased work MUST use `BatchRenewLeases` or `BatchFinalize`. Operator
repair APIs may define stronger mutation rights in a separate contract.

A gate-blocked item is `pending`, non-leased, and non-terminal; `BatchUpdate` of
a gate-blocked item MUST succeed (it is not a `conflict`). Only active lease
state blocks update.

**`eligible_since` on a `BatchUpdate` that changes `gate_keys`.** Changing an
item's `gate_keys` set via `BatchUpdate` is a per-item eligibility-affecting
mutation and is governed by the same `eligible_since` rule as any other
`BatchUpdate` field change: it bumps `item_version` and **MUST preserve
`eligible_since`** (v1 does not reset eligible age on update). The item's
*current* eligibility is then re-derived from its new gate-key set against
current gate state at claim time. If the update adds a gate key that is currently
`blocked`, the item becomes ineligible (not reported toward the progress bound
while ineligible, FR-10) without changing `eligible_since`; if the update removes
the gate key that was blocking it, the item becomes eligible again and is
measured from its unchanged `eligible_since`. This is distinct from a `SetGates`
flip, which changes *queue* gate state and touches no item row at all.

### Gate State

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `SetGates` | operation | yes when `gate_keys = dynamic` | MUST set the blocked/open state of one or more gate keys for one queue. O(1) in affected items; the acknowledged commit path is O(keys × shards occupied). Per-shard application is atomic; the queue converges (below). | Queue-scoped only; not tenant-wide. |
| `request_id` | string | yes | MUST provide envelope idempotency per the shared idempotency rules, fingerprinted over the canonical gate set (below). Retried by `request_id` to drive convergence. | |
| `gates[]` | array | yes | MUST contain one or more `{gate_key, state}` entries. After canonicalization (dedup by `gate_key`, last write wins, sorted), MUST NOT exceed the queue's effective `max_gates_per_request`. Empty batch MUST fail `invalid-request`. | |
| `gates[].gate_key` | string | yes | MUST match `^[A-Za-z0-9._:-]{1,256}$`. | Opaque. |
| `gates[].state` | enum | yes | MUST be one of `blocked`, `open`. | Default state of any unset key is `open` (fail-open). |
| `SetGates.response.gate_epoch` | integer | yes | MUST return the queue gate epoch assigned to this committed gate set (monotonic per queue). A claimer or caller MAY pass this back to fence reads. | The visibility token. |
| `SetGates.response.shards[]` | array | yes | MUST report, per occupied shard, `{shard opaque-handle, applied_command_position, converged: bool}`. `shard_id` itself is never client-visible (ADR-004); the handle is opaque. | Per-shard convergence report. |
| `SetGates.response.gates[]` | array | yes | MUST report, for each canonical input key, its `gate_key` and the requested committed `state`, in canonical (sorted) order. | A state report, not a per-item batch. |

**Validation atomicity.** If any entry is invalid (bad charset, unknown `state`,
over cap, or the queue is not `gate_keys = dynamic`), the **entire envelope MUST
fail validation and MUST apply nothing on any shard**. Validation is evaluated
before any shard append.

**Per-shard atomicity + queue convergence.** pqueue does not assume a cross-shard
distributed transaction; the storage contract exposes only per-shard append.
Therefore the same canonical gate set, tagged with one queue **`gate_epoch`**,
MUST be applied to every shard the queue occupies; **each shard's gate
application is individually atomic**. A `SetGates` MUST be **idempotently retried
by `request_id` and `gate_epoch` until every occupied shard has durably applied
that epoch's gate set** (per the durable-ack and `commit-timeout` rules). The
response's `shards[].converged` reports per-shard progress; the operation is
**fully converged** only when every occupied shard reports the committed
`applied_command_position` for this `gate_epoch`. Because group co-residency
places a whole group on one shard, a gate that blocks a single group is
single-shard and converges atomically; only a gate key spanning multiple groups
fans out, and during convergence such a key MAY be blocking on some shards before
others — which is correct, because each shard's items are independently gated and
no cross-shard item set needs simultaneous visibility (a group never spans
shards).

**Effect.** Setting a gate key to `blocked` on a shard MUST make every `pending`,
non-leased item on that shard carrying that key ineligible for claim, with no
per-item mutation and no `item_version` change. Setting it to `open` MUST restore
eligibility unless another gate key or eligibility rule still blocks the item.
Gate changes MUST NOT affect already-leased or terminal items, and MUST NOT
change any item's `eligible_since`. Time spent gate-blocked is ineligible time
and MUST NOT be reported toward the progress bound (Eligibility Precedence;
FR-10); v1 does NOT deduct the blocked interval on reopen.

**Claim linearization with a projection-position fence.** A `BatchClaim` against
a shard whose local projection has applied the gate command position for the
latest committed `gate_epoch` MUST NOT return an item blocked by that gate.
Because log-backed backends serve claims from a local projection that may lag the
committed log, the engine MUST enforce a **projection-position fence**: a claim on
a shard MUST only return items after confirming that shard's projection has
applied at least the gate command position of every `gate_epoch` known-committed
at claim-selection start. A claim whose candidate selection demonstrably began
before a `SetGates` commit (its read snapshot precedes that gate command
position) MAY still complete and return such items. A claim MUST NOT return newly
blocked work merely because its local projection has not yet replayed the gate
command — stale projections MUST fence, not leak. Idempotent claim replay (a
repeated claim `request_id` with active leases) is not a new claim and MAY return
already-leased items even after a gate later closes.

### Batch Claim

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchClaim` | operation | yes | MUST atomically lease up to `max_items` eligible items. | Empty success is allowed. |
| `request_id` | string | yes | MUST provide envelope idempotency for claim retries. Duplicate claim requests within retention MUST return the same claimed set while leases are active. | Claim retry safety. |
| `worker_id` | string | yes | MUST identify the claiming worker or consumer group member for observability. | Not an auth principal. |
| `max_items` | integer | yes | MUST be greater than 0 and no more than queue/deployment max claim batch size. | Upper bound, not guarantee. |
| `lease_duration_ms` | integer | yes | MUST be greater than 0 and no more than `max_lease_duration_ms` or a lower deployment cap. | Creates invisibility window. |
| `compatibility.same_group_key` | boolean | no | If true, returned items MUST share one server-selected `group_key`. | Used for downstream batch compatibility. |
| `compatibility.group_key` | string | no | If present, returned items MUST match this exact group key. | Caller-selected group. |
| `compatibility.metadata_equals` | object | no | If present, returned items MUST have metadata equal to every specified key/value pair. | v1 predicate shape. |
| `compatibility.group_batching` | object | no | If present, the server MUST select whole eligible groups instead of individual items (see group-batching prose). MUST NOT be combined with `same_group_key` or an explicit `group_key`. MAY be combined with `metadata_equals` (filters within each whole group). Valid ONLY on queues created with group co-residency (`group_co_residency=true`, ADR-004 / D2) that also define `max_eligible_group_size`; otherwise envelope error `invalid-request`. | New whole-eligible-group claim mode; distinct from `same_group_key`. |
| `compatibility.group_batching.max_groups` | integer | yes when `group_batching` present | MUST be greater than 0. Caps the number of distinct `group_key` values selected in one claim. | Bounds distinct groups, not total items. |
| `compatibility.group_batching.group_completeness` | enum | yes when `group_batching` present | v1 MUST be `whole_eligible`: each selected group MUST be returned as ALL of its currently-eligible items within the effective claim domain (per Eligibility Precedence), as an all-or-nothing unit. | Reserved enum for future relaxation. |
| `compatibility.whole_cohort` | boolean | no | If true, the server MUST select exactly one complete, claim-eligible cohort and MUST atomically lease **every** member under one shared `cohort_lease_token`, or return empty when no complete claim-eligible cohort exists. The server MUST NOT lease a partial cohort and MUST NOT make any cohort member available to a non-`whole_cohort` claim. Valid only when `cohort_policy.enabled=true` (else envelope `invalid-request`). MUST NOT be combined with `same_group_key`, `compatibility.group_key`, or `compatibility.group_batching` (else envelope `invalid-request`). | The third claim unit: all-or-nothing complete-cohort claim. |
| `BatchClaim.response.items[]` | array | yes | MUST return claimed items in deterministic result order for the queue's ordering mode, computed over the request's **effective claim domain** (the candidate set after the queue Eligibility Precedence and the request's `group_key` / `same_group_key` / `metadata_equals` filters and any active claim-unit mode). When the effective claim domain is a single `group_key` on a `group_co_residency=true` queue, this order is the exact per-group priority order; on a `group_co_residency=false` queue a `group_key` filter restricts the domain but does not promise per-group total order across shards. Returned items MUST all satisfy the request's declared filters (no item outside the caller's filter is ever returned). `shard_id` MUST NOT influence result order. | Each item includes `lease_token` (except whole-cohort results, see below). |
| `claimed_item.lease_expires_at` | timestamp | yes | MUST indicate when item may become eligible if not renewed/finalized. | Server time. |
| `cohort_lease_token` | string | conditional | Present at the top level of a `whole_cohort` claim response when a cohort was leased; absent otherwise. It is the ONLY lease handle for the cohort. | Cohort is the lease unit. |
| `cohort_id` | string | conditional | Present with `cohort_lease_token`; stable cohort identity for renew/finalize/observability. | |
| `items[].lease_token` (cohort) | string | conditional | For `whole_cohort` results, per-item `lease_token` MUST be ABSENT (the cohort lease replaces it). For item-level / `whole_group` results it is present as today. | Distinguishes cohort results from item/group results. |

Group-aware claim selection MUST NOT permanently favor one group when the server
selects among groups. Compatibility predicates are conjunctive: `group_key`,
`metadata_equals`, and `same_group_key` all apply when provided. Combining
`same_group_key=true` with an explicit `group_key` is valid and means all
returned items MUST match the explicit group. Explicit caller filters restrict
the claim domain for that request; pqueue can preserve progress within requested
domains, but operators remain responsible for running workers that cover all
required domains.

A `BatchClaim` resolves to exactly one claim unit: item-level (default),
`whole_group` (`compatibility.group_batching`), or `whole_cohort`
(`compatibility.whole_cohort`).

#### Claimed Item Response Shape

Every item in `BatchClaim.response.items[]` MUST be returned with the fields
below, so an adapter author can map a claim to a downstream unit of work without
any out-of-band knowledge. Unless marked conditional, each field MUST be present
on every item-level and `whole_group` result. Field semantics are defined once in
**Common Types**; this table is the authoritative enumeration of what a claim
returns.

| Field | Required | Source / Rule |
|-------|----------|----------------|
| `item_id` | yes | Server-assigned stable item id. |
| `client_item_key` | yes | The caller's logical key from push; the durable secondary key for correlation and dedup. |
| `item_version` | yes | Monotonic version as of this claim (the claim bumps it). |
| `lease_token` | conditional | Present on item-level and `whole_group` results; **absent** on `whole_cohort` results, where the shared top-level `cohort_lease_token` replaces it. |
| `lease_expires_at` | yes | When the lease expires if not renewed or finalized (server time). |
| `priority` | yes when the queue is orderable | The item's priority in the queue's declared priority model. |
| `not_before` | conditional | Present when the item carries a `not_before`; absent otherwise. |
| `group_key` | conditional | Present when the queue is `group_co_residency=true` (where it is required on every item) or when the item was pushed with a `group_key`; absent otherwise. |
| `payload` | conditional | Present when the item was pushed with a payload; returned verbatim and uninterpreted. |
| `metadata` | conditional | Present when the item carries caller metadata; returned verbatim. |
| `gate_keys` | conditional | Present **only** on queues created with `eligibility_policy.gate_keys = dynamic` and only when the item declared one or more gate keys; **absent** on `gate_keys = none` queues. |

pqueue MUST NOT add, drop, or reinterpret any field of a claimed item; the shape
above is the complete claimed-item contract. The JSON example below shows an
item-level result whose source item carries no `not_before` and no `gate_keys`,
so those conditional fields are correctly omitted.

When `compatibility.group_batching` is present, the server MUST select up to
`max_groups` distinct **wholly-available** eligible `group_key` values and return
**all** currently-eligible items of each selected group (where "eligible" is
defined solely by the **Eligibility Precedence** subsection, and, when
`metadata_equals` is present, restricted to items satisfying that filter) as an
all-or-nothing unit; it MUST NOT return a strict subset of a selected group's
eligible items. A group is **wholly available** only if it has at least one
currently-eligible item and has **no active lease held by any other claim**
(including a prior generic or `same_group_key` item-level claim that leased a
subset of the group); a group with any active lease is **contended** and MUST be
skipped to the next group in order, not partially returned. Skipping a contended
group does not violate the queue's progress bound, because that group is making
progress under the other claim. Group representatives MUST be ordered by the
queue's deterministic claim ordering tuple, and groups MUST be selected in that
order. For `ordering_mode=strict` this order is strict; for
`ordering_mode=bounded_relaxed` selection MAY relax within the queue's
bounded-relaxed window but MUST still select any group containing an eligible
item near `progress_bound_ms` before newer groups, so no eligible group is
starved beyond the queue's single progress bound. Whole groups are accumulated in
order until including the next whole group would exceed `max_items`; the server
then stops and MAY return fewer than `max_groups` groups. The server MUST NOT
split a group to fit `max_items`. If the next group in order alone exceeds
`max_items`, the claim MUST fail the envelope with `batch-too-large` and lease
nothing. Items with a null `group_key` are not eligible for a `group_batching`
claim. A `group_batching` claim MUST acquire an exclusive logical lock on each
selected `group_key` for the claim's serialized critical section, acquiring locks
for all claim modes in a single canonical order (ascending by group lock identity
within the shard) to avoid deadlock; a contended group's lock is skipped
(non-blocking), never split. No concurrent claim of any mode may lease a subset
of a locked group, so whole-group atomicity holds against generic and
`same_group_key` claims. `group_batching` is valid only on queues created with
group co-residency (`group_co_residency=true`); group selection is shard-local
and the target shard is resolved server-side (the request carries no `shard_id`),
chosen so the claim drains the queue-global oldest groups first. `group_batching`
introduces no rate or quota admission stage; pacing to downstream systems is the
caller's responsibility (see PRD downstream-rate non-goal).

When `whole_cohort=true`, a cohort is *complete* when the count of its
non-terminal members equals `cohort_size`, and *claim-eligible* only when, in
addition, every member is individually claim-eligible under **Eligibility
Precedence conditions 0–5** (queue admin state, lifecycle, lease, timing, static
gates, dynamic gates). A member excluded by any of those conditions makes the whole cohort not
claim-eligible. The server MUST select the most-urgent complete, claim-eligible
cohort by its representative priority (the most-urgent member under the queue's
ordering direction and tie-breaker) and MUST lease all members in one atomic
operation under one `cohort_lease_token`, returning one `cohort_id`. The selected
cohort's row MUST be locked first and its completeness and per-member eligibility
rechecked under that lock before leasing (see TD-002 / TD-004 claim flow). A
cohort whose row or members cannot be locked, or whose under-lock recheck fails,
MUST be skipped to the next complete claim-eligible cohort, or return empty if
none remains; the server MUST NOT block on a contended cohort and MUST NOT
partially lease. Item order **within** the returned cohort MUST be the queue's
deterministic claim order over the cohort as the effective claim domain
(ADR-004); order **across** distinct cohorts is unspecified beyond representative
priority. Cohort members MUST NOT be claimed by any item-level or `whole_group`
claim while any sibling is non-terminal; the only transition that exposes members
to other paths is `cohort_policy.on_incomplete`, which makes them terminal. A
cohort that is not complete-and-claim-eligible by its expiry deadline
(`min(cohort_created_at, first_eligible_at) + completion_bound_ms`) MUST be
expired per `on_incomplete`. If a complete cohort's `cohort_size` exceeds
`max_items`, the envelope MUST fail with `batch-too-large` (it MUST NOT split or
silently skip the cohort). `whole_cohort` selection is shard-local because the
queue's group co-residency capability places every `group_key` on one shard
(ADR-004 / D2).

**Caller-driven downstream pacing.** pqueue does not enforce downstream API rate
limits or quotas; the claim path applies **no downstream-rate admission or
throttling**. A `BatchClaim` MAY still return fewer than `max_items` items, or an
empty batch, for ordinary reasons — eligibility, compatibility filters, active
leases, contention, the `max_items` upper bound, whole-unit no-fit behavior, or
backend concurrency — but it never withholds otherwise-eligible work for a
downstream-rate reason. A caller paces work to a downstream system using the
following native-contract levers: `max_items` (cap a batch to the downstream
batch limit), the caller's interval between claim calls (honor a per-window
request limit), `not_before` on push/update and `retry.not_before` on finalize
(defer individual items, including reactive backoff after a downstream
rejection), group selection (`compatibility.group_key`,
`compatibility.same_group_key`, `compatibility.metadata_equals`) to pace one
downstream target independently, the per-claim group-batching bound
(`compatibility.group_batching.max_groups`, g1), and active-scope routing
(`DiscoverActiveScopes`, g4, which MAY be omitted in embedded single-queue
deployments). pqueue adds no `rate_policy`, token bucket, or per-claim rate gate.

### Eligibility Precedence

This subsection is the single authoritative definition of claim eligibility and
of an "active group/scope." Every other operation and design that needs "eligible
item," "eligible age," or "active group" MUST reference this subsection rather
than restating it.

Eligibility is defined in two clearly separated parts: **(A) base item
eligibility** (intrinsic to the item and queue state, independent of any request)
and **(B) request-specific claim resolution** (which eligible items a particular
`BatchClaim` returns). `DiscoverActiveScopes` and progress-bound metrics use part
(A) only; they have no claim request and MUST NOT apply part (B) filters.

#### (A) Base item eligibility

An item is **base-eligible** only if **all** of the following hold (a
conjunction; later conditions MUST NOT override earlier exclusions):

0. **Queue admin state**: the queue is not operator-paused (`queue_admin_paused`
   is unset; API-002 `PauseQueue`). A paused queue makes every item ineligible
   under this single definition — it is not a separate eligibility or accrual
   rule.
1. **Lifecycle**: the item is `pending` and not terminal.
2. **Lease**: the item has no active lease.
3. **Timing**: `not_before` has passed and any retry backoff has elapsed.
4. **Static metadata gates**: no `eligibility_policy.metadata_blockers` entry
   matches the item.
5. **Dynamic gates**: none of the item's `gate_keys` is `blocked` in the queue's
   gate state.

An item that fails any of 0–5 is **ineligible** and MUST NOT be reported toward
the progress bound while it remains so (FR-10). pqueue defines **no rate, quota,
or downstream-pacing eligibility condition**; downstream pacing is the caller's
responsibility and, if a caller chooses to pause a scope for pacing, it does so
by `blocked`ing a gate key it owns (condition 5) — ordinary ineligibility, not a
separate accrual class. An "active group/scope" (for `DiscoverActiveScopes`) is a
`group_key` with at least one base-eligible item.

#### (B) Request-specific claim resolution

A `BatchClaim` resolves, in order, over the base-eligible set. These stages select
*which* base-eligible items are returned; they MUST NOT promote an item excluded
by 0–5:

6. **Claim-domain filters**: the request's `compatibility.group_key`,
   `compatibility.same_group_key`, and `compatibility.metadata_equals` restrict to
   the requested **effective claim domain** (ADR-004). `same_group_key` is an
   **item-level domain filter** that constrains a single claim to one
   server-selected `group_key`; it is NOT a whole-group claim unit and carries no
   completeness or atomicity guarantee.
7. **Effective claim domain + claim-unit resolution**: the resolved claim unit is
   one of `item` (default, bounded by `max_items`), `whole_group`
   (`compatibility.group_batching`, bounded by `max_groups`), or `whole_cohort`
   (`cohort_policy`). Whole-group/whole-cohort units are selected only from
   base-eligible items (0–5) within the effective claim domain and are leased
   all-or-nothing per their owning contracts. **No `claim_scope` field exists**;
   group co-residency (D2/ADR-004) is a placement capability, not a claim scope.
   Whole-group / whole-cohort atomicity is provided by co-residency placement, not
   by any scope field.
8. **Progress protection (FR-9/FR-12, queue-global)**: progress-bound protection
   is folded **into** claim-unit ordering, not applied as a post-selection stage:
   among base-eligible items (and their representatives, for unit modes), the
   progress guard MAY reorder selection to prevent starvation, but MUST NOT promote
   an item excluded by 0–5. The progress bound is **queue-global** (D1): one
   queue-wide bound, aggregated across shards (TD-003); the gate predicate
   (condition 5) is applied per shard before aggregation so gate-blocked items
   never count toward queue-global oldest-eligible age. Per-group fairness is
   achieved by routing workers via `DiscoverActiveScopes`, not by a per-group
   progress invariant.

**Eligible age** is measured from the item's stored `eligible_since` and is
reported only while the item is base-eligible (conditions 0–5). A gate flip
changes which items satisfy condition 5 with no write to any item row; it
therefore changes reported eligible age **without** changing `eligible_since`.
Reopening a blocked gate does not reset or retroactively deduct the time the item
spent blocked.

**Recurring re-arm and the timing stage.** A `rearm` finalize returns a recurring
item to `pending` with a caller-supplied `not_before`, resets the per-cycle
transient-retry counter to 0, and materializes the effective eligible instant at
finalize commit as `max(commit_time, rearm.not_before)`, recorded as
`eligible_since`; pqueue does NOT fire a command at wall-clock passage of
`not_before`. The re-armed item re-enters the precedence at **stage 3 (Timing)**:
it is ineligible until wall-clock reaches `not_before` (idle), then becomes a
candidate that MUST still satisfy stages 1–2 (lifecycle, lease) and stages 4–5
(static and dynamic gates) like any other item. The interval before `not_before`
is *ineligible time* and MUST NOT accrue eligible age (FR-10). Once eligible, the
item is subject to the single queue-global progress bound measured from the
recorded `eligible_since`, identical to any other eligible pending item. There is
no recurring-specific eligibility rule and no per-group progress bound.

### Batch Renew

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchRenewLeases` | operation | yes | MUST renew one or more active leases and return per-item results. | Best-effort per item. |
| `leases[].item_id` | string | yes (item/group leases) | MUST identify the leased item. | |
| `leases[].lease_token` | string | yes (item/group leases) | MUST match the active lease token. | Stale token fails per item. |
| `leases[].cohort_id` + `leases[].cohort_lease_token` | string + string | conditional | For a cohort, `BatchRenewLeases` MUST target the cohort by `cohort_id`+`cohort_lease_token`, renewing all members together. A per-member renew of a cohort member (by `item_id`+`lease_token`) MUST be rejected `invalid` (no per-member token exists). | Cohort is the renew unit. |
| `lease_duration_ms` | integer | yes | MUST be greater than 0 and no more than `max_lease_duration_ms` or a lower deployment cap. | Applies to all submitted leases. |

### Batch Finalize

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchFinalize` | operation | yes | MUST finalize or release one or more leased items and return per-item results. | Best-effort per item. |
| `finalizations[].item_id` | string | yes (item/group leases) | MUST identify the leased item. | |
| `finalizations[].lease_token` | string | yes (item/group leases) | MUST match the active lease token. | Stale token fails per item. |
| `finalizations[].cohort_id` + `finalizations[].cohort_lease_token` | string + string | conditional | For a cohort, `BatchFinalize` MUST target the cohort by `cohort_id`+`cohort_lease_token` with one `outcome` applied to the whole cohort. `complete`/`fail` make all members terminal; `release` re-pends the complete cohort; `retry` re-pends it with one shared `not_before`. Per-member partial finalize MUST be rejected `invalid`. | Whole-cohort finalize/release/retry. |
| `finalizations[].outcome` | enum | yes | MUST be one of `complete`, `fail`, `retry`, `release`, `rearm`. `rearm` is valid only when the queue's `recurrence.mode` is `recurring` and `recurrence.until` (if set) has not passed; otherwise the item MUST fail per item with `invalid` (wrong mode) or `terminal` (past `until`). | `rearm` is the recurring re-arm outcome. |
| `finalizations[].retry.not_before` | timestamp | required for `retry` | MUST set next eligibility time. | v1 has no implicit retry delay default. |
| `finalizations[].retry.priority` | tagged scalar | no | If present, MUST replace priority and match queue priority model. | |
| `finalizations[].rearm.not_before` | timestamp | **yes for `rearm`** | MUST be present for `rearm`. The item's effective eligible instant MUST be `max(commit_time, rearm.not_before)` (deterministic; see Eligibility Precedence timing stage). A `rearm` without `not_before` MUST fail per item with `invalid`. | Caller owns backoff math in v1. |
| `finalizations[].rearm.priority` | tagged scalar | no | If present, MUST replace the item's priority and MUST match the queue priority model; otherwise priority is unchanged. | |
| `finalizations[].failure_code` | string | no | SHOULD be present for `fail`. | Caller-defined. |
| `finalizations[].metadata` | JSON object / map | no | MAY store finalization or retry metadata. | Transport adapters define size limits. |
| `finalizations[].member_outcomes` | array | no | MAY carry per-member `{item_id, status}` for observability ONLY; it MUST NOT split the cohort lifecycle and MUST NOT cause a partial outcome. | Observability of which members succeeded inside an all-or-nothing batch. |

**Cohort renew/finalize target.** For a cohort claimed with `whole_cohort=true`,
the per-item required fields `leases[].item_id`+`leases[].lease_token` (renew) and
`finalizations[].item_id`+`finalizations[].lease_token` (finalize) are REPLACED by
a single cohort target. A renew/finalize entry targets a cohort when it carries
`cohort_id`+`cohort_lease_token`; item-level `lease_token` MUST NOT be present for
cohort members (they were never issued). Mixing a `cohort_lease_token` with an
`item_id`+`lease_token` in the same entry MUST be rejected `invalid`. The
renew/finalize response MUST report one result for the cohort (keyed by
`cohort_id`), not N per-member results, with status `ok` / `request-expired` /
`invalid` for the cohort as a unit. If `member_outcomes` was supplied for
observability, it MAY be echoed back unchanged; it carries no lifecycle authority.

A `rearm` finalize MUST release the lease, set `not_before` to the supplied
value, reset the per-cycle transient-retry counter to 0, and return the item to
`pending`. The item's effective eligible instant MUST be materialized at finalize
commit as `max(commit_time, rearm.not_before)` and recorded as `eligible_since`.
A `rearm` finalize with a stale lease token MUST return `stale_lease` (item
unchanged), exactly like other lease-bearing finalize outcomes.

### Purge Items

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `PurgeItems` | operation | yes | MUST remove one or more items by `client_item_key` (and/or `item_id`) regardless of lifecycle state, returning per-item results. MUST be idempotent under `request_id`. MUST fail per item with `conflict` if the item is currently `leased`, unless `force=true` is supplied (which MUST also invalidate the active lease). A successful per-item result MUST be `purged`; an absent item MUST return `not_found` (a duplicate-replay-friendly success-equivalent, see replay rules). | Targeted parent-driven teardown for recurring items. P0 native scope; distinct from the P1 operator purge contract. |
| `items[].client_item_key` | string | conditional | MUST identify the item to purge when `item_id` is absent. | |
| `items[].item_id` | string | conditional | MUST identify the item to purge when `client_item_key` is absent; if both present they MUST refer to the same item. | |
| `force` | boolean | no | If true, MUST purge even a `leased` recurring item and invalidate its lease. | Teardown while a tick is in flight. |

A `PurgeItems` removal MUST be recorded as a durable `PurgeItemsCommand` and MUST
write a **tombstone** keyed by `(tenant_id, queue_id, client_item_key)` that
persists for at least `client_item_key_retention_ms`. While the tombstone is
live: a duplicate `request_id` MUST return the recorded per-item result
(`purged`/`not_found`) and MUST NOT re-delete or change `item_version`; any
`complete`/`fail`/`retry`/`release`/`rearm` for a purged item MUST fail per item
with `not_found`; a `BatchPush` with the same `client_item_key` after the
tombstone window MUST create a fresh item (new `item_id`, `item_version` reset to
its initial value). The item row is deleted (never a live row again under the same
`item_id`); replay and audit are served by the command log + tombstone.

### Queue Metrics

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `GetQueueMetrics` | operation | yes | MUST return point-in-time queue metrics for one queue. | Observability operation. |
| `metrics.lifecycle_counts` | object | yes | MUST include `pending`, `leased`, `complete`, and `failed`. `failed` MUST NOT be inflated by recurring items; a recurring item reaches `failed` only via an explicit terminal finalize or in-cycle retry exhaustion. | May be approximate if documented. |
| `metrics.retry_backlog` | integer | yes | MUST count pending items with transient-retry metadata that are not terminal. Recurring items that are merely re-armed (no in-cycle `retry` outstanding) MUST NOT be counted. | May be approximate if documented. |
| `metrics.oldest_eligible_age_ms` | integer / null | yes | MUST be null if no eligible item exists. For a sharded queue, MUST be the queue-global oldest-eligible age computed across all shards (the maximum eligible age over shards = `now() - min(oldest_eligible_at)`). MUST be authoritative/exact as of the aggregate `as_of` (minimum shard watermark). | Single queue-global bound (D1). |
| `metrics.progress_bound_risk_count` | integer | yes | MUST count or estimate eligible items near `progress_bound_ms` summed across shards. MAY be approximate when documented; the oldest-eligible age MUST remain exact. | Counts MAY lag; age MUST NOT. |
| `metrics.active_leases` | integer | yes | MUST count active leases. | |
| `metrics.recurring_pending` | integer | conditional | On a `recurring` queue MUST count recurring items in `pending` (armed/idle), whether or not `not_before` is in the future. MUST be 0 / omitted on `oneshot` queues. MAY be approximate if documented. | Idle recurring inventory. |
| `metrics.recurring_leased` | integer | conditional | On a `recurring` queue MUST count recurring items currently `leased` (actively ticking). MAY be approximate if documented. | Active recurring work. |

The recurring counters are served from the `metrics` envelope (the
`lifecycle_counts`-family observability fields), NOT from `pqueue_group_summary`,
which holds only `oldest_eligible_at` and eligible/at-risk counts and is the
selection/discovery source. `metrics.oldest_eligible_age_ms` is unchanged for
recurring items: an idle (future `not_before`) recurring item is ineligible and
MUST NOT contribute; a re-armed item that has become eligible contributes exactly
like any eligible item.

### Active-Scope Discovery

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `DiscoverActiveScopes` | operation | should (MUST native service mode) | MUST return a read-only, top-N ranking of queues, or group keys within a queue, that currently have eligible work for the principal, computed as of an observed projection frontier (`as_of`). MUST NOT lease, mutate, reserve, or append to the command log. MUST NOT be used to implement atomic multi-group or cohort selection; `BatchClaim` and cohort mode remain authoritative for reservation. | Per-group fairness routing mechanism. |
| `tenant_id` | string | yes | MUST scope discovery; results MUST be restricted to queues the principal holds `queue:read` for. | Per-queue authorization, see Authorization Rules. |
| `queue_id` | string | no | If present, discovery MUST be restricted to that queue and `granularity` defaults to `group`. If absent, discovery spans the principal's authorized queues (tenant-scoped top-N across queues) and `granularity` defaults to `queue`. | Operation-specific exception to the global `queue_id` requirement. |
| `granularity` | enum | no | MUST be `queue` or `group`. `group` MUST require a resolvable `queue_id`. | Default per `queue_id` presence. |
| `group_key` | string | no | If present, results MUST be restricted to that group key. Valid whether or not the queue has `group_co_residency=true`. | Reuses claim group vocabulary; carries no claim-unit semantics. |
| `max_results` | integer | no | If present, MUST be greater than 0; bounds returned descriptors after the cross-shard merge. Server MAY enforce a lower deployment cap. | Top-N by rank, AFTER auth + enumeration bounding. |
| `page_token` | string | no | Opaque forward cursor. When more authorized queues exist than the enumeration bound, the server MUST return `response.next_page_token`; the caller MUST page to enumerate all queues. `max_results` bounds returned descriptors, NOT control-plane enumeration or auth fanout. | Bounds enumeration/auth fanout. |
| `response.as_of` | timestamp | yes | MUST be the **observed projection frontier**: the most conservative (minimum) per-row `updated_at` watermark across EVERY shard read for the result, including shards that returned no rows and shards that were stale or unowned at read time (their last-known watermark). `oldest_eligible_age_ms` is exact as of this frontier; `eligible_count`/`progress_bound_risk_count`, when present, MAY lag it. | Observed-frontier guarantee, NOT a global serializable point-in-time. |
| `response.next_page_token` | string / null | no | Present when more authorized queues remain to enumerate. | Pagination. |
| `response.active_scopes[]` | array | yes | MUST be ordered by `oldest_eligible_age_ms` descending (oldest-eligible first), across all the queue's shards, using the queue's Eligibility Precedence predicate, gate-current at read time. Scopes with no eligible work MUST be omitted. Empty array is valid. | Top-N, NOT a per-shard top-N union. |
| `active_scopes[].queue_id` | string | yes | MUST identify the queue. | |
| `active_scopes[].group_key` | string / null | present when `granularity=group` | MUST identify the group when group granularity is used; MUST be absent for queue granularity. Items with no `group_key` MUST be aggregated under a single `null` group descriptor when `granularity=group`. This `null` descriptor is the **ungrouped-items** scope and is NOT a per-queue rollup row. | Null-group handling, see below. |
| `active_scopes[].oldest_eligible_age_ms` | integer | yes | MUST be the eligible age of the oldest eligible item in the descriptor's scope, across the queue's shards, using the Eligibility Precedence predicate, gate-current. MUST be exact as of `as_of`. | Same semantic as `metrics.oldest_eligible_age_ms`. |
| `active_scopes[].eligible_count` | integer / null | no | If present, MAY be approximate/lagged and MUST be documented as such. v1 MAY omit it. | Routing hint, not a claim guarantee. |
| `active_scopes[].progress_bound_risk_count` | integer / null | no | If present, MAY be approximate/lagged and MUST be documented as such. v1 MAY omit it. | At-risk hint. |

**Eligibility (single source).** A scope's eligibility MUST be defined by the
exact predicate in the **Eligibility Precedence** subsection of this contract
(the same predicate `BatchClaim` uses). An item contributes to a scope's
`oldest_eligible_age_ms`/counts if and only if it satisfies that subsection's
item-level eligibility conditions (lifecycle, lease, timing, static metadata
gates, dynamic gates) and the request's caller filters. Discovery MUST NOT report
as eligible any item `BatchClaim` would not currently return. There is no second
definition of "eligible" or "active group".

**Gate-current discovery (advance, do not just exclude).** Dynamic gate state is
queue-scoped and flips O(1) without writing item rows. Discovery MUST evaluate
the gate predicate at read time. A scope's `oldest_eligible_age_ms` MUST be the
age of the oldest item that satisfies Eligibility Precedence **including current
gate state** — if the stored representative (oldest) item is currently
gate-blocked, discovery MUST advance to the next item satisfying Eligibility
Precedence and report THAT item's age, rather than excluding the scope. A scope
is omitted only when NO item in it currently satisfies Eligibility Precedence.

**Cross-shard aggregation.** When a queue spans multiple shards,
`oldest_eligible_age_ms` and counts MUST be aggregated across all the queue's
shards before `max_results` is applied. The shard set, shard lease validity, and
epoch fencing are owned by the control-plane / shard-ownership design (TD-003);
discovery reads each shard's per-group summary projection (keyed
`(tenant_id, queue_id, shard_id, group_key)`) and merges by `(queue_id,
group_key)` taking the minimum oldest-eligible timestamp (== maximum age) and
summing counts BEFORE applying `max_results`. The returned top-N MUST be the true
cross-shard top-N, never a per-shard top-N union. This merge is correct whether
or not the queue has `group_co_residency=true`: under co-residency each
`group_key` appears in exactly one shard's summary (the merge is a union of
disjoint groups), and without co-residency a `group_key` MAY appear in several
shards' summaries (the merge takes the min timestamp across them). Discovery MUST
NOT require co-residency.

**Group descriptors and progress (D1).** A `group` descriptor names a fairness
routing target, NOT an independently progress-bounded scheduling partition. The
engine guarantees only the single queue-global progress bound; a group
descriptor's `oldest_eligible_age_ms` lets a fleet route to avoid per-group
starvation operationally. The response MUST NOT imply any per-group progress
guarantee.

**Null `group_key` (ungrouped items) vs. queue rollups.** For
`granularity=group`, items lacking a `group_key` MUST be aggregated under one
descriptor with `group_key: null` (not omitted, not a sentinel string). This
`null` descriptor represents the **ungrouped-items scope only**. It MUST NOT be
reused as a per-queue rollup of all groups. A per-queue oldest-eligible value is
obtained by `granularity=queue` (one descriptor per queue, `group_key` absent),
which the server derives as the min oldest-eligible across the queue's group rows.

**Advisory, staleness, and the `as_of` frontier.** Discovery is best-effort for
reservation: a descriptor reported active MAY be empty by claim time, and an
active scope MAY be absent if it became eligible after `as_of`. `as_of` is an
**observed projection frontier**, not a global serializable snapshot: it is the
most conservative watermark across every shard read (empty, stale, and unowned
shards included). Results are exact AS OF that frontier for
`oldest_eligible_age_ms`; absence of a scope means "no eligible item observed at
or before `as_of` on the shards read," not a proof of global absence. Workers
MUST treat `BatchClaim` as authoritative for reservation. Discovery MUST NOT
affect any item's progress-bound clock.

### Versioning Rules

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `item_version` bump on first accept | mutation rule | yes | Successful first acceptance MUST set `item_version=1`. | |
| `item_version` bump on update | mutation rule | yes | Successful `BatchUpdate` MUST increment `item_version`. | |
| `item_version` bump on claim | mutation rule | yes | Successful `BatchClaim` MUST increment `item_version` for each claimed item. | Lease state changed. |
| `item_version` bump on renew | mutation rule | yes | Successful `BatchRenewLeases` MUST increment `item_version`. | Lease expiry changed. |
| `item_version` bump on finalize | mutation rule | yes | Successful `BatchFinalize` outcomes `complete`, `fail`, `retry`, and `release` MUST increment `item_version`. | Lifecycle, retry, or lease state changed. |
| `item_version` bump on rearm | mutation rule | yes | A successful `rearm` MUST increment `item_version` (lease released, schedule re-armed, retry counter reset). Duplicate `request_id` replay MUST NOT increment again. | |
| command position on purge | mutation rule | yes | A successful `PurgeItems` removal MUST delete the item row and record a terminal `PurgeItemsCommand` position plus a tombstone for audit/replay. It does NOT bump a surviving `item_version` (the row is gone); duplicate replay returns the recorded result. | |
| `item_version` on `SetGates` | mutation rule | yes | `SetGates` MUST NOT change `item_version`, `eligible_since`, or any other field of any affected item. Gate state is queue-scoped, not item-scoped. (A `BatchUpdate` of an item's `gate_keys` IS an item mutation and bumps `item_version`.) | Basis of O(1) gating; reopen does not reset eligible age. |
| `item_version` on duplicate replay | mutation rule | yes | Duplicate `request_id` replay and duplicate push convergence MUST NOT increment `item_version`. | No new mutation. |
| `item_version` on lease expiry | mutation rule | no | Lease expiry MAY increment `item_version` when materialized by a backend, but clients MUST NOT depend on expiry alone preserving or changing the version. | Backend detail. |

## Precedence and Compatibility

- Versioning: breaking changes require a new major contract version.
- Transport compatibility: transport-specific APIs MAY add headers, pagination,
  compression, streaming, or authentication envelopes, but MUST preserve this
  operation model and response semantics.
- Exposure precedence: the native operation model is authoritative. Rust,
  HTTP, SDK, and compatibility surfaces are bindings over the native model.
  When a compatibility adapter cannot represent a native behavior, the adapter
  MUST document the limitation rather than weakening the native contract.
- Request idempotency: mutating operations MUST deduplicate repeated
  `request_id` values for the same tenant, queue, operation, and request body
  within `request_id_retention_ms`. A repeated `request_id` with
  a different request body MUST fail with `request-id-conflict`.
- Claim idempotency: `BatchClaim` request replay MUST return the same claimed
  set while the returned leases are active. Once all returned leases are
  finalized, released, or expired, replaying the same `request_id` MUST fail
  with `request-expired`; clients MUST use a new `request_id` for a new claim.
  A replayed `whole_cohort` claim MUST return the same cohort member set, the
  same `cohort_id`, and the same `cohort_lease_token` while the cohort lease is
  active; after the cohort lease is finalized, released, or expired, replay MUST
  fail with `request-expired`.
- Configuration identity: `group_co_residency`, `shard_count`, `ordering_mode`,
  `priority_model`, `recurrence.mode`, `eligibility_policy.gate_keys`, and
  `cohort_policy.enabled` participate in the queue's stable configuration identity
  used for idempotent create. A repeated `CreateQueue` with the same
  `tenant_id`/`queue_id` but a differing value for any of these MUST be rejected
  as a definition conflict (`queue-definition-conflict`).
- Push idempotency for cohort members: `BatchPush` convergence on
  `client_item_key` MUST be evaluated and committed in the same transaction as,
  and strictly **before**, any cohort `member_count` mutation. A duplicate
  `client_item_key` for an already-accepted member MUST be an idempotent no-op:
  it returns the existing member and MUST NOT increment `member_count` and MUST
  NOT affect completeness. Only a *new* distinct member increments `member_count`,
  after the overfill and `cohort_size`-conflict checks. Once a cohort is terminal
  and its retention window has elapsed, the same `group_key` MAY be reused to
  start a fresh cohort under a NEW `cohort_id`; a push to a `group_key` whose
  cohort is terminal but still within retention MUST be rejected per-item
  `conflict`.
- Command replay for rearm/purge: the finalize command for a `rearm` outcome MUST
  record the effective values applied (resulting `lifecycle_state` `pending`, the
  supplied `not_before`, the recorded effective `eligible_since` =
  `max(commit_time, not_before)`, the effective `priority`, the reset retry
  counter, the released lease state, and the resulting `item_version`). Replay of
  a duplicate `request_id` MUST return the recorded result and MUST NOT recompute
  eligibility, priority, version, or `eligible_since`. `PurgeItems` replay returns
  the recorded per-item result and MUST NOT re-delete.
- Discovery semantics: `DiscoverActiveScopes` is a non-transactional read
  returning a top-N ranking computed as of an observed projection frontier
  (`as_of`) across the shards read, with no claim reservation. It MUST NOT be used
  as an atomic multi-group or cohort selector. It is advisory for reservation but
  is the per-group fairness routing mechanism. A compatibility adapter MAY omit
  discovery and MUST document the omission.
- Batch precedence: batch envelopes fail only for envelope-level problems such
  as authentication, authorization, missing queue, malformed request, or backend
  outage. Item-level validation, duplicates, stale leases, terminal-state
  conflicts, and not-found conditions MUST be reported per item.
- Atomicity: `CreateQueue` is atomic. `BatchClaim` MUST atomically create each
  returned lease. When `compatibility.group_batching` is used, each **selected
  group is additionally an all-or-nothing unit**: every currently-eligible item of
  a selected group (within the effective claim domain) is leased together or the
  group is not leased at all. When `compatibility.whole_cohort=true`, `BatchClaim`
  is additionally all-or-nothing for the selected cohort: either every member of
  one complete, claim-eligible cohort is leased in the same atomic operation under
  one `cohort_lease_token`, or no cohort member is leased; the cohort row is the
  lock unit, taken by item/`whole_group` claims, `whole_cohort` claims, and the
  expiry sweeper, so a member is never simultaneously individually-claimed,
  cohort-claimed, and expired. Because each `group_key` is co-resident on one shard
  (ADR-004), these whole-unit atomicity guarantees are shard-local. `BatchPush`,
  `BatchUpdate`, `BatchRenewLeases`, and `BatchFinalize` remain best-effort with
  per-item outcomes (except that cohort renew/finalize act on the whole cohort
  under `cohort_id`+`cohort_lease_token`). These whole-group and whole-cohort modes
  are the explicit all-or-nothing claim modes anticipated by this contract.
- Ordering: response result arrays for push, update, renew, and finalize MUST
  preserve request order. Claim responses MUST preserve the queue's deterministic
  claim result order **within the request's effective claim domain** (ADR-004); a
  single-`group_key` domain on a `group_co_residency=true` queue yields exact
  per-group order. Ordering across distinct `group_key`s within one response (e.g.
  a `group_batching` multi-group claim) is unspecified except as that claim mode's
  own contract defines it. `shard_id` MUST NOT influence client-visible result
  order.
- Backward compatibility: v1 clients MAY ignore unknown response fields. v1
  servers MUST NOT remove or rename fields in this contract without a new major
  version.
- Deprecation: deprecated fields MUST remain accepted for at least one stable
  minor release after replacement is documented.

## Error Semantics

Envelope errors SHOULD use RFC 9457 problem-details shape when transported over
HTTP. Library bindings SHOULD map the same `code` values to typed errors.

| Condition | Error / Outcome | Retry | Recovery Expectation |
|-----------|------------------|-------|----------------------|
| Missing or unauthorized tenant/queue | Envelope error `queue-not-found` or `queue-forbidden` | no | Use a queue visible to the caller. |
| Queue definition conflicts with existing queue | Envelope error `queue-definition-conflict` | no | Use existing compatible definition or create a new queue ID. |
| Reused `request_id` with different request body | Envelope error `request-id-conflict` | no | Generate a new `request_id` for different logical work. |
| Reused claim `request_id` after leases are no longer active | Envelope error `request-expired` | yes with new request ID | Submit a new claim request. |
| Malformed envelope or unsupported priority type | Envelope error `invalid-request` | yes after fix | Correct request shape. |
| Batch exceeds configured maximum | Envelope error `batch-too-large` | yes after fix | Split the batch. |
| Backend cannot durably commit before timeout | Envelope error `commit-timeout` or per-item `unavailable` | yes with same `request_id` and item keys | Retry after backoff; duplicate accepted items converge. |
| Duplicate push within retention window | Per-item `duplicate` | no | Treat as successful convergence. |
| Item priority does not match queue model | Per-item `invalid` | yes after fix | Submit a valid priority. |
| Update targets a leased item | Per-item `conflict` | maybe | Re-claim or wait for lease expiry; use finalize/renew for active leases. |
| `expected_item_version` does not match current version | Per-item `conflict` | yes after refresh | Read/claim current item state or retry with updated version. |
| Target item is terminal | Per-item `terminal` | no | Do not update/finalize terminal items except through repair APIs. |
| Lease token is stale, missing, or expired | Per-item `stale_lease` | no for same token | Re-claim if item becomes eligible. |
| Item not found by `item_id` or `client_item_key` | Per-item `not_found` | maybe | Verify reference or wait for eventual visibility only if backend documents it. |
| Item pushed without `group_key` to a `group_co_residency=true` queue | Per-item `invalid` | yes after fix | Supply `group_key`. |
| `group_batching` combined with `same_group_key` or explicit `group_key`, or `max_groups` <= 0, or used on a queue without group co-residency or without `max_eligible_group_size` | Envelope error `invalid-request` | yes after fix | Use one claim mode; enable `group_batching` only on `group_co_residency=true` queues that define `max_eligible_group_size`. |
| Next selected whole group exceeds `max_items` for a `group_batching` claim | Envelope error `batch-too-large` | yes after fix | Raise `max_items` so one whole group fits; ensure `max_eligible_group_size <= max_claim_batch_size`. |
| `BatchPush` item would push its `group_key` over `max_eligible_group_size` | Per-item `group-too-large` | yes after fix | Drain the group, or raise `max_eligible_group_size` (still `<= max_claim_batch_size`). |
| `whole_cohort=true` on a queue without `cohort_policy.enabled` | Envelope error `invalid-request` | yes after fix | Enable cohort policy, or use group-aware claim. |
| `whole_cohort` combined with `same_group_key`, `group_key`, or `group_batching` | Envelope error `invalid-request` | yes after fix | Use exactly one claim unit. |
| Cohort-enabled queue created without group co-residency capability | `queue-definition-conflict` | yes after fix | Create the queue with `group_co_residency=true`. |
| `completion_bound_ms > progress_bound_ms` at `CreateQueue` | `queue-definition-conflict` | yes after fix | Set `completion_bound_ms <= progress_bound_ms`. |
| `cohort_size` conflicts across members of one `group_key` | Per-item `conflict` | yes after fix | Push all members with the same `cohort_size`. |
| Member would overfill `cohort_size` (`member_count` would exceed `cohort_size`) | Per-item `conflict` | no | The cohort is already full; do not push extra members. |
| `cohort_size` exceeds `max_cohort_size`, or cohort fields on a non-cohort queue | Per-item `invalid` | yes after fix | Reduce cohort size or remove cohort fields. |
| Complete cohort's `cohort_size` exceeds `max_items` for a `whole_cohort` claim | Envelope `batch-too-large` | yes after fix | Raise `max_items` to at least `cohort_size`. |
| Per-member finalize/renew on a cohort member, or mixed cohort+item-level target | Per-item `invalid` | no | Use `cohort_id`+`cohort_lease_token` for the whole cohort. |
| Incomplete or gated cohort past its expiry deadline (`on_incomplete=expire_cohort`) | Members terminal `failed`, code `cohort-incomplete` | no | Re-push the full cohort (new `cohort_id`) if still needed. |
| `rearm` on a non-recurring queue | Per-item `invalid` | no | Use `complete`/`fail`/`retry`/`release`, or create the queue with `recurrence.mode=recurring`. |
| `rearm` without `rearm.not_before` | Per-item `invalid` | yes after fix | Supply `rearm.not_before`. |
| `rearm` after `recurrence.until` | Per-item `terminal` | no | Finalize terminally or `PurgeItems`. |
| `PurgeItems` on a leased item without `force` | Per-item `conflict` | maybe | Wait for lease expiry/finalize, or retry with `force=true`. |
| Finalize (`complete`/`fail`/`retry`/`release`/`rearm`) on a purged item (live tombstone) | Per-item `not_found` | no | The item was intentionally removed; do not re-create unless re-pushing a fresh logical item. |
| `SetGates` on a queue with `gate_keys = none` | Envelope error `gates-not-enabled` | no | Create a queue with `eligibility_policy.gate_keys = dynamic`. |
| Item carries `gate_keys` on a `gate_keys = none` queue | Per-item `invalid` | yes after fix | Remove `gate_keys` or use a gate-enabled queue. |
| `SetGates` batch exceeds `max_gates_per_request`, is empty, or has an invalid key/state | Envelope error `invalid-request` (whole envelope rejected, nothing applied) | yes after fix | Correct/split the gate set. |
| `SetGates` not converged across all occupied shards before `commit-timeout` | Envelope `commit-timeout` (some shards may have applied this `gate_epoch`) | yes by same `request_id` | Retry by `request_id`; convergence is idempotent per `gate_epoch`. |
| Discovery `granularity=group` with no resolvable queue | Envelope error `invalid-request` | yes after fix | Provide a `queue_id` for group-granularity discovery. |
| Discovery names a queue the principal cannot read | Envelope error `queue-forbidden` or `queue-not-found` | no | Use a queue visible to the caller. |
| pqueue deployment or tenant capacity limit exceeded | Envelope error (rate-limit / capacity error) | yes | Back off per retry guidance. This applies ONLY to pqueue's own deployment/tenant capacity controls (P1) and is an envelope-level admission outcome — pqueue rejects or defers the whole request before per-item processing. pqueue does not rate-limit on behalf of a caller's downstream API. |

## Examples

```json
{
  "operation": "BatchPush",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "req_20260606_001",
  "items": [
    {
      "client_item_key": "action:123",
      "priority": { "timestamp": "2026-06-06T14:30:00Z" },
      "not_before": "2026-06-06T14:30:00Z",
      "payload": { "action_id": 123 },
      "metadata": {
        "account_id": "acct_7",
        "connector": "marketo",
        "campaign_id": "cmp_55"
      },
      "group_key": "acct_7:marketo"
    }
  ]
}
```

```json
{
  "request_id": "req_20260606_001",
  "results": [
    {
      "client_item_key": "action:123",
      "item_id": "itm_01JX2A7Y6VMT5DRF7YZ1DN7G6W",
      "item_version": 1,
      "status": "accepted"
    }
  ]
}
```

```json
{
  "operation": "BatchUpdate",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "req_20260606_001b",
  "updates": [
    {
      "item_ref": {
        "client_item_key": "action:123"
      },
      "expected_item_version": 1,
      "priority": { "timestamp": "2026-06-06T14:45:00Z" },
      "not_before": "2026-06-06T14:45:00Z",
      "metadata": {
        "account_id": "acct_7",
        "connector": "marketo",
        "campaign_id": "cmp_55",
        "paused": false
      }
    }
  ]
}
```

```json
{
  "request_id": "req_20260606_001b",
  "results": [
    {
      "client_item_key": "action:123",
      "item_id": "itm_01JX2A7Y6VMT5DRF7YZ1DN7G6W",
      "item_version": 2,
      "status": "updated"
    }
  ]
}
```

```json
{
  "operation": "BatchClaim",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "req_20260606_claim_001",
  "worker_id": "worker_17",
  "max_items": 100,
  "lease_duration_ms": 300000,
  "compatibility": {
    "same_group_key": true,
    "metadata_equals": {
      "connector": "marketo"
    }
  }
}
```

```json
{
  "request_id": "req_20260606_claim_001",
  "items": [
    {
      "item_id": "itm_01JX2A7Y6VMT5DRF7YZ1DN7G6W",
      "client_item_key": "action:123",
      "item_version": 3,
      "priority": { "timestamp": "2026-06-06T14:45:00Z" },
      "payload": { "action_id": 123 },
      "metadata": {
        "account_id": "acct_7",
        "connector": "marketo",
        "campaign_id": "cmp_55"
      },
      "group_key": "acct_7:marketo",
      "lease_token": "lease_7Fz6T3uA2w",
      "lease_expires_at": "2026-06-06T14:35:03Z"
    }
  ]
}
```

```json
{
  "operation": "BatchFinalize",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "req_20260606_002",
  "finalizations": [
    {
      "item_id": "itm_01JX2A7Y6VMT5DRF7YZ1DN7G6W",
      "lease_token": "lease_7Fz6T3uA2w",
      "outcome": "complete"
    }
  ]
}
```

```json
{
  "request_id": "req_20260606_002",
  "results": [
    {
      "item_id": "itm_01JX2A7Y6VMT5DRF7YZ1DN7G6W",
      "status": "completed"
    }
  ]
}
```

Whole-eligible-group claim (`group_batching`). Response: up to 300 distinct
wholly-available `group_key` values, all currently-eligible items per selected
group that match `connector=marketo`, each item carrying its own `lease_token`;
total items <= 5000; no group partially returned. Target shard resolved
server-side.

```json
{
  "operation": "BatchClaim",
  "tenant_id": "tenant_acme",
  "queue_id": "marketo_lead_enrichment",
  "request_id": "req_20260606_claim_leads_001",
  "worker_id": "enrich_worker_3",
  "max_items": 5000,
  "lease_duration_ms": 300000,
  "compatibility": {
    "group_batching": { "max_groups": 300, "group_completeness": "whole_eligible" },
    "metadata_equals": { "connector": "marketo" }
  }
}
```

Whole-cohort claim and response (one shared `cohort_lease_token`, NO per-item
`lease_token`):

```json
{
  "operation": "BatchClaim",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "req_20260606_cohort_001",
  "worker_id": "worker_17",
  "max_items": 500,
  "lease_duration_ms": 300000,
  "compatibility": { "whole_cohort": true }
}
```

```json
{
  "request_id": "req_20260606_cohort_001",
  "cohort_lease_token": "clease_9Qz2",
  "cohort_id": "coh_01JX...",
  "items": [
    { "item_id": "itm_a", "group_key": "callback_42", "item_version": 2 },
    { "item_id": "itm_b", "group_key": "callback_42", "item_version": 2 },
    { "item_id": "itm_c", "group_key": "callback_42", "item_version": 2 }
  ]
}
```

```json
{
  "operation": "BatchFinalize",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "req_20260606_cohort_fin_001",
  "finalizations": [
    { "cohort_id": "coh_01JX...", "cohort_lease_token": "clease_9Qz2", "outcome": "complete" }
  ]
}
```

Dynamic gate flip (`SetGates`). The `gate_key` values below are generic
illustrations of opaque gate keys, independent of any queue's `group_key`
topology:

```json
{
  "operation": "SetGates",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
  "request_id": "req_20260606_gate_001",
  "gates": [ { "gate_key": "acct_7", "state": "blocked" } ]
}
```

```json
{
  "request_id": "req_20260606_gate_001",
  "gate_epoch": 42,
  "gates": [ { "gate_key": "acct_7", "state": "blocked" } ],
  "shards": [ { "shard": "<opaque>", "applied_command_position": "...", "converged": true } ]
}
```

Active-scope discovery (`DiscoverActiveScopes`). The `group_key: null` descriptor
is the ungrouped-items scope, NOT a queue rollup; there is no `claim_scope` field:

```json
{ "operation": "DiscoverActiveScopes", "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions", "granularity": "group", "max_results": 10 }
```

```json
{ "as_of": "2026-06-06T12:00:00Z",
  "active_scopes": [
    { "queue_id": "scheduled_actions", "group_key": "acct_7:marketo",
      "oldest_eligible_age_ms": 30000 },
    { "queue_id": "scheduled_actions", "group_key": null,
      "oldest_eligible_age_ms": 5000 } ] }
```

## Non-Normative Notes

The native contract intentionally exposes batch operations first. Transport
adapters may offer convenience single-item methods, but those should be client
wrappers over batch operations because pqueue's cost, durability, and throughput
model depends on batching.

`CreateQueue` is included in the native API because queue definition controls
client-visible priority, eligibility, idempotency, and batch semantics. Broader
administrative operations such as shard placement, backend migration, repair,
redrive, and retention management should be defined in separate operator
contracts. Targeted recurring teardown (`PurgeItems`, addressed per-key/item-id)
is in-band native scope (P0); broad operator purge/redrive/retention (queue-wide,
time-window, or policy-driven) remains a separate P1 operator contract. The two
MUST NOT be conflated.

Postgres-native deployments may implement every operation directly in Postgres.
S3/object-log deployments may buffer commands until a durable segment commit
boundary is reached, but commands must not be acknowledged before that durable
commit boundary. All storage modes must preserve the same client semantics once
a response is returned.

The client claim contract is backend- and shard-count-agnostic. A multi-shard
backend routes group/cohort claims to the single shard owning the group (group
co-residency), fans out non-group claims and returns a deterministic merged
order, and computes one queue-global progress bound across shards. A fan-out
claim is a composition of independent per-shard atomic claims anchored by a
queue-scope claim-intent keyed on `request_id`: it MAY return a committed partial
set when some shards are unavailable, retries of the same `request_id` converge
to one stable lease set, and `request-expired` is evaluated over the union of
leases across all shards. None of this changes the normative claim semantics
already in this contract (atomic per-lease creation, claim idempotency,
`max_items` as an upper bound, deterministic ordering). The mechanics are
specified in TD-001 (multi-shard claim and cross-shard progress), TD-003 (shard
ownership/fencing/progress), and TD-004 (object-log backend). Shard placement,
rebalance, drain, and backend migration administrative surfaces are
operator-contract concerns per the note above.

## Validation Checklist

- [x] Normative fields and rules are explicit.
- [x] Compatibility and precedence rules are explicit.
- [x] Error handling is explicit.
- [x] At least one executable test can be derived from this contract.
- [x] Non-normative notes cannot be mistaken for contract requirements.
