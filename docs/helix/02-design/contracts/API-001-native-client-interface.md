---
ddx:
  id: api-native-client-interface
  depends_on:
    - prd
    - concerns
    - adr-cqrs-log-projection-storage-model
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
| `queue_id` | string | yes | MUST be stable within `tenant_id`; MUST be used for routing, storage partitioning, and metrics. | Client-visible queue namespace. |
| `request_id` | string | yes for mutating operations | MUST be stable across retries of the same logical request; MUST be unique for different logical requests; MUST be returned in responses. | Envelope idempotency key and trace correlation ID. |
| `client_item_key` | string | yes for push | MUST identify the caller's logical item within a queue; MUST remain a durable secondary key for non-terminal lookup and for terminal lookup until item retention expires. | Duplicate pushes converge by this key. |
| `item_id` | string | response / update / finalize | MUST be server-assigned and stable for the accepted queue item. | Used after first accept. |
| `item_version` | integer | response / conditional update | MUST monotonically increase for each committed mutation of an item. | Used for optional optimistic concurrency. |
| `lease_token` | string | claim / renew / finalize | MUST be unguessable; MUST authorize lease renewal and finalization for one active lease. | Stale tokens fail per item. |
| `priority` | tagged scalar | yes when item should be orderable | MUST match the queue's declared priority model. | Timestamp queues use RFC 3339 UTC timestamps. |
| `not_before` | timestamp | no | If present, item MUST NOT be claimable before this timestamp. | Distinct from priority. |
| `payload` | opaque bytes or JSON value | no | MUST be stored and returned to claimers without pqueue interpreting application meaning. | Transport adapters define encoding. |
| `metadata` | JSON object / map | no | MUST be caller-defined and queryable only through supported predicates. | Used for gates, group keys, and observability dimensions. |
| `group_key` | string | no | MAY identify claim compatibility groups. | Examples: account, connector, campaign, domain. |
| `lifecycle_state` | enum | response | MUST be one of `pending`, `leased`, `complete`, `failed`. | Retry is represented as pending with retry metadata and `not_before`. |
| `item_result.status` | enum | response | MUST be one of `accepted`, `updated`, `duplicate`, `claimed`, `renewed`, `completed`, `failed`, `retried`, `released`, `not_found`, `invalid`, `conflict`, `stale_lease`, `terminal`, `rate_limited`, `unavailable`. | Per-item outcome. |

### Tenant and Authorization Rules

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| HTTP principal | authenticated identity | yes for service mode | MUST be resolved before authorizing any route. The provider is intentionally outside this contract. | Examples: machine token, service account, user session. |
| Tenant binding | authorization rule | yes for service mode | `tenant_id` from the route MUST be authorized for the HTTP principal. Servers MAY infer a default tenant only when that inference is unambiguous and authorized. | Prevents route-level tenant spoofing. |
| Embedded tenant | configuration | yes for embedded mode | Embedded or local deployments MAY bind all operations to a configured default `tenant_id`. | Keeps local/library mode simple. |
| `worker_id` | observability identity | yes for claim | MUST NOT be treated as the authenticated principal. | Worker names are caller-supplied labels. |

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
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/items:claim` | HTTP operation | yes | MUST bind to `BatchClaim`. | Data-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/leases:renew` | HTTP operation | yes | MUST bind to `BatchRenewLeases`. | Data-plane route. |
| `POST /v1/tenants/{tenant_id}/queues/{queue_id}/items:finalize` | HTTP operation | yes | MUST bind to `BatchFinalize`. | Data-plane route. |
| `GET /v1/tenants/{tenant_id}/queues/{queue_id}/metrics` | HTTP operation | yes | MUST bind to `GetQueueMetrics`. | Observability route. |

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
| `progress_bound_ms` | integer | yes | MUST be greater than 0. | Eligible items cannot be ignored beyond this bound. |
| `eligibility_policy.metadata_blockers` | object | no | If present, keys map to arrays of blocked JSON scalar values. An item whose metadata key equals any blocked value MUST be ineligible. Nested object and array equality are not part of v1. | Generic support for paused, suppressed, disabled, or quota-blocked states. |
| `request_id_retention_ms` | integer | yes | MUST be greater than 0. | Bounds mutating request replay/deduplication. |
| `client_item_key_retention_ms` | integer | yes | MUST be greater than 0. | Bounds duplicate push convergence after terminal retention rules no longer keep the item addressable. |
| `max_lease_duration_ms` | integer | yes | MUST be greater than 0. | Caps claim and renew lease durations. |
| `retry_policy.max_attempts` | integer | yes | MUST be greater than 0. Retry beyond this count MUST make the item terminal failed. | Defines terminal retry exhaustion. |
| `max_push_batch_size` | integer | yes | MUST be greater than 0. | Server may enforce a lower deployment cap. |
| `max_claim_batch_size` | integer | yes | MUST be greater than 0. | Server may enforce a lower deployment cap. |
| `CreateQueue.response` | object | yes | MUST include the stored queue definition and `created` boolean. | `created=false` means compatible idempotent create. |

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
| `BatchUpdate.response.results[]` | array | yes | MUST preserve request update order. | Terminal items fail per item. |

Updates to leased items MUST fail with per-item `conflict`. Workers that need to
change leased work MUST use `BatchRenewLeases` or `BatchFinalize`. Operator
repair APIs may define stronger mutation rights in a separate contract.

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
| `BatchClaim.response.items[]` | array | yes | MUST return claimed items in deterministic result order for the queue's ordering mode. | Each item includes `lease_token`. |
| `claimed_item.lease_expires_at` | timestamp | yes | MUST indicate when item may become eligible if not renewed/finalized. | Server time. |

Group-aware claim selection MUST NOT permanently favor one group when the server
selects among groups. Compatibility predicates are conjunctive: `group_key`,
`metadata_equals`, and `same_group_key` all apply when provided. Combining
`same_group_key=true` with an explicit `group_key` is valid and means all
returned items MUST match the explicit group. Explicit caller filters restrict
the claim domain for that request; pqueue can preserve progress within requested
domains, but operators remain responsible for running workers that cover all
required domains.

### Batch Renew

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchRenewLeases` | operation | yes | MUST renew one or more active leases and return per-item results. | Best-effort per item. |
| `leases[].item_id` | string | yes | MUST identify the leased item. | |
| `leases[].lease_token` | string | yes | MUST match the active lease token. | Stale token fails per item. |
| `lease_duration_ms` | integer | yes | MUST be greater than 0 and no more than `max_lease_duration_ms` or a lower deployment cap. | Applies to all submitted leases. |

### Batch Finalize

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchFinalize` | operation | yes | MUST finalize or release one or more leased items and return per-item results. | Best-effort per item. |
| `finalizations[].item_id` | string | yes | MUST identify the leased item. | |
| `finalizations[].lease_token` | string | yes | MUST match the active lease token. | Stale token fails per item. |
| `finalizations[].outcome` | enum | yes | MUST be one of `complete`, `fail`, `retry`, `release`. | |
| `finalizations[].retry.not_before` | timestamp | required for `retry` | MUST set next eligibility time. | v1 has no implicit retry delay default. |
| `finalizations[].retry.priority` | tagged scalar | no | If present, MUST replace priority and match queue priority model. | |
| `finalizations[].failure_code` | string | no | SHOULD be present for `fail`. | Caller-defined. |
| `finalizations[].metadata` | JSON object / map | no | MAY store finalization or retry metadata. | Transport adapters define size limits. |

### Queue Metrics

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `GetQueueMetrics` | operation | yes | MUST return point-in-time queue metrics for one queue. | Observability operation. |
| `metrics.lifecycle_counts` | object | yes | MUST include `pending`, `leased`, `complete`, and `failed`. | May be approximate if documented. |
| `metrics.retry_backlog` | integer | yes | MUST count pending items with retry metadata that are not terminal. | May be approximate if documented. |
| `metrics.oldest_eligible_age_ms` | integer / null | yes | MUST be null if no eligible item exists. | |
| `metrics.progress_bound_risk_count` | integer | yes | MUST count or estimate eligible items whose eligible age is near `progress_bound_ms`. | v1 progress metric is eligible age. |
| `metrics.active_leases` | integer | yes | MUST count active leases. | |

### Versioning Rules

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `item_version` bump on first accept | mutation rule | yes | Successful first acceptance MUST set `item_version=1`. | |
| `item_version` bump on update | mutation rule | yes | Successful `BatchUpdate` MUST increment `item_version`. | |
| `item_version` bump on claim | mutation rule | yes | Successful `BatchClaim` MUST increment `item_version` for each claimed item. | Lease state changed. |
| `item_version` bump on renew | mutation rule | yes | Successful `BatchRenewLeases` MUST increment `item_version`. | Lease expiry changed. |
| `item_version` bump on finalize | mutation rule | yes | Successful `BatchFinalize` outcomes `complete`, `fail`, `retry`, and `release` MUST increment `item_version`. | Lifecycle, retry, or lease state changed. |
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
- Batch precedence: batch envelopes fail only for envelope-level problems such
  as authentication, authorization, missing queue, malformed request, or backend
  outage. Item-level validation, duplicates, stale leases, terminal-state
  conflicts, and not-found conditions MUST be reported per item.
- Atomicity: `CreateQueue` is atomic. `BatchClaim` MUST atomically create each
  returned lease. `BatchPush`, `BatchUpdate`, `BatchRenewLeases`, and
  `BatchFinalize` are best-effort with per-item outcomes unless a future
  contract adds an explicit all-or-nothing mode.
- Ordering: response result arrays for push, update, renew, and finalize MUST
  preserve request order. Claim responses MUST preserve the queue's deterministic
  claim result order.
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
| Queue/deployment rate limit exceeded | Envelope error or per-item `rate_limited` | yes | Back off according to retry guidance. |

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

## Non-Normative Notes

The native contract intentionally exposes batch operations first. Transport
adapters may offer convenience single-item methods, but those should be client
wrappers over batch operations because pqueue's cost, durability, and throughput
model depends on batching.

`CreateQueue` is included in the native API because queue definition controls
client-visible priority, eligibility, idempotency, and batch semantics. Broader
administrative operations such as shard placement, backend migration, repair,
redrive, purge, and retention management should be defined in separate operator
contracts.

Postgres-native deployments may implement every operation directly in Postgres.
S3/object-log deployments may buffer commands until a durable segment commit
boundary is reached, but commands must not be acknowledged before that durable
commit boundary. All storage modes must preserve the same client semantics once
a response is returned.

## Validation Checklist

- [x] Normative fields and rules are explicit.
- [x] Compatibility and precedence rules are explicit.
- [x] Error handling is explicit.
- [x] At least one executable test can be derived from this contract.
- [x] Non-normative notes cannot be mistaken for contract requirements.
