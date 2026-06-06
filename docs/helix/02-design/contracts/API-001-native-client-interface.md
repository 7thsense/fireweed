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
**Status**: draft
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
| `request_id` | string | yes for mutating batch operations | MUST be unique for a client retry attempt; MUST be returned in responses. | Used for request tracing, not item idempotency. |
| `client_item_key` | string | yes for push | MUST identify the caller's logical item within a queue for the configured idempotency retention window. | Duplicate pushes converge by this key. |
| `item_id` | string | response / update / finalize | MUST be server-assigned and stable for the accepted queue item. | Used after first accept. |
| `lease_token` | string | claim / renew / finalize | MUST be unguessable; MUST authorize lease renewal and finalization for one active lease. | Stale tokens fail per item. |
| `priority` | tagged scalar | yes when item should be orderable | MUST match the queue's declared priority model. | Timestamp queues use RFC 3339 UTC timestamps. |
| `not_before` | timestamp | no | If present, item MUST NOT be claimable before this timestamp. | Distinct from priority. |
| `payload` | opaque bytes or JSON value | no | MUST be stored and returned to claimers without pqueue interpreting application meaning. | Transport adapters define encoding. |
| `metadata` | JSON object / map | no | MUST be caller-defined and queryable only through supported predicates. | Used for gates, group keys, and observability dimensions. |
| `group_key` | string | no | MAY identify claim compatibility groups. | Examples: account, connector, campaign, domain. |
| `lifecycle_state` | enum | response | MUST be one of `pending`, `leased`, `complete`, `failed`. | Retry is represented as pending with retry metadata and `not_before`. |
| `item_result.status` | enum | response | MUST be one of `accepted`, `updated`, `duplicate`, `claimed`, `renewed`, `completed`, `failed`, `retried`, `released`, `not_found`, `invalid`, `conflict`, `stale_lease`, `terminal`, `rate_limited`, `unavailable`. | Per-item outcome. |

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
| `idempotency_retention_ms` | integer | yes | MUST be greater than 0. | Bounds duplicate push convergence. |
| `max_push_batch_size` | integer | yes | MUST be greater than 0. | Server may enforce a lower deployment cap. |
| `max_claim_batch_size` | integer | yes | MUST be greater than 0. | Server may enforce a lower deployment cap. |
| `CreateQueue.response` | object | yes | MUST include the stored queue definition and `created` boolean. | `created=false` means compatible idempotent create. |

### Batch Push

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchPush` | operation | yes | MUST accept one or more items and return one `item_result` per submitted item. | Best-effort per item. |
| `BatchPush.items[]` | array | yes | MUST NOT exceed queue or deployment max push batch size. | Empty batch is invalid. |
| `items[].client_item_key` | string | yes | MUST drive duplicate convergence within `idempotency_retention_ms`. | Required even when payload differs. |
| `items[].priority` | tagged scalar | yes | MUST match queue priority model. | Invalid values fail per item. |
| `items[].not_before` | timestamp | no | MUST make item ineligible until the timestamp. | `priority` still determines order once eligible. |
| `items[].payload` | opaque bytes or JSON value | no | MUST be stored as caller data. | May be omitted for pointer-only queues. |
| `items[].metadata` | JSON object / map | no | MUST be stored as caller metadata. | Size limits are deployment-defined. |
| `BatchPush.response.results[]` | array | yes | MUST preserve request item order. | Each result includes submitted `client_item_key`. |

### Batch Update

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchUpdate` | operation | yes | MUST update one or more non-terminal items and return one `item_result` per submitted update. | Best-effort per item. |
| `updates[].item_ref` | object | yes | MUST identify the target by `item_id` or `client_item_key`. | If both are present they MUST refer to the same item. |
| `updates[].priority` | tagged scalar | no | If present, MUST replace the current priority and match queue priority model. | Allows ingest before final schedule. |
| `updates[].not_before` | timestamp / null | no | If present, MUST replace or clear not-before eligibility. | `null` clears. |
| `updates[].payload` | opaque bytes or JSON value | no | If present, MUST replace payload. | Patch semantics are not v1. |
| `updates[].metadata` | JSON object / map | no | If present, MUST replace metadata. | Patch semantics are not v1. |
| `BatchUpdate.response.results[]` | array | yes | MUST preserve request update order. | Terminal items fail per item. |

### Batch Claim

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchClaim` | operation | yes | MUST atomically lease up to `max_items` eligible items. | Empty success is allowed. |
| `worker_id` | string | yes | MUST identify the claiming worker or consumer group member for observability. | Not an auth principal. |
| `max_items` | integer | yes | MUST be greater than 0 and no more than queue/deployment max claim batch size. | Upper bound, not guarantee. |
| `lease_duration_ms` | integer | yes | MUST be greater than 0 and no more than queue/deployment max lease duration. | Creates invisibility window. |
| `compatibility` | object | no | MAY restrict claim to compatible items by `group_key` or supported metadata predicate. | Must not violate progress bound. |
| `BatchClaim.response.items[]` | array | yes | MUST return claimed items in deterministic result order for the queue's ordering mode. | Each item includes `lease_token`. |
| `claimed_item.lease_expires_at` | timestamp | yes | MUST indicate when item may become eligible if not renewed/finalized. | Server time. |

### Batch Renew

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchRenewLeases` | operation | yes | MUST renew one or more active leases and return per-item results. | Best-effort per item. |
| `leases[].item_id` | string | yes | MUST identify the leased item. | |
| `leases[].lease_token` | string | yes | MUST match the active lease token. | Stale token fails per item. |
| `lease_duration_ms` | integer | yes | MUST be greater than 0 and no more than queue/deployment max lease duration. | Applies to all submitted leases. |

### Batch Finalize

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BatchFinalize` | operation | yes | MUST finalize or release one or more leased items and return per-item results. | Best-effort per item. |
| `finalizations[].item_id` | string | yes | MUST identify the leased item. | |
| `finalizations[].lease_token` | string | yes | MUST match the active lease token. | Stale token fails per item. |
| `finalizations[].outcome` | enum | yes | MUST be one of `complete`, `fail`, `retry`, `release`. | |
| `finalizations[].retry.not_before` | timestamp | required for `retry` unless queue policy supplies default | MUST set next eligibility time. | |
| `finalizations[].retry.priority` | tagged scalar | no | If present, MUST replace priority and match queue priority model. | |
| `finalizations[].failure_code` | string | no | SHOULD be present for `fail`. | Caller-defined. |
| `finalizations[].metadata` | JSON object / map | no | MAY store finalization or retry metadata. | Transport adapters define size limits. |

### Queue Metrics

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `GetQueueMetrics` | operation | yes | MUST return point-in-time queue metrics for one queue. | Observability operation. |
| `metrics.lifecycle_counts` | object | yes | MUST include `pending`, `leased`, `complete`, and `failed`. | May be approximate if documented. |
| `metrics.oldest_eligible_age_ms` | integer / null | yes | MUST be null if no eligible item exists. | |
| `metrics.progress_bound_risk_count` | integer | yes | MUST count or estimate eligible items near progress-bound violation. | |
| `metrics.active_leases` | integer | yes | MUST count active leases. | |

## Precedence and Compatibility

- Versioning: breaking changes require a new major contract version.
- Transport compatibility: transport-specific APIs MAY add headers, pagination,
  compression, streaming, or authentication envelopes, but MUST preserve this
  operation model and response semantics.
- Exposure precedence: the native operation model is authoritative. Rust,
  HTTP, SDK, and compatibility surfaces are bindings over the native model.
  When a compatibility adapter cannot represent a native behavior, the adapter
  MUST document the limitation rather than weakening the native contract.
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
| Malformed envelope or unsupported priority type | Envelope error `invalid-request` | yes after fix | Correct request shape. |
| Batch exceeds configured maximum | Envelope error `batch-too-large` | yes after fix | Split the batch. |
| Backend cannot durably commit before timeout | Envelope error `commit-timeout` or per-item `unavailable` | yes with same idempotency keys | Retry after backoff; duplicate accepted items converge. |
| Duplicate push within retention window | Per-item `duplicate` | no | Treat as successful convergence. |
| Item priority does not match queue model | Per-item `invalid` | yes after fix | Submit a valid priority. |
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
      "status": "accepted"
    }
  ]
}
```

```json
{
  "operation": "BatchClaim",
  "tenant_id": "tenant_acme",
  "queue_id": "scheduled_actions",
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
  "items": [
    {
      "item_id": "itm_01JX2A7Y6VMT5DRF7YZ1DN7G6W",
      "client_item_key": "action:123",
      "priority": { "timestamp": "2026-06-06T14:30:00Z" },
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
