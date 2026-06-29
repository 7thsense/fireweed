---
ddx:
  id: adr-auth-tenancy-and-storage-isolation
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
    - adr-queue-as-shard-unit-and-projection-families
  review:
    self_hash: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
    deps:
      adr-queue-as-shard-unit-and-projection-families: 77d1e2feb6a27e0a093564e3f07247cd8cc2c6fba6c3d20b5eeade568ba25964
      api-native-client-interface: a97e014a176aa9e37a93fbab151c31ffb47aa8428c62e802c98fa3be0413426b
      concerns: 7e3b81e376f75f71691f55ac1ca4d9599eddcfe6eefe70f614c366c132e07992
      prd: a910dd5fb95102767b4ddf81115569d39d85c7e082a40c62ce424dea73ca8533
    reviewed_at: "2026-06-25T04:21:18Z"
---

# ADR-002: Auth, Tenancy, and Storage Isolation

## Context

pqueue is intended to work as an embedded Rust library and as a stateless
service. The service form must not rely only on an outer application auth gate:
tenant boundaries need to reach API authorization, control-plane routing, and
storage predicates so noisy or unauthorized tenants cannot read, mutate, or
starve each other.

The core product should remain generally usable and open-source friendly. It
must not bake in a human signup flow, a specific identity provider, or
Seventh Sense-specific account concepts. API-001 already requires `tenant_id`
for service mode and defines principal-to-tenant authorization as a service
responsibility. TD-001 and TD-002 require `tenant_id` in storage keys.

## Decision Drivers

- Preserve provider-neutral auth for open-source and embedded use.
- Enforce tenant scope before every data-returning or state-changing operation.
- Carry tenant identity into storage keys and indexes.
- Support queue namespaces that are distinct from tenant/account identity.
- Keep machine-to-machine service deployments first-class.
- Avoid weakening native API semantics for compatibility adapters.
- Make noisy-neighbor isolation testable at queue and tenant boundaries.

## Decision

pqueue will use a layered auth and tenancy model:

1. **Principal**: authenticated actor resolved by the host service or embedding
   application. Examples: service account, machine token, user session, or local
   embedded principal.
2. **Tenant**: storage and authorization boundary named by `tenant_id`.
3. **Queue namespace**: client-visible queue identifier named by `queue_id`,
   unique within a tenant.
4. **Queue ownership**: the queue is the unit of sharding (ADR-008) — a whole
   queue is owned by exactly one node at a time, placed by a deterministic
   function of `(tenant_id, queue_id)`. An optional internal item-table partition
   (`hash(tenant_id, queue_id) % N`, TD-002) is a client-invisible storage detail,
   never an ownership/routing/authorization unit.
5. **Group**: `group_key` is a client-visible logical ordering/compatibility
   partition within a queue (ADR-004). Claim result order is exact per-group
   order on any queue, because every item of a `group_key` is co-resident on the
   queue's single owner by construction (ADR-008). `group_key` carries no
   progress-bound meaning (progress is queue-global, computed locally on the
   owner).

The core queue engine requires an already-resolved `PrincipalContext` for
service-mode operations. It does not implement login, signup, session storage,
or external IdP integration. Provider-specific authentication is a host-service
adapter concern.

Authorization is deny-by-default. A principal must be authorized for the tenant,
queue, and operation before the operation reads control-plane, log, projection,
or snapshot state.

## Permission Model

The first service implementation uses operation-scoped permissions:

| Permission | Applies To |
|------------|------------|
| `queue:create` | `CreateQueue` |
| `queue:read` | queue definition and metrics reads; `DiscoverActiveScopes` |
| `item:push` | `BatchPush` |
| `item:update` | `BatchUpdate` |
| `item:claim` | `BatchClaim` |
| `lease:renew` | `BatchRenewLeases` |
| `item:finalize` | `BatchFinalize` |
| `item:update` (native purge) | native per-key `PurgeItems` (API-001, recurring teardown) |
| `operator:inspect` | API-002 operator reads (`GetItem`, `ListItems`, `GetQueueAdminState`, `GetOperation`, `ListOperations`) |
| `operator:repair` | API-002 repair/redrive/archive/retention/pause/resume |
| `operator:purge` | API-002 bulk operator `PurgeQueueItems` (most destructive; may require a distinct grant) |
| `admin:queue` | queue placement / ownership handoff and backend migration (migration design) |

The operator surface (`operator:inspect`/`operator:repair`/`operator:purge`) is
defined by API-002. Operator mutations may act on leased and terminal items and
therefore require these stronger, deny-by-default grants distinct from the
API-001 data-plane permissions.

The policy engine may be RBAC, ABAC, or host-provided callback. The pqueue
service surface sees only a policy decision:

```rust
pub struct PrincipalContext {
    pub principal_id: String,
    pub tenant_scopes: Vec<TenantScope>,
    pub authn_method: AuthnMethod,
}

pub trait Authorizer {
    async fn authorize(
        &self,
        principal: &PrincipalContext,
        tenant_id: &TenantId,
        queue_id: Option<&QueueId>,
        permission: Permission,
    ) -> Result<(), AuthorizationError>;
}
```

Embedded/local deployments may use a fixed local principal and fixed default
tenant. That shortcut is not valid for multi-tenant service deployments.

## Storage Isolation

Every storage backend must include `tenant_id` in durable records, projection
records, idempotency records, metrics, and snapshots. Backend implementations
must make cross-tenant access explicit enough to test.

The first Postgres-native implementation uses shared tables with leading
`tenant_id, queue_id` keys and mandatory tenant predicates. Stronger isolation
is allowed by backend profile or deployment class:

| Isolation Level | Use |
|-----------------|-----|
| Shared tables with tenant predicates | Default open-source and small deployments. |
| Shared database with tenant partitions/schemas | Hot tenants or stricter operational isolation. |
| Dedicated database or cluster per tenant class | Large, regulated, or noisy tenants. |

The control plane may assign queues or tenants to different Postgres databases,
clusters, object buckets, or log partitions through backend profile and
queue-owner assignment metadata. That placement is not visible in the native
queue API.

## Noisy-Neighbor Controls

Noisy-neighbor isolation is both a capacity and correctness concern. The design
requires:

- queue and tenant identifiers in metrics;
- configurable max batch size and lease duration per queue;
- backend profile per queue;
- pqueue deployment/tenant rate-limit and capacity outcomes in API-001 error
  semantics (the envelope rate-limit error and the per-item `rate_limited`
  partial-batch status protect the pqueue deployment, not a caller's downstream
  API);
- progress-bound metrics per queue;
- load tests where one hot queue does not prevent another queue from claiming
  eligible work within its configured limits;
- queue density: a single node MUST support at least 1000 concurrently active
  queues without cross-queue degradation. This makes noisy-neighbor isolation a
  density concern as well as a capacity one: per-queue background work
  (lease-expiry sweeps, progress-bound aggregation, summary recompute, recurring
  rearm, idempotency/retention GC) MUST be multiplexed onto bounded shared
  per-node resources (worker pools, connection pools, sweeper batches), never one
  task, loop, or connection per queue, so that
  the 1000th active queue costs no more than bounded incremental resource and
  every active queue still meets its progress bound.

pqueue deployment-level rate limits, quotas, and tenant capacity controls are P1
product features, but v1 storage and metrics must not make them impossible.
Enforcing a caller's downstream API rate limits or quotas is a permanent
non-goal of the pqueue engine; callers pace their own claim output (claim batch
size, claim cadence, `not_before`, and group selection) and pqueue performs no
downstream-rate admission.

## Security Rules

- Resolve the principal before route handling reaches pqueue core.
- Authorize before loading queue data or returning whether an unauthorized queue
  exists.
- Tenant-wide reads (discovery) MUST authorize `queue:read` per candidate queue
  and MUST exclude unauthorized queues from results without leaking their
  existence; a request naming an unauthorized queue MUST return
  forbidden/not-found. Enumeration and per-queue auth fanout MUST be bounded by
  pagination or a documented per-tenant queue ceiling. The
  `Authorizer::authorize(..., queue_id: Option<&QueueId>, ...)` signature already
  supports both tenant-wide (`None`) and per-queue cases.
- Never trust `worker_id` as an authenticated principal.
- Store lease tokens only as hashes.
- Treat payload and metadata as caller data; validate queue-owned fields and
  size limits.
- Use parameterized queries and typed storage APIs.
- Emit audit/trace identifiers for mutating operations without leaking payloads
  into logs by default.

## Consequences

Positive:

- Core queue semantics remain independent of auth providers.
- Tenant isolation is testable at the API and storage layer.
- Embedded/library mode remains simple.
- Service deployments can use machine tokens, sessions, or external IdPs.

Negative:

- The service layer must thread `PrincipalContext` through every route.
- Shared-table isolation depends on strong query discipline and negative tests.
- Dedicated tenant placement remains a deployment/control-plane concern rather
  than a native API feature.

## Required Tests

- Tenant A cannot create, read, push, update, claim, renew, finalize, or view
  metrics for tenant B.
- Unauthorized queue IDs return forbidden/not-found without leaking data.
- `worker_id` cannot authorize lease renewal or finalization without the
  principal's permission.
- Storage queries include tenant scope for control plane, log, projection,
  idempotency, and metrics.
- A hot tenant/queue load test does not break another queue's progress-bound
  behavior under the same deployment profile.

## Status

Accepted as the initial auth and tenancy boundary. Provider-specific
authentication remains a host-service adapter decision.
