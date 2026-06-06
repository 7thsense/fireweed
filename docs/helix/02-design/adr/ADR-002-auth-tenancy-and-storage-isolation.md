---
ddx:
  id: adr-auth-tenancy-and-storage-isolation
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
    - td-storage-architecture-backend-contracts
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
4. **Shard**: physical routing and capacity unit named by
   `tenant_id/queue_id/shard_id`.

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
| `queue:read` | queue definition and metrics reads |
| `item:push` | `BatchPush` |
| `item:update` | `BatchUpdate` |
| `item:claim` | `BatchClaim` |
| `lease:renew` | `BatchRenewLeases` |
| `item:finalize` | `BatchFinalize` |
| `operator:repair` | future repair/redrive/purge APIs |
| `admin:shard` | future shard placement and migration APIs |

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
clusters, object buckets, or log partitions through backend profile and shard
placement metadata. That placement is not visible in the native queue API.

## Noisy-Neighbor Controls

Noisy-neighbor isolation is both a capacity and correctness concern. The design
requires:

- queue and tenant identifiers in metrics;
- configurable max batch size and lease duration per queue;
- backend profile and shard count per queue;
- rate-limit outcomes in API-001 error semantics;
- progress-bound metrics per queue;
- load tests where one hot queue does not prevent another queue from claiming
  eligible work within its configured limits.

Queue-level rate limits, quotas, and tenant capacity controls are P1 product
features, but v1 storage and metrics must not make them impossible.

## Security Rules

- Resolve the principal before route handling reaches pqueue core.
- Authorize before loading queue data or returning whether an unauthorized queue
  exists.
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
