---
ddx:
  id: guide-scheduler-router-boundary
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: api-native-client-interface}
---

# Scheduler and Router Boundary

pqueue owns durable queue state and queue-local leasing. Routers and schedulers
may decide where a worker should ask next and how much downstream work the
worker is allowed to start, but they must treat pqueue claims as the only
authoritative reservation.

This guide applies the [PRD](../prd.md) and
[API-001](../../02-design/contracts/API-001-native-client-interface.md)
boundary: pqueue exposes queue-native pacing controls such as `max_items`,
`not_before`, group filters, gates, and finalize retry. It does not issue or
spend downstream API tokens.

## Responsibility Matrix

| Responsibility | pqueue | Router | Scheduler | Worker | Downstream | Stateless helper library |
|---|---|---|---|---|---|---|
| Queue definition and immutable queue configuration | Owns `CreateQueue`, queue identity, priority model, ordering mode, eligibility policy, retry policy, lease limits, idempotency retention, and queue capacity limits. | Chooses an existing queue target; never creates queues implicitly. | Supplies caller-owned configuration inputs when the application wants to create queues. | Calls explicit create or ensure flows chosen by the application. | No role. | May build validated request structs from caller data. |
| Eligibility, priority, and progress | Computes eligible work from priority, `not_before`, gates, group filters, leases, and queue progress rules. | May use advisory discovery to pick a queue or group. | May decide when to poll or withhold polling. | Sends claim filters and respects empty or partial claims. | No role. | May rank or filter advisory descriptors without persisting state. |
| Leases and reservation | Authoritatively leases items through `BatchClaim`; a returned lease is the reservation. | Must not treat discovery as a reservation. | Must acquire downstream capacity before asking a worker to claim. | Claims only after capacity is available and finalizes every lease it receives. | No role. | May translate claimed items into worker input values. |
| Lifecycle and finalize outcomes | Owns complete, fail, retry, release, rearm, renewal, lease expiry, idempotent mutation replay, and stale-lease rejection. | No role beyond routing later attempts. | May choose downstream backoff values for retry requests. | Maps application results to finalize outcomes and retries finalization failures until resolved. | Returns application success, failure, or retry advice. | May provide pure mapping helpers for outcomes. |
| Durable and idempotent mutations | Owns durable push, update, claim, finalize, renew, purge, metrics, and request-id replay semantics. | No durable mutation state. | No pqueue mutation state. | Provides stable `request_id` and `client_item_key` values where required. | No role. | May derive stable identifiers from application inputs. |
| Advisory selection and fan-in | Exposes `DiscoverActiveScopes` when the surface supports it, and keeps it read-only and stale-tolerant. | Owns cross-queue or cross-group polling order, fan-in bounds, and caller-side dispersion. | May feed worker demand into routing decisions. | Treats router output as a hint, then uses ordinary claim. | No role. | The pqueue library optionally supplies B-006 `claim_across_queues` bounded fan-in and B-007 `select_active_scope_from_prefix` dispersion; both are stateless caller helpers, not scheduler traits, state, or persistence. |
| Downstream/callee rate, quota, concurrency, protection, and compute admission | Does not enforce downstream API limits or worker-runtime placement. | May route to the queue/group that best matches available downstream capacity. | Owns downstream tokens, quotas, concurrency budgets, circuit breakers, and compute admission. | Acquires capacity before claim and never claims more items than it can start or safely finalize. | Defines accepted rate, quotas, concurrency, and failure modes. | May compute request sizes from granted capacity. |
| pqueue deployment and tenant capacity controls | Owns deployment-level rate limits, noisy-tenant quotas, and tenant capacity controls as PRD P1/API-001 `rate_limited`; this is currently unbuilt. | Surfaces pqueue `rate_limited` as pqueue capacity pressure, not downstream pressure. | Does not replace pqueue capacity controls. | Retries or backs off according to pqueue errors. | No role. | May classify pqueue errors for callers. |

Optional helper libraries must stay stateless unless they are explicitly part of
the caller's router or scheduler. They may build requests, rank advisory
descriptors, or map downstream outcomes to pqueue finalization requests. They
must not expose scheduler traits from pqueue, create token buckets in pqueue, or
persist routing state inside pqueue.

## Reference Loop

1. The router selects an existing queue or group from configuration, metrics, or
   advisory discovery. Discovery is stale: it says where work appeared at one
   projection frontier, not where a lease is reserved now.
2. The scheduler obtains downstream capacity for the specific callee, tenant,
   account, connector, campaign, or compute pool before any claim.
3. The worker sets `max_items` to the granted capacity, also respecting the
   queue's `max_claim_batch_size` and any application batch ceiling.
4. The worker calls pqueue claim. pqueue may return fewer than `max_items`, or
   zero items, because eligibility, ordering, leases, gates, group filters, and
   contention remain authoritative.
5. The worker attempts downstream work only for returned leases.
6. The worker finalizes every returned lease with complete, fail, retry, or
   release. If finalization is interrupted, the worker retries with the same
   durable request semantics where available, or lets lease expiry recover the
   work according to the normal pqueue contract.

`not_before` and finalize retry are queue eligibility mechanisms. Use them to
defer an item in pqueue after the caller has decided a later attempt is needed.
Do not treat them as downstream tokens. Gates and group filters are pqueue
selection predicates, not callee quota counters. A downstream token, quota slot,
or concurrency permit belongs to the scheduler/worker side of the boundary.

## Prohibited Coupling

- Do not access pqueue backends directly from a scheduler or router. Use the
  public Rust facade or a committed wire surface.
- Do not rely on implicit queue creation from push, claim, discovery, routing,
  or scheduler loops. Queue creation is an explicit control-plane action.
- Do not assume cross-queue atomicity. Multi-queue routing and fan-in are
  caller-side orchestration over independent queue-local operations.
- Do not interpret pqueue `rate_limited` as a downstream API rate result.
  API-001 reserves it for pqueue deployment or tenant capacity pressure.
- Do not add scheduler traits, downstream token buckets, quota registries, or
  persisted routing state to pqueue core.
