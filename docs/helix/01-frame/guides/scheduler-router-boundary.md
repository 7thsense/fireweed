---
ddx:
  id: guide-scheduler-router-boundary
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: api-fireweed-kernel-contract}
---

# Scheduler and Router Boundary

fireweed owns durable queue state and queue-local leasing. Routers and
schedulers may decide which queue a worker should claim from next and how much
downstream work the worker may start, but a fireweed claim is the only
authoritative reservation.

This guide applies the [PRD](../prd.md) and
[API-006](../../02-design/contracts/API-006-fireweed-kernel-contract.md)
boundary. fireweed exposes queue-native pacing controls: `max_records`,
`WholeGroups { max_groups }`, a future `rank`, and gates. It does not issue or
spend downstream API tokens.

## Responsibility Matrix

| Responsibility | fireweed | Router | Scheduler | Worker | Downstream |
|---|---|---|---|---|---|
| Queue definition | Owns `define_queue` (create-or-verify of stages, fields and indexes), queue identity, queue metadata, and `delete_queue`. The definition is immutable. | Chooses an existing queue; never creates queues implicitly. | Supplies caller-owned configuration when the application creates queues. | Calls the explicit create-or-verify flow the application chooses. | No role. |
| Claimability and order | Decides candidates from stage, lease state, rank against the horizon, and gates, and returns them in rank order (API-006 C1, C4). | Chooses which queue and stage to claim next, from configuration or `stats`. | Decides when to poll or withhold polling. | Names the stage and selector, and accepts short or empty claims. | No role. |
| Leases and reservation | Leases records through `claim`; the returned token is the reservation. | Must not treat `stats` as a reservation. | Acquires downstream capacity before asking a worker to claim. | Claims only after capacity is available, and settles every record it receives. | No role. |
| Settling | Applies settling writes (stage move, new rank, release, delete), claim extension, lease reclaim, request-id replay, and `StaleLease` rejection. | No role beyond routing later attempts. | May choose backoff values for retries, as a future rank. | Maps application results to settling writes, and resolves interrupted writes by request id or `read`. | Returns success, failure, or retry advice. |
| Durable, idempotent mutations | Owns durable writes, claims, and request-id replay. | No durable state. | No fireweed state. | Provides stable record keys and `request_id` values. | No role. |
| Routing across queues | Has no discovery and no cross-queue claim. `stats` reports a queue's stage counts, ready groups, and oldest ready time. | Owns cross-queue polling order, fan-in bounds, and dispersion. | May feed worker demand into routing. | Treats router output as a hint, then claims normally. | No role. |
| Downstream rate, quota, concurrency, protection, and compute admission | Does not enforce downstream API limits or worker-runtime placement. | May route to the queue that best matches available downstream capacity. | Owns downstream tokens, quotas, concurrency budgets, circuit breakers, and compute admission. | Acquires capacity before claiming, and never claims more records than it can start or safely settle. | Defines accepted rate, quotas, concurrency, and failure modes. |
| fireweed deployment and tenant capacity | Owns deployment-level rate limits, noisy-tenant quotas, and tenant capacity controls (PRD P1-2), and reports its own pressure as `Busy { retry_after }`. | Treats `Busy` as fireweed capacity pressure, not downstream pressure. | Does not replace fireweed capacity controls. | Backs off on `Busy`. | No role. |

Helper code that builds requests, ranks queues, or maps downstream outcomes to
settling writes stays in the caller's router or scheduler. fireweed ships no
scheduler traits, token buckets, or routing state.

## Reference Loop

1. The router selects a queue and stage from configuration or `stats`. `stats`
   is a count at one moment, not a reservation.
2. The scheduler obtains downstream capacity for the specific callee, tenant,
   account, connector, campaign, or compute pool before any claim.
3. The worker sets `max_records`, and `max_groups` for a `WholeGroups` claim, to
   the granted capacity, within the contract limit of 1000 records and any
   application batch ceiling.
4. The worker calls `claim`. fireweed may return fewer records than asked, or
   none, because stage, rank, horizon, gates, leases, and contention decide; a
   short or empty claim is a successful poll.
5. The worker attempts downstream work only for the records it claimed.
6. The worker settles every claimed record with a write that carries the
   token: a stage move, a new rank, or a delete. If a settling write is
   interrupted, the worker resends it with the same `request_id`, or reads the
   records to learn the outcome. If the worker is gone, lease expiry returns
   the records.

A future rank and a stage move are queue mechanisms. Use them to defer a record
in fireweed after the caller has decided a later attempt is needed. They are
not downstream tokens. Gates and groups are fireweed selection mechanisms, not
callee quota counters. A downstream token, quota slot, or concurrency permit
belongs to the scheduler and worker side of the boundary.

## Prohibited Coupling

- Do not reach fireweed storage (the object log or the Turso files) from a
  scheduler or router. Use `Store` through the public Rust facade, or a later
  transport over it.
- Do not rely on implicit queue creation. Every op on an undefined queue fails
  with `QueueNotFound`; queue creation is an explicit `define_queue`.
- Do not assume cross-queue atomicity. Multi-queue routing and fan-in are
  caller-side orchestration over independent queue-local operations (API-006
  A4).
- Do not read fireweed `Busy` as a downstream rate result. It reports
  fireweed's own capacity pressure.
- Do not use `Operator::force_clear` or the admin handle from a scheduler,
  router, or worker. It is for on-call repair only (API-006 OP4).
- Do not add scheduler traits, downstream token buckets, quota registries, or
  persisted routing state to fireweed core.
