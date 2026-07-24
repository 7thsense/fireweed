---
ddx:
  id: adr-facade-lifecycle-and-routing-orchestration
  depends_on:
    - adr-queue-template-and-exact-ensure
    - api-native-client-interface
    - adr-granularity-mapping-and-claim-domain
    - adr-queue-as-shard-unit-and-projection-families
    - build-foqs-inspired-interface-and-boundary
  links:
    - {kind: informed_by, to: adr-queue-template-and-exact-ensure}
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: adr-granularity-mapping-and-claim-domain}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: governs, to: build-foqs-inspired-interface-and-boundary}
  status: accepted
---

# ADR-019: Lifecycle and routing orchestration are additive Rust-facade helpers

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-07-24 | Accepted | Project owner | API-001, ADR-004, ADR-008, ADR-018, B-005, B-006, B-007, B-011 | High |

## Context

API-001 already defines the transport-neutral, batch-native operations and their guarantees. In particular,
`BatchClaim` is atomic only on one queue owner, `BatchFinalize` owns lifecycle transitions, and
`DiscoverActiveScopes` returns an exact oldest-eligible-first advisory ranking as of an observed projection
frontier. ADR-004 fixes the effective claim domain; ADR-008 makes one queue the shard and ownership unit.

Rust-library callers need narrower conveniences: worker-loop names, one call that fans out independent
claims across queues, an exact stamp around queue-local discovery, and a deterministic way to disperse
workers within an oldest-first prefix. These conveniences must not imply a cross-queue transaction, change
backend ordering, conceal missing discovery descriptors, or add scheduler state to pqueue.

This ADR governs B-005 through B-007 and the discovery contract required from B-011. It is an additive
Rust-facade contract only. **API-001 remains unchanged**: no operation, field, error, atomicity rule, or
transport obligation is added to its batch-native surface.

## Decision

### 1. Lifecycle vocabulary is batch-shaped aliasing

The Rust facade adds `complete`, `retry`, `release`, and `discover` as thin aliases over its existing
`ack`, `nack`, and `discover_active_scopes` paths. Existing names remain public and are not deprecated.

| Alias | Existing path | Required parity |
|-------|---------------|-----------------|
| `complete` | `ack` / complete finalization | Same batch input, lifecycle transition, result, idempotency, and structured error. |
| `retry` | `nack` / retry finalization | Same relative or absolute retry timing, priority behavior, retry exhaustion, result, and structured error. |
| `release` | `nack` / release finalization | Same batch input, immediate pending transition, result, and structured error. |
| `discover` | `discover_active_scopes` | Same request semantics and descriptor order; no mutation or reservation. |

The aliases remain batch-shaped even when a helper accepts an iterator. They do not introduce scalar
engine operations. For lifecycle aliases, one facade call retains the existing all-or-nothing batch-call
failure behavior: a facade/backend error fails the call rather than returning a new alias-specific partial
result shape. API-001's native per-item batch outcomes remain authoritative where exposed by its transport
surface; these names do not redefine them.

### 2. Exact discovery is preserved and stamped

The existing Vec-returning Rust method remains unchanged. B-011 adds a second read-only accessor returning:

```text
ActiveScopeDiscovery {
    queue: QueueKey,
    granularity: DiscoveryGranularity,
    scopes: Vec<ActiveScope>,
}
```

The stamp records the exact queue and granularity used for the call. `scopes` is byte-for-byte/value-for-
value equivalent and in the identical order produced by the existing method. The accessor neither filters
nor re-ranks results. On facade/backend combinations without relational active-scope discovery, both paths
return the existing `EngineError::Unavailable`; this ADR does not require a synthetic or log-scanning
fallback.

API-001's ordering remains authoritative: group and queue discovery are ordered by
`oldest_eligible_age_ms` descending, exact as of the reported observed projection frontier. A helper may
consume that ordering but may not change discovery, backend claim ordering, eligibility, reservation, or
the queue-global progress contract.

### 3. B-011 makes relational discovery truthful

The current relational omission of eligible ungrouped work is a defect, not selector policy. B-011 must
repair the discovery source before B-007 depends on it:

- At group granularity, eligible items with no `group_key` produce one `group_key=None` descriptor. It is
  the ungrouped-items scope, never a queue rollup and never a sentinel key.
- Keyed and ungrouped descriptor ages are derived from the same live relational item source and the same
  API-001 Eligibility Precedence predicate, including current gates and time eligibility.
- A `not_before` crossing becomes visible without a later mutation or discovery write. Discovery remains
  read-only.
- Relational implementations may combine or replace stored summary reads with a bounded indexed query of
  live item rows. The implementation must document the supporting index and query plan/cost delta from a
  summary-only lookup; it must not preserve a stale summary optimization by returning false absence.
- Equal `oldest_eligible_age_ms` values use one deterministic secondary order. Ungrouped (`None`) ties sort
  before keyed (`Some`) groups; keyed values then use their stable key order. This tie-break refines
  deterministic representation only; oldest-first age remains the primary rank.

B-011 proves ungrouped-only, grouped/ungrouped mixed, time-only crossing, stale keyed summary mixed with
live ungrouped work, no eligible work, and equal-age `None`-before-`Some` behavior in SQLite-relational and
PostgreSQL-relational projections. It also proves that the stamped accessor preserves queue, granularity,
and source order through the public relational SQLite facade.

### 4. Multi-queue claim is bounded, non-atomic orchestration

The Rust facade adds:

```text
MultiQueueClaimTarget { queue: QueueKey, claim: ClaimAt }
MultiQueueClaimLimits { max_targets, max_total_items }
claim_across_queues(targets, limits) -> ordered per-target EngineResult<Claimed>
```

The result contains exactly one correlated entry per input target, in input order. Each entry identifies
its target queue and carries that queue's `EngineResult<Claimed>`. The helper is named
`claim_across_queues`, not `commit_multi_claim`: `commit_multi_claim` is a single-queue atomic operation and
must not be confused with this fan-out.

`MultiQueueClaimLimits::default()` caps one call at 16 targets and 1,024 requested items in aggregate.
These defaults are library safety ceilings and cannot be raised; callers may supply lower positive values.
The aggregate ceiling is evaluated before per-queue caps, so one ordinary queue claim configured above
1,024 may still be rejected by this helper.

There is no cross-queue transaction, rollback, snapshot, ordering, request-id/idempotency envelope, or
all-or-nothing lease guarantee. Each dispatched target remains an independent queue-local claim on that
queue's authoritative owner. Success on one target may coexist with `Backpressure`, `Unavailable`, or any
other runtime error on another.

### 5. Preflight and ownership precede lease effects

`claim_across_queues` runs these phases in order:

1. **Structural preflight, no effects.** Reject empty input, non-positive caller limits, more than 16
   targets, an aggregate request above 1,024 or the caller's lower limit, a target with `claim.max == 0`,
   duplicate `QueueKey` values, or explicit per-target `ClaimAt::lease_time`. Each failure has a distinct
   `EngineError::Invalid` reason; byte-oriented `RequestTooLarge` is not reused.
2. **Definition preflight, no effects.** Load every queue definition and validate the target's requested
   size and compatibility against its queue definition and batch cap. Any failure is one outer error and
   occurs before ownership acquisition or leasing.
3. **Coordinated ownership acquisition, no item leases.** When the facade mode requires ownership sessions,
   acquire every target in facade-local lexical `(tenant_id, queue_id)` order, independent of input order.
   `QueueKey` need not implement `Ord`. Acquisition failure is an outer no-lease error. Earlier sessions,
   ownership records, or advanced fence epochs may nevertheless remain and follow their normal ownership
   lifecycle; they are not rolled back.
4. **Independent claim dispatch.** Only after every required acquisition succeeds, dispatch all target
   claims. Preserve input order for correlation even though ownership was acquired in lexical order.

The helper resolves one common lease time after preflight. An explicit target eligibility time stays
unchanged; an unset eligibility time resolves to that common time, giving all targets one eligibility
snapshot. Each target then reuses `claim_response_at`, preserving compatibility modes, response envelopes,
owner fences, drain behavior, and per-queue caps.

### 6. Fan-in, backpressure, and cancellation are explicit

While the outer future is awaited, a bounded, non-short-circuit fan-in such as `join_all` polls every target
to completion. Target futures are not detached tasks. A runtime failure is stored in that target's result
and never suppresses another target's success. `EngineError::Backpressure` after dispatch is a retryable
per-target result; it is not promoted to an outer error and does not cancel sibling targets.

Dropping the outer future is cancellation, not rollback:

- A target not admitted to its execution boundary has no queue-local lease effect.
- A target admitted to a durable owned blocking executor may continue and commit after polling stops.
- A committed claim remains leased until ordinary finalize, release, retry, or lease expiry.
- Coordinated ownership sessions and advanced fence epochs may outlive the dropped future and follow the
  existing ownership lifecycle.
- No background collector retains correlated results for a dropped caller. A caller that requires every
  per-target outcome must retain and await the outer future.

Target and aggregate ceilings bound future count and requested item fan-in. The helper adds no
unbounded queue, scheduler registry, token bucket, or persisted routing state.

### 7. Dispersion is a pure caller-side choice over an attested prefix

B-007 may add `OldestFirstScopePrefix::attest` and `select_active_scope_from_prefix` as stateless library
helpers. They own no scheduler state and persist nothing. They operate only on one stamped, group-
granularity `ActiveScopeDiscovery` result for one full `QueueKey`; queue-granularity input is invalid.

The caller attests that the supplied scopes are an **unfiltered leading prefix** of that queue's unchanged
discovery result. The wrapper validates the stamp, every descriptor's `queue_id`, and non-increasing
`oldest_eligible_age_ms`. It cannot prove prefix completeness and must say so. Unsorted, cross-queue, empty,
zero-window, or otherwise invalid policy input is rejected before selection.

Selection follows two rules:

1. **Urgency first.** Select source index 0 when
   `oldest_eligible_age_ms + observed_age_skew_ms + urgency_guard_ms >= progress_bound_ms`, using saturating
   arithmetic. This preserves the oldest discovered scope near or beyond the queue-global progress bound.
2. **Bounded dispersion otherwise.** Select deterministically only within
   `min(candidate_window, scopes.len())`. The stable hash is length-framed over the routing-key bytes,
   tenant identity, queue identity, and optional group identity so concatenation ambiguities cannot collide
   by framing. Source order and descriptors are not mutated.

An ungrouped `None` descriptor is selectable and participates distinctly in the hash, but returns
`group_filter_available=false`: `None` cannot be translated into API-001's exact `group_key` claim filter.
Returned index/scope is advisory. The authoritative claim may be empty or different by dispatch time.
The selector makes no fairness or progress promise when callers stop polling, omit part of the leading
prefix, or ignore urgency. B-011's time-only-crossed descriptors must be able to trigger this urgency rule.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Extend API-001 with multi-queue claim and selector operations | Makes every transport implement one shape | Falsely elevates library orchestration into a server atomicity/compatibility obligation | Rejected |
| Add scheduler/router state and worker-capacity traits to pqueue | Centralizes routing | Crosses the scheduler boundary, creates lifecycle/persistence requirements, and duplicates caller policy | Rejected |
| Hide ungrouped omission in the selector | Avoids relational query work | Makes discovery untruthful and permanently excludes valid work | Rejected |
| Short-circuit fan-in on first target error | Lower latency on failure | Loses correlated results and leaves completed sibling effects unobserved | Rejected |
| **Add bounded stateless Rust-facade orchestration** | Keeps API-001 and queue-local authority intact while reducing caller boilerplate | Requires explicit partial-effect and cancellation documentation | **Selected** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Worker loops gain familiar lifecycle names without losing the existing facade or batch-native contract. |
| Positive | Multi-queue callers receive bounded, input-correlated outcomes without a false atomicity claim. |
| Positive | Dispersion can spread ordinary polling while preserving oldest-first discovery and urgency. |
| Negative | Callers must handle partial success, per-target backpressure, leases after cancellation, and ownership effects after failed acquisition. |
| Negative | Relational discovery performs indexed live-item work rather than trusting summary rows alone; B-011 must measure and document that cost. |
| Neutral | API-001, backend claim ordering, durable formats, and scheduler ownership remain unchanged. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Callers treat `claim_across_queues` as atomic | M | H | Distinct name, correlated per-target results, rustdoc contrast with `commit_multi_claim`, and partial-success tests. |
| Cancellation is mistaken for rollback | M | H | Admission-barrier tests prove pre-admission no-effect and post-admission possible commit. |
| Selector input is filtered or stale | H | M | Explicit caller attestation, stamp/order validation, observed skew, urgency guard, and advisory-only result. |
| Live relational discovery exceeds its bounded cost | M | M | Supporting index, query-plan evidence, mixed/stale-summary tests, and review trigger below. |
| Ungrouped work cannot form an exact group filter | H | M | Return `group_filter_available=false`; callers use an unfiltered ordinary claim or another supported filter. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| Alias tests prove lifecycle, retry-time, error, and discovery-order parity while legacy names remain | Any alias changes a transition, timing rule, result, or structured error. |
| Structural/definition failures produce no ownership or lease effect; acquisition failures produce no leases | A preflight reaches ownership, or a failed acquisition dispatches a claim. |
| Fan-in returns one input-correlated result per target and drains all targets while awaited | One runtime error short-circuits a sibling or changes result order. |
| Durable admission-barrier tests distinguish pre-admission cancellation from post-admission possible commit | Cancellation is documented or implemented as rollback. |
| SQLite/PostgreSQL relational discovery includes live ungrouped and time-crossed work with exact stamps and `None` before `Some` on age ties | An eligible scope is omitted, a stamp/order differs, or discovery writes state. |
| Selector tests prove prefix validation, framing, bounded choice, ungrouped diagnostics, and exact urgency threshold | Selection reaches outside the window or bypasses index 0 at the guard. |
| `ddx doc validate`, `ddx doc audit`, `ddx doctor`, `ddx doc stale`, prose checks, and `git diff --check` complete without a new graph defect | This ADR introduces a missing dependency, duplicate ID, broken link, or prose error. |

## Supersession

- **Supersedes**: None.
- **Superseded by**: None.

## Concern Impact

- `durable-priority-queue-semantics`: queue-local claim authority, eligibility, ordering, and progress remain
  unchanged; dispersion consumes but does not replace oldest-first discovery.
- `concurrency-model`: target and aggregate ceilings bound fan-in; cancellation and ownership effects are
  explicit.
- `resilience`: `Backpressure` is per-target and retryable after dispatch; no cross-queue rollback is
  claimed.
- `api-style`: API-001 remains unchanged; all new names and types are Rust-facade additions.
- `o11y-otel`: correlation remains the caller's ordered target/result pair; this ADR adds no new metric or
  unsupported observability claim.

No concern selection or project override changes.

## References

- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/adr/ADR-004-granularity-mapping-and-claim-domain.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/02-design/adr/ADR-018-queue-template-and-exact-ensure.md`
- `docs/helix/04-build/foqs-inspired-interface-and-boundary-plan.md`
