---
ddx:
  id: build-foqs-inspired-interface-and-boundary
  depends_on:
    - prd
    - api-native-client-interface
    - discover-foqs-scaling-distributed-priority-queue
    - discover-foqs-disaster-ready
    - discover-meta-asynchronous-computing-learnings
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: discover-foqs-scaling-distributed-priority-queue}
    - {kind: informed_by, to: discover-foqs-disaster-ready}
    - {kind: informed_by, to: discover-meta-asynchronous-computing-learnings}
  status: draft
---

# FOQS-Inspired Interface and Boundary Build Plan

## Scope

Implement six improvements derived from the FOQS discovery review while preserving fireweed's existing
queue-local transaction, ordering, progress, and backend contracts.

**Governing artifacts**:

- `docs/helix/01-frame/prd.md`
- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/adr/ADR-004-granularity-mapping-and-claim-domain.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/00-discover/foqs-scaling-distributed-priority-queue.md`
- `docs/helix/00-discover/foqs-disaster-ready.md`
- `docs/helix/00-discover/meta-asynchronous-computing-learnings.md`

## Shared Constraints

- The Rust façade's single-queue binding of `DiscoverActiveScopes` remains an exact oldest-eligible-first
  advisory read; this does not narrow API-001's transport-neutral tenant scope. Dispersion consumes one
  unchanged queue-local result and its `QueueKey`; it never changes backend ordering,
  eligibility, reservation, or progress semantics, and rejects input that is not age-ranked.
- Cross-queue claim is client-side orchestration. Each `BatchClaim` remains owner-local and authoritative;
  there is no cross-queue transaction, rollback, snapshot, ordering, or idempotency envelope.
- All structural validation for cross-queue claim occurs before queue-local control-plane or lease effects.
  Dispatch may acquire coordinated ownership sessions before item leasing. After dispatch starts, every
  target produces a result while the outer future is awaited; one failure cannot short-circuit another.
- Dropping a dispatched cross-queue claim future is cancellation, not rollback: queue-local leases may
  already exist and recover through their ordinary lease-expiry/finalization contract, and a durable target
  admitted to an owned blocking executor may still commit after polling stops; coordinated
  ownership sessions and advanced fence epochs may also outlive the dropped future and follow the ownership
  lifecycle. Callers that need every correlated result must retain and await the future.
- The existing façade already exposes `push`, `claim`, `ack`, `nack`, delayed retry, and discovery. New
  lifecycle names are aliases over those paths, not new engine operations.
- Queue templates are caller-owned configuration values. They do not create a durable registry, change the
  stored `QueueDefinition`, or permit implicit creation from push or claim.
- `ensure_queue` uses atomic `CreateQueue`, then compares the returned stored effective definition with the
  desired resolved definition. It never performs a check-then-create race and never trusts the current
  partial compatibility check as proof of exact equality.
- B-004 inherits each composition's ownership scope: B-010 establishes same-process atomicity for
  in-process control planes and cross-process atomicity only for backends that support multi-process use.
- Downstream/callee rate limits and quotas, worker-runtime capacity, compute admission, and load balancing
  stay outside the fireweed engine. Fireweed deployment/tenant capacity controls remain a P1 fireweed concern.
  No downstream scheduler/router trait, token bucket, or lifecycle state is added to core.
- Public names remain `fireweed` until the separate identity migration epic lands.

## Implementation Slices

| Slice | Outcome | In-Scope Files | Depends On | Validation Gate |
|---|---|---|---|---|
| B-001 | Specify queue-template and ensure semantics | ADR-018 linked to API-001 | None | `ddx doc validate`; `ddx doc audit` |
| B-002 | Publish queue-versus-stream selection guide | `docs/helix/01-frame/guides/choosing-fireweed.md`, `README.md` | None | HELIX graph validation; guide decision cases reviewed |
| B-003 | Publish scheduler/router integration boundary | `docs/helix/01-frame/guides/scheduler-router-boundary.md`, `crates/fireweed/examples/`, `README.md` | None | Example compiles; existing downstream-pacing validation passes |
| B-004 | Add caller-owned queue templates and exact ensure | `fireweed` façade/tests | B-001, B-010, B-013–B-016 | Façade template tests; Clippy/fmt |
| B-005 | Complete ergonomic lifecycle vocabulary | `crates/fireweed/src/lib.rs`, façade tests and rustdoc | B-009 | Alias lifecycle/error parity tests; rustdoc |
| B-006 | Add non-atomic multi-queue claim orchestration | `crates/fireweed/src/lib.rs`, dedicated integration test | B-009 | Preflight/no-effect, partial-result, ordering, and backend tests |
| B-007 | Add progress-aware active-scope dispersion selector | `crates/fireweed/src/lib.rs`, dedicated routing test | B-009, B-011 | Stability, dispersion-window, framing, and urgency tests |
| B-008 | Run final workspace and documentation gates | Workspace and DDx graph | B-001–B-007, B-009–B-016 | Workspace fmt, Clippy, tests; documentation graph validation |
| B-009 | Specify optional façade and routing orchestration | ADR-019 linked to API-001, ADR-004, and ADR-008 | B-001 | `ddx doc validate`; `ddx doc audit` |
| B-010 | Make in-process queue creation atomic under races | In-process planes, wrappers, and conformance tests | B-001 | Cross-handle race tests; engine/façade gates |
| B-011 | Repair relational active-scope omission | Relational group-summary refresh/query, `crates/fireweed/src/lib.rs`, and conformance tests | B-009, B-016 | Ungrouped/time-only discovery and stamp-fidelity tests |
| B-012 | Integrate the public workflow example | Example and README pointer | B-002–B-007, B-009–B-011 | Example compile/run tests |
| B-013 | Make object-log queue creation atomic | Object-log planes and tests | B-001 | Sole-owner/cross-handle race and reopen tests |
| B-014 | Make PostgreSQL queue creation atomic | PostgreSQL native/relational planes and live tests | B-001 | Mandatory cross-process race plus losing-handle use |
| B-015 | Align server SQLite/Turso create planes | Server durable control planes and tests | B-001 | Per-plane create/read conformance |
| B-016 | Make embedded SQLite catalog creation atomic | SQLite log/relational catalogs and public constructors | B-001 | Two-handle races and non-default durable round trips |

## Issue Decomposition

### B-001 — Specify queue-template and ensure semantics

**Goal**: Govern the mandatory template stage without depending on optional convenience work or restamping
the transport contract's existing dependents.

**Acceptance**:

1. The ADR defines caller-owned queue templates, pinned creation policy, explicit `EnsureQueue`, exact
   effective-definition drift rejection, and no implicit create on push/claim. It records direct
   `create_queue` compatibility as an existing, separate contract-alignment gap.
2. Template identity, validation, exact comparison, durable reopen behavior, and non-persisted diagnostics
   are explicit. The ADR specifies `EnsureQueueOutcome` and `EnsureQueueError`, including desired/stored
   definitions, created-state, template name/revision, validation failures, and backend failures.
   Rust-library creation-policy resolution is caller-side, not server-applied.
3. `ddx doc validate` and `ddx doc audit` pass.
4. The artifact is `ADR-018`; it is the normative additive Rust-library contract for B-004, B-010, and
   B-013 through B-015.

### B-009 — Specify optional façade and routing orchestration

**Goal**: Govern additive aliases, fan-in, and dispersion without blocking mandatory queue templates.

**Acceptance**:

1. The ADR retains API-001's exact oldest-first discovery and defines dispersion as optional single-queue,
   caller-side selection over an unfiltered leading prefix at group granularity with a stable routing key
   and advisory progress guard. It records the existing missing ungrouped descriptor as a defect rather
   than hiding it in the selector.
2. The ADR defines non-atomic multi-queue claim inputs, bounded target and aggregate item counts,
   duplicate-target rejection before effects, input-correlated per-queue outcomes, and the absence of
   cross-queue guarantees.
3. The ADR records `complete`, `retry`, `release`, and `discover` as convenience aliases without removing
   `ack`, `nack`, or the batch-native surface.
4. Cancellation, coordinated ownership acquisition, common lease-time anchoring, and relational-only
   discovery availability are explicit. `ddx doc validate` and `ddx doc audit` pass.
5. This ADR is the normative contract for additive Rust-library orchestration; it does not modify or claim
   to extend API-001's batch-native transport surface.
6. The artifact is `ADR-019`; it records durable-admission cancellation, retryable per-target
   `Backpressure`, non-raisable fan-in defaults, the ungrouped descriptor derivation/tie-break, and the
   relationship to B-011.

### B-002 — Publish queue-versus-stream selection guide

**Goal**: Make the product boundary usable without reading internal planning artifacts.

**Acceptance**:

1. `docs/helix/01-frame/guides/choosing-fireweed.md` has DDx id `guide-choosing-fireweed`, an `informed_by`
   link to `prd`, an `informed_by` link to `discover-meta-asynchronous-computing-learnings`, and a decision
   table covering mutable/arbitrary priority,
   `not_before`, leases, item-level delayed retry, groups/cohorts, immutable sequential batches, offsets,
   replay, and broadcast consumption.
2. The guide contains concrete use and do-not-use examples and distinguishes fireweed's change log from a
   stream consumption model.
3. The guide cites the local Meta Async discovery asset without claiming FOQS compatibility or a fireweed
   performance advantage.
4. `README.md` links the guide; `ddx doc validate`, `ddx doc audit`, and an explicit local-target link check
   for every new README/guide link pass.

### B-003 — Publish scheduler/router integration boundary

**Goal**: Turn the existing rate-control non-goal into actionable integration guidance.

**Acceptance**:

1. `docs/helix/01-frame/guides/scheduler-router-boundary.md` has DDx id
   `guide-scheduler-router-boundary`, `informed_by` links to `prd` and `api-native-client-interface`, and
   assigns queue definition, eligibility, priority/progress,
   leases, lifecycle, retry, and durable/idempotent mutations to fireweed; advisory selection/fan-in to a
   router; and downstream/callee rate/quota/concurrency/protection/compute admission to a scheduler or
   worker. A separate row keeps fireweed deployment-level rate limits, noisy-tenant quotas, and tenant
   capacity controls (PRD P1, API-001 `rate_limited`) in fireweed and marks them currently unbuilt.
2. The reference loop obtains downstream capacity before claiming, bounds `max_items` by granted capacity,
   treats discovery as stale, and finalizes every lease.
3. The guide distinguishes `not_before`, gates, group filters, and finalize retry from downstream rate
   tokens; it forbids raw backend access, implicit queue creation, and cross-queue atomicity assumptions.
4. A checked-in, deliberately non-discovery scheduler-boundary example obtains capacity before claim,
   bounds `max_items`, and finalizes every returned lease; `cargo check -p fireweed --example
   scheduler_boundary` and
   `cargo test -p fireweed --test product_validation_tests downstream_pacing_non_goal_e2e -- --nocapture`
   pass, and `README.md` links the guide.
5. The example uses inline fake capacity acquisition and ordinary façade operations only; it exports no
   scheduler/router trait and introduces no token bucket, registry, or persisted routing state.
6. `ddx doc validate`, `ddx doc audit`, `git diff --check`, and explicit local-target validation for every
   new README/guide link pass.

### B-004 — Add caller-owned queue templates and exact ensure

**Goal**: Support cheap dynamic queues without implicit semantics or a new control-plane subsystem.

**Acceptance**:

1. A public, library-only `QueueTemplate` owns a private keyless specification built by exhaustively
   destructuring a `CreateQueue`, plus a pinned `QueueCreationPolicy` and template name/revision diagnostics.
   Prototype queue keys are discarded and cannot affect template identity or diagnostics; new create fields
   fail compilation until mapped. Resolution injects a `QueueKey` and runs `CreateQueue::validate` with the
   pinned policy. The template does not become persisted domain state.
2. `Fireweed::ensure_queue(&QueueKey, &QueueTemplate) -> Result<EnsureQueueOutcome, EnsureQueueError>`
   atomically calls create, then accepts only structural equality between the full
   returned stored effective `QueueDefinition` and the full resolved desired definition. Any drift returns
   `QueueDefinitionConflict`, including rank-error, retention, index, schema, change-record, or other fields
   ignored by current create compatibility checks. Runtime code uses direct `QueueDefinition::eq`; tests use
   that same path plus the exhaustive mutation helper rather than a second comparison implementation.
   The façade-local `EnsureQueueOutcome` carries `created`, the effective definition, and non-durable
   template name/revision while leaving `EngineError` and wire mappings unchanged. The façade-local
   `EnsureQueueError` has validation/backend variants plus `DefinitionConflict { created, desired, stored,
   template_name, template_revision }`; if backend create reports its legacy conflict, ensure reads the
   authoritative stored definition before returning this typed conflict. The `created` flag is required
   because no rollback/delete primitive exists.
3. Same template/same key returns `created=false`; the template applied to distinct keys produces identical
   policies; concurrent create remains delegated to the backend's atomic create. Focused drift tests cover
   every current `QueueDefinition` field, not only fields checked by backend compatibility logic. Separate
   exhaustive destructures over `CreateQueue` and `QueueDefinition` make a newly added field fail
   compilation until template mapping and drift coverage are updated independently.
4. No push, claim, or discovery path calls `ensure_queue`; direct `create_queue` and durable formats remain
   unchanged.
5. Focused façade template tests and reopen-then-ensure tests pass for every supported durable public
   constructor, including `open_sqlite`, `open_sqlite_relational`, `open_objectlog`, PostgreSQL sole-owner/
   coordinated constructors, and embedded durable variants. Fireweed Clippy and workspace fmt pass. The
   ensure path does not claim to repair direct `create_queue`/RESP compatibility.
6. A pre-template or older-build definition that differs after default rehydration returns the documented
   exact conflict. The available remedies are to adopt a new `queue_id` or align the template to the stored
   definition; no delete/recreate or silent normalization is claimed.
7. Each durable constructor's verification creates a definition with every non-default field set,
   drops/reopens the handle, reads the stored definition, and ensures again. The result must remain exactly
   equal and idempotent.
   Tests also assert `resolved == resolved.clone()` and ADR-018 records the `PartialEq`-not-`Eq` caveat.
8. ADR-018 states that ensuring a queue created through another plane requires that plane's exact pinned
   `QueueCreationPolicy`. Tests show policy-default divergence produces a typed conflict carrying both
   resolved definitions. Publicly reachable schema/index types needed to build templates are re-exported or
   their direct dependency requirement is documented.

### B-010 — Make in-process queue creation atomic under races

**Goal**: Establish the create-or-read primitive that exact `ensure_queue` requires.

**Acceptance**:

1. Every in-process `ControlPlane`/`AsyncControlPlane` implementation atomically creates once or reads the
   winning definition; no same-process check-then-overwrite window remains.
2. In-process compose, memory, and `BlockingLibBackend` are tested across concurrent handles in one process;
   durable SQLite composition authority belongs to B-016.
3. Concurrent compatible creators produce one `created=true`, all others `created=false`, and the same
   effective definition. Incompatible losers return `QueueDefinitionConflict`, never storage/PK errors.
4. In-process engine conformance and in-crate `BlockingLibBackend` tests pass with Clippy and fmt.

### B-013 — Make object-log queue creation atomic

1. Sole-owner object-log create is atomic across supported concurrent handles and never overwrites a winner.
2. The winning durable path returns the definition decoded from authoritative storage, not the input echo;
   `decode(encode(definition)) == definition` covers every non-default field.
3. Compatible losers return `created=false`; incompatible losers return conflict; reopen/use tests pass.

### B-014 — Make PostgreSQL queue creation atomic

1. Native and relational PostgreSQL use conflict-safe insert plus authoritative durable re-read on winner
   and loser paths; returned definitions are decoded from the row, not echoed inputs.
2. Losing handles hydrate every derived cache/projection/schema/cursor/counter and immediately push and
   claim successfully.
3. A mandatory live cross-process race proves one winner, compatible losers, incompatible conflict, and
   losing-handle use. Absence of the live gate blocks bead close.

### B-015 — Align server SQLite/Turso create planes

1. Server-owned SQLite/Turso/object-log composition control planes meet atomic create-or-read within their
   documented ownership scope and return an authoritative decoded definition where durable.
2. Per-plane conformance covers compatible/incompatible races and non-default encode/decode equality.
3. Backend tests, Clippy, and fmt pass.

### B-016 — Make embedded SQLite catalog creation atomic

1. Add public `open_sqlite_relational` over `fireweed_sqlite::composed_sqlite_relational`, wrapped in
   `BlockingLibBackend`, and document its discovery capability delta from `open_sqlite`. Both constructors
   derive create authority from their durable `queue_defs`/`queues` catalogs rather than per-handle
   `InProcessControlPlane` state.
2. Conflict-safe durable insert plus authoritative re-read determines winner/loser outcomes; no failure can
   leave a memory-only created queue that disappears on reopen.
3. Two handles on one path race compatible and incompatible creates. Exactly one compatible creator wins,
   losers can immediately use the queue, and incompatible definitions never overwrite the winner.
4. Both catalogs return definitions decoded from durable JSON. Every-non-default-field encode/decode and
   reopen-then-ensure tests cover `open_sqlite` and `open_sqlite_relational` explicitly.
5. SQLite composition/recovery tests, fireweed façade tests, Clippy, and fmt pass.

### B-011 — Repair relational active-scope omission

**Goal**: Make the discovery input required by routing truthful for ungrouped and time-deferred work.

**Acceptance**:

1. Relational group discovery emits API-001's `group_key=null` descriptor for eligible ungrouped work.
2. A group whose `not_before` crosses due with no subsequent mutation appears in read-only discovery with a
   correct observed age. Discovery computes both keyed and ungrouped scope ages live from `fireweed_items`
   with the same eligibility predicate, performs no writes, and uses a documented supporting index. The ADR
   records the query/cost delta from summary-only lookup.
3. ADR-019 governs the ungrouped descriptor as a derived scope without requiring a nullable stored summary
   key and fixes age ties as ungrouped (`None`) before keyed (`Some`) groups. Group and queue granularity
   remain exact oldest-eligible-first.
4. SQLite-relational and PostgreSQL-relational tests cover ungrouped-only, mixed grouped/ungrouped, pure
   time crossing, a stale stored keyed summary mixed with live ungrouped work, no-eligible-work, and
   equal-age ungrouped/keyed ties where `None` sorts before `Some`;
   discovery conformance and backend gates pass.
5. Close with a façade-level ungrouped-discovery test over B-016's `open_sqlite_relational` constructor.
6. Add public `ActiveScopeDiscovery { queue: QueueKey, granularity: DiscoveryGranularity, scopes:
   Vec<ActiveScope> }` and a new façade accessor alongside the unchanged Vec-returning method. A fidelity
   test proves the stamp matches the request and element order is identical to the existing path.

### B-005 — Complete ergonomic lifecycle vocabulary

**Goal**: Present the five conceptual worker verbs without breaking the established façade.

**Acceptance**:

1. Add thin, batch-shaped `complete`, `retry`, `release`, and `discover` aliases over `ack`, `nack`, and
   `discover_active_scopes`; retain all existing names without deprecation.
2. Alias tests prove lifecycle state, relative/absolute retry timing, and structured errors are identical to
   the finalize paths; only `discover` asserts ordered-result parity.
3. Crate rustdoc documents the conceptual worker loop, states that the aliases remain batch-shaped, and
   makes their all-or-nothing batch failure mode explicit.
4. `cargo test -p fireweed --test facade`, `cargo test -p fireweed --doc`, Clippy, and fmt pass.

### B-006 — Add non-atomic multi-queue claim orchestration

**Goal**: Let one library call fan into several queue-local claims without pretending they are one claim.

**Acceptance**:

1. Public `MultiQueueClaimTarget { queue: QueueKey, claim: ClaimAt }`, correlated result types, and a
   distinctly named `claim_across_queues` helper (not `commit_multi_claim`) plus
   `MultiQueueClaimLimits { max_targets, max_total_items }` accept ordered queue-local targets and return
   exactly one `EngineResult<Claimed>` per input in input order. Defaults are 16 targets and 1,024 requested
   items in aggregate; these are non-raisable safety ceilings and callers may pass lower positive limits.
   Rustdoc states that this may reject even one queue whose ordinary claim cap exceeds 1,024.
2. Empty input, any target with `claim.max == 0`, duplicate queue keys, and either exceeded ceiling return
   distinct `EngineError::Invalid` reasons before queue-local ownership or lease effects; byte-oriented
   `RequestTooLarge` is not reused.
3. Structural preflight loads every queue definition and validates per-queue batch size and compatibility
   before any ownership session or backend claim; failure is an outer no-effect error. It then acquires all
   required coordinated ownership sessions in façade-local `(tenant_id, queue_id)` lexical order before
   leasing; `QueueKey` need not gain `Ord`. Acquire failure is an
   outer no-lease error but may retain earlier session/fence effects. Claim dispatch starts only after all
   acquisitions succeed: `Backpressure` and runtime claim failures are per-target and do not suppress
   successes. While the outer future is awaited a non-short-circuit
   `join_all` drains all target futures. Targets are not spawned as detached tasks: dropping the outer future
   stops polling, but a target admitted to a durable blocking executor may still commit after cancellation;
   already-acquired sessions/fences follow ownership lifecycle and committed claims remain leased until
   finalized or expired.
4. Targets with explicit `ClaimAt::lease_time` are rejected in preflight; the helper resolves one common
   lease time. Explicit eligibility times remain unchanged, while unset eligibility resolves to that common
   time so all targets share an eligibility snapshot. It then reuses `claim_response_at`, preserving
   compatibility modes, response envelopes, owner fences, drain behavior, and per-queue batch caps.
5. Dedicated tests cover two successful queues, empty rejection, success plus error, input correlation,
   zero-max/duplicate/target-limit/aggregate-limit no-effect, drop-mid-fan-out behavior, coordinated-mode
   ownership footprint and fence-epoch advance after partial failure, reverse-input `QueueKey` order proving
   sorted acquisition with input-correlated results, explicit-lease-time rejection, common lease
   time, and a durable-backend cancellation fixture with an instrumented admission barrier proving
   pre-admission drop has no effect while post-admission drop may still commit. Rustdoc
   aggregate-ceiling precedence over per-queue caps, and contrasts this non-atomic helper with
   single-queue atomic `commit_multi_claim`.

### B-007 — Add progress-aware active-scope dispersion selector

**Goal**: Spread ordinary worker routing without weakening discovery or progress urgency.

**Acceptance**:

1. B-011 adds a granularity-stamped `ActiveScopeDiscovery` result without breaking the existing Vec-returning
   method. A lightweight public `OldestFirstScopePrefix::attest` wrapper accepts that stamped result, makes
   caller attestation prominent, and validates age ordering plus queue identity. A pure public
   `select_active_scope_from_prefix` helper rejects `Queue` granularity and consumes a one-queue,
   group-granularity prefix, its full `QueueKey`, a stable routing key, a
   nonzero candidate window, that queue's progress bound, observed-age skew, and an urgency guard. It
   returns only a selected index or scope from the unchanged source list and never substitutes for discovery
   ordering. It validates every descriptor's `queue_id` against the supplied key. Ungrouped descriptors are
   selectable but carry an explicit `group_filter_available=false` diagnostic because `None` cannot form an
   exact group claim filter.
2. The selector returns the first scope when `oldest_eligible_age_ms + observed_age_skew_ms +
   urgency_guard_ms >= progress_bound_ms`, using saturating arithmetic; otherwise it chooses
   deterministically within the bounded leading window using a stable, length-framed hash of routing key,
   tenant/queue identity, and optional group identity.
3. The caller attests that input is an unfiltered leading prefix of one queue's `discover_active_scopes`
   result. The selector
   can validate only age sorting: it rejects input whose `oldest_eligible_age_ms` is not non-increasing and
   explicitly does not claim to prove prefix completeness. Tests cover empty input,
   invalid policy, ungrouped diagnostic behavior, unsorted input, window larger than input, stability,
   stale-input skew,
   exclusion outside the window, tenant/queue/group framing, just-below/at-guard behavior, and exceeded
   progress bounds.
4. Documentation calls the result advisory and makes no fairness/progress promise when callers stop polling.
   A time-only-crossed group repaired by B-011 triggers the urgency guard in a focused test.
5. Add a public read-only `Fireweed::queue_definition` accessor so callers can obtain the configured progress
   bound without retaining a create response.
6. Focused routing tests, Clippy, and fmt pass; backend discovery ordering remains unchanged.

### B-012 — Integrate the public workflow example

**Goal**: Demonstrate that the pieces compose without turning the example into a scheduler implementation.

**Acceptance**:

1. Compile-test a discovery-bearing example using B-016's `open_sqlite_relational` constructor and showing
   explicit ensure, push,
   discover/select, queue-local or multi-queue claim, and complete/retry/release. The example handles
   discovery `Unavailable` unconditionally and exercises both grouped and ungrouped descriptors.
2. B-002 and B-003 own their README guide links; B-012 adds only the example pointer.
3. The scheduler guide's matrix explicitly identifies B-006/B-007 as optional stateless caller helpers in
   the fireweed library, not scheduler state, traits, or persistence.

### B-008 — Run final workspace and documentation gates

1. B-012 owns the optional-feature integration example and README pointer; this slice only verifies the
   repository state that actually landed.
2. `cargo +1.97.1 fmt --all --check`,
   `cargo +1.97.1 clippy --workspace --all-targets -- -D warnings`, and
   `cargo +1.97.1 test --workspace` pass.
3. `ddx doc validate`, `ddx doc audit`, and `git diff --check` pass.

## Validation Plan

- [ ] Every public type and helper has focused behavior tests before its bead closes.
- [ ] B-001 and B-009 ADRs land before their corresponding additive Rust-library surfaces; API-001 remains
      the unchanged batch-native transport contract.
- [ ] B-002 and B-003 land first so mandatory public guidance cannot be blocked by API implementation.
- [ ] Each landed stage is independently verified, committed, and pushed to `origin/main`.
- [ ] Full workspace gates pass after integration.

## Risks and Rollbacks

| Risk | Impact | Response | Rollback |
|---|---|---|---|
| Dispersion weakens exact discovery or progress | High | Keep selection caller-side; force oldest when urgent | Remove selector; backend contract is untouched |
| Multi-queue helper suggests atomic fan-in | High | Per-target result type and explicit contract disclaimers | Remove helper without storage migration |
| Template drift passes silently | High | Compare exact returned effective definition after atomic create | Remove `ensure_queue`; retain `create_queue` |
| New aliases split lifecycle behavior | Medium | Delegate to existing methods and parity-test errors/timing | Remove aliases; existing methods remain |
| Guides become scheduler scope creep | Medium | Responsibility matrix and explicit core non-goals | Revert guide; no runtime state changes |
| Additive APIs destabilize downstream users | Medium | Keep the batch-native API-001 surface unchanged; retain established methods | Revert additive commit independently |

## Exit Criteria

- [ ] Detailed DDx beads exist for B-001 through B-016 with the dependencies above.
- [ ] Multi-harness adversarial review has zero BLOCKING findings and every warning is resolved or recorded.
- [ ] B-002, B-003, and B-004 are landed, verified, committed, and pushed even if B-005, B-006, or B-007 is
      narrowed by review. B-004 depends only on B-001 and its atomic-create prerequisites B-010 and
      B-013 through B-016.
- [ ] All implementation beads close with commit and command evidence.
- [ ] The final workspace is clean and matches `origin/main`.
