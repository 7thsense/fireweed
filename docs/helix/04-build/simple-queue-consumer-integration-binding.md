---
ddx:
  id: build-simple-queue-consumer-integration-binding
  depends_on:
    - api-queue-claim-commit-contract
    - build-simple-queue-postgres-refactor
  status: accepted
---

# Actual consumer integration binding

The four primary operations remain insert, indexed filtered claim, atomic commit
and stats. State read/query/receipt access and an explicitly read-only preview are
auxiliary operations; no public transaction session, stages or whole-group claim
mode is introduced. API-006 remains superseded and archived. This binding closes
requirements exposed by Astra Ultra's actual-source audit before consumer cutover.
The current native adapter is not yet the complete Snorri interface.

## Source evidence

Snorri WorkflowMutationRule permits any literal top-level payload key and any JSON
Value, with exact Value equality (snorri-core/workflow_queue.rs:1140). The adapter
escapes literal key names as RFC6901 segments (snorri-fireweed/lib.rs:14689); its
regression includes route/~tier and first-rule precedence (31484+). Restricting
this to a fixed list of declared workflow fields is incorrect.

Live mutations select already-enqueued work, not a persistent policy over future
inserts (workflow_queue.rs:1160). Pause, suppression and status operations change
per-item gate membership (lib.rs:14504,14944). Suppression rule membership can
outlive the current status label. Active lease and evaluated_at take precedence
when deriving lifecycle (14810,15255); a cached status field alone is insufficient.

Native selector semantics require one queue serialization boundary for selection,
validation and application; original stored response replay without reevaluation;
first-match ownership before mutation preconditions; resolved durable targets and
patches; and no append/idempotency retention on preview (API-001 sections around
376-391; engine port.rs:79). Preview followed by execution with the same identity
is tested (snorri-fireweed/lib.rs:31529). Zero matches and an empty suppression
rule vector are valid execution intents with stable real-execution replay.

There is no selector match cap in WorkflowLiveMutationRequest or the existing
ItemMutationRequest. The old SelectFirst planner materializes a complete queue
image; its Turso-only 1000 addressed-entry cap does not apply to selectors
and is not a generic port limit. Silently applying
API-007's ordinary 4096 addressed-record limit to a broad selector is not valid.

Broad controls consume aggregate outcomes and per-rule attribution. Addressed
UpdateStatus additionally consumes before-lifecycle/lease/schedule and known
overlay memberships for item-specific legality findings (lib.rs:15153). Full body
snapshots are unnecessary. Legacy documentation says invalid entries abort, but
the actual planner reports business Invalid per row and continues (projection
lib.rs:3347-3394,3607 and Turso parity regression). Preserve that actual observable
behavior; envelope/schema/policy errors still abort the operation before effects.

Engagement visibility has up to 50 nonoverlapping request-defined numeric [lo,hi)
buckets plus a missing/null bucket (lib.rs:16718). Results include zero-count bins
in declaration order, separate missing values and omit present values outside all
bins. Queries share one covered per-queue snapshot. Existing shard fanout is not
a globally atomic cross-queue read.

The existing object-log raw tenant+NUL+queue partition and slash metadata paths
are not injective over API-007's accepted NUL/slash/long-name address domain
(log_engine_store.rs:79,744). Identifier restrictions would violate the binding.

## Indexed attributes and eligibility

Keep declared scalar fields strict. Add a mandatory indexed property-bag namespace
for additional typed scalar fields; identity is the complete namespace/name, not
an ordinal, digest or prefix alone. Physical stores index every admitted additional
field and retain its value type. Predicate type mismatch never coerces or matches.
Missing is distinct from present JSON null. Full values disambiguate probe hashes.
All names remain bounded by common request/result bytes, not a new identifier cap.

Snorri projects arbitrary payload entries into a reserved payload namespace using
RFC6901 escaping and canonical JSON equality bytes. The encoding must agree with
serde_json::Value equality for key order, arrays, signed/unsigned integers, floats
and zero signs; a digest-only representation is forbidden. Gate membership uses a
separate boolean namespace. Every whole-record replacement regenerates payload
attributes and removes stale ones atomically. Authoritative scalar overlay fields
compose the returned domain view; storage does not interpret opaque work bodies.

Native workflow claims include storage-side eligibility from the known
paused/suppressed/failed overlay memberships or an atomically maintained dispatch
field. Generic selector expressions provide All/Any/Not, presence/absence, typed
field predicates and intrinsic lifecycle, attempts, lease-active-at-evaluated_at
and exact due-time tests. Membership patches support add/remove/prefix removal.
They do not require loading opaque bodies. Preserve existing overlay precedence
and complete migrated memberships, including any blocked/open authority. This
mapping must not be described as general O(1) SetGates equivalence.

Claim/lease authority continues to use the independent operational clock. Explicit
operator evaluated_at classifies selector snapshots, preserving the existing
preview/control semantics; it does not confer a claimed lease token or make a
caller-supplied timestamp an ownership clock.

## Atomic administrative commits and preview

A claimless commit group may contain one server-resolved administrative intent:
Addressed targets with input order, missing targets and optional expected versions,
or ordered SelectFirst clauses. Both retain one immutable whole-operation identity
and positional/per-clause business outcomes. Addressed administration is distinct
from ordinary all-or-reject CAS work_updates; independent groups are not a
substitute for its original single operation identity/publication. Freeze first
matching clause ownership for every selected row before its preconditions. Plan
per-row business outcomes, accepted patches and aggregate attribution under one
queue operation; failed preconditions do not fall through to later clauses.
Selection and addressed resolution use the containing group's initial authoritative
snapshot after prior successful groups. Own new inserts cannot become targets.
An overlap between any administration-owned target (including a skipped target)
and explicit claimed/work updates or new work inserts rejects the containing
group before effects; disjoint state/audit/new-work additions remain atomic with
the administrative result.

Commit the complete resolved mutation unit and its immutable group identity and
compact outcome histogram together. Missing addressed targets and expected-version conflicts remain positional.
Business no-op/terminal/lease/precondition/
invalid/conflict outcomes belong to the committed selector result, rather than
rejecting all successful sibling rows. Any external state-CAS failure still rejects
the containing group atomically. Existing addressed/claimed group semantics remain
all-or-reject and continue to include state/audit updates and new work.

Broad selector receipts return exact outcome histograms and ordered per-clause
matched counts. Addressed controls may request compact per-item before metadata
and findings inputs; no full body is required. Real empty selections retain an
immutable receipt. Rebatching preserves independent group identities.

Administrative uniqueness is evaluated against the initial fenced owner image,
not a sequential first-wins overlay. A:x→z followed by B:y→x rejects B as a
per-row UniqueConflict (Snorri preserves its legacy Invalid classification).
Two otherwise-valid administrative proposals claiming the same initially free
tuple reject the containing group with UniqueConflict and zero effects. Ordinary
disjoint updates participate in the final all-group uniqueness check, which may
reuse a tuple released by accepted administration and preserves ordinary atomic
swaps. Preview uses this identical final validation; it does not reproduce the
legacy dry-run shortcut that omitted proposed commands from conflict validation.
Native SelectFirst targets must exist once in the initial image and carry their
actual first-matching clause index. Missing/misattributed native targets fail
closed as authority corruption, rather than addressed NotFound results.

Preview bypasses retained execution-receipt lookup as well as writes. It always
evaluates the current covered snapshot and cannot conflict with an execution id.
Preview uses the identical administrative planner and validation/overflow policy against
one covered queue snapshot. Its typed result explicitly says it is a preview;
it appends nothing, changes no version and retains no request identity. Execution
with the same caller identity therefore remains valid. Do not fabricate a durable
CommitReceipt for preview or infer exact counts from subsequent mutable records.

Administrative evaluated_at governs legacy LeaseActive/RequireActive/RejectActive
classification and before metadata. Ordinary claimed-token validation, renewal,
expiry and ownership fencing retain the independent operational clock; historical
admin classification cannot make a stale claimed token valid.

Bound retained planning/log buffers by byte admission and stream/spool resolved
large selections internally while holding queue serialization. No silent match
cap, truncation, partial publish or client-side selector scan is permitted. A
logical SelectFirst intent above 4096 matches must be tested. If the complete resolved unit
exceeds an explicit common publication-byte bound after data resolution, return a
positional durably rejected TooLarge outcome for that group. It changes no work
or state in that group; successful independent sibling groups retain their
effects and receipts. Never return a known envelope QueueError::TooLarge after
any sibling has published. Preview returns the equivalent non-durable rejection
with no partial successful counts. Wire/envelope overflow is checked before any
group begins and may return a known envelope TooLarge. This is an explicit new boundedness
binding, not evidence the old unlimited SelectFirst workload already conforms.
Qualify actual consumer sizes before cutover. Physical multi-object handling, if
needed, stays inside the committed log unit and introduces no public transaction
or stage protocol.

## Snapshot stats and transport identity

Add request-defined numeric bucket dimensions to stats with explicit [lo,hi)
intervals and missing bucket. Preserve request order, zero bins and out-of-range
omission, using one covered per-queue snapshot. Do not issue independent reads per
bin. Exact scheduled nanoseconds remain authoritative; hourly grouping floors
negative epochs correctly through a generic time dimension or atomically maintained
UTC-hour scalar. Common result budgets bound group expansion identically.

Reuse existing object-log epoch ownership, durable publication, failure distinction,
reader isolation, poison/drain and exact genesis replay machinery. Add a versioned
collision-free full-address encoding or indirection, including full-identity
verification and old-address migration. NUL/slash/long-name isolation, reopen and
genesis reconstruction are mandatory. Do not restore a custom Turso fork.

## Reviewed implementation sequence and exit gates

1. Review this entire binding and integration sequence with Astra Ultra before
   implementing extensions. Fold it into API-007 and the refactor plan.
2. Implement additional indexed attributes, generic addressed/selector administration and compact outcomes,
   true preview and bucket stats in the shared binding/planner. Version persistence
   explicitly; existing definitions, receipts and lease authority cannot be
   silently discarded. Add the same new conformance oracles to both backends.
3. Complete native PostgreSQL extensions and covered-snapshot, large-selector,
   preview/replay, Unicode/literal keys, JSON equality and bucket tests. Review and
   push each working iteration. Preserve completed durability/collision evidence.
4. Implement S3/Turso through shared semantics and existing reliability machinery,
   with versioned resolved log materialization distinguishing Preserve from empty.
   Native server-side indexed selection, process-loss/paused apply/ownership/rebuild
   tests and the identical public workload remain mandatory.
5. Expose the one concrete facade, migrate the single Snorri adapter and host-owned
   maintenance, then qualify all real worker/control/visibility behaviors against
   both stores. Do not merge unrelated pre-existing Snorri branch commits.
6. Finish matched release benchmarks, numeric progress/10M/density, legacy migration
   and cleanup gates from the original plan. No activation, exceptional-performance
   claim or legacy API deletion until those gates pass.
