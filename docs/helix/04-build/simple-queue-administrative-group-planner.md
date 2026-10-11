---
ddx:
  id: build-simple-queue-administrative-group-planner
  depends_on:
    - build-simple-queue-administrative-proposals
    - build-simple-queue-consumer-integration-binding
  status: accepted
---

# Combined administrative group planning oracle

propose_group resolves an already-fenced initial record/probe image into one
speculative administrative-plus-ordinary delta. It never mutates the source.
Its ordinary request envelope must already be validated. This is a planning
oracle, not an activated backend operation: the legacy ordinary receipt inside
the delta is not a durable administrative receipt and must never be acknowledged
as one. The separate administration result is absent on every outer rejection.

Addressed results retain input order. Missing targets and duplicate positions
are counted; later duplicates are Invalid and use initial before metadata.
SelectFirst returns only compact total/per-clause histograms, including empty
clauses. Native target ownership is verified: each target exists exactly once
and names its actual first matching clause. Misattribution/missing native rows
fail closed as authority corruption, not addressed NotFound. Empty intents and
zero matches are valid speculative plans even with an empty ordinary group.

Any administration-owned identity, including missing and precondition-rejected
identities, overlaps explicitly claimed/updated/inserted work only by rejecting
the containing group before effects. Disjoint state changes and new work remain
atomic with accepted administrative proposals. A state CAS, stale claimed lease,
ordinary precondition or final uniqueness failure discards every proposed effect
and all speculative administrative successes. Operator evaluated_at does not
make a claimed token valid beyond the independent ownership_now expiry boundary.

Administrative unique tuples are checked against initial owner/nonowner authority.
A:x→z and B:y→x leave B a per-row UniqueConflict; Snorri must preserve the old
Invalid classification/counts/findings for that outcome. Two accepted proposals
claiming the same initially free tuple reject the group. Ordinary disjoint work
participates in the final combined uniqueness check and may reuse a tuple released
by accepted administration. Final collisions reject every effect. Preview will
use this identical validator, including proposal collisions, rather than legacy
dry-run shortcuts.

Target iteration borrows clause actions/ASTs without cloning an AdministrativeEntry
for every selected row. Update proposals still copy per-target resolved patches
and candidate replacement scratch; the caller must admit these buffers. Existing pure commit validation, transition, unique codec
and delta coalescing are reused. No full source/unique-owner map is cloned; only
ordinary addressed records, per-target proposals and accepted effects are copied.
Histogram addition is checked and fail-closed on overflow.

The shipped planner now uses one admitted accumulator and finalizer for addressed
and broad controls. A broad source supplies byte-ordered, first-clause-owned rows
and must establish fenced completeness. The driver reaches the finalizer only
after successful EOF. Owned one-row sources retain hydration admission throughout
validation/planning; the complete-image reference adapter validates its caller-owned
snapshot once and admits its sorting references. That adapter is an oracle route,
not the native backend selection strategy.

No-op/rejected broad rows retain no record/probe images. Accepted mutations retain
only exact edited/composite dependencies and necessary initial ownership probes;
selector-only values, coverage prefixes and preserved bodies are discarded. One
aggregate delta carries the common receipt identity once. The output owns buffer
guards until drop; explicit caller copies through borrowed accessors require their
own admission. Source/ordering/capacity failures are unknown outcomes with no
partial result. Late overlap dominates deferred sibling uniqueness conflict;
the independent ordinary ownership clock is sampled only after selection EOF.

Allocation bounds cover absent-name/prefix certificate containers, ordinary request
field headers, administrative action containers, and long identities multiplied
across configured indexes before tuple generation. Index identity accounting is
conservative; it is not a qualified performance result. Differential tests apply
both planners' exact commands to the same complete initial image and compare full
records, lease metadata, state, versions, deletions and insertion sequence. The old
whole-image group planner remains test-only as that differential oracle.

Native indexed streaming and fenced EOF authority, canonical result/publication
size admission, positional durable TooLarge receipts and preview remain required
before backend activation. This internal planner does not acknowledge an
administrative operation or implement the public control API.

Pure regressions cover atomic administration/state/new work, CAS rollback,
positional missing/duplicates/initial before metadata, missing/skipped overlap,
zero selections, corrupt native ownership, initial-owner and fresh-tuple conflicts,
ordinary reuse/collision, independent clocks and compact 5001-target no-op results.
The 5001 oracle proves no planner match cap; native indexed streaming, durability,
preview/replay, immutable complete-result receipts, bounded decoding/admission,
above-4096 physical publication, S3/Turso and consumer activation remain required.
