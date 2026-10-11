---
ddx:
  id: build-simple-queue-administrative-proposals
  depends_on:
    - build-simple-queue-consumer-integration-binding
    - build-simple-queue-indexed-attributes
  status: accepted
---

# Shared administrative selection and target proposals

This iteration adds supporting vocabulary and pure per-target proposals. It does
not activate an administrative QueueStore operation or manufacture a durable
receipt. The operation planner must still enforce queue fencing, native indexed
selection, initial-snapshot ownership, positional duplicates, disjoint atomic
state/new-work updates, uniqueness, retained replay, overflow, preview and exact
resolved publication. No unsupported backend method or new transaction API exists.

Selectors express All/Any/Not, typed field predicates, presence/absence, key sets,
stored or effective lifecycle, attempts, exact signed-nanosecond available time,
and lease activity at explicit evaluated_at. Stored lifecycle is preserved raw;
expired leases may separately classify effectively Pending. Empty All is true and
empty Any false. Presence of canonical JSON-null bytes differs from field absence.
SelectFirst freezes the earliest matching clause before checking its preconditions.
Failure cannot transfer a target to another rule.

Addressed entries retain input order, optional expected versions and per-target
business outcomes. Standalone validation enforces the common addressed count,
types, structure and individual body bounds; complete serialized-intent and
resolved-publication byte admission remain integration gates. The old Turso-only
1000-entry cap is not a generic interface requirement. Duplicate keys remain representable: the operation planner
must report later duplicates positionally Invalid. An expected version of zero
is representable and conflicts with a real positive version. Broad selections
have no helper match-count cap and return aggregates rather than before rows.

Before metadata retains stored lifecycle, version, attempts, due/eligible time and
lease expiry plus explicitly selected fields, never the body or a lease token.
Prefix removal is restricted to provisioned additional namespaces. Removal happens
before explicit edits, so additions win. Per-target proposals accept certified
selective field authority. The checked selector helper validates every Field/Present
reference, including type validity of loaded values, before evaluating any truth;
unloaded absence cannot become a match. Before fields require the same certificates.
Prefix removals require complete prefix enumeration. Explicit edits are resolved
once into exact Edit/Preserve commands; every actually touched composite-index
component is certified, including nonunique indexes. Deletion requires all unique
components. Unrelated payload fields need not be loaded or republished.

Old body authority is required only when comparing an explicit body replacement.
Equal explicit bodies produce Preserve; changed bodies, including empty bytes,
produce Replace. Metadata-only proposals preserve unloaded bodies. Only Changed
has a candidate and exact field/body commands; no-op/rejection/deletion has none.
Proposals validate durable version high water and fail closed on inconsistent
authority or exhausted versions. The complete-image group oracle consumes these
exact commands and retains initial-owner/final combined uniqueness checks; its
complete-only input requirement remains until the streaming planner is qualified.

Existing version-fenced Administrative updates now call one extracted transition
helper with unchanged operational-clock semantics. Administrative proposals call
that same helper with explicit evaluated_at. This timestamp confers no claim
credentials and does not change ordinary claim/token/renewal ownership clocks.
Candidates are not uniqueness-qualified or durably committed; final operation
planning must still reject or attribute unique conflicts before publication.

Selectors now own a flat unshared postfix program. All/Any carry explicit child
counts; Not has one child. Checked scalar operand accounting rejects underflow,
empty programs and disconnected roots. Reverse traversal uses a fixed 64-frame
parent stack, independent of expression width, and evaluates every leaf. The
maximum depth counts leaves and empty operators: 63 Not operators plus one leaf
is valid; 64 Not operators plus one leaf is invalid. Every field predicate is
validated independently of boolean truth.

Clone, drop and derived serialization/deserialization no longer recurse through
selector ownership. This does not make unbounded decoding safe: byte admission
must precede owned decoding and account for flat node/value allocation. No live
CommitGroup, fingerprint, receipt or transport format changes in this prerequisite.
Native indexed selection, selective field coverage and durable controls remain
separate gates; the reference evaluator is never a client-side claim filter.

Pure regressions cover first-match failed preconditions over 5001 targets, wide
selector traversal, exact expiry boundaries, raw/effective lifecycle distinction,
missing/null bytes, prefix precedence, compact before fields, no-op versions,
missing/conflicting rows, unloaded-body preservation and materialization failures.
The 5001-target test establishes absence of a helper cap only. Whole-selector
atomicity, bounded spooling, above-4096 publication, preview/replay, native backend
parity, legacy migration and consumer activation remain required next gates.
