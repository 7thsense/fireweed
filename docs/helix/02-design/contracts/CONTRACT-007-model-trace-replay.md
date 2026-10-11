---
ddx:
  id: contract-model-trace-replay
  type: contract
  depends_on: [contract-formal-queue-semantics, adr-s3-turso-public-cell]
  status: accepted
---

# CONTRACT-007: Model trace replay and bounded refinement witnesses

A trace interpreter compares the implementation's normalized observations with
independent model predictions. Mapping real item IDs to bootstrap item ranks is
allowed; changing expected transitions based on arbitrary SUT outcomes is not.
For each observed successor, at least one permitted model successor must match.
The full set of matching successor states becomes the source set for the next
action; a later step may use only transitions from that set. Greedy branch
selection or switching to an unrelated allowed state is forbidden.

The bounded replay fixture contains one queue, two ordered items, two accepted
claim requests, two owner identities, one handoff and one graceful reopen. The
logical clock is fixed and all leases remain active; expiry belongs to the
separate lifecycle models. Initial projection covers the bootstrap push, then
background apply is paused. Model durable claim history and applied lease rows
are separate. A successful claim has durable envelope/replay metadata even while
its projection is stale. Drain applies all commands through a captured frontier;
reopen reconstructs durable history and receipts before returning to paused mode.

Each operation produces an expected normalized result and state. Durable history
includes operation kind, original request ID, selected item ranks, clear replay
token identity and publication epoch. Projection observations use direct committed
reads rather than log overlays; rows must match the model's applied prefix.
Owner/session state includes separate assignment/storage credentials. Retry
results preserve original request/token identity and append no second effect.
Stale-owner probes append no unauthorized claim effect. Concurrent claims permit
both serial orders, without accepting arbitrary disjoint outcomes.

Observe is side-effect free with respect to serving projection coverage. Readers
must not accidentally force deferred apply merely to make the comparison pass.
Execution evidence records both physical watermark diagnostics and normalized
state, the exact corpus hash, source hashes and final attempted/succeeded counts.
Zero cases, unknown actions/schema, graph/search limits, malformed observations
or any unmatched step fail closed and retain the shortest observed failing prefix.

Complete graph exploration is different from implementation replay coverage.
Fast semantic-transition coverage is selected coverage. Exhaustive-edge replay
covers one shortest reachable prefix intended to exercise each edge in the declared
model. Nondeterministic SUT branches can realize different edges, so reports
retain intended and actually matched source/action/target coverage separately; it is not
all unbounded executions or all internal storage/network interleavings. A
close/reopen is not process-kill qualification. Lost response is modeled client
nonreceipt, not a packet-loss injection. The ephemeral CP kernel does not qualify
shared multi-process ownership. These distinctions must remain in reports.

## Ownership-incarnation credentials

Three issued sessions may share only two owner identities. Each new ephemeral
ownership incarnation, including a fresh CP after reopen and a different owner
after that reopen, must establish a new durable fence above previous issued
credentials before serving. Same-session reaffirmation preserves its fence.
StaleClaim names the superseded issued session, including the immediately prior
epoch after Reopen → Handoff. Both Handoff → Reopen and Reopen → Handoff are
required generated interaction schedules.

## Exact observation and reopen contract

Bootstrap creates an injective rank→ItemId mapping for exactly two known items.
Unknown, duplicate or extra row/response/history IDs fail. Request-bound synthetic
token strings are fixed before execution; independently renaming observed tokens
is forbidden. Compare lifecycle, item identity, worker, expiry, version and attempt
count, plus exact durable envelope/receipt operation, request ID, fingerprint,
disposition, multiplicity and ordering. Model-side claim fingerprints are computed
independently from the existing documented body format, not by calling the SUT
fingerprint function. Applied receipt rows are compared separately from durable
receipts. Observation samples committed coverage before and after and fails if it
advances; observational reads do not call facade catch-up helpers.

Graceful reopen captures pre-close projection/history, closes/drains already
published owned operations while apply remains paused, records post-close
coverage, then drops the adapter/cache/ephemeral CP. It opens the same S3 namespace
and Turso file, lets startup repair any unapplied tail, establishes a fresh fenced
session, pauses apply and observes post-open history/projection. Evidence records
retained-state versus tail-repair reopening and both physical frontiers. This is
neither projection_control rebuild nor OS process-loss recovery.

## Bounded ordinary-claim exclusion and repair boundary

When serving rows lag only a completely captured, contiguous suffix of at most
16 authoritative unexpired item-Claim commands (at most 8,192 distinct items),
ordinary claim may exclude those leased IDs while polling applied rows. Publication
epochs may advance across ownership fences but cannot regress. Capture/validation
and selection run under the queue generation turn and exclusive selection fence;
each member's ownership credential is checked independently. The exact serving
connection used for SELECT must cover the cursor from which the suffix was proven.
Other suffixes retain the conservative coverage wait. This bounded repair does
not establish full polling compliance for mixed or over-limit unapplied tails.

A previously Assigned same-owner session may reaffirm its fence under the existing
control-plane lease policy. PendingFence is non-serving; a new assignment epoch
or reacquisition from PendingFence is a new incarnation and must not reuse an old
storage fence. Receipt expiry observations are restricted to sources that actually
store expiry: returned item, durable Claim and applied item row. Request receipt
payloads contain no expiry field.

## Complete all-edge corpus qualification

The canonical `edges` profile contains 17,002 distinct prefixes with 127,208
operation steps and intends all 20,913 edges of the 6,386-state graph. Completion
requires every selected prefix and every step to execute successfully against
real S3/Turso, with exact identity/count/action evidence and unchanged source
hashes. One corpus pass can realize only one of two mutually exclusive concurrent
claim orders per prefix. Full corpus completion therefore does not imply all
model edges were actually witnessed; report intended, matched and unwitnessed
edge counts separately. No expected outcome is changed to force a selected edge.

Replay may run 1–16 independent worlds concurrently (default1). Each world retains
its own CP kernel, S3 namespace and Turso file. Per-trace20s timeout and independent
oracle remain unchanged. After a failure launch no further worlds, drain those
already started, retain their failing/completed prefixes and return FAIL. Durable
progress identifies each completed trace and is never sufficient for final PASS;
final evidence reconciles it against the canonical corpus and full observations.

Each world uses canonical `/fwlog/` and `/fwmeta/` data/metadata suffixes.
Storage-generation detection must resolve their common namespace parent, never
fall back to an empty whole-bucket prefix. A regression checks the production
prefix resolver against the actual fixture factory. This prevents unrelated
completed worlds from changing the cost/scope of initial open and reopen.
