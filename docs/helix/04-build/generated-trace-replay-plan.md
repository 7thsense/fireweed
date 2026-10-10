---
ddx:
  id: build-generated-trace-replay
  type: implementation-plan
  depends_on: [contract-formal-queue-semantics, tp-formal-analysis, build-formal-analysis]
  status: complete
---

# Generated model-to-S3/Turso trace replay

## Objective and authority

Connect independently generated finite queue/ownership traces to the real public
S3 object-log × Turso × AsyncProjection implementation. Preserve PRD/vision and
CONTRACT-006, existing user PostgreSQL edits and all commit/audit history.
No universal Rust refinement or production-cloud/process-kill guarantee is added.
This is the next bounded execution goal, not implementation of every subsequent
phase in the formal-analysis roadmap.

## Model and schema

Add a small independent combined replay model: one queue, two items, two accepted
request IDs, two owner identities, one ownership handoff and one close/reopen.
Bootstrap durably pushes two ordered items and acquires owner 0; trace operations
begin with apply paused and no item leases. Fixed logical time keeps all item and
owner leases valid. Storage epochs may reach 3 after handoff plus ephemeral CP
restart. Previously admitted commands retain their publication epoch; assignment
and storage epochs are separate credentials.

Operations: Claim(request), LoseResponse(request), DrainApply, Replay(request),
Handoff, StaleClaim(issued_session), ConcurrentClaims and Reopen. Claim is the typed operation's
logical publication boundary, not a separately injected S3 upload cut. LoseResponse
means withholding an already captured server response; it does not kill a socket.
DrainApply resumes to captured authoritative coverage then pauses again. Reopen
closes/drops/reopens the adapter and reconstructs its ephemeral control-plane
kernel; it does not unlink projection files or simulate an OS process kill.
ConcurrentClaims admits either serialized request order and must match a permitted
serial witness; no fixed Tokio scheduling order is assumed.

Schema versioned JSON contains model source hash, declared bounds, complete graph
counts, selected trace IDs, typed actions and independently expected normalized
observations after each step. Item IDs are normalized by bootstrap push identity;
request/token identities remain distinct. Observations cover ordered durable
claim records and request receipts, applied item states/tokens, response outcomes,
publication credentials, and serving-owner/storage-epoch mapping. Physical
positions are retained for diagnostics rather than assumed to equal model ranks.
Unsupported actions, malformed/empty corpora, truncated generation and zero
executed cases fail closed. Seeds/path IDs and failing prefixes are reproducible.

## Execution steps

1. Capture trace/refinement definitions in CONTRACT-007 and extend TP-006 with
   exact independent-model and S3/Turso obligations. Review plan with Astra Ultra
   and fold compatible corrections before implementation.
2. Implement deterministic complete graph exploration and corpus generation.
   A fast corpus covers explicitly enumerated semantic transition classes and
   interacting schedules; an exhaustive-edge corpus emits shortest prefixes for
   every reachable modeled edge as intended coverage; nondeterministic SUT paths
   have separately reported matched-edge coverage. Reports distinguish graph completeness from
   selected SUT replay coverage. Do not label sampled SUT replay exhaustive.
3. Implement a Rust test-side trace interpreter over real S3 LogEngine and real
   Turso with namespaced storage per trace. It checks normalized model outcomes
   after every step without using SUT results to construct the expected oracle.
   Concurrent-operation nondeterminism is represented by allowed successor states,
   with later model propagation retaining every matching successor state.
4. Add checker/decoder/oracle negative controls: malformed schema, wrong durable
   history, duplicate lease, corrupted request identity, stale-owner effect and
   changed replay token. Controls corrupt observed test data, not production code;
   the same comparison oracle must reject them. Preserve a failing trace/prefix.
5. Run generated replay against isolated task-owned RustFS, native CAS preflight,
   existing formal/control suite and affected conformance tests. Fix confirmed
   production defects and rerun affected gates proactively. Capture final Astra
   review and provenance with exact source/corpus/log hashes.
6. Wire fast generated replay to the existing correctness lane and provide an
   explicit deeper exhaustive-edge command. Commit only task files and preserve
   the existing PostgreSQL edit. No publication or remote merge is requested.

## Acceptance criteria

- Corpus deterministically derives from a complete finite graph, with source hash,
  schema/bounds, actual state/edge counts and auditable selected edge coverage.
- Real S3/Turso executes every selected trace with exact per-step comparisons.
  Parallel claims accept only allowed serial outcomes and preserve identities.
- At least one generated trace covers each required interaction: durable claim
  with lost response → deferred apply → handoff → replay; competing claims while
  applied rows remain stale; close/reopen of durable receipt history.
- Stale owner probes have no unauthorized effects or new claim history; active
  item leases survive owner handoff and receipts replay their original token.
- Ordinary observation does not advance the serving projection; observations use
  direct committed Turso reads, independently of overlay-serving helpers.
- Same oracles detect all defined observation/decoder controls; failed generation,
  malformed or zero-case input and truncated execution never produce PASS.
- Evidence distinguishes complete model graph, selected SUT traces, one-process
  CP kernel, real local-S3 publication, graceful reopen and genuine process loss.

## Verification and review boundary

`python3 -m unittest discover -s scripts/formal -p 'test_*.py'`
`python3 scripts/formal/generate_replay.py --output target/replay-corpus.json`
`python3 scripts/formal/with_rustfs.py --binary /absolute/path/to/rustfs --output target/formal-conformance.json`
A dedicated generated-replay runner uses the corpus/environment-driven Rust test
and retains per-step normalized observations, source identity and failing prefix.
Final commands/test names become concrete in TP-006 after implementation.

## Astra plan corrections folded before implementation

Retain up to three issued sessions across two owner identities and require both
Handoff → Reopen and Reopen → Handoff. Stale probes reference the actual
superseded issued credential, not only the bootstrap owner. The full matching
model-state set propagates between steps; intended model-edge selection is
separate from actually matched SUT coverage. The ownership equality branches are
a generated defect hypothesis until independently reproduced by the real runner.

Astra additionally required an exact injective observation schema (IDs, fixed
request tokens, workers/expiries/versions, original durable receipts/fingerprints,
applied receipt multiplicity), side-effect-free observation watermark checks, and
explicit pre-close/post-close/post-open graceful-reopen frontiers. CONTRACT-007
captures these. Decoder/oracle controls include extra/missing rows, changed expiry
and a cross-branch history that has individually legal steps but no model path.
All five plan findings are folded; implementation proceeds within this scope.

## Execution disposition

Implemented independent complete graph generation, canonical corpus validation,
real S3/Turso interpreter, concrete observation controls and CI smoke execution.
The combined graph has 6,386 states and 20,913 edges; the smoke corpus has 91
selected traces targeting 83 semantic edge classes. Both concurrent claim orders
remain model-permitted and actual matched edges are reported independently.

Generated witnesses confirmed two production defects: ephemeral ownership could
reuse an equal durable fence on a new incarnation, and sequential claims under
bounded claim-only apply debt timed out instead of excluding already leased IDs.
Both are repaired with regression evidence. Astra additionally reproduced an
expired PendingFence same-owner fence reuse, repaired in both ownership APIs.
Existing Assigned same-owner reaffirmation is preserved. The narrow claim repair
validates cross-epoch contiguous tails under exclusive selection fencing and
checks each request credential and the exact serving connection's coverage.
Mixed/over-limit tails still use conservative coverage waits; full ordinary-poll
compliance for those tails is outside this completed bounded execution goal.

All compatible Astra findings were folded. Exact commands, source-bound results,
review dispositions and limitations are recorded in
[execution evidence](generated-trace-replay-evidence.md).
