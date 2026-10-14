---
ddx:
  id: build-simple-queue-postgres-refactor
  depends_on:
    - api-queue-claim-commit-contract
    - adr-postgres-baseline-queue-contract
  status: accepted
---

# Queue refactor and identical durable backends

## Scope

Governing artifacts: PRD, Architecture, ADR-026, API-007, Snorri integration,
construction configuration, diagnostics and durable-record Contracts; TP-004,
TP-005 and TP-006. Use HELIX v0.15.4 for concern propagation and authoring.

Deliver one concrete facade, one Snorri adapter, native PostgreSQL and S3/Turso
with identical mandatory behavior, shared business planning and comparable
settled benchmarks. Implementation is authorized; Astra Ultra reviews the whole
plan before code and every final iteration before committing/pushing main.
Preserve DDx history without squash, rebase or rewritten closing pointers.

## Shared Constraints

Native storage filtering precedes claim; never broad-claim/client-filter.
Claim-group commit includes atomic workflow/audit state, claimed updates, new
work and immutable results. Original request identity survives rebatching,
rejected replay and expired retention. Fresh successful reads/claims cover ACKed
authority. Current lease validation uses a fresh independent operational clock.

Core/planners own semantic policy once; adapters own SQL and durable publication.
Field/body knowledge is not write intent. Preserve full unsigned versions,
complete identities, JSON equality and original raw definition/secret. Keep
stock Turso, log authority, conditional fencing, exact genesis and real durability
checks. Runtime resource and internal frame limitations cannot weaken the common
consumer domain or manufacture a known rejection after ambiguous publication.

## Implementation Slices

| Slice | Area | Depends on | Gate |
|---|---|---|---|
| Boundary adoption and document cleanup | core/api/testing | Current Architecture and Contracts | Real Cargo dependency/visibility checker, permitted/forbidden controls, link/graph/consumer checks |
| Shared interface and planners | core/api | Boundary map | DTO/limit/fingerprint/outcome tests; complete/grouped/partial-knowledge invariants |
| PostgreSQL baseline | data/testing | Shared semantics | Concrete-handle mandatory suite; native selection, state/new-work atomicity, physical loss/reopen and cancellation |
| S3/Turso full interface | data/infra | Shared semantics and revised formal correspondence | All ten mandatory operations, native selection/hydration, conditional publication, fresh observations, replay and exact rebuild |
| Bounded authoritative metadata | infra | Native object-log contracts | No full-manifest retained/rewrite growth outside admission; corruption/missing-authority/fencing tests; scale measurements |
| Single Snorri adapter | api | Both complete constructors | Real worker/control/visibility/recovery tests, JSON/model pins, accumulated state, transitions and await consumption |
| Diagnostics/configuration integration | api/infra | Exact Contracts | Validation/redaction/no-I/O controls; real OTel receiver and loss/retrieval tests; overhead measurements |
| Formal correspondence | core/testing | Revised properties | Reachable positive witnesses, targeted replayable negative controls, bounds/assumptions and model/code/test maps |
| Settled matched benchmarks | testing/infra | Identical qualified behavior | Same host/source/seed/limits/durability; 10M hot, 1000 cold+hot, density/numeric/drain/cost evidence |
| Consumer cutover and legacy removal | api/data | All preceding gates | One adapter/facade, no legacy side-record or capability fallback; affected tests, examples and docs align |

Boundary adoption is first. The actual command is
`python3 scripts/ci/check_queue_boundaries.py --controls`, shared locally, in
pre-commit configuration and CI. Precise legacy exceptions name source, target,
owner and removal trigger. Do not enlarge a baseline to accept new violations.

## Execution Contract

For each iteration, implement its complete bounded unit, run meaningful checks,
obtain Astra Ultra actual-source approval, then commit and push main. Review
findings remain in scope until resolved. Do not request already-granted approval
again. A passing internal primitive does not activate a complete backend or
establish exceptional durability/performance. Do not stop at a partial slice.

Keep chronological review transcripts, subprocess logs, benchmark snapshots and
historical completion narratives out of active HELIX. Git preserves retired
text; runner-owned target artifacts and CI uploads retain current evidence with
source/config/tool identity and explicit retention/loss. Exact live test fixtures
belong with tests, not among governed specifications. Keep current requirements,
contracts, designs and this execution plan concise and maintained in place.

## Continuation Evidence

Recovery observers share one structurally checked view of the exact admitted
record while retaining independent scope, continuity and poison checks. Fixed
phase totals partition elapsed recovery time into fresh authority, planned
read/worker/transfer, checked history/proof folding and native checkpoint/apply.
Quiescent deltas reject saturation and inconsistent totals. Worker-side parsing
is included in read time; elapsed buckets are not CPU or provider-request counts.


The concrete PostgreSQL facade and mandatory suite are implemented. The hidden
Linux native port assembles all ten operations from shared planners, native
filtered selection, stock-Turso atomic projection and conditional log publication.
Owned response handoff retains admission through delivery or abandoned reply
cleanup. Shared shutdown drains admission and workers and serializes final
resource teardown. These internal operations do not activate the public backend.

Native mutation retries require a strictly newer durable fence, complete checked
log history and covered original-receipt absence. Operation-owned descriptors
bind the requested literal identities and domains to that complete source cut.
Insert, claim and commit preparation and replay compare cached presence,
fingerprint and outcome bytes with these descriptors before accepting an
original result. Matching callers cannot trust an unwitnessed cached expiry;
independently proven caller conflicts retain their existing precedence.
`check_domains.py` bounds domain/identity proof use, including proved absence;
`check_originals.py` bounds metadata and opaque-outcome classification. Their
production mappings remain documentary. Ordered receipt lookups bind the whole
request vector, including aliases and empty requests, before native reads;
cached outcomes require the same source metadata and byte checks.
`check_lookups.py` bounds vector scope, order and duplicate-source rejection.
Addressed state reads bind ordered keys and their current presence/high-water
history to the checked cut. Source-derived exact response sizes precede payload
admission; cached metadata, complete fields and values require source agreement.
`check_state_authority.py` bounds predecessor, scope, cache and size ordering.
Commit and preview bind the complete declared state component, including every
batch sibling, before fresh planning. Metadata-only checks verify presence and
retained high-water; replacement/deletion verify complete old fields without
reading old values. Immutable original replay precedes current-state validation.
`check_state_preparation.py` bounds this ordering and payload knowledge; its
production map remains documentary. State query paths, current work, unique
owners and selection indexes still require full cached integrity.
Fresh catalog-known missing indexes and previously observed malformed authority
fail closed across reopen, independent handles and later metadata deletion.
Native S3 conditional writes classify typed conflicts and never retry a lost
write reply inside the SDK.

Local and real S3/Turso pass the same PostgreSQL conformance functions, including
independent claimers, atomic state/new work, administration and cold rebuild.
The public PostgreSQL baseline passes those same scenarios. Public native
construction and production qualification remain pending. Constructor
configuration must integrate provisioning capacity and validate before external
resource access. Scratch roots are resolved once and shared by cloned handles.

Complete stock SDK memory-peak correspondence, cached integrity and bounded
manifest retention/rewrite remain gates. In particular, native `typeof`/`length`
can materialize overflow records before later per-key admission; opcode and
corruption controls establish correctness, not a hard allocation bound. Settled
release density, scale and cost workloads must qualify performance.

The single Snorri adapter, diagnostics receiver integration, revised formal
correspondence and matched settled release benchmarks remain required. Snorri
binds stats as_of to signed Unix nanoseconds, including negative-epoch hourly
flooring. No completion claim is permitted while any required gate is absent.

## Validation Plan

Run the identical public-handle scenarios on both stores without capability skips.
Exercise exact filtered priority/age/schedule order, wide key/field/value sets,
collisions, full unsigned values, state CAS and uniqueness, successful/rejected
regrouping, empty replay, renewal/current versions, whole-claim expiry, preview,
large administration, result limits and shutdown/caller cancellation.

Use real PostgreSQL and qualified S3/stock Turso. Missing fixtures, ignored tests,
only memory whitebox passes and current debug primitive timings are not parity or
capacity evidence. Include paused apply, lost replies, writer/server process
loss, fencing races, corrupt authority, native rollback/poison and exact rebuild.

Snorri retains vectorized_resplit_replay_check and
same_instance_siblings_use_independent_candidate_projections. Verify arbitrary JSON
state and the complete JsonEqualityEncoding namespace/schema pin before indexed
reads and writes in both numeric modes. State_version advances on transitions
and await consumption, not dispatch/timeout/timer bookkeeping. Host maintenance
must preserve ownership and resource drain semantics.

Release performance acceptance retains the napkin baseline's fixed 10,000
complete recipients/second and 12,500/second stretch target, with the declared
campaign and primitive workload shapes. Both backends must be measured against
that baseline with matched durability and resources; small reference diagnostics
and historical source results do not qualify the current release.

Benchmark useful completed work after full settle/drain. Measure p50/p95/p99,
RSS/retained memory, projection debt, storage volume and S3 requests/cost, with
matched state/fanout/filter mixes and interleaved controls. Failed correctness
invalidates every reported rate; shorter or cheaper durability is not comparable.

## Risks and Rollbacks

| Risk | Response |
|---|---|
| Stale/partial authority or missing original genesis | Fail closed; preserve authority; resolve original identity |
| Large selector or value set | Native index intersection and admitted streaming/spool; no hidden cap |
| Native parser/VM/cache resource growth | Bounded-depth query shape, explicit admission and measured SDK residency |
| Documentation pruning breaks a live gate | Relocate exact fixtures and update readers/generators atomically; run negative controls |
| Model/spec drift | Invalidate affected assurance and recheck correspondence before readiness |
| Consumer cutover mismatch | Keep accepted semantics; fix adapter and rerun both real lanes before cutover |

## Exit Criteria

Both constructors implement API-007 identically. One real Snorri adapter passes
both mandatory suites and recovery/durability paths. Architecture/configuration/
diagnostics/formal obligations have executed evidence. Bounded metadata and
matched settled 10M/density/numeric/drain/cost workloads qualify the claimed
performance envelope. Every required slice and final Astra review passes.
