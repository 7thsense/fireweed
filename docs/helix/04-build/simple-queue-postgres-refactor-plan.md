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

The concrete PostgreSQL interface and public mandatory suite exist. Native S3
transport/fence/recovery and stock-Turso atomic projection, current claim replay
and filtered candidate selection, resolved claim preparation, owned conditional
claim publication with original-identity resolution, immutable native insert
replay, selective shared-planner insert preparation, owned conditional insert
publication with original-receipt resolution and covered owned commit-receipt
lookup, atomic ordinary claim-group commit preparation and owned conditional
commit publication with fingerprint-bound original-receipt resolution are
internal primitives. Addressed administrative preparation shares PostgreSQL's
row proxy, prefix resolution, positional source, scan/finalization and complete
publication decision. It prepares atomic FWQP2 effects or a zero-effect rejection,
with immutable FP4 replay. Addressed preview shares this canonical preparation,
bypasses execution receipts and uses covered-read freshness without publication.
Broad native selectors and ordinary preview remain required.
Covered owned state reads share complete payload hydration
and addressed-state metadata validation with commit planning. Receipt/state
reads and native indexed queries use one owned completion, admission and
fresh-coverage orchestrator. Query cursors share the PostgreSQL format; native
filters share bounded SQL construction with claims. Native stats aggregate totals,
ordered numeric bins and bounded representative grouping values in one covered
snapshot, with shared typed field hydration and raw-authority checks. Scan/sort cost still requires
the settled density and performance qualification below.
Claim completion validates fresh current ownership and a subsequent authoritative
head cut; provisional tokens cannot become a response. These local checks are
not a public S3/Turso qualification. The full constructor/QueueStore, remaining
reads/mutations and public parity must still land. Full-manifest memory/rewrite
growth remains unqualified.

The single Snorri adapter and matched settled release workloads remain required.
Stats exposes the exact operational classification sample as signed Unix nanoseconds on both native paths. Snorri as_of conversion and negative-epoch hourly grouping must satisfy the consumer binding at adapter cutover.
Old formal models/corpora that permit stale empty claims or expired identity reuse
are superseded for changed properties; revised correspondence is mandatory.
No completion claim is permitted while any of these required gates is absent.

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
