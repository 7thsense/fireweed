---
ddx:
  id: build-slatedb-pattern-adoption-roadmap
  depends_on:
    - adr-async-commit-strategy-and-dispatch
    - td-s3-object-log-sqlite-projection-mode
    - td-sharding-and-shard-ownership
    - tp-scale-substantiation
    - tp-verification-acceptance-criteria
  links:
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: informed_by, to: td-sharding-and-shard-ownership}
    - {kind: verified_by, to: tp-scale-substantiation}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: 2795c75a109b566663002b98334a23a3a38d328918835ee10b33d0744eebc519
    deps:
      adr-async-commit-strategy-and-dispatch: 61bf761b8f8b84581b174eb8f1c64a8893ede0dce9353707fb284f751fb82b5e
      td-s3-object-log-sqlite-projection-mode: 9c3b4dd2e25107fee51941c98dde6875e786d5627ab2704d58b79a30679918fa
      td-sharding-and-shard-ownership: bbb831efc281b902cc54122b99e39ea67da87dd2db8be0a8c144064d54c2ec17
      tp-scale-substantiation: 8d4b9a39799bd01ceb6007fd17832590e7af854bae5092894579b3bcb660d842
      tp-verification-acceptance-criteria: 499b3c2c4300fa311a7189c64fc1321903ad8b2f67045f9bd95c993d690158d5
    reviewed_at: "2026-07-19T02:12:30Z"
---

# Build Roadmap: SlateDB Pattern Adoption

## Scope

Evaluate and land seven independently reversible improvements derived from the SlateDB comparison: byte
budgets, deterministic failure simulation, typed sequenced metadata, object-store telemetry, maintenance
policy separation, ownership-handoff warmup, and stronger segment integrity. Each iteration begins from a
green `main`, updates governing artifacts before behavior, and ends with focused correctness, performance,
and compatibility evidence.

Niflheim and quiet-host tests remain out of scope. Turso remains a derived projection, never log authority.
No iteration may add a broad GitHub Actions matrix dimension; focused jobs require path filters and an
explicit cost ceiling.

## Shared Constraints

- Preserve API-001 success, replay, visibility, fencing, retention, and whole-cohort invariants.
- Keep public and storage-facing boundaries async and `Send`; blocking adapters own and offload a complete
  transaction without holding a standard lock or borrowed transaction across `.await`.
- Put reusable policy and state machines in narrow modules; adapters translate rather than duplicate.
- Every new limit is byte- or work-bounded, observable, configurable, and validated as nonzero.
- Every migration reads the prior durable format and writes only the newest format.
- Each iteration records a before/after microbenchmark or bounded-work metric. A regression above 5% in a
  directly affected hot-path median or 10% at p99 blocks landing unless the governing design accepts it.
- Tests land with behavior: unit state-machine tests, backend differential tests, failure injection, and a
  focused performance guard where the change affects a hot path.
- Claude Fable reviews each individual plan. A Terra implementation agent works in an isolated branch or
  worktree. The primary agent reviews, verifies, merges without history rewriting, commits, and pushes each
  iteration before starting the next.

## Iteration Order

| Order | Plan | Mode | Why this order | Required end state |
|---|---|---|---|---|
| 1 | SP-01 byte admission | Implement | Prevents memory blow-up during all later fault tests | Global and tenant byte budgets with exact permit release and telemetry |
| 2 | SP-02 deterministic simulation | Local GO with conditions: independent model + real segmented-log trace runner | Makes later metadata/GC migrations testable under durable schedule faults | Stable replay, real cuts, typed corpus, <=32-op identity shrink; cross-host/clean-CI/untargeted gates pending |
| 3 | SP-03 sequenced metadata | Implemented locally; release gates pending | Shared eligibility classification removes repeated race logic before maintenance refactor | Typed retained create-only publication, real advance→delete and delete→advance paths, ambiguity reread, HCAS-F1/F2 crash evidence; full release perf matrix pending |
| 4 | SP-04 object-store telemetry | Implement | Supplies evidence for maintenance and warmup policy | Metrics below retry layer with stable low-cardinality labels |
| 5 | SP-05 maintenance separation | Implemented locally; hybrid-async frontier negative spike | Uses typed metadata and telemetry to bound execution | Pure planner plus bounded resumable dry-run executor; async object reclamation conservatively retained pending complete authority API |
| 6 | SP-06 targeted handoff warmup | Negative spike complete; no cache | Avoidable manifest candidates pass identification, but projected p95 gain is only 8.97%–11.69%; each unapplied segment is fetched once | Retain cold authoritative recovery; separately design constant-time head access and async bounded-parallel tail replay |
| 7 | SP-07 segment integrity v3 | Implement | Highest migration risk; benefits from simulation and maintenance tooling | CRC32C corruption check, content identity, legacy v2 decode |

The order is dependency-driven, not priority-driven. SP-01 and SP-02 are P0. SP-03 and SP-04 are P1.
SP-05 through SP-07 are P2 and may stop at a documented negative spike where their individual gates say so.

## Implementation Slices

Each individual plan owns its exact slices. The common landing sequence is:

1. Capture a clean baseline and the plan's affected invariant/performance measurements.
2. Update ADR/TD/test-plan language; validate HELIX links and stale-document status.
3. Add the smallest reusable abstraction and model/unit tests.
4. Migrate one reference path, then remaining adapters through differential tests.
5. Run fault, compatibility, and performance gates; remove superseded code.
6. Review the diff, commit the iteration, merge with `--ff-only` or `--no-ff`, push, and remove its worktree.

## Issue Decomposition

The seven plans are execution units. Each Terra handoff expands only one plan into commit-sized tasks and
must name files, tests, rollback, and forbidden scope. Tracker mutation is not required for this roadmap;
if beads are later requested, each plan becomes an epic and its implementation slices become dependent
beads without changing the acceptance contract here.

## Validation Plan

- [ ] Baseline async/cohort/purge work is independently green and pushed before SP-01.
- [ ] Seven individual plans have Claude Fable review records with no unresolved blocker.
- [ ] Every implementation preserves full-async boundaries and backend conformance.
- [ ] SQLite, Postgres, Turso, in-memory, and object-log paths share policy rather than fork semantics.
- [ ] Object-log crash/replay, epoch handoff, retention, and format-compatibility suites are green.
- [ ] No quiet-host or Niflheim tests run or change.
- [ ] CI workflow cost and runner size do not increase without a measured need.
- [ ] Final workspace tests, clippy, formatting, HELIX alignment, and release evidence pass.

## Risks and Rollbacks

| Risk | Impact | Response | Rollback |
|---|---|---|---|
| Shared abstractions become a second framework | High | Require one reference migration and delete duplicated helpers in the same iteration | Revert the iteration commit |
| Failure harness encodes implementation details | High | Model only durable states/invariants; adapters expose named cut points | Keep spike report, drop harness code |
| New limits reduce throughput | Medium | Separate hard cap from seal target; benchmark hot/cold tenants | Restore defaults or disable tenant sub-budget |
| Telemetry creates cardinality/cost growth | Medium | Fixed operation/error classes; never label tenant, queue, key, or URL | Disable exporter while retaining local counters |
| Durable-format migration strands old logs | Critical | Golden v2 fixtures, mixed-version replay, write-v3/read-v2-v3 | Keep v2 writer selectable until release gate |

## Exit Criteria

SP-04 is implemented through deterministic gates: one construction funnel, typed S3 faults, physical-attempt
and logical-retry separation, scoped E3 snapshots, and counter reconciliation. Its remaining release
condition is the deferred quiet-host no-op median-overhead measurement; later functional iterations may
proceed, but the release performance claim cannot be promoted without that record.

SP-06 completed as a negative spike. Its explicit 200-handoff, two-queue-size, 25/100 ms matrix found
content-addressed candidates but only 8.97% to 11.69% projected p95 benefit, below the 20% gate. No warmup
code landed. The spike
identified two prerequisite gaps for separately governed work: the versioned authority-head walk is not
constant-time, and ownership hydration still runs synchronous storage work without a node-global bounded
background dispatcher. Quiet-host/live timing remains deferred.

- [ ] Every accepted iteration is individually committed and pushed with its tests and governing docs.
- [ ] Any rejected spike has reproducible evidence, a recorded decision, and no dormant production code.
- [ ] The codebase has fewer duplicated policy implementations and no new unbounded resource.
- [ ] The release candidate passes compatibility, conformance, and affected performance gates.
