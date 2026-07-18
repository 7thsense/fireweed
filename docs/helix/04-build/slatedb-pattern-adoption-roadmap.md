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
    self_hash: 94bfe631039cd60cfc7238a2556f0218aff8a4dd69d49a54619353b5cb7bb869
    deps:
      adr-async-commit-strategy-and-dispatch: 61bf761b8f8b84581b174eb8f1c64a8893ede0dce9353707fb284f751fb82b5e
      td-s3-object-log-sqlite-projection-mode: f3ce514406d6394b25a637b03b4661e5cd112ef18dbb0d86b0a7d372526dfa4e
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
      tp-scale-substantiation: 6ea31f7e002127ffc5bb82fb1e4c3711085f0e96f8c4960393e77877c3fa67cd
      tp-verification-acceptance-criteria: 37fa4c0857ad98ff397edca5e20d2078b4fdeef9b1ba764f35afff50922610cd
    reviewed_at: "2026-07-18T19:52:55Z"
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
| 3 | SP-03 sequenced metadata | Implement | Removes repeated metadata/deletion race logic before maintenance refactor | Typed fenced-publication and monotone-marker families with per-class ordering and scoped post-create validation |
| 4 | SP-04 object-store telemetry | Implement | Supplies evidence for maintenance and warmup policy | Metrics below retry layer with stable low-cardinality labels |
| 5 | SP-05 maintenance separation | Implement | Uses typed metadata and telemetry to bound execution | Pure planner plus resumable, dry-run executor |
| 6 | SP-06 targeted handoff warmup | Evidence-gated spike | Avoids speculative cache complexity | Adopt only if failover/recovery bars improve within memory budget |
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

- [ ] Every accepted iteration is individually committed and pushed with its tests and governing docs.
- [ ] Any rejected spike has reproducible evidence, a recorded decision, and no dormant production code.
- [ ] The codebase has fewer duplicated policy implementations and no new unbounded resource.
- [ ] The release candidate passes compatibility, conformance, and affected performance gates.
