---
ddx:
  id: build-formal-analysis
  type: implementation-plan
  depends_on: [prd, td-acquire-fence-fault-simulation, adr-s3-turso-public-cell]
  status: complete
---

# Fireweed formal analysis implementation plan

## Intent and authority

Implement executable, bounded formal analysis of the current S3 object-log ×
Turso × AsyncProjection public cell. Preserve vision, PRD, ADR-024 and at-least-once
execution; historical Postgres/SQLite recovery defects are counterexample seeds,
not additional product cells. Preserve existing user edits in relational.rs.
The plan does not authorize a strict read-after-ack barrier, SQL-first claims,
multiple queue owners, or weakening FR-9..12 progress requirements.

## Deliverables and execution order

1. Add CONTRACT-006 formal definitions and TP-006 verification obligations.
   Define durable publication, apply prefix, acknowledgment, fencing, lease
   validity, idempotent identity, batch refinement, eligible age, resource
   conservation and conditional progress. Link relevant ADR/TD/feature specs.
2. Review this plan and definitions with gpt-6-astra at ultra effort. Capture
   evidence-bearing findings and their disposition in reviews/formal-analysis.
   Fold compatible findings proactively; escalate only vision/PRD conflicts.
3. Implement a dependency-free finite-state explorer with exhaustive reachable
   state enumeration, predecessor traces, invariant checks and SCC-based
   eventual-progress checks under explicitly declared fairness. Use separate
   bounded models for recovery/ownership, leases/idempotency, batching and
   admission/progress rather than hiding complexity in an unbounded world.
   Configuration bounds, transition counts, invariant results and model hashes
   accompany every run. Limits fail closed: truncated search is never PASS.
4. Add deliberately faulty variants to prove each important oracle detects its
   target defect: stale-hint authority, acknowledgment before publication,
   stale-owner publication, stale lease completion, duplicate request effect,
   overlapping claim selection, premature admission release and stalled apply.
5. Tie formal transitions to exact production seams and executable conformance
   tests. Extend existing deterministic simulations and facade/engine regressions
   where mapping reveals an untested behavior. Fix concrete production defects
   identified by traces and rerun affected tests; never claim implementation
   refinement from a passing abstract model alone.
6. Integrate a bounded deterministic CI gate, retain reproducible run evidence,
   and review final coverage/gaps. Commit only task changes, preserve DDx audit
   history, and leave user edits untouched. Do not publish or merge remotely.

## Acceptance criteria

- Every contract property has a model oracle or explicit implementation-test
  obligation and a named command. No generic claim of whole-system verification.
- Baseline models exhaust their declared finite domains; negative variants
  produce replayable shortest safety traces or an explicit liveness cycle.
- Item lease validity includes time and current token; owner authority is checked
  independently at publication. Late external execution
  is allowed and is not confused with a second active queue lease.
- AsyncProjection acknowledgment requires durable log publication but does not
  require projection visibility. Empty claims before apply remain legal.
- Recovery discovers authoritative committed segments independently of hints;
  corrupt/missing authority fails closed rather than destructive empty rebuild.
- Batched requests refine serial semantics with separate identities and tokens.
- Admission remains charged while owned jobs retain bytes after cancellation.
- Eventual apply is checked only with fairness and repairable/nonpoisoned input;
  FR-12's numeric deadline additionally requires explicit bounded service/load
  assumptions and workload evidence. Conditional liveness does not prove FR-12.
- Model tests, negative controls and relevant production conformance tests pass;
  evidence states bounds, assumptions, source identity, exclusions and results.

## Verification

`python3 scripts/formal/check.py --output /tmp/fireweed-formal-evidence.json`
`python3 -m unittest discover -s scripts/formal -p 'test_*.py'`
Production commands and exact transition mappings are recorded in TP-006 after
source inspection. DDx doctor/doc stale are attempted if DDx is installed;
absence is recorded, not papered over with direct tracker edits.

## Completion boundary

A reproducible bounded analysis package for all six opportunities, production
conformance evidence for represented behaviors, and fixes for discovered
implementation defects. Unbounded proofs, external side-effect exactly-once,
physical network/storage guarantees and universal capacity deadlines are outside
what this package can establish. These are evidence limitations, not relaxed
product requirements.

## Review corrections adopted before execution

Astra Ultra requested changes compatible with vision/PRD: reconcile TD-013's
strict visibility and epoch equality and TD-003's single-counter law; enumerate
all six opportunities; use per-command labeled weak-fairness liveness with
actual fair lasso traces, deadlock controls and explicit finite frontiers; add
client-item-key dedup, cohort/mixed-result fixtures, records-plus-seal reservations
and cross-boundary litmus cases. No PRD requirement is weakened.

| Opportunity | Formal obligations | Executable coverage |
|---|---|---|
| Crash recovery/acknowledgment | F-ACK/PREFIX/EVENTUAL | recovery graph + recovery litmus + adapter tests |
| Ownership/fencing | F-FENCE | recovery graph + acquire/fence production tests |
| Item leases/lifecycle | F-LEASE | lifecycle graph + Turso expiry/cohort tests |
| Concurrent claims/microbatching | F-BATCH | item/cohort serial-witness graphs + packed apply tests |
| Idempotency/replay | F-REPLAY/KEY | receipt/key graph + retained receipt production tests |
| Progress/resource bounds | F-BUDGET/DEADLINE | admission graph + timed eligibility/service fixtures + dispatcher tests |

Execution retains explicit exclusions and source-to-test mappings for behavior
beyond each finite fixture. Baseline and negative controls both run in CI.

## Completed result

Astra Ultra's final implementation disposition is APPROVE within the declared
bounded scope. All compatible plan and implementation feedback was captured and
folded. Nine models exhaust 21,517 states; 20 negative controls and 17 checker/
litmus tests pass. Seven real conformance suites pass 365 Rust tests, with zero
ignored, including public S3/Turso receipt recovery and two new adapter witnesses.
See [execution evidence](formal-analysis-evidence.md) for reproducible commands,
source/log hashes, native-CAS preflight, exact source mappings and exclusions.

DDx doctor/doc-stale could not run because DDx is absent; both attempts and their
exit codes are captured. This does not change the passed formal/Rust/CI evidence.
Existing PostgreSQL user edits were preserved. No PRD/vision conflict arose.
