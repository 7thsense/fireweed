# Adversarial review: writer-contention recovery round 6

Act as a specification enforcer, distributed-systems critic, and performance
engineer. Review the plan; do not implement it.

Read in this order:

1. `docs/helix/04-build/writer-contention-recovery-plan.md`
2. `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
3. `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
4. `docs/helix/02-design/adr/ADR-017-async-commit-strategy-and-dispatch.md`
5. `docs/helix/03-test/test-plans/TP-005-fireweed-performance-matrix.md`
6. `docs/helix/04-build/ss-objectlog-turso-memory-goal.md`
7. The current implementations named by the plan, especially
   `crates/fireweed/src/turso_compose.rs`,
   `crates/fireweed-objectlog/src/log_engine_store.rs`,
   `crates/fireweed-relational/src/apply.rs`, and
   `crates/fireweed/tests/ss_phased_capacity.rs`.

Also read `docs/releases/v0.31.20.md`, commit `0567e232`, and
`docs/perf/evidence/ss-phased/1787274546/summary.json`. The plan was updated
after `v0.31.20`: the landed global lease pack is a measured precursor, not an
unimplemented assumption. Check that the remaining slices refine it without
double-counting completed work.

Round 5 blocked SQL-first Claim because it could commit a lease before log PUT,
under-reserve apply debt, and reorder concurrent produce/claim. Round 6 replaces
that design: a queue-local gate covers caught-up read selection through append
and apply; selection is read-only; exact encoded commands are admitted before
append; append precedes one packed projection transaction. Confirm the new
protocol actually resolves those faults rather than assuming the prose does.

Pressure-test:

- consistency with log authority, epoch fencing, replay, cancellation, and
  per-request semantics;
- whether queue gating plus a shared keyed coordinator is implementable without
  deadlock, permanent task-per-queue state, or double admission;
- whether one reader selection can be safely partitioned across compatible
  requests and whether the compatibility key is complete;
- whether the sealed command vector can reach one SQL transaction without
  breaking waiter completion or existing response barriers;
- whether Complete validation can isolate one invalid request without changing
  atomicity after packing;
- whether the benchmark can expose ack, settlement, shutdown, and process wall
  without moving apply debt between phases;
- whether every implementation slice and terminal bead is bounded, dependency
  ordered, and executable from its acceptance criteria;
- whether the numerical gates are supported by the governing goal rather than
  invented.

Do not request a public API change when an internal mechanism suffices. Do not
recommend higher inflight as a substitute for fewer transactions. Cite plan
sections and repository paths/symbols for every BLOCKING or WARNING finding.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` when a severity has no findings. A BLOCKING finding means the plan
cannot safely drive beads. A WARNING must be either fixed or explicitly
accepted with a concrete rationale before convergence.

### Prior-blocker audit

For each round-5 blocker (log ordering, apply admission, performance boundary,
and acceptance ambiguity), state `RESOLVED` or `UNRESOLVED` with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only when there are no BLOCKING findings and no unaddressed WARNINGs.
Otherwise `NO`.

### Summary

Two to five sentences. Do not soften a blocking defect.
