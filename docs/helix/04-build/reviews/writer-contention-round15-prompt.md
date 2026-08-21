# Adversarial review: writer-contention recovery round 15

Act as a specification enforcer and distributed-systems/performance critic.
Review the plan; do not implement it.

Read the current plan, round-6 through round-14 Claude reviews, governing
artifacts, source release `v0.31.21`, evidence `1787274546`, and cited source.

Round 14 was folded by:

- defining `Finalize` disposition as the maximum over every per-item outcome,
  so any Retry/Release/Rearm escalates a mixed command to shared; S3b and S5
  both test Complete+Retry Finalize→Claim ordering;
- requiring every item/group/cohort exclusive holder to release at append
  publication through a new hook, before apply/render, with one-way
  `KeyedQueueGate`→selection-fence acquisition;
- assigning reachable derived reads: `catch_up_produce`/`live_items` and
  `planner_update_snapshot` wait coordinator coverage before their server
  reads, while the dead derived `snapshot_live_items` helper is deleted;
- splitting the in-fence wait from the 500 ms select/admit/encode work bound:
  S0 records committed catch-up p99 and S5 chooses the next power of two above
  2×p99 (500 ms floor, 5 s ceiling), refusing activation above the ceiling;
- tying reservation-head blocking to the selected backend's append deadline
  plus linger and scheduling slack, with a 30 s watchdog only for local tests;
- preserving byte-split suffixes in FIFO with original `now`, atomically
  re-electing a driver, never bypassing the head, running an oversized head
  alone, and gating completion within at most eight driver rounds;
- adding `eight_way_claim_does_not_restore_47b1a223_wal_freeze` as an explicit
  S5 zero-hang regression;
- declaring S5 a reviewed four-file exception and one atomic revert unit back
  to the provisional pack, bounded by the inert S2/S3b work;
- naming gate keys in S-1 and limiting that slice to response-fidelity loss;
- changing `AfterAppendBeforeApply` fault injection to durable poison in S3p;
- mirroring every decomposition dependency in the slice table.

Concentrate on whether every round-14 blocker/warning is closed and whether any
new correctness, fairness, testability, or slice-order defect remains. Treat
any slice that relies on a later correctness repair as blocking.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-14 audit

For every round-14 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
