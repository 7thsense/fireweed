# Adversarial review: writer-contention recovery round 14

Act as a specification enforcer and distributed-systems/performance critic.
Review the plan; do not implement it.

Read the current plan, round-6 through round-13 Claude reviews, governing
artifacts, source release `v0.31.21`, evidence `1787274546`, and the cited
implementation paths.

Round 13 was folded by:

- keeping the S2/S3b coordinator, fence, and exhaustive classifier inert until
  S5 atomically activates all production dispositions with log-first item
  Claim, eliminating the unsafe interim exclusion domain;
- landing S3c exact committed coverage before S3b and S5, with ownership of the
  dedicated committed recovery connection in `fireweed-turso/src/local.rs`;
- replacing grouped/cohort split prepare→commit with an S5 gate-first exclusive
  operation in `async_composed.rs` that holds continuously from in-fence exact
  waits through candidate selection and append publication, then advances the
  queue-scoped Claim frontier;
- moving all live producer/Claim ordering and CohortClaim×item-Claim gates to
  S5, while S3b retains only executable classifier/fence model tests;
- treating any outstanding same-shard reservation as a possible gap filler,
  independent of deque index, and poisoning only after no reservations remain
  plus a 500 ms no-progress deadline;
- bounding/metring a reservation-head stall at 30 seconds, with expiry causing
  poison rather than out-of-order apply;
- allowing reservation cancel only before append durability and making a
  durable-but-unpublished guard poison/wake immediately;
- removing both the uncommitted recovery cursor and all-views-present live
  shortcuts from `snapshot_live_items`;
- naming exact serialized `Vec<CommandEnvelope>` bytes as authoritative apply
  debt and charging rendered response bytes separately;
- splitting S8c cleanup (`fireweed-ec528b80`) before terminal S8q qualification
  (`fireweed-59eae996`), so the N=100k gates run on final serving code.

Concentrate on whether every round-13 blocker/warning is closed and whether the
atomic S5 activation, gate→fence order, exact-wait liveness, or new dependency
graph remains infeasible. Treat any slice that relies on a later correctness
repair as blocking.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-13 audit

For every round-13 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
