# Claude review — round 8

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Ordinary eligibility key | Exact resolved `eligibility_at()` includes per-call nanosecond `now`, so ordinary buckets never fill. | Key ordinary `None` claims as one lane, retain exact scheduled `Some(v)`, and prove per-request due filtering under `SystemClock`. |
| BLOCKING | Cross-key Claim frontier | `last_claim` is compatibility-keyed while exclusion is queue-wide, allowing different-key drivers to double-select. | Make the Claim frontier queue-scoped and define initialization/recovery/epoch behavior. |
| BLOCKING | Seal-time fencing | `packed_append` checks epoch before linger and does not recheck at PUT; no slice owns the stale window. | Recheck under the per-shard metadata permit at seal/PUT and test epoch bump during linger. |
| BLOCKING | Driver cancellation/shutdown | Caller-owned driver cancellation after durable append can strand unpublished apply and hang followers. | Dispatch driver as owned work; make publication drop-safe; define queued/dispatched shutdown. |
| BLOCKING | Producer convoy | Exact candidate catch-up occurs inside the exclusive fence and can cover full delayed producer apply despite prose saying apply is excluded. | State the actual scope, instrument hold causes, bound the window, and gate producer starvation. |
| WARNING | Reader isolation | Flipping the shared `read_uncommitted=ON` reader would regress unrelated reads. | Add a dedicated committed selection connection and preserve the shared reader. |
| WARNING | Concurrent replay IDs | Same request ID can be queued twice before either outcome applies. | Deduplicate within queued/in-flight coordinator state and share the outcome. |
| WARNING | Interim regression | Replacing the landed lease pack in S2 before S4/S5 restores per-request transactions. | Keep it live until the new path and batched apply are ready, or declare a floor. |
| WARNING | Frontier recovery | Existing `produce_caught_up` can memoize false coverage; new frontiers lack open/epoch/recovery initialization. | Repair memo semantics and define frontier seeding/reset. |
| WARNING | Retry/reclaim coverage | `note_produce_positions` ignores retry/reclaim. | Extend it and add reclaim→claim ordering coverage. |
| WARNING | Force-seal scope | Current force seal drains unrelated shards/lanes under one produce lock. | Force-seal only the driver's shard/epoch/lane group. |

### Round-7 audit

Resolved: new exact-wait semantics, fence call-site ownership, replay ordering,
mixed-load aggregate gate, tracker transition, stable-prefix admission,
S3 attribution, linger evidence, direct mixed-vector scope, migration drain, and
streaming scheduling.

Unresolved: ordinary eligibility bucketing, queue-wide Claim frontier,
seal-time fencing, driver cancellation, bounded exclusive wait, and dedicated
reader isolation.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The plan is close structurally, but ordinary claims would not batch, cross-key
Claims could still double-lease, the claimed append-time fence is currently a
pre-linger check, and caller cancellation can strand durable work. The
producer-drain wait also needs an explicit bounded convoy policy before the
design is safe to decompose.

Full raw session: `.ddx/agent-logs/svc-1787276878741081474.jsonl`.
