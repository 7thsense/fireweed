# Claude review — round 7

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Claim-vs-Claim linearization | Releasing the selection fence before apply lets the next Claim driver re-select rows still `Pending`. | Track the last Claim frontier and require exact projection coverage before the next selection, or define bounded reservations. |
| BLOCKING | Frontier wait | Current `wait_for_projection` may return uncovered when no apply batch is ready and then memoize the target as caught up. | Shared holders release after apply publication; use an exact wait that cannot take the `!has_ready` shortcut; bound its failure behavior. |
| BLOCKING | Fence ownership | No slice wires the shared fence into Push/Update/retry/reclaim call sites; the object-log lane cannot infer Complete versus other Finalize. | Give wiring an owning slice including `turso_compose.rs` and test that an unfenced producer fails ordering. |
| BLOCKING | Replay ordering | Replay lookup precedes catch-up, so appended-but-unapplied original Claim appears absent; current Class-S envelopes also omit request identity. | Resolve replay after exact catch-up and persist request identity/outcome in S4. |
| BLOCKING | Blind non-regression gate | Phase-isolated SS cannot detect mixed producer/consumer stalls. | Establish a same-SHA mixed-load control before S4 or move streaming earlier. |
| BLOCKING | Bead transition | Three open beads still assert SQL-first Claim and B0–B8 do not exist. | Rewrite/supersede open work after convergence, bind new beads to this plan, and give every slice a named parent-SHA failing test. |
| WARNING | Admission independence | Whole-vector reservation can fail where solo requests succeed. | Degrade deterministically to smaller buckets/per-request dispatch. |
| WARNING | Reader isolation | The current independent reader enables `read_uncommitted`. | Require/read back committed isolation for selection. |
| WARNING | S3 delta | Most multi-envelope packed behavior already exists; only force-seal/fence behavior fails today. | Attribute the delta to `packed_append`/`ready_locked`, not the lease pack. |
| WARNING | Linger ownership | No slice owns the composed 0/1/5/20 ms curve. | Add it to a slice exit gate with an evidence path. |
| WARNING | Mixed pack reachability | Claim and Finalize use separate `pack_lane`s, so a live mixed sealed pack is unreachable. | Test direct relational mixed vectors or explicitly change lane ownership. |
| WARNING | Outbox migration | Pre-upgrade outbox rows still need recovery drain after new writes stop. | Retain drain/schema until reopen migration evidence passes. |
| WARNING | Streaming scheduling | Continuous load makes the one-shot 300 ms produce apply delay and ordered apply policy live. | State/gate the no-starvation apply scheduling policy. |

### Round-6 audit

Resolved: exact eligibility key, append-time epoch fencing, join-before-permit,
M1 control, per-row lease values, Complete idempotency, replay intent,
cancelled-waiter rule, evidence v4, and diagnostic scale ratio.

Unresolved: Claim-vs-Claim and exact-frontier coverage under the narrowed gate;
S3's failing delta; mixed-load visibility; tracker transition.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The log-first inversion and exact compatibility/fencing rules are sound, but
the selection fence still lacks Claim-vs-Claim exclusion and uses a wait that
can report false coverage. Shared-fence wiring, replay ordering, a meaningful
mixed-load gate, and the tracker transition must be explicit before beads can
drive implementation.

Full raw session: `.ddx/agent-logs/svc-1787275837536720297.jsonl`.
