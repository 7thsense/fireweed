# Astra Ultra plan review and disposition

Reviewer: `gpt-6-astra`, reasoning effort `ultra`, isolated read-only subagent.
Verdict on initial draft: **REQUEST_CHANGES**. No finding required a vision/PRD
change and no production defect was asserted. Reviewer authorized proceeding
with the bounded first package after folding the corrections.

| ID | Priority | Feedback / evidence | Disposition |
|---|---|---|---|
| R1 | P1 | TD-013 postappend/preapply cut and VIS-1 wrongly require serving visibility; OWN-1 and TD-003 single-counter law contradict ownership restart reconciliation. Item-token validity is distinct from owner authority. Rejected per-item dispositions may advance the log without an unauthorized lifecycle effect. | Amend TD-013/TD-003 and CONTRACT-006; retain AsyncProjection and separate epoch credentials. |
| R2 | P1 | Define per-command leads-to, action enabling, weak fairness, environmental suffix, semantic stuttering/deadlocks and finite frontier. Stalled Apply must execute fairly without advancing. | Labeled graph explorer constructs and validates fair lassos; checker fixtures cover fair/unfair service, intermittent enabling, sink deadlocks, continuing arrivals, and limits. |
| R3 | P1 | PRD FR-18/19 logical client-item-key convergence is independent of request receipts; API-001 cohort keys precede member-count mutation. Retention, scopes, body conflicts, renewal and claim expiry matter. | Add F-KEY, receipt/key models and scoped replay cases; cohort-key/member-count remains explicitly separate production evidence. |
| R4 | P1 | Serial batch reference must represent durable-but-unapplied selections and reservations, per-request IDs/tokens/outcomes, selection-changing mutation and item vs atomic cohort units. | Independent serial selection oracle, item/cohort fixtures, deferred polls, mixed addressed result, lost-response litmus and real packed apply tests. |
| R5 | P1 | Owner absence, projection lag and caller filters cannot stop eligible age (TD-003). Round-robin arithmetic is not a production FR-12 proof. | Explicit age model and faulty variants; service-envelope safety fixture with declared assumptions; production deadline remains an independent obligation. |
| R6 | P2 | Peak byte charge includes records + seal frame; global/tenant, waiting/running limits and owned cancellation/ambiguous CAS must be covered. | Admission model plus actual engine/object-log admission tests; define reservation conservation separately from instantaneous allocated bytes. |
| R7 | P1 | Enumerate six opportunities, map exclusions, and test interactions rather than infer composition from isolated families. | Six-row matrix; cancellation/ambiguity/recovery, deferred claim/handoff/replay and stale projection/competing claim litmus cases. No whole-system refinement claim. |
| R8 | P2 | Record dirty-file hashes plus HEAD, complete/truncated status, states/edges/frontiers/fairness/commands, exact counterexamples and separate generic/Turso/public-S3 attribution. | JSON provenance report and implementation evidence, independent test classifications; external qualification never inferred from abstract PASS. |

## Review acceptance boundary

Complete exploration of declared finite models, sensitive independent oracles,
honest exclusions, and executed conformance evidence for represented behavior.
Full product state-space composition, physical storage/network proof and
universal capacity deadlines are not established. All model controls are
injected specification faults, not claims that production contains those bugs.
