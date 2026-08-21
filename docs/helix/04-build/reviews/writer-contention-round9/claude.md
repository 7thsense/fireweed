# Claude review — round 9

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Claim frontier TOCTOU | Waiting queue `last_claim` only before the exclusive fence lets it change while acquiring the fence. | Re-read/wait it inside the fence before replay and selection; re-budget the bounded hold. |
| BLOCKING | Seal lock order | Holding metadata permit through PUT self-deadlocks when `produce_immediate` reacquires it during high-water advance and creates ABBA with `produce_lock`. | Enforce metadata→produce lock order and add permit-held high-water helper/tests. |
| BLOCKING | Baseline fidelity | Current thin Class-S rows hardcode empty fields/metadata/entity, so evidence `1787274546` measures less than the public contract. | Restore response fidelity first, add conformance, and re-baseline before performance gates. |
| BLOCKING | Driver drain ownership | Object-log dispatcher drain resolves immediately and no slice owns a real product shutdown/drain. | Add a coordinator-owned active-driver registry and lifecycle close/drain call site. |
| WARNING | Log-first apply integrity | Token index updates can be emitted for IDs that did not move from Pending. | Mark authority-first Claim commands, require all named rows to move, and retain legacy outbox behavior. |
| WARNING | Update fence membership | Pending-versus-leased classification can be stale. | Conservatively give every Update the shared fence. |
| WARNING | Hold bound | PUT is not abortable; the 500 ms bound cannot include append. | Bound only pre-append causes, cancel reservation on expiry, and instrument uninterruptible append separately. |
| WARNING | Committed reader blocking | Dedicated committed selection may wait behind an `IMMEDIATE` writer. | Add a held-writer non-blocking selection conformance test. |
| WARNING | Lease expiry | Linger/retries can return an already-short/expired lease. | Enforce a minimum remaining fraction or retry before append; test short leases. |
| WARNING | Byte bounds | Command/debt bytes are not response-body bytes. | Separate command admission from aggregate rendered-response admission. |

### Round-8 audit

Resolved: ordinary eligibility lane, queue-scoped frontier ownership in intent,
dedicated reader, duplicate request attachment, additive slice order, frontier
recovery, retry/reclaim coverage, and scoped force seal.

Unresolved: in-fence Claim frontier recheck, deadlock-free seal-time fencing,
contract-faithful baseline, and real owned-driver drain.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The remaining blockers are specific: a Claim-frontier TOCTOU, seal lock-order
deadlock, a lossy baseline that cannot govern performance, and no implementable
owned-driver drain. Restore public response fidelity and re-baseline before
using the current rates as gates.

Full raw session: `.ddx/agent-logs/svc-1787277844928470687.jsonl`.
