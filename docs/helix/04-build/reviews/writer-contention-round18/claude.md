# Claude review — round 18

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Deadline scope | Linger/lock/encode/follower wait precede position allocation but were poisoned. | Split timeout immediately before `engine.produce`; pre-position expires retryably, post-position is ambiguous poison. |
| WARNING | Ambiguity carrier | Waiters receive untyped Storage errors. | Add a typed packed-append pre/post-position outcome consumed by every product. |
| WARNING | Stale constraint | Shared text still cites the unrelated metadata loop. | Align it with the real produce/high-water path. |
| WARNING | Inert calibration | No shared shadow holders contend for the fence. | Route representative shared participants through the shadow fence. |
| WARNING | Claimed-target wait | Frontier/bound/error/metric are unnamed. | Wait a named frontier under a bound; return Backpressure, never StaleLease. |

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787285601444359849.jsonl`.
