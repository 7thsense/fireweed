# Claude review — round 20

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Universal render isolation | Atomic/derived/public render all share the uncommitted multi-statement helper. | Make one committed coherent helper mandatory for every Turso product and assert pragmas. |
| WARNING | Coverage-cap input | Not every port supplies remaining lease/deadline. | Define explicit per-port caps. |
| WARNING | Slice boundary | Projection changes exceed S3c files; commit_validate transaction is new. | Split/own files and say transaction is added. |
| WARNING | Admission arithmetic | KeyedQueueGate excludes active permits. | Count active+queued or state the true unbounded-per-key term. |
| WARNING | Selection connection | One dedicated connection adds global serialization. | Pool/per-driver it or disclose and meter. |
| NOTE | Recovery append | Reopen outbox drain is outside serving admission. | Classify it as pre-serving recovery. |
| NOTE | Terminology | “gate-first” conflicts with the admission map. | Use SelectionFenceAdmission-first. |

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787286996292347023.jsonl`.
