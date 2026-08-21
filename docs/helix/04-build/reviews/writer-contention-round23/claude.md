# Claude review — round 23

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Phase-blind admission | Prepare gates may be released before derived append, leaving retrying Finalize unadmitted while Push can appear double-admitted. | Classify admission at the append site and phase; assert the class where the fence is taken. |
| BLOCKING | BatchUpdate append | Direct `BatchUpdate` calls `commit_strategy().commit()` without `submit_operation`, so the shared fence would lack admission; the atomic map was also inaccurate. | Add derived BatchUpdate to SelectionFenceAdmission and state that atomic direct paths use no derived fence/admission. |
| WARNING | Incomplete lock order | KeyedQueueGate is held across reads moved to the pool, but the stated order covered only pool and fence. | Define gate→pool→fence, forbid connection→gate, and test a gate-held borrower. |
| WARNING | Unbounded pool wait | A connection can be held idle across bounded fence/drain waits while other callers wait for the pool without a deadline. | Bound pool borrow with named retryable Backpressure and state why pool size is not a liveness premise. |
| NOTE | Stale B-0 wording | The issue graph retained the deleted autocommit fallback. | Use hard-prerequisite wording. |
| NOTE | Pool gate timing | Eight-reader WAL liveness was deferred until S5 after S3r creates the pool. | Move it into S3r. |
| NOTE | Coordinator ceiling | ClaimCoordinator still imposes a process-wide 1,024 active+queued ceiling. | State that deliberate behavior and test distinct active queues. |
| NOTE | S-1 gate keys | Satisfied gate keys were already omitted before `5999aa77`. | Cover and identify the pre-existing gap. |
| NOTE | Zero-time coverage | Immediate lease-target reads can surface apply-lag StaleLease at nonpositive remaining expiry. | Perform a nonblocking coverage probe and return Backpressure if uncovered. |
| NOTE | Calibration order | S3m preceded S4's packed apply shape. | Make S3m depend on S4. |

### Prior-round audit

Eight of round 22's ten folds held against source. The admission map remained
phase-blind, and the resource-order fold omitted KeyedQueueGate. The command
classifier, queued-only KeyedQueueGate semantics, shared-reader retirement,
Deferred-only isolation, pool ownership, response shape, per-product capability,
and numeric rebaseline were otherwise sound.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787289337170047347.jsonl`.
