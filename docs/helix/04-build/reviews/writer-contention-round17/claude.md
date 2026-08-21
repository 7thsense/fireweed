# Claude review — round 17

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Durable then cancel | `advance_high_water` can fail after `engine.produce` allocated a position. | Split pre-position cancellable errors from post-position ambiguous poison across every product. |
| BLOCKING | Append path scope | The append path uses `engine.produce` plus `advance_high_water`/`put_json`, not the cited create-only retry loop. | Wrap the real end-to-end `produce_immediate` path and drop the unreachable clause. |
| BLOCKING | Fault products | AC-TXN-4 exercises memory/sqlite/postgres product cancel sites outside S3f. | Give every composition poison-on-durable-unpublished ownership. |
| BLOCKING | Dependency | S3f asserts poison-visible reads before S3c adds them. | Move read assertion to S3c. |
| BLOCKING | Finalize read | `claimed_targets` also serves Finalize. | Put coverage inside the helper for Renew/Reassign/Finalize. |
| WARNING | Conflicting wait bound | Protocol step 4 still says 500 ms for the wait. | Reserve 500 ms for work; use S3m bound for wait. |
| WARNING | Calibration shape | Shadow hold omits real append publication. | Include a representative packed append in the shadow. |
| WARNING | Correctness rebaseline | Mandatory S3c may exceed 10%. | Record an accepted correctness rebaseline and carry it into T2. |
| NOTE | Opt-in calibration | N=100k cannot be an ordinary unit test. | Name ignored command/evidence path. |
| NOTE | Timeout test | A 30 s injected hang is too slow. | Permit short override while asserting default separately. |
| NOTE | Additive admission | Claim and queue-gate caps are separate. | State whether the ~2,048 maximum is intentional. |

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787284951327917424.jsonl`.
