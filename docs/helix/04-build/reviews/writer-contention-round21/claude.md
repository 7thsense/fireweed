# Claude review — round 21

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Outcome-read isolation | Push/finalize/pause validators remain on uncommitted reader. | Route every public-outcome projection read through committed isolation. |
| BLOCKING | Strict transaction | validate/apply transaction would span object-log I/O. | Use validation-only committed snapshot; keep apply separate and cover TOCTOU in S6. |
| WARNING | Reassign cap | Reassign supplies a new expiry; nonpositive durations are undefined. | Match Renew and return Invalid before waiting. |
| WARNING | Pragma evidence | ON/OFF support is unproven. | Add S-0 readback/live-writer evidence and follow it. |
| WARNING | S2 files | KeyedQueueGate change belongs in async_commit.rs and affects all products. | Own it and add fan-out/close accounting tests. |
| WARNING | Pool ordering | Borrowing after fence extends the exclusive hold. | Borrow before fence and test eight readers under live apply. |
| NOTE | Per-entry cap | Strict wait must be per request. | Hoist before entry loop. |
| NOTE | Atomic scope | Atomic Turso should not consume derived admission. | Branch explicitly. |
| NOTE | Terminology | S3b retains gate→fence wording. | Use admission→fence. |
| NOTE | S5 SQL | Selection SQL needs projection.rs or an explicit local.rs home. | Admit fifth file or assign it. |

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787287745940307540.jsonl`.
