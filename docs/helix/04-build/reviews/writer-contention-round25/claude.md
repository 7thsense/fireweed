# Claude review — round 25

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Pool cache budget | Sixteen new connections inherit 128 MiB each, raising the configured page-cache ceiling to 2.25 GiB and undermining M1/M3. | Give pooled readers a smaller explicit cache and assert/record the aggregate ceiling. |
| BLOCKING | Shared-appender starvation | Claim drivers and shared Push/Update/Retry/Purge validation share the driver pool, so saturated Claim waiters can consume every slot. | Reserve capacity for shared appenders or cap concurrent Claim borrows; test progress under saturation. |
| WARNING | Writer/apply order | The total order omitted the Turso writer and global apply worker that advance high-water. | State the one-way dependency, prohibit writer→earlier-resource acquisition, and test it. |
| WARNING | Reader-count premise | The liveness probe still covered eight readers while the design permits sixteen. | Probe and test the full reachable count. |
| NOTE | Carrier scope | Generic packed append and RawCommitRequest callers prevent a whole-workspace compile gate within S2a's files. | Make generic builders explicitly non-derived and describe the derived carrier check as a source-audit/test gate. |
| NOTE | Pool observability | Both pools shared one error name and only driver wait was named. | Use distinct resource names and metrics. |
| NOTE | Zero-starvation wording | Expected pool Backpressure conflicted with a blanket zero-expiry assertion. | Scope zero to fence/drain/coverage and count pool/slot expiry separately. |
| NOTE | Idle connection hold | The plan did not state the maximum pre-fence connection hold. | Bound it and distinguish borrow deadline from lease duration. |

### Prior-round audit

All round-24 folds were present. The remaining failures were the unbudgeted
memory/capacity consequences of the split pools and missing writer dependency.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787291192563731023.jsonl`.
