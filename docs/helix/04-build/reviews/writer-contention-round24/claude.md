# Claude review — round 24

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Pool sizing vs fence waiters | Eight pre-fence Claim drivers can exhaust the shared pool and backpressure healthy renderers on other queues. | Bound driver concurrency relative to capacity or use dedicated driver connections with reserved serving capacity; test more Claim queues than connections. |
| WARNING | Selection admission ceiling | Active+queued global accounting adds an unexplained multi-tenant ceiling to Push, BatchUpdate, Finalize, and bypass appends. | Use queued-only/per-queue accounting, skip bypass, and test distinct active queues. |
| WARNING | Gate-held fence waiters | Existing KeyedQueueGate holders can wait on a shared fence and transitively block later Complete operations behind the queue permit. | Move the fence or document and bound the transitive serialization. |
| NOTE | Calibration shape | The shadow used one 100-item Claim without representative apply-deque contention. | Calibrate eight envelopes/800 items with concurrent same-queue apply traffic. |
| NOTE | S-1 files | Entity rehydration also requires `claimed_from_class_s` in Turso `local.rs`. | Add the file and route through `echo_entity_document`. |
| NOTE | Admission carrier | The shared committer cannot otherwise observe whether the caller holds a keyed permit. | Name the carrier and owner. |
| NOTE | Lock-order suffix | The total order omitted metadata permit and produce lock. | Extend and test the full graph. |
| NOTE | Admission terminology | Byte reservation was still called admission. | Rename it to reservation. |
| NOTE | Error resources | Strict normalization would erase the new Backpressure resource names. | Whitelist the names or narrow the claim. |

### Prior-round audit

All round-23 folds were present and source-accurate. The total-order correction
exposed a shared-pool capacity problem, while active SelectionFenceAdmission and
transitive queue-permit blocking still needed explicit treatment.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787290148713587002.jsonl`.
