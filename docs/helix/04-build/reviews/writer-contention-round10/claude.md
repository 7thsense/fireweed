# Claude review — round 10

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Global seal lock order | Existing Complete/other append paths still take produce-lock→metadata-permit, deadlocking a new fenced path that takes the reverse. | Refactor every produce path to one global metadata→produce order and test fence-bypassing Complete. |
| BLOCKING | Exact-wait authority | Removing the empty/not-ready shortcut without a non-lagging coverage source reopens the P4 hang fixed by `a7ba4320`. | Use coordinator committed high-water, seed it after recovery, and guarantee every reservation publishes or cancels. |
| BLOCKING | Fence membership | Enumerated Push/Update/retry/reclaim omits other commands that change the Claim predicate. | Add exhaustive no-wildcard `affects_candidates(QueueCommand)` and gate SetGates/Purge ordering. |
| BLOCKING | Fused integrity bypass | Claim+Complete fusion discards row counts and bypasses authority-first `pending_moved` validation. | Enforce the marker/count in the fused path inside S4. |
| BLOCKING | Missing lifecycle hook | Object-log × Turso has no `ProjectionLifecycle`/Drop call site for `close_and_drain`. | Add an owned lifecycle slice naming `lib.rs` and the flavor-safe async drain bridge. |
| WARNING | Cross-queue PUT | Store-wide produce lock contradicts full unrelated-queue independence. | Scope the claim and measure cross-queue append wait, or shard the lock. |
| WARNING | Lease expiry | Retry/linger erodes absolute expiry. | Stamp expiry from requested duration at seal or cap retry. |
| WARNING | Snapshot WAL/stability | One committed snapshot may grow WAL and needs stability proof. | Bound/measure hold and WAL growth; test snapshot stability. |
| WARNING | Publisher liveness | Existing pack followers can wait forever if leader never notifies. | Make/gate owned leader publication for all command lanes. |
| WARNING | S3 regression | S3 changes live waits before S5 but lacks its own baseline gate. | Add S0 non-regression to S3 or defer behavior. |

### Round-9 audit

Resolved: fidelity-first baseline, in-fence frontier re-read, pre-append bound,
dedicated reader, separate byte bounds, and explicit T2 disposition.

Unresolved: global lock ordering, live exact-wait authority, exhaustive fence
membership, fused authority-first integrity, and actual lifecycle drain.

### Verdict

`BLOCK`

### Convergence

`NO`

### Summary

The remaining defects are narrow and verifiable in current code. Globalize the
seal lock order, make coordinator high-water authoritative for live exact waits,
classify every candidate-affecting command exhaustively, cover fused apply, and
create the missing object-log × Turso lifecycle drain hook.

Full raw session: `.ddx/agent-logs/svc-1787278765067977039.jsonl`.
