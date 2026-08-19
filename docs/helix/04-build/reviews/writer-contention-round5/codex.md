### Findings

| Severity | Area | Finding |
|---|---|---|
| BLOCKING | RETURNING fallback | The fold is still wrong in Cuts: `docs/helix/04-build/writer-contention-recovery-plan.md:78` still says `UPDATE-then-SELECT-by-hash`, contradicting the corrected fallback at `:40` and the explicit round-5 constraint. |
| BLOCKING | Claim/produce ordering | The produce cursor only waits for appends already reflected in `last_produce` (`:50-52`). The plan does not require a queue-local mutation gate across wait → SQL lease txn → log append, so a Push/Update can append after the wait and before claim append. If claim then hits `EpochFenced`, the plan leaves SQL leases/outbox in place (`:46`) while apply skips updates on `Leased` rows (`:70`), allowing live/rebuild divergence. |
| BLOCKING | Apply admission | `reserve()` is required before `BEGIN` (`:46`), but the actual Claim envelope item ids are unknown until the lease txn selects them. The existing coordinator reserves debt from the supplied command bytes, so a stub reservation undercharges, while reserving after selection either holds the writer or can fail after partial planning. The plan needs an explicit upper-bound reservation/API change. |
| BLOCKING | Performance gate | The folded hard gate is present in Performance (`:90`) but contradicted by Done When (`:116`), which still unconditionally requires N=10k produce ≥90% of memory. Acceptance remains ambiguous when profiling proves the remaining gap is physical and the ≥50% hard gate is met. |
| WARNING | Outbox recovery | `:46` says drain outbox once and skip if already on the log, but Done When (`:108-117`) has no acceptance criterion for crash-after-PUT-before-delete or duplicate-envelope suppression. |
| NOTE | Review metadata | Frontmatter still records `review.round: 4` (`:14`) even though this is round 5. |

### Verdict: BLOCK

### Summary
BLOCK. The main folded items are improved, but two folds still contradict themselves in later sections, and the claim ordering story still lacks the serialization needed to make the produce cursor sufficient. The reservation-before-BEGIN requirement also needs a concrete bounded-debt design before implementation.