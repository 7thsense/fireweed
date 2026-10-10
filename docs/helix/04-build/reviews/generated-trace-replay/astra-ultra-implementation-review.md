# Astra Ultra implementation review and proactive corrections

Reviewer: `gpt-6-astra`, reasoning effort `ultra`.
Initial implementation review requested compatible corrections; all were folded.

| Finding | Correction and verification |
|---|---|
| Same owner ID alone can classify expired PendingFence reacquisition as the same session. | Require prior same owner, unchanged assignment epoch and non-PendingFence state. Both APIs failed the new regression before correction; Assigned expiry reaffirmation remains unchanged. |
| Claim-tail bypass must prove complete bounded exclusions under a stable selection interval. | Independent narrow validator rejects gaps, truncation, duplicate IDs, expired leases, foreign queues, regressions, wrong command family and bounds overflow; exclusive selection fence and generation turn span capture/selection. |
| A current grouped request could authorize a stale member. | Validate every accepted member's expected fence independently; generated concurrent current/stale traces require one success and one EpochFenced with no stale effect. |
| Serving-reader selection could consult an older prefix than the validated suffix. | Recheck minimum cursor on the exact locked SELECT connection; direct Turso regression proves foreign/uncovered rejection and unchanged Pending rows. |
| Corpus metadata/coverage could be forged. | Recompute canonical graph, state identities, predictions, transitions and complete selection; Python corruption controls reject omitted cases and forged counts. |
| Observer only tested cursor stuttering. | Compare cursor with actual durable position of independently modeled applied prefix, and require unchanged after-observation watermark. |
| Owner and history checks missed lease state/expiry and authority_first. | Require Assigned unexpired CP lease and authority-first durable claims. |
| Receipt payload does not store lease expiry. | Remove invented receipt expiry; check actual returned item, durable command and applied row expiries. |
| Generic null controls did not cover declared concrete faults. | Add changed token/worker/expiry/request/fingerprint, duplicate history, extra/missing rows, duplicate lease, actual Replay token corruption, legal incompatible next-branch continuation and explicit full-state-set retention controls. |

The final static disposition was **APPROVE within the declared bounded scope**,
conditioned on final source-bound execution evidence and the explicit controls.
The final verification/disposition is recorded in the execution evidence.
No PRD/vision requirement was weakened to accept a failing trace. The bounded
claim-only repair retains conservative waits for mixed/over-limit suffixes;
review expressly excludes a claim of complete poll compliance for those paths.

## Final exact-source disposition

**APPROVE for the exact current source. No remaining corrections.**

After the final duplicate-token control was tightened to two genuinely applied
leased rows, Astra verified the refreshed qualified replay: 91/91 traces,
282 passing action observations, 119 matched edges and all four Rust tests passed.
It verified refreshed conformance: 370 tests, zero ignored, seven suites. Both
reports have stable source hashes matching the final workspace; all recorded log,
corpus and observation hashes and both native S3 CAS preflights were checked.
The previous test-only source delta is resolved. Implementation graph-edge
exhaustiveness remains explicitly false, consistent with selected replay scope.
