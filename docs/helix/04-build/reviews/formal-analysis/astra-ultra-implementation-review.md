# Astra Ultra implementation review and corrections

Reviewer: `gpt-6-astra`, `ultra`, same isolated read-only reviewer as plan review.
Verdict on initial implementation: **REQUEST_CHANGES**. Reviewer reproduced nine
baseline models / 21,517 states and all 15 then-current Python tests; found no
checker soundness defect in SCC/weak-fair-walk construction. All identified
corrections are compatible with vision/PRD and were folded proactively.

| Finding | Correction | Verification |
|---|---|---|
| Namespace oracle activated only for scope_alias fault (`models.py` Lifecycle.violation). | Every replay/conflict validates addressed scope fingerprint and live receipt deadline independent of injected fault. | scope_alias negative control trips F-REPLAY with the same baseline oracle. |
| Actual/expected eligible age called the same predicate. | Reference half-open interval eligibility is independently derived from gate/not-before/lease state; production-side predicate mutant counts leased time. | leased_counts_age trips F-DEADLINE:age-accounting; owner/expiry controls remain sensitive. |
| Prepared job could dispatch after CloseAdmission. | Close disables new dispatch; prepared cleanup remains enabled while owned jobs drain. | dispatch_after_close trips F-BUDGET; shutdown/cancellation litmus verifies charges. |
| Provenance omitted facade/tests/Cargo inputs. | Hash all workspace Rust source/tests, manifests, lockfile and toolchain plus formal scripts/contracts/test plans; conformance compares before/after identities. | Report includes new formal_analysis_tests.rs and preserved dirty PostgreSQL file. |
| Competing-claim witness resumed apply before checking watermark. | Check unchanged applied_high_water, positive deferred command debt and advanced authoritative log head before resume. | Real adapter witness asserts distinct items/tokens while apply is paused. |
| Frozenset iteration could change fair cycle order. | Construct fairness witnesses in sorted-label order. | Cross-PYTHONHASHSEED fixture compares exact serialized reports for seeds 1/2/3. |

Reviewer qualifications were also addressed: lifecycle model now retains both
request-bound claim generations and checks expired old receipt after redelivery;
old_receipt_rebind is a negative control. Admission graph has one job per tenant;
it does not establish tenant contention, selection-waiter capacity or cross-queue
progress. Those remain separately attributed to executed engine byte-admission,
claim-batch selection-fence and async-composed unrelated-queue tests.

The reviewer concluded the package is suitable for approval within its bounded
scope after these corrections and rerunning affected evidence. This record does
not present the initial REQUEST_CHANGES verdict as an unconditional approval.

## Final Astra Ultra disposition

**APPROVE within the declared bounded scope. All six implementation findings are
resolved; no further changes requested.**

The reviewer independently reran 17 Python tests and nine complete models with
21,517 states / 20 detected negative controls. It verified all 365 passing Rust
tests, zero ignored, matching before/after and current source hashes, and all
seven retained log hashes. It also verified isolated RustFS native conditional
create/update evidence. The documented exclusions remain appropriate: this does
not establish whole-program refinement, universal FR-12 deadlines, process-kill
qualification or production-cloud S3 guarantees.
