# Astra Ultra iteration 53 review

Reviewer: gpt-6-astra, Ultra; existing read-only astra_plan_review agent.
Base: 03c8161e. Decision: APPROVE, including all four source files.

Scope: shared admitted exact-command materialization, bounded private PostgreSQL
administrative application, and correction of sparse composite ownership flags.
Application reloads certified predecessors, requires an existing physical ID,
and preserves initial insertion order. It does not reevaluate selectors, user
CAS, lease ownership, clocks, or versions. Commands apply one row at a time;
failures discard the whole outer transaction, and owners immediately abandon
its Rust handle. Owner cancellation drops/rolls back the transaction; public
caller cancellation must leave the admitted backend job running.

Astra requested two materializer corrections: full replacement no longer reads
unknown old index components, and absent-name/prefix certificates are charged
before cloning. Regressions cover both. Final review also included repeated
index-component amplification and the equivalent candidate-before-probe
error-cleanup correction in the shared administrative resolver. Sparse query
index rows remain stored; only complete composite tuples own uniqueness.
Existing sparse rows cannot match complete prospective typed tuple bytes.

Verification: 153 engine queue tests; 15 actual PostgreSQL fixtures (54.24
seconds); public concrete queue conformance/reopen regression (5.09 seconds).
After the final shared resolver fix, all six native administrative fixtures
passed again (54.82 seconds). These include 5,001 resolved SQL updates under
constant admitted memory below 1 MiB, with no physical ID allocation, and
forced hash-collision update/delete/preservation/stale/absent/late-failure
fixtures. Five pure materializer regressions pass. Strict engine library and
PostgreSQL library/tests Clippy, formatting and diff checks pass; Astra
independently confirmed the final clean diff.

Approval excludes public administration, complete receipt/size guarantees,
preview, S3/Turso integration, Snorri cutover and settled comparative benchmarks.
