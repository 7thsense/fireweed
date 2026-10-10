# Astra Ultra iteration 3 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE after corrections.
Base: 88b1587c. Scope: shared planner/admission, max_attempts schema binding and docs.

Resolved findings: RetryAt follows main's supplied-time age and max-attempts
failure, distinct from Rearm; corrupt version/sequence authority fails closed
before effects/receipt; rejected plans leave snapshots unchanged; admission and
close linearize; failed post-admission growth preserves charge and reports
OutcomeUnknown. No remaining blockers. Root verified 34 matching engine tests,
strict core/engine library Clippy, format and diff checks. Reviewer verified source
and diff checks; its build context is read-only.

Approval covers semantic primitives, not storage qualification. Adapters still
must resolve durable identity, protect loaded snapshots, atomically publish
complete deltas plus receipts, conservatively charge buffers, own jobs through
caller cancellation and drain at shutdown. PostgreSQL and S3/Turso remain pending.
