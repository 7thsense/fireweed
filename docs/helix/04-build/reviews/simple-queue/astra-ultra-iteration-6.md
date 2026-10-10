# Astra Ultra iteration 6 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE after compatibility corrections.
Base ed9eb1e9. Scope: shared DTO/planner plus API-007 and binding documents.

Source review corrected a draft assumption: existing UpsertPort replaces only
Pending work, rejects stored-Leased work (including expired awaiting reclaim),
and returns Terminal for completed/failed work. Fresh Replace now preserves
these constraints and resets attempts/insertion sequence only for Pending work.
Administrative rearm is a distinct explicit operation, not ordinary enrollment.

Actual Snorri live controls require lifecycle intent and lease policy, beyond
cancellation: CancelRun/CancelItems, force-Due, unsuppression, resume and operator
completion. Explicit Administrative controls model Keep/Pending/Completed/Failed
with RejectActive/RequireActive/InvalidateActive, protected by expected versions
and group atomicity. They never transfer lease authority. Terminal Complete/Fail
reject; Pending can explicitly rearm. Attempts/insertion sequence are preserved;
Pending state/schedule changes reset eligibility age to max(now,due). No-change
preserves the version. Missing previous body authority for body comparison fails
closed in both planning and unique-probe discovery.

Regression coverage includes terminal nonresurrection, expired stored-lease
protection, cancellation rollback on state conflict, stale claimant rejection,
active operator completion without a token, expiry rejection, rearm identity/age,
and identical empty/nonempty body no-ops versus missing body authority.

DTO encoding is explicitly v3 before adapter activation. The 28-byte claim golden
fingerprint is f9ecef4a79efc3846a7a78fa7a1f96d1c872c42db2898cf92d822dbc67456c68.
Root verified 55 engine queue and 22 core tests, strict library Clippy, formatting
and diff checks. Reviewer inspected final corrections and checked the diff.
No blocking findings remain in this scope. Native PostgreSQL and shared physical
adapter scenarios are separate pending work and are excluded from this approval.
