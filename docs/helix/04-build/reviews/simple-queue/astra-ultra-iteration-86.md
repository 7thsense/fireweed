# Astra Ultra review: iteration 86

Reviewer: gpt-6-astra, Ultra. Decision: APPROVED, corrected source and scope.

Shared claim transition preserves raw expired-lease authority, truthful partial
field/body knowledge, insertion age/order and checked version/attempt updates.
Native indexed selection and atomic PostgreSQL publication remain in the
adapter. RetryAt attempt classification does not become a claim cap.

Astra's hydration finding was corrected: raw claim reads and effective query
reads are explicit; partial reads use conservative empty certificates without
uncharged per-row requested-name copying. Shared conformance now exercises
exact-expiry reclaim beyond max_attempts with selective payload omission.
Reviewer independently checked diff hygiene.

Before landing, final verification passed: three focused shared tests, fifteen
real PostgreSQL physical fixtures (--ignored), mandatory concrete Queue public
conformance/reopen, strict engine production-library and PostgreSQL all-target
Clippy, and diff hygiene. Eight preexisting unrelated engine lib-test lints
prevent a strict engine all-target claim; they were not suppressed.

Approval covers common claim logic and its PostgreSQL integration. S3/Turso
claim publication/replay, full backend activation, Snorri cutover and matched
settled benchmarks remain outstanding.
