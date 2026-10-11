# Iteration 91: current lease ownership on claim replay

A common ClaimOwnership validator checks raw lifecycle, expiry and token hash.
Receipt creation versions are deliberately excluded: retaining and renewing a
claim must replay the current record version and expiry with its original token.
Any expired, completed or otherwise unowned member rejects the entire group.
Malformed raw lease shape or hash length returns OutcomeUnknown.

PostgreSQL uses this validator before payload admission/hydration and again on
hydrated records. Its native metadata SELECT returns only a valid 32-byte hash,
NULL, or an invalid non-NULL empty sentinel. Corrupt oversized stored hashes
cannot be materialized into an uncharged multi-megabyte replay metadata value.

The mandatory shared suite now renews a two-record claim, checks current versions
and expiries with stable tokens, and uses the renewed records for subsequent
atomic work/state changes. Existing partial-settlement and expiry-before-payload
checks remain mandatory. A native PostgreSQL regression injects a four-MiB hash
and verifies OutcomeUnknown through a request limited to 4096 response bytes.

This prepares identical current-ownership semantics for the native Turso reader.
It does not activate that reader or qualify the still-incomplete public S3/Turso
backend, full-manifest scaling, provider durability or matched settled benchmarks.
