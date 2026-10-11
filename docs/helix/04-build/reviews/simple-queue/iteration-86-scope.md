# Iteration 86: one common claim transition

The engine now owns the claim metadata transition used by PostgreSQL and intended
for S3/Turso. Native filtered/ordered selection, payload admission, random token
generation, transaction/receipt publication remain adapter responsibilities.
The pure planner consumes admitted StoredWork, checks raw lease shape, field
knowledge, predecessor version/sequence and native eligibility, and computes
checked version/attempt increments before applying the new lease hash/expiry.
It preserves insertion order, eligible age and field/body knowledge without
copying payload storage. Common expiry calculation rejects overflow.

PostgreSQL calls this planner after its unchanged native priority/progress SQL
selection. Claim hydration preserves raw expired leases; query hydration retains
effective lifecycle normalization. Selected hydration conservatively certifies
only present values (empty absent-name/prefix certificate), without uncharged
copies of potentially large requested-name sets per row. body_loaded reflects
include_body. The claim transition remains metadata-only.

max_attempts remains RetryAt classification, not a new claim cap. Mandatory
shared conformance now reclaims at exact expiry into a fourth attempt beyond the
configured bound, rejects old request replay and checks selected fields/body
omission plus new lease identity. Focused common tests cover pending/expired,
partial knowledge, no payload copy, malformed/stale/overflow authority and
expiry arithmetic. PostgreSQL native/public tests must pass before landing.

The initial broad engine all-target Clippy invocation encounters eight existing
codec/schema/replay test lints outside this change. Production engine library
and PostgreSQL all-target strict checks are run separately; unrelated test code
is not suppressed or changed. S3/Turso activation and durability/performance
qualification remain outstanding.

Final validation passed: three focused common claim tests, fifteen explicitly
invoked PostgreSQL physical storage/administration fixtures, the mandatory
concrete Queue public conformance/reopen fixture including the enhanced expiry
scenario, strict engine production-library and PostgreSQL all-target Clippy,
and diff hygiene. Physical fixtures were run with --ignored and the actual
isolated PostgreSQL test URL; their default skipped listing is not counted.
