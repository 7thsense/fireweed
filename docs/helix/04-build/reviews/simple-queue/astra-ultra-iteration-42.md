# Astra Ultra review — iteration 42

Model: gpt-6-astra, Ultra. Base: c91bc2cc.

APPROVE private accepted changed-identity resolution after the exact predecessor
correction. Initial review found highwater-only lookup could confuse live rows
with retained tombstones. Lookup now checks present-record version against
positive highwater, stored highwater and physical active versus frozen presence.
Never-seen absence remains distinct from retained inactive identity.

Paired full-identity lookup, duplicate/cardinality/ordinal checks, precharged maps,
more-than-512 transport coverage, collection separation and no sequence allocation
are sound. The actual writer fixture preserves IDs for update/delete and tests
same-version wrong-presence failures in both directions. It rolls back its
speculative publication under a savepoint.

Verification: final physical fixture passed (0.36 seconds), strict PostgreSQL
library/tests Clippy, formatting and diff checks passed. Astra independently
checked the diff and reread source, tests and scope documentation.

Approval covers this private prerequisite only. Public administration, preview,
bounded spool execution, FWQP2 and S3/Turso qualification remain pending.
