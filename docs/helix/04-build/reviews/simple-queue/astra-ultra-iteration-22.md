# Astra Ultra review — iteration 22

Model: gpt-6-astra, Ultra. Base: 372c26ee.

APPROVE, scoped to guarded physical reader staging. Production load_group still
passes None, preserving complete reads; the partial-persistence guard remains.
Shared read closure and replacement old-unique-only optimization are consistent.

Review corrections: reserve absent-name certificate multiplicity and temporary
SQL containers before allocation; selected ordinals use only supplied record IDs;
COALESCE preserves corrupt-ordinal payload sizing. Physical regression covers
mixed work/state ordinals, unrelated megabyte attributes, long absent names over
multiple rows, and rollback-only corruption rejected during admission preflight.

Parent verification: 91 engine queue tests, staged native fixture, three native
PostgreSQL conformance scenarios, strict Clippy and formatting. Reviewer verified
git diff --check independently.

Automatic approval review rejected the earlier combined production selective-read
activation and guard removal. That command did not execute. This safer slice
retains the production guards. Selective writer activation and guard removal
remain pending a concrete reviewed implementation and required approval.
