# Astra Ultra review — iteration 20

Model: gpt-6-astra, Ultra. Base: 23d7d352.

APPROVE, scoped to exact initial-to-final uniqueness effects with complete field
authority. Contradictory releases now fail closed and have a no-mutation
regression. Releases, claims, rejected-group isolation, administrative initial
owners, and batched shrinking-upsert probes are consistent.

Parent verification: 86 engine queue tests, strict Clippy, three real PostgreSQL
conformance scenarios, formatting. Reviewer independently verified diff hygiene.

Selective loading, resolved command composition and physical selective writes
remain for later review. This removes a version-based ownership inference before
selectivity; it is not a demonstrated metadata-only violation in the full-image
path.
