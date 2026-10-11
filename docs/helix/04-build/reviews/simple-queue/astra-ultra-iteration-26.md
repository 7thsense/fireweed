# Astra Ultra review — iteration 26

Model: gpt-6-astra, Ultra. Base: dda7ea11.

APPROVE selective PostgreSQL read/apply activation and DefinitionPin. Explicit
user approval for this exact activation received on 2026-10-10. The shared field
closure feeds production reads; one writer consumes exact field/body commands,
with no Complete-only guard or alternate persistence mode. Claim/read hydration,
whole-group atomicity and existing receipt semantics remain intact. Unique-owner
freshness, affected composite indexes, sparse keys, packed replacement/delete/
reinsert, claimed disposition and administrative body behavior were reviewed.

Astra requested and reviewed pre-planning fixture admission, explicit physical
megabyte-to-empty body replacement followed by metadata Preserve/unchanged ctid,
and correction of superseded scope-document statements. DefinitionPin hashes
exact original persisted bytes, independently of decode/semantic equivalence.

Verification: 94 engine queue tests; three public native PostgreSQL contract,
reopen and legacy scenarios; six isolated native tests including cancellation/
drain/reopen, schema writer fencing, saturated admission and forced collisions;
acknowledged group replay after writer process kill in a fresh verifier; strict
four-library Clippy; formatting and diff hygiene. The shared debug benchmark
smoke passes exact correctness/replay/final-state assertions for 256 source plus
256 emitted next records (~313 acknowledged settled items/sec). This rate is
unqualified: dirty source based on dda7ea11, debug, concurrent local test traffic,
one client/pool two, drain/apply debt unmeasured. No server-crash durability result
or matched S3/Turso performance claim follows.

S3/Turso publication/projector, bounded administrative whole-operation wire,
Snorri concrete binding and release performance qualification remain outstanding.
