# Astra Ultra review — iteration 32

Model: gpt-6-astra, Ultra. Base: 9cc6b95b.

APPROVE frozen ordinary durable compatibility. The production borrowed v3 commit
projection preserves the original five-field order and shapes without cloning
payloads. Domain prefix and streaming hash writer remain unchanged. Empty and rich
historical byte fixtures pin concrete fingerprints; Astra independently verified
both SHA-256 preimages (20 and 169 bytes) with Python.

Every existing GroupOutcome and Rejection ordinal and ReceiptLookup discriminant
has a literal byte fixture. Retained receipt bytes decode/re-encode unchanged,
meet the conservative borrowed header measurement and reject trailing measured
persisted data. No blocking review findings; independent diff hygiene passed.

Verification: 25 core contract tests, 110 engine queue tests, strict core/engine
library Clippy and formatting pass. The public PostgreSQL fixture was actually
executed against isolated PostgreSQL 16.15, passing shared semantic scenarios and
exact retained receipt reopen/replay (7.47 seconds).

No administrative input, result, preview, rejection or FWQP2 is activated. When
extending the live DTO, preserve the literal goldens and replace the derived-group
comparison with a dedicated legacy five-field fixture. New persisted result variants
must extend borrowed measurement in the same iteration. Administrative commits
share the existing commit identity domain while using an explicit new fingerprint
format; ordinary commits keep these v3 fingerprints.
