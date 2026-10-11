# Iteration 90: checksummed atomic claim projection

FWQL2 appends kind2 for the admitted FWQC1 wrapper. FWQL1 kind2 remains forbidden;
existing genesis/publication bytes and goldens remain unchanged. Full record
checksum covers the new kind, timing, commands and receipt. ColdQueueHistory
checks admitted full-key binding, original definition pin and unchanged insertion
sequence for claim records, including skipped cached-prefix records.

Binding now exposes one immutable admitted borrowed full-key index used by the
native applicator, rather than rescanning selection order for each sorted command.
Strict unique FWQP1 commands plus equal counts/full-key lookups prove the
bijection. Map allocations precede their guard in drop order.

Stock-Turso claims reuse the existing publication authority checks, command
applicator, immediate transaction and rollback/panic handling. Bounded scalar
queries load raw predecessor metadata, then common plan_claim reconstructs the
expected transition using recorded claim time rather than a new clock/selector.
Priority, availability, age/order, attempts+1, version, raw expiry/hash all match
physical authority. Reusing an expired predecessor token is rejected by the
common planner. No field/body payload is hydrated or rewritten.

Lease changes, one immutable domain1 claim-v3 receipt containing fixed FWCR1
bytes, and the external cursor publish in that same transaction. Empty claims
publish a durable empty receipt with unchanged insertion sequence. Existing or
expired identity conflicts fail and roll back all effects.

Twelve physical projector tests pass, including non-key priority selection,
empty claim, one-MiB opaque field/body preservation under one-MiB logical
admission, exact-expiry reclaim, late preserved-metadata/stale-predecessor and
receipt failure rollback, full unchanged receipt/fingerprint cold reopen.
Engine wrapper tests reject legacy kind2 and every single-byte checksum change;
common claim tests reject reused expired tokens. Strict engine/Turso checks and
full regression suites are required before landing.

This qualifies internal claim creation/application only. Live current-record
replay (renewal/current version/partial settlement), native filtered selection,
QueueStore factory, S3/provider qualification, cached-row integrity audit,
bounded full-manifest storage and settled matched benchmarks remain outstanding.

Astra's final evidence controls pass: actual genesis/seed/claim cold history,
valid-checksum bad pin/sequence/token binding, unchanged offset and sticky poison;
exact empty FWCR1 bytes, fingerprint and format after native reopen. The stale
fixture's version/highwater are consistent so failure occurs at physical apply.
A system /tmp quota exhaustion interrupted a rerun; relocating task fixtures via
TMPDIR to target/api007-test-tmp completed successfully. Final checks pass:
twelve physical projector tests, all 472 engine tests, strict engine and
stock-Turso local all-target Clippy, and diff hygiene.
