# Astra Ultra iteration 8 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base 15ae4d84.
Scope: mandatory shared acknowledged writer-process-loss assertions and native
PostgreSQL subprocess fixture. No backend behavior or production activation changes.

The writer acknowledges settlement, state and emitted work before publishing a
control payload. The parent kills it without shutdown and invokes a fresh verifier
process. Shared assertions compare retained and replayed receipts with the original
acknowledged receipt, check settled source work, state value and emitted work, and
verify conflicting payloads cannot replace the durable receipt.

Review found and resolved two fixture gaps: readiness is now an atomic rename of
a completed staging file, and the original acknowledged receipts travel with the
commit payload rather than being inferred from post-loss reads. Child lifetime
cleanup and bounded polling keep failures from leaving a running writer.

Root verified the real subprocess test against isolated PostgreSQL 16.15,
strict Clippy for the affected test, formatting and diff checks. Approval covers
acknowledged writer-process loss only. Abrupt database/server crash, S3/Turso parity,
legacy migration, Snorri cutover, progress and matched performance remain required.
