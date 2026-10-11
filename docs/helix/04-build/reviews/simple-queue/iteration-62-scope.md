# Iteration 62: public PostgreSQL administration and fresh preview

Base: 2a2ec1ff. CommitGroup appends optional AdministrativeCommit (intent and full
receipt budget). None retains explicit frozen FP3/five-field group and envelope
accounting; Some selects the explicit FP4 six-field projection. Administrative
outcome ordinal 4 and TooLarge rejection ordinal 8 append after all legacy cases.
Independent legacy fixtures retain old literal bytes/hashes and exact boundaries.

QueueStore and the concrete Queue add mandatory PreviewRequest -> PreviewResult.
Commit/preview share borrowed group validation without cloning an envelope.
Preview uses fresh fenced authority, bypasses execution receipt reads and writes,
never persists deltas or calls physical nextval, and returns a distinct outcome.

PostgreSQL owns bounded scratch workers/quota and drains them after all admitted
jobs. Administrative groups use native selection, complete evidence/business
finalization, exact result/publication gates and shared frame assembly before SQL
application. State/new work apply with administrative changes; complete Postcard
receipts persist in the same synchronous WAL transaction. Rejected/oversized
groups have zero effects, sibling order/visibility holds, retained identities are
resolved before fresh loads and changed budget conflicts with the retained FP4.
Any infrastructure failure aborts the whole outer transaction.

All copied input/header accounting is shared queue_memory, including flat
selectors and container headers. New retained receipt scanning skips borrowed
before text/bytes without allocating result maps; admitted owned decoding then
validates result consistency. Shared fallback preflight runs for every group
before admission/effects, and declared/full response bounds are enforced.

Mandatory backend-neutral scenarios cover atomic claimed/state/new-work/control
updates, fresh and retained-ID preview, exact replay/changed budget, success ->
TooLarge -> success, reversed replay, discarded state/new work, complete before
bytes, invalid middle preflight and 5001-row broad selection. A real PostgreSQL
fixture observes the physical record sequence around preview and injects late
SQL failure after two groups, checking rollback of data and both new receipts.

This iteration activates PostgreSQL, not S3/Turso or Snorri. Their same mandatory
suite, consumer cutover, settled comparative benchmarks and remaining legacy
cleanup still must be completed.

Review corrections: retained receipts admit payload size ×4 under the queue fence
before fetching outcome bytes, with structural headers admitted before owned
decoding. Guards remain through output consumption. Raw lease shape is checked
without a clock before business classification, including early CAS and deletion.
