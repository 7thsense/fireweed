# Astra Ultra iteration 11 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base 4d4ecec2.
Scope: extract storage-independent insert orchestration and delta/read-set packing.

The engine now owns ordered insert decisions and positional outcome mapping;
physical PostgreSQL retains fencing, loading, immutable identity resolution and
atomic publication. Pending-only replacement, clock-per-item sampling, speculative
unique-owner updates and all original per-item deltas remain unchanged. Borrowing
receipt/old-version data removes unnecessary clones without changing decisions.

Reviewer mechanically compared the moved combine_deltas and load_envelope bodies
with the original PostgreSQL helpers. They are unchanged. Documentation makes
explicit that coalescing physical data never coalesces request receipts, and that
a read-set envelope may contain cross-group duplicate keys and must never execute
as one atomic group.

Root verified both real PostgreSQL runners, shared partial-batch priority oracle,
58 engine queue tests, strict library Clippy, formatting and diff checks. No new
backend, production activation or performance qualification is implied.
