# Astra Ultra iteration 9 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base 854a9e00.
Scope: batched insert-vector unique authority and constructor schema lock ordering.

The adapter loads all prospective complete tuples once before per-item planning;
SQL probe deduplication does not deduplicate positional outcomes. Accepted earlier
rekeys release ownership, rejected siblings leave it intact, and immutable vector
replay returns the original outcomes. Review found missing old-field/index release
amplification for shrinking upserts; shared accounting now includes loaded existing
records addressed through insert keys. A budget regression and mandatory physical
large-old/small-new/reuse scenario verify that correction.

Final shared runs exposed a PostgreSQL DDL/write deadlock during reopening.
Constructor DDL now first fences queues with ACCESS EXCLUSIVE while retaining the
schema advisory lock. All current operation transactions acquire a queues relation
lock before dependent tables, so this drains writers before DDL can invert their
lock order. A deterministic pg_locks regression forces the writer/open interleaving.
The global opening barrier and absence of an online migration claim are documented.

Root verified both shared PostgreSQL runners, acknowledged subprocess loss,
shrinking budget and schema lock regressions, strict library Clippy, formatting and
diff checks. No performance qualification is claimed. S3/Turso parity, matched
benchmarks, legacy migration and consumer cutover remain required.
