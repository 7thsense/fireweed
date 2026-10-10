---
ddx:
  id: build-simple-queue-index-compatibility
  depends_on:
    - api-queue-claim-commit-contract
    - build-simple-queue-planner-admission
  status: accepted
---

# Indexed selection and Snorri compatibility, iteration 4

Actual Snorri supplies unrestricted payload-derived workflow/run/target/status
strings, long await/idempotency keys, and the load-bearing unique by_run_target_key
index. The new binding must not silently add 256-byte keys, 1024-byte values,
length-prefixed tuple ordering, or lose uniqueness. Earlier provisional identifier
caps are removed. Common request/response admission still bounds resources.

Queue collection definitions name unique composite indexes. Their complete tuple
is unique when all components are present (existing PostgreSQL NULLS DISTINCT).
State/work key namespaces remain separate. Patches/rekeys enforce final resulting
values, not only individual patch values. Shared planning releases all affected
old unique owners, then validates the final candidate tuples, allowing legitimate
atomic swaps. Storage loads complete prospective tuple ownership/nonexistence
under the same protected queue boundary before planning. Missing authority fails
closed; conflicting owners reject the entire group without effects.

Ordered keys use a separate v1 memcomparable codec: presence marker, declared type,
signed integer sign-bit transform, ordered IEEE finite float bits, and escaped
NUL-terminated UTF-8/byte strings. Missing is first ascending, reversed descending.
The codec preserves prefixes, Unicode, embedded zeroes and complete composite
lexical order. Request fingerprint postcard and ESF length-framed composite keys
are not substituted for this ordering representation.

## PostgreSQL physical overflow policy

Use compact shared queue/field/index IDs and shared tables, not per-queue DDL.
Complete logical keys and values remain BYTEA/off-row data where necessary.
Bounded prefix and full SHA-256 indexes discover equality/range candidates; full
byte comparisons are authoritative. Hash collisions are not unique collisions:
under the queue lock, disambiguate complete values and permit distinct originals.
Unique ownership uses the complete tuple; never truncate, omit, or demote it.

Ranges use conservative prefix bounds plus full storage-side comparisons. Query
ordering/cursors compare complete memcomparable keys, not prefixes; prefix ties
require full-value sorting/recheck, with bounded memory/spill. This may cost more
for pathological equal-prefix inputs; measure it explicitly rather than claiming
O(log n) behavior for unbounded values. No unrelated item is leased, no whole
queue hydration or client predicate filter occurs, and no silently narrowed
predicate shape is introduced. Qualify actual PostgreSQL page/index entry budgets
for bounded probes before activation. S3/Turso uses identical logical constraints.

## Exact timestamp storage

Timestamps are exact signed epoch nanoseconds; durations are unsigned nanoseconds, matching actual Snorri
ExecutionTime, time ranges and due-at comparisons. Conversion uses an i128
intermediate and checked i64; inverse uses Euclidean division/remainder. No
rounding or saturation is allowed. The supported storage interval is i64 nanos
(about 1677–2262), narrower than the source timestamp type; out-of-range legacy
values must be identified and resolved at migration, never silently clamped.
Request fingerprint domain is explicitly v2 because unit semantics changed before
activation; ordered scalar codec remains v1. Existing old Fireweed log readers
remain separate and required for migration.

Pathological selective ranges sharing a prefix can examine a full bucket. This
is not by itself proof of bounded indexed performance: such cases must pass the
same 10M and numeric-progress gates before activation, or the overflow index must
be redesigned. Exact storage-side predicates and bounded spill do not waive that
performance obligation.

## Required proof before adapter activation

Boundary tests include 1025-byte targets, 3x400-byte composite strings, long
await/client keys, Unicode/NUL, prefix collisions and forced hash collisions;
comparator/property and ascending/descending cursor equivalence; uniqueness,
rekey and atomic swap; missing unique authority; legacy fixtures reopened without
new length rejection. Detect and resolve any legacy unique-float tuples that
distinguished -0 from +0 before activating normalized ordered keys. SQL uncertain transactions invalidate speculative snapshots
before later groups. Existing serialization/legacy migration gates still apply.
