---
ddx:
  id: build-simple-queue-numeric-buckets
  depends_on:
    - build-simple-queue-consumer-integration-binding
    - build-simple-queue-indexed-attributes
  status: accepted
---

# Numeric bucket stats

StatsRequest.buckets optionally declares one numeric dimension alongside ordinary
field group_by dimensions. They return independent aggregates, not a cross product.
Each interval is [lower_inclusive, upper_exclusive); all bounds use one exact
Integer or Float type and finite floats. A declared field must have that type;
an additional field retains its own scalar type. No integer/float coercion occurs.
Bounds must be ordered and intervals pairwise nonoverlapping; numerical sorting
is unnecessary. Up to 50 intervals are permitted, including zero intervals.

QueueStats.buckets returns every interval in declaration order, including zero
counts, followed by the missing bucket. Labels may repeat, including the missing
label: this output is positional. Present values outside all intervals or of a
different scalar type are omitted. Missing means field absence, not opaque JSON
null bytes. Snorri must project engagement_probability using its existing as_f64
mapping: absent, null and nonnumeric payload values omit that numeric attribute.
Canonical JSON equality attributes remain separate from numeric projections.

PostgreSQL computes all bins in one aggregate SQL statement using native indexed
field predicates and complete-name/value checks. The full field name/hash is
bound once and reused across all bins; long names do not multiply by bin count. Totals, ordinary groups and bins
share the existing queue transaction fence; intervening same-queue publications
cannot split the snapshot. No body hydration, per-bin read or missing-count
subtraction is used. Counts retain lifecycle and oldest eligible age semantics.
Memory admission charges incoming dimension containers, the common response
budget and checked per-bin/filter SQL scratch before materialization. The owned
job retains the scratch charge; previous grouping/filter parameters are released
before the bucket filter is regenerated. A byte-saturation test uses a small
response allowance to distinguish scratch admission from ordinary wire/result bytes. Existing request/receipt identities are unchanged.

Finite-bound validation is an explicit tightening relative to API-001's bucket
validator, which did not reject all nonfinite bounds. JSON numeric inputs cannot
represent these, but nonfinite internal requests are now invalid. Empty interval
vectors, duplicate labels and unsorted declarations preserve consumer behavior.

Shared conformance covers boundaries, gaps, mixed types, missing-only output,
repeated labels, unsorted intervals, zero bins, simultaneous field grouping,
foreign-row filtering, invalid overlap/NaN/empty intervals/51 bins, and active then
expired lease counts and lifecycle-filtered query pages. The same scenario runs
under forced hash collisions, using an 80 KiB NUL/UTF-8 field name and all 50 bins.
A saturated-admission regression proves the 51-bin envelope fails validation
before Busy. Effective-lifecycle SQL parameters are cast explicitly to smallint
for both stats and query, fixing the expiry CASE expression type mismatch.

This slice does not activate Snorri visibility. Its QueryVisibilityRequest.as_of
classification must be mapped explicitly before cutover: current API-007 stats
uses the operational clock for lease expiry and eligible_at for due-age selection.
S3/Turso must run this identical oracle before backend qualification; release
performance and full consumer visibility tests remain outstanding.
