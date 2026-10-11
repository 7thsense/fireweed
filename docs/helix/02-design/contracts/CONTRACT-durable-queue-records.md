---
ddx:
  id: contract-durable-queue-records
  type: contract
  authoring:
    home: repo
  links:
    - kind: informed_by
      to: api-queue-claim-commit-contract
---

# Durable queue representations

**Type**: internal protocol and persistence compatibility. **Version**: v1.

## Purpose

Preserve exact definition, replay and resolved mutation semantics through upgrade,
restart and derived projection rebuild. These formats are not consumer transactions.

## Scope and Boundaries

Core/engine own shared codecs and pure semantics; PostgreSQL and S3/Turso own
physical representations. Consumer work and state bodies remain opaque. Data
migration never silently resets definitions, versions, leases or request identities.

## Normative Surface

- Full tenant/queue, collection/name and record key are authoritative. Hashes,
  bounded prefixes, slots and schema ordinals only locate candidates. NUL, slash,
  literal escaped names and long accepted identifiers must isolate and reopen.
- Definition equality is semantic for ensure; the original raw definition bytes
  and cursor secret remain exact durable authority for publication and rebuild.
- Work/state versions and insertion sequences retain the full unsigned 64-bit
  domain. PostgreSQL numeric and Turso ordered eight-byte representations must
  compare identically. Deletion retains version high-water; reinsertion cannot ABA.
- Declared and admitted additional fields retain type and full identity. Native
  equality/ranges compare complete ordered keys after hash/prefix discovery.
  Missing differs from JSON null; no cross-type coercion is introduced.
- Snorri JSON projections use exact canonical serde_json::Value equality,
  including objects, arrays, signed/unsigned numbers, floats and zero signs.
  Full bytes disambiguate digests. Replacement removes obsolete attributes.
- Resolved fields/body intent is Preserve, Replace or Edit as applicable. Partial
  read knowledge is not replacement intent; absent/unloaded values cannot delete
  opaque data. Metadata-only operations do not hydrate or rewrite large bodies.
- Ordinary commit fingerprints retain the original explicit five-field FP3
  projection. Administration uses explicit six-field FP4, sharing the immutable
  commit identity domain. Option wrappers or live DTO extensions cannot change
  old fingerprints. Persisted outcomes/rejections/lookups append ordinals;
  existing ordinals and independently pinned golden bytes/hashes never reorder.
- FWQP1 and FWQP2 encode resolved expected-existence/version/high-water authority,
  final metadata, exact field/body intent and immutable receipts. All commands and
  complete receipts publish atomically; ordered groups observe prior accepted
  effects. Preview retains neither identity nor durable commands.
- FWCR1 retains full keys, original tokens and creation versions in selection
  order. FWQC1 binds that logical receipt to recorded claim time, request identity,
  fingerprint, raw definition pin and metadata-only resolved lease transitions.
  Replay checks current whole-group ownership and returns current versions/expiry;
  creation versions are not equality guards after renewal.
- Checksummed FWQL2 covers the complete record header and payload. Genesis is
  offset zero; publication and claim records follow contiguous offsets and
  nonregressing historical epochs. The acquired writer epoch is distinct from
  the historical last-record epoch. Checksums detect corruption, not authenticated
  provenance or valid-record substitution/removal.
- Legacy FWQL1 is identified explicitly and cannot masquerade as checked FWQL2
  or admit its new claim kind. Any migration requires validated original genesis,
  exact lineage and explicit qualification; writers do not reinterpret old bytes.
- Parsing checks the complete structure and EOF, including rejected/false paths.
  Lengths, headers, duplicate identities and values are admitted before allocation.
  Charge ownership covers aliases, queued/native work and terminal cleanup.

## Precedence and Compatibility

API-007 is observable authority. Internal wire/frame limits cannot weaken its
accepted domain. Mixed fingerprint/receipt formats resolve their original stored
format. Unknown/corrupt authority fails closed; an unsupported old encoding is
not an empty namespace. Independent literal fixtures and native reopen controls
must accompany any format extension or migration.

## Error Semantics

Malformed structure, authority mismatch, duplicate full identities or changed
fixed-width values yield OutcomeUnknown and poison the affected recovery path.
Known pre-publication envelope overflow has zero effects. Resolved per-group
TooLarge rejection and its receipt are atomic with accepted sibling groups;
internal physical capacity failure is not relabeled as a known public rejection
after publication may have occurred. Ambiguous replies resolve original identity.

## Examples

An expired two-item claim replays ClaimExpired as a whole after either member is
completed. A renewed claim replays the same tokens with current versions/expiry.
A priority-only update preserves an omitted one-MiB body and property value.
A mixed state/update/new-work group with a late failure leaves no partial effect.
Each example must agree on PostgreSQL, native projection apply and cold rebuild.

## JSON equality attributes

FWJ1 is an opaque Bytes encoding for indexed JSON equality, not a workflow-body envelope. Numeric model 0 (NumericVariants) distinguishes integers from floats, preserves full signed/unsigned range and normalizes floating +/-zero. Model 1 (LiteralRepresentations) retains exact numeric spellings under arbitrary_precision. Object keys sort by full UTF-8 bytes; arrays retain order; null, string "null" and absent fields differ. This is not JSON range ordering.

RFC6901 escaping applies to the literal top-level key only: tilde becomes ~0 and slash becomes ~1. Namespace remains separate. No Unicode normalization or path traversal changes identity. Physical indexes compare complete names and bytes as well as digests.

The consumer persists the complete JsonEqualityEncoding namespace/schema pin and checks it before BOTH queries and writes. Per-value headers alone cannot detect query misses after feature drift. A changed numeric model requires an explicit checked attribute rewrite before activation; old values cannot be silently reinterpreted. Both adapters use identical source/features/encoding.

The iterative encoder bounds output and pre-admits traversal/sorting scratch with an ancestor/sibling pending-minimum budget. Input JSON remains caller-owned. Corpus tests compare encoded equality to actual Value equality under default and arbitrary_precision+preserve_order features, including extrema, exponent spellings, signed zero, arbitrary literal keys and deeply nested values. Compatibility goldens and byte-boundary controls are mandatory.

Workflow state_version advances on transitions and await/result consumption; dispatch, timeout and timer bookkeeping must not invent a workflow transition.

## Timestamp and ordered-index migration

Queue timestamps are exact signed i64 epoch nanoseconds; durations are unsigned nanoseconds. Convert source timestamps through checked i128 arithmetic into i64, and invert with Euclidean division/remainder. No rounding, saturation or clamping is allowed. Identify and explicitly resolve out-of-range legacy timestamps before consumer activation; the source timestamp type may cover a wider interval than storage. Historical timestamp-unit fingerprint v2 remains a migration compatibility obligation only; current ordinary commit FP3 and administrative FP4 remain authoritative. Ordered scalar codec v1 is separate from request/postcard and length-framed address encodings.

Ordered indexes use presence/type tags, signed sign-bit transformation, finite IEEE float ordering and escaped NUL-terminated complete strings/bytes. Missing sorts first ascending and reverses descending. Detect and resolve legacy unique float tuples that distinguished -0 from +0 before normalized ordered-index activation. Complete prospective unique tuples are authoritative, including atomic swaps/rekeys; release only exact changed old tuples and claim exact changed new tuples under the same protected queue boundary. Unchanged tuples retain ownership even when their record version changes. Missing components preserve NULLS DISTINCT semantics.

Bounded prefixes and digests are discovery probes only. Complete storage-side comparisons decide predicates, ordering, cursors and uniqueness; collisions cannot demote uniqueness or lease unrelated work. Prefix-tie sorting/rechecks require bounded admission/spill. Pathological equal-prefix ranges may scan a bucket, but must still pass the same 10M and numeric-progress qualification or the overflow index must be redesigned. No client filter or narrowed predicate substitutes for that gate.
