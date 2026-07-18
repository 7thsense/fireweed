---
ddx:
  id: api-hot-projection-query-surface
  depends_on:
    - prd
    - api-native-client-interface
    - adr-queue-as-shard-unit-and-projection-families
    - adr-typed-payload-schemas-and-indexes-via-axon-esf
  review:
    self_hash: 2b943bd1b915099f9b16bc28b2e0640beb2b85df9c54b936ebbf6380ca0578f1
    deps:
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
      adr-typed-payload-schemas-and-indexes-via-axon-esf: bc29e64f6e6f89932496a4803282d3e388bea665db6c526a92ba17fe49422347
      api-native-client-interface: 852a753af558d8b8a21e4a86e87915b14c030fefcb4a27473bcbb08cfe044580
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
    reviewed_at: "2026-07-18T02:36:05Z"
---

# Contract

**Contract ID**: API-004
**Type**: native query contract (transport-neutral)
**Version**: v1 (draft)
**Status**: **Descoped (draft, not built).** The implementing work item
(`pqueue-630dbeaa`, "Add hot indexed projection query surface for Snorri workflow visibility") was
**cancelled**; this contract is retained as a design record but is **not part of the shipped
surface**. The shipped read surface remains API-001 (`peek`, `claimed`, `live_items`, `metrics`, and
exact typed-index lookup via `IndexQueryPort`). Re-scope or re-open via a new tracked bead before
building against this contract.
**Related**: PRD (FR-44..47 Seventh Sense validation), API-001 (native client interface, claimed-item
shape, Eligibility Precedence), ADR-008 (queue as shard unit), ADR-011 (typed indexes via `axon-esf`),
epic pqueue-45e13e4d (ship Snorri hot projection query substrate)

## Purpose

This contract defines the **hot projection query substrate**: a domain-neutral set of typed,
indexed-record query capabilities that let a caller (Snorri, or any other embedder) read, group,
paginate, and safely mutate hot (pre-archival) queue-resident and side/projection records without
pqueue interpreting the caller's business semantics.

It exists because API-001's read surface (`peek`, `claimed`, `live_items`, `metrics`, and exact typed
index lookup via `IndexQueryPort`) is sufficient for claim-oriented work but insufficient for
operational reporting over a hot queue: range scans over an ordered index, grouped/bucketed
aggregation, stable cursor pagination, and bounded (predicate-scoped, version-fenced) mutation outside
the claim/finalize lifecycle.

pqueue MUST remain domain-neutral. This contract defines the generic substrate only. Snorri-specific
workflow vocabulary (job/run/instance semantics, recycling policy, open-rate filters, engagement
classification) is **out of scope** and MUST be built by Snorri on top of these primitives, not folded
into pqueue.

## Scope and Boundaries

- In scope: capability names and their advertised availability per backend; the typed indexed record
  shape; range-scan, grouping, cursor-pagination, bounded-mutation, and claim-by-query operation
  shapes; consistency/watermark rules; cursor invalidation; declared-bucket segmentation including null
  handling; aggregate limits; side/projection record semantics (declared, not implemented, in this
  contract).
- Out of scope: Rust API implementation, backend query implementation, and any Snorri/Cayce/MCP-facing
  API. Those are downstream build beads (epic pqueue-45e13e4d) and MUST cite this contract rather than
  redefine it.
- Out of scope: arbitrary SQL, JSONPath, or other caller-supplied expression execution (see Non-Goals).
- Owning system or team: pqueue core.

## Example Fixture (illustrative, non-normative)

The six scheduled-action records from the superseded task pqueue-630dbeaa are retained here as a
worked example of a typed indexed record populated by an embedder. They are **not** a pqueue schema;
`scheduled_action_projection`-style field names are Snorri/7th-Sense domain vocabulary, shown only so
the capability definitions below have a concrete referent.

```json
[
  {"action_id":"act_001","target_key":"contact:001","scheduled_at":"2026-07-02T14:05:00Z","status":"scheduled","action_type":"message.send","scheduler_algorithm":"personalized","engagement_probability":0.0825,"engagement_threshold":0.10,"suppressed_by_recycling":true,"is_enrolled_using_open_rate_filter":false},
  {"action_id":"act_002","target_key":"contact:002","scheduled_at":"2026-07-02T14:37:00Z","status":"scheduled","action_type":"message.send","scheduler_algorithm":"personalized","engagement_probability":0.1280,"engagement_threshold":0.10,"suppressed_by_recycling":false,"is_enrolled_using_open_rate_filter":true},
  {"action_id":"act_003","target_key":"contact:003","scheduled_at":"2026-07-02T15:02:00Z","status":"suppressed","action_type":"message.send","scheduler_algorithm":"randomized","engagement_probability":0.0000,"engagement_threshold":0.10,"suppressed_by_recycling":true,"is_enrolled_using_open_rate_filter":false},
  {"action_id":"act_004","target_key":"contact:004","scheduled_at":"2026-07-02T15:45:00Z","status":"scheduled","action_type":"message.send","scheduler_algorithm":"randomized","engagement_probability":null,"engagement_threshold":0.10,"suppressed_by_recycling":false,"is_enrolled_using_open_rate_filter":true},
  {"action_id":"act_005","target_key":"contact:005","scheduled_at":"2026-07-03T09:15:00Z","status":"failed","action_type":"message.send","scheduler_algorithm":"personalized","engagement_probability":0.4510,"engagement_threshold":0.10,"suppressed_by_recycling":false,"is_enrolled_using_open_rate_filter":true},
  {"action_id":"act_006","target_key":"contact:006","scheduled_at":"2026-07-03T09:50:00Z","status":"scheduled","action_type":"subject.mutation","scheduler_algorithm":"personalized","engagement_probability":0.9100,"engagement_threshold":0.10,"suppressed_by_recycling":false,"is_enrolled_using_open_rate_filter":true}
]
```

Implementation beads under epic pqueue-45e13e4d extend this fixture in
`crates/pqueue/tests/hot_projection_queries.rs`; this contract does not itself define that Rust type.

## Normative Surface

Use MUST, MUST NOT, MAY, and SHOULD intentionally.

### Typed Indexed Record Shape

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| indexed record | queue item or side/projection record | yes | MUST carry an ESF-typed entity document (ADR-011 `EntitySchemaDocument`) whose declared `typed_indexes` (`QueueIndex`, ADR-011) back every range scan, grouping, and bucket query in this contract. | Same typed-index vocabulary as API-001/ADR-011; this contract adds query shapes over it, not a new index model. |
| declared index | `QueueIndex { name, declaration }` | yes | The caller declares which fields compose an index at `CreateQueue` time (ADR-011). A range scan, order-by, or group-by field used by a query MUST be a field of some declared index; pqueue MUST reject a query naming a field outside every declared index with `unindexed-field`. | `action_id`-style caller identifiers are NOT automatically indexed fields; if a caller wants to filter/sort by such a field it MUST be a member of a declared index. |
| record identity | internal item id | yes | Every indexed record MUST carry pqueue's internal item id (`item_id` for queue items; an equivalent stable id for side/projection records), used only as the ultimate ordering tie-break (see Cursor Ordering) and never required as a caller-visible filter/sort field. | Distinct from `client_item_key`/business keys. |

### Query Capability Names

pqueue advertises a fixed, versioned set of hot-projection query capabilities per queue/backend. A
backend or queue that does not advertise a capability MUST reject a request for it with a structured
`capability-unavailable` error naming the missing capability; it MUST NOT silently degrade to a full
scan or partial result.

| Capability | Meaning | v1 status |
|------------|---------|-----------|
| `range_scan` | Ordered scan over a declared index with inclusive/exclusive bounds on its fields, returning cursor-paginated rows (see Range Scan). | Required for every backend that advertises this contract. |
| `grouped_aggregate` | Grouping and counting rows over a declared index prefix, including time-bucketed grouping (see Grouping / Aggregation). | Required. |
| `declared_bucket_segment` | Segmenting rows into caller-declared numeric buckets over one declared numeric-indexed field, including the null/no-activity bucket (see Declared Numeric Buckets). | Required. |
| `bounded_mutation` | Scanning and mutating a bounded, predicate-scoped set of records with per-record CAS (see Bounded Mutation). | Required. |
| `claim_by_query` | Claiming due work selected by a range-scan predicate instead of the default priority-ordered claim (see Claim By Query). | Required. |
| `side_record_query` | Querying non-claimable side/projection records (see Side/Projection Records). | **Deferred beyond this epic (pqueue-45e13e4d).** Every backend MUST advertise this capability as unavailable and MUST return `capability-unavailable("side_record_query")` for any request naming it. |

A backend that advertises any of `range_scan`, `grouped_aggregate`, `declared_bucket_segment`,
`bounded_mutation`, or `claim_by_query` MUST advertise all of them; this contract does not define a
partial-capability backend for those five. `side_record_query` is independently gated per the row
above.

### Consistency and Watermark Rules

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `as_of` (implicit read watermark) | opaque, monotonic per queue | yes | Every hot-projection query reads at an implicit watermark: the owner's locally-applied projection position at query-selection start. A query MUST NOT observe a mutation whose committing command has not yet applied to that local projection, and MUST observe every mutation the owner had already applied before selection started (read-your-writes on the owner). | Same single-owner-projection model as API-001 claims (ADR-008); no cross-shard fan-out. |
| staleness | none (by construction) | yes | Because the queue is the unit of sharding (ADR-008), a hot-projection query against one queue is always served from that queue's single owner projection; there is no replica-lag or multi-shard-merge staleness to bound. | Distinct from any future read-replica design, which is out of scope here. |
| grouped/bucketed counts | approximate under concurrent writes | may | `grouped_aggregate` and `declared_bucket_segment` results MAY reflect a torn read across concurrently-committing rows (a row may be counted in its pre- or post-mutation bucket, never both and never neither, for any single group-by/bucket dimension). Detail-page `range_scan` rows MUST NOT be torn: each returned row is a single committed record version. | Aggregate torn-read tolerance mirrors API-001's existing "counts MAY lag" rule for `pqueue_group_summary`. |

### Range Scan

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `RangeScan` | operation | yes (`range_scan`) | MUST scan a declared index in index order, applying equality filters on a leading prefix of the index's fields and an inclusive/exclusive range on the next field, returning cursor-paginated rows. | Backs "detail page" style queries (filter by leading fields, range on a trailing timestamp/numeric field). |
| `RangeScan.filters[]` | array of `{field, op, value}` | yes | `field` MUST be a member of the declared index named by the query (or omitted to let pqueue select a matching declared index). `op` MUST be one of `eq`, `gte`, `gt`, `lte`, `lt`. Fields other than the last non-equality field MUST use `eq`. | Leading-prefix-equality-then-range shape, matching ESF compound index leftmost-prefix semantics (ADR-011). |
| `RangeScan.order_by[]` | array of `{field, direction}` | yes | Every `field` MUST be a member of the same declared index used for the scan, in an order consistent with that index's declared field order (or its exact reverse). pqueue MUST NOT support ordering by a field outside the declared index. | See Cursor Ordering for the mandatory implicit tie-break. |
| `RangeScan.page_size` | integer | yes | MUST be greater than 0 and MUST NOT exceed the deployment's max page size. | Bounds one page's row count. |
| `RangeScan.cursor` | opaque token | no | Absent on the first page. MUST encode the last returned row's `order_by` field values plus its internal item id. | See Cursor Ordering / Cursor Invalidation. |
| `RangeScan.response.rows[]` | array | yes | MUST preserve `order_by` order. Each row MUST include every field of the record's declared index used by the scan and MAY include the record's other declared/indexed fields; it MUST NOT include undeclared/unindexed fields (this is an index query, not a full-record fetch). | Callers needing the full record fetch it via `BatchGetLiveItems` (API-001) or an equivalent side-record read by the record's own key. |
| `RangeScan.response.next_cursor` | opaque token / absent | yes | Present when more rows may exist; absent when the scan is exhausted. | Standard cursor-pagination shape. |

### Cursor Ordering

Every `RangeScan.order_by` (and every `grouped_aggregate` intra-group ordering) is a caller-declared
ordering over fields of one declared index, **with the record's internal item id implicitly appended
as the final ordering field** (ascending) whenever the declared fields do not already resolve to a
total order. Callers MUST NOT be required to name the internal item id explicitly, and a declared
sort field that is not itself a member of the queried index (for example, an opaque caller identifier
such as `action_id` when the canonical range index is `(tenant_id, run_id, scheduled_at)`) MUST be
rejected with `unindexed-field` rather than silently accepted as a tie-break. This closes the
ambiguity the superseded pqueue-630dbeaa fixture left open by mixing `scheduled_at` with `action_id`
sort behavior without naming a tie-break field.

### Cursor Invalidation

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| cursor validity | opaque token semantics | yes | A cursor issued by page N MUST resolve page N+1 to rows strictly after the last returned `(order_by..., item_id)` tuple, regardless of insertions/deletions/mutations elsewhere in the index. A row already returned on an earlier page MUST NOT reappear (no duplication); a row that existed and matched the filter at scan start MUST NOT be silently skipped (no loss) solely because of a later, unrelated mutation. | Matches the pqueue-630dbeaa requirement that later inserts must not duplicate or skip already-visible rows. |
| explicit invalidation | error | yes | If a cursor's anchor row was deleted or mutated such that it would no longer match the original scan's filters in a way that makes correct resumption impossible (for example, a range-narrowing update to the anchor row's ordering field), pqueue MUST return a structured, explicitly retryable `cursor-invalidated` error rather than returning an incorrect page. | "Retryable" means: the caller MAY restart pagination from an empty cursor and MAY safely deduplicate by item id across the restarted sequence. |

### Grouping / Aggregation

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `GroupedAggregate` | operation | yes (`grouped_aggregate`) | MUST group filtered rows by one or more declared-index fields (or a caller-declared time-bucket function of a declared datetime-indexed field, e.g. hour-of, day-of) and return a count per group. | Backs hourly-distribution and status/algorithm/recycling-bucket style queries. |
| `GroupedAggregate.filters[]` | array | yes | Same shape and leading-prefix rule as `RangeScan.filters[]`. | |
| `GroupedAggregate.group_by[]` | array of `{field}` or `{field, time_bucket}` | yes | Every `field` MUST be a member of the declared index the filters resolve to. `time_bucket` (when present) MUST be one of a fixed enum (`hour`, `day`) applied to a datetime-typed indexed field. | `time_bucket` is the only caller-declared transform allowed on a group-by field; it is not general expression evaluation (see Non-Goals). |
| `GroupedAggregate.response.groups[]` | array | yes | MUST report each distinct group key tuple and its row count. Empty groups (a combination with zero matching rows) MUST NOT be reported. | Matches the pqueue-630dbeaa hourly/status and recycling-preview expected-output shape. |
| `GroupedAggregate.limits.max_groups` | integer | yes | The response MUST reject with `aggregate-too-large` rather than silently truncate when the number of distinct groups produced by the query would exceed the deployment's configured `max_groups` limit. | See Aggregate Limits. |

### Declared Numeric Buckets

Numeric segmentation (for example, the engagement-probability example fixture) is entirely
**caller-declared**, not a fixed pqueue bucket ladder. A `declared_bucket_segment` query supplies an
explicit, ordered list of half-open or closed intervals over one declared numeric-indexed field, each
with a caller-chosen label, plus a null-handling rule.

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `DeclaredBucketSegment.field` | string | yes | MUST be a member of a declared numeric (`Float` or `Integer`) index. | |
| `DeclaredBucketSegment.buckets[]` | array of `{label, exact?, gt?, gte?, lt?, lte?}` | yes | Each bucket MUST be either an exact-value match (`exact`) or a half-open/closed range (`gt`/`gte` lower bound with `lt`/`lte` upper bound). Buckets MUST NOT overlap; pqueue MUST reject an overlapping bucket set with `invalid-request`. A row not matched by any declared bucket and not null is excluded from every bucket's count (not an error). | The canonical conformance fixture (below) is the worked example. |
| `DeclaredBucketSegment.null_bucket_label` | string | yes | Names the bucket that reports rows where the declared field is null or absent from the record. | Required, not optional — a query with numeric buckets and no `null_bucket_label` MUST fail `invalid-request`, so null handling is never silently omitted. |
| null accounting | counting rule | yes | The `null_bucket_label` count MUST equal (rows matching the query's base filters) minus (rows counted in any non-null declared bucket) minus (rows whose field value is non-null but unmatched by every declared bucket). Equivalently: it is computed as a base-filter row count minus indexed non-null-bucketed rows, **not** via a null-sentinel index key. | Resolves REQUIRED DECISION 1: a record with a null/missing indexed field is absent from the field's own index (`index_key` returns `None`, `crates/pqueue-projection/src/lib.rs`); pqueue answers "how many have no value" by subtracting indexed-and-bucketed counts from a base-filter count over the record set, not by minting a synthetic null key into the index. |

**Canonical conformance fixture** (over `engagement_probability`, informed by the pqueue-630dbeaa
example fixture, with the bucket ladder made explicit and caller-declared rather than a fixed 1%-wide
ladder):

| Bucket | Rule | Label |
|--------|------|-------|
| 1 | `exact: 0` | `"0%"` |
| 2 | `gt: 0.08, lte: 0.10` | `"8.01-10%"` |
| 3 | `gt: 0.10, lte: 0.15` | `"10.01-15%"` |
| 4 | `gt: 0.45, lte: 0.50` | `"45.01-50%"` |
| — | null / missing | `"no-activity"` |

Against the fixture in "Example Fixture" above (filtered to `action_type = message.send`, excluding
`act_006`), this MUST report: `"0%" = 1` (act_003), `"8.01-10%" = 1` (act_001), `"10.01-15%" = 1`
(act_002), `"45.01-50%" = 1` (act_005), `"no-activity" = 1` (act_004).

### Aggregate Limits

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `max_groups` | deployment/queue configuration | yes | Bounds distinct groups returned by one `GroupedAggregate` (see above). | |
| `max_buckets` | deployment/queue configuration | yes | Bounds the number of declared buckets in one `DeclaredBucketSegment` request; a request exceeding it MUST fail `invalid-request` before execution. | Prevents unbounded caller-declared bucket lists from becoming an unindexed full-scan cost surface. |
| `max_scan_rows_per_page` | deployment/queue configuration | yes | Every capability in this contract that scans an index (`RangeScan`, `GroupedAggregate`, `DeclaredBucketSegment`, `BoundedMutation`) MUST bound the rows examined per internal execution step by this limit, using the index (not a full scan) to seek past skipped ranges. | Keeps aggregate/bucket/mutation cost proportional to matched rows, not queue size. |

### Bounded Mutation

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `BoundedMutation` | operation | yes (`bounded_mutation`) | MUST scan a declared-index predicate (same filter shape as `RangeScan.filters[]`) and apply a caller-specified field update to every matching record, with per-record optimistic concurrency. | Backs "safe recycling rule update" style operations. |
| `BoundedMutation.filters[]` | array | yes | Same leading-prefix rule as `RangeScan.filters[]`. | Scopes which records are touched; this is not an arbitrary predicate (see Non-Goals). |
| `BoundedMutation.set_fields` | map field -> value | yes | MUST replace only the named fields on each matched record; MUST NOT be interpreted by pqueue beyond type validation against the record's entity schema. | Same "caller data, uninterpreted" posture as API-001 `BatchUpdate`. |
| `BoundedMutation.per_record_cas` | implicit | yes | Each matched record's mutation MUST be conditioned on the record's version observed at match time (equivalent to API-001's `expected_item_version`). If a concurrent claim, finalize, or other mutation changes a record's version between match and apply, that record's update MUST fail per-record `conflict` (not abort the whole batch) and MUST NOT be silently retried by pqueue. | Preserves the "must reject or retry cleanly on concurrent claim/commit" requirement without inventing whole-batch atomicity `BoundedMutation` does not claim. |
| `BoundedMutation.response.results[]` | array | yes | MUST report one outcome (`updated`, `conflict`, or `not_found`) per matched record at scan time. | Best-effort per record, matching API-001's batch-result posture. |
| leased records | interaction rule | yes | `BoundedMutation` MUST NOT bypass an active lease: a record with an active lease follows the same `conflict` rule as any other version mismatch caused by concurrent activity, per the `per_record_cas` rule above. It MUST NOT use a separate leased-record code path. | Keeps one conflict semantics for "something else touched this record concurrently." |

### Claim By Query

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `ClaimByQuery` | operation | yes (`claim_by_query`) | MUST claim due records selected by a declared-index range/equality predicate, in the order of a declared index (typically a scheduled-time field), instead of the queue's default priority order. | Backs "claim due scheduled work" style operations. |
| `ClaimByQuery.filters[]` | array | yes | Same leading-prefix rule as `RangeScan.filters[]`; MUST resolve to a declared index. | Example: `tenant_id`/`run_id`/`action_type`/`status` equality with `scheduled_at <= as_of`. |
| `ClaimByQuery.order_by` | `{field, direction}` | yes | MUST be a field of the same declared index as `filters[]`, subject to the same implicit item-id tie-break as `RangeScan` (Cursor Ordering). | |
| `ClaimByQuery.max_items`, `.lease_duration_ms`, `.worker_id`, `.request_id` | as API-001 `BatchClaim` | yes | MUST carry the same meaning, idempotency, and lease semantics as API-001 `BatchClaim`'s equivalent fields. | `ClaimByQuery` is an alternate **selection** path into the same claim/lease/finalize lifecycle, not a parallel lifecycle. |
| `ClaimByQuery.response` | as API-001 `BatchClaim.response` | yes | MUST return the same Claimed Item Response Shape as API-001 (`item_id`, `client_item_key`, `item_version`, `lease_token`, `lease_expires_at`, conditional fields). Returned lease refs MUST be accepted by the existing `BatchRenewLeases`/`BatchFinalize` commit/finalize path unchanged. | No second claimed-item shape; no second finalize path. |
| eligibility interaction | rule | yes | `ClaimByQuery` MUST still honor Eligibility Precedence (API-001): a record excluded by lease state, gates, or metadata blockers MUST NOT be claimed even if it matches the query filters. | One Eligibility Precedence definition, referenced not restated (API-001). |

### Side/Projection Records (Deferred)

A **side/projection record** is a non-claimable record that carries typed indexed fields and
participates in `RangeScan`/`GroupedAggregate`/`DeclaredBucketSegment` queries but has no lifecycle
state, no lease, and is never returned by `BatchClaim`/`ClaimByQuery`. Its purpose is to let a caller
maintain query-only derived or reference rows (for example, a per-run summary row) alongside claimable
queue items in the same typed-index space.

This contract defines the **shape and the capability flag only**:

| Element | Type / Shape | Required | Rules | Notes |
|---------|--------------|----------|-------|-------|
| `side_record_query` | capability flag | yes (declared) | Every backend MUST advertise this capability as unavailable in v1 and MUST reject any request that names a side/projection record target with a structured `capability-unavailable("side_record_query")` error. | See REQUIRED DECISION 4. |
| side/projection record storage | (unimplemented) | n/a | No bead in epic pqueue-45e13e4d implements side-record storage, write path, or query execution. A future epic MUST scope that work; it MUST NOT be added as an incidental extension of a `range_scan`/`bounded_mutation`/`claim_by_query` implementation bead. | Matches the epic's explicit SIDE/PROJECTION RECORDS (DEFERRED) note. |

## Non-Goals

- **No Snorri-specific vocabulary in pqueue.** Job/run/instance semantics, recycling policy, open-rate
  filter membership, and engagement classification are Snorri/Cayce policy and MUST be implemented
  above this contract, not inside pqueue.
- **No Niflheim/ClickHouse archival analytics.** Long-retention, cold, or cross-run analytical queries
  are out of scope; this contract governs only hot (pre-archival), queue-resident and side/projection
  records.
- **No arbitrary SQL, JSONPath, or caller-supplied expression execution.** Every filter, order-by,
  group-by, and bucket definition in this contract is a bounded, declared-index-constrained shape
  (leading-prefix equality + range, declared time-bucket enum, declared numeric interval list). pqueue
  MUST NOT accept a caller-supplied query expression, SQL fragment, or JSONPath string for evaluation.
- **No RESP-specific reporting surface.** Per ADR-007/TD-006, richer read/report operations are
  `library-only`; this contract's capabilities are exposed through the Rust library face. A RESP
  binding, if ever added, is a separate follow-on decision and is not implied by this contract.
- **No side/projection record implementation.** Declared here (capability flag + shape), implemented
  nowhere in epic pqueue-45e13e4d (see Side/Projection Records above).
