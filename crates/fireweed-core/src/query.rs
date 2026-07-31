//! Hot projection query substrate (API-004): domain-neutral typed request/response shapes for
//! range-scan, grouped/bucketed aggregation, bounded mutation, and claim-by-query over a queue's
//! declared indexes.
//!
//! fireweed MUST remain domain-neutral (API-004 Purpose): these types carry no Snorri/Cayce vocabulary,
//! only the generic filter/order/bucket/mutation shapes the contract defines. Every request type here
//! is a bounded, declared-index-constrained shape — never an arbitrary caller-supplied expression
//! (API-004 Non-Goals).

use std::collections::BTreeMap;

use crate::{ItemId, RequestId, UtcTimestamp, WorkerId};

/// A typed scalar value carried in a query filter, bucket bound, or mutation `set_fields` entry
/// (API-004 Typed Indexed Record Shape). Mirrors the ESF-typed field types (`axon_esf::IndexType`)
/// this contract's queries are constrained to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    DateTime(UtcTimestamp),
}

/// Comparison operator for a [`QueryFilter`] (API-004 Range Scan `RangeScan.filters[]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Gte,
    Gt,
    Lte,
    Lt,
}

/// One filter term: `field <op> value`, over a field of the declared index the query resolves to.
/// Fields other than the last non-equality field in a query MUST use [`FilterOp::Eq`] (API-004
/// leading-prefix-equality-then-range rule) — enforcement against a queue's actual declared indexes
/// is a backend concern; this type carries the shape only.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueryFilter {
    pub field: String,
    pub op: FilterOp,
    pub value: TypedValue,
}

/// Sort direction for an `order_by` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// One `order_by` term (API-004 Cursor Ordering). The record's internal item id is implicitly
/// appended as the final ordering field whenever these declared fields do not already resolve to a
/// total order — callers never name it explicitly.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderField {
    pub field: String,
    pub direction: SortDirection,
}

/// Opaque cursor pagination token (API-004 Cursor Ordering / Cursor Invalidation). fireweed mints and
/// interprets the contents; callers pass it back verbatim.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryCursor(pub String);

/// A `RangeScan` request (API-004 Range Scan): an ordered scan over a declared index, applying
/// equality filters on a leading prefix and a range on the next field.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RangeScanRequest {
    /// The declared index to scan. `None` lets fireweed select a matching declared index.
    pub index: Option<String>,
    pub filters: Vec<QueryFilter>,
    pub order_by: Vec<OrderField>,
    pub page_size: u32,
    pub cursor: Option<QueryCursor>,
}

/// Structural rejection of a query request, independent of any queue's declared indexes (API-004).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueryRequestError {
    /// A named field is not a member of any declared index (API-004 declared-index rule). Index
    /// membership can only be checked against a queue's actual declared indexes, so this variant is
    /// raised by backends, not by the structural `validate` methods here.
    UnindexedField(String),
    /// `page_size` was zero or exceeded the deployment's configured maximum.
    InvalidPageSize,
    /// The declared bucket set overlapped (API-004 Declared Numeric Buckets).
    OverlappingBuckets,
    /// The bucket count exceeded the deployment's configured `max_buckets` (API-004 Aggregate Limits).
    TooManyBuckets,
    /// `null_bucket_label` was empty — REQUIRED, never silently omitted (API-004).
    MissingNullBucketLabel,
    /// The number of distinct groups would exceed the deployment's configured `max_groups`.
    AggregateTooLarge,
}

impl RangeScanRequest {
    /// Structural validation of the `page_size` bound (API-004: `page_size` MUST be greater than 0
    /// and MUST NOT exceed the deployment's max page size). Index-membership and leading-prefix rules
    /// require a queue's declared indexes and are a backend concern.
    pub fn validate(&self, max_page_size: u32) -> Result<(), QueryRequestError> {
        if self.page_size == 0 || self.page_size > max_page_size {
            return Err(QueryRequestError::InvalidPageSize);
        }
        Ok(())
    }
}

/// One row returned by a `RangeScan` — the declared-index fields only (API-004: "MUST NOT include
/// undeclared/unindexed fields"). Callers needing the full record fetch it separately by key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RangeScanRow {
    pub item_id: ItemId,
    pub fields: BTreeMap<String, TypedValue>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RangeScanResponse {
    pub rows: Vec<RangeScanRow>,
    /// Present when more rows may exist; absent when the scan is exhausted.
    pub next_cursor: Option<QueryCursor>,
}

/// A caller-declared time-bucket transform applied to a datetime-indexed group-by field (API-004
/// Grouping / Aggregation) — the only transform this contract allows on a group-by field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBucket {
    Hour,
    Day,
}

/// One `group_by` term: a declared-index field, optionally bucketed by [`TimeBucket`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GroupByField {
    pub field: String,
    pub time_bucket: Option<TimeBucket>,
}

/// A `GroupedAggregate` request (API-004 Grouping / Aggregation): group filtered rows by one or more
/// declared-index fields and return a count per group.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GroupedAggregateRequest {
    pub index: Option<String>,
    pub filters: Vec<QueryFilter>,
    pub group_by: Vec<GroupByField>,
    /// Bounds the distinct groups this request may return (API-004 Aggregate Limits `max_groups`).
    pub max_groups: u32,
}

/// A `MetricsByQuery` request: filter a declared index and return lifecycle counts for the matching
/// rows. Unlike `GroupedAggregate`, the response is the queue lifecycle metric shape itself.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MetricsByQueryRequest {
    pub index: Option<String>,
    pub filters: Vec<QueryFilter>,
}

/// One reported group: its key tuple and row count. Empty groups (zero matching rows) are never
/// reported (API-004).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateGroup {
    pub key: BTreeMap<String, TypedValue>,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GroupedAggregateResponse {
    pub groups: Vec<AggregateGroup>,
}

/// One caller-declared bucket: either an exact-value match or a half-open/closed numeric range
/// (API-004 Declared Numeric Buckets). Exactly one of `exact` or a `{gt|gte, lt|lte}` pair MUST be
/// populated — [`DeclaredBucketSegmentRequest::validate`] does not enforce that shape (a bucket with
/// no bounds at all matches nothing and is treated as non-overlapping with everything).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BucketRule {
    pub label: String,
    pub exact: Option<f64>,
    pub gt: Option<f64>,
    pub gte: Option<f64>,
    pub lt: Option<f64>,
    pub lte: Option<f64>,
}

/// A half-open interval used only to check declared buckets for overlap. `NEG_INFINITY`/`INFINITY`
/// stand in for an absent lower/upper bound.
struct BucketInterval {
    lo: f64,
    lo_inclusive: bool,
    hi: f64,
    hi_inclusive: bool,
}

impl BucketRule {
    fn to_interval(&self) -> BucketInterval {
        if let Some(v) = self.exact {
            return BucketInterval {
                lo: v,
                lo_inclusive: true,
                hi: v,
                hi_inclusive: true,
            };
        }
        let (lo, lo_inclusive) = self
            .gte
            .map(|v| (v, true))
            .or(self.gt.map(|v| (v, false)))
            .unwrap_or((f64::NEG_INFINITY, false));
        let (hi, hi_inclusive) = self
            .lte
            .map(|v| (v, true))
            .or(self.lt.map(|v| (v, false)))
            .unwrap_or((f64::INFINITY, false));
        BucketInterval {
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        }
    }
}

/// Whether `a` and `b` share any point, honoring boundary inclusivity (touching exclusive bounds do
/// not overlap; touching inclusive bounds do).
fn intervals_overlap(a: &BucketInterval, b: &BucketInterval) -> bool {
    fn le(x: f64, x_inclusive: bool, y: f64, y_inclusive: bool) -> bool {
        if x < y {
            true
        } else if x == y {
            x_inclusive && y_inclusive
        } else {
            false
        }
    }
    le(a.lo, a.lo_inclusive, b.hi, b.hi_inclusive) && le(b.lo, b.lo_inclusive, a.hi, a.hi_inclusive)
}

/// A `DeclaredBucketSegment` request (API-004 Declared Numeric Buckets): segment rows into
/// caller-declared numeric buckets over one declared numeric-indexed field, plus a required
/// null-handling bucket.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeclaredBucketSegmentRequest {
    pub index: Option<String>,
    pub filters: Vec<QueryFilter>,
    pub field: String,
    pub buckets: Vec<BucketRule>,
    /// Names the bucket reporting rows where `field` is null or absent. REQUIRED — never optional, so
    /// null handling is never silently omitted (API-004).
    pub null_bucket_label: String,
}

impl DeclaredBucketSegmentRequest {
    /// Structural validation of the REQUIRED rules that don't need a queue's declared indexes:
    /// non-overlapping buckets, a present `null_bucket_label`, and the deployment's `max_buckets`
    /// bound (API-004 Declared Numeric Buckets / Aggregate Limits).
    pub fn validate(&self, max_buckets: u32) -> Result<(), QueryRequestError> {
        if self.null_bucket_label.is_empty() {
            return Err(QueryRequestError::MissingNullBucketLabel);
        }
        if self.buckets.len() as u32 > max_buckets {
            return Err(QueryRequestError::TooManyBuckets);
        }
        let intervals: Vec<BucketInterval> =
            self.buckets.iter().map(BucketRule::to_interval).collect();
        for i in 0..intervals.len() {
            for j in (i + 1)..intervals.len() {
                if intervals_overlap(&intervals[i], &intervals[j]) {
                    return Err(QueryRequestError::OverlappingBuckets);
                }
            }
        }
        Ok(())
    }
}

/// One bucket's reported count, keyed by its caller-declared label.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BucketCount {
    pub label: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeclaredBucketSegmentResponse {
    pub buckets: Vec<BucketCount>,
}

/// A `BoundedMutation` request (API-004 Bounded Mutation): scan a declared-index predicate and apply
/// a caller-specified field update to every matching record, with per-record optimistic concurrency.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundedMutationRequest {
    pub index: Option<String>,
    pub filters: Vec<QueryFilter>,
    /// Replaces only the named fields on each matched record; never interpreted by fireweed beyond type
    /// validation against the record's entity schema (API-004: "caller data, uninterpreted").
    pub set_fields: BTreeMap<String, TypedValue>,
    /// Bounds the rows examined per internal execution step (API-004 Aggregate Limits
    /// `max_scan_rows_per_page`).
    pub max_scan_rows: u32,
}

/// The per-record outcome of a [`BoundedMutationRequest`] (API-004: "Best-effort per record").
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOutcome {
    Updated,
    /// A concurrent claim/finalize/mutation changed the record's version between match and apply.
    Conflict,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MutationResult {
    pub item_id: ItemId,
    pub outcome: MutationOutcome,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundedMutationResponse {
    pub results: Vec<MutationResult>,
}

/// A `ClaimByQuery` request (API-004 Claim By Query): claim due records selected by a declared-index
/// predicate, in the order of a declared index, instead of the queue's default priority order. The
/// claim/lease fields carry the same meaning as API-001 `BatchClaim`'s equivalents — this is an
/// alternate *selection* path into the same claim/lease/finalize lifecycle, not a parallel lifecycle.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClaimByQueryRequest {
    pub index: Option<String>,
    pub filters: Vec<QueryFilter>,
    pub order_by: OrderField,
    pub max_items: u32,
    pub lease_duration_ms: u64,
    pub worker_id: WorkerId,
    pub request_id: Option<RequestId>,
}

/// API-001 `BatchClaimByItemIds`: lease exactly the caller-supplied `item_id` set (external-trigger /
/// pre-resolved reserve). One durable command; partial per-id outcomes; never leases outside the set.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClaimByItemIdsRequest {
    /// Distinct ids after collapse are leased at most once; request order of first occurrence is preserved
    /// for outcomes. MUST be non-empty and no longer than the queue max claim batch size.
    pub item_ids: Vec<ItemId>,
    pub lease_duration_ms: u64,
    pub worker_id: WorkerId,
    /// Required envelope idempotency key (same rules as `BatchClaim` / `claim_by_query`).
    pub request_id: RequestId,
    /// Optional caller-supplied lease token. When `None`, the server mints an unguessable token
    /// (library default). RESP `XCLAIM` sets this to the consumer name so the lease identity matches
    /// the Redis consumer (TD-006: consumer **is** the lease token).
    #[serde(default)]
    pub lease_token: Option<crate::LeaseToken>,
}

/// Pre-mutation point-lookup classification of one `item_id` for `claim_by_item_ids`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimByItemIdClass {
    /// Pending, not superseded, due, gates open, queue not paused — may be leased.
    Claimable,
    NotFound,
    /// Gates / `not_before` / queue pause / other Eligibility Precedence exclusion.
    NotEligible,
    /// Active lease held by any worker.
    Leased,
    Terminal,
}

/// Per-id disposition in a `ClaimByItemIds` response (API-001 partial outcomes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimByItemIdsDisposition {
    Claimed,
    NotFound,
    NotEligible,
    Leased,
    Terminal,
}

impl From<ClaimByItemIdClass> for ClaimByItemIdsDisposition {
    fn from(class: ClaimByItemIdClass) -> Self {
        match class {
            ClaimByItemIdClass::Claimable => ClaimByItemIdsDisposition::Claimed,
            ClaimByItemIdClass::NotFound => ClaimByItemIdsDisposition::NotFound,
            ClaimByItemIdClass::NotEligible => ClaimByItemIdsDisposition::NotEligible,
            ClaimByItemIdClass::Leased => ClaimByItemIdsDisposition::Leased,
            ClaimByItemIdClass::Terminal => ClaimByItemIdsDisposition::Terminal,
        }
    }
}

/// One per-id outcome for API-001 `BatchClaimByItemIds` (order = first-occurrence request order).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClaimByItemIdsOutcome {
    pub item_id: ItemId,
    pub disposition: ClaimByItemIdsDisposition,
}

/// Advertised hot-projection query capabilities for a queue/backend (API-004 Query Capability Names).
/// Every flag defaults to `false` — the safe default for a backend that has not implemented this
/// contract, so a consumer rejects unsupported capabilities before use rather than the backend
/// silently degrading to a full scan.
///
/// A backend that advertises any of `range_scan`, `grouped_aggregate`, `declared_bucket_segment`,
/// `bounded_mutation`, or `claim_by_query` MUST advertise all five together (API-004: this contract
/// does not define a partial-capability backend for those five). `side_record_query` is independently
/// gated and is deferred beyond epic pqueue-45e13e4d for every backend — no backend in this epic may
/// set it `true`. `claim_by_item_ids` (API-001 BatchClaimByItemIds) is independently gated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct QueryCapabilityFlags {
    pub range_scan: bool,
    pub grouped_aggregate: bool,
    pub declared_bucket_segment: bool,
    pub bounded_mutation: bool,
    pub claim_by_query: bool,
    /// Deferred beyond epic pqueue-45e13e4d (API-004 Side/Projection Records). MUST remain `false`.
    pub side_record_query: bool,
    /// API-001 `BatchClaimByItemIds` / library `claim_by_item_ids`. Independent of the five paired
    /// hot-query capabilities.
    #[serde(default)]
    pub claim_by_item_ids: bool,
}

impl QueryCapabilityFlags {
    /// `true` iff the five paired capabilities (everything but `side_record_query`) are all set the
    /// same way — the API-004 "advertise all five together" rule.
    pub fn paired_capabilities_consistent(&self) -> bool {
        let flags = [
            self.range_scan,
            self.grouped_aggregate,
            self.declared_bucket_segment,
            self.bounded_mutation,
            self.claim_by_query,
        ];
        flags.iter().all(|f| *f == flags[0])
    }
}

#[cfg(test)]
mod core_domain_tests_hot_projection_query_types {
    use super::*;

    fn ts(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(seconds, 0).expect("valid timestamp")
    }

    #[test]
    fn query_capability_flags_default_to_unavailable() {
        let flags = QueryCapabilityFlags::default();
        assert!(!flags.range_scan);
        assert!(!flags.grouped_aggregate);
        assert!(!flags.declared_bucket_segment);
        assert!(!flags.bounded_mutation);
        assert!(!flags.claim_by_query);
        assert!(!flags.side_record_query);
        assert!(!flags.claim_by_item_ids);
        assert!(flags.paired_capabilities_consistent());
    }

    #[test]
    fn query_capability_flags_reject_partial_paired_capability() {
        let flags = QueryCapabilityFlags {
            range_scan: true,
            ..QueryCapabilityFlags::default()
        };
        assert!(!flags.paired_capabilities_consistent());
    }

    #[test]
    fn range_scan_request_serde_round_trip() {
        let req = RangeScanRequest {
            index: Some("by_tenant_run_scheduled".to_string()),
            filters: vec![QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Gte,
                value: TypedValue::DateTime(ts(1_800_000_000)),
            }],
            order_by: vec![OrderField {
                field: "scheduled_at".to_string(),
                direction: SortDirection::Ascending,
            }],
            page_size: 50,
            cursor: Some(QueryCursor("opaque-cursor-1".to_string())),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let round_tripped: RangeScanRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, round_tripped);
    }

    #[test]
    fn range_scan_request_rejects_zero_and_oversized_page_size() {
        let base = RangeScanRequest {
            index: None,
            filters: vec![],
            order_by: vec![],
            page_size: 0,
            cursor: None,
        };
        assert_eq!(base.validate(100), Err(QueryRequestError::InvalidPageSize));

        let oversized = RangeScanRequest {
            page_size: 101,
            ..base
        };
        assert_eq!(
            oversized.validate(100),
            Err(QueryRequestError::InvalidPageSize)
        );

        let ok = RangeScanRequest {
            page_size: 100,
            ..RangeScanRequest {
                index: None,
                filters: vec![],
                order_by: vec![],
                page_size: 0,
                cursor: None,
            }
        };
        assert_eq!(ok.validate(100), Ok(()));
    }

    /// The API-004 canonical conformance fixture: non-overlapping buckets over `engagement_probability`
    /// MUST validate cleanly and a present `null_bucket_label` is required.
    fn canonical_buckets() -> Vec<BucketRule> {
        vec![
            BucketRule {
                label: "0%".to_string(),
                exact: Some(0.0),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            },
            BucketRule {
                label: "8.01-10%".to_string(),
                exact: None,
                gt: Some(0.08),
                gte: None,
                lt: None,
                lte: Some(0.10),
            },
            BucketRule {
                label: "10.01-15%".to_string(),
                exact: None,
                gt: Some(0.10),
                gte: None,
                lt: None,
                lte: Some(0.15),
            },
            BucketRule {
                label: "45.01-50%".to_string(),
                exact: None,
                gt: Some(0.45),
                gte: None,
                lt: None,
                lte: Some(0.50),
            },
        ]
    }

    #[test]
    fn declared_bucket_segment_accepts_canonical_conformance_fixture() {
        let req = DeclaredBucketSegmentRequest {
            index: Some("by_engagement_probability".to_string()),
            filters: vec![QueryFilter {
                field: "action_type".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("message.send".to_string()),
            }],
            field: "engagement_probability".to_string(),
            buckets: canonical_buckets(),
            null_bucket_label: "no-activity".to_string(),
        };
        assert_eq!(req.validate(10), Ok(()));
    }

    #[test]
    fn declared_bucket_segment_rejects_overlapping_buckets() {
        let mut buckets = canonical_buckets();
        // Overlaps the "10.01-15%" bucket at 0.12.
        buckets.push(BucketRule {
            label: "overlap".to_string(),
            exact: None,
            gt: Some(0.05),
            gte: None,
            lt: None,
            lte: Some(0.12),
        });
        let req = DeclaredBucketSegmentRequest {
            index: None,
            filters: vec![],
            field: "engagement_probability".to_string(),
            buckets,
            null_bucket_label: "no-activity".to_string(),
        };
        assert_eq!(req.validate(10), Err(QueryRequestError::OverlappingBuckets));
    }

    #[test]
    fn declared_bucket_segment_rejects_missing_null_bucket_label() {
        let req = DeclaredBucketSegmentRequest {
            index: None,
            filters: vec![],
            field: "engagement_probability".to_string(),
            buckets: canonical_buckets(),
            null_bucket_label: String::new(),
        };
        assert_eq!(
            req.validate(10),
            Err(QueryRequestError::MissingNullBucketLabel)
        );
    }

    #[test]
    fn declared_bucket_segment_rejects_too_many_buckets() {
        let req = DeclaredBucketSegmentRequest {
            index: None,
            filters: vec![],
            field: "engagement_probability".to_string(),
            buckets: canonical_buckets(),
            null_bucket_label: "no-activity".to_string(),
        };
        assert_eq!(req.validate(2), Err(QueryRequestError::TooManyBuckets));
    }

    #[test]
    fn bounded_mutation_request_serde_round_trip() {
        let mut set_fields = BTreeMap::new();
        set_fields.insert(
            "suppressed_by_recycling".to_string(),
            TypedValue::Bool(true),
        );
        let req = BoundedMutationRequest {
            index: Some("by_run_status".to_string()),
            filters: vec![QueryFilter {
                field: "status".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("scheduled".to_string()),
            }],
            set_fields,
            max_scan_rows: 500,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let round_tripped: BoundedMutationRequest =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, round_tripped);
    }

    #[test]
    fn claim_by_query_request_serde_round_trip() {
        let req = ClaimByQueryRequest {
            index: Some("by_run_scheduled_at".to_string()),
            filters: vec![QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Lte,
                value: TypedValue::DateTime(ts(1_800_000_000)),
            }],
            order_by: OrderField {
                field: "scheduled_at".to_string(),
                direction: SortDirection::Ascending,
            },
            max_items: 25,
            lease_duration_ms: 30_000,
            worker_id: WorkerId::new("worker-1").unwrap(),
            request_id: Some(RequestId::new("req-1").unwrap()),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let round_tripped: ClaimByQueryRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, round_tripped);
    }

    #[test]
    fn grouped_aggregate_request_serde_round_trip() {
        let req = GroupedAggregateRequest {
            index: Some("by_status_scheduled_at".to_string()),
            filters: vec![],
            group_by: vec![
                GroupByField {
                    field: "status".to_string(),
                    time_bucket: None,
                },
                GroupByField {
                    field: "scheduled_at".to_string(),
                    time_bucket: Some(TimeBucket::Hour),
                },
            ],
            max_groups: 1000,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let round_tripped: GroupedAggregateRequest =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, round_tripped);
    }

    #[test]
    fn metrics_by_query_request_serde_round_trip() {
        let req = MetricsByQueryRequest {
            index: Some("by_record_kind_scheduled_at".to_string()),
            filters: vec![QueryFilter {
                field: "record_kind".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("transition".to_string()),
            }],
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let round_tripped: MetricsByQueryRequest =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, round_tripped);
    }
}
