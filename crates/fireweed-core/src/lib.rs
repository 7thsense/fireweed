#![forbid(unsafe_code)]

mod domain;
mod query;

pub mod scaffold {
    pub const NAME: &str = "fireweed-core";

    pub fn name() -> &'static str {
        NAME
    }
}

pub use domain::{
    ApiError, ApiErrorCode, ApiResult, BodyHash, ClientItemKey, CohortId, CohortOnIncomplete,
    CohortPolicy, CompoundIndexDef, CompoundIndexField, CreateQueue, CreateQueueError,
    CreateQueueErrorKind, CreateQueueResponse, DecimalValue, EligibilityPolicy,
    EligibilitySnapshot, EntitySchemaDocument, GateKeyPolicy, GroupKey, IdempotencyOutcome,
    IdentifierError, IndexDeclaration, IndexDef, IndexSpec, IndexType, IneligibilityReason,
    ItemEvent, ItemId, ItemResult, ItemResultStatus, ItemState, LeaseToken, Metadata,
    MetadataValue, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueDefinition, QueueEligibilityRules,
    QueueId, QueueIndex, RecurrenceMode, RecurrencePolicy, RequestId, RetryPolicy, TenantId,
    TimestampError, TransitionError, UtcTimestamp, WorkerId, apply_transition, check_idempotency,
    evaluate_eligibility, failure_event, is_retry_exhausted, priority_sort,
};
pub use query::{
    AggregateGroup, BoundedMutationRequest, BoundedMutationResponse, BucketCount, BucketRule,
    ClaimByItemIdClass, ClaimByItemIdsDisposition, ClaimByItemIdsOutcome, ClaimByItemIdsRequest,
    ClaimByQueryRequest, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, FilterOp,
    GroupByField, GroupedAggregateRequest, GroupedAggregateResponse, MetricsByQueryRequest,
    MutationOutcome, MutationResult, OrderField, QueryCapabilityFlags, QueryCursor, QueryFilter,
    QueryRequestError, RangeScanRequest, RangeScanResponse, RangeScanRow, SortDirection,
    TimeBucket, TypedValue,
};
