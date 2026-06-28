#![forbid(unsafe_code)]

mod domain;

pub mod scaffold {
    pub const NAME: &str = "pqueue-core";

    pub fn name() -> &'static str {
        NAME
    }
}

pub use domain::{
    ApiError, ApiErrorCode, ApiResult, BodyHash, ClientItemKey, CohortOnIncomplete, CohortPolicy,
    CreateQueue, CreateQueueError, CreateQueueErrorKind, CreateQueueResponse, DecimalValue,
    EligibilityPolicy, EligibilitySnapshot, GateKeyPolicy, GroupKey, IdempotencyOutcome,
    IdentifierError, IndexSpec, IneligibilityReason, ItemEvent, ItemId, ItemResult,
    ItemResultStatus, ItemState, LeaseToken, Metadata, MetadataValue, OrderingMode, OwnerId,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueCreationPolicy, QueueDefinition, QueueEligibilityRules, QueueId, RecurrenceMode,
    RecurrencePolicy, RequestId, RetryPolicy, TenantId, TimestampError, TransitionError,
    UtcTimestamp, WorkerId, apply_transition, check_idempotency, evaluate_eligibility,
    failure_event, is_retry_exhausted, priority_sort,
};
