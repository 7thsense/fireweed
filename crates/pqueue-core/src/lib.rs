#![forbid(unsafe_code)]

mod domain;

pub mod scaffold {
    pub const NAME: &str = "pqueue-core";

    pub fn name() -> &'static str {
        NAME
    }
}

pub use domain::{
    ApiError, ApiErrorCode, ApiResult, BodyHash, CohortOnIncomplete, CohortPolicy, ClientItemKey,
    CreateQueue, CreateQueueError, CreateQueueErrorKind, CreateQueueResponse, DecimalValue,
    EligibilityPolicy, EligibilitySnapshot, GateKeyPolicy, GroupKey, IdempotencyOutcome,
    IdentifierError, IneligibilityReason, ItemId, ItemResult, ItemResultStatus, ItemState,
    ItemEvent, TransitionError, LeaseToken, Metadata, MetadataValue, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueCreationPolicy, QueueDefinition, QueueEligibilityRules, QueueId, RecurrenceMode,
    RecurrencePolicy, RequestId, RetryPolicy, TenantId, TimestampError, UtcTimestamp, WorkerId,
    apply_transition, check_idempotency, evaluate_eligibility, failure_event,
    is_retry_exhausted, priority_sort,
};
