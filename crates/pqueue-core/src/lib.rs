#![forbid(unsafe_code)]

mod domain;

pub mod scaffold {
    pub const NAME: &str = "pqueue-core";

    pub fn name() -> &'static str {
        NAME
    }
}

pub use domain::{
    ApiError, ApiErrorCode, ApiResult, CohortOnIncomplete, CohortPolicy, ClientItemKey,
    CreateQueue, CreateQueueError, CreateQueueErrorKind, CreateQueueResponse, DecimalValue,
    EligibilityPolicy, GateKeyPolicy, GroupKey, IdentifierError, ItemId, ItemResult,
    ItemResultStatus, ItemState, ItemEvent, TransitionError, LeaseToken, Metadata, MetadataValue,
    OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker,
    PriorityValue, QueueCreationPolicy, QueueDefinition, QueueId, RecurrenceMode,
    RecurrencePolicy, RequestId, RetryPolicy, TenantId, TimestampError, UtcTimestamp, WorkerId,
    apply_transition, priority_sort,
};
