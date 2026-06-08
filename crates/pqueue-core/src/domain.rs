use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifierError {
    pub message: String,
}

impl IdentifierError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for IdentifierError {}

macro_rules! identifier_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(IdentifierError::new(concat!(
                        stringify!($name),
                        " must not be empty"
                    )));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

identifier_type!(TenantId);
identifier_type!(QueueId);
identifier_type!(RequestId);
identifier_type!(ClientItemKey);
identifier_type!(ItemId);
identifier_type!(LeaseToken);
identifier_type!(GroupKey);
identifier_type!(WorkerId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcTimestamp {
    pub seconds: i64,
    pub nanoseconds: u32,
}

impl UtcTimestamp {
    pub fn new(seconds: i64, nanoseconds: u32) -> Result<Self, TimestampError> {
        if nanoseconds >= 1_000_000_000 {
            return Err(TimestampError::new(
                "nanoseconds must be less than 1_000_000_000",
            ));
        }

        Ok(Self {
            seconds,
            nanoseconds,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampError {
    pub message: String,
}

impl TimestampError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for TimestampError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for TimestampError {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecimalValue {
    pub mantissa: i128,
    pub scale: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PriorityValue {
    Timestamp(UtcTimestamp),
    Int64(i64),
    Decimal(DecimalValue),
    Text(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityModelKind {
    Timestamp,
    Int64,
    Decimal,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityTieBreaker {
    CreatedSequence,
    ClientItemKey,
    ItemId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriorityModel {
    pub kind: PriorityModelKind,
    pub direction: PriorityDirection,
    pub tie_breaker: PriorityTieBreaker,
}

impl PriorityModel {
    pub fn timestamp_ascending() -> Self {
        Self {
            kind: PriorityModelKind::Timestamp,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingMode {
    Strict,
    BoundedRelaxed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetadataValue {
    Null,
    Bool(bool),
    Integer(i64),
    Number(DecimalValue),
    String(String),
    Array(Vec<MetadataValue>),
    Object(Metadata),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Metadata {
    entries: BTreeMap<String, MetadataValue>,
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: MetadataValue,
    ) -> Option<MetadataValue> {
        self.entries.insert(key.into(), value)
    }

    pub fn get(&self, key: &str) -> Option<&MetadataValue> {
        self.entries.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn into_inner(self) -> BTreeMap<String, MetadataValue> {
        self.entries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateKeyPolicy {
    None,
    Dynamic,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EligibilityPolicy {
    pub metadata_blockers: BTreeMap<String, Vec<MetadataValue>>,
    pub gate_keys: GateKeyPolicy,
    pub max_gate_keys_per_item: Option<u64>,
    pub max_gates_per_request: Option<u64>,
}

impl Default for EligibilityPolicy {
    fn default() -> Self {
        Self {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::None,
            max_gate_keys_per_item: None,
            max_gates_per_request: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CohortOnIncomplete {
    ExpireCohort,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CohortPolicy {
    pub enabled: bool,
    pub completion_bound_ms: Option<u64>,
    pub on_incomplete: Option<CohortOnIncomplete>,
    pub max_cohort_size: Option<u64>,
}

impl CohortPolicy {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            completion_bound_ms: None,
            on_incomplete: None,
            max_cohort_size: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecurrenceMode {
    Oneshot,
    Recurring,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecurrencePolicy {
    pub mode: RecurrenceMode,
    pub until: Option<UtcTimestamp>,
}

impl Default for RecurrencePolicy {
    fn default() -> Self {
        Self {
            mode: RecurrenceMode::Oneshot,
            until: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueueCreationPolicy {
    pub deployment_max_shard_count: u32,
    pub default_max_gate_keys_per_item: u64,
    pub default_max_gates_per_request: u64,
}

impl Default for QueueCreationPolicy {
    fn default() -> Self {
        Self {
            deployment_max_shard_count: 1,
            default_max_gate_keys_per_item: 1,
            default_max_gates_per_request: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateQueueErrorKind {
    InvalidRequest,
    QueueDefinitionConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateQueueError {
    pub kind: CreateQueueErrorKind,
    pub message: String,
}

impl CreateQueueError {
    fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            kind: CreateQueueErrorKind::InvalidRequest,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: CreateQueueErrorKind::QueueDefinitionConflict,
            message: message.into(),
        }
    }
}

impl fmt::Display for CreateQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for CreateQueueError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiErrorCode {
    InvalidRequest,
    QueueDefinitionConflict,
    InvalidSelector,
    RequestIdConflict,
    NotFound,
    Conflict,
    StaleLease,
    Terminal,
    RateLimited,
    Unavailable,
    QueueForbidden,
    QueueNotFound,
    GatesNotEnabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub code: ApiErrorCode,
    pub message: String,
}

impl ApiError {
    pub fn new(code: ApiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl Error for ApiError {}

pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemResultStatus {
    Accepted,
    Updated,
    Duplicate,
    Claimed,
    Renewed,
    Completed,
    Failed,
    Retried,
    Released,
    Rearmed,
    Purged,
    NotFound,
    Invalid,
    Conflict,
    StaleLease,
    Terminal,
    RateLimited,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemResult {
    pub client_item_key: ClientItemKey,
    pub item_id: Option<ItemId>,
    pub item_version: Option<u64>,
    pub status: ItemResultStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateQueue {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub priority_model: PriorityModel,
    pub ordering_mode: OrderingMode,
    pub group_co_residency: bool,
    pub progress_bound_ms: u64,
    pub eligibility_policy: EligibilityPolicy,
    pub cohort_policy: CohortPolicy,
    pub recurrence: RecurrencePolicy,
    pub request_id_retention_ms: u64,
    pub client_item_key_retention_ms: u64,
    pub max_lease_duration_ms: u64,
    pub retry_policy: RetryPolicy,
    pub max_push_batch_size: u64,
    pub max_claim_batch_size: u64,
    pub max_eligible_group_size: Option<u64>,
    pub shard_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDefinition {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub priority_model: PriorityModel,
    pub ordering_mode: OrderingMode,
    pub group_co_residency: bool,
    pub progress_bound_ms: u64,
    pub eligibility_policy: EligibilityPolicy,
    pub cohort_policy: Option<CohortPolicy>,
    pub recurrence: RecurrencePolicy,
    pub request_id_retention_ms: u64,
    pub client_item_key_retention_ms: u64,
    pub max_lease_duration_ms: u64,
    pub retry_policy: RetryPolicy,
    pub max_push_batch_size: u64,
    pub max_claim_batch_size: u64,
    pub max_eligible_group_size: Option<u64>,
    pub shard_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateQueueResponse {
    pub created: bool,
    pub queue: QueueDefinition,
}

impl CreateQueue {
    pub fn validate(
        self,
        policy: &QueueCreationPolicy,
    ) -> Result<QueueDefinition, CreateQueueError> {
        if self.progress_bound_ms == 0 {
            return Err(CreateQueueError::invalid_request(
                "progress_bound_ms must be greater than 0",
            ));
        }

        if self.request_id_retention_ms == 0 {
            return Err(CreateQueueError::invalid_request(
                "request_id_retention_ms must be greater than 0",
            ));
        }

        if self.client_item_key_retention_ms == 0 {
            return Err(CreateQueueError::invalid_request(
                "client_item_key_retention_ms must be greater than 0",
            ));
        }

        if self.max_lease_duration_ms == 0 {
            return Err(CreateQueueError::invalid_request(
                "max_lease_duration_ms must be greater than 0",
            ));
        }

        if self.retry_policy.max_attempts == 0 {
            return Err(CreateQueueError::invalid_request(
                "retry_policy.max_attempts must be greater than 0",
            ));
        }

        if self.max_push_batch_size == 0 {
            return Err(CreateQueueError::invalid_request(
                "max_push_batch_size must be greater than 0",
            ));
        }

        if self.max_claim_batch_size == 0 {
            return Err(CreateQueueError::invalid_request(
                "max_claim_batch_size must be greater than 0",
            ));
        }

        if self.priority_model.kind == PriorityModelKind::Timestamp
            && self.priority_model.tie_breaker != PriorityTieBreaker::CreatedSequence
        {
            return Err(CreateQueueError::invalid_request(
                "timestamp priority queues must use created_sequence tie breaking",
            ));
        }

        if self.cohort_policy.enabled {
            if !self.group_co_residency {
                return Err(CreateQueueError::conflict(
                    "cohort-enabled queues require group_co_residency=true",
                ));
            }

            if self.recurrence.mode == RecurrenceMode::Recurring {
                return Err(CreateQueueError::invalid_request(
                    "recurrence.mode=recurring is mutually exclusive with cohort_policy.enabled=true",
                ));
            }

            let completion_bound_ms = self.cohort_policy.completion_bound_ms.ok_or_else(|| {
                CreateQueueError::invalid_request(
                    "cohort_policy.completion_bound_ms is required when cohort_policy.enabled=true",
                )
            })?;

            if completion_bound_ms == 0 {
                return Err(CreateQueueError::invalid_request(
                    "cohort_policy.completion_bound_ms must be greater than 0",
                ));
            }

            if completion_bound_ms > self.progress_bound_ms {
                return Err(CreateQueueError::conflict(
                    "cohort_policy.completion_bound_ms must be less than or equal to progress_bound_ms",
                ));
            }

            match self.cohort_policy.on_incomplete {
                Some(CohortOnIncomplete::ExpireCohort) => {}
                None => {
                    return Err(CreateQueueError::invalid_request(
                        "cohort_policy.on_incomplete is required when cohort_policy.enabled=true",
                    ));
                }
            }

            let max_cohort_size = self.cohort_policy.max_cohort_size.ok_or_else(|| {
                CreateQueueError::invalid_request(
                    "cohort_policy.max_cohort_size is required when cohort_policy.enabled=true",
                )
            })?;

            if max_cohort_size == 0 {
                return Err(CreateQueueError::invalid_request(
                    "cohort_policy.max_cohort_size must be greater than 0",
                ));
            }

            if max_cohort_size > self.max_claim_batch_size {
                return Err(CreateQueueError::conflict(
                    "cohort_policy.max_cohort_size must be less than or equal to max_claim_batch_size",
                ));
            }
        } else if self.cohort_policy.completion_bound_ms.is_some()
            || self.cohort_policy.on_incomplete.is_some()
            || self.cohort_policy.max_cohort_size.is_some()
        {
            return Err(CreateQueueError::invalid_request(
                "cohort_policy fields other than enabled must be omitted when cohort_policy.enabled=false",
            ));
        }

        if self.recurrence.mode == RecurrenceMode::Oneshot && self.recurrence.until.is_some() {
            return Err(CreateQueueError::invalid_request(
                "recurrence.until is valid only when recurrence.mode=recurring",
            ));
        }

        if self.recurrence.mode == RecurrenceMode::Recurring && self.recurrence.until.is_none() {
            return Err(CreateQueueError::invalid_request(
                "recurrence.until is required when recurrence.mode=recurring",
            ));
        }

        if let Some(max_eligible_group_size) = self.max_eligible_group_size {
            if max_eligible_group_size == 0 {
                return Err(CreateQueueError::invalid_request(
                    "max_eligible_group_size must be greater than 0",
                ));
            }
            if !self.group_co_residency {
                return Err(CreateQueueError::invalid_request(
                    "max_eligible_group_size requires group_co_residency=true",
                ));
            }
            if max_eligible_group_size > self.max_claim_batch_size {
                return Err(CreateQueueError::conflict(
                    "max_eligible_group_size must be less than or equal to max_claim_batch_size",
                ));
            }
        }

        if self.shard_count == Some(0) {
            return Err(CreateQueueError::invalid_request(
                "shard_count must be greater than or equal to 1",
            ));
        }

        let shard_count = self.shard_count.unwrap_or(1);
        if shard_count > policy.deployment_max_shard_count {
            return Err(CreateQueueError::invalid_request(
                "shard_count exceeds deployment_max_shard_count",
            ));
        }

        let mut eligibility_policy = self.eligibility_policy;
        eligibility_policy = match eligibility_policy.gate_keys {
            GateKeyPolicy::None => {
                if eligibility_policy.max_gate_keys_per_item.is_some()
                    || eligibility_policy.max_gates_per_request.is_some()
                {
                    return Err(CreateQueueError::invalid_request(
                        "gate-key caps must be omitted when eligibility_policy.gate_keys=none",
                    ));
                }

                eligibility_policy
            }
            GateKeyPolicy::Dynamic => {
                let mut policy_out = eligibility_policy;

                let max_gate_keys_per_item = policy_out
                    .max_gate_keys_per_item
                    .unwrap_or(policy.default_max_gate_keys_per_item);
                if max_gate_keys_per_item == 0 {
                    return Err(CreateQueueError::invalid_request(
                        "eligibility_policy.max_gate_keys_per_item must be greater than 0",
                    ));
                }
                policy_out.max_gate_keys_per_item = Some(max_gate_keys_per_item);

                let max_gates_per_request = policy_out
                    .max_gates_per_request
                    .unwrap_or(policy.default_max_gates_per_request);
                if max_gates_per_request == 0 {
                    return Err(CreateQueueError::invalid_request(
                        "eligibility_policy.max_gates_per_request must be greater than 0",
                    ));
                }
                policy_out.max_gates_per_request = Some(max_gates_per_request);

                policy_out
            }
        };

        Ok(QueueDefinition {
            tenant_id: self.tenant_id,
            queue_id: self.queue_id,
            priority_model: self.priority_model,
            ordering_mode: self.ordering_mode,
            group_co_residency: self.group_co_residency,
            progress_bound_ms: self.progress_bound_ms,
            eligibility_policy,
            cohort_policy: if self.cohort_policy.enabled {
                Some(self.cohort_policy)
            } else {
                None
            },
            recurrence: self.recurrence,
            request_id_retention_ms: self.request_id_retention_ms,
            client_item_key_retention_ms: self.client_item_key_retention_ms,
            max_lease_duration_ms: self.max_lease_duration_ms,
            retry_policy: self.retry_policy,
            max_push_batch_size: self.max_push_batch_size,
            max_claim_batch_size: self.max_claim_batch_size,
            max_eligible_group_size: self.max_eligible_group_size,
            shard_count,
        })
    }
}

impl QueueDefinition {
    pub fn create_response(self, created: bool) -> CreateQueueResponse {
        CreateQueueResponse {
            created,
            queue: self,
        }
    }
}

pub type DomainResult<T> = Result<T, CreateQueueError>;
