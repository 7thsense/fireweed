use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

pub use axon_esf::{
    CompoundIndexDef, CompoundIndexField, EntitySchemaDocument, IndexDeclaration, IndexDef,
    IndexType,
};

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

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        // Deserialize through the validating constructor so a persisted id can
        // never be reconstituted in an invalid (e.g. empty) state.
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

identifier_type!(TenantId);
identifier_type!(QueueId);
identifier_type!(RequestId);
identifier_type!(ClientItemKey);
identifier_type!(LeaseToken);
identifier_type!(GroupKey);
identifier_type!(CohortId);
identifier_type!(WorkerId);
identifier_type!(OwnerId);

/// Server-assigned item identity (ADR-009): a packed `u64` laid out **high → low** as
/// `[ epoch : 24 ][ node : 8 ][ counter : 32 ]`.
///
/// The layout is chosen for the `(tenant, queue, item_id)` pkey: `epoch` (strictly-increasing per queue on
/// every ownership change) is the high order and `counter` (per-tenure, +1 per push) the low order, so
/// item_ids increase monotonically with insertion order — **append-only** B-tree inserts, and numeric order
/// equals stream/insertion order. `node` (the writer's node id) sits in the middle as split-brain
/// disambiguation; single-writer-per-epoch already makes `(epoch, counter)` unique, so `node` is
/// defense-in-depth, not the primary guarantee. Generated **locally** by the owning node (no central
/// sequence — works on the log); the counter resets each acquire (a new, strictly-greater epoch).
///
/// Serialized as its **decimal string** on the log/wire (no JSON-number precision footgun, stable token);
/// stored as a native `BIGINT`/`INTEGER` ([`as_u64`](Self::as_u64)) in a relational projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ItemId(u64);

impl ItemId {
    const NODE_SHIFT: u32 = 32;
    const EPOCH_SHIFT: u32 = 40;
    const EPOCH_MASK: u64 = (1 << 24) - 1;

    /// Pack `(epoch, node, counter)` into the id. Only the low 24 bits of `epoch` are used (it wraps after
    /// 2^24 ownership changes — a centuries-away event, see the epoch-exhaustion guard at the owner).
    pub fn mint(epoch: u64, node: u8, counter: u32) -> Self {
        Self(
            ((epoch & Self::EPOCH_MASK) << Self::EPOCH_SHIFT)
                | ((node as u64) << Self::NODE_SHIFT)
                | counter as u64,
        )
    }

    /// Wrap a raw packed value (read from durable storage / the wire).
    pub fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// Parse a persisted/wire rendering — the inverse of [`Display`](std::fmt::Display). Accepts the
    /// canonical decimal of the packed value; used when reading an id back from a TEXT column or a RESP
    /// frame. (Kept named `new` so the many read-from-storage call sites are unchanged.)
    pub fn new(rendered: impl AsRef<str>) -> Result<Self, IdentifierError> {
        rendered.as_ref().parse()
    }

    /// The packed value — store this as the native `BIGINT`/`INTEGER` pkey in a relational projection.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// The 24-bit epoch field (low bits of the queue's `assignment_epoch`).
    pub fn epoch(&self) -> u64 {
        self.0 >> Self::EPOCH_SHIFT
    }

    /// The writer's node id.
    pub fn node(&self) -> u8 {
        (self.0 >> Self::NODE_SHIFT) as u8
    }

    /// The per-tenure counter.
    pub fn counter(&self) -> u32 {
        self.0 as u32
    }
}

impl fmt::Display for ItemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Decimal of the packed value: stable, opaque token; numeric order == stream/insertion order.
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ItemId {
    type Err = IdentifierError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>()
            .map(Self)
            .map_err(|_| IdentifierError::new("ItemId must be a u64 decimal string"))
    }
}

impl serde::Serialize for ItemId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // String on the wire/log — avoids the JSON >2^53 number-precision footgun.
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for ItemId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
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

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DecimalValue {
    pub mantissa: i128,
    pub scale: u32,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PriorityValue {
    Timestamp(UtcTimestamp),
    Int64(i64),
    Decimal(DecimalValue),
    Text(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PriorityModelKind {
    Timestamp,
    Int64,
    Decimal,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PriorityDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PriorityTieBreaker {
    CreatedSequence,
    ClientItemKey,
    ItemId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OrderingMode {
    Strict,
    BoundedRelaxed,
}

/// Default rank-error bound (strict-equivalent). A `0` bound means claim selection never deviates from
/// strict priority order, so a `BoundedRelaxed` queue with a `0` bound behaves byte-for-byte like `Strict`.
pub fn default_max_rank_error() -> u32 {
    0
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MetadataValue {
    Null,
    Bool(bool),
    Integer(i64),
    Number(DecimalValue),
    String(String),
    Array(Vec<MetadataValue>),
    Object(Metadata),
}

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    entries: BTreeMap<String, MetadataValue>,
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_entries(entries: BTreeMap<String, MetadataValue>) -> Self {
        Self { entries }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GateKeyPolicy {
    None,
    Dynamic,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CohortOnIncomplete {
    ExpireCohort,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RecurrenceMode {
    Oneshot,
    Recurring,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueueCreationPolicy {
    pub default_max_gate_keys_per_item: u64,
    pub default_max_gates_per_request: u64,
}

impl Default for QueueCreationPolicy {
    fn default() -> Self {
        Self {
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

/// Declaration of one per-queue secondary index over configured item fields (ADR-010).
///
/// An index belongs to one queue (no cross-queue lookup) and is generic over field *names* and opaque
/// *bytes* values (fireweed stays domain-agnostic). The composite key is built from `fields` in order; a
/// `unique` index rejects a push/upsert/update that would create a duplicate key with
/// [`ApiErrorCode::Conflict`] semantics, atomically committing nothing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexSpec {
    /// Unique index name within the queue (the lookup handle).
    pub name: String,
    /// Ordered list of field names whose values compose the key. Order is significant.
    pub fields: Vec<String>,
    /// `true` => at most one live item may carry a given composite key (atomic Conflict on violation).
    pub unique: bool,
}

/// A named typed secondary index for fireweed query ergonomics (ADR-011).
///
/// Wraps an `axon_esf::IndexDeclaration` (single-field or compound) with a fireweed-specific `name`
/// so that callers can address indexes by name rather than by field path. The declaration drives
/// typed key encoding via `axon_esf::encode_index_value` / `encode_compound_index_key` — keys are
/// byte-identical to those produced by axon and sort correctly for all ESF `IndexType` variants.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueueIndex {
    /// Unique index name within the queue (the query handle).
    pub name: String,
    /// The ESF typed index declaration — single-field or compound.
    pub declaration: IndexDeclaration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateQueue {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub priority_model: PriorityModel,
    pub ordering_mode: OrderingMode,
    /// Maximum rank error (in priority-rank positions) the claim path may introduce under
    /// `OrderingMode::BoundedRelaxed`. A delivered item's rank error is how far its delivered position
    /// deviates from its strict-priority position; selection keeps that deviation `<= max_rank_error`.
    /// Only meaningful when `ordering_mode == BoundedRelaxed`; ignored (treated as `0`) under `Strict`.
    pub max_rank_error: u32,
    pub progress_bound_ms: u64,
    pub eligibility_policy: EligibilityPolicy,
    pub cohort_policy: CohortPolicy,
    pub recurrence: RecurrencePolicy,
    pub request_id_retention_ms: u64,
    pub client_item_key_retention_ms: u64,
    pub terminal_retention_ms: u64,
    pub max_lease_duration_ms: u64,
    pub retry_policy: RetryPolicy,
    pub max_push_batch_size: u64,
    pub max_claim_batch_size: u64,
    pub max_eligible_group_size: Option<u64>,
    /// Per-queue secondary indexes over configured item fields (ADR-010). Empty (default) = no indexes.
    pub secondary_indexes: Vec<IndexSpec>,
    /// Optional ESF entity schema document (ADR-011). Absent = no payload validation.
    pub entity_schema: Option<EntitySchemaDocument>,
    /// Typed secondary indexes (ADR-011), each wrapping an ESF declaration with a fireweed name.
    /// Empty = no typed indexes. Must not overlap `secondary_indexes` by name.
    pub typed_indexes: Vec<QueueIndex>,
    // Whether this queue emits change records to the history sink. Default-on so operators get
    // history unless they explicitly opt out per queue.
    pub emit_change_records: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueueDefinition {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub priority_model: PriorityModel,
    pub ordering_mode: OrderingMode,
    /// Maximum rank error (in priority-rank positions) tolerated on the claim path under
    /// `OrderingMode::BoundedRelaxed` (see [`CreateQueue::max_rank_error`]). `#[serde(default)]` keeps
    /// existing persisted definitions + the wire compatible (absent => `0`, i.e. strict-equivalent).
    #[serde(default = "default_max_rank_error")]
    pub max_rank_error: u32,
    pub progress_bound_ms: u64,
    pub eligibility_policy: EligibilityPolicy,
    pub cohort_policy: Option<CohortPolicy>,
    pub recurrence: RecurrencePolicy,
    pub request_id_retention_ms: u64,
    pub client_item_key_retention_ms: u64,
    #[serde(default = "default_terminal_retention_ms")]
    pub terminal_retention_ms: u64,
    pub max_lease_duration_ms: u64,
    pub retry_policy: RetryPolicy,
    pub max_push_batch_size: u64,
    pub max_claim_batch_size: u64,
    pub max_eligible_group_size: Option<u64>,
    /// Per-queue secondary indexes over configured item fields (ADR-010). Empty (default) = no indexes;
    /// `#[serde(default)]` keeps existing persisted definitions and the wire compatible.
    #[serde(default)]
    pub secondary_indexes: Vec<IndexSpec>,
    /// Optional ESF entity schema document (ADR-011). Absent = no payload validation.
    /// `#[serde(default)]` keeps existing persisted definitions and the wire compatible.
    #[serde(default)]
    pub entity_schema: Option<EntitySchemaDocument>,
    /// Typed secondary indexes (ADR-011), each wrapping an ESF declaration with a fireweed name.
    /// `#[serde(default)]` keeps existing persisted definitions and the wire compatible.
    #[serde(default)]
    pub typed_indexes: Vec<QueueIndex>,
    // Whether this queue emits change records to the niflheim history sink. Default-on so operators
    // get history unless they explicitly opt out per queue.
    #[serde(default = "default_emit_change_records")]
    pub emit_change_records: bool,
}

fn default_terminal_retention_ms() -> u64 {
    3_600_000
}

fn default_emit_change_records() -> bool {
    true
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

        if self.terminal_retention_ms == 0 {
            return Err(CreateQueueError::invalid_request(
                "terminal_retention_ms must be greater than 0",
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

        if self.max_rank_error != 0 && self.ordering_mode != OrderingMode::BoundedRelaxed {
            return Err(CreateQueueError::invalid_request(
                "max_rank_error is only meaningful when ordering_mode=bounded_relaxed",
            ));
        }

        if self.cohort_policy.enabled {
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
            if max_eligible_group_size > self.max_claim_batch_size {
                return Err(CreateQueueError::conflict(
                    "max_eligible_group_size must be less than or equal to max_claim_batch_size",
                ));
            }
        }

        // Secondary-index declarations (ADR-010 §3): each index needs a non-empty name unique within the
        // queue, and a non-empty list of non-empty field names. Field names are NOT checked against pushed
        // items — fields are dynamic per item, and a missing field simply leaves the item out of the index
        // (sparse rule).
        let mut seen_index_names = std::collections::BTreeSet::new();
        for spec in &self.secondary_indexes {
            if spec.name.trim().is_empty() {
                return Err(CreateQueueError::invalid_request(
                    "secondary index name must not be empty",
                ));
            }
            if !seen_index_names.insert(spec.name.as_str()) {
                return Err(CreateQueueError::invalid_request(
                    "secondary index names must be unique within the queue",
                ));
            }
            if spec.fields.is_empty() {
                return Err(CreateQueueError::invalid_request(
                    "secondary index must declare at least one field",
                ));
            }
            if spec.fields.iter().any(|f| f.trim().is_empty()) {
                return Err(CreateQueueError::invalid_request(
                    "secondary index field name must not be empty",
                ));
            }
        }

        // Typed-index declarations (ADR-011): each QueueIndex needs a non-empty name unique within
        // the queue. Names must not collide with legacy secondary_indexes (mixing both forms for the
        // same logical index is a QueueDefinitionConflict). A per-queue cap of 32 typed indexes is
        // enforced; compound indexes must have at least one field.
        const MAX_TYPED_INDEXES: usize = 32;
        if self.typed_indexes.len() > MAX_TYPED_INDEXES {
            return Err(CreateQueueError::invalid_request(format!(
                "typed_indexes: at most {MAX_TYPED_INDEXES} typed indexes are allowed per queue"
            )));
        }
        let mut seen_typed_names = std::collections::BTreeSet::new();
        for idx in &self.typed_indexes {
            if idx.name.trim().is_empty() {
                return Err(CreateQueueError::invalid_request(
                    "typed index name must not be empty",
                ));
            }
            if !seen_typed_names.insert(idx.name.as_str()) {
                return Err(CreateQueueError::invalid_request(format!(
                    "typed index names must be unique within the queue: '{}' appears more than once",
                    idx.name
                )));
            }
            match &idx.declaration {
                IndexDeclaration::Compound(c) if c.fields.is_empty() => {
                    return Err(CreateQueueError::invalid_request(format!(
                        "typed index '{}': compound index must declare at least one field",
                        idx.name
                    )));
                }
                _ => {}
            }
        }
        // QueueDefinitionConflict: typed_indexes and secondary_indexes must not share a name
        // (the same logical index cannot be declared in both forms simultaneously).
        for idx in &self.typed_indexes {
            if seen_index_names.contains(idx.name.as_str()) {
                return Err(CreateQueueError::conflict(format!(
                    "index '{}' appears in both secondary_indexes (ADR-010) and typed_indexes \
                     (ADR-011); declare it in one form only",
                    idx.name
                )));
            }
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
            max_rank_error: self.max_rank_error,
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
            terminal_retention_ms: self.terminal_retention_ms,
            max_lease_duration_ms: self.max_lease_duration_ms,
            retry_policy: self.retry_policy,
            max_push_batch_size: self.max_push_batch_size,
            max_claim_batch_size: self.max_claim_batch_size,
            max_eligible_group_size: self.max_eligible_group_size,
            secondary_indexes: self.secondary_indexes,
            entity_schema: self.entity_schema,
            typed_indexes: self.typed_indexes,
            emit_change_records: self.emit_change_records,
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

    /// Whether lifecycle pushes staged across multiple `commit_transition` entries need full-set
    /// unique-index validation (fireweed-a355d82b).
    ///
    /// Queues without unique secondary/typed indexes can validate each entry's push delta alone —
    /// re-validating the entire staged set every entry made per-entry commit cost superlinear.
    /// Unique indexes still require the full staged candidate so within-commit cross-entry
    /// uniqueness is caught before the durable append.
    pub fn requires_cross_entry_push_validation(&self) -> bool {
        if self.secondary_indexes.iter().any(|spec| spec.unique) {
            return true;
        }
        self.typed_indexes
            .iter()
            .any(|index| match &index.declaration {
                IndexDeclaration::Single(def) => def.unique,
                IndexDeclaration::Compound(def) => def.unique,
            })
    }
}

// ---------------------------------------------------------------------------
// B-011: priority_sort encoding
// ---------------------------------------------------------------------------

/// Encode a PriorityValue as sortable bytes for the given model.
///
/// Ascending: smaller values → smaller byte sequences.
/// Descending: all bits flipped relative to ascending.
pub fn priority_sort(value: &PriorityValue, model: &PriorityModel) -> Vec<u8> {
    let mut bytes = encode_priority_ascending(value);
    if model.direction == PriorityDirection::Descending {
        for b in bytes.iter_mut() {
            *b ^= 0xff;
        }
    }
    bytes
}

fn encode_priority_ascending(value: &PriorityValue) -> Vec<u8> {
    match value {
        PriorityValue::Timestamp(ts) => {
            let mut b = Vec::with_capacity(12);
            // Flip sign bit so i64 byte order matches ascending order.
            let s = (ts.seconds as u64) ^ (1u64 << 63);
            b.extend_from_slice(&s.to_be_bytes());
            b.extend_from_slice(&ts.nanoseconds.to_be_bytes());
            b
        }
        PriorityValue::Int64(v) => {
            // Flip sign bit to map i64 order onto u64 byte order.
            let u = (*v as u64) ^ (1u64 << 63);
            u.to_be_bytes().to_vec()
        }
        PriorityValue::Decimal(d) => encode_decimal_ascending(d.mantissa, d.scale),
        PriorityValue::Text(s) => {
            // Append a null terminator so empty strings get a byte to invert
            // (0x00 → 0xff) and sort last in descending mode.
            let mut b = s.as_bytes().to_vec();
            b.push(0x00);
            b
        }
    }
}

// Precomputed powers of ten up to 10^38 (fits in u128).
const POW10: [u128; 39] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
    100_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000_000,
];

fn decimal_digit_count(n: u128) -> u32 {
    if n == 0 {
        return 1;
    }
    let mut n = n;
    let mut count = 0u32;
    while n > 0 {
        n /= 10;
        count += 1;
    }
    count
}

/// Encode a decimal value as 21 sortable bytes (ascending order).
///
/// Layout: 1 sign byte | 4 biased-exponent bytes | 16 normalized-mantissa bytes.
///
/// Zero encodes with sign byte 0x80 and all-zero remainder.
/// Positive values use sign byte 0xc0; the exponent and mantissa are left as-is.
/// Negative values use sign byte 0x40; exponent and mantissa are bitwise-inverted
/// so that larger absolute values sort as smaller.
fn encode_decimal_ascending(mantissa: i128, scale: u32) -> Vec<u8> {
    const NORMALIZED_DIGITS: u32 = 38;
    let mut out = vec![0u8; 21];

    if mantissa == 0 {
        out[0] = 0x80;
        return out;
    }

    let positive = mantissa > 0;
    let abs_m = mantissa.unsigned_abs();

    let digits = decimal_digit_count(abs_m);
    let effective_exp: i32 = digits as i32 - 1 - scale as i32;
    let biased_exp = ((effective_exp as i64).wrapping_add(1i64 << 31)) as u32;

    // Normalize to NORMALIZED_DIGITS significant digits.
    let fractional: u128 = if digits >= NORMALIZED_DIGITS {
        abs_m / POW10[(digits - NORMALIZED_DIGITS) as usize]
    } else {
        abs_m * POW10[(NORMALIZED_DIGITS - digits) as usize]
    };

    if positive {
        out[0] = 0xc0;
        out[1..5].copy_from_slice(&biased_exp.to_be_bytes());
        out[5..21].copy_from_slice(&fractional.to_be_bytes());
    } else {
        out[0] = 0x40;
        out[1..5].copy_from_slice(&(!biased_exp).to_be_bytes());
        out[5..21].copy_from_slice(&(!fractional).to_be_bytes());
    }

    out
}

// ---------------------------------------------------------------------------
// B-012: item lifecycle state machine
// ---------------------------------------------------------------------------

/// Lifecycle state of a fireweed item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ItemState {
    Pending,
    Leased,
    /// Terminal: successfully completed.
    Complete,
    /// Terminal: failed (retry budget exhausted or explicit terminal fail).
    Failed,
}

impl ItemState {
    pub fn is_terminal(self) -> bool {
        matches!(self, ItemState::Complete | ItemState::Failed)
    }
}

/// Events that drive the item lifecycle state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemEvent {
    /// A worker successfully claimed the item.
    Claim,
    /// Worker finalized with a successful complete outcome.
    FinalizeComplete,
    /// Worker finalized with a terminal fail outcome (retry budget exhausted).
    FinalizeFail,
    /// Worker finalized with a retryable failure; item returns to pending.
    FinalizeRetry,
    /// Worker released the item without consuming a retry attempt.
    FinalizeRelease,
    /// Worker rearmed a recurring item; resets attempt count, returns to pending.
    FinalizeRearm,
    /// The active lease expired; item returns to pending.
    LeaseExpired,
}

/// Error returned when an illegal state transition is attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionError {
    pub state: ItemState,
    pub event: ItemEvent,
    pub message: &'static str,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "illegal transition: {:?} + {:?} — {}",
            self.state, self.event, self.message
        )
    }
}

impl Error for TransitionError {}

/// Apply an event to the current item state, returning the next state.
///
/// Returns `Err(TransitionError)` for any illegal (state, event) pair.
pub fn apply_transition(state: ItemState, event: ItemEvent) -> Result<ItemState, TransitionError> {
    match (state, event) {
        (ItemState::Pending, ItemEvent::Claim) => Ok(ItemState::Leased),

        (ItemState::Leased, ItemEvent::FinalizeComplete) => Ok(ItemState::Complete),
        (ItemState::Leased, ItemEvent::FinalizeFail) => Ok(ItemState::Failed),
        (ItemState::Leased, ItemEvent::FinalizeRetry) => Ok(ItemState::Pending),
        (ItemState::Leased, ItemEvent::FinalizeRelease) => Ok(ItemState::Pending),
        (ItemState::Leased, ItemEvent::FinalizeRearm) => Ok(ItemState::Pending),
        (ItemState::Leased, ItemEvent::LeaseExpired) => Ok(ItemState::Pending),

        (ItemState::Complete, _) | (ItemState::Failed, _) => Err(TransitionError {
            state,
            event,
            message: "item is terminal; no further transitions are accepted",
        }),

        (ItemState::Pending, _) => Err(TransitionError {
            state,
            event,
            message: "event requires a leased item",
        }),

        (ItemState::Leased, ItemEvent::Claim) => Err(TransitionError {
            state,
            event,
            message: "item is already leased",
        }),
    }
}

#[allow(dead_code)]
pub type DomainResult<T> = Result<T, CreateQueueError>;

// ---------------------------------------------------------------------------
// B-012 cont.: retry exhaustion (AC-CORE-4)
// ---------------------------------------------------------------------------

/// Whether a failure at the given attempt count should terminate the item.
///
/// Returns `true` when `attempts_so_far >= max_attempts`, meaning the next
/// failure event must be `FinalizeFail` (→ `Failed`) rather than
/// `FinalizeRetry` (→ `Pending`).
pub fn is_retry_exhausted(attempts_so_far: u32, max_attempts: u32) -> bool {
    attempts_so_far >= max_attempts
}

/// The event to apply for a failure, given the current attempt count.
///
/// Callers use this to choose between `FinalizeRetry` and `FinalizeFail`.
pub fn failure_event(attempts_so_far: u32, max_attempts: u32) -> ItemEvent {
    if is_retry_exhausted(attempts_so_far, max_attempts) {
        ItemEvent::FinalizeFail
    } else {
        ItemEvent::FinalizeRetry
    }
}

// ---------------------------------------------------------------------------
// B-012 cont.: idempotency rules (AC-CORE-3)
// ---------------------------------------------------------------------------

/// Canonical body hash used for request-id conflict detection.
///
/// Two push bodies are considered identical when their hash matches.
/// A hash collision is treated as a match (safe: the worst case is a
/// duplicate being misidentified as a replay).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BodyHash(pub u64);

/// The outcome of an idempotency check against existing request records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    /// No prior record; proceed with the operation.
    Proceed,
    /// Prior `request_id` record with identical body; replay the prior outcome.
    Replay,
    /// Prior `request_id` record with a different body; reject with conflict.
    RequestIdConflict,
    /// Item with the same `client_item_key` already exists (non-terminal).
    ClientItemKeyDuplicate,
}

/// Idempotency check given a prior request record (if any) and a prior item
/// record (if any), both looked up by their respective keys.
pub fn check_idempotency(
    request_id: &RequestId,
    body_hash: BodyHash,
    prior_request: Option<(RequestId, BodyHash)>,
    prior_item_key: Option<&ClientItemKey>,
    client_item_key: &ClientItemKey,
) -> IdempotencyOutcome {
    // request_id check takes priority.
    if let Some((_prior_rid, prior_hash)) = prior_request.filter(|(rid, _)| rid == request_id) {
        return if prior_hash == body_hash {
            IdempotencyOutcome::Replay
        } else {
            IdempotencyOutcome::RequestIdConflict
        };
    }
    // client_item_key duplicate check.
    if prior_item_key.is_some_and(|k| k == client_item_key) {
        return IdempotencyOutcome::ClientItemKeyDuplicate;
    }
    IdempotencyOutcome::Proceed
}

// ---------------------------------------------------------------------------
// B-012 cont.: eligibility precedence evaluator (AC-CLAIM-3 pure layer)
// ---------------------------------------------------------------------------

/// Snapshot of an item's scheduling state for pure eligibility evaluation.
#[derive(Debug, Clone)]
pub struct EligibilitySnapshot {
    /// Item must be in Pending state to be eligible.
    pub state: ItemState,
    /// Earliest wall-clock time the item is eligible for claim; None = immediately.
    pub not_before: Option<UtcTimestamp>,
    /// Retry backoff: item is ineligible until this time (None = no backoff).
    pub retry_backoff_until: Option<UtcTimestamp>,
    /// Item-level metadata (checked against queue-level blockers).
    pub metadata: Metadata,
    /// Active gate keys on this item (blocked keys make item ineligible).
    pub gate_keys: Vec<String>,
}

/// Queue-level eligibility rules.
#[derive(Debug, Clone)]
pub struct QueueEligibilityRules {
    /// Keys in `metadata_blockers` map to sets of values that block eligibility.
    pub metadata_blockers: std::collections::BTreeMap<String, Vec<MetadataValue>>,
    /// Gate keys that are currently in a `blocked` state for this queue.
    pub blocked_gate_keys: std::collections::HashSet<String>,
}

/// Reasons an item may be ineligible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IneligibilityReason {
    NotPending,
    NotBeforeInFuture,
    RetryBackoff,
    MetadataBlocked { key: String },
    GateBlocked { gate_key: String },
}

/// Evaluate whether an item is eligible for claim at `now`.
///
/// Returns `Ok(())` if eligible, or the first `IneligibilityReason` found
/// following the Eligibility Precedence order.
///
/// NOTE (BQ-14d): this is the *reference* eligibility specification, not the live claim path of any
/// backend. The relational family re-expresses this predicate in SQL (incl. the gate anti-join); the
/// in-memory family re-expresses it inline in `ProjectionData::eligible_candidates` and does NOT consult
/// gates (gates are relational-mode only). A passing `GateBlocked` test here does NOT imply the in-memory
/// claim path enforces gates — see `PushItem.gate_keys` scope.
pub fn evaluate_eligibility(
    snapshot: &EligibilitySnapshot,
    rules: &QueueEligibilityRules,
    now: &UtcTimestamp,
) -> Result<(), IneligibilityReason> {
    if snapshot.state != ItemState::Pending {
        return Err(IneligibilityReason::NotPending);
    }

    if snapshot
        .not_before
        .as_ref()
        .is_some_and(|nb| cmp_timestamp(nb, now) == std::cmp::Ordering::Greater)
    {
        return Err(IneligibilityReason::NotBeforeInFuture);
    }

    if snapshot
        .retry_backoff_until
        .as_ref()
        .is_some_and(|b| cmp_timestamp(b, now) == std::cmp::Ordering::Greater)
    {
        return Err(IneligibilityReason::RetryBackoff);
    }

    for (key, blocked_values) in &rules.metadata_blockers {
        if snapshot
            .metadata
            .get(key)
            .is_some_and(|v| blocked_values.contains(v))
        {
            return Err(IneligibilityReason::MetadataBlocked { key: key.clone() });
        }
    }

    for gate_key in &snapshot.gate_keys {
        if rules.blocked_gate_keys.contains(gate_key.as_str()) {
            return Err(IneligibilityReason::GateBlocked {
                gate_key: gate_key.clone(),
            });
        }
    }

    Ok(())
}

fn cmp_timestamp(a: &UtcTimestamp, b: &UtcTimestamp) -> std::cmp::Ordering {
    a.seconds
        .cmp(&b.seconds)
        .then(a.nanoseconds.cmp(&b.nanoseconds))
}

#[cfg(test)]
mod coverage_tests {
    //! Targeted unit tests that exercise the mechanical impls (serde, conversions,
    //! Display, validation/error branches, decimal encoding) the release coverage
    //! gate flagged as unhit. Tests only — no production logic lives here.
    use super::*;

    fn valid_create_queue() -> CreateQueue {
        CreateQueue {
            tenant_id: TenantId::new("tenant_acme").unwrap(),
            queue_id: QueueId::new("scheduled_actions").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 10_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: CohortPolicy::disabled(),
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 3_600_000,
            client_item_key_retention_ms: 86_400_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 5 },
            max_push_batch_size: 100,
            max_claim_batch_size: 50,
            max_eligible_group_size: Some(25),
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records: true,
        }
    }

    fn policy() -> QueueCreationPolicy {
        QueueCreationPolicy {
            default_max_gate_keys_per_item: 12,
            default_max_gates_per_request: 6,
        }
    }

    #[test]
    fn identifier_newtype_conversions_and_serde_round_trip() {
        let id = TenantId::new("tenant_acme").unwrap();

        // AsRef<str>
        let as_ref: &str = id.as_ref();
        assert_eq!(as_ref, "tenant_acme");

        // From<$name> for String
        let owned: String = String::from(id.clone());
        assert_eq!(owned, "tenant_acme");

        // Display
        assert_eq!(format!("{id}"), "tenant_acme");

        // Serialize -> Deserialize round trip
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"tenant_acme\"");
        let back: TenantId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);

        // Deserialize error branch: empty string fails the validating constructor.
        let empty = serde_json::from_str::<TenantId>("\"\"");
        assert!(empty.is_err());

        // Deserialize error branch: wrong wire type (number, not string).
        let wrong_type = serde_json::from_str::<TenantId>("123");
        assert!(wrong_type.is_err());

        // Other id newtypes exercise the same generated impls.
        let queue = QueueId::new("q1").unwrap();
        assert_eq!(queue.as_ref(), "q1");
        let q_back: QueueId =
            serde_json::from_str(&serde_json::to_string(&queue).unwrap()).unwrap();
        assert_eq!(q_back, queue);
    }

    #[test]
    fn item_id_serde_round_trip_and_error_branch() {
        let id = ItemId::mint(7, 3, 42);

        // Serialize is the decimal string of the packed value.
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{}\"", id.as_u64()));

        // Deserialize valid string -> equal value.
        let back: ItemId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);

        // Deserialize error branch: not a u64 decimal string.
        let bad = serde_json::from_str::<ItemId>("\"not-a-number\"");
        assert!(bad.is_err());

        // Accessors.
        assert_eq!(id.epoch(), 7);
        assert_eq!(id.node(), 3);
        assert_eq!(id.counter(), 42);
        assert_eq!(ItemId::from_u64(id.as_u64()), id);
    }

    #[test]
    fn small_helpers_and_constructors() {
        assert_eq!(default_max_rank_error(), 0);
        assert_eq!(default_terminal_retention_ms(), 3_600_000);

        let mut entries = BTreeMap::new();
        entries.insert("k".to_string(), MetadataValue::Bool(true));
        let md = Metadata::from_entries(entries);
        assert_eq!(md.len(), 1);
        assert_eq!(md.get("k"), Some(&MetadataValue::Bool(true)));

        let creation = QueueCreationPolicy::default();
        assert_eq!(creation.default_max_gate_keys_per_item, 1);
        assert_eq!(creation.default_max_gates_per_request, 1);
    }

    #[test]
    fn create_queue_error_display() {
        let mut request = valid_create_queue();
        request.progress_bound_ms = 0;
        let err = request.validate(&policy()).unwrap_err();
        let rendered = format!("{err}");
        assert!(rendered.contains("InvalidRequest"));
        assert!(rendered.contains("progress_bound_ms"));
    }

    #[test]
    fn max_rank_error_requires_bounded_relaxed() {
        let mut request = valid_create_queue();
        request.max_rank_error = 5;
        request.ordering_mode = OrderingMode::Strict;
        let err = request.validate(&policy()).unwrap_err();
        assert_eq!(err.kind, CreateQueueErrorKind::InvalidRequest);
        assert!(err.message.contains("max_rank_error"));

        // Ok side: bounded-relaxed with a non-zero bound validates.
        let mut ok = valid_create_queue();
        ok.max_rank_error = 5;
        ok.ordering_mode = OrderingMode::BoundedRelaxed;
        assert!(ok.validate(&policy()).is_ok());
    }

    #[test]
    fn max_eligible_group_size_must_not_exceed_claim_batch() {
        let mut request = valid_create_queue();
        request.max_eligible_group_size = Some(1_000);
        request.max_claim_batch_size = 50;
        let err = request.validate(&policy()).unwrap_err();
        assert_eq!(err.kind, CreateQueueErrorKind::QueueDefinitionConflict);
        assert!(err.message.contains("max_eligible_group_size"));
    }

    #[test]
    fn cohort_max_size_must_not_exceed_claim_batch() {
        let mut request = valid_create_queue();
        request.max_claim_batch_size = 5;
        request.max_eligible_group_size = None;
        request.cohort_policy = CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(9_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(10),
        };
        let err = request.validate(&policy()).unwrap_err();
        assert_eq!(err.kind, CreateQueueErrorKind::QueueDefinitionConflict);
        assert!(err.message.contains("max_cohort_size"));
    }

    #[test]
    fn cohort_enabled_valid_definition_carries_policy() {
        let mut request = valid_create_queue();
        request.cohort_policy = CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(9_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(10),
        };
        let definition = request.validate(&policy()).unwrap();
        let cohort = definition.cohort_policy.expect("cohort policy retained");
        assert!(cohort.enabled);
        assert_eq!(cohort.max_cohort_size, Some(10));
    }

    fn index_request(specs: Vec<IndexSpec>) -> CreateQueue {
        let mut request = valid_create_queue();
        request.secondary_indexes = specs;
        request
    }

    #[test]
    fn secondary_index_validation_branches() {
        // Empty index name.
        let err = index_request(vec![IndexSpec {
            name: "  ".to_string(),
            fields: vec!["region".to_string()],
            unique: false,
        }])
        .validate(&policy())
        .unwrap_err();
        assert!(err.message.contains("index name must not be empty"));

        // Duplicate index names.
        let err = index_request(vec![
            IndexSpec {
                name: "by_region".to_string(),
                fields: vec!["region".to_string()],
                unique: false,
            },
            IndexSpec {
                name: "by_region".to_string(),
                fields: vec!["zone".to_string()],
                unique: false,
            },
        ])
        .validate(&policy())
        .unwrap_err();
        assert!(err.message.contains("unique within the queue"));

        // No fields declared.
        let err = index_request(vec![IndexSpec {
            name: "by_region".to_string(),
            fields: vec![],
            unique: false,
        }])
        .validate(&policy())
        .unwrap_err();
        assert!(err.message.contains("at least one field"));

        // Empty field name.
        let err = index_request(vec![IndexSpec {
            name: "by_region".to_string(),
            fields: vec!["region".to_string(), "  ".to_string()],
            unique: true,
        }])
        .validate(&policy())
        .unwrap_err();
        assert!(err.message.contains("field name must not be empty"));

        // Ok side: a well-formed unique index validates.
        let definition = index_request(vec![IndexSpec {
            name: "by_region".to_string(),
            fields: vec!["region".to_string(), "zone".to_string()],
            unique: true,
        }])
        .validate(&policy())
        .unwrap();
        assert_eq!(definition.secondary_indexes.len(), 1);
    }

    #[test]
    fn queue_definition_serde_defaults_terminal_retention() {
        // A persisted definition missing terminal_retention_ms must rehydrate via the
        // serde default (exercising default_terminal_retention_ms through serde).
        let mut request = valid_create_queue();
        request.terminal_retention_ms = 3_600_000;
        let definition = request.validate(&policy()).unwrap();
        let mut json: serde_json::Value = serde_json::to_value(&definition).unwrap();
        json.as_object_mut()
            .unwrap()
            .remove("terminal_retention_ms");
        let restored: QueueDefinition = serde_json::from_value(json).unwrap();
        assert_eq!(restored.terminal_retention_ms, 3_600_000);
    }

    #[test]
    fn decimal_digit_count_handles_zero_and_multi_digit() {
        assert_eq!(decimal_digit_count(0), 1);
        assert_eq!(decimal_digit_count(7), 1);
        assert_eq!(decimal_digit_count(12_345), 5);
    }

    #[test]
    fn decimal_encoding_zero_small_and_large_mantissa() {
        // Zero encodes with the 0x80 sign byte and an all-zero remainder.
        let zero = encode_decimal_ascending(0, 0);
        assert_eq!(zero.len(), 21);
        assert_eq!(zero[0], 0x80);
        assert!(zero[1..].iter().all(|&b| b == 0));

        // Positive value: 0xc0 sign byte.
        let pos = encode_decimal_ascending(12_345, 2);
        assert_eq!(pos[0], 0xc0);

        // Negative value: 0x40 sign byte.
        let neg = encode_decimal_ascending(-12_345, 2);
        assert_eq!(neg[0], 0x40);

        // Larger absolute negative sorts before (less than) a smaller one.
        let neg_big = encode_decimal_ascending(-99_999, 0);
        assert!(neg_big < neg);

        // >= 38 significant digits exercises the divide-down normalization branch.
        let huge = 12_345_678_901_234_567_890_123_456_789_012_345_678_i128; // 38 digits
        let enc = encode_decimal_ascending(huge, 0);
        assert_eq!(enc[0], 0xc0);
        assert_eq!(enc.len(), 21);

        // Reached through the public priority_sort entry point (descending inverts bytes).
        let model = PriorityModel {
            kind: PriorityModelKind::Decimal,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::ItemId,
        };
        let sorted = priority_sort(
            &PriorityValue::Decimal(DecimalValue {
                mantissa: 42,
                scale: 0,
            }),
            &model,
        );
        assert_eq!(sorted.len(), 21);
    }

    #[test]
    fn validate_remaining_error_branches() {
        // terminal_retention_ms == 0
        let mut request = valid_create_queue();
        request.terminal_retention_ms = 0;
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("terminal_retention_ms"));

        // timestamp priority queues must use created_sequence tie breaking
        let mut request = valid_create_queue();
        request.priority_model = PriorityModel {
            kind: PriorityModelKind::Timestamp,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::ItemId,
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("created_sequence"));

        // cohort fields present while cohort disabled
        let mut request = valid_create_queue();
        request.cohort_policy = CohortPolicy {
            enabled: false,
            completion_bound_ms: Some(1_000),
            on_incomplete: None,
            max_cohort_size: None,
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("must be omitted"));

        // recurrence.until present on a oneshot queue
        let mut request = valid_create_queue();
        request.recurrence = RecurrencePolicy {
            mode: RecurrenceMode::Oneshot,
            until: Some(UtcTimestamp::new(1_700_000_000, 0).unwrap()),
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("recurrence.until"));

        // recurrence.until required when recurring
        let mut request = valid_create_queue();
        request.recurrence = RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: None,
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(
            err.message
                .contains("required when recurrence.mode=recurring")
        );

        // max_eligible_group_size == 0
        let mut request = valid_create_queue();
        request.max_eligible_group_size = Some(0);
        let err = request.validate(&policy()).unwrap_err();
        assert!(
            err.message
                .contains("max_eligible_group_size must be greater")
        );

        // None branch of the max_eligible_group_size guard validates.
        let mut request = valid_create_queue();
        request.max_eligible_group_size = None;
        assert!(request.validate(&policy()).is_ok());

        // gate-key caps present while gate_keys=none
        let mut request = valid_create_queue();
        request.eligibility_policy = EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::None,
            max_gate_keys_per_item: Some(3),
            max_gates_per_request: None,
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("gate-key caps must be omitted"));
    }

    #[test]
    fn validate_short_circuit_branch_arms() {
        // Non-timestamp priority kind short-circuits the timestamp tie-breaker check.
        let mut request = valid_create_queue();
        request.priority_model = PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::ItemId,
        };
        assert!(request.validate(&policy()).is_ok());

        // Cohort disabled with only max_cohort_size set walks the full `||` chain.
        let mut request = valid_create_queue();
        request.cohort_policy = CohortPolicy {
            enabled: false,
            completion_bound_ms: None,
            on_incomplete: None,
            max_cohort_size: Some(3),
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("must be omitted"));

        // A valid recurring queue exercises the `until.is_none()` false arm.
        let mut request = valid_create_queue();
        request.recurrence = RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: Some(UtcTimestamp::new(1_700_000_000, 0).unwrap()),
        };
        assert!(request.validate(&policy()).is_ok());

        // gate_keys=none with only max_gates_per_request set exercises the `||` second arm.
        let mut request = valid_create_queue();
        request.eligibility_policy = EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::None,
            max_gate_keys_per_item: None,
            max_gates_per_request: Some(2),
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("gate-key caps must be omitted"));
    }

    #[test]
    fn validate_dynamic_gate_key_policy_branches() {
        // Happy path: defaults pulled from the creation policy.
        let mut request = valid_create_queue();
        request.eligibility_policy = EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: None,
            max_gates_per_request: None,
        };
        let definition = request.validate(&policy()).unwrap();
        assert_eq!(
            definition.eligibility_policy.max_gate_keys_per_item,
            Some(12)
        );
        assert_eq!(definition.eligibility_policy.max_gates_per_request, Some(6));

        // max_gate_keys_per_item == 0 rejected.
        let mut request = valid_create_queue();
        request.eligibility_policy = EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(0),
            max_gates_per_request: Some(2),
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("max_gate_keys_per_item"));

        // max_gates_per_request == 0 rejected.
        let mut request = valid_create_queue();
        request.eligibility_policy = EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(4),
            max_gates_per_request: Some(0),
        };
        let err = request.validate(&policy()).unwrap_err();
        assert!(err.message.contains("max_gates_per_request"));
    }

    #[test]
    fn transition_error_display() {
        let err = apply_transition(ItemState::Complete, ItemEvent::Claim).unwrap_err();
        let rendered = format!("{err}");
        assert!(rendered.contains("illegal transition"));
        assert!(rendered.contains("Complete"));
        assert!(rendered.contains("Claim"));
    }
}
