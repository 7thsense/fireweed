//! API-007's shared, storage-independent batch queue vocabulary.
//!
//! A commit group owns the atomic boundary. No database transaction handle or
//! backend capability is part of this module. Times are exact signed UTC epoch nanoseconds.
use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QueueAddress {
    pub tenant: String,
    pub queue: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(String),
    Bytes(Vec<u8>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Integer,
    Float,
    Boolean,
    Text,
    Bytes,
}
impl FieldValue {
    pub fn field_type(&self) -> FieldType {
        match self {
            Self::Integer(_) => FieldType::Integer,
            Self::Float(_) => FieldType::Float,
            Self::Boolean(_) => FieldType::Boolean,
            Self::Text(_) => FieldType::Text,
            Self::Bytes(_) => FieldType::Bytes,
        }
    }
}
pub type Fields = BTreeMap<String, FieldValue>;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionDefinition {
    pub fields: BTreeMap<String, FieldType>,
    /// Declared composite query indexes. Every declared field additionally has
    /// an automatic typed index; adapters cannot accept then scan/filter broadly.
    pub indexes: BTreeMap<String, Vec<String>>,
    /// Named declared indexes whose complete nonmissing tuple is unique.
    pub unique_indexes: BTreeSet<String>,
    /// Explicit additional-field namespaces. Every admitted full name/value is
    /// automatically indexed; names retain literal UTF-8 identity and value type.
    pub additional_field_prefixes: BTreeSet<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSchema {
    pub work: CollectionDefinition,
    pub state: BTreeMap<String, CollectionDefinition>,
    pub progress_bound_nanos: u64,
    pub max_attempts: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lifecycle {
    Pending,
    Leased,
    Completed,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewWork {
    pub key: String,
    pub priority: i64,
    pub available_at: i64,
    pub fields: Fields,
    pub body: Bytes,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseCredential {
    pub token: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkRecord {
    pub work: NewWork,
    pub version: u64,
    pub insertion_sequence: u64,
    pub lifecycle: Lifecycle,
    pub eligible_since: i64,
    pub attempts: u32,
    pub lease_expires_at: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedWork {
    pub record: WorkRecord,
    pub lease: LeaseCredential,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateExpect {
    Absent,
    Version(u64),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateRecord {
    pub collection: String,
    pub key: String,
    pub version: u64,
    pub fields: Fields,
    pub value: Bytes,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StateChange {
    Put {
        fields: Fields,
        value: Bytes,
    },
    Delete,
    /// Test StateExpect atomically without changing the row or its version.
    Check,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateUpdate {
    pub collection: String,
    pub key: String,
    pub expect: StateExpect,
    pub change: StateChange,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkPatch {
    pub priority: Option<i64>,
    pub available_at: Option<i64>,
    pub fields: BTreeMap<String, Option<FieldValue>>,
    pub body: Option<Bytes>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Disposition {
    Complete,
    Fail,
    Release,
    RetryAt(i64),
    Retain {
        renew_nanos: Option<u64>,
    },
    Delete,
    /// Test current lease and version atomically; requires an empty patch and
    /// neither renews the lease nor changes the work or its version.
    Check,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedUpdate {
    pub key: String,
    pub lease: LeaseCredential,
    pub expected_version: u64,
    pub patch: WorkPatch,
    pub disposition: Disposition,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdministrativeLifecycle {
    Keep,
    Pending,
    Completed,
    Failed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdministrativeLeaseGuard {
    RejectActive,
    RequireActive,
    InvalidateActive,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnclaimedChange {
    Patch(WorkPatch),
    /// Explicit version-fenced lifecycle control; never transfers lease authority.
    Administrative {
        patch: WorkPatch,
        lifecycle: AdministrativeLifecycle,
        lease_guard: AdministrativeLeaseGuard,
    },
    /// Replace pending work with fresh pending work, resetting attempts and insertion age.
    /// The key must match its update; the durable version still increases.
    Replace(NewWork),
    Delete,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnclaimedUpdate {
    pub key: String,
    pub expect: StateExpect,
    pub change: UnclaimedChange,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeCommit {
    pub intent: crate::queue_administration::AdministrativeIntent,
    pub max_result_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitGroup {
    pub request_id: String,
    pub claimed_updates: Vec<ClaimedUpdate>,
    pub state_updates: Vec<StateUpdate>,
    pub work_inserts: Vec<NewWork>,
    pub work_updates: Vec<UnclaimedUpdate>,
    #[serde(default)]
    pub administration: Option<AdministrativeCommit>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitBatch {
    pub queue: QueueAddress,
    pub groups: Vec<CommitGroup>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreviewRequest {
    pub queue: QueueAddress,
    pub group: CommitGroup,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PreviewOutcome {
    Committed {
        versions: Vec<RecordVersion>,
    },
    AdministrativeCommitted {
        versions: Vec<RecordVersion>,
        administration: crate::queue_administration::AdministrativeResult,
    },
    Rejected(Rejection),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreviewResult {
    pub outcome: PreviewOutcome,
}
impl TryFrom<GroupOutcome> for PreviewResult {
    type Error = QueueError;
    fn try_from(outcome: GroupOutcome) -> QueueResult<Self> {
        Ok(Self {
            outcome: match outcome {
                GroupOutcome::Committed { versions } => PreviewOutcome::Committed { versions },
                GroupOutcome::AdministrativeCommitted {
                    versions,
                    administration,
                } => PreviewOutcome::AdministrativeCommitted {
                    versions,
                    administration,
                },
                GroupOutcome::Rejected(reason) => PreviewOutcome::Rejected(reason),
                GroupOutcome::RequestConflict | GroupOutcome::RequestExpired => {
                    return Err(QueueError::OutcomeUnknown(
                        "preview returned execution receipt status".into(),
                    ));
                }
            },
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rejection {
    StaleLease,
    VersionConflict,
    AlreadyExists,
    NotFound,
    Leased,
    Invalid(String),
    UniqueConflict,
    Terminal,
    TooLarge,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordVersion {
    pub collection: Option<String>, // None is work, Some is state.
    pub key: String,
    pub version: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GroupOutcome {
    RequestConflict,
    RequestExpired,
    Committed {
        versions: Vec<RecordVersion>,
    },
    Rejected(Rejection),
    AdministrativeCommitted {
        versions: Vec<RecordVersion>,
        administration: crate::queue_administration::AdministrativeResult,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitReceipt {
    pub request_id: String,
    pub outcome: GroupOutcome,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReceiptLookup {
    Found(CommitReceipt),
    Absent,
    Expired,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueError {
    Invalid(String),
    TooLarge,
    QueueNotFound,
    DefinitionConflict,
    RequestConflict,
    RequestExpired,
    ClaimExpired,
    Busy,
    /// No success or rejection can be inferred. Resolve using the request identity.
    OutcomeUnknown(String),
}
pub type QueueResult<T> = Result<T, QueueError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Bound {
    Included(FieldValue),
    Excluded(FieldValue),
}
impl Bound {
    fn value(&self) -> &FieldValue {
        match self {
            Self::Included(v) | Self::Excluded(v) => v,
        }
    }
}
/// Predicates never match absent fields. Text/bytes compare bytewise; numeric
/// fields compare numerically. No coercion or database collation is permitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    Equal(FieldValue),
    In(Vec<FieldValue>),
    Range {
        lower: Option<Bound>,
        upper: Option<Bound>,
    },
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
/// Each admitted declared-field predicate has an automatically provisioned
/// typed field index. Conjunctions use storage-side index intersections, never
/// broad leasing/client filtering. Key sets use the intrinsic key index.
pub struct Filter {
    pub fields: BTreeMap<String, Predicate>,
    pub keys: Option<BTreeSet<String>>,
    pub lifecycle: Option<Lifecycle>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimOrder {
    PriorityAscending,
    PriorityDescending,
    ScheduledAscending,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub queue: QueueAddress,
    pub request_id: String,
    pub filter: Filter,
    pub order: ClaimOrder,
    pub eligible_at: i64,
    pub lease_nanos: u64,
    pub max_records: u32,
    pub max_bytes: u64,
    /// None returns all fields, Some(empty) returns no fields. Body separately selected.
    pub fields: Option<BTreeSet<String>>,
    pub include_body: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InsertMode {
    InsertOnly,
    Upsert,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertRequest {
    pub queue: QueueAddress,
    pub request_id: String,
    pub mode: InsertMode,
    pub items: Vec<NewWork>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InsertOutcome {
    Inserted { key: String, version: u64 },
    Replaced { key: String, version: u64 },
    Rejected { key: String, reason: Rejection },
}
/// Numeric interval bounds are exact values of one scalar type; no coercion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericBucket {
    pub label: String,
    pub lower_inclusive: FieldValue,
    pub upper_exclusive: FieldValue,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericBucketDimension {
    pub field: String,
    pub buckets: Vec<NumericBucket>,
    pub missing_label: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCounts {
    pub label: String,
    pub counts: Counts,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsRequest {
    pub buckets: Option<NumericBucketDimension>,
    pub queue: QueueAddress,
    pub filter: Filter,
    pub group_by: Vec<String>,
    pub eligible_at: i64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub pending: u64,
    pub leased: u64,
    pub completed: u64,
    pub failed: u64,
    pub oldest_eligible_since: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsGroup {
    pub values: Vec<Option<FieldValue>>,
    pub counts: Counts,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueStats {
    /// Declaration-order bins, including zero counts, followed by missing.
    pub buckets: Vec<BucketCounts>,
    pub totals: Counts,
    pub groups: Vec<StatsGroup>,
    /// Signed Unix nanoseconds from the operational clock sampled under this
    /// result's authority. The same sample classifies current lease expiry.
    pub as_of: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordSource {
    Work,
    State(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateReadRequest {
    pub queue: QueueAddress,
    pub collection: String,
    pub keys: Vec<String>,
    pub max_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    pub queue: QueueAddress,
    pub source: RecordSource,
    pub index: String,
    pub filter: Filter,
    pub descending: bool,
    pub cursor: Option<Vec<u8>>,
    pub max_records: u32,
    pub max_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryRecord {
    Work(WorkRecord),
    State(StateRecord),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPage {
    pub records: Vec<QueryRecord>,
    pub cursor: Option<Vec<u8>>,
}

/// One normalized admission configuration is used by both implementations.
/// Defaults cover 1 MiB transitions, 512 effects + 512 awaits, and 50 MiB hot
/// history with headroom; final Snorri accumulated-state measurement gates activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLimits {
    pub max_records: u32,
    pub max_groups: u32,
    pub max_group_bytes: u64,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_body_bytes: u64,
    pub max_state_bytes: u64,
}
impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_records: 4096,
            max_groups: 64,
            max_group_bytes: 128 << 20,
            max_request_bytes: 256 << 20,
            max_response_bytes: 128 << 20,
            max_body_bytes: 1 << 20,
            max_state_bytes: 64 << 20,
        }
    }
}
/// Exact conversion; i128 intermediate preserves the negative i64 boundary.
pub fn timestamp_nanos(value: crate::UtcTimestamp) -> QueueResult<i64> {
    if value.nanoseconds >= 1_000_000_000 {
        return Err(QueueError::Invalid("invalid nanosecond component".into()));
    }
    let exact = i128::from(value.seconds) * 1_000_000_000 + i128::from(value.nanoseconds);
    i64::try_from(exact).map_err(|_| {
        QueueError::Invalid("timestamp outside signed nanosecond storage interval".into())
    })
}
pub fn timestamp_from_nanos(value: i64) -> crate::UtcTimestamp {
    crate::UtcTimestamp {
        seconds: value.div_euclid(1_000_000_000),
        nanoseconds: value.rem_euclid(1_000_000_000) as u32,
    }
}
pub(crate) fn invalid(message: &str) -> QueueError {
    QueueError::Invalid(message.into())
}
pub(crate) fn name(value: &str) -> QueueResult<()> {
    if value.trim().is_empty() {
        return Err(invalid("invalid identifier"));
    }
    Ok(())
}
impl QueueAddress {
    pub fn validate(&self) -> QueueResult<()> {
        name(&self.tenant)?;
        name(&self.queue)
    }
}
impl QueueLimits {
    pub fn validate(&self) -> QueueResult<()> {
        if self.max_records == 0
            || self.max_groups == 0
            || self.max_body_bytes == 0
            || self.max_state_bytes == 0
            || self.max_response_bytes == 0
            || self.max_group_bytes > self.max_request_bytes
            || self.max_body_bytes > self.max_group_bytes
            || self.max_state_bytes > self.max_group_bytes
        {
            return Err(invalid("inconsistent admission limits"));
        }
        Ok(())
    }
}
struct CanonicalWriter {
    hash: Option<Sha256>,
    bytes: u64,
    limit: u64,
}
impl std::io::Write for CanonicalWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .filter(|next| *next <= self.limit)
            .ok_or_else(|| std::io::Error::other("canonical byte limit"))?;
        if let Some(hash) = &mut self.hash {
            hash.update(bytes);
        }
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Domain-separated versioned canonical encoding: sorted map keys, preserved
/// vector order and exact byte values. Streaming avoids a second full body copy.
pub fn fingerprint<T: Serialize>(domain: &str, value: &T) -> QueueResult<[u8; 32]> {
    fingerprint_format(b"fireweed-api007-v3\0", domain, value)
}
fn fingerprint_format<T: Serialize>(
    format: &[u8],
    domain: &str,
    value: &T,
) -> QueueResult<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(format);
    hash.update((domain.len() as u64).to_le_bytes());
    hash.update(domain.as_bytes());
    let writer = CanonicalWriter {
        hash: Some(hash),
        bytes: 0,
        limit: u64::MAX,
    };
    let writer =
        postcard::to_io(value, writer).map_err(|_| invalid("canonical encoding failed"))?;
    Ok(writer.hash.expect("hash writer").finalize().into())
}
pub fn encoded_len<T: Serialize>(value: &T) -> QueueResult<u64> {
    let writer = CanonicalWriter {
        hash: None,
        bytes: 0,
        limit: u64::MAX,
    };
    Ok(postcard::to_io(value, writer)
        .map_err(|_| invalid("canonical encoding failed"))?
        .bytes)
}
fn byte_limit<T: Serialize>(value: &T, max: u64) -> QueueResult<()> {
    let writer = CanonicalWriter {
        hash: None,
        bytes: 0,
        limit: max,
    };
    postcard::to_io(value, writer).map_err(|_| QueueError::TooLarge)?;
    Ok(())
}
fn field(value: &FieldValue, expected: FieldType) -> QueueResult<()> {
    if value.field_type() != expected {
        return Err(invalid("field type mismatch"));
    }
    if let FieldValue::Float(n) = value
        && !n.is_finite()
    {
        return Err(invalid("float must be finite"));
    }
    Ok(())
}
impl CollectionDefinition {
    pub fn admits_field(&self, key: &str) -> bool {
        self.fields.contains_key(key)
            || self
                .additional_field_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
    }
    /// Validate a storage-side scalar probe using the same rules as Filter.
    pub fn validate_predicate(&self, key: &str, predicate: &Predicate) -> QueueResult<()> {
        validate_field_predicate(self, key, predicate)
    }
    pub fn validate_value(&self, key: &str, value: &FieldValue) -> QueueResult<()> {
        name(key)?;
        if let Some(expected) = self.fields.get(key) {
            field(value, *expected)
        } else if self.admits_field(key) {
            field(value, value.field_type())
        } else {
            Err(invalid("undeclared field"))
        }
    }
}
fn fields(values: &Fields, definition: &CollectionDefinition) -> QueueResult<()> {
    for (key, value) in values {
        definition.validate_value(key, value)?;
    }
    Ok(())
}
impl QueueSchema {
    pub fn validate(&self) -> QueueResult<()> {
        if self.progress_bound_nanos == 0 || self.max_attempts == 0 {
            return Err(invalid("progress bound must be positive"));
        }
        for (collection, definition) in std::iter::once(("work", &self.work))
            .chain(self.state.iter().map(|(key, value)| (key.as_str(), value)))
        {
            name(collection)?;
            if definition
                .unique_indexes
                .iter()
                .any(|key| !definition.indexes.contains_key(key))
            {
                return Err(invalid("unique index must be declared"));
            }
            for prefix in &definition.additional_field_prefixes {
                name(prefix)?;
                if !prefix.ends_with('/') {
                    return Err(invalid("additional field namespace must end in slash"));
                }
            }
            for key in definition.fields.keys() {
                name(key)?;
            }
            for (index, columns) in &definition.indexes {
                name(index)?;
                if index.starts_with('$') {
                    return Err(invalid("reserved index name"));
                }
                if columns.is_empty() || columns.len() > 8 {
                    return Err(invalid("invalid index width"));
                }
                let mut seen = BTreeSet::new();
                for column in columns {
                    if !definition.fields.contains_key(column) || !seen.insert(column) {
                        return Err(invalid("index needs distinct declared fields"));
                    }
                }
            }
        }
        Ok(())
    }
}
impl Filter {
    pub fn validate(&self, definition: &CollectionDefinition) -> QueueResult<()> {
        if let Some(keys) = &self.keys {
            for key in keys {
                name(key)?;
            }
        }
        for (key, predicate) in &self.fields {
            definition.validate_predicate(key, predicate)?;
        }
        Ok(())
    }
}
pub(crate) fn validate_field_predicate(
    definition: &CollectionDefinition,
    key: &str,
    predicate: &Predicate,
) -> QueueResult<()> {
    name(key)?;
    if !definition.admits_field(key) {
        return Err(invalid("undeclared filter field"));
    }
    match predicate {
        Predicate::Equal(value) => definition.validate_value(key, value)?,
        Predicate::In(values) => {
            if values.is_empty() {
                return Err(invalid("empty membership predicate"));
            }
            for value in values {
                definition.validate_value(key, value)?;
            }
        }
        Predicate::Range { lower, upper } => {
            if lower.is_none() && upper.is_none() {
                return Err(invalid("empty range"));
            }
            for bound in lower.iter().chain(upper.iter()) {
                definition.validate_value(key, bound.value())?;
            }
            if let (Some(lower), Some(upper)) = (lower, upper)
                && matches!(
                    compare_fields(lower.value(), upper.value()),
                    None | Some(std::cmp::Ordering::Greater)
                )
            {
                return Err(invalid("inverted range"));
            }
        }
    }
    Ok(())
}
/// Shared comparison oracle for filters, stats and cursor ordering.
pub fn compare_fields(a: &FieldValue, b: &FieldValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (FieldValue::Integer(a), FieldValue::Integer(b)) => Some(a.cmp(b)),
        (FieldValue::Float(a), FieldValue::Float(b)) if a.is_finite() && b.is_finite() => {
            a.partial_cmp(b)
        }
        (FieldValue::Boolean(a), FieldValue::Boolean(b)) => Some(a.cmp(b)),
        (FieldValue::Text(a), FieldValue::Text(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        (FieldValue::Bytes(a), FieldValue::Bytes(b)) => Some(a.cmp(b)),
        _ => None,
    }
}
/// Missing index/group fields sort first ascending and last descending.
/// Index columns have one declared type; mismatched present values are invalid.
pub fn compare_optional_fields(
    a: Option<&FieldValue>,
    b: Option<&FieldValue>,
) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (None, None) => Some(std::cmp::Ordering::Equal),
        (None, Some(_)) => Some(std::cmp::Ordering::Less),
        (Some(_), None) => Some(std::cmp::Ordering::Greater),
        (Some(a), Some(b)) => compare_fields(a, b),
    }
}
impl Predicate {
    pub fn matches(&self, value: Option<&FieldValue>) -> bool {
        let Some(value) = value else {
            return false;
        };
        use std::cmp::Ordering::{Equal, Greater, Less};
        match self {
            Self::Equal(expected) => compare_fields(value, expected) == Some(Equal),
            Self::In(values) => values
                .iter()
                .any(|expected| compare_fields(value, expected) == Some(Equal)),
            Self::Range { lower, upper } => {
                lower.as_ref().is_none_or(|b| match b {
                    Bound::Included(n) => matches!(compare_fields(value, n), Some(Equal | Greater)),
                    Bound::Excluded(n) => compare_fields(value, n) == Some(Greater),
                }) && upper.as_ref().is_none_or(|b| match b {
                    Bound::Included(n) => matches!(compare_fields(value, n), Some(Equal | Less)),
                    Bound::Excluded(n) => compare_fields(value, n) == Some(Less),
                })
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionRequest {
    pub queue: QueueAddress,
    pub schema: QueueSchema,
}
impl ProvisionRequest {
    pub fn validate(&self, limits: &QueueLimits) -> QueueResult<()> {
        self.queue.validate()?;
        self.schema.validate()?;
        limits.validate()?;
        if self.schema.state.len() > limits.max_groups as usize
            || std::iter::once(&self.schema.work)
                .chain(self.schema.state.values())
                .any(|d| {
                    d.fields.len() > limits.max_records as usize
                        || d.indexes.len() > limits.max_groups as usize
                })
        {
            return Err(invalid("schema count limit exceeded"));
        }
        byte_limit(self, limits.max_request_bytes)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptReadRequest {
    pub queue: QueueAddress,
    pub request_ids: Vec<String>,
    pub max_bytes: u64,
}
impl ReceiptReadRequest {
    pub fn validate(&self, limits: &QueueLimits) -> QueueResult<()> {
        self.queue.validate()?;
        limits.validate()?;
        if self.request_ids.len() > limits.max_records as usize
            || self.max_bytes == 0
            || self.max_bytes > limits.max_response_bytes
        {
            return Err(invalid("invalid receipt read limits"));
        }
        for id in &self.request_ids {
            name(id)?;
        }
        byte_limit(self, limits.max_request_bytes)
    }
}
/// Reference oracle for selected candidates, never a broad-scan implementation.
pub fn matches_claim(record: &WorkRecord, request: &ClaimRequest) -> bool {
    record.lifecycle == Lifecycle::Pending
        && record.work.available_at <= request.eligible_at
        && request
            .filter
            .keys
            .as_ref()
            .is_none_or(|keys| keys.contains(&record.work.key))
        && request
            .filter
            .fields
            .iter()
            .all(|(key, predicate)| predicate.matches(record.work.fields.get(key)))
}
fn work(item: &NewWork, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
    name(&item.key)?;
    fields(&item.fields, &schema.work)?;
    if item.body.len() as u64 > limits.max_body_bytes {
        return Err(QueueError::TooLarge);
    }
    Ok(())
}
pub(crate) fn patch(
    value: &WorkPatch,
    schema: &QueueSchema,
    limits: &QueueLimits,
) -> QueueResult<()> {
    for (key, value) in &value.fields {
        name(key)?;
        if !schema.work.admits_field(key) {
            return Err(invalid("undeclared patch field"));
        }
        if let Some(value) = value {
            schema.work.validate_value(key, value)?;
        }
    }
    if value
        .body
        .as_ref()
        .is_some_and(|body| body.len() as u64 > limits.max_body_bytes)
    {
        return Err(QueueError::TooLarge);
    }
    Ok(())
}
impl CommitBatch {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        validate_commit_groups(&self.queue, &self.groups, schema, limits)
    }
}
impl PreviewRequest {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        validate_commit_groups(
            &self.queue,
            std::slice::from_ref(&self.group),
            schema,
            limits,
        )
    }
}
impl CommitGroup {
    /// Identities in successful receipt order. Condition-only checks have no
    /// output version because they do not modify a record.
    pub fn modified_records(&self) -> impl Iterator<Item = (Option<&str>, &str)> {
        self.claimed_updates
            .iter()
            .filter(|u| u.disposition != Disposition::Check)
            .map(|u| (None, u.key.as_str()))
            .chain(
                self.state_updates
                    .iter()
                    .filter(|u| u.change != StateChange::Check)
                    .map(|u| (Some(u.collection.as_str()), u.key.as_str())),
            )
            .chain(self.work_updates.iter().map(|u| (None, u.key.as_str())))
            .chain(self.work_inserts.iter().map(|u| (None, u.key.as_str())))
    }
    /// The same borrowed one-group envelope checks used by batch commit/preview.
    pub fn preflight_for(&self, queue: &QueueAddress, limits: &QueueLimits) -> QueueResult<()> {
        preflight_commit_groups(queue, std::slice::from_ref(self), limits)
    }
    pub fn validate_for(
        &self,
        queue: &QueueAddress,
        schema: &QueueSchema,
        limits: &QueueLimits,
    ) -> QueueResult<()> {
        validate_commit_groups(queue, std::slice::from_ref(self), schema, limits)
    }
}
fn validate_commit_groups(
    queue: &QueueAddress,
    groups: &[CommitGroup],
    schema: &QueueSchema,
    limits: &QueueLimits,
) -> QueueResult<()> {
    preflight_commit_groups(queue, groups, limits)?;
    schema.validate()?;
    limits.validate()?;
    if groups.is_empty() || groups.len() > limits.max_groups as usize {
        return Err(invalid("invalid commit group count"));
    }
    let mut identities = BTreeSet::new();
    for group in groups {
        name(&group.request_id)?;
        if !identities.insert(&group.request_id) {
            return Err(invalid("duplicate group identity"));
        }
        let count = group
            .claimed_updates
            .len()
            .checked_add(group.state_updates.len())
            .and_then(|n| n.checked_add(group.work_inserts.len()))
            .and_then(|n| n.checked_add(group.work_updates.len()))
            .ok_or(QueueError::TooLarge)?;
        if (count == 0 && group.administration.is_none()) || count > limits.max_records as usize {
            return Err(invalid("invalid group record count"));
        }
        if let Some(admin) = &group.administration {
            admin.intent.validate(schema, limits)?;
        }
        let mut keys = BTreeSet::new();
        for update in &group.claimed_updates {
            name(&update.key)?;
            if !keys.insert((None, update.key.as_str())) {
                return Err(invalid("duplicate record in group"));
            }
            if update.lease.token.is_empty() || update.expected_version == 0 {
                return Err(invalid("invalid claim credential"));
            }
            if let Disposition::Retain {
                renew_nanos: Some(n),
            } = update.disposition
            {
                lease_duration(n)?;
            }
            if let Disposition::RetryAt(time) = update.disposition
                && update
                    .patch
                    .available_at
                    .is_some_and(|patched| patched != time)
            {
                return Err(invalid("conflicting retry times"));
            }
            patch(&update.patch, schema, limits)?;
        }
        for item in &group.work_inserts {
            if !keys.insert((None, item.key.as_str())) {
                return Err(invalid("duplicate record in group"));
            }
            work(item, schema, limits)?;
        }
        for update in &group.work_updates {
            name(&update.key)?;
            if !keys.insert((None, update.key.as_str())) {
                return Err(invalid("duplicate record in group"));
            }
            expect(&update.expect)?;
            if matches!(update.expect, StateExpect::Absent) {
                return Err(invalid(
                    "unclaimed update requires existing version; creation uses work_inserts",
                ));
            }
            match &update.change {
                UnclaimedChange::Patch(value)
                | UnclaimedChange::Administrative { patch: value, .. } => {
                    patch(value, schema, limits)?
                }
                UnclaimedChange::Replace(value) => {
                    if value.key != update.key {
                        return Err(invalid("replacement key mismatch"));
                    }
                    work(value, schema, limits)?;
                }
                UnclaimedChange::Delete => {}
            }
        }
        for update in &group.state_updates {
            name(&update.key)?;
            expect(&update.expect)?;
            let definition = schema
                .state
                .get(&update.collection)
                .ok_or_else(|| invalid("undeclared state collection"))?;
            if !keys.insert((Some(update.collection.as_str()), update.key.as_str())) {
                return Err(invalid("duplicate record in group"));
            }
            if let StateChange::Put {
                fields: values,
                value,
            } = &update.change
            {
                fields(values, definition)?;
                if value.len() as u64 > limits.max_state_bytes {
                    return Err(QueueError::TooLarge);
                }
            }
        }
        if let Some(admin) = &group.administration
            && (admin.max_result_bytes == 0 || admin.max_result_bytes > limits.max_response_bytes)
        {
            return Err(QueueError::TooLarge);
        }
        byte_limit(&CommitAccountingGroup(group), limits.max_group_bytes)?;
    }
    // Bound the complete positional response before any admitted group runs.
    // u64::MAX reserves maximum version encoding; rejected messages are capped.
    let mut response_bytes = 10u64;
    for group in groups {
        if let Some(admin) = &group.administration {
            response_bytes = response_bytes
                .checked_add(admin.max_result_bytes)
                .ok_or(QueueError::TooLarge)?;
            if response_bytes > limits.max_response_bytes {
                return Err(QueueError::TooLarge);
            }
            continue;
        }
        let versions: Vec<RecordVersion> = group
            .modified_records()
            .map(|(collection, key)| RecordVersion {
                collection: collection.map(str::to_owned),
                key: key.to_owned(),
                version: u64::MAX,
            })
            .collect();
        let success = CommitReceipt {
            request_id: group.request_id.clone(),
            outcome: GroupOutcome::Committed { versions },
        };
        let rejected = CommitReceipt {
            request_id: group.request_id.clone(),
            outcome: GroupOutcome::Rejected(Rejection::Invalid("x".repeat(256))),
        };
        response_bytes = response_bytes
            .checked_add(encoded_len(&success)?.max(encoded_len(&rejected)?))
            .ok_or(QueueError::TooLarge)?;
        if response_bytes > limits.max_response_bytes {
            return Err(QueueError::TooLarge);
        }
    }
    byte_limit(
        &BorrowedCommitEnvelope { queue, groups },
        limits.max_request_bytes,
    )
}

fn expect(value: &StateExpect) -> QueueResult<()> {
    if matches!(value, StateExpect::Version(0)) {
        return Err(invalid("record versions start at one"));
    }
    Ok(())
}
#[doc(hidden)]
pub fn lease_duration(value: u64) -> QueueResult<()> {
    if !(1_000_000..=3_600_000_000_000).contains(&value) {
        return Err(invalid("lease outside 1ms to 1h"));
    }
    Ok(())
}
impl ClaimRequest {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        self.preflight(limits)?;
        name(&self.request_id)?;
        limits.validate()?;
        self.filter.validate(&schema.work)?;
        lease_duration(self.lease_nanos)?;
        if self
            .filter
            .lifecycle
            .is_some_and(|state| state != Lifecycle::Pending)
        {
            return Err(invalid("claims select pending work only"));
        }
        if self.max_records == 0
            || self.max_records > limits.max_records
            || self.max_bytes == 0
            || self.max_bytes > limits.max_response_bytes
        {
            return Err(invalid("invalid claim limits"));
        }
        if let Some(selected) = &self.fields
            && selected.iter().any(|key| !schema.work.admits_field(key))
        {
            return Err(invalid("undeclared return field"));
        }
        byte_limit(self, limits.max_request_bytes)
    }
    pub fn fingerprint(&self) -> QueueResult<[u8; 32]> {
        fingerprint("claim", self)
    }
}
impl InsertRequest {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        self.preflight(limits)?;
        name(&self.request_id)?;
        limits.validate()?;
        if self.items.is_empty() || self.items.len() > limits.max_records as usize {
            return Err(invalid("invalid insert count"));
        }
        let mut keys = BTreeSet::new();
        for item in &self.items {
            if !keys.insert(&item.key) {
                return Err(invalid("duplicate insert key"));
            }
            work(item, schema, limits)?;
        }
        let mut response_bytes = 10u64;
        for item in &self.items {
            let success = InsertOutcome::Replaced {
                key: item.key.clone(),
                version: u64::MAX,
            };
            let rejected = InsertOutcome::Rejected {
                key: item.key.clone(),
                reason: Rejection::Invalid("x".repeat(256)),
            };
            response_bytes = response_bytes
                .checked_add(encoded_len(&success)?.max(encoded_len(&rejected)?))
                .ok_or(QueueError::TooLarge)?;
            if response_bytes > limits.max_response_bytes {
                return Err(QueueError::TooLarge);
            }
        }
        byte_limit(self, limits.max_request_bytes)
    }
}
/// Frozen ordinary commit identity: original five fields, in their original
/// order. Future optional control fields must not enter this v3 projection.
#[derive(Serialize)]
struct CommitFingerprintV3<'a> {
    request_id: &'a str,
    claimed_updates: &'a [ClaimedUpdate],
    state_updates: &'a [StateUpdate],
    work_inserts: &'a [NewWork],
    work_updates: &'a [UnclaimedUpdate],
}
impl<'a> From<&'a CommitGroup> for CommitFingerprintV3<'a> {
    fn from(group: &'a CommitGroup) -> Self {
        Self {
            request_id: &group.request_id,
            claimed_updates: &group.claimed_updates,
            state_updates: &group.state_updates,
            work_inserts: &group.work_inserts,
            work_updates: &group.work_updates,
        }
    }
}
/// Explicit six-field projection. The sixth field is the administration VALUE,
/// with no Option::Some marker. Future DTO additions cannot alter this layout.
#[derive(Serialize)]
struct CommitFingerprintV4<'a> {
    request_id: &'a str,
    claimed_updates: &'a [ClaimedUpdate],
    state_updates: &'a [StateUpdate],
    work_inserts: &'a [NewWork],
    work_updates: &'a [UnclaimedUpdate],
    administration: AdministrativeFingerprintValue<'a>,
}
#[derive(Serialize)]
struct AdministrativeFingerprintValue<'a> {
    intent: &'a crate::queue_administration::AdministrativeIntent,
    max_result_bytes: u64,
}
struct CommitAccountingGroup<'a>(&'a CommitGroup);
impl Serialize for CommitAccountingGroup<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0.administration {
            None => CommitFingerprintV3::from(self.0).serialize(serializer),
            Some(admin) => CommitFingerprintV4 {
                request_id: &self.0.request_id,
                claimed_updates: &self.0.claimed_updates,
                state_updates: &self.0.state_updates,
                work_inserts: &self.0.work_inserts,
                work_updates: &self.0.work_updates,
                administration: AdministrativeFingerprintValue {
                    intent: &admin.intent,
                    max_result_bytes: admin.max_result_bytes,
                },
            }
            .serialize(serializer),
        }
    }
}
/// Borrowed legacy envelope accounting. Serialize each frozen group directly;
/// neither an added DTO field nor a temporary vector can alter ordinary limits.
struct BorrowedCommitEnvelope<'a> {
    queue: &'a QueueAddress,
    groups: &'a [CommitGroup],
}
impl Serialize for BorrowedCommitEnvelope<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut envelope = serializer.serialize_struct("CommitBatch", 2)?;
        envelope.serialize_field("queue", self.queue)?;
        envelope.serialize_field("groups", &OrdinaryCommitGroups(self.groups))?;
        envelope.end()
    }
}
#[cfg(test)]
struct OrdinaryCommitEnvelope<'a>(&'a CommitBatch);
struct OrdinaryCommitGroups<'a>(&'a [CommitGroup]);
impl Serialize for OrdinaryCommitGroups<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for group in self.0 {
            sequence.serialize_element(&CommitAccountingGroup(group))?;
        }
        sequence.end()
    }
}
#[cfg(test)]
impl Serialize for OrdinaryCommitEnvelope<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        BorrowedCommitEnvelope {
            queue: &self.0.queue,
            groups: &self.0.groups,
        }
        .serialize(serializer)
    }
}

impl CommitGroup {
    pub fn fingerprint(&self, queue: &QueueAddress) -> QueueResult<[u8; 32]> {
        match &self.administration {
            None => fingerprint("commit", &(queue, CommitFingerprintV3::from(self))),
            Some(admin) => {
                self.administrative_fingerprint(queue, &admin.intent, admin.max_result_bytes)
            }
        }
    }
    /// Administrative binding alone uses format 4; ordinary groups, claims and
    /// inserts retain format 3. Storage receipt domain remains commit/domain 2.
    pub fn administrative_fingerprint(
        &self,
        queue: &QueueAddress,
        intent: &crate::queue_administration::AdministrativeIntent,
        max_result_bytes: u64,
    ) -> QueueResult<[u8; 32]> {
        fingerprint_format(
            b"fireweed-api007-v4\0",
            "commit",
            &(
                queue,
                CommitFingerprintV4 {
                    request_id: &self.request_id,
                    claimed_updates: &self.claimed_updates,
                    state_updates: &self.state_updates,
                    work_inserts: &self.work_inserts,
                    work_updates: &self.work_updates,
                    administration: AdministrativeFingerprintValue {
                        intent,
                        max_result_bytes,
                    },
                },
            ),
        )
    }
}

impl NumericBucketDimension {
    pub fn validate(&self, definition: &CollectionDefinition) -> QueueResult<()> {
        name(&self.field)?;
        name(&self.missing_label)?;
        if !definition.admits_field(&self.field) || self.buckets.len() > 50 {
            return Err(invalid("invalid numeric bucket dimension"));
        }
        let mut kind = definition.fields.get(&self.field).copied();
        for bucket in &self.buckets {
            name(&bucket.label)?;
            let ty = bucket.lower_inclusive.field_type();
            if !matches!(ty, FieldType::Integer | FieldType::Float) || kind.is_some_and(|k| k != ty)
            {
                return Err(invalid("numeric bucket type mismatch"));
            }
            kind = Some(ty);
            definition.validate_value(&self.field, &bucket.lower_inclusive)?;
            definition.validate_value(&self.field, &bucket.upper_exclusive)?;
            if compare_fields(&bucket.lower_inclusive, &bucket.upper_exclusive)
                != Some(std::cmp::Ordering::Less)
            {
                return Err(invalid("invalid numeric bucket interval"));
            }
        }
        for (i, a) in self.buckets.iter().enumerate() {
            for b in &self.buckets[..i] {
                if compare_fields(&a.lower_inclusive, &b.upper_exclusive)
                    == Some(std::cmp::Ordering::Less)
                    && compare_fields(&b.lower_inclusive, &a.upper_exclusive)
                        == Some(std::cmp::Ordering::Less)
                {
                    return Err(invalid("overlapping numeric buckets"));
                }
            }
        }
        if kind.is_some_and(|k| !matches!(k, FieldType::Integer | FieldType::Float)) {
            return Err(invalid("non-numeric bucket field"));
        }
        Ok(())
    }
    /// Reference oracle for indexed aggregate implementations. Present values
    /// of another scalar type and present out-of-range values are omitted.
    pub fn classify(&self, value: Option<&FieldValue>) -> Option<usize> {
        let Some(value) = value else {
            return Some(self.buckets.len());
        };
        self.buckets.iter().position(|bucket| {
            compare_fields(value, &bucket.lower_inclusive)
                .is_some_and(|o| o != std::cmp::Ordering::Less)
                && compare_fields(value, &bucket.upper_exclusive) == Some(std::cmp::Ordering::Less)
        })
    }
}
impl StatsRequest {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        self.preflight(limits)?;
        limits.validate()?;
        self.filter.validate(&schema.work)?;
        if let Some(dimension) = &self.buckets {
            dimension.validate(&schema.work)?;
        }
        let mut seen = BTreeSet::new();
        for key in &self.group_by {
            if !schema.work.admits_field(key) || !seen.insert(key) {
                return Err(invalid("invalid stats grouping"));
            }
        }
        byte_limit(self, limits.max_request_bytes)
    }
}
impl StateReadRequest {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        self.preflight(limits)?;
        limits.validate()?;
        if !schema.state.contains_key(&self.collection) {
            return Err(invalid("undeclared state collection"));
        }
        if self.keys.len() > limits.max_records as usize
            || self.max_bytes == 0
            || self.max_bytes > limits.max_response_bytes
        {
            return Err(invalid("invalid state read limits"));
        }
        for key in &self.keys {
            name(key)?;
        }
        byte_limit(self, limits.max_request_bytes)
    }
}
impl QueryRequest {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        self.preflight(limits)?;
        limits.validate()?;
        let definition = match &self.source {
            RecordSource::Work => &schema.work,
            RecordSource::State(collection) => {
                if self.filter.lifecycle.is_some() {
                    return Err(invalid("state has no work lifecycle"));
                }
                schema
                    .state
                    .get(collection)
                    .ok_or_else(|| invalid("undeclared state collection"))?
            }
        };
        if self.index != "$key" && !definition.indexes.contains_key(&self.index) {
            return Err(invalid("undeclared query index"));
        }
        self.filter.validate(definition)?;
        if self.max_records == 0
            || self.max_records > limits.max_records
            || self.max_bytes == 0
            || self.max_bytes > limits.max_response_bytes
        {
            return Err(invalid("invalid query limits"));
        }
        byte_limit(self, limits.max_request_bytes)
    }
    /// A cursor is bound to the query semantics, not its requested page size.
    pub fn cursor_fingerprint(&self, schema: &QueueSchema) -> QueueResult<[u8; 32]> {
        fingerprint(
            "cursor",
            &(
                &self.queue,
                &self.source,
                &self.index,
                &self.filter,
                self.descending,
                schema,
            ),
        )
    }
}

/// Schema-independent envelope checks run before transferring ownership/admission.
/// The storage-specific schema/type check follows under its atomic boundary.
pub trait RequestPreflight {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()>;
}
fn envelope<T: Serialize>(
    queue: &QueueAddress,
    request: &T,
    limits: &QueueLimits,
) -> QueueResult<()> {
    queue.validate()?;
    limits.validate()?;
    byte_limit(request, limits.max_request_bytes)
}
fn response_limits(records: Option<u32>, bytes: u64, limits: &QueueLimits) -> QueueResult<()> {
    if records.is_some_and(|count| count == 0 || count > limits.max_records)
        || bytes == 0
        || bytes > limits.max_response_bytes
    {
        return Err(invalid("invalid response limits"));
    }
    Ok(())
}
impl RequestPreflight for ClaimRequest {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        envelope(&self.queue, self, limits)?;
        name(&self.request_id)?;
        lease_duration(self.lease_nanos)?;
        response_limits(Some(self.max_records), self.max_bytes, limits)
    }
}
impl RequestPreflight for QueryRequest {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        envelope(&self.queue, self, limits)?;
        response_limits(Some(self.max_records), self.max_bytes, limits)
    }
}
impl RequestPreflight for StateReadRequest {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        envelope(&self.queue, self, limits)?;
        name(&self.collection)?;
        response_limits(None, self.max_bytes, limits)?;
        if self.keys.len() > limits.max_records as usize {
            return Err(invalid("invalid state read limits"));
        }
        for key in &self.keys {
            name(key)?;
        }
        Ok(())
    }
}
impl RequestPreflight for StatsRequest {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        envelope(&self.queue, self, limits)?;
        if self.buckets.as_ref().is_some_and(|d| d.buckets.len() > 50) {
            return Err(invalid("too many numeric buckets"));
        }
        Ok(())
    }
}
impl RequestPreflight for InsertRequest {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        envelope(&self.queue, self, limits)?;
        name(&self.request_id)?;
        if self.items.is_empty() || self.items.len() > limits.max_records as usize {
            return Err(invalid("invalid insert count"));
        }
        let mut keys = BTreeSet::new();
        for item in &self.items {
            name(&item.key)?;
            if !keys.insert(&item.key) {
                return Err(invalid("duplicate insert key"));
            }
            if item.body.len() as u64 > limits.max_body_bytes {
                return Err(QueueError::TooLarge);
            }
        }
        Ok(())
    }
}
impl RequestPreflight for CommitBatch {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        preflight_commit_groups(&self.queue, &self.groups, limits)
    }
}
impl RequestPreflight for PreviewRequest {
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        preflight_commit_groups(&self.queue, std::slice::from_ref(&self.group), limits)
    }
}
fn preflight_commit_groups(
    queue: &QueueAddress,
    groups: &[CommitGroup],
    limits: &QueueLimits,
) -> QueueResult<()> {
    envelope(queue, &BorrowedCommitEnvelope { queue, groups }, limits)?;
    if groups.is_empty() || groups.len() > limits.max_groups as usize {
        return Err(invalid("invalid commit group count"));
    }
    let mut ids = BTreeSet::new();
    for group in groups {
        name(&group.request_id)?;
        if !ids.insert(&group.request_id) {
            return Err(invalid("duplicate group identity"));
        }
        let count = group
            .claimed_updates
            .len()
            .checked_add(group.work_updates.len())
            .and_then(|n| n.checked_add(group.work_inserts.len()))
            .and_then(|n| n.checked_add(group.state_updates.len()))
            .ok_or(QueueError::TooLarge)?;
        if (count == 0 && group.administration.is_none()) || count > limits.max_records as usize {
            return Err(invalid("invalid group record count"));
        }
        if let Some(admin) = &group.administration
            && (admin.max_result_bytes == 0 || admin.max_result_bytes > limits.max_response_bytes)
        {
            return Err(QueueError::TooLarge);
        }
        for update in &group.claimed_updates {
            if update.disposition == Disposition::Check && update.patch != WorkPatch::default() {
                return Err(invalid("claim check requires an empty patch"));
            }
        }
        let mut keys = BTreeSet::new();
        for key in group
            .claimed_updates
            .iter()
            .map(|u| (None, u.key.as_str()))
            .chain(group.work_updates.iter().map(|u| (None, u.key.as_str())))
            .chain(group.work_inserts.iter().map(|u| (None, u.key.as_str())))
            .chain(
                group
                    .state_updates
                    .iter()
                    .map(|u| (Some(u.collection.as_str()), u.key.as_str())),
            )
        {
            if !keys.insert(key) {
                return Err(invalid("duplicate record in group"));
            }
        }
        byte_limit(&CommitAccountingGroup(group), limits.max_group_bytes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn check_variants_append_wire_tags_and_reject_nonempty_claim_patches() {
        for (value, bytes) in [
            (Disposition::Complete, vec![0]),
            (Disposition::Fail, vec![1]),
            (Disposition::Release, vec![2]),
            (Disposition::RetryAt(-1), vec![3, 1]),
            (Disposition::Retain { renew_nanos: None }, vec![4, 0]),
            (
                Disposition::Retain {
                    renew_nanos: Some(1),
                },
                vec![4, 1, 1],
            ),
            (Disposition::Delete, vec![5]),
            (Disposition::Check, vec![6]),
        ] {
            assert_eq!(postcard::to_stdvec(&value).unwrap(), bytes);
        }
        assert_eq!(
            postcard::to_stdvec(&StateChange::Put {
                fields: Fields::new(),
                value: Bytes::new()
            })
            .unwrap(),
            [0, 0, 0]
        );
        assert_eq!(postcard::to_stdvec(&StateChange::Delete).unwrap(), [1]);
        assert_eq!(postcard::to_stdvec(&StateChange::Check).unwrap(), [2]);
        let mut group = CommitGroup {
            request_id: "check".into(),
            claimed_updates: vec![ClaimedUpdate {
                key: "work".into(),
                lease: LeaseCredential { token: vec![1] },
                expected_version: u64::MAX,
                patch: WorkPatch::default(),
                disposition: Disposition::Check,
            }],
            state_updates: vec![StateUpdate {
                collection: "instance".into(),
                key: "state".into(),
                expect: StateExpect::Version(u64::MAX),
                change: StateChange::Check,
            }],
            work_inserts: vec![],
            work_updates: vec![],
            administration: None,
        };
        assert_eq!(group.modified_records().count(), 0);
        let limits = QueueLimits::default();
        group.validate_for(&queue(), &schema(), &limits).unwrap();
        for patch in [
            WorkPatch {
                priority: Some(0),
                ..Default::default()
            },
            WorkPatch {
                available_at: Some(0),
                ..Default::default()
            },
            WorkPatch {
                fields: BTreeMap::from([("kind".into(), Some(FieldValue::Text("x".into())))]),
                ..Default::default()
            },
            WorkPatch {
                body: Some(Bytes::new()),
                ..Default::default()
            },
        ] {
            group.claimed_updates[0].patch = patch;
            assert!(matches!(
                group.preflight_for(&queue(), &limits),
                Err(QueueError::Invalid(_))
            ));
        }
    }

    #[test]
    fn check_receipt_bounds_exclude_guard_keys_but_keep_record_and_duplicate_limits() {
        let mut group = CommitGroup {
            request_id: "check".into(),
            claimed_updates: vec![],
            state_updates: vec![StateUpdate {
                collection: "instance".into(),
                key: "x".repeat(4096),
                expect: StateExpect::Absent,
                change: StateChange::Check,
            }],
            work_inserts: vec![],
            work_updates: vec![],
            administration: None,
        };
        let success = CommitReceipt {
            request_id: group.request_id.clone(),
            outcome: GroupOutcome::Committed { versions: vec![] },
        };
        let rejected = CommitReceipt {
            request_id: group.request_id.clone(),
            outcome: GroupOutcome::Rejected(Rejection::Invalid("x".repeat(256))),
        };
        let mut limits = QueueLimits {
            max_response_bytes: 10
                + encoded_len(&success)
                    .unwrap()
                    .max(encoded_len(&rejected).unwrap()),
            ..Default::default()
        };
        group.validate_for(&queue(), &schema(), &limits).unwrap();
        limits.max_response_bytes -= 1;
        assert_eq!(
            group.validate_for(&queue(), &schema(), &limits),
            Err(QueueError::TooLarge)
        );
        limits = QueueLimits::default();
        let mut mutation = group.state_updates[0].clone();
        mutation.change = StateChange::Put {
            fields: Fields::new(),
            value: Bytes::new(),
        };
        group.state_updates.push(mutation);
        assert!(matches!(
            group.preflight_for(&queue(), &limits),
            Err(QueueError::Invalid(_))
        ));
        assert!(matches!(
            group.validate_for(&queue(), &schema(), &limits),
            Err(QueueError::Invalid(_))
        ));
        group.state_updates[1].key = "other".into();
        group.state_updates[1].change = StateChange::Check;
        limits.max_records = 1;
        assert!(matches!(
            group.preflight_for(&queue(), &limits),
            Err(QueueError::Invalid(_))
        ));
    }

    fn schema() -> QueueSchema {
        let definition = CollectionDefinition {
            additional_field_prefixes: BTreeSet::new(),
            fields: BTreeMap::from([
                ("kind".into(), FieldType::Text),
                ("score".into(), FieldType::Float),
            ]),
            indexes: BTreeMap::from([("by_kind".into(), vec!["kind".into()])]),
            unique_indexes: BTreeSet::new(),
        };
        QueueSchema {
            work: definition.clone(),
            state: BTreeMap::from([("instance".into(), definition)]),
            progress_bound_nanos: 1_000_000_000,
            max_attempts: 3,
        }
    }
    fn queue() -> QueueAddress {
        QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        }
    }
    fn item(key: &str) -> NewWork {
        NewWork {
            key: key.into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::new(),
            body: Bytes::new(),
        }
    }
    fn group(id: &str) -> CommitGroup {
        CommitGroup {
            administration: None,
            request_id: id.into(),
            claimed_updates: vec![],
            state_updates: vec![StateUpdate {
                collection: "instance".into(),
                key: "same-instance".into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: Fields::new(),
                    value: Bytes::new(),
                },
            }],
            work_inserts: vec![],
            work_updates: vec![],
        }
    }
    fn claim() -> ClaimRequest {
        ClaimRequest {
            queue: queue(),
            request_id: "claim-1".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000_000,
            max_records: 100,
            max_bytes: 1 << 20,
            fields: None,
            include_body: true,
        }
    }
    #[test]
    fn overlapping_groups_are_valid_but_duplicate_within_group_is_not() {
        let mut request = CommitBatch {
            queue: queue(),
            groups: vec![group("one"), group("two")],
        };
        assert!(request.validate(&schema(), &QueueLimits::default()).is_ok());
        let duplicate = request.groups[0].state_updates[0].clone();
        request.groups[0].state_updates.push(duplicate);
        assert!(matches!(
            request.validate(&schema(), &QueueLimits::default()),
            Err(QueueError::Invalid(_))
        ));
    }
    #[test]
    fn transition_fanout_and_megabyte_state_are_not_reduced() {
        let mut g = group("large");
        if let StateChange::Put { value, .. } = &mut g.state_updates[0].change {
            *value = Bytes::from(vec![1; 1 << 20]);
        }
        g.work_inserts = (0..1025)
            .map(|n| item(&format!("effect-or-await-{n}")))
            .collect();
        assert!(
            CommitBatch {
                queue: queue(),
                groups: vec![g]
            }
            .validate(&schema(), &QueueLimits::default())
            .is_ok()
        );
    }
    #[test]
    fn rebatching_does_not_change_group_identity() {
        let a = group("one");
        let b = group("two");
        let batch = CommitBatch {
            queue: queue(),
            groups: vec![a.clone(), b],
        };
        let first = batch.groups[0].fingerprint(&batch.queue).unwrap();
        let resplit = CommitBatch {
            queue: queue(),
            groups: vec![group("three"), a],
        };
        assert_eq!(
            first,
            resplit.groups[1].fingerprint(&resplit.queue).unwrap()
        );
        let mut altered = resplit.groups[1].clone();
        altered.state_updates[0].expect = StateExpect::Version(1);
        assert_ne!(first, altered.fingerprint(&queue()).unwrap());
    }
    #[test]
    fn every_claim_selection_change_changes_identity() {
        let base = claim();
        let original = base.fingerprint().unwrap();
        let mut variants = vec![];
        let mut changed = base.clone();
        changed.max_records += 1;
        variants.push(changed);
        let mut changed = base.clone();
        changed.max_bytes += 1;
        variants.push(changed);
        let mut changed = base.clone();
        changed.eligible_at += 1;
        variants.push(changed);
        let mut changed = base.clone();
        changed.lease_nanos += 1;
        variants.push(changed);
        let mut changed = base.clone();
        changed.order = ClaimOrder::PriorityDescending;
        variants.push(changed);
        let mut changed = base.clone();
        changed.include_body = false;
        variants.push(changed);
        let mut changed = base.clone();
        changed.fields = Some(BTreeSet::new());
        variants.push(changed);
        let mut changed = base.clone();
        changed.filter.keys = Some(BTreeSet::from(["k".into()]));
        variants.push(changed);
        for changed in variants {
            assert_ne!(original, changed.fingerprint().unwrap());
        }
    }
    #[test]
    fn canonical_maps_and_operation_domains_are_stable() {
        let a = BTreeMap::from([("b", 2), ("a", 1)]);
        let b = BTreeMap::from([("a", 1), ("b", 2)]);
        assert_eq!(
            fingerprint("commit", &a).unwrap(),
            fingerprint("commit", &b).unwrap()
        );
        assert_ne!(
            fingerprint("claim", &a).unwrap(),
            fingerprint("commit", &a).unwrap()
        );
    }
    #[test]
    fn invalid_filters_are_rejected_before_storage() {
        let mut request = claim();
        request.filter.fields.insert(
            "undeclared".into(),
            Predicate::Equal(FieldValue::Text("x".into())),
        );
        assert!(
            request
                .validate(&schema(), &QueueLimits::default())
                .is_err()
        );
        request.filter.fields.clear();
        request
            .filter
            .fields
            .insert("kind".into(), Predicate::Equal(FieldValue::Integer(1)));
        assert!(
            request
                .validate(&schema(), &QueueLimits::default())
                .is_err()
        );
        request.filter.fields.clear();
        request.filter.lifecycle = Some(Lifecycle::Completed);
        assert!(
            request
                .validate(&schema(), &QueueLimits::default())
                .is_err()
        );
    }
    #[test]
    fn canonical_byte_boundary_is_exact_and_rejects_oversize() {
        let batch = CommitBatch {
            queue: queue(),
            groups: vec![group("one")],
        };
        let exact = encoded_len(&CommitFingerprintV3::from(&batch.groups[0])).unwrap();
        let mut limits = QueueLimits {
            max_group_bytes: exact,
            max_body_bytes: 1,
            max_state_bytes: 1,
            ..Default::default()
        };
        assert!(batch.validate(&schema(), &limits).is_ok());
        limits.max_group_bytes -= 1;
        assert_eq!(
            batch.validate(&schema(), &limits),
            Err(QueueError::TooLarge)
        );
    }
    #[test]
    fn claims_reject_unknown_projection_fields_and_invalid_durations() {
        let mut request = claim();
        request.fields = Some(BTreeSet::from(["absent".into()]));
        assert!(
            request
                .validate(&schema(), &QueueLimits::default())
                .is_err()
        );
        request.fields = None;
        request.lease_nanos = 0;
        assert!(
            request
                .validate(&schema(), &QueueLimits::default())
                .is_err()
        );
    }
    #[test]
    fn state_does_not_have_work_lifecycle_or_capability_filter() {
        let request = QueryRequest {
            queue: queue(),
            source: RecordSource::State("instance".into()),
            index: "by_kind".into(),
            filter: Filter {
                lifecycle: Some(Lifecycle::Pending),
                ..Filter::default()
            },
            descending: false,
            cursor: None,
            max_records: 10,
            max_bytes: 1024,
        };
        assert!(
            request
                .validate(&schema(), &QueueLimits::default())
                .is_err()
        );
    }
    #[test]
    fn duplicate_group_id_and_nonfinite_values_fail_validation() {
        let batch = CommitBatch {
            queue: queue(),
            groups: vec![group("same"), group("same")],
        };
        assert!(batch.validate(&schema(), &QueueLimits::default()).is_err());
        let mut insert = InsertRequest {
            queue: queue(),
            request_id: "insert".into(),
            mode: InsertMode::InsertOnly,
            items: vec![item("k")],
        };
        insert.items[0]
            .fields
            .insert("score".into(), FieldValue::Float(f64::NAN));
        assert!(insert.validate(&schema(), &QueueLimits::default()).is_err());
    }
    #[test]
    fn half_open_time_range_and_missing_field_semantics() {
        let p = Predicate::Range {
            lower: Some(Bound::Included(FieldValue::Integer(10))),
            upper: Some(Bound::Excluded(FieldValue::Integer(20))),
        };
        assert!(p.matches(Some(&FieldValue::Integer(10))));
        assert!(p.matches(Some(&FieldValue::Integer(19))));
        assert!(!p.matches(Some(&FieldValue::Integer(20))));
        assert!(!p.matches(None));
        assert!(!p.matches(Some(&FieldValue::Text("10".into()))));
        let p = Predicate::Range {
            lower: None,
            upper: Some(Bound::Included(FieldValue::Integer(20))),
        };
        assert!(p.matches(Some(&FieldValue::Integer(20))));
    }
    #[test]
    fn reserved_index_and_unbounded_receipt_reads_reject() {
        let mut definition = schema();
        definition
            .work
            .indexes
            .insert("$key".into(), vec!["kind".into()]);
        assert!(definition.validate().is_err());
        let request = ReceiptReadRequest {
            queue: queue(),
            request_ids: vec!["id".into(); 4097],
            max_bytes: 1024,
        };
        assert!(request.validate(&QueueLimits::default()).is_err());
    }
    #[test]
    fn codec_v3_golden_commit_projection() {
        let address = QueueAddress {
            tenant: "t\0/x".into(),
            queue: "q/é".into(),
        };
        let empty = CommitGroup {
            administration: None,
            request_id: "empty".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let rich = CommitGroup {
            administration: None,
            request_id: "commit\0/é".into(),
            claimed_updates: vec![ClaimedUpdate {
                key: "claimed".into(),
                lease: LeaseCredential {
                    token: vec![0, 255, 1],
                },
                expected_version: u64::MAX,
                patch: WorkPatch {
                    priority: Some(-7),
                    available_at: Some(i64::MIN),
                    fields: BTreeMap::from([
                        ("nul\0/key".into(), Some(FieldValue::Bytes(vec![0, 255]))),
                        ("removed".into(), None),
                    ]),
                    body: Some(Bytes::new()),
                },
                disposition: Disposition::RetryAt(-1),
            }],
            state_updates: vec![StateUpdate {
                collection: "instance".into(),
                key: "s".into(),
                expect: StateExpect::Version(128),
                change: StateChange::Put {
                    fields: BTreeMap::from([("flag".into(), FieldValue::Boolean(true))]),
                    value: Bytes::from_static(&[0, 1, 255]),
                },
            }],
            work_inserts: vec![NewWork {
                key: "next".into(),
                priority: i64::MAX,
                available_at: -2,
                fields: BTreeMap::from([
                    ("a".into(), FieldValue::Integer(-128)),
                    ("b".into(), FieldValue::Float(-0.0)),
                    ("c".into(), FieldValue::Text("é\0/".into())),
                ]),
                body: Bytes::from_static(&[255, 0]),
            }],
            work_updates: vec![UnclaimedUpdate {
                key: "remove".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Delete,
            }],
        };
        for (group, wire, digest) in [
            (
                &empty,
                "0474002f7804712fc3a905656d70747900000000",
                "ee3a8791dd0a46a3445d71b36d2c0b3f67cd9082b2c26aa2aae8f81cdf68ce63",
            ),
            (
                &rich,
                "0474002f7804712fc3a90a636f6d6d6974002fc3a90107636c61696d65640300ff01ffffffffffffffffff01010d01ffffffffffffffffff0102086e756c002f6b657901040200ff0772656d6f76656400010003010108696e7374616e63650173018001000104666c61670201030001ff01046e657874feffffffffffffffff010303016100ff01016201000000000000008001630304c3a9002f02ff00010672656d6f7665010103",
                "f6bffe65f454e14c35943a1438cd252a47bb8dbc74a5641977998ddcb13c419e",
            ),
        ] {
            #[derive(Serialize)]
            struct LegacyGroup<'a> {
                request_id: &'a str,
                claimed_updates: &'a [ClaimedUpdate],
                state_updates: &'a [StateUpdate],
                work_inserts: &'a [NewWork],
                work_updates: &'a [UnclaimedUpdate],
            }
            let legacy = LegacyGroup {
                request_id: &group.request_id,
                claimed_updates: &group.claimed_updates,
                state_updates: &group.state_updates,
                work_inserts: &group.work_inserts,
                work_updates: &group.work_updates,
            };
            let original = postcard::to_allocvec(&(&address, &legacy)).unwrap();
            let projected =
                postcard::to_allocvec(&(&address, CommitFingerprintV3::from(group))).unwrap();
            assert_eq!(original, projected);
            assert_eq!(
                group.fingerprint(&address).unwrap(),
                fingerprint("commit", &(&address, &legacy)).unwrap()
            );
            let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
            assert_eq!(hex(&original), wire);
            assert_eq!(hex(&group.fingerprint(&address).unwrap()), digest);
        }
    }
    #[test]
    fn ordinary_envelope_accounting_preserves_exact_legacy_boundaries() {
        let group = CommitGroup {
            administration: None,
            request_id: "ordinary\0/é".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![item("next")],
            work_updates: vec![],
        };
        let batch = CommitBatch {
            queue: queue(),
            groups: vec![group.clone(), group],
        };
        #[derive(Serialize)]
        struct LegacyGroup<'a> {
            request_id: &'a str,
            claimed_updates: &'a [ClaimedUpdate],
            state_updates: &'a [StateUpdate],
            work_inserts: &'a [NewWork],
            work_updates: &'a [UnclaimedUpdate],
        }
        #[derive(Serialize)]
        struct LegacyEnvelope<'a> {
            queue: &'a QueueAddress,
            groups: Vec<LegacyGroup<'a>>,
        }
        fn legacy_group(group: &CommitGroup) -> LegacyGroup<'_> {
            LegacyGroup {
                request_id: &group.request_id,
                claimed_updates: &group.claimed_updates,
                state_updates: &group.state_updates,
                work_inserts: &group.work_inserts,
                work_updates: &group.work_updates,
            }
        }
        let legacy = LegacyEnvelope {
            queue: &batch.queue,
            groups: batch.groups.iter().map(legacy_group).collect(),
        };
        let projected = OrdinaryCommitEnvelope(&batch);
        assert_eq!(
            postcard::to_allocvec(&legacy).unwrap(),
            postcard::to_allocvec(&projected).unwrap()
        );
        let size = encoded_len(&projected).unwrap();
        assert!(byte_limit(&projected, size).is_ok());
        assert_eq!(byte_limit(&projected, size - 1), Err(QueueError::TooLarge));
        for group in &batch.groups {
            let projection = CommitFingerprintV3::from(group);
            assert_eq!(
                postcard::to_allocvec(&legacy_group(group)).unwrap(),
                postcard::to_allocvec(&projection).unwrap()
            );
            let size = encoded_len(&projection).unwrap();
            assert!(byte_limit(&projection, size).is_ok());
            assert_eq!(byte_limit(&projection, size - 1), Err(QueueError::TooLarge));
        }
    }
    #[test]
    fn retained_receipt_ordinals_and_bytes_are_frozen() {
        let outcomes = [
            (GroupOutcome::RequestConflict, vec![0]),
            (GroupOutcome::RequestExpired, vec![1]),
            (
                GroupOutcome::Committed {
                    versions: vec![RecordVersion {
                        collection: Some("s".into()),
                        key: "k".into(),
                        version: 128,
                    }],
                },
                vec![2, 1, 1, 1, b's', 1, b'k', 128, 1],
            ),
            (GroupOutcome::Rejected(Rejection::StaleLease), vec![3, 0]),
            (
                GroupOutcome::Rejected(Rejection::VersionConflict),
                vec![3, 1],
            ),
            (GroupOutcome::Rejected(Rejection::AlreadyExists), vec![3, 2]),
            (GroupOutcome::Rejected(Rejection::NotFound), vec![3, 3]),
            (GroupOutcome::Rejected(Rejection::Leased), vec![3, 4]),
            (
                GroupOutcome::Rejected(Rejection::Invalid("x".into())),
                vec![3, 5, 1, b'x'],
            ),
            (
                GroupOutcome::Rejected(Rejection::UniqueConflict),
                vec![3, 6],
            ),
            (GroupOutcome::Rejected(Rejection::Terminal), vec![3, 7]),
        ];
        for (outcome, encoded) in outcomes {
            assert_eq!(postcard::to_allocvec(&outcome).unwrap(), encoded);
            let receipt = CommitReceipt {
                request_id: "r".into(),
                outcome,
            };
            let raw = [vec![1, b'r'], encoded].concat();
            assert_eq!(postcard::to_allocvec(&receipt).unwrap(), raw);
            assert_eq!(
                postcard::from_bytes::<CommitReceipt>(&raw).unwrap(),
                receipt
            );
            assert_eq!(
                postcard::from_bytes::<ReceiptLookup>(&[vec![0], raw].concat()).unwrap(),
                ReceiptLookup::Found(receipt)
            );
        }
        assert_eq!(postcard::to_allocvec(&ReceiptLookup::Absent).unwrap(), [1]);
        assert_eq!(postcard::to_allocvec(&ReceiptLookup::Expired).unwrap(), [2]);
    }
    #[test]
    fn codec_v3_golden_claim() {
        let request = claim();
        let digest = request
            .fingerprint()
            .unwrap()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(encoded_len(&request).unwrap(), 28);
        assert_eq!(
            digest,
            "f9ecef4a79efc3846a7a78fa7a1f96d1c872c42db2898cf92d822dbc67456c68"
        );
    }
    #[test]
    fn commit_response_budget_is_checked_before_effects() {
        let batch = CommitBatch {
            queue: queue(),
            groups: vec![group("one")],
        };
        let limits = QueueLimits {
            max_response_bytes: 10,
            ..Default::default()
        };
        assert_eq!(
            batch.validate(&schema(), &limits),
            Err(QueueError::TooLarge)
        );
    }

    #[test]
    fn sparse_index_order_is_identical_across_storage_engines() {
        let v = FieldValue::Integer(1);
        assert_eq!(
            compare_optional_fields(None, Some(&v)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_optional_fields(Some(&v), None),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            compare_optional_fields(None, None),
            Some(std::cmp::Ordering::Equal)
        );
    }
    #[test]
    fn mutation_preconditions_do_not_hide_create_or_retry_precedence() {
        let mut g = group("mutation");
        g.work_updates.push(UnclaimedUpdate {
            key: "missing".into(),
            expect: StateExpect::Absent,
            change: UnclaimedChange::Patch(WorkPatch::default()),
        });
        assert!(
            CommitBatch {
                queue: queue(),
                groups: vec![g]
            }
            .validate(&schema(), &QueueLimits::default())
            .is_err()
        );
        let mut g = group("retry");
        g.claimed_updates.push(ClaimedUpdate {
            key: "work".into(),
            lease: LeaseCredential { token: vec![1] },
            expected_version: 1,
            patch: WorkPatch {
                available_at: Some(1),
                ..WorkPatch::default()
            },
            disposition: Disposition::RetryAt(2),
        });
        assert!(
            CommitBatch {
                queue: queue(),
                groups: vec![g]
            }
            .validate(&schema(), &QueueLimits::default())
            .is_err()
        );
    }
    #[test]
    fn insert_response_budget_rejects_before_mutation() {
        let request = InsertRequest {
            queue: queue(),
            request_id: "insert".into(),
            mode: InsertMode::InsertOnly,
            items: vec![item("one")],
        };
        let limits = QueueLimits {
            max_response_bytes: 1,
            ..Default::default()
        };
        assert_eq!(
            request.validate(&schema(), &limits),
            Err(QueueError::TooLarge)
        );
    }
    #[test]
    fn nanosecond_interval_conversions_never_round_or_saturate() {
        for n in [i64::MIN, -1, 0, 100, 299, 300, i64::MAX] {
            assert_eq!(timestamp_nanos(timestamp_from_nanos(n)).unwrap(), n);
        }
        let mut beyond = timestamp_from_nanos(i64::MAX);
        beyond.nanoseconds += 1;
        assert!(timestamp_nanos(beyond).is_err());
        let mut before = timestamp_from_nanos(i64::MIN);
        before.nanoseconds -= 1;
        assert!(timestamp_nanos(before).is_err());
        let p = Predicate::Range {
            lower: Some(Bound::Included(FieldValue::Integer(100))),
            upper: Some(Bound::Excluded(FieldValue::Integer(300))),
        };
        assert!(p.matches(Some(&FieldValue::Integer(299))));
        assert!(!p.matches(Some(&FieldValue::Integer(300))));
    }
    #[test]
    fn legacy_long_identifiers_remain_valid() {
        let mut request = InsertRequest {
            queue: queue(),
            request_id: "rid".repeat(100),
            mode: InsertMode::InsertOnly,
            items: vec![item(&"await-key".repeat(100))],
        };
        request.items[0]
            .fields
            .insert("kind".into(), FieldValue::Text("target".repeat(200)));
        assert!(request.validate(&schema(), &QueueLimits::default()).is_ok());
    }
    #[test]
    fn future_three_hundred_nanoseconds_is_not_claimable_early() {
        let mut record = WorkRecord {
            work: item("future"),
            version: 1,
            insertion_sequence: 1,
            lifecycle: Lifecycle::Pending,
            eligible_since: 300,
            attempts: 0,
            lease_expires_at: None,
        };
        record.work.available_at = 300;
        let mut request = claim();
        request.eligible_at = 0;
        assert!(!matches_claim(&record, &request));
        request.eligible_at = 299;
        assert!(!matches_claim(&record, &request));
        request.eligible_at = 300;
        assert!(matches_claim(&record, &request));
    }
    #[test]
    fn claim_and_renewal_enforce_nanosecond_wall_limits() {
        for (duration, valid) in [
            (999_999, false),
            (1_000_000, true),
            (3_600_000_000_000, true),
            (3_600_000_000_001, false),
        ] {
            let mut c = claim();
            c.lease_nanos = duration;
            assert_eq!(
                c.validate(&schema(), &QueueLimits::default()).is_ok(),
                valid
            );
            let mut g = group("renew");
            g.claimed_updates.push(ClaimedUpdate {
                key: "work".into(),
                lease: LeaseCredential { token: vec![1] },
                expected_version: 1,
                patch: WorkPatch::default(),
                disposition: Disposition::Retain {
                    renew_nanos: Some(duration),
                },
            });
            assert_eq!(
                CommitBatch {
                    queue: queue(),
                    groups: vec![g]
                }
                .validate(&schema(), &QueueLimits::default())
                .is_ok(),
                valid
            );
        }
    }
    #[test]
    fn bytes_payloads_preserve_vec_wire_and_fingerprint() {
        for payload in [vec![], vec![0, 1, 127, 128, 255], vec![42; 300]] {
            let legacy = (
                "item".to_string(),
                -4i64,
                17i64,
                Fields::new(),
                payload.clone(),
            );
            let record = NewWork {
                key: "item".into(),
                priority: -4,
                available_at: 17,
                fields: Fields::new(),
                body: payload.clone().into(),
            };
            let old = postcard::to_allocvec(&legacy).unwrap();
            let new = postcard::to_allocvec(&record).unwrap();
            assert_eq!(old, new);
            assert_eq!(
                fingerprint("compatibility", &legacy).unwrap(),
                fingerprint("compatibility", &record).unwrap()
            );
            assert_eq!(postcard::from_bytes::<NewWork>(&old).unwrap(), record);
            assert_eq!(
                postcard::from_bytes::<(String, i64, i64, Fields, Vec<u8>)>(&new).unwrap(),
                legacy
            );
            let state = StateRecord {
                collection: "instances".into(),
                key: "i".into(),
                version: 1,
                fields: Fields::new(),
                value: payload.clone().into(),
            };
            let legacy_state = (
                state.collection.clone(),
                state.key.clone(),
                1u64,
                Fields::new(),
                payload.clone(),
            );
            let encoded = postcard::to_allocvec(&state).unwrap();
            assert_eq!(encoded, postcard::to_allocvec(&legacy_state).unwrap());
            assert_eq!(
                postcard::from_bytes::<StateRecord>(&encoded).unwrap(),
                state
            );
            assert_eq!(
                postcard::from_bytes::<(String, String, u64, Fields, Vec<u8>)>(&encoded).unwrap(),
                legacy_state
            );
            let patch = WorkPatch {
                body: Some(payload.clone().into()),
                ..WorkPatch::default()
            };
            let legacy_patch = (
                None::<i64>,
                None::<i64>,
                BTreeMap::<String, Option<FieldValue>>::new(),
                Some(payload),
            );
            let encoded = postcard::to_allocvec(&patch).unwrap();
            assert_eq!(encoded, postcard::to_allocvec(&legacy_patch).unwrap());
            assert_eq!(postcard::from_bytes::<WorkPatch>(&encoded).unwrap(), patch);
            assert_eq!(
                postcard::from_bytes::<(
                    Option<i64>,
                    Option<i64>,
                    BTreeMap<String, Option<FieldValue>>,
                    Option<Vec<u8>>
                )>(&encoded)
                .unwrap(),
                legacy_patch
            );
        }
    }
    #[test]
    fn preflight_rejects_wire_size_and_declared_limits_without_schema() {
        let limits = QueueLimits {
            max_records: 4,
            max_groups: 2,
            max_group_bytes: 512,
            max_request_bytes: 1024,
            max_response_bytes: 512,
            max_body_bytes: 256,
            max_state_bytes: 512,
        };
        let mut request = claim();
        request.max_records = 4;
        request.max_bytes = 512;
        assert!(request.preflight(&limits).is_ok());
        request.max_bytes = 513;
        assert!(matches!(
            request.preflight(&limits),
            Err(QueueError::Invalid(_))
        ));
        request.max_bytes = 512;
        request.filter.keys = Some(BTreeSet::from(["k".repeat(1025)]));
        assert_eq!(request.preflight(&limits), Err(QueueError::TooLarge));
    }
}

#[cfg(test)]
mod administrative_fingerprint_tests {
    use super::*;
    use crate::queue_administration::{AdministrativeIntent, AdministrativeOperation};
    #[test]
    fn administrative_v4_sixth_value_is_direct_and_ordinary_v3_remains_distinct() {
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let group = CommitGroup {
            administration: None,
            request_id: "g".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            operation: AdministrativeOperation::SelectFirst(vec![]),
            before_fields: None,
        };
        let projection = CommitFingerprintV4 {
            request_id: &group.request_id,
            claimed_updates: &group.claimed_updates,
            state_updates: &group.state_updates,
            work_inserts: &group.work_inserts,
            work_updates: &group.work_updates,
            administration: AdministrativeFingerprintValue {
                intent: &intent,
                max_result_bytes: 300,
            },
        };
        assert_eq!(
            postcard::to_allocvec(&(&queue, projection)).unwrap(),
            b"\x01t\x01q\x01g\0\0\0\0\0\x01\0\0\xac\x02"
        );
        let fingerprint = group
            .administrative_fingerprint(&queue, &intent, 300)
            .unwrap();
        assert_eq!(
            fingerprint
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "737cd7ad8cc1315aab34c1132738be2081bf8c1b1048d6626f82e3de4de89bcc"
        );
        assert_ne!(fingerprint, group.fingerprint(&queue).unwrap());
        assert_ne!(
            fingerprint,
            group
                .administrative_fingerprint(&queue, &intent, 301)
                .unwrap()
        );
        let mut changed = intent.clone();
        changed.evaluated_at = 1;
        assert_ne!(
            fingerprint,
            group
                .administrative_fingerprint(&queue, &changed, 300)
                .unwrap()
        );
        assert_eq!(
            group.fingerprint(&queue).unwrap(),
            fingerprint_format(
                b"fireweed-api007-v3\0",
                "commit",
                &(&queue, CommitFingerprintV3::from(&group))
            )
            .unwrap()
        );
    }
}
