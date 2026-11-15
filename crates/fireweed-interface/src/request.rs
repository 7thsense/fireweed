//! Request and response types (API-006 §Request and response types).

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::caps::DurabilityLevel;
use crate::expr::{EvalError, Expr};
use crate::ids::{
    ClientItemKey, CohortId, Continuation, FieldName, GroupKey, ItemId, JsonPointer, LeaseToken,
    ObjectName, PageToken, ParamName, ReadName, RequestId, SpaceKey, UtcMillis, WorkerId,
};
use crate::schema::SpaceSchema;
use crate::value::{BlobRef, Value};

pub type Params = BTreeMap<ParamName, Value>;
/// Canonical JSON scalars by key.
pub type Metadata = BTreeMap<String, serde_json::Value>;

// ------------------------------------------------------------------ common

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WriteHeader {
    pub space: SpaceKey,
    pub request_id: RequestId,
    pub min_durability: Option<DurabilityLevel>,
    /// Recorded in history entries; not authorization.
    pub actor: Option<String>,
    pub reason: Option<String>,
}

impl WriteHeader {
    pub fn new(space: SpaceKey, request_id: impl Into<String>) -> Self {
        Self {
            space,
            request_id: RequestId::new(request_id),
            min_durability: None,
            actor: None,
            reason: None,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemRef {
    Id(ItemId),
    Key(ClientItemKey),
    IdAndKey(ItemId, ClientItemKey),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRef {
    pub item_id: ItemId,
    pub lease_token: LeaseToken,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortLeaseRef {
    pub cohort_id: CohortId,
    pub cohort_lease_token: LeaseToken,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseTarget {
    Item(LeaseRef),
    Cohort(CohortLeaseRef),
}

/// `After` is measured from request time.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Schedule {
    At(UtcMillis),
    After { delay_ms: u64 },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GateState {
    Open,
    Blocked,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Body {
    Bytes(Bytes),
    Json(serde_json::Value),
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PriorityValue {
    Int(i64),
    Timestamp(UtcMillis),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortSpec {
    pub cohort_id: CohortId,
    pub cohort_size: u32,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemState {
    Pending,
    Leased,
    Complete,
    Failed,
}

impl ItemState {
    pub fn as_str(self) -> &'static str {
        match self {
            ItemState::Pending => "pending",
            ItemState::Leased => "leased",
            ItemState::Complete => "complete",
            ItemState::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryResult {
    pub status: EntryStatus,
    pub item_id: Option<ItemId>,
    /// `ReplacePending`: the replaced item.
    pub superseded_item_id: Option<ItemId>,
    /// `item_version` or `fw_row_version` after the entry.
    pub version: Option<u64>,
    /// Claim, renew, reassign.
    pub lease: Option<NewLease>,
}

impl EntryResult {
    pub fn status(status: EntryStatus) -> Self {
        Self {
            status,
            item_id: None,
            superseded_item_id: None,
            version: None,
            lease: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewLease {
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcMillis,
    pub item_version: u64,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EntryStatus {
    Accepted,
    Duplicate,
    Replaced,
    Updated,
    Completed,
    Failed,
    Retried,
    Released,
    Rearmed,
    Renewed,
    Reassigned,
    Redriven,
    Purged,
    Inserted,
    Deleted,
    Appended,
    Skipped,
    Inapplicable,
    NotFound,
    Conflict,
    UniqueViolation { field: FieldName },
    StaleLease,
    VersionMismatch,
    Terminal,
    Superseded,
    NotEligible,
    Leased,
    CohortFailed,
    MissingField { field: FieldName },
    ValueTooLarge,
    BlobMissing,
    EvaluationFault(EvalError),
    Invalid { reason: String },
}

impl EntryStatus {
    /// True if the status rejects a group commit (API-006 §Rejecting conditions).
    pub fn rejects_group(&self) -> bool {
        matches!(
            self,
            EntryStatus::Conflict
                | EntryStatus::UniqueViolation { .. }
                | EntryStatus::StaleLease
                | EntryStatus::VersionMismatch
                | EntryStatus::Terminal
                | EntryStatus::Superseded
                | EntryStatus::CohortFailed
                | EntryStatus::MissingField { .. }
                | EntryStatus::ValueTooLarge
                | EntryStatus::BlobMissing
                | EntryStatus::EvaluationFault(_)
                | EntryStatus::Invalid { .. }
        )
    }
}

// ------------------------------------------------------------------ schema

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApplySchemaRequest {
    pub space: SpaceKey,
    pub schema: SpaceSchema,
    pub expected_version: Option<u64>,
    pub min_durability: Option<DurabilityLevel>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplySchemaResponse {
    pub version: u64,
    pub changed: bool,
    pub durability: DurabilityLevel,
}

// --------------------------------------------------------------- hot verbs

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewItem {
    /// `None`: no dedupe.
    pub client_item_key: Option<ClientItemKey>,
    pub priority: PriorityValue,
    pub not_before: Option<Schedule>,
    pub group_key: Option<GroupKey>,
    pub cohort: Option<CohortSpec>,
    pub gate_keys: BTreeSet<String>,
    pub metadata: Metadata,
    pub body: Option<Body>,
    /// Stored fields only.
    pub fields: BTreeMap<FieldName, Value>,
}

impl NewItem {
    pub fn new(key: Option<&str>, priority: PriorityValue) -> Self {
        Self {
            client_item_key: key.map(ClientItemKey::new),
            priority,
            not_before: None,
            group_key: None,
            cohort: None,
            gate_keys: BTreeSet::new(),
            metadata: Metadata::new(),
            body: None,
            fields: BTreeMap::new(),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyConflict {
    Duplicate,
    ReplacePending,
    Reject,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnqueueRequest {
    pub header: WriteHeader,
    pub queue: ObjectName,
    pub items: Vec<NewItem>,
    pub on_key_conflict: KeyConflict,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnqueueResponse {
    pub entries: Vec<EntryResult>,
    pub durability: DurabilityLevel,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimUnit {
    Items,
    WholeGroups { max_groups: u32 },
    WholeCohort,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub header: WriteHeader,
    pub queue: ObjectName,
    pub worker_id: WorkerId,
    pub max_items: u32,
    pub lease_duration_ms: u64,
    pub eligibility_time: Option<UtcMillis>,
    pub filter: Option<Expr>,
    pub params: Params,
    pub group_key: Option<GroupKey>,
    pub same_group_key: bool,
    pub unit: ClaimUnit,
}

impl ClaimRequest {
    pub fn new(
        header: WriteHeader,
        queue: &str,
        worker: &str,
        max_items: u32,
        lease_duration_ms: u64,
    ) -> Self {
        Self {
            header,
            queue: ObjectName::new(queue),
            worker_id: WorkerId::new(worker),
            max_items,
            lease_duration_ms,
            eligibility_time: None,
            filter: None,
            params: Params::new(),
            group_key: None,
            same_group_key: false,
            unit: ClaimUnit::Items,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClaimedItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    /// `None` for members of a cohort lease.
    pub lease_token: Option<LeaseToken>,
    pub lease_expires_at: UtcMillis,
    pub item_version: u64,
    pub attempt_count: u32,
    pub priority: PriorityValue,
    pub not_before: Option<UtcMillis>,
    pub group_key: Option<GroupKey>,
    pub cohort_id: Option<CohortId>,
    pub gate_keys: BTreeSet<String>,
    pub metadata: Metadata,
    pub body: Option<Body>,
    pub fields: BTreeMap<FieldName, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CohortLease {
    pub cohort_id: CohortId,
    pub cohort_lease_token: LeaseToken,
    pub lease_expires_at: UtcMillis,
    pub member_ids: Vec<ItemId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClaimResponse {
    pub items: Vec<ClaimedItem>,
    pub cohort: Option<CohortLease>,
    pub queue_paused: bool,
    pub scan_limited: bool,
    pub as_of: UtcMillis,
    pub durability: DurabilityLevel,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClaimByItemIdsRequest {
    pub header: WriteHeader,
    pub queue: ObjectName,
    pub worker_id: WorkerId,
    pub item_ids: Vec<ItemId>,
    pub lease_duration_ms: u64,
    pub eligibility_time: Option<UtcMillis>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClaimByItemIdsResponse {
    /// Distinct ids, first-occurrence order.
    pub results: Vec<(ItemId, Result<ClaimedItem, EntryStatus>)>,
    pub durability: DurabilityLevel,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RenewRequest {
    pub header: WriteHeader,
    pub queue: ObjectName,
    pub leases: Vec<LeaseTarget>,
    pub lease_duration_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RenewResponse {
    pub entries: Vec<EntryResult>,
    pub durability: DurabilityLevel,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FinalizeOutcome {
    Complete,
    Fail {
        failure_code: String,
    },
    Retry {
        not_before: Schedule,
        priority: Option<PriorityValue>,
    },
    Release,
    Rearm {
        not_before: Schedule,
        priority: Option<PriorityValue>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FinalizeEntry {
    pub target: LeaseTarget,
    /// Item targets only.
    pub expected_item_version: Option<u64>,
    pub outcome: FinalizeOutcome,
    /// Replaces item metadata.
    pub metadata: Option<Metadata>,
    /// Replaces the body, then `patch`, then `set`.
    pub body: Option<Body>,
    pub patch: Vec<BodyPatch>,
    pub set: Vec<(FieldName, Expr)>,
}

impl FinalizeEntry {
    pub fn new(target: LeaseTarget, outcome: FinalizeOutcome) -> Self {
        Self {
            target,
            expected_item_version: None,
            outcome,
            metadata: None,
            body: None,
            patch: vec![],
            set: vec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FinalizeRequest {
    pub header: WriteHeader,
    pub queue: ObjectName,
    pub entries: Vec<FinalizeEntry>,
    pub params: Params,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FinalizeResponse {
    pub entries: Vec<EntryResult>,
    pub durability: DurabilityLevel,
}

// ------------------------------------------------------------ group commit

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub header: WriteHeader,
    pub ops: Vec<WriteOp>,
    pub params: Params,
    pub dry_run: bool,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WriteOp {
    Enqueue {
        queue: ObjectName,
        items: Vec<NewItem>,
        on_key_conflict: KeyConflict,
    },
    Finalize {
        queue: ObjectName,
        entries: Vec<FinalizeEntry>,
    },
    Renew {
        queue: ObjectName,
        leases: Vec<LeaseTarget>,
        lease_duration_ms: u64,
    },
    Reassign {
        queue: ObjectName,
        leases: Vec<LeaseRef>,
        to_worker: WorkerId,
        lease_duration_ms: u64,
    },
    SetGates {
        queue: ObjectName,
        gates: Vec<(String, GateState)>,
    },
    SetQueueState {
        queue: ObjectName,
        paused: Option<bool>,
        drain_intake: Option<bool>,
    },
    PutRows {
        table: ObjectName,
        rows: Vec<BTreeMap<FieldName, Value>>,
        on_conflict: RowConflict,
    },
    Append {
        log: ObjectName,
        entries: Vec<LogEntry>,
    },
    Mutate(Mutate),
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowConflict {
    Reject,
    Skip,
    Replace,
    Merge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub fields: BTreeMap<FieldName, Value>,
    pub body: Option<Body>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Mutate {
    /// A queue or table.
    pub target: ObjectName,
    pub selection: Selection,
    pub action: Action,
    /// `1 ..= max_selector_affected`.
    pub limit: u32,
    pub expect: Option<MatchExpect>,
    /// Queues only.
    pub lease_policy: LeasePolicy,
    pub chunk: Option<Chunk>,
}

impl Mutate {
    pub fn new(target: &str, selection: Selection, action: Action, limit: u32) -> Self {
        Self {
            target: ObjectName::new(target),
            selection,
            action,
            limit,
            expect: None,
            lease_policy: LeasePolicy::Skip,
            chunk: None,
        }
    }
}

/// On `matched`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchExpect {
    pub min: u64,
    pub max: u64,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Selection {
    Keys {
        keys: Vec<Keyed>,
        filter: Option<Expr>,
    },
    Where {
        predicate: Expr,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Keyed {
    pub key: KeyRef,
    /// `Some(0)`: must not exist.
    pub expected_version: Option<u64>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum KeyRef {
    Item(ItemRef),
    Row(Vec<Value>),
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Action {
    Set(Vec<(FieldName, Expr)>),
    Patch(Vec<BodyPatch>),
    ReplaceBody(Option<Body>),
    /// `None` removes a key.
    SetMetadata(BTreeMap<String, Option<serde_json::Value>>),
    Reschedule {
        not_before: ScheduleChange,
        priority: Option<Expr>,
    },
    Transition(Transition),
    Gates {
        add: BTreeSet<String>,
        remove: BTreeSet<String>,
    },
    Retire,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ScheduleChange {
    Keep,
    Clear,
    Set(Expr),
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Transition {
    Complete { code: Option<String> },
    Fail { failure_code: String },
    Retry { not_before: Expr },
    Release,
    Redrive,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BodyPatch {
    Set {
        pointer: JsonPointer,
        value: Expr,
    },
    Remove {
        pointer: JsonPointer,
    },
    Append {
        pointer: JsonPointer,
        value: Expr,
        keep_last: Option<u32>,
    },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LeasePolicy {
    Skip,
    Reject,
    Preserve,
    MatchToken(LeaseToken),
    MatchCohortToken(LeaseToken),
    Break,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Chunk {
    Start,
    Continue(Continuation),
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitStatus {
    Committed,
    Rejected,
    DryRun,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitResponse {
    pub status: CommitStatus,
    /// Request time.
    pub as_of: UtcMillis,
    /// Committed and Rejected (the record's level).
    pub durability: Option<DurabilityLevel>,
    /// Aligned with request ops.
    pub ops: Vec<OpResult>,
    /// Rejected only.
    pub failure: Option<OpFailure>,
    pub continuation: Option<Continuation>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OpResult {
    /// False after the failing op.
    pub evaluated: bool,
    pub matched: u64,
    pub affected: u64,
    pub skipped_leased: u64,
    /// Chunk steps only.
    pub skipped_cohorts: u64,
    pub inapplicable: u64,
    /// Explicit entries and addressed keys.
    pub entries: Vec<EntryResult>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpFailure {
    pub op_index: u32,
    pub entry_index: Option<u32>,
    pub reason: FailureReason,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FailureReason {
    Entry(EntryStatus),
    MatchExpectUnmet { matched: u64 },
    LimitExceeded { matched_at_least: u64 },
    ScanLimitExceeded,
    LeasedMatch,
    CohortSplit { cohort_id: CohortId },
    Evaluation(EvalError),
}

// --------------------------------------------------------- request records

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetRequestRequest {
    pub space: SpaceKey,
    pub request_id: RequestId,
}

/// A recorded response, by request kind.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RecordedResponse {
    Enqueue(EnqueueResponse),
    Claim(ClaimResponse),
    ClaimByItemIds(ClaimByItemIdsResponse),
    Renew(RenewResponse),
    Finalize(FinalizeResponse),
    Commit(CommitResponse),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestRecord {
    pub request_id: RequestId,
    /// Hex SHA-256 of the canonical request with `min_durability` removed.
    pub fingerprint: String,
    pub recorded_at: UtcMillis,
    /// Lease tokens are redacted when returned by `get_request`.
    pub response: RecordedResponse,
}

// ------------------------------------------------------------------ reads

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetRequest {
    pub space: SpaceKey,
    pub targets: Vec<GetTarget>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GetTarget {
    Item {
        queue: ObjectName,
        item: ItemRef,
        include_terminal: bool,
    },
    Row {
        table: ObjectName,
        key: Vec<Value>,
    },
    QueueState {
        queue: ObjectName,
    },
    Blob {
        blob: BlobRef,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetResponse {
    pub as_of: UtcMillis,
    pub results: Vec<Option<GetResult>>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GetResult {
    Item(ItemView),
    Row(RowView),
    QueueState(QueueStateView),
    Blob { size: u64, media_type: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ItemView {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub state: ItemState,
    pub item_version: u64,
    pub attempt_count: u32,
    pub priority: PriorityValue,
    pub not_before: Option<UtcMillis>,
    pub group_key: Option<GroupKey>,
    pub cohort_id: Option<CohortId>,
    pub gate_keys: BTreeSet<String>,
    /// Never the token.
    pub lease: Option<LeaseSummary>,
    pub terminal: Option<TerminalSummary>,
    pub metadata: Metadata,
    pub body: Option<Body>,
    pub fields: BTreeMap<FieldName, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseSummary {
    pub worker_id: WorkerId,
    pub lease_expires_at: UtcMillis,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSummary {
    pub outcome: ItemState,
    pub code: Option<String>,
    pub terminal_at: UtcMillis,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowView {
    pub fields: BTreeMap<FieldName, Value>,
    pub row_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStateView {
    pub paused: bool,
    pub drain_intake: bool,
    pub blocked_gates: Vec<(String, u64)>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadRequest {
    pub space: SpaceKey,
    pub name: ReadName,
    pub version: u32,
    pub params: Params,
    pub page_token: Option<PageToken>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadResponse {
    pub as_of: UtcMillis,
    pub rows: Vec<BTreeMap<FieldName, Value>>,
    pub next_page_token: Option<PageToken>,
    pub scan_limited: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadLogRequest {
    pub space: SpaceKey,
    pub log: ObjectName,
    pub from: LogCursor,
    pub max_entries: u32,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LogCursor {
    Start,
    After(u64),
    Partition { key: Value, after: Option<u64> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadLogResponse {
    pub entries: Vec<LogEntryView>,
    pub next: LogCursor,
    pub earliest_retained: Option<u64>,
    pub as_of: UtcMillis,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntryView {
    pub seq: u64,
    pub appended_at: UtcMillis,
    pub fields: BTreeMap<FieldName, Value>,
    pub body: Option<Body>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsRequest {
    pub space: SpaceKey,
    pub queue: ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetrics {
    pub as_of: UtcMillis,
    pub pending: u64,
    pub leased: u64,
    pub complete: u64,
    pub failed: u64,
    pub retry_backlog: u64,
    pub oldest_eligible_age_ms: Option<u64>,
    pub progress_bound_risk_count: u64,
    pub active_leases: u64,
    pub recurring_pending: Option<u64>,
    pub recurring_leased: Option<u64>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Granularity {
    Queue,
    Group,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscoverRequest {
    pub space: SpaceKey,
    /// Allow-list from the adapter; `None` = all.
    pub queues: Option<BTreeSet<ObjectName>>,
    pub granularity: Granularity,
    pub group_key: Option<GroupKey>,
    pub max_results: u32,
    pub page_token: Option<PageToken>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscoverResponse {
    pub scopes: Vec<ActiveScope>,
    pub next_page_token: Option<PageToken>,
    pub partial: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActiveScope {
    pub queue: ObjectName,
    pub group_key: Option<GroupKey>,
    pub oldest_eligible_age_ms: u64,
    pub eligible_count: Option<u64>,
    pub observed_at: UtcMillis,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PutBlobRequest {
    pub space: SpaceKey,
    pub bytes: Bytes,
    pub media_type: String,
    pub min_durability: Option<DurabilityLevel>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PutBlobResponse {
    pub blob: BlobRef,
    pub durability: DurabilityLevel,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetBlobRequest {
    pub space: SpaceKey,
    pub blob: BlobRef,
}
