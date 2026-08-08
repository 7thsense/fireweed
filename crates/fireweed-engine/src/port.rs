//! Driven and driving ports (TD-007 §2, plan §2.1).
//!
//! Hexagonal: these traits are defined by the domain and implemented by adapters. The engine
//! depends on nothing outward. Storage-facing operations are asynchronous through the complete engine
//! path. Ordinary mutations use typed operation ports; conformance and fault injection use the owned
//! [`RawCommitRequest`] seam. Backends never accept caller-supplied transaction closures.

use std::collections::BTreeMap;

use bytes::Bytes;
use fireweed_core::{
    BodyHash, BoundedMutationRequest, BoundedMutationResponse, ClaimByQueryRequest, ClientItemKey,
    CohortId, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, GroupKey,
    GroupedAggregateRequest, GroupedAggregateResponse, ItemId, ItemState, LeaseToken, Metadata,
    MetricsByQueryRequest, PriorityValue, QueryCapabilityFlags, QueueDefinition, QueueId,
    RangeScanRequest, RangeScanResponse, RequestId, TenantId, UtcTimestamp, WorkerId,
};

// ---------------------------------------------------------------------------
// Backend-erased item mutation
// ---------------------------------------------------------------------------

/// Mandatory, backend-independent item mutation port. Implementations resolve selectors and plan the
/// complete mutation while holding their queue-local write gate, then persist only addressed item ids and
/// exact patches. A selector is never part of the durable application command.
pub trait ItemMutationPort: Send + Sync {
    fn mutate_items(
        &self,
        shard: &QueueKey,
        request: ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<ItemMutationResponse>> + Send;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ItemMutationRequest {
    pub request_id: RequestId,
    pub evaluated_at: UtcTimestamp,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub returning: ItemMutationReturning,
    #[serde(default)]
    pub gate_changes: Vec<GateChange>,
    pub operation: ItemMutationOperation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ItemMutationReturning {
    #[default]
    Identity,
    BeforeSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GateChange {
    pub gate_keys: Vec<String>,
    pub blocked: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ItemMutationOperation {
    Addressed { entries: Vec<AddressedMutation> },
    SelectFirst { clauses: Vec<SelectedMutation> },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AddressedMutation {
    pub item_id: ItemId,
    pub expected_item_version: Option<u64>,
    #[serde(default)]
    pub predicates: Vec<ItemPredicate>,
    #[serde(default)]
    pub lease_guard: LeaseGuard,
    pub patch: ItemPatch,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectedMutation {
    pub selector_id: String,
    pub selector: ItemSelector,
    /// Preconditions evaluated after this selector wins first-match ownership.
    /// A failed precondition remains a matched, rejected result and MUST NOT
    /// fall through to a later selector.
    #[serde(default)]
    pub predicates: Vec<ItemPredicate>,
    #[serde(default)]
    pub lease_guard: LeaseGuard,
    pub patch: ItemPatch,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ItemSelector {
    pub scope: ItemSelectorScope,
    #[serde(default)]
    pub predicates: Vec<ItemPredicate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ItemSelectorScope {
    Live,
    Retained,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ItemPredicate {
    Any(Vec<ItemPredicate>),
    All(Vec<ItemPredicate>),
    Not(Box<ItemPredicate>),
    StateIn(Vec<ItemState>),
    AttemptCountEq(u32),
    LeaseActive(bool),
    NotBefore {
        comparison: TimestampComparison,
        value: UtcTimestamp,
    },
    ClientItemKeyEq(ClientItemKey),
    GroupKeyEq(Option<GroupKey>),
    FieldEq {
        name: String,
        value: Option<Bytes>,
    },
    MetadataEq {
        name: String,
        value: Option<fireweed_core::MetadataValue>,
    },
    EntityEq {
        pointer: String,
        value: EntityPredicateValue,
    },
    GateKeyPresent(String),
    GateKeyAbsent(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimestampComparison {
    Equal,
    Before,
    BeforeOrEqual,
    After,
    AfterOrEqual,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EntityPredicateValue {
    Missing,
    Value(serde_json::Value),
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LeaseGuard {
    #[default]
    RejectActive,
    RequireActive,
    Match(LeaseToken),
    InvalidateActive,
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ItemPatch {
    #[serde(default)]
    pub lifecycle: LifecyclePatch,
    #[serde(default)]
    pub priority: BatchUpdateValue<Option<PriorityValue>>,
    #[serde(default)]
    pub not_before: BatchUpdateValue<Option<UtcTimestamp>>,
    #[serde(default)]
    pub payload: BatchUpdateValue<Option<Bytes>>,
    #[serde(default)]
    pub metadata: BatchUpdateValue<Metadata>,
    #[serde(default)]
    pub gate_keys: GateKeyDelta,
    #[serde(default)]
    pub field_edits: BTreeMap<String, Option<Bytes>>,
    #[serde(default)]
    pub entity_edits: Vec<EntityEdit>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LifecyclePatch {
    #[default]
    Keep,
    SetPending,
    SetComplete,
    SetFailed,
    Purge,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GateKeyDelta {
    pub add: Vec<String>,
    pub remove: Vec<String>,
    /// Remove every current membership whose key starts with one of these
    /// non-empty prefixes. Resolution happens during planning; the durable
    /// command stores only the final gate-key set.
    #[serde(default)]
    pub remove_prefixes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EntityEdit {
    /// RFC 6901 JSON Pointer. The empty pointer addresses the document root.
    pub pointer: String,
    pub operation: EntityEditOperation,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EntityEditOperation {
    Set(serde_json::Value),
    Remove,
}

/// Complete caller-owned projection row returned by `BeforeSnapshot`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ItemMutationSnapshot {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub item_version: u64,
    pub lifecycle_state: ItemState,
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub cohort_size: Option<u64>,
    pub not_before: Option<UtcTimestamp>,
    pub eligible_since: UtcTimestamp,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
    pub gate_keys: Vec<String>,
    pub entity: Option<serde_json::Value>,
    pub lease_token: Option<LeaseToken>,
    pub lease_expires_at: Option<UtcTimestamp>,
    pub lease_is_cohort: bool,
    pub worker_id: Option<WorkerId>,
    pub fenced: bool,
    pub superseded: bool,
    pub terminal_at: Option<UtcTimestamp>,
    pub terminal_position: Option<CommandPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ItemMutationOutcome {
    Updated { item_version: u64, state: ItemState },
    Purged,
    WouldUpdate { item_version: u64, state: ItemState },
    WouldPurge,
    NoChange,
    NotFound,
    Conflict { actual_version: u64 },
    StaleLease,
    PreconditionFailed(ItemMutationPrecondition),
    Invalid,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ItemMutationPrecondition {
    ActiveLease,
    Lifecycle,
    Predicate,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ItemMutationResult {
    pub item_id: ItemId,
    pub selector_id: Option<String>,
    pub outcome: ItemMutationOutcome,
    pub before: Option<ItemMutationSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ItemMutationSelectorAggregate {
    pub selector_id: String,
    pub matched: u64,
    pub changed: u64,
    pub purged: u64,
    pub rejected: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ItemMutationSummary {
    pub matched: u64,
    pub changed: u64,
    pub purged: u64,
    pub unchanged: u64,
    pub rejected: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ItemMutationResponse {
    pub request_id: RequestId,
    pub position: Option<CommandPosition>,
    pub dry_run: bool,
    pub results: Vec<ItemMutationResult>,
    pub selectors: Vec<ItemMutationSelectorAggregate>,
    pub summary: ItemMutationSummary,
}

use crate::claim_validation::ClaimCompatibility;
use crate::command::{
    ChangeRecord, CommandEnvelope, CommandId, FinalizeKind, FinalizeOutcome, SetGatesCommand,
    SideRecord,
};
use crate::error::{EngineError, EngineResult};
use crate::types::{CommandPosition, DurabilityClass, QueueKey};
use crate::{ProjectionStore, RawCommitOutcome, RawCommitRequest};

/// API-001 write-reserved item field names. These names are emitted by the claimed-item / lease wire
/// shapes, so user-authored field writes must reject them before commit or RESP rendering.
pub fn is_api001_reserved_write_field(field: &str) -> bool {
    field.eq_ignore_ascii_case("item_id")
        || field.eq_ignore_ascii_case("client_item_key")
        || field.eq_ignore_ascii_case("item_version")
        || field.eq_ignore_ascii_case("lifecycle_state")
        || field.eq_ignore_ascii_case("priority")
        || field.eq_ignore_ascii_case("attempt_count")
        || field.eq_ignore_ascii_case("payload")
        || field.eq_ignore_ascii_case("group_key")
        || field.eq_ignore_ascii_case("not_before")
        || field.eq_ignore_ascii_case("metadata")
        || field.eq_ignore_ascii_case("max_attempts")
        || field.eq_ignore_ascii_case("gate_keys")
        || field.eq_ignore_ascii_case("cohort_id")
        || field.eq_ignore_ascii_case("lease_token")
        || field.eq_ignore_ascii_case("lease_expires_at")
}

/// Reject a write delta that collides with API-001 reserved field names.
pub fn validate_api001_reserved_write_fields(
    field_ops: &BTreeMap<String, Option<Bytes>>,
) -> EngineResult<()> {
    if field_ops
        .keys()
        .any(|field| is_api001_reserved_write_field(field))
    {
        return Err(EngineError::Invalid("reserved field name"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Backend descriptors and typed raw-commit seam
// ---------------------------------------------------------------------------

/// Common backend descriptors plus the owned raw-commit operation used by conformance and recovery probes.
/// Ordinary production mutations use their operation-specific ports.
pub trait Backend: Send + Sync {
    fn durability_class(&self) -> DurabilityClass;

    /// Whether this backend stores gate membership and enforces `SetGates` at claim selection.
    fn supports_gates(&self) -> bool {
        false
    }

    /// The authoritative-commit capability descriptors (Snorri StateStore boundary, epic pqueue-2201fd37).
    /// Default = [`CommitCapabilities::default`] (all-false): a backend that has not wired the atomic commit
    /// boundary advertises NO commit guarantees, so a consumer rejects it before activation. Memory +
    /// sqlite-relational override this to advertise what they actually implement.
    fn commit_capabilities(&self) -> CommitCapabilities {
        CommitCapabilities::default()
    }

    /// Drive the append/apply boundary with an owned, typed request.
    /// Every input is owned, so a native-async adapter can transfer the request and its transaction
    /// capability into backend-owned execution before the first suspension. Dropping the caller after that
    /// transfer may discard only the response; the outcome remains resolvable by request-id replay.
    fn commit_raw(
        &self,
        request: RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<RawCommitOutcome>> + Send;
}

// ---------------------------------------------------------------------------
// Read side (async)
// ---------------------------------------------------------------------------

/// A page of committed commands for replay/rebuild.
#[derive(Debug, Clone)]
pub struct CommandPage {
    pub entries: Vec<(CommandPosition, CommandEnvelope)>,
    pub next: Option<CommandPosition>,
}

pub trait LogRead: Send + Sync {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send;
}

/// Durable change-record sink. The engine emits ordered batches to one queue shard at a time.
pub trait ChangeRecordSink: Send + Sync {
    fn emit(&self, shard: &QueueKey, records: &[ChangeRecord]) -> EngineResult<()>;
}

/// A non-destructive view of an eligible item (RESP `peek` / library read).
#[derive(Debug, Clone)]
pub struct ItemView {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub priority: Option<PriorityValue>,
    pub item_version: u64,
}

/// A live item addressed by `client_item_key`.
///
/// "Live" means still owned by the queue as active work: pending or leased, not terminal and not
/// superseded. The view intentionally includes the existing opaque payload plus the structured field map
/// so fireweed can serve as hot storage for compound work records without forcing callers to maintain a
/// second snapshot store.
#[derive(Debug, Clone)]
pub struct LiveItemView {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub item_version: u64,
    pub lifecycle_state: ItemState,
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub attempt_count: u32,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
}

/// A view of an in-flight (leased) item (RESP `XPENDING` / library read).
#[derive(Debug, Clone)]
pub struct LeaseView {
    pub item_id: ItemId,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    pub attempt_count: u32,
}

/// Set-based summary of the visible pending-entry list (PEL).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingSummary {
    pub count: u64,
    pub min_id: Option<ItemId>,
    pub max_id: Option<ItemId>,
    pub consumers: Vec<(LeaseToken, u64)>,
}

/// One bounded, insertion-id-ordered PEL page. `next` is the first entry not
/// returned and can be passed back as the next inclusive cursor.
#[derive(Debug, Clone, Default)]
pub struct PendingPage {
    pub entries: Vec<LeaseView>,
    pub next: Option<ItemId>,
}

/// Lifecycle counts + bound metrics (RESP `XLEN`/`XINFO` basic; rich is library-only).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueueMetrics {
    pub pending: u64,
    pub leased: u64,
    pub complete: u64,
    pub failed: u64,
    #[serde(default)]
    pub resident_terminal_count: u64,
}

/// Terminal-item residency plus emission-lag observability for production metrics surfaces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TerminalEmissionMetrics {
    pub resident_terminal_count: u64,
    pub emission_lag_commands: u64,
    pub emission_oldest_unemitted_age_ms: u64,
}

pub trait ProjectionRead: Send + Sync {
    /// Priority-ordered eligible candidates (Eligibility Precedence, API-001). The claim path
    /// leases from these in the same unit of work (Invariant 1: per-item delivery, no cursor).
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send;

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send;

    /// Aggregate the PEL without returning one Rust value per leased item.
    ///
    /// The default preserves source compatibility for external backends. Production
    /// backends override it with an aggregate/index-backed implementation.
    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        async move { Ok(summarize_pending(self.pending(shard).await?)) }
    }

    /// Read at most `limit` PEL entries at or after `start`, plus an opaque next
    /// cursor. Production implementations push the cursor and `limit + 1` into
    /// their storage/index layer.
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        async move { Ok(page_pending(self.pending(shard).await?, start, limit)) }
    }

    /// Read a bounded XPENDING range. Bounds are inclusive; `consumer` narrows
    /// the result to one live lease token.
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        async move {
            let start = start.map(|id| id.as_u64()).unwrap_or(0);
            let end = end.map(|id| id.as_u64()).unwrap_or(u64::MAX);
            let mut leases = self.pending(shard).await?;
            leases.sort_by_key(|lease| lease.item_id);
            Ok(leases
                .into_iter()
                .filter(|lease| {
                    (start..=end).contains(&lease.item_id.as_u64())
                        && consumer.is_none_or(|token| token == &lease.lease_token)
                })
                .take(limit)
                .collect())
        }
    }

    /// Fetch PEL metadata for only the requested IDs, preserving request order.
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        async move {
            let wanted: std::collections::HashSet<_> = ids.iter().copied().collect();
            let by_id: std::collections::HashMap<_, _> = self
                .pending(shard)
                .await?
                .into_iter()
                .filter(|lease| wanted.contains(&lease.item_id))
                .map(|lease| (lease.item_id, lease))
                .collect();
            Ok(ids.iter().filter_map(|id| by_id.get(id).cloned()).collect())
        }
    }

    /// Render the rich claimed-item shape for specific (currently-leased) `ids` — the RESP `XCLAIM` reply
    /// (and any read that needs an in-flight item's full payload/fields, not just the [`LeaseView`]).
    /// Ids that are absent or not in a renderable state are silently omitted (the caller knows the set it
    /// just acted on).
    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send;

    /// Render live hot-storage items by client key, preserving input order. A missing, terminal, purged,
    /// or superseded item renders as `None`; leased items are still live and render normally.
    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send;

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send;

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&crate::types::CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send;
}

/// Compatibility helper used by the default PEL read methods and small in-memory
/// projections. Storage backends should aggregate in storage instead.
pub fn summarize_pending(leases: Vec<LeaseView>) -> PendingSummary {
    let mut consumers = std::collections::HashMap::<LeaseToken, u64>::new();
    let mut min_id = None;
    let mut max_id = None;
    for lease in &leases {
        min_id = Some(min_id.map_or(lease.item_id, |id: ItemId| id.min(lease.item_id)));
        max_id = Some(max_id.map_or(lease.item_id, |id: ItemId| id.max(lease.item_id)));
        *consumers.entry(lease.lease_token.clone()).or_default() += 1;
    }
    let mut consumers: Vec<_> = consumers.into_iter().collect();
    consumers.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
    PendingSummary {
        count: leases.len() as u64,
        min_id,
        max_id,
        consumers,
    }
}

/// Compatibility helper that bounds allocation to `limit + 1` after ordering.
pub fn page_pending(
    mut leases: Vec<LeaseView>,
    start: Option<ItemId>,
    limit: usize,
) -> PendingPage {
    leases.sort_by_key(|lease| lease.item_id);
    let start = start.map(|id| id.as_u64()).unwrap_or(0);
    let mut selected = leases
        .into_iter()
        .filter(|lease| lease.item_id.as_u64() >= start)
        .take(limit.saturating_add(1));
    let entries: Vec<_> = selected.by_ref().take(limit).collect();
    let next = selected.next().map(|lease| lease.item_id);
    PendingPage { entries, next }
}

// ---------------------------------------------------------------------------
// Secondary-index query (ADR-010): exact composite-key lookup over configured item fields
// ---------------------------------------------------------------------------

/// One hit from a secondary-index lookup — enough to identify and re-read the item. Always carries the
/// item's CURRENT `item_version` (read-after-write).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHit {
    pub client_item_key: ClientItemKey,
    pub item_id: ItemId,
    pub item_version: u64,
}

/// Read port for per-queue secondary indexes (ADR-010 §6). The `key` is the per-field value bytes in
/// field order; the port encodes the §4.1 composite key and probes the index. The in-memory log-replay
/// family implements this over its shared `ProjectionData`; the relational family returns
/// [`EngineError::Unavailable`](crate::EngineError::Unavailable) until Phase 2 wires the side index table.
#[doc(hidden)]
pub trait IndexQueryPort: Send + Sync {
    /// Exact composite-key get on a UNIQUE index. `Ok(None)` if no item holds the key;
    /// [`EngineError::Invalid`](crate::EngineError::Invalid) if `index` is not a unique index on this queue.
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send;

    /// Exact composite-key lookup on a (non-unique or unique) index. Returns all matching items ordered
    /// by `item_id` ascending; empty if none.
    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send;
}

// ---------------------------------------------------------------------------
// Claim & upsert (atomic with selection)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ClaimRequest {
    pub shard: QueueKey,
    pub worker_id: WorkerId,
    pub max_items: usize,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    /// Operational claim time: what the lease/command stamping is measured against (`lease_expires_at`
    /// is `now + lease duration` for the ordinary caller), NOT necessarily what decides due-ness — see
    /// [`eligibility_time`](Self::eligibility_time).
    pub now: UtcTimestamp,
    /// Caller-resolved eligibility epoch — the "as of" time that decides which items are DUE
    /// (`not_before <= eligibility_time`, half-open, so an item is due AT its `not_before`). `None` ⇒
    /// fall back to `now`, which is the single-clock behaviour every pre-existing caller had.
    ///
    /// Set this when selecting *scheduled* work for an execution epoch that is not the operational
    /// clock: the eligibility scan runs at this epoch while `now` / `lease_expires_at` stay on
    /// operational time, so the resulting leases remain valid against the real clock. It is purely a
    /// SELECTION input — backends MUST NOT stamp commands, leases, or lease expiry with it.
    ///
    /// Read it through [`eligibility_at`](Self::eligibility_at) rather than matching on the `Option`.
    pub eligibility_time: Option<UtcTimestamp>,
    /// API-001 Batch Claim compatibility options (group_key / same_group_key / metadata_equals /
    /// group_batching / whole_cohort). `ClaimCompatibility::default()` is an item-level claim
    /// ([`ClaimUnit::Item`](crate::ClaimUnit)) — backends resolve the unit via
    /// [`require_item_level_claim`](crate::require_item_level_claim) and (BQ-14a) admit Item; the
    /// group/cohort selection units land in BQ-14b/c.
    pub compatibility: ClaimCompatibility,
    /// The owner's cached acquire-time fence epoch (ADR-009 / TD-003 In-Process Library Owner-Runtime).
    /// `Some(e)` ⇒ the claim's atomic commit is fenced against `e`: if `e` is not the queue's current
    /// durable epoch (the owner has been superseded), the claim is rejected `EpochFenced` at commit and
    /// NOTHING is leased. `None` ⇒ the degenerate sole-owner path: stamp the current epoch, never fence
    /// (behaviour-preserving). The epoch MUST be the value cached at `acquire_queue_lease`, never re-read
    /// from `current_epoch` (re-reading defeats the fence).
    pub expected_epoch: Option<u64>,
}

impl ClaimRequest {
    /// The epoch a backend MUST resolve due-ness against (`not_before <= t`): the explicit
    /// [`eligibility_time`](Self::eligibility_time) when the caller supplied one, else the operational
    /// [`now`](Self::now). Every candidate-selection call in a claim goes through this; `now` stays the
    /// stamping/lease clock. Keeping the fallback in one place is what makes an unset `eligibility_time`
    /// byte-identical to the pre-existing single-clock claim.
    pub fn eligibility_at(&self) -> UtcTimestamp {
        self.eligibility_time.unwrap_or(self.now)
    }
}

/// A claimed item in the API-001 claimed-item shape (lease fields included).
///
/// `metadata`, `group_key`, `not_before`, `gate_keys`, `attempt_count`, and `max_attempts` are core
/// data-model fields included so adapters built on this shape don't force a breaking widening later
/// (review I2/I3). Per-item `max_attempts` is required for composed finalize sealing: Retry
/// exhaustion is item-scoped, not queue-default-scoped.
#[derive(Debug, Clone)]
pub struct ClaimedItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub item_version: u64,
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub lease_token: Option<LeaseToken>,
    pub lease_expires_at: UtcTimestamp,
    /// Delivery/reclaim count as of this claim (RESP delivery-count semantics; flavor-diff 7).
    pub attempt_count: u32,
    /// Item-scoped retry bound used by finalize sealing (`is_retry_exhausted`).
    pub max_attempts: u32,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
    pub gate_keys: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Claimed {
    pub items: Vec<ClaimedItem>,
    pub cohort_lease_token: Option<LeaseToken>,
    pub cohort_id: Option<CohortId>,
}

/// API-001 `BatchClaimByItemIds` response: successfully claimed items plus per-id dispositions.
#[derive(Debug, Clone, Default)]
pub struct ClaimByItemIdsResponse {
    /// Successfully claimed rows (Claimed Item Response Shape), first-occurrence request order.
    pub items: Vec<ClaimedItem>,
    /// One outcome per distinct requested id (first-occurrence order after collapsing duplicates).
    pub outcomes: Vec<fireweed_core::ClaimByItemIdsOutcome>,
}

impl PartialEq for ClaimByItemIdsResponse {
    fn eq(&self, other: &Self) -> bool {
        self.outcomes == other.outcomes
            && self.items.len() == other.items.len()
            && self
                .items
                .iter()
                .zip(other.items.iter())
                .all(|(a, b)| a.item_id == b.item_id && a.lease_token == b.lease_token)
    }
}

impl Eq for ClaimByItemIdsResponse {}

#[derive(Debug, Clone)]
pub struct CohortLeaseTarget {
    pub cohort_id: CohortId,
    pub cohort_lease_token: LeaseToken,
}

/// A backend that leases candidates atomically with selection (TD-007 §2.2). The engine is the
/// single *logical* claim authority; a backend MAY implement claim in one transaction.
#[doc(hidden)]
pub trait ClaimPort: Send + Sync {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send;
}

/// Result of `replace_if_pending` (Invariant 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpsertOutcome {
    /// No collision: a new item was appended.
    Inserted { item_id: ItemId },
    /// Colliding pending item atomically superseded; the new monotonic id is returned.
    Replaced {
        new_item_id: ItemId,
        superseded_item_id: ItemId,
    },
}

/// Whether a request-id push applied new work or returned a retained idempotent result.
///
/// Distinct from [`UpsertOutcome`]: that is `client_item_key` collision semantics, not
/// API-001 request-id batch idempotency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushDisposition {
    /// The request body was applied for the first time (or after retention expiry).
    Fresh,
    /// Same request id + same body: retained outcome was returned without re-applying.
    Replayed,
}

/// Outcome of [`PushPort::push_with_request_id`]: item ids plus replay-vs-fresh disposition.
///
/// The disposition is per **request** (the whole batch shares one request id), not per item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushBatchOutcome {
    pub disposition: PushDisposition,
    pub item_ids: Vec<ItemId>,
}

impl PushBatchOutcome {
    pub fn fresh(item_ids: Vec<ItemId>) -> Self {
        Self {
            disposition: PushDisposition::Fresh,
            item_ids,
        }
    }

    pub fn replayed(item_ids: Vec<ItemId>) -> Self {
        Self {
            disposition: PushDisposition::Replayed,
            item_ids,
        }
    }

    pub fn is_replayed(&self) -> bool {
        matches!(self.disposition, PushDisposition::Replayed)
    }

    pub fn is_fresh(&self) -> bool {
        matches!(self.disposition, PushDisposition::Fresh)
    }

    pub fn into_item_ids(self) -> Vec<ItemId> {
        self.item_ids
    }
}

impl std::ops::Deref for PushBatchOutcome {
    type Target = [ItemId];

    fn deref(&self) -> &Self::Target {
        &self.item_ids
    }
}

impl AsRef<[ItemId]> for PushBatchOutcome {
    fn as_ref(&self) -> &[ItemId] {
        &self.item_ids
    }
}

impl From<PushBatchOutcome> for Vec<ItemId> {
    fn from(value: PushBatchOutcome) -> Self {
        value.item_ids
    }
}

/// Pending-item replacement, executed in the **same unit of work as claim** so upsert and claim on
/// one item mutually exclude (TD-007 §2.3). Required for RESP `XADD` with `client_item_key`.
/// Group-commit / ack-after-seal backends may implement
/// [`replace_if_pending_ordered_independent`](UpsertPort::replace_if_pending_ordered_independent)
/// so a pipelined batch co-buffers into one seal (otherwise each scalar upsert waits alone).
#[doc(hidden)]
pub trait UpsertPort: Send + Sync {
    /// Upsert on `client_item_key`. The backend ASSIGNS the new item id from its own command sequence
    /// (restart-safe, unique across handles — like [`PushPort`]) and returns it in the `UpsertOutcome`;
    /// callers never supply an id (that would collide across two servers/handles on one backend).
    #[allow(clippy::too_many_arguments)]
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        metadata: Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send;

    /// Bounded pipelined upserts: each `PushSpec` **must** carry `client_item_key`. Default is sequential
    /// [`replace_if_pending`](Self::replace_if_pending). Group-commit backends override to enqueue every
    /// command before awaiting durability so N items share segment seals.
    fn replace_if_pending_ordered_independent(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = Vec<EngineResult<UpsertOutcome>>> + Send {
        async move {
            if items.len() > MAX_ORDERED_INDEPENDENT_PUSH_ITEMS {
                return vec![
                    Err(EngineError::Invalid(
                        "ordered independent upsert exceeds bounded item limit",
                    ));
                    items.len()
                ];
            }
            let mut outcomes = Vec::with_capacity(items.len());
            for item in items {
                let Some(key) = item.client_item_key.as_ref() else {
                    outcomes.push(Err(EngineError::Invalid(
                        "ordered independent upsert requires client_item_key on every item",
                    )));
                    continue;
                };
                outcomes.push(
                    self.replace_if_pending(
                        shard,
                        key,
                        item.priority,
                        item.group_key,
                        item.not_before,
                        item.payload,
                        item.fields,
                        item.metadata,
                        item.entity,
                        now,
                        expected_epoch,
                    )
                    .await,
                );
            }
            outcomes
        }
    }
}

/// A new-item spec for [`PushPort`]. The backend assigns the `item_id` (unique + restart-safe via its
/// own command sequence — NOT a caller-side counter, so two handles / a restart can't collide); the
/// dedup `client_item_key` defaults to that id (a unique append) when `None`.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PushSpec {
    pub client_item_key: Option<ClientItemKey>,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    pub payload: Option<Bytes>,
    /// Structured hot-storage fields for compound work records. These are item-local, mutable by
    /// replacement/upsert, and exposed through Redis-hash-shaped live read commands.
    pub fields: BTreeMap<String, Bytes>,
    /// Caller-owned item metadata used by API-001 compatibility predicates and returned verbatim in the
    /// claimed-item shape. fireweed stores and filters it without interpreting application meaning.
    pub metadata: Metadata,
    /// Declared cohort size (BQ-14c) — see [`crate::PushItem::cohort_size`]. `None` for non-cohort items.
    pub cohort_size: Option<u64>,
    /// Gate keys this item carries (BQ-14d) — see [`crate::PushItem::gate_keys`]. Empty for un-gated items.
    pub gate_keys: Vec<String>,
    /// Typed JSON entity document (ADR-011). The canonical typed representation for schema-validated
    /// typed queues. `None` for schema-less queues that use the opaque `payload` bytes carrier.
    pub entity: Option<serde_json::Value>,
}

/// Appends new items (server-assigned ids). The backend builds the envelope from its own command
/// sequence and commits through its atomic append+apply UoW after confirming the shard exists, so a
/// Push can never leave the log ahead of the projection (divergence-safe) and ids are unique across
/// handles + restart. The library facade's `push` routes here rather than reaching for the raw commit seam.
#[doc(hidden)]
pub const MAX_ORDERED_INDEPENDENT_PUSH_ITEMS: usize = 1_000;

#[doc(hidden)]
pub trait PushPort: Send + Sync {
    /// `expected_epoch`: the owner's cached acquire-time fence epoch (ADR-009 / TD-003). `Some(e)` fences the
    /// append at commit (a superseded owner → `EpochFenced`, nothing appended); `None` is the degenerate
    /// sole-owner path (stamp current, never fence).
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;

    /// Execute a bounded input sequence as distinct one-item transactions while preserving input order in
    /// the queue's serializable mutation history. Each result is independent: rejecting one item has no
    /// effect on accepted siblings. The default is deliberately sequential for backend-independent
    /// correctness; group-commit backends may override by enqueueing distinct commands in order before
    /// awaiting their durability barriers.
    fn push_ordered_independent(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = Vec<EngineResult<ItemId>>> + Send {
        async move {
            if items.len() > MAX_ORDERED_INDEPENDENT_PUSH_ITEMS {
                return vec![
                    Err(EngineError::Invalid(
                        "ordered independent push exceeds bounded item limit",
                    ));
                    items.len()
                ];
            }
            let mut outcomes = Vec::with_capacity(items.len());
            for item in items {
                outcomes.push(
                    self.push(shard, vec![item], now, expected_epoch)
                        .await
                        .and_then(|ids| {
                            if ids.len() == 1 {
                                Ok(ids[0])
                            } else {
                                Err(EngineError::Storage(
                                    "scalar push returned a non-scalar result".into(),
                                ))
                            }
                        }),
                );
            }
            outcomes
        }
    }

    /// Same append operation, but carrying API-001's envelope-level `request_id`. This is part of the
    /// external fireweed contract, so every `PushPort` implementation must provide retained replay/conflict
    /// semantics rather than silently degrading to a request-id-less push.
    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<PushBatchOutcome>> + Send;
}

/// AC-TXN-3 fault-injection seam (TP-003 §3.10 row 208, `request_id` unknown-outcome replay). Build the
/// EXACT durable `request_id`-bearing push envelope [`PushPort::push_with_request_id`] would append — the
/// same `request_id`, body fingerprint, [`crate::RequestOutcome`], and server-minted item ids (reserving the
/// counter/command-id identically) — WITHOUT committing it or recording the in-memory idempotency entry.
///
/// A fault-injection harness drives the returned envelope through the `append→apply` unit-of-work seam
/// ([`Backend::commit_raw`]) and injects a kill *before* apply; on reopen, recovery rebuilds the push-idempotency
/// map from this durable envelope (the same log fold `push_with_request_id` recovery uses), so a retry by
/// `request_id` replays the one committed result. This is what makes the mid-pipeline
/// (`AfterAppendBeforeApply`) cut point `request_id`-bearing rather than item-level: the public
/// `push_with_request_id` call is atomic and cannot be interrupted between its internal append and apply, so
/// the harness needs the durable envelope it *would* have appended in order to strike that exact instant.
///
/// This is NOT a commit path — it appends nothing and records nothing; it only reserves ids and builds the
/// envelope. Implemented by the composed log+projection backend; other backends need not provide it.
#[doc(hidden)]
pub trait RequestIdReplayProbe: Send + Sync {
    /// Build (but do not commit) the durable `request_id`-bearing push envelope and return it alongside the
    /// server-minted item ids it will carry. Validates gate/entity/index constraints exactly like
    /// `push_with_request_id` so a rejection here matches the real path; on success the caller drives the
    /// envelope through [`Backend::commit_raw`] with a mid-pipeline fault to exercise the append→apply kill window.
    fn build_request_id_push_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, Vec<ItemId>)>;

    /// The `commit_transition` twin of [`Self::build_request_id_push_envelope`], for the OTHER
    /// request_id-bearing mutating op. Build (but do not commit) the durable `request_id`-bearing FINALIZE
    /// envelope of a SINGLE-entry `commit_transition` (finalize one claimed input, no side records / lifecycle
    /// items / instance fence — so the commit is exactly one envelope), stamped with the SAME whole-body
    /// fingerprint `commit_transition` computes over that body, and return it alongside that fingerprint.
    /// Validates the `claim_ref` exactly like the real path (so a rejection here matches it). The caller drives
    /// the envelope through [`Backend::commit_raw`] with a mid-pipeline (`AfterAppendBeforeApply`) fault so the
    /// append→apply kill window is `request_id`-bearing for `commit_transition`; on reopen, recovery rebuilds
    /// the commit-idempotency cache from this durable envelope so a retry by `request_id` replays the one
    /// committed per-entry outcome. Not a commit path — appends nothing and records nothing.
    fn build_request_id_commit_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        claim_ref: ClaimRef,
        finalize: FinalizeKind,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, BodyHash)>;

    /// The MIXED-commit generalization of [`Self::build_request_id_commit_envelope`] (bead pqueue-db60657d).
    /// Build (but do not commit) the FULL durable envelope sequence a `commit_transition` of a FINALIZE-ONLY
    /// body (each entry finalizes one claimed input; no side records / lifecycle items / instance fence) would
    /// append: the committed entries' `Finalize` envelopes AND, when the result is MIXED (at least one
    /// committed AND at least one rejected), the terminal
    /// [`RequestOutcome::CommitTransition`](crate::command::RequestOutcome) marker
    /// carrying the whole per-entry outcome vec (committed AND rejected, each rejection's structured error
    /// projected durably). Every envelope is stamped with the SAME whole-body fingerprint `commit_transition`
    /// computes, so a post-reopen retry of the same body Replays (not Conflicts). Each `claim_ref` is validated
    /// exactly like the real commit path (a rejection here matches it), against the CURRENT projection with no
    /// intervening apply — correct for INDEPENDENT entries (the conformance mixed case). Appends/applies/records
    /// NOTHING: the caller drives the returned envelopes through [`crate::Backend::commit_raw`] with an
    /// `AfterAppendBeforeApply` fault to strike the durable-but-unapplied window for a mixed commit, then
    /// reopens so recovery replays the durable tail AND rebuilds `commit_idempotency` from the durable marker.
    /// Returns the envelopes plus the whole-body fingerprint. Default: [`EngineError::Unavailable`].
    fn build_request_id_commit_envelopes(
        &self,
        _shard: &QueueKey,
        _request_id: RequestId,
        _entries: Vec<CommitTransitionEntry>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> EngineResult<(Vec<CommandEnvelope>, BodyHash)> {
        Err(EngineError::Unavailable)
    }
}

/// Operator gate-state mutation. Gate support is backend-capability-specific: relational backends
/// enforce it, while log-replay backends reject it before the command is appended.
#[doc(hidden)]
pub trait SetGatesPort: Send + Sync {
    fn set_gates(
        &self,
        _shard: &QueueKey,
        _command: SetGatesCommand,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Extends the lease on in-flight items, atomically pre-validating exactly like [`FinalizePort`]: a
/// fenced lease → `StaleLease`, a superseded id → `Superseded`, terminal → `Terminal`, non-leased →
/// `Invalid`, and the `RenewLease` command is NOT appended on rejection (no divergence). Lets a long-
/// running worker extend its lease without surrendering the claim.
#[doc(hidden)]
pub trait RenewLeasePort: Send + Sync {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

#[doc(hidden)]
pub trait CohortRenewLeasePort: Send + Sync {
    fn renew_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let _ = (shard, target, new_lease_expires_at, now, expected_epoch);
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Transfer an in-flight lease to a NEW consumer (RESP cross-consumer `XCLAIM`): swap the lease token and
/// charge exactly one delivery. Pre-validated identically to renew (`reassign_validate`): the items must
/// be Leased + not fenced/superseded/terminal, else a structured rejection with NOTHING appended. The
/// same-consumer case (token unchanged) is a no-charge [`RenewLeasePort::renew`] instead.
#[doc(hidden)]
pub trait ReassignLeasePort: Send + Sync {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

/// Internal typed hard-delete port for already-resolved item ids (RESP `XDEL`, operator/library purge).
/// Returns the count actually removed
/// (ids absent from the projection are no-ops, like Redis `XDEL`). The `PurgeItems` apply is infallible
/// (remove-if-present), so the only pre-commit check is the API-001 force gate: purging a **leased** item
/// requires `force` (else `EngineError::Conflict`, nothing appended). `XDEL` passes `force = true`
/// (Redis deletes unconditionally); a library purge may pass `force = false` to honor the gate.
/// This is not the complete public API-001 `PurgeItems` surface: key-or-id targeting, request-id replay,
/// and per-item `purged`/`not_found` results are adapter responsibilities layered above this count port.
#[doc(hidden)]
pub trait PurgePort: Send + Sync {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;
}

/// Finalizes claimed items (complete/fail/retry/release/rearm), atomically validating the lease
/// before committing: an **operator-fenced** lease is rejected with `EngineError::StaleLease` and the
/// Finalize command is NOT appended (no log/projection divergence; the fencing check is pre-commit).
/// Batch is all-or-nothing in this launch slice: any fenced item fails the whole call (per-item
/// results are a later refinement).
#[doc(hidden)]
pub trait FinalizePort: Send + Sync {
    /// `expected_epoch`: the owner's cached acquire-time fence epoch (ADR-009 / TD-003). `Some(e)` fences
    /// the commit (a superseded owner → `EpochFenced`, nothing appended); `None` = degenerate sole-owner.
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

#[doc(hidden)]
pub trait CohortFinalizePort: Send + Sync {
    fn finalize_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        kind: FinalizeKind,
        not_before: Option<UtcTimestamp>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let _ = (shard, target, kind, not_before, now, expected_epoch);
        std::future::ready(Err(EngineError::Unavailable))
    }
}

// ---------------------------------------------------------------------------
// Authoritative vectorized claimed-work commit (Snorri StateStore boundary, ADR-009 / epic
// pqueue-2201fd37)
// ---------------------------------------------------------------------------

/// A lease-token-bearing reference to a claimed item, validated INSIDE the commit boundary. Public
/// finalization no longer keys on item id alone: the presented `lease_token` must equal the stored token,
/// the lease must be unexpired (half-open: valid through `lease_expires_at`), and `item_version` must
/// equal the stored version (the optimistic state fence).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClaimRef {
    pub item_id: ItemId,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    pub item_version: u64,
}

/// A caller-supplied OPAQUE instance/state fence advanced or validated INSIDE the commit boundary (Snorri
/// authoritative-commit boundary, ADR-009 / epic pqueue-2201fd37). `instance_key` is opaque bytes fireweed
/// never interprets (e.g. a workflow instance key). The commit accepts the entry only if the queue's stored
/// fence for `instance_key` equals `expected` (an `instance_key` never advanced reads as `0` — the unset
/// convention), and `next > expected` (strictly monotonic). On accept the stored fence advances to `next`
/// ATOMICALLY in the same durable boundary as the side-record writes + input finalize; on a stale `expected`
/// the entry is rejected `Conflict` and NOTHING is written; on `next <= expected` it is rejected `Invalid`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstanceFence {
    #[serde(default)]
    pub instance_key: Vec<u8>,
    #[serde(default)]
    pub expected: u64,
    #[serde(default)]
    pub next: u64,
}

/// Validate a caller-supplied [`InstanceFence`] against the queue's currently-stored fence (`0` when the
/// `instance_key` has never advanced — the unset convention). Shared by every commit backend so the
/// accept/reject decision is identical regardless of where the fence is physically stored: `next <= expected`
/// → `Invalid` (non-monotonic, a structural request error, checked first); stored `!= expected` → `Conflict`
/// (the optimistic state fence). Mutates nothing.
pub fn validate_instance_fence(stored: u64, fence: &InstanceFence) -> EngineResult<()> {
    if fence.next <= fence.expected {
        return Err(EngineError::Invalid("instance fence is not monotonic"));
    }
    if stored != fence.expected {
        return Err(EngineError::Conflict);
    }
    Ok(())
}

/// Reject a malformed atomic entry that names the same claim more than once. Without this guard the
/// validation phase could pass twice and the finalization command would attempt two lifecycle transitions
/// for one item.
pub fn validate_distinct_commit_claims(
    primary: &ClaimRef,
    additional: &[ClaimRef],
) -> EngineResult<()> {
    let mut ids = std::collections::HashSet::with_capacity(1 + additional.len());
    ids.insert(primary.item_id);
    if additional.iter().any(|claim| !ids.insert(claim.item_id)) {
        return Err(EngineError::Invalid("duplicate claim in commit entry"));
    }
    Ok(())
}

/// One entry of a vectorized transition commit: validate `claim_ref` plus any
/// `additional_claim_refs`, write opaque non-work `side_records`, enqueue ordinary `lifecycle_items`
/// (dispatchable outbox/await/timer work), and finalize every claim with `finalize`. Each entry's writes
/// commit atomically; per-entry outcomes are independent.
///
/// `Serialize` (not `Deserialize`, since [`PushSpec`] is serialize-only) so a backend can fingerprint the
/// whole commit body for request-id idempotency.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CommitTransitionEntry {
    pub claim_ref: ClaimRef,
    /// Additional claimed items finalized with the same disposition as `claim_ref`. Every claim is
    /// validated before any entry write becomes visible, so the claims and the entry's side records,
    /// lifecycle items, and instance fence form one atomic transition boundary.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_claim_refs: Vec<ClaimRef>,
    pub finalize: FinalizeKind,
    pub side_records: Vec<SideRecord>,
    pub lifecycle_items: Vec<PushSpec>,
    /// Optional caller-supplied instance/state fence advanced/validated atomically with this entry (C6).
    /// `#[serde(default)]` so existing serialized commit bodies/definitions don't churn their fingerprint.
    #[serde(default)]
    pub instance_fence: Option<InstanceFence>,
}

/// A vectorized claimed-work commit request. `request_id` drives retained replay/conflict/expired
/// idempotency over the WHOLE body (TD-007 §4); `entries` are applied independently with per-entry outcomes.
#[derive(Debug, Clone)]
pub struct CommitTransition {
    pub request_id: Option<RequestId>,
    pub entries: Vec<CommitTransitionEntry>,
}

/// The per-entry result of a [`CommitTransitionPort::commit_transition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitEntryOutcome {
    /// The entry validated and committed atomically. `lifecycle_item_ids` are the server-assigned ids of the
    /// entry's newly enqueued dispatchable items, in order (empty when the entry enqueued none).
    Committed { lifecycle_item_ids: Vec<ItemId> },
    /// The entry's `claim_ref` (or a lifecycle write) was rejected; NOTHING was mutated for this entry.
    Rejected(EngineError),
}

/// The authoritative vectorized claimed-work commit (Snorri StateStore boundary). One durable, recoverable
/// transition boundary per entry: lease-token + version-fence validation, opaque non-work side-record
/// writes, ordinary lifecycle enqueues, and input finalization — all atomic per entry, fenced by
/// `expected_epoch` like the other write ports. The default impl returns
/// [`EngineError::Unavailable`](crate::EngineError::Unavailable) so non-atomic / eventual-apply backends
/// (which cannot offer one atomic transition boundary) reject the operation rather than silently splitting it.
#[doc(hidden)]
pub trait CommitTransitionPort: Send + Sync {
    fn commit_transition(
        &self,
        _shard: &QueueKey,
        _transition: CommitTransition,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Capability descriptors for the authoritative vectorized claimed-work commit (Snorri StateStore boundary,
/// epic pqueue-2201fd37 acceptance, ADR-009). A consumer (Snorri) reads these BEFORE activation and rejects a
/// backend that does not advertise the guarantees it needs — every bool defaults to `false` (the safe default
/// when no authoritative commit boundary has been declared). Backends and composed log/projection pairs
/// advertise the capabilities they actually implement. `EventualApply` describes projection visibility; it
/// does not negate an atomic transition batch committed to an authoritative log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitCapabilities {
    /// Each commit entry's writes (side records + instance fence + lifecycle + finalize) commit atomically.
    pub atomic_transition_commit: bool,
    /// A single call commits a VECTOR of independent entries with per-entry outcomes.
    pub vectorized_commit: bool,
    /// The claim reference's lease token + lease expiry are validated inside the commit boundary.
    pub lease_validation: bool,
    /// Caller `request_id`s have retained replay/conflict/expired semantics over the whole commit body.
    pub retained_commit_idempotency: bool,
    /// Opaque non-work side records that are NOT claimable/peekable ordinary work.
    pub non_work_side_records: bool,
    /// Recovery/explain reads reconstruct the committed transition (request id, instance fence, consumed
    /// input id, side-record keys, lifecycle ids, per-entry status) from authoritative durable state.
    pub authoritative_recovery_reads: bool,
    /// Delayed/timer lifecycle items (awaits/due timers) are supported as ordinary lifecycle work.
    pub delayed_awaits_timers: bool,
    /// The durability class of the commit boundary (the clear durability boundary Snorri keys off).
    pub durability_class: DurabilityClass,
    /// A short human-readable note on the consistency boundary (e.g. "atomic append+apply under one lock").
    pub consistency: &'static str,
}

impl Default for CommitCapabilities {
    /// The safe all-false default: a backend that has not opted in advertises NO commit guarantees, so Snorri
    /// rejects it before activation. `durability_class` defaults to the weakest (`EventualApply`).
    fn default() -> Self {
        Self {
            atomic_transition_commit: false,
            vectorized_commit: false,
            lease_validation: false,
            retained_commit_idempotency: false,
            non_work_side_records: false,
            authoritative_recovery_reads: false,
            delayed_awaits_timers: false,
            durability_class: DurabilityClass::EventualApply,
            consistency: "no authoritative commit boundary",
        }
    }
}

impl CommitCapabilities {
    /// Whether the backend advertises the atomic commit boundary required by atomic-only ports.
    pub fn is_atomic(&self) -> bool {
        self.atomic_transition_commit && self.durability_class == DurabilityClass::Atomic
    }
}

/// Per-entry commit status surfaced by a recovery/explain read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitEntryStatus {
    /// The entry validated and committed atomically.
    Committed,
    /// The entry was rejected; nothing was mutated for it. Carries the structured rejection.
    Rejected(EngineError),
}

/// One entry's reconstructed transition record (epic pqueue-2201fd37 acceptance #5). Built from the retained
/// commit idempotency record plus current durable state, so committed state/audit side records are provably
/// recoverable after input finalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryRecovery {
    /// The input event id this entry consumed/finalized.
    pub consumed_input_id: ItemId,
    /// Any additional input ids finalized by the same atomic entry.
    pub additional_consumed_input_ids: Vec<ItemId>,
    /// The advanced instance/state fence, if the entry carried one: `(instance_key, fence_after_advance)`.
    pub instance: Option<(Vec<u8>, u64)>,
    /// Always empty (fireweed-bf03cbf5). Formerly the opaque non-work side-record keys this entry wrote;
    /// no longer retained because the keys are a pure function of the caller's own `side_records` for the
    /// entry — echoing them back in the durable retained outcome cost ~948 B/entry (up to 781 KB per
    /// 500-entry batch retention row) for data the caller already has. A caller that needs the keys for a
    /// `request_id` it just committed already has them in its own request; a caller reconstructing them from
    /// `explain_commit` after a restart must derive them from its own record of what it sent, not from this
    /// field.
    pub side_record_keys: Vec<Vec<u8>>,
    /// The server-assigned ids of the entry's dispatchable lifecycle items (empty when it enqueued none).
    pub lifecycle_item_ids: Vec<ItemId>,
    /// The per-entry commit status.
    pub status: CommitEntryStatus,
}

/// The reconstructed record of a vectorized claimed-work commit, addressed by its `request_id`
/// (epic pqueue-2201fd37 acceptance #5). Proves the committed transition is recoverable for retry/replay/audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecovery {
    pub request_id: RequestId,
    pub entries: Vec<EntryRecovery>,
}

/// One page of an ordered, key-prefix-scanned side-record read (bead fireweed-e47e9287). `entries` are
/// `(key, payload)` pairs ordered by key ascending. `next_cursor` is `Some(key)` — the first not-yet-returned
/// matching key, passed back verbatim to resume — iff more entries under `prefix` remain; `None` means the
/// page reached the end of the prefix's key range.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SideRecordPage {
    pub entries: Vec<(Vec<u8>, Bytes)>,
    pub next_cursor: Option<Vec<u8>>,
}

/// Recovery/explain reads for the authoritative commit boundary (epic pqueue-2201fd37 acceptance #5). The
/// default impl returns [`EngineError::Unavailable`](crate::EngineError::Unavailable) so backends without an
/// authoritative commit boundary expose no (misleading) recovery surface.
#[doc(hidden)]
pub trait RecoveryReadPort: Send + Sync {
    /// Reconstruct the committed transition addressed by `request_id` from the retained commit idempotency
    /// record (plus current durable state). `Ok(None)` when no such record is retained (never committed under
    /// that id, or its retention window has elapsed).
    fn explain_commit(
        &self,
        _shard: &QueueKey,
        _request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Read an opaque non-work side record by key (recovery/audit read). `Ok(None)` if unwritten. Side records
    /// are disjoint from work items, so this never reflects claimable work and survives input finalization.
    fn side_record(
        &self,
        _shard: &QueueKey,
        _key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Paged, key-ascending-ordered scan of opaque side records whose key starts with `prefix`
    /// (recovery/audit read, bead fireweed-e47e9287). A pure read: no epoch/fence check, so a concurrent
    /// writer under the same prefix may or may not be visible in the page. `cursor` resumes from a prior
    /// page's `next_cursor` (`None` starts at `prefix` itself). Returns at most `page_size` entries;
    /// `page_size == 0` yields an empty page whose `next_cursor` still reports the first match, if any.
    /// The default returns [`EngineError::Unavailable`] — a backend must opt in.
    fn side_records_by_prefix(
        &self,
        _shard: &QueueKey,
        _prefix: &[u8],
        _page_size: usize,
        _cursor: Option<Vec<u8>>,
    ) -> impl std::future::Future<Output = EngineResult<SideRecordPage>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

#[doc(hidden)]
/// In-place merge of a **live** item's hot-storage `fields`/`payload` — the write half of the
/// `LiveItemView` map (FAC-1, ADR-009). User field names that collide with the API-001 reserved
/// claimed-item / lease shape are rejected before commit, then the normal live-item validation runs:
/// an absent / terminal / superseded id rejects and nothing is appended; an `expected_item_version`
/// mismatch rejects with `EngineError::Conflict` (optimistic concurrency for the rolling-update case).
/// Legal while the item is Pending OR Leased; touches neither lifecycle state nor the lease. Bumps and
/// returns the new `item_version`. Atomic class only; on eventual-apply the engine returns
/// `EngineError::Unavailable`.
pub trait UpdateFieldsPort: Send + Sync {
    /// `expected_epoch`: the owner's cached acquire-time fence epoch — `Some(e)` fences the commit
    /// (superseded owner → `EpochFenced`, nothing appended); `None` is the sole-owner path.
    #[allow(clippy::too_many_arguments)]
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: crate::PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;
}

/// Full API-001 `BatchUpdate` operation.
///
/// The request is idempotent at the envelope level through
/// [`BatchUpdateRequest::request_id`]. Implementations MUST preserve update order in the returned
/// outcomes, apply successful entries independently, and reject leased items with
/// [`BatchUpdateOutcome::Conflict`]. Successful entries replace every field whose disposition is
/// [`BatchUpdateValue::Replace`], leave `Keep` fields unchanged, preserve `eligible_since`, and bump
/// `item_version`. The default deliberately reports `Unavailable`: a backend must implement the
/// operation as a batch rather than inheriting an N-call scalar loop.
pub trait BatchUpdatePort: Send + Sync {
    fn batch_update(
        &self,
        _shard: &QueueKey,
        _request: BatchUpdateRequest,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<BatchUpdateResponse>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Envelope for API-001 `BatchUpdate`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BatchUpdateRequest {
    /// Stable idempotency key for this logical batch. Reuse with a different body is a conflict.
    pub request_id: RequestId,
    /// One or more independent updates. Outcomes are returned in this order.
    pub updates: Vec<BatchUpdateEntry>,
}

/// API-001 response envelope. Echoing `request_id` is part of retry convergence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BatchUpdateResponse {
    pub request_id: RequestId,
    /// Exactly one result per request update, in request order.
    pub results: Vec<BatchUpdateOutcome>,
}

/// One target in an API-001 `BatchUpdate` request.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BatchUpdateItemRef {
    ItemId(ItemId),
    ClientItemKey(ClientItemKey),
    /// When both identifiers are supplied, they MUST resolve to the same live item.
    Both {
        item_id: ItemId,
        client_item_key: ClientItemKey,
    },
}

/// Presence-aware full-replacement disposition for a `BatchUpdate` field.
///
/// `Keep` means the request omitted the field. `Replace(value)` means it was present. Optional
/// values use `Replace(None)` for the contract's explicit JSON `null`/clear operation.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BatchUpdateValue<T> {
    #[default]
    Keep,
    Replace(T),
}

/// One API-001 `BatchUpdate` entry. Every mutable value has full-replacement semantics; this is not
/// a patch API. A pending gate-blocked item remains updateable because gate state affects
/// eligibility, not lifecycle.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BatchUpdateEntry {
    pub item_ref: BatchUpdateItemRef,
    pub expected_item_version: Option<u64>,
    pub priority: BatchUpdateValue<PriorityValue>,
    pub not_before: BatchUpdateValue<Option<UtcTimestamp>>,
    pub payload: BatchUpdateValue<Option<Bytes>>,
    pub metadata: BatchUpdateValue<Metadata>,
    pub gate_keys: BatchUpdateValue<Vec<String>>,
    pub fields: BatchUpdateValue<BTreeMap<String, Bytes>>,
}

/// Ordered per-item API-001 `BatchUpdate` result.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BatchUpdateOutcome {
    Updated {
        item_id: ItemId,
        client_item_key: ClientItemKey,
        item_version: u64,
    },
    /// The target resolved, but this entry violates API-001 shape or queue-policy validation. Invalid
    /// entries do not mutate or prevent valid siblings in the same best-effort batch from committing.
    Invalid,
    Conflict,
    NotFound,
    Terminal,
}

/// Reschedule a **live** item's `priority`/`not_before` after push (BQ pqueue-7a96f929) — the operator/
/// owner-runtime "change when/where this item is delivered" seam, distinct from the [`UpdateFieldsPort`]
/// field/payload merge. Pre-validated exactly like [`UpdateFieldsPort::update_fields`]: an absent / terminal
/// / superseded id rejects and nothing is appended, and an `expected_item_version` mismatch rejects with
/// `EngineError::Conflict`. Legal while the item is Pending OR Leased; a priority change re-keys the item in
/// the eligibility order and a `not_before` change re-gates its eligibility. Bumps and returns the new
/// `item_version`. The default impl returns [`EngineError::Unavailable`] so a backend that has not wired
/// reschedule (the eventual-apply object-log family, the relational family) refuses rather than silently
/// dropping the change.
#[doc(hidden)]
pub trait ReschedulePort: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn reschedule(
        &self,
        _shard: &QueueKey,
        _item_id: ItemId,
        _set_priority: crate::ScheduleUpdate<PriorityValue>,
        _set_not_before: crate::ScheduleUpdate<UtcTimestamp>,
        _expected_item_version: Option<u64>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Reclaims **this queue's** leases that expired strictly before `now` (Leased → Pending), appending one
/// `LeaseExpired` command fenced by `expected_epoch`, and returns the reclaimed ids (FAC-2). Unlike the
/// global background [`ReclaimDriver::tick`], this is per-queue and fenced, so an owner-runtime sweeps
/// only the queue it owns under its own epoch — the host-driven "reclaim before claim" seam. `limit` caps
/// the batch (`None` = all expired). Idempotent: a second call with nothing newly expired returns empty.
#[doc(hidden)]
pub trait ReclaimPort: Send + Sync {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;
}

// ---------------------------------------------------------------------------
// Active-scope discovery (BQ-14e)
// ---------------------------------------------------------------------------

/// Operator discovery of a queue's **active scopes** — the groups that currently hold eligible work,
/// summarized for ranking (`DiscoverActiveScopes`, API-001 / TD-002 §Discovery). A read-only rollup over
/// the per-group summary projection (`fireweed_group_summary`): each group with `oldest_eligible_at` set
/// becomes one source [`ActiveScope`] (age from `now`, eligible count; at-risk is `None` while its
/// derivation is deferred), then [`project_scopes`](crate::project_scopes) collapses to the requested
/// granularity (per-group detail, or a single queue rollup). The returned list is ranked **owner-local,
/// oldest-first** (most-starved scope first; deterministic group-key tiebreak) — the queue has one owner
/// (ADR-008), so this ranking is authoritative for the queue without cross-owner merge.
///
/// LAYERING: this port performs the granularity projection (incl. the per-queue rollup) and the owner-local
/// sort for ITS ONE queue. A tenant-wide adapter therefore CONCATENATES these per-queue results and
/// re-ranks — it must NOT re-run [`project_scopes`](crate::project_scopes) at `Queue` granularity (the rows
/// are already one-per-queue; a second rollup is a no-op but the contract is "roll up once, here"). The
/// adapter still owns wire concerns the port does not: `tenant_id`/`as_of` stamping, `max_results`
/// truncation, and any `queue_id`/`group_key` filtering.
///
/// PAUSE: discovery reports INTRINSIC eligibility and does not short-circuit on a paused queue (it shows
/// pause-induced buildup) — a deliberate divergence from the claim path. Implementations derive exact
/// read-time eligibility either from live items or an equivalently exact relational query, so a pure
/// `not_before` time crossing is visible without a write.
pub trait DiscoveryPort: Send + Sync {
    /// The default impl returns [`EngineError::Unavailable`]; projections that maintain enough item or
    /// summary state override it with an exact rollup.
    fn discover_active_scopes(
        &self,
        _shard: &QueueKey,
        _granularity: crate::active_scope::DiscoveryGranularity,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::active_scope::ActiveScope>>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

// ---------------------------------------------------------------------------
// Hot projection query substrate (API-004)
// ---------------------------------------------------------------------------

/// Hot-projection query operations over a queue's declared indexes (API-004): range scan, grouped/
/// bucketed aggregation, bounded mutation, and claim-by-query. Every default impl returns
/// [`EngineError::Unavailable`] and [`hot_projection_capabilities`](Self::hot_projection_capabilities)
/// defaults to [`QueryCapabilityFlags::default`] (all-false), so a backend that has not implemented
/// this contract explicitly rejects a request rather than silently degrading to a full scan (API-004
/// Query Capability Names). `side_record_query` is deferred beyond epic pqueue-45e13e4d for every
/// backend (API-004 Side/Projection Records) — no override in this epic may advertise it `true`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedMutationContext {
    /// Server-owned operational time used to stamp every committed update.
    pub now: UtcTimestamp,
    /// Cached coordinated-owner fence epoch. `None` is the sole-owner path.
    pub expected_epoch: Option<u64>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimByQueryContext {
    /// Server-owned operational time used to stamp the lease and idempotency retention window.
    pub now: UtcTimestamp,
    /// Optional caller-selected eligibility epoch. This can widen/narrow due selection, but never changes
    /// the operational lease start; public facade calls leave it absent and use the injected clock for both.
    pub eligibility_time: Option<UtcTimestamp>,
    /// Cached coordinated-owner fence epoch. `None` is the sole-owner path.
    pub expected_epoch: Option<u64>,
}

impl ClaimByQueryContext {
    pub fn eligibility_at(self) -> UtcTimestamp {
        self.eligibility_time.unwrap_or(self.now)
    }

    pub fn lease_expires_at(self, lease_duration_ms: u64) -> UtcTimestamp {
        let nanos = u64::from(self.now.nanoseconds)
            .saturating_add((lease_duration_ms % 1_000).saturating_mul(1_000_000));
        let seconds_to_add = lease_duration_ms
            .saturating_div(1_000)
            .saturating_add(nanos / 1_000_000_000);
        let nanoseconds = (nanos % 1_000_000_000) as u32;
        let Some(seconds_to_add) = i64::try_from(seconds_to_add).ok() else {
            return UtcTimestamp {
                seconds: i64::MAX,
                nanoseconds: 999_999_999,
            };
        };
        let Some(seconds) = self.now.seconds.checked_add(seconds_to_add) else {
            return UtcTimestamp {
                seconds: i64::MAX,
                nanoseconds: 999_999_999,
            };
        };
        UtcTimestamp {
            seconds,
            nanoseconds,
        }
    }
}

/// Mint an unguessable server-owned lease capability for query claims.
#[doc(hidden)]
pub fn generate_query_lease_token() -> EngineResult<LeaseToken> {
    use std::fmt::Write as _;

    let mut entropy = [0_u8; 32];
    getrandom::fill(&mut entropy).map_err(|error| {
        EngineError::Storage(format!("lease-token entropy unavailable: {error}"))
    })?;
    let mut encoded = String::with_capacity(4 + entropy.len() * 2);
    encoded.push_str("cbq-");
    for byte in entropy {
        write!(&mut encoded, "{byte:02x}").expect("writing to String is infallible");
    }
    LeaseToken::new(encoded).map_err(|error| EngineError::Storage(error.to_string()))
}

#[doc(hidden)]
pub trait HotProjectionQueryPort: Send + Sync {
    /// Advertised capability flags for `shard`. The default advertises every capability
    /// unavailable.
    fn hot_projection_capabilities(&self, _shard: &QueueKey) -> QueryCapabilityFlags {
        QueryCapabilityFlags::default()
    }

    fn range_scan(
        &self,
        _shard: &QueueKey,
        _request: RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<RangeScanResponse>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn grouped_aggregate(
        &self,
        _shard: &QueueKey,
        _request: GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<GroupedAggregateResponse>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn metrics_by_query(
        &self,
        _shard: &QueueKey,
        _request: MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn declared_bucket_segment(
        &self,
        _shard: &QueueKey,
        _request: DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<Output = EngineResult<DeclaredBucketSegmentResponse>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn bounded_mutation(
        &self,
        _shard: &QueueKey,
        _request: BoundedMutationRequest,
        _context: BoundedMutationContext,
    ) -> impl std::future::Future<Output = EngineResult<BoundedMutationResponse>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Claim due records selected by a declared-index predicate (API-004 Claim By Query). Returns the
    /// same [`Claimed`] shape as [`ClaimPort::claim`] — an alternate *selection* path into the same
    /// claim/lease/finalize lifecycle, not a parallel one.
    fn claim_by_query(
        &self,
        _shard: &QueueKey,
        _request: ClaimByQueryRequest,
        _context: ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// API-001 `BatchClaimByItemIds`: lease exactly the caller-supplied `item_id` set with partial
    /// per-id outcomes. Resulting leases are ordinary claim leases.
    fn claim_by_item_ids(
        &self,
        _shard: &QueueKey,
        _request: fireweed_core::ClaimByItemIdsRequest,
        _context: ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<ClaimByItemIdsResponse>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

// ---------------------------------------------------------------------------
// Clock, IdGen, ReclaimDriver
// ---------------------------------------------------------------------------

/// Injected clock — keeps the engine deterministic/testable.
pub trait Clock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}

/// Injected id generation.
pub trait IdGen: Send + Sync {
    fn next_item_id(&self) -> ItemId;
    fn next_command_id(&self) -> CommandId;
}

/// What a `tick` fired (TD-007 §3). Empty when nothing was due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceStopReason {
    BudgetExhausted,
    EpochFenced,
    OwnershipUnproven,
    FrontierProofMissing,
    RetryableFailure,
    PermanentFailure,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintenanceSummary {
    pub scanned: u64,
    pub retained: u64,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
    pub object_requests: u64,
    pub retryable_failures: u64,
    pub permanent_failures: u64,
    pub fenced: bool,
    pub cursor_pending: bool,
    pub stopped_by: Option<MaintenanceStopReason>,
    pub orphan_branches_reclaimed: u64,
}

impl MaintenanceSummary {
    /// Merge another bounded maintenance operation into this tick's aggregate report.
    pub fn merge(&mut self, other: Self) {
        self.scanned += other.scanned;
        self.retained += other.retained;
        self.objects_deleted += other.objects_deleted;
        self.bytes_deleted += other.bytes_deleted;
        self.object_requests += other.object_requests;
        self.retryable_failures += other.retryable_failures;
        self.permanent_failures += other.permanent_failures;
        self.fenced |= other.fenced;
        self.cursor_pending |= other.cursor_pending;
        self.stopped_by = other.stopped_by.or(self.stopped_by);
        self.orphan_branches_reclaimed += other.orphan_branches_reclaimed;
    }

    /// Whether a bounded deletion pass proved that its entire target was processed.
    pub fn deletion_pass_complete(&self) -> bool {
        !self.cursor_pending
            && self.retryable_failures == 0
            && self.permanent_failures == 0
            && !self.fenced
            && self.stopped_by.is_none()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    pub leases_reclaimed: u64,
    pub cohorts_expired: u64,
    pub items_promoted: u64,
    pub progress_bound_breaches: u64,
    pub maintenance: MaintenanceSummary,
}

impl TickReport {
    pub fn is_empty(&self) -> bool {
        *self == TickReport::default()
    }
}

/// Fires timed lifecycle transitions (lease expiry, cohort timeout, not_before/recurrence
/// promotion, progress-bound metering). The *logic* is domain; the *clock* is the composition
/// root's. `tick(now)` is idempotent (re-running at the same/earlier `now` makes no further
/// transitions) and serializes against claim via the same unit of work (TD-007 §3).
pub trait ReclaimDriver: Send + Sync {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send;
}

// ---------------------------------------------------------------------------
// Control plane: queue definitions + epoch source (plan §2.1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CreateQueueOutcome {
    /// `false` for a compatible idempotent re-create (API-001).
    pub created: bool,
    pub definition: QueueDefinition,
}

/// Stores queue definitions and supplies the `backend_epoch` that `CommandPosition` carries and that
/// lease/gate fencing keys off (TD-003). At launch (single shard) the epoch is shard-local.
pub trait ControlPlaneStore: Send + Sync {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send;

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send;

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send;

    /// The current assignment epoch for `shard` (the `backend_epoch` of new positions).
    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;

    /// Acquire the queue at a NEW, strictly-greater `assignment_epoch` and durably record it (TD-003
    /// Single Authoritative Fencing Rule, step 1: "durable fence before use"). Returns the new epoch. This
    /// is the ownership-handoff primitive: after it commits, the previous epoch's writers are fenced at
    /// their next typed commit (step 2), before any new-epoch segment exists. `assignment_epoch`
    /// MUST increase strictly and MUST NOT decrease or repeat for a queue (TD-003 epoch monotonicity).
    /// NOTE (BQ-21/BQ-23 binding): this is the storage backend's durable epoch. Some control-plane
    /// implementations, notably postgres-native, bind their acquire transaction directly to this value and
    /// make `acquire_epoch` a fallback only for control planes that cannot update the storage fence
    /// atomically. Callers should stamp the acquired owner's cached epoch on every data-plane write.
    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;

    /// Durably fence storage to the exact control-plane target epoch. Shared object-log implementations
    /// override this with one authoritative conditional-head transition. The compatibility default advances
    /// monotonically and fails if a concurrent writer skips beyond the requested target.
    fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            let mut current = self.current_epoch(shard).await?;
            if current > target_epoch {
                return Err(crate::EngineError::EpochFenced);
            }
            while current < target_epoch {
                current = self.acquire_epoch(shard).await?;
                if current > target_epoch {
                    return Err(crate::EngineError::EpochFenced);
                }
            }
            Ok(current)
        }
    }

    /// Hydrate the local serving projection from durable storage after the ownership fence is installed
    /// and before the control plane publishes the new owner as serving. Backends whose serving state is
    /// already authoritative may use this default no-op. Derived, pod-local projections must override it
    /// and replay from their durable high-water so a greater-epoch owner never serves a stale/empty image.
    fn hydrate_projection_for_ownership(
        &self,
        _shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// Snapshot store: replay acceleration + the persisted command_position high-water (TD-007 §4)
// ---------------------------------------------------------------------------

/// Serialized projection snapshot payload (opaque to the engine).
#[derive(Debug, Clone)]
pub struct ProjectionSnapshot {
    pub payload: Vec<u8>,
}

/// A reference to a written snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotRef {
    pub queue: QueueKey,
    pub position: CommandPosition,
    pub ref_id: String,
}

/// Persists projection snapshots and — crucially — the `command_position` **high-water mark**, so
/// replay after retention/compaction is monotonic and `item_version` never regresses (TD-007 §4).
/// The high-water mark is read from here, never recomputed by counting a (possibly compacted) log.
pub trait SnapshotStore: Send + Sync {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send;

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send;

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send;

    /// Find the newest snapshot whose position is `<= position`.
    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let latest = self.latest_snapshot(shard);
        async move {
            let latest = latest.await?;
            Ok(match latest {
                Some(snapshot)
                    if snapshot.position.precedes(position) || snapshot.position == *position =>
                {
                    Some(snapshot)
                }
                _ => None,
            })
        }
    }

    /// The persisted monotonic `command_position` high-water for `shard` (TD-007 §4).
    fn high_water(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send;

    /// Advance the persisted high-water mark. MUST be monotonic (reject a lower position).
    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

/// An in-memory projection store that can reconstruct an ephemeral read view as of a historical
/// position.
///
/// The associated `AsOfProjection` is the ephemeral projection instance used to answer the bounded
/// query. Implementations hydrate from the supplied snapshot if present, then allow the caller to
/// replay the remaining log tail into the returned projection before running the query.
pub trait AsOfProjectionStore: ProjectionStore {
    type AsOfProjection: ProjectionStore + Send;

    /// Whether this projection store can serve historical/as-of reads by log replay.
    ///
    /// Log-replayable projection families (the object-log / in-memory default) reconstruct an
    /// ephemeral view from a snapshot plus the replayed command tail, so they return `true`.
    /// Relational projection stores (`SqliteRelational`, `PostgresRelational`) keep no replayable
    /// command log and cannot reconstruct historical state, so they override this to `false`. The
    /// composed backend consults this up-front and declines as-of reads with `EngineError::Unavailable`
    /// (matching the monolithic relational backends) before performing a queue-existence lookup.
    fn supports_as_of(&self) -> bool {
        true
    }

    fn reconstruct_as_of(
        &self,
        definition: &QueueDefinition,
        snapshot: Option<ProjectionSnapshot>,
    ) -> EngineResult<Self::AsOfProjection>;
}

/// Historical read access at a specific durable command position.
pub trait HistoricalProjectionRead: Send + Sync {
    type AsOfProjection: ProjectionStore + Send;

    fn current_position(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<CommandPosition>> + Send;

    fn read_as_of<T, F>(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        query: F,
    ) -> impl std::future::Future<Output = EngineResult<T>> + Send
    where
        T: Send + 'static,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send + 'static;
}

#[cfg(test)]
mod maintenance_summary_tests {
    use super::{MaintenanceStopReason, MaintenanceSummary};

    #[test]
    fn merge_preserves_bounded_progress_and_failure_state() {
        let mut aggregate = MaintenanceSummary {
            scanned: 2,
            objects_deleted: 1,
            bytes_deleted: 11,
            object_requests: 3,
            ..MaintenanceSummary::default()
        };
        aggregate.merge(MaintenanceSummary {
            scanned: 5,
            retained: 1,
            objects_deleted: 2,
            bytes_deleted: 17,
            object_requests: 4,
            retryable_failures: 1,
            fenced: true,
            cursor_pending: true,
            stopped_by: Some(MaintenanceStopReason::RetryableFailure),
            orphan_branches_reclaimed: 1,
            ..MaintenanceSummary::default()
        });

        assert_eq!(aggregate.scanned, 7);
        assert_eq!(aggregate.retained, 1);
        assert_eq!(aggregate.objects_deleted, 3);
        assert_eq!(aggregate.bytes_deleted, 28);
        assert_eq!(aggregate.object_requests, 7);
        assert_eq!(aggregate.retryable_failures, 1);
        assert!(aggregate.fenced);
        assert!(aggregate.cursor_pending);
        assert_eq!(
            aggregate.stopped_by,
            Some(MaintenanceStopReason::RetryableFailure)
        );
        assert_eq!(aggregate.orphan_branches_reclaimed, 1);
    }

    #[test]
    fn only_a_fully_completed_deletion_pass_allows_a_watermark() {
        assert!(MaintenanceSummary::default().deletion_pass_complete());

        for incomplete in [
            MaintenanceSummary {
                cursor_pending: true,
                ..MaintenanceSummary::default()
            },
            MaintenanceSummary {
                retryable_failures: 1,
                ..MaintenanceSummary::default()
            },
            MaintenanceSummary {
                permanent_failures: 1,
                ..MaintenanceSummary::default()
            },
            MaintenanceSummary {
                fenced: true,
                ..MaintenanceSummary::default()
            },
            MaintenanceSummary {
                stopped_by: Some(MaintenanceStopReason::BudgetExhausted),
                ..MaintenanceSummary::default()
            },
        ] {
            assert!(!incomplete.deletion_pass_complete());
        }
    }
}
