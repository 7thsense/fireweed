#![forbid(unsafe_code)]
//! # fireweed-projection
//!
//! The priority-ordered projection state machine ([`ProjectionData`]) and per-shard command log
//! ([`LogData`]), as pure in-memory types with no I/O. This is the **domain materialized view**: apply
//! rules, the eligibility index, lifecycle transitions, `item_version` bumps, lease/fence fields, and
//! the read queries the ports expose. Driven adapters (memory/sqlite/postgres) own only the
//! *persistence* of these, so every backend shares one correct projection rather than re-implementing
//! the apply/eligibility/lease logic.
//!
//! `LogData` and `ProjectionData` are kept SEPARATE (not bundled) so a backend can hold them in
//! disjoint maps and apply backend-owned typed commits without exposing transaction borrows to callers.
//! two-writer unit of work. The free [`commit`] couples them for the orchestration ports. The owning
//! backend supplies the [`QueueKey`] (to stamp positions) and constructs each [`CommandEnvelope`] (so
//! each backend keeps its own command-id scheme); everything else is here.
//!
//! INVARIANT (TD-007 §1 / commit_locked): [`commit`] appends to the log BEFORE applying to the
//! projection and does NOT roll back. Callers that can reject a command (finalize fencing, upsert
//! collisions) MUST pre-validate via the provided helpers ([`ProjectionData::finalize_validate`],
//! [`ProjectionData::item_state`]) so `apply_command` is infallible for the command they commit.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Included, Unbounded};

mod compose_impls;
pub use compose_impls::{AsyncInMemoryProjection, AsyncMemoryLog, InMemoryProjection, MemoryLog};

use bytes::Bytes;
use fireweed_core::{
    AggregateGroup, BoundedMutationRequest, BoundedMutationResponse, BucketCount, ClientItemKey,
    DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, FilterOp, GateKeyPolicy, GroupKey,
    GroupedAggregateRequest, GroupedAggregateResponse, IndexDeclaration, IndexSpec, IndexType,
    ItemEvent, ItemId, ItemState, LeaseToken, Metadata, MetricsByQueryRequest, MutationOutcome,
    MutationResult, OrderField, OrderingMode, PriorityModel, PriorityValue, QueryCursor,
    QueryFilter, QueueDefinition, QueueIndex, RangeScanRequest, RangeScanResponse, RangeScanRow,
    RecurrenceMode, RecurrencePolicy, SortDirection, TimeBucket, TypedValue, UtcTimestamp,
    apply_transition, failure_event, priority_sort,
};
use fireweed_engine::{
    ActiveScope, BatchUpdateItemRef, BatchUpdateSnapshotItem, BoundedMutationPlan,
    BoundedMutationUpdate, ClaimRef, ClaimedItem, CommandEnvelope, CommandPosition,
    DiscoveryGranularity, EngineError, EngineResult, EntityEditOperation, EntityPredicateValue,
    FinalizeKind, FinalizeOutcome, IndexHit, ItemMutationOperation, ItemMutationOutcome,
    ItemMutationPlan, ItemMutationPrecondition, ItemMutationRequest, ItemMutationResponse,
    ItemMutationResult, ItemMutationReturning, ItemMutationSelectorAggregate, ItemMutationSnapshot,
    ItemMutationSummary, ItemPatch, ItemPredicate, ItemSelectorScope, ItemView, LeaseGuard,
    LeaseView, LifecyclePatch, LiveItemView, MutateItemsCommand, PayloadUpdate, PendingPage,
    PendingSummary, ProjectionSnapshot, PushItem, QueueCommand, QueueCounters, QueueKey,
    QueueMetrics, ResolvedItemMutation, ResolvedItemMutationAction, ResolvedItemValues,
    ScheduleUpdate, SnapshotRef, TerminalEmissionMetrics, UpdateFieldsCommand, project_scopes,
};
use serde_json::Value;

type FastHashMap<K, V> = rustc_hash::FxHashMap<K, V>;

// ---------------------------------------------------------------------------
// Projection record + eligibility key
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ItemRecord {
    item_id: ItemId,
    explicit_client_item_key: Option<ClientItemKey>,
    priority: Option<PriorityValue>,
    not_before: Option<UtcTimestamp>,
    /// When the item most recently became eligible under scheduling semantics. Gate state and `now`
    /// are applied at read time, so blocking/unblocking a gate does not rewrite this timestamp.
    eligible_since: UtcTimestamp,
    group_key: Option<GroupKey>,
    cohort_size: Option<u64>,
    payload: Option<Bytes>,
    fields: BTreeMap<String, Bytes>,
    metadata: Metadata,
    gate_keys: Vec<String>,
    /// Typed JSON entity document (ADR-011). Carries the canonical typed representation through the
    /// projection so schema validation and axon_esf index-key computation can address it.
    /// `None` for schema-less queues that use the opaque `payload` bytes carrier.
    entity_document: Option<serde_json::Value>,
    state: ItemState,
    item_version: u64,
    attempt_count: u32,
    /// Retry bound (B'): a `Finalize{Retry}` once `attempt_count >= max_attempts` drives the item terminal
    /// (Failed) instead of back to pending — see the `Finalize` apply arm.
    max_attempts: u32,
    created_seq: u64,
    lease_token: Option<LeaseToken>,
    lease_expires_at: Option<UtcTimestamp>,
    lease_is_cohort: bool,
    worker_id: Option<fireweed_core::WorkerId>,
    fenced: bool,
    superseded: bool,
    terminal_at: Option<UtcTimestamp>,
    terminal_position: Option<CommandPosition>,
}

/// Portable, typed representation of one item in a [`ProjectionImage`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProjectionImageItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    /// Optional only for backwards compatibility with snapshots written before active-scope discovery
    /// was materialized by the shared projection.
    #[serde(default)]
    pub eligible_since: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    #[serde(default)]
    pub cohort_size: Option<u64>,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
    pub gate_keys: Vec<String>,
    pub entity_document: Option<serde_json::Value>,
    pub state: ItemState,
    pub item_version: u64,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub created_seq: u64,
    pub lease_token: Option<LeaseToken>,
    pub lease_expires_at: Option<UtcTimestamp>,
    /// Whether the active lease was acquired as a whole cohort. Older images
    /// predate cohort leasing and therefore default to an ordinary lease.
    #[serde(default)]
    pub lease_is_cohort: bool,
    #[serde(default)]
    pub worker_id: Option<fireweed_core::WorkerId>,
    pub fenced: bool,
    pub superseded: bool,
    pub terminal_at: Option<UtcTimestamp>,
    pub terminal_position: Option<CommandPosition>,
}

impl From<&ItemRecord> for ProjectionImageItem {
    fn from(rec: &ItemRecord) -> Self {
        Self {
            item_id: rec.item_id,
            client_item_key: rec.client_item_key(),
            priority: rec.priority.clone(),
            not_before: rec.not_before,
            eligible_since: Some(rec.eligible_since),
            group_key: rec.group_key.clone(),
            cohort_size: rec.cohort_size,
            payload: rec.payload.clone(),
            fields: rec.fields.clone(),
            metadata: rec.metadata.clone(),
            gate_keys: rec.gate_keys.clone(),
            entity_document: rec.entity_document.clone(),
            state: rec.state,
            item_version: rec.item_version,
            attempt_count: rec.attempt_count,
            max_attempts: rec.max_attempts,
            created_seq: rec.created_seq,
            lease_token: rec.lease_token.clone(),
            lease_expires_at: rec.lease_expires_at,
            lease_is_cohort: rec.lease_is_cohort,
            worker_id: rec.worker_id.clone(),
            fenced: rec.fenced,
            superseded: rec.superseded,
            terminal_at: rec.terminal_at,
            terminal_position: rec.terminal_position.clone(),
        }
    }
}

impl From<ProjectionImageItem> for ItemRecord {
    fn from(item: ProjectionImageItem) -> Self {
        let explicit_client_item_key = explicit_client_item_key(item.item_id, item.client_item_key);
        Self {
            item_id: item.item_id,
            explicit_client_item_key,
            priority: item.priority,
            not_before: item.not_before,
            eligible_since: item
                .eligible_since
                .or(item.not_before)
                .unwrap_or(UtcTimestamp {
                    seconds: 0,
                    nanoseconds: 0,
                }),
            group_key: item.group_key,
            cohort_size: item.cohort_size,
            payload: item.payload,
            fields: item.fields,
            metadata: item.metadata,
            gate_keys: item.gate_keys,
            entity_document: item.entity_document,
            state: item.state,
            item_version: item.item_version,
            attempt_count: item.attempt_count,
            max_attempts: item.max_attempts,
            created_seq: item.created_seq,
            lease_token: item.lease_token,
            lease_expires_at: item.lease_expires_at,
            lease_is_cohort: item.lease_is_cohort,
            worker_id: item.worker_id,
            fenced: item.fenced,
            superseded: item.superseded,
            terminal_at: item.terminal_at,
            terminal_position: item.terminal_position,
        }
    }
}

/// Complete queue projection image at a durable high-water.
///
/// The item list is the source of truth for lifecycle, ordering, fields, payloads, metadata, gates,
/// entity documents, lease state, secondary indexes, and metrics. Derived maps are rebuilt on import.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProjectionImage {
    pub high_water: Option<CommandPosition>,
    pub paused: bool,
    #[serde(default)]
    pub pause_drain_intake: bool,
    /// Operator-blocked dynamic gate keys. Default keeps older snapshots readable.
    #[serde(default)]
    pub blocked_gates: BTreeSet<String>,
    pub next_seq: u64,
    pub items: Vec<ProjectionImageItem>,
    pub side_records: BTreeMap<Vec<u8>, Bytes>,
    pub instance_fences: BTreeMap<Vec<u8>, u64>,
    pub metrics: QueueMetrics,
}

impl ProjectionImage {
    pub fn to_bytes(&self) -> EngineResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| EngineError::Storage(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> EngineResult<Self> {
        serde_json::from_slice(bytes).map_err(|e| EngineError::Storage(e.to_string()))
    }
}

impl ItemRecord {
    fn client_item_key(&self) -> ClientItemKey {
        self.explicit_client_item_key
            .clone()
            .unwrap_or_else(|| default_client_item_key(self.item_id))
    }

    fn to_claimed(&self) -> Option<ClaimedItem> {
        Some(ClaimedItem {
            item_id: self.item_id,
            client_item_key: self.client_item_key(),
            item_version: self.item_version,
            priority: self.priority.clone(),
            group_key: self.group_key.clone(),
            not_before: self.not_before,
            lease_token: Some(self.lease_token.clone()?),
            lease_expires_at: self.lease_expires_at?,
            attempt_count: self.attempt_count,
            payload: self.payload.clone(),
            fields: self.fields.clone(),
            metadata: self.metadata.clone(),
            gate_keys: self.gate_keys.clone(),
        })
    }

    fn lease_view(&self) -> Option<LeaseView> {
        if self.state != ItemState::Leased || self.superseded {
            return None;
        }
        Some(LeaseView {
            item_id: self.item_id,
            lease_token: self.lease_token.clone()?,
            lease_expires_at: self.lease_expires_at?,
            attempt_count: self.attempt_count,
        })
    }

    fn to_live(&self) -> Option<LiveItemView> {
        if self.superseded || self.state.is_terminal() {
            return None;
        }
        Some(LiveItemView {
            item_id: self.item_id,
            client_item_key: self.client_item_key(),
            item_version: self.item_version,
            lifecycle_state: self.state,
            priority: self.priority.clone(),
            group_key: self.group_key.clone(),
            not_before: self.not_before,
            attempt_count: self.attempt_count,
            payload: self.payload.clone(),
            fields: self.fields.clone(),
        })
    }
}

fn default_client_item_key(item_id: ItemId) -> ClientItemKey {
    ClientItemKey::new(item_id.to_string()).expect("item id is a valid default client key")
}

fn explicit_client_item_key(item_id: ItemId, key: ClientItemKey) -> Option<ClientItemKey> {
    (key.as_str() != item_id.to_string()).then_some(key)
}

fn is_explicit_client_item_key(item_id: ItemId, key: &ClientItemKey) -> bool {
    key.as_str() != item_id.to_string()
}

/// Priority-ordered eligibility key. Ascending order = claim order: priced items first (tag 0, then
/// `priority_sort` bytes), unpriced last (tag 1), FIFO by `created_seq` within ties.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum EligRank {
    Priced(Vec<u8>),
    Unpriced,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EligKey {
    rank: EligRank,
    created_seq: u64,
    item: ItemId,
    not_before: Option<UtcTimestamp>,
    group_key: Option<GroupKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EligToken {
    Compact { created_seq: u64, item: ItemId },
    Rich(EligKey),
}

impl EligToken {
    fn rich(self) -> EligKey {
        match self {
            EligToken::Compact { created_seq, item } => EligKey {
                rank: EligRank::Unpriced,
                created_seq,
                item,
                not_before: None,
                group_key: None,
            },
            EligToken::Rich(key) => key,
        }
    }
}

#[derive(Clone)]
enum EligibilityIndex {
    Compact(BTreeSet<(u64, ItemId)>),
    Rich(BTreeSet<EligKey>),
}

impl EligibilityIndex {
    fn new() -> Self {
        Self::Compact(BTreeSet::new())
    }

    fn can_compact(rec: &ItemRecord) -> bool {
        rec.priority.is_none() && rec.not_before.is_none() && rec.group_key.is_none()
    }

    fn token(rec: &ItemRecord, model: &PriorityModel) -> EligToken {
        if Self::can_compact(rec) {
            EligToken::Compact {
                created_seq: rec.created_seq,
                item: rec.item_id,
            }
        } else {
            EligToken::Rich(elig_key(rec, model))
        }
    }

    fn promote(&mut self, items: &FastHashMap<ItemId, ItemRecord>, model: &PriorityModel) {
        let Self::Compact(compact) = self else {
            return;
        };
        let mut rich = BTreeSet::new();
        for (_, item) in compact.iter() {
            if let Some(rec) = items.get(item) {
                rich.insert(elig_key(rec, model));
            }
        }
        *self = Self::Rich(rich);
    }

    fn insert(
        &mut self,
        rec: &ItemRecord,
        items: &FastHashMap<ItemId, ItemRecord>,
        model: &PriorityModel,
    ) {
        match self {
            Self::Compact(compact) if Self::can_compact(rec) => {
                compact.insert((rec.created_seq, rec.item_id));
            }
            Self::Compact(_) => {
                self.promote(items, model);
                if let Self::Rich(rich) = self {
                    rich.insert(elig_key(rec, model));
                }
            }
            Self::Rich(rich) => {
                rich.insert(elig_key(rec, model));
            }
        }
    }

    fn remove(&mut self, token: EligToken) {
        match self {
            Self::Compact(compact) => match token {
                EligToken::Compact { created_seq, item } => {
                    compact.remove(&(created_seq, item));
                }
                EligToken::Rich(key) => {
                    compact.remove(&(key.created_seq, key.item));
                }
            },
            Self::Rich(rich) => {
                rich.remove(&token.rich());
            }
        }
    }

    fn strict_candidates(&self, now: UtcTimestamp, max: usize) -> Vec<ItemId> {
        match self {
            Self::Compact(compact) => compact.iter().take(max).map(|(_, item)| *item).collect(),
            Self::Rich(rich) => rich
                .iter()
                .filter(|k| due_at(k, now))
                .take(max)
                .map(|k| k.item)
                .collect(),
        }
    }

    fn strict_candidates_after(
        &self,
        now: UtcTimestamp,
        after: &ItemRecord,
        model: &PriorityModel,
        max: usize,
    ) -> Vec<ItemId> {
        use std::ops::Bound::{Excluded, Unbounded};
        match self {
            Self::Compact(compact) if Self::can_compact(after) => compact
                .range((Excluded(&(after.created_seq, after.item_id)), Unbounded))
                .take(max)
                .map(|(_, item)| *item)
                .collect(),
            Self::Compact(_) => self.strict_candidates(now, max),
            Self::Rich(rich) => {
                let after_key = elig_key(after, model);
                rich.range((Excluded(&after_key), Unbounded))
                    .filter(|k| due_at(k, now))
                    .take(max)
                    .map(|k| k.item)
                    .collect()
            }
        }
    }

    fn relaxed_candidates(&self, now: UtcTimestamp, max: usize, bound: u32) -> Vec<ItemId> {
        match self {
            Self::Compact(compact) => compact.iter().take(max).map(|(_, item)| *item).collect(),
            Self::Rich(rich) => {
                let mut selected: Vec<&EligKey> =
                    rich.iter().filter(|k| due_at(k, now)).take(max).collect();
                let block = bound as usize + 1;
                for chunk in selected.chunks_mut(block) {
                    chunk.sort_by(|a, b| locality_key(a).cmp(&locality_key(b)));
                }
                selected.into_iter().map(|k| k.item).collect()
            }
        }
    }

    fn ordered_items(&self, limit: usize) -> Vec<ItemId> {
        match self {
            Self::Compact(compact) => compact.iter().take(limit).map(|(_, item)| *item).collect(),
            Self::Rich(rich) => rich.iter().take(limit).map(|key| key.item).collect(),
        }
    }

    fn ordered_items_after(
        &self,
        after: Option<&ItemRecord>,
        model: &PriorityModel,
        limit: usize,
    ) -> Vec<ItemId> {
        match self {
            Self::Compact(compact) => match after {
                Some(record) => compact
                    .range((Excluded(&(record.created_seq, record.item_id)), Unbounded))
                    .take(limit)
                    .map(|(_, item)| *item)
                    .collect(),
                None => compact.iter().take(limit).map(|(_, item)| *item).collect(),
            },
            Self::Rich(rich) => match after {
                Some(record) => rich
                    .range((Excluded(&elig_key(record, model)), Unbounded))
                    .take(limit)
                    .map(|key| key.item)
                    .collect(),
                None => rich.iter().take(limit).map(|key| key.item).collect(),
            },
        }
    }
}

fn elig_key(rec: &ItemRecord, model: &PriorityModel) -> EligKey {
    let rank = match &rec.priority {
        Some(p) => EligRank::Priced(priority_sort(p, model)),
        None => EligRank::Unpriced,
    };
    EligKey {
        rank,
        created_seq: rec.created_seq,
        item: rec.item_id,
        not_before: rec.not_before,
        group_key: rec.group_key.clone(),
    }
}

/// Bounded-relaxed locality key: items sharing a `group_key` cluster together; ungrouped items (None) sort
/// last so grouped work batches ahead within a rank window. Total + `Ord` so selection is deterministic.
fn locality_key(key: &EligKey) -> (bool, Option<&GroupKey>) {
    (key.group_key.is_none(), key.group_key.as_ref())
}

fn due_at(key: &EligKey, now: UtcTimestamp) -> bool {
    key.not_before.map(|nb| nb <= now).unwrap_or(true)
}

fn gate_keys_blocked(blocked_gates: &BTreeSet<String>, gate_keys: &[String]) -> bool {
    gate_keys.iter().any(|key| blocked_gates.contains(key))
}

fn timestamp_to_ms(ts: UtcTimestamp) -> i128 {
    ts.seconds as i128 * 1_000 + (ts.nanoseconds as i128 / 1_000_000)
}

fn add_millis(ts: UtcTimestamp, ms: u64) -> UtcTimestamp {
    let total_ms = timestamp_to_ms(ts) + ms as i128;
    let seconds = total_ms.div_euclid(1_000) as i64;
    let nanoseconds = (total_ms.rem_euclid(1_000) as u32) * 1_000_000;
    UtcTimestamp::new(seconds, nanoseconds).expect("valid timestamp arithmetic")
}

// ---------------------------------------------------------------------------
// Secondary indexes: per-queue, name-keyed maps over configured item fields and typed entity indexes.
// ---------------------------------------------------------------------------

/// One per-queue secondary index. Unique maps a composite key to exactly one item; non-unique maps a
/// key to the (id-ordered) set of items that carry it.
#[derive(Clone)]
enum SecondaryIndex {
    Unique(BTreeMap<Vec<u8>, ItemId>),
    NonUnique(BTreeMap<Vec<u8>, BTreeSet<ItemId>>),
}

enum IndexLookupSpec<'a> {
    Legacy(&'a IndexSpec),
    Typed(&'a QueueIndex),
}

impl<'a> IndexLookupSpec<'a> {
    fn unique(&self) -> bool {
        match self {
            Self::Legacy(spec) => spec.unique,
            Self::Typed(spec) => match &spec.declaration {
                IndexDeclaration::Single(def) => def.unique,
                IndexDeclaration::Compound(def) => def.unique,
            },
        }
    }

    fn lookup_key(&self, key_values: &[Vec<u8>]) -> EngineResult<Vec<u8>> {
        match self {
            Self::Legacy(_) => {
                let slices: Vec<&[u8]> = key_values.iter().map(|v| v.as_slice()).collect();
                Ok(legacy_raw_key(&slices))
            }
            Self::Typed(spec) => match &spec.declaration {
                IndexDeclaration::Single(def) => {
                    let value = decode_typed_lookup_value(&def.index_type, &key_values[0])?;
                    let mut record = serde_json::Map::new();
                    record.insert(def.field.clone(), value);
                    let key: Result<Option<Vec<u8>>, _> = def.index_key(&Value::Object(record));
                    key.map_err(|err| EngineError::Storage(err.to_string()))?
                        .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
                }
                IndexDeclaration::Compound(def) => {
                    let mut record = serde_json::Map::new();
                    for (field, value_bytes) in def.fields.iter().zip(key_values.iter()) {
                        let value = decode_typed_lookup_value(&field.index_type, value_bytes)?;
                        record.insert(field.field.clone(), value);
                    }
                    let key: Result<Option<Vec<u8>>, _> = def.index_key(&Value::Object(record));
                    key.map_err(|err| EngineError::Storage(err.to_string()))?
                        .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
                }
            },
        }
    }
}

/// Length-prefix raw byte encoding for legacy index keys. Each field is prefixed with its 4-byte
/// big-endian length so concatenated multi-field keys round-trip losslessly regardless of byte
/// content (no UTF-8 assumption, no JSON quoting).
fn legacy_raw_key(field_bytes: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for bytes in field_bytes {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

fn legacy_index_key(
    spec: &IndexSpec,
    fields: &BTreeMap<String, Bytes>,
) -> EngineResult<Option<Vec<u8>>> {
    let mut field_bytes: Vec<&[u8]> = Vec::new();
    for field_name in &spec.fields {
        match fields.get(field_name) {
            Some(v) => field_bytes.push(v.as_ref()),
            None => return Ok(None),
        }
    }
    Ok(Some(legacy_raw_key(&field_bytes)))
}

/// Decode a raw lookup byte slice into a JSON `Value` appropriate for `index_type`. String fields
/// are treated as strict UTF-8 (not JSON-parsed), so `b"123"` stays `"123"` rather than the number
/// 123. Datetime fields accept either RFC 3339 UTF-8 or JSON numeric epoch-nanos because axon-esf
/// treats both as valid representations of the same instant.
fn decode_typed_lookup_value(index_type: &IndexType, bytes: &[u8]) -> EngineResult<Value> {
    match index_type {
        IndexType::String => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(Value::String(s.to_owned()))
        }
        IndexType::Datetime => {
            if let Ok(value @ Value::Number(_)) = serde_json::from_slice::<Value>(bytes) {
                return Ok(value);
            }
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(Value::String(s.to_owned()))
        }
        IndexType::Integer | IndexType::Float => serde_json::from_slice::<Value>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON number")),
        IndexType::Boolean => serde_json::from_slice::<Value>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON boolean")),
    }
}

fn typed_index_key(spec: &QueueIndex, entity: Option<&Value>) -> EngineResult<Option<Vec<u8>>> {
    let Some(entity) = entity else {
        return Ok(None);
    };
    match &spec.declaration {
        IndexDeclaration::Single(def) => {
            let key: Result<Option<Vec<u8>>, _> = def.index_key(entity);
            key.map_err(|err| EngineError::Storage(err.to_string()))
        }
        IndexDeclaration::Compound(def) => {
            let key: Result<Option<Vec<u8>>, _> = def.index_key(entity);
            key.map_err(|err| EngineError::Storage(err.to_string()))
        }
    }
}

fn typed_index_key_err(spec: &QueueIndex, entity: Option<&Value>) -> EngineResult<Option<Vec<u8>>> {
    typed_index_key(spec, entity)
}

/// Every `(index_name, composite_key)` this record currently belongs to. A free function over `specs`
/// so callers can compute keys while holding other shared borrows of `self`.
fn legacy_index_keys(
    specs: &[IndexSpec],
    fields: &BTreeMap<String, Bytes>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    for spec in specs {
        if let Some(key) = legacy_index_key(spec, fields)? {
            out.push((spec.name.clone(), key));
        }
    }
    Ok(out)
}

fn typed_index_keys(
    specs: &[QueueIndex],
    entity: Option<&Value>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    for spec in specs {
        if let Some(key) = typed_index_key_err(spec, entity)? {
            out.push((spec.name.clone(), key));
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct RangeScanCursorState {
    index: String,
    filters: Vec<QueryFilter>,
    order_by: Vec<OrderField>,
    anchor_item_id: ItemId,
    anchor_values: Vec<TypedValue>,
    #[serde(default)]
    anchor_index_key: Option<Vec<u8>>,
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn parse_utc_timestamp(value: &str) -> EngineResult<UtcTimestamp> {
    let Some(value) = value
        .strip_suffix('Z')
        .or_else(|| value.strip_suffix("+00:00"))
    else {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    };
    let (date, time) = value.split_once('T').ok_or(EngineError::Invalid(
        "typed index value is not a valid datetime",
    ))?;

    let mut date_parts = date.split('-');
    let year: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let month: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let day: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    if date_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    }

    let mut time_parts = time.split(':');
    let hour: i64 = time_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let minute: i64 =
        time_parts
            .next()
            .and_then(|v| v.parse().ok())
            .ok_or(EngineError::Invalid(
                "typed index value is not a valid datetime",
            ))?;
    let sec_part = time_parts.next().ok_or(EngineError::Invalid(
        "typed index value is not a valid datetime",
    ))?;
    if time_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    }

    let (second, nanos) = match sec_part.split_once('.') {
        Some((whole, frac)) => {
            let second: i64 = whole
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?;
            if frac.is_empty() || frac.len() > 9 || !frac.chars().all(|c| c.is_ascii_digit()) {
                return Err(EngineError::Invalid(
                    "typed index value is not a valid datetime",
                ));
            }
            let mut digits = frac.to_string();
            while digits.len() < 9 {
                digits.push('0');
            }
            let nanos: u32 = digits
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?;
            (second, nanos)
        }
        None => (
            sec_part
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?,
            0,
        ),
    };

    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    UtcTimestamp::new(seconds, nanos)
        .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))
}

fn typed_value_for_field(
    entity: &Value,
    field: &str,
    index_type: &IndexType,
) -> EngineResult<Option<TypedValue>> {
    let Value::Object(map) = entity else {
        return Err(EngineError::Invalid("typed index entity is not an object"));
    };
    let Some(value) = map.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let typed = match index_type {
        IndexType::String => value
            .as_str()
            .map(|s| TypedValue::String(s.to_string()))
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Integer => {
            value
                .as_i64()
                .map(TypedValue::Integer)
                .ok_or(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ))?
        }
        IndexType::Float => value
            .as_f64()
            .map(TypedValue::Float)
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Boolean => value
            .as_bool()
            .map(TypedValue::Bool)
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Datetime => match value {
            Value::String(s) => TypedValue::DateTime(parse_utc_timestamp(s)?),
            Value::Number(n) => {
                let seconds = n.as_i64().ok_or({
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?;
                TypedValue::DateTime(UtcTimestamp::new(seconds, 0).map_err(|_| {
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?)
            }
            _ => {
                return Err(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ));
            }
        },
    };
    Ok(Some(typed))
}

fn typed_value_from_filter_value(
    value: &TypedValue,
    index_type: &IndexType,
) -> EngineResult<TypedValue> {
    match (value, index_type) {
        (TypedValue::String(v), IndexType::String) => Ok(TypedValue::String(v.clone())),
        (TypedValue::Integer(v), IndexType::Integer) => Ok(TypedValue::Integer(*v)),
        (TypedValue::Float(v), IndexType::Float) => Ok(TypedValue::Float(*v)),
        (TypedValue::Bool(v), IndexType::Boolean) => Ok(TypedValue::Bool(*v)),
        (TypedValue::DateTime(v), IndexType::Datetime) => Ok(TypedValue::DateTime(*v)),
        _ => Err(EngineError::Invalid(
            "typed index value is not valid for declared type",
        )),
    }
}

fn typed_value_matches_query(value: &TypedValue, filter: &TypedValue) -> bool {
    match (value, filter) {
        (TypedValue::String(a), TypedValue::String(b)) => a == b,
        (TypedValue::Integer(a), TypedValue::Integer(b)) => a == b,
        (TypedValue::Float(a), TypedValue::Float(b)) => a == b,
        (TypedValue::Bool(a), TypedValue::Bool(b)) => a == b,
        (TypedValue::DateTime(a), TypedValue::DateTime(b)) => a == b,
        _ => false,
    }
}

fn typed_value_compare(a: &TypedValue, b: &TypedValue) -> EngineResult<Ordering> {
    match (a, b) {
        (TypedValue::String(a), TypedValue::String(b)) => Ok(a.cmp(b)),
        (TypedValue::Integer(a), TypedValue::Integer(b)) => Ok(a.cmp(b)),
        (TypedValue::Float(a), TypedValue::Float(b)) => a.partial_cmp(b).ok_or(
            EngineError::Invalid("typed index value comparison is undefined"),
        ),
        (TypedValue::Bool(a), TypedValue::Bool(b)) => Ok(a.cmp(b)),
        (TypedValue::DateTime(a), TypedValue::DateTime(b)) => Ok(a.cmp(b)),
        _ => Err(EngineError::Invalid(
            "typed index value is not valid for declared type",
        )),
    }
}

fn compare_rows(
    lhs: &RangeScanRow,
    rhs: &RangeScanRow,
    order_by: &[OrderField],
) -> EngineResult<Ordering> {
    for field in order_by {
        let left = lhs
            .fields
            .get(&field.field)
            .ok_or(EngineError::Invalid("unindexed-field"))?;
        let right = rhs
            .fields
            .get(&field.field)
            .ok_or(EngineError::Invalid("unindexed-field"))?;
        let ord = typed_value_compare(left, right)?;
        let ord = match field.direction {
            SortDirection::Ascending => ord,
            SortDirection::Descending => ord.reverse(),
        };
        if !ord.is_eq() {
            return Ok(ord);
        }
    }
    Ok(lhs.item_id.cmp(&rhs.item_id))
}

fn index_fields(spec: &QueueIndex) -> Vec<(&str, &IndexType)> {
    match &spec.declaration {
        IndexDeclaration::Single(def) => vec![(def.field.as_str(), &def.index_type)],
        IndexDeclaration::Compound(def) => def
            .fields
            .iter()
            .map(|field| (field.field.as_str(), &field.index_type))
            .collect(),
    }
}

fn index_field_type<'a>(spec: &'a QueueIndex, field: &str) -> Option<&'a IndexType> {
    index_fields(spec)
        .into_iter()
        .find(|(name, _)| *name == field)
        .map(|(_, ty)| ty)
}

fn truncate_timestamp(value: UtcTimestamp, bucket: TimeBucket) -> UtcTimestamp {
    let seconds = match bucket {
        TimeBucket::Hour => (value.seconds.div_euclid(3_600)) * 3_600,
        TimeBucket::Day => (value.seconds.div_euclid(86_400)) * 86_400,
    };
    UtcTimestamp {
        seconds,
        nanoseconds: 0,
    }
}

fn value_matches_bucket(value: &TypedValue, rule: &fireweed_core::BucketRule) -> bool {
    let numeric = match value {
        TypedValue::Integer(v) => *v as f64,
        TypedValue::Float(v) => *v,
        _ => return false,
    };
    if let Some(exact) = rule.exact {
        return numeric == exact;
    }
    if let Some(gt) = rule.gt
        && numeric <= gt
    {
        return false;
    }
    if let Some(gte) = rule.gte
        && numeric < gte
    {
        return false;
    }
    if let Some(lt) = rule.lt
        && numeric >= lt
    {
        return false;
    }
    if let Some(lte) = rule.lte
        && numeric > lte
    {
        return false;
    }
    true
}

fn entity_index_value(
    entity: &Value,
    field: &str,
    index_type: &IndexType,
) -> EngineResult<Option<TypedValue>> {
    typed_value_for_field(entity, field, index_type)
}

fn matches_filter_on_entity(entity: &Value, filter: &QueryFilter) -> EngineResult<bool> {
    let Value::Object(map) = entity else {
        return Err(EngineError::Invalid("typed index entity is not an object"));
    };
    let Some(value) = map.get(&filter.field) else {
        return Ok(false);
    };
    if value.is_null() {
        return Ok(false);
    }
    let typed = match &filter.value {
        TypedValue::String(_) => value
            .as_str()
            .map(|s| TypedValue::String(s.to_string()))
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        TypedValue::Integer(_) => {
            value
                .as_i64()
                .map(TypedValue::Integer)
                .ok_or(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ))?
        }
        TypedValue::Float(_) => {
            value
                .as_f64()
                .map(TypedValue::Float)
                .ok_or(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ))?
        }
        TypedValue::Bool(_) => {
            value
                .as_bool()
                .map(TypedValue::Bool)
                .ok_or(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ))?
        }
        TypedValue::DateTime(_) => match value {
            Value::String(s) => TypedValue::DateTime(parse_utc_timestamp(s)?),
            Value::Number(n) => {
                let seconds = n.as_i64().ok_or({
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?;
                TypedValue::DateTime(UtcTimestamp::new(seconds, 0).map_err(|_| {
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?)
            }
            _ => {
                return Err(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ));
            }
        },
    };
    let ord = typed_value_compare(&typed, &filter.value)?;
    let ok = match filter.op {
        FilterOp::Eq => ord.is_eq(),
        FilterOp::Gte => ord.is_ge(),
        FilterOp::Gt => ord.is_gt(),
        FilterOp::Lte => ord.is_le(),
        FilterOp::Lt => ord.is_lt(),
    };
    Ok(ok)
}

fn matches_filters_on_entity(entity: &Value, filters: &[QueryFilter]) -> EngineResult<bool> {
    for filter in filters {
        if !matches_filter_on_entity(entity, filter)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Count lifecycle states for records matching filters on one declared typed index.
///
/// This is the backend-neutral evaluation seam used by projection implementations whose authoritative
/// rows live outside [`ProjectionData`] (for example, a relational projection). Callers provide only the
/// lifecycle state, supersession flag, and public entity document; backend-private row types never escape.
pub fn filtered_lifecycle_metrics<'a>(
    typed_indexes: &[QueueIndex],
    request: &MetricsByQueryRequest,
    records: impl IntoIterator<Item = (ItemState, bool, Option<&'a Value>)>,
) -> EngineResult<QueueMetrics> {
    let spec = match request.index.as_deref() {
        Some(name) => typed_indexes
            .iter()
            .find(|spec| spec.name == name)
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
        None => typed_indexes
            .first()
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
    };
    let fields = index_fields(spec);
    for filter in &request.filters {
        let Some((_, index_type)) = fields
            .iter()
            .find(|(field, _)| *field == filter.field.as_str())
        else {
            return Err(EngineError::Invalid("unindexed-field"));
        };
        typed_value_from_filter_value(&filter.value, index_type)?;
    }

    let mut metrics = QueueMetrics::default();
    for (state, superseded, entity) in records {
        if superseded {
            continue;
        }
        let Some(entity) = entity else {
            continue;
        };
        if !matches_filters_on_entity(entity, &request.filters)? {
            continue;
        }
        match state {
            ItemState::Pending => metrics.pending += 1,
            ItemState::Leased => metrics.leased += 1,
            ItemState::Complete => metrics.complete += 1,
            ItemState::Failed => metrics.failed += 1,
        }
    }
    metrics.resident_terminal_count = metrics.complete + metrics.failed;
    Ok(metrics)
}

/// Count lifecycle states from canonical Axon index keys held by a relational projection.
///
/// Canonical compound keys frame every declared field independently, so this evaluator can apply typed
/// equality and range predicates without reconstructing a backend's private item row or persisting a
/// duplicate entity document.
pub fn filtered_lifecycle_metrics_by_index_key<'a>(
    typed_indexes: &[QueueIndex],
    request: &MetricsByQueryRequest,
    records: impl IntoIterator<Item = (ItemState, bool, &'a [u8])>,
) -> EngineResult<QueueMetrics> {
    let spec = match request.index.as_deref() {
        Some(name) => typed_indexes
            .iter()
            .find(|spec| spec.name == name)
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
        None => typed_indexes
            .first()
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
    };
    let fields = index_fields(spec);
    let mut encoded_filters = Vec::with_capacity(request.filters.len());
    for filter in &request.filters {
        let Some((position, (_, index_type))) = fields
            .iter()
            .enumerate()
            .find(|(_, (field, _))| *field == filter.field.as_str())
        else {
            return Err(EngineError::Invalid("unindexed-field"));
        };
        typed_value_from_filter_value(&filter.value, index_type)?;
        let value = typed_value_to_json(&filter.value)?;
        let encoded = axon_esf::encode_index_value(&value, index_type).map_err(|_| {
            EngineError::Invalid("typed index value is not valid for declared type")
        })?;
        encoded_filters.push((position, filter.op, encoded));
    }

    let mut metrics = QueueMetrics::default();
    for (state, superseded, key) in records {
        if superseded {
            continue;
        }
        let mut components = Vec::with_capacity(fields.len());
        let mut offset = 0usize;
        for _ in &fields {
            let length_bytes: [u8; 4] = key
                .get(offset..offset + 4)
                .ok_or_else(|| EngineError::Storage("invalid canonical index key".into()))?
                .try_into()
                .expect("four-byte length slice");
            offset += 4;
            let length = u32::from_be_bytes(length_bytes) as usize;
            let value = key
                .get(offset..offset + length)
                .ok_or_else(|| EngineError::Storage("invalid canonical index key".into()))?;
            offset += length;
            components.push(value);
        }
        if offset != key.len() {
            return Err(EngineError::Storage("invalid canonical index key".into()));
        }
        let matches = encoded_filters.iter().all(|(position, op, filter)| {
            let ordering = components[*position].cmp(filter.as_slice());
            match op {
                FilterOp::Eq => ordering.is_eq(),
                FilterOp::Gte => ordering.is_ge(),
                FilterOp::Gt => ordering.is_gt(),
                FilterOp::Lte => ordering.is_le(),
                FilterOp::Lt => ordering.is_lt(),
            }
        });
        if !matches {
            continue;
        }
        match state {
            ItemState::Pending => metrics.pending += 1,
            ItemState::Leased => metrics.leased += 1,
            ItemState::Complete => metrics.complete += 1,
            ItemState::Failed => metrics.failed += 1,
        }
    }
    metrics.resident_terminal_count = metrics.complete + metrics.failed;
    Ok(metrics)
}

/// Reconstruct the smallest projection image needed to evaluate API-004 read queries from the canonical
/// typed keys persisted by a relational adapter.
///
/// Relational projections deliberately persist the canonical index representation rather than a second
/// copy of the caller's entity document. This adapter keeps the query semantics in one place: backends load
/// `(item_id, index_key)` rows, this function decodes only the selected declaration, and callers invoke the
/// ordinary [`ProjectionData`] range/group/bucket evaluators. A missing key represents a sparse/null row.
pub fn query_projection_from_index_keys(
    definition: &QueueDefinition,
    index_name: Option<&str>,
    records: impl IntoIterator<Item = (ItemId, Option<Vec<u8>>)>,
) -> EngineResult<ProjectionData> {
    let spec = match index_name {
        Some(name) => definition
            .typed_indexes
            .iter()
            .find(|spec| spec.name == name)
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
        None => definition
            .typed_indexes
            .first()
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
    };
    let fields = index_fields(spec);

    fn decode_component(bytes: &[u8], index_type: &IndexType) -> EngineResult<TypedValue> {
        let storage = || EngineError::Storage("invalid canonical index key".into());
        Ok(match index_type {
            IndexType::String => TypedValue::String(
                std::str::from_utf8(bytes)
                    .map_err(|_| storage())?
                    .to_owned(),
            ),
            IndexType::Integer => {
                let encoded = u64::from_be_bytes(bytes.try_into().map_err(|_| storage())?);
                TypedValue::Integer((encoded ^ (1_u64 << 63)) as i64)
            }
            IndexType::Float => {
                let encoded = u64::from_be_bytes(bytes.try_into().map_err(|_| storage())?);
                let bits = if encoded & (1_u64 << 63) != 0 {
                    encoded & !(1_u64 << 63)
                } else {
                    !encoded
                };
                TypedValue::Float(f64::from_bits(bits))
            }
            IndexType::Boolean => match bytes {
                [0] => TypedValue::Bool(false),
                [1] => TypedValue::Bool(true),
                _ => return Err(storage()),
            },
            IndexType::Datetime => {
                let encoded = u64::from_be_bytes(bytes.try_into().map_err(|_| storage())?);
                let nanos = (encoded ^ (1_u64 << 63)) as i64;
                TypedValue::DateTime(UtcTimestamp {
                    seconds: nanos.div_euclid(1_000_000_000),
                    nanoseconds: nanos.rem_euclid(1_000_000_000) as u32,
                })
            }
        })
    }

    fn insert_dotted(root: &mut Value, path: &str, value: Value) -> EngineResult<()> {
        // ProjectionData's query evaluator addresses declared names directly while Axon's index-key
        // builder resolves dotted paths. Retain both views in this transient document.
        root.as_object_mut()
            .ok_or_else(|| EngineError::Storage("typed index entity is not an object".into()))?
            .insert(path.to_owned(), value.clone());
        let mut cursor = root;
        let mut parts = path.split('.').peekable();
        while let Some(part) = parts.next() {
            let object = cursor
                .as_object_mut()
                .ok_or_else(|| EngineError::Storage("overlapping typed index paths".into()))?;
            if parts.peek().is_none() {
                object.insert(part.to_owned(), value);
                return Ok(());
            }
            cursor = object
                .entry(part)
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
        }
        Err(EngineError::Storage("empty typed index path".into()))
    }

    let mut items = Vec::new();
    for (created_seq, (item_id, key)) in records.into_iter().enumerate() {
        let mut entity = Value::Object(serde_json::Map::new());
        if let Some(key) = key {
            let mut offset = 0usize;
            for (field, index_type) in &fields {
                let length_bytes: [u8; 4] = key
                    .get(offset..offset + 4)
                    .ok_or_else(|| EngineError::Storage("invalid canonical index key".into()))?
                    .try_into()
                    .expect("four-byte length slice");
                offset += 4;
                let length = u32::from_be_bytes(length_bytes) as usize;
                let component = key
                    .get(offset..offset + length)
                    .ok_or_else(|| EngineError::Storage("invalid canonical index key".into()))?;
                offset += length;
                let typed = decode_component(component, index_type)?;
                insert_dotted(&mut entity, field, typed_value_to_json(&typed)?)?;
            }
            if offset != key.len() {
                return Err(EngineError::Storage("invalid canonical index key".into()));
            }
        }
        items.push(ProjectionImageItem {
            item_id,
            client_item_key: ClientItemKey::new(format!("query-{item_id}"))
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            priority: None,
            not_before: None,
            eligible_since: Some(UtcTimestamp {
                seconds: 0,
                nanoseconds: 0,
            }),
            group_key: None,
            cohort_size: None,
            payload: None,
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            gate_keys: Vec::new(),
            entity_document: Some(entity),
            state: ItemState::Pending,
            item_version: 1,
            attempt_count: 0,
            max_attempts: 1,
            created_seq: created_seq as u64,
            lease_token: None,
            lease_expires_at: None,
            lease_is_cohort: false,
            worker_id: None,
            fenced: false,
            superseded: false,
            terminal_at: None,
            terminal_position: None,
        });
    }
    let mut query_definition = definition.clone();
    query_definition.typed_indexes = vec![spec.clone()];
    ProjectionData::from_image(
        &query_definition,
        ProjectionImage {
            high_water: None,
            paused: false,
            pause_drain_intake: false,
            blocked_gates: BTreeSet::new(),
            next_seq: items.len() as u64,
            items,
            side_records: BTreeMap::new(),
            instance_fences: BTreeMap::new(),
            metrics: QueueMetrics::default(),
        },
    )
}

fn typed_value_to_json(value: &TypedValue) -> EngineResult<Value> {
    Ok(match value {
        TypedValue::String(v) => Value::String(v.clone()),
        TypedValue::Integer(v) => Value::Number((*v).into()),
        TypedValue::Float(v) => {
            Value::Number(serde_json::Number::from_f64(*v).ok_or({
                EngineError::Invalid("typed index value is not valid for declared type")
            })?)
        }
        TypedValue::Bool(v) => Value::Bool(*v),
        TypedValue::DateTime(v) => Value::Number(v.seconds.into()),
    })
}

fn typed_value_to_field_bytes(value: &TypedValue) -> EngineResult<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|e| EngineError::Storage(e.to_string()))
}

fn merge_entity_document(
    entity: Option<&Value>,
    set_fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<Value> {
    let mut object = match entity {
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return Err(EngineError::Invalid("typed index entity is not an object"));
        }
        None => serde_json::Map::new(),
    };
    for (field, value) in set_fields {
        object.insert(field.clone(), typed_value_to_json(value)?);
    }
    Ok(Value::Object(object))
}

fn merge_field_bytes(
    fields: &BTreeMap<String, Bytes>,
    set_fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<BTreeMap<String, Bytes>> {
    let mut merged = fields.clone();
    for (field, value) in set_fields {
        merged.insert(field.clone(), typed_value_to_field_bytes(value)?);
    }
    Ok(merged)
}

// ---------------------------------------------------------------------------
// LogData: the per-shard command log + persisted high-water + snapshots
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct LogData {
    epoch: u64,
    /// Each entry is stored with the `assignment_epoch` it was appended under (BQ-20), so a position
    /// replayed across an epoch boundary carries its true epoch — not a relabel to the current one.
    entries: Vec<(u64, CommandEnvelope)>,
    /// Persisted command_position high-water — a stored field, NOT recomputed from `entries.len()`,
    /// so it survives log retention/compaction and `item_version` never regresses (TD-007 §4).
    high_water: Option<CommandPosition>,
    /// Sequence number of `entries[0]` when the in-process log was seeded past a durable projection
    /// high-water (ADR-013 Class B reopen). Normal process-local logs keep this at `0` so
    /// `sequence == entries` index. After Class B seed, new appends continue at `entry_base_seq +
    /// entries.len()` so the projection's `next_seq` cursor is not regressed or gapped.
    entry_base_seq: u64,
    snapshots: Vec<(SnapshotRef, ProjectionSnapshot)>,
}

impl LogData {
    /// Typed commit append — append `commands` to this shard's log under `expected_epoch`, advancing the
    /// persisted high-water, returning the committed positions in order. TD-003 fencing rule: an
    /// `expected_epoch` that is not the log's current epoch is rejected with [`EngineError::EpochFenced`]
    /// (a stale owner), appending nothing.
    pub fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if expected_epoch != self.epoch {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for cmd in commands {
            let seq = self
                .entry_base_seq
                .saturating_add(self.entries.len() as u64);
            self.entries.push((self.epoch, cmd.clone()));
            let pos = CommandPosition::new(shard.clone(), self.epoch, seq);
            self.high_water = Some(pos.clone());
            positions.push(pos);
        }
        Ok(positions)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Advance to a new, strictly-greater `assignment_epoch` (TD-003 acquire / "durable fence before
    /// use"). Returns the new epoch. The seq counter is continuous across epochs (a new epoch fences who
    /// may extend the log; it never rewinds it — TD-003 Recovery), so positions stay monotonic by
    /// `(epoch, seq)`.
    pub fn advance_epoch(&mut self) -> u64 {
        self.epoch += 1;
        self.epoch
    }

    /// `LogRead::read_from` — a page of committed commands for replay/rebuild.
    pub fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> fireweed_engine::CommandPage {
        // Map absolute sequence → index into the in-process entry buffer (Class B may have
        // `entry_base_seq > 0` with an empty or short buffer after projection-only reopen).
        let start = match &from {
            Some(p) => p
                .sequence
                .saturating_add(1)
                .saturating_sub(self.entry_base_seq) as usize,
            None => 0,
        };
        let mut entries = Vec::new();
        for (i, (entry_epoch, cmd)) in self.entries.iter().enumerate().skip(start).take(limit) {
            let seq = self.entry_base_seq.saturating_add(i as u64);
            entries.push((
                CommandPosition::new(shard.clone(), *entry_epoch, seq),
                cmd.clone(),
            ));
        }
        let next = (start + entries.len() < self.entries.len()).then(|| {
            let idx = start + entries.len();
            let (next_epoch, _) = &self.entries[idx];
            CommandPosition::new(
                shard.clone(),
                *next_epoch,
                self.entry_base_seq.saturating_add(idx as u64),
            )
        });
        fireweed_engine::CommandPage { entries, next }
    }

    pub fn high_water(&self) -> Option<CommandPosition> {
        self.high_water.clone()
    }

    /// Set the persisted high-water, rejecting a regression (TD-007 §4 monotonicity).
    ///
    /// When the in-process entry buffer is empty (Class B reopen over a durable projection), also
    /// advances [`Self::entry_base_seq`] so the next append continues past the projection's absorbed
    /// prefix instead of restarting at sequence 0 (which would be skipped/gapped by the projection).
    pub fn set_high_water(&mut self, position: CommandPosition) -> EngineResult<()> {
        if let Some(cur) = &self.high_water
            && !cur.precedes(&position)
            && cur != &position
        {
            return Err(EngineError::Invalid("high-water regression"));
        }
        if self.entries.is_empty() {
            self.entry_base_seq = position.sequence.saturating_add(1);
            // Keep the live process's epoch at least the projection's recorded epoch so a later
            // append is not fenced by a higher assignment_epoch already stored on the projection.
            if position.backend_epoch > self.epoch {
                self.epoch = position.backend_epoch;
            }
        }
        self.high_water = Some(position);
        Ok(())
    }

    pub fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> SnapshotRef {
        let snap_ref = SnapshotRef {
            queue: shard.clone(),
            position,
            ref_id: format!("snap-{}", self.snapshots.len()),
        };
        self.snapshots.push((snap_ref.clone(), snapshot));
        snap_ref
    }

    pub fn latest_snapshot(&self) -> Option<SnapshotRef> {
        self.snapshots.last().map(|(r, _)| r.clone())
    }

    pub fn snapshot_at_or_before(&self, position: &CommandPosition) -> Option<SnapshotRef> {
        self.snapshots
            .iter()
            .rev()
            .find(|(r, _)| r.position.precedes(position) || r.position == *position)
            .map(|(r, _)| r.clone())
    }

    pub fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        self.snapshots
            .iter()
            .find(|(r, _)| r.ref_id == snapshot_ref.ref_id)
            .map(|(_, s)| s.clone())
            .ok_or(EngineError::NotFound)
    }
}

/// Atomic append + apply (TD-007 §1): append `env` to `log`, then apply it to `proj`. The caller MUST
/// have pre-validated rejectable commands (module INVARIANT) so the apply is infallible. `log` and
/// `proj` are passed separately so a backend can hold them in disjoint maps for the two-writer UoW.
pub fn commit(
    log: &mut LogData,
    proj: &mut ProjectionData,
    shard: &QueueKey,
    env: CommandEnvelope,
    expected_epoch: Option<u64>,
) -> EngineResult<()> {
    // The append is stamped with the queue's current epoch. An owner that supplies its cached acquire-time
    // epoch (`Some`) is fenced here if it has been superseded (ADR-009 / TD-003); `None` is the degenerate
    // sole-owner path (stamp current, never fence).
    let epoch = log.epoch();
    if expected_epoch.is_some_and(|e| e != epoch) {
        return Err(EngineError::EpochFenced);
    }
    log.append(shard, std::slice::from_ref(&env), epoch)?;
    proj.apply_command(&env.command)
}

// ---------------------------------------------------------------------------
// ProjectionData: items + eligibility index + pause flag
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ProjectionData {
    items: FastHashMap<ItemId, ItemRecord>,
    /// Ordered PEL indexes. These keep cursor/range reads proportional to the
    /// requested page instead of scanning every resident item or lease.
    leased_ids: BTreeSet<ItemId>,
    leased_by_consumer: FastHashMap<LeaseToken, BTreeSet<ItemId>>,
    /// Ordinary (non-cohort) leases keyed first by their expiry. The nested id
    /// set makes a same-expiry continuation exact without walking unrelated
    /// resident items.
    ordinary_leases_by_expiry: BTreeMap<UtcTimestamp, BTreeSet<ItemId>>,
    by_key: FastHashMap<ClientItemKey, ItemId>,
    eligible: EligibilityIndex,
    metrics: QueueMetrics,
    next_seq: u64,
    priority_model: PriorityModel,
    /// Queue ordering discipline (ADR / TP-003). `Strict` selects in exact priority order; `BoundedRelaxed`
    /// permits the claim path to reorder within `max_rank_error` rank positions for locality/throughput.
    ordering_mode: OrderingMode,
    /// Effective rank-error bound for `BoundedRelaxed` selection (positions). `0` (and `Strict`) =>
    /// strict-equivalent selection. See [`ProjectionData::eligible_candidates`].
    max_rank_error: u32,
    /// The queue's recurrence policy (BQ pqueue-8cbae731). Read by the `Finalize{Rearm}` apply arm to
    /// enforce `RecurrencePolicy.until`: a rearm whose next occurrence (`not_before`) falls past `until`
    /// ends the series (the item goes terminal) instead of re-arming. Defaults to `Oneshot`/no-`until`.
    recurrence: RecurrencePolicy,
    paused: bool,
    pause_drain_intake: bool,
    /// Dynamic operator gate state. A pending item is indexed as eligible only
    /// when none of its `gate_keys` are present here.
    blocked_gates: BTreeSet<String>,
    gate_key_policy: GateKeyPolicy,
    max_gate_keys_per_item: Option<u64>,
    max_gates_per_request: Option<u64>,
    /// Reverse membership index keeps a gate flip proportional to items using
    /// the named gates rather than total resident queue cardinality.
    gate_members: FastHashMap<String, BTreeSet<ItemId>>,
    /// Per-queue secondary indexes, keyed by declaration name. Built once from the queue's specs and
    /// maintained in the same `apply_command` arms that maintain `eligible`.
    indexes: BTreeMap<String, SecondaryIndex>,
    /// Legacy secondary-index declarations (field lists), needed to recompute keys from a record's fields.
    index_specs: Vec<IndexSpec>,
    /// Typed secondary-index declarations, keyed by `QueueIndex.name`.
    typed_index_specs: Vec<QueueIndex>,
    /// Opaque non-work side records (Snorri authoritative-commit boundary, epic pqueue-2201fd37). Wholly
    /// SEPARATE from `items`/`eligible`/`by_key`: these are NOT claimable work — they never enter the
    /// eligibility index, do not appear in claim/peek/metrics-as-work, and survive input finalization. Both
    /// key and payload are opaque bytes fireweed never interprets.
    side_records: BTreeMap<Vec<u8>, Bytes>,
    /// Per-queue caller-supplied instance/state fences (Snorri authoritative-commit boundary, epic
    /// pqueue-2201fd37). `instance_key -> fence`; an absent key reads as `0` (unset). Wholly SEPARATE from the
    /// work-item projection — never claimable/peekable. Advanced atomically by `AdvanceInstanceFence`.
    instance_fences: BTreeMap<Vec<u8>, u64>,
}

impl ProjectionData {
    pub fn new(
        priority_model: PriorityModel,
        ordering_mode: OrderingMode,
        max_rank_error: u32,
        recurrence: RecurrencePolicy,
        specs: &[IndexSpec],
    ) -> Self {
        let mut indexes = BTreeMap::new();
        for spec in specs {
            let index = if spec.unique {
                SecondaryIndex::Unique(BTreeMap::new())
            } else {
                SecondaryIndex::NonUnique(BTreeMap::new())
            };
            indexes.insert(spec.name.clone(), index);
        }
        Self {
            items: FastHashMap::default(),
            leased_ids: BTreeSet::new(),
            leased_by_consumer: FastHashMap::default(),
            ordinary_leases_by_expiry: BTreeMap::new(),
            by_key: FastHashMap::default(),
            eligible: EligibilityIndex::new(),
            metrics: QueueMetrics::default(),
            next_seq: 0,
            priority_model,
            ordering_mode,
            max_rank_error,
            recurrence,
            paused: false,
            pause_drain_intake: false,
            blocked_gates: BTreeSet::new(),
            gate_key_policy: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: None,
            max_gates_per_request: None,
            gate_members: FastHashMap::default(),
            indexes,
            index_specs: specs.to_vec(),
            typed_index_specs: Vec::new(),
            side_records: BTreeMap::new(),
            instance_fences: BTreeMap::new(),
        }
    }

    /// Attach typed indexes to the projection. Intended for tests and typed queue projections.
    pub fn with_typed_indexes(mut self, specs: &[QueueIndex]) -> Self {
        for spec in specs {
            let index = if match &spec.declaration {
                IndexDeclaration::Single(def) => def.unique,
                IndexDeclaration::Compound(def) => def.unique,
            } {
                SecondaryIndex::Unique(BTreeMap::new())
            } else {
                SecondaryIndex::NonUnique(BTreeMap::new())
            };
            self.indexes.insert(spec.name.clone(), index);
        }
        self.typed_index_specs = specs.to_vec();
        self
    }

    pub fn with_eligibility_policy(mut self, policy: &fireweed_core::EligibilityPolicy) -> Self {
        self.gate_key_policy = policy.gate_keys;
        self.max_gate_keys_per_item = policy.max_gate_keys_per_item;
        self.max_gates_per_request = policy.max_gates_per_request;
        self
    }

    /// Export the complete materialized queue state. `high_water` is supplied by the durable projection
    /// owner because `ProjectionData` itself is log-position agnostic.
    pub fn to_image(&self, high_water: Option<CommandPosition>) -> ProjectionImage {
        let mut items: Vec<ProjectionImageItem> =
            self.items.values().map(ProjectionImageItem::from).collect();
        items.sort_by_key(|item| (item.created_seq, item.item_id));
        ProjectionImage {
            high_water,
            paused: self.paused,
            pause_drain_intake: self.pause_drain_intake,
            blocked_gates: self.blocked_gates.clone(),
            next_seq: self.next_seq,
            items,
            side_records: self.side_records.clone(),
            instance_fences: self.instance_fences.clone(),
            metrics: self.metrics(),
        }
    }

    /// Rebuild a projection from a portable image, reconstructing all derived lookup, eligibility, and
    /// secondary-index state from the item records.
    pub fn from_image(definition: &QueueDefinition, image: ProjectionImage) -> EngineResult<Self> {
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        )
        .with_typed_indexes(&definition.typed_indexes)
        .with_eligibility_policy(&definition.eligibility_policy);
        projection.paused = image.paused;
        projection.pause_drain_intake = image.pause_drain_intake;
        projection.blocked_gates = image.blocked_gates;
        projection.next_seq = image.next_seq;
        projection.side_records = image.side_records;
        projection.instance_fences = image.instance_fences;

        for item in image.items {
            let rec = ItemRecord::from(item);
            for gate_key in &rec.gate_keys {
                projection
                    .gate_members
                    .entry(gate_key.clone())
                    .or_default()
                    .insert(rec.item_id);
            }
            if !rec.superseded {
                if let Some(key) = rec.explicit_client_item_key.clone() {
                    projection.by_key.insert(key, rec.item_id);
                }
                let keys =
                    projection.record_index_keys(&rec.fields, rec.entity_document.as_ref())?;
                projection.index_insert_keys(rec.item_id, &keys);
            }
            if rec.state == ItemState::Pending
                && !rec.superseded
                && !gate_keys_blocked(&projection.blocked_gates, &rec.gate_keys)
            {
                projection
                    .eligible
                    .insert(&rec, &projection.items, &projection.priority_model);
            }
            if rec.state == ItemState::Leased
                && !rec.superseded
                && let Some(token) = rec.lease_token.clone()
            {
                projection.leased_ids.insert(rec.item_id);
                projection
                    .leased_by_consumer
                    .entry(token)
                    .or_default()
                    .insert(rec.item_id);
            }
            if rec.state == ItemState::Leased
                && !rec.superseded
                && !rec.lease_is_cohort
                && let Some(expires) = rec.lease_expires_at
            {
                projection
                    .ordinary_leases_by_expiry
                    .entry(expires)
                    .or_default()
                    .insert(rec.item_id);
            }
            if !rec.superseded {
                projection.metrics_inc(rec.state);
            }
            projection.next_seq = projection.next_seq.max(rec.created_seq.saturating_add(1));
            projection.items.insert(rec.item_id, rec);
        }

        Ok(projection)
    }

    /// Add `(item_id, keys)` to every covering index (Unique: set/replace the holder; NonUnique: add to
    /// the key's id set). Keys are precomputed by the caller so this can run after other borrows release.
    fn index_insert_keys(&mut self, item_id: ItemId, keys: &[(String, Vec<u8>)]) {
        for (name, key) in keys {
            match self.indexes.get_mut(name) {
                Some(SecondaryIndex::Unique(map)) => {
                    map.insert(key.clone(), item_id);
                }
                Some(SecondaryIndex::NonUnique(map)) => {
                    map.entry(key.clone()).or_default().insert(item_id);
                }
                None => {}
            }
        }
    }

    /// Remove `item_id` from every covering index for `keys` (Unique: drop the entry only if it still
    /// maps to this id; NonUnique: drop the id from the set, dropping the set when it empties).
    fn index_remove_keys(&mut self, item_id: ItemId, keys: &[(String, Vec<u8>)]) {
        for (name, key) in keys {
            match self.indexes.get_mut(name) {
                Some(SecondaryIndex::Unique(map)) => {
                    if map.get(key) == Some(&item_id) {
                        map.remove(key);
                    }
                }
                Some(SecondaryIndex::NonUnique(map)) => {
                    if let Some(set) = map.get_mut(key) {
                        set.remove(&item_id);
                        if set.is_empty() {
                            map.remove(key);
                        }
                    }
                }
                None => {}
            }
        }
    }

    fn replace_gate_memberships(&mut self, item_id: ItemId, old: &[String], new: &[String]) {
        for key in old.iter().filter(|key| !new.contains(key)) {
            if let Some(items) = self.gate_members.get_mut(key) {
                items.remove(&item_id);
                if items.is_empty() {
                    self.gate_members.remove(key);
                }
            }
        }
        for key in new.iter().filter(|key| !old.contains(key)) {
            self.gate_members
                .entry(key.clone())
                .or_default()
                .insert(item_id);
        }
    }

    fn record_index_keys(
        &self,
        fields: &BTreeMap<String, Bytes>,
        entity: Option<&Value>,
    ) -> EngineResult<Vec<(String, Vec<u8>)>> {
        let mut keys = legacy_index_keys(&self.index_specs, fields)?;
        keys.extend(typed_index_keys(&self.typed_index_specs, entity)?);
        Ok(keys)
    }

    fn insert_pending(
        &mut self,
        item: PushItem,
        command_at: Option<UtcTimestamp>,
    ) -> EngineResult<()> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let command_at = command_at.unwrap_or(UtcTimestamp {
            seconds: 0,
            nanoseconds: 0,
        });
        // Relational parity: a deferred push ages from its scheduled time; an immediate push ages from
        // command creation. A past `not_before` remains the authoritative eligible-since timestamp.
        let eligible_since = item.not_before.unwrap_or(command_at);
        let rec = ItemRecord {
            item_id: item.item_id,
            explicit_client_item_key: explicit_client_item_key(
                item.item_id,
                item.client_item_key.clone(),
            ),
            priority: item.priority,
            not_before: item.not_before,
            eligible_since,
            group_key: item.group_key,
            cohort_size: item.cohort_size,
            payload: item.payload,
            fields: item.fields,
            metadata: item.metadata,
            gate_keys: item.gate_keys,
            entity_document: item.entity_document,
            state: ItemState::Pending,
            item_version: 1,
            attempt_count: 0,
            max_attempts: item.max_attempts,
            created_seq: seq,
            lease_token: None,
            lease_expires_at: None,
            lease_is_cohort: false,
            worker_id: None,
            fenced: false,
            superseded: false,
            terminal_at: None,
            terminal_position: None,
        };
        // If this item_id is already materialised (corrupt re-mint after recovery without
        // counter reseed, or a double-apply), drop any prior eligibility row before insert so
        // the index cannot hold two keys for one id (fireweed-6e38e2b4).
        if let Some(old) = self.items.get(&rec.item_id)
            && old.state == ItemState::Pending
            && !old.superseded
            && !gate_keys_blocked(&self.blocked_gates, &old.gate_keys)
        {
            self.eligible
                .remove(EligibilityIndex::token(old, &self.priority_model));
        }
        if !gate_keys_blocked(&self.blocked_gates, &rec.gate_keys) {
            self.eligible
                .insert(&rec, &self.items, &self.priority_model);
        }
        if let Some(key) = rec.explicit_client_item_key.clone() {
            self.by_key.insert(key, rec.item_id);
        }
        for gate_key in &rec.gate_keys {
            self.gate_members
                .entry(gate_key.clone())
                .or_default()
                .insert(rec.item_id);
        }
        let keys = self.record_index_keys(&rec.fields, rec.entity_document.as_ref())?;
        self.index_insert_keys(rec.item_id, &keys);
        self.items.insert(rec.item_id, rec);
        self.metrics.pending += 1;
        Ok(())
    }

    fn metrics_inc(&mut self, state: ItemState) {
        match state {
            ItemState::Pending => self.metrics.pending += 1,
            ItemState::Leased => self.metrics.leased += 1,
            ItemState::Complete => {
                self.metrics.complete += 1;
                self.metrics.resident_terminal_count += 1;
            }
            ItemState::Failed => {
                self.metrics.failed += 1;
                self.metrics.resident_terminal_count += 1;
            }
        }
    }

    fn metrics_dec(&mut self, state: ItemState) {
        match state {
            ItemState::Pending => self.metrics.pending = self.metrics.pending.saturating_sub(1),
            ItemState::Leased => self.metrics.leased = self.metrics.leased.saturating_sub(1),
            ItemState::Complete => {
                self.metrics.complete = self.metrics.complete.saturating_sub(1);
                self.metrics.resident_terminal_count =
                    self.metrics.resident_terminal_count.saturating_sub(1);
            }
            ItemState::Failed => {
                self.metrics.failed = self.metrics.failed.saturating_sub(1);
                self.metrics.resident_terminal_count =
                    self.metrics.resident_terminal_count.saturating_sub(1);
            }
        }
    }

    fn metrics_transition(&mut self, old: ItemState, new: ItemState) {
        if old != new {
            self.metrics_dec(old);
            self.metrics_inc(new);
        }
    }

    /// Drive the lifecycle state machine for one item, keeping the eligibility index in sync and
    /// bumping `item_version` (API-001: version bumps on every committed mutation).
    fn transition(
        &mut self,
        id: &ItemId,
        ev: ItemEvent,
        terminal_at: Option<UtcTimestamp>,
        terminal_position: Option<&CommandPosition>,
    ) -> EngineResult<ItemState> {
        let model = self.priority_model;
        let (old_key, new_key, old_state, new_state, old_token, old_expiry) = {
            let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
            // A superseded id (replaced by upsert) must never re-enter eligible or mutate
            // (TD-007 §2.3): the orchestration ports map this to `-ERR fireweed superseded`.
            if rec.superseded {
                return Err(EngineError::Superseded);
            }
            let old_state = rec.state;
            let old = (old_state == ItemState::Pending
                && !gate_keys_blocked(&self.blocked_gates, &rec.gate_keys))
            .then(|| EligibilityIndex::token(rec, &model));
            let new = apply_transition(old_state, ev)
                .map_err(|_| EngineError::Invalid("illegal lifecycle transition"))?;
            rec.state = new;
            rec.item_version += 1;
            if new != ItemState::Leased {
                rec.worker_id = None;
            }
            if new.is_terminal() {
                rec.terminal_at = terminal_at;
                rec.terminal_position = terminal_position.cloned();
            } else if old_state.is_terminal() {
                rec.terminal_at = None;
                rec.terminal_position = None;
            }
            let nk = (new == ItemState::Pending
                && !gate_keys_blocked(&self.blocked_gates, &rec.gate_keys))
            .then(|| EligibilityIndex::token(rec, &model));
            (
                old,
                nk,
                old_state,
                new,
                rec.lease_token.clone(),
                rec.lease_expires_at,
            )
        };
        if old_state == ItemState::Leased && new_state != ItemState::Leased {
            self.leased_ids.remove(id);
            if let Some(expires) = old_expiry {
                self.remove_ordinary_lease(expires, id);
            }
            if let Some(token) = old_token
                && let Some(ids) = self.leased_by_consumer.get_mut(&token)
            {
                ids.remove(id);
                if ids.is_empty() {
                    self.leased_by_consumer.remove(&token);
                }
            }
        }
        if let Some(k) = old_key {
            self.eligible.remove(k);
        }
        if new_key.is_some() {
            let rec = self.items.get(id).ok_or(EngineError::NotFound)?;
            self.eligible.insert(rec, &self.items, &self.priority_model);
        }
        self.metrics_transition(old_state, new_state);
        Ok(new_state)
    }

    fn apply_gate_change(&mut self, gate_keys: &[String], blocked: bool) -> EngineResult<()> {
        let affected = gate_keys
            .iter()
            .filter_map(|key| self.gate_members.get(key))
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        let old = affected
            .into_iter()
            .filter_map(|item_id| {
                self.items.get(&item_id).and_then(|record| {
                    (record.state == ItemState::Pending && !record.superseded).then(|| {
                        (
                            item_id,
                            gate_keys_blocked(&self.blocked_gates, &record.gate_keys),
                        )
                    })
                })
            })
            .collect::<Vec<_>>();
        for key in gate_keys {
            if blocked {
                self.blocked_gates.insert(key.clone());
            } else {
                self.blocked_gates.remove(key);
            }
        }
        for (item_id, was_blocked) in old {
            let record = self.items.get(&item_id).ok_or(EngineError::NotFound)?;
            let is_blocked = gate_keys_blocked(&self.blocked_gates, &record.gate_keys);
            match (was_blocked, is_blocked) {
                (false, true) => {
                    self.eligible
                        .remove(EligibilityIndex::token(record, &self.priority_model));
                }
                (true, false) => {
                    self.eligible
                        .insert(record, &self.items, &self.priority_model);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn apply_command(&mut self, cmd: &QueueCommand) -> EngineResult<()> {
        self.apply_command_at(None, None, cmd)
    }

    fn apply_command_at(
        &mut self,
        terminal_at: Option<UtcTimestamp>,
        terminal_position: Option<&CommandPosition>,
        cmd: &QueueCommand,
    ) -> EngineResult<()> {
        match cmd {
            // Queue creation is handled by the control plane; idempotent no-op if replayed here.
            QueueCommand::CreateQueue(_) => Ok(()),
            QueueCommand::Push(c) => {
                self.items.reserve(c.items.len());
                self.by_key.reserve(
                    c.items
                        .iter()
                        .filter(|item| {
                            is_explicit_client_item_key(item.item_id, &item.client_item_key)
                        })
                        .count(),
                );
                for it in &c.items {
                    self.insert_pending(it.clone(), terminal_at)?;
                }
                Ok(())
            }
            QueueCommand::Claim(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::Claim, None, None)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.lease_is_cohort = false;
                    rec.worker_id = c.worker_id.clone();
                    rec.attempt_count += 1; // delivery count (flavor-diff 7)
                    self.leased_ids.insert(*id);
                    self.leased_by_consumer
                        .entry(c.lease_token.clone())
                        .or_default()
                        .insert(*id);
                    self.ordinary_leases_by_expiry
                        .entry(c.lease_expires_at)
                        .or_default()
                        .insert(*id);
                }
                Ok(())
            }
            QueueCommand::CohortClaim(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::Claim, None, None)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.lease_is_cohort = true;
                    rec.attempt_count += 1;
                    self.leased_ids.insert(*id);
                    self.leased_by_consumer
                        .entry(c.lease_token.clone())
                        .or_default()
                        .insert(*id);
                }
                Ok(())
            }
            QueueCommand::RenewLease(c) => {
                for id in &c.item_ids {
                    let old_expiry = self
                        .items
                        .get(id)
                        .ok_or(EngineError::NotFound)?
                        .lease_expires_at;
                    if let Some(expires) = old_expiry {
                        self.remove_ordinary_lease(expires, id);
                    }
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    // Unlike the `transition()`-routed arms, renew bare-mutates the deadline, so it
                    // relies entirely on every caller pre-validating via `renew_validate`. Assert the
                    // pre-condition so a divergent replay is LOUD in debug/test rather than silently
                    // extending a non-leased lease (apply stays infallible in release).
                    debug_assert!(
                        rec.state == ItemState::Leased
                            && !rec.fenced
                            && !rec.superseded
                            && !rec.state.is_terminal(),
                        "RenewLease applied to a non-renewable item; renew_validate was bypassed"
                    );
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.item_version += 1;
                    self.ordinary_leases_by_expiry
                        .entry(c.lease_expires_at)
                        .or_default()
                        .insert(*id);
                }
                Ok(())
            }
            QueueCommand::CohortRenewLease(_) => Ok(()),
            QueueCommand::ReassignLease(c) => {
                for id in &c.item_ids {
                    let old_expiry = self
                        .items
                        .get(id)
                        .ok_or(EngineError::NotFound)?
                        .lease_expires_at;
                    if let Some(expires) = old_expiry {
                        self.remove_ordinary_lease(expires, id);
                    }
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    // Like RenewLease, this bare-mutates an already-Leased item, so it relies on the
                    // caller pre-validating via `reassign_validate`. Assert the pre-condition so a
                    // divergent replay is LOUD (apply stays infallible in release).
                    debug_assert!(
                        rec.state == ItemState::Leased
                            && !rec.fenced
                            && !rec.superseded
                            && !rec.state.is_terminal(),
                        "ReassignLease applied to a non-renewable item; reassign_validate was bypassed"
                    );
                    let old_token = rec.lease_token.replace(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.attempt_count += 1; // a re-delivery to a new consumer is a delivery (TD-006:129)
                    rec.item_version += 1;
                    self.ordinary_leases_by_expiry
                        .entry(c.lease_expires_at)
                        .or_default()
                        .insert(*id);
                    if let Some(old_token) = old_token
                        && let Some(ids) = self.leased_by_consumer.get_mut(&old_token)
                    {
                        ids.remove(id);
                        if ids.is_empty() {
                            self.leased_by_consumer.remove(&old_token);
                        }
                    }
                    self.leased_by_consumer
                        .entry(c.lease_token.clone())
                        .or_default()
                        .insert(*id);
                }
                Ok(())
            }
            QueueCommand::UpdateFields(c) => {
                let model = self.priority_model;
                let (old_keys, old_elig, new_keys, new_elig, old_gate_keys, new_gate_keys) = {
                    let rec = self.items.get(&c.item_id).ok_or(EngineError::NotFound)?;
                    // A field/payload merge and/or a priority/not_before reschedule (no lifecycle change),
                    // so it relies on `update_fields_validate` having run pre-commit.
                    debug_assert!(
                        !rec.state.is_terminal() && !rec.superseded && !rec.fenced,
                        "UpdateFields applied to a non-updatable item; update_fields_validate was bypassed"
                    );
                    let old_keys =
                        self.record_index_keys(&rec.fields, rec.entity_document.as_ref())?;
                    let repricing = matches!(c.set_priority, ScheduleUpdate::Set(_))
                        || matches!(c.set_not_before, ScheduleUpdate::Set(_));
                    let eligibility_changed = repricing || c.set_gate_keys.is_some();
                    let was_pending = rec.state == ItemState::Pending;
                    let old_elig = (eligibility_changed
                        && was_pending
                        && !gate_keys_blocked(&self.blocked_gates, &rec.gate_keys))
                    .then(|| EligibilityIndex::token(rec, &model));

                    let mut next_fields =
                        c.set_fields.clone().unwrap_or_else(|| rec.fields.clone());
                    for (k, op) in &c.field_ops {
                        match op {
                            Some(v) => {
                                next_fields.insert(k.clone(), v.clone());
                            }
                            None => {
                                next_fields.remove(k);
                            }
                        }
                    }
                    let next_entity = c
                        .set_entity_document
                        .as_ref()
                        .or(rec.entity_document.as_ref());
                    let new_keys = self.record_index_keys(&next_fields, next_entity)?;

                    let mut next_rec = rec.clone();
                    next_rec.fields = next_fields;
                    if c.set_entity_document.is_some() {
                        next_rec.entity_document = c.set_entity_document.clone();
                    }
                    if let PayloadUpdate::Set(p) = &c.payload {
                        next_rec.payload = p.clone();
                    }
                    if let Some(gate_keys) = &c.set_gate_keys {
                        next_rec.gate_keys = gate_keys.clone();
                    }
                    if let ScheduleUpdate::Set(p) = &c.set_priority {
                        next_rec.priority = p.clone();
                    }
                    if let ScheduleUpdate::Set(nb) = &c.set_not_before {
                        next_rec.not_before = *nb;
                    }
                    next_rec.item_version += 1;
                    let new_elig = (eligibility_changed
                        && was_pending
                        && !gate_keys_blocked(&self.blocked_gates, &next_rec.gate_keys))
                    .then(|| EligibilityIndex::token(&next_rec, &model));
                    (
                        old_keys,
                        old_elig,
                        new_keys,
                        new_elig,
                        rec.gate_keys.clone(),
                        next_rec.gate_keys,
                    )
                };
                let rec = self
                    .items
                    .get_mut(&c.item_id)
                    .ok_or(EngineError::NotFound)?;
                if let Some(fields) = &c.set_fields {
                    rec.fields = fields.clone();
                }
                for (k, op) in &c.field_ops {
                    match op {
                        Some(v) => {
                            rec.fields.insert(k.clone(), v.clone());
                        }
                        None => {
                            rec.fields.remove(k);
                        }
                    }
                }
                match &c.payload {
                    PayloadUpdate::Keep => {}
                    PayloadUpdate::Set(p) => rec.payload = p.clone(),
                }
                if let Some(metadata) = &c.set_metadata {
                    rec.metadata = metadata.clone();
                }
                if let Some(gate_keys) = &c.set_gate_keys {
                    rec.gate_keys = gate_keys.clone();
                }
                if c.set_entity_document.is_some() {
                    rec.entity_document = c.set_entity_document.clone();
                }
                if let ScheduleUpdate::Set(p) = &c.set_priority {
                    rec.priority = p.clone();
                }
                if let ScheduleUpdate::Set(nb) = &c.set_not_before {
                    rec.not_before = *nb;
                    if !c.api001_batch {
                        let now = terminal_at.unwrap_or(rec.eligible_since);
                        rec.eligible_since = nb.unwrap_or(now).max(now);
                    }
                }
                rec.item_version += 1;
                let item_id = c.item_id;
                let removed: Vec<(String, Vec<u8>)> = old_keys
                    .iter()
                    .filter(|k| !new_keys.contains(k))
                    .cloned()
                    .collect();
                let added: Vec<(String, Vec<u8>)> = new_keys
                    .iter()
                    .filter(|k| !old_keys.contains(k))
                    .cloned()
                    .collect();
                self.index_remove_keys(item_id, &removed);
                self.index_insert_keys(item_id, &added);
                self.replace_gate_memberships(item_id, &old_gate_keys, &new_gate_keys);
                // Re-key the eligibility index for a repriced/rescheduled Pending item (no-op otherwise —
                // a non-reprice/non-reschedule or a Leased item leaves the eligibility set unchanged).
                if let Some(old) = old_elig {
                    self.eligible.remove(old);
                }
                if new_elig.is_some() {
                    let rec = self.items.get(&item_id).ok_or(EngineError::NotFound)?;
                    self.eligible.insert(rec, &self.items, &self.priority_model);
                }
                Ok(())
            }
            QueueCommand::MutateItems(c) => {
                for mutation in &c.items {
                    match &mutation.action {
                        ResolvedItemMutationAction::Purge => {
                            if let Some(record) = self.items.remove(&mutation.item_id) {
                                self.remove_record(record)?;
                            }
                        }
                        ResolvedItemMutationAction::Replace(values) => {
                            let old = self
                                .items
                                .get(&mutation.item_id)
                                .cloned()
                                .ok_or(EngineError::NotFound)?;
                            let old_index_keys =
                                self.record_index_keys(&old.fields, old.entity_document.as_ref())?;
                            if old.state == ItemState::Pending
                                && !old.superseded
                                && !gate_keys_blocked(&self.blocked_gates, &old.gate_keys)
                            {
                                self.eligible
                                    .remove(EligibilityIndex::token(&old, &self.priority_model));
                            }
                            let lease_ends = old.state == ItemState::Leased
                                && (values.invalidate_lease || values.state != ItemState::Leased);
                            if lease_ends {
                                self.leased_ids.remove(&mutation.item_id);
                                if !old.lease_is_cohort
                                    && let Some(expires) = old.lease_expires_at
                                {
                                    self.remove_ordinary_lease(expires, &mutation.item_id);
                                }
                                if let Some(token) = old.lease_token.as_ref()
                                    && let Some(ids) = self.leased_by_consumer.get_mut(token)
                                {
                                    ids.remove(&mutation.item_id);
                                    if ids.is_empty() {
                                        self.leased_by_consumer.remove(token);
                                    }
                                }
                            }

                            let new_index_keys = self.record_index_keys(
                                &values.fields,
                                values.entity_document.as_ref(),
                            )?;
                            let record = self
                                .items
                                .get_mut(&mutation.item_id)
                                .ok_or(EngineError::NotFound)?;
                            record.state = values.state;
                            record.item_version = values.item_version;
                            record.priority = values.priority.clone();
                            record.not_before = values.not_before;
                            record.eligible_since = values.eligible_since;
                            record.payload = values.payload.clone();
                            record.fields = values.fields.clone();
                            record.metadata = values.metadata.clone();
                            record.gate_keys = values.gate_keys.clone();
                            record.entity_document = values.entity_document.clone();
                            if lease_ends {
                                record.lease_token = None;
                                record.lease_expires_at = None;
                                record.lease_is_cohort = false;
                                record.worker_id = None;
                                record.fenced = false;
                            }
                            if values.state.is_terminal() {
                                record.terminal_at = terminal_at;
                                record.terminal_position = terminal_position.cloned();
                            } else {
                                record.terminal_at = None;
                                record.terminal_position = None;
                            }

                            let removed = old_index_keys
                                .iter()
                                .filter(|key| !new_index_keys.contains(key))
                                .cloned()
                                .collect::<Vec<_>>();
                            let added = new_index_keys
                                .iter()
                                .filter(|key| !old_index_keys.contains(key))
                                .cloned()
                                .collect::<Vec<_>>();
                            self.index_remove_keys(mutation.item_id, &removed);
                            self.index_insert_keys(mutation.item_id, &added);
                            self.replace_gate_memberships(
                                mutation.item_id,
                                &old.gate_keys,
                                &values.gate_keys,
                            );
                            if old.state != values.state {
                                self.metrics_transition(old.state, values.state);
                            }
                            let record = self
                                .items
                                .get(&mutation.item_id)
                                .ok_or(EngineError::NotFound)?;
                            if record.state == ItemState::Pending
                                && !record.superseded
                                && !gate_keys_blocked(&self.blocked_gates, &record.gate_keys)
                            {
                                self.eligible
                                    .insert(record, &self.items, &self.priority_model);
                            }
                        }
                    }
                }
                for change in &c.gate_changes {
                    self.apply_gate_change(&change.gate_keys, change.blocked)?;
                }
                Ok(())
            }
            QueueCommand::Finalize(c) => {
                for o in &c.outcomes {
                    let ev = match o.kind {
                        FinalizeKind::Complete => ItemEvent::FinalizeComplete,
                        FinalizeKind::Fail => ItemEvent::FinalizeFail,
                        FinalizeKind::Retry => {
                            // Retry-exhaustion (B'): `attempt_count` = deliveries so far (Claim charges,
                            // reclaim/release do not). `failure_event` (the canonical core predicate) sends
                            // a retry that has used all `max_attempts` deliveries to TERMINAL (Failed)
                            // instead of back to pending; a retry UNDER the bound returns it to pending
                            // (claimable again, the next claim charging the next delivery). Only `Retry` is
                            // bounded — `Release` (no-fault give-back) and `Rearm` (recurrence) are not.
                            // NOTE (scope): this bounds the EXPLICIT-retry path only. The claim/reclaim path
                            // is NOT attempt-bounded — an item whose lease repeatedly EXPIRES (LeaseExpired
                            // → pending → re-Claim, +1 each) can exceed `max_attempts` deliveries without
                            // terminating; bounding that poison-loop is separate, owed policy.
                            // The decision is deterministic from the replayed projection, so apply stays
                            // infallible (both Leased→Pending and Leased→Failed are legal transitions).
                            let rec = self.items.get(&o.item_id).ok_or(EngineError::NotFound)?;
                            failure_event(rec.attempt_count, rec.max_attempts)
                        }
                        FinalizeKind::Release => ItemEvent::FinalizeRelease,
                        FinalizeKind::Rearm => {
                            // recurrence.until cutoff (BQ pqueue-8cbae731): a rearm whose next occurrence
                            // (`not_before`) falls strictly PAST `until` ends the series — the item goes
                            // terminal (Complete) instead of re-arming. `until` only bites on a recurring
                            // queue with an explicit next-occurrence; an immediate rearm (no `not_before`)
                            // or a non-recurring queue re-arms as before. Deterministic from the replayed
                            // command, so apply stays infallible.
                            if matches!(self.recurrence.mode, RecurrenceMode::Recurring)
                                && let (Some(nb), Some(until)) =
                                    (o.not_before, self.recurrence.until)
                                && nb > until
                            {
                                ItemEvent::FinalizeComplete
                            } else {
                                ItemEvent::FinalizeRearm
                            }
                        }
                    };
                    self.transition(&o.item_id, ev, terminal_at, terminal_position)?;
                    let should_reinsert = {
                        let rec = self
                            .items
                            .get_mut(&o.item_id)
                            .ok_or(EngineError::NotFound)?;
                        rec.lease_token = None;
                        rec.lease_expires_at = None;
                        rec.lease_is_cohort = false;
                        rec.fenced = false;
                        // A rearm that returned to Pending (within `until`) resets the delivery count and,
                        // when the caller supplied the next-occurrence time, defers re-eligibility to that
                        // new `not_before` (the idle interval). Re-key after the record mutation.
                        let old_elig = (rec.state == ItemState::Pending
                            && !gate_keys_blocked(&self.blocked_gates, &rec.gate_keys))
                        .then(|| {
                            let model = self.priority_model;
                            EligibilityIndex::token(rec, &model)
                        });
                        if matches!(o.kind, FinalizeKind::Rearm) && rec.state == ItemState::Pending
                        {
                            rec.attempt_count = 0;
                            if let Some(nb) = o.not_before {
                                rec.not_before = Some(nb);
                            }
                            let now = terminal_at.unwrap_or(rec.eligible_since);
                            rec.eligible_since = o.not_before.unwrap_or(now).max(now);
                        }
                        // Queue-native retry backoff: a Retry that returned the item to Pending (still under
                        // the attempt bound) defers its re-eligibility to `not_before`. Guarded on Pending so
                        // an exhausted Retry (-> Failed) gets no backoff.
                        if matches!(o.kind, FinalizeKind::Retry)
                            && rec.state == ItemState::Pending
                            && let Some(nb) = o.not_before
                        {
                            rec.not_before = Some(nb);
                            rec.eligible_since = nb;
                        }
                        if let Some(old) = old_elig {
                            let model = self.priority_model;
                            let new = EligibilityIndex::token(rec, &model);
                            if old != new {
                                self.eligible.remove(old);
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };
                    if should_reinsert {
                        let rec = self.items.get(&o.item_id).ok_or(EngineError::NotFound)?;
                        self.eligible.insert(rec, &self.items, &self.priority_model);
                    }
                }
                Ok(())
            }
            QueueCommand::CohortFinalize(_) => Ok(()),
            QueueCommand::ReplacePending(c) => {
                // Supersede the old pending item; the old id thereafter reads as deleted/superseded.
                let model = self.priority_model;
                // Drop the superseded record's index keys (ADR-010 §5): a superseded item leaves every
                // index, then the replacement is inserted via `insert_pending`.
                let superseded_keys = self
                    .items
                    .get(&c.superseded_item_id)
                    .map(|rec| self.record_index_keys(&rec.fields, rec.entity_document.as_ref()))
                    .transpose()?;
                let superseded_gate_keys = self
                    .items
                    .get(&c.superseded_item_id)
                    .map(|rec| rec.gate_keys.clone())
                    .unwrap_or_default();
                if let Some(rec) = self.items.get_mut(&c.superseded_item_id) {
                    let old = (rec.state == ItemState::Pending)
                        .then(|| EligibilityIndex::token(rec, &model));
                    let old_state = rec.state;
                    let was_live = !rec.superseded;
                    rec.superseded = true;
                    if let Some(k) = old {
                        self.eligible.remove(k);
                    }
                    if was_live {
                        self.metrics_dec(old_state);
                    }
                }
                if let Some(keys) = superseded_keys {
                    self.index_remove_keys(c.superseded_item_id, &keys);
                }
                self.replace_gate_memberships(c.superseded_item_id, &superseded_gate_keys, &[]);
                self.by_key.remove(&c.client_item_key);
                self.insert_pending(c.replacement.clone(), terminal_at)?;
                Ok(())
            }
            QueueCommand::LeaseExpired(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::LeaseExpired, None, None)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = None;
                    rec.lease_expires_at = None;
                    rec.lease_is_cohort = false;
                    // INVARIANT: `attempt_count` = number of times the item was handed to a worker, so
                    // it increments ONLY in the Claim arm. A reclaim returns the item to pending (not a
                    // delivery) and does NOT charge — the subsequent redelivery (a fresh Claim) charges
                    // the one attempt. (TD-006:129 reconciliation; poison detection is preserved since
                    // every redelivery still increments.)
                }
                Ok(())
            }
            QueueCommand::CohortExpired(c) => {
                let model = self.priority_model;
                let ids: Vec<ItemId> = self
                    .items
                    .values()
                    .filter(|r| {
                        r.group_key.as_ref() == Some(&c.group_key) && !r.state.is_terminal()
                    })
                    .map(|r| r.item_id)
                    .collect();
                for id in ids {
                    let old_token = if let Some(rec) = self.items.get_mut(&id) {
                        let old = (rec.state == ItemState::Pending)
                            .then(|| EligibilityIndex::token(rec, &model));
                        let old_state = rec.state;
                        let old_token = (old_state == ItemState::Leased)
                            .then(|| rec.lease_token.clone())
                            .flatten();
                        rec.state = ItemState::Failed; // forced terminal (cohort-incomplete)
                        rec.item_version += 1;
                        rec.terminal_at = terminal_at;
                        rec.terminal_position = terminal_position.cloned();
                        if let Some(k) = old {
                            self.eligible.remove(k);
                        }
                        self.metrics_transition(old_state, ItemState::Failed);
                        old_token
                    } else {
                        None
                    };
                    if let Some(token) = old_token {
                        self.leased_ids.remove(&id);
                        if let Some(leased) = self.leased_by_consumer.get_mut(&token) {
                            leased.remove(&id);
                            if leased.is_empty() {
                                self.leased_by_consumer.remove(&token);
                            }
                        }
                    }
                }
                Ok(())
            }
            QueueCommand::FenceLease(c) => {
                for id in &c.item_ids {
                    if let Some(rec) = self.items.get_mut(id) {
                        rec.fenced = true;
                    }
                }
                Ok(())
            }
            QueueCommand::UnfenceLease(c) => {
                for id in &c.item_ids {
                    if let Some(rec) = self.items.get_mut(id) {
                        rec.fenced = false;
                    }
                }
                Ok(())
            }
            QueueCommand::PauseQueue(c) => {
                self.paused = true;
                self.pause_drain_intake = c.drain_intake;
                Ok(())
            }
            QueueCommand::ResumeQueue => {
                self.paused = false;
                self.pause_drain_intake = false;
                Ok(())
            }
            QueueCommand::SetGates(c) => self.apply_gate_change(&c.gate_keys, c.blocked),
            // Opaque non-work side records (Snorri authoritative-commit boundary): write each key -> payload
            // into the SEPARATE side-record map. Deliberately touches NOTHING in the work-item projection —
            // not `items`, `eligible`, `by_key`, the secondary indexes, nor metrics — so a side record is
            // never claimable/peekable work and survives input finalization. Infallible (insert-or-overwrite).
            QueueCommand::WriteSideRecords(c) => {
                for record in &c.records {
                    self.side_records
                        .insert(record.key.clone(), record.payload.clone());
                }
                Ok(())
            }
            // Advance a caller-supplied opaque instance/state fence (Snorri authoritative-commit boundary).
            // Validated pre-commit (stored == expected, next > expected), so this overwrite is infallible.
            // Touches NOTHING in the work-item projection — a fence is never claimable/peekable work.
            QueueCommand::AdvanceInstanceFence(c) => {
                self.instance_fences.insert(c.instance_key.clone(), c.next);
                Ok(())
            }
            QueueCommand::PurgeItems(c) => {
                for id in &c.item_ids {
                    if let Some(rec) = self.items.remove(id) {
                        self.remove_record(rec)?;
                    }
                }
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Read / decision queries the orchestration ports build on
// ---------------------------------------------------------------------------

fn replacement_value<T: Clone>(update: &fireweed_engine::BatchUpdateValue<T>, current: &T) -> T {
    match update {
        fireweed_engine::BatchUpdateValue::Keep => current.clone(),
        fireweed_engine::BatchUpdateValue::Replace(value) => value.clone(),
    }
}

fn valid_gate_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 256
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn mutation_snapshot(record: &ItemRecord) -> ItemMutationSnapshot {
    ItemMutationSnapshot {
        item_id: record.item_id,
        client_item_key: record.client_item_key(),
        item_version: record.item_version,
        lifecycle_state: record.state,
        priority: record.priority.clone(),
        group_key: record.group_key.clone(),
        cohort_size: record.cohort_size,
        not_before: record.not_before,
        eligible_since: record.eligible_since,
        attempt_count: record.attempt_count,
        max_attempts: record.max_attempts,
        payload: record.payload.clone(),
        fields: record.fields.clone(),
        metadata: record.metadata.clone(),
        gate_keys: record.gate_keys.clone(),
        entity: record.entity_document.clone(),
        lease_token: record.lease_token.clone(),
        lease_expires_at: record.lease_expires_at,
        lease_is_cohort: record.lease_is_cohort,
        worker_id: record.worker_id.clone(),
        fenced: record.fenced,
        superseded: record.superseded,
        terminal_at: record.terminal_at,
        terminal_position: record.terminal_position.clone(),
    }
}

fn mutation_predicate_matches(
    record: &ItemRecord,
    predicate: &ItemPredicate,
    evaluated_at: UtcTimestamp,
) -> bool {
    match predicate {
        ItemPredicate::Any(predicates) => predicates
            .iter()
            .any(|predicate| mutation_predicate_matches(record, predicate, evaluated_at)),
        ItemPredicate::All(predicates) => predicates
            .iter()
            .all(|predicate| mutation_predicate_matches(record, predicate, evaluated_at)),
        ItemPredicate::Not(predicate) => {
            !mutation_predicate_matches(record, predicate, evaluated_at)
        }
        ItemPredicate::StateIn(states) => states.contains(&record.state),
        ItemPredicate::AttemptCountEq(expected) => record.attempt_count == *expected,
        ItemPredicate::LeaseActive(expected) => {
            let active = record.state == ItemState::Leased
                && record
                    .lease_expires_at
                    .is_some_and(|expires_at| expires_at > evaluated_at);
            active == *expected
        }
        ItemPredicate::NotBefore { comparison, value } => {
            record.not_before.is_some_and(|actual| match comparison {
                fireweed_engine::TimestampComparison::Equal => actual == *value,
                fireweed_engine::TimestampComparison::Before => actual < *value,
                fireweed_engine::TimestampComparison::BeforeOrEqual => actual <= *value,
                fireweed_engine::TimestampComparison::After => actual > *value,
                fireweed_engine::TimestampComparison::AfterOrEqual => actual >= *value,
            })
        }
        ItemPredicate::ClientItemKeyEq(key) => &record.client_item_key() == key,
        ItemPredicate::GroupKeyEq(group) => &record.group_key == group,
        ItemPredicate::FieldEq { name, value } => record.fields.get(name) == value.as_ref(),
        ItemPredicate::MetadataEq { name, value } => record.metadata.get(name) == value.as_ref(),
        ItemPredicate::EntityEq { pointer, value } => {
            if pointer_tokens(pointer).is_err() {
                return false;
            }
            let actual = record
                .entity_document
                .as_ref()
                .and_then(|document| document.pointer(pointer));
            match value {
                EntityPredicateValue::Missing => actual.is_none(),
                EntityPredicateValue::Value(expected) => actual == Some(expected),
            }
        }
        ItemPredicate::GateKeyPresent(key) => record.gate_keys.contains(key),
        ItemPredicate::GateKeyAbsent(key) => !record.gate_keys.contains(key),
    }
}

fn pointer_tokens(pointer: &str) -> Result<Vec<String>, ()> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    let Some(rest) = pointer.strip_prefix('/') else {
        return Err(());
    };
    rest.split('/')
        .map(|token| {
            let mut decoded = String::with_capacity(token.len());
            let mut chars = token.chars();
            while let Some(ch) = chars.next() {
                if ch != '~' {
                    decoded.push(ch);
                    continue;
                }
                match chars.next() {
                    Some('0') => decoded.push('~'),
                    Some('1') => decoded.push('/'),
                    _ => return Err(()),
                }
            }
            Ok(decoded)
        })
        .collect()
}

fn apply_entity_edit(
    entity: &mut Option<Value>,
    pointer: &str,
    operation: &EntityEditOperation,
) -> Result<(), ()> {
    let tokens = pointer_tokens(pointer)?;
    if tokens.is_empty() {
        *entity = match operation {
            EntityEditOperation::Set(value) => Some(value.clone()),
            EntityEditOperation::Remove => None,
        };
        return Ok(());
    }
    let mut current = entity.as_mut().ok_or(())?;
    for token in &tokens[..tokens.len() - 1] {
        current = match current {
            Value::Object(map) => map.get_mut(token).ok_or(())?,
            Value::Array(values) => values
                .get_mut(token.parse::<usize>().map_err(|_| ())?)
                .ok_or(())?,
            _ => return Err(()),
        };
    }
    let leaf = tokens.last().expect("non-root pointer has a leaf");
    match (current, operation) {
        (Value::Object(map), EntityEditOperation::Set(value)) => {
            map.insert(leaf.clone(), value.clone());
        }
        (Value::Object(map), EntityEditOperation::Remove) => {
            if map.remove(leaf).is_none() {
                return Err(());
            }
        }
        (Value::Array(values), EntityEditOperation::Set(value)) => {
            let index = leaf.parse::<usize>().map_err(|_| ())?;
            let slot = values.get_mut(index).ok_or(())?;
            *slot = value.clone();
        }
        (Value::Array(values), EntityEditOperation::Remove) => {
            let index = leaf.parse::<usize>().map_err(|_| ())?;
            if index >= values.len() {
                return Err(());
            }
            values.remove(index);
        }
        _ => return Err(()),
    }
    Ok(())
}

fn update_selector_aggregate(
    aggregate: &mut ItemMutationSelectorAggregate,
    outcome: &ItemMutationOutcome,
) {
    match outcome {
        ItemMutationOutcome::Updated { .. } | ItemMutationOutcome::WouldUpdate { .. } => {
            aggregate.changed += 1;
        }
        ItemMutationOutcome::Purged | ItemMutationOutcome::WouldPurge => {
            aggregate.changed += 1;
            aggregate.purged += 1;
        }
        ItemMutationOutcome::NoChange => {}
        _ => aggregate.rejected += 1,
    }
}

impl ProjectionData {
    pub fn plan_item_mutation(
        &self,
        request: &ItemMutationRequest,
    ) -> EngineResult<ItemMutationPlan> {
        let mut results = Vec::new();
        let mut commands = Vec::new();
        let mut selectors = match &request.operation {
            ItemMutationOperation::SelectFirst { clauses } => clauses
                .iter()
                .map(|clause| ItemMutationSelectorAggregate {
                    selector_id: clause.selector_id.clone(),
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
            ItemMutationOperation::Addressed { .. } => Vec::new(),
        };
        let mut seen = BTreeSet::new();

        match &request.operation {
            ItemMutationOperation::Addressed { entries } => {
                for entry in entries {
                    let before = self.items.get(&entry.item_id);
                    let before_snapshot = before.and_then(|record| {
                        (request.returning == ItemMutationReturning::BeforeSnapshot)
                            .then(|| mutation_snapshot(record))
                    });
                    let (outcome, command) = if !seen.insert(entry.item_id) {
                        (ItemMutationOutcome::Invalid, None)
                    } else if let Some(record) = before {
                        if entry
                            .expected_item_version
                            .is_some_and(|expected| expected != record.item_version)
                        {
                            (
                                ItemMutationOutcome::Conflict {
                                    actual_version: record.item_version,
                                },
                                None,
                            )
                        } else if !entry.predicates.iter().all(|predicate| {
                            mutation_predicate_matches(record, predicate, request.evaluated_at)
                        }) {
                            (
                                ItemMutationOutcome::PreconditionFailed(
                                    ItemMutationPrecondition::Predicate,
                                ),
                                None,
                            )
                        } else {
                            self.plan_record_mutation(
                                record,
                                &entry.lease_guard,
                                &entry.patch,
                                request.evaluated_at,
                                request.dry_run,
                            )?
                        }
                    } else {
                        (ItemMutationOutcome::NotFound, None)
                    };
                    if let Some(command) = command {
                        commands.push(command);
                    }
                    results.push(ItemMutationResult {
                        item_id: entry.item_id,
                        selector_id: None,
                        outcome,
                        before: before_snapshot,
                    });
                }
            }
            ItemMutationOperation::SelectFirst { clauses } => {
                let mut records = self.items.values().collect::<Vec<_>>();
                records.sort_by_key(|record| (record.created_seq, record.item_id));
                for record in records {
                    let Some((selector_index, clause)) =
                        clauses.iter().enumerate().find(|(_, clause)| {
                            let in_scope = match clause.selector.scope {
                                ItemSelectorScope::Live => {
                                    !record.superseded && !record.state.is_terminal()
                                }
                                ItemSelectorScope::Retained => !record.superseded,
                            };
                            in_scope
                                && clause.selector.predicates.iter().all(|predicate| {
                                    mutation_predicate_matches(
                                        record,
                                        predicate,
                                        request.evaluated_at,
                                    )
                                })
                        })
                    else {
                        continue;
                    };
                    selectors[selector_index].matched += 1;
                    let (outcome, command) = if clause.predicates.iter().all(|predicate| {
                        mutation_predicate_matches(record, predicate, request.evaluated_at)
                    }) {
                        self.plan_record_mutation(
                            record,
                            &clause.lease_guard,
                            &clause.patch,
                            request.evaluated_at,
                            request.dry_run,
                        )?
                    } else {
                        (
                            ItemMutationOutcome::PreconditionFailed(
                                ItemMutationPrecondition::Predicate,
                            ),
                            None,
                        )
                    };
                    update_selector_aggregate(&mut selectors[selector_index], &outcome);
                    if let Some(command) = command {
                        commands.push(command);
                    }
                    results.push(ItemMutationResult {
                        item_id: record.item_id,
                        selector_id: Some(clause.selector_id.clone()),
                        outcome,
                        before: (request.returning == ItemMutationReturning::BeforeSnapshot)
                            .then(|| mutation_snapshot(record)),
                    });
                }
            }
        }

        let mut summary = ItemMutationSummary::default();
        for result in &results {
            summary.matched += 1;
            match result.outcome {
                ItemMutationOutcome::Updated { .. } | ItemMutationOutcome::WouldUpdate { .. } => {
                    summary.changed += 1;
                }
                ItemMutationOutcome::Purged | ItemMutationOutcome::WouldPurge => {
                    summary.changed += 1;
                    summary.purged += 1;
                }
                ItemMutationOutcome::NoChange => summary.unchanged += 1,
                _ => summary.rejected += 1,
            }
        }
        let request_gate_keys = request
            .gate_changes
            .iter()
            .flat_map(|change| change.gate_keys.iter())
            .collect::<BTreeSet<_>>();
        if (!request_gate_keys.is_empty() && self.gate_key_policy != GateKeyPolicy::Dynamic)
            || self
                .max_gates_per_request
                .is_some_and(|limit| request_gate_keys.len() as u64 > limit)
        {
            return Err(EngineError::Invalid("gate changes violate queue policy"));
        }
        for change in &request.gate_changes {
            if change.gate_keys.iter().any(|key| !valid_gate_key(key)) {
                return Err(EngineError::Invalid("invalid gate key"));
            }
        }
        // Unique keys produced by successful siblings must not collide with each other. Existing-row
        // collisions were checked while planning each record.
        let mut batch_unique = BTreeMap::<(String, Vec<u8>), ItemId>::new();
        for command in &commands {
            let ResolvedItemMutationAction::Replace(values) = &command.action else {
                continue;
            };
            for (name, key) in
                self.record_index_keys(&values.fields, values.entity_document.as_ref())?
            {
                if matches!(self.indexes.get(&name), Some(SecondaryIndex::Unique(_)))
                    && let Some(other) = batch_unique.insert((name, key), command.item_id)
                    && other != command.item_id
                {
                    return Err(EngineError::Conflict);
                }
            }
        }
        let command = MutateItemsCommand {
            items: commands,
            gate_changes: request.gate_changes.clone(),
        };
        // Apply to a private image before append. This proves the resolved command cannot fail halfway
        // through the serving projection after the log has accepted it.
        if !request.dry_run {
            let mut scratch = self.clone();
            scratch.apply_command(&QueueCommand::MutateItems(command.clone()))?;
        }
        Ok(ItemMutationPlan {
            response: ItemMutationResponse {
                request_id: request.request_id.clone(),
                position: None,
                dry_run: request.dry_run,
                results,
                selectors,
                summary,
            },
            command,
        })
    }

    fn plan_record_mutation(
        &self,
        record: &ItemRecord,
        lease_guard: &LeaseGuard,
        patch: &ItemPatch,
        evaluated_at: UtcTimestamp,
        dry_run: bool,
    ) -> EngineResult<(ItemMutationOutcome, Option<ResolvedItemMutation>)> {
        let stored_lease = record.state == ItemState::Leased;
        let active_lease = stored_lease
            && record
                .lease_expires_at
                .is_some_and(|expires_at| expires_at > evaluated_at);
        match lease_guard {
            LeaseGuard::RejectActive if active_lease => {
                return Ok((
                    ItemMutationOutcome::PreconditionFailed(ItemMutationPrecondition::ActiveLease),
                    None,
                ));
            }
            LeaseGuard::RequireActive if !active_lease => {
                return Ok((
                    ItemMutationOutcome::PreconditionFailed(ItemMutationPrecondition::ActiveLease),
                    None,
                ));
            }
            LeaseGuard::Match(token)
                if !active_lease || record.lease_token.as_ref() != Some(token) =>
            {
                return Ok((ItemMutationOutcome::StaleLease, None));
            }
            _ => {}
        }
        if record.state.is_terminal()
            && !matches!(
                patch.lifecycle,
                LifecyclePatch::Keep | LifecyclePatch::SetPending | LifecyclePatch::Purge
            )
        {
            return Ok((ItemMutationOutcome::Terminal, None));
        }
        if matches!(patch.lifecycle, LifecyclePatch::Purge) {
            return Ok((
                if dry_run {
                    ItemMutationOutcome::WouldPurge
                } else {
                    ItemMutationOutcome::Purged
                },
                (!dry_run).then_some(ResolvedItemMutation {
                    item_id: record.item_id,
                    action: ResolvedItemMutationAction::Purge,
                }),
            ));
        }

        let state = match patch.lifecycle {
            LifecyclePatch::Keep
                if stored_lease && matches!(lease_guard, LeaseGuard::InvalidateActive) =>
            {
                ItemState::Pending
            }
            LifecyclePatch::Keep => record.state,
            LifecyclePatch::SetPending => ItemState::Pending,
            LifecyclePatch::SetComplete => ItemState::Complete,
            LifecyclePatch::SetFailed => ItemState::Failed,
            LifecyclePatch::Purge => unreachable!(),
        };
        if active_lease
            && state != ItemState::Leased
            && !matches!(
                lease_guard,
                LeaseGuard::RequireActive | LeaseGuard::Match(_) | LeaseGuard::InvalidateActive
            )
        {
            return Ok((
                ItemMutationOutcome::PreconditionFailed(ItemMutationPrecondition::ActiveLease),
                None,
            ));
        }

        let priority = replacement_value(&patch.priority, &record.priority);
        if priority.as_ref().is_some_and(|priority| {
            !matches!(
                (&self.priority_model.kind, priority),
                (
                    fireweed_core::PriorityModelKind::Timestamp,
                    PriorityValue::Timestamp(_)
                ) | (
                    fireweed_core::PriorityModelKind::Int64,
                    PriorityValue::Int64(_)
                ) | (
                    fireweed_core::PriorityModelKind::Decimal,
                    PriorityValue::Decimal(_)
                ) | (
                    fireweed_core::PriorityModelKind::Text,
                    PriorityValue::Text(_)
                )
            )
        }) {
            return Ok((ItemMutationOutcome::Invalid, None));
        }
        let not_before = replacement_value(&patch.not_before, &record.not_before);
        let payload = replacement_value(&patch.payload, &record.payload);
        let metadata = replacement_value(&patch.metadata, &record.metadata);
        let mut gate_keys = record.gate_keys.clone();
        if patch.gate_keys.remove_prefixes.iter().any(String::is_empty) {
            return Ok((ItemMutationOutcome::Invalid, None));
        }
        gate_keys.retain(|key| !patch.gate_keys.remove.contains(key));
        gate_keys.retain(|key| {
            !patch
                .gate_keys
                .remove_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
        });
        gate_keys.extend(patch.gate_keys.add.iter().cloned());
        gate_keys.sort();
        gate_keys.dedup();
        if gate_keys.iter().any(|key| !valid_gate_key(key))
            || (!gate_keys.is_empty() && self.gate_key_policy != GateKeyPolicy::Dynamic)
            || self
                .max_gate_keys_per_item
                .is_some_and(|limit| gate_keys.len() as u64 > limit)
        {
            return Ok((ItemMutationOutcome::Invalid, None));
        }
        let mut fields = record.fields.clone();
        for (name, value) in &patch.field_edits {
            if fireweed_engine::is_api001_reserved_write_field(name) {
                return Ok((ItemMutationOutcome::Invalid, None));
            }
            match value {
                Some(value) => {
                    fields.insert(name.clone(), value.clone());
                }
                None => {
                    fields.remove(name);
                }
            }
        }
        let mut entity = record.entity_document.clone();
        for edit in &patch.entity_edits {
            if apply_entity_edit(&mut entity, &edit.pointer, &edit.operation).is_err() {
                return Ok((ItemMutationOutcome::Invalid, None));
            }
        }
        if self
            .index_validate_with_entity(&record.item_id, &fields, entity.as_ref(), None)
            .is_err()
        {
            return Ok((ItemMutationOutcome::Invalid, None));
        }
        let invalidate_lease = stored_lease
            && (matches!(lease_guard, LeaseGuard::InvalidateActive) || state != ItemState::Leased);
        let changed = state != record.state
            || priority != record.priority
            || not_before != record.not_before
            || payload != record.payload
            || metadata != record.metadata
            || gate_keys != record.gate_keys
            || fields != record.fields
            || entity != record.entity_document
            || invalidate_lease;
        if !changed {
            return Ok((ItemMutationOutcome::NoChange, None));
        }
        let item_version = record.item_version.saturating_add(1);
        let eligible_since = if state == ItemState::Pending
            && (not_before != record.not_before || record.state != ItemState::Pending)
        {
            not_before.unwrap_or(evaluated_at).max(evaluated_at)
        } else {
            record.eligible_since
        };
        let outcome = if dry_run {
            ItemMutationOutcome::WouldUpdate {
                item_version,
                state,
            }
        } else {
            ItemMutationOutcome::Updated {
                item_version,
                state,
            }
        };
        Ok((
            outcome,
            (!dry_run).then_some(ResolvedItemMutation {
                item_id: record.item_id,
                action: ResolvedItemMutationAction::Replace(Box::new(ResolvedItemValues {
                    state,
                    item_version,
                    priority,
                    not_before,
                    eligible_since,
                    payload,
                    fields,
                    metadata,
                    gate_keys,
                    entity_document: entity,
                    invalidate_lease,
                })),
            }),
        ))
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }

    pub fn is_intake_blocked(&self) -> bool {
        self.paused && self.pause_drain_intake
    }

    /// Exact intrinsic active-scope discovery over the shared projection. This intentionally ignores the
    /// queue pause flag (operators still need to see pause-induced buildup), but applies every item-level
    /// eligibility predicate at `now`, including due time, supersession, lifecycle, and dynamic gates.
    pub fn discover_active_scopes(
        &self,
        queue_id: &str,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> Vec<ActiveScope> {
        let mut groups: BTreeMap<Option<String>, (UtcTimestamp, u64)> = BTreeMap::new();
        for record in self.items.values().filter(|record| {
            record.state == ItemState::Pending
                && !record.superseded
                && record.not_before.is_none_or(|not_before| not_before <= now)
                && !gate_keys_blocked(&self.blocked_gates, &record.gate_keys)
        }) {
            groups
                .entry(record.group_key.as_ref().map(|key| key.as_str().to_owned()))
                .and_modify(|(oldest, count)| {
                    *oldest = (*oldest).min(record.eligible_since);
                    *count = count.saturating_add(1);
                })
                .or_insert((record.eligible_since, 1));
        }

        let now_ns = i128::from(now.seconds) * 1_000_000_000 + i128::from(now.nanoseconds);
        let mut source = groups
            .into_iter()
            .map(|(group_key, (oldest, count))| {
                let oldest_ns =
                    i128::from(oldest.seconds) * 1_000_000_000 + i128::from(oldest.nanoseconds);
                (
                    oldest,
                    ActiveScope {
                        queue_id: queue_id.to_owned(),
                        group_key,
                        oldest_eligible_age_ms: now_ns.saturating_sub(oldest_ns).max(0) as u64
                            / 1_000_000,
                        eligible_count: Some(count),
                        progress_bound_risk_count: None,
                    },
                )
            })
            .collect::<Vec<_>>();
        // Relational parity: oldest timestamp first; equal timestamps put the ungrouped scope first,
        // followed by lexical group key.
        source.sort_by(|(left_time, left), (right_time, right)| {
            left_time
                .cmp(right_time)
                .then_with(|| left.group_key.cmp(&right.group_key))
        });
        project_scopes(
            source.into_iter().map(|(_, scope)| scope).collect(),
            granularity,
        )
    }

    /// Priority-ordered eligible candidates (pending, not superseded, due at `now`), capped at `max`.
    /// Returns empty while the queue is paused. This is the claim/select selection (Invariant 1:
    /// per-item, in eligible order).
    ///
    /// Under `OrderingMode::Strict` (or a `0` bound) this is exact strict priority order. Under
    /// `OrderingMode::BoundedRelaxed` with `max_rank_error > 0` it delegates to the bounded-relaxed
    /// selection (`relaxed_candidates`), which may reorder for locality WITHIN the declared bound.
    pub fn eligible_candidates(&self, now: UtcTimestamp, max: usize) -> Vec<ItemId> {
        if self.paused {
            return Vec::new();
        }
        let bound = match self.ordering_mode {
            OrderingMode::BoundedRelaxed => self.max_rank_error,
            OrderingMode::Strict => 0,
        };
        if bound == 0 {
            // Strict / 0-bound: byte-for-byte the original strict selection (no relaxation). `eligible`
            // contains only pending, non-superseded items; due-time lives on the key so the claim hot path
            // does not need one HashMap lookup per candidate.
            return self.eligible.strict_candidates(now, max);
        }
        self.relaxed_candidates(now, max, bound)
    }

    /// Strict/0-bound candidate page after a previously selected item. Used by group-commit claim
    /// reservation to avoid rescanning the same reserved prefix for every queued claim.
    pub fn eligible_candidates_after(
        &self,
        now: UtcTimestamp,
        after: Option<ItemId>,
        max: usize,
    ) -> Vec<ItemId> {
        if self.paused || max == 0 {
            return Vec::new();
        }
        let bound = match self.ordering_mode {
            OrderingMode::BoundedRelaxed => self.max_rank_error,
            OrderingMode::Strict => 0,
        };
        if bound != 0 {
            return self.eligible_candidates(now, max);
        }
        let Some(after) = after else {
            return self.eligible_candidates(now, max);
        };
        let Some(rec) = self.items.get(&after) else {
            return self.eligible_candidates(now, max);
        };
        self.eligible
            .strict_candidates_after(now, rec, &self.priority_model, max)
    }

    /// Bounded-relaxed claim selection (TP-003 INV-6 + INV-4). Takes the strict-priority eligible prefix
    /// (the lowest-rank `max` items — selection itself never starves anything), then reorders each
    /// consecutive block of `bound + 1` items by locality so same-group work is batched together for claim
    /// throughput/locality. `bound == max_rank_error`; locality key = `group_key` (None sorts last),
    /// tie-broken by strict order (a stable sort preserves strict order within a group).
    ///
    /// INV-6 (bounded rank error): an item only ever moves WITHIN its block of `bound + 1` consecutive
    /// strict positions, so its delivered position deviates from its strict position by at most `bound` —
    /// in either direction. The bound holds per claim AND composes across batched claims: because
    /// selection is the strict prefix, an item with strict rank `r` is always claimed in the same batch it
    /// would be under strict ordering, and only reordered within that batch's blocks.
    ///
    /// INV-4 (progress / no starvation): selection is the exact strict prefix, so no eligible item is ever
    /// passed over for selection — every pushed item is claimed in strict batch order. The intra-block
    /// reordering only permutes delivery order within the `bound`, it never defers an item to a later batch.
    fn relaxed_candidates(&self, now: UtcTimestamp, max: usize, bound: u32) -> Vec<ItemId> {
        if max == 0 {
            return Vec::new();
        }
        self.eligible.relaxed_candidates(now, max, bound)
    }

    /// `ProjectionRead::select_eligible`.
    pub fn select_eligible(&self, now: UtcTimestamp, limit: usize) -> Vec<ItemId> {
        self.eligible_candidates(now, limit)
    }

    /// `ProjectionRead::peek` — non-destructive eligible view (shows the pending order).
    pub fn peek(&self, limit: usize) -> Vec<ItemView> {
        let mut out = Vec::new();
        for item in self.eligible.ordered_items(limit) {
            if out.len() >= limit {
                break;
            }
            if let Some(rec) = self.items.get(&item)
                && rec.state == ItemState::Pending
                && !rec.superseded
            {
                out.push(ItemView {
                    item_id: rec.item_id,
                    client_item_key: rec.client_item_key(),
                    priority: rec.priority.clone(),
                    item_version: rec.item_version,
                });
            }
        }
        out
    }

    /// Bounded page of the authoritative pending order, used by recovery verification without
    /// materializing resident cardinality.
    pub fn peek_page(&self, after: Option<ItemId>, limit: usize) -> Vec<ItemView> {
        let after = after.and_then(|item| self.items.get(&item));
        self.eligible
            .ordered_items_after(after, &self.priority_model, limit)
            .into_iter()
            .filter_map(|item| self.items.get(&item))
            .filter(|record| record.state == ItemState::Pending && !record.superseded)
            .map(|record| ItemView {
                item_id: record.item_id,
                client_item_key: record.client_item_key(),
                priority: record.priority.clone(),
                item_version: record.item_version,
            })
            .collect()
    }

    /// `ProjectionRead::pending` — the in-flight (leased) items.
    pub fn pending_leases(&self) -> Vec<LeaseView> {
        self.leased_ids
            .iter()
            .filter_map(|id| self.items.get(id))
            .filter_map(|r| {
                Some(LeaseView {
                    item_id: r.item_id,
                    lease_token: r.lease_token.clone()?,
                    lease_expires_at: r.lease_expires_at?,
                    attempt_count: r.attempt_count,
                })
            })
            .collect()
    }

    pub fn pending_summary(&self) -> PendingSummary {
        let consumers = self
            .leased_by_consumer
            .iter()
            .map(|(token, ids)| (token.clone(), ids.len() as u64))
            .collect::<Vec<_>>();
        let mut consumers = consumers;
        consumers.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
        PendingSummary {
            count: self.leased_ids.len() as u64,
            min_id: self.leased_ids.first().copied(),
            max_id: self.leased_ids.last().copied(),
            consumers,
        }
    }

    pub fn pending_page(&self, start: Option<ItemId>, limit: usize) -> PendingPage {
        use std::ops::Bound::{Included, Unbounded};
        let bounds = (start.map_or(Unbounded, Included), Unbounded);
        let mut leases = self
            .leased_ids
            .range(bounds)
            .filter_map(|id| self.items.get(id).and_then(ItemRecord::lease_view))
            .take(limit.saturating_add(1));
        let entries = leases.by_ref().take(limit).collect();
        let next = leases.next().map(|lease| lease.item_id);
        PendingPage { entries, next }
    }

    pub fn pending_range(
        &self,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> Vec<LeaseView> {
        use std::ops::Bound::{Included, Unbounded};
        let bounds = (
            start.map_or(Unbounded, Included),
            end.map_or(Unbounded, Included),
        );
        let ids = consumer
            .and_then(|token| self.leased_by_consumer.get(token))
            .unwrap_or(&self.leased_ids);
        ids.range(bounds)
            .filter_map(|id| self.items.get(id).and_then(ItemRecord::lease_view))
            .take(limit)
            .collect()
    }

    pub fn pending_by_ids(&self, ids: &[ItemId]) -> Vec<LeaseView> {
        ids.iter()
            .filter(|id| self.leased_ids.contains(id))
            .filter_map(|id| self.items.get(id).and_then(ItemRecord::lease_view))
            .collect()
    }

    /// `ProjectionRead::metrics` — per-state counts (superseded items excluded).
    pub fn metrics(&self) -> QueueMetrics {
        self.metrics.clone()
    }

    /// Filtered lifecycle metrics over the authoritative projection.
    pub fn metrics_by_query(&self, request: MetricsByQueryRequest) -> EngineResult<QueueMetrics> {
        filtered_lifecycle_metrics(
            &self.typed_index_specs,
            &request,
            self.items
                .values()
                .map(|rec| (rec.state, rec.superseded, rec.entity_document.as_ref())),
        )
    }

    fn remove_record(&mut self, rec: ItemRecord) -> EngineResult<()> {
        if !rec.superseded {
            self.metrics_dec(rec.state);
        }
        if let Some(key) = &rec.explicit_client_item_key {
            self.by_key.remove(key);
        }
        self.replace_gate_memberships(rec.item_id, &rec.gate_keys, &[]);
        if rec.state == ItemState::Pending {
            self.eligible
                .remove(EligibilityIndex::token(&rec, &self.priority_model));
        }
        if rec.state == ItemState::Leased {
            self.leased_ids.remove(&rec.item_id);
            if let Some(expires) = rec.lease_expires_at {
                self.remove_ordinary_lease(expires, &rec.item_id);
            }
            if let Some(token) = rec.lease_token.as_ref()
                && let Some(ids) = self.leased_by_consumer.get_mut(token)
            {
                ids.remove(&rec.item_id);
                if ids.is_empty() {
                    self.leased_by_consumer.remove(token);
                }
            }
        }
        let keys = self.record_index_keys(&rec.fields, rec.entity_document.as_ref())?;
        self.index_remove_keys(rec.item_id, &keys);
        Ok(())
    }

    fn terminal_records(&self) -> impl Iterator<Item = &ItemRecord> {
        self.items
            .values()
            .filter(|rec| rec.state.is_terminal() && !rec.superseded)
    }

    fn terminal_is_reapable(
        rec: &ItemRecord,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> bool {
        let Some(terminal_at) = rec.terminal_at else {
            return false;
        };
        if add_millis(terminal_at, terminal_retention_ms) > now {
            return false;
        }
        if !emit_change_records {
            return true;
        }
        let Some(terminal_position) = rec.terminal_position.as_ref() else {
            return false;
        };
        emission_cursor.is_some_and(|cursor| !cursor.precedes(terminal_position))
    }

    pub fn terminal_emission_metrics(
        &self,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> TerminalEmissionMetrics {
        let resident_terminal_count = self.terminal_records().count() as u64;
        if !emit_change_records {
            return TerminalEmissionMetrics {
                resident_terminal_count,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            };
        }

        let mut emission_lag_commands = 0u64;
        let mut emission_oldest_unemitted_age_ms = 0u64;
        for rec in self.terminal_records() {
            let Some(terminal_position) = rec.terminal_position.as_ref() else {
                continue;
            };
            let behind = match emission_cursor {
                None => true,
                Some(cursor) => cursor.precedes(terminal_position),
            };
            if !behind {
                continue;
            }
            emission_lag_commands += 1;
            if let Some(terminal_at) = rec.terminal_at {
                let now_ms = timestamp_to_ms(now);
                let terminal_ms = timestamp_to_ms(terminal_at);
                let age_ms = if now_ms > terminal_ms {
                    (now_ms - terminal_ms) as u64
                } else {
                    0
                };
                emission_oldest_unemitted_age_ms = emission_oldest_unemitted_age_ms.max(age_ms);
            }
        }
        TerminalEmissionMetrics {
            resident_terminal_count,
            emission_lag_commands,
            emission_oldest_unemitted_age_ms,
        }
    }

    pub fn reap_terminal_items(
        &mut self,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> Vec<ItemId> {
        let ids: Vec<ItemId> = self
            .terminal_records()
            .filter(|rec| {
                Self::terminal_is_reapable(
                    rec,
                    now,
                    terminal_retention_ms,
                    emit_change_records,
                    emission_cursor,
                )
            })
            .map(|rec| rec.item_id)
            .collect();
        let mut reaped = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(rec) = self.items.remove(&id) {
                reaped.push(id);
                self.remove_record(rec)
                    .expect("terminal reap record removal must remain infallible");
            }
        }
        reaped
    }

    /// Render the given ids into the rich claimed-item shape (lease fields must be `Some`). Used right
    /// after a Claim commit to build the `ClaimPort` response.
    pub fn render_claimed(&self, ids: &[ItemId]) -> Vec<ClaimedItem> {
        ids.iter()
            .filter_map(|id| self.items.get(id))
            .filter_map(ItemRecord::to_claimed)
            .collect()
    }

    /// Render live hot-storage items by client key, preserving input order.
    pub fn live_items_by_key(&self, keys: &[ClientItemKey]) -> Vec<Option<LiveItemView>> {
        keys.iter()
            .map(|key| {
                self.item_id_for_client_key(key)
                    .and_then(|id| self.items.get(id))
                    .and_then(ItemRecord::to_live)
            })
            .collect()
    }

    /// The item id currently mapped to `client_item_key`, if any (upsert collision lookup).
    pub fn lookup_by_key(&self, client_item_key: &ClientItemKey) -> Option<ItemId> {
        self.item_id_for_client_key(client_item_key).copied()
    }

    /// Resolve all BatchUpdate references from this immutable projection image in one call.
    pub fn batch_update_snapshot(
        &self,
        refs: &[BatchUpdateItemRef],
    ) -> Vec<BatchUpdateSnapshotItem> {
        let mut ids = BTreeSet::new();
        for item_ref in refs {
            match item_ref {
                BatchUpdateItemRef::ItemId(item_id) => {
                    ids.insert(*item_id);
                }
                BatchUpdateItemRef::ClientItemKey(key) => {
                    if let Some(item_id) = self.item_id_for_client_key(key) {
                        ids.insert(*item_id);
                    }
                }
                BatchUpdateItemRef::Both {
                    item_id,
                    client_item_key,
                } => {
                    ids.insert(*item_id);
                    if let Some(key_item_id) = self.item_id_for_client_key(client_item_key) {
                        ids.insert(*key_item_id);
                    }
                }
            }
        }
        ids.into_iter()
            .filter_map(|item_id| {
                self.items
                    .get(&item_id)
                    .map(|record| BatchUpdateSnapshotItem {
                        item_id,
                        client_item_key: record.client_item_key(),
                        state: record.state,
                        item_version: record.item_version,
                        fenced: record.fenced,
                        superseded: record.superseded,
                    })
            })
            .collect()
    }

    /// Validate replacement commands without mutating the projection. Unique-index violations are
    /// entry-local; two siblings claiming the same new unique key invalidate both.
    pub fn batch_update_preflight(
        &self,
        commands: &[UpdateFieldsCommand],
    ) -> EngineResult<Vec<bool>> {
        let mut accepted = vec![true; commands.len()];
        let mut batch_unique: BTreeMap<(String, Vec<u8>), usize> = BTreeMap::new();
        for (index, command) in commands.iter().enumerate() {
            let Some(record) = self.items.get(&command.item_id) else {
                accepted[index] = false;
                continue;
            };
            if record.state != ItemState::Pending
                || record.fenced
                || record.superseded
                || record.state.is_terminal()
            {
                accepted[index] = false;
                continue;
            }
            let mut fields = command
                .set_fields
                .clone()
                .unwrap_or_else(|| record.fields.clone());
            for (name, operation) in &command.field_ops {
                match operation {
                    Some(value) => {
                        fields.insert(name.clone(), value.clone());
                    }
                    None => {
                        fields.remove(name);
                    }
                }
            }
            let entity = command
                .set_entity_document
                .as_ref()
                .or(record.entity_document.as_ref());
            match self.index_validate_with_entity(&command.item_id, &fields, entity, None) {
                Ok(()) => {}
                Err(EngineError::Conflict) => {
                    accepted[index] = false;
                    continue;
                }
                Err(error) => return Err(error),
            }
            for (name, key) in self.record_index_keys(&fields, entity)? {
                if !matches!(self.indexes.get(&name), Some(SecondaryIndex::Unique(_))) {
                    continue;
                }
                if let Some(previous) = batch_unique.insert((name, key), index) {
                    accepted[previous] = false;
                    accepted[index] = false;
                }
            }
        }
        Ok(accepted)
    }

    fn item_id_for_client_key(&self, client_item_key: &ClientItemKey) -> Option<&ItemId> {
        if let Some(id) = self.by_key.get(client_item_key) {
            return Some(id);
        }
        let id = ItemId::new(client_item_key.as_str()).ok()?;
        self.items
            .get_key_value(&id)
            .and_then(|(id, rec)| (!rec.superseded).then_some(id))
    }

    /// The lifecycle state of `id`, if present (upsert collision classification).
    pub fn item_state(&self, id: &ItemId) -> Option<ItemState> {
        self.items.get(id).map(|r| r.state)
    }

    /// Point-lookup claim classification for API-001 `BatchClaimByItemIds`.
    ///
    /// Resolves via the primary `items` map (`O(1)` per id). MUST NOT scan the eligible-candidate
    /// index or iterate unrelated pending rows. Regression: engine test
    /// `claim_by_item_ids_point_lookup_cost_independent_of_unrelated_pending` would fail if this
    /// path devolved into a full-shard eligible scan (fireweed-0ef12e8c).
    pub fn classify_claim_by_item_id(
        &self,
        id: &ItemId,
        now: UtcTimestamp,
    ) -> fireweed_core::ClaimByItemIdClass {
        use fireweed_core::ClaimByItemIdClass;
        let Some(rec) = self.items.get(id) else {
            return ClaimByItemIdClass::NotFound;
        };
        if rec.superseded {
            return ClaimByItemIdClass::NotFound;
        }
        if rec.state.is_terminal() {
            return ClaimByItemIdClass::Terminal;
        }
        if rec.state == ItemState::Leased {
            return ClaimByItemIdClass::Leased;
        }
        if rec.state != ItemState::Pending {
            return ClaimByItemIdClass::NotEligible;
        }
        // Base eligibility: queue pause, not_before, gates (Eligibility Precedence).
        if self.paused {
            return ClaimByItemIdClass::NotEligible;
        }
        if rec.not_before.map(|nb| nb > now).unwrap_or(false) {
            return ClaimByItemIdClass::NotEligible;
        }
        if gate_keys_blocked(&self.blocked_gates, &rec.gate_keys) {
            return ClaimByItemIdClass::NotEligible;
        }
        ClaimByItemIdClass::Claimable
    }

    /// Seed restart item-id counters from this already-materialized projection. Hybrid recovery has just
    /// hydrated memory from SQLite, so this avoids a second full durable-store item scan.
    pub fn observe_item_counters(&self, shard: &QueueKey, counters: &QueueCounters) {
        for id in self.items.keys() {
            counters.observe(shard, *id);
        }
    }

    /// The current `item_version` of `id`, if present (read post-apply to return the bumped version
    /// from an `UpdateFields`).
    pub fn item_version(&self, id: &ItemId) -> Option<u64> {
        self.items.get(id).map(|r| r.item_version)
    }

    /// Pre-commit validation for a finalize batch (commit_locked has no rollback): every targeted item
    /// must be present, not fenced, and currently `Leased`. Returns the structured rejection otherwise,
    /// WITHOUT mutating anything.
    pub fn finalize_validate(&self, outcomes: &[FinalizeOutcome]) -> EngineResult<()> {
        self.validate_leased(outcomes.iter().map(|o| &o.item_id))
    }

    /// Read an opaque non-work side record by key (Snorri recovery/explain read). `None` if unwritten.
    /// Side records live in a map disjoint from the work-item projection, so this never reflects claimable
    /// work and is unaffected by item finalization.
    pub fn side_record(&self, key: &[u8]) -> Option<&Bytes> {
        self.side_records.get(key)
    }

    /// Read the stored instance/state fence for `key` (Snorri authoritative-commit boundary). `None` if the
    /// `instance_key` has never advanced (callers treat absent as the unset value `0`). The fence map is
    /// disjoint from the work-item projection, so this never reflects claimable work.
    pub fn instance_fence(&self, key: &[u8]) -> Option<u64> {
        self.instance_fences.get(key).copied()
    }

    /// Pre-commit validation for a vectorized claimed-work commit (Snorri StateStore boundary, epic
    /// pqueue-2201fd37). Mirrors [`finalize_validate`]'s lease-state precedence (absent → `NotFound`,
    /// fenced → `StaleLease`, terminal → `Terminal`, superseded → `Superseded`, non-leased → `Invalid`) and
    /// ADDS, for each presented [`ClaimRef`], three claim-authority/state-fence checks on a live leased item:
    /// the stored `lease_token` must equal the presented token and the lease must be unexpired (half-open:
    /// expired iff `lease_expires_at < now`), else `StaleLease`; the stored `item_version` must equal
    /// `claim_ref.item_version`, else `Conflict` (the optimistic state fence). Pre-commit: nothing is
    /// appended or mutated on rejection.
    pub fn commit_validate(&self, refs: &[ClaimRef], now: UtcTimestamp) -> EngineResult<()> {
        for r in refs {
            match self.items.get(&r.item_id) {
                None => return Err(EngineError::NotFound),
                Some(rec) if rec.fenced => return Err(EngineError::StaleLease),
                Some(rec) if rec.state.is_terminal() => return Err(EngineError::Terminal),
                Some(rec) if rec.superseded => return Err(EngineError::Superseded),
                Some(rec) if rec.state != ItemState::Leased => {
                    return Err(EngineError::Invalid("item is not leased"));
                }
                Some(rec) => {
                    // Claim authority: the presented lease token must match the stored one (token mismatch is
                    // a stale/forged claim, never the version-fence `Conflict`).
                    if rec.lease_token.as_ref() != Some(&r.lease_token) {
                        return Err(EngineError::StaleLease);
                    }
                    // The lease must be unexpired (half-open, identical to `expired_leases`: expired iff the
                    // deadline is strictly before `now`).
                    if rec.lease_expires_at.is_some_and(|exp| exp < now) {
                        return Err(EngineError::StaleLease);
                    }
                    // Optimistic state fence: the caller's observed version must equal the committed version.
                    if rec.item_version != r.item_version {
                        return Err(EngineError::Conflict);
                    }
                }
            }
        }
        Ok(())
    }

    /// Pre-commit validation for a lease RENEW batch — IDENTICAL rejection semantics to
    /// [`finalize_validate`] (a renew of a fenced/superseded/terminal/non-leased item rejects with the
    /// same structured error, appending nothing), so renew and finalize never diverge.
    pub fn renew_validate(&self, ids: &[ItemId]) -> EngineResult<()> {
        self.validate_leased(ids.iter())
    }

    /// Pre-commit validation for a lease REASSIGN batch (cross-consumer `XCLAIM`) — IDENTICAL rejection
    /// semantics to [`renew_validate`]/[`finalize_validate`]: only a live, non-fenced, non-superseded,
    /// non-terminal leased item may be transferred.
    pub fn reassign_validate(&self, ids: &[ItemId]) -> EngineResult<()> {
        self.validate_leased(ids.iter())
    }

    /// Pre-commit validation for an in-place field/payload update (FAC-1). Legal while the item is live
    /// (Pending OR Leased) and not fenced/superseded; terminal/superseded/absent reject with the same
    /// structured errors as finalize. An `expected_item_version` mismatch rejects with `Conflict`
    /// (optimistic concurrency). Mutates nothing.
    pub fn update_fields_validate(
        &self,
        item_id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        match self.items.get(item_id) {
            None => Err(EngineError::NotFound),
            Some(rec) if rec.fenced => Err(EngineError::StaleLease),
            Some(rec) if rec.state.is_terminal() => Err(EngineError::Terminal),
            Some(rec) if rec.superseded => Err(EngineError::Superseded),
            Some(rec) => match expected_item_version {
                Some(v) if rec.item_version != v => Err(EngineError::Conflict),
                _ => Ok(()),
            },
        }
    }

    // -----------------------------------------------------------------------
    // Secondary-index pre-commit validation + reads (ADR-010 §5.1/§6)
    // -----------------------------------------------------------------------

    /// Pre-commit unique-index validation (ADR-010 §5.1; `commit` has no rollback). Returns
    /// [`EngineError::Conflict`] if inserting/keeping `item_id` with `fields` would land on a UNIQUE
    /// composite key already held by a DIFFERENT item — `exclude` (e.g. the superseded item in an upsert)
    /// is ignored. Mutates nothing.
    pub fn index_validate(
        &self,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        self.index_validate_with_entity(item_id, fields, None, exclude)
    }

    fn index_validate_with_entity(
        &self,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        entity: Option<&Value>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        for (name, key) in self.record_index_keys(fields, entity)? {
            if let Some(SecondaryIndex::Unique(map)) = self.indexes.get(&name)
                && let Some(holder) = map.get(&key)
                && holder != item_id
                && Some(holder) != exclude
            {
                return Err(EngineError::Conflict);
            }
        }
        Ok(())
    }

    /// Pre-commit unique-index validation for a PUSH batch: each item is checked against the existing
    /// index AND against earlier items in the same batch (a violating batch appends nothing).
    pub fn index_validate_push(&self, items: &[PushItem]) -> EngineResult<()> {
        let mut batch: BTreeMap<(String, Vec<u8>), ItemId> = BTreeMap::new();
        for item in items {
            self.index_validate_with_entity(
                &item.item_id,
                &item.fields,
                item.entity_document.as_ref(),
                None,
            )?;
            for (name, key) in
                self.record_index_keys(&item.fields, item.entity_document.as_ref())?
            {
                if matches!(self.indexes.get(&name), Some(SecondaryIndex::Unique(_)))
                    && let Some(prev) = batch.insert((name, key), item.item_id)
                    && prev != item.item_id
                {
                    return Err(EngineError::Conflict);
                }
            }
        }
        Ok(())
    }

    /// Pre-commit unique-index validation for an in-place field update: the item's keys are recomputed
    /// from its CURRENT fields merged with `field_ops`, then checked (its own existing entries do not
    /// conflict). Mutates nothing.
    pub fn index_validate_update(
        &self,
        item_id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
    ) -> EngineResult<()> {
        self.index_validate_update_with_entity(item_id, field_ops, None)
    }

    pub fn index_validate_update_with_entity(
        &self,
        item_id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
        entity: Option<&Value>,
    ) -> EngineResult<()> {
        let rec = self.items.get(item_id).ok_or(EngineError::NotFound)?;
        let mut merged = rec.fields.clone();
        for (k, op) in field_ops {
            match op {
                Some(v) => {
                    merged.insert(k.clone(), v.clone());
                }
                None => {
                    merged.remove(k);
                }
            }
        }
        self.index_validate_with_entity(
            item_id,
            &merged,
            entity.or(rec.entity_document.as_ref()),
            None,
        )
    }

    /// Pre-commit unique-index validation for an upsert replacement: the replacement's keys are checked
    /// against every item EXCEPT the superseded one (which is removed in the same command).
    pub fn index_validate_replace(
        &self,
        superseded_item_id: &ItemId,
        replacement: &PushItem,
    ) -> EngineResult<()> {
        self.index_validate_with_entity(
            &replacement.item_id,
            &replacement.fields,
            replacement.entity_document.as_ref(),
            Some(superseded_item_id),
        )
    }

    /// Build the [`IndexHit`] for `id` from its current record (current `client_item_key`/`item_version`).
    fn index_hit(&self, id: &ItemId) -> Option<IndexHit> {
        self.items.get(id).map(|rec| IndexHit {
            client_item_key: rec.client_item_key(),
            item_id: rec.item_id,
            item_version: rec.item_version,
        })
    }

    /// Resolve and validate a lookup against `index_name`: the index must exist and the supplied key value
    /// count must equal the spec's field count.
    fn index_spec(&self, index_name: &str, key_arity: usize) -> EngineResult<IndexLookupSpec<'_>> {
        if let Some(spec) = self.index_specs.iter().find(|s| s.name == index_name) {
            if key_arity != spec.fields.len() {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            return Ok(IndexLookupSpec::Legacy(spec));
        }
        if let Some(spec) = self.typed_index_specs.iter().find(|s| s.name == index_name) {
            let arity = match &spec.declaration {
                IndexDeclaration::Single(_) => 1,
                IndexDeclaration::Compound(def) => def.fields.len(),
            };
            if key_arity != arity {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            return Ok(IndexLookupSpec::Typed(spec));
        }
        Err(EngineError::Invalid("unknown secondary index"))
    }

    /// Exact composite-key get on a UNIQUE index (ADR-010 §6). `Ok(None)` if no item holds the key;
    /// [`EngineError::Invalid`] if `index_name` is not a unique index on this queue or the key arity is wrong.
    pub fn index_get_unique(
        &self,
        index_name: &str,
        key_values: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        let info = self.index_spec(index_name, key_values.len())?;
        if !info.unique() {
            return Err(EngineError::Invalid("secondary index is not unique"));
        }
        match self.indexes.get(index_name) {
            Some(SecondaryIndex::Unique(map)) => {
                let key = info.lookup_key(key_values)?;
                Ok(map.get(&key).and_then(|id| self.index_hit(id)))
            }
            _ => Err(EngineError::Invalid("secondary index is not unique")),
        }
    }

    /// Exact composite-key lookup on a (unique or non-unique) index (ADR-010 §6). Returns all matching
    /// items ordered by `item_id` ascending; empty if none.
    pub fn index_lookup(
        &self,
        index_name: &str,
        key_values: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        let info = self.index_spec(index_name, key_values.len())?;
        let key = info.lookup_key(key_values)?;
        let ids: Vec<ItemId> = match self.indexes.get(index_name) {
            Some(SecondaryIndex::Unique(map)) => map.get(&key).copied().into_iter().collect(),
            Some(SecondaryIndex::NonUnique(map)) => map
                .get(&key)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default(),
            None => Vec::new(),
        };
        Ok(ids.iter().filter_map(|id| self.index_hit(id)).collect())
    }

    fn typed_range_index(&self, index_name: Option<&str>) -> EngineResult<&QueueIndex> {
        if let Some(name) = index_name {
            return self
                .typed_index_specs
                .iter()
                .find(|spec| spec.name == name)
                .ok_or(EngineError::Invalid("unknown secondary index"));
        }
        self.typed_index_specs
            .first()
            .ok_or(EngineError::Invalid("unknown secondary index"))
    }

    fn range_scan_matches(
        &self,
        spec: &QueueIndex,
        filters: &[QueryFilter],
        row: &RangeScanRow,
    ) -> EngineResult<bool> {
        let fields: Vec<(&str, &IndexType)> = match &spec.declaration {
            IndexDeclaration::Single(def) => vec![(def.field.as_str(), &def.index_type)],
            IndexDeclaration::Compound(def) => def
                .fields
                .iter()
                .map(|field| (field.field.as_str(), &field.index_type))
                .collect(),
        };
        let mut filter_map: BTreeMap<&str, &QueryFilter> = BTreeMap::new();
        for filter in filters {
            filter_map.insert(filter.field.as_str(), filter);
        }
        let mut prefix_len = 0usize;
        for (field_name, index_type) in &fields {
            let Some(filter) = filter_map.get(field_name).copied() else {
                break;
            };
            let typed = typed_value_from_filter_value(&filter.value, index_type)?;
            let Some(value) = row.fields.get(*field_name) else {
                return Ok(false);
            };
            if filter.op != FilterOp::Eq || !typed_value_matches_query(value, &typed) {
                break;
            }
            prefix_len += 1;
        }
        for filter in filters {
            let Some((idx, (_, index_type))) = fields
                .iter()
                .enumerate()
                .find(|(_, (field_name, _))| *field_name == filter.field.as_str())
            else {
                return Err(EngineError::Invalid("unindexed-field"));
            };
            if idx < prefix_len {
                continue;
            }
            let Some(value) = row.fields.get(filter.field.as_str()) else {
                return Ok(false);
            };
            let typed = typed_value_from_filter_value(&filter.value, index_type)?;
            let ord = typed_value_compare(value, &typed)?;
            let ok = match filter.op {
                FilterOp::Eq => ord.is_eq(),
                FilterOp::Gte => ord.is_ge(),
                FilterOp::Gt => ord.is_gt(),
                FilterOp::Lte => ord.is_le(),
                FilterOp::Lt => ord.is_lt(),
            };
            if !ok {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn range_scan_row(
        &self,
        spec: &QueueIndex,
        item_id: ItemId,
        entity: &Value,
    ) -> EngineResult<Option<RangeScanRow>> {
        let mut fields = BTreeMap::new();
        match &spec.declaration {
            IndexDeclaration::Single(def) => {
                let Some(value) = typed_value_for_field(entity, &def.field, &def.index_type)?
                else {
                    return Ok(None);
                };
                fields.insert(def.field.clone(), value);
            }
            IndexDeclaration::Compound(def) => {
                for field in &def.fields {
                    let Some(value) =
                        typed_value_for_field(entity, &field.field, &field.index_type)?
                    else {
                        return Ok(None);
                    };
                    fields.insert(field.field.clone(), value);
                }
            }
        }
        Ok(Some(RangeScanRow { item_id, fields }))
    }

    /// Ordered scan over a declared typed index with stable cursor pagination.
    pub fn range_scan(&self, request: RangeScanRequest) -> EngineResult<RangeScanResponse> {
        const MAX_PAGE_SIZE: u32 = 1_000;
        request
            .validate(MAX_PAGE_SIZE)
            .map_err(|_| EngineError::Invalid("invalid page size"))?;
        let index_name = request.index.as_deref();
        let spec = self.typed_range_index(index_name)?;
        if request.order_by.is_empty() {
            return Err(EngineError::Invalid("range-scan order_by required"));
        }
        let fields: Vec<(&str, &IndexType)> = match &spec.declaration {
            IndexDeclaration::Single(def) => vec![(def.field.as_str(), &def.index_type)],
            IndexDeclaration::Compound(def) => def
                .fields
                .iter()
                .map(|field| (field.field.as_str(), &field.index_type))
                .collect(),
        };
        if request.order_by.iter().any(|order| {
            !fields
                .iter()
                .any(|(field, _)| *field == order.field.as_str())
        }) {
            return Err(EngineError::Invalid("unindexed-field"));
        }
        if let Some(first_direction) = request.order_by.first().map(|o| o.direction)
            && !request
                .order_by
                .iter()
                .all(|o| o.direction == first_direction)
        {
            return Err(EngineError::Invalid(
                "mixed order directions are unsupported",
            ));
        }

        let cursor_state = match &request.cursor {
            Some(cursor) => Some(
                serde_json::from_str::<RangeScanCursorState>(&cursor.0)
                    .map_err(|_| EngineError::Invalid("cursor-invalidated"))?,
            ),
            None => None,
        };
        if let Some(state) = &cursor_state
            && (state.index != spec.name
                || state.filters != request.filters
                || state.order_by != request.order_by)
        {
            return Err(EngineError::Invalid("cursor-invalidated"));
        }

        let mut rows = Vec::new();
        for rec in self.items.values() {
            let Some(entity) = rec.entity_document.as_ref() else {
                continue;
            };
            let Some(row) = self.range_scan_row(spec, rec.item_id, entity)? else {
                continue;
            };
            if !self.range_scan_matches(spec, &request.filters, &row)? {
                continue;
            }
            rows.push(row);
        }
        rows.sort_by(|lhs, rhs| {
            compare_rows(lhs, rhs, &request.order_by).expect("typed order compare")
        });

        let start = if let Some(state) = &cursor_state {
            let anchor = rows
                .iter()
                .position(|row| row.item_id == state.anchor_item_id)
                .ok_or(EngineError::Invalid("cursor-invalidated"))?;
            let current = &rows[anchor];
            let current_values: Vec<TypedValue> = request
                .order_by
                .iter()
                .map(|field| {
                    current
                        .fields
                        .get(&field.field)
                        .cloned()
                        .ok_or(EngineError::Invalid("cursor-invalidated"))
                })
                .collect::<EngineResult<_>>()?;
            if current_values != state.anchor_values {
                return Err(EngineError::Invalid("cursor-invalidated"));
            }
            anchor + 1
        } else {
            0
        };

        let page_size = request.page_size as usize;
        let page_rows = rows
            .iter()
            .skip(start)
            .take(page_size)
            .cloned()
            .collect::<Vec<_>>();
        let next_cursor = if start + page_rows.len() < rows.len() {
            let last = page_rows
                .last()
                .expect("page has at least one row when next_cursor exists");
            let payload = RangeScanCursorState {
                index: spec.name.clone(),
                filters: request.filters.clone(),
                order_by: request.order_by.clone(),
                anchor_item_id: last.item_id,
                anchor_values: request
                    .order_by
                    .iter()
                    .map(|field| {
                        last.fields
                            .get(&field.field)
                            .cloned()
                            .ok_or(EngineError::Invalid("cursor-invalidated"))
                    })
                    .collect::<EngineResult<_>>()?,
                anchor_index_key: None,
            };
            Some(QueryCursor(
                serde_json::to_string(&payload).expect("cursor serialization"),
            ))
        } else {
            None
        };

        Ok(RangeScanResponse {
            rows: page_rows,
            next_cursor,
        })
    }

    /// Group filtered rows by declared index fields or hour/day buckets over a datetime field.
    pub fn grouped_aggregate(
        &self,
        request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        if request.group_by.is_empty() {
            return Err(EngineError::Invalid("group-by required"));
        }
        let spec = self.typed_range_index(request.index.as_deref())?;
        let fields = index_fields(spec);
        for group in &request.group_by {
            let Some((_, index_type)) = fields
                .iter()
                .find(|(field, _)| *field == group.field.as_str())
            else {
                return Err(EngineError::Invalid("unindexed-field"));
            };
            if group.time_bucket.is_some() && !matches!(index_type, IndexType::Datetime) {
                return Err(EngineError::Invalid("unsupported time bucket"));
            }
        }

        let mut groups: BTreeMap<String, (BTreeMap<String, TypedValue>, u64)> = BTreeMap::new();
        for rec in self.items.values() {
            let Some(entity) = rec.entity_document.as_ref() else {
                continue;
            };
            if !matches_filters_on_entity(entity, &request.filters)? {
                continue;
            }

            let mut key = BTreeMap::new();
            let mut skip = false;
            for group in &request.group_by {
                let index_type = index_field_type(spec, &group.field)
                    .ok_or(EngineError::Invalid("unindexed-field"))?;
                let Some(value) = entity_index_value(entity, &group.field, index_type)? else {
                    skip = true;
                    break;
                };
                let value = match (group.time_bucket, value) {
                    (Some(bucket), TypedValue::DateTime(ts)) => {
                        TypedValue::DateTime(truncate_timestamp(ts, bucket))
                    }
                    (Some(_), _) => return Err(EngineError::Invalid("unsupported time bucket")),
                    (None, value) => value,
                };
                key.insert(group.field.clone(), value);
            }
            if skip {
                continue;
            }

            let key_string =
                serde_json::to_string(&key).map_err(|e| EngineError::Storage(e.to_string()))?;
            let is_new_group = !groups.contains_key(&key_string);
            if is_new_group && groups.len() as u32 >= request.max_groups {
                return Err(EngineError::Invalid("aggregate-too-large"));
            }
            let entry = groups.entry(key_string).or_insert((key, 0));
            entry.1 += 1;
        }

        let groups = groups
            .into_values()
            .map(|(key, count)| AggregateGroup { key, count })
            .collect();
        Ok(GroupedAggregateResponse { groups })
    }

    /// Segment filtered rows into caller-declared numeric buckets plus a required null bucket.
    pub fn declared_bucket_segment(
        &self,
        request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        request
            .validate(1_000)
            .map_err(|_| EngineError::Invalid("invalid request"))?;
        let spec = self.typed_range_index(request.index.as_deref())?;
        let index_type = index_field_type(spec, &request.field)
            .ok_or(EngineError::Invalid("unindexed-field"))?;
        if !matches!(index_type, IndexType::Integer | IndexType::Float) {
            return Err(EngineError::Invalid("unsupported bucket field"));
        }

        let mut counts: Vec<u64> = vec![0; request.buckets.len()];
        let mut null_count = 0u64;
        for rec in self.items.values() {
            let Some(entity) = rec.entity_document.as_ref() else {
                continue;
            };
            if !matches_filters_on_entity(entity, &request.filters)? {
                continue;
            }

            let Some(value) = entity_index_value(entity, &request.field, index_type)? else {
                null_count += 1;
                continue;
            };

            let mut matched = false;
            for (idx, bucket) in request.buckets.iter().enumerate() {
                if value_matches_bucket(&value, bucket) {
                    counts[idx] += 1;
                    matched = true;
                    break;
                }
            }
            if !matched {
                continue;
            }
        }

        let mut buckets = request
            .buckets
            .into_iter()
            .zip(counts)
            .map(|(bucket, count)| BucketCount {
                label: bucket.label,
                count,
            })
            .collect::<Vec<_>>();
        buckets.push(BucketCount {
            label: request.null_bucket_label,
            count: null_count,
        });
        Ok(DeclaredBucketSegmentResponse { buckets })
    }

    /// Scan a declared-index predicate and apply typed field updates to every matching record.
    /// Matching records that are not currently pending are treated as conflicts, preserving the
    /// active-lease fence required by API-004 bounded mutation.
    pub fn bounded_mutation(
        &mut self,
        request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationResponse> {
        let plan = self.plan_bounded_mutation(request)?;
        for update in plan.updates {
            self.apply_command(&QueueCommand::UpdateFields(update.command))?;
        }
        Ok(plan.response)
    }

    /// Produce version-fenced update commands without changing the projection. This is the authoritative
    /// planning seam for log-backed compositions: append each command first, then apply it normally.
    pub fn plan_bounded_mutation(
        &self,
        request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationPlan> {
        if request.max_scan_rows == 0 {
            return Err(EngineError::Invalid("invalid page size"));
        }
        let spec = self.typed_range_index(request.index.as_deref())?;

        let mut matches = Vec::new();
        for rec in self.items.values() {
            let Some(entity) = rec.entity_document.as_ref() else {
                continue;
            };
            let Some(row) = self.range_scan_row(spec, rec.item_id, entity)? else {
                continue;
            };
            if !self.range_scan_matches(spec, &request.filters, &row)? {
                continue;
            }
            matches.push((rec.item_id, rec.item_version));
        }
        matches.sort_by_key(|(item_id, _)| *item_id);

        let mut results = Vec::with_capacity(matches.len());
        let mut updates = Vec::new();
        let mut reservations: BTreeMap<(String, Vec<u8>), ItemId> = BTreeMap::new();
        for (item_id, seen_version) in matches {
            let outcome = match self.items.get(&item_id) {
                None => MutationOutcome::NotFound,
                Some(rec)
                    if rec.state != ItemState::Pending
                        || rec.fenced
                        || rec.superseded
                        || rec.state.is_terminal() =>
                {
                    MutationOutcome::Conflict
                }
                Some(rec) if rec.item_version != seen_version => MutationOutcome::Conflict,
                Some(rec) => {
                    let new_entity =
                        merge_entity_document(rec.entity_document.as_ref(), &request.set_fields)?;
                    let new_fields = merge_field_bytes(&rec.fields, &request.set_fields)?;
                    self.index_validate_with_entity(
                        &item_id,
                        &new_fields,
                        Some(&new_entity),
                        None,
                    )?;
                    let new_keys = self.record_index_keys(&new_fields, Some(&new_entity))?;
                    let reservation_conflict = new_keys.iter().any(|(name, key)| {
                        matches!(self.indexes.get(name), Some(SecondaryIndex::Unique(_)))
                            && reservations
                                .get(&(name.clone(), key.clone()))
                                .is_some_and(|holder| *holder != item_id)
                    });
                    if reservation_conflict {
                        MutationOutcome::Conflict
                    } else {
                        for (name, key) in &new_keys {
                            if matches!(self.indexes.get(name), Some(SecondaryIndex::Unique(_))) {
                                reservations.insert((name.clone(), key.clone()), item_id);
                            }
                        }
                        updates.push(BoundedMutationUpdate {
                            command: UpdateFieldsCommand {
                                item_id,
                                field_ops: BTreeMap::new(),
                                payload: PayloadUpdate::Keep,
                                set_priority: ScheduleUpdate::Keep,
                                set_not_before: ScheduleUpdate::Keep,
                                set_entity_document: Some(new_entity),
                                set_fields: Some(new_fields),
                                set_metadata: None,
                                set_gate_keys: None,
                                api001_batch: false,
                            },
                            expected_item_version: seen_version,
                        });
                        MutationOutcome::Updated
                    }
                }
            };
            results.push(MutationResult { item_id, outcome });
        }

        Ok(BoundedMutationPlan {
            response: BoundedMutationResponse { results },
            updates,
        })
    }

    /// Shared "every id is present + Leased + not fenced + not superseded" check used by finalize/renew.
    fn validate_leased<'a>(&self, ids: impl Iterator<Item = &'a ItemId>) -> EngineResult<()> {
        for id in ids {
            match self.items.get(id) {
                None => return Err(EngineError::NotFound),
                Some(rec) if rec.fenced => return Err(EngineError::StaleLease),
                Some(rec) if rec.state.is_terminal() => return Err(EngineError::Terminal),
                // A superseded id (replaced by an upsert) is an explicit `superseded` failure, NOT the
                // generic not-leased `Invalid` (TD-006 §3/§6.5). Check before the not-leased catch-all.
                Some(rec) if rec.superseded => return Err(EngineError::Superseded),
                Some(rec) if rec.state != ItemState::Leased => {
                    return Err(EngineError::Invalid("item is not leased"));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Ids whose lease has expired strictly before `now` (half-open: valid through `lease_expires_at`).
    /// Drives the reclaim tick.
    pub fn expired_leases(&self, now: UtcTimestamp) -> Vec<ItemId> {
        self.ordinary_leases_by_expiry
            .range(..now)
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect()
    }

    fn remove_ordinary_lease(&mut self, expires: UtcTimestamp, id: &ItemId) {
        let remove_bucket = if let Some(ids) = self.ordinary_leases_by_expiry.get_mut(&expires) {
            ids.remove(id);
            ids.is_empty()
        } else {
            false
        };
        if remove_bucket {
            self.ordinary_leases_by_expiry.remove(&expires);
        }
    }

    /// A bounded expiry-ordered slice for composed background maintenance.
    /// The ordering member of `after` compares this projection's queue with
    /// the queue embedded in the global cursor. The returned work count is
    /// deliberately the number of index rows visited, enabling a
    /// scale-independent proof.
    pub(crate) fn expired_leases_after(
        &self,
        now: UtcTimestamp,
        after: Option<(UtcTimestamp, Ordering, &ItemId)>,
        limit: usize,
    ) -> (Vec<(UtcTimestamp, ItemId)>, usize) {
        let mut rows = Vec::with_capacity(limit);
        let mut visited = 0;
        let lower = after.map_or(Unbounded, |(expires, _, _)| Included(expires));
        for (expires, ids) in self.ordinary_leases_by_expiry.range((lower, Excluded(now))) {
            let cursor_ids = after.and_then(|(cursor_expiry, queue_order, cursor_id)| {
                (*expires == cursor_expiry && queue_order == Ordering::Equal).then_some(cursor_id)
            });
            if after.is_some_and(|(cursor_expiry, queue_order, _)| {
                *expires == cursor_expiry && queue_order == Ordering::Less
            }) {
                continue;
            }
            let remaining = limit.saturating_sub(rows.len());
            let mut append = |id: &ItemId| {
                visited += 1;
                rows.push((*expires, *id));
            };
            match cursor_ids {
                Some(cursor_id) => ids
                    .range((Excluded(cursor_id), Unbounded))
                    .take(remaining)
                    .for_each(&mut append),
                None => ids.iter().take(remaining).for_each(&mut append),
            }
            if rows.len() == limit {
                break;
            }
        }
        (rows, visited)
    }
}

#[cfg(test)]
mod tests {
    //! White-box tests over the projection's private state (item_version, log compaction). Behavioral
    //! port-level conformance is exercised against the backends in `fireweed-conformance`.
    use super::*;
    use axon_esf::IndexDef;
    use fireweed_core::{
        CohortId, CohortPolicy, EligibilityPolicy, GateKeyPolicy, IndexSpec, MetadataValue,
        PriorityDirection, PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId,
        RetryPolicy, TenantId, WorkerId,
    };
    use fireweed_engine::{
        AdvanceInstanceFenceCommand, AsyncLogReplayBackend, ChangeRecordSink, ClaimCommand,
        ClaimCompatibility, ClaimPort, ClaimRequest, CohortClaimCommand, CommandChecksum,
        CommandId, ControlPlaneStore, FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort,
        LogStore, PauseQueueCommand, ProjectionStore, PurgeItemsCommand, PushCommand, PushPort,
        PushSpec, QueueKey, QueueMetrics, ReassignLeaseCommand, RenewLeaseCommand, SideRecord,
        UpdateFieldsCommand, WriteSideRecordsCommand, assemble_async_log_replay,
    };
    #[derive(Default)]
    struct NullChangeRecordSink;

    impl ChangeRecordSink for NullChangeRecordSink {
        fn emit(
            &self,
            _shard: &QueueKey,
            _records: &[fireweed_engine::ChangeRecord],
        ) -> EngineResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct ObservedLog {
        logs: FastHashMap<QueueKey, LogData>,
        emission_cursor: FastHashMap<QueueKey, CommandPosition>,
        definitions: BTreeMap<QueueKey, QueueDefinition>,
    }

    impl LogStore for ObservedLog {
        fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
            self.logs.entry(shard.clone()).or_default();
            Ok(())
        }

        fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
            self.logs
                .get(shard)
                .map(LogData::epoch)
                .ok_or(EngineError::NotFound)
        }

        fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
            self.logs
                .get_mut(shard)
                .map(LogData::advance_epoch)
                .ok_or(EngineError::NotFound)
        }

        fn append(
            &mut self,
            shard: &QueueKey,
            commands: &[CommandEnvelope],
            expected_epoch: u64,
        ) -> EngineResult<Vec<CommandPosition>> {
            self.logs
                .get_mut(shard)
                .ok_or(EngineError::NotFound)?
                .append(shard, commands, expected_epoch)
        }

        fn read_from(
            &self,
            shard: &QueueKey,
            from: Option<CommandPosition>,
            limit: usize,
        ) -> EngineResult<fireweed_engine::CommandPage> {
            Ok(self
                .logs
                .get(shard)
                .ok_or(EngineError::NotFound)?
                .read_from(shard, from, limit))
        }

        fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
            Ok(self.logs.get(shard).and_then(LogData::high_water))
        }

        fn set_high_water(
            &mut self,
            shard: &QueueKey,
            position: CommandPosition,
        ) -> EngineResult<()> {
            self.logs
                .get_mut(shard)
                .ok_or(EngineError::NotFound)?
                .set_high_water(position)
        }

        fn write_snapshot(
            &mut self,
            shard: &QueueKey,
            position: CommandPosition,
            snapshot: fireweed_engine::ProjectionSnapshot,
        ) -> EngineResult<fireweed_engine::SnapshotRef> {
            Ok(self
                .logs
                .get_mut(shard)
                .ok_or(EngineError::NotFound)?
                .write_snapshot(shard, position, snapshot))
        }

        fn latest_snapshot(
            &self,
            shard: &QueueKey,
        ) -> EngineResult<Option<fireweed_engine::SnapshotRef>> {
            Ok(self.logs.get(shard).and_then(LogData::latest_snapshot))
        }

        fn read_snapshot(
            &self,
            snapshot_ref: &fireweed_engine::SnapshotRef,
        ) -> EngineResult<fireweed_engine::ProjectionSnapshot> {
            self.logs
                .get(&snapshot_ref.queue)
                .ok_or(EngineError::NotFound)?
                .read_snapshot(snapshot_ref)
        }

        fn emission_cursor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
            Ok(self.emission_cursor.get(shard).cloned())
        }

        fn set_emission_cursor(
            &mut self,
            shard: &QueueKey,
            position: CommandPosition,
        ) -> EngineResult<()> {
            self.emission_cursor.insert(shard.clone(), position);
            Ok(())
        }

        fn persist_definition(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.definitions.insert(key, definition.clone());
            Ok(())
        }

        fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
            Ok(self.definitions.values().cloned().collect())
        }
    }

    type ObservedBackend = AsyncLogReplayBackend<ObservedLog, InMemoryProjection>;

    fn shard() -> QueueKey {
        QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
    }
    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }
    fn model() -> PriorityModel {
        PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        }
    }
    fn iid(s: &str) -> ItemId {
        ItemId::new(s).unwrap()
    }
    fn qdef() -> QueueDefinition {
        qdef_with_emit_change_records(true)
    }

    #[test]
    fn canonical_index_keys_reuse_shared_range_group_and_bucket_semantics() {
        let spec = QueueIndex {
            name: "by_score".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "score".into(),
                index_type: IndexType::Integer,
                unique: false,
            }),
        };
        let mut definition = qdef();
        definition.typed_indexes = vec![spec.clone()];
        let key = |score: i64| match &spec.declaration {
            IndexDeclaration::Single(index) => index
                .index_key(&serde_json::json!({ "score": score }))
                .unwrap()
                .unwrap(),
            IndexDeclaration::Compound(_) => unreachable!(),
        };
        let mut projection = query_projection_from_index_keys(
            &definition,
            Some("by_score"),
            vec![
                (iid("2"), Some(key(2))),
                (iid("1"), Some(key(1))),
                (iid("3"), None),
            ],
        )
        .unwrap();

        let range = projection
            .range_scan(RangeScanRequest {
                index: Some("by_score".into()),
                filters: vec![QueryFilter {
                    field: "score".into(),
                    op: FilterOp::Gte,
                    value: TypedValue::Integer(1),
                }],
                order_by: vec![OrderField {
                    field: "score".into(),
                    direction: SortDirection::Ascending,
                }],
                page_size: 10,
                cursor: None,
            })
            .unwrap();
        assert_eq!(
            range.rows.iter().map(|row| row.item_id).collect::<Vec<_>>(),
            vec![iid("1"), iid("2")]
        );

        let grouped = projection
            .grouped_aggregate(GroupedAggregateRequest {
                index: Some("by_score".into()),
                filters: Vec::new(),
                group_by: vec![fireweed_core::GroupByField {
                    field: "score".into(),
                    time_bucket: None,
                }],
                max_groups: 10,
            })
            .unwrap();
        assert_eq!(grouped.groups.len(), 2);

        let bucketed = projection
            .declared_bucket_segment(DeclaredBucketSegmentRequest {
                index: Some("by_score".into()),
                filters: Vec::new(),
                field: "score".into(),
                buckets: vec![fireweed_core::BucketRule {
                    label: "one".into(),
                    exact: Some(1.0),
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }],
                null_bucket_label: "missing".into(),
            })
            .unwrap();
        assert_eq!(bucketed.buckets[0].count, 1);
        assert_eq!(bucketed.buckets[1].count, 1);

        let mut set_fields = BTreeMap::new();
        set_fields.insert("score".into(), TypedValue::Integer(3));
        let plan = projection
            .plan_bounded_mutation(BoundedMutationRequest {
                index: Some("by_score".into()),
                filters: vec![QueryFilter {
                    field: "score".into(),
                    op: FilterOp::Eq,
                    value: TypedValue::Integer(1),
                }],
                set_fields,
                max_scan_rows: 10,
            })
            .unwrap();
        assert_eq!(plan.updates.len(), 1);
        assert_eq!(projection.items.get(&iid("1")).unwrap().item_version, 1);
        projection
            .apply_command(&QueueCommand::UpdateFields(plan.updates[0].command.clone()))
            .unwrap();
        assert_eq!(projection.items.get(&iid("1")).unwrap().item_version, 2);
    }

    fn qdef_with_emit_change_records(emit_change_records: bool) -> QueueDefinition {
        QueueDefinition {
            tenant_id: shard().tenant_id,
            queue_id: shard().queue_id,
            priority_model: model(),
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 10_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: Some(CohortPolicy::disabled()),
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: vec![IndexSpec {
                name: "by_color".to_string(),
                fields: vec!["color".to_string()],
                unique: false,
            }],
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records,
        }
    }
    fn push_item(id: &str, key: &str, priority: i64) -> PushItem {
        PushItem {
            client_item_key: ClientItemKey::new(key).unwrap(),
            item_id: iid(id),
            priority: Some(PriorityValue::Int64(priority)),
            not_before: None,
            group_key: None,
            max_attempts: 3,
            payload: None,
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            cohort_size: None,
            gate_keys: Vec::new(),
            entity_document: None,
        }
    }
    fn rich_push_item(id: &str, key: &str, priority: i64) -> PushItem {
        let mut fields = BTreeMap::new();
        fields.insert("color".to_string(), Bytes::from_static(b"red"));
        let mut metadata = Metadata::new();
        metadata.insert(
            "origin",
            MetadataValue::String("projection-image".to_string()),
        );
        PushItem {
            payload: Some(Bytes::from_static(b"payload")),
            fields,
            metadata,
            gate_keys: vec!["gate-a".to_string()],
            entity_document: Some(serde_json::json!({"kind":"job","rank":7})),
            ..push_item(id, key, priority)
        }
    }
    fn env(command: QueueCommand) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new("c"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![],
            command,
            checksum: CommandChecksum(0),
            created_at: ts(0),
        }
    }

    fn observed_backend() -> ObservedBackend {
        assemble_async_log_replay(ObservedLog::default(), InMemoryProjection::new(), 0)
            .expect("assemble observed backend")
    }

    fn create_observed_queue_with_definition(
        backend: &ObservedBackend,
        definition: QueueDefinition,
    ) {
        futures::executor::block_on(backend.create_queue(definition))
            .expect("create observed queue");
    }

    fn seed_terminal_item_via_commit(
        backend: &ObservedBackend,
        claim_lease_expires_at: UtcTimestamp,
    ) -> (ItemId, CommandPosition) {
        seed_terminal_item_via_commit_with_definition(backend, qdef(), claim_lease_expires_at)
    }

    fn seed_terminal_item_via_commit_with_definition(
        backend: &ObservedBackend,
        definition: QueueDefinition,
        claim_lease_expires_at: UtcTimestamp,
    ) -> (ItemId, CommandPosition) {
        create_observed_queue_with_definition(backend, definition);
        let shard = shard();
        let pushed = futures::executor::block_on(backend.push(
            &shard,
            vec![PushSpec::default()],
            ts(0),
            None,
        ))
        .expect("push observed terminal item");
        let item_id = pushed[0];

        let claim = backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: claim_lease_expires_at,
            now: ts(1),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        futures::executor::block_on(claim).expect("claim observed terminal item");

        let finalize = backend.finalize(
            &shard,
            vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
            ts(2),
            None,
        );
        futures::executor::block_on(finalize).expect("finalize observed terminal item");

        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(Some(ItemState::Complete)),
            "finalize must leave the item terminal"
        );
        let terminal_position = backend
            .with_log(|log| log.high_water(&shard).unwrap())
            .expect("finalize must advance the durable high-water");
        (item_id, terminal_position)
    }
    fn version_of(proj: &ProjectionData, id: &str) -> u64 {
        proj.items.get(&iid(id)).unwrap().item_version
    }

    fn terminal_record(
        id: &str,
        terminal_at: UtcTimestamp,
        terminal_position: CommandPosition,
    ) -> ItemRecord {
        ItemRecord {
            item_id: iid(id),
            explicit_client_item_key: None,
            priority: None,
            not_before: None,
            eligible_since: ts(0),
            group_key: None,
            cohort_size: None,
            payload: None,
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            gate_keys: Vec::new(),
            entity_document: None,
            state: ItemState::Complete,
            item_version: 2,
            attempt_count: 0,
            max_attempts: 3,
            created_seq: 0,
            lease_token: None,
            lease_expires_at: None,
            lease_is_cohort: false,
            worker_id: None,
            fenced: false,
            superseded: false,
            terminal_at: Some(terminal_at),
            terminal_position: Some(terminal_position),
        }
    }

    #[test]
    fn default_client_keys_are_derived_without_by_key_entries() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        let id = ItemId::mint(1, 7, 42);
        let default_key = default_client_item_key(id);

        projection
            .apply_command(&QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    client_item_key: default_key.clone(),
                    item_id: id,
                    priority: None,
                    not_before: None,
                    group_key: None,
                    max_attempts: 3,
                    payload: None,
                    fields: BTreeMap::new(),
                    metadata: Metadata::default(),
                    cohort_size: None,
                    gate_keys: Vec::new(),
                    entity_document: None,
                }],
            }))
            .unwrap();

        assert!(
            projection.by_key.is_empty(),
            "default client keys must not allocate by_key entries"
        );
        assert!(
            matches!(projection.eligible, EligibilityIndex::Compact(_)),
            "plain FIFO pending items should use the compact eligibility index"
        );
        assert_eq!(projection.lookup_by_key(&default_key), Some(id));
        let live = projection.live_items_by_key(std::slice::from_ref(&default_key));
        assert_eq!(live[0].as_ref().unwrap().client_item_key, default_key);

        let image = projection.to_image(None);
        assert_eq!(image.items[0].client_item_key, default_key);
        let restored = ProjectionData::from_image(&definition, image).unwrap();
        assert!(
            restored.by_key.is_empty(),
            "image import should keep default keys compact"
        );
        assert_eq!(restored.lookup_by_key(&default_key), Some(id));

        projection
            .apply_command(&QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![id],
                force: false,
            }))
            .unwrap();
        assert_eq!(projection.lookup_by_key(&default_key), None);
    }

    #[test]
    fn explicit_client_keys_still_index_and_roundtrip() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        let id = iid("42");
        let explicit_key = ClientItemKey::new("campaign-member-42").unwrap();

        projection
            .apply_command(&QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    client_item_key: explicit_key.clone(),
                    item_id: id,
                    priority: None,
                    not_before: None,
                    group_key: None,
                    max_attempts: 3,
                    payload: None,
                    fields: BTreeMap::new(),
                    metadata: Metadata::default(),
                    cohort_size: None,
                    gate_keys: Vec::new(),
                    entity_document: None,
                }],
            }))
            .unwrap();

        assert_eq!(projection.by_key.len(), 1);
        assert_eq!(projection.lookup_by_key(&explicit_key), Some(id));
        let image = projection.to_image(None);
        let restored = ProjectionData::from_image(&definition, image).unwrap();
        assert_eq!(restored.by_key.len(), 1);
        assert_eq!(restored.lookup_by_key(&explicit_key), Some(id));
    }

    #[test]
    fn rich_eligibility_promotes_compact_fifo_index() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );

        projection
            .apply_command(&QueueCommand::Push(PushCommand {
                items: vec![
                    PushItem {
                        priority: None,
                        ..push_item("1", "1", 10)
                    },
                    PushItem {
                        priority: None,
                        not_before: Some(ts(10)),
                        ..push_item("2", "2", 20)
                    },
                ],
            }))
            .unwrap();

        assert!(
            matches!(projection.eligible, EligibilityIndex::Rich(_)),
            "priority, delay, or group usage should promote to the rich eligibility index"
        );
        assert_eq!(projection.eligible_candidates(ts(0), 10), vec![iid("1")]);
        assert_eq!(
            projection.eligible_candidates(ts(10), 10),
            vec![iid("1"), iid("2")]
        );
    }

    fn push_item_g(id: &str, key: &str, priority: i64, group: &str) -> PushItem {
        PushItem {
            group_key: Some(GroupKey::new(group).unwrap()),
            ..push_item(id, key, priority)
        }
    }

    #[test]
    fn eligibility_key_uses_compact_unpriced_rank() {
        let unpriced = ItemRecord {
            item_id: iid("1"),
            explicit_client_item_key: None,
            priority: None,
            not_before: None,
            eligible_since: ts(0),
            group_key: None,
            cohort_size: None,
            payload: None,
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            gate_keys: Vec::new(),
            entity_document: None,
            state: ItemState::Pending,
            item_version: 1,
            attempt_count: 0,
            max_attempts: 3,
            created_seq: 0,
            lease_token: None,
            lease_expires_at: None,
            lease_is_cohort: false,
            worker_id: None,
            fenced: false,
            superseded: false,
            terminal_at: None,
            terminal_position: None,
        };
        let priced = ItemRecord {
            item_id: iid("2"),
            priority: Some(PriorityValue::Int64(7)),
            created_seq: 1,
            ..unpriced.clone()
        };

        let unpriced_key = elig_key(&unpriced, &model());
        let priced_key = elig_key(&priced, &model());

        assert!(matches!(unpriced_key.rank, EligRank::Unpriced));
        assert!(matches!(priced_key.rank, EligRank::Priced(_)));
        assert!(
            priced_key < unpriced_key,
            "priced work must continue to sort ahead of unpriced FIFO work"
        );
    }

    #[test]
    fn metrics_are_maintained_across_lifecycle_replace_and_purge() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        projection
            .apply_command(&QueueCommand::Push(PushCommand {
                items: vec![push_item("1", "k1", 10), push_item("2", "k2", 20)],
            }))
            .unwrap();
        assert_eq!(
            projection.metrics(),
            QueueMetrics {
                pending: 2,
                resident_terminal_count: 0,
                ..QueueMetrics::default()
            }
        );

        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("1")],
                lease_token: LeaseToken::new("lt").unwrap(),
                lease_expires_at: ts(60),
                worker_id: None,
            }))
            .unwrap();
        assert_eq!(
            projection.metrics(),
            QueueMetrics {
                pending: 1,
                leased: 1,
                resident_terminal_count: 0,
                ..QueueMetrics::default()
            }
        );

        projection
            .apply_command(&QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(iid("1"), FinalizeKind::Complete)],
            }))
            .unwrap();
        assert_eq!(
            projection.metrics(),
            QueueMetrics {
                pending: 1,
                complete: 1,
                resident_terminal_count: 1,
                ..QueueMetrics::default()
            }
        );

        projection
            .apply_command(&QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![iid("1"), iid("2")],
                force: true,
            }))
            .unwrap();
        assert_eq!(projection.metrics(), QueueMetrics::default());
    }

    #[test]
    fn pel_indexes_bound_pages_and_requested_id_reads() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        let items: Vec<_> = (1..=1_000)
            .map(|id| push_item(&id.to_string(), &format!("k{id}"), id))
            .collect();
        projection
            .apply_command(&QueueCommand::Push(PushCommand { items }))
            .unwrap();
        let first = LeaseToken::new("consumer-a").unwrap();
        let second = LeaseToken::new("consumer-b").unwrap();
        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: (1..=1_000).map(ItemId::from_u64).collect(),
                lease_token: first.clone(),
                lease_expires_at: ts(60),
                worker_id: None,
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: vec![iid("500"), iid("900")],
                lease_token: second.clone(),
                lease_expires_at: ts(120),
            }))
            .unwrap();

        let page = projection.pending_page(Some(iid("498")), 3);
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.item_id)
                .collect::<Vec<_>>(),
            vec![iid("498"), iid("499"), iid("500")]
        );
        assert_eq!(page.next, Some(iid("501")));
        assert_eq!(page.entries.len(), 3, "page output is request-bounded");

        let consumer_page = projection.pending_range(None, None, Some(&second), 1);
        assert_eq!(consumer_page.len(), 1);
        assert_eq!(consumer_page[0].item_id, iid("500"));
        let requested = projection.pending_by_ids(&[iid("900"), iid("1"), iid("4040")]);
        assert_eq!(
            requested
                .iter()
                .map(|entry| entry.item_id)
                .collect::<Vec<_>>(),
            vec![iid("900"), iid("1")]
        );
        let summary = projection.pending_summary();
        assert_eq!(summary.count, 1_000);
        assert_eq!(summary.min_id, Some(iid("1")));
        assert_eq!(summary.max_id, Some(iid("1000")));
        assert_eq!(
            summary.consumers,
            vec![(first, 998), (second, 2)],
            "consumer counts come from the maintained set index"
        );
    }

    #[test]
    fn ordinary_expiry_index_tracks_every_lease_lifecycle_transition() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        projection
            .apply_command(&QueueCommand::Push(PushCommand {
                items: (1..=5)
                    .map(|id| push_item(&id.to_string(), &format!("k{id}"), id))
                    .collect(),
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("1")],
                lease_token: LeaseToken::new("ordinary-1").unwrap(),
                lease_expires_at: ts(10),
                worker_id: None,
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("2")],
                lease_token: LeaseToken::new("ordinary-2").unwrap(),
                lease_expires_at: ts(100),
                worker_id: None,
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::CohortClaim(CohortClaimCommand {
                cohort_id: CohortId::new("cohort").unwrap(),
                item_ids: vec![iid("3")],
                lease_token: LeaseToken::new("cohort-lease").unwrap(),
                lease_expires_at: ts(5),
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("4")],
                lease_token: LeaseToken::new("ordinary-4").unwrap(),
                lease_expires_at: ts(30),
                worker_id: None,
            }))
            .unwrap();
        assert_eq!(projection.expired_leases(ts(50)), vec![iid("1"), iid("4")]);

        projection
            .apply_command(&QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![iid("1")],
                lease_expires_at: ts(60),
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: vec![iid("2")],
                lease_token: LeaseToken::new("ordinary-2b").unwrap(),
                lease_expires_at: ts(20),
            }))
            .unwrap();
        assert_eq!(projection.expired_leases(ts(50)), vec![iid("2"), iid("4")]);

        projection
            .apply_command(&QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(iid("2"), FinalizeKind::Complete)],
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::LeaseExpired(
                fireweed_engine::LeaseExpiredCommand {
                    item_ids: vec![iid("4")],
                },
            ))
            .unwrap();
        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("5")],
                lease_token: LeaseToken::new("ordinary-5").unwrap(),
                lease_expires_at: ts(15),
                worker_id: None,
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![iid("5")],
                force: true,
            }))
            .unwrap();
        assert!(projection.expired_leases(ts(50)).is_empty());

        let image = projection.to_image(None);
        let restored = ProjectionData::from_image(&definition, image).unwrap();
        assert_eq!(restored.expired_leases(ts(70)), vec![iid("1")]);
        assert!(!restored.expired_leases(ts(200)).contains(&iid("3")));
    }

    #[test]
    fn ten_million_analog_expiry_page_work_ignores_nonexpired_residents() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        projection
            .ordinary_leases_by_expiry
            .entry(ts(1))
            .or_default()
            .extend([ItemId::from_u64(1), ItemId::from_u64(2)]);
        let (_, baseline_work) = projection.expired_leases_after(ts(10), None, 2);

        // Each synthetic row stands for one hundred resident leases in the 10M proof scale. The
        // work counter measures index rows actually visited, not wall-clock host performance.
        projection
            .ordinary_leases_by_expiry
            .entry(ts(1_000))
            .or_default()
            .extend((10_000..110_000).map(ItemId::from_u64));
        let (rows, scaled_work) = projection.expired_leases_after(ts(10), None, 2);

        assert_eq!(rows.len(), 2);
        assert_eq!(baseline_work, 2);
        assert_eq!(scaled_work, baseline_work);
    }

    #[test]
    fn expiry_cursor_is_exact_across_queues_sharing_a_deadline() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        projection
            .ordinary_leases_by_expiry
            .entry(ts(10))
            .or_default()
            .extend([iid("1"), iid("2")]);
        projection
            .ordinary_leases_by_expiry
            .entry(ts(20))
            .or_default()
            .insert(iid("3"));

        let (queue_after_cursor, _) = projection.expired_leases_after(
            ts(30),
            Some((ts(10), Ordering::Greater, &iid("999"))),
            8,
        );
        assert_eq!(
            queue_after_cursor,
            vec![(ts(10), iid("1")), (ts(10), iid("2")), (ts(20), iid("3"))]
        );
        let (same_queue, _) =
            projection.expired_leases_after(ts(30), Some((ts(10), Ordering::Equal, &iid("1"))), 8);
        assert_eq!(same_queue, vec![(ts(10), iid("2")), (ts(20), iid("3"))]);
        let (queue_before_cursor, _) =
            projection.expired_leases_after(ts(30), Some((ts(10), Ordering::Less, &iid("0"))), 8);
        assert_eq!(queue_before_cursor, vec![(ts(20), iid("3"))]);
    }

    #[test]
    fn reap_waits_for_emission() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        let item_id = iid("1");
        let terminal_at = ts(0);
        let terminal_position = CommandPosition::new(shard(), 0, 3);
        projection.items.insert(
            item_id,
            terminal_record("1", terminal_at, terminal_position.clone()),
        );
        projection.metrics.complete = 1;
        projection.metrics.resident_terminal_count = 1;

        let now_before_retention = ts(30);
        let cursor_passed = CommandPosition::new(shard(), 0, 3);
        assert!(
            projection
                .reap_terminal_items(
                    now_before_retention,
                    definition.terminal_retention_ms,
                    true,
                    Some(&cursor_passed),
                )
                .is_empty()
        );
        assert!(projection.items.contains_key(&item_id));

        let now = ts(90);
        let cursor_behind = CommandPosition::new(shard(), 0, 2);
        assert!(
            projection
                .reap_terminal_items(
                    now,
                    definition.terminal_retention_ms,
                    true,
                    Some(&cursor_behind),
                )
                .is_empty()
        );
        assert!(projection.items.contains_key(&item_id));
        assert_eq!(
            projection.terminal_emission_metrics(now, true, Some(&cursor_behind)),
            TerminalEmissionMetrics {
                resident_terminal_count: 1,
                emission_lag_commands: 1,
                emission_oldest_unemitted_age_ms: 90_000,
            }
        );

        assert_eq!(
            projection.terminal_emission_metrics(now, true, Some(&cursor_passed)),
            TerminalEmissionMetrics {
                resident_terminal_count: 1,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            }
        );

        let reaped = projection.reap_terminal_items(
            now,
            definition.terminal_retention_ms,
            true,
            Some(&cursor_passed),
        );
        assert_eq!(reaped, vec![item_id]);
        assert!(!projection.items.contains_key(&item_id));
        assert_eq!(
            projection.terminal_emission_metrics(now, true, Some(&cursor_passed)),
            TerminalEmissionMetrics {
                resident_terminal_count: 0,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            }
        );

        println!(
            "TD008_OBSERVED reap_waits_for_emission reaped=1 lag_before=1 lag_after=0 oldest_unemitted_age_ms=90000"
        );
    }

    #[test]
    fn reap_ignores_emission_when_disabled() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
        let item_id = iid("1");
        let terminal_at = ts(0);
        let terminal_position = CommandPosition::new(shard(), 0, 7);
        projection.items.insert(
            item_id,
            terminal_record("1", terminal_at, terminal_position),
        );
        projection.metrics.complete = 1;
        projection.metrics.resident_terminal_count = 1;

        let now = ts(90);
        let cursor_behind = CommandPosition::new(shard(), 0, 2);
        assert_eq!(
            projection.terminal_emission_metrics(now, false, Some(&cursor_behind)),
            TerminalEmissionMetrics {
                resident_terminal_count: 1,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            }
        );

        let reaped = projection.reap_terminal_items(
            now,
            definition.terminal_retention_ms,
            false,
            Some(&cursor_behind),
        );
        assert_eq!(reaped, vec![item_id]);
        assert!(!projection.items.contains_key(&item_id));
        assert_eq!(projection.metrics(), QueueMetrics::default());

        println!(
            "TD008_OBSERVED reap_ignores_emission_when_disabled reaped=1 emit_change_records=false"
        );
    }

    #[test]
    fn td008_observed_terminal_reap_frontier_run() {
        let backend = observed_backend();
        let shard = shard();
        let definition = qdef();
        let (item_id, terminal_position) = seed_terminal_item_via_commit(&backend, ts(120));
        let sink = NullChangeRecordSink;
        let expired_now = ts(63);

        assert_eq!(
            backend.with_projection(|projection| projection.metrics(&shard)),
            Ok(QueueMetrics {
                pending: 0,
                leased: 0,
                complete: 1,
                failed: 0,
                resident_terminal_count: 1,
            })
        );

        assert_eq!(
            backend.reap_terminal_items(
                &shard,
                expired_now,
                definition.terminal_retention_ms,
                true,
            ),
            Ok(0),
            "emit-enabled queues must stay fail-closed until a durable emission cursor exists"
        );
        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(Some(ItemState::Complete))
        );

        backend
            .emit_change_record_tail(&shard, &sink, 2, ts(61), None)
            .unwrap();
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            Some(CommandPosition::new(shard.clone(), 0, 1)),
            "partial emission must advance the cursor only to the last emitted command"
        );
        assert_eq!(
            backend.reap_terminal_items(
                &shard,
                expired_now,
                definition.terminal_retention_ms,
                true,
            ),
            Ok(0),
            "the terminal item must survive until the cursor reaches its terminal position"
        );
        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(Some(ItemState::Complete))
        );

        backend
            .emit_change_record_tail(&shard, &sink, 1, ts(61), None)
            .unwrap();
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            Some(terminal_position.clone()),
            "the durable cursor must advance to the terminal command position"
        );
        assert_eq!(
            backend.reap_terminal_items(
                &shard,
                expired_now,
                definition.terminal_retention_ms,
                true,
            ),
            Ok(1),
            "once the frontier is satisfied, reap should remove the terminal item immediately"
        );
        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(None)
        );
    }

    #[test]
    fn td008_observed_terminal_reap_no_premature_deletion() {
        let backend = observed_backend();
        let shard = shard();
        let definition = qdef();
        let (item_id, terminal_position) = seed_terminal_item_via_commit(&backend, ts(120));
        let sink = NullChangeRecordSink;
        let not_yet_expired = ts(30);
        let expired_now = ts(63);

        backend
            .emit_change_record_tail(&shard, &sink, 8, ts(10), None)
            .unwrap();
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            Some(terminal_position.clone()),
            "the committed terminal command should be fully emitted before we test retention"
        );

        assert_eq!(
            backend.reap_terminal_items(
                &shard,
                not_yet_expired,
                definition.terminal_retention_ms,
                true,
            ),
            Ok(0),
            "retention must keep the terminal row alive even after the frontier is satisfied"
        );
        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(Some(ItemState::Complete))
        );

        assert_eq!(
            backend.reap_terminal_items(
                &shard,
                expired_now,
                definition.terminal_retention_ms,
                true,
            ),
            Ok(1),
            "the row should be reaped immediately after retention expires"
        );
        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(None)
        );
    }

    #[test]
    fn td008_observed_retention_only_reap_run() {
        let backend = observed_backend();
        let shard = shard();
        let definition = qdef_with_emit_change_records(false);
        let (item_id, terminal_position) =
            seed_terminal_item_via_commit_with_definition(&backend, definition.clone(), ts(120));
        let expired_now = ts(63);

        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            None,
            "opted-out queues should not require a durable emission cursor"
        );

        assert_eq!(
            backend.reap_terminal_items(
                &shard,
                expired_now,
                definition.terminal_retention_ms,
                definition.emit_change_records,
            ),
            Ok(1),
            "retention alone must be sufficient to reap opted-out terminal items"
        );
        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(None)
        );
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            None,
            "reaping an opted-out queue must not advance or require emission-cursor state"
        );
        assert!(
            terminal_position.sequence > 0,
            "the observed commit path should produce a terminal command position"
        );
    }

    #[test]
    fn td008_observed_retention_only_ignores_emission_cursor() {
        let backend = observed_backend();
        let shard = shard();
        let definition = qdef_with_emit_change_records(false);
        let (item_id, terminal_position) =
            seed_terminal_item_via_commit_with_definition(&backend, definition.clone(), ts(120));
        let sink = NullChangeRecordSink;
        let expired_now = ts(63);
        assert!(
            terminal_position.sequence >= 2,
            "the observed commit path should produce a terminal command position"
        );
        assert_eq!(
            backend
                .emit_change_record_tail(&shard, &sink, 1, ts(61), None)
                .unwrap(),
            1
        );

        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            Some(CommandPosition::new(
                shard.clone(),
                terminal_position.backend_epoch,
                terminal_position.sequence - 2,
            ))
        );

        assert_eq!(
            backend.reap_terminal_items(
                &shard,
                expired_now,
                definition.terminal_retention_ms,
                definition.emit_change_records,
            ),
            Ok(1),
            "retention-based reap must succeed even when emission-cursor state is behind"
        );
        assert_eq!(
            backend.with_projection(|projection| projection.item_state(&shard, &item_id)),
            Ok(None)
        );
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            Some(CommandPosition::new(
                shard,
                terminal_position.backend_epoch,
                terminal_position.sequence - 2,
            ))
        );
    }

    #[test]
    fn projection_image_roundtrips_full_projection_state() {
        let mut definition = qdef();
        definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
        definition.eligibility_policy.max_gate_keys_per_item = Some(4);
        definition.eligibility_policy.max_gates_per_request = Some(4);
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        )
        .with_eligibility_policy(&definition.eligibility_policy);
        let item_id = iid("1");
        let lease_token = LeaseToken::new("lease-1").unwrap();

        projection
            .apply_command(&QueueCommand::Push(PushCommand {
                items: vec![rich_push_item("1", "k1", 10)],
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item_id],
                lease_token: lease_token.clone(),
                lease_expires_at: ts(60),
                worker_id: None,
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::PauseQueue(PauseQueueCommand::default()))
            .unwrap();
        projection
            .apply_command(&QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: vec![SideRecord {
                    key: b"side-key".to_vec(),
                    payload: Bytes::from_static(b"side-payload"),
                }],
            }))
            .unwrap();
        projection
            .apply_command(&QueueCommand::AdvanceInstanceFence(
                AdvanceInstanceFenceCommand {
                    instance_key: b"instance".to_vec(),
                    expected: 0,
                    next: 9,
                },
            ))
            .unwrap();

        let high_water = Some(CommandPosition::new(shard(), 2, 42));
        let image = projection.to_image(high_water.clone());
        let restored = ProjectionData::from_image(&definition, image.clone()).unwrap();

        assert_eq!(image.high_water, high_water);
        assert_eq!(restored.to_image(high_water.clone()), image);
        assert!(restored.is_paused());
        assert_eq!(restored.metrics().leased, 1);
        assert_eq!(restored.pending_leases()[0].lease_token, lease_token);
        assert_eq!(
            restored.side_record(b"side-key"),
            Some(&Bytes::from_static(b"side-payload"))
        );
        assert_eq!(restored.instance_fence(b"instance"), Some(9));
        assert_eq!(
            restored
                .index_lookup("by_color", &[b"red".to_vec()])
                .unwrap()[0]
                .item_id,
            item_id
        );
        let live = restored.live_items_by_key(&[ClientItemKey::new("k1").unwrap()]);
        let item = live[0].as_ref().unwrap();
        assert_eq!(item.fields["color"], Bytes::from_static(b"red"));
        assert_eq!(item.payload, Some(Bytes::from_static(b"payload")));
    }

    /// Bounded-relaxed claim selection (TP-003 INV-6 + INV-4). A deterministic eligible set with
    /// group-locality keys + a rank-error bound: assert the delivered order is genuinely reordered
    /// (NON-ZERO rank error) yet every item's displacement from its strict-priority position stays
    /// `<= bound` (INV-6), and that strict mode / a 0 bound still picks the exact strict head order.
    #[test]
    fn bounded_relaxed_selection_reorders_within_the_rank_bound() {
        let bound = 2u32;
        // Strict (ascending) order by priority is items 1..=5; groups make locality reorder within a
        // window of `bound + 1`. "a" sorts before "z", so the "a"-group items get batched ahead.
        let pushes = vec![
            push_item_g("1", "k1", 1, "z"),
            push_item_g("2", "k2", 2, "a"),
            push_item_g("3", "k3", 3, "a"),
            push_item_g("4", "k4", 4, "z"),
            push_item_g("5", "k5", 5, "z"),
        ];

        let build = |mode: OrderingMode, b: u32| {
            let mut log = LogData::default();
            let mut proj = ProjectionData::new(model(), mode, b, RecurrencePolicy::default(), &[]);
            for p in &pushes {
                commit(
                    &mut log,
                    &mut proj,
                    &shard(),
                    env(QueueCommand::Push(PushCommand {
                        items: vec![p.clone()],
                    })),
                    None,
                )
                .unwrap();
            }
            proj
        };

        // Strict reference order (what the rank error is measured against).
        let strict = build(OrderingMode::Strict, 0);
        let strict_order = strict.eligible_candidates(ts(1_000), 100);
        assert_eq!(
            strict_order,
            vec![iid("1"), iid("2"), iid("3"), iid("4"), iid("5")],
            "strict selects exact priority-ascending head order"
        );

        // A BoundedRelaxed queue with a 0 bound is byte-for-byte strict (no regression).
        let zero = build(OrderingMode::BoundedRelaxed, 0);
        assert_eq!(
            zero.eligible_candidates(ts(1_000), 100),
            strict_order,
            "a 0 bound is strict-equivalent"
        );

        // Bounded-relaxed with bound=2: locality reorders within the window.
        let relaxed = build(OrderingMode::BoundedRelaxed, bound);
        let order = relaxed.eligible_candidates(ts(1_000), 100);
        assert_eq!(order.len(), 5, "INV-4: every eligible item is selected");
        assert_ne!(order, strict_order, "selection genuinely relaxed");

        // Measure rank error = max |delivered_pos - strict_pos| over all items.
        let strict_pos: std::collections::HashMap<ItemId, usize> = strict_order
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i))
            .collect();
        let rank_error = order
            .iter()
            .enumerate()
            .map(|(delivered, id)| (delivered as i64 - strict_pos[id] as i64).unsigned_abs())
            .max()
            .unwrap();
        assert!(rank_error > 0, "INV-6: relaxation observed (non-zero)");
        assert!(
            rank_error <= bound as u64,
            "INV-6: rank error {rank_error} exceeds bound {bound}"
        );
    }

    #[test]
    fn eligibility_key_rekeys_when_not_before_changes() {
        let mut log = LogData::default();
        let mut proj = ProjectionData::new(
            model(),
            OrderingMode::Strict,
            0,
            RecurrencePolicy::default(),
            &[],
        );
        for p in [push_item("1", "k1", 1), push_item("2", "k2", 2)] {
            commit(
                &mut log,
                &mut proj,
                &shard(),
                env(QueueCommand::Push(PushCommand { items: vec![p] })),
                None,
            )
            .unwrap();
        }

        commit(
            &mut log,
            &mut proj,
            &shard(),
            env(QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id: iid("1"),
                field_ops: BTreeMap::new(),
                payload: PayloadUpdate::Keep,
                set_priority: ScheduleUpdate::Keep,
                set_not_before: ScheduleUpdate::Set(Some(ts(5_000))),
                set_entity_document: None,
                set_fields: None,
                set_metadata: None,
                set_gate_keys: None,
                api001_batch: false,
            })),
            None,
        )
        .unwrap();
        assert_eq!(proj.eligible_candidates(ts(1_000), 10), vec![iid("2")]);
        assert_eq!(
            proj.eligible_candidates(ts(6_000), 10),
            vec![iid("1"), iid("2")]
        );

        let lease = LeaseToken::new("lease-1").unwrap();
        commit(
            &mut log,
            &mut proj,
            &shard(),
            env(QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("1")],
                lease_token: lease,
                lease_expires_at: ts(7_000),
                worker_id: None,
            })),
            None,
        )
        .unwrap();
        commit(
            &mut log,
            &mut proj,
            &shard(),
            env(QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome {
                    item_id: iid("1"),
                    kind: FinalizeKind::Retry,
                    applied_state: Some(ItemState::Pending),
                    not_before: Some(ts(10_000)),
                }],
            })),
            None,
        )
        .unwrap();

        assert_eq!(proj.eligible_candidates(ts(9_000), 10), vec![iid("2")]);
        assert_eq!(
            proj.eligible_candidates(ts(11_000), 10),
            vec![iid("1"), iid("2")]
        );
    }

    #[test]
    fn api001_update_command_applies_full_replacements() {
        let mut log = LogData::default();
        let mut proj = ProjectionData::new(
            model(),
            OrderingMode::Strict,
            0,
            RecurrencePolicy::default(),
            &[],
        );
        commit(
            &mut log,
            &mut proj,
            &shard(),
            env(QueueCommand::Push(PushCommand {
                items: vec![push_item("1", "k1", 1)],
            })),
            None,
        )
        .unwrap();

        let mut fields = BTreeMap::new();
        fields.insert("replacement".into(), Bytes::from_static(b"value"));
        let mut metadata = Metadata::new();
        metadata.insert("source", MetadataValue::String("batch".into()));
        commit(
            &mut log,
            &mut proj,
            &shard(),
            env(QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id: iid("1"),
                field_ops: BTreeMap::new(),
                payload: PayloadUpdate::Set(Some(Bytes::from_static(b"payload"))),
                set_priority: ScheduleUpdate::Keep,
                set_not_before: ScheduleUpdate::Keep,
                set_entity_document: None,
                set_fields: Some(fields.clone()),
                set_metadata: Some(metadata.clone()),
                set_gate_keys: Some(vec!["gate-a".into()]),
                api001_batch: true,
            })),
            None,
        )
        .unwrap();

        let row = proj
            .live_items_by_key(&[ClientItemKey::new("k1").unwrap()])
            .pop()
            .flatten()
            .unwrap();
        assert_eq!(row.fields, fields);
        assert_eq!(row.payload.as_deref(), Some(&b"payload"[..]));
        assert_eq!(row.item_version, 2);
        let stored = proj.items.get(&iid("1")).unwrap();
        assert_eq!(stored.metadata, metadata);
        assert_eq!(stored.gate_keys, vec!["gate-a"]);
    }

    #[test]
    fn projection_image_does_not_rehydrate_superseded_pending_as_eligible() {
        let definition = qdef();
        let mut image = ProjectionImage {
            high_water: None,
            paused: false,
            pause_drain_intake: false,
            blocked_gates: BTreeSet::new(),
            next_seq: 2,
            items: vec![
                ProjectionImageItem {
                    item_id: iid("10"),
                    client_item_key: ClientItemKey::new("k-old").unwrap(),
                    priority: Some(PriorityValue::Int64(1)),
                    not_before: None,
                    eligible_since: Some(ts(0)),
                    group_key: None,
                    cohort_size: None,
                    payload: None,
                    fields: BTreeMap::new(),
                    metadata: Metadata::default(),
                    gate_keys: Vec::new(),
                    entity_document: None,
                    state: ItemState::Pending,
                    item_version: 1,
                    attempt_count: 0,
                    max_attempts: 3,
                    created_seq: 0,
                    lease_token: None,
                    lease_expires_at: None,
                    lease_is_cohort: false,
                    worker_id: None,
                    fenced: false,
                    superseded: true,
                    terminal_at: None,
                    terminal_position: None,
                },
                ProjectionImageItem {
                    item_id: iid("11"),
                    client_item_key: ClientItemKey::new("k-live").unwrap(),
                    priority: Some(PriorityValue::Int64(2)),
                    not_before: None,
                    eligible_since: Some(ts(0)),
                    group_key: None,
                    cohort_size: None,
                    payload: None,
                    fields: BTreeMap::new(),
                    metadata: Metadata::default(),
                    gate_keys: Vec::new(),
                    entity_document: None,
                    state: ItemState::Pending,
                    item_version: 1,
                    attempt_count: 0,
                    max_attempts: 3,
                    created_seq: 1,
                    lease_token: None,
                    lease_expires_at: None,
                    lease_is_cohort: false,
                    worker_id: None,
                    fenced: false,
                    superseded: false,
                    terminal_at: None,
                    terminal_position: None,
                },
            ],
            side_records: BTreeMap::new(),
            instance_fences: BTreeMap::new(),
            metrics: QueueMetrics::default(),
        };
        image
            .items
            .sort_by_key(|item| (item.created_seq, item.item_id));

        let restored = ProjectionData::from_image(&definition, image).unwrap();
        assert_eq!(restored.eligible_candidates(ts(1_000), 10), vec![iid("11")]);
        assert!(restored.live_items_by_key(&[ClientItemKey::new("k-old").unwrap()])[0].is_none());
    }

    /// BQ-20: an epoch advance fences future appends to the new epoch but does NOT rewind the log; a
    /// position replayed across the boundary carries its TRUE per-entry epoch (not a relabel to the
    /// current one), so `read_from` is consistent with the durably-stamped position and the high-water
    /// guard never false-regresses.
    #[test]
    fn read_from_carries_true_per_entry_epoch_across_an_advance() {
        let mut log = LogData::default();
        // Two appends at epoch 0.
        log.append(
            &shard(),
            &[env(QueueCommand::PauseQueue(PauseQueueCommand::default()))],
            0,
        )
        .unwrap();
        log.append(&shard(), &[env(QueueCommand::ResumeQueue)], 0)
            .unwrap();
        // Acquire E+1 (durable fence), then one append at epoch 1.
        assert_eq!(log.advance_epoch(), 1);
        let pos = log
            .append(
                &shard(),
                &[env(QueueCommand::PauseQueue(PauseQueueCommand::default()))],
                1,
            )
            .unwrap();
        // A stale epoch-0 append is now fenced (the seq counter is unchanged — no rewind).
        assert_eq!(
            log.append(&shard(), &[env(QueueCommand::ResumeQueue)], 0),
            Err(EngineError::EpochFenced)
        );

        // read_from labels each entry with the epoch it was written under, not the current epoch.
        let page = log.read_from(&shard(), None, 10);
        let epochs: Vec<u64> = page.entries.iter().map(|(p, _)| p.backend_epoch).collect();
        let seqs: Vec<u64> = page.entries.iter().map(|(p, _)| p.sequence).collect();
        assert_eq!(
            epochs,
            vec![0, 0, 1],
            "historical entries keep their true epoch"
        );
        assert_eq!(
            seqs,
            vec![0, 1, 2],
            "seq is continuous across the epoch boundary"
        );
        // The durably-returned append position matches what read_from reconstructs (epoch 1, seq 2).
        assert_eq!((pos[0].backend_epoch, pos[0].sequence), (1, 2));
        // The high-water (epoch 1, seq 2) does NOT regress against the replayed last position.
        let last = &page.entries.last().unwrap().0;
        assert_eq!(log.high_water().as_ref(), Some(last));
    }

    #[test]
    fn item_version_is_monotonic_per_item() {
        let sk = shard();
        let mut log = LogData::default();
        let mut proj = ProjectionData::new(
            model(),
            OrderingMode::Strict,
            0,
            RecurrencePolicy::default(),
            &[],
        );

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Push(PushCommand {
                items: vec![push_item("1", "ka", 5)],
            })),
            None,
        )
        .unwrap();
        let v0 = version_of(&proj, "1"); // push -> 1

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("1")],
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: ts(500),
                worker_id: None,
            })),
            None,
        )
        .unwrap();
        let v1 = version_of(&proj, "1"); // claim -> 2

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![iid("1")],
                lease_expires_at: ts(600),
            })),
            None,
        )
        .unwrap();
        let v2 = version_of(&proj, "1"); // renew -> 3

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(iid("1"), FinalizeKind::Complete)],
            })),
            None,
        )
        .unwrap();
        let v3 = version_of(&proj, "1"); // finalize -> 4

        assert_eq!(
            (v0, v1, v2, v3),
            (1, 2, 3, 4),
            "item_version bumps exactly once per committed mutation (API-001)"
        );
    }

    #[test]
    fn high_water_survives_log_compaction() {
        let sk = shard();
        let mut log = LogData::default();
        let mut proj = ProjectionData::new(
            model(),
            OrderingMode::Strict,
            0,
            RecurrencePolicy::default(),
            &[],
        );
        for p in [10_i64, 20, 30] {
            commit(
                &mut log,
                &mut proj,
                &sk,
                env(QueueCommand::Push(PushCommand {
                    items: vec![push_item(&format!("{p}"), &format!("k{p}"), p)],
                })),
                None,
            )
            .unwrap();
        }
        let before = log.high_water().unwrap();
        // Simulate log compaction: drop the stored entries (retention). The persisted high-water is a
        // separate field, NOT recomputed from entries.len() — so it MUST be unchanged (TD-007 §4).
        log.entries.clear();
        let after = log.high_water().unwrap();
        assert_eq!(
            before, after,
            "high-water is persisted, not recomputed from a compacted log"
        );
        assert_eq!(
            after.sequence, 2,
            "3 commits -> seq 2 (would be 0 if recomputed from empty entries)"
        );
    }
}
