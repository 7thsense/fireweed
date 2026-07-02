#![forbid(unsafe_code)]
//! # pqueue-projection
//!
//! The priority-ordered projection state machine ([`ProjectionData`]) and per-shard command log
//! ([`LogData`]), as pure in-memory types with no I/O. This is the **domain materialized view**: apply
//! rules, the eligibility index, lifecycle transitions, `item_version` bumps, lease/fence fields, and
//! the read queries the ports expose. Driven adapters (memory/sqlite/postgres) own only the
//! *persistence* of these, so every backend shares one correct projection rather than re-implementing
//! the apply/eligibility/lease logic.
//!
//! `LogData` and `ProjectionData` are kept SEPARATE (not bundled) so a backend can hold them in
//! disjoint maps and hand out `&mut dyn LogWriter` + `&mut dyn ProjectionWriter` simultaneously for the
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

mod compose_impls;
pub use compose_impls::{InMemoryProjection, MemoryLog};

use bytes::Bytes;
use pqueue_core::{
    AggregateGroup, BoundedMutationRequest, BoundedMutationResponse, BucketCount, ClientItemKey,
    DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, FilterOp, GroupKey,
    GroupedAggregateRequest, GroupedAggregateResponse, IndexDeclaration, IndexSpec, IndexType,
    ItemEvent, ItemId, ItemState, LeaseToken, Metadata, MutationOutcome, MutationResult,
    OrderField, OrderingMode, PriorityModel, PriorityValue, QueryCursor, QueryFilter,
    QueueDefinition, QueueIndex, RangeScanRequest, RangeScanResponse, RangeScanRow, RecurrenceMode,
    RecurrencePolicy, SortDirection, TimeBucket, TypedValue, UtcTimestamp, apply_transition,
    failure_event, priority_sort,
};
use pqueue_engine::{
    ClaimRef, ClaimedItem, CommandEnvelope, CommandPosition, EngineError, EngineResult,
    FinalizeKind, FinalizeOutcome, IndexHit, ItemView, LeaseView, LiveItemView, PayloadUpdate,
    ProjectionSnapshot, PushItem, QueueCommand, QueueCounters, QueueKey, QueueMetrics,
    ScheduleUpdate, SnapshotRef,
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
    group_key: Option<GroupKey>,
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
    fenced: bool,
    superseded: bool,
}

/// Portable, typed representation of one item in a [`ProjectionImage`].
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionImageItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
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
    pub fenced: bool,
    pub superseded: bool,
}

impl From<&ItemRecord> for ProjectionImageItem {
    fn from(rec: &ItemRecord) -> Self {
        Self {
            item_id: rec.item_id,
            client_item_key: rec.client_item_key(),
            priority: rec.priority.clone(),
            not_before: rec.not_before,
            group_key: rec.group_key.clone(),
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
            fenced: rec.fenced,
            superseded: rec.superseded,
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
            group_key: item.group_key,
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
            fenced: item.fenced,
            superseded: item.superseded,
        }
    }
}

/// Complete queue projection image at a durable high-water.
///
/// The item list is the source of truth for lifecycle, ordering, fields, payloads, metadata, gates,
/// entity documents, lease state, secondary indexes, and metrics. Derived maps are rebuilt on import.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionImage {
    pub high_water: Option<CommandPosition>,
    pub paused: bool,
    pub next_seq: u64,
    pub items: Vec<ProjectionImageItem>,
    pub side_records: BTreeMap<Vec<u8>, Bytes>,
    pub instance_fences: BTreeMap<Vec<u8>, u64>,
    pub metrics: QueueMetrics,
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

// ---------------------------------------------------------------------------
// Secondary indexes: per-queue, name-keyed maps over configured item fields and typed entity indexes.
// ---------------------------------------------------------------------------

/// One per-queue secondary index. Unique maps a composite key to exactly one item; non-unique maps a
/// key to the (id-ordered) set of items that carry it.
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
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
        IndexType::Integer => value
            .as_i64()
            .map(TypedValue::Integer)
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
        IndexType::Float => value
            .as_f64()
            .map(TypedValue::Float)
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
        IndexType::Boolean => value
            .as_bool()
            .map(TypedValue::Bool)
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
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

fn value_matches_bucket(value: &TypedValue, rule: &pqueue_core::BucketRule) -> bool {
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
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
        TypedValue::Integer(_) => value
            .as_i64()
            .map(TypedValue::Integer)
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
        TypedValue::Float(_) => value
            .as_f64()
            .map(TypedValue::Float)
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
        TypedValue::Bool(_) => value
            .as_bool()
            .map(TypedValue::Bool)
            .ok_or({ EngineError::Invalid("typed index value is not valid for declared type") })?,
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
    snapshots: Vec<(SnapshotRef, ProjectionSnapshot)>,
}

impl LogData {
    /// `LogWriter::append` — append `commands` to this shard's log under `expected_epoch`, advancing the
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
            let seq = self.entries.len() as u64;
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
    ) -> pqueue_engine::CommandPage {
        let start = match &from {
            Some(p) => p.sequence as usize + 1,
            None => 0,
        };
        let mut entries = Vec::new();
        for (i, (entry_epoch, cmd)) in self.entries.iter().enumerate().skip(start).take(limit) {
            entries.push((
                CommandPosition::new(shard.clone(), *entry_epoch, i as u64),
                cmd.clone(),
            ));
        }
        let next = (start + entries.len() < self.entries.len()).then(|| {
            let (next_epoch, _) = &self.entries[start + entries.len()];
            CommandPosition::new(shard.clone(), *next_epoch, (start + entries.len()) as u64)
        });
        pqueue_engine::CommandPage { entries, next }
    }

    pub fn high_water(&self) -> Option<CommandPosition> {
        self.high_water.clone()
    }

    /// Set the persisted high-water, rejecting a regression (TD-007 §4 monotonicity).
    pub fn set_high_water(&mut self, position: CommandPosition) -> EngineResult<()> {
        if let Some(cur) = &self.high_water
            && !cur.precedes(&position)
            && cur != &position
        {
            return Err(EngineError::Invalid("high-water regression"));
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

pub struct ProjectionData {
    items: FastHashMap<ItemId, ItemRecord>,
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
    /// key and payload are opaque bytes pqueue never interprets.
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
            by_key: FastHashMap::default(),
            eligible: EligibilityIndex::new(),
            metrics: QueueMetrics::default(),
            next_seq: 0,
            priority_model,
            ordering_mode,
            max_rank_error,
            recurrence,
            paused: false,
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

    /// Export the complete materialized queue state. `high_water` is supplied by the durable projection
    /// owner because `ProjectionData` itself is log-position agnostic.
    pub fn to_image(&self, high_water: Option<CommandPosition>) -> ProjectionImage {
        let mut items: Vec<ProjectionImageItem> =
            self.items.values().map(ProjectionImageItem::from).collect();
        items.sort_by_key(|item| (item.created_seq, item.item_id));
        ProjectionImage {
            high_water,
            paused: self.paused,
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
        .with_typed_indexes(&definition.typed_indexes);
        projection.paused = image.paused;
        projection.next_seq = image.next_seq;
        projection.side_records = image.side_records;
        projection.instance_fences = image.instance_fences;

        for item in image.items {
            let rec = ItemRecord::from(item);
            if !rec.superseded {
                if let Some(key) = rec.explicit_client_item_key.clone() {
                    projection.by_key.insert(key, rec.item_id);
                }
                let keys =
                    projection.record_index_keys(&rec.fields, rec.entity_document.as_ref())?;
                projection.index_insert_keys(rec.item_id, &keys);
            }
            if rec.state == ItemState::Pending && !rec.superseded {
                projection
                    .eligible
                    .insert(&rec, &projection.items, &projection.priority_model);
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

    fn record_index_keys(
        &self,
        fields: &BTreeMap<String, Bytes>,
        entity: Option<&Value>,
    ) -> EngineResult<Vec<(String, Vec<u8>)>> {
        let mut keys = legacy_index_keys(&self.index_specs, fields)?;
        keys.extend(typed_index_keys(&self.typed_index_specs, entity)?);
        Ok(keys)
    }

    fn insert_pending(&mut self, item: PushItem) -> EngineResult<()> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let rec = ItemRecord {
            item_id: item.item_id,
            explicit_client_item_key: explicit_client_item_key(
                item.item_id,
                item.client_item_key.clone(),
            ),
            priority: item.priority,
            not_before: item.not_before,
            group_key: item.group_key,
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
            fenced: false,
            superseded: false,
        };
        self.eligible
            .insert(&rec, &self.items, &self.priority_model);
        if let Some(key) = rec.explicit_client_item_key.clone() {
            self.by_key.insert(key, rec.item_id);
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
            ItemState::Complete => self.metrics.complete += 1,
            ItemState::Failed => self.metrics.failed += 1,
        }
    }

    fn metrics_dec(&mut self, state: ItemState) {
        match state {
            ItemState::Pending => self.metrics.pending = self.metrics.pending.saturating_sub(1),
            ItemState::Leased => self.metrics.leased = self.metrics.leased.saturating_sub(1),
            ItemState::Complete => self.metrics.complete = self.metrics.complete.saturating_sub(1),
            ItemState::Failed => self.metrics.failed = self.metrics.failed.saturating_sub(1),
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
    fn transition(&mut self, id: &ItemId, ev: ItemEvent) -> EngineResult<ItemState> {
        let model = self.priority_model;
        let (old_key, new_key, old_state, new_state) = {
            let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
            // A superseded id (replaced by upsert) must never re-enter eligible or mutate
            // (TD-007 §2.3): the orchestration ports map this to `-ERR pqueue superseded`.
            if rec.superseded {
                return Err(EngineError::Superseded);
            }
            let old_state = rec.state;
            let old =
                (old_state == ItemState::Pending).then(|| EligibilityIndex::token(rec, &model));
            let new = apply_transition(old_state, ev)
                .map_err(|_| EngineError::Invalid("illegal lifecycle transition"))?;
            rec.state = new;
            rec.item_version += 1;
            let nk = (new == ItemState::Pending).then(|| EligibilityIndex::token(rec, &model));
            (old, nk, old_state, new)
        };
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

    pub fn apply_command(&mut self, cmd: &QueueCommand) -> EngineResult<()> {
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
                    self.insert_pending(it.clone())?;
                }
                Ok(())
            }
            QueueCommand::Claim(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::Claim)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.attempt_count += 1; // delivery count (flavor-diff 7)
                }
                Ok(())
            }
            QueueCommand::CohortClaim(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::Claim)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.attempt_count += 1;
                }
                Ok(())
            }
            QueueCommand::RenewLease(c) => {
                for id in &c.item_ids {
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
                }
                Ok(())
            }
            QueueCommand::CohortRenewLease(_) => Ok(()),
            QueueCommand::ReassignLease(c) => {
                for id in &c.item_ids {
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
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.attempt_count += 1; // a re-delivery to a new consumer is a delivery (TD-006:129)
                    rec.item_version += 1;
                }
                Ok(())
            }
            QueueCommand::UpdateFields(c) => {
                let model = self.priority_model;
                let (old_keys, old_elig, new_keys, new_elig) = {
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
                    let was_pending = rec.state == ItemState::Pending;
                    let old_elig =
                        (repricing && was_pending).then(|| EligibilityIndex::token(rec, &model));

                    let mut next_fields = rec.fields.clone();
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
                    if let ScheduleUpdate::Set(p) = &c.set_priority {
                        next_rec.priority = p.clone();
                    }
                    if let ScheduleUpdate::Set(nb) = &c.set_not_before {
                        next_rec.not_before = *nb;
                    }
                    next_rec.item_version += 1;
                    let new_elig = (repricing && was_pending)
                        .then(|| EligibilityIndex::token(&next_rec, &model));
                    (old_keys, old_elig, new_keys, new_elig)
                };
                let rec = self
                    .items
                    .get_mut(&c.item_id)
                    .ok_or(EngineError::NotFound)?;
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
                if c.set_entity_document.is_some() {
                    rec.entity_document = c.set_entity_document.clone();
                }
                if let ScheduleUpdate::Set(p) = &c.set_priority {
                    rec.priority = p.clone();
                }
                if let ScheduleUpdate::Set(nb) = &c.set_not_before {
                    rec.not_before = *nb;
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
                    self.transition(&o.item_id, ev)?;
                    let should_reinsert = {
                        let rec = self
                            .items
                            .get_mut(&o.item_id)
                            .ok_or(EngineError::NotFound)?;
                        rec.lease_token = None;
                        rec.lease_expires_at = None;
                        rec.fenced = false;
                        // A rearm that returned to Pending (within `until`) resets the delivery count and,
                        // when the caller supplied the next-occurrence time, defers re-eligibility to that
                        // new `not_before` (the idle interval). Re-key after the record mutation.
                        let old_elig = (rec.state == ItemState::Pending).then(|| {
                            let model = self.priority_model;
                            EligibilityIndex::token(rec, &model)
                        });
                        if matches!(o.kind, FinalizeKind::Rearm) && rec.state == ItemState::Pending
                        {
                            rec.attempt_count = 0;
                            if let Some(nb) = o.not_before {
                                rec.not_before = Some(nb);
                            }
                        }
                        // Queue-native retry backoff: a Retry that returned the item to Pending (still under
                        // the attempt bound) defers its re-eligibility to `not_before`. Guarded on Pending so
                        // an exhausted Retry (-> Failed) gets no backoff.
                        if matches!(o.kind, FinalizeKind::Retry)
                            && rec.state == ItemState::Pending
                            && let Some(nb) = o.not_before
                        {
                            rec.not_before = Some(nb);
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
                self.by_key.remove(&c.client_item_key);
                self.insert_pending(c.replacement.clone())?;
                Ok(())
            }
            QueueCommand::LeaseExpired(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::LeaseExpired)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = None;
                    rec.lease_expires_at = None;
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
                    if let Some(rec) = self.items.get_mut(&id) {
                        let old = (rec.state == ItemState::Pending)
                            .then(|| EligibilityIndex::token(rec, &model));
                        let old_state = rec.state;
                        rec.state = ItemState::Failed; // forced terminal (cohort-incomplete)
                        rec.item_version += 1;
                        if let Some(k) = old {
                            self.eligible.remove(k);
                        }
                        self.metrics_transition(old_state, ItemState::Failed);
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
            QueueCommand::PauseQueue => {
                self.paused = true;
                Ok(())
            }
            QueueCommand::ResumeQueue => {
                self.paused = false;
                Ok(())
            }
            // Gates (BQ-14d) are a relational-mode feature; the in-memory family stores no gate state and
            // no item gate keys, so a gate flip is a no-op here (the log-replay backends replay it as such).
            QueueCommand::SetGates(_) => Ok(()),
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
                let model = self.priority_model;
                for id in &c.item_ids {
                    if let Some(rec) = self.items.remove(id) {
                        if !rec.superseded {
                            self.metrics_dec(rec.state);
                        }
                        if let Some(key) = &rec.explicit_client_item_key {
                            self.by_key.remove(key);
                        }
                        if rec.state == ItemState::Pending {
                            self.eligible.remove(EligibilityIndex::token(&rec, &model));
                        }
                        let keys =
                            self.record_index_keys(&rec.fields, rec.entity_document.as_ref())?;
                        self.index_remove_keys(rec.item_id, &keys);
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

impl ProjectionData {
    pub fn is_paused(&self) -> bool {
        self.paused
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

    /// `ProjectionRead::pending` — the in-flight (leased) items.
    pub fn pending_leases(&self) -> Vec<LeaseView> {
        self.items
            .values()
            .filter(|r| r.state == ItemState::Leased)
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

    /// `ProjectionRead::metrics` — per-state counts (superseded items excluded).
    pub fn metrics(&self) -> QueueMetrics {
        self.metrics.clone()
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
                    let old_keys =
                        self.record_index_keys(&rec.fields, rec.entity_document.as_ref())?;
                    let new_keys = self.record_index_keys(&new_fields, Some(&new_entity))?;

                    let rec = self.items.get_mut(&item_id).ok_or(EngineError::NotFound)?;
                    rec.fields = new_fields;
                    rec.entity_document = Some(new_entity);
                    rec.item_version += 1;
                    let removed: Vec<(String, Vec<u8>)> = old_keys
                        .iter()
                        .filter(|key| !new_keys.contains(key))
                        .cloned()
                        .collect();
                    let added: Vec<(String, Vec<u8>)> = new_keys
                        .iter()
                        .filter(|key| !old_keys.contains(key))
                        .cloned()
                        .collect();
                    self.index_remove_keys(item_id, &removed);
                    self.index_insert_keys(item_id, &added);
                    MutationOutcome::Updated
                }
            };
            results.push(MutationResult { item_id, outcome });
        }

        Ok(BoundedMutationResponse { results })
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
        self.items
            .values()
            .filter(|r| {
                r.state == ItemState::Leased
                    && r.lease_expires_at.map(|exp| exp < now).unwrap_or(false)
            })
            .map(|r| r.item_id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    //! White-box tests over the projection's private state (item_version, log compaction). Behavioral
    //! port-level conformance is exercised against the backends in `pqueue-conformance`.
    use super::*;
    use pqueue_core::{
        CohortPolicy, EligibilityPolicy, IndexSpec, MetadataValue, PriorityDirection,
        PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RetryPolicy, TenantId,
    };
    use pqueue_engine::{
        AdvanceInstanceFenceCommand, ClaimCommand, CommandChecksum, CommandId, FinalizeCommand,
        FinalizeKind, FinalizeOutcome, PurgeItemsCommand, PushCommand, RenewLeaseCommand,
        SideRecord, UpdateFieldsCommand, WriteSideRecordsCommand,
    };

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
    fn version_of(proj: &ProjectionData, id: &str) -> u64 {
        proj.items.get(&iid(id)).unwrap().item_version
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
            group_key: None,
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
            fenced: false,
            superseded: false,
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
                ..QueueMetrics::default()
            }
        );

        projection
            .apply_command(&QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("1")],
                lease_token: LeaseToken::new("lt").unwrap(),
                lease_expires_at: ts(60),
            }))
            .unwrap();
        assert_eq!(
            projection.metrics(),
            QueueMetrics {
                pending: 1,
                leased: 1,
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
    fn projection_image_roundtrips_full_projection_state() {
        let definition = qdef();
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        );
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
            }))
            .unwrap();
        projection.apply_command(&QueueCommand::PauseQueue).unwrap();
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
    fn projection_image_does_not_rehydrate_superseded_pending_as_eligible() {
        let definition = qdef();
        let mut image = ProjectionImage {
            high_water: None,
            paused: false,
            next_seq: 2,
            items: vec![
                ProjectionImageItem {
                    item_id: iid("10"),
                    client_item_key: ClientItemKey::new("k-old").unwrap(),
                    priority: Some(PriorityValue::Int64(1)),
                    not_before: None,
                    group_key: None,
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
                    fenced: false,
                    superseded: true,
                },
                ProjectionImageItem {
                    item_id: iid("11"),
                    client_item_key: ClientItemKey::new("k-live").unwrap(),
                    priority: Some(PriorityValue::Int64(2)),
                    not_before: None,
                    group_key: None,
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
                    fenced: false,
                    superseded: false,
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
        log.append(&shard(), &[env(QueueCommand::PauseQueue)], 0)
            .unwrap();
        log.append(&shard(), &[env(QueueCommand::ResumeQueue)], 0)
            .unwrap();
        // Acquire E+1 (durable fence), then one append at epoch 1.
        assert_eq!(log.advance_epoch(), 1);
        let pos = log
            .append(&shard(), &[env(QueueCommand::PauseQueue)], 1)
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
