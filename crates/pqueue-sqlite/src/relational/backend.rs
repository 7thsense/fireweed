use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    AggregateGroup, BoundedMutationRequest, BoundedMutationResponse, BucketCount,
    ClaimByQueryRequest, ClientItemKey, CohortId, DeclaredBucketSegmentRequest,
    DeclaredBucketSegmentResponse, FilterOp, GroupKey, GroupedAggregateRequest,
    GroupedAggregateResponse, IndexDeclaration, IndexType, ItemId, ItemState, LeaseToken, Metadata,
    MetricsByQueryRequest, MutationOutcome, MutationResult, PriorityValue, QueryCapabilityFlags,
    QueryCursor, QueueDefinition, QueueId, RangeScanRequest, RangeScanResponse, RequestId,
    TenantId, TimeBucket, TypedValue, UtcTimestamp,
};
use pqueue_engine::ClaimUnit;
use pqueue_engine::TerminalEmissionMetrics;
use pqueue_engine::{
    ActiveScope, AdvanceInstanceFenceCommand, Backend, ClaimCommand, ClaimCompatibility, ClaimPort,
    ClaimRequest, Claimed, ClaimedItem, CohortClaimCommand, CohortExpiredCommand,
    CohortFinalizeCommand, CohortFinalizePort, CohortLeaseTarget, CohortRenewLeaseCommand,
    CohortRenewLeasePort, CommandEnvelope, CommandPosition, CommitCapabilities, CommitEntryOutcome,
    CommitEntryStatus, CommitRecovery, CommitTransition, ControlPlaneStore, CreateQueueOutcome,
    DiscoveryGranularity, DiscoveryPort, DurabilityClass, EngineError, EngineResult, EntryRecovery,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort, IndexHit, IndexQueryPort,
    ItemView, LeaseExpiredCommand, LeaseView, LiveItemView, PayloadUpdate, PendingPage,
    PendingSummary, ProjectionRead, PurgeItemsCommand, PurgePort, PushCommand, PushItem, PushPort,
    PushSpec, QueueCommand, QueueCounters, QueueKey, QueueMetrics, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort, RenewLeaseCommand,
    RenewLeasePort, ReplacePendingCommand, SetGatesCommand, SetGatesPort, TickReport,
    UpdateFieldsCommand, UpdateFieldsPort, UpsertOutcome, UpsertPort, WriteSideRecordsCommand,
    build_push_items, validate_api001_reserved_write_fields, validate_claim_compatibility,
    validate_entity, validate_gate_push, validate_instance_fence, validate_purge_force,
};
use rusqlite::types::Value as SqlValue;
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, params_from_iter,
};
use serde_json::Value as JsonValue;

use super::*;

#[derive(Debug)]
struct HotQueryCandidate {
    index_key: Vec<u8>,
    item_id: ItemId,
    entity: JsonValue,
    fields: String,
    item_version: i64,
    lifecycle_state: String,
    fenced: bool,
    superseded: bool,
}

#[derive(Debug)]
struct HotQueryShape {
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    equality_fields: usize,
}

fn typed_value_json(value: &TypedValue) -> JsonValue {
    match value {
        TypedValue::String(value) => JsonValue::String(value.clone()),
        TypedValue::Integer(value) => JsonValue::Number((*value).into()),
        TypedValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        TypedValue::Bool(value) => JsonValue::Bool(*value),
        TypedValue::DateTime(value) => JsonValue::Number(ts_nanos(*value).into()),
    }
}

fn index_fields(spec: &pqueue_core::QueueIndex) -> Vec<(&str, &IndexType)> {
    match &spec.declaration {
        IndexDeclaration::Single(def) => vec![(def.field.as_str(), &def.index_type)],
        IndexDeclaration::Compound(def) => def
            .fields
            .iter()
            .map(|field| (field.field.as_str(), &field.index_type))
            .collect(),
    }
}

fn index_field_type<'a>(spec: &'a pqueue_core::QueueIndex, field: &str) -> Option<&'a IndexType> {
    index_fields(spec)
        .into_iter()
        .find(|(name, _)| *name == field)
        .map(|(_, ty)| ty)
}

fn truncate_query_timestamp(value: UtcTimestamp, bucket: TimeBucket) -> UtcTimestamp {
    let seconds = match bucket {
        TimeBucket::Hour => value.seconds.div_euclid(3_600) * 3_600,
        TimeBucket::Day => value.seconds.div_euclid(86_400) * 86_400,
    };
    UtcTimestamp::new(seconds, 0).expect("bucketed timestamp")
}

fn value_matches_bucket(value: &TypedValue, rule: &pqueue_core::BucketRule) -> bool {
    let value = match value {
        TypedValue::Integer(value) => *value as f64,
        TypedValue::Float(value) => *value,
        _ => return false,
    };
    rule.exact.is_some_and(|exact| value == exact)
        || (rule.exact.is_none()
            && rule.gt.is_none_or(|bound| value > bound)
            && rule.gte.is_none_or(|bound| value >= bound)
            && rule.lt.is_none_or(|bound| value < bound)
            && rule.lte.is_none_or(|bound| value <= bound))
}

fn prefix_successor(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
    for byte in prefix.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Some(prefix);
        }
        *byte = 0;
    }
    None
}

/// Resolve the equality-constrained leading portion of a declared index to its canonical byte prefix.
/// The SQL seek is deliberately followed by typed predicate evaluation: the seek bounds I/O while the
/// shared codec remains the semantic authority for range comparisons and coercion.
fn hot_query_shape(
    spec: &pqueue_core::QueueIndex,
    filters: &[pqueue_core::QueryFilter],
) -> EngineResult<HotQueryShape> {
    let fields = index_fields(spec);
    let mut encoded = Vec::new();
    let mut equality_fields = 0;
    while equality_fields < fields.len() {
        let (field, index_type) = fields[equality_fields];
        let matches = filters
            .iter()
            .filter(|filter| filter.field == field)
            .collect::<Vec<_>>();
        if matches.is_empty() || matches.iter().any(|filter| filter.op != FilterOp::Eq) {
            break;
        }
        if matches.len() != 1 {
            return Err(EngineError::Invalid("duplicate index predicate"));
        }
        let filter = matches[0];
        let json = typed_value_json(&filter.value);
        let Some(component) =
            axon_esf::encode_compound_index_key(&[(&json, index_type)]).map_err(|_| {
                EngineError::Invalid("typed index value is not valid for declared type")
            })?
        else {
            return Err(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ));
        };
        encoded.extend(component);
        equality_fields += 1;
    }

    let mut lower = if encoded.is_empty() {
        None
    } else {
        Some(encoded.clone())
    };
    let mut upper = if encoded.is_empty() {
        None
    } else {
        prefix_successor(encoded.clone())
    };
    let range_field = fields.get(equality_fields).map(|(field, ty)| (*field, *ty));
    for filter in filters {
        let Some(position) = fields.iter().position(|(field, _)| *field == filter.field) else {
            return Err(EngineError::Invalid("unindexed-field"));
        };
        if position < equality_fields {
            continue;
        }
        if position != equality_fields || filter.op == FilterOp::Eq || range_field.is_none() {
            return Err(EngineError::Invalid("invalid index predicate shape"));
        }
        let (_, index_type) = range_field.expect("checked");
        if matches!(index_type, IndexType::String | IndexType::Boolean) {
            return Err(EngineError::Invalid(
                "range field has no order-preserving SQL key encoding",
            ));
        }
        let json = typed_value_json(&filter.value);
        let Some(component) =
            axon_esf::encode_compound_index_key(&[(&json, index_type)]).map_err(|_| {
                EngineError::Invalid("typed index value is not valid for declared type")
            })?
        else {
            return Err(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ));
        };
        let mut bound = encoded.clone();
        bound.extend(component);
        match filter.op {
            FilterOp::Gte => {
                if lower.is_some() && lower.as_ref() != Some(&encoded) {
                    return Err(EngineError::Invalid("duplicate lower bound"));
                }
                lower = Some(bound);
            }
            FilterOp::Gt => {
                if lower.is_some() && lower.as_ref() != Some(&encoded) {
                    return Err(EngineError::Invalid("duplicate lower bound"));
                }
                lower = prefix_successor(bound);
            }
            FilterOp::Lt => {
                if upper.is_some() && upper != prefix_successor(encoded.clone()) {
                    return Err(EngineError::Invalid("duplicate upper bound"));
                }
                upper = Some(bound);
            }
            FilterOp::Lte => {
                if upper.is_some() && upper != prefix_successor(encoded.clone()) {
                    return Err(EngineError::Invalid("duplicate upper bound"));
                }
                upper = prefix_successor(bound);
            }
            FilterOp::Eq => unreachable!(),
        }
    }
    Ok(HotQueryShape {
        lower,
        upper,
        equality_fields,
    })
}

fn validate_hot_query_order(
    spec: &pqueue_core::QueueIndex,
    shape: &HotQueryShape,
    order_by: &[pqueue_core::OrderField],
) -> EngineResult<pqueue_core::SortDirection> {
    let direction = order_by
        .first()
        .ok_or(EngineError::Invalid("range-scan order_by required"))?
        .direction;
    if order_by.iter().any(|order| order.direction != direction) {
        return Err(EngineError::Invalid(
            "mixed order directions are unsupported",
        ));
    }
    let fields = index_fields(spec);
    let expected_start = if shape.equality_fields == fields.len() {
        fields.len().saturating_sub(1)
    } else {
        shape.equality_fields
    };
    let expected = &fields[expected_start..];
    if order_by.len() != expected.len()
        || order_by
            .iter()
            .zip(expected)
            .any(|(actual, (field, _))| actual.field != *field)
    {
        return Err(EngineError::Invalid(
            "order-by does not follow declared index order",
        ));
    }
    Ok(direction)
}

fn resolving_query_index<'a>(
    definition: &'a QueueDefinition,
    preferred: &'a pqueue_core::QueueIndex,
    filters: &[pqueue_core::QueryFilter],
) -> EngineResult<&'a pqueue_core::QueueIndex> {
    std::iter::once(preferred)
        .chain(
            definition
                .typed_indexes
                .iter()
                .filter(|candidate| candidate.name != preferred.name),
        )
        .find(|candidate| hot_query_shape(candidate, filters).is_ok())
        .ok_or(EngineError::Invalid(
            "no declared index resolves the predicate shape",
        ))
}

fn resolving_bucket_base_index<'a>(
    definition: &'a QueueDefinition,
    preferred: &'a pqueue_core::QueueIndex,
    filters: &[pqueue_core::QueryFilter],
) -> EngineResult<&'a pqueue_core::QueueIndex> {
    std::iter::once(preferred)
        .chain(
            definition
                .typed_indexes
                .iter()
                .filter(|candidate| candidate.name != preferred.name),
        )
        .find(|candidate| {
            hot_query_shape(candidate, filters).is_ok()
                && index_fields(candidate)
                    .iter()
                    .all(|(field, _)| filters.iter().any(|filter| filter.field == *field))
        })
        .ok_or(EngineError::Invalid(
            "no declared index covers the bucket base population",
        ))
}

fn entity_matches_filters(
    entity: &JsonValue,
    filters: &[pqueue_core::QueryFilter],
) -> EngineResult<bool> {
    for filter in filters {
        let index_type = match filter.value {
            TypedValue::String(_) => IndexType::String,
            TypedValue::Integer(_) => IndexType::Integer,
            TypedValue::Float(_) => IndexType::Float,
            TypedValue::Bool(_) => IndexType::Boolean,
            TypedValue::DateTime(_) => IndexType::Datetime,
        };
        let Some(value) = typed_value_for_json(
            entity.get(&filter.field).unwrap_or(&JsonValue::Null),
            &index_type,
        )?
        else {
            return Ok(false);
        };
        let order = typed_value_compare(&value, &filter.value)?;
        let matches = match filter.op {
            FilterOp::Eq => order.is_eq(),
            FilterOp::Gte => order.is_ge(),
            FilterOp::Gt => order.is_gt(),
            FilterOp::Lte => order.is_le(),
            FilterOp::Lt => order.is_lt(),
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn hot_query_candidate_page(
    conn: &Connection,
    shard: &QueueKey,
    spec: &pqueue_core::QueueIndex,
    filters: &[pqueue_core::QueryFilter],
    direction: pqueue_core::SortDirection,
    cursor: Option<(&[u8], &str)>,
    limit: usize,
    strict_shape: bool,
) -> EngineResult<Vec<HotQueryCandidate>> {
    let shape = if strict_shape {
        hot_query_shape(spec, filters)?
    } else {
        let fields = index_fields(spec);
        let mut prefix_filters = Vec::new();
        for (field, _) in fields {
            let Some(filter) = filters
                .iter()
                .find(|filter| filter.field == field && filter.op == FilterOp::Eq)
            else {
                break;
            };
            prefix_filters.push(filter.clone());
        }
        hot_query_shape(spec, &prefix_filters)?
    };
    let (tenant, queue) = parts(shard);
    let descending = direction == pqueue_core::SortDirection::Descending;
    let index = if descending {
        "pqueue_item_index_key_item_desc_idx"
    } else {
        "pqueue_item_index_key_item_asc_idx"
    };
    let mut sql = format!(
        "SELECT x.index_key,i.item_id,i.entity_document,i.fields,i.item_version,i.lifecycle_state,i.fenced,i.superseded \
         FROM pqueue_item_index AS x INDEXED BY {index} \
         JOIN pqueue_items AS i ON i.tenant_id=x.tenant_id AND i.queue_id=x.queue_id AND i.item_id=x.item_id \
         WHERE x.tenant_id=? AND x.queue_id=? AND x.index_name=?"
    );
    let mut values = vec![
        SqlValue::Text(tenant),
        SqlValue::Text(queue),
        SqlValue::Text(spec.name.clone()),
    ];
    if let Some(lower) = shape.lower {
        sql.push_str(" AND x.index_key>=?");
        values.push(SqlValue::Blob(lower));
    }
    if let Some(upper) = shape.upper {
        sql.push_str(" AND x.index_key<?");
        values.push(SqlValue::Blob(upper));
    }
    if let Some((key, item_id)) = cursor {
        if descending {
            sql.push_str(" AND (x.index_key<? OR (x.index_key=? AND x.item_id>?))");
        } else {
            sql.push_str(" AND (x.index_key>? OR (x.index_key=? AND x.item_id>?))");
        }
        values.extend([
            SqlValue::Blob(key.to_vec()),
            SqlValue::Blob(key.to_vec()),
            SqlValue::Text(item_id.to_owned()),
        ]);
    }
    sql.push_str(if descending {
        " ORDER BY x.index_key DESC,x.item_id ASC LIMIT ?"
    } else {
        " ORDER BY x.index_key ASC,x.item_id ASC LIMIT ?"
    });
    values.push(SqlValue::Integer(limit as i64));
    let mut stmt = st(conn.prepare(&sql))?;
    let mut candidates = Vec::new();
    let rows = st(stmt.query_map(params_from_iter(values), |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    }))?;
    for row in rows {
        let (index_key, item_id, entity, fields, item_version, lifecycle_state, fenced, superseded) =
            st(row)?;
        let Some(entity) = entity else { continue };
        let item_id = ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?;
        let entity =
            serde_json::from_str(&entity).map_err(|e| EngineError::Storage(e.to_string()))?;
        if entity_matches_filters(&entity, filters)? {
            candidates.push(HotQueryCandidate {
                index_key,
                item_id,
                entity,
                fields,
                item_version,
                lifecycle_state,
                fenced: fenced != 0,
                superseded: superseded != 0,
            });
        }
    }
    Ok(candidates)
}

#[cfg(test)]
fn hot_query_candidates(
    conn: &Connection,
    shard: &QueueKey,
    spec: &pqueue_core::QueueIndex,
    filters: &[pqueue_core::QueryFilter],
) -> EngineResult<Vec<HotQueryCandidate>> {
    const PAGE: usize = 1_000;
    let mut all = Vec::new();
    let mut cursor: Option<(Vec<u8>, String)> = None;
    loop {
        let page = hot_query_candidate_page(
            conn,
            shard,
            spec,
            filters,
            pqueue_core::SortDirection::Ascending,
            cursor
                .as_ref()
                .map(|(key, id)| (key.as_slice(), id.as_str())),
            PAGE,
            true,
        )?;
        let done = page.len() < PAGE;
        if let Some(last) = page.last() {
            cursor = Some((last.index_key.clone(), last.item_id.to_string()));
        }
        all.extend(page);
        if done {
            break;
        }
    }
    Ok(all)
}

fn for_each_hot_query_candidate(
    conn: &Connection,
    shard: &QueueKey,
    spec: &pqueue_core::QueueIndex,
    filters: &[pqueue_core::QueryFilter],
    mut visit: impl FnMut(HotQueryCandidate) -> EngineResult<()>,
) -> EngineResult<()> {
    const MAX_SCAN_ROWS: usize = 1_000;
    let mut cursor: Option<(Vec<u8>, String)> = None;
    loop {
        let page = hot_query_candidate_page(
            conn,
            shard,
            spec,
            filters,
            pqueue_core::SortDirection::Ascending,
            cursor
                .as_ref()
                .map(|(key, id)| (key.as_slice(), id.as_str())),
            MAX_SCAN_ROWS,
            true,
        )?;
        let done = page.len() < MAX_SCAN_ROWS;
        if let Some(last) = page.last() {
            cursor = Some((last.index_key.clone(), last.item_id.to_string()));
        }
        for candidate in page {
            visit(candidate)?;
        }
        if done {
            return Ok(());
        }
    }
}

// ---------------------------------------------------------------------------
// SqliteRelationalBackend
// ---------------------------------------------------------------------------

/// Sqlite-backed **relational** projection family: `pqueue_items` holds the durable item projection and
/// `relational_cursor` persists the applied high-water for reopen / recovery. Atomic durability class.
pub struct SqliteRelationalBackend {
    pub(crate) inner: Mutex<Inner>,
    /// This instance's node id, packed into every minted [`ItemId`] (ADR-009). `0` single-instance.
    node_id: u8,
    /// Per-(queue, epoch) item-id sequence — see `QueueCounters`.
    counters: QueueCounters,
}

impl SqliteRelationalBackend {
    /// Open (or create) the relational store at `path` and load the queue-definition cache. The durable
    /// cursor and item projection are both recovered from the sqlite file.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` relational store.
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    /// Tag this backend with `node_id` — packed into the disambiguation byte of every minted [`ItemId`]
    /// so distinct nodes competing for one queue never mint a colliding id (ADR-009).
    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    /// Snapshot recovery seam (bead pqueue-8a76daad): the last command position already absorbed by the
    /// durable relational cursor. This mirrors the projection-store high-water so callers can recover the
    /// applied position from the monolithic backend after a reopen.
    pub fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let g = self.inner.lock().expect("projection store poisoned");
        let (t, q) = parts(shard);
        let row: Option<(i64, i64)> = st(g
            .conn
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?;
        Ok(row.and_then(|(next_seq, epoch)| {
            (next_seq > 0)
                .then(|| CommandPosition::new(shard.clone(), epoch as u64, next_seq as u64 - 1))
        }))
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        let inner = open_inner(conn)?;
        let backend = Self {
            inner: Mutex::new(inner),
            node_id: 0,
            counters: QueueCounters::default(),
        };
        backend.restore_counters()?;
        Ok(backend)
    }

    /// Restart recovery from one durable high-water row per queue. Work is proportional to queue count, not
    /// resident or retained item count.
    fn restore_counters(&self) -> EngineResult<()> {
        let g = self.inner.lock().expect("poisoned");
        observe_all_id_high_water_sql(&g.conn, &self.counters)
    }
}

// --- Typed raw commit (one owned transaction) ---------------------------------------------------------

struct RelLogTxn<'a> {
    tx: &'a Transaction<'a>,
}

impl RelLogTxn<'_> {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let (mut next, epoch): (i64, i64) = st(self
            .tx
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        if expected_epoch != epoch as u64 {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for _ in commands {
            positions.push(CommandPosition::new(
                shard.clone(),
                epoch as u64,
                next as u64,
            ));
            next += 1;
        }
        st(self.tx.execute(
            "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
            params![t, q, next],
        ))?;
        Ok(positions)
    }
}

struct RelProjectionTxn<'a> {
    tx: &'a Transaction<'a>,
    queues: &'a HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &'a mut HashSet<QueueKey>,
    claim_scan_hints: &'a mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &'a mut HashMap<QueueKey, bool>,
    /// Token mutations accumulate here and are replayed onto the live map by `write` AFTER commit (F4).
    token_ops: &'a mut Vec<TokenOp>,
}

impl RelProjectionTxn<'_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, env) in positions.iter().zip(commands) {
            apply_command_sql(
                self.tx,
                self.queues,
                self.grouped_shards,
                self.claim_scan_hints,
                self.claim_scan_default_fifo,
                self.token_ops,
                &pos.queue,
                pos,
                pos.sequence,
                env.created_at,
                &env.command,
            )?;
        }
        Ok(())
    }
}

impl Backend for SqliteRelationalBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn supports_gates(&self) -> bool {
        true
    }

    /// Rebuildable-commit capabilities (epic pqueue-2201fd37). The relational backend implements the full
    /// vectorized claimed-work commit boundary in one sqlite transaction: atomic per-entry transition,
    /// vectorized commit, lease-token (hash) + version + lease-expiry validation, retained whole-body
    /// request-id idempotency (`pqueue_request_idempotency`), opaque non-work side records
    /// (`pqueue_side_records`), and recovery/explain reads against the durable projection cache.
    /// Delayed/timer lifecycle work is supported (`not_before`). The boundary is `Atomic`
    /// (single-transaction durability).
    fn commit_capabilities(&self) -> CommitCapabilities {
        CommitCapabilities {
            atomic_transition_commit: true,
            vectorized_commit: true,
            lease_validation: true,
            retained_commit_idempotency: true,
            non_work_side_records: true,
            authoritative_recovery_reads: true,
            delayed_awaits_timers: true,
            durability_class: DurabilityClass::Atomic,
            consistency: "atomic single-transaction commit on sqlite",
        }
    }

    fn commit_raw(
        &self,
        request: pqueue_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<pqueue_engine::RawCommitOutcome>> + Send
    {
        let result = (|| {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            if fault == pqueue_engine::RawCommitFault::BeforeAppend {
                return Err(EngineError::Invalid("fault-injection: kill before append"));
            }
            let mut guard = self.inner.lock().expect("relational backend poisoned");
            let Inner {
                conn,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                live_tokens,
                live_tokens_by_consumer,
                ..
            } = &mut *guard;
            let tx = st(conn.transaction())?;
            let mut token_ops = Vec::new();
            let positions = {
                let mut log_txn = RelLogTxn { tx: &tx };
                log_txn.append(&shard, &commands, expected_epoch)?
            };
            if fault == pqueue_engine::RawCommitFault::AfterAppendBeforeApply {
                return Ok(pqueue_engine::RawCommitOutcome::appended(positions));
            }
            {
                let mut projection_txn = RelProjectionTxn {
                    tx: &tx,
                    queues,
                    grouped_shards,
                    claim_scan_hints,
                    claim_scan_default_fifo,
                    token_ops: &mut token_ops,
                };
                projection_txn.apply(&positions, &commands)?;
            }
            st(tx.commit())?;
            apply_token_ops(live_tokens, live_tokens_by_consumer, token_ops); // only after a durable commit (F4)
            Ok(pqueue_engine::RawCommitOutcome::applied(positions))
        })();
        std::future::ready(result)
    }
}

impl ControlPlaneStore for SqliteRelationalBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            create_queue_sql(&mut g, definition)
        };
        std::future::ready(result)
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .queues
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result: Vec<QueueId> = self
            .inner
            .lock()
            .expect("poisoned")
            .queues
            .keys()
            .filter(|k| k.tenant_id.as_str() == tenant.as_str())
            .map(|k| k.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let (t, q) = parts(shard);
        let result = {
            let g = self.inner.lock().expect("poisoned");
            st(g.conn
                .query_row(
                    "SELECT assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| row.get::<_, i64>(0),
                )
                .optional())
            .and_then(|opt| opt.ok_or(EngineError::NotFound).map(|e| e as u64))
        };
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let (t, q) = parts(shard);
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            // TD-003 acquire: strictly-greater epoch, durably recorded (the fence authority advances).
            let new_epoch: Option<i64> = st(g
                .conn
                .query_row(
                    "UPDATE relational_cursor SET assignment_epoch = assignment_epoch + 1 \
                     WHERE tenant=?1 AND queue=?2 RETURNING assignment_epoch",
                    params![t, q],
                    |row| row.get(0),
                )
                .optional())?;
            new_epoch.ok_or(EngineError::NotFound).map(|e| e as u64)
        })();
        std::future::ready(result)
    }
}

impl ProjectionRead for SqliteRelationalBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            let Inner {
                conn,
                claim_scan_hints,
                claim_scan_default_fifo,
                ..
            } = &mut *g;
            select_eligible_sql_with_scan_hint(
                conn,
                claim_scan_hints,
                claim_scan_default_fifo,
                shard,
                now,
                limit,
            )
        };
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            peek_sql(&g.conn, shard, limit)
        };
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            pending_sql(&g.conn, &g.live_tokens, shard)
        };
        std::future::ready(result)
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            Ok(pending_summary_sql(
                &g.live_tokens,
                &g.live_tokens_by_consumer,
                shard,
            ))
        };
        std::future::ready(result)
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            pending_page_sql(&g.conn, &g.live_tokens, shard, start, limit)
        };
        std::future::ready(result)
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            pending_range_sql(
                &g.conn,
                &g.live_tokens,
                &g.live_tokens_by_consumer,
                shard,
                crate::relational::query::PendingRange {
                    start,
                    end,
                    consumer,
                    limit,
                },
            )
        };
        std::future::ready(result)
    }

    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            pending_by_ids_sql(&g.conn, &g.live_tokens, shard, ids)
        };
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            render_claimed(&g.conn, shard, ids, |id| {
                g.live_tokens
                    .get(shard)
                    .and_then(|tokens| tokens.get(id))
                    .cloned()
            })
        };
        std::future::ready(result)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            live_items_sql(&g.conn, shard, keys)
        };
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            metrics_sql(&g.conn, queue)
        };
        std::future::ready(result)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        _now: UtcTimestamp,
        _emit_change_records: bool,
        _emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            metrics_sql(&g.conn, shard).map(|metrics| TerminalEmissionMetrics {
                resident_terminal_count: metrics.resident_terminal_count,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            })
        };
        std::future::ready(result)
    }
}

/// ADR-011 (pqueue-f4ffd679): typed secondary index queries backed by `pqueue_item_index`.
impl IndexQueryPort for SqliteRelationalBackend {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("projection store poisoned");
            let qi = g
                .queues
                .get(shard)
                .and_then(|d| d.typed_indexes.iter().find(|qi| qi.name == index))
                .ok_or(EngineError::Invalid("unknown secondary index"))?;
            if !index_is_unique(qi) {
                return Err(EngineError::Invalid("secondary index is not unique"));
            }
            let expected_arity = match &qi.declaration {
                IndexDeclaration::Single(_) => 1,
                IndexDeclaration::Compound(def) => def.fields.len(),
            };
            if key.len() != expected_arity {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            let canonical = typed_lookup_canonical_key(qi, key)?;
            let (t, q) = parts(shard);
            let row: Option<(String, String, i64)> = st(g
                .conn
                .query_row(
                    "SELECT i.item_id, i.client_item_key, i.item_version \
                     FROM pqueue_item_index idx \
                     JOIN pqueue_items i \
                       ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id \
                      AND i.item_id=idx.item_id \
                     WHERE idx.tenant_id=?1 AND idx.queue_id=?2 \
                       AND idx.index_name=?3 AND idx.index_key=?4 \
                     LIMIT 1",
                    params![t, q, index, canonical.as_slice()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional())?;
            Ok(row.map(|(id_str, ck_str, ver)| IndexHit {
                item_id: ItemId::new(id_str).expect("valid stored item_id"),
                client_item_key: ClientItemKey::new(ck_str).expect("valid stored client_item_key"),
                item_version: ver as u64,
            }))
        })();
        std::future::ready(result)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("projection store poisoned");
            let qi = g
                .queues
                .get(shard)
                .and_then(|d| d.typed_indexes.iter().find(|qi| qi.name == index))
                .ok_or(EngineError::Invalid("unknown secondary index"))?;
            let expected_arity = match &qi.declaration {
                IndexDeclaration::Single(_) => 1,
                IndexDeclaration::Compound(def) => def.fields.len(),
            };
            if key.len() != expected_arity {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            let canonical = typed_lookup_canonical_key(qi, key)?;
            let (t, q) = parts(shard);
            let mut stmt = st(g.conn.prepare(
                "SELECT i.item_id, i.client_item_key, i.item_version \
                 FROM pqueue_item_index idx \
                 JOIN pqueue_items i \
                   ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id \
                  AND i.item_id=idx.item_id \
                 WHERE idx.tenant_id=?1 AND idx.queue_id=?2 \
                   AND idx.index_name=?3 AND idx.index_key=?4 \
                 ORDER BY i.item_id",
            ))?;
            let rows = st(
                stmt.query_map(params![t, q, index, canonical.as_slice()], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }),
            )?;
            let mut out = Vec::new();
            for r in rows {
                let (id_str, ck_str, ver) = st(r)?;
                out.push(IndexHit {
                    item_id: ItemId::new(id_str)
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    client_item_key: ClientItemKey::new(ck_str)
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    item_version: ver as u64,
                });
            }
            Ok(out)
        })();
        std::future::ready(result)
    }
}

impl DiscoveryPort for SqliteRelationalBackend {
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ActiveScope>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            discover_active_scopes_sql(&g.conn, shard, granularity, now)
        };
        std::future::ready(result)
    }
}

impl PushPort for SqliteRelationalBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        // Bead pqueue-7bac12ce: fence_epoch is now threaded through every data-plane port. The
        // relational backend checks `expected_epoch` against the durable cursor epoch inside
        // `commit_command` — a stale value is `EpochFenced`.
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let mut g = self.inner.lock().expect("poisoned");
            {
                let schema = g.schemas.get(shard);
                for item in &items {
                    validate_entity(schema, item.entity.as_ref())?;
                }
            }
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            g.commit_command(
                shard,
                QueueCommand::Push(PushCommand { items: push_items }),
                now,
                expected_epoch,
            )?;
            Ok(ids)
        })();
        std::future::ready(result)
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let mut g = self.inner.lock().expect("poisoned");
            {
                let schema = g.schemas.get(shard);
                for item in &items {
                    validate_entity(schema, item.entity.as_ref())?;
                }
            }
            let fingerprint = push_request_fingerprint(&items)?;
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            let expires_at = request_expires_at(&g.queues, shard, now)?;
            let epoch = expected_epoch.unwrap_or(0);
            let Inner {
                conn,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                live_tokens,
                live_tokens_by_consumer,
                ..
            } = &mut *g;
            let (t, q) = parts(shard);
            let tx = st(conn.transaction())?;
            let (seq, cursor_epoch): (i64, i64) = st(tx
                .query_row(
                    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            if expected_epoch.is_some_and(|e| e != cursor_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            if let Some(ids) = check_request_idempotency(
                &tx,
                shard,
                IDEMPOTENCY_OPERATION_PUSH,
                &request_id,
                &fingerprint,
                ts_nanos(now),
            )? {
                return Ok(ids);
            }

            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let mut token_ops = Vec::new();
            apply_command_sql(
                &tx,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                &mut token_ops,
                shard,
                &CommandPosition::new(shard.clone(), cursor_epoch as u64, seq as u64),
                seq as u64,
                now,
                &QueueCommand::Push(PushCommand { items: push_items }),
            )?;
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                params![t, q, seq + 1],
            ))?;
            let positions = [CommandPosition::new(
                shard.clone(),
                cursor_epoch as u64,
                seq as u64,
            )];
            record_request_idempotency(
                &tx,
                shard,
                IDEMPOTENCY_OPERATION_PUSH,
                &request_id,
                &fingerprint,
                &ids,
                &positions,
                now,
                expires_at,
            )?;
            st(tx.commit())?;
            apply_token_ops(live_tokens, live_tokens_by_consumer, token_ops);
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl pqueue_engine::ReschedulePort for SqliteRelationalBackend {}

impl SetGatesPort for SqliteRelationalBackend {
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            g.commit_command(shard, QueueCommand::SetGates(command), now, expected_epoch)
        };
        std::future::ready(result)
    }
}

impl ClaimPort for SqliteRelationalBackend {
    /// BQ-11b: the TD-002 serialized claim CTE — candidate selection and the lease land in **one**
    /// transaction (`with candidates as (select … order by … limit … for update skip locked) update …
    /// returning`), so there is no select-then-lease TOCTOU (unlike the BQ-11a two-transaction form).
    ///
    /// CONCURRENCY NOTE: the serialization that makes the in-one-transaction select+lease safe here comes
    /// from the whole-backend `Mutex<Inner>` (one writer at a time), NOT from the sqlite transaction — a
    /// deferred transaction takes no row lock at SELECT time. The transaction provides failure-atomicity
    /// (rollback on error/crash). BQ-12 (postgres_native) has no such Mutex and MUST use a real `FOR UPDATE
    /// SKIP LOCKED` candidate lock; it cannot inherit this pattern unchanged.
    ///
    /// Eligibility ordering is the strict-claim key (`priority_sort, created_seq`), exact parity with the
    /// in-memory reference; `progress_guard_sort` bounded-relaxed promotion is a cross-family enhancement
    /// deferred so the two families never diverge on the conformance core class (TD-002:649;
    /// group/`same_group_key` selection is BQ-14).
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // BQ-14a/b: resolve the claim unit from the compatibility options. Item-level (the default) is
            // byte-identical; WholeGroup / SameGroupKey select group-aware from `pqueue_group_summary`;
            // WholeCohort is gated to `Unavailable` until BQ-14c. An invalid combo propagates the
            // structured validation error.
            let unit = if req.compatibility != ClaimCompatibility::default() {
                let def = g.queues.get(&req.shard).ok_or(EngineError::NotFound)?;
                validate_claim_compatibility(&req.compatibility, req.max_items as u64, def)?
            } else {
                ClaimUnit::Item
            };
            let Inner {
                conn,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                live_tokens,
                live_tokens_by_consumer,
                ..
            } = &mut *g;
            let (t, q) = parts(&req.shard);
            let tx = st(conn.transaction())?;
            // ADR-009 / TD-003 fence: a superseded owner (cached `expected_epoch` != the durable
            // assignment_epoch) is rejected BEFORE selecting/leasing — nothing is claimed. `None` = sole-owner.
            let claim_epoch: i64 = st(tx
                .query_row(
                    "SELECT assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| row.get(0),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            if req.expected_epoch.is_some_and(|e| e != claim_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            // Every candidate-selection read below resolves due-ness at the caller-resolved eligibility epoch
            // (`ClaimRequest::eligibility_at`: the explicit `eligibility_time`, else `now`). The lease and
            // the `apply_command_sql` stamping further down deliberately stay on the operational `req.now`,
            // so selecting work for another execution epoch never back-dates a lease.
            let eligibility_at = req.eligibility_at();
            if matches!(unit, ClaimUnit::WholeGroup | ClaimUnit::SameGroupKey) {
                refresh_due_group_summaries(&tx, &req.shard, eligibility_at)?;
            }
            // Candidate selection inside the claim transaction (serialized under the backend Mutex). The
            // item-level path is the strict-claim order; the group/cohort paths consume their projections.
            let mut selected_cohort: Option<CohortId> = None;
            let candidates = match unit {
                ClaimUnit::Item => select_eligible_sql_with_scan_hint(
                    &tx,
                    claim_scan_hints,
                    claim_scan_default_fifo,
                    &req.shard,
                    eligibility_at,
                    req.max_items,
                )?,
                ClaimUnit::WholeGroup => {
                    let max_groups = req
                        .compatibility
                        .group_batching
                        .as_ref()
                        .map(|gb| gb.max_groups)
                        .unwrap_or(0);
                    select_group_batching(
                        &tx,
                        &req.shard,
                        eligibility_at,
                        req.max_items,
                        max_groups,
                        &req.compatibility,
                    )?
                }
                ClaimUnit::SameGroupKey => select_same_group(
                    &tx,
                    &req.shard,
                    eligibility_at,
                    req.max_items,
                    &req.compatibility,
                )?,
                ClaimUnit::WholeCohort => {
                    match select_whole_cohort(
                        &tx,
                        &req.shard,
                        eligibility_at,
                        req.max_items,
                        &req.compatibility,
                    )? {
                        Some(selected) => {
                            selected_cohort = Some(selected.cohort_id);
                            selected.item_ids
                        }
                        None => Vec::new(),
                    }
                }
            };
            if candidates.is_empty() {
                return Ok(Claimed::default()); // tx dropped (rolled back) — nothing leased
            }
            // Lease the selected candidates in the SAME transaction (the CTE's `update … returning`).
            let seq: i64 = st(tx
                .query_row(
                    "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| row.get(0),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            let mut token_ops = Vec::new();
            let claim_command = if let Some(cohort_id) = selected_cohort.clone() {
                QueueCommand::CohortClaim(CohortClaimCommand {
                    cohort_id,
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                })
            } else {
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                    worker_id: Some(req.worker_id.clone()),
                })
            };
            apply_command_sql(
                &tx,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                &mut token_ops,
                &req.shard,
                &CommandPosition::new(req.shard.clone(), claim_epoch as u64, seq as u64),
                seq as u64,
                req.now,
                &claim_command,
            )?;
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                params![t, q, seq + 1],
            ))?;
            // Render the reply from the just-leased rows + the token we just minted (the CTE's RETURNING).
            let items = render_claimed(&tx, &req.shard, &candidates, |_| {
                Some(req.lease_token.clone())
            })?;
            // Every selected candidate was just leased in this txn, so it must render (parity guard the
            // in-memory backend also carries) — a miss means an apply/render divergence, not a no-op.
            debug_assert_eq!(
                items.len(),
                candidates.len(),
                "every claimed candidate must render"
            );
            st(tx.commit())?;
            apply_token_ops(live_tokens, live_tokens_by_consumer, token_ops); // only after a durable commit (F4)
            let mut claimed = Claimed {
                items,
                ..Default::default()
            };
            if matches!(unit, ClaimUnit::WholeCohort) {
                claimed.cohort_lease_token = Some(req.lease_token.clone());
                let _ = apply_whole_cohort_response_shape(&mut claimed.items);
                claimed.cohort_id = selected_cohort;
            }
            Ok(claimed)
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for SqliteRelationalBackend {
    /// Insert / replace-pending / reject-claimed / reject-terminal. BQ-11c adds the `client_item_key`
    /// retention tombstone: when no active item exists but a non-expired retention record does (an item
    /// under this key was purged within `client_item_key_retention_ms`), the re-push is still rejected
    /// as a duplicate (`Terminal`) rather than resurrecting the work — duplicate-push convergence across a
    /// purge (TD-002 §Idempotency). Data-plane request-id replay is a separate concern (no port carries a
    /// `request_id` yet — see the module note).
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
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-commit entity schema validation (ADR-011): reject before any mutation.
            validate_entity(g.schemas.get(shard), entity.as_ref())?;
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            // Active item under this key (superseded predecessors excluded by the partial index).
            let existing: Option<(String, String)> = st(g
                .conn
                .query_row(
                    "SELECT item_id, lifecycle_state FROM pqueue_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 AND superseded=0",
                    params![t, q, client_item_key.as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional())?;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, 1);
            let new_item_id = ItemId::mint(epoch, self.node_id, counter_base);
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id,
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                fields,
                metadata,
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: entity,
            };
            match existing {
                None => {
                    // No active item — but a non-expired retention tombstone (an item under this
                    // key was purged within retention) keeps the re-push a duplicate (TD-002).
                    let retained: Option<i64> = st(g
                        .conn
                        .query_row(
                            "SELECT expires_at FROM pqueue_item_key_retention \
                             WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3",
                            params![t, q, client_item_key.as_str()],
                            |row| row.get(0),
                        )
                        .optional())?;
                    if let Some(expires) = retained {
                        if expires > ts_nanos(now) {
                            return Err(EngineError::Terminal);
                        }
                        // Expired: the key is reusable again — clear the stale tombstone, then insert.
                        st(g.conn.execute(
                            "DELETE FROM pqueue_item_key_retention \
                             WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3",
                            params![t, q, client_item_key.as_str()],
                        ))?;
                    }
                    g.commit_command(
                        shard,
                        QueueCommand::Push(PushCommand { items: vec![item] }),
                        now,
                        expected_epoch,
                    )?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                Some((existing_id, state)) => {
                    let existing_id = ItemId::new(existing_id)
                        .map_err(|e| EngineError::Storage(e.to_string()))?;
                    match parse_state(&state)? {
                        ItemState::Pending => {
                            g.commit_command(
                                shard,
                                QueueCommand::ReplacePending(ReplacePendingCommand {
                                    client_item_key: client_item_key.clone(),
                                    superseded_item_id: existing_id,
                                    replacement: item,
                                }),
                                now,
                                expected_epoch,
                            )?;
                            Ok(UpsertOutcome::Replaced {
                                new_item_id,
                                superseded_item_id: existing_id,
                            })
                        }
                        ItemState::Leased => {
                            Err(EngineError::Invalid("collision with claimed item"))
                        }
                        ItemState::Complete | ItemState::Failed => Err(EngineError::Terminal),
                    }
                }
            }
        })();
        std::future::ready(result)
    }
}

/// Snorri vectorized claimed-work commit on the rebuildable relational family (C9, epic pqueue-2201fd37)
/// - "at least one durable backend" parity for the commit boundary. The WHOLE request body runs in ONE
///   sqlite transaction so request-id check + per-entry validate + side-record/lifecycle/finalize writes +
///   outcome record commit atomically (or roll back together on a storage fault).
impl pqueue_engine::CommitTransitionPort for SqliteRelationalBackend {
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        let result = (|| {
            let CommitTransition {
                request_id,
                entries,
            } = transition;
            let fingerprint = commit_request_fingerprint(&entries)?;
            let mut g = self.inner.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            let expires_at = request_expires_at(&g.queues, shard, now)?;
            let epoch = expected_epoch.unwrap_or(0);
            let schema = g.schemas.get(shard).cloned();
            let Inner {
                conn,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                live_tokens,
                live_tokens_by_consumer,
                ..
            } = &mut *g;
            let (t, q) = parts(shard);
            let tx = st(conn.transaction())?;
            // ADR-009 / TD-003: read the durable assignment_epoch with the cursor and fence the owner's cached
            // acquire-time epoch (`Some`) — a superseded owner is rejected `EpochFenced`, nothing applied.
            let (seq0, cursor_epoch): (i64, i64) = st(tx
                .query_row(
                    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            if expected_epoch.is_some_and(|e| e != cursor_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }

            // (1) Request-id idempotency over the WHOLE commit body (same retained-request-id table/path as
            //     the relational push). A retained body+id REPLAYS the prior per-entry outcomes (no re-write);
            //     a different body under that id is `RequestIdConflict`; an expired/absent record proceeds.
            if let Some(rid) = &request_id
                && let Some(stored) =
                    check_commit_idempotency(&tx, shard, rid, &fingerprint, ts_nanos(now))?
            {
                return Ok(recovery_to_outcomes(&stored));
            }

            // (2) Per entry: validate the lease-token + version-fenced claim_ref, then apply the entry's
            //     side-records + lifecycle push + input finalize in this same transaction. A rejected entry
            //     applies nothing (its outcome is captured; later entries still proceed). The caller's
            //     `request_id` is recorded with the whole-body outcome (no `request_id: None` on this path).
            let mut token_ops = Vec::new();
            let mut seq = seq0 as u64;
            let mut positions: Vec<CommandPosition> = Vec::new();
            let mut apply =
                |command: &QueueCommand, token_ops: &mut Vec<TokenOp>| -> EngineResult<()> {
                    apply_command_sql(
                        &tx,
                        queues,
                        grouped_shards,
                        claim_scan_hints,
                        claim_scan_default_fifo,
                        token_ops,
                        shard,
                        &CommandPosition::new(shard.clone(), cursor_epoch as u64, seq),
                        seq,
                        now,
                        command,
                    )?;
                    positions.push(CommandPosition::new(
                        shard.clone(),
                        cursor_epoch as u64,
                        seq,
                    ));
                    seq += 1;
                    Ok(())
                };

            let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
            for entry in entries {
                let consumed_input_id = entry.claim_ref.item_id;
                let additional_consumed_input_ids = entry
                    .additional_claim_refs
                    .iter()
                    .map(|claim| claim.item_id)
                    .collect::<Vec<_>>();
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids: additional_consumed_input_ids.clone(),
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };
                if let Err(error) = pqueue_engine::validate_distinct_commit_claims(
                    &entry.claim_ref,
                    &entry.additional_claim_refs,
                ) {
                    recovery.push(reject(error));
                    continue;
                }
                if let Some(e) = std::iter::once(&entry.claim_ref)
                    .chain(&entry.additional_claim_refs)
                    .find_map(|claim| commit_validate_sql(&tx, shard, claim, now).err())
                {
                    recovery.push(reject(e));
                    continue;
                }
                // C6: validate the caller-supplied instance fence against the durable fence (absent == 0).
                // A stale `expected` -> Conflict, a non-monotonic `next` -> Invalid; NOTHING is applied.
                if let Some(fence) = &entry.instance_fence {
                    let (it, iq) = parts(shard);
                    let stored: i64 = st(tx
                        .query_row(
                            "SELECT fence FROM pqueue_instance_fences \
                             WHERE tenant_id=?1 AND queue_id=?2 AND instance_key=?3",
                            params![it, iq, fence.instance_key],
                            |row| row.get(0),
                        )
                        .optional())?
                    .unwrap_or(0);
                    if let Err(e) = validate_instance_fence(stored as u64, fence) {
                        recovery.push(reject(e));
                        continue;
                    }
                }
                if !entry.lifecycle_items.is_empty()
                    && let Some(e) = entry.lifecycle_items.iter().find_map(|item| {
                        validate_entity(schema.as_ref(), item.entity.as_ref()).err()
                    })
                {
                    recovery.push(reject(e));
                    continue;
                }
                let side_record_keys: Vec<Vec<u8>> =
                    entry.side_records.iter().map(|r| r.key.clone()).collect();
                let instance = entry
                    .instance_fence
                    .as_ref()
                    .map(|f| (f.instance_key.clone(), f.next));

                if !entry.side_records.is_empty() {
                    apply(
                        &QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                            records: entry.side_records,
                        }),
                        &mut token_ops,
                    )?;
                }
                if let Some(fence) = entry.instance_fence {
                    apply(
                        &QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                            instance_key: fence.instance_key,
                            expected: fence.expected,
                            next: fence.next,
                        }),
                        &mut token_ops,
                    )?;
                }
                let mut lifecycle_item_ids = Vec::new();
                if !entry.lifecycle_items.is_empty() {
                    let counter_base =
                        self.counters
                            .reserve(shard, epoch, entry.lifecycle_items.len() as u32);
                    let (push_items, ids) = build_push_items(
                        entry.lifecycle_items,
                        epoch,
                        self.node_id,
                        counter_base,
                        max_attempts,
                    );
                    lifecycle_item_ids = ids;
                    apply(
                        &QueueCommand::Push(PushCommand { items: push_items }),
                        &mut token_ops,
                    )?;
                }
                apply(
                    &QueueCommand::Finalize(FinalizeCommand {
                        outcomes: std::iter::once(&entry.claim_ref)
                            .chain(&entry.additional_claim_refs)
                            .map(|claim| FinalizeOutcome::new(claim.item_id, entry.finalize))
                            .collect(),
                    }),
                    &mut token_ops,
                )?;
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }
            let outcomes = recovery_to_outcomes(&recovery);

            // Advance the durable command sequence past every command this body applied.
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                params![t, q, seq as i64],
            ))?;

            // (3) Record the whole-body outcome (only when a request_id was supplied) BEFORE commit, so a
            //     later replay returns it verbatim with no second write.
            if let Some(rid) = &request_id {
                record_commit_idempotency(
                    &tx,
                    shard,
                    rid,
                    &fingerprint,
                    &recovery,
                    &positions,
                    now,
                    expires_at,
                )?;
            }
            st(tx.commit())?;
            apply_token_ops(live_tokens, live_tokens_by_consumer, token_ops); // only after a durable commit (F4)
            Ok(outcomes)
        })();
        std::future::ready(result)
    }
}

impl RecoveryReadPort for SqliteRelationalBackend {
    /// Reconstruct a committed transition from the retained `pqueue_request_idempotency` record (epic
    /// pqueue-2201fd37 acceptance #5). The durable `response_payload` already holds every per-entry recovery
    /// field; we only re-attach the `request_id`. `Ok(None)` when nothing is retained under that id. Survives
    /// a reopen (the record is a durable table row).
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let entries = read_commit_recovery(&g.conn, shard, &request_id)?;
            Ok(entries.map(|entries| CommitRecovery {
                request_id,
                entries,
            }))
        })();
        std::future::ready(result)
    }

    /// Read an opaque non-work side record by key from `pqueue_side_records` (recovery/audit read). Disjoint
    /// from `pqueue_items`, so it never reflects claimable work and survives input finalization + reopen.
    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let (t, q) = parts(shard);
            let payload: Option<Vec<u8>> = st(g
                .conn
                .query_row(
                    "SELECT payload FROM pqueue_side_records \
                     WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
                    params![t, q, key],
                    |row| row.get(0),
                )
                .optional())?;
            Ok(payload.map(Bytes::from))
        })();
        std::future::ready(result)
    }
}

/// SQLite API-004 implementation. Every advertised operation resolves through the durable declared-index
/// projection; mutations retain the same transaction, fencing, idempotency, and response-barrier rules as
/// the ordinary item lifecycle.
impl pqueue_engine::HotProjectionQueryPort for SqliteRelationalBackend {
    fn hot_projection_capabilities(&self, _shard: &QueueKey) -> QueryCapabilityFlags {
        QueryCapabilityFlags {
            range_scan: true,
            grouped_aggregate: true,
            declared_bucket_segment: true,
            bounded_mutation: true,
            claim_by_query: true,
            side_record_query: false,
        }
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<RangeScanResponse>> + Send {
        let result = (|| {
            const MAX_PAGE_SIZE: u32 = 1_000;
            request
                .validate(MAX_PAGE_SIZE)
                .map_err(|_| EngineError::Invalid("invalid page size"))?;

            let g = self.inner.lock().expect("projection store poisoned");
            let def = g.queues.get(shard).ok_or(EngineError::NotFound)?;
            let spec = if let Some(name) = request.index.as_deref() {
                def.typed_indexes
                    .iter()
                    .find(|qi| qi.name == name)
                    .ok_or(EngineError::Invalid("unknown secondary index"))?
            } else {
                def.typed_indexes
                    .first()
                    .ok_or(EngineError::Invalid("unknown secondary index"))?
            };
            let shape = hot_query_shape(spec, &request.filters)?;
            let direction = validate_hot_query_order(spec, &shape, &request.order_by)?;

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

            if let Some(state) = &cursor_state {
                let (tenant, queue) = parts(shard);
                let anchor: Option<(Vec<u8>, Option<String>)> = st(g.conn.query_row(
                    "SELECT x.index_key,i.entity_document FROM pqueue_item_index x \
                     JOIN pqueue_items i ON i.tenant_id=x.tenant_id AND i.queue_id=x.queue_id AND i.item_id=x.item_id \
                     WHERE x.tenant_id=?1 AND x.queue_id=?2 AND x.index_name=?3 AND x.item_id=?4",
                    params![tenant, queue, spec.name, state.anchor_item_id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ).optional())?;
                let (index_key, entity_json) =
                    anchor.ok_or(EngineError::Invalid("cursor-invalidated"))?;
                if state.anchor_index_key.as_ref() != Some(&index_key) {
                    return Err(EngineError::Invalid("cursor-invalidated"));
                }
                let entity: JsonValue = serde_json::from_str(
                    entity_json
                        .as_deref()
                        .ok_or(EngineError::Invalid("cursor-invalidated"))?,
                )
                .map_err(|_| EngineError::Invalid("cursor-invalidated"))?;
                if !entity_matches_filters(&entity, &request.filters)? {
                    return Err(EngineError::Invalid("cursor-invalidated"));
                }
                let row = typed_index_row_from_entity(spec, state.anchor_item_id, &entity)?
                    .ok_or(EngineError::Invalid("cursor-invalidated"))?;
                let values = request
                    .order_by
                    .iter()
                    .map(|field| {
                        row.fields
                            .get(&field.field)
                            .cloned()
                            .ok_or(EngineError::Invalid("cursor-invalidated"))
                    })
                    .collect::<EngineResult<Vec<_>>>()?;
                if values != state.anchor_values {
                    return Err(EngineError::Invalid("cursor-invalidated"));
                }
            }

            let cursor_key = cursor_state
                .as_ref()
                .and_then(|state| state.anchor_index_key.as_ref())
                .ok_or_else(|| {
                    if cursor_state.is_some() {
                        EngineError::Invalid("cursor-invalidated")
                    } else {
                        EngineError::NotFound
                    }
                });
            let cursor_pair = match (&cursor_state, cursor_key) {
                (Some(state), Ok(key)) => Some((key.as_slice(), state.anchor_item_id.to_string())),
                (Some(_), Err(error)) => return Err(error),
                (None, _) => None,
            };
            let mut candidates = hot_query_candidate_page(
                &g.conn,
                shard,
                spec,
                &request.filters,
                direction,
                cursor_pair.as_ref().map(|(key, id)| (*key, id.as_str())),
                request.page_size as usize + 1,
                true,
            )?;
            let has_more = candidates.len() > request.page_size as usize;
            if has_more {
                candidates.pop();
            }
            let last_key = candidates
                .last()
                .map(|candidate| candidate.index_key.clone());
            let page_rows = candidates
                .into_iter()
                .map(|candidate| {
                    typed_index_row_from_entity(spec, candidate.item_id, &candidate.entity)?
                        .ok_or(EngineError::Invalid("typed index row disappeared"))
                })
                .collect::<EngineResult<Vec<_>>>()?;
            let next_cursor = if has_more {
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
                    anchor_index_key: last_key,
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
        })();
        std::future::ready(result)
    }

    fn claim_by_query(
        &self,
        shard: &QueueKey,
        request: ClaimByQueryRequest,
        context: pqueue_engine::ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("projection store poisoned");
            let definition = g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?;
            if request.max_items == 0
                || u64::from(request.max_items) > definition.max_claim_batch_size
            {
                return Err(EngineError::Invalid("invalid claim_by_query max_items"));
            }
            if request.lease_duration_ms == 0
                || request.lease_duration_ms > definition.max_lease_duration_ms
            {
                return Err(EngineError::Invalid(
                    "invalid claim_by_query lease_duration_ms",
                ));
            }
            let request_id = request
                .request_id
                .clone()
                .ok_or(EngineError::Invalid("claim_by_query request_id required"))?;
            let request_fingerprint = claim_by_query_fingerprint(&request)?;
            let request_expires_at = request_expires_at(&g.queues, shard, context.now)?;
            let created_at = context.now;
            let lease_expires_at = context.lease_expires_at(request.lease_duration_ms);
            let eligibility_nanos = ts_nanos(context.eligibility_at());
            let Inner {
                conn,
                live_tokens,
                live_tokens_by_consumer,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                ..
            } = &mut *g;
            let (t, q) = parts(shard);
            // Conflict-first is the cross-backend request-id policy: once the basic envelope can be
            // fingerprinted, a changed body reusing a retained id conflicts even if its query structure is
            // itself invalid. Structural validation applies only to a genuinely new execution.
            let tx = st(conn.transaction_with_behavior(TransactionBehavior::Immediate))?;
            if let Some(replay) = check_claim_by_query_idempotency(
                &tx,
                shard,
                &request_id,
                &request_fingerprint,
                context.now,
            )? {
                if replay
                    .worker_id
                    .as_ref()
                    .is_some_and(|worker_id| worker_id != &request.worker_id)
                {
                    return Err(EngineError::Storage(
                        "claim_by_query replay worker attribution mismatch".into(),
                    ));
                }
                let items = render_claimed(&tx, shard, &replay.item_ids, |_| {
                    Some(replay.lease_token.clone())
                })?;
                st(tx.commit())?;
                for item_id in &replay.item_ids {
                    live_tokens
                        .entry(shard.clone())
                        .or_default()
                        .insert(*item_id, replay.lease_token.clone());
                }
                return Ok(Claimed {
                    items,
                    ..Default::default()
                });
            }
            let spec = if let Some(name) = request.index.as_deref() {
                definition
                    .typed_indexes
                    .iter()
                    .find(|qi| qi.name == name)
                    .ok_or(EngineError::Invalid("unknown secondary index"))?
            } else {
                definition
                    .typed_indexes
                    .first()
                    .ok_or(EngineError::Invalid("unknown secondary index"))?
            };
            let shape = hot_query_shape(spec, &request.filters)?;
            let direction =
                validate_hot_query_order(spec, &shape, std::slice::from_ref(&request.order_by))?;
            let paused = queue_paused(&tx, shard)?;
            let mut selected = Vec::new();
            if !paused {
                let descending = direction == pqueue_core::SortDirection::Descending;
                let index = if descending {
                    "pqueue_item_index_key_item_desc_idx"
                } else {
                    "pqueue_item_index_key_item_asc_idx"
                };
                let mut sql = format!(
                    "SELECT i.item_id,i.item_version,i.entity_document FROM pqueue_item_index AS x INDEXED BY {index} \
                     JOIN pqueue_items AS i ON i.tenant_id=x.tenant_id AND i.queue_id=x.queue_id AND i.item_id=x.item_id \
                     WHERE x.tenant_id=? AND x.queue_id=? AND x.index_name=?"
                );
                let mut values = vec![
                    SqlValue::Text(t.clone()),
                    SqlValue::Text(q.clone()),
                    SqlValue::Text(spec.name.clone()),
                ];
                if let Some(lower) = shape.lower {
                    sql.push_str(" AND x.index_key>=?");
                    values.push(SqlValue::Blob(lower));
                }
                if let Some(upper) = shape.upper {
                    sql.push_str(" AND x.index_key<?");
                    values.push(SqlValue::Blob(upper));
                }
                sql.push_str(" AND i.lifecycle_state='Pending' AND i.superseded=0 AND i.fenced=0 \
                    AND i.cohort_size IS NULL AND (i.not_before IS NULL OR i.not_before<=?) \
                    AND i.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
                    JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
                    AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id \
                    AND ig.item_id=i.item_id)");
                values.push(SqlValue::Integer(eligibility_nanos));
                sql.push_str(if descending {
                    " ORDER BY x.index_key DESC,x.item_id ASC LIMIT ?"
                } else {
                    " ORDER BY x.index_key ASC,x.item_id ASC LIMIT ?"
                });
                values.push(SqlValue::Integer(request.max_items as i64));
                let mut stmt = st(tx.prepare(&sql))?;
                let rows = st(stmt.query_map(params_from_iter(values), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                }))?;
                for row in rows {
                    let (item_id, version, entity_json) = st(row)?;
                    let entity_json = entity_json.ok_or_else(|| {
                        EngineError::Storage("indexed item has no entity document".into())
                    })?;
                    let entity: JsonValue = serde_json::from_str(&entity_json)
                        .map_err(|e| EngineError::Storage(e.to_string()))?;
                    if !entity_matches_filters(&entity, &request.filters)? {
                        continue;
                    }
                    selected.push((
                        ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?,
                        version,
                    ));
                }
            }
            let (seq, assignment_epoch): (i64, i64) = st(tx
                .query_row(
                    "SELECT next_seq, assignment_epoch FROM relational_cursor \
                     WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            let lease_token = pqueue_engine::generate_query_lease_token()?;
            let hash = lease_hash(&lease_token);
            let lease_expires_nanos = ts_nanos(lease_expires_at);
            let mut token_ops = Vec::new();
            let mut updated = HashSet::new();
            if !selected.is_empty() {
                st(tx.execute_batch(
                    "CREATE TEMP TABLE IF NOT EXISTS pqueue_hot_query_stage(ordinal INTEGER PRIMARY KEY,item_id TEXT NOT NULL,item_version INTEGER NOT NULL); \
                     DELETE FROM pqueue_hot_query_stage;",
                ))?;
                let values_clause = std::iter::repeat_n("(?,?,?)", selected.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let mut stage_values = Vec::with_capacity(selected.len() * 3);
                for (ordinal, (item_id, version)) in selected.iter().enumerate() {
                    stage_values.extend([
                        SqlValue::Integer(ordinal as i64),
                        SqlValue::Text(item_id.to_string()),
                        SqlValue::Integer(*version),
                    ]);
                }
                st(tx.execute(
                    &format!("INSERT INTO pqueue_hot_query_stage(ordinal,item_id,item_version) VALUES {values_clause}"),
                    params_from_iter(stage_values),
                ))?;
                let mut update = st(tx.prepare(
                    "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?1, \
                     lease_expires_at=?2, worker_id=?3, retry_count=retry_count+1, \
                     item_version=item_version+1, updated_at=?4, last_command_sequence=?5 \
                     WHERE rowid IN (SELECT i2.rowid FROM pqueue_hot_query_stage s \
                         JOIN pqueue_items i2 ON i2.tenant_id=?6 AND i2.queue_id=?7 \
                         AND i2.item_id=s.item_id AND i2.item_version=s.item_version) \
                     AND lifecycle_state='Pending' AND superseded=0 AND fenced=0 \
                     AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?8) \
                     AND eligible_since IS NOT NULL \
                     AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                         ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
                         AND gs.gate_key=ig.gate_key \
                         WHERE ig.tenant_id=pqueue_items.tenant_id \
                         AND ig.queue_id=pqueue_items.queue_id \
                         AND ig.item_id=pqueue_items.item_id) RETURNING item_id",
                ))?;
                let rows = st(update.query_map(
                    params![
                        hash,
                        lease_expires_nanos,
                        request.worker_id.as_str(),
                        ts_nanos(created_at),
                        seq,
                        t,
                        q,
                        eligibility_nanos,
                    ],
                    |row| row.get::<_, String>(0),
                ))?;
                for row in rows {
                    updated.insert(st(row)?);
                }
            }
            let claimed_ids = selected
                .into_iter()
                .map(|(item_id, _)| item_id)
                .filter(|item_id| updated.contains(&item_id.to_string()))
                .collect::<Vec<_>>();
            for item_id in &claimed_ids {
                token_ops.push(TokenOp::Set(shard.clone(), *item_id, lease_token.clone()));
            }
            let mut positions = Vec::new();
            if !claimed_ids.is_empty() {
                let groups = groups_of(&tx, shard, &claimed_ids)?;
                if !groups.is_empty() {
                    grouped_shards.insert(shard.clone());
                }
                refresh_group_summaries(&tx, shard, &groups, created_at)?;
                reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
                st(tx.execute(
                    "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                    params![t, q, seq + 1],
                ))?;
                positions.push(CommandPosition::new(
                    shard.clone(),
                    assignment_epoch as u64,
                    seq as u64,
                ));
            }
            record_claim_by_query_idempotency(
                &tx,
                shard,
                &request_id,
                &request_fingerprint,
                &ClaimByQueryReplay {
                    item_ids: claimed_ids.clone(),
                    lease_token: lease_token.clone(),
                    worker_id: Some(request.worker_id.clone()),
                },
                &positions,
                context.now,
                if claimed_ids.is_empty() {
                    request_expires_at
                } else {
                    request_expires_at.max(lease_expires_nanos)
                },
            )?;
            let items = render_claimed(&tx, shard, &claimed_ids, |_| Some(lease_token.clone()))?;
            debug_assert_eq!(
                items.len(),
                claimed_ids.len(),
                "every queried claim candidate must render"
            );
            st(tx.commit())?;
            apply_token_ops(live_tokens, live_tokens_by_consumer, token_ops);
            Ok(Claimed {
                items,
                ..Default::default()
            })
        })();
        std::future::ready(result)
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<GroupedAggregateResponse>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("projection store poisoned");
            let definition = g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?;
            if request.group_by.is_empty() {
                return Err(EngineError::Invalid("group-by required"));
            }
            let spec = match request.index.as_deref() {
                Some(name) => definition
                    .typed_indexes
                    .iter()
                    .find(|index| index.name == name),
                None => definition.typed_indexes.first(),
            }
            .ok_or(EngineError::Invalid("unknown secondary index"))?;
            let scan_spec = resolving_query_index(&definition, spec, &request.filters)?;
            let mut groups: BTreeMap<String, (BTreeMap<String, TypedValue>, u64)> = BTreeMap::new();
            for_each_hot_query_candidate(
                &g.conn,
                shard,
                scan_spec,
                &request.filters,
                |candidate| {
                    let mut key = BTreeMap::new();
                    for group in &request.group_by {
                        let index_type = index_field_type(spec, &group.field)
                            .ok_or(EngineError::Invalid("unindexed-field"))?;
                        let Some(mut value) = typed_value_for_json(
                            candidate
                                .entity
                                .get(&group.field)
                                .unwrap_or(&JsonValue::Null),
                            index_type,
                        )?
                        else {
                            return Ok(());
                        };
                        if let Some(bucket) = group.time_bucket {
                            value = match value {
                                TypedValue::DateTime(value) => {
                                    TypedValue::DateTime(truncate_query_timestamp(value, bucket))
                                }
                                _ => return Err(EngineError::Invalid("unsupported time bucket")),
                            };
                        }
                        key.insert(group.field.clone(), value);
                    }
                    if key.len() != request.group_by.len() {
                        return Ok(());
                    }
                    let serialized = serde_json::to_string(&key)
                        .map_err(|e| EngineError::Storage(e.to_string()))?;
                    if !groups.contains_key(&serialized)
                        && groups.len() as u32 >= request.max_groups
                    {
                        return Err(EngineError::Invalid("aggregate-too-large"));
                    }
                    groups.entry(serialized).or_insert((key, 0)).1 += 1;
                    Ok(())
                },
            )?;
            Ok(GroupedAggregateResponse {
                groups: groups
                    .into_values()
                    .map(|(key, count)| AggregateGroup { key, count })
                    .collect(),
            })
        })();
        std::future::ready(result)
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("projection store poisoned");
            let definition = g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?;
            let spec = match request.index.as_deref() {
                Some(name) => definition
                    .typed_indexes
                    .iter()
                    .find(|index| index.name == name),
                None => definition.typed_indexes.first(),
            }
            .ok_or(EngineError::Invalid("unknown secondary index"))?;
            let scan_spec = resolving_query_index(&definition, spec, &request.filters)?;
            let mut metrics = QueueMetrics::default();
            for_each_hot_query_candidate(
                &g.conn,
                shard,
                scan_spec,
                &request.filters,
                |candidate| {
                    if candidate.superseded {
                        return Ok(());
                    }
                    match parse_state(&candidate.lifecycle_state)? {
                        ItemState::Pending => metrics.pending += 1,
                        ItemState::Leased => metrics.leased += 1,
                        ItemState::Complete => metrics.complete += 1,
                        ItemState::Failed => metrics.failed += 1,
                    }
                    Ok(())
                },
            )?;
            metrics.resident_terminal_count = metrics.complete + metrics.failed;
            Ok(metrics)
        })();
        std::future::ready(result)
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<Output = EngineResult<DeclaredBucketSegmentResponse>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("projection store poisoned");
            let definition = g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?;
            request
                .validate(1_000)
                .map_err(|_| EngineError::Invalid("invalid request"))?;
            let spec = match request.index.as_deref() {
                Some(name) => definition
                    .typed_indexes
                    .iter()
                    .find(|index| index.name == name),
                None => definition.typed_indexes.first(),
            }
            .ok_or(EngineError::Invalid("unknown secondary index"))?;
            let index_type = index_field_type(spec, &request.field)
                .ok_or(EngineError::Invalid("unindexed-field"))?;
            if !matches!(index_type, IndexType::Integer | IndexType::Float) {
                return Err(EngineError::Invalid("unsupported bucket field"));
            }
            let scan_spec = resolving_bucket_base_index(&definition, spec, &request.filters)?;
            let mut counts = vec![0_u64; request.buckets.len()];
            let mut null_count = 0_u64;
            // Resolve the authoritative base population through a dense-enough declared predicate
            // index.  The bucket field's own index may be sparse and therefore cannot supply NULLs.
            for_each_hot_query_candidate(
                &g.conn,
                shard,
                scan_spec,
                &request.filters,
                |candidate| {
                    let Some(value) = typed_value_for_json(
                        candidate
                            .entity
                            .get(&request.field)
                            .unwrap_or(&JsonValue::Null),
                        index_type,
                    )?
                    else {
                        null_count += 1;
                        return Ok(());
                    };
                    if let Some((position, _)) = request
                        .buckets
                        .iter()
                        .enumerate()
                        .find(|(_, bucket)| value_matches_bucket(&value, bucket))
                    {
                        counts[position] += 1;
                    }
                    Ok(())
                },
            )?;
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
        })();
        std::future::ready(result)
    }

    fn bounded_mutation(
        &self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
    ) -> impl std::future::Future<Output = EngineResult<BoundedMutationResponse>> + Send {
        let result = (|| {
            if request.max_scan_rows == 0 {
                return Err(EngineError::Invalid("invalid page size"));
            }

            let mut g = self.inner.lock().expect("projection store poisoned");
            let definition = g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?;
            let spec = if let Some(name) = request.index.as_deref() {
                definition
                    .typed_indexes
                    .iter()
                    .find(|qi| qi.name == name)
                    .ok_or(EngineError::Invalid("unknown secondary index"))?
            } else {
                definition
                    .typed_indexes
                    .first()
                    .ok_or(EngineError::Invalid("unknown secondary index"))?
            };
            hot_query_shape(spec, &request.filters)?;
            let page_size = (request.max_scan_rows as usize).min(1_000);
            let mut matches = Vec::new();
            let mut scan_cursor: Option<(Vec<u8>, String)> = None;
            loop {
                let page = hot_query_candidate_page(
                    &g.conn,
                    shard,
                    spec,
                    &request.filters,
                    pqueue_core::SortDirection::Ascending,
                    scan_cursor
                        .as_ref()
                        .map(|(key, id)| (key.as_slice(), id.as_str())),
                    page_size,
                    true,
                )?;
                let done = page.len() < page_size;
                if let Some(last) = page.last() {
                    scan_cursor = Some((last.index_key.clone(), last.item_id.to_string()));
                }
                matches.extend(page);
                if done {
                    break;
                }
            }
            matches.sort_by_key(|candidate| candidate.item_id);

            struct PlannedMutation {
                candidate: HotQueryCandidate,
                entity: JsonValue,
                fields: String,
                keys: Vec<(String, Vec<u8>)>,
            }
            let mut planned = Vec::new();
            let mut results = Vec::with_capacity(matches.len());
            for candidate in matches {
                if candidate.fenced
                    || candidate.superseded
                    || parse_state(&candidate.lifecycle_state)? != ItemState::Pending
                {
                    results.push(MutationResult {
                        item_id: candidate.item_id,
                        outcome: MutationOutcome::Conflict,
                    });
                    continue;
                }
                let entity = merge_entity_document(Some(&candidate.entity), &request.set_fields)?;
                validate_entity(g.schemas.get(shard), Some(&entity))?;
                let mut fields = fields_from_json(candidate.fields.clone())?;
                for (field, value) in &request.set_fields {
                    fields.insert(
                        field.clone(),
                        Bytes::from(
                            serde_json::to_vec(value)
                                .map_err(|e| EngineError::Storage(e.to_string()))?,
                        ),
                    );
                }
                planned.push(PlannedMutation {
                    keys: typed_index_keys_for_entity(&definition.typed_indexes, Some(&entity))?,
                    fields: fields_to_json(&fields)?,
                    candidate,
                    entity,
                });
            }

            let (tenant, queue) = parts(shard);
            let now = {
                let d = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid ts")
            };
            let tx = st(g
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate))?;
            let (seq, _epoch): (i64, i64) = st(tx.query_row(
                "SELECT next_seq,assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![tenant, queue],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ))?;
            // Match the canonical UpdateFields unique-index behavior per record.  Existing holders win;
            // for a free target key, deterministic item-id order reserves it for the first mutation.
            let mut reservations: HashMap<(String, Vec<u8>), String> = HashMap::new();
            let mut unique_conflicts = HashSet::new();
            for mutation in &planned {
                let item_id = mutation.candidate.item_id.to_string();
                for (name, key) in &mutation.keys {
                    let unique = definition
                        .typed_indexes
                        .iter()
                        .find(|index| index.name == *name)
                        .is_some_and(index_is_unique);
                    if !unique {
                        continue;
                    }
                    let holder: Option<String> = st(tx.query_row(
                        "SELECT item_id FROM pqueue_item_index WHERE tenant_id=?1 AND queue_id=?2 \
                         AND index_name=?3 AND index_key=?4 AND item_id!=?5 LIMIT 1",
                        params![tenant, queue, name, key, item_id],
                        |row| row.get(0),
                    ).optional())?;
                    let reservation = reservations.get(&(name.clone(), key.clone()));
                    if holder.is_some() || reservation.is_some_and(|holder| holder != &item_id) {
                        unique_conflicts.insert(item_id.clone());
                        break;
                    }
                }
                if !unique_conflicts.contains(&item_id) {
                    for (name, key) in &mutation.keys {
                        if definition
                            .typed_indexes
                            .iter()
                            .find(|index| index.name == *name)
                            .is_some_and(index_is_unique)
                        {
                            reservations.insert((name.clone(), key.clone()), item_id.clone());
                        }
                    }
                }
            }
            for item_id in &unique_conflicts {
                results.push(MutationResult {
                    item_id: ItemId::new(item_id.clone())
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    outcome: MutationOutcome::Conflict,
                });
            }
            planned.retain(|mutation| {
                !unique_conflicts.contains(&mutation.candidate.item_id.to_string())
            });
            let mut updated = HashSet::new();
            if !planned.is_empty() {
                st(tx.execute_batch(
                    "CREATE TEMP TABLE IF NOT EXISTS pqueue_hot_mutation_stage(ordinal INTEGER PRIMARY KEY,item_id TEXT NOT NULL UNIQUE,item_version INTEGER NOT NULL,fields TEXT NOT NULL,entity TEXT NOT NULL); \
                     DELETE FROM pqueue_hot_mutation_stage;",
                ))?;
            }
            for planned_chunk in planned.chunks(1_000) {
                st(tx.execute("DELETE FROM pqueue_hot_mutation_stage", []))?;
                let values_clause = std::iter::repeat_n("(?,?,?,?,?)", planned_chunk.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let mut values = Vec::with_capacity(planned_chunk.len() * 5);
                for (ordinal, mutation) in planned_chunk.iter().enumerate() {
                    values.push(SqlValue::Integer(ordinal as i64));
                    values.push(SqlValue::Text(mutation.candidate.item_id.to_string()));
                    values.push(SqlValue::Integer(mutation.candidate.item_version));
                    values.push(SqlValue::Text(mutation.fields.clone()));
                    values.push(SqlValue::Text(
                        serde_json::to_string(&mutation.entity)
                            .map_err(|e| EngineError::Storage(e.to_string()))?,
                    ));
                }
                st(tx.execute(
                    &format!("INSERT INTO pqueue_hot_mutation_stage(ordinal,item_id,item_version,fields,entity) VALUES {values_clause}"),
                    params_from_iter(values),
                ))?;
                let mut statement = st(tx.prepare(
                    "UPDATE pqueue_items AS i SET fields=m.fields,entity_document=m.entity, \
                     item_version=i.item_version+1,updated_at=?1,last_command_sequence=?2 \
                     FROM pqueue_hot_mutation_stage AS m WHERE i.tenant_id=?3 AND i.queue_id=?4 \
                     AND i.item_id=m.item_id AND i.item_version=m.item_version \
                     AND i.lifecycle_state='Pending' AND i.fenced=0 AND i.superseded=0 RETURNING item_id",
                ))?;
                let rows = st(statement
                    .query_map(params![ts_nanos(now), seq, tenant, queue], |row| {
                        row.get::<_, String>(0)
                    }))?;
                let mut chunk_updated = HashSet::new();
                for row in rows {
                    let item_id = st(row)?;
                    chunk_updated.insert(item_id.clone());
                    updated.insert(item_id);
                }
                drop(statement);

                if !chunk_updated.is_empty() {
                    if chunk_updated.len() != planned_chunk.len() {
                        let placeholders = std::iter::repeat_n("?", chunk_updated.len())
                            .collect::<Vec<_>>()
                            .join(",");
                        st(tx.execute(
                            &format!("DELETE FROM pqueue_hot_mutation_stage WHERE item_id NOT IN ({placeholders})"),
                            params_from_iter(chunk_updated.iter()),
                        ))?;
                    }
                    st(tx.execute(
                        "DELETE FROM pqueue_item_index WHERE rowid IN (SELECT x.rowid FROM pqueue_hot_mutation_stage m \
                         JOIN pqueue_item_index x ON x.tenant_id=?1 AND x.queue_id=?2 AND x.item_id=m.item_id)",
                        params![tenant, queue],
                    ))?;

                    let index_rows = planned_chunk
                        .iter()
                        .filter(|mutation| {
                            chunk_updated.contains(&mutation.candidate.item_id.to_string())
                        })
                        .flat_map(|mutation| {
                            mutation.keys.iter().map(move |(name, key)| {
                                (
                                    mutation.candidate.item_id.to_string(),
                                    name.clone(),
                                    key.clone(),
                                )
                            })
                        })
                        .collect::<Vec<_>>();
                    if !index_rows.is_empty() {
                        // Stay below SQLite's 32,766-variable ceiling even for queues with many
                        // declared indexes.  The common <=6-index/1,000-row path remains one statement.
                        for chunk in index_rows.chunks(6_000) {
                            let values_clause = std::iter::repeat_n("(?,?,?,?,?)", chunk.len())
                                .collect::<Vec<_>>()
                                .join(",");
                            let insert_sql = format!(
                                "INSERT INTO pqueue_item_index \
                                 (tenant_id,queue_id,index_name,index_key,item_id) VALUES {values_clause}"
                            );
                            let mut index_values = Vec::with_capacity(chunk.len() * 5);
                            for (item_id, name, key) in chunk {
                                index_values.extend([
                                    SqlValue::Text(tenant.clone()),
                                    SqlValue::Text(queue.clone()),
                                    SqlValue::Text(name.clone()),
                                    SqlValue::Blob(key.clone()),
                                    SqlValue::Text(item_id.clone()),
                                ]);
                            }
                            st(tx.execute(&insert_sql, params_from_iter(index_values)))?;
                        }
                    }
                }
            }
            let unresolved = planned
                .iter()
                .filter(|mutation| !updated.contains(&mutation.candidate.item_id.to_string()))
                .map(|mutation| mutation.candidate.item_id.to_string())
                .collect::<Vec<_>>();
            let mut existing = HashSet::new();
            for unresolved_chunk in unresolved.chunks(1_000) {
                let placeholders = std::iter::repeat_n("?", unresolved_chunk.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "SELECT item_id FROM pqueue_items WHERE tenant_id=? AND queue_id=? \
                     AND item_id IN ({placeholders})"
                );
                let mut values = vec![
                    SqlValue::Text(tenant.clone()),
                    SqlValue::Text(queue.clone()),
                ];
                values.extend(unresolved_chunk.iter().cloned().map(SqlValue::Text));
                let mut statement = st(tx.prepare(&sql))?;
                let rows =
                    st(statement
                        .query_map(params_from_iter(values), |row| row.get::<_, String>(0)))?;
                for row in rows {
                    existing.insert(st(row)?);
                }
            }
            for mutation in &planned {
                let item_id = mutation.candidate.item_id;
                results.push(MutationResult {
                    item_id,
                    outcome: if updated.contains(&item_id.to_string()) {
                        MutationOutcome::Updated
                    } else if existing.contains(&item_id.to_string()) {
                        MutationOutcome::Conflict
                    } else {
                        MutationOutcome::NotFound
                    },
                });
            }
            let updated_any = !updated.is_empty();
            if updated_any {
                let updated_ids = updated
                    .iter()
                    .map(|item_id| {
                        ItemId::new(item_id.clone())
                            .map_err(|e| EngineError::Storage(e.to_string()))
                    })
                    .collect::<EngineResult<Vec<_>>>()?;
                let groups = groups_of(&tx, shard, &updated_ids)?;
                refresh_group_summaries(&tx, shard, &groups, now)?;
                st(tx.execute(
                    "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                    params![tenant, queue, seq + 1],
                ))?;
            }
            st(tx.commit())?;
            results.sort_by_key(|result| result.item_id);

            Ok(BoundedMutationResponse { results })
        })();
        std::future::ready(result)
    }
}

impl FinalizePort for SqliteRelationalBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            validate_leased(&g.conn, shard, &ids)?;
            g.commit_command(
                shard,
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl CohortFinalizePort for SqliteRelationalBackend {
    fn finalize_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        kind: FinalizeKind,
        not_before: Option<UtcTimestamp>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            if matches!(kind, FinalizeKind::Rearm) {
                return Err(EngineError::Invalid("cohort rearm is invalid"));
            }
            let mut g = self.inner.lock().expect("poisoned");
            validate_cohort_lease(&g.conn, shard, &target)?;
            g.commit_command(
                shard,
                QueueCommand::CohortFinalize(CohortFinalizeCommand {
                    cohort_id: target.cohort_id,
                    kind,
                    not_before,
                }),
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl RenewLeasePort for SqliteRelationalBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            validate_leased(&g.conn, shard, &item_ids)?;
            g.commit_command(
                shard,
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids,
                    lease_expires_at: new_lease_expires_at,
                }),
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl CohortRenewLeasePort for SqliteRelationalBackend {
    fn renew_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            validate_cohort_lease(&g.conn, shard, &target)?;
            g.commit_command(
                shard,
                QueueCommand::CohortRenewLease(CohortRenewLeaseCommand {
                    cohort_id: target.cohort_id,
                    lease_expires_at: new_lease_expires_at,
                }),
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl ReassignLeasePort for SqliteRelationalBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            validate_leased(&g.conn, shard, &item_ids)?;
            g.commit_command(
                shard,
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids,
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl PurgePort for SqliteRelationalBackend {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // Classify every candidate from ONE batched read (was one SELECT per id), then preserve the
            // exact in-order, deduped, force-gated `present` set the per-item loop produced.
            let flags = item_flags_map(&g.conn, shard, &item_ids)?;
            let mut present: Vec<ItemId> = Vec::new();
            for id in &item_ids {
                if present.contains(id) {
                    continue; // de-dup: remove + count once (XDEL semantics)
                }
                if let Some((state, _, _, _)) = flags.get(&id.to_string()) {
                    validate_purge_force(*state == ItemState::Leased, force)?;
                    present.push(*id);
                }
            }
            if present.is_empty() {
                return Ok(0);
            }
            let count = present.len() as u64;
            g.commit_command(
                shard,
                QueueCommand::PurgeItems(PurgeItemsCommand {
                    item_ids: present,
                    force,
                }),
                now,
                expected_epoch,
            )?;
            Ok(count)
        })();
        std::future::ready(result)
    }
}

impl UpdateFieldsPort for SqliteRelationalBackend {
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            validate_api001_reserved_write_fields(&field_ops)?;
            // Pre-commit entity schema validation (ADR-011): reject before any mutation.
            validate_entity(g.schemas.get(shard), entity.as_ref())?;
            // Pre-validate with the SAME error precedence as `ProjectionData::update_fields_validate`
            // (commit has no rollback): absent => NotFound, fenced => StaleLease, terminal => Terminal,
            // superseded => Superseded, version mismatch => Conflict.
            let (t, q) = parts(shard);
            let row: Option<(String, i64, i64, i64)> = st(g
                .conn
                .query_row(
                    "SELECT lifecycle_state, superseded, fenced, item_version FROM pqueue_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, item_id.to_string()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .optional())?;
            let (state, superseded, fenced, version) = row.ok_or(EngineError::NotFound)?;
            if fenced != 0 {
                return Err(EngineError::StaleLease);
            }
            if parse_state(&state)?.is_terminal() {
                return Err(EngineError::Terminal);
            }
            if superseded != 0 {
                return Err(EngineError::Superseded);
            }
            if expected_item_version.is_some_and(|v| v != version as u64) {
                return Err(EngineError::Conflict);
            }
            g.commit_command(
                shard,
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops,
                    payload,
                    set_priority: Default::default(),
                    set_not_before: Default::default(),
                    set_entity_document: entity,
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                }),
                now,
                expected_epoch,
            )?;
            // The apply bumped item_version by one (the row was validated live above).
            Ok(version as u64 + 1)
        })();
        std::future::ready(result)
    }
}

impl ReclaimPort for SqliteRelationalBackend {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let (t, q) = parts(shard);
            let now_n = ts_nanos(now);
            // This queue's leases expired strictly before `now` (half-open, like the tick), optionally capped.
            let base = "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                        AND lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
                        AND lease_expires_at<?3 ORDER BY item_id";
            let id_strs: Vec<String> = {
                let mut out = Vec::new();
                if let Some(lim) = limit {
                    let sql = format!("{base} LIMIT ?4");
                    let mut stmt = st(g.conn.prepare(&sql))?;
                    let rows = st(stmt.query_map(params![t, q, now_n, lim as i64], |row| {
                        row.get::<_, String>(0)
                    }))?;
                    for r in rows {
                        out.push(st(r)?);
                    }
                } else {
                    let mut stmt = st(g.conn.prepare(base))?;
                    let rows =
                        st(stmt.query_map(params![t, q, now_n], |row| row.get::<_, String>(0)))?;
                    for r in rows {
                        out.push(st(r)?);
                    }
                }
                out
            };
            let ids: Vec<ItemId> = id_strs
                .into_iter()
                .map(|s| ItemId::new(s).map_err(|e| EngineError::Storage(e.to_string())))
                .collect::<EngineResult<Vec<_>>>()?;
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // Per-queue and FENCED (unlike the global ReclaimDriver::tick, which passes None).
            g.commit_command(
                shard,
                QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: ids.clone(),
                }),
                now,
                expected_epoch,
            )?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl pqueue_engine::HistoricalProjectionRead for SqliteRelationalBackend {
    // TD-009: the relational family serves only "now" until the ADR-013 rebuild-from-log migration
    // lands; `read_as_of` fails closed with `Unavailable` (mirrors `PostgresRelationalBackend`).
    type AsOfProjection = SqliteProjectionStore;

    fn current_position(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<CommandPosition>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let (t, q) = parts(shard);
            let row: Option<(i64, i64)> = st(g
                .conn
                .query_row(
                    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional())?;
            row.and_then(|(next, epoch)| {
                (next > 0)
                    .then(|| CommandPosition::new(shard.clone(), epoch as u64, (next as u64) - 1))
            })
            .ok_or(EngineError::NotFound)
        })();
        std::future::ready(result)
    }

    fn read_as_of<T, F>(
        &self,
        _shard: &QueueKey,
        _position: CommandPosition,
        _query: F,
    ) -> impl std::future::Future<Output = EngineResult<T>> + Send
    where
        T: Send,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send,
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ReclaimDriver for SqliteRelationalBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // Expired (half-open: valid through lease_expires_at) leased items, per queue.
            let now_n = ts_nanos(now);
            let expired: Vec<(QueueKey, Vec<ItemId>)> = {
                let mut stmt = st(g.conn.prepare(
                    "SELECT tenant_id, queue_id, item_id FROM pqueue_items \
                     WHERE lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
                     AND lease_expires_at<?1 ORDER BY tenant_id, queue_id",
                ))?;
                let rows = st(stmt.query_map(params![now_n], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                }))?;
                let mut by_queue: Vec<(QueueKey, Vec<ItemId>)> = Vec::new();
                for r in rows {
                    let (t, q, id) = st(r)?;
                    let key = QueueKey::new(
                        TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                        QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
                    );
                    let id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
                    match by_queue.last_mut() {
                        Some((k, ids)) if *k == key => ids.push(id),
                        _ => by_queue.push((key, vec![id])),
                    }
                }
                by_queue
            };
            let mut report = TickReport::default();
            for (shard, ids) in expired {
                report.leases_reclaimed += ids.len() as u64;
                g.commit_command(
                    &shard,
                    QueueCommand::LeaseExpired(LeaseExpiredCommand { item_ids: ids }),
                    now,
                    None,
                )?;
            }
            let due_cohorts: Vec<(QueueKey, GroupKey, u64)> = {
                let mut stmt = st(g.conn.prepare(
                    "SELECT c.tenant_id, c.queue_id, c.group_key, c.cohort_created_at, \
                     c.first_eligible_at, r.assignment_epoch \
                     FROM pqueue_cohorts c \
                     JOIN relational_cursor r ON r.tenant=c.tenant_id AND r.queue=c.queue_id \
                     WHERE c.state IN ('forming','complete') \
                     ORDER BY c.tenant_id, c.queue_id, c.group_key \
                     LIMIT ?1",
                ))?;
                let rows = st(
                    stmt.query_map(params![COHORT_EXPIRY_SWEEP_LIMIT as i64], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    }),
                )?;
                let mut out = Vec::new();
                for r in rows {
                    let (t, q, group, cohort_created_at, first_eligible_at, epoch) = st(r)?;
                    let shard = QueueKey::new(
                        TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                        QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
                    );
                    let Some(definition) = g.queues.get(&shard) else {
                        continue;
                    };
                    let Some(deadline) =
                        cohort_expiry_deadline(definition, cohort_created_at, first_eligible_at)
                    else {
                        continue;
                    };
                    if deadline <= now_n {
                        out.push((
                            shard,
                            GroupKey::new(group)
                                .map_err(|e| EngineError::Storage(e.to_string()))?,
                            epoch as u64,
                        ));
                    }
                }
                out
            };
            for (shard, group_key, epoch) in due_cohorts {
                g.commit_command(
                    &shard,
                    QueueCommand::CohortExpired(CohortExpiredCommand { group_key }),
                    now,
                    Some(epoch),
                )?;
                report.cohorts_expired += 1;
            }
            // TD-008 CL-6: reap terminal items whose retention has elapsed. For emit-enabled queues the reap
            // is ALSO gated on the durable emission cursor having passed the item (retention AND cursor), so a
            // terminal row is never dropped before its change record is durably emitted; opt-out queues
            // (`emit_change_records=false`) reap on retention alone. Mirrors `PostgresRelationalBackend::tick`.
            let terminal_sweeps: Vec<(QueueKey, u64, bool)> = g
                .queues
                .iter()
                .map(|(shard, definition)| {
                    (
                        shard.clone(),
                        definition.terminal_retention_ms,
                        definition.emit_change_records,
                    )
                })
                .collect();
            for (shard, terminal_retention_ms, emit_change_records) in terminal_sweeps {
                let emission_cursor = if emit_change_records {
                    let (t, q) = parts(&shard);
                    let row: Option<(i64, i64)> = st(g
                        .conn
                        .query_row(
                            "SELECT epoch, seq FROM relational_emission_cursor \
                             WHERE tenant=?1 AND queue=?2",
                            params![t, q],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .optional())?;
                    row.map(|(epoch, seq)| {
                        CommandPosition::new(shard.clone(), epoch as u64, seq as u64)
                    })
                } else {
                    None
                };
                let tx = st(g.conn.transaction())?;
                let _ = reap_terminal_items_sql(
                    &tx,
                    &shard,
                    now,
                    terminal_retention_ms,
                    emit_change_records,
                    emission_cursor.as_ref(),
                )?;
                st(tx.commit())?;
            }
            Ok(report)
        })();
        std::future::ready(result)
    }
}

// ===========================================================================
// ADR-012 P1b-ii: the UNIFIED relational store as `LogStore + ProjectionStore`
// ===========================================================================
//
// The keystone "same robustness as flat postgres" composition: ONE store value implements BOTH the log
// axis and the projection axis, so the generic [`ComposedBackend::commit_locked`] drives append+apply into
// ONE durable relational transaction with NO phantom log row. The mechanism (ADR-012 §"The atomic write
// seam", unified-transactional path):
//
//   * [`LogStore::append`] STAGES — it reads the durable `relational_cursor` (next_seq + assignment_epoch),
//     applies the TD-003 fence (`expected_epoch` must equal the recorded epoch), and MINTS the
//     `CommandPosition`s in memory. It performs NO durable write and does NOT advance the cursor. There is
//     therefore no log row that can exist without its projection apply.
//   * [`ProjectionStore::apply`] COMMITS — it runs the single durable relational transaction (the projection
//     rows via the 14-arm `apply_command_sql`, the request-id/idempotency rows where applicable, and the
//     cursor `next_seq` advance), exactly what the monolith's `commit_command` / `apply_committed_batch_sql`
//     do. The cursor advance lands in the SAME transaction as the projection write, so a crash leaves the
//     cursor behind the (un-applied) work — never ahead of it.
//
// Because `commit_locked` holds the composed unit-of-work lock across append→apply and the two axes share
// ONE `Inner` (one connection) behind an `Arc<Mutex<_>>`, the mint and the durable apply are consistent:
// `append` mints at the cursor, `apply` applies at that position and advances the cursor by one.
//
// This reaches capability parity with the monolithic [`SqliteRelationalBackend`] for the CORE conformance
// class: the orthogonal orchestration (already proven against `InMemoryProjection`) gets correct answers
// from the relational SQL projection, so the composition passes `core_suite!(@atomic)` identically.

#[cfg(test)]
mod hot_query_sql_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pqueue_core::{
        EligibilityPolicy, FilterOp, GroupKey, IndexDeclaration, IndexDef, OrderingMode,
        PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueryFilter,
        QueueDefinition, QueueIndex, RecurrencePolicy, RetryPolicy, TypedValue,
    };
    use pqueue_engine::{
        ClaimPort, ClaimRequest, ControlPlaneStore, HotProjectionQueryPort, ProjectionRead,
        PushPort, PushSpec,
    };

    use super::*;

    // rusqlite's trace hook accepts a function pointer, so each concurrently runnable
    // proof needs its own counter. Sharing one atomic made the PEL and bounded-mutation
    // tests reset/increment each other's observations under the default parallel test runner.
    static PEL_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static MUTATION_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static GROUP_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static GROUP_PUSH_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static GROUP_CLAIM_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static GROUP_FINALIZE_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static COUNTER_RESTORE_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static LIVE_ITEMS_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static SET_GATES_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static SIDE_RECORD_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_pel_statement(_: &str) {
        PEL_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_mutation_statement(_: &str) {
        MUTATION_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_group_statement(_: &str) {
        GROUP_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_group_push_statement(_: &str) {
        GROUP_PUSH_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_group_claim_statement(_: &str) {
        GROUP_CLAIM_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_group_finalize_statement(_: &str) {
        GROUP_FINALIZE_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_counter_restore_statement(_: &str) {
        COUNTER_RESTORE_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_live_items_statement(sql: &str) {
        if sql.starts_with("SELECT client_item_key, item_id") {
            LIVE_ITEMS_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn count_set_gates_statement(sql: &str) {
        if sql.starts_with("INSERT INTO pqueue_gate_state") {
            SET_GATES_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn count_side_record_statement(sql: &str) {
        if sql.starts_with("INSERT INTO pqueue_side_records") {
            SIDE_RECORD_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mutation_queue() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("queue").unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 1_000,
            max_claim_batch_size: 1_000,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![QueueIndex {
                name: "by_status".into(),
                declaration: IndexDeclaration::Single(IndexDef {
                    field: "status".into(),
                    index_type: IndexType::String,
                    unique: false,
                }),
            }],
            emit_change_records: true,
        }
    }

    async fn pel_read_statement_count(rows: usize) -> usize {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                (0..rows).map(|_| PushSpec::default()).collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        backend
            .claim(ClaimRequest {
                eligibility_time: None,
                shard: shard.clone(),
                worker_id: pqueue_core::WorkerId::new("worker").unwrap(),
                max_items: rows,
                lease_token: LeaseToken::new("consumer").unwrap(),
                lease_expires_at: UtcTimestamp::new(60, 0).unwrap(),
                now: UtcTimestamp::new(0, 0).unwrap(),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        PEL_TRACE_COUNT.store(0, Ordering::Relaxed);
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .trace(Some(count_pel_statement));
        let page = backend.pending_page(&shard, None, 3).await.unwrap();
        assert_eq!(page.entries.len(), 3);
        let requested = [page.entries[2].item_id, page.entries[0].item_id];
        assert_eq!(
            backend
                .pending_by_ids(&shard, &requested)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            backend
                .pending_range(&shard, None, None, None, 2)
                .await
                .unwrap()
                .len(),
            2
        );
        backend.inner.lock().unwrap().conn.trace(None);
        PEL_TRACE_COUNT.load(Ordering::Relaxed)
    }

    #[tokio::test]
    async fn pel_reads_issue_request_bounded_index_queries() {
        let ten = pel_read_statement_count(10).await;
        let thousand = pel_read_statement_count(1_000).await;
        assert_eq!(ten, thousand, "resident PEL size must not add SQL queries");
        assert_eq!(ten, 3, "each bounded read is one set-based indexed query");
    }

    async fn mutation_statement_count(rows: usize) -> usize {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                (0..rows)
                    .map(|_| PushSpec {
                        entity: Some(serde_json::json!({"status":"ready"})),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        MUTATION_TRACE_COUNT.store(0, Ordering::Relaxed);
        {
            let mut inner = backend.inner.lock().unwrap();
            inner.conn.trace(Some(count_mutation_statement));
        }
        let response = backend
            .bounded_mutation(
                &shard,
                BoundedMutationRequest {
                    index: Some("by_status".into()),
                    filters: vec![QueryFilter {
                        field: "status".into(),
                        op: FilterOp::Eq,
                        value: TypedValue::String("ready".into()),
                    }],
                    set_fields: BTreeMap::from([(
                        "status".into(),
                        TypedValue::String("done".into()),
                    )]),
                    max_scan_rows: rows as u32,
                },
            )
            .await
            .unwrap();
        assert_eq!(response.results.len(), rows);
        backend.inner.lock().unwrap().conn.trace(None);
        MUTATION_TRACE_COUNT.load(Ordering::Relaxed)
    }

    async fn grouped_push_statement_count(groups: usize) -> usize {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        GROUP_PUSH_TRACE_COUNT.store(0, Ordering::Relaxed);
        {
            let mut inner = backend.inner.lock().unwrap();
            inner.conn.trace(Some(count_group_push_statement));
        }
        backend
            .push(
                &shard,
                (0..groups)
                    .map(|ordinal| PushSpec {
                        group_key: Some(GroupKey::new(format!("group-{ordinal:04}")).unwrap()),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        backend.inner.lock().unwrap().conn.trace(None);
        GROUP_PUSH_TRACE_COUNT.load(Ordering::Relaxed)
    }

    async fn counter_restore_statement_count(items: usize) -> usize {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                (0..items).map(|_| PushSpec::default()).collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        COUNTER_RESTORE_TRACE_COUNT.store(0, Ordering::Relaxed);
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .trace(Some(count_counter_restore_statement));
        backend.restore_counters().unwrap();
        backend.inner.lock().unwrap().conn.trace(None);
        COUNTER_RESTORE_TRACE_COUNT.load(Ordering::Relaxed)
    }

    async fn live_items_statement_count(items: usize) -> usize {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let keys = (0..items)
            .map(|ordinal| ClientItemKey::new(format!("key-{ordinal:04}")).unwrap())
            .collect::<Vec<_>>();
        backend
            .push(
                &shard,
                keys.iter()
                    .cloned()
                    .map(|client_item_key| PushSpec {
                        client_item_key: Some(client_item_key),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        LIVE_ITEMS_TRACE_COUNT.store(0, Ordering::Relaxed);
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .trace(Some(count_live_items_statement));
        let rows = backend.live_items(&shard, &keys).await.unwrap();
        backend.inner.lock().unwrap().conn.trace(None);
        assert_eq!(rows.len(), items);
        assert!(rows.into_iter().all(|row| row.is_some()));
        LIVE_ITEMS_TRACE_COUNT.load(Ordering::Relaxed)
    }

    async fn set_gates_statement_count(gates: usize) -> usize {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        SET_GATES_TRACE_COUNT.store(0, Ordering::Relaxed);
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .trace(Some(count_set_gates_statement));
        backend
            .set_gates(
                &shard,
                SetGatesCommand {
                    gate_keys: (0..gates)
                        .map(|ordinal| format!("gate-{ordinal:04}"))
                        .collect(),
                    blocked: true,
                },
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        backend.inner.lock().unwrap().conn.trace(None);
        SET_GATES_TRACE_COUNT.load(Ordering::Relaxed)
    }

    async fn side_record_statement_count(records: usize) -> usize {
        use pqueue_engine::{
            ClaimRef, CommitTransition, CommitTransitionEntry, CommitTransitionPort, FinalizeKind,
            SideRecord,
        };

        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                vec![PushSpec::default()],
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let claimed = backend
            .claim(ClaimRequest {
                eligibility_time: None,
                shard: shard.clone(),
                worker_id: pqueue_core::WorkerId::new("worker").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("lease").unwrap(),
                lease_expires_at: UtcTimestamp::new(60, 0).unwrap(),
                now: UtcTimestamp::new(0, 0).unwrap(),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        let item = &claimed.items[0];
        SIDE_RECORD_TRACE_COUNT.store(0, Ordering::Relaxed);
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .trace(Some(count_side_record_statement));
        backend
            .commit_transition(
                &shard,
                CommitTransition {
                    request_id: None,
                    entries: vec![CommitTransitionEntry {
                        claim_ref: ClaimRef {
                            item_id: item.item_id,
                            lease_token: item.lease_token.clone().unwrap(),
                            lease_expires_at: item.lease_expires_at,
                            item_version: item.item_version,
                        },
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: (0..records)
                            .map(|ordinal| SideRecord {
                                key: format!("side-{ordinal:04}").into_bytes(),
                                payload: Bytes::from_static(b"payload"),
                            })
                            .collect(),
                        lifecycle_items: Vec::new(),
                        instance_fence: None,
                    }],
                },
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        backend.inner.lock().unwrap().conn.trace(None);
        SIDE_RECORD_TRACE_COUNT.load(Ordering::Relaxed)
    }

    async fn grouped_claim_finalize_statement_count(groups: usize) -> (usize, usize) {
        use pqueue_engine::{FinalizeKind, FinalizeOutcome, FinalizePort};

        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                (0..groups)
                    .map(|ordinal| PushSpec {
                        group_key: Some(GroupKey::new(format!("group-{ordinal:04}")).unwrap()),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();

        GROUP_CLAIM_TRACE_COUNT.store(0, Ordering::Relaxed);
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .trace(Some(count_group_claim_statement));
        let claimed = backend
            .claim(ClaimRequest {
                eligibility_time: None,
                shard: shard.clone(),
                worker_id: pqueue_core::WorkerId::new("worker").unwrap(),
                max_items: groups,
                lease_token: LeaseToken::new("lease").unwrap(),
                lease_expires_at: UtcTimestamp::new(60, 0).unwrap(),
                now: UtcTimestamp::new(0, 0).unwrap(),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        backend.inner.lock().unwrap().conn.trace(None);
        assert_eq!(claimed.items.len(), groups);
        let claim_count = GROUP_CLAIM_TRACE_COUNT.load(Ordering::Relaxed);

        GROUP_FINALIZE_TRACE_COUNT.store(0, Ordering::Relaxed);
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .trace(Some(count_group_finalize_statement));
        backend
            .finalize(
                &shard,
                claimed
                    .items
                    .iter()
                    .map(|item| FinalizeOutcome::new(item.item_id, FinalizeKind::Complete))
                    .collect(),
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        backend.inner.lock().unwrap().conn.trace(None);
        (
            claim_count,
            GROUP_FINALIZE_TRACE_COUNT.load(Ordering::Relaxed),
        )
    }

    #[tokio::test]
    async fn grouped_push_statement_count_is_independent_of_distinct_groups() {
        let one = grouped_push_statement_count(1).await;
        let hundred = grouped_push_statement_count(100).await;
        let thousand = grouped_push_statement_count(1_000).await;
        assert_eq!((one, hundred, thousand), (one, one, one));
    }

    #[tokio::test]
    async fn grouped_claim_and_finalize_statements_are_independent_of_distinct_groups() {
        let one = grouped_claim_finalize_statement_count(1).await;
        let hundred = grouped_claim_finalize_statement_count(100).await;
        let thousand = grouped_claim_finalize_statement_count(1_000).await;
        assert_eq!((one, hundred, thousand), (one, one, one));
    }

    #[tokio::test]
    async fn counter_restore_statement_count_is_independent_of_resident_items() {
        let one = counter_restore_statement_count(1).await;
        let hundred = counter_restore_statement_count(100).await;
        let thousand = counter_restore_statement_count(1_000).await;
        assert_eq!((one, hundred, thousand), (one, one, one));
        assert_eq!(
            one, 1,
            "counter recovery reads only durable queue high-waters"
        );
    }

    #[tokio::test]
    async fn live_item_batch_read_uses_one_set_query_at_1_100_1000() {
        assert_eq!(
            (
                live_items_statement_count(1).await,
                live_items_statement_count(100).await,
                live_items_statement_count(1_000).await,
            ),
            (1, 1, 1)
        );
    }

    #[tokio::test]
    async fn gate_batch_write_uses_bounded_sql_chunks_at_1_100_1000() {
        assert_eq!(
            (
                set_gates_statement_count(1).await,
                set_gates_statement_count(100).await,
                set_gates_statement_count(1_000).await,
            ),
            (1, 1, 2)
        );
    }

    #[tokio::test]
    async fn side_record_batch_write_uses_bounded_sql_chunks_at_1_100_1000() {
        assert_eq!(
            (
                side_record_statement_count(1).await,
                side_record_statement_count(100).await,
                side_record_statement_count(1_000).await,
            ),
            (1, 1, 3)
        );
    }

    #[tokio::test]
    async fn bounded_mutation_statement_count_is_independent_of_match_count() {
        let one = mutation_statement_count(1).await;
        let hundred = mutation_statement_count(100).await;
        let thousand = mutation_statement_count(1_000).await;
        assert_eq!((one, hundred, thousand), (one, one, one));
    }

    #[tokio::test]
    async fn bounded_mutation_keyset_loops_past_each_internal_scan_page() {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                (0..250)
                    .map(|_| PushSpec {
                        entity: Some(serde_json::json!({"status":"ready"})),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let response = backend
            .bounded_mutation(
                &shard,
                BoundedMutationRequest {
                    index: Some("by_status".into()),
                    filters: vec![QueryFilter {
                        field: "status".into(),
                        op: FilterOp::Eq,
                        value: TypedValue::String("ready".into()),
                    }],
                    set_fields: BTreeMap::from([(
                        "status".into(),
                        TypedValue::String("done".into()),
                    )]),
                    max_scan_rows: 17,
                },
            )
            .await
            .unwrap();
        assert_eq!(response.results.len(), 250);
        assert!(
            response
                .results
                .iter()
                .all(|result| result.outcome == MutationOutcome::Updated)
        );
    }

    #[tokio::test]
    async fn bounded_mutation_cas_loser_keeps_old_index_rows() {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let ids = backend
            .push(
                &shard,
                (0..2)
                    .map(|_| PushSpec {
                        entity: Some(serde_json::json!({"status":"ready"})),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let loser = ids[1];
        backend
            .inner
            .lock()
            .unwrap()
            .conn
            .execute_batch(&format!(
                "CREATE TEMP TRIGGER skip_hot_mutation BEFORE UPDATE ON pqueue_items \
             WHEN OLD.item_id='{}' BEGIN SELECT RAISE(IGNORE); END;",
                loser
            ))
            .unwrap();
        let response = backend
            .bounded_mutation(
                &shard,
                BoundedMutationRequest {
                    index: Some("by_status".into()),
                    filters: vec![QueryFilter {
                        field: "status".into(),
                        op: FilterOp::Eq,
                        value: TypedValue::String("ready".into()),
                    }],
                    set_fields: BTreeMap::from([(
                        "status".into(),
                        TypedValue::String("done".into()),
                    )]),
                    max_scan_rows: 10,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .results
                .iter()
                .find(|result| result.item_id == loser)
                .unwrap()
                .outcome,
            MutationOutcome::Conflict
        );
        let key = axon_esf::encode_compound_index_key(&[(
            &JsonValue::String("ready".into()),
            &IndexType::String,
        )])
        .unwrap()
        .unwrap();
        let retained: i64 = backend.inner.lock().unwrap().conn.query_row(
            "SELECT COUNT(*) FROM pqueue_item_index WHERE tenant_id='tenant' AND queue_id='queue' AND index_name='by_status' AND item_id=?1 AND index_key=?2",
            params![loser.to_string(), key], |row| row.get(0),
        ).unwrap();
        assert_eq!(retained, 1);
    }

    #[tokio::test]
    async fn range_cursor_rejects_a_deleted_anchor() {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                (0..3)
                    .map(|_| PushSpec {
                        entity: Some(serde_json::json!({"status":"ready"})),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let request = RangeScanRequest {
            index: Some("by_status".into()),
            filters: vec![QueryFilter {
                field: "status".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("ready".into()),
            }],
            order_by: vec![pqueue_core::OrderField {
                field: "status".into(),
                direction: pqueue_core::SortDirection::Ascending,
            }],
            page_size: 1,
            cursor: None,
        };
        let first = backend.range_scan(&shard, request.clone()).await.unwrap();
        let cursor = first.next_cursor.clone().unwrap();
        let state: RangeScanCursorState = serde_json::from_str(&cursor.0).unwrap();
        backend.inner.lock().unwrap().conn.execute(
            "DELETE FROM pqueue_item_index WHERE tenant_id='tenant' AND queue_id='queue' AND index_name='by_status' AND item_id=?1",
            params![state.anchor_item_id.to_string()],
        ).unwrap();
        assert!(matches!(
            backend
                .range_scan(
                    &shard,
                    RangeScanRequest {
                        cursor: Some(cursor),
                        ..request
                    }
                )
                .await,
            Err(EngineError::Invalid("cursor-invalidated"))
        ));
    }

    #[test]
    fn invalid_index_shapes_are_rejected_before_sql() {
        let spec = QueueIndex {
            name: "compound".into(),
            declaration: IndexDeclaration::Compound(pqueue_core::CompoundIndexDef {
                fields: vec![
                    pqueue_core::CompoundIndexField {
                        field: "a".into(),
                        index_type: IndexType::String,
                    },
                    pqueue_core::CompoundIndexField {
                        field: "b".into(),
                        index_type: IndexType::Integer,
                    },
                    pqueue_core::CompoundIndexField {
                        field: "c".into(),
                        index_type: IndexType::Integer,
                    },
                ],
                unique: false,
            }),
        };
        let gap = vec![
            QueryFilter {
                field: "a".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("x".into()),
            },
            QueryFilter {
                field: "c".into(),
                op: FilterOp::Eq,
                value: TypedValue::Integer(1),
            },
        ];
        assert!(matches!(
            hot_query_shape(&spec, &gap),
            Err(EngineError::Invalid(_))
        ));
        let shape = hot_query_shape(
            &spec,
            &[QueryFilter {
                field: "a".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("x".into()),
            }],
        )
        .unwrap();
        assert!(matches!(
            validate_hot_query_order(
                &spec,
                &shape,
                &[pqueue_core::OrderField {
                    field: "c".into(),
                    direction: pqueue_core::SortDirection::Ascending,
                }]
            ),
            Err(EngineError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn aggregate_ports_reject_unresolvable_predicate_shapes() {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let definition = mutation_queue();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let filters = vec![QueryFilter {
            field: "not_declared".into(),
            op: FilterOp::Eq,
            value: TypedValue::String("x".into()),
        }];
        assert!(matches!(
            backend
                .metrics_by_query(
                    &shard,
                    MetricsByQueryRequest {
                        index: Some("by_status".into()),
                        filters: filters.clone(),
                    }
                )
                .await,
            Err(EngineError::Invalid(_))
        ));
        assert!(matches!(
            backend
                .grouped_aggregate(
                    &shard,
                    GroupedAggregateRequest {
                        index: Some("by_status".into()),
                        filters,
                        group_by: vec![pqueue_core::GroupByField {
                            field: "status".into(),
                            time_bucket: None
                        }],
                        max_groups: 10,
                    }
                )
                .await,
            Err(EngineError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn bounded_mutation_preserves_unique_index_conflicts_per_record() {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let mut definition = mutation_queue();
        definition.typed_indexes.push(QueueIndex {
            name: "by_code".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "code".into(),
                index_type: IndexType::String,
                unique: true,
            }),
        });
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                ["a", "b", "c"]
                    .into_iter()
                    .map(|code| PushSpec {
                        entity: Some(serde_json::json!({"status":"ready","code":code})),
                        ..PushSpec::default()
                    })
                    .collect(),
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let response = backend
            .bounded_mutation(
                &shard,
                BoundedMutationRequest {
                    index: Some("by_status".into()),
                    filters: vec![QueryFilter {
                        field: "status".into(),
                        op: FilterOp::Eq,
                        value: TypedValue::String("ready".into()),
                    }],
                    set_fields: BTreeMap::from([(
                        "code".into(),
                        TypedValue::String("shared".into()),
                    )]),
                    max_scan_rows: 10,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .results
                .iter()
                .filter(|result| result.outcome == MutationOutcome::Updated)
                .count(),
            1
        );
        assert_eq!(
            response
                .results
                .iter()
                .filter(|result| result.outcome == MutationOutcome::Conflict)
                .count(),
            2
        );
        let inner = backend.inner.lock().unwrap();
        let key = axon_esf::encode_compound_index_key(&[(
            &JsonValue::String("shared".into()),
            &IndexType::String,
        )])
        .unwrap()
        .unwrap();
        let holders: i64 = inner.conn.query_row(
            "SELECT COUNT(*) FROM pqueue_item_index WHERE tenant_id=?1 AND queue_id=?2 AND index_name='by_code' AND index_key=?3",
            params!["tenant", "queue", key], |row| row.get(0),
        ).unwrap();
        assert_eq!(holders, 1);
    }

    #[tokio::test]
    async fn sparse_numeric_index_null_bucket_uses_authoritative_population() {
        let backend = SqliteRelationalBackend::in_memory().unwrap();
        let mut definition = mutation_queue();
        definition.typed_indexes.push(QueueIndex {
            name: "by_score".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "score".into(),
                index_type: IndexType::Float,
                unique: false,
            }),
        });
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                vec![
                    PushSpec {
                        entity: Some(serde_json::json!({"status":"ready","score":0.5})),
                        ..PushSpec::default()
                    },
                    PushSpec {
                        entity: Some(serde_json::json!({"status":"ready"})),
                        ..PushSpec::default()
                    },
                ],
                UtcTimestamp::new(0, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let response = backend
            .declared_bucket_segment(
                &shard,
                DeclaredBucketSegmentRequest {
                    index: Some("by_score".into()),
                    filters: vec![QueryFilter {
                        field: "status".into(),
                        op: FilterOp::Eq,
                        value: TypedValue::String("ready".into()),
                    }],
                    field: "score".into(),
                    buckets: vec![pqueue_core::BucketRule {
                        label: "half".into(),
                        exact: Some(0.5),
                        gt: None,
                        gte: None,
                        lt: None,
                        lte: None,
                    }],
                    null_bucket_label: "missing".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .buckets
                .iter()
                .find(|bucket| bucket.label == "half")
                .unwrap()
                .count,
            1
        );
        assert_eq!(
            response
                .buckets
                .iter()
                .find(|bucket| bucket.label == "missing")
                .unwrap()
                .count,
            1
        );
        assert!(matches!(
            backend
                .declared_bucket_segment(
                    &shard,
                    DeclaredBucketSegmentRequest {
                        index: Some("by_score".into()),
                        filters: vec![],
                        field: "score".into(),
                        buckets: vec![pqueue_core::BucketRule {
                            label: "half".into(),
                            exact: Some(0.5),
                            gt: None,
                            gte: None,
                            lt: None,
                            lte: None,
                        }],
                        null_bucket_label: "missing".into(),
                    },
                )
                .await,
            Err(EngineError::Invalid(
                "no declared index covers the bucket base population"
            ))
        ));
    }

    #[test]
    fn high_cardinality_matches_stop_at_the_sql_limit() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE pqueue_items (tenant_id TEXT,queue_id TEXT,item_id TEXT,entity_document TEXT,fields TEXT,item_version INTEGER,lifecycle_state TEXT,fenced INTEGER,superseded INTEGER,PRIMARY KEY(tenant_id,queue_id,item_id));
             CREATE TABLE pqueue_item_index (tenant_id TEXT,queue_id TEXT,index_name TEXT,index_key BLOB,item_id TEXT,PRIMARY KEY(tenant_id,queue_id,index_name,item_id));
             CREATE INDEX pqueue_item_index_key_item_asc_idx ON pqueue_item_index(tenant_id,queue_id,index_name,index_key,item_id);",
        ).unwrap();
        let key = axon_esf::encode_compound_index_key(&[(
            &JsonValue::String("ready".into()),
            &IndexType::String,
        )])
        .unwrap()
        .unwrap();
        let tx = conn.transaction().unwrap();
        for ordinal in 0..20_000_u32 {
            let item_id = ItemId::mint(1, 0, ordinal).to_string();
            tx.execute("INSERT INTO pqueue_items VALUES('tenant','queue',?1,'{\"status\":\"ready\"}','{}',1,'Pending',0,0)", params![item_id]).unwrap();
            tx.execute(
                "INSERT INTO pqueue_item_index VALUES('tenant','queue','by_status',?1,?2)",
                params![key, item_id],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let steps = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&steps);
        conn.progress_handler(
            1,
            Some(move || {
                observed.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        let rows = hot_query_candidate_page(
            &conn,
            &QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
            &QueueIndex {
                name: "by_status".into(),
                declaration: IndexDeclaration::Single(IndexDef {
                    field: "status".into(),
                    index_type: IndexType::String,
                    unique: false,
                }),
            },
            &[QueryFilter {
                field: "status".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("ready".into()),
            }],
            pqueue_core::SortDirection::Ascending,
            None,
            33,
            true,
        )
        .unwrap();
        conn.progress_handler(0, None::<fn() -> bool>);
        assert_eq!(rows.len(), 33);
        assert!(
            steps.load(Ordering::Relaxed) < 10_000,
            "{}",
            steps.load(Ordering::Relaxed)
        );
    }

    fn grouped_refresh_cost(groups: usize) -> usize {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE pqueue_items(tenant_id TEXT,queue_id TEXT,item_id TEXT,group_key TEXT,lifecycle_state TEXT,superseded INTEGER,not_before INTEGER,eligible_since INTEGER,priority_sort BLOB,created_at INTEGER,created_seq INTEGER,PRIMARY KEY(tenant_id,queue_id,item_id));
             CREATE TABLE pqueue_item_gates(tenant_id TEXT,queue_id TEXT,item_id TEXT,gate_key TEXT);
             CREATE TABLE pqueue_gate_state(tenant_id TEXT,queue_id TEXT,gate_key TEXT);
             CREATE TABLE pqueue_group_summary(tenant_id TEXT,queue_id TEXT,group_key TEXT,oldest_eligible_at INTEGER,rep_progress_guard_sort BLOB,rep_priority_sort BLOB,rep_created_at INTEGER,rep_item_id TEXT,eligible_item_count INTEGER NOT NULL,at_risk_count INTEGER NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(tenant_id,queue_id,group_key));
             CREATE INDEX pqueue_items_group_due_idx ON pqueue_items(tenant_id,queue_id,lifecycle_state,group_key,not_before,priority_sort,created_seq);",
        ).unwrap();
        conn.trace(Some(count_group_statement));
        let steps = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&steps);
        conn.progress_handler(
            1,
            Some(move || {
                observed.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        let tx = conn.transaction().unwrap();
        let mut keys = Vec::new();
        for ordinal in 0..groups {
            let group = format!("group-{ordinal:04}");
            tx.execute(
                "INSERT INTO pqueue_items VALUES('tenant','queue',?1,?2,'Pending',0,NULL,0,X'00',0,?3)",
                params![ordinal.to_string(), group, ordinal as i64],
            ).unwrap();
            keys.push(GroupKey::new(group).unwrap());
        }
        GROUP_TRACE_COUNT.store(0, Ordering::Relaxed);
        steps.store(0, Ordering::Relaxed);
        refresh_group_summaries(
            &tx,
            &QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
            &keys,
            UtcTimestamp::new(1, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(GROUP_TRACE_COUNT.load(Ordering::Relaxed), 1);
        let summary_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM pqueue_group_summary WHERE eligible_item_count=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(summary_count as usize, groups);
        steps.load(Ordering::Relaxed)
    }

    #[test]
    fn grouped_summary_refresh_has_constant_statements_and_linear_vm_work() {
        let one = grouped_refresh_cost(1);
        let hundred = grouped_refresh_cost(100);
        let thousand = grouped_refresh_cost(1_000);
        assert!(
            hundred < one.saturating_mul(200).max(100_000),
            "{one}/{hundred}"
        );
        assert!(
            thousand < hundred.saturating_mul(20).max(1_000_000),
            "{hundred}/{thousand}"
        );
    }

    #[test]
    fn indexed_candidate_seek_ignores_a_large_nonmatching_inventory() {
        let mut conn = Connection::open_in_memory().expect("sqlite");
        conn.execute_batch(
            "CREATE TABLE pqueue_items (
                tenant_id TEXT,queue_id TEXT,item_id TEXT,entity_document TEXT,fields TEXT,
                item_version INTEGER,lifecycle_state TEXT,fenced INTEGER,superseded INTEGER,
                PRIMARY KEY(tenant_id,queue_id,item_id));
             CREATE TABLE pqueue_item_index (
                tenant_id TEXT,queue_id TEXT,index_name TEXT,index_key BLOB,item_id TEXT,
                PRIMARY KEY(tenant_id,queue_id,index_name,item_id));
             CREATE INDEX pqueue_item_index_key_item_asc_idx ON pqueue_item_index
                (tenant_id,queue_id,index_name,index_key,item_id);
             WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n WHERE v<100000)
             INSERT INTO pqueue_item_index
                SELECT 'tenant','queue','by_status',X'000000056F74686572',printf('noise-%06d',v) FROM n;",
        )
        .expect("seed nonmatches");
        let spec = QueueIndex {
            name: "by_status".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "status".into(),
                index_type: IndexType::String,
                unique: false,
            }),
        };
        let target = JsonValue::String("ready".into());
        let key = axon_esf::encode_compound_index_key(&[(&target, &IndexType::String)])
            .expect("key")
            .expect("present");
        let tx = conn.transaction().expect("seed transaction");
        for ordinal in 0..32 {
            let item_id = ItemId::mint(1, 0, ordinal).to_string();
            tx.execute(
                "INSERT INTO pqueue_items VALUES(?1,?2,?3,?4,'{}',1,'Pending',0,0)",
                params!["tenant", "queue", item_id, r#"{"status":"ready"}"#],
            )
            .expect("item");
            tx.execute(
                "INSERT INTO pqueue_item_index VALUES(?1,?2,?3,?4,?5)",
                params!["tenant", "queue", spec.name, key, item_id],
            )
            .expect("index row");
        }
        tx.commit().expect("seed commit");

        let shape = hot_query_shape(
            &spec,
            &[QueryFilter {
                field: "status".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("ready".into()),
            }],
        )
        .expect("shape");
        let plan = conn
            .prepare(
                "EXPLAIN QUERY PLAN SELECT x.index_key,i.item_id,i.entity_document,i.fields,\
                i.item_version,i.lifecycle_state,i.fenced,i.superseded FROM pqueue_item_index x \
                INDEXED BY pqueue_item_index_key_item_asc_idx JOIN pqueue_items i ON \
                i.tenant_id=x.tenant_id AND i.queue_id=x.queue_id AND i.item_id=x.item_id \
                WHERE x.tenant_id=?1 AND x.queue_id=?2 AND x.index_name=?3 AND x.index_key>=?4 \
                AND x.index_key<?5 ORDER BY x.index_key,x.item_id LIMIT 1000",
            )
            .expect("plan")
            .query_map(
                params!["tenant", "queue", spec.name, shape.lower, shape.upper],
                |row| row.get::<_, String>(3),
            )
            .expect("plan rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("plan details")
            .join("\n");
        assert!(
            plan.contains("pqueue_item_index_key_item_asc_idx"),
            "{plan}"
        );
        assert!(!plan.contains("SCAN i"), "{plan}");

        let vm_steps = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&vm_steps);
        conn.progress_handler(
            1,
            Some(move || {
                observed.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        let shard = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        );
        let rows = hot_query_candidates(
            &conn,
            &shard,
            &spec,
            &[QueryFilter {
                field: "status".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("ready".into()),
            }],
        )
        .expect("seek");
        conn.progress_handler(0, None::<fn() -> bool>);
        assert_eq!(rows.len(), 32);
        assert!(vm_steps.load(Ordering::Relaxed) < 20_000);
    }
}
