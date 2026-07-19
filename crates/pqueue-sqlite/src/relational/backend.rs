use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    BoundedMutationRequest, BoundedMutationResponse, ClaimByQueryRequest, ClientItemKey, CohortId,
    DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, FilterOp, GroupKey,
    GroupedAggregateRequest, GroupedAggregateResponse, IndexDeclaration, IndexType, ItemId,
    ItemState, LeaseToken, Metadata, MetricsByQueryRequest, MutationOutcome, MutationResult,
    PriorityValue,
    QueryCapabilityFlags, QueryCursor, QueueDefinition, QueueId, RangeScanRequest,
    RangeScanResponse, RangeScanRow, RequestId, TenantId, TypedValue, UtcTimestamp,
};
use pqueue_engine::ClaimUnit;
use pqueue_engine::ProjectionStore;
use pqueue_engine::TerminalEmissionMetrics;
use pqueue_engine::{
    ActiveScope, AdvanceInstanceFenceCommand, Backend, ClaimCommand, ClaimCompatibility, ClaimPort,
    ClaimRequest, Claimed, ClaimedItem, CohortClaimCommand, CohortExpiredCommand,
    CohortFinalizeCommand, CohortFinalizePort, CohortLeaseTarget, CohortRenewLeaseCommand,
    CohortRenewLeasePort, CommandEnvelope, CommandPosition, CommitCapabilities, CommitEntryOutcome,
    CommitEntryStatus, CommitRecovery, CommitTransition, ControlPlaneStore, CreateQueueOutcome,
    DiscoveryGranularity, DiscoveryPort, DurabilityClass, EngineError, EngineResult, EntryRecovery,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort, IndexHit, IndexQueryPort,
    ItemView, LeaseExpiredCommand, LeaseView, LiveItemView, LogWriter, PayloadUpdate,
    ProjectionRead, ProjectionWriter, PurgeItemsCommand, PurgePort, PushCommand, PushItem,
    PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey, QueueMetrics, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort, RenewLeaseCommand,
    RenewLeasePort, ReplacePendingCommand, SetGatesCommand, SetGatesPort, TickReport,
    UpdateFieldsCommand, UpdateFieldsPort, UpsertOutcome, UpsertPort, WriteSideRecordsCommand,
    build_push_items, validate_api001_reserved_write_fields, validate_claim_compatibility,
    validate_entity, validate_gate_push, validate_instance_fence, validate_purge_force,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::Value as JsonValue;

use super::*;

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

    /// Restart recovery: seed the per-queue mint counter past every id already in `pqueue_items`, so a push
    /// after reopen never re-mints an existing item id (the durable items table is the authority — there is
    /// no log to replay). `observe` decodes `(epoch, counter)` from each packed id and only advances.
    fn restore_counters(&self) -> EngineResult<()> {
        let g = self.inner.lock().expect("poisoned");
        let mut stmt = st(g
            .conn
            .prepare("SELECT tenant_id, queue_id, item_id FROM pqueue_items"))?;
        let rows = st(stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        }))?;
        for r in rows {
            let (t, q, id) = st(r)?;
            let key = QueueKey::new(
                TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
            );
            let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
            self.counters.observe(&key, item_id);
        }
        // Terminal-item reaping (`reap_terminal_items_sql`) deletes rows, so the scan above is no longer the
        // complete minted set; also restore the durable mint-counter floor for every queue, or a reopen after
        // a full reap could re-mint a reaped id (ADR-009). Inert when no reap has advanced the floor.
        observe_all_id_high_water_sql(&g.conn, &self.counters)
    }
}

// --- Backend::write unit of work (disjoint borrows: tx over conn, &mut live-token map, &queues) -------

struct RelLogWriter<'a> {
    tx: &'a Transaction<'a>,
}

impl LogWriter for RelLogWriter<'_> {
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

struct RelProjectionWriter<'a> {
    tx: &'a Transaction<'a>,
    queues: &'a HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &'a mut HashSet<QueueKey>,
    claim_scan_hints: &'a mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &'a mut HashMap<QueueKey, bool>,
    /// Token mutations accumulate here and are replayed onto the live map by `write` AFTER commit (F4).
    token_ops: &'a mut Vec<TokenOp>,
}

impl ProjectionWriter for RelProjectionWriter<'_> {
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

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        let result = (|| {
            let mut guard = self.inner.lock().expect("relational backend poisoned");
            let Inner {
                conn,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                live_tokens,
                ..
            } = &mut *guard;
            let tx = st(conn.transaction())?;
            let mut token_ops = Vec::new();
            let r = {
                let mut lw = RelLogWriter { tx: &tx };
                let mut pw = RelProjectionWriter {
                    tx: &tx,
                    queues,
                    grouped_shards,
                    claim_scan_hints,
                    claim_scan_default_fifo,
                    token_ops: &mut token_ops,
                };
                f(&mut lw, &mut pw)?
            };
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
            Ok(r)
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

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            render_claimed(&g.conn, shard, ids, |id| g.live_tokens.get(id).cloned())
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
            apply_token_ops(live_tokens, token_ops);
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
            apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
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
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };
                if let Err(e) = commit_validate_sql(&tx, shard, &entry.claim_ref, now) {
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
                        outcomes: vec![FinalizeOutcome::new(
                            entry.claim_ref.item_id,
                            entry.finalize,
                        )],
                    }),
                    &mut token_ops,
                )?;
                recovery.push(EntryRecovery {
                    consumed_input_id,
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
            apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
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

/// Hot projection query substrate (API-004) is not implemented for any backend in epic pqueue-45e13e4d;
/// the sqlite-relational family inherits the all-`Unavailable` default except for the range-scan slice
/// wired in this bead.
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

            let (t, q) = parts(shard);
            let mut stmt = st(g.conn.prepare(
                "SELECT item_id, entity_document FROM pqueue_items \
                 WHERE tenant_id=?1 AND queue_id=?2",
            ))?;
            let rows = st(stmt.query_map(params![t, q], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            }))?;

            let mut matched = Vec::new();
            for row in rows {
                let (item_id, entity_json): (String, Option<String>) = st(row)?;
                let item_id =
                    ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?;
                let Some(entity_json) = entity_json else {
                    continue;
                };
                let entity: JsonValue = serde_json::from_str(&entity_json)
                    .map_err(|e| EngineError::Storage(e.to_string()))?;
                let mut fields_map = BTreeMap::new();
                match &spec.declaration {
                    IndexDeclaration::Single(def) => {
                        let Some(value) = typed_value_for_json(
                            entity.get(&def.field).unwrap_or(&JsonValue::Null),
                            &def.index_type,
                        )?
                        else {
                            continue;
                        };
                        fields_map.insert(def.field.clone(), value);
                    }
                    IndexDeclaration::Compound(def) => {
                        let mut missing = false;
                        for field in &def.fields {
                            let Some(value) = typed_value_for_json(
                                entity.get(&field.field).unwrap_or(&JsonValue::Null),
                                &field.index_type,
                            )?
                            else {
                                missing = true;
                                break;
                            };
                            fields_map.insert(field.field.clone(), value);
                        }
                        if missing {
                            continue;
                        }
                    }
                }
                let row = RangeScanRow {
                    item_id,
                    fields: fields_map,
                };

                let mut accepted = true;
                let mut prefix_len = 0usize;
                for (field_name, index_type) in &fields {
                    let Some(filter) = request
                        .filters
                        .iter()
                        .find(|filter| filter.field == *field_name)
                    else {
                        break;
                    };
                    if filter.op != FilterOp::Eq {
                        break;
                    }
                    let typed = typed_value_from_filter_value(&filter.value, index_type)?;
                    let Some(value) = row.fields.get(*field_name) else {
                        accepted = false;
                        break;
                    };
                    if !typed_value_matches_query(value, &typed) {
                        accepted = false;
                        break;
                    }
                    prefix_len += 1;
                }
                if accepted {
                    for filter in &request.filters {
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
                            accepted = false;
                            break;
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
                            accepted = false;
                            break;
                        }
                    }
                }
                if accepted {
                    matched.push(row);
                }
            }

            matched.sort_by(|lhs, rhs| {
                compare_rows(lhs, rhs, &request.order_by).expect("typed order compare")
            });

            let start = if let Some(state) = &cursor_state {
                let anchor = matched
                    .iter()
                    .position(|row| row.item_id == state.anchor_item_id)
                    .ok_or(EngineError::Invalid("cursor-invalidated"))?;
                let current = &matched[anchor];
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

            let page_rows = matched
                .iter()
                .skip(start)
                .take(request.page_size as usize)
                .cloned()
                .collect::<Vec<_>>();
            let next_cursor = if start + page_rows.len() < matched.len() {
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
                    live_tokens.insert(*item_id, replay.lease_token.clone());
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
            if request.order_by.field.is_empty() {
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
            if !fields
                .iter()
                .any(|(field, _)| *field == request.order_by.field.as_str())
            {
                return Err(EngineError::Invalid("unindexed-field"));
            }

            let paused = queue_paused(&tx, shard)?;
            let mut matched = Vec::new();
            let mut stmt = (!paused)
                .then(|| {
                    tx.prepare(
                        "SELECT item_id, entity_document, item_version FROM pqueue_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' \
                 AND superseded=0 AND fenced=0 AND cohort_size IS NULL \
                 AND (not_before IS NULL OR not_before<=?3) AND eligible_since IS NOT NULL \
                 AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                     ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
                     AND gs.gate_key=ig.gate_key \
                     WHERE ig.tenant_id=pqueue_items.tenant_id \
                     AND ig.queue_id=pqueue_items.queue_id \
                     AND ig.item_id=pqueue_items.item_id)",
                    )
                })
                .transpose()
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            if let Some(stmt) = stmt.as_mut() {
                let rows = st(stmt.query_map(params![t, q, eligibility_nanos], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }))?;

                for row in rows {
                    let (item_id, entity_json, item_version): (String, Option<String>, i64) =
                        st(row)?;
                    let item_id =
                        ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?;
                    let Some(entity_json) = entity_json else {
                        continue;
                    };
                    let entity: JsonValue = serde_json::from_str(&entity_json)
                        .map_err(|e| EngineError::Storage(e.to_string()))?;
                    let mut fields_map = BTreeMap::new();
                    match &spec.declaration {
                        IndexDeclaration::Single(def) => {
                            let Some(value) = typed_value_for_json(
                                entity.get(&def.field).unwrap_or(&JsonValue::Null),
                                &def.index_type,
                            )?
                            else {
                                continue;
                            };
                            fields_map.insert(def.field.clone(), value);
                        }
                        IndexDeclaration::Compound(def) => {
                            let mut missing = false;
                            for field in &def.fields {
                                let Some(value) = typed_value_for_json(
                                    entity.get(&field.field).unwrap_or(&JsonValue::Null),
                                    &field.index_type,
                                )?
                                else {
                                    missing = true;
                                    break;
                                };
                                fields_map.insert(field.field.clone(), value);
                            }
                            if missing {
                                continue;
                            }
                        }
                    }
                    let row = RangeScanRow {
                        item_id,
                        fields: fields_map,
                    };

                    let mut accepted = true;
                    let mut prefix_len = 0usize;
                    for (field_name, index_type) in &fields {
                        let Some(filter) = request
                            .filters
                            .iter()
                            .find(|filter| filter.field == *field_name)
                        else {
                            break;
                        };
                        if filter.op != FilterOp::Eq {
                            break;
                        }
                        let typed = typed_value_from_filter_value(&filter.value, index_type)?;
                        let Some(value) = row.fields.get(*field_name) else {
                            accepted = false;
                            break;
                        };
                        if !typed_value_matches_query(value, &typed) {
                            accepted = false;
                            break;
                        }
                        prefix_len += 1;
                    }
                    if accepted {
                        for filter in &request.filters {
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
                                accepted = false;
                                break;
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
                                accepted = false;
                                break;
                            }
                        }
                    }
                    if accepted {
                        matched.push((row, item_version));
                    }
                }
            }

            matched.sort_by(|lhs, rhs| {
                compare_rows(&lhs.0, &rhs.0, std::slice::from_ref(&request.order_by))
                    .expect("typed order compare")
            });

            let selected: Vec<(ItemId, i64)> = matched
                .into_iter()
                .take(request.max_items as usize)
                .map(|(row, version)| (row.item_id, version))
                .collect();

            drop(stmt);
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
            let mut claimed_ids = Vec::with_capacity(selected.len());
            for (item_id, expected_version) in selected {
                let changed = st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?1, \
                     lease_expires_at=?2, worker_id=?3, retry_count=retry_count+1, \
                     item_version=item_version+1, updated_at=?4, last_command_sequence=?5 \
                     WHERE tenant_id=?6 AND queue_id=?7 AND item_id=?8 AND item_version=?9 \
                     AND lifecycle_state='Pending' AND superseded=0 AND fenced=0 \
                     AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?10) \
                     AND eligible_since IS NOT NULL \
                     AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                         ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
                         AND gs.gate_key=ig.gate_key \
                         WHERE ig.tenant_id=pqueue_items.tenant_id \
                         AND ig.queue_id=pqueue_items.queue_id \
                         AND ig.item_id=pqueue_items.item_id)",
                    params![
                        hash,
                        lease_expires_nanos,
                        request.worker_id.as_str(),
                        ts_nanos(created_at),
                        seq,
                        t,
                        q,
                        item_id.to_string(),
                        expected_version,
                        eligibility_nanos,
                    ],
                ))?;
                if changed == 1 {
                    token_ops.push(TokenOp::Set(item_id, lease_token.clone()));
                    claimed_ids.push(item_id);
                }
            }
            let mut positions = Vec::new();
            if !claimed_ids.is_empty() {
                let groups = groups_of(&tx, shard, &claimed_ids)?;
                if !groups.is_empty() {
                    grouped_shards.insert(shard.clone());
                }
                for group in &groups {
                    refresh_group_summary(&tx, shard, group, created_at)?;
                }
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
            apply_token_ops(live_tokens, token_ops);
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
            let image = export_projection_image_sql(&g.conn, shard)?;
            query_projection_image(&definition, image, |projection, shard| {
                projection.grouped_aggregate(shard, request)
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
            let image = export_projection_image_sql(&g.conn, shard)?;
            query_projection_image(&definition, image, |projection, _shard| {
                projection.metrics_by_query(request)
            })
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
            let image = export_projection_image_sql(&g.conn, shard)?;
            query_projection_image(&definition, image, |projection, shard| {
                projection.declared_bucket_segment(shard, request)
            })
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
            if request
                .filters
                .iter()
                .any(|filter| match &spec.declaration {
                    IndexDeclaration::Single(def) => filter.field != def.field,
                    IndexDeclaration::Compound(def) => {
                        !def.fields.iter().any(|field| field.field == filter.field)
                    }
                })
            {
                return Err(EngineError::Invalid("unindexed-field"));
            }

            let (t, q) = parts(shard);
            let mut matches = {
                let mut stmt = st(g.conn.prepare(
                    "SELECT item_id,lifecycle_state,fenced,superseded,entity_document \
                     FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2",
                ))?;
                let rows = st(stmt.query_map(params![t, q], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                }))?;
                let mut matches = Vec::new();
                for row in rows {
                    let (item_id, lifecycle_state, fenced, superseded, entity_json) = st(row)?;
                    let item_id =
                        ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?;
                    let entity_json = match entity_json {
                        Some(raw) => serde_json::from_str::<JsonValue>(&raw)
                            .map_err(|e| EngineError::Storage(e.to_string()))?,
                        None => continue,
                    };
                    let row = typed_index_row_from_entity(spec, item_id, &entity_json)?;
                    let Some(row) = row else {
                        continue;
                    };
                    if !typed_index_row_matches(spec, &request.filters, &row)? {
                        continue;
                    }
                    matches.push((
                        item_id,
                        lifecycle_state,
                        fenced != 0,
                        superseded != 0,
                        entity_json,
                    ));
                }
                matches
            };
            matches.sort_by_key(|(item_id, ..)| *item_id);

            let mut results = Vec::with_capacity(matches.len());
            for (item_id, lifecycle_state, fenced, superseded, entity) in matches {
                let outcome = if fenced
                    || superseded
                    || parse_state(&lifecycle_state)?.is_terminal()
                    || parse_state(&lifecycle_state)? != ItemState::Pending
                {
                    MutationOutcome::Conflict
                } else {
                    let new_entity = merge_entity_document(Some(&entity), &request.set_fields)?;
                    validate_entity(g.schemas.get(shard), Some(&new_entity))?;
                    let now = {
                        let d = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default();
                        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid ts")
                    };
                    g.commit_command(
                        shard,
                        QueueCommand::UpdateFields(UpdateFieldsCommand {
                            item_id,
                            field_ops: request
                                .set_fields
                                .iter()
                                .map(|(field, value)| {
                                    serde_json::to_vec(value)
                                        .map(Bytes::from)
                                        .map_err(|e| EngineError::Storage(e.to_string()))
                                        .map(|bytes| (field.clone(), Some(bytes)))
                                })
                                .collect::<EngineResult<BTreeMap<_, _>>>()?,
                            payload: PayloadUpdate::Keep,
                            set_priority: Default::default(),
                            set_not_before: Default::default(),
                            set_entity_document: Some(new_entity),
                        }),
                        now,
                        None,
                    )?;
                    MutationOutcome::Updated
                };
                results.push(MutationResult { item_id, outcome });
            }

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
