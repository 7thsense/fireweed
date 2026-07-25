use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse,
    GroupedAggregateRequest, GroupedAggregateResponse, ItemId, ItemState, LeaseToken,
    MetricsByQueryRequest, QueryCapabilityFlags, QueueDefinition, RangeScanRequest,
    RangeScanResponse, RequestId, UtcTimestamp,
};
use fireweed_engine::ClaimUnit;
use fireweed_engine::TerminalEmissionMetrics;
use fireweed_engine::{
    ActiveScope, AsOfProjectionStore, BatchUpdateItemRef, BatchUpdateSnapshotItem,
    ClaimCompatibility, ClaimRef, ClaimedItem, CommandEnvelope, CommandPosition,
    DiscoveryGranularity, DurabilityClass, EngineError, EngineResult, FinalizeOutcome, IndexHit,
    ItemView, LeaseView, LiveItemView, PendingPage, PendingSummary, PushItem, PushSpec, QueueKey,
    QueueMetrics, UpdateFieldsCommand,
};
use fireweed_engine::{
    CommandPage, LogStore, ProjectionSnapshot, ProjectionStore, RichClaimSelection, SnapshotRef,
};
use fireweed_projection::{InMemoryProjection, ProjectionData};
use rusqlite::{Connection, OptionalExtension, params};

use super::*;

/// The unified sqlite-relational store: ONE value, shared behind `Arc<Mutex<Inner>>`, that implements BOTH
/// the [`LogStore`] (epoch/fence + position mint) and [`ProjectionStore`] (durable apply + the full read /
/// validate / commit-class surface) axes of [`ComposedBackend`]. Two clones (one per axis field) point at
/// the same `Inner`, so `commit_locked`'s append→apply is one transactional unit (ADR-012 P1b-ii).
#[derive(Clone)]
pub struct SqliteRelational {
    inner: Arc<Mutex<Inner>>,
}

impl SqliteRelational {
    /// Open (or create) the unified relational store at `path`.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` unified relational store.
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(open_inner(conn)?)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("sqlite relational store poisoned")
    }
}

impl LogStore for SqliteRelational {
    fn durability_class(&self) -> DurabilityClass {
        // append+apply land in ONE relational transaction (apply commits both projection + cursor advance).
        DurabilityClass::Atomic
    }

    fn supports_emission_cursor(&self) -> bool {
        true
    }

    fn ensure_shard(&mut self, _shard: &QueueKey) -> EngineResult<()> {
        // The durable cursor/queue rows are created by the projection axis' `ensure_shard` (which has the
        // full `QueueDefinition`); the log axis shares the same `Inner`, so there is nothing extra to do.
        Ok(())
    }

    fn create_or_read_definition(
        &mut self,
        definition: &QueueDefinition,
    ) -> EngineResult<Option<fireweed_engine::CreateQueueOutcome>> {
        let mut g = self.lock();
        create_queue_sql(&mut g, definition.clone()).map(Some)
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let g = self.lock();
        let (t, q) = parts(shard);
        st(g.conn
            .query_row(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get::<_, i64>(0),
            )
            .optional())?
        .ok_or(EngineError::NotFound)
        .map(|e| e as u64)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        let g = self.lock();
        let (t, q) = parts(shard);
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
    }

    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        // STAGE only: read the cursor, fence, and MINT positions. No durable write — the apply axis advances
        // the cursor inside its own transaction, so no log row can outlive a failed projection apply.
        let g = self.lock();
        let (t, q) = parts(shard);
        let (next, epoch): (i64, i64) = st(g
            .conn
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        if expected_epoch != epoch as u64 {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for (i, _) in commands.iter().enumerate() {
            positions.push(CommandPosition::new(
                shard.clone(),
                epoch as u64,
                (next as u64) + i as u64,
            ));
        }
        Ok(positions)
    }

    fn read_from(
        &self,
        _shard: &QueueKey,
        _from: Option<CommandPosition>,
        _limit: usize,
    ) -> EngineResult<CommandPage> {
        // The relational family is rebuildable-cache backed: there is no replayable command log (the
        // projection cache is the source of truth). The conformance CORE class never reads the log; surface
        // an empty page.
        Ok(CommandPage {
            entries: Vec::new(),
            next: None,
        })
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        // The durable projection cursor is the high-water analogue: the next sequence is `next_seq`, so the
        // last absorbed position is `next_seq - 1`. `None` before any command is applied.
        let g = self.lock();
        let (t, q) = parts(shard);
        let (next, epoch): (i64, i64) = match st(g
            .conn
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        {
            Some(v) => v,
            None => return Ok(None),
        };
        Ok(
            (next > 0)
                .then(|| CommandPosition::new(shard.clone(), epoch as u64, (next as u64) - 1)),
        )
    }

    fn emission_cursor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let g = self.lock();
        let (t, q) = parts(shard);
        let row: Option<(i64, i64)> = st(g
            .conn
            .query_row(
                "SELECT epoch, seq FROM relational_emission_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?;
        Ok(row.map(|(epoch, seq)| CommandPosition::new(shard.clone(), epoch as u64, seq as u64)))
    }

    fn set_emission_cursor(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        let g = self.lock();
        let (t, q) = parts(shard);
        let current: Option<(i64, i64)> = st(g
            .conn
            .query_row(
                "SELECT epoch, seq FROM relational_emission_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?;
        if let Some((epoch, seq)) = current {
            let cur = CommandPosition::new(shard.clone(), epoch as u64, seq as u64);
            if !cur.precedes(&position) && cur != position {
                return Err(EngineError::Invalid("emission cursor regression"));
            }
        }
        st(g.conn.execute(
            "INSERT INTO relational_emission_cursor(tenant,queue,epoch,seq) VALUES(?1,?2,?3,?4) \
             ON CONFLICT(tenant,queue) DO UPDATE SET epoch=excluded.epoch, seq=excluded.seq",
            params![
                t,
                q,
                position.backend_epoch as i64,
                position.sequence as i64
            ],
        ))?;
        Ok(())
    }

    fn set_high_water(
        &mut self,
        _shard: &QueueKey,
        _position: CommandPosition,
    ) -> EngineResult<()> {
        // The cursor advances transactionally inside `apply`; an external high-water set is a no-op for the
        // rebuildable-cache family (there is no detached log tail to acknowledge).
        Ok(())
    }

    fn write_snapshot(
        &mut self,
        _shard: &QueueKey,
        _position: CommandPosition,
        _snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef> {
        Err(EngineError::Unavailable)
    }

    fn latest_snapshot(&self, _shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
        Ok(None)
    }

    fn read_snapshot(&self, _snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        Err(EngineError::Unavailable)
    }
}

impl ProjectionStore for SqliteRelational {
    fn hot_projection_capabilities(&self) -> QueryCapabilityFlags {
        QueryCapabilityFlags {
            range_scan: true,
            grouped_aggregate: true,
            declared_bucket_segment: true,
            bounded_mutation: true,
            claim_by_query: true,
            side_record_query: false,
        }
    }

    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let mut g = self.lock();
        create_queue_sql(&mut g, definition.clone())?;
        Ok(())
    }

    fn replay_durable_push(
        &mut self,
        shard: &QueueKey,
        request_id: &RequestId,
        items: &[PushSpec],
        now: UtcTimestamp,
    ) -> EngineResult<Option<Vec<ItemId>>> {
        let fingerprint = fireweed_engine::push_specs_fingerprint_sha256(items)?;
        let mut g = self.lock();
        let tx = st(g.conn.transaction())?;
        let result = check_request_idempotency(
            &tx,
            shard,
            IDEMPOTENCY_OPERATION_PUSH,
            request_id,
            &fingerprint,
            ts_nanos(now),
        );
        if result.is_ok() {
            st(tx.commit())?;
        }
        result
    }

    fn replay_durable_batch_update(
        &mut self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> EngineResult<Option<fireweed_engine::BatchUpdateResponse>> {
        let mut g = self.lock();
        let tx = st(g.conn.transaction())?;
        let (tenant, queue) = parts(shard);
        let prior: Option<(Vec<u8>, String, i64)> = st(tx
            .query_row(
                "SELECT request_fingerprint,response_payload,expires_at \
                 FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation=?3 AND request_id=?4",
                params![
                    tenant,
                    queue,
                    IDEMPOTENCY_OPERATION_BATCH_UPDATE,
                    request_id.as_str()
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional())?;
        let Some((stored_fingerprint, response_payload, expires_at)) = prior else {
            st(tx.commit())?;
            return Ok(None);
        };
        if expires_at <= ts_nanos(now) {
            st(tx.execute(
                "DELETE FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation=?3 AND request_id=?4",
                params![
                    tenant,
                    queue,
                    IDEMPOTENCY_OPERATION_BATCH_UPDATE,
                    request_id.as_str()
                ],
            ))?;
            st(tx.commit())?;
            return Ok(None);
        }
        if stored_fingerprint != fingerprint.to_be_bytes() {
            return Err(EngineError::RequestIdConflict);
        }
        let response = serde_json::from_str(&response_payload)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        st(tx.commit())?;
        Ok(Some(response))
    }

    fn replay_durable_item_mutation(
        &mut self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> EngineResult<Option<fireweed_engine::ItemMutationResponse>> {
        let mut g = self.lock();
        let tx = st(g.conn.transaction())?;
        let (tenant, queue) = parts(shard);
        let prior: Option<(Vec<u8>, String, String, i64)> = st(tx
            .query_row(
                "SELECT request_fingerprint,response_payload,command_positions,expires_at \
                 FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation=?3 AND request_id=?4",
                params![tenant, queue, IDEMPOTENCY_OPERATION_ITEM_MUTATION, request_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional())?;
        let Some((stored_fingerprint, response_payload, positions_json, expires_at)) = prior else {
            st(tx.commit())?;
            return Ok(None);
        };
        if expires_at <= ts_nanos(now) {
            st(tx.execute(
                "DELETE FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation=?3 AND request_id=?4",
                params![tenant, queue, IDEMPOTENCY_OPERATION_ITEM_MUTATION, request_id.as_str()],
            ))?;
            st(tx.commit())?;
            return Ok(None);
        }
        if stored_fingerprint != fingerprint.to_be_bytes() {
            return Err(EngineError::RequestIdConflict);
        }
        let mut response: fireweed_engine::ItemMutationResponse = serde_json::from_str(&response_payload)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let positions = positions_from_json(shard, &positions_json)?;
        response.position = positions.last().cloned();
        st(tx.commit())?;
        Ok(Some(response))
    }

    fn plan_item_mutation(
        &self,
        shard: &QueueKey,
        request: &fireweed_engine::ItemMutationRequest,
    ) -> EngineResult<fireweed_engine::ItemMutationPlan> {
        projection_data_sql(&self.lock(), shard)?.plan_item_mutation(request)
    }

    fn replay_durable_commit(
        &mut self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> EngineResult<Option<Vec<fireweed_engine::CommitOutcomeEntry>>> {
        let mut g = self.lock();
        let tx = st(g.conn.transaction())?;
        let (tenant, queue) = parts(shard);
        let prior: Option<(Vec<u8>, String, i64)> = st(tx
            .query_row(
                "SELECT request_fingerprint,response_payload,expires_at \
                 FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation=?3 AND request_id=?4",
                params![
                    tenant,
                    queue,
                    IDEMPOTENCY_OPERATION_COMMIT,
                    request_id.as_str()
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional())?;
        let Some((stored_fingerprint, response_payload, expires_at)) = prior else {
            st(tx.commit())?;
            return Ok(None);
        };
        if expires_at <= ts_nanos(now) {
            st(tx.execute(
                "DELETE FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation=?3 AND request_id=?4",
                params![
                    tenant,
                    queue,
                    IDEMPOTENCY_OPERATION_COMMIT,
                    request_id.as_str()
                ],
            ))?;
            st(tx.commit())?;
            return Ok(None);
        }
        if stored_fingerprint != fingerprint.to_be_bytes() {
            return Err(EngineError::RequestIdConflict);
        }
        let entries = serde_json::from_str(&response_payload)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        st(tx.commit())?;
        Ok(Some(entries))
    }

    fn read_durable_commit(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
    ) -> EngineResult<Option<Vec<fireweed_engine::CommitOutcomeEntry>>> {
        let g = self.lock();
        let (tenant, queue) = parts(shard);
        let response_payload: Option<String> = st(g
            .conn
            .query_row(
                "SELECT response_payload FROM fireweed_request_idempotency \
                 WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
                params![
                    tenant,
                    queue,
                    IDEMPOTENCY_OPERATION_COMMIT,
                    request_id.as_str()
                ],
                |row| row.get(0),
            )
            .optional())?;
        response_payload
            .map(|payload| {
                serde_json::from_str(&payload)
                    .map_err(|error| EngineError::Storage(error.to_string()))
            })
            .transpose()
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        // COMMIT: the single durable relational transaction (projection rows + cursor advance), at the
        // positions the log axis just minted. Reuses the group-commit apply verbatim.
        let mut g = self.lock();
        apply_committed_batch_sql(&mut g, positions, commands)
    }

    fn install_recovery_shard(
        &mut self,
        _definition: &QueueDefinition,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        // The unified relational apply is one SQLite transaction and therefore an atomic installation.
        self.apply(positions, commands)
    }

    // -- recovery-on-open (ADR-012 P2): the durable cursor records the last absorbed position, so a reopen
    //    can resume replay from the persisted tail while still repopulating the in-process control plane and
    //    id-mint counters from the durable sqlite rows.

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(self.lock().queues.values().cloned().collect())
    }

    fn recover_definitions_page(
        &self,
        cursor: Option<&fireweed_engine::DefinitionCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<fireweed_engine::DefinitionPage> {
        fireweed_engine::definition_page_from_sorted_rows(
            self.lock().queues.values().cloned(),
            cursor,
            limit,
            worker_partition,
        )
    }

    fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        // `SqliteRelational` is the unified log+projection store, so the durable replay cursor is the
        // relational high-water already tracked by the shared sqlite cursor.
        LogStore::high_water(self, shard)
    }

    // -- rich (non-item) claim selection + relational-class capabilities (BQ-14). The unified relational
    //    store materializes the per-group summary, cohort, and gate tables, so it implements what the
    //    log-replay family refuses — the composition's `claim_rich` / `SetGates` / discovery ports delegate
    //    here. Ported from the monolithic `SqliteRelationalBackend` (parity).

    fn supports_gates(&self) -> bool {
        true
    }

    fn select_rich_claim(
        &self,
        shard: &QueueKey,
        unit: ClaimUnit,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> EngineResult<RichClaimSelection> {
        let mut g = self.lock();
        let tx = st(g.conn.transaction())?;
        // Group-aware units refresh the bounded set of groups made eligible by time alone before selecting
        // (the summary is a mutation-time hint); the read-only discovery path deliberately does not.
        if matches!(unit, ClaimUnit::WholeGroup | ClaimUnit::SameGroupKey) {
            refresh_due_group_summaries(&tx, shard, now)?;
        }
        let mut cohort_id = None;
        let item_ids = match unit {
            // Item-level selection is the composition's own hot path; it never routes here.
            ClaimUnit::Item => return Err(EngineError::Unavailable),
            ClaimUnit::WholeGroup => {
                let max_groups = compatibility
                    .group_batching
                    .as_ref()
                    .map(|gb| gb.max_groups)
                    .unwrap_or(0);
                select_group_batching(&tx, shard, now, max_items, max_groups, compatibility)?
            }
            ClaimUnit::SameGroupKey => {
                select_same_group(&tx, shard, now, max_items, compatibility)?
            }
            ClaimUnit::WholeCohort => {
                match select_whole_cohort(&tx, shard, now, max_items, compatibility)? {
                    Some(selected) => {
                        cohort_id = Some(selected.cohort_id);
                        selected.item_ids
                    }
                    None => Vec::new(),
                }
            }
        };
        // ROLL BACK the selection transaction (drop without commit): this is a SELECT-only unit of work, and
        // the `refresh_due_group_summaries` write above is a transient selection aid, NOT a durable mutation.
        // The composition's `commit_locked` runs the epoch fence + append AFTER this returns; if the claim is
        // fenced (stale epoch) or selects nothing (empty / paused), NOTHING is appended and there must be no
        // durable side effect. Persisting the refresh here would durably mutate `fireweed_group_summary` for a
        // no-op/fenced claim — violating that invariant. The durable summary update for the groups actually
        // leased instead rides the `Claim` / `CohortClaim` apply arm (which re-refreshes their summaries),
        // so a successful claim still leaves the summary current — faithful to the monolith, which commits
        // the refresh ONLY inside a claim transaction that actually leases.
        drop(tx);
        Ok(RichClaimSelection {
            item_ids,
            cohort_id,
        })
    }

    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ActiveScope>> {
        discover_active_scopes_sql(&self.lock().conn, shard, granularity, now)
    }

    fn recovery_counter_high_water(&self, shard: &QueueKey) -> EngineResult<Option<ItemId>> {
        let g = self.lock();
        recovery_id_high_water_sql(&g.conn, shard)
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        let mut g = self.lock();
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
            max,
        )
    }

    fn select_item_claim(
        &self,
        shard: &QueueKey,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        filter_item_claim_candidates(&self.lock().conn, shard, compatibility, now, max)
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        let g = self.lock();
        render_claimed(&g.conn, shard, ids, |id| {
            g.live_tokens
                .get(shard)
                .and_then(|tokens| tokens.get(id))
                .cloned()
        })
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        lookup_active_by_key(&self.lock().conn, shard, client_item_key)
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        item_state_sql(&self.lock().conn, shard, id)
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        item_version_sql(&self.lock().conn, shard, id)
    }

    fn batch_update_snapshot(
        &self,
        shard: &QueueKey,
        refs: &[BatchUpdateItemRef],
    ) -> EngineResult<Vec<BatchUpdateSnapshotItem>> {
        batch_update_snapshot_sql(&self.lock().conn, shard, refs)
    }

    fn batch_update_preflight(
        &self,
        _shard: &QueueKey,
        commands: &[UpdateFieldsCommand],
    ) -> EngineResult<Vec<bool>> {
        Ok(vec![true; commands.len()])
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        expired_leases_sql(&self.lock().conn, shard, now)
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        all_expired_leases_sql(&self.lock().conn, now).unwrap_or_default()
    }

    fn expired_leases_page(
        &self,
        now: UtcTimestamp,
        cursor: Option<&fireweed_engine::ExpiredLeaseCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<fireweed_engine::ExpiredLeasePage> {
        expired_leases_page_sql(&self.lock().conn, now, cursor, limit, worker_partition)
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
        validate_leased(&self.lock().conn, shard, &ids)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&self.lock().conn, shard, ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&self.lock().conn, shard, ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        update_fields_validate_sql(&self.lock().conn, shard, id, expected_item_version)
    }

    // Field-based secondary indexes are a deferred relational feature (the family stubs them) — validation
    // is a no-op and queries report `Unavailable`, exactly like the monolithic `SqliteRelationalBackend`.
    fn index_validate(
        &self,
        _shard: &QueueKey,
        _item_id: &ItemId,
        _fields: &BTreeMap<String, Bytes>,
        _entity: Option<&serde_json::Value>,
        _exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        Ok(())
    }

    // ADR-011 typed indexes ARE enforced (see `maintain_typed_indexes_on_insert`), so pre-commit push
    // validation must reject an in-commit duplicate UNIQUE key HERE (before the durable append) or the
    // composed commit path could append a batch that then fails apply → relational recovery poison
    // (pqueue-29bef1e4). Mirrors the in-memory `InMemoryProjection::index_validate_push`.
    fn index_validate_push(&self, shard: &QueueKey, items: &[PushItem]) -> EngineResult<()> {
        let g = self.lock();
        let typed_indexes = g
            .queues
            .get(shard)
            .map(|d| d.typed_indexes.as_slice())
            .unwrap_or(&[]);
        validate_typed_unique_push(&g.conn, shard, typed_indexes, items)
    }

    fn index_validate_replace(
        &self,
        _shard: &QueueKey,
        _existing_id: &ItemId,
        _item: &PushItem,
    ) -> EngineResult<()> {
        Ok(())
    }

    fn index_validate_update(
        &self,
        _shard: &QueueKey,
        _id: &ItemId,
        _field_ops: &BTreeMap<String, Option<Bytes>>,
        _entity: Option<&serde_json::Value>,
    ) -> EngineResult<()> {
        Ok(())
    }

    // -- commit-class (Snorri vectorized commit boundary): the relational projection materializes the full
    //    read model, so it opts in and answers the pre-commit reads from its own SQL.
    fn supports_commit_transition(&self) -> bool {
        true
    }

    fn commit_validate(
        &self,
        shard: &QueueKey,
        refs: &[ClaimRef],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let g = self.lock();
        let tx = st(g.conn.unchecked_transaction())?;
        for claim_ref in refs {
            commit_validate_sql(&tx, shard, claim_ref, now)?;
        }
        Ok(())
    }

    fn instance_fence(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<u64>> {
        instance_fence_sql(&self.lock().conn, shard, key)
    }

    fn side_record(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        side_record_sql(&self.lock().conn, shard, key)
    }

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        let mut g = self.lock();
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
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        peek_sql(&self.lock().conn, shard, limit)
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let g = self.lock();
        pending_sql(&g.conn, &g.live_tokens, shard)
    }

    fn pending_summary(&self, shard: &QueueKey) -> EngineResult<PendingSummary> {
        let g = self.lock();
        Ok(pending_summary_sql(
            &g.live_tokens,
            &g.live_tokens_by_consumer,
            shard,
        ))
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        let g = self.lock();
        pending_page_sql(&g.conn, &g.live_tokens, shard, start, limit)
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        let g = self.lock();
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
    }

    fn pending_by_ids(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<LeaseView>> {
        let g = self.lock();
        pending_by_ids_sql(&g.conn, &g.live_tokens, shard, ids)
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        metrics_sql(&self.lock().conn, shard)
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> EngineResult<RangeScanResponse> {
        projection_data(&self.lock(), shard)?.range_scan(request)
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        projection_data(&self.lock(), shard)?.grouped_aggregate(request)
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        projection_data(&self.lock(), shard)?.metrics_by_query(request)
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        projection_data(&self.lock(), shard)?.declared_bucket_segment(request)
    }

    fn plan_bounded_mutation(
        &self,
        shard: &QueueKey,
        request: fireweed_core::BoundedMutationRequest,
    ) -> EngineResult<fireweed_engine::BoundedMutationPlan> {
        projection_data(&self.lock(), shard)?.plan_bounded_mutation(request)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        _now: UtcTimestamp,
        _emit_change_records: bool,
        _emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<TerminalEmissionMetrics> {
        let metrics = metrics_sql(&self.lock().conn, shard)?;
        Ok(TerminalEmissionMetrics {
            resident_terminal_count: metrics.resident_terminal_count,
            emission_lag_commands: 0,
            emission_oldest_unemitted_age_ms: 0,
        })
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        live_items_sql(&self.lock().conn, shard, keys)
    }

    fn reap_terminal_items(
        &mut self,
        shard: &QueueKey,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<Vec<ItemId>> {
        let mut g = self.lock();
        let tx = st(g.conn.transaction())?;
        let reaped = reap_terminal_items_sql(
            &tx,
            shard,
            now,
            terminal_retention_ms,
            emit_change_records,
            emission_cursor,
        )?;
        st(tx.commit())?;
        Ok(reaped)
    }

    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        projection_data(&self.lock(), shard)?.index_get_unique(index, key)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        projection_data(&self.lock(), shard)?.index_lookup(index, key)
    }
}

fn projection_data(inner: &Inner, shard: &QueueKey) -> EngineResult<ProjectionData> {
    projection_data_sql(inner, shard)
}

impl AsOfProjectionStore for SqliteRelational {
    // TD-009: the unified relational store serves only "now". It keeps NO replayable command log
    // (`LogStore::read_from` returns an empty page), so the composition's blanket `read_as_of`
    // (which rebuilds by replaying `read_from`) would otherwise materialize a BOGUS EMPTY projection.
    // Fail closed here with `Unavailable` so the composed relational backend refuses historical reads
    // exactly like the monolithic `SqliteRelationalBackend` (`HistoricalProjectionRead::read_as_of`),
    // rather than advertise an as-of read it cannot serve. The associated type is only a placeholder to
    // satisfy the internal composition bound; it is never constructed.
    type AsOfProjection = InMemoryProjection;

    // No replayable command log: cannot serve historical reads. Decline as-of up-front.
    fn supports_as_of(&self) -> bool {
        false
    }

    fn reconstruct_as_of(
        &self,
        _definition: &QueueDefinition,
        _snapshot: Option<ProjectionSnapshot>,
    ) -> EngineResult<Self::AsOfProjection> {
        Err(EngineError::Unavailable)
    }
}
