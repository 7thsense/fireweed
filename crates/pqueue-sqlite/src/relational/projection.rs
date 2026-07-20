use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, ItemId, ItemState, MetricsByQueryRequest, QueueDefinition, RequestId,
    UtcTimestamp,
};
use pqueue_engine::TerminalEmissionMetrics;
use pqueue_engine::{
    AsOfProjectionStore, ClaimRef, ClaimedItem, CohortLeaseTarget, CommandEnvelope,
    CommandPosition, CreateQueueOutcome, EngineError, EngineResult, FinalizeOutcome,
    IdempotencyDecision, IndexHit, ItemView, LeaseView, LiveItemView, ProjectionRead,
    PushFingerprint, PushItem, QueueCounters, QueueKey, QueueMetrics,
};
use pqueue_engine::{ProjectionSnapshot, ProjectionStore};
use pqueue_projection::{InMemoryProjection, ProjectionData, ProjectionImage};
use rusqlite::{Connection, OptionalExtension, params};

use super::*;

type RenewValidationRow = (String, i64, i64, Option<i64>, Option<i64>, Option<Vec<u8>>);
type FinalizeValidationRow = (
    String,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    Option<Vec<u8>>,
    i64,
    i64,
    i64,
);
type CohortValidationRow = (String, String, i64, i64, Option<Vec<u8>>);

pub(crate) const EXPIRED_LEASES_BOUNDED_SQL: &str = "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
     AND lifecycle_state='Leased' AND cohort_size IS NULL AND fenced=0 AND superseded=0 \
     AND lease_expires_at IS NOT NULL AND lease_expires_at<?3 ORDER BY item_id LIMIT ?4";

/// SQLite materialized projection fed by an external command-log authority.
///
/// This is intentionally not a full backend: it does not mint ids, append log entries, or expose
/// data-plane mutation ports. It reuses the relational SQL apply path so an object-log composite can
/// rebuild/read from SQLite without duplicating the 14-arm command projection.
pub struct SqliteProjectionStore {
    pub(crate) inner: Mutex<Inner>,
}

impl SqliteProjectionStore {
    /// Bounded page of the authoritative pending order for recovery verification.
    pub fn peek_page(
        &self,
        shard: &QueueKey,
        after: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<Vec<ItemView>> {
        peek_page_sql(&self.lock().conn, shard, after, limit)
    }

    pub(crate) fn purge_items_validate(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
        force: bool,
    ) -> EngineResult<Vec<ItemId>> {
        let g = self.lock();
        let (tenant, queue) = parts(shard);
        let mut present = Vec::new();
        for id in ids {
            if present.contains(id) {
                continue;
            }
            let state: Option<String> = st(g.conn.query_row(
                "SELECT lifecycle_state FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                params![tenant, queue, id.to_string()], |row| row.get(0)).optional())?;
            if let Some(state) = state {
                pqueue_engine::validate_purge_force(
                    parse_state(&state)? == ItemState::Leased,
                    force,
                )?;
                present.push(*id);
            }
        }
        Ok(present)
    }
    /// Open (or create) a SQLite projection database at `path`.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` projection store for tests.
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    pub(crate) fn renew_targets_validate(
        &self,
        shard: &QueueKey,
        targets: &[pqueue_engine::RenewTarget],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let g = self.lock();
        let (tenant, queue) = parts(shard);
        let now_nanos = ts_nanos(now);
        for target in targets {
            let row: Option<RenewValidationRow> = st(g
                .conn
                .query_row(
                    "SELECT lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash \
                     FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![tenant, queue, target.item_id.to_string()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .optional())?;
            let Some((state, fenced, superseded, cohort_size, lease_expires_at, stored_hash)) = row
            else {
                return Err(EngineError::NotFound);
            };
            let state = parse_state(&state)?;
            if fenced != 0 {
                return Err(EngineError::StaleLease);
            }
            if state.is_terminal() {
                return Err(EngineError::Terminal);
            }
            if superseded != 0 {
                return Err(EngineError::Superseded);
            }
            if cohort_size.is_some() {
                return Err(EngineError::Invalid("cohort member requires cohort lease"));
            }
            if state != ItemState::Leased {
                return Err(EngineError::Invalid("item is not leased"));
            }
            if stored_hash.as_deref() != Some(lease_hash(&target.lease_token).as_slice())
                || lease_expires_at.is_none_or(|expires| expires < now_nanos)
            {
                return Err(EngineError::StaleLease);
            }
        }
        Ok(())
    }

    pub(crate) fn cohort_lease_validate(
        &self,
        shard: &QueueKey,
        target: &CohortLeaseTarget,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<pqueue_engine::CohortLeaseMember>> {
        let g = self.lock();
        let tx = st(g.conn.unchecked_transaction())?;
        let (tenant, queue) = parts(shard);
        let cohort: Option<CohortValidationRow> = st(tx
            .query_row(
                "SELECT group_key,state,cohort_size,member_count,cohort_lease_token_hash \
                 FROM pqueue_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                params![tenant, queue, target.cohort_id.as_str()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional())?;
        let Some((group, state, expected, recorded, token_hash)) = cohort else {
            return Err(EngineError::NotFound);
        };
        if state == "terminal" {
            return Err(EngineError::Terminal);
        }
        if state != "leased" {
            return Err(EngineError::Invalid("cohort is not leased"));
        }
        if token_hash.as_deref() != Some(lease_hash(&target.cohort_lease_token).as_slice()) {
            return Err(EngineError::StaleLease);
        }
        if expected <= 0 || expected != recorded {
            return Err(EngineError::Conflict);
        }
        let mut statement = st(tx.prepare(
            "SELECT item_id,lifecycle_state,fenced,superseded,lease_expires_at,retry_count,max_attempts \
             FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
             AND cohort_size IS NOT NULL AND superseded=0 \
             AND lifecycle_state NOT IN ('Complete','Failed') ORDER BY priority_sort,created_seq",
        ))?;
        let rows = st(statement.query_map(params![tenant, queue, group], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        }))?;
        let mut item_ids = Vec::new();
        for row in rows {
            let (item_id, state, fenced, superseded, expires, attempts, max_attempts) = st(row)?;
            let state = parse_state(&state)?;
            if fenced != 0 {
                return Err(EngineError::StaleLease);
            }
            if state.is_terminal() {
                return Err(EngineError::Terminal);
            }
            if superseded != 0 {
                return Err(EngineError::Superseded);
            }
            if state != ItemState::Leased {
                return Err(EngineError::Invalid("cohort member is not leased"));
            }
            if expires.is_none_or(|expires| expires < ts_nanos(now)) {
                return Err(EngineError::StaleLease);
            }
            item_ids.push(pqueue_engine::CohortLeaseMember {
                item_id: ItemId::new(item_id)
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
                attempt_count: u32::try_from(attempts)
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
                max_attempts: u32::try_from(max_attempts)
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
            });
        }
        if i64::try_from(item_ids.len()).ok() != Some(expected) {
            return Err(EngineError::Conflict);
        }
        Ok(item_ids)
    }

    pub(crate) fn expired_leases_bounded(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(max).map_err(|error| EngineError::Storage(error.to_string()))?;
        let g = self.lock();
        let (tenant, queue) = parts(shard);
        let mut statement = st(g.conn.prepare(EXPIRED_LEASES_BOUNDED_SQL))?;
        let rows = st(
            statement.query_map(params![tenant, queue, ts_nanos(now), limit], |row| {
                row.get::<_, String>(0)
            }),
        )?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(
                ItemId::new(st(row)?).map_err(|error| EngineError::Storage(error.to_string()))?,
            );
        }
        Ok(ids)
    }

    pub(crate) fn finalize_targets_validate(
        &self,
        shard: &QueueKey,
        targets: &[pqueue_engine::FinalizeTarget],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<pqueue_engine::FinalizeLeaseMember>> {
        let g = self.lock();
        let tx = st(g.conn.unchecked_transaction())?;
        let (tenant, queue) = parts(shard);
        let now_nanos = ts_nanos(now);
        let result = (|| {
            let mut attempts = Vec::with_capacity(targets.len());
            for target in targets {
                let row: Option<FinalizeValidationRow> = st(tx
                    .query_row(
                        "SELECT lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash,item_version,retry_count,max_attempts \
                         FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        params![tenant, queue, target.item_id.to_string()],
                        |row| {
                            Ok((
                                row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                                row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?,
                            ))
                        },
                    )
                    .optional())?;
                let Some((
                    state,
                    fenced,
                    superseded,
                    cohort_size,
                    expires,
                    hash,
                    version,
                    retry_count,
                    max_attempts,
                )) = row
                else {
                    return Err(EngineError::NotFound);
                };
                let state = parse_state(&state)?;
                if fenced != 0 {
                    return Err(EngineError::StaleLease);
                }
                if state.is_terminal() {
                    return Err(EngineError::Terminal);
                }
                if superseded != 0 {
                    return Err(EngineError::Superseded);
                }
                if cohort_size.is_some() {
                    return Err(EngineError::Invalid("cohort member requires cohort lease"));
                }
                if state != ItemState::Leased {
                    return Err(EngineError::Invalid("item is not leased"));
                }
                if hash.as_deref() != Some(lease_hash(&target.lease_token).as_slice())
                    || expires.is_none_or(|value| value < now_nanos)
                {
                    return Err(EngineError::StaleLease);
                }
                if version < 0 || version as u64 != target.item_version {
                    return Err(EngineError::Conflict);
                }
                attempts.push(pqueue_engine::FinalizeLeaseMember {
                    item_id: target.item_id,
                    attempt_count: u32::try_from(retry_count).map_err(|_| {
                        EngineError::Storage("sqlite retry_count is outside the u32 range".into())
                    })?,
                    max_attempts: u32::try_from(max_attempts).map_err(|_| {
                        EngineError::Storage("sqlite max_attempts is outside the u32 range".into())
                    })?,
                });
            }
            Ok(attempts)
        })();
        st(tx.rollback())?;
        result
    }

    /// Read-only validation of every SQLite constraint that a fresh push can violate at apply time.
    ///
    /// This deliberately runs before the authoritative append. It covers active client-key and item-id
    /// uniqueness, cohort generation consistency/capacity, and typed UNIQUE indexes, including conflicts
    /// wholly within the candidate batch.
    pub(crate) fn validate_push_constraints(
        &self,
        shard: &QueueKey,
        items: &[PushItem],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let g = self.lock();
        let definition = g.queues.get(shard).ok_or(EngineError::NotFound)?;
        validate_typed_unique_push(&g.conn, shard, &definition.typed_indexes, items)?;

        let (tenant, queue) = parts(shard);
        let mut item_ids = BTreeSet::new();
        let mut client_keys = BTreeSet::new();
        let mut cohorts: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        let mut group_additions: BTreeMap<String, u64> = BTreeMap::new();
        let now_nanos = ts_nanos(now);
        for item in items {
            let item_id = item.item_id.to_string();
            if !item_ids.insert(item_id.clone()) {
                return Err(EngineError::Conflict);
            }
            let client_key = item.client_item_key.as_str().to_string();
            if !client_keys.insert(client_key.clone()) {
                return Err(EngineError::Conflict);
            }
            let occupied: Option<i64> = st(g
                .conn
                .query_row(
                    "SELECT 1 FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                     AND (item_id=?3 OR (client_item_key=?4 AND superseded=0)) LIMIT 1",
                    params![tenant, queue, item_id, client_key],
                    |row| row.get(0),
                )
                .optional())?;
            if occupied.is_some() {
                return Err(EngineError::Conflict);
            }
            let retained_until: Option<i64> = st(g
                .conn
                .query_row(
                    "SELECT expires_at FROM pqueue_item_key_retention \
                     WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3",
                    params![tenant, queue, client_key],
                    |row| row.get(0),
                )
                .optional())?;
            if retained_until.is_some_and(|until| until > now_nanos) {
                return Err(EngineError::Conflict);
            }

            if let Some(group) = &item.group_key {
                *group_additions
                    .entry(group.as_str().to_string())
                    .or_default() += 1;
            }

            match (&item.group_key, item.cohort_size) {
                (None, Some(_)) => {
                    return Err(EngineError::Invalid("cohort_size requires group_key"));
                }
                (Some(group), Some(size)) => {
                    let cohort_policy = definition.cohort_policy.as_ref();
                    if !cohort_policy.is_some_and(|policy| policy.enabled) {
                        return Err(EngineError::Invalid(
                            "cohort_size requires an enabled cohort policy",
                        ));
                    }
                    if cohort_policy
                        .and_then(|policy| policy.max_cohort_size)
                        .is_some_and(|max| size > max)
                    {
                        return Err(EngineError::Conflict);
                    }
                    let entry = cohorts
                        .entry(group.as_str().to_string())
                        .or_insert((size, 0));
                    if entry.0 != size {
                        return Err(EngineError::Conflict);
                    }
                    entry.1 += 1;
                }
                _ => {}
            }
        }

        if let Some(max_group_size) = definition.max_eligible_group_size {
            for (group, added) in group_additions {
                // API-001 caps every non-terminal member carrying the group key. Cohort members therefore
                // count too: they are still Pending/Leased members of that group, while terminal and
                // superseded generations do not consume the cap.
                let existing: i64 = st(g.conn.query_row(
                    "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                     AND group_key=?3 AND superseded=0 AND lifecycle_state IN ('Pending','Leased')",
                    params![tenant, queue, group],
                    |row| row.get(0),
                ))?;
                if existing.saturating_add(added as i64) > max_group_size as i64 {
                    return Err(EngineError::Conflict);
                }
            }
        }

        for (group, (size, added)) in cohorts {
            let existing: Option<(i64, i64, String, Option<i64>)> = st(g
                .conn
                .query_row(
                    "SELECT cohort_size,member_count,state,retention_until FROM pqueue_cohorts \
                     WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                    params![tenant, queue, group],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional())?;
            match existing {
                None if added > size => return Err(EngineError::Conflict),
                Some((_, _, state, retention_until))
                    if state == "terminal"
                        && retention_until.is_some_and(|until| until > now_nanos) =>
                {
                    return Err(EngineError::Conflict);
                }
                // An expired terminal generation is replaced rather than extended. Its old declared size
                // and member count therefore do not participate in validation of the new generation.
                Some((_, _, state, _)) if state == "terminal" && added > size => {
                    return Err(EngineError::Conflict);
                }
                Some((_, _, state, _)) if state == "terminal" => {}
                Some((existing_size, member_count, _, _))
                    if existing_size != size as i64
                        || member_count.saturating_add(added as i64) > existing_size =>
                {
                    return Err(EngineError::Conflict);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn pause_blocks_push_intake(&self, shard: &QueueKey) -> EngineResult<bool> {
        let g = self.lock();
        let (tenant, queue) = parts(shard);
        let drain: i64 = st(g
            .conn
            .query_row(
                "SELECT pause_drain_intake FROM queues WHERE tenant=?1 AND queue=?2",
                params![tenant, queue],
                |row| row.get(0),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        Ok(drain != 0)
    }

    /// Consult the durable projection replay row without deleting an expired record or otherwise mutating
    /// projection state. Expiry cleanup remains a later committed maintenance concern.
    pub(crate) fn push_idempotency_decision(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: PushFingerprint,
        now: UtcTimestamp,
    ) -> EngineResult<IdempotencyDecision<Vec<ItemId>>> {
        let g = self.lock();
        let (tenant, queue) = parts(shard);
        let prior: Option<(Vec<u8>, String, i64)> = st(g
            .conn
            .query_row(
                "SELECT request_fingerprint,response_payload,expires_at \
                 FROM pqueue_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation=?3 AND request_id=?4",
                params![
                    tenant,
                    queue,
                    IDEMPOTENCY_OPERATION_PUSH,
                    request_id.as_str()
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional())?;
        let Some((stored_fingerprint, response, expires_at)) = prior else {
            return Ok(IdempotencyDecision::Proceed);
        };
        if expires_at <= ts_nanos(now) {
            return Ok(IdempotencyDecision::Expired);
        }
        if stored_fingerprint == fingerprint.canonical_sha256
            || stored_fingerprint == fingerprint.legacy_body_hash.0.to_be_bytes()
        {
            Ok(IdempotencyDecision::Replay(item_ids_from_json(response)?))
        } else {
            Ok(IdempotencyDecision::Conflict)
        }
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        Ok(Self {
            inner: Mutex::new(open_inner(conn)?),
        })
    }

    /// Delete and recreate the disposable projection schema in place.
    ///
    /// This is a lifecycle seam for compositions whose authoritative history lives outside SQLite. It
    /// deliberately keeps the connection open (and therefore works for both file-backed and in-memory
    /// stores), removes every application table atomically, recreates the current schema, and clears the
    /// process-local definition/token caches. Callers must replay authoritative queue definitions and
    /// commands before treating the durable projection as current again.
    pub fn reset_projection(&self) -> EngineResult<()> {
        let mut g = self.inner.lock().expect("projection store poisoned");
        st(g.conn.execute_batch("BEGIN IMMEDIATE"))?;
        let reset = (|| -> EngineResult<()> {
            for table in pqueue_relational::OWNED_PROJECTION_TABLES {
                st(g.conn
                    .execute_batch(&format!("DROP TABLE IF EXISTS {table}")))?;
            }
            st(g.conn.execute_batch(RELATIONAL_SCHEMA))?;
            Ok(())
        })();
        match reset {
            Ok(()) => st(g.conn.execute_batch("COMMIT"))?,
            Err(error) => {
                let _ = g.conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        }

        g.queues.clear();
        g.schemas.clear();
        g.grouped_shards.clear();
        g.claim_scan_hints.clear();
        g.claim_scan_default_fifo.clear();
        g.live_tokens.clear();
        Ok(())
    }

    /// Create or validate queue projection metadata.
    pub fn create_queue_projection(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        let mut g = self.inner.lock().expect("projection store poisoned");
        create_queue_sql(&mut g, definition)
    }

    /// Apply one already-durable command at its externally assigned log position.
    pub fn apply_committed(
        &self,
        position: &CommandPosition,
        envelope: &CommandEnvelope,
    ) -> EngineResult<()> {
        let mut g = self.inner.lock().expect("projection store poisoned");
        apply_committed_sql(&mut g, position, envelope)
    }

    /// Apply a whole sealed segment's worth of already-durable commands in **one** SQLite transaction.
    ///
    /// This is the group-commit batch apply for the segmented object-log backend: instead of paying a
    /// BEGIN/COMMIT (and rollback-journal create/delete) per command, the entire batch commits once. Each
    /// `positions[i]` is the externally assigned log position of `envelopes[i]`; positions for a given
    /// queue MUST be contiguous and start at that queue's `next_seq` (already-applied prefixes are skipped
    /// idempotently, so a recovery replay that overlaps prior state is a no-op). A gap is a hard error.
    pub fn apply_committed_batch(
        &self,
        positions: &[CommandPosition],
        envelopes: &[CommandEnvelope],
    ) -> EngineResult<()> {
        if positions.len() != envelopes.len() {
            return Err(EngineError::Storage(
                "apply_committed_batch: positions/envelopes length mismatch".into(),
            ));
        }
        if positions.is_empty() {
            return Ok(());
        }
        let mut g = self.inner.lock().expect("projection store poisoned");
        apply_committed_batch_sql(&mut g, positions, envelopes)
    }

    fn apply_live_batch(
        &self,
        positions: &[CommandPosition],
        envelopes: &[CommandEnvelope],
    ) -> EngineResult<()> {
        if positions.len() != envelopes.len() {
            return Err(EngineError::Storage(
                "apply_live_batch: positions/envelopes length mismatch".into(),
            ));
        }
        let mut g = self.lock();
        let mut epoch_floor = BTreeMap::<QueueKey, u64>::new();
        for position in positions {
            let floor = match epoch_floor.get(&position.queue) {
                Some(floor) => *floor,
                None => {
                    let (tenant, queue) = parts(&position.queue);
                    let stored: i64 = st(g
                        .conn
                        .query_row(
                            "SELECT assignment_epoch FROM relational_cursor \
                             WHERE tenant=?1 AND queue=?2",
                            params![tenant, queue],
                            |row| row.get(0),
                        )
                        .optional())?
                    .ok_or(EngineError::NotFound)?;
                    u64::try_from(stored).map_err(|_| {
                        EngineError::Storage("negative SQLite assignment epoch".into())
                    })?
                }
            };
            if position.backend_epoch < floor {
                return Err(EngineError::EpochFenced);
            }
            epoch_floor.insert(position.queue.clone(), position.backend_epoch.max(floor));
        }
        apply_committed_batch_sql(&mut g, positions, envelopes)
    }

    /// Lightweight finalize pre-validation for object-log backends: true when every distinct `id` is
    /// currently leased in the durable projection. This avoids rendering full claimed-item payloads when a
    /// caller only needs leased-state validation.
    pub fn all_leased(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<bool> {
        let g = self.inner.lock().expect("projection store poisoned");
        Ok(leased_id_count_sql(&g.conn, shard, ids)? == ids.len())
    }

    /// Snapshot recovery seam (bead pqueue-8a76daad): the per-queue **applied high-water** durably recorded
    /// by the relational cursor. The returned position is the last command already absorbed
    /// (`relational_cursor.next_seq - 1`), so a reopen can resume replay from the durable tail after that
    /// point. `None` if the queue has no projection row yet (a never-created queue → caller falls back to a
    /// full replay).
    ///
    /// Because every committed batch advances this cursor INSIDE the same SQLite transaction that applies
    /// the batch, the persisted high-water can never be ahead of what is durably materialized.
    pub fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let g = self.inner.lock().expect("projection store poisoned");
        let (t, q) = parts(shard);
        let row: Option<(i64, i64)> = st(g
            .conn
            .query_row(
                pqueue_relational::SELECT_RELATIONAL_CURSOR,
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?;
        Ok(row.and_then(|(next_seq, epoch)| {
            (next_seq > 0)
                .then(|| CommandPosition::new(shard.clone(), epoch as u64, next_seq as u64 - 1))
        }))
    }

    /// Export the durable SQLite projection rows for `shard` as a typed in-memory projection image.
    pub fn export_projection_image(&self, shard: &QueueKey) -> EngineResult<ProjectionImage> {
        let g = self.inner.lock().expect("projection store poisoned");
        export_projection_image_sql(&g.conn, shard)
    }

    /// The object-log lineage the async checkpoint worker durably recorded for `shard`, or `None` if no
    /// lineage row exists (a queue whose projection was materialized synchronously, or never checkpointed).
    /// Recovery cross-validates this against the log identity via
    /// [`HybridProjectionStore::validate_recovery_lineage`].
    pub fn checkpoint_lineage(&self, shard: &QueueKey) -> EngineResult<Option<CheckpointLineage>> {
        let g = self.inner.lock().expect("projection store poisoned");
        Ok(read_checkpoint_lineage_sql(&g.conn, shard)?.map(|(l, _)| l))
    }

    /// Restart recovery for the object-log backends' item-id mint counter: seed `counters` past every item
    /// id already materialized in the snapshot (`pqueue_items`), so a push after a snapshot-tail reopen never
    /// re-mints an id that the full-genesis replay would have observed. Safe because the object_log_sqlite
    /// backends never delete item rows (purge / replace-pending are `Unavailable` on the eventual-apply
    /// class), so the persisted items are the complete minted set up to the high-water; the bounded tail
    /// then observes any ids minted beyond it.
    pub fn observe_item_counters(
        &self,
        shard: &QueueKey,
        counters: &QueueCounters,
    ) -> EngineResult<()> {
        let g = self.inner.lock().expect("projection store poisoned");
        let (t, q) = parts(shard);
        let mut stmt = st(g
            .conn
            .prepare(pqueue_relational::SELECT_MATERIALIZED_ITEM_IDS))?;
        let rows = st(stmt.query_map(params![t, q], |row| row.get::<_, String>(0)))?;
        for r in rows {
            let id = ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?;
            counters.observe(shard, id);
        }
        // Terminal-item reaping deletes rows, so the surviving set above is NOT the complete minted set;
        // restore the durable mint-counter floor too, or a reopen after a full reap could re-mint a reaped id.
        observe_id_high_water_sql(&g.conn, shard, counters)
    }

    /// Restore ONLY the durable item-id high-water (ADR-009 mint-counter recovery floor) into `counters`. The
    /// `objectlog/hybrid-async` store seeds counters from its hydrated hot memory (the surviving rows), so it
    /// calls this separately to also fold in the ceiling of any REAPED rows the survivors no longer carry.
    pub fn observe_id_high_water(
        &self,
        shard: &QueueKey,
        counters: &QueueCounters,
    ) -> EngineResult<()> {
        observe_id_high_water_sql(&self.lock().conn, shard, counters)
    }
}

impl ProjectionRead for SqliteProjectionStore {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("projection store poisoned");
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
            let g = self.inner.lock().expect("projection store poisoned");
            peek_sql(&g.conn, shard, limit)
        };
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("projection store poisoned");
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
            let g = self.inner.lock().expect("projection store poisoned");
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
            let g = self.inner.lock().expect("projection store poisoned");
            live_items_sql(&g.conn, shard, keys)
        };
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = {
            let g = self.inner.lock().expect("projection store poisoned");
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
            let g = self.inner.lock().expect("projection store poisoned");
            metrics_sql(&g.conn, shard).map(|metrics| TerminalEmissionMetrics {
                resident_terminal_count: metrics.resident_terminal_count,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            })
        };
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// ADR-012 P1b-ii (Part B): the DERIVED sqlite projection as a `ProjectionStore`
// ---------------------------------------------------------------------------
//
// `SqliteProjectionStore` is the relational SQL projection fed by an EXTERNAL command-log authority (the
// object log, or a sqlite/postgres LOG axis). Wrapping it as a [`ProjectionStore`] lets the generic
// [`ComposedBackend`] pair it with any [`LogStore`] — `ComposedBackend<SqliteLog, SqliteProjectionStore>`
// (atomic) and `ComposedBackend<ObjectLog, SqliteProjectionStore>` (eventual-apply) — instead of the
// hand-written `ObjectLogSqliteBackend` monolith. `apply` is the same group-commit `apply_committed_batch`
// the monolith uses (idempotent prefix-skip, gap error), so a committed log position is materialized once.

impl SqliteProjectionStore {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("projection store poisoned")
    }
}

impl ProjectionStore for SqliteProjectionStore {
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let mut g = self.lock();
        create_queue_sql(&mut g, definition.clone())?;
        Ok(())
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        // Idempotent group-commit apply at the externally assigned log positions (one sqlite transaction).
        self.apply_committed_batch(positions, commands)
    }

    fn apply_live(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.apply_live_batch(positions, commands)
    }

    fn apply_recovery(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.apply_committed_batch(positions, commands)
    }

    // -- recovery-on-open (ADR-012 P2): this derived sqlite projection persists its high-water + definitions,
    //    so a reopened composition replays only the object-/sqlite-log tail beyond the snapshot.

    fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        SqliteProjectionStore::recovery_high_water(self, shard)
    }

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(self.lock().queues.values().cloned().collect())
    }

    fn restore_counters(&self, shard: &QueueKey, counters: &QueueCounters) -> EngineResult<()> {
        self.observe_item_counters(shard, counters)
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
        compatibility: &pqueue_engine::ClaimCompatibility,
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

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        expired_leases_sql(&self.lock().conn, shard, now)
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        all_expired_leases_sql(&self.lock().conn, now).unwrap_or_default()
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

    // ADR-011 typed indexes ARE enforced at apply time (`maintain_typed_indexes_on_insert`), so the composed
    // sqlite_log + sqlite-projection commit path MUST reject an in-commit duplicate UNIQUE typed-index key
    // HERE (before the durable log append) — otherwise the batch appends durably then fails apply, poisoning
    // recovery (pqueue-29bef1e4). Mirrors the in-memory `InMemoryProjection::index_validate_push`.
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

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        metrics_sql(&self.lock().conn, shard)
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        let definition = {
            let g = self.lock();
            g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?
        };
        let image = self.export_projection_image(shard)?;
        let projection = ProjectionData::from_image(&definition, image)?;
        projection.metrics_by_query(request)
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
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        Err(EngineError::Unavailable)
    }

    fn index_lookup(
        &self,
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        Err(EngineError::Unavailable)
    }
}

impl AsOfProjectionStore for SqliteProjectionStore {
    type AsOfProjection = InMemoryProjection;

    fn reconstruct_as_of(
        &self,
        definition: &QueueDefinition,
        snapshot: Option<ProjectionSnapshot>,
    ) -> EngineResult<Self::AsOfProjection> {
        let mut projection = InMemoryProjection::new();
        projection.ensure_shard(definition)?;
        if let Some(snapshot) = snapshot {
            let image = ProjectionImage::from_bytes(&snapshot.payload)?;
            projection.hydrate_shard(definition, image)?;
        }
        Ok(projection)
    }
}
