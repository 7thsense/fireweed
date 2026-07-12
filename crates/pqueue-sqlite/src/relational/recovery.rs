use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use pqueue_core::{ClientItemKey, GroupKey, ItemId, QueueDefinition, QueueId, RequestId, TenantId};
use pqueue_engine::{
    CommandEnvelope, CommandPosition, CreateQueueOutcome, EngineError, EngineResult, QueueCounters,
    QueueKey, RequestOutcome, compile_entity_schema,
};
use pqueue_projection::{ProjectionImage, ProjectionImageItem};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use super::*;

/// Advance the durable per-queue item-id high-water past the greatest of `reaped` (ADR-009 mint-counter
/// recovery floor). MONOTONIC by `(epoch, counter)`: a reap that deletes only lower-id rows never lowers the
/// stored floor. This is what keeps terminal-item reaping from re-minting a reaped id — the deleted rows are
/// no longer in `pqueue_items`, but their ceiling is preserved here and restored by
/// [`observe_id_high_water_sql`]. No-op when `reaped` is empty.
pub(crate) fn advance_id_high_water_sql(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    reaped: &[ItemId],
) -> EngineResult<()> {
    let Some(max_reaped) = reaped
        .iter()
        .max_by_key(|id| (id.epoch(), id.counter()))
        .copied()
    else {
        return Ok(());
    };
    let (t, q) = parts(shard);
    let existing: Option<String> = st(tx
        .query_row(
            "SELECT item_id FROM pqueue_id_high_water WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?;
    let keep = match existing {
        Some(s) => {
            let cur = ItemId::new(s).map_err(|e| EngineError::Storage(e.to_string()))?;
            if (max_reaped.epoch(), max_reaped.counter()) > (cur.epoch(), cur.counter()) {
                max_reaped
            } else {
                cur
            }
        }
        None => max_reaped,
    };
    st(tx.execute(
        "INSERT INTO pqueue_id_high_water(tenant,queue,item_id) VALUES(?1,?2,?3) \
         ON CONFLICT(tenant,queue) DO UPDATE SET item_id=?3",
        params![t, q, keep.to_string()],
    ))?;
    Ok(())
}

/// Restore the durable item-id high-water into `counters` (ADR-009 mint-counter recovery floor), so a reopen
/// resumes minting past every previously-minted id even after retention reaping deleted the rows that carried
/// them. [`QueueCounters::observe`] is epoch-aware + monotonic, so a stale lower-epoch floor is safely ignored
/// once a fresh tenure resets the sequence. No-op if the queue never reaped.
pub(crate) fn observe_id_high_water_sql(
    conn: &Connection,
    shard: &QueueKey,
    counters: &QueueCounters,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let stored: Option<String> = st(conn
        .query_row(
            "SELECT item_id FROM pqueue_id_high_water WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?;
    if let Some(s) = stored {
        let id = ItemId::new(s).map_err(|e| EngineError::Storage(e.to_string()))?;
        counters.observe(shard, id);
    }
    Ok(())
}

/// Restore the durable item-id high-water for EVERY queue into `counters` — the all-queues counterpart to
/// [`observe_id_high_water_sql`] for the monolithic [`SqliteRelationalBackend`], whose restore scans the whole
/// `pqueue_items` table in one pass rather than per shard. Inert when no reap has ever advanced a floor.
pub(crate) fn observe_all_id_high_water_sql(
    conn: &Connection,
    counters: &QueueCounters,
) -> EngineResult<()> {
    let mut stmt = st(conn.prepare("SELECT tenant, queue, item_id FROM pqueue_id_high_water"))?;
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
        counters.observe(&key, item_id);
    }
    Ok(())
}

/// The persisted LOGICAL high-water for `shard`: `relational_cursor.next_seq`, or `None` if the queue has
/// no projection row yet.
pub(crate) fn recovery_high_water_sql(
    conn: &Connection,
    shard: &QueueKey,
) -> EngineResult<Option<u64>> {
    let (t, q) = parts(shard);
    let next_seq: Option<i64> = st(conn
        .query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?;
    Ok(next_seq.map(|n| n as u64))
}

/// Read the recorded object-log lineage (+ cumulative applied-command count) for `shard`, if any.
pub(crate) fn read_checkpoint_lineage_sql(
    conn: &Connection,
    shard: &QueueKey,
) -> EngineResult<Option<(CheckpointLineage, u64)>> {
    let (t, q) = parts(shard);
    let row: Option<(i64, String, i64)> = st(conn
        .query_row(
            "SELECT source_epoch, source_segment, applied_commands \
             FROM pqueue_checkpoint_lineage WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    Ok(row.map(|(epoch, segment, applied)| {
        (
            CheckpointLineage {
                source_epoch: epoch as u64,
                source_segment: segment,
            },
            applied as u64,
        )
    }))
}

/// Read the durably persisted push response ids for `(shard, request_id)`, or `None` if absent.
pub(crate) fn read_push_replay_sql(
    conn: &Connection,
    shard: &QueueKey,
    request_id: &RequestId,
) -> EngineResult<Option<Vec<ItemId>>> {
    let (t, q) = parts(shard);
    let payload: Option<String> = st(conn
        .query_row(
            "SELECT response_payload FROM pqueue_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_PUSH, request_id.as_str()],
            |row| row.get(0),
        )
        .optional())?;
    payload.map(item_ids_from_json).transpose()
}

/// Persist the durable request-id idempotency/outcome row for a committed request-id-bearing command, so a
/// committed-but-unreturned retry converges after restart (plan §Request-Id Replay). Only push commands
/// currently carry replayable outcomes; a command with no `request_id`, or one missing replay metadata, is
/// skipped (nothing durable to replay). Written inside the caller's checkpoint transaction.
pub(crate) fn persist_request_outcome_sql(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    env: &CommandEnvelope,
    pos: &CommandPosition,
) -> EngineResult<()> {
    let Some(request_id) = env.request_id.as_ref() else {
        return Ok(());
    };
    let (Some(fingerprint), Some(RequestOutcome::Push { item_ids })) =
        (env.request_fingerprint, env.request_outcome.as_ref())
    else {
        return Ok(());
    };
    let expires_at = request_expires_at(queues, shard, env.created_at)?;
    record_request_idempotency(
        tx,
        shard,
        IDEMPOTENCY_OPERATION_PUSH,
        request_id,
        &fingerprint.to_be_bytes(),
        item_ids,
        std::slice::from_ref(pos),
        env.created_at,
        expires_at,
    )
}

/// Single-shard checkpoint apply (bead pqueue-16b85e28): apply an ordered batch of already-committed
/// object-log commands, persist request-id idempotency rows, record object-log lineage, and advance the
/// LOGICAL high-water LAST — all in ONE transaction. Mirrors [`apply_committed_batch_sql`] but is bound to
/// a single shard so it can additionally stamp that queue's checkpoint-lineage row.
pub(crate) fn checkpoint_batch_sql(
    g: &mut Inner,
    shard: &QueueKey,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    lineage: &CheckpointLineage,
) -> EngineResult<CheckpointProgress> {
    if !g.queues.contains_key(shard) {
        return Err(EngineError::NotFound);
    }
    for pos in positions {
        if &pos.queue != shard {
            return Err(EngineError::Storage(
                "checkpoint: position does not belong to the checkpointed shard".into(),
            ));
        }
    }
    if positions.is_empty() {
        // Nothing to checkpoint; report current durable progress without opening a transaction.
        let logical_high_water = recovery_high_water_sql(&g.conn, shard)?;
        let lineage_row = read_checkpoint_lineage_sql(&g.conn, shard)?;
        return Ok(CheckpointProgress {
            logical_high_water,
            applied_commands: lineage_row.as_ref().map(|(_, n)| *n).unwrap_or(0),
            lineage: lineage_row.map(|(l, _)| l),
        });
    }
    let Inner {
        conn,
        queues,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        live_tokens,
        ..
    } = g;
    let (t, q) = parts(shard);
    let tx = st(conn.transaction())?;
    let mut cursor: i64 = st(tx
        .query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .ok_or(EngineError::NotFound)?;
    let mut max_epoch = lineage.source_epoch as i64;
    let mut token_ops = Vec::new();
    let mut applied_this_batch: u64 = 0;
    for (pos, env) in positions.iter().zip(envelopes) {
        let incoming = pos.sequence as i64;
        if incoming < cursor {
            // Idempotent replay of an already-absorbed prefix.
            continue;
        }
        if incoming > cursor {
            return Err(EngineError::Storage(format!(
                "sqlite checkpoint replay gap for {}:{}: expected sequence {cursor}, got {incoming}",
                shard.tenant_id.as_str(),
                shard.queue_id.as_str()
            )));
        }
        apply_command_sql(
            &tx,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            &mut token_ops,
            shard,
            pos,
            pos.sequence,
            env.created_at,
            &env.command,
        )?;
        // Persist request-id idempotency/outcome BEFORE the cursor advance, so the row lands under the
        // same high-water it belongs to.
        persist_request_outcome_sql(&tx, queues, shard, env, pos)?;
        cursor = incoming
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
        let e = pos.backend_epoch as i64;
        if e > max_epoch {
            max_epoch = e;
        }
        applied_this_batch += 1;
    }
    // Object-log lineage: cumulative applied-command count + the segment/manifest this high-water derives
    // from. Upserted in the SAME transaction, BEFORE the high-water write.
    let prior_applied: i64 = st(tx
        .query_row(
            "SELECT applied_commands FROM pqueue_checkpoint_lineage WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .unwrap_or(0);
    let total_applied = prior_applied + applied_this_batch as i64;
    let updated_at = ts_nanos(envelopes[envelopes.len() - 1].created_at);
    st(tx.execute(
        "INSERT INTO pqueue_checkpoint_lineage \
         (tenant,queue,logical_high_water,source_epoch,source_segment,applied_commands,updated_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7) \
         ON CONFLICT(tenant,queue) DO UPDATE SET \
          logical_high_water=excluded.logical_high_water, \
          source_epoch=excluded.source_epoch, \
          source_segment=excluded.source_segment, \
          applied_commands=excluded.applied_commands, \
          updated_at=excluded.updated_at",
        params![
            t,
            q,
            cursor,
            lineage.source_epoch as i64,
            lineage.source_segment,
            total_applied,
            updated_at,
        ],
    ))?;
    // LOGICAL high-water LAST: advance the command cursor (and the durable ownership epoch) as the final
    // write before commit, so the cursor can never be ahead of the applied projection + persisted lineage.
    st(tx.execute(
        "UPDATE relational_cursor SET \
         next_seq=?3, \
         assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
         WHERE tenant=?1 AND queue=?2",
        params![t, q, cursor, max_epoch],
    ))?;
    st(tx.commit())?;
    apply_token_ops(live_tokens, token_ops);
    Ok(CheckpointProgress {
        logical_high_water: Some(cursor as u64),
        applied_commands: total_applied as u64,
        lineage: Some(lineage.clone()),
    })
}

pub(crate) fn open_inner(conn: Connection) -> EngineResult<Inner> {
    // WAL + synchronous=NORMAL: the group-commit projection seals one batched transaction per segment and
    // wants commits cheap. Default DELETE journaling pays a rollback-journal create/delete (and an extra
    // directory fsync) per COMMIT; WAL appends and checkpoints lazily, and NORMAL drops the per-commit
    // fsync (durable at checkpoint). The projection is rebuildable from the durable object log, so this
    // trades nothing the object-log authority does not already guarantee. `busy_timeout` keeps a
    // concurrent checkpoint/reader from turning into a spurious SQLITE_BUSY. (No-ops on `:memory:`.)
    st(conn.execute_batch(
        "PRAGMA journal_mode=WAL;\
         PRAGMA synchronous=NORMAL;\
         PRAGMA busy_timeout=5000;",
    ))?;
    st(conn.execute_batch(RELATIONAL_SCHEMA))?;
    ensure_item_fields_column(&conn)?;
    ensure_item_metadata_column(&conn)?;
    ensure_item_entity_document_column(&conn)?;
    ensure_item_terminal_command_epoch_column(&conn)?;
    ensure_cohort_lifecycle_columns(&conn)?;
    let mut inner = Inner {
        conn,
        queues: HashMap::new(),
        schemas: HashMap::new(),
        grouped_shards: HashSet::new(),
        claim_scan_hints: HashMap::new(),
        claim_scan_default_fifo: HashMap::new(),
        live_tokens: HashMap::new(),
    };
    inner.reload()?;
    Ok(inner)
}

pub(crate) fn create_queue_sql(
    g: &mut Inner,
    definition: QueueDefinition,
) -> EngineResult<CreateQueueOutcome> {
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    if let Some(existing) = g.queues.get(&key) {
        if existing.ordering_mode != definition.ordering_mode
            || existing.priority_model != definition.priority_model
        {
            return Err(EngineError::QueueDefinitionConflict);
        }
        return Ok(CreateQueueOutcome {
            created: false,
            definition: existing.clone(),
        });
    }
    // Compile the entity schema once at create time (ADR-011).
    let compiled_schema = definition
        .entity_schema
        .as_ref()
        .and_then(|esd| esd.entity_schema.as_ref())
        .map(compile_entity_schema)
        .transpose()?;
    let (t, q) = parts(&key);
    let def_json = to_json(&definition)?;
    st(g.conn.execute(
        "INSERT INTO queues(tenant,queue,definition,paused) VALUES(?1,?2,?3,0)",
        params![t, q, def_json],
    ))?;
    st(g.conn.execute(
        "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq) VALUES(?1,?2,0,0)",
        params![t, q],
    ))?;
    if let Some(cs) = compiled_schema {
        g.schemas.insert(key.clone(), cs);
    }
    g.queues.insert(key, definition.clone());
    Ok(CreateQueueOutcome {
        created: true,
        definition,
    })
}

pub(crate) fn export_projection_image_sql(
    conn: &Connection,
    shard: &QueueKey,
) -> EngineResult<ProjectionImage> {
    let (t, q) = parts(shard);
    let cursor: Option<(i64, i64, i64)> = st(conn
        .query_row(
            "SELECT next_seq,next_item_seq,assignment_epoch FROM relational_cursor \
             WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let (next_seq, next_item_seq, assignment_epoch) = cursor.ok_or(EngineError::NotFound)?;
    let paused: i64 = st(conn.query_row(
        "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2",
        params![t, q],
        |row| row.get(0),
    ))?;

    let mut gate_keys_by_item = item_gate_key_map(conn, shard)?;
    let mut stmt = st(conn.prepare(
        "SELECT item_id,client_item_key,lifecycle_state,priority,not_before,group_key,payload,\
         fields,metadata,entity_document,retry_count,item_version,lease_expires_at,fenced,\
         superseded,max_attempts,created_seq \
         FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 ORDER BY created_seq,item_id",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<Vec<u8>>>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, i64>(11)?,
            row.get::<_, Option<i64>>(12)?,
            row.get::<_, i64>(13)?,
            row.get::<_, i64>(14)?,
            row.get::<_, i64>(15)?,
            row.get::<_, i64>(16)?,
        ))
    }))?;
    let mut items = Vec::new();
    for row in rows {
        let (
            item_id,
            client_item_key,
            lifecycle_state,
            priority,
            not_before,
            group_key,
            payload,
            fields,
            metadata,
            entity_document,
            retry_count,
            item_version,
            lease_expires_at,
            fenced,
            superseded,
            max_attempts,
            created_seq,
        ) = st(row)?;
        let item_id = ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?;
        let entity_document = entity_document
            .map(|raw| serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string())))
            .transpose()?;
        items.push(ProjectionImageItem {
            item_id,
            client_item_key: ClientItemKey::new(client_item_key)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            priority: parse_priority(priority)?,
            not_before: not_before.map(nanos_ts),
            group_key: group_key
                .map(GroupKey::new)
                .transpose()
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            payload: payload.map(Bytes::from),
            fields: fields_from_json(fields)?,
            metadata: metadata_from_json(metadata)?,
            gate_keys: gate_keys_by_item.remove(&item_id).unwrap_or_default(),
            entity_document,
            state: parse_state(&lifecycle_state)?,
            item_version: item_version as u64,
            attempt_count: retry_count as u32,
            max_attempts: max_attempts as u32,
            created_seq: created_seq as u64,
            lease_token: None,
            lease_expires_at: lease_expires_at.map(nanos_ts),
            fenced: fenced != 0,
            superseded: superseded != 0,
            terminal_at: None,
            terminal_position: None,
        });
    }

    let mut side_records = BTreeMap::new();
    let mut stmt = st(conn.prepare(
        "SELECT key,payload FROM pqueue_side_records \
         WHERE tenant_id=?1 AND queue_id=?2 ORDER BY key",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    }))?;
    for row in rows {
        let (key, payload) = st(row)?;
        side_records.insert(key, Bytes::from(payload));
    }

    let mut instance_fences = BTreeMap::new();
    let mut stmt = st(conn.prepare(
        "SELECT instance_key,fence FROM pqueue_instance_fences \
         WHERE tenant_id=?1 AND queue_id=?2 ORDER BY instance_key",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
    }))?;
    for row in rows {
        let (key, fence) = st(row)?;
        instance_fences.insert(key, fence as u64);
    }

    let high_water = (next_seq > 0)
        .then(|| CommandPosition::new(shard.clone(), assignment_epoch as u64, next_seq as u64 - 1));
    Ok(ProjectionImage {
        high_water,
        paused: paused != 0,
        pause_drain_intake: false,
        next_seq: next_item_seq as u64,
        items,
        side_records,
        instance_fences,
        metrics: metrics_sql(conn, shard)?,
    })
}

pub(crate) fn apply_committed_sql(
    g: &mut Inner,
    position: &CommandPosition,
    envelope: &CommandEnvelope,
) -> EngineResult<()> {
    if !g.queues.contains_key(&position.queue) {
        return Err(EngineError::NotFound);
    }
    let Inner {
        conn,
        queues,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        live_tokens,
        ..
    } = g;
    let (t, q) = parts(&position.queue);
    let tx = st(conn.transaction())?;
    let next_seq: i64 = st(tx
        .query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .ok_or(EngineError::NotFound)?;
    let incoming_seq = position.sequence as i64;
    if incoming_seq < next_seq {
        return Ok(());
    }
    if incoming_seq > next_seq {
        return Err(EngineError::Storage(format!(
            "sqlite projection replay gap for {}:{}: expected sequence {next_seq}, got {incoming_seq}",
            position.queue.tenant_id.as_str(),
            position.queue.queue_id.as_str()
        )));
    }
    let mut token_ops = Vec::new();
    apply_command_sql(
        &tx,
        queues,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        &mut token_ops,
        &position.queue,
        position,
        position.sequence,
        envelope.created_at,
        &envelope.command,
    )?;
    let new_next_seq = position
        .sequence
        .checked_add(1)
        .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
    st(tx.execute(
        "UPDATE relational_cursor SET \
         next_seq=?3, \
         assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
         WHERE tenant=?1 AND queue=?2",
        params![t, q, new_next_seq as i64, position.backend_epoch as i64],
    ))?;
    st(tx.commit())?;
    apply_token_ops(live_tokens, token_ops);
    Ok(())
}

/// Batched analogue of [`apply_committed_sql`]: apply many already-durable commands in ONE transaction,
/// reading each queue's cursor once and advancing it once at the end (group-commit apply). Already-applied
/// positions (`sequence < next_seq`) are skipped idempotently; an out-of-order position is a hard gap error.
pub(crate) fn apply_committed_batch_sql(
    g: &mut Inner,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
) -> EngineResult<()> {
    let Inner {
        conn,
        queues,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        live_tokens,
        ..
    } = g;
    for pos in positions {
        if !queues.contains_key(&pos.queue) {
            return Err(EngineError::NotFound);
        }
    }
    let tx = st(conn.transaction())?;
    let mut token_ops = Vec::new();
    // Per-queue running cursor (next expected sequence) + the highest epoch observed, so the cursor row is
    // read once and written once per queue across the whole batch.
    let mut next_seq: HashMap<QueueKey, i64> = HashMap::new();
    let mut max_epoch: HashMap<QueueKey, i64> = HashMap::new();
    for (pos, env) in positions.iter().zip(envelopes) {
        let (t, q) = parts(&pos.queue);
        let cursor = match next_seq.get(&pos.queue) {
            Some(&n) => n,
            None => {
                let n: i64 = st(tx
                    .query_row(
                        "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                        params![t, q],
                        |row| row.get(0),
                    )
                    .optional())?
                .ok_or(EngineError::NotFound)?;
                next_seq.insert(pos.queue.clone(), n);
                n
            }
        };
        let incoming = pos.sequence as i64;
        if incoming < cursor {
            // Already applied (idempotent replay of a prefix the projection has already absorbed).
            continue;
        }
        if incoming > cursor {
            return Err(EngineError::Storage(format!(
                "sqlite projection replay gap for {}:{}: expected sequence {cursor}, got {incoming}",
                pos.queue.tenant_id.as_str(),
                pos.queue.queue_id.as_str()
            )));
        }
        apply_command_sql(
            &tx,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            &mut token_ops,
            &pos.queue,
            pos,
            pos.sequence,
            env.created_at,
            &env.command,
        )?;
        let new_next = incoming
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
        next_seq.insert(pos.queue.clone(), new_next);
        let e = pos.backend_epoch as i64;
        let slot = max_epoch.entry(pos.queue.clone()).or_insert(e);
        if e > *slot {
            *slot = e;
        }
    }
    for (queue, &next) in &next_seq {
        let (t, q) = parts(queue);
        let epoch = max_epoch.get(queue).copied().unwrap_or(0);
        st(tx.execute(
            "UPDATE relational_cursor SET \
             next_seq=?3, \
             assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
             WHERE tenant=?1 AND queue=?2",
            params![t, q, next, epoch],
        ))?;
    }
    st(tx.commit())?;
    apply_token_ops(live_tokens, token_ops);
    Ok(())
}
