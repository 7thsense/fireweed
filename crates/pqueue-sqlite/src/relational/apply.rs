use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use axon_esf::CompiledSchema;
use pqueue_core::{
    CohortId, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityModel, QueueDefinition,
    QueueId, TenantId, UtcTimestamp, is_retry_exhausted,
};
use pqueue_engine::{
    CommandPosition, EngineError, EngineResult, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    PayloadUpdate, PushItem, QueueCommand, QueueKey, compile_entity_schema,
};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

use super::*;

// ---------------------------------------------------------------------------
// Inner: the durable connection + the queue-definition cache + the live-token map
// ---------------------------------------------------------------------------

pub(crate) struct Inner {
    pub(crate) conn: Connection,
    /// Definitions cache (priority model for `priority_sort`, retry bound). Rebuilt from `queues` on open.
    pub(crate) queues: HashMap<QueueKey, QueueDefinition>,
    /// Compiled entity schemas (ADR-011). Rebuilt from `queues` on open; keyed by queue.
    pub(crate) schemas: HashMap<QueueKey, Arc<CompiledSchema>>,
    pub(crate) grouped_shards: HashSet<QueueKey>,
    /// Process-local rowid cursor for high-volume FIFO claim scans. Never persisted; reset on reopen or rich
    /// queue shapes, so correctness comes from the fallback SQL path rather than from the hint.
    pub(crate) claim_scan_hints: HashMap<QueueKey, i64>,
    pub(crate) claim_scan_default_fifo: HashMap<QueueKey, bool>,
    /// Ephemeral live lease tokens (cleartext is never persisted; only the hash is). Lost on reopen.
    pub(crate) live_tokens: HashMap<ItemId, LeaseToken>,
}

impl Inner {
    /// Rebuild the in-RAM definition cache from the durable `queues` table. The item projection itself is
    /// already durable in `pqueue_items` as a rebuildable cache - nothing to replay.
    pub(crate) fn reload(&mut self) -> EngineResult<()> {
        let rows: Vec<String> = {
            let mut stmt = st(self.conn.prepare("SELECT definition FROM queues"))?;
            let mapped = st(stmt.query_map([], |row| row.get::<_, String>(0)))?;
            let mut out = Vec::new();
            for r in mapped {
                out.push(st(r)?);
            }
            out
        };
        for def_json in rows {
            let definition: QueueDefinition =
                serde_json::from_str(&def_json).map_err(|e| EngineError::Storage(e.to_string()))?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            if let Some(cs) = definition
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?
            {
                self.schemas.insert(key.clone(), cs);
            }
            self.queues.insert(key, definition);
        }
        self.grouped_shards.clear();
        let mut stmt = st(self.conn.prepare(
            "SELECT DISTINCT tenant_id, queue_id FROM pqueue_items WHERE group_key IS NOT NULL",
        ))?;
        let mapped = st(stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }))?;
        for r in mapped {
            let (tenant, queue) = st(r)?;
            self.grouped_shards.insert(QueueKey::new(
                TenantId::new(tenant).map_err(|e| EngineError::Storage(e.to_string()))?,
                QueueId::new(queue).map_err(|e| EngineError::Storage(e.to_string()))?,
            ));
        }
        // NOTE: item-id restart-safety is handled by `restore_counters` (it seeds `QueueCounters` past the
        // highest durable id, decoding `(epoch, counter)` straight from the packed id — ADR-009).
        Ok(())
    }

    /// Assign the next command sequence for `shard`, apply `command` to `pqueue_items`, and advance the
    /// cursor — all in one transaction (the atomic append+apply UoW the async ports rely on).
    ///
    /// BQ-20/BQ-21/BQ-22 (bead pqueue-7bac12ce): the owner's cached `fence_epoch` is now threaded through
    /// every data-plane port as `expected_epoch`, and this function checks it against the durable cursor
    /// epoch — a stale value is `EpochFenced` (see the `expected_epoch.is_some_and` check below). This
    /// closes the end-to-end fencing gap for the data-plane fast path.
    pub(crate) fn commit_command(
        &mut self,
        shard: &QueueKey,
        command: QueueCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        let Inner {
            conn,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            live_tokens,
            ..
        } = self;
        let (t, q) = parts(shard);
        let tx = st(conn.transaction())?;
        // ADR-009 / TD-003: read the durable assignment_epoch with the cursor and fence against the owner's
        // cached acquire-time epoch (`Some`) — a superseded owner is rejected `EpochFenced`, nothing applied.
        // `None` is the degenerate sole-owner path (no fence). Brings this data-plane path to parity with the
        // `RelLogWriter::append` seam.
        let (seq, epoch): (i64, i64) = st(tx
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        if expected_epoch.is_some_and(|e| e != epoch as u64) {
            return Err(EngineError::EpochFenced);
        }
        let mut token_ops = Vec::new();
        apply_command_sql(
            &tx,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            &mut token_ops,
            shard,
            &CommandPosition::new(shard.clone(), epoch as u64, seq as u64),
            seq as u64,
            now,
            &command,
        )?;
        st(tx.execute(
            "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
            params![t, q, seq + 1],
        ))?;
        st(tx.commit())?;
        apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// apply: the 14-arm command -> SQL projection write (the BQ-11a headline)
// ---------------------------------------------------------------------------

/// Max rows per dynamically-built multi-row / `IN (...)` statement. Each `pqueue_items` row binds 19
/// params; 1,500 rows ≈ 28.5k params, under sqlite's 32,766 bound-variable ceiling (bundled SQLite) while
/// cutting large Push materialization from tens of thousands of statements to a few thousand.
pub(crate) const SQLITE_BATCH: usize = 1_500;
pub(crate) const COHORT_EXPIRY_SWEEP_LIMIT: usize = 128;
pub(crate) const GROUP_DUE_REFRESH_LIMIT: i64 = 128;

pub(crate) fn opt_text(v: Option<String>) -> Value {
    v.map_or(Value::Null, Value::Text)
}
pub(crate) fn opt_int(v: Option<i64>) -> Value {
    v.map_or(Value::Null, Value::Integer)
}
pub(crate) fn opt_blob(v: Option<Vec<u8>>) -> Value {
    v.map_or(Value::Null, Value::Blob)
}

/// Batch-insert all `items` of a Push (or the single ReplacePending replacement) as set-based statements:
/// chunked multi-row INSERTs into `pqueue_items`, `pqueue_item_gates`, and `pqueue_cohorts` — replacing the
/// former per-item `insert_item` (N+ round-trips → a handful, chunked to the bound-variable limit). Column
/// values, the `fields` TEXT-JSON encoding, and the `eligible_since`/`not_before` pairing are identical to
/// the per-item path; `created_seq` is bulk-allocated (`base + i`) so the FIFO order is preserved.
pub(crate) fn insert_items(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    model: &PriorityModel,
    shard: &QueueKey,
    items: &[PushItem],
    seq: u64,
    now: UtcTimestamp,
) -> EngineResult<()> {
    if items.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let seqi = seq as i64;
    // Bulk-allocate the stable FIFO positions in one read+advance (was a read+UPDATE per item).
    let base_seq: i64 = st(tx.query_row(
        "SELECT next_item_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
        params![t, q],
        |row| row.get(0),
    ))?;
    st(tx.execute(
        "UPDATE relational_cursor SET next_item_seq=?3 WHERE tenant=?1 AND queue=?2",
        params![t, q, base_seq + items.len() as i64],
    ))?;
    let typed_indexes = queues
        .get(shard)
        .map(|d| d.typed_indexes.as_slice())
        .unwrap_or(&[]);
    if typed_indexes.is_empty() && items.iter().all(is_default_empty_push_item) {
        insert_default_empty_items(tx, &t, &q, items, seqi, now_n, base_seq)?;
        return Ok(());
    }
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let not_before = ts_nanos_opt(item.not_before);
        rows.push(vec![
            Value::Text(t.clone()),
            Value::Text(q.clone()),
            Value::Text(item.item_id.to_string()),
            Value::Text(item.client_item_key.as_str().to_string()),
            opt_text(item.priority.as_ref().map(to_json).transpose()?),
            Value::Blob(elig_sort(&item.priority, model)),
            opt_int(not_before),
            Value::Integer(not_before.unwrap_or(now_n)),
            opt_text(item.group_key.as_ref().map(|g| g.as_str().to_string())),
            opt_int(item.cohort_size.map(|s| s as i64)),
            opt_blob(item.payload.as_ref().map(|b| b.to_vec())),
            Value::Text(fields_to_json(&item.fields)?),
            Value::Text(metadata_to_json(&item.metadata)?),
            opt_text(item.entity_document.as_ref().map(to_json).transpose()?),
            Value::Integer(seqi),
            Value::Integer(now_n),
            Value::Integer(now_n),
            Value::Integer(item.max_attempts as i64),
            Value::Integer(base_seq + i as i64),
        ]);
    }
    const ROW_PH: &str =
        "(?,?,?,?,'Pending',?,?,?,?,?,?,?,?,?,?,0,1,NULL,NULL,NULL,?,?,?,NULL,NULL,0,0,?,?)";
    for chunk in rows.chunks(SQLITE_BATCH) {
        let values = vec![ROW_PH; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO pqueue_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,entity_document,retry_count,\
              item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,terminal_command_epoch,fenced,superseded,max_attempts,created_seq) VALUES {values}"
        );
        let flat = chunk.iter().flatten();
        st(tx.execute(&sql, params_from_iter(flat)))?;
    }
    insert_gates(tx, &t, &q, items)?;
    upsert_cohorts(tx, queues, shard, &t, &q, items, now_n)?;
    // ADR-011: typed secondary index maintenance.
    maintain_typed_indexes_on_insert(tx, &t, &q, typed_indexes, items)?;
    Ok(())
}

pub(crate) fn is_default_empty_push_item(item: &PushItem) -> bool {
    item.client_item_key.as_str() == item.item_id.to_string()
        && item.priority.is_none()
        && item.not_before.is_none()
        && item.group_key.is_none()
        && item.cohort_size.is_none()
        && item.payload.is_none()
        && item.fields.is_empty()
        && item.metadata == Metadata::default()
        && item.gate_keys.is_empty()
        && item.entity_document.is_none()
}

pub(crate) fn insert_default_empty_items(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    items: &[PushItem],
    seqi: i64,
    now_n: i64,
    base_seq: i64,
) -> EngineResult<()> {
    const ROW_PH: &str = "(?,?,?,?,'Pending',NULL,X'01',NULL,?,NULL,NULL,NULL,'{}','{}',NULL,0,1,NULL,NULL,NULL,?,?,?,NULL,NULL,0,0,?,?)";
    for (chunk_idx, chunk) in items.chunks(SQLITE_BATCH).enumerate() {
        let values = vec![ROW_PH; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO pqueue_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,entity_document,retry_count,\
             item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,terminal_command_epoch,fenced,superseded,max_attempts,created_seq) VALUES {values}"
        );
        let mut params = Vec::with_capacity(chunk.len() * 10);
        let offset = chunk_idx * SQLITE_BATCH;
        for (i, item) in chunk.iter().enumerate() {
            params.push(Value::Text(t.to_string()));
            params.push(Value::Text(q.to_string()));
            let item_id = item.item_id.to_string();
            params.push(Value::Text(item_id.clone()));
            params.push(Value::Text(item_id));
            params.push(Value::Integer(now_n));
            params.push(Value::Integer(seqi));
            params.push(Value::Integer(now_n));
            params.push(Value::Integer(now_n));
            params.push(Value::Integer(item.max_attempts as i64));
            params.push(Value::Integer(base_seq + offset as i64 + i as i64));
        }
        st(tx.execute(&sql, params_from_iter(params.iter())))?;
    }
    Ok(())
}

/// Batch the per-item gate-membership inserts (BQ-14d) into chunked multi-row INSERTs. Pairs are deduped so
/// one statement never proposes the same `(item_id, gate_key)` twice.
pub(crate) fn insert_gates(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    items: &[PushItem],
) -> EngineResult<()> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for item in items {
        let id = item.item_id.to_string();
        for gk in &item.gate_keys {
            let g = gk.as_str().to_string();
            if !pairs.iter().any(|(a, b)| a == &id && b == &g) {
                pairs.push((id.clone(), g));
            }
        }
    }
    if pairs.is_empty() {
        return Ok(());
    }
    for chunk in pairs.chunks(SQLITE_BATCH) {
        let values = vec!["(?,?,?,?)"; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO pqueue_item_gates (tenant_id,queue_id,item_id,gate_key) VALUES {values} \
             ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING"
        );
        let mut p: Vec<Value> = Vec::with_capacity(chunk.len() * 4);
        for (id, g) in chunk {
            p.push(Value::Text(t.to_string()));
            p.push(Value::Text(q.to_string()));
            p.push(Value::Text(id.clone()));
            p.push(Value::Text(g.clone()));
        }
        st(tx.execute(&sql, params_from_iter(p.iter())))?;
    }
    Ok(())
}

pub(crate) fn cohort_id_for(group_key: &str, now_n: i64) -> String {
    format!("coh:{group_key}:{now_n}")
}

pub(crate) fn cohort_retention_until(
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    now_n: i64,
) -> EngineResult<i64> {
    let retention_ms = queues
        .get(shard)
        .map(|d| d.terminal_retention_ms)
        .ok_or(EngineError::NotFound)?;
    Ok(now_n.saturating_add((retention_ms as i64).saturating_mul(1_000_000)))
}

pub(crate) fn cohort_expiry_deadline(
    definition: &QueueDefinition,
    cohort_created_at: i64,
    first_eligible_at: Option<i64>,
) -> Option<i64> {
    let bound_ms = definition.cohort_policy.as_ref()?.completion_bound_ms?;
    let start = first_eligible_at
        .map(|first| cohort_created_at.min(first))
        .unwrap_or(cohort_created_at);
    Some(start.saturating_add((bound_ms as i64).saturating_mul(1_000_000)))
}

pub(crate) fn cohort_member_count_state(count: i64, size: i64) -> &'static str {
    if count >= size { "complete" } else { "forming" }
}

/// Maintain TD-002 cohort lifecycle projection for newly accepted cohort members.
pub(crate) fn upsert_cohorts(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    t: &str,
    q: &str,
    items: &[PushItem],
    now_n: i64,
) -> EngineResult<()> {
    let mut cohorts: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for item in items {
        if let (Some(group), Some(size)) = (&item.group_key, item.cohort_size) {
            let gk = group.as_str().to_string();
            let size = size as i64;
            let entry = cohorts.entry(gk).or_insert((size, 0));
            if entry.0 != size {
                return Err(EngineError::Conflict);
            }
            entry.1 += 1;
        }
    }
    if cohorts.is_empty() {
        return Ok(());
    }
    let _ = cohort_retention_until(queues, shard, now_n)?;
    for (gk, (size, added)) in cohorts {
        let existing: Option<(i64, i64, String, Option<i64>)> = st(tx
            .query_row(
                "SELECT cohort_size, member_count, state, retention_until FROM pqueue_cohorts \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                params![t, q, gk],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional())?;
        match existing {
            None => {
                if added > size {
                    return Err(EngineError::Conflict);
                }
                let state = cohort_member_count_state(added, size);
                let first_eligible_at = if state == "complete" {
                    Some(now_n)
                } else {
                    None
                };
                st(tx.execute(
                    "INSERT INTO pqueue_cohorts \
                     (tenant_id,queue_id,group_key,cohort_id,cohort_size,member_count,state,\
                      cohort_created_at,first_eligible_at,created_at) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?8)",
                    params![
                        t,
                        q,
                        gk,
                        cohort_id_for(&gk, now_n),
                        size,
                        added,
                        state,
                        now_n,
                        first_eligible_at,
                    ],
                ))?;
            }
            Some((existing_size, member_count, state, retention_until)) => {
                if state == "terminal" {
                    if retention_until.is_some_and(|until| until > now_n) {
                        return Err(EngineError::Conflict);
                    }
                    if added > size {
                        return Err(EngineError::Conflict);
                    }
                    let next_state = cohort_member_count_state(added, size);
                    let first_eligible_at = if next_state == "complete" {
                        Some(now_n)
                    } else {
                        None
                    };
                    st(tx.execute(
                        "UPDATE pqueue_cohorts SET cohort_id=?4, cohort_size=?5, member_count=?6, \
                         state=?7, cohort_created_at=?8, first_eligible_at=?9, expire_command_pos=NULL, \
                         cohort_lease_token_hash=NULL, retention_until=NULL, created_at=?8 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                        params![
                            t,
                            q,
                            gk,
                            cohort_id_for(&gk, now_n),
                            size,
                            added,
                            next_state,
                            now_n,
                            first_eligible_at,
                        ],
                    ))?;
                    continue;
                }
                if existing_size != size {
                    return Err(EngineError::Conflict);
                }
                if member_count + added > existing_size {
                    return Err(EngineError::Conflict);
                }
                let next_count = member_count + added;
                let next_state = if state == "leased" {
                    state.as_str()
                } else {
                    cohort_member_count_state(next_count, existing_size)
                };
                let set_first = next_state == "complete";
                st(tx.execute(
                    "UPDATE pqueue_cohorts SET member_count=?4, state=?5, \
                     first_eligible_at=CASE WHEN ?6 AND first_eligible_at IS NULL THEN ?7 ELSE first_eligible_at END \
                     WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                    params![t, q, gk, next_count, next_state, set_first, now_n],
                ))?;
            }
        }
    }
    Ok(())
}

/// Run `{prefix} (chunk)` (e.g. an `UPDATE … item_id IN` or `DELETE … item_id IN`) once per ≤256-id chunk.
/// `lead` are the bound values for the prefix's leading placeholders (the SET clause, if any); the prefix's
/// trailing `tenant_id=? AND queue_id=?` then bind `t`,`q`, followed by the chunk ids. Chunking keeps the
/// bound-variable count under sqlite's limit.
pub(crate) fn exec_items_in(
    tx: &Transaction<'_>,
    prefix: &str,
    lead: &[Value],
    t: &str,
    q: &str,
    ids: &[String],
) -> EngineResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!("{prefix} ({ph})");
        let mut p: Vec<Value> = lead.to_vec();
        p.push(Value::Text(t.to_string()));
        p.push(Value::Text(q.to_string()));
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        st(tx.execute(&sql, params_from_iter(p.iter())))?;
    }
    Ok(())
}

pub(crate) fn reap_terminal_items_sql(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
    terminal_retention_ms: u64,
    emit_change_records: bool,
    emission_cursor: Option<&CommandPosition>,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let cutoff = now_n.saturating_sub((terminal_retention_ms as i64).saturating_mul(1_000_000));
    let (sql, params): (String, Vec<Value>) = if emit_change_records {
        let Some(cursor) = emission_cursor else {
            return Ok(Vec::new());
        };
        (
            "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND superseded=0 AND lifecycle_state IN ('Complete','Failed') \
             AND terminal_at IS NOT NULL AND terminal_at<=?3 \
             AND terminal_command_epoch IS NOT NULL \
             AND (terminal_command_epoch<?4 \
                  OR (terminal_command_epoch=?4 AND last_command_sequence<=?5))"
                .to_string(),
            vec![
                Value::Text(t.clone()),
                Value::Text(q.clone()),
                Value::Integer(cutoff),
                Value::Integer(cursor.backend_epoch as i64),
                Value::Integer(cursor.sequence as i64),
            ],
        )
    } else {
        (
            "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND superseded=0 AND lifecycle_state IN ('Complete','Failed') \
             AND terminal_at IS NOT NULL AND terminal_at<=?3"
                .to_string(),
            vec![
                Value::Text(t.clone()),
                Value::Text(q.clone()),
                Value::Integer(cutoff),
            ],
        )
    };
    let mut stmt = st(tx.prepare(&sql))?;
    let rows = st(stmt.query_map(params_from_iter(params.iter()), |row| {
        row.get::<_, String>(0)
    }))?;
    let mut id_strs = Vec::new();
    for row in rows {
        id_strs.push(st(row)?);
    }
    let ids: Vec<ItemId> = id_strs
        .into_iter()
        .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
        .collect::<EngineResult<Vec<_>>>()?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    exec_items_in(
        tx,
        "DELETE FROM pqueue_items WHERE tenant_id=? AND queue_id=? AND item_id IN",
        &[],
        &t,
        &q,
        &ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
    )?;
    let id_strs = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    exec_items_in(
        tx,
        "DELETE FROM pqueue_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN",
        &[],
        &t,
        &q,
        &id_strs,
    )?;
    delete_typed_index_rows(tx, &t, &q, &id_strs)?;
    // Preserve the mint-counter recovery floor BEFORE the deleted rows vanish: the surviving `pqueue_items`
    // are no longer the complete minted set, so recovery must restore the id ceiling from here or it could
    // re-mint a reaped id (ADR-009 id-uniqueness). Runs in the same reap transaction, so the floor advance is
    // atomic with the deletion — a crash never leaves rows deleted without the floor recorded.
    advance_id_high_water_sql(tx, shard, &ids)?;
    Ok(ids)
}

/// A deferred mutation of the in-RAM live-token map, collected during apply and replayed onto the map
/// ONLY after the transaction commits — so a commit failure can never leave the RAM tokens ahead of the
/// durable `pqueue_items` state (F4).
pub(crate) enum TokenOp {
    Set(ItemId, LeaseToken),
    Clear(ItemId),
}

pub(crate) fn apply_token_ops(live_tokens: &mut HashMap<ItemId, LeaseToken>, ops: Vec<TokenOp>) {
    for op in ops {
        match op {
            TokenOp::Set(id, token) => {
                live_tokens.insert(id, token);
            }
            TokenOp::Clear(id) => {
                live_tokens.remove(&id);
            }
        }
    }
}

/// The distinct non-null `group_key`s of the given item ids (for summary refresh). For arms that DELETE
/// (purge), call this BEFORE the delete so the groups are still discoverable.
pub(crate) fn groups_of(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Vec<GroupKey>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
    let mut seen: Vec<GroupKey> = Vec::new();
    // One set-based round-trip per chunk (was one SELECT per item): distinct non-null group keys.
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT DISTINCT group_key FROM pqueue_items WHERE tenant_id=? AND queue_id=? \
             AND item_id IN ({ph}) AND group_key IS NOT NULL"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let mut stmt = st(tx.prepare(&sql))?;
        let mapped = st(stmt.query_map(params_from_iter(p.iter()), |row| row.get::<_, String>(0)))?;
        for r in mapped {
            let gk = GroupKey::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?;
            if !seen.contains(&gk) {
                seen.push(gk);
            }
        }
    }
    Ok(seen)
}

pub(crate) fn cohort_group_for_id(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<GroupKey> {
    let (t, q) = parts(shard);
    let group: String = st(tx.query_row(
        "SELECT group_key FROM pqueue_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
        params![t, q, cohort_id.as_str()],
        |row| row.get(0),
    ))?;
    GroupKey::new(group).map_err(|e| EngineError::Storage(e.to_string()))
}

pub(crate) fn cohort_item_ids(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let group = cohort_group_for_id(tx, shard, cohort_id)?;
    let mut stmt = st(tx.prepare(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND superseded=0 AND cohort_size IS NOT NULL AND lifecycle_state NOT IN ('Complete','Failed') \
         ORDER BY priority_sort, created_seq",
    ))?;
    let mapped = st(stmt.query_map(params![t, q, group.as_str()], |row| row.get::<_, String>(0)))?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// Recompute `pqueue_group_summary` for one group from `pqueue_items` (exact aggregate over the group's
/// currently-eligible items, in the SAME transaction as the mutation that touched it). The representative
/// is the would-be-first-claimed eligible item (strict-claim key `priority_sort, created_seq`), matching
/// the claim selection, including live gate state; `rep_progress_guard_sort`/`at_risk_count` stay NULL/0
/// while the progress-guard derivation is deferred (parity with the strict claim ordering, BQ-14).
///
/// EXACT AT MUTATION TIME, lagged across a time-only `not_before` crossing: the aggregate filters
/// `not_before<=now`, so a deferred item that becomes due WITHOUT a subsequent mutation is not reflected
/// in `oldest_eligible_at`/`rep_*`/`eligible_item_count` until the next mutation refreshes its group. The
/// per-item `select_eligible` path re-evaluates `not_before` on read and is unaffected. BQ-14 g1/g4
/// consumers refresh due groups before mutation-backed group claims; read-only discovery still cannot
/// mutate and therefore may under-report until a due sweep or later mutation refreshes the group.
pub(crate) fn refresh_group_summary(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    group_key: &GroupKey,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    // Eligible aggregate: pending, not superseded, due at `now`.
    let (count, oldest): (i64, Option<i64>) = st(tx.query_row(
        "SELECT COUNT(*), MIN(eligible_since) FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND lifecycle_state='Pending' AND superseded=0 AND (not_before IS NULL OR not_before<=?4) \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id)",
        params![t, q, group_key.as_str(), now_n],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ))?;
    // Representative = first-claimable eligible item of the group.
    let rep: Option<(Vec<u8>, i64, String)> = st(tx
        .query_row(
            "SELECT priority_sort, created_at, item_id FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
             AND lifecycle_state='Pending' AND superseded=0 AND (not_before IS NULL OR not_before<=?4) \
             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                 ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
                 AND ig.item_id=pqueue_items.item_id) \
             ORDER BY priority_sort, created_seq LIMIT 1",
            params![t, q, group_key.as_str(), now_n],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let (rep_psort, rep_created, rep_item): (Option<Vec<u8>>, Option<i64>, Option<String>) =
        match rep {
            Some((p, c, i)) => (Some(p), Some(c), Some(i)),
            None => (None, None, None),
        };
    st(tx.execute(
        "INSERT INTO pqueue_group_summary \
         (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort,\
          rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
         VALUES (?1,?2,?3,?4,NULL,?5,?6,?7,?8,0,?9) \
         ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
          oldest_eligible_at=excluded.oldest_eligible_at, \
          rep_progress_guard_sort=excluded.rep_progress_guard_sort, \
          rep_priority_sort=excluded.rep_priority_sort, rep_created_at=excluded.rep_created_at, \
          rep_item_id=excluded.rep_item_id, eligible_item_count=excluded.eligible_item_count, \
          at_risk_count=excluded.at_risk_count, updated_at=excluded.updated_at",
        params![
            t,
            q,
            group_key.as_str(),
            oldest,
            rep_psort,
            rep_created,
            rep_item,
            count,
            now_n,
        ],
    ))?;
    Ok(())
}

/// Apply one command to `pqueue_items` as SQL. Mirrors `ProjectionData::apply_command` arm-for-arm. The
/// caller must have pre-validated rejectable commands (commit has no rollback past this point), so the
/// only errors here are storage/`NotFound` faults, never behavioral rejections. Live-token mutations are
/// appended to `token_ops` (applied post-commit by the caller), never mutated in place. Grouped-item
/// mutations also refresh `pqueue_group_summary` for the affected group(s) in this same transaction.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_command_sql(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &mut HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    position: &CommandPosition,
    seq: u64,
    now: UtcTimestamp,
    command: &QueueCommand,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    match command {
        // Queue creation is a control-plane concern; idempotent no-op if it reaches the apply path.
        QueueCommand::CreateQueue(_) => {
            claim_scan_hints.remove(shard);
            claim_scan_default_fifo.insert(shard.clone(), true);
            Ok(())
        }
        QueueCommand::Push(c) => {
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            insert_items(tx, queues, &model, shard, &c.items, seq, now)?;
            observe_push_for_claim_scan(claim_scan_hints, claim_scan_default_fifo, shard, &c.items);
            let mut groups: Vec<GroupKey> = Vec::new();
            for it in &c.items {
                if let Some(g) = &it.group_key
                    && !groups.contains(g)
                {
                    groups.push(g.clone());
                }
            }
            if !groups.is_empty() {
                grouped_shards.insert(shard.clone());
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
        QueueCommand::Claim(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let worker_id = c.worker_id.as_ref().map(|worker| worker.as_str());
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            if claim_scan_default_fifo.get(shard).copied().unwrap_or(false)
                && let Some((min_rowid, max_rowid)) =
                    fifo_rowid_range_for_id_strings(tx, shard, &ids, Some("Pending"))?
            {
                let changed = st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?1, \
                     lease_expires_at=?2, worker_id=?3, retry_count=retry_count+1, \
                     item_version=item_version+1, updated_at=?4, last_command_sequence=?5 \
                     WHERE tenant_id=?6 AND queue_id=?7 AND rowid BETWEEN ?8 AND ?9",
                    params![
                        hash, exp, worker_id, now_n, seq as i64, t, q, min_rowid, max_rowid
                    ],
                ))?;
                if changed != ids.len() {
                    return Err(EngineError::Storage(
                        "sqlite fifo claim range update changed an unexpected row count".into(),
                    ));
                }
                let next = max_rowid
                    .checked_add(1)
                    .ok_or_else(|| EngineError::Storage("claim scan hint overflow".into()))?;
                let slot = claim_scan_hints.entry(shard.clone()).or_insert(0);
                if next > *slot {
                    *slot = next;
                }
            } else {
                exec_items_in(
                    tx,
                    "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?, \
                     lease_expires_at=?, worker_id=?, retry_count=retry_count+1, \
                     item_version=item_version+1, updated_at=?, last_command_sequence=? \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[
                        Value::Blob(hash),
                        Value::Integer(exp),
                        worker_id.map_or(Value::Null, |worker| Value::Text(worker.to_string())),
                        Value::Integer(now_n),
                        Value::Integer(seq as i64),
                    ],
                    &t,
                    &q,
                    &ids,
                )?;
                advance_claim_scan_hint_for_ids(
                    tx,
                    claim_scan_hints,
                    claim_scan_default_fifo,
                    shard,
                    &c.item_ids,
                )?;
            }
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &c.item_ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
            }
            Ok(())
        }
        QueueCommand::CohortClaim(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?, \
                 lease_expires_at=?, retry_count=retry_count+1, item_version=item_version+1, \
                 updated_at=?, last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Blob(hash.clone()),
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state='leased', cohort_lease_token_hash=?4 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                params![t, q, c.cohort_id.as_str(), hash],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &c.item_ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
            }
            Ok(())
        }
        QueueCommand::RenewLease(c) => {
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lease_expires_at=?, item_version=item_version+1, \
                 updated_at=?, last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            extend_claim_by_query_idempotency_for_renewal(
                tx,
                shard,
                &c.item_ids,
                c.lease_expires_at,
            )?;
            Ok(())
        }
        QueueCommand::CohortRenewLease(c) => {
            let ids = cohort_item_ids(tx, shard, &c.cohort_id)?;
            let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
            let exp = ts_nanos(c.lease_expires_at);
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lease_expires_at=?, item_version=item_version+1, \
                 updated_at=?, last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &id_strs,
            )?;
            Ok(())
        }
        QueueCommand::ReassignLease(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lease_token_hash=?, lease_expires_at=?, \
                 retry_count=retry_count+1, item_version=item_version+1, updated_at=?, \
                 last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Blob(hash),
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            Ok(())
        }
        QueueCommand::UpdateFields(c) => {
            // FAC-1 in-place merge of a LIVE item's fields/payload (no lifecycle change). Read-merge-write
            // the `fields` JSON map in the same representation as insert/read (`fields_to_json`/
            // `fields_from_json`), apply the per-key delta, then UPDATE within this transaction. The caller
            // pre-validated, so the row is live (Pending/Leased, not superseded/fenced); if it is gone here
            // (a divergence) we apply nothing rather than fault, mirroring the in-memory `debug_assert`.
            let current: Option<String> = st(tx
                .query_row(
                    "SELECT fields FROM pqueue_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                    params![t, q, c.item_id.to_string()],
                    |row| row.get(0),
                )
                .optional())?;
            if let Some(raw) = current {
                let mut fields = fields_from_json(raw)?;
                for (k, op) in &c.field_ops {
                    match op {
                        Some(v) => {
                            fields.insert(k.clone(), v.clone());
                        }
                        None => {
                            fields.remove(k);
                        }
                    }
                }
                let fields_json = fields_to_json(&fields)?;
                match &c.payload {
                    // Keep: leave `payload` untouched (fields-only update).
                    PayloadUpdate::Keep => {
                        st(tx.execute(
                            "UPDATE pqueue_items SET fields=?4, item_version=item_version+1, \
                             updated_at=?5, last_command_sequence=?6 \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                             AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                            params![t, q, c.item_id.to_string(), fields_json, now_n, seq as i64],
                        ))?;
                    }
                    // Set(Some)=replace BLOB, Set(None)=NULL.
                    PayloadUpdate::Set(p) => {
                        let payload = p.as_ref().map(|b| b.to_vec());
                        st(tx.execute(
                            "UPDATE pqueue_items SET fields=?4, payload=?5, item_version=item_version+1, \
                             updated_at=?6, last_command_sequence=?7 \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                             AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                            params![
                                t,
                                q,
                                c.item_id.to_string(),
                                fields_json,
                                payload,
                                now_n,
                                seq as i64,
                            ],
                        ))?;
                    }
                }
                if let Some(ref doc) = c.set_entity_document {
                    st(tx.execute(
                        "UPDATE pqueue_items SET entity_document=?4 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        params![t, q, c.item_id.to_string(), to_json(doc)?],
                    ))?;
                }
                // ADR-011: if a new entity document was supplied, re-index this item. Delete the
                // old rows first so the unique slot is freed before the conflict check fires.
                if let Some(ref doc) = c.set_entity_document {
                    let typed_indexes = queues
                        .get(shard)
                        .map(|d| d.typed_indexes.as_slice())
                        .unwrap_or(&[]);
                    if !typed_indexes.is_empty() {
                        let item_id_str = c.item_id.to_string();
                        delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&item_id_str))?;
                        let new_keys = typed_index_keys_for_entity(typed_indexes, Some(doc))?;
                        check_typed_unique_conflicts(tx, &t, &q, typed_indexes, &new_keys, None)?;
                        insert_typed_index_rows(tx, &t, &q, &item_id_str, &new_keys)?;
                    }
                }
            }
            Ok(())
        }
        QueueCommand::Finalize(c) => {
            // Resolve Retry-exhaustion for all Retry outcomes in ONE read (was one SELECT per outcome).
            let retry_ids: Vec<String> = c
                .outcomes
                .iter()
                .filter(|o| matches!(o.kind, FinalizeKind::Retry))
                .map(|o| o.item_id.to_string())
                .collect();
            let mut retry_info: HashMap<String, (i64, i64)> = HashMap::new();
            for chunk in retry_ids.chunks(SQLITE_BATCH) {
                if chunk.is_empty() {
                    break;
                }
                let ph = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT item_id, retry_count, max_attempts FROM pqueue_items \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
                );
                let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
                for id in chunk {
                    p.push(Value::Text(id.clone()));
                }
                let mut stmt = st(tx.prepare(&sql))?;
                let mapped = st(stmt.query_map(params_from_iter(p.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }))?;
                for r in mapped {
                    let (id, rc, ma) = st(r)?;
                    retry_info.insert(id, (rc, ma));
                }
            }
            // Bucket outcomes by the target SET (disposition fully determines new_state/terminal/reset → at
            // most four buckets), then issue ONE UPDATE per bucket.
            let mut to_complete: Vec<String> = Vec::new();
            let mut to_failed: Vec<String> = Vec::new();
            let mut to_pending: Vec<String> = Vec::new();
            let mut to_pending_rearm: Vec<String> = Vec::new();
            let mut backoff: BTreeMap<i64, Vec<String>> = BTreeMap::new();
            for o in &c.outcomes {
                let id = o.item_id.to_string();
                let new_state = match o.kind {
                    FinalizeKind::Complete => ItemState::Complete,
                    FinalizeKind::Fail => ItemState::Failed,
                    FinalizeKind::Retry => {
                        // Retry-exhaustion (B'): a retry that has used all `max_attempts` deliveries goes
                        // terminal (Failed); under the bound it returns to pending (claimable again).
                        let (rc, ma) = retry_info.get(&id).copied().ok_or(EngineError::NotFound)?;
                        if is_retry_exhausted(rc as u32, ma as u32) {
                            ItemState::Failed
                        } else {
                            ItemState::Pending
                        }
                    }
                    FinalizeKind::Release => ItemState::Pending,
                    FinalizeKind::Rearm => ItemState::Pending,
                };
                match new_state {
                    ItemState::Complete => to_complete.push(id.clone()),
                    ItemState::Failed => to_failed.push(id.clone()),
                    ItemState::Pending if matches!(o.kind, FinalizeKind::Rearm) => {
                        to_pending_rearm.push(id.clone())
                    }
                    ItemState::Pending => to_pending.push(id.clone()),
                    ItemState::Leased => unreachable!("Finalize never targets Leased"),
                }
                // Queue-native retry backoff: a Retry that returned the item to Pending (still under the
                // attempt bound) defers its re-eligibility to `not_before`. Mirror insert_item's pairing of
                // not_before + eligible_since. Grouped by identical not_before so each value is one UPDATE.
                if matches!(o.kind, FinalizeKind::Retry)
                    && new_state == ItemState::Pending
                    && let Some(nb) = o.not_before
                {
                    backoff.entry(ts_nanos(nb)).or_default().push(id.clone());
                }
                token_ops.push(TokenOp::Clear(o.item_id));
            }
            const FINALIZE_SET: &str = "UPDATE pqueue_items SET lifecycle_state=?, lease_token_hash=NULL, \
                 lease_expires_at=NULL, worker_id=NULL, fenced=0, item_version=item_version+1, \
                 retry_count=CASE WHEN ? THEN 0 ELSE retry_count END, terminal_at=?, \
                 terminal_command_epoch=?, updated_at=?, last_command_sequence=? \
                 WHERE tenant_id=? AND queue_id=? AND item_id IN";
            let buckets = [
                (
                    state_str(ItemState::Complete),
                    false,
                    Value::Integer(now_n),
                    Value::Integer(position.backend_epoch as i64),
                    &to_complete,
                ),
                (
                    state_str(ItemState::Failed),
                    false,
                    Value::Integer(now_n),
                    Value::Integer(position.backend_epoch as i64),
                    &to_failed,
                ),
                (
                    state_str(ItemState::Pending),
                    false,
                    Value::Null,
                    Value::Null,
                    &to_pending,
                ),
                (
                    state_str(ItemState::Pending),
                    true,
                    Value::Null,
                    Value::Null,
                    &to_pending_rearm,
                ),
            ];
            for (state, reset, terminal_at, terminal_epoch, ids) in buckets {
                if claim_scan_default_fifo.get(shard).copied().unwrap_or(false)
                    && matches!(state, "Complete" | "Failed")
                    && let Some((min_rowid, max_rowid)) =
                        fifo_rowid_range_for_id_strings(tx, shard, ids, Some("Leased"))?
                {
                    let changed = st(tx.execute(
                        "UPDATE pqueue_items SET lifecycle_state=?1, lease_token_hash=NULL, \
                         lease_expires_at=NULL, worker_id=NULL, fenced=0, item_version=item_version+1, \
                         retry_count=CASE WHEN ?2 THEN 0 ELSE retry_count END, terminal_at=?3, \
                         terminal_command_epoch=?4, updated_at=?5, last_command_sequence=?6 \
                         WHERE tenant_id=?7 AND queue_id=?8 AND rowid BETWEEN ?9 AND ?10",
                        params![
                            state,
                            reset as i64,
                            terminal_at,
                            terminal_epoch,
                            now_n,
                            seq as i64,
                            t,
                            q,
                            min_rowid,
                            max_rowid
                        ],
                    ))?;
                    if changed != ids.len() {
                        return Err(EngineError::Storage(
                            "sqlite fifo finalize range update changed an unexpected row count"
                                .into(),
                        ));
                    }
                } else {
                    exec_items_in(
                        tx,
                        FINALIZE_SET,
                        &[
                            Value::Text(state.to_string()),
                            Value::Integer(reset as i64),
                            terminal_at,
                            terminal_epoch,
                            Value::Integer(now_n),
                            Value::Integer(seq as i64),
                        ],
                        &t,
                        &q,
                        ids,
                    )?;
                }
            }
            for (nb_n, ids) in &backoff {
                exec_items_in(
                    tx,
                    "UPDATE pqueue_items SET not_before=?, eligible_since=? \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[Value::Integer(*nb_n), Value::Integer(*nb_n)],
                    &t,
                    &q,
                    ids,
                )?;
            }
            let ids: Vec<ItemId> = c.outcomes.iter().map(|o| o.item_id).collect();
            if c.outcomes.iter().any(|o| {
                matches!(
                    o.kind,
                    FinalizeKind::Retry | FinalizeKind::Release | FinalizeKind::Rearm
                )
            }) {
                reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            }
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
            }
            Ok(())
        }
        QueueCommand::CohortFinalize(c) => {
            let ids = cohort_item_ids(tx, shard, &c.cohort_id)?;
            if ids.is_empty() {
                return Err(EngineError::NotFound);
            }
            let outcomes: Vec<FinalizeOutcome> = ids
                .iter()
                .map(|item_id| FinalizeOutcome {
                    item_id: *item_id,
                    kind: c.kind,
                    applied_state: None,
                    not_before: c.not_before,
                })
                .collect();
            apply_command_sql(
                tx,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                token_ops,
                shard,
                position,
                seq,
                now,
                &QueueCommand::Finalize(FinalizeCommand { outcomes }),
            )?;
            let next_state = match c.kind {
                FinalizeKind::Complete | FinalizeKind::Fail => "terminal",
                FinalizeKind::Retry | FinalizeKind::Release => "complete",
                FinalizeKind::Rearm => return Err(EngineError::Invalid("cohort rearm is invalid")),
            };
            let retention_until = if next_state == "terminal" {
                Some(cohort_retention_until(queues, shard, now_n)?)
            } else {
                None
            };
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state=?4, cohort_lease_token_hash=NULL, retention_until=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                params![t, q, c.cohort_id.as_str(), next_state, retention_until],
            ))?;
            Ok(())
        }
        QueueCommand::ReplacePending(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            // Supersede the old pending item (drops it from the active partial-unique index + eligibility),
            // then insert the replacement under the same client_item_key.
            // ADR-011: delete the superseded item's index rows first so the replacement can claim
            // the same unique key without a spurious Conflict.
            let superseded_str = c.superseded_item_id.to_string();
            delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&superseded_str))?;
            st(tx.execute(
                "UPDATE pqueue_items SET superseded=1, updated_at=?4, last_command_sequence=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                params![t, q, c.superseded_item_id.to_string(), now_n, seq as i64],
            ))?;
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            insert_items(
                tx,
                queues,
                &model,
                shard,
                std::slice::from_ref(&c.replacement),
                seq,
                now,
            )?;
            // Refresh both the superseded item's group and the replacement's (often the same).
            let mut groups = if grouped_shards.contains(shard) {
                groups_of(tx, shard, std::slice::from_ref(&c.superseded_item_id))?
            } else {
                Vec::new()
            };
            if let Some(g) = &c.replacement.group_key
                && !groups.contains(g)
            {
                grouped_shards.insert(shard.clone());
                groups.push(g.clone());
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
        QueueCommand::LeaseExpired(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lifecycle_state='Pending', lease_token_hash=NULL, \
                 lease_expires_at=NULL, worker_id=NULL, item_version=item_version+1, updated_at=?, \
                 last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[Value::Integer(now_n), Value::Integer(seq as i64)],
                &t,
                &q,
                &ids,
            )?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(*id));
            }
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &c.item_ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
            }
            Ok(())
        }
        QueueCommand::CohortExpired(c) => {
            // Force every non-terminal member of the cohort to Failed (cohort-incomplete).
            let ids: Vec<ItemId> = {
                let mut stmt = st(tx.prepare(
                    "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                     AND group_key=?3 AND lifecycle_state NOT IN ('Complete','Failed')",
                ))?;
                let mapped = st(stmt.query_map(params![t, q, c.group_key.as_str()], |row| {
                    row.get::<_, String>(0)
                }))?;
                let mut out = Vec::new();
                for r in mapped {
                    out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
                }
                out
            };
            let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
            // Force terminal only (parity with the in-memory arm, which leaves the lease fields as-is on the
            // now-terminal row); the live token is dropped from the RAM map post-commit.
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lifecycle_state='Failed', item_version=item_version+1, \
                 terminal_at=?, terminal_command_epoch=?, updated_at=?, last_command_sequence=? \
                 WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Integer(now_n),
                    Value::Integer(position.backend_epoch as i64),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &id_strs,
            )?;
            for id in &ids {
                token_ops.push(TokenOp::Clear(*id));
            }
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state='terminal', expire_command_pos=?4, \
                 cohort_lease_token_hash=NULL, retention_until=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                params![
                    t,
                    q,
                    c.group_key.as_str(),
                    seq as i64,
                    cohort_retention_until(queues, shard, now_n)?,
                ],
            ))?;
            // The whole cohort (group) is now terminal — refresh its summary to empty.
            refresh_group_summary(tx, shard, &c.group_key, now)?;
            Ok(())
        }
        QueueCommand::FenceLease(c) => {
            // Operator fence: no item_version bump (parity with the in-memory arm).
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET fenced=1 WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &ids,
            )?;
            Ok(())
        }
        QueueCommand::UnfenceLease(c) => {
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET fenced=0 WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &ids,
            )?;
            Ok(())
        }
        QueueCommand::PauseQueue(command) => {
            st(tx.execute(
                "UPDATE queues SET paused=1,pause_drain_intake=?3 WHERE tenant=?1 AND queue=?2",
                params![t, q, command.drain_intake],
            ))?;
            Ok(())
        }
        QueueCommand::ResumeQueue => {
            st(tx.execute(
                "UPDATE queues SET paused=0,pause_drain_intake=0 WHERE tenant=?1 AND queue=?2",
                params![t, q],
            ))?;
            Ok(())
        }
        QueueCommand::PurgeItems(c) => {
            let retention_ms = queues
                .get(shard)
                .map(|d| d.client_item_key_retention_ms)
                .unwrap_or(0);
            let id_strs: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            let mut groups: Vec<GroupKey> = Vec::new();
            // (client_item_key, item_id) tombstones for terminal items, deduped LAST-wins on key so the
            // batched upsert never touches the same conflict target twice (DO UPDATE cardinality).
            let mut retention: Vec<(String, String)> = Vec::new();
            // One set-based read of every purged item (was one SELECT per item).
            for chunk in id_strs.chunks(SQLITE_BATCH) {
                let ph = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT item_id, group_key, client_item_key, lifecycle_state FROM pqueue_items \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
                );
                let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
                for id in chunk {
                    p.push(Value::Text(id.clone()));
                }
                let mut stmt = st(tx.prepare(&sql))?;
                let mapped = st(stmt.query_map(params_from_iter(p.iter()), |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                }))?;
                for r in mapped {
                    let (item_id, gk, ck, state) = st(r)?;
                    // TD-002 retention tombstone: purging a TERMINAL item keeps its client_item_key a
                    // duplicate (re-push rejected) until `client_item_key_retention_ms` elapses. A pending
                    // purge records nothing (its key is freely reusable, matching the log-replay family).
                    if parse_state(&state)?.is_terminal() && retention_ms > 0 {
                        retention.retain(|(k, _)| k != &ck);
                        retention.push((ck, item_id));
                    }
                    if let Some(g) = gk {
                        let gk2 =
                            GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?;
                        if !groups.contains(&gk2) {
                            groups.push(gk2);
                        }
                    }
                }
            }
            if !retention.is_empty() {
                let expires = now_n.saturating_add((retention_ms as i64).saturating_mul(1_000_000));
                for chunk in retention.chunks(SQLITE_BATCH) {
                    let values = vec!["(?,?,?,?,?)"; chunk.len()].join(",");
                    let sql = format!(
                        "INSERT INTO pqueue_item_key_retention \
                         (tenant_id,queue_id,client_item_key,item_id,expires_at) VALUES {values} \
                         ON CONFLICT(tenant_id,queue_id,client_item_key) \
                         DO UPDATE SET item_id=excluded.item_id, expires_at=excluded.expires_at"
                    );
                    let mut p: Vec<Value> = Vec::with_capacity(chunk.len() * 5);
                    for (ck, item_id) in chunk {
                        p.push(Value::Text(t.clone()));
                        p.push(Value::Text(q.clone()));
                        p.push(Value::Text(ck.clone()));
                        p.push(Value::Text(item_id.clone()));
                        p.push(Value::Integer(expires));
                    }
                    st(tx.execute(&sql, params_from_iter(p.iter())))?;
                }
            }
            // Set-based deletes (item rows + their gate membership) — one round-trip per chunk each.
            exec_items_in(
                tx,
                "DELETE FROM pqueue_items WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
            // BQ-14d: drop the purged items' gate membership (the anti-join source).
            exec_items_in(
                tx,
                "DELETE FROM pqueue_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
            // ADR-011: drop the purged items' typed secondary index rows.
            delete_typed_index_rows(tx, &t, &q, &id_strs)?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(*id));
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
        QueueCommand::SetGates(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            // BQ-14d (TD-002 §gate): set/clear queue gate-key block state. A blocked gate key makes every
            // item carrying it ineligible (enforced by the eligibility anti-join). This is exact-on-read:
            // toggling a gate flips eligibility on the next claim with no per-item rewrite.
            if c.blocked {
                for gk in &c.gate_keys {
                    st(tx.execute(
                        "INSERT INTO pqueue_gate_state (tenant_id,queue_id,gate_key) VALUES (?1,?2,?3) \
                         ON CONFLICT(tenant_id,queue_id,gate_key) DO NOTHING",
                        params![t, q, gk.as_str()],
                    ))?;
                }
            } else {
                for gk in &c.gate_keys {
                    st(tx.execute(
                        "DELETE FROM pqueue_gate_state WHERE tenant_id=?1 AND queue_id=?2 AND gate_key=?3",
                        params![t, q, gk.as_str()],
                    ))?;
                }
            }
            Ok(())
        }
        // C9 (epic pqueue-2201fd37): opaque NON-WORK side records (Snorri authoritative-commit boundary).
        // Upsert each (key,payload) into `pqueue_side_records` — a table disjoint from `pqueue_items`, so a
        // side record is never claimable/eligible/peekable nor counted as work. Apply is infallible
        // (insert-or-overwrite by key), exactly like the in-memory `side_records` map.
        QueueCommand::WriteSideRecords(c) => {
            for rec in &c.records {
                st(tx.execute(
                    "INSERT INTO pqueue_side_records (tenant_id,queue_id,key,payload) \
                     VALUES (?1,?2,?3,?4) \
                     ON CONFLICT(tenant_id,queue_id,key) DO UPDATE SET payload=excluded.payload",
                    params![t, q, rec.key, rec.payload.as_ref()],
                ))?;
            }
            Ok(())
        }
        // C6 (epic pqueue-2201fd37): advance a caller-supplied opaque instance/state fence. Validated
        // pre-commit (stored==expected, next>expected), so the upsert is infallible. Disjoint from
        // `pqueue_items` — a fence is never claimable/peekable work.
        QueueCommand::AdvanceInstanceFence(c) => {
            st(tx.execute(
                "INSERT INTO pqueue_instance_fences (tenant_id,queue_id,instance_key,fence) \
                 VALUES (?1,?2,?3,?4) \
                 ON CONFLICT(tenant_id,queue_id,instance_key) DO UPDATE SET fence=excluded.fence",
                params![t, q, c.instance_key, c.next as i64],
            ))?;
            Ok(())
        }
    }
}
