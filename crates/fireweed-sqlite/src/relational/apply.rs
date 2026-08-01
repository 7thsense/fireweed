use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use axon_esf::CompiledSchema;
use fireweed_core::{
    CohortId, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityModel, QueueDefinition,
    QueueId, TenantId, UtcTimestamp, is_retry_exhausted,
};
use fireweed_engine::{
    CommandPosition, EngineError, EngineResult, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    PayloadUpdate, PushItem, QueueCommand, QueueKey, ResolvedItemMutationAction, ScheduleUpdate,
    SetGatesCommand, compile_entity_schema,
};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

use super::*;

type UpdateFieldsRow = (
    String,
    String,
    Option<String>,
    Option<i64>,
    i64,
    Option<Vec<u8>>,
    String,
);

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
    /// Item ids are queue-local, so the queue key is part of the identity here as it is in SQLite.
    pub(crate) live_tokens: HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    pub(crate) live_tokens_by_consumer: HashMap<QueueKey, HashMap<LeaseToken, BTreeSet<ItemId>>>,
}

impl Inner {
    /// Rebuild the in-RAM definition cache from the durable `queues` table. The item projection itself is
    /// already durable in `fireweed_items` as a rebuildable cache - nothing to replay.
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
            "SELECT DISTINCT tenant_id, queue_id FROM fireweed_items WHERE group_key IS NOT NULL",
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

    /// Assign the next command sequence for `shard`, apply `command` to `fireweed_items`, and advance the
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
            live_tokens_by_consumer,
            ..
        } = self;
        let (t, q) = parts(shard);
        let tx = st(conn.transaction())?;
        // ADR-009 / TD-003: read the durable assignment_epoch with the cursor and fence against the owner's
        // cached acquire-time epoch (`Some`) — a superseded owner is rejected `EpochFenced`, nothing applied.
        // `None` is the degenerate sole-owner path (no fence). Brings this data-plane path to parity with the
        // typed relational commit seam.
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
        apply_token_ops(live_tokens, live_tokens_by_consumer, token_ops); // only after a durable commit (F4)
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// apply: the 14-arm command -> SQL projection write (the BQ-11a headline)
// ---------------------------------------------------------------------------

/// Max rows per dynamically-built multi-row / `IN (...)` statement. Each `fireweed_items` row binds 19
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

/// One item row to materialize, carrying the durable command sequence and wall time of the Push (or
/// ReplacePending) that minted it. Group-commit apply coalesces many single-item Push envelopes into one
/// multi-row insert while preserving per-command `last_command_sequence` and timestamps.
pub(crate) struct InsertItemSpec<'a> {
    pub item: &'a PushItem,
    pub command_seq: u64,
    pub now: UtcTimestamp,
}

/// Batch-insert all `items` of a Push (or the single ReplacePending replacement) as set-based statements:
/// chunked multi-row INSERTs into `fireweed_items`, `fireweed_item_gates`, and `fireweed_cohorts` — replacing the
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
    let specs: Vec<InsertItemSpec<'_>> = items
        .iter()
        .map(|item| InsertItemSpec {
            item,
            command_seq: seq,
            now,
        })
        .collect();
    insert_item_specs(tx, queues, model, shard, &specs)
}

/// Like [`insert_items`], but each row may carry its own command sequence and timestamp (coalesced Push
/// envelopes from `apply_committed_batch_sql`).
pub(crate) fn insert_item_specs(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    model: &PriorityModel,
    shard: &QueueKey,
    specs: &[InsertItemSpec<'_>],
) -> EngineResult<()> {
    if specs.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    // Bulk-allocate the stable FIFO positions in one read+advance (was a read+UPDATE per item).
    let base_seq: i64 = st(tx.query_row(
        "SELECT next_item_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
        params![t, q],
        |row| row.get(0),
    ))?;
    st(tx.execute(
        "UPDATE relational_cursor SET next_item_seq=?3 WHERE tenant=?1 AND queue=?2",
        params![t, q, base_seq + specs.len() as i64],
    ))?;
    let typed_indexes = queues
        .get(shard)
        .map(|d| d.typed_indexes.as_slice())
        .unwrap_or(&[]);
    let items_only: Vec<&PushItem> = specs.iter().map(|s| s.item).collect();
    // Fast path: homogeneous default-empty rows with a single wall clock (common internal benches).
    let first_now = specs[0].now;
    let homogeneous_now = specs.iter().all(|s| s.now == first_now);
    if typed_indexes.is_empty()
        && homogeneous_now
        && items_only
            .iter()
            .all(|item| is_default_empty_push_item(item))
    {
        insert_default_empty_item_specs(tx, &t, &q, specs, base_seq)?;
        return Ok(());
    }
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(specs.len());
    for (i, spec) in specs.iter().enumerate() {
        let item = spec.item;
        let now_n = ts_nanos(spec.now);
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
            Value::Integer(spec.command_seq as i64),
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
            "INSERT INTO fireweed_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,entity_document,retry_count,\
              item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,terminal_command_epoch,fenced,superseded,max_attempts,created_seq) VALUES {values}"
        );
        // prepare_cached: chunk lengths are stable (full SQLITE_BATCH or a fixed remainder per batch size),
        // so statement compile cost is paid once per distinct SQL shape rather than once per chunk execute.
        let mut stmt = st(tx.prepare_cached(&sql))?;
        let flat = chunk.iter().flatten();
        st(stmt.execute(params_from_iter(flat)))?;
    }
    insert_gates(tx, &t, &q, &items_only)?;
    // Cohort id stamping uses wall time; for a coalesced run use the latest now in the run.
    let cohort_now_n = specs
        .iter()
        .map(|s| ts_nanos(s.now))
        .max()
        .unwrap_or_else(|| ts_nanos(first_now));
    upsert_cohorts(tx, queues, shard, &t, &q, &items_only, cohort_now_n)?;
    // ADR-011: typed secondary index maintenance.
    maintain_typed_indexes_on_insert(tx, &t, &q, typed_indexes, &items_only)?;
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

pub(crate) fn insert_default_empty_item_specs(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    specs: &[InsertItemSpec<'_>],
    base_seq: i64,
) -> EngineResult<()> {
    const ROW_PH: &str = "(?,?,?,?,'Pending',NULL,X'01',NULL,?,NULL,NULL,NULL,'{}','{}',NULL,0,1,NULL,NULL,NULL,?,?,?,NULL,NULL,0,0,?,?)";
    for (chunk_idx, chunk) in specs.chunks(SQLITE_BATCH).enumerate() {
        let values = vec![ROW_PH; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO fireweed_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,entity_document,retry_count,\
             item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,terminal_command_epoch,fenced,superseded,max_attempts,created_seq) VALUES {values}"
        );
        let mut params = Vec::with_capacity(chunk.len() * 10);
        let offset = chunk_idx * SQLITE_BATCH;
        for (i, spec) in chunk.iter().enumerate() {
            let now_n = ts_nanos(spec.now);
            params.push(Value::Text(t.to_string()));
            params.push(Value::Text(q.to_string()));
            let item_id = spec.item.item_id.to_string();
            params.push(Value::Text(item_id.clone()));
            params.push(Value::Text(item_id));
            params.push(Value::Integer(now_n));
            params.push(Value::Integer(spec.command_seq as i64));
            params.push(Value::Integer(now_n));
            params.push(Value::Integer(now_n));
            params.push(Value::Integer(spec.item.max_attempts as i64));
            params.push(Value::Integer(base_seq + offset as i64 + i as i64));
        }
        let mut stmt = st(tx.prepare_cached(&sql))?;
        st(stmt.execute(params_from_iter(params.iter())))?;
    }
    Ok(())
}

/// Batch the per-item gate-membership inserts (BQ-14d) into chunked multi-row INSERTs. Pairs are deduped so
/// one statement never proposes the same `(item_id, gate_key)` twice.
pub(crate) fn insert_gates(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    items: &[&PushItem],
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
            "INSERT INTO fireweed_item_gates (tenant_id,queue_id,item_id,gate_key) VALUES {values} \
             ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING"
        );
        let mut p: Vec<Value> = Vec::with_capacity(chunk.len() * 4);
        for (id, g) in chunk {
            p.push(Value::Text(t.to_string()));
            p.push(Value::Text(q.to_string()));
            p.push(Value::Text(id.clone()));
            p.push(Value::Text(g.clone()));
        }
        let mut stmt = st(tx.prepare_cached(&sql))?;
        st(stmt.execute(params_from_iter(p.iter())))?;
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
    items: &[&PushItem],
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
                "SELECT cohort_size, member_count, state, retention_until FROM fireweed_cohorts \
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
                    "INSERT INTO fireweed_cohorts \
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
                        "UPDATE fireweed_cohorts SET cohort_id=?4, cohort_size=?5, member_count=?6, \
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
                    "UPDATE fireweed_cohorts SET member_count=?4, state=?5, \
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
            "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
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
            "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
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
        "DELETE FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN",
        &[],
        &t,
        &q,
        &ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
    )?;
    let id_strs = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    exec_items_in(
        tx,
        "DELETE FROM fireweed_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN",
        &[],
        &t,
        &q,
        &id_strs,
    )?;
    delete_typed_index_rows(tx, &t, &q, &id_strs)?;
    // Preserve the mint-counter recovery floor BEFORE the deleted rows vanish: the surviving `fireweed_items`
    // are no longer the complete minted set, so recovery must restore the id ceiling from here or it could
    // re-mint a reaped id (ADR-009 id-uniqueness). Runs in the same reap transaction, so the floor advance is
    // atomic with the deletion — a crash never leaves rows deleted without the floor recorded.
    advance_id_high_water_sql(tx, shard, &ids)?;
    Ok(ids)
}

/// A deferred mutation of the in-RAM live-token map, collected during apply and replayed onto the map
/// ONLY after the transaction commits — so a commit failure can never leave the RAM tokens ahead of the
/// durable `fireweed_items` state (F4).
pub(crate) enum TokenOp {
    Set(QueueKey, ItemId, LeaseToken),
    Clear(QueueKey, ItemId),
}

/// Collect process-local lease cleartext ops from a command without mutating durable SQL.
///
/// Used when replaying a prefix the projection has already absorbed (`incoming < cursor`):
/// durable rows keep only `lease_token_hash`, so reopen must rehydrate [`Inner::live_tokens`]
/// from the authoritative log or render_claimed / renew after kill returns `StaleLease`.
pub(crate) fn collect_token_ops_from_command(
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    command: &QueueCommand,
) {
    match command {
        QueueCommand::Claim(c) => {
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
            }
        }
        QueueCommand::CohortClaim(c) => {
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
            }
        }
        QueueCommand::ReassignLease(c) => {
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
            }
        }
        QueueCommand::Finalize(c) => {
            for outcome in &c.outcomes {
                token_ops.push(TokenOp::Clear(shard.clone(), outcome.item_id));
            }
        }
        QueueCommand::PurgeItems(c) => {
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(shard.clone(), *id));
            }
        }
        QueueCommand::LeaseExpired(c) => {
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(shard.clone(), *id));
            }
        }
        QueueCommand::ReplacePending(c) => {
            // Replace supersedes the prior item; clear any prior lease cleartext.
            token_ops.push(TokenOp::Clear(shard.clone(), c.superseded_item_id));
        }
        _ => {}
    }
}

pub(crate) fn apply_token_ops(
    live_tokens: &mut HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    by_consumer: &mut HashMap<QueueKey, HashMap<LeaseToken, BTreeSet<ItemId>>>,
    ops: Vec<TokenOp>,
) {
    for op in ops {
        match op {
            TokenOp::Set(shard, id, token) => {
                if let Some(old) = live_tokens
                    .entry(shard.clone())
                    .or_default()
                    .insert(id, token.clone())
                    && let Some(consumers) = by_consumer.get_mut(&shard)
                    && let Some(ids) = consumers.get_mut(&old)
                {
                    ids.remove(&id);
                    if ids.is_empty() {
                        consumers.remove(&old);
                    }
                }
                by_consumer
                    .entry(shard)
                    .or_default()
                    .entry(token)
                    .or_default()
                    .insert(id);
            }
            TokenOp::Clear(shard, id) => {
                if let Some(tokens) = live_tokens.get_mut(&shard) {
                    let old = tokens.remove(&id);
                    if tokens.is_empty() {
                        live_tokens.remove(&shard);
                    }
                    if let Some(old) = old
                        && let Some(consumers) = by_consumer.get_mut(&shard)
                    {
                        if let Some(ids) = consumers.get_mut(&old) {
                            ids.remove(&id);
                            if ids.is_empty() {
                                consumers.remove(&old);
                            }
                        }
                        if consumers.is_empty() {
                            by_consumer.remove(&shard);
                        }
                    }
                }
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
            "SELECT DISTINCT group_key FROM fireweed_items WHERE tenant_id=? AND queue_id=? \
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
        "SELECT group_key FROM fireweed_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
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
        "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
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

/// Recompute `fireweed_group_summary` for one group from `fireweed_items` (exact aggregate over the group's
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
/// Refresh an arbitrary number of group summaries with one set-based statement.  The target CTE is
/// deliberately authoritative: groups which now have no eligible items are still upserted with a zero
/// count, so callers never need a per-group cleanup loop.
pub(crate) fn refresh_group_summaries(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    group_keys: &[GroupKey],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if group_keys.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let targets =
        serde_json::to_string(&group_keys.iter().map(GroupKey::as_str).collect::<Vec<_>>())
            .map_err(|error| EngineError::Storage(error.to_string()))?;
    st(tx.execute(
        "WITH target(group_key) AS (SELECT DISTINCT value FROM json_each(?3)), \
         eligible AS (SELECT i.group_key,i.eligible_since,i.priority_sort,i.created_at,i.item_id,i.created_seq \
           FROM fireweed_items i JOIN target t ON t.group_key=i.group_key \
           WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.lifecycle_state='Pending' AND i.superseded=0 \
           AND (i.not_before IS NULL OR i.not_before<=?4) AND NOT EXISTS (SELECT 1 \
             FROM fireweed_item_gates ig JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id \
             AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=i.tenant_id \
             AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id)), \
         ranked AS (SELECT *,ROW_NUMBER() OVER (PARTITION BY group_key ORDER BY priority_sort,created_seq) AS rn FROM eligible), \
         aggregate AS (SELECT group_key,COUNT(*) AS item_count,MIN(eligible_since) AS oldest FROM eligible GROUP BY group_key) \
         INSERT INTO fireweed_group_summary \
         (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort,\
          rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
         SELECT ?1,?2,t.group_key,a.oldest,NULL,r.priority_sort,r.created_at,r.item_id,COALESCE(a.item_count,0),0,?4 \
         FROM target t LEFT JOIN aggregate a ON a.group_key=t.group_key \
         LEFT JOIN ranked r ON r.group_key=t.group_key AND r.rn=1 WHERE true \
         ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
          oldest_eligible_at=excluded.oldest_eligible_at, \
          rep_progress_guard_sort=excluded.rep_progress_guard_sort, \
          rep_priority_sort=excluded.rep_priority_sort, rep_created_at=excluded.rep_created_at, \
          rep_item_id=excluded.rep_item_id, eligible_item_count=excluded.eligible_item_count, \
          at_risk_count=excluded.at_risk_count, updated_at=excluded.updated_at",
        params![t, q, targets, now_n],
    ))?;
    Ok(())
}

/// Apply one command to `fireweed_items` as SQL. Mirrors `ProjectionData::apply_command` arm-for-arm. The
/// caller must have pre-validated rejectable commands (commit has no rollback past this point), so the
/// only errors here are storage/`NotFound` faults, never behavioral rejections. Live-token mutations are
/// appended to `token_ops` (applied post-commit by the caller), never mutated in place. Grouped-item
/// mutations also refresh `fireweed_group_summary` for the affected group(s) in this same transaction.
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
            let minted_ids: Vec<ItemId> = c.items.iter().map(|item| item.item_id).collect();
            advance_id_high_water_sql(tx, shard, &minted_ids)?;
            let item_refs: Vec<&PushItem> = c.items.iter().collect();
            observe_push_for_claim_scan(
                claim_scan_hints,
                claim_scan_default_fifo,
                shard,
                &item_refs,
            );
            let groups: HashSet<GroupKey> = c
                .items
                .iter()
                .filter_map(|item| item.group_key.clone())
                .collect();
            if !groups.is_empty() {
                grouped_shards.insert(shard.clone());
            }
            let groups: Vec<GroupKey> = groups.into_iter().collect();
            refresh_group_summaries(tx, shard, &groups, now)?;
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
                    "UPDATE fireweed_items SET lifecycle_state='Leased', lease_token_hash=?1, \
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
                    "UPDATE fireweed_items SET lifecycle_state='Leased', lease_token_hash=?, \
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
                token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
            }
            if grouped_shards.contains(shard) {
                let groups = groups_of(tx, shard, &c.item_ids)?;
                refresh_group_summaries(tx, shard, &groups, now)?;
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
                "UPDATE fireweed_items SET lifecycle_state='Leased', lease_token_hash=?, \
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
                "UPDATE fireweed_cohorts SET state='leased', cohort_lease_token_hash=?4 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                params![t, q, c.cohort_id.as_str(), hash],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
            }
            if grouped_shards.contains(shard) {
                let groups = groups_of(tx, shard, &c.item_ids)?;
                refresh_group_summaries(tx, shard, &groups, now)?;
            }
            Ok(())
        }
        QueueCommand::RenewLease(c) => {
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE fireweed_items SET lease_expires_at=?, item_version=item_version+1, \
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
                "UPDATE fireweed_items SET lease_expires_at=?, item_version=item_version+1, \
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
                "UPDATE fireweed_items SET lease_token_hash=?, lease_expires_at=?, \
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
                token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
            }
            Ok(())
        }
        QueueCommand::UpdateFields(c) => {
            // FAC-1 in-place merge of a LIVE item's fields/payload (no lifecycle change). Read-merge-write
            // the `fields` JSON map in the same representation as insert/read (`fields_to_json`/
            // `fields_from_json`), apply the per-key delta, then UPDATE within this transaction. The caller
            // pre-validated, so the row is live (Pending/Leased, not superseded/fenced); if it is gone here
            // (a divergence) we apply nothing rather than fault, mirroring the in-memory `debug_assert`.
            let current: Option<UpdateFieldsRow> = st(tx
                .query_row(
                    "SELECT fields,lifecycle_state,priority,not_before,eligible_since,payload,metadata FROM fireweed_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                    params![t, q, c.item_id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
                )
                .optional())?;
            if let Some((
                raw,
                _lifecycle_state,
                mut priority_json,
                mut not_before,
                mut eligible_since,
                mut payload,
                mut metadata_json,
            )) = current
            {
                let mut fields = c.set_fields.clone().unwrap_or(fields_from_json(raw)?);
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
                if let PayloadUpdate::Set(next) = &c.payload {
                    payload = next.as_ref().map(|bytes| bytes.to_vec());
                }
                if let Some(metadata) = &c.set_metadata {
                    metadata_json = metadata_to_json(metadata)?;
                }
                if let ScheduleUpdate::Set(next) = &c.set_priority {
                    priority_json = next.as_ref().map(to_json).transpose()?;
                }
                if let ScheduleUpdate::Set(next) = &c.set_not_before {
                    not_before = (*next).map(ts_nanos);
                    if !c.api001_batch {
                        eligible_since = not_before.unwrap_or(now_n).max(now_n);
                    }
                }
                let priority = priority_json
                    .as_deref()
                    .map(|raw| {
                        serde_json::from_str(raw)
                            .map_err(|error| EngineError::Storage(error.to_string()))
                    })
                    .transpose()?;
                let priority_sort = elig_sort(
                    &priority,
                    &queues
                        .get(shard)
                        .ok_or(EngineError::NotFound)?
                        .priority_model,
                );
                st(tx.execute(
                    "UPDATE fireweed_items SET fields=?4,payload=?5,metadata=?6,priority=?7,priority_sort=?8, \
                     not_before=?9,eligible_since=?10,item_version=item_version+1,updated_at=?11,last_command_sequence=?12 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                    params![t, q, c.item_id.to_string(), fields_json, payload, metadata_json,
                        priority_json, priority_sort, not_before, eligible_since, now_n, seq as i64],
                ))?;
                if let Some(gate_keys) = &c.set_gate_keys {
                    st(tx.execute(
                        "DELETE FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        params![t, q, c.item_id.to_string()],
                    ))?;
                    for gate_key in gate_keys {
                        st(tx.execute(
                            "INSERT OR IGNORE INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) VALUES(?1,?2,?3,?4)",
                            params![t, q, c.item_id.to_string(), gate_key],
                        ))?;
                    }
                }
                if let Some(ref doc) = c.set_entity_document {
                    st(tx.execute(
                        "UPDATE fireweed_items SET entity_document=?4 \
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
                    "SELECT item_id, retry_count, max_attempts FROM fireweed_items \
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
            let mut rearm_schedule: BTreeMap<(Option<i64>, i64), Vec<String>> = BTreeMap::new();
            for o in &c.outcomes {
                let id = o.item_id.to_string();
                let computed_state = match o.kind {
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
                if o.applied_state
                    .is_some_and(|sealed| sealed != computed_state)
                {
                    return Err(EngineError::Conflict);
                }
                let new_state = o.applied_state.unwrap_or(computed_state);
                match new_state {
                    ItemState::Complete => to_complete.push(id.clone()),
                    ItemState::Failed => to_failed.push(id.clone()),
                    ItemState::Pending if matches!(o.kind, FinalizeKind::Rearm) => {
                        to_pending_rearm.push(id.clone());
                        let not_before = o.not_before.map(ts_nanos);
                        rearm_schedule
                            .entry((not_before, not_before.unwrap_or(now_n).max(now_n)))
                            .or_default()
                            .push(id.clone());
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
                token_ops.push(TokenOp::Clear(shard.clone(), o.item_id));
            }
            const FINALIZE_SET: &str = "UPDATE fireweed_items SET lifecycle_state=?, lease_token_hash=NULL, \
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
                        "UPDATE fireweed_items SET lifecycle_state=?1, lease_token_hash=NULL, \
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
                    "UPDATE fireweed_items SET not_before=?, eligible_since=? \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[Value::Integer(*nb_n), Value::Integer(*nb_n)],
                    &t,
                    &q,
                    ids,
                )?;
            }
            for ((not_before, eligible_since), ids) in &rearm_schedule {
                exec_items_in(
                    tx,
                    "UPDATE fireweed_items SET not_before=?, eligible_since=? \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[opt_int(*not_before), Value::Integer(*eligible_since)],
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
                let groups = groups_of(tx, shard, &ids)?;
                refresh_group_summaries(tx, shard, &groups, now)?;
            }
            Ok(())
        }
        QueueCommand::CohortFinalize(c) => {
            let ids = cohort_item_ids(tx, shard, &c.cohort_id)?;
            if ids.is_empty() {
                return Err(EngineError::NotFound);
            }
            let effective_kind = if matches!(c.kind, FinalizeKind::Retry) {
                let id_strings = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
                let placeholders = std::iter::repeat_n("?", id_strings.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "SELECT retry_count,max_attempts FROM fireweed_items \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN ({placeholders})"
                );
                let mut stmt = st(tx.prepare(&sql))?;
                let params = std::iter::once(Value::Text(t.to_string()))
                    .chain(std::iter::once(Value::Text(q.to_string())))
                    .chain(id_strings.into_iter().map(Value::Text))
                    .collect::<Vec<_>>();
                let rows = st(stmt.query_map(rusqlite::params_from_iter(params), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                }))?;
                let exhausted = rows
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| EngineError::Storage(error.to_string()))?
                    .into_iter()
                    .any(|(attempts, max_attempts)| {
                        is_retry_exhausted(attempts as u32, max_attempts as u32)
                    });
                if exhausted {
                    FinalizeKind::Fail
                } else {
                    FinalizeKind::Retry
                }
            } else {
                c.kind
            };
            let effective_not_before = matches!(effective_kind, FinalizeKind::Retry)
                .then_some(c.not_before)
                .flatten();
            let outcomes = ids
                .iter()
                .map(|item_id| FinalizeOutcome {
                    item_id: *item_id,
                    kind: effective_kind,
                    applied_state: None,
                    not_before: effective_not_before,
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
            let next_state = match effective_kind {
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
                "UPDATE fireweed_cohorts SET state=?4, cohort_lease_token_hash=NULL, retention_until=?5 \
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
                "UPDATE fireweed_items SET superseded=1, updated_at=?4, last_command_sequence=?5 \
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
            refresh_group_summaries(tx, shard, &groups, now)?;
            Ok(())
        }
        QueueCommand::LeaseExpired(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE fireweed_items SET lifecycle_state='Pending', lease_token_hash=NULL, \
                 lease_expires_at=NULL, worker_id=NULL, item_version=item_version+1, updated_at=?, \
                 last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[Value::Integer(now_n), Value::Integer(seq as i64)],
                &t,
                &q,
                &ids,
            )?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(shard.clone(), *id));
            }
            if grouped_shards.contains(shard) {
                let groups = groups_of(tx, shard, &c.item_ids)?;
                refresh_group_summaries(tx, shard, &groups, now)?;
            }
            Ok(())
        }
        QueueCommand::CohortExpired(c) => {
            // Force every non-terminal member of the cohort to Failed (cohort-incomplete).
            let ids: Vec<ItemId> = {
                let mut stmt = st(tx.prepare(
                    "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
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
                "UPDATE fireweed_items SET lifecycle_state='Failed', item_version=item_version+1, \
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
                token_ops.push(TokenOp::Clear(shard.clone(), *id));
            }
            st(tx.execute(
                "UPDATE fireweed_cohorts SET state='terminal', expire_command_pos=?4, \
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
            refresh_group_summaries(tx, shard, std::slice::from_ref(&c.group_key), now)?;
            Ok(())
        }
        QueueCommand::FenceLease(c) => {
            // Operator fence: no item_version bump (parity with the in-memory arm).
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE fireweed_items SET fenced=1 WHERE tenant_id=? AND queue_id=? AND item_id IN",
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
                "UPDATE fireweed_items SET fenced=0 WHERE tenant_id=? AND queue_id=? AND item_id IN",
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
            // (client_item_key, item_id) tombstones for every removed item, deduped LAST-wins on key so the
            // batched upsert never touches the same conflict target twice (DO UPDATE cardinality).
            let mut retention: Vec<(String, String)> = Vec::new();
            // One set-based read of every purged item (was one SELECT per item).
            for chunk in id_strs.chunks(SQLITE_BATCH) {
                let ph = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT item_id, group_key, client_item_key, lifecycle_state FROM fireweed_items \
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
                    // API-001/TD-002 retention tombstone: every successful removal keeps its
                    // client_item_key a duplicate until `client_item_key_retention_ms` elapses,
                    // regardless of the item's lifecycle state at removal.
                    let _ = parse_state(&state)?;
                    if retention_ms > 0 {
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
                        "INSERT INTO fireweed_item_key_retention \
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
                "DELETE FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
            // BQ-14d: drop the purged items' gate membership (the anti-join source).
            exec_items_in(
                tx,
                "DELETE FROM fireweed_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
            // ADR-011: drop the purged items' typed secondary index rows.
            delete_typed_index_rows(tx, &t, &q, &id_strs)?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(shard.clone(), *id));
            }
            refresh_group_summaries(tx, shard, &groups, now)?;
            Ok(())
        }
        QueueCommand::SetGates(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            // BQ-14d (TD-002 §gate): set/clear queue gate-key block state. A blocked gate key makes every
            // item carrying it ineligible (enforced by the eligibility anti-join). This is exact-on-read:
            // toggling a gate flips eligibility on the next claim with no per-item rewrite.
            if c.blocked {
                for chunk in c.gate_keys.chunks((SQLITE_BATCH / 3).max(1)) {
                    let values = vec!["(?,?,?)"; chunk.len()].join(",");
                    let mut parameters = Vec::with_capacity(chunk.len() * 3);
                    for gate in chunk {
                        parameters.extend([
                            Value::Text(t.clone()),
                            Value::Text(q.clone()),
                            Value::Text(gate.as_str().to_string()),
                        ]);
                    }
                    st(tx.execute(
                        &format!(
                            "INSERT INTO fireweed_gate_state (tenant_id,queue_id,gate_key) VALUES {values} \
                             ON CONFLICT(tenant_id,queue_id,gate_key) DO NOTHING"
                        ),
                        params_from_iter(parameters.iter()),
                    ))?;
                }
            } else {
                for chunk in c.gate_keys.chunks(SQLITE_BATCH) {
                    let placeholders = vec!["?"; chunk.len()].join(",");
                    let mut parameters = Vec::with_capacity(chunk.len() + 2);
                    parameters.extend([Value::Text(t.clone()), Value::Text(q.clone())]);
                    parameters.extend(
                        chunk
                            .iter()
                            .map(|gate| Value::Text(gate.as_str().to_string())),
                    );
                    st(tx.execute(
                        &format!(
                            "DELETE FROM fireweed_gate_state WHERE tenant_id=? AND queue_id=? \
                             AND gate_key IN ({placeholders})"
                        ),
                        params_from_iter(parameters.iter()),
                    ))?;
                }
            }
            Ok(())
        }
        // C9 (epic pqueue-2201fd37): opaque NON-WORK side records (Snorri authoritative-commit boundary).
        // Upsert each (key,payload) into `fireweed_side_records` — a table disjoint from `fireweed_items`, so a
        // side record is never claimable/eligible/peekable nor counted as work. Apply is infallible
        // (insert-or-overwrite by key), exactly like the in-memory `side_records` map.
        QueueCommand::WriteSideRecords(c) => {
            for chunk in c.records.chunks((SQLITE_BATCH / 4).max(1)) {
                let values = vec!["(?,?,?,?)"; chunk.len()].join(",");
                let mut parameters = Vec::with_capacity(chunk.len() * 4);
                for record in chunk {
                    parameters.extend([
                        Value::Text(t.clone()),
                        Value::Text(q.clone()),
                        Value::Blob(record.key.clone()),
                        Value::Blob(record.payload.to_vec()),
                    ]);
                }
                st(tx.execute(
                    &format!(
                        "INSERT INTO fireweed_side_records (tenant_id,queue_id,key,payload) \
                         VALUES {values} ON CONFLICT(tenant_id,queue_id,key) DO UPDATE SET \
                         payload=excluded.payload"
                    ),
                    params_from_iter(parameters.iter()),
                ))?;
            }
            Ok(())
        }
        // C6 (epic pqueue-2201fd37): advance a caller-supplied opaque instance/state fence. Validated
        // pre-commit (stored==expected, next>expected), so the upsert is infallible. Disjoint from
        // `fireweed_items` — a fence is never claimable/peekable work.
        QueueCommand::AdvanceInstanceFence(c) => {
            st(tx.execute(
                "INSERT INTO fireweed_instance_fences (tenant_id,queue_id,instance_key,fence) \
                 VALUES (?1,?2,?3,?4) \
                 ON CONFLICT(tenant_id,queue_id,instance_key) DO UPDATE SET fence=excluded.fence",
                params![t, q, c.instance_key, c.next as i64],
            ))?;
            Ok(())
        }
        QueueCommand::MutateItems(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            let item_ids = c.items.iter().map(|item| item.item_id).collect::<Vec<_>>();
            let groups = groups_of(tx, shard, &item_ids)?;
            let typed_indexes = queues
                .get(shard)
                .map(|definition| definition.typed_indexes.as_slice())
                .unwrap_or(&[]);

            for item in &c.items {
                let item_id = item.item_id.to_string();
                match &item.action {
                    ResolvedItemMutationAction::Purge => {
                        let exists: bool = st(tx.query_row(
                            "SELECT EXISTS(SELECT 1 FROM fireweed_items \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3)",
                            params![t, q, item_id],
                            |row| row.get(0),
                        ))?;
                        if !exists {
                            return Err(EngineError::Conflict);
                        }
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
                            &QueueCommand::PurgeItems(fireweed_engine::PurgeItemsCommand {
                                item_ids: vec![item.item_id],
                                force: true,
                            }),
                        )?;
                    }
                    ResolvedItemMutationAction::Replace(values) => {
                        let priority_json = values.priority.as_ref().map(to_json).transpose()?;
                        let priority_sort = elig_sort(
                            &values.priority,
                            &queues
                                .get(shard)
                                .ok_or(EngineError::NotFound)?
                                .priority_model,
                        );
                        let terminal =
                            matches!(values.state, ItemState::Complete | ItemState::Failed);
                        let (lease_hash_sql, lease_expiry_sql, worker_sql, fenced_sql) = if values
                            .invalidate_lease
                        {
                            (Value::Null, Value::Null, Value::Null, Value::Integer(0))
                        } else {
                            let current: (Option<Vec<u8>>, Option<i64>, Option<String>, i64) = st(tx.query_row(
                                    "SELECT lease_token_hash,lease_expires_at,worker_id,fenced FROM fireweed_items \
                                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                                    params![t, q, item_id],
                                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                                ))?;
                            (
                                current.0.map(Value::Blob).unwrap_or(Value::Null),
                                current.1.map(Value::Integer).unwrap_or(Value::Null),
                                current.2.map(Value::Text).unwrap_or(Value::Null),
                                Value::Integer(current.3),
                            )
                        };
                        let changed = st(tx.execute(
                            "UPDATE fireweed_items SET lifecycle_state=?4,priority=?5,priority_sort=?6,not_before=?7,\
                             eligible_since=?8,payload=?9,fields=?10,metadata=?11,entity_document=?12,\
                             lease_token_hash=?13,lease_expires_at=?14,worker_id=?15,fenced=?16,item_version=?17,\
                             terminal_at=?18,terminal_command_epoch=?19,updated_at=?20,last_command_sequence=?21 \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 AND item_version=?22",
                            params![
                                t, q, item_id, state_str(values.state), priority_json, priority_sort,
                                values.not_before.map(ts_nanos), ts_nanos(values.eligible_since),
                                values.payload.as_ref().map(|payload| payload.to_vec()),
                                fields_to_json(&values.fields)?, metadata_to_json(&values.metadata)?,
                                values.entity_document.as_ref().map(to_json).transpose()?,
                                lease_hash_sql, lease_expiry_sql, worker_sql, fenced_sql,
                                values.item_version as i64,
                                terminal.then_some(now_n),
                                terminal.then_some(position.backend_epoch as i64),
                                now_n, seq as i64, values.item_version.saturating_sub(1) as i64,
                            ],
                        ))?;
                        if changed != 1 {
                            return Err(EngineError::Conflict);
                        }
                        st(tx.execute(
                            "DELETE FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                            params![t, q, item_id],
                        ))?;
                        for gate_key in &values.gate_keys {
                            st(tx.execute(
                                "INSERT INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) VALUES(?1,?2,?3,?4)",
                                params![t, q, item_id, gate_key],
                            ))?;
                        }
                        if !typed_indexes.is_empty() {
                            delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&item_id))?;
                            let keys = typed_index_keys_for_entity(
                                typed_indexes,
                                values.entity_document.as_ref(),
                            )?;
                            check_typed_unique_conflicts(tx, &t, &q, typed_indexes, &keys, None)?;
                            insert_typed_index_rows(tx, &t, &q, &item_id, &keys)?;
                        }
                        if values.invalidate_lease {
                            token_ops.push(TokenOp::Clear(shard.clone(), item.item_id));
                        }
                    }
                }
            }

            for change in &c.gate_changes {
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
                    &QueueCommand::SetGates(SetGatesCommand {
                        gate_keys: change.gate_keys.clone(),
                        blocked: change.blocked,
                    }),
                )?;
            }
            refresh_group_summaries(tx, shard, &groups, now)?;
            Ok(())
        }
    }
}
