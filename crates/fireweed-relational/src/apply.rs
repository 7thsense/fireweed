//! Relational projection apply used by the native Turso adapter.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use fireweed_core::{
    CohortId, GroupKey, IndexDeclaration, ItemId, ItemState, LeaseToken, Metadata, PriorityModel,
    QueueDefinition, QueueIndex, UtcTimestamp, is_retry_exhausted,
};
use fireweed_engine::{
    BatchUpdateResponse, ClaimCommand, CommandEnvelope, CommandPosition, EngineError, EngineResult,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, PayloadUpdate, PushItem, QueueCommand,
    QueueKey, RequestOutcome, ResolvedItemMutationAction, ScheduleUpdate, SetGatesCommand,
    UpdateFieldsCommand,
};
use serde_json::Value as JsonValue;

use crate::{
    RelRow, RelTx, RelValue, TypedIndexRows, elig_sort, fields_from_json, fields_to_json,
    has_blocked_gates, lease_hash, metadata_to_json, observe_push_for_claim_scan,
    observe_uniform_schedule_for_claim_scan, parse_priority, parse_state, parts,
    reset_claim_scan_hint, state_str, to_json, ts_nanos, ts_nanos_opt,
};

pub use crate::RELATIONAL_BATCH as SQLITE_BATCH;

const TYPED_INDEX_CHECK_CHUNK: usize = 1_000;
const TYPED_INDEX_INSERT_CHUNK: usize = 1_500;
/// Turso/SQLite default variable limit. VALUES and IN-list chunks must stay at or under this.
pub const SQLITE_BIND_CAP: usize = 900;

fn bind_chunk_size(per_row: usize, extra: usize) -> usize {
    (SQLITE_BIND_CAP.saturating_sub(extra) / per_row.max(1)).max(1)
}
const IDEMPOTENCY_OPERATION_CLAIM: &str = "claim";
const IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY: &str = "claim_by_query";
const IDEMPOTENCY_OPERATION_PUSH: &str = "push";
const IDEMPOTENCY_OPERATION_BATCH_UPDATE: &str = "batch_update";
const IDEMPOTENCY_OPERATION_ITEM_MUTATION: &str = "item_mutation";
const IDEMPOTENCY_OPERATION_COMMIT: &str = "commit";

/// Sixteen row binds and four shared binds fit 56 replacements under the
/// 900-bind ceiling. A 16-row cap increased measured campaign CPU and wall time.
pub const CLEARING_ITEM_REPLACEMENT_BATCH: usize = 56;

/// SQL used by the adapter for bounded resolved replacement writes.
/// The incoming rows drive full-key target and rowid seeks directly, avoiding
/// an intermediate rowid list and a second index over the incoming values.
/// Exposed for native query-plan qualification of the executed statement.
pub fn clearing_item_replacements_sql(row_count: usize) -> String {
    let values_sql = vec!["(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; row_count].join(",");
    format!(
        "WITH incoming(item_id,state,priority,priority_sort,not_before,eligible_since,fields,metadata,\
             entity_document,index_fields,new_version,terminal_at,terminal_epoch,expected_version,claimed_here,keeps_payload) \
             AS (VALUES {values_sql}) UPDATE fireweed_items SET \
             lifecycle_state=incoming.state,priority=incoming.priority,priority_sort=incoming.priority_sort,\
             not_before=incoming.not_before,eligible_since=incoming.eligible_since,\
             payload=CASE WHEN incoming.keeps_payload THEN fireweed_items.payload ELSE NULL END,\
             fields=incoming.fields,metadata=incoming.metadata,entity_document=incoming.entity_document,\
             index_fields=incoming.index_fields,lease_token_hash=NULL,lease_expires_at=NULL,worker_id=NULL,fenced=0,\
             item_version=incoming.new_version,terminal_at=incoming.terminal_at,terminal_command_epoch=incoming.terminal_epoch,\
             updated_at=?,last_command_sequence=?,retry_count=fireweed_items.retry_count+incoming.claimed_here \
             FROM incoming CROSS JOIN fireweed_items target \
             INDEXED BY sqlite_autoindex_fireweed_items_1 \
             ON target.tenant_id=? AND target.queue_id=? AND target.item_id=incoming.item_id \
             WHERE fireweed_items.rowid=target.rowid \
             AND fireweed_items.item_id=incoming.item_id \
             AND fireweed_items.item_version=incoming.expected_version \
             AND (incoming.claimed_here=0 OR (fireweed_items.lifecycle_state='Pending' AND fireweed_items.superseded=0))"
    )
}

/// Apply independent lease-clearing replacements in bounded VALUES chunks.
/// Every row keeps its version and fused-claim guard. A short update is an error;
/// the owning apply transaction rolls back this chunk and all preceding chunks.
#[allow(
    clippy::too_many_arguments,
    reason = "SQL replacement binds retain explicit command position, time, and fused-claim guards"
)]
fn apply_clearing_item_replacements(
    tx: &impl RelTx,
    shard: &QueueKey,
    definition: &QueueDefinition,
    position: &CommandPosition,
    seq: u64,
    now: UtcTimestamp,
    items: &[fireweed_engine::ResolvedItemMutation],
    fused_claims: Option<&HashSet<ItemId>>,
) -> EngineResult<()> {
    const ROW_BINDS: usize = 16;
    let (tenant, queue) = parts(shard);
    let now_n = ts_nanos(now);
    for chunk in items.chunks(bind_chunk_size(ROW_BINDS, 4).min(CLEARING_ITEM_REPLACEMENT_BATCH)) {
        let mut params = Vec::with_capacity(chunk.len() * ROW_BINDS + 4);
        for item in chunk {
            let values = item
                .action
                .replacement_values()
                .ok_or(EngineError::Conflict)?;
            if !values.invalidate_lease {
                return Err(EngineError::Conflict);
            }
            let claimed_here =
                i64::from(fused_claims.is_some_and(|ids| ids.contains(&item.item_id)));
            let terminal = matches!(values.state, ItemState::Complete | ItemState::Failed);
            params.extend([
                item.item_id.to_string().into(),
                state_str(values.state).into(),
                values.priority.as_ref().map(to_json).transpose()?.into(),
                elig_sort(&values.priority, &definition.priority_model).into(),
                values.not_before.map(ts_nanos).into(),
                ts_nanos(values.eligible_since).into(),
                fields_to_json(&values.fields)?.into(),
                metadata_to_json(&values.metadata)?.into(),
                values
                    .entity_document
                    .as_ref()
                    .map(to_json)
                    .transpose()?
                    .into(),
                fireweed_engine::index_fields::encode_index_fields_blob(&values.index_fields)?
                    .into(),
                (values.item_version as i64).into(),
                terminal.then_some(now_n).into(),
                terminal.then_some(position.backend_epoch as i64).into(),
                (values.item_version.saturating_sub(1 + claimed_here as u64) as i64).into(),
                claimed_here.into(),
                item.action.keeps_payload().into(),
            ]);
        }
        params.extend([
            now_n.into(),
            (seq as i64).into(),
            RelValue::Text(tenant.clone()),
            RelValue::Text(queue.clone()),
        ]);
        let changed =
            crate::rel_exec_owned(tx, &clearing_item_replacements_sql(chunk.len()), params)?;
        if changed != chunk.len() {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

fn request_expires_at(
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<i64> {
    let retention_ms = queues
        .get(shard)
        .map(|definition| definition.request_id_retention_ms)
        .ok_or(EngineError::NotFound)?;
    Ok(ts_nanos(now).saturating_add((retention_ms as i64).saturating_mul(1_000_000)))
}

fn command_positions_json(position: &CommandPosition) -> EngineResult<String> {
    serde_json::to_string(&vec![(position.backend_epoch, position.sequence)])
        .map_err(|error| EngineError::Storage(error.to_string()))
}

/// Opportunistic bounded cleanup uses the same expiry boundary as request replay.
/// Run inside the apply transaction, before inserting a fresh receipt. Queue expiry
/// index avoids scanning the queue's live receipt population when nothing has expired.
fn collect_expired_request_rows(
    tx: &impl RelTx,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let rows = crate::rel_query(
        tx,
        "SELECT rowid,operation,request_id FROM fireweed_request_idempotency \
         INDEXED BY fireweed_request_idempotency_queue_expiry_idx \
         WHERE expires_at<=?1 AND tenant_id=?2 AND queue_id=?3 ORDER BY expires_at,rowid LIMIT 64",
        [
            RelValue::Integer(ts_nanos(now)),
            RelValue::Text(t.clone()),
            RelValue::Text(q.clone()),
        ],
    )?;
    if rows.is_empty() {
        return Ok(());
    }
    let mut rowids = Vec::with_capacity(rows.len());
    let mut claims = Vec::new();
    for row in rows {
        rowids.push(RelValue::Integer(row.get::<i64>(0)?));
        if row.get::<String>(1)? == IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY {
            claims.push(RelValue::Text(row.get::<String>(2)?));
        }
    }
    if !claims.is_empty() {
        let values = vec!["(?)"; claims.len()].join(",");
        let mut params = claims;
        params.extend([RelValue::Text(t), RelValue::Text(q)]);
        crate::rel_exec(
            tx,
            &format!(
                "WITH expired(request_id) AS (VALUES {values}) \
            DELETE FROM fireweed_claim_replay_items WHERE rowid IN ( \
            SELECT c.rowid FROM expired CROSS JOIN fireweed_claim_replay_items c \
            INDEXED BY sqlite_autoindex_fireweed_claim_replay_items_1 \
            ON c.tenant_id=? AND c.queue_id=? AND c.request_id=expired.request_id)"
            ),
            params,
        )?;
    }
    let placeholders = vec!["?"; rowids.len()].join(",");
    crate::rel_exec(
        tx,
        &format!("DELETE FROM fireweed_request_idempotency WHERE rowid IN ({placeholders})"),
        rowids,
    )?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "Parameters correspond to persisted request columns and the conflict policy"
)]
fn persist_request_row(
    tx: &impl RelTx,
    shard: &QueueKey,
    operation: &str,
    request_id: &str,
    fingerprint: Vec<u8>,
    response_payload: String,
    position: &CommandPosition,
    created_at: UtcTimestamp,
    expires_at: i64,
    extend_only: bool,
) -> EngineResult<()> {
    collect_expired_request_rows(tx, shard, created_at)?;
    let (t, q) = parts(shard);
    let conflict = if extend_only {
        "ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
         expires_at=max(fireweed_request_idempotency.expires_at,excluded.expires_at) \
         WHERE fireweed_request_idempotency.request_fingerprint=excluded.request_fingerprint \
           AND fireweed_request_idempotency.response_payload=excluded.response_payload"
    } else {
        "ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
         request_fingerprint=excluded.request_fingerprint,response_payload=excluded.response_payload,\
         command_positions=excluded.command_positions,expires_at=excluded.expires_at,\
         created_at=excluded.created_at \
         WHERE fireweed_request_idempotency.expires_at<=excluded.created_at OR \
          (fireweed_request_idempotency.request_fingerprint=excluded.request_fingerprint \
           AND fireweed_request_idempotency.response_payload=excluded.response_payload)"
    };
    let affected = crate::rel_exec(
        tx,
        &format!(
            "INSERT INTO fireweed_request_idempotency \
             (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
              command_positions,expires_at,created_at) \
             VALUES (?,?,?,?,?,?,?,?,?) {conflict}"
        ),
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            RelValue::Text(operation.to_string()),
            RelValue::Text(request_id.to_string()),
            RelValue::Blob(fingerprint),
            RelValue::Text(response_payload),
            RelValue::Text(command_positions_json(position)?),
            RelValue::Integer(expires_at),
            RelValue::Integer(ts_nanos(created_at)),
        ],
    )?;
    if affected == 0 {
        return Err(EngineError::RequestIdConflict);
    }
    Ok(())
}

/// Persist a replayable request-id outcome in the same apply transaction.
pub fn persist_request_outcome_sql(
    tx: &impl RelTx,
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    envelope: &CommandEnvelope,
    position: &CommandPosition,
) -> EngineResult<()> {
    if let (
        Some(request_id),
        Some(fingerprint),
        Some(RequestOutcome::ItemMutation { response_payload }),
    ) = (
        envelope.request_id.as_ref(),
        envelope.request_fingerprint,
        envelope.request_outcome.as_ref(),
    ) {
        let _: fireweed_engine::ItemMutationResponse = serde_json::from_str(response_payload)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        return persist_request_row(
            tx,
            shard,
            IDEMPOTENCY_OPERATION_ITEM_MUTATION,
            request_id.as_str(),
            fingerprint.to_be_bytes().to_vec(),
            response_payload.clone(),
            position,
            envelope.created_at,
            request_expires_at(queues, shard, envelope.created_at)?,
            false,
        );
    }
    if let (
        Some(request_id),
        Some(fingerprint),
        Some(RequestOutcome::BatchUpdate { response_payload }),
    ) = (
        envelope.request_id.as_ref(),
        envelope.request_fingerprint,
        envelope.request_outcome.as_ref(),
    ) {
        let _: BatchUpdateResponse = serde_json::from_str(response_payload)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        return persist_request_row(
            tx,
            shard,
            IDEMPOTENCY_OPERATION_BATCH_UPDATE,
            request_id.as_str(),
            fingerprint.to_be_bytes().to_vec(),
            response_payload.clone(),
            position,
            envelope.created_at,
            request_expires_at(queues, shard, envelope.created_at)?,
            false,
        );
    }
    if let (
        Some(request_id),
        Some(fingerprint),
        Some(RequestOutcome::CommitTransition { entries }),
    ) = (
        envelope.request_id.as_ref(),
        envelope.request_fingerprint,
        envelope.request_outcome.as_ref(),
    ) {
        let response_payload = serde_json::to_string(entries)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        return persist_request_row(
            tx,
            shard,
            IDEMPOTENCY_OPERATION_COMMIT,
            request_id.as_str(),
            fingerprint.to_be_bytes().to_vec(),
            response_payload,
            position,
            envelope.created_at,
            request_expires_at(queues, shard, envelope.created_at)?,
            false,
        );
    }
    if let (
        Some(request_id),
        Some(fingerprint),
        Some(RequestOutcome::BatchClaim {
            item_ids,
            lease_token,
            worker_id,
        }),
    ) = (
        envelope.request_id.as_ref(),
        envelope.request_fingerprint,
        envelope.request_outcome.as_ref(),
    ) {
        let response = serde_json::to_string(&serde_json::json!({
            "item_ids": item_ids,
            "lease_token": lease_token,
            "worker_id": worker_id,
        }))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
        persist_request_row(
            tx,
            shard,
            IDEMPOTENCY_OPERATION_CLAIM,
            request_id.as_str(),
            fingerprint.to_be_bytes().to_vec(),
            response,
            position,
            envelope.created_at,
            ts_nanos(match envelope.command {
                QueueCommand::Claim(ref claim) if !item_ids.is_empty() => claim.lease_expires_at,
                _ => envelope.created_at,
            })
            .max(request_expires_at(queues, shard, envelope.created_at)?),
            true,
        )?;
        return Ok(());
    }
    if let (
        Some(request_id),
        Some(fingerprint),
        Some(RequestOutcome::ClaimByQuery {
            item_ids,
            lease_token,
            worker_id,
        }),
    ) = (
        envelope.request_id.as_ref(),
        envelope.request_fingerprint,
        envelope.request_outcome.as_ref(),
    ) {
        let response = serde_json::to_string(&serde_json::json!({
            "item_ids": item_ids,
            "lease_token": lease_token,
            "worker_id": worker_id,
        }))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
        persist_request_row(
            tx,
            shard,
            IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY,
            request_id.as_str(),
            fingerprint.to_be_bytes().to_vec(),
            response,
            position,
            envelope.created_at,
            ts_nanos(match envelope.command {
                QueueCommand::Claim(ref claim) if !item_ids.is_empty() => claim.lease_expires_at,
                QueueCommand::CohortClaim(ref claim) if !item_ids.is_empty() => {
                    claim.lease_expires_at
                }
                _ => envelope.created_at,
            })
            .max(request_expires_at(queues, shard, envelope.created_at)?),
            true,
        )?;
        let ids =
            serde_json::to_string(&item_ids.iter().map(ToString::to_string).collect::<Vec<_>>())
                .map_err(|error| EngineError::Storage(error.to_string()))?;
        let (t, q) = parts(shard);
        crate::rel_exec(
            tx,
            "INSERT OR IGNORE INTO fireweed_claim_replay_items \
             (tenant_id,queue_id,request_id,item_id) \
             SELECT ?,?,?,value FROM json_each(?)",
            [
                RelValue::Text(t.to_string()),
                RelValue::Text(q.to_string()),
                RelValue::Text(request_id.as_str().to_string()),
                RelValue::Text(ids),
            ],
        )?;
        return Ok(());
    }
    if let (
        Some(request_id),
        Some(fingerprint),
        Some(RequestOutcome::ClaimByItemIds {
            claimed_item_ids,
            lease_token,
            outcomes,
            worker_id,
        }),
    ) = (
        envelope.request_id.as_ref(),
        envelope.request_fingerprint,
        envelope.request_outcome.as_ref(),
    ) {
        let response = serde_json::to_string(&serde_json::json!({
            "claimed_item_ids": claimed_item_ids, "lease_token": lease_token,
            "outcomes": outcomes, "worker_id": worker_id,
        }))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
        let lease_expiry = match &envelope.command {
            QueueCommand::Claim(claim) if !claimed_item_ids.is_empty() => {
                ts_nanos(claim.lease_expires_at)
            }
            _ => ts_nanos(envelope.created_at),
        };
        return persist_request_row(
            tx,
            shard,
            "claim_by_item_ids",
            request_id.as_str(),
            fingerprint.to_be_bytes().to_vec(),
            response,
            position,
            envelope.created_at,
            lease_expiry.max(request_expires_at(queues, shard, envelope.created_at)?),
            true,
        );
    }
    let QueueCommand::Push(_) = &envelope.command else {
        return Ok(());
    };
    let Some(request_id) = envelope.request_id.as_ref() else {
        return Ok(());
    };
    let (Some(fingerprint), Some(RequestOutcome::Push { item_ids })) = (
        envelope.request_fingerprint,
        envelope.request_outcome.as_ref(),
    ) else {
        return Ok(());
    };
    let response =
        serde_json::to_string(&item_ids.iter().map(ToString::to_string).collect::<Vec<_>>())
            .map_err(|error| EngineError::Storage(error.to_string()))?;
    persist_request_row(
        tx,
        shard,
        IDEMPOTENCY_OPERATION_PUSH,
        request_id.as_str(),
        // Admission can discard a fully indexed entity document. Hashing the
        // admitted PushItems therefore need not identify the caller's body.
        // Persist the original request fingerprint carried by the durable log,
        // just as log-only replay does, rather than inventing a new identity.
        fingerprint.to_be_bytes().to_vec(),
        response,
        position,
        envelope.created_at,
        request_expires_at(queues, shard, envelope.created_at)?,
        false,
    )
}

/// Apply many already-durable commands in one RelTx. Reads each queue cursor once and writes it
/// once at the end. Consecutive Push envelopes coalesce into one insert; Claim+Complete fuse into
/// one UPDATE; consecutive Complete-only Finalize envelopes coalesce into one VALUES UPDATE;
/// consecutive set-based UpdateFields coalesce into VALUES UPDATE.
#[allow(clippy::too_many_arguments)]
pub fn apply_committed_batch_sql(
    tx: &impl RelTx,
    queues: &HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &mut HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
) -> EngineResult<bool> {
    apply_committed_batch_sql_with_cursor_seeds(
        tx,
        queues,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        token_ops,
        positions,
        envelopes,
        &HashMap::new(),
    )
}

/// Apply commands with transaction-local cursor values already read by the driver.
///
/// A driver that must read the same cursor to enforce a live epoch can pass that value here and
/// avoid repeating the query through its synchronous compatibility bridge. Recovery callers leave
/// the seed map empty and retain the normal durable cursor lookup.
#[allow(clippy::too_many_arguments)]
pub fn apply_committed_batch_sql_with_cursor_seeds(
    tx: &impl RelTx,
    queues: &HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &mut HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    cursor_seeds: &HashMap<QueueKey, i64>,
) -> EngineResult<bool> {
    if positions.len() != envelopes.len() {
        return Err(EngineError::Storage(
            "apply_committed_batch: positions/envelopes length mismatch".into(),
        ));
    }
    if positions.is_empty() {
        return Ok(false);
    }
    let mut applied_update_fields = false;
    let mut next_seq = cursor_seeds.clone();
    let mut max_epoch: HashMap<QueueKey, i64> = HashMap::new();
    let mut i = 0usize;
    while i < positions.len() {
        let pos = &positions[i];
        let env = &envelopes[i];
        let (t, q) = parts(&pos.queue);
        let cursor = match next_seq.get(&pos.queue) {
            Some(&n) => n,
            None => {
                let n: i64 = crate::query_optional(
                    tx,
                    "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    [RelValue::Text(t.to_string()), RelValue::Text(q.to_string())],
                    |row| row.get(0),
                )?
                .ok_or(EngineError::NotFound)?;
                next_seq.insert(pos.queue.clone(), n);
                n
            }
        };
        let incoming = pos.sequence as i64;
        if incoming < cursor {
            collect_token_ops_from_command(token_ops, &pos.queue, &env.command);
            i += 1;
            continue;
        }
        if incoming > cursor {
            return Err(EngineError::Storage(format!(
                "relational projection replay gap for {}:{}: expected sequence {cursor}, got {incoming}",
                pos.queue.tenant_id.as_str(),
                pos.queue.queue_id.as_str()
            )));
        }
        if !queues.contains_key(&pos.queue) {
            return Err(EngineError::NotFound);
        }

        if !grouped_shards.contains(&pos.queue)
            && let Some((run_end, fused)) = claim_mutation_run(positions, envelopes, i)
        {
            for index in i..run_end {
                let position = &positions[index];
                let envelope = &envelopes[index];
                if let QueueCommand::Claim(claim) = &envelope.command {
                    let mut remaining = claim.clone();
                    remaining.item_ids.retain(|id| !fused.contains(id));
                    if !remaining.item_ids.is_empty() {
                        apply_command_sql(
                            tx,
                            queues,
                            grouped_shards,
                            claim_scan_hints,
                            claim_scan_default_fifo,
                            token_ops,
                            &position.queue,
                            position,
                            position.sequence,
                            envelope.created_at,
                            &QueueCommand::Claim(remaining),
                        )?;
                    }
                    crate::delete_claim_outbox(tx, &t, &q, &envelope.command_id.0)?;
                } else {
                    apply_command_sql_with_claims(
                        tx,
                        queues,
                        grouped_shards,
                        claim_scan_hints,
                        claim_scan_default_fifo,
                        token_ops,
                        &position.queue,
                        position,
                        position.sequence,
                        envelope.created_at,
                        &envelope.command,
                        Some(&fused),
                    )?;
                }
                persist_request_outcome_sql(tx, queues, &position.queue, envelope, position)?;
            }
            let last = &positions[run_end - 1];
            let next = last
                .sequence
                .checked_add(1)
                .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
            next_seq.insert(pos.queue.clone(), next as i64);
            max_epoch
                .entry(pos.queue.clone())
                .and_modify(|epoch| *epoch = (*epoch).max(last.backend_epoch as i64))
                .or_insert(last.backend_epoch as i64);
            i = run_end;
            continue;
        }

        if let QueueCommand::Claim(claim) = &env.command
            && i + 1 < positions.len()
            && positions[i + 1].queue == pos.queue
            && positions[i + 1].sequence == pos.sequence.saturating_add(1)
            && let QueueCommand::Finalize(fin) = &envelopes[i + 1].command
            && finalize_completes_claim(claim, fin)
        {
            apply_fused_claim_complete_sql(
                tx,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                token_ops,
                &pos.queue,
                &positions[i + 1],
                envelopes[i + 1].created_at,
                claim,
            )?;
            crate::delete_claim_outbox(tx, &t, &q, &env.command_id.0)?;
            persist_request_outcome_sql(tx, queues, &pos.queue, env, pos)?;
            persist_request_outcome_sql(
                tx,
                queues,
                &positions[i + 1].queue,
                &envelopes[i + 1],
                &positions[i + 1],
            )?;
            let fin_seq = positions[i + 1].sequence as i64;
            let new_next = fin_seq
                .checked_add(1)
                .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
            next_seq.insert(pos.queue.clone(), new_next);
            let e = positions[i + 1].backend_epoch as i64;
            let slot = max_epoch.entry(pos.queue.clone()).or_insert(e);
            if e > *slot {
                *slot = e;
            }
            i += 2;
            continue;
        }

        if let Some((claim_end, complete_end)) =
            coalescible_claim_complete_runs(positions, envelopes, i)
        {
            let shard = pos.queue.clone();
            apply_fused_claim_complete_run_sql(
                tx,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                token_ops,
                &shard,
                &positions[i..claim_end],
                &envelopes[i..claim_end],
                &positions[claim_end..complete_end],
                &envelopes[claim_end..complete_end],
            )?;
            let mut exp = incoming;
            for (p, e) in positions[i..complete_end]
                .iter()
                .zip(&envelopes[i..complete_end])
            {
                if matches!(e.command, QueueCommand::Claim(_)) {
                    let (tenant, queue) = parts(&p.queue);
                    crate::delete_claim_outbox(tx, &tenant, &queue, &e.command_id.0)?;
                }
                persist_request_outcome_sql(tx, queues, &p.queue, e, p)?;
                exp = (p.sequence as i64)
                    .checked_add(1)
                    .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
                let ep = p.backend_epoch as i64;
                let slot = max_epoch.entry(p.queue.clone()).or_insert(ep);
                if ep > *slot {
                    *slot = ep;
                }
            }
            next_seq.insert(shard, exp);
            i = complete_end;
            continue;
        }

        if let Some(run_end) = coalescible_claim_run_end(positions, envelopes, i) {
            let shard = pos.queue.clone();
            apply_claim_run_sql(
                tx,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                token_ops,
                &shard,
                &positions[i..run_end],
                &envelopes[i..run_end],
            )?;
            let mut exp = incoming;
            for (p, e) in positions[i..run_end].iter().zip(&envelopes[i..run_end]) {
                let (tenant, queue) = parts(&p.queue);
                crate::delete_claim_outbox(tx, &tenant, &queue, &e.command_id.0)?;
                persist_request_outcome_sql(tx, queues, &p.queue, e, p)?;
                exp = (p.sequence as i64)
                    .checked_add(1)
                    .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
                let ep = p.backend_epoch as i64;
                let slot = max_epoch.entry(p.queue.clone()).or_insert(ep);
                if ep > *slot {
                    *slot = ep;
                }
            }
            next_seq.insert(shard, exp);
            i = run_end;
            continue;
        }

        if let Some(run_end) = coalescible_complete_run_end(positions, envelopes, i) {
            let shard = pos.queue.clone();
            apply_complete_run_sql(
                tx,
                token_ops,
                &shard,
                &positions[i..run_end],
                &envelopes[i..run_end],
            )?;
            let mut exp = incoming;
            for (p, e) in positions[i..run_end].iter().zip(&envelopes[i..run_end]) {
                persist_request_outcome_sql(tx, queues, &p.queue, e, p)?;
                exp = (p.sequence as i64)
                    .checked_add(1)
                    .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
                let ep = p.backend_epoch as i64;
                let slot = max_epoch.entry(p.queue.clone()).or_insert(ep);
                if ep > *slot {
                    *slot = ep;
                }
            }
            next_seq.insert(shard, exp);
            i = run_end;
            continue;
        }

        if matches!(env.command, QueueCommand::Push(_)) {
            let shard = pos.queue.clone();
            let mut run_end = i;
            let mut expected = incoming;
            while run_end < positions.len() {
                let p = &positions[run_end];
                let e = &envelopes[run_end];
                if p.queue != shard || !matches!(e.command, QueueCommand::Push(_)) {
                    break;
                }
                let seq_i = p.sequence as i64;
                if seq_i < expected {
                    run_end += 1;
                    continue;
                }
                if seq_i > expected {
                    break;
                }
                expected = seq_i
                    .checked_add(1)
                    .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
                run_end += 1;
            }
            apply_push_run_sql(
                tx,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                &shard,
                &positions[i..run_end],
                &envelopes[i..run_end],
                cursor,
            )?;
            let mut exp = cursor;
            for (p, e) in positions[i..run_end]
                .iter()
                .zip(envelopes[i..run_end].iter())
            {
                let seq_i = p.sequence as i64;
                if seq_i < exp {
                    continue;
                }
                persist_request_outcome_sql(tx, queues, &p.queue, e, p)?;
                exp = seq_i
                    .checked_add(1)
                    .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
                let ep = p.backend_epoch as i64;
                let slot = max_epoch.entry(p.queue.clone()).or_insert(ep);
                if ep > *slot {
                    *slot = ep;
                }
            }
            next_seq.insert(shard, exp);
            i = run_end;
            continue;
        }

        if let Some(run_end) = coalescible_update_run_end(positions, envelopes, i) {
            let shard = pos.queue.clone();
            let mut collected = Vec::new();
            let mut last_seq = pos.sequence;
            let mut last_now = env.created_at;
            for (p, e) in positions[i..run_end].iter().zip(&envelopes[i..run_end]) {
                last_seq = p.sequence;
                last_now = e.created_at;
                match &e.command {
                    QueueCommand::UpdateFields(update) => collected.push(update.clone()),
                    QueueCommand::UpdateFieldsBatch(batch) => {
                        collected.extend(batch.updates.iter().cloned())
                    }
                    _ => {}
                }
            }
            apply_update_fields_batch_sql(
                tx,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                &shard,
                last_seq,
                last_now,
                &collected,
            )?;
            applied_update_fields = true;
            let mut exp = incoming;
            for (p, e) in positions[i..run_end].iter().zip(&envelopes[i..run_end]) {
                persist_request_outcome_sql(tx, queues, &p.queue, e, p)?;
                exp = (p.sequence as i64)
                    .checked_add(1)
                    .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
                let ep = p.backend_epoch as i64;
                let slot = max_epoch.entry(p.queue.clone()).or_insert(ep);
                if ep > *slot {
                    *slot = ep;
                }
            }
            next_seq.insert(shard, exp);
            i = run_end;
            continue;
        }

        applied_update_fields |= matches!(
            env.command,
            QueueCommand::UpdateFields(_) | QueueCommand::UpdateFieldsBatch(_)
        );
        apply_command_sql(
            tx,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            token_ops,
            &pos.queue,
            pos,
            pos.sequence,
            env.created_at,
            &env.command,
        )?;
        if matches!(env.command, QueueCommand::Claim(_)) {
            crate::delete_claim_outbox(tx, &t, &q, &env.command_id.0)?;
        }
        persist_request_outcome_sql(tx, queues, &pos.queue, env, pos)?;
        let new_next = incoming
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
        next_seq.insert(pos.queue.clone(), new_next);
        let e = pos.backend_epoch as i64;
        let slot = max_epoch.entry(pos.queue.clone()).or_insert(e);
        if e > *slot {
            *slot = e;
        }
        i += 1;
    }
    for (queue, &next) in &next_seq {
        let (t, q) = parts(queue);
        let epoch = max_epoch.get(queue).copied().unwrap_or(0);
        crate::rel_exec(
            tx,
            "UPDATE relational_cursor SET \
             next_seq=?3, \
             assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
             WHERE tenant=?1 AND queue=?2",
            [
                RelValue::Text(t.to_string()),
                RelValue::Text(q.to_string()),
                RelValue::Integer(next),
                RelValue::Integer(epoch),
            ],
        )?;
    }
    Ok(applied_update_fields)
}

/// Fuse claims with one later lease-invalidating replacement in a contiguous
/// Claim/MutateItems run, including interleaved independent handlers. Commands
/// retain their order; unpaired rows retain the ordinary claim path. A repeated
/// identity or an unsupported command ends the window before that command.
fn claim_mutation_run(
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    start: usize,
) -> Option<(usize, HashSet<ItemId>)> {
    let first = positions.get(start)?;
    if !matches!(envelopes.get(start)?.command, QueueCommand::Claim(_)) {
        return None;
    }
    let contiguous = |i: usize| {
        positions[i].queue == first.queue
            && positions[i].backend_epoch == first.backend_epoch
            && Some(positions[i].sequence) == first.sequence.checked_add((i - start) as u64)
    };
    let mut claimed = HashSet::new();
    let mut end = start;
    let mut visited = HashSet::new();
    let mut fused = HashSet::new();
    while end < envelopes.len() && contiguous(end) {
        match &envelopes[end].command {
            QueueCommand::Claim(claim) => {
                if !claim.authority_first
                    || claim
                        .item_ids
                        .iter()
                        .any(|id| visited.contains(id) || !claimed.insert(*id))
                {
                    break;
                }
            }
            QueueCommand::MutateItems(mutation) => {
                if mutation
                    .items
                    .iter()
                    .any(|item| !visited.insert(item.item_id))
                {
                    break;
                }
                // Validate the whole command before adding any pairs. On a
                // duplicate we end before this command, so partially extended
                // seen sets above are never used again.
                for item in &mutation.items {
                    if claimed.contains(&item.item_id)
                        && matches!(&item.action, ResolvedItemMutationAction::Replace(values) | ResolvedItemMutationAction::ReplaceKeepingPayload(values)
                            if values.invalidate_lease && values.state != ItemState::Leased
                                && values.item_version >= 2)
                    {
                        fused.insert(item.item_id);
                    }
                }
            }
            _ => break,
        }
        end += 1;
    }
    (!fused.is_empty()).then_some((end, fused))
}

fn coalescible_update_run_end(
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    start: usize,
) -> Option<usize> {
    let first = envelopes.get(start)?;
    if !command_is_set_based_update(&first.command) {
        return None;
    }
    let shard = &positions[start].queue;
    let mut expected = positions[start].sequence;
    let mut end = start;
    while end < positions.len() {
        let p = &positions[end];
        let e = &envelopes[end];
        if p.queue != *shard || !command_is_set_based_update(&e.command) {
            break;
        }
        if p.sequence != expected {
            break;
        }
        expected = expected.saturating_add(1);
        end += 1;
    }
    (end > start).then_some(end)
}

fn command_is_set_based_update(command: &QueueCommand) -> bool {
    match command {
        QueueCommand::UpdateFields(update) => {
            update_fields_batch_is_set_based(std::slice::from_ref(update))
        }
        QueueCommand::UpdateFieldsBatch(batch) => update_fields_batch_is_set_based(&batch.updates),
        _ => false,
    }
}

fn claim_fuses_with_next(
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    index: usize,
) -> bool {
    let Some(QueueCommand::Claim(claim)) = envelopes.get(index).map(|envelope| &envelope.command)
    else {
        return false;
    };
    index + 1 < positions.len()
        && positions[index + 1].queue == positions[index].queue
        && positions[index + 1].sequence == positions[index].sequence.saturating_add(1)
        && matches!(
            &envelopes[index + 1].command,
            QueueCommand::Finalize(finalize) if finalize_completes_claim(claim, finalize)
        )
}

fn coalescible_claim_run_end(
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    start: usize,
) -> Option<usize> {
    if !matches!(envelopes.get(start)?.command, QueueCommand::Claim(_)) {
        return None;
    }
    if claim_fuses_with_next(positions, envelopes, start) {
        return None;
    }
    let shard = &positions[start].queue;
    let mut expected = positions[start].sequence;
    let mut end = start;
    while end < positions.len() {
        let position = &positions[end];
        let envelope = &envelopes[end];
        if position.queue != *shard || position.sequence != expected {
            break;
        }
        if !matches!(envelope.command, QueueCommand::Claim(_)) {
            break;
        }
        if claim_fuses_with_next(positions, envelopes, end) {
            break;
        }
        expected = expected.saturating_add(1);
        end += 1;
    }
    (end > start).then_some(end)
}

fn finalize_is_complete_only(command: &QueueCommand) -> bool {
    match command {
        QueueCommand::Finalize(finalize) => {
            !finalize.outcomes.is_empty()
                && finalize
                    .outcomes
                    .iter()
                    .all(|outcome| matches!(outcome.kind, FinalizeKind::Complete))
        }
        _ => false,
    }
}

fn consecutive_claim_run_end(
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    start: usize,
) -> Option<usize> {
    if !matches!(envelopes.get(start)?.command, QueueCommand::Claim(_)) {
        return None;
    }
    let shard = &positions[start].queue;
    let mut expected = positions[start].sequence;
    let mut end = start;
    while end < positions.len() {
        let position = &positions[end];
        let envelope = &envelopes[end];
        if position.queue != *shard || position.sequence != expected {
            break;
        }
        if !matches!(envelope.command, QueueCommand::Claim(_)) {
            break;
        }
        expected = expected.saturating_add(1);
        end += 1;
    }
    (end > start).then_some(end)
}

fn coalescible_claim_complete_runs(
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    start: usize,
) -> Option<(usize, usize)> {
    if claim_fuses_with_next(positions, envelopes, start) {
        return None;
    }
    let claim_end = consecutive_claim_run_end(positions, envelopes, start)?;
    let complete_end = coalescible_complete_run_end(positions, envelopes, claim_end)?;
    let mut claimed = BTreeSet::new();
    for envelope in &envelopes[start..claim_end] {
        let QueueCommand::Claim(claim) = &envelope.command else {
            return None;
        };
        for id in &claim.item_ids {
            if !claimed.insert(*id) {
                return None;
            }
        }
    }
    let mut completed = BTreeSet::new();
    for envelope in &envelopes[claim_end..complete_end] {
        let QueueCommand::Finalize(finalize) = &envelope.command else {
            return None;
        };
        for outcome in &finalize.outcomes {
            if !matches!(outcome.kind, FinalizeKind::Complete) {
                return None;
            }
            if !completed.insert(outcome.item_id) {
                return None;
            }
        }
    }
    (claimed == completed && !claimed.is_empty()).then_some((claim_end, complete_end))
}

fn coalescible_complete_run_end(
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    start: usize,
) -> Option<usize> {
    if !finalize_is_complete_only(&envelopes.get(start)?.command) {
        return None;
    }
    let shard = &positions[start].queue;
    let mut expected = positions[start].sequence;
    let mut end = start;
    while end < positions.len() {
        let position = &positions[end];
        let envelope = &envelopes[end];
        if position.queue != *shard || position.sequence != expected {
            break;
        }
        if !finalize_is_complete_only(&envelope.command) {
            break;
        }
        expected = expected.saturating_add(1);
        end += 1;
    }
    (end > start).then_some(end)
}

fn apply_complete_run_sql(
    tx: &impl RelTx,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
) -> EngineResult<()> {
    let mut rows: Vec<(String, i64, i64, i64)> = Vec::new();
    let mut bearer_ids: Vec<String> = Vec::new();
    for (position, envelope) in positions.iter().zip(envelopes) {
        let QueueCommand::Finalize(finalize) = &envelope.command else {
            return Err(EngineError::Storage(
                "apply_complete_run: non-complete envelope".into(),
            ));
        };
        let seq = position.sequence as i64;
        let epoch = position.backend_epoch as i64;
        let now_n = ts_nanos(envelope.created_at);
        for outcome in &finalize.outcomes {
            let id = outcome.item_id.to_string();
            bearer_ids.push(id.clone());
            rows.push((id, seq, epoch, now_n));
            token_ops.push(TokenOp::Clear(shard.clone(), outcome.item_id));
        }
    }
    let (t, q) = parts(shard);
    exec_items_in(
        tx,
        "DELETE FROM fireweed_lease_bearers WHERE tenant_id=? AND queue_id=? AND item_id IN",
        &[],
        &t,
        &q,
        &bearer_ids,
    )?;
    let Some((_, seq, epoch, now_n)) = rows.last() else {
        return Ok(());
    };
    exec_items_in(
        tx,
        "UPDATE fireweed_items SET lifecycle_state='Complete', lease_token_hash=NULL, \
         lease_expires_at=NULL, worker_id=NULL, fenced=0, item_version=item_version+1, \
         terminal_at=?, terminal_command_epoch=?, updated_at=?, last_command_sequence=? \
         WHERE tenant_id=? AND queue_id=? AND item_id IN",
        &[
            RelValue::Integer(*now_n),
            RelValue::Integer(*epoch),
            RelValue::Integer(*now_n),
            RelValue::Integer(*seq),
        ],
        &t,
        &q,
        &bearer_ids,
    )?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "Apply runs explicitly borrow transaction state, scan hints, and durable command inputs"
)]
fn apply_push_run_sql(
    tx: &impl RelTx,
    queues: &HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &mut HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    cursor: i64,
) -> EngineResult<()> {
    let model = queues
        .get(shard)
        .map(|d| d.priority_model)
        .ok_or(EngineError::NotFound)?;
    let mut specs: Vec<InsertItemSpec<'_>> = Vec::new();
    let mut minted_ids: Vec<ItemId> = Vec::new();
    let mut groups: HashSet<GroupKey> = HashSet::new();
    let mut last_now: Option<UtcTimestamp> = None;
    for (pos, env) in positions.iter().zip(envelopes.iter()) {
        if (pos.sequence as i64) < cursor {
            continue;
        }
        let QueueCommand::Push(c) = &env.command else {
            return Err(EngineError::Storage(
                "apply_push_run_sql: non-Push envelope in Push run".into(),
            ));
        };
        last_now = Some(env.created_at);
        for item in &c.items {
            specs.push(InsertItemSpec {
                item,
                command_seq: pos.sequence,
                now: env.created_at,
            });
            minted_ids.push(item.item_id);
            if let Some(g) = &item.group_key {
                groups.insert(g.clone());
            }
        }
    }
    if specs.is_empty() {
        return Ok(());
    }
    let base_seq = insert_item_specs(tx, queues, &model, shard, &specs)?;
    advance_id_high_water_sql(tx, shard, &minted_ids)?;
    let item_refs: Vec<&PushItem> = specs.iter().map(|s| s.item).collect();
    observe_push_for_claim_scan(claim_scan_hints, claim_scan_default_fifo, shard, &item_refs);
    if !groups.is_empty() {
        let now = last_now.ok_or_else(|| {
            EngineError::Storage("apply_push_run_sql: group refresh without timestamps".into())
        })?;
        grouped_shards.insert(shard.clone());
        let added = group_item_refs_from_specs(&specs, &model, base_seq);
        let group_list: Vec<GroupKey> = groups.into_iter().collect();
        apply_group_summary_add(tx, shard, &group_list, &added, now)?;
    }
    Ok(())
}
type TypedIndexKey = (String, Vec<u8>);
type TypedIndexBatchItem = (String, Vec<TypedIndexKey>);

type UpdateFieldsRow = (
    String,
    String,
    Option<String>,
    Option<i64>,
    i64,
    Option<Vec<u8>>,
    String,
);

fn maybe_advance_claim_scan_hint(
    tx: &impl RelTx,
    grouped_shards: &HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    item_ids: &[ItemId],
) -> EngineResult<()> {
    if grouped_shards.contains(shard) {
        return Ok(());
    }
    advance_claim_scan_hint_for_ids(
        tx,
        claim_scan_hints,
        claim_scan_default_fifo,
        shard,
        item_ids,
    )
}

pub fn advance_claim_scan_hint_for_ids(
    tx: &impl RelTx,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    item_ids: &[ItemId],
) -> EngineResult<()> {
    if item_ids.is_empty() || !claim_scan_default_fifo.get(shard).copied().unwrap_or(false) {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let mut seen = 0_i64;
    let mut max_rowid: Option<i64> = None;
    let mut rich_rows = 0_i64;
    let ids: Vec<String> = item_ids.iter().map(|id| id.to_string()).collect();
    for chunk in ids.chunks(SQLITE_BATCH) {
        let sql = named_rowid_bounds_sql(chunk.len());
        let mut p: Vec<RelValue> = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        for id in chunk {
            p.push(RelValue::Text(id.clone()));
        }
        let (count, chunk_max_rowid, chunk_rich): (i64, Option<i64>, i64) =
            crate::query_row(tx, &sql, &p, |row| {
                Ok((row.get(0)?, row.get(2)?, row.get(3)?))
            })?;
        seen += count;
        rich_rows += chunk_rich;
        if let Some(rowid) = chunk_max_rowid {
            max_rowid = Some(max_rowid.map_or(rowid, |current| current.max(rowid)));
        }
    }
    if seen != item_ids.len() as i64 || rich_rows > 0 {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
        return Ok(());
    }
    if let Some(rowid) = max_rowid {
        let next = rowid
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("claim scan hint overflow".into()))?;
        let slot = claim_scan_hints.entry(shard.clone()).or_insert(0);
        if next > *slot {
            *slot = next;
        }
    }
    Ok(())
}

pub fn fifo_rowid_range_for_id_strings(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[String],
    expected_state: Option<&str>,
) -> EngineResult<Option<(i64, i64)>> {
    if ids.is_empty() {
        return Ok(None);
    }
    let (t, q) = parts(shard);
    let mut seen = 0_i64;
    let mut min_rowid: Option<i64> = None;
    let mut max_rowid: Option<i64> = None;
    let mut rich_rows = 0_i64;
    for chunk in ids.chunks(SQLITE_BATCH) {
        let sql = named_rowid_bounds_sql(chunk.len());
        let mut p: Vec<RelValue> = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        for id in chunk {
            p.push(RelValue::Text(id.clone()));
        }
        let (count, chunk_min, chunk_max, chunk_rich): (i64, Option<i64>, Option<i64>, i64) =
            crate::query_row(tx, &sql, &p, |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
        seen += count;
        rich_rows += chunk_rich;
        if let Some(rowid) = chunk_min {
            min_rowid = Some(min_rowid.map_or(rowid, |current| current.min(rowid)));
        }
        if let Some(rowid) = chunk_max {
            max_rowid = Some(max_rowid.map_or(rowid, |current| current.max(rowid)));
        }
    }
    if seen != ids.len() as i64 || rich_rows > 0 {
        return Ok(None);
    }
    let (Some(min_rowid), Some(max_rowid)) = (min_rowid, max_rowid) else {
        return Ok(None);
    };
    let range_count: i64 = if let Some(state) = expected_state {
        crate::query_row(
            tx,
            "SELECT COUNT(*) FROM fireweed_items NOT INDEXED WHERE tenant_id=?1 AND queue_id=?2 \
             AND rowid BETWEEN ?3 AND ?4 AND lifecycle_state=?5",
            [
                RelValue::Text(t.to_string()),
                RelValue::Text(q.to_string()),
                min_rowid.into(),
                max_rowid.into(),
                state.into(),
            ],
            |row| row.get(0),
        )?
    } else {
        crate::query_row(
            tx,
            "SELECT COUNT(*) FROM fireweed_items NOT INDEXED WHERE tenant_id=?1 AND queue_id=?2 \
             AND rowid BETWEEN ?3 AND ?4",
            [
                RelValue::Text(t.to_string()),
                RelValue::Text(q.to_string()),
                min_rowid.into(),
                max_rowid.into(),
            ],
            |row| row.get(0),
        )?
    };
    if range_count == ids.len() as i64 {
        Ok(Some((min_rowid, max_rowid)))
    } else {
        Ok(None)
    }
}

/// Full-key endpoint seeks; IN on the composite key otherwise scans the queue in Turso.
pub const NAMED_ROWID_ENDPOINTS_SQL: &str = "WITH incoming(item_id) AS (VALUES (?3),(?4)) \
    SELECT i.item_id,i.rowid FROM incoming CROSS JOIN fireweed_items i \
    INDEXED BY sqlite_autoindex_fireweed_items_1 \
    ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=incoming.item_id";

/// Force the integer primary-key range instead of the tenant/queue index prefix.
pub const NAMED_ROWID_SLICE_SQL: &str = "SELECT item_id FROM fireweed_items NOT INDEXED \
    WHERE tenant_id=? AND queue_id=? AND rowid BETWEEN ? AND ?";

/// Named FIFO bookkeeping is bounded by addressed IDs, including duplicate-ID semantics.
pub fn named_rowid_bounds_sql(count: usize) -> String {
    let values = (0..count)
        .map(|i| format!("(?{})", i + 3))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "WITH incoming(item_id) AS (VALUES {values}), named AS (SELECT DISTINCT item_id FROM incoming) \
        SELECT COUNT(*),MIN(i.rowid),MAX(i.rowid), \
        COALESCE(SUM(CASE WHEN i.priority IS NOT NULL OR i.not_before IS NOT NULL \
        OR i.group_key IS NOT NULL OR i.cohort_size IS NOT NULL THEN 1 ELSE 0 END),0) \
        FROM named CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
        ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=named.item_id"
    )
}

/// Packed waves occupy a dense rowid slice of *exactly* the named ids. Two
/// endpoint PK lookups stay cheap; the slice SELECT (integer rowid range, n
/// rows) proves occupancy without an 800-id TEXT IN probe. COUNT==n alone is
/// not identity: a same-queue neighbor can sit inside `[rowid(min), rowid(max)]`
/// while a named id sits outside.
fn named_ids_occupy_rowid_slice(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Option<(i64, i64)>> {
    let n = ids.len();
    if n == 0 {
        return Ok(None);
    }
    let mut named = HashSet::with_capacity(n);
    let mut min_id = ids[0];
    let mut max_id = ids[0];
    for &id in ids {
        if !named.insert(id) {
            return Ok(None);
        }
        if id < min_id {
            min_id = id;
        }
        if id > max_id {
            max_id = id;
        }
    }
    if max_id
        .as_u64()
        .saturating_sub(min_id.as_u64())
        .saturating_add(1)
        != n as u64
    {
        return Ok(None);
    }
    let (t, q) = parts(shard);
    let rows = crate::rel_query(
        tx,
        NAMED_ROWID_ENDPOINTS_SQL,
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            RelValue::Text(min_id.to_string()),
            RelValue::Text(max_id.to_string()),
        ],
    )?;
    let mut min_rowid: Option<i64> = None;
    let mut max_rowid: Option<i64> = None;
    for row in rows {
        let id =
            ItemId::new(row.get::<String>(0)?).map_err(|e| EngineError::Storage(e.to_string()))?;
        let rowid: i64 = row.get(1)?;
        if id == min_id {
            min_rowid = Some(rowid);
        }
        if id == max_id {
            max_rowid = Some(rowid);
        }
    }
    let (Some(min_rowid), Some(max_rowid)) = (min_rowid, max_rowid) else {
        return Ok(None);
    };
    if max_rowid < min_rowid || max_rowid.saturating_sub(min_rowid).saturating_add(1) != n as i64 {
        return Ok(None);
    }
    let slice = crate::rel_query(
        tx,
        NAMED_ROWID_SLICE_SQL,
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            RelValue::Integer(min_rowid),
            RelValue::Integer(max_rowid),
        ],
    )?;
    if slice.len() != n {
        return Ok(None);
    }
    let mut seen = HashSet::with_capacity(n);
    for row in slice {
        let id =
            ItemId::new(row.get::<String>(0)?).map_err(|e| EngineError::Storage(e.to_string()))?;
        if !named.contains(&id) || !seen.insert(id) {
            return Ok(None);
        }
    }
    if seen.len() != n {
        return Ok(None);
    }
    Ok(Some((min_rowid, max_rowid)))
}

fn fused_complete_moved_unnamed(
    returning: &[RelRow],
    named: &HashSet<ItemId>,
) -> EngineResult<bool> {
    for row in returning {
        let id =
            ItemId::new(row.get::<String>(1)?).map_err(|e| EngineError::Storage(e.to_string()))?;
        if !named.contains(&id) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[allow(
    clippy::too_many_arguments,
    reason = "Completion SQL keeps command binds and lifecycle/version guards explicit"
)]
fn complete_named_ids_pk_sql(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[String],
    now_n: i64,
    epoch: i64,
    seq: i64,
    version_sql: &str,
    pending_pred: &str,
    lease_hash: Option<&[u8]>,
) -> EngineResult<usize> {
    if ids.is_empty() {
        return Ok(0);
    }
    let (t, q) = parts(shard);
    let extra = if lease_hash.is_some() { 7 } else { 6 };
    let mut pending_moved = 0usize;
    for chunk in ids.chunks(bind_chunk_size(1, extra)) {
        let values = vec!["(?)"; chunk.len()].join(",");
        let sql = if tx.prefer_point_updates() {
            format!(
                "WITH incoming(item_id) AS (VALUES {values}) \
                UPDATE fireweed_items SET lifecycle_state='Complete', lease_token_hash=NULL, \
                lease_expires_at=NULL, worker_id=NULL, fenced=0, {version_sql} \
                terminal_at=?, terminal_command_epoch=?, updated_at=?, last_command_sequence=? \
                WHERE rowid IN (SELECT i.rowid FROM incoming \
                    CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
                    ON i.tenant_id=? AND i.queue_id=? AND i.item_id=incoming.item_id) \
                AND superseded=0 AND {pending_pred}"
            )
        } else {
            format!(
                "WITH incoming(item_id) AS (VALUES {values}) \
             UPDATE fireweed_items SET lifecycle_state='Complete', lease_token_hash=NULL, \
             lease_expires_at=NULL, worker_id=NULL, fenced=0, \
             {version_sql} \
             terminal_at=?, terminal_command_epoch=?, updated_at=?, last_command_sequence=? \
             FROM incoming \
             WHERE tenant_id=? AND queue_id=? AND superseded=0 \
               AND fireweed_items.item_id=incoming.item_id \
               AND {pending_pred}"
            )
        };
        let mut params: Vec<RelValue> = chunk.iter().cloned().map(RelValue::Text).collect();
        params.extend([
            RelValue::Integer(now_n),
            RelValue::Integer(epoch),
            RelValue::Integer(now_n),
            RelValue::Integer(seq),
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
        ]);
        if let Some(hash) = lease_hash {
            params.push(RelValue::Blob(hash.to_vec()));
        }
        pending_moved += crate::rel_exec(tx, &sql, params)?;
    }
    Ok(pending_moved)
}

// ---------------------------------------------------------------------------
// ADR-011 typed secondary index helpers
// ---------------------------------------------------------------------------

/// Compute the canonical `index_key` bytes for a lookup against a named index.
pub fn typed_lookup_canonical_key(
    qi: &QueueIndex,
    key_values: &[Vec<u8>],
) -> EngineResult<Vec<u8>> {
    fireweed_engine::index_fields::typed_lookup_key(&qi.declaration, key_values)
}

/// Prefer native [`PushItem::index_fields`]; fall back to entity JSON for pre-native rows.
pub fn typed_index_keys_for_native(
    typed_indexes: &[QueueIndex],
    index_fields: &std::collections::BTreeMap<String, fireweed_core::TypedValue>,
    entity: Option<&JsonValue>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    if typed_indexes.is_empty() {
        return Ok(vec![]);
    }
    fireweed_engine::index_fields::typed_index_keys_for_item(typed_indexes, index_fields, entity)
}

/// Canonical keys for both opaque byte-field declarations and typed entity indexes.
pub fn secondary_index_keys(
    definition: &QueueDefinition,
    fields: &BTreeMap<String, bytes::Bytes>,
    index_fields: &BTreeMap<String, fireweed_core::TypedValue>,
    entity: Option<&JsonValue>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    let mut keys =
        Vec::with_capacity(definition.secondary_indexes.len() + definition.typed_indexes.len());
    for spec in &definition.secondary_indexes {
        let mut key = Vec::new();
        let mut complete = true;
        for field in &spec.fields {
            let Some(value) = fields.get(field) else {
                complete = false;
                break;
            };
            let length = u32::try_from(value.len())
                .map_err(|_| EngineError::Invalid("index field exceeds byte length limit"))?;
            key.extend_from_slice(&length.to_be_bytes());
            key.extend_from_slice(value);
        }
        if complete {
            keys.push((spec.name.clone(), key));
        }
    }
    keys.extend(typed_index_keys_for_native(
        &definition.typed_indexes,
        index_fields,
        entity,
    )?);
    Ok(keys)
}

pub fn secondary_index_is_unique(definition: &QueueDefinition, name: &str) -> bool {
    definition
        .secondary_indexes
        .iter()
        .find(|index| index.name == name)
        .map(|index| index.unique)
        .or_else(|| {
            definition
                .typed_indexes
                .iter()
                .find(|index| index.name == name)
                .map(index_is_unique)
        })
        .unwrap_or(false)
}

fn unique_index_names(definition: &QueueDefinition) -> HashSet<&str> {
    definition
        .secondary_indexes
        .iter()
        .filter(|index| index.unique)
        .map(|index| index.name.as_str())
        .chain(
            definition
                .typed_indexes
                .iter()
                .filter(|index| index_is_unique(index))
                .map(|index| index.name.as_str()),
        )
        .collect()
}

pub fn index_is_unique(qi: &QueueIndex) -> bool {
    match &qi.declaration {
        IndexDeclaration::Single(def) => def.unique,
        IndexDeclaration::Compound(def) => def.unique,
    }
}

/// Check unique-index constraints for `keys` against existing DB rows. Returns `Conflict` if any
/// unique index already maps the same key to a *different* item. Pass `exclude_item_id = Some(id)`
/// when the item whose old rows were just deleted might still appear in DB (i.e. for UpdateFields).
pub fn check_secondary_unique_conflicts(
    tx: &impl RelTx,
    t: &str,
    q: &str,
    definition: &QueueDefinition,
    keys: &[(String, Vec<u8>)],
    exclude_item_id: Option<&str>,
) -> EngineResult<()> {
    for (name, key) in keys {
        let unique = secondary_index_is_unique(definition, name);
        if !unique {
            continue;
        }
        let holder: Option<String> = match exclude_item_id {
            Some(excl) => crate::query_optional(
                tx,
                "SELECT item_id FROM fireweed_item_index \
                     WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3 AND index_key=?4 \
                     AND item_id!=?5 LIMIT 1",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    name.into(),
                    key.as_slice().into(),
                    excl.into(),
                ],
                |row| row.get(0),
            )?,
            None => crate::query_optional(
                tx,
                "SELECT item_id FROM fireweed_item_index \
                     WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3 AND index_key=?4 \
                     LIMIT 1",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    name.into(),
                    key.as_slice().into(),
                ],
                |row| row.get(0),
            )?,
        };
        if holder.is_some() {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

/// Insert `fireweed_item_index` rows for one item's `(name, key)` pairs (upsert so a retry is safe).
pub fn insert_typed_index_rows(
    tx: &impl RelTx,
    t: &str,
    q: &str,
    item_id: &str,
    keys: &[(String, Vec<u8>)],
) -> EngineResult<()> {
    insert_typed_index_rows_batch(tx, t, q, &[(item_id.to_string(), keys.to_vec())])
}

fn check_secondary_unique_conflicts_batch(
    tx: &impl RelTx,
    t: &str,
    q: &str,
    definition: &QueueDefinition,
    items: &[TypedIndexBatchItem],
) -> EngineResult<()> {
    let unique_names = unique_index_names(definition);
    let mut staged_unique = HashMap::new();
    for (item_id, keys) in items {
        for (name, key) in keys {
            if unique_names.contains(name.as_str())
                && staged_unique
                    .insert((name, key), item_id)
                    .is_some_and(|previous| previous != item_id)
            {
                return Err(EngineError::Conflict);
            }
        }
    }
    let rows: Vec<_> = items
        .iter()
        .flat_map(|(item_id, keys)| {
            keys.iter()
                .filter(|(name, _)| unique_names.contains(name.as_str()))
                .map(move |(name, key)| (item_id, name, key))
        })
        .collect();
    for chunk in rows.chunks(TYPED_INDEX_CHECK_CHUNK) {
        let values = vec!["(?,?,?)"; chunk.len()].join(",");
        let mut parameters = Vec::with_capacity(chunk.len() * 3 + 2);
        for (item_id, name, key) in chunk {
            parameters.extend([
                RelValue::Text((*item_id).clone()),
                RelValue::Text((*name).clone()),
                RelValue::Blob((*key).clone()),
            ]);
        }
        parameters.extend([RelValue::Text(t.to_string()), RelValue::Text(q.to_string())]);
        let conflict: Option<i64> = crate::query_optional(
            tx,
            &format!(
                "WITH incoming(item_id,index_name,index_key) AS (VALUES {values}) \
                     SELECT 1 FROM fireweed_item_index existing JOIN incoming \
                       ON existing.index_name=incoming.index_name \
                      AND existing.index_key=incoming.index_key \
                      AND existing.item_id!=incoming.item_id \
                     WHERE existing.tenant_id=? AND existing.queue_id=? LIMIT 1"
            ),
            &parameters,
            |row| row.get(0),
        )?;
        if conflict.is_some() {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

fn insert_typed_index_rows_batch(
    tx: &impl RelTx,
    t: &str,
    q: &str,
    items: &[TypedIndexBatchItem],
) -> EngineResult<()> {
    let mut rows: Vec<_> = items
        .iter()
        .flat_map(|(item_id, keys)| keys.iter().map(move |(name, key)| (item_id, name, key)))
        .collect();
    // Insert in index/key order so B-tree leaf splits stay sequential.
    rows.sort_unstable_by(|a, b| a.1.cmp(b.1).then_with(|| a.2.cmp(b.2)));
    for chunk in rows.chunks(TYPED_INDEX_INSERT_CHUNK) {
        let values = vec!["(?,?,?,?,?)"; chunk.len()].join(",");
        let mut parameters = Vec::with_capacity(chunk.len() * 5);
        let tenant = RelValue::Text(t.to_string());
        let queue = RelValue::Text(q.to_string());
        for (item_id, name, key) in chunk {
            parameters.push(tenant.clone());
            parameters.push(queue.clone());
            parameters.extend([
                RelValue::Text((*name).clone()),
                RelValue::Blob((*key).clone()),
                RelValue::Text((*item_id).clone()),
            ]);
        }
        crate::rel_exec(
            tx,
            &format!(
                "INSERT INTO fireweed_item_index \
                 (tenant_id,queue_id,index_name,index_key,item_id) VALUES {values}"
            ),
            &parameters,
        )?;
    }
    Ok(())
}

/// Delete all `fireweed_item_index` rows for the given item IDs.
pub fn delete_typed_index_rows(
    tx: &impl RelTx,
    t: &str,
    q: &str,
    item_ids: &[String],
) -> EngineResult<()> {
    for chunk in item_ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "DELETE FROM fireweed_item_index \
             WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<RelValue> =
            vec![RelValue::Text(t.to_string()), RelValue::Text(q.to_string())];
        for id in chunk {
            p.push(RelValue::Text(id.clone()));
        }
        crate::rel_exec(tx, &sql, &p)?;
    }
    Ok(())
}

/// Rebuild changed rows from their final SQL values inside the apply transaction.
/// Remove all replaced keys before checking the batch, retaining outside holders.
fn refresh_secondary_indexes(
    tx: &impl RelTx,
    definition: &QueueDefinition,
    shard: &QueueKey,
    ids: &[String],
) -> EngineResult<()> {
    type StoredIndexInputs = (String, Option<Vec<u8>>, Option<String>);
    let (t, q) = parts(shard);
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let current: Option<StoredIndexInputs> = crate::query_optional(
            tx,
            "SELECT fields,index_fields,entity_document FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 AND superseded=0",
            [
                RelValue::Text(t.clone()),
                RelValue::Text(q.clone()),
                RelValue::Text(id.clone()),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if let Some((fields, native, entity)) = current {
            let entity = entity
                .map(|raw| serde_json::from_str::<JsonValue>(&raw))
                .transpose()
                .map_err(|error| EngineError::Storage(error.to_string()))?;
            let keys = secondary_index_keys(
                definition,
                &fields_from_json(fields)?,
                &fireweed_engine::index_fields::decode_index_fields_blob(native.as_deref())?,
                entity.as_ref(),
            )?;
            rows.push((id.clone(), keys));
        }
    }
    delete_typed_index_rows(tx, &t, &q, ids)?;
    check_secondary_unique_conflicts_batch(tx, &t, &q, definition, &rows)?;
    insert_typed_index_rows_batch(tx, &t, &q, &rows)
}

/// Pack and check unique conflicts, then insert all declared indexes for a push batch.
pub fn maintain_secondary_indexes_on_insert(
    tx: &impl RelTx,
    t: &str,
    q: &str,
    definition: &QueueDefinition,
    items: &[&PushItem],
) -> EngineResult<()> {
    if definition.typed_indexes.is_empty() && definition.secondary_indexes.is_empty() {
        return Ok(());
    }
    // Every declared typed index is a query handle (ADR-011): `fireweed_item_index` is the
    // only durable row hot queries (bounded_mutation, range scans, aggregates) seek against,
    // so non-unique indexes need rows here too, not just the ones with a uniqueness
    // constraint to enforce.
    let unique_names = unique_index_names(definition);
    // Collect (item_id, keys) and enforce within-batch uniqueness in a single pass.
    let mut batch_unique: std::collections::HashMap<(String, Vec<u8>), String> =
        std::collections::HashMap::new();
    let mut item_keys: TypedIndexRows = Vec::with_capacity(items.len());
    for item in items {
        let keys = secondary_index_keys(
            definition,
            &item.fields,
            &item.index_fields,
            item.entity_document.as_ref(),
        )?;
        let id_str = item.item_id.to_string();
        for (name, key) in &keys {
            if unique_names.contains(name.as_str()) {
                let bk = (name.clone(), key.clone());
                if let Some(prev) = batch_unique.get(&bk) {
                    if prev != &id_str {
                        return Err(EngineError::Conflict);
                    }
                } else {
                    batch_unique.insert(bk, id_str.clone());
                }
            }
        }
        if !keys.is_empty() {
            item_keys.push((id_str, keys));
        }
    }
    check_secondary_unique_conflicts_batch(tx, t, q, definition, &item_keys)?;
    insert_typed_index_rows_batch(tx, t, q, &item_keys)?;
    Ok(())
}

pub fn extend_claim_by_query_idempotency_for_renewal(
    tx: &impl RelTx,
    shard: &QueueKey,
    renewed_item_ids: &[ItemId],
    renewed_expires_at: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    if renewed_item_ids.is_empty() {
        return Ok(());
    }
    let renewed = serde_json::to_string(
        &renewed_item_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| EngineError::Storage(error.to_string()))?;
    let renewed_expires_at = ts_nanos(renewed_expires_at);
    crate::rel_exec(
        tx,
        "UPDATE fireweed_request_idempotency SET expires_at=max(expires_at,?4) \
         WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id IN ( \
           SELECT edge.request_id FROM fireweed_claim_replay_items edge \
           JOIN json_each(?5) renewed ON renewed.value=edge.item_id \
           WHERE edge.tenant_id=?1 AND edge.queue_id=?2 GROUP BY edge.request_id \
           HAVING COUNT(*)=(SELECT COUNT(*) FROM fireweed_claim_replay_items all_edges \
             WHERE all_edges.tenant_id=?1 AND all_edges.queue_id=?2 \
               AND all_edges.request_id=edge.request_id))",
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY.into(),
            renewed_expires_at.into(),
            renewed.clone().into(),
        ],
    )?;
    // Addressed query claims have an independent request namespace. Keep their
    // receipts through a whole-claim renewal without mixing equal request IDs
    // from the index-query namespace in the historical replay-edge table.
    crate::rel_exec(
        tx,
        "UPDATE fireweed_request_idempotency SET expires_at=max(expires_at,?3) \
         WHERE tenant_id=?1 AND queue_id=?2 AND operation='claim_by_item_ids' \
           AND json_array_length(response_payload,'$.claimed_item_ids')>0 \
           AND NOT EXISTS (SELECT 1 FROM json_each(response_payload,'$.claimed_item_ids') member \
             WHERE member.value NOT IN (SELECT value FROM json_each(?4)))",
        [
            t.into(),
            q.into(),
            renewed_expires_at.into(),
            renewed.into(),
        ],
    )?;
    Ok(())
}

/// Advance the durable per-queue item-id high-water past the greatest of `reaped` (ADR-009 mint-counter
/// recovery floor). MONOTONIC by `(epoch, counter)`: a reap that deletes only lower-id rows never lowers the
/// stored floor. This is what keeps terminal-item reaping from re-minting a reaped id — the deleted rows are
/// no longer in `fireweed_items`, but their ceiling is preserved here and restored by
/// [`observe_id_high_water_sql`]. No-op when `reaped` is empty.
pub fn advance_id_high_water_sql(
    tx: &impl RelTx,
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
    crate::rel_exec(
        tx,
        "INSERT INTO fireweed_id_high_water(tenant,queue,item_id) VALUES(?1,?2,?3) \
         ON CONFLICT(tenant,queue) DO UPDATE SET item_id=excluded.item_id \
         WHERE length(excluded.item_id)>length(fireweed_id_high_water.item_id) \
            OR (length(excluded.item_id)=length(fireweed_id_high_water.item_id) \
                AND excluded.item_id>fireweed_id_high_water.item_id)",
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            max_reaped.to_string().into(),
        ],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// apply: the 14-arm command -> SQL projection write (the BQ-11a headline)
// ---------------------------------------------------------------------------

/// Max rows per dynamically-built multi-row / `IN (...)` statement. Each `fireweed_items` row binds 19
/// params; 1,500 rows ≈ 28.5k params, under sqlite's 32,766 bound-variable ceiling (bundled SQLite) while
/// cutting large Push materialization from tens of thousands of statements to a few thousand.
pub const COHORT_EXPIRY_SWEEP_LIMIT: usize = 128;
pub const GROUP_DUE_REFRESH_LIMIT: i64 = 128;

pub fn opt_text(v: Option<String>) -> RelValue {
    v.map_or(RelValue::Null, RelValue::Text)
}
pub fn opt_int(v: Option<i64>) -> RelValue {
    v.map_or(RelValue::Null, RelValue::Integer)
}
pub fn opt_blob(v: Option<Vec<u8>>) -> RelValue {
    v.map_or(RelValue::Null, RelValue::Blob)
}

/// One item row to materialize, carrying the durable command sequence and wall time of the Push (or
/// ReplacePending) that minted it. Group-commit apply coalesces many single-item Push envelopes into one
/// multi-row insert while preserving per-command `last_command_sequence` and timestamps.
pub struct InsertItemSpec<'a> {
    pub item: &'a PushItem,
    pub command_seq: u64,
    pub now: UtcTimestamp,
}

/// Batch-insert all `items` of a Push (or the single ReplacePending replacement) as set-based statements:
/// chunked multi-row INSERTs into `fireweed_items`, `fireweed_item_gates`, and `fireweed_cohorts` — replacing the
/// former per-item `insert_item` (N+ round-trips → a handful, chunked to the bound-variable limit). Column
/// values, the `fields` TEXT-JSON encoding, and the `eligible_since`/`not_before` pairing are identical to
/// the per-item path; `created_seq` is bulk-allocated (`base + i`) so the FIFO order is preserved.
pub fn insert_items(
    tx: &impl RelTx,
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
    insert_item_specs(tx, queues, model, shard, &specs)?;
    Ok(())
}

fn group_item_refs_from_specs(
    specs: &[InsertItemSpec<'_>],
    model: &PriorityModel,
    base_seq: i64,
) -> Vec<GroupItemRef> {
    let mut added = Vec::new();
    for (offset, spec) in specs.iter().enumerate() {
        let Some(group) = spec.item.group_key.clone() else {
            continue;
        };
        let now_n = ts_nanos(spec.now);
        let not_before = ts_nanos_opt(spec.item.not_before);
        if not_before.is_some_and(|ts| ts > now_n) {
            continue;
        }
        added.push(GroupItemRef {
            group_key: group,
            item_id: spec.item.item_id.to_string(),
            eligible_since: not_before.unwrap_or(now_n),
            priority_sort: elig_sort(&spec.item.priority, model),
            created_at: now_n,
            created_seq: base_seq + offset as i64,
        });
    }
    added
}

fn upsert_item_payloads(
    tx: &impl RelTx,
    tenant: &str,
    queue: &str,
    rows: impl IntoIterator<Item = (String, Option<Vec<u8>>)>,
) -> EngineResult<()> {
    let mut params = Vec::new();
    let mut count = 0usize;
    for (item_id, payload) in rows {
        params.push(RelValue::Text(tenant.to_string()));
        params.push(RelValue::Text(queue.to_string()));
        params.push(RelValue::Text(item_id));
        params.push(payload.map(RelValue::Blob).unwrap_or(RelValue::Null));
        count += 1;
    }
    if count == 0 {
        return Ok(());
    }
    let values = vec!["(?,?,?,?)"; count].join(",");
    crate::rel_exec_owned(
        tx,
        &format!(
            "INSERT INTO fireweed_item_payloads(tenant_id,queue_id,item_id,payload) \
             VALUES {values} \
             ON CONFLICT(tenant_id,queue_id,item_id) DO UPDATE SET payload=excluded.payload \
             WHERE fireweed_item_payloads.payload != excluded.payload \
             OR (fireweed_item_payloads.payload IS NULL) != (excluded.payload IS NULL)"
        ),
        params,
    )?;
    Ok(())
}

/// Like [`insert_items`], but each row may carry its own command sequence and timestamp (coalesced Push
/// envelopes from `apply_committed_batch_sql`).
pub fn insert_item_specs(
    tx: &impl RelTx,
    queues: &HashMap<QueueKey, QueueDefinition>,
    model: &PriorityModel,
    shard: &QueueKey,
    specs: &[InsertItemSpec<'_>],
) -> EngineResult<i64> {
    if specs.is_empty() {
        return Ok(0);
    }
    let (t, q) = parts(shard);
    // Bulk-allocate the stable FIFO positions in one read+advance (was a read+UPDATE per item).
    let base_seq: i64 = crate::query_row(
        tx,
        "SELECT next_item_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
        [RelValue::Text(t.to_string()), RelValue::Text(q.to_string())],
        |row| row.get(0),
    )?;
    crate::rel_exec(
        tx,
        "UPDATE relational_cursor SET next_item_seq=?3 WHERE tenant=?1 AND queue=?2",
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            (base_seq + specs.len() as i64).into(),
        ],
    )?;
    let definition = queues.get(shard).ok_or(EngineError::NotFound)?;
    let items_only: Vec<&PushItem> = specs.iter().map(|s| s.item).collect();
    // Fast path: homogeneous default-empty rows with a single wall clock (common internal benches).
    let first_now = specs[0].now;
    let homogeneous_now = specs.iter().all(|s| s.now == first_now);
    if definition.typed_indexes.is_empty()
        && definition.secondary_indexes.is_empty()
        && homogeneous_now
        && items_only
            .iter()
            .all(|item| is_default_empty_push_item(item))
    {
        insert_default_empty_item_specs(tx, &t, &q, specs, base_seq)?;
        return Ok(base_seq);
    }
    if homogeneous_now
        && items_only
            .iter()
            .all(|item| is_payload_index_push_item(item))
    {
        insert_payload_index_item_specs(tx, &t, &q, specs, base_seq)?;
        upsert_item_payloads(
            tx,
            &t,
            &q,
            specs.iter().map(|spec| {
                (
                    spec.item.item_id.to_string(),
                    spec.item.payload.as_ref().map(|b| b.to_vec()),
                )
            }),
        )?;
        maintain_secondary_indexes_on_insert(tx, &t, &q, definition, &items_only)?;
        return Ok(base_seq);
    }
    const ROW_PH: &str =
        "(?,?,?,?,'Pending',?,?,?,?,?,?,?,?,?,?,?,0,1,NULL,NULL,NULL,?,?,?,NULL,NULL,0,0,?,?)";
    for (chunk_idx, chunk) in specs.chunks(SQLITE_BATCH).enumerate() {
        let values = vec![ROW_PH; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO fireweed_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,entity_document,index_fields,retry_count,\
              item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,terminal_command_epoch,fenced,superseded,max_attempts,created_seq) VALUES {values}"
        );
        let mut flat = Vec::with_capacity(chunk.len() * 20);
        let offset = chunk_idx * SQLITE_BATCH;
        for (i, spec) in chunk.iter().enumerate() {
            let item = spec.item;
            let now_n = ts_nanos(spec.now);
            let not_before = ts_nanos_opt(item.not_before);
            flat.extend([
                RelValue::Text(t.clone()),
                RelValue::Text(q.clone()),
                RelValue::Text(item.item_id.to_string()),
                RelValue::Text(item.client_item_key.as_str().to_string()),
                opt_text(item.priority.as_ref().map(to_json).transpose()?),
                RelValue::Blob(elig_sort(&item.priority, model)),
                opt_int(not_before),
                RelValue::Integer(not_before.unwrap_or(now_n)),
                opt_text(item.group_key.as_ref().map(|g| g.as_str().to_string())),
                opt_int(item.cohort_size.map(|s| s as i64)),
                RelValue::Null,
                RelValue::Text(fields_to_json(&item.fields)?),
                RelValue::Text(metadata_to_json(&item.metadata)?),
                opt_text(item.entity_document.as_ref().map(to_json).transpose()?),
                opt_blob(fireweed_engine::index_fields::encode_index_fields_blob(
                    &item.index_fields,
                )?),
                RelValue::Integer(spec.command_seq as i64),
                RelValue::Integer(now_n),
                RelValue::Integer(now_n),
                RelValue::Integer(item.max_attempts as i64),
                RelValue::Integer(base_seq + offset as i64 + i as i64),
            ]);
        }
        crate::rel_exec_owned(tx, &sql, flat)?;
    }
    upsert_item_payloads(
        tx,
        &t,
        &q,
        specs.iter().map(|spec| {
            (
                spec.item.item_id.to_string(),
                spec.item.payload.as_ref().map(|b| b.to_vec()),
            )
        }),
    )?;
    insert_gates(tx, &t, &q, &items_only)?;
    // Cohort id stamping uses wall time; for a coalesced run use the latest now in the run.
    let cohort_now_n = specs
        .iter()
        .map(|s| ts_nanos(s.now))
        .max()
        .unwrap_or_else(|| ts_nanos(first_now));
    upsert_cohorts(tx, queues, shard, &t, &q, &items_only, cohort_now_n)?;
    // ADR-011: typed secondary index maintenance.
    maintain_secondary_indexes_on_insert(tx, &t, &q, definition, &items_only)?;
    Ok(base_seq)
}

pub fn is_default_empty_push_item(item: &PushItem) -> bool {
    is_payload_index_push_item(item) && item.payload.is_none() && item.index_fields.is_empty()
}

/// Snorri / cycle shape: opaque payload + native index_fields, no entity/fields/metadata/gates.
pub fn is_payload_index_push_item(item: &PushItem) -> bool {
    item.client_item_key.as_str() == item.item_id.to_string()
        && item.priority.is_none()
        && item.not_before.is_none()
        && item.group_key.is_none()
        && item.cohort_size.is_none()
        && item.fields.is_empty()
        && item.metadata == Metadata::default()
        && item.gate_keys.is_empty()
        && item.entity_document.is_none()
}

pub fn insert_default_empty_item_specs(
    tx: &impl RelTx,
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
            params.push(RelValue::Text(t.to_string()));
            params.push(RelValue::Text(q.to_string()));
            let item_id = spec.item.item_id.to_string();
            params.push(RelValue::Text(item_id.clone()));
            params.push(RelValue::Text(item_id));
            params.push(RelValue::Integer(now_n));
            params.push(RelValue::Integer(spec.command_seq as i64));
            params.push(RelValue::Integer(now_n));
            params.push(RelValue::Integer(now_n));
            params.push(RelValue::Integer(spec.item.max_attempts as i64));
            params.push(RelValue::Integer(base_seq + offset as i64 + i as i64));
        }
        crate::rel_exec(tx, &sql, &params)?;
    }
    Ok(())
}

/// Fast insert for payload + native index_fields items (no per-row JSON encode).
pub fn insert_payload_index_item_specs(
    tx: &impl RelTx,
    t: &str,
    q: &str,
    specs: &[InsertItemSpec<'_>],
    base_seq: i64,
) -> EngineResult<()> {
    const ROW_PH: &str = "(?,?,?,?,'Pending',NULL,X'01',NULL,?,NULL,NULL,NULL, '{}','{}',NULL,?,0,1,NULL,NULL,NULL,?,?,?,NULL,NULL,0,0,?,?)";
    for (chunk_idx, chunk) in specs.chunks(SQLITE_BATCH).enumerate() {
        let values = vec![ROW_PH; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO fireweed_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,entity_document,index_fields,retry_count,\
             item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,terminal_command_epoch,fenced,superseded,max_attempts,created_seq) VALUES {values}"
        );
        let mut params = Vec::with_capacity(chunk.len() * 12);
        let offset = chunk_idx * SQLITE_BATCH;
        for (i, spec) in chunk.iter().enumerate() {
            let now_n = ts_nanos(spec.now);
            params.push(RelValue::Text(t.to_string()));
            params.push(RelValue::Text(q.to_string()));
            let item_id = spec.item.item_id.to_string();
            params.push(RelValue::Text(item_id.clone()));
            params.push(RelValue::Text(item_id));
            params.push(RelValue::Integer(now_n));
            params.push(opt_blob(
                fireweed_engine::index_fields::encode_index_fields_blob(&spec.item.index_fields)?,
            ));
            params.push(RelValue::Integer(spec.command_seq as i64));
            params.push(RelValue::Integer(now_n));
            params.push(RelValue::Integer(now_n));
            params.push(RelValue::Integer(spec.item.max_attempts as i64));
            params.push(RelValue::Integer(base_seq + offset as i64 + i as i64));
        }
        crate::rel_exec(tx, &sql, &params)?;
    }
    Ok(())
}

/// Batch the per-item gate-membership inserts (BQ-14d) into chunked multi-row INSERTs. Pairs are deduped so
/// one statement never proposes the same `(item_id, gate_key)` twice.
pub fn insert_gates(tx: &impl RelTx, t: &str, q: &str, items: &[&PushItem]) -> EngineResult<()> {
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
        let mut p: Vec<RelValue> = Vec::with_capacity(chunk.len() * 4);
        for (id, g) in chunk {
            p.push(RelValue::Text(t.to_string()));
            p.push(RelValue::Text(q.to_string()));
            p.push(RelValue::Text(id.clone()));
            p.push(RelValue::Text(g.clone()));
        }
        crate::rel_exec(tx, &sql, &p)?;
    }
    Ok(())
}

pub fn cohort_id_for(group_key: &str, now_n: i64) -> String {
    format!("coh:{group_key}:{now_n}")
}

pub fn cohort_retention_until(
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

pub fn cohort_expiry_deadline(
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

pub fn cohort_member_count_state(count: i64, size: i64) -> &'static str {
    if count >= size { "complete" } else { "forming" }
}

/// Maintain TD-002 cohort lifecycle projection for newly accepted cohort members.
pub fn upsert_cohorts(
    tx: &impl RelTx,
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
        let existing: Option<(i64, i64, String, Option<i64>)> = crate::query_optional(
            tx,
            "SELECT cohort_size, member_count, state, retention_until FROM fireweed_cohorts \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
            [
                RelValue::Text(t.to_string()),
                RelValue::Text(q.to_string()),
                RelValue::from(&gk),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
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
                crate::rel_exec(
                    tx,
                    "INSERT INTO fireweed_cohorts \
                     (tenant_id,queue_id,group_key,cohort_id,cohort_size,member_count,state,\
                      cohort_created_at,first_eligible_at,created_at) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?8)",
                    [
                        RelValue::Text(t.to_string()),
                        RelValue::Text(q.to_string()),
                        RelValue::from(&gk),
                        cohort_id_for(&gk, now_n).into(),
                        size.into(),
                        added.into(),
                        state.into(),
                        now_n.into(),
                        first_eligible_at.into(),
                    ],
                )?;
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
                    crate::rel_exec(
                        tx,
                        "UPDATE fireweed_cohorts SET cohort_id=?4, cohort_size=?5, member_count=?6, \
                         state=?7, cohort_created_at=?8, first_eligible_at=?9, expire_command_pos=NULL, \
                         cohort_lease_token_hash=NULL, retention_until=NULL, created_at=?8 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                        [
                            RelValue::Text(t.to_string()),
                            RelValue::Text(q.to_string()),
                            RelValue::from(&gk),
                            cohort_id_for(&gk, now_n).into(),
                            size.into(),
                            added.into(),
                            next_state.into(),
                            now_n.into(),
                            first_eligible_at.into(),
                        ],
                    )?;
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
                crate::rel_exec(
                    tx,
                    "UPDATE fireweed_cohorts SET member_count=?4, state=?5, \
                     first_eligible_at=CASE WHEN ?6 AND first_eligible_at IS NULL THEN ?7 ELSE first_eligible_at END \
                     WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                    [
                        RelValue::Text(t.to_string()),
                        RelValue::Text(q.to_string()),
                        RelValue::from(&gk),
                        next_count.into(),
                        next_state.into(),
                        set_first.into(),
                        now_n.into(),
                    ],
                )?;
            }
        }
    }
    Ok(())
}

/// Run `{prefix} (chunk)` (e.g. an `UPDATE … item_id IN` or `DELETE … item_id IN`).
/// `lead` are the bound values for the prefix's leading placeholders (the SET clause, if any); the prefix's
/// trailing `tenant_id=? AND queue_id=?` then bind `t`,`q`, followed by the chunk ids. Chunking keeps the
/// bound-variable count under the 900-variable cap.
pub fn exec_items_in(
    tx: &impl RelTx,
    prefix: &str,
    lead: &[RelValue],
    t: &str,
    q: &str,
    ids: &[String],
) -> EngineResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    // IN on a composite key can scan an entire queue in Turso. For this
    // exact helper shape, resolve rowids by indexed points first, then mutate
    // those integer primary keys. Keep one statement per bounded batch.
    if tx.prefer_point_updates()
        && let Some(statement) =
            prefix.strip_suffix(" WHERE tenant_id=? AND queue_id=? AND item_id IN")
    {
        let table = statement.strip_prefix("DELETE FROM ").or_else(|| {
            statement
                .strip_prefix("UPDATE ")
                .and_then(|s| s.split_once(" SET ").map(|(table, _)| table))
        });
        if let Some(
            table @ ("fireweed_items"
            | "fireweed_item_payloads"
            | "fireweed_item_gates"
            | "fireweed_lease_bearers"),
        ) = table
        {
            for chunk in ids.chunks(bind_chunk_size(1, lead.len() + 2)) {
                let values = vec!["(?)"; chunk.len()].join(",");
                let sql = format!(
                    "WITH incoming(item_id) AS (VALUES {values}) \
                        {statement} WHERE rowid IN (SELECT i.rowid FROM incoming \
                        CROSS JOIN {table} i INDEXED BY sqlite_autoindex_{table}_1 \
                        ON i.tenant_id=? AND i.queue_id=? AND i.item_id=incoming.item_id)"
                );
                let mut params = chunk
                    .iter()
                    .cloned()
                    .map(RelValue::Text)
                    .collect::<Vec<_>>();
                params.extend_from_slice(lead);
                params.extend([RelValue::Text(t.to_string()), RelValue::Text(q.to_string())]);
                crate::rel_exec(tx, &sql, params)?;
            }
            return Ok(());
        }
    }
    let chunk_size = bind_chunk_size(1, lead.len() + 2);
    for chunk in ids.chunks(chunk_size) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!("{prefix} ({ph})");
        let mut p: Vec<RelValue> = lead.to_vec();
        p.push(RelValue::Text(t.to_string()));
        p.push(RelValue::Text(q.to_string()));
        for id in chunk {
            p.push(RelValue::Text(id.clone()));
        }
        crate::rel_exec(tx, &sql, &p)?;
    }
    Ok(())
}

fn persist_lease_bearers(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[ItemId],
    token: &LeaseToken,
) -> EngineResult<()> {
    let rows: Vec<(ItemId, LeaseToken)> =
        ids.iter().copied().map(|id| (id, token.clone())).collect();
    persist_lease_bearer_pairs(tx, shard, &rows)
}

fn persist_lease_bearer_pairs(
    tx: &impl RelTx,
    shard: &QueueKey,
    rows: &[(ItemId, LeaseToken)],
) -> EngineResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let chunk_size = bind_chunk_size(4, 0);
    for chunk in rows.chunks(chunk_size) {
        let placeholders = vec!["(?,?,?,?)"; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO fireweed_lease_bearers(tenant_id,queue_id,item_id,lease_token) \
             VALUES {placeholders} \
             ON CONFLICT(tenant_id,queue_id,item_id) DO UPDATE SET lease_token=excluded.lease_token"
        );
        let mut params = Vec::with_capacity(chunk.len() * 4);
        for (id, token) in chunk {
            params.extend([
                RelValue::Text(t.clone()),
                RelValue::Text(q.clone()),
                RelValue::Text(id.to_string()),
                RelValue::Text(token.as_str().to_string()),
            ]);
        }
        crate::rel_exec(tx, &sql, params)?;
    }
    Ok(())
}

fn leased_item_id_set(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[String],
) -> EngineResult<HashSet<String>> {
    item_ids_in_state(tx, shard, ids, "Leased")
}

fn item_ids_in_state(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[String],
    state: &str,
) -> EngineResult<HashSet<String>> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let (t, q) = parts(shard);
    let mut out = HashSet::new();
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id FROM fireweed_items WHERE tenant_id=? AND queue_id=? \
             AND lifecycle_state=? AND item_id IN ({ph})"
        );
        let mut params = vec![
            RelValue::Text(t.clone()),
            RelValue::Text(q.clone()),
            RelValue::Text(state.to_string()),
        ];
        params.extend(chunk.iter().cloned().map(RelValue::Text));
        for row in crate::rel_query(tx, &sql, params)? {
            out.insert(row.get(0)?);
        }
    }
    Ok(out)
}

fn item_ids_with_lease_hash(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[String],
    hash: &[u8],
) -> EngineResult<HashSet<String>> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let (t, q) = parts(shard);
    let mut out = HashSet::new();
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id FROM fireweed_items WHERE tenant_id=? AND queue_id=? \
             AND lease_token_hash=? AND item_id IN ({ph})"
        );
        let mut params = vec![
            RelValue::Text(t.clone()),
            RelValue::Text(q.clone()),
            RelValue::Blob(hash.to_vec()),
        ];
        params.extend(chunk.iter().cloned().map(RelValue::Text));
        for row in crate::rel_query(tx, &sql, params)? {
            out.insert(row.get(0)?);
        }
    }
    Ok(out)
}

pub fn reap_terminal_items_sql(
    tx: &impl RelTx,
    shard: &QueueKey,
    now: UtcTimestamp,
    terminal_retention_ms: u64,
    emit_change_records: bool,
    emission_cursor: Option<&CommandPosition>,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let cutoff = now_n.saturating_sub((terminal_retention_ms as i64).saturating_mul(1_000_000));
    let (sql, params): (String, Vec<RelValue>) = if emit_change_records {
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
                RelValue::Text(t.clone()),
                RelValue::Text(q.clone()),
                RelValue::Integer(cutoff),
                RelValue::Integer(cursor.backend_epoch as i64),
                RelValue::Integer(cursor.sequence as i64),
            ],
        )
    } else {
        (
            "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND superseded=0 AND lifecycle_state IN ('Complete','Failed') \
             AND terminal_at IS NOT NULL AND terminal_at<=?3"
                .to_string(),
            vec![
                RelValue::Text(t.clone()),
                RelValue::Text(q.clone()),
                RelValue::Integer(cutoff),
            ],
        )
    };
    let mut id_strs = Vec::new();
    for row in crate::rel_query(tx, &sql, &params)? {
        id_strs.push(row.get::<String>(0)?);
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
    exec_items_in(
        tx,
        "DELETE FROM fireweed_item_payloads WHERE tenant_id=? AND queue_id=? AND item_id IN",
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
pub enum TokenOp {
    Set(QueueKey, ItemId, LeaseToken),
    Clear(QueueKey, ItemId),
}

/// Collect process-local lease cleartext ops from a command without mutating durable SQL.
///
/// Used when replaying a prefix the projection has already absorbed (`incoming < cursor`):
/// durable rows keep only `lease_token_hash`, so reopen must rehydrate [`Inner::live_tokens`]
/// from the authoritative log or render_claimed / renew after kill returns `StaleLease`.
pub fn collect_token_ops_from_command(
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

pub fn apply_token_ops(
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
pub fn groups_of(tx: &impl RelTx, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<GroupKey>> {
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
        let mut p: Vec<RelValue> = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        for id in chunk {
            p.push(RelValue::Text(id.clone()));
        }
        for row in crate::rel_query(tx, &sql, &p)? {
            let gk = GroupKey::new(row.get::<String>(0)?)
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            if !seen.contains(&gk) {
                seen.push(gk);
            }
        }
    }
    Ok(seen)
}

pub fn cohort_group_for_id(
    tx: &impl RelTx,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<GroupKey> {
    let (t, q) = parts(shard);
    let group: String = crate::query_row(
        tx,
        "SELECT group_key FROM fireweed_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            cohort_id.as_str().into(),
        ],
        |row| row.get(0),
    )?;
    GroupKey::new(group).map_err(|e| EngineError::Storage(e.to_string()))
}

pub fn cohort_item_ids(
    tx: &impl RelTx,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let group = cohort_group_for_id(tx, shard, cohort_id)?;
    let mut out = Vec::new();
    for row in crate::rel_query(
        tx,
        "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND superseded=0 AND cohort_size IS NOT NULL AND lifecycle_state NOT IN ('Complete','Failed') \
         ORDER BY priority_sort, created_seq",
        [
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            group.as_str().into(),
        ],
    )? {
        out.push(
            ItemId::new(row.get::<String>(0)?).map_err(|e| EngineError::Storage(e.to_string()))?,
        );
    }
    Ok(out)
}

/// One currently-eligible (or newly-eligible) grouped item used to maintain `fireweed_group_summary`
/// without rescanning the whole group.
struct GroupItemRef {
    group_key: GroupKey,
    item_id: String,
    eligible_since: i64,
    priority_sort: Vec<u8>,
    created_at: i64,
    created_seq: i64,
}

struct LoadedSummary {
    oldest: Option<i64>,
    rep_priority_sort: Option<Vec<u8>>,
    rep_created_at: Option<i64>,
    rep_item_id: Option<String>,
    rep_created_seq: Option<i64>,
    count: i64,
}

struct SummaryWrite {
    group_key: GroupKey,
    oldest: Option<i64>,
    rep_priority_sort: Option<Vec<u8>>,
    rep_created_at: Option<i64>,
    rep_created_seq: Option<i64>,
    rep_item_id: Option<String>,
    count: i64,
}

const SUMMARY_UPSERT_BATCH: usize = 50;
const GATE_ANTI_JOIN: &str = " AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
     JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
     AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
     AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id)";

fn unique_groups(items: &[GroupItemRef]) -> Vec<GroupKey> {
    let mut groups = Vec::new();
    for item in items {
        if !groups.contains(&item.group_key) {
            groups.push(item.group_key.clone());
        }
    }
    groups
}

fn best_item(items: &[GroupItemRef]) -> Option<&GroupItemRef> {
    items.iter().min_by(|left, right| {
        left.priority_sort
            .cmp(&right.priority_sort)
            .then(left.created_seq.cmp(&right.created_seq))
    })
}

fn item_beats(item: &GroupItemRef, sort: &[u8], created_seq: i64) -> bool {
    item.priority_sort.as_slice() < sort
        || (item.priority_sort.as_slice() == sort && item.created_seq < created_seq)
}

fn write_from_items(group: &GroupKey, items: &[GroupItemRef]) -> SummaryWrite {
    let best = best_item(items);
    SummaryWrite {
        group_key: group.clone(),
        oldest: items.iter().map(|item| item.eligible_since).min(),
        rep_priority_sort: best.map(|item| item.priority_sort.clone()),
        rep_created_at: best.map(|item| item.created_at),
        rep_created_seq: best.map(|item| item.created_seq),
        rep_item_id: best.map(|item| item.item_id.clone()),
        count: items.len() as i64,
    }
}

fn load_grouped_items(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Vec<GroupItemRef>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let mut out = Vec::new();
    let id_strs: Vec<String> = ids.iter().map(ToString::to_string).collect();
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT group_key,item_id,eligible_since,priority_sort,created_at,created_seq,not_before \
             FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN ({placeholders}) \
             AND group_key IS NOT NULL"
        );
        let mut params = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        params.extend(chunk.iter().cloned().map(RelValue::Text));
        for row in crate::rel_query(tx, &sql, params)? {
            let Some(group) = row.get::<Option<String>>(0)? else {
                continue;
            };
            out.push(GroupItemRef {
                group_key: GroupKey::new(group).map_err(|e| EngineError::Storage(e.to_string()))?,
                item_id: row.get(1)?,
                eligible_since: row.get(2)?,
                priority_sort: row.get(3)?,
                created_at: row.get(4)?,
                created_seq: row.get(5)?,
            });
        }
    }
    Ok(out)
}

fn load_summaries(
    tx: &impl RelTx,
    shard: &QueueKey,
    groups: &[GroupKey],
) -> EngineResult<HashMap<GroupKey, LoadedSummary>> {
    let mut out = HashMap::new();
    if groups.is_empty() {
        return Ok(out);
    }
    let (t, q) = parts(shard);
    for chunk in groups.chunks(SQLITE_BATCH) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT group_key,oldest_eligible_at,rep_priority_sort,rep_created_at,\
                    rep_item_id,eligible_item_count,rep_created_seq \
             FROM fireweed_group_summary \
             WHERE tenant_id=? AND queue_id=? AND group_key IN ({placeholders})"
        );
        let mut params = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        params.extend(
            chunk
                .iter()
                .map(|group| RelValue::Text(group.as_str().to_string())),
        );
        for row in crate::rel_query(tx, &sql, params)? {
            let group = GroupKey::new(row.get::<String>(0)?)
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            out.insert(
                group,
                LoadedSummary {
                    oldest: row.get(1)?,
                    rep_priority_sort: row.get(2)?,
                    rep_created_at: row.get(3)?,
                    rep_item_id: row.get(4)?,
                    count: row.get(5)?,
                    rep_created_seq: row.get(6)?,
                },
            );
        }
    }
    Ok(out)
}

fn upsert_group_summaries(
    tx: &impl RelTx,
    shard: &QueueKey,
    rows: &[SummaryWrite],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    const ROW: &str = "(?,?,?,?,NULL,?,?,?,?,?,0,?)";
    for chunk in rows.chunks(SUMMARY_UPSERT_BATCH) {
        let values = vec![ROW; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO fireweed_group_summary \
             (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort,\
              rep_created_at,rep_created_seq,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
             VALUES {values} \
             ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
              oldest_eligible_at=excluded.oldest_eligible_at, \
              rep_progress_guard_sort=excluded.rep_progress_guard_sort, \
              rep_priority_sort=excluded.rep_priority_sort, \
              rep_created_at=excluded.rep_created_at, \
              rep_created_seq=excluded.rep_created_seq, \
              rep_item_id=excluded.rep_item_id, \
              eligible_item_count=excluded.eligible_item_count, \
              at_risk_count=excluded.at_risk_count, \
              updated_at=excluded.updated_at"
        );
        let mut params = Vec::with_capacity(chunk.len() * 10);
        for row in chunk {
            params.extend([
                RelValue::Text(t.clone()),
                RelValue::Text(q.clone()),
                RelValue::Text(row.group_key.as_str().to_string()),
                RelValue::opt_int(row.oldest),
                RelValue::opt_blob(row.rep_priority_sort.clone()),
                RelValue::opt_int(row.rep_created_at),
                RelValue::opt_int(row.rep_created_seq),
                RelValue::opt_text(row.rep_item_id.clone()),
                RelValue::Integer(row.count),
                RelValue::Integer(now_n),
            ]);
        }
        crate::rel_exec(tx, &sql, params)?;
    }
    Ok(())
}

fn items_by_group(items: &[GroupItemRef]) -> HashMap<GroupKey, Vec<GroupItemRef>> {
    let mut grouped: HashMap<GroupKey, Vec<GroupItemRef>> = HashMap::new();
    for item in items {
        grouped
            .entry(item.group_key.clone())
            .or_default()
            .push(GroupItemRef {
                group_key: item.group_key.clone(),
                item_id: item.item_id.clone(),
                eligible_since: item.eligible_since,
                priority_sort: item.priority_sort.clone(),
                created_at: item.created_at,
                created_seq: item.created_seq,
            });
    }
    grouped
}

/// Newly eligible items join the summary (Push, lease expiry, finalize release/retry).
fn apply_group_summary_add(
    tx: &impl RelTx,
    shard: &QueueKey,
    groups: &[GroupKey],
    added: &[GroupItemRef],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if groups.is_empty() {
        return Ok(());
    }
    if has_blocked_gates(tx, shard)? {
        return relect_group_summaries(tx, shard, groups, now);
    }
    let existing = load_summaries(tx, shard, groups)?;
    let added = items_by_group(added);
    let mut writes = Vec::new();
    let mut fallback = Vec::new();
    for group in groups {
        let incoming = added.get(group).map(Vec::as_slice).unwrap_or(&[]);
        match existing.get(group) {
            None => writes.push(write_from_items(group, incoming)),
            Some(current) if current.count > 0 && current.rep_created_seq.is_none() => {
                fallback.push(group.clone());
            }
            Some(current) => {
                let count = current.count + incoming.len() as i64;
                let oldest = match (
                    current.oldest,
                    incoming.iter().map(|item| item.eligible_since).min(),
                ) {
                    (Some(left), Some(right)) => Some(left.min(right)),
                    (Some(left), None) => Some(left),
                    (None, Some(right)) => Some(right),
                    (None, None) => None,
                };
                let winner = match (
                    current.rep_item_id.as_ref(),
                    current.rep_priority_sort.as_deref(),
                    current.rep_created_seq,
                    best_item(incoming),
                ) {
                    (_, _, _, None) => (
                        current.rep_priority_sort.clone(),
                        current.rep_created_at,
                        current.rep_created_seq,
                        current.rep_item_id.clone(),
                    ),
                    (None, _, _, Some(item))
                    | (_, None, _, Some(item))
                    | (_, _, None, Some(item)) => (
                        Some(item.priority_sort.clone()),
                        Some(item.created_at),
                        Some(item.created_seq),
                        Some(item.item_id.clone()),
                    ),
                    (Some(_), Some(sort), Some(seq), Some(item)) if item_beats(item, sort, seq) => {
                        (
                            Some(item.priority_sort.clone()),
                            Some(item.created_at),
                            Some(item.created_seq),
                            Some(item.item_id.clone()),
                        )
                    }
                    _ => (
                        current.rep_priority_sort.clone(),
                        current.rep_created_at,
                        current.rep_created_seq,
                        current.rep_item_id.clone(),
                    ),
                };
                writes.push(SummaryWrite {
                    group_key: group.clone(),
                    oldest,
                    rep_priority_sort: winner.0,
                    rep_created_at: winner.1,
                    rep_created_seq: winner.2,
                    rep_item_id: winner.3,
                    count,
                });
            }
        }
    }
    upsert_group_summaries(tx, shard, &writes, now)?;
    relect_group_summaries(tx, shard, &fallback, now)
}

/// Drop claimed (or otherwise no-longer-eligible) items from `fireweed_group_summary`.
/// Used by the sqlite Class S lease txn (same IMMEDIATE as the lease). Live Turso apply
/// of an already-leased Claim skips this; rebuild still goes through `QueueCommand::Claim`.
pub fn remove_ids_from_group_summaries(
    tx: &impl RelTx,
    shard: &QueueKey,
    ids: &[ItemId],
    now: UtcTimestamp,
) -> EngineResult<()> {
    let removed = load_grouped_items(tx, shard, ids)?;
    apply_group_summary_remove(tx, shard, &removed, now)
}

/// Items leaving eligibility (Claim, or becoming deferred). Recompute only groups that lost their
/// representative or whose oldest timestamp may have moved.
fn apply_group_summary_remove(
    tx: &impl RelTx,
    shard: &QueueKey,
    removed: &[GroupItemRef],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if removed.is_empty() {
        return Ok(());
    }
    let groups = unique_groups(removed);
    if has_blocked_gates(tx, shard)? {
        return relect_group_summaries(tx, shard, &groups, now);
    }
    let existing = load_summaries(tx, shard, &groups)?;
    let removed = items_by_group(removed);
    let mut writes = Vec::new();
    let mut fallback = Vec::new();
    let mut lost_reps = Vec::new();
    for group in &groups {
        let leaving = removed.get(group).map(Vec::as_slice).unwrap_or(&[]);
        let Some(current) = existing.get(group) else {
            fallback.push(group.clone());
            continue;
        };
        let count = current.count - leaving.len() as i64;
        if count < 0 {
            fallback.push(group.clone());
            continue;
        }
        let leaving_ids: HashSet<&str> = leaving.iter().map(|item| item.item_id.as_str()).collect();
        let lost_rep = current
            .rep_item_id
            .as_deref()
            .is_some_and(|id| leaving_ids.contains(id));
        if count == 0 {
            writes.push(SummaryWrite {
                group_key: group.clone(),
                oldest: None,
                rep_priority_sort: None,
                rep_created_at: None,
                rep_created_seq: None,
                rep_item_id: None,
                count: 0,
            });
            continue;
        }
        if lost_rep {
            lost_reps.push((group.clone(), count));
            continue;
        }
        writes.push(SummaryWrite {
            group_key: group.clone(),
            oldest: current.oldest,
            rep_priority_sort: current.rep_priority_sort.clone(),
            rep_created_at: current.rep_created_at,
            rep_created_seq: current.rep_created_seq,
            rep_item_id: current.rep_item_id.clone(),
            count,
        });
    }
    upsert_group_summaries(tx, shard, &writes, now)?;
    refresh_lost_group_reps_sql(tx, shard, &lost_reps, now)?;
    relect_group_summaries(tx, shard, &fallback, now)
}

fn refresh_lost_group_reps_sql(
    tx: &impl RelTx,
    shard: &QueueKey,
    lost: &[(GroupKey, i64)],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if lost.is_empty() {
        return Ok(());
    }
    let (tenant, queue) = parts(shard);
    let now_n = ts_nanos(now);
    const ROW_BINDS: usize = 2;
    let extra = 6;
    for chunk in lost.chunks(bind_chunk_size(ROW_BINDS, extra)) {
        let values = vec!["(?,?)"; chunk.len()].join(",");
        let mut params = Vec::with_capacity(chunk.len() * ROW_BINDS + extra);
        for (group, count) in chunk {
            params.extend([
                RelValue::Text(group.as_str().to_string()),
                RelValue::Integer(*count),
            ]);
        }
        params.extend([
            RelValue::Text(tenant.clone()),
            RelValue::Text(queue.clone()),
            RelValue::Integer(now_n),
            RelValue::Integer(now_n),
            RelValue::Text(tenant.clone()),
            RelValue::Text(queue.clone()),
        ]);
        crate::rel_exec(
            tx,
            &format!(
                "WITH incoming(group_key,new_count) AS (VALUES {values}), \
                 head AS ( \
                   SELECT incoming.group_key AS group_key, i.item_id, i.priority_sort, \
                          i.created_at, i.created_seq, i.eligible_since \
                   FROM incoming \
                   LEFT JOIN fireweed_items i ON i.rowid = ( \
                     SELECT i2.rowid FROM fireweed_items i2 \
                     INDEXED BY fireweed_items_pending_group_nonnull_idx \
                     WHERE i2.tenant_id=? AND i2.queue_id=? \
                       AND i2.group_key=incoming.group_key AND i2.group_key IS NOT NULL \
                       AND i2.lifecycle_state='Pending' AND i2.superseded=0 \
                       AND (i2.not_before IS NULL OR i2.not_before<=?) \
                     ORDER BY i2.priority_sort, i2.created_seq, i2.item_id \
                     LIMIT 1 \
                   ) \
                 ) \
                 UPDATE fireweed_group_summary SET \
                   eligible_item_count=incoming.new_count, \
                   oldest_eligible_at=head.eligible_since, \
                   rep_item_id=head.item_id, \
                   rep_priority_sort=head.priority_sort, \
                   rep_created_at=head.created_at, \
                   rep_created_seq=head.created_seq, \
                   updated_at=? \
                 FROM incoming \
                 LEFT JOIN head ON head.group_key=incoming.group_key \
                 WHERE fireweed_group_summary.tenant_id=? AND fireweed_group_summary.queue_id=? \
                   AND fireweed_group_summary.group_key=incoming.group_key"
            ),
            params,
        )?;
    }
    Ok(())
}

/// Priority / schedule change for items that stay eligible. Recompute a group only when its current
/// representative was itself rewritten (another resident item may now win).
fn apply_group_summary_rerank(
    tx: &impl RelTx,
    shard: &QueueKey,
    updated: &[GroupItemRef],
    eligible_since_changed: bool,
    now: UtcTimestamp,
) -> EngineResult<()> {
    if updated.is_empty() {
        return Ok(());
    }
    let groups = unique_groups(updated);
    if has_blocked_gates(tx, shard)? {
        return relect_group_summaries(tx, shard, &groups, now);
    }
    if eligible_since_changed {
        return relect_group_summaries(tx, shard, &groups, now);
    }
    let updated = items_by_group(updated);
    let mut fallback = Vec::new();
    // One conditional UPDATE replaces the former summary SELECT plus batched UPSERTs. The changed-id
    // JSON lets SQL detect whether the old representative was rewritten. A representative that moved
    // later cannot be repaired from only the changed rows, so that group is deliberately omitted from
    // RETURNING and sent through the exact resident-row fallback below.
    const INPUT_COLUMNS: usize = 6;
    const FIXED_BINDS: usize = 3;
    let (tenant, queue) = parts(shard);
    for chunk in groups.chunks((900 - FIXED_BINDS) / INPUT_COLUMNS) {
        let mut params = Vec::with_capacity(chunk.len() * INPUT_COLUMNS + FIXED_BINDS);
        for group in chunk {
            let changed = updated
                .get(group)
                .expect("unique groups came from updated items");
            let best = best_item(changed).expect("updated group is non-empty");
            params.extend([
                RelValue::Text(group.as_str().to_string()),
                RelValue::Blob(best.priority_sort.clone()),
                RelValue::Integer(best.created_at),
                RelValue::Integer(best.created_seq),
                RelValue::Text(best.item_id.clone()),
                RelValue::Text(
                    serde_json::to_string(
                        &changed
                            .iter()
                            .map(|item| item.item_id.as_str())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
                ),
            ]);
        }
        params.extend([
            RelValue::Integer(ts_nanos(now)),
            RelValue::Text(tenant.clone()),
            RelValue::Text(queue.clone()),
        ]);
        let incoming_rows = vec!["(?,?,?,?,?,?)"; chunk.len()].join(",");
        let incoming_wins = "(incoming.rep_priority_sort < fireweed_group_summary.rep_priority_sort \
            OR (incoming.rep_priority_sort = fireweed_group_summary.rep_priority_sort \
                AND incoming.rep_created_seq <= fireweed_group_summary.rep_created_seq))";
        let sql = format!(
            "WITH incoming(group_key,rep_priority_sort,rep_created_at,rep_created_seq,rep_item_id,changed_ids) \
             AS (VALUES {incoming_rows}) \
             UPDATE fireweed_group_summary SET \
               rep_priority_sort=CASE WHEN {incoming_wins} THEN incoming.rep_priority_sort \
                                      ELSE fireweed_group_summary.rep_priority_sort END, \
               rep_created_at=CASE WHEN {incoming_wins} THEN incoming.rep_created_at \
                                   ELSE fireweed_group_summary.rep_created_at END, \
               rep_created_seq=CASE WHEN {incoming_wins} THEN incoming.rep_created_seq \
                                    ELSE fireweed_group_summary.rep_created_seq END, \
               rep_item_id=CASE WHEN {incoming_wins} THEN incoming.rep_item_id \
                                ELSE fireweed_group_summary.rep_item_id END, \
               updated_at=? \
             FROM incoming WHERE fireweed_group_summary.tenant_id=? \
               AND fireweed_group_summary.queue_id=? \
               AND fireweed_group_summary.group_key=incoming.group_key \
               AND fireweed_group_summary.eligible_item_count>0 \
               AND fireweed_group_summary.rep_priority_sort IS NOT NULL \
               AND fireweed_group_summary.rep_created_seq IS NOT NULL \
               AND fireweed_group_summary.rep_item_id IS NOT NULL \
               AND NOT (EXISTS (SELECT 1 FROM json_each(incoming.changed_ids) changed \
                                WHERE changed.value=fireweed_group_summary.rep_item_id) \
                        AND NOT {incoming_wins}) \
             RETURNING group_key"
        );
        let maintained = crate::rel_query(tx, &sql, params)?
            .into_iter()
            .map(|row| row.get::<String>(0))
            .collect::<EngineResult<HashSet<_>>>()?;
        fallback.extend(
            chunk
                .iter()
                .filter(|group| !maintained.contains(group.as_str()))
                .cloned(),
        );
    }
    relect_group_summaries(tx, shard, &fallback, now)
}

/// Recompute `fireweed_group_summary` from `fireweed_items` for the named groups.
///
/// Incremental add/remove/rerank keep the summary row from the mutated refs when that is enough.
/// This is the fallback when they cannot: COUNT + MIN over the group, and one `LIMIT 1` index seek
/// for the would-be-first-claimed item (`priority_sort, created_seq, item_id`). Blocked gates add
/// the same eligibility anti-join the claim path uses; they do not require loading every remaining
/// member into the process. `rep_progress_guard_sort`/`at_risk_count` stay NULL/0 while that
/// derivation is deferred (BQ-14).
///
/// Exact at mutation time except item Claim/Complete and uniform-priority BatchUpdate, which
/// leave the summary lagged so the serving apply path does not rewrite `G` group-head rows.
/// Time-only `not_before` crossings lag the same way: the filter is `not_before<=now`, so a
/// deferred item that becomes due without a later mutation is not in
/// `oldest_eligible_at`/`rep_*`/`eligible_item_count` until the next write to its group. Per-item
/// `select_eligible` re-evaluates `not_before` on read. BQ-14 g1/g4 refresh due groups before
/// mutation-backed group claims; read-only discovery may under-report until then.
pub fn relect_group_summaries(
    tx: &impl RelTx,
    shard: &QueueKey,
    group_keys: &[GroupKey],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if group_keys.is_empty() {
        return Ok(());
    }
    let gate = if has_blocked_gates(tx, shard)? {
        GATE_ANTI_JOIN
    } else {
        ""
    };
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut writes = Vec::with_capacity(group_keys.len());
    for chunk in group_keys.chunks(SQLITE_BATCH.saturating_sub(3)) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let mut params = vec![
            RelValue::Text(t.clone()),
            RelValue::Text(q.clone()),
            RelValue::Integer(now_n),
        ];
        params.extend(
            chunk
                .iter()
                .map(|group| RelValue::Text(group.as_str().to_string())),
        );
        let mut selected: HashMap<GroupKey, SummaryWrite> = HashMap::new();
        for row in crate::rel_query(
            tx,
            &format!(
                "SELECT group_key,item_id,priority_sort,created_at,created_seq,eligible_item_count,oldest_eligible_at \
                 FROM (SELECT group_key,item_id,priority_sort,created_at,created_seq, \
                   COUNT(*) OVER (PARTITION BY group_key) AS eligible_item_count, \
                   MIN(eligible_since) OVER (PARTITION BY group_key) AS oldest_eligible_at, \
                   ROW_NUMBER() OVER (PARTITION BY group_key ORDER BY priority_sort,created_seq,item_id) AS claim_rank \
                   FROM fireweed_items WHERE tenant_id=? AND queue_id=? \
                     AND lifecycle_state='Pending' AND superseded=0 \
                     AND (not_before IS NULL OR not_before<=?) \
                     AND group_key IN ({placeholders}){gate}) \
                 WHERE claim_rank=1"
            ),
            params,
        )? {
            let group = GroupKey::new(row.get::<String>(0)?)
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            selected.insert(
                group.clone(),
                SummaryWrite {
                    group_key: group,
                    oldest: row.get(6)?,
                    rep_priority_sort: Some(row.get(2)?),
                    rep_created_at: Some(row.get(3)?),
                    rep_created_seq: Some(row.get(4)?),
                    rep_item_id: Some(row.get(1)?),
                    count: row.get(5)?,
                },
            );
        }
        for group in chunk {
            writes.push(selected.remove(group).unwrap_or_else(|| SummaryWrite {
                group_key: group.clone(),
                oldest: None,
                rep_priority_sort: None,
                rep_created_at: None,
                rep_created_seq: None,
                rep_item_id: None,
                count: 0,
            }));
        }
    }
    upsert_group_summaries(tx, shard, &writes, now)
}

/// True when `finalize` is Complete for exactly `claim.item_ids` (same order not required).
pub fn finalize_completes_claim(
    claim: &fireweed_engine::ClaimCommand,
    finalize: &FinalizeCommand,
) -> bool {
    if claim.item_ids.len() != finalize.outcomes.len() {
        return false;
    }
    if !finalize
        .outcomes
        .iter()
        .all(|o| matches!(o.kind, FinalizeKind::Complete))
    {
        return false;
    }
    let mut claimed: BTreeSet<ItemId> = claim.item_ids.iter().copied().collect();
    for o in &finalize.outcomes {
        if !claimed.remove(&o.item_id) {
            return false;
        }
    }
    claimed.is_empty()
}

pub const AUTHORITY_FIRST_CLAIM_SHORT_MOVE: &str =
    "authority-first claim did not move all named rows";

fn authority_first_short_move(moved: usize, named: usize) -> EngineError {
    EngineError::Storage(format!(
        "{AUTHORITY_FIRST_CLAIM_SHORT_MOVE}: moved {moved} of {named}"
    ))
}

fn class_s_live_claim(
    tx: &impl RelTx,
    shard: &QueueKey,
    claim: &ClaimCommand,
) -> EngineResult<bool> {
    if claim.authority_first {
        return Ok(false);
    }
    let Some(first) = claim.item_ids.first() else {
        return Ok(true);
    };
    Ok(item_ids_in_state(tx, shard, &[first.to_string()], "Pending")?.is_empty())
}

#[allow(
    clippy::too_many_arguments,
    reason = "Claim apply explicitly borrows scan hints and token operations alongside command binds"
)]
fn apply_one_claim_sql(
    tx: &impl RelTx,
    grouped_shards: &HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    seq: u64,
    now: UtcTimestamp,
    claim: &ClaimCommand,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let hash = lease_hash(&claim.lease_token);
    let exp = ts_nanos(claim.lease_expires_at);
    let worker_id = claim.worker_id.as_ref().map(|worker| worker.as_str());
    let ids: Vec<String> = claim.item_ids.iter().map(|id| id.to_string()).collect();
    if class_s_live_claim(tx, shard, claim)? {
        for id in &claim.item_ids {
            token_ops.push(TokenOp::Set(shard.clone(), *id, claim.lease_token.clone()));
        }
        return Ok(());
    }
    let mut pending_moved = 0usize;
    let named_range = named_ids_occupy_rowid_slice(tx, shard, &claim.item_ids)?;
    let range = if named_range.is_some() {
        named_range
    } else if claim_scan_default_fifo.get(shard).copied().unwrap_or(false) {
        fifo_rowid_range_for_id_strings(tx, shard, &ids, Some("Pending"))?
    } else {
        None
    };
    if let Some((min_rowid, max_rowid)) = range {
        pending_moved = crate::rel_exec(
            tx,
            "UPDATE fireweed_items NOT INDEXED SET lifecycle_state='Leased', lease_token_hash=?1, \
             lease_expires_at=?2, worker_id=?3, retry_count=retry_count+1, \
             item_version=item_version+1, updated_at=?4, last_command_sequence=?5 \
             WHERE tenant_id=?6 AND queue_id=?7 AND rowid BETWEEN ?8 AND ?9 \
             AND lifecycle_state='Pending' AND superseded=0",
            [
                hash.into(),
                exp.into(),
                worker_id.into(),
                now_n.into(),
                (seq as i64).into(),
                RelValue::Text(t.to_string()),
                RelValue::Text(q.to_string()),
                min_rowid.into(),
                max_rowid.into(),
            ],
        )?;
        let next = max_rowid
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("claim scan hint overflow".into()))?;
        let slot = claim_scan_hints.entry(shard.clone()).or_insert(0);
        if next > *slot {
            *slot = next;
        }
    } else {
        let worker = worker_id.map_or(RelValue::Null, |worker| RelValue::Text(worker.to_string()));
        pending_moved +=
            lease_pending_ids_sql(tx, &t, &q, &ids, &hash, exp, &worker, now_n, seq as i64)?;
        maybe_advance_claim_scan_hint(
            tx,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            shard,
            &claim.item_ids,
        )?;
        if claim.authority_first && pending_moved != claim.item_ids.len() {
            return Err(authority_first_short_move(
                pending_moved,
                claim.item_ids.len(),
            ));
        }
        for id in &claim.item_ids {
            token_ops.push(TokenOp::Set(shard.clone(), *id, claim.lease_token.clone()));
        }
        if pending_moved > 0 {
            persist_lease_bearers(tx, shard, &claim.item_ids, &claim.lease_token)?;
        }
        return Ok(());
    }
    if claim.authority_first && pending_moved != claim.item_ids.len() {
        return Err(authority_first_short_move(
            pending_moved,
            claim.item_ids.len(),
        ));
    }
    for id in &claim.item_ids {
        token_ops.push(TokenOp::Set(shard.clone(), *id, claim.lease_token.clone()));
    }
    if pending_moved > 0 {
        persist_lease_bearers(tx, shard, &claim.item_ids, &claim.lease_token)?;
    }
    Ok(())
}

fn apply_packed_claims_sql(
    tx: &impl RelTx,
    grouped_shards: &HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    units: &[(&ClaimCommand, u64, UtcTimestamp)],
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let mut mutating = Vec::new();
    for &(claim, seq, now) in units {
        if class_s_live_claim(tx, shard, claim)? {
            for id in &claim.item_ids {
                token_ops.push(TokenOp::Set(shard.clone(), *id, claim.lease_token.clone()));
            }
            continue;
        }
        mutating.push((claim, seq, now));
    }
    if mutating.is_empty() {
        return Ok(());
    }
    let mut rows: Vec<(String, Vec<u8>, i64, RelValue, i64, i64)> = Vec::new();
    let mut bearer_rows = Vec::new();
    let mut all_ids = Vec::new();
    let mut named = 0usize;
    let mut authority_first = false;
    for (claim, seq, now) in &mutating {
        authority_first |= claim.authority_first;
        named += claim.item_ids.len();
        let hash = lease_hash(&claim.lease_token);
        let exp = ts_nanos(claim.lease_expires_at);
        let worker = claim
            .worker_id
            .as_ref()
            .map(|worker| RelValue::Text(worker.as_str().to_string()))
            .unwrap_or(RelValue::Null);
        let now_n = ts_nanos(*now);
        let seq_n = *seq as i64;
        for id in &claim.item_ids {
            rows.push((
                id.to_string(),
                hash.clone(),
                exp,
                worker.clone(),
                now_n,
                seq_n,
            ));
            token_ops.push(TokenOp::Set(shard.clone(), *id, claim.lease_token.clone()));
            bearer_rows.push((*id, claim.lease_token.clone()));
            all_ids.push(*id);
        }
    }
    let pending_moved = if tx.prefer_point_updates() {
        let mut changed = 0;
        for (claim, seq, now) in &mutating {
            let ids = claim
                .item_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let worker = claim
                .worker_id
                .as_ref()
                .map(|w| RelValue::Text(w.as_str().to_string()))
                .unwrap_or(RelValue::Null);
            changed += lease_pending_ids_sql(
                tx,
                &t,
                &q,
                &ids,
                &lease_hash(&claim.lease_token),
                ts_nanos(claim.lease_expires_at),
                &worker,
                ts_nanos(*now),
                *seq as i64,
            )?;
        }
        changed
    } else {
        const ROW_BINDS: usize = 6;
        let extra = 2;
        let mut pending_moved = 0usize;
        for chunk in rows.chunks(bind_chunk_size(ROW_BINDS, extra)) {
            let values = vec!["(?,?,?,?,?,?)"; chunk.len()].join(",");
            let mut params = Vec::with_capacity(chunk.len() * ROW_BINDS + extra);
            for (item_id, hash, exp, worker, now_n, seq) in chunk {
                params.extend([
                    RelValue::Text(item_id.clone()),
                    RelValue::Blob(hash.clone()),
                    RelValue::Integer(*exp),
                    worker.clone(),
                    RelValue::Integer(*now_n),
                    RelValue::Integer(*seq),
                ]);
            }
            params.extend([RelValue::Text(t.clone()), RelValue::Text(q.clone())]);
            pending_moved += crate::rel_exec(
                tx,
                &format!(
                    "WITH incoming(item_id,hash,exp,worker,now_n,seq) AS (VALUES {values}) \
                 UPDATE fireweed_items SET \
                   lifecycle_state='Leased', lease_token_hash=incoming.hash, \
                   lease_expires_at=incoming.exp, worker_id=incoming.worker, \
                   retry_count=retry_count+1, item_version=item_version+1, \
                   updated_at=incoming.now_n, last_command_sequence=incoming.seq \
                 FROM incoming \
                 WHERE tenant_id=? AND queue_id=? \
                   AND fireweed_items.item_id=incoming.item_id \
                   AND lifecycle_state='Pending' AND superseded=0"
                ),
                params,
            )?;
        }
        pending_moved
    };
    if authority_first && pending_moved != named {
        return Err(authority_first_short_move(pending_moved, named));
    }
    persist_lease_bearer_pairs(tx, shard, &bearer_rows)?;
    maybe_advance_claim_scan_hint(
        tx,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        shard,
        &all_ids,
    )?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "Parameters mirror the bounded lease UPDATE's queue, identity, and command binds"
)]
fn lease_pending_ids_sql(
    tx: &impl RelTx,
    tenant: &str,
    queue: &str,
    ids: &[String],
    hash: &[u8],
    exp: i64,
    worker: &RelValue,
    now_n: i64,
    seq: i64,
) -> EngineResult<usize> {
    if tx.prefer_point_updates() {
        let mut changed = 0;
        for chunk in ids.chunks(bind_chunk_size(1, 7)) {
            let values = vec!["(?)"; chunk.len()].join(",");
            let sql = format!(
                "WITH incoming(item_id) AS (VALUES {values}) \
                UPDATE fireweed_items SET lifecycle_state='Leased', lease_token_hash=?, \
                lease_expires_at=?, worker_id=?, retry_count=retry_count+1, item_version=item_version+1, \
                updated_at=?, last_command_sequence=? \
                WHERE rowid IN (SELECT i.rowid FROM incoming \
                    CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
                    ON i.tenant_id=? AND i.queue_id=? AND i.item_id=incoming.item_id) \
                AND lifecycle_state='Pending' AND superseded=0"
            );
            let mut params = chunk
                .iter()
                .cloned()
                .map(RelValue::Text)
                .collect::<Vec<_>>();
            params.extend([
                RelValue::Blob(hash.to_vec()),
                exp.into(),
                worker.clone(),
                now_n.into(),
                seq.into(),
                tenant.into(),
                queue.into(),
            ]);
            changed += crate::rel_exec(tx, &sql, params)?;
        }
        return Ok(changed);
    }
    let mut pending_moved = 0usize;
    let extra = 7;
    for chunk in ids.chunks(bind_chunk_size(1, extra)) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "UPDATE fireweed_items SET lifecycle_state='Leased', lease_token_hash=?,\
             lease_expires_at=?, worker_id=?, retry_count=retry_count+1, \
             item_version=item_version+1, updated_at=?, last_command_sequence=? \
             WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph}) \
             AND lifecycle_state='Pending' AND superseded=0"
        );
        let mut params = Vec::with_capacity(chunk.len() + extra);
        params.extend([
            RelValue::Blob(hash.to_vec()),
            RelValue::Integer(exp),
            worker.clone(),
            RelValue::Integer(now_n),
            RelValue::Integer(seq),
            RelValue::Text(tenant.to_string()),
            RelValue::Text(queue.to_string()),
        ]);
        params.extend(chunk.iter().cloned().map(RelValue::Text));
        pending_moved += crate::rel_exec(tx, &sql, params)?;
    }
    Ok(pending_moved)
}

#[allow(
    clippy::too_many_arguments,
    reason = "Apply runs explicitly borrow scan/token state and paired durable command inputs"
)]
fn apply_claim_run_sql(
    tx: &impl RelTx,
    grouped_shards: &HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
) -> EngineResult<()> {
    let mut units = Vec::with_capacity(envelopes.len());
    for (position, envelope) in positions.iter().zip(envelopes) {
        let QueueCommand::Claim(claim) = &envelope.command else {
            return Err(EngineError::Storage(
                "apply_claim_run: non-claim envelope".into(),
            ));
        };
        units.push((claim, position.sequence, envelope.created_at));
    }
    if units.len() == 1 {
        let (claim, seq, now) = units[0];
        return apply_one_claim_sql(
            tx,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            token_ops,
            shard,
            seq,
            now,
            claim,
        );
    }
    apply_packed_claims_sql(
        tx,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        token_ops,
        shard,
        &units,
    )
}

/// One UPDATE: Pending → Complete with claim+finalize version accounting (retry+1, version+2).
///
/// Used when Claim + Finalize(Complete) share an apply batch so we never write Leased /
/// lease-index rows that would be deleted in the next statement.
#[allow(
    clippy::too_many_arguments,
    reason = "fused claim+complete needs both scan maps, the shard cursor, and the claim body"
)]
pub fn apply_fused_claim_complete_sql(
    tx: &impl RelTx,
    grouped_shards: &HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    finalize_position: &CommandPosition,
    now: UtcTimestamp,
    claim: &ClaimCommand,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let ids: Vec<String> = claim.item_ids.iter().map(|i| i.to_string()).collect();
    let seq = finalize_position.sequence as i64;
    let epoch = finalize_position.backend_epoch as i64;
    let hash = lease_hash(&claim.lease_token);
    let pending_only = claim.authority_first;
    let pending_pred = if pending_only {
        "lifecycle_state='Pending'"
    } else {
        "(lifecycle_state='Pending' OR (lifecycle_state='Leased' AND lease_token_hash=?))"
    };
    const PAIR_VERSION_SQL: &str = "retry_count=CASE WHEN lifecycle_state='Pending' THEN retry_count+1 ELSE retry_count END, \
             item_version=CASE WHEN lifecycle_state='Pending' THEN item_version+2 ELSE item_version+1 END,";
    let named: HashSet<ItemId> = claim.item_ids.iter().copied().collect();
    let pending_moved;
    if let Some((min_rowid, max_rowid)) = named_ids_occupy_rowid_slice(tx, shard, &claim.item_ids)?
    {
        let mut between_params = vec![
            RelValue::Integer(now_n),
            RelValue::Integer(epoch),
            RelValue::Integer(now_n),
            RelValue::Integer(seq),
            RelValue::Text(t.to_string()),
            RelValue::Text(q.to_string()),
            RelValue::Integer(min_rowid),
            RelValue::Integer(max_rowid),
        ];
        if !pending_only {
            between_params.push(RelValue::Blob(hash.clone()));
        }
        let returning = crate::rel_query(
            tx,
            &format!(
                "UPDATE fireweed_items NOT INDEXED SET lifecycle_state='Complete', lease_token_hash=NULL, \
                 lease_expires_at=NULL, worker_id=NULL, fenced=0, \
                 {PAIR_VERSION_SQL} \
                 terminal_at=?, terminal_command_epoch=?, updated_at=?, last_command_sequence=? \
                 WHERE tenant_id=? AND queue_id=? AND rowid BETWEEN ? AND ? AND superseded=0 \
                   AND {pending_pred} \
                 RETURNING group_key,item_id,eligible_since,priority_sort,created_at,created_seq"
            ),
            between_params,
        )?;
        if fused_complete_moved_unnamed(&returning, &named)? {
            return Err(EngineError::Storage(
                "fused complete moved a row that was not in the named set".into(),
            ));
        }
        pending_moved = returning.len();
        let next = max_rowid
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("claim scan hint overflow".into()))?;
        let slot = claim_scan_hints.entry(shard.clone()).or_insert(0);
        if next > *slot {
            *slot = next;
        }
        maybe_advance_claim_scan_hint(
            tx,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            shard,
            &claim.item_ids,
        )?;
        if claim.authority_first && pending_moved != claim.item_ids.len() {
            return Err(authority_first_short_move(
                pending_moved,
                claim.item_ids.len(),
            ));
        }
        for id in &claim.item_ids {
            token_ops.push(TokenOp::Clear(shard.clone(), *id));
        }
        return Ok(());
    }
    pending_moved = complete_named_ids_pk_sql(
        tx,
        shard,
        &ids,
        now_n,
        epoch,
        seq,
        PAIR_VERSION_SQL,
        pending_pred,
        (!pending_only).then_some(hash.as_slice()),
    )?;
    maybe_advance_claim_scan_hint(
        tx,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        shard,
        &claim.item_ids,
    )?;
    if claim.authority_first && pending_moved != claim.item_ids.len() {
        return Err(authority_first_short_move(
            pending_moved,
            claim.item_ids.len(),
        ));
    }
    for id in &claim.item_ids {
        token_ops.push(TokenOp::Clear(shard.clone(), *id));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "Fused apply keeps scan/token state and the two durable command runs explicit"
)]
fn apply_fused_claim_complete_run_sql(
    tx: &impl RelTx,
    grouped_shards: &HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    _claim_positions: &[CommandPosition],
    claim_envelopes: &[CommandEnvelope],
    complete_positions: &[CommandPosition],
    complete_envelopes: &[CommandEnvelope],
) -> EngineResult<()> {
    let mut named = 0usize;
    let mut authority_first = false;
    let mut all_ids = Vec::new();
    let mut last_now = complete_envelopes[0].created_at;
    for envelope in claim_envelopes {
        let QueueCommand::Claim(claim) = &envelope.command else {
            return Err(EngineError::Storage(
                "fused claim-complete run: non-claim envelope".into(),
            ));
        };
        authority_first |= claim.authority_first;
        named += claim.item_ids.len();
        all_ids.extend_from_slice(&claim.item_ids);
    }
    let mut ids: Vec<String> = Vec::new();
    for envelope in complete_envelopes {
        let QueueCommand::Finalize(finalize) = &envelope.command else {
            return Err(EngineError::Storage(
                "fused claim-complete run: non-complete envelope".into(),
            ));
        };
        last_now = envelope.created_at;
        for outcome in &finalize.outcomes {
            ids.push(outcome.item_id.to_string());
            token_ops.push(TokenOp::Clear(shard.clone(), outcome.item_id));
        }
    }
    let (t, q) = parts(shard);
    let last_position = complete_positions.last().ok_or_else(|| {
        EngineError::Storage("fused claim-complete run: empty complete positions".into())
    })?;
    let seq = last_position.sequence as i64;
    let epoch = last_position.backend_epoch as i64;
    let now_n = ts_nanos(last_now);
    const RUN_VERSION_SQL: &str = "retry_count=retry_count+1, item_version=item_version+2,";
    let named_set: HashSet<ItemId> = all_ids.iter().copied().collect();
    let pending_moved = if let Some((min_rowid, max_rowid)) =
        named_ids_occupy_rowid_slice(tx, shard, &all_ids)?
    {
        let returning = crate::rel_query(
            tx,
            "UPDATE fireweed_items NOT INDEXED SET lifecycle_state='Complete', lease_token_hash=NULL, \
                 lease_expires_at=NULL, worker_id=NULL, fenced=0, \
                 retry_count=retry_count+1, item_version=item_version+2, \
                 terminal_at=?, terminal_command_epoch=?, updated_at=?, last_command_sequence=? \
                 WHERE tenant_id=? AND queue_id=? AND superseded=0 \
                   AND lifecycle_state='Pending' AND rowid BETWEEN ? AND ? \
                 RETURNING group_key,item_id,eligible_since,priority_sort,created_at,created_seq",
            [
                RelValue::Integer(now_n),
                RelValue::Integer(epoch),
                RelValue::Integer(now_n),
                RelValue::Integer(seq),
                RelValue::Text(t.to_string()),
                RelValue::Text(q.to_string()),
                RelValue::Integer(min_rowid),
                RelValue::Integer(max_rowid),
            ],
        )?;
        if fused_complete_moved_unnamed(&returning, &named_set)? {
            return Err(EngineError::Storage(
                "fused complete moved a row that was not in the named set".into(),
            ));
        }
        returning.len()
    } else {
        complete_named_ids_pk_sql(
            tx,
            shard,
            &ids,
            now_n,
            epoch,
            seq,
            RUN_VERSION_SQL,
            "lifecycle_state='Pending'",
            None,
        )?
    };
    if authority_first && pending_moved != named {
        return Err(authority_first_short_move(pending_moved, named));
    }
    maybe_advance_claim_scan_hint(
        tx,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        shard,
        &all_ids,
    )?;
    Ok(())
}

/// Bind-safe VALUES chunk for set-based BatchUpdate. 8 columns × 100 rows stays under Turso's ~900 bind cap.
const UPDATE_FIELDS_BATCH: usize = 100;

fn update_fields_batch_is_set_based(updates: &[UpdateFieldsCommand]) -> bool {
    updates
        .iter()
        .all(|update| update.field_ops.is_empty() && update.set_entity_document.is_none())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Api001UpdateAddress {
    ItemId,
    ClientItemKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Api001UpdateShape {
    address: Api001UpdateAddress,
    fields: bool,
    payload: bool,
    metadata: bool,
    priority: bool,
    not_before: bool,
}

fn api001_update_shape(update: &UpdateFieldsCommand) -> Option<Api001UpdateShape> {
    if !update.api001_batch
        || !update.field_ops.is_empty()
        || update.set_entity_document.is_some()
        || update.set_gate_keys.is_some()
        || update.expected_item_version.is_some()
    {
        return None;
    }
    let address = if update.client_item_key.is_some() {
        Api001UpdateAddress::ClientItemKey
    } else {
        Api001UpdateAddress::ItemId
    };
    Some(Api001UpdateShape {
        address,
        fields: update.set_fields.is_some(),
        payload: matches!(update.payload, PayloadUpdate::Set(_)),
        metadata: update.set_metadata.is_some(),
        priority: matches!(update.set_priority, ScheduleUpdate::Set(_)),
        not_before: matches!(update.set_not_before, ScheduleUpdate::Set(_)),
    })
}

fn maintain_grouped_schedule_summaries(
    tx: &impl RelTx,
    shard: &QueueKey,
    added: Vec<GroupItemRef>,
    ranked: Vec<GroupItemRef>,
    left_eligible: Vec<GroupItemRef>,
    eligible_since_changed: bool,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let mut transitions = HashMap::<GroupKey, u8>::new();
    for (items, flag) in [(&added, 1_u8), (&ranked, 2_u8), (&left_eligible, 4_u8)] {
        for item in items {
            transitions
                .entry(item.group_key.clone())
                .and_modify(|seen| *seen |= flag)
                .or_insert(flag);
        }
    }
    let mixed = transitions
        .into_iter()
        .filter_map(|(group, flags)| (flags.count_ones() > 1).then_some(group))
        .collect::<HashSet<_>>();
    if !mixed.is_empty() {
        let groups: Vec<GroupKey> = mixed.iter().cloned().collect();
        relect_group_summaries(tx, shard, &groups, now)?;
    }
    let added = added
        .into_iter()
        .filter(|item| !mixed.contains(&item.group_key))
        .collect::<Vec<_>>();
    let ranked = ranked
        .into_iter()
        .filter(|item| !mixed.contains(&item.group_key))
        .collect::<Vec<_>>();
    let left = left_eligible
        .into_iter()
        .filter(|item| !mixed.contains(&item.group_key))
        .collect::<Vec<_>>();
    let added_groups = unique_groups(&added);
    apply_group_summary_add(tx, shard, &added_groups, &added, now)?;
    apply_group_summary_rerank(tx, shard, &ranked, eligible_since_changed, now)?;
    apply_group_summary_remove(tx, shard, &left, now)
}

fn collect_grouped_schedule_rows(
    rows: Vec<RelRow>,
    was_eligible: bool,
    now_n: i64,
    added: &mut Vec<GroupItemRef>,
    ranked: &mut Vec<GroupItemRef>,
    left_eligible: &mut Vec<GroupItemRef>,
) -> EngineResult<HashSet<String>> {
    let mut matched = HashSet::with_capacity(rows.len());
    for row in rows {
        matched.insert(row.get::<String>(0)?);
        let Some(group) = row.get::<Option<String>>(1)? else {
            continue;
        };
        let item = GroupItemRef {
            group_key: GroupKey::new(group)
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            item_id: row.get(2)?,
            eligible_since: row.get(3)?,
            priority_sort: row.get(4)?,
            created_at: row.get(6)?,
            created_seq: row.get(7)?,
        };
        let is_eligible = !row
            .get::<Option<i64>>(5)?
            .is_some_and(|timestamp| timestamp > now_n);
        match (was_eligible, is_eligible) {
            (false, true) => added.push(item),
            (true, true) => ranked.push(item),
            (true, false) => left_eligible.push(item),
            (false, false) => {}
        }
    }
    Ok(matched)
}

/// Apply the common API-001 replacement form without materializing current rows.
///
/// This is deliberately narrower than the general BatchUpdate path. Conditional versions,
/// gate membership, legacy delta updates, entity indexes, grouped eligibility, mixed shapes,
/// and duplicate targets retain the read/transform implementation below. The fast path is for
/// one homogeneous replacement shape over distinct pending rows, which is exactly the phased
/// enrich and schedule workload.
#[allow(clippy::too_many_arguments)]
fn try_apply_operation_shaped_api001_batch(
    tx: &impl RelTx,
    grouped_shards: &HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    seq: u64,
    now: UtcTimestamp,
    model: &PriorityModel,
    updates: &[UpdateFieldsCommand],
) -> EngineResult<bool> {
    let now_n = ts_nanos(now);
    let Some(shape) = updates.first().and_then(api001_update_shape) else {
        return Ok(false);
    };
    if !updates
        .iter()
        .all(|update| api001_update_shape(update) == Some(shape))
    {
        return Ok(false);
    }
    let grouped_schedule = grouped_shards.contains(shard) && (shape.priority || shape.not_before);

    let mut targets = HashSet::with_capacity(updates.len());
    for update in updates {
        let target = match shape.address {
            Api001UpdateAddress::ItemId => update.item_id.to_string(),
            Api001UpdateAddress::ClientItemKey => update
                .client_item_key
                .as_ref()
                .expect("shape requires client item key")
                .as_str()
                .to_string(),
        };
        if !targets.insert(target) {
            return Ok(false);
        }
    }

    let (tenant, queue) = parts(shard);
    let address_column = match shape.address {
        Api001UpdateAddress::ItemId => "item_id",
        Api001UpdateAddress::ClientItemKey => "client_item_key",
    };
    let target = |update: &UpdateFieldsCommand| match shape.address {
        Api001UpdateAddress::ItemId => update.item_id.to_string(),
        Api001UpdateAddress::ClientItemKey => update
            .client_item_key
            .as_ref()
            .expect("shape requires client item key")
            .as_str()
            .to_string(),
    };
    let mut replacement_columns = Vec::new();
    if shape.fields {
        replacement_columns.push("fields");
    }
    if shape.metadata {
        replacement_columns.push("metadata");
    }
    if shape.priority {
        replacement_columns.extend(["priority", "priority_sort"]);
    }
    if shape.not_before {
        replacement_columns.push("not_before");
    }
    let replacement_values = updates
        .iter()
        .map(|update| {
            let mut values = Vec::with_capacity(replacement_columns.len());
            if shape.fields {
                values.push(RelValue::Text(fields_to_json(
                    update
                        .set_fields
                        .as_ref()
                        .expect("shape requires fields replacement"),
                )?));
            }
            if shape.metadata {
                values.push(RelValue::Text(metadata_to_json(
                    update
                        .set_metadata
                        .as_ref()
                        .expect("shape requires metadata replacement"),
                )?));
            }
            if shape.priority {
                let ScheduleUpdate::Set(priority) = &update.set_priority else {
                    unreachable!("shape requires priority replacement")
                };
                values.push(RelValue::opt_text(
                    priority.as_ref().map(to_json).transpose()?,
                ));
                values.push(RelValue::Blob(elig_sort(priority, model)));
            }
            if shape.not_before {
                let ScheduleUpdate::Set(not_before) = update.set_not_before else {
                    unreachable!("shape requires schedule replacement")
                };
                values.push(RelValue::opt_int(not_before.map(ts_nanos)));
            }
            Ok::<_, EngineError>(values)
        })
        .collect::<EngineResult<Vec<_>>>()?;
    if tx.prefer_point_updates() && !grouped_schedule {
        let index = match shape.address {
            Api001UpdateAddress::ItemId => "sqlite_autoindex_fireweed_items_1",
            Api001UpdateAddress::ClientItemKey => "fireweed_items_active_key",
        };
        let assignments = replacement_columns
            .iter()
            .map(|c| format!("{c}=?"))
            .chain([
                "item_version=item_version+1".into(),
                "updated_at=?".into(),
                "last_command_sequence=?".into(),
            ])
            .collect::<Vec<_>>()
            .join(",");
        // Equality on every index key is essential: an index hint with IN still
        // scans the queue prefix in Turso. Keep all points in this transaction.
        let sql = format!(
            "UPDATE fireweed_items INDEXED BY {index} SET {assignments} \
            WHERE tenant_id=? AND queue_id=? AND {address_column}=? \
            AND superseded=0 AND lifecycle_state='Pending' AND fenced=0"
        );
        for (update, values) in updates.iter().zip(&replacement_values) {
            let mut params = values.clone();
            params.extend([
                now_n.into(),
                (seq as i64).into(),
                tenant.clone().into(),
                queue.clone().into(),
                target(update).into(),
            ]);
            crate::rel_exec(tx, &sql, params)?;
        }
        if shape.priority || shape.not_before {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
        }
        write_shaped_payloads(tx, shard, updates, shape)?;
        return Ok(true);
    }
    let uniform_values = replacement_values
        .first()
        .filter(|first| replacement_values.iter().all(|values| values == *first));
    if let Some(values) = uniform_values
        && updates.len() > 1
        && claim_scan_default_fifo.get(shard).copied().unwrap_or(false)
    {
        // Bulk uniform FIFO replacements keep relative (priority_sort, created_seq) order.
        // Skip RETURNING + group-summary rerank; grouped Claim relects stale heads (BQ-14).
        // A 1-item replacement can still move a mixed-queue head, so it keeps the rerank path.
        let range_bind_count = usize::from(shape.address == Api001UpdateAddress::ClientItemKey) * 2;
        let chunk_size = SQLITE_BIND_CAP - values.len() - 4 - range_bind_count;
        for chunk in updates.chunks(chunk_size.max(1)) {
            let mut assignments = replacement_columns
                .iter()
                .map(|column| format!("{column}=?"))
                .collect::<Vec<_>>();
            assignments.extend([
                "item_version=item_version+1".to_string(),
                "updated_at=?".to_string(),
                "last_command_sequence=?".to_string(),
            ]);
            let mut params = values.clone();
            params.extend([
                RelValue::Integer(now_n),
                RelValue::Integer(seq as i64),
                RelValue::Text(tenant.to_string()),
                RelValue::Text(queue.to_string()),
            ]);
            let targets = chunk.iter().map(target).collect::<Vec<_>>();
            let range_predicate = if shape.address == Api001UpdateAddress::ClientItemKey {
                let first = targets.iter().min().expect("non-empty update chunk");
                let last = targets.iter().max().expect("non-empty update chunk");
                params.extend([RelValue::Text(first.clone()), RelValue::Text(last.clone())]);
                format!(" AND {address_column} BETWEEN ? AND ?")
            } else {
                String::new()
            };
            params.extend(targets.into_iter().map(RelValue::Text));
            let sql = format!(
                "UPDATE fireweed_items SET {} \
                 WHERE tenant_id=? AND queue_id=?{range_predicate} AND {address_column} IN ({}) \
                   AND lifecycle_state='Pending' AND superseded=0 AND fenced=0",
                assignments.join(","),
                vec!["?"; chunk.len()].join(",")
            );
            crate::rel_exec(tx, &sql, params)?;
        }
        if schedule_reorders_rowid(updates, shape) {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
        } else if shape.priority || shape.not_before {
            observe_uniform_schedule_for_claim_scan(claim_scan_default_fifo, shard);
        }
        write_shaped_payloads(tx, shard, updates, shape)?;
        return Ok(true);
    }

    let mut added = Vec::new();
    let mut ranked = Vec::new();
    let mut left_eligible = Vec::new();

    let incoming_names = std::iter::once(address_column)
        .chain(replacement_columns.iter().copied())
        .collect::<Vec<_>>();
    let row_binds = incoming_names.len();
    let extra_binds = 4 + usize::from(grouped_schedule);
    let chunk_size = bind_chunk_size(row_binds, extra_binds);
    let incoming_row = format!("({})", vec!["?"; row_binds].join(","));
    let assignments = replacement_columns
        .iter()
        .map(|column| format!("{column}=incoming.{column}"))
        .chain([
            "item_version=item_version+1".to_string(),
            "updated_at=?".to_string(),
            "last_command_sequence=?".to_string(),
        ])
        .collect::<Vec<_>>();
    let assignment_sql = assignments.join(",");
    let incoming_header = incoming_names.join(",");
    for (chunk, value_chunk) in updates
        .chunks(chunk_size)
        .zip(replacement_values.chunks(chunk_size))
    {
        if !grouped_schedule {
            let values_sql = vec![incoming_row.as_str(); chunk.len()].join(",");
            let mut params = Vec::with_capacity(chunk.len() * row_binds + 4);
            for (update, values) in chunk.iter().zip(value_chunk) {
                params.push(RelValue::Text(target(update)));
                params.extend(values.iter().cloned());
            }
            params.extend([
                RelValue::Integer(now_n),
                RelValue::Integer(seq as i64),
                RelValue::Text(tenant.to_string()),
                RelValue::Text(queue.to_string()),
            ]);
            crate::rel_exec(
                tx,
                &format!(
                    "WITH incoming({incoming_header}) AS (VALUES {values_sql}) \
                     UPDATE fireweed_items SET {assignment_sql} \
                     FROM incoming WHERE tenant_id=? AND queue_id=? \
                       AND fireweed_items.{address_column}=incoming.{address_column} \
                       AND lifecycle_state='Pending' AND superseded=0 AND fenced=0"
                ),
                params,
            )?;
            continue;
        }

        let mut remaining = chunk.iter().zip(value_chunk).collect::<Vec<_>>();
        for was_eligible in [true, false] {
            if remaining.is_empty() {
                break;
            }
            let values_sql = vec![incoming_row.as_str(); remaining.len()].join(",");
            let mut params = Vec::with_capacity(remaining.len() * row_binds + extra_binds);
            for (update, values) in &remaining {
                params.push(RelValue::Text(target(update)));
                params.extend(values.iter().cloned());
            }
            params.extend([
                RelValue::Integer(now_n),
                RelValue::Integer(seq as i64),
                RelValue::Text(tenant.to_string()),
                RelValue::Text(queue.to_string()),
                RelValue::Integer(now_n),
            ]);
            let eligibility_predicate = if was_eligible {
                "(fireweed_items.not_before IS NULL OR fireweed_items.not_before<=?)"
            } else {
                "fireweed_items.not_before IS NOT NULL AND fireweed_items.not_before>?"
            };
            let sql = format!(
                "WITH incoming({incoming_header}) AS (VALUES {values_sql}) \
                 UPDATE fireweed_items SET {assignment_sql} \
                 FROM incoming WHERE tenant_id=? AND queue_id=? AND {eligibility_predicate} \
                   AND fireweed_items.{address_column}=incoming.{address_column} \
                   AND lifecycle_state='Pending' AND superseded=0 AND fenced=0 \
                 RETURNING fireweed_items.{address_column},group_key,item_id,eligible_since,priority_sort,not_before,created_at,created_seq"
            );
            let matched = collect_grouped_schedule_rows(
                crate::rel_query(tx, &sql, params)?,
                was_eligible,
                now_n,
                &mut added,
                &mut ranked,
                &mut left_eligible,
            )?;
            if was_eligible {
                remaining.retain(|(update, _)| !matched.contains(&target(update)));
            }
        }
    }
    if schedule_reorders_rowid(updates, shape) {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
    } else if shape.priority || shape.not_before {
        observe_uniform_schedule_for_claim_scan(claim_scan_default_fifo, shard);
    }
    if grouped_schedule {
        maintain_grouped_schedule_summaries(tx, shard, added, ranked, left_eligible, false, now)?;
    }
    write_shaped_payloads(tx, shard, updates, shape)?;
    Ok(true)
}

fn schedule_reorders_rowid(updates: &[UpdateFieldsCommand], shape: Api001UpdateShape) -> bool {
    if !shape.priority && !shape.not_before {
        return false;
    }
    let Some(first) = updates.first() else {
        return false;
    };
    !updates.iter().all(|update| {
        std::mem::discriminant(&update.set_priority) == std::mem::discriminant(&first.set_priority)
            && std::mem::discriminant(&update.set_not_before)
                == std::mem::discriminant(&first.set_not_before)
            && match (&update.set_priority, &first.set_priority) {
                (ScheduleUpdate::Keep, ScheduleUpdate::Keep) => true,
                (ScheduleUpdate::Set(left), ScheduleUpdate::Set(right)) => left == right,
                _ => false,
            }
            && match (&update.set_not_before, &first.set_not_before) {
                (ScheduleUpdate::Keep, ScheduleUpdate::Keep) => true,
                (ScheduleUpdate::Set(left), ScheduleUpdate::Set(right)) => left == right,
                _ => false,
            }
    })
}

fn write_shaped_payloads(
    tx: &impl RelTx,
    shard: &QueueKey,
    updates: &[UpdateFieldsCommand],
    shape: Api001UpdateShape,
) -> EngineResult<()> {
    if !shape.payload {
        return Ok(());
    }
    let (tenant, queue) = parts(shard);
    let mut by_id = Vec::new();
    for update in updates {
        let PayloadUpdate::Set(payload) = &update.payload else {
            continue;
        };
        if update.item_id.as_u64() != 0 || update.client_item_key.is_none() {
            by_id.push((
                update.item_id.to_string(),
                payload.as_ref().map(|bytes| bytes.to_vec()),
            ));
            continue;
        }
        let Some(key) = update.client_item_key.as_ref() else {
            continue;
        };
        crate::rel_exec(
            tx,
            "INSERT INTO fireweed_item_payloads(tenant_id,queue_id,item_id,payload) \
             SELECT tenant_id,queue_id,item_id,? \
             FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND client_item_key=? AND superseded=0 \
             ON CONFLICT(tenant_id,queue_id,item_id) DO UPDATE SET payload=excluded.payload",
            [
                RelValue::opt_blob(payload.as_ref().map(|bytes| bytes.to_vec())),
                RelValue::Text(tenant.clone()),
                RelValue::Text(queue.clone()),
                RelValue::Text(key.as_str().to_string()),
            ],
        )?;
    }
    upsert_item_payloads(tx, &tenant, &queue, by_id)
}

#[allow(
    clippy::too_many_arguments,
    reason = "Batch apply explicitly borrows queue/scan state alongside update command inputs"
)]
fn apply_update_fields_batch_sql(
    tx: &impl RelTx,
    queues: &HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &mut HashSet<QueueKey>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    seq: u64,
    now: UtcTimestamp,
    updates: &[UpdateFieldsCommand],
) -> EngineResult<()> {
    if updates.is_empty() {
        return Ok(());
    }
    if !update_fields_batch_is_set_based(updates) {
        for update in updates {
            apply_command_sql(
                tx,
                queues,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                &mut Vec::new(),
                shard,
                &CommandPosition::new(shard.clone(), 0, seq),
                seq,
                now,
                &QueueCommand::UpdateFields(update.clone()),
            )?;
        }
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let model = queues
        .get(shard)
        .map(|definition| definition.priority_model)
        .ok_or(EngineError::NotFound)?;
    let definition = queues.get(shard).ok_or(EngineError::NotFound)?;
    let legacy_fields_changed = !definition.secondary_indexes.is_empty()
        && updates.iter().any(|update| update.set_fields.is_some());
    if !legacy_fields_changed
        && try_apply_operation_shaped_api001_batch(
            tx,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            shard,
            seq,
            now,
            &model,
            updates,
        )?
    {
        return Ok(());
    }
    if updates.iter().any(|update| {
        matches!(update.set_priority, ScheduleUpdate::Set(_))
            || matches!(update.set_not_before, ScheduleUpdate::Set(_))
    }) {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
    }
    struct CurrentUpdateRow {
        fields: String,
        priority: Option<String>,
        not_before: Option<i64>,
        eligible_since: i64,
        payload: Option<Vec<u8>>,
        metadata: String,
        group_key: Option<String>,
        created_at: i64,
        created_seq: i64,
        item_version: i64,
        lifecycle_state: String,
    }
    let write_payload = updates
        .iter()
        .any(|update| matches!(update.payload, PayloadUpdate::Set(_)));
    let keep_payload = updates
        .iter()
        .any(|update| matches!(update.payload, PayloadUpdate::Keep));
    let need_payload = write_payload && keep_payload;
    let mut key_resolved: HashMap<String, String> = HashMap::new();
    let unresolved_keys: Vec<String> = updates
        .iter()
        .filter(|update| update.item_id.as_u64() == 0 && update.client_item_key.is_some())
        .filter_map(|update| {
            update
                .client_item_key
                .as_ref()
                .map(|key| key.as_str().to_string())
        })
        .collect();
    for chunk in unresolved_keys.chunks(UPDATE_FIELDS_BATCH) {
        if chunk.is_empty() {
            continue;
        }
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id,client_item_key FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND client_item_key IN ({placeholders}) \
             AND superseded=0"
        );
        let mut params = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        params.extend(chunk.iter().cloned().map(RelValue::Text));
        for row in crate::rel_query(tx, &sql, params)? {
            key_resolved.insert(row.get::<String>(1)?, row.get::<String>(0)?);
        }
    }
    let mut current: HashMap<String, CurrentUpdateRow> = HashMap::new();
    let ids: Vec<String> = updates
        .iter()
        .filter_map(|update| {
            if update.item_id.as_u64() == 0 && update.client_item_key.is_some() {
                update
                    .client_item_key
                    .as_ref()
                    .and_then(|key| key_resolved.get(key.as_str()).cloned())
            } else {
                Some(update.item_id.to_string())
            }
        })
        .collect();
    for chunk in ids.chunks(UPDATE_FIELDS_BATCH) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = if need_payload {
            format!(
                "SELECT item_id,fields,lifecycle_state,priority,not_before,eligible_since,CASE WHEN EXISTS(SELECT 1 FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) THEN (SELECT p.payload FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) ELSE payload END,metadata,\
                        group_key,created_at,created_seq,item_version \
                 FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN ({placeholders}) \
                 AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0"
            )
        } else {
            format!(
                "SELECT item_id,fields,lifecycle_state,priority,not_before,eligible_since,NULL,metadata,\
                        group_key,created_at,created_seq,item_version \
                 FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN ({placeholders}) \
                 AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0"
            )
        };
        let mut params = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        params.extend(chunk.iter().cloned().map(RelValue::Text));
        for row in crate::rel_query(tx, &sql, params)? {
            current.insert(
                row.get::<String>(0)?,
                CurrentUpdateRow {
                    fields: row.get(1)?,
                    priority: row.get(3)?,
                    not_before: row.get(4)?,
                    eligible_since: row.get(5)?,
                    payload: if need_payload { row.get(6)? } else { None },
                    metadata: row.get(7)?,
                    group_key: row.get(8)?,
                    created_at: row.get(9)?,
                    created_seq: row.get(10)?,
                    item_version: row.get(11)?,
                    lifecycle_state: row.get(2)?,
                },
            );
        }
    }
    let mut staged: Vec<Vec<RelValue>> = Vec::with_capacity(updates.len());
    let mut ranked: Vec<GroupItemRef> = Vec::new();
    let mut left_eligible: Vec<GroupItemRef> = Vec::new();
    let mut gate_deletes = Vec::new();
    let mut gate_inserts = Vec::new();
    for update in updates {
        let id = if update.item_id.as_u64() == 0 && update.client_item_key.is_some() {
            update
                .client_item_key
                .as_ref()
                .and_then(|key| key_resolved.get(key.as_str()).cloned())
                .unwrap_or_default()
        } else {
            update.item_id.to_string()
        };
        let Some(row) = current.remove(&id) else {
            continue;
        };
        if update.api001_batch && row.lifecycle_state != "Pending" {
            continue;
        }
        if update
            .expected_item_version
            .is_some_and(|expected| expected != row.item_version as u64)
        {
            continue;
        }
        if let Some(gates) = &update.set_gate_keys {
            gate_deletes.push(id.clone());
            gate_inserts.extend(gates.iter().map(|gate| (id.clone(), gate.clone())));
        }
        let CurrentUpdateRow {
            fields: raw_fields,
            priority: mut priority_json,
            mut not_before,
            mut eligible_since,
            mut payload,
            metadata: mut metadata_json,
            group_key,
            created_at,
            created_seq,
            item_version: _,
            lifecycle_state: _,
        } = row;
        let fields_json = match &update.set_fields {
            Some(fields) => fields_to_json(fields)?,
            None => raw_fields,
        };
        if let PayloadUpdate::Set(next) = &update.payload {
            payload = next.as_ref().map(|bytes| bytes.to_vec());
        }
        if let Some(metadata) = &update.set_metadata {
            metadata_json = metadata_to_json(metadata)?;
        }
        if let ScheduleUpdate::Set(next) = &update.set_priority {
            priority_json = next.as_ref().map(to_json).transpose()?;
        }
        if let ScheduleUpdate::Set(next) = &update.set_not_before {
            not_before = (*next).map(ts_nanos);
            if !update.api001_batch {
                eligible_since = not_before.unwrap_or(now_n).max(now_n);
            }
        }
        let priority = parse_priority(priority_json.clone())?;
        let priority_sort = elig_sort(&priority, &model);
        if let Some(group) = group_key {
            let item = GroupItemRef {
                group_key: GroupKey::new(group).map_err(|e| EngineError::Storage(e.to_string()))?,
                item_id: id.clone(),
                eligible_since,
                priority_sort: priority_sort.clone(),
                created_at,
                created_seq,
            };
            if not_before.is_some_and(|ts| ts > now_n) {
                left_eligible.push(item);
            } else {
                ranked.push(item);
            }
        }
        if write_payload {
            staged.push(vec![
                RelValue::Text(id),
                RelValue::Text(fields_json),
                RelValue::opt_blob(payload),
                RelValue::Text(metadata_json),
                RelValue::opt_text(priority_json),
                RelValue::Blob(priority_sort),
                RelValue::opt_int(not_before),
                RelValue::Integer(eligible_since),
            ]);
        } else {
            staged.push(vec![
                RelValue::Text(id),
                RelValue::Text(fields_json),
                RelValue::Text(metadata_json),
                RelValue::opt_text(priority_json),
                RelValue::Blob(priority_sort),
                RelValue::opt_int(not_before),
                RelValue::Integer(eligible_since),
            ]);
        }
    }
    for chunk in staged.chunks(UPDATE_FIELDS_BATCH) {
        let (row_ph, sql) = if write_payload {
            (
                "(?,?,?,?,?,?,?,?)",
                format!(
                    "WITH incoming(item_id,fields,payload,metadata,priority,priority_sort,not_before,eligible_since) \
                     AS (VALUES {}) \
                     UPDATE fireweed_items SET fields=incoming.fields,\
                     metadata=incoming.metadata,priority=incoming.priority,priority_sort=incoming.priority_sort,\
                     not_before=incoming.not_before,eligible_since=incoming.eligible_since,\
                     item_version=item_version+1,updated_at=?,last_command_sequence=? \
                     FROM incoming WHERE tenant_id=? AND queue_id=? AND fireweed_items.item_id=incoming.item_id \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                    vec!["(?,?,?,?,?,?,?,?)"; chunk.len()].join(",")
                ),
            )
        } else {
            (
                "(?,?,?,?,?,?,?)",
                format!(
                    "WITH incoming(item_id,fields,metadata,priority,priority_sort,not_before,eligible_since) \
                     AS (VALUES {}) \
                     UPDATE fireweed_items SET fields=incoming.fields,\
                     metadata=incoming.metadata,priority=incoming.priority,priority_sort=incoming.priority_sort,\
                     not_before=incoming.not_before,eligible_since=incoming.eligible_since,\
                     item_version=item_version+1,updated_at=?,last_command_sequence=? \
                     FROM incoming WHERE tenant_id=? AND queue_id=? AND fireweed_items.item_id=incoming.item_id \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                    vec!["(?,?,?,?,?,?,?)"; chunk.len()].join(",")
                ),
            )
        };
        let _ = row_ph;
        let mut params = Vec::with_capacity(chunk.len() * 8 + 4);
        for row in chunk {
            params.extend(row.iter().cloned());
        }
        params.extend([
            RelValue::Integer(now_n),
            RelValue::Integer(seq as i64),
            RelValue::Text(t.clone()),
            RelValue::Text(q.clone()),
        ]);
        crate::rel_exec(tx, &sql, params)?;
        if write_payload {
            upsert_item_payloads(
                tx,
                &t,
                &q,
                chunk.iter().map(|row| {
                    let id = match &row[0] {
                        RelValue::Text(id) => id.clone(),
                        _ => String::new(),
                    };
                    let payload = match &row[2] {
                        RelValue::Blob(bytes) => Some(bytes.clone()),
                        RelValue::Null => None,
                        _ => None,
                    };
                    (id, payload)
                }),
            )?;
        }
    }
    if legacy_fields_changed {
        let ids = staged
            .iter()
            .filter_map(|row| match &row[0] {
                RelValue::Text(id) => Some(id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        refresh_secondary_indexes(tx, definition, shard, &ids)?;
    }
    for chunk in gate_deletes.chunks(UPDATE_FIELDS_BATCH) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let mut params = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
        params.extend(chunk.iter().cloned().map(RelValue::Text));
        crate::rel_exec(
            tx,
            &format!(
                "DELETE FROM fireweed_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN ({placeholders})"
            ),
            params,
        )?;
    }
    for chunk in gate_inserts.chunks(UPDATE_FIELDS_BATCH) {
        let values = vec!["(?,?,?,?)"; chunk.len()].join(",");
        let mut params = Vec::with_capacity(chunk.len() * 4);
        for (item_id, gate) in chunk {
            params.extend([
                RelValue::Text(t.clone()),
                RelValue::Text(q.clone()),
                RelValue::Text(item_id.clone()),
                RelValue::Text(gate.clone()),
            ]);
        }
        crate::rel_exec(
            tx,
            &format!(
                "INSERT INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) VALUES {values} \
                 ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING"
            ),
            params,
        )?;
    }
    let touches_eligibility = updates.iter().any(|update| {
        matches!(update.set_priority, ScheduleUpdate::Set(_))
            || matches!(update.set_not_before, ScheduleUpdate::Set(_))
    });
    if touches_eligibility && grouped_shards.contains(shard) {
        let since_changed = updates.iter().any(|update| {
            matches!(update.set_not_before, ScheduleUpdate::Set(_)) && !update.api001_batch
        });
        maintain_grouped_schedule_summaries(
            tx,
            shard,
            Vec::new(),
            ranked,
            left_eligible,
            since_changed,
            now,
        )?;
    }
    Ok(())
}

/// Apply one command to `fireweed_items` as SQL. Mirrors `ProjectionData::apply_command` arm-for-arm. The
/// caller must have pre-validated rejectable commands (commit has no rollback past this point), so the
/// only errors here are storage/`NotFound` faults, never behavioral rejections. Live-token mutations are
/// appended to `token_ops` (applied post-commit by the caller), never mutated in place. Grouped-item
/// mutations refresh `fireweed_group_summary` except item Claim/Complete and uniform-priority
/// BatchUpdate, which lag until grouped Claim (BQ-14 `refresh_due_group_summaries`) or a
/// re-entry mutation (Release/Retry/Rearm/LeaseExpired).
#[allow(clippy::too_many_arguments)]
pub fn apply_command_sql(
    tx: &impl RelTx,
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
    apply_command_sql_with_claims(
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
        command,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_command_sql_with_claims(
    tx: &impl RelTx,
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
    fused_claims: Option<&HashSet<ItemId>>,
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
            let specs: Vec<InsertItemSpec<'_>> = c
                .items
                .iter()
                .map(|item| InsertItemSpec {
                    item,
                    command_seq: seq,
                    now,
                })
                .collect();
            let base_seq = insert_item_specs(tx, queues, &model, shard, &specs)?;
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
            if !groups.is_empty() {
                let added = group_item_refs_from_specs(&specs, &model, base_seq);
                apply_group_summary_add(tx, shard, &groups, &added, now)?;
            }
            Ok(())
        }
        QueueCommand::Claim(c) => apply_one_claim_sql(
            tx,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            token_ops,
            shard,
            seq,
            now,
            c,
        ),
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
                    RelValue::Blob(hash.clone()),
                    RelValue::Integer(exp),
                    RelValue::Integer(now_n),
                    RelValue::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            crate::rel_exec(
                tx,
                "UPDATE fireweed_cohorts SET state='leased', cohort_lease_token_hash=?4 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    c.cohort_id.as_str().into(),
                    hash.into(),
                ],
            )?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
            }
            // Committed claim rendering reads these rows rather than process-local tokens.
            // Cohort members need the same persisted bearer as ordinary claimed items.
            persist_lease_bearers(tx, shard, &c.item_ids, &c.lease_token)?;
            if grouped_shards.contains(shard) {
                let groups = groups_of(tx, shard, &c.item_ids)?;
                relect_group_summaries(tx, shard, &groups, now)?;
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
                    RelValue::Integer(exp),
                    RelValue::Integer(now_n),
                    RelValue::Integer(seq as i64),
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
                    RelValue::Integer(exp),
                    RelValue::Integer(now_n),
                    RelValue::Integer(seq as i64),
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
                 last_command_sequence=? WHERE tenant_id=? AND queue_id=? \
                 AND lifecycle_state='Leased' AND item_id IN",
                &[
                    RelValue::Blob(hash.clone()),
                    RelValue::Integer(exp),
                    RelValue::Integer(now_n),
                    RelValue::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            let assigned = item_ids_with_lease_hash(tx, shard, &ids, &hash)?;
            for id in &c.item_ids {
                if assigned.contains(&id.to_string()) {
                    token_ops.push(TokenOp::Set(shard.clone(), *id, c.lease_token.clone()));
                }
            }
            persist_lease_bearers(tx, shard, &c.item_ids, &c.lease_token)?;
            Ok(())
        }
        QueueCommand::UpdateFields(c) => {
            // FAC-1 in-place merge of a LIVE item's fields/payload (no lifecycle change). Read-merge-write
            // the `fields` JSON map in the same representation as insert/read (`fields_to_json`/
            // `fields_from_json`), apply the per-key delta, then UPDATE within this transaction. The caller
            // pre-validated, so the row is live (Pending/Leased, not superseded/fenced); if it is gone here
            // (a divergence) we apply nothing rather than fault, mirroring the in-memory `debug_assert`.
            let current: Option<UpdateFieldsRow> = crate::query_optional(
                tx,
                "SELECT fields,lifecycle_state,priority,not_before,eligible_since,CASE WHEN EXISTS(SELECT 1 FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) THEN (SELECT p.payload FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) ELSE payload END,metadata FROM fireweed_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    c.item_id.to_string().into(),
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )?;
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
                crate::rel_exec(
                    tx,
                    "UPDATE fireweed_items SET fields=?4,payload=?5,metadata=?6,priority=?7,priority_sort=?8, \
                     not_before=?9,eligible_since=?10,item_version=item_version+1,updated_at=?11,last_command_sequence=?12 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                    [
                        RelValue::Text(t.to_string()),
                        RelValue::Text(q.to_string()),
                        c.item_id.to_string().into(),
                        fields_json.into(),
                        RelValue::Null,
                        metadata_json.into(),
                        priority_json.into(),
                        priority_sort.clone().into(),
                        not_before.into(),
                        eligible_since.into(),
                        now_n.into(),
                        (seq as i64).into(),
                    ],
                )?;
                upsert_item_payloads(tx, &t, &q, [(c.item_id.to_string(), payload)])?;
                if let Some(gate_keys) = &c.set_gate_keys {
                    crate::rel_exec(
                        tx,
                        "DELETE FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        [
                            RelValue::Text(t.to_string()),
                            RelValue::Text(q.to_string()),
                            c.item_id.to_string().into(),
                        ],
                    )?;
                    for gate_key in gate_keys {
                        crate::rel_exec(
                            tx,
                            "INSERT OR IGNORE INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) VALUES(?1,?2,?3,?4)",
                            [
                                RelValue::Text(t.to_string()),
                                RelValue::Text(q.to_string()),
                                c.item_id.to_string().into(),
                                gate_key.into(),
                            ],
                        )?;
                    }
                }
                if matches!(c.set_priority, ScheduleUpdate::Set(_))
                    || matches!(c.set_not_before, ScheduleUpdate::Set(_))
                {
                    reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
                    if grouped_shards.contains(shard) {
                        relect_group_summaries(
                            tx,
                            shard,
                            &groups_of(tx, shard, std::slice::from_ref(&c.item_id))?,
                            now,
                        )?;
                    }
                }
                if let Some(ref doc) = c.set_entity_document {
                    let typed_indexes = queues
                        .get(shard)
                        .map(|d| d.typed_indexes.as_slice())
                        .unwrap_or(&[]);
                    let extracted =
                        fireweed_engine::index_fields::extract_index_fields_from_entity(
                            typed_indexes,
                            doc,
                        )?;
                    crate::rel_exec(
                        tx,
                        "UPDATE fireweed_items SET entity_document=?4,index_fields=?5 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        [
                            RelValue::Text(t.to_string()),
                            RelValue::Text(q.to_string()),
                            c.item_id.to_string().into(),
                            to_json(doc)?.into(),
                            fireweed_engine::index_fields::encode_index_fields_blob(&extracted)?
                                .into(),
                        ],
                    )?;
                }
                let definition = queues.get(shard).ok_or(EngineError::NotFound)?;
                let typed_changed =
                    !definition.typed_indexes.is_empty() && c.set_entity_document.is_some();
                let legacy_changed = !definition.secondary_indexes.is_empty()
                    && (c.set_fields.is_some() || !c.field_ops.is_empty());
                if typed_changed || legacy_changed {
                    refresh_secondary_indexes(tx, definition, shard, &[c.item_id.to_string()])?;
                }
            }
            Ok(())
        }
        QueueCommand::UpdateFieldsBatch(c) => apply_update_fields_batch_sql(
            tx,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            shard,
            seq,
            now,
            &c.updates,
        ),
        QueueCommand::Finalize(c) => {
            let finalize_ids: Vec<String> =
                c.outcomes.iter().map(|o| o.item_id.to_string()).collect();
            if !finalize_ids.is_empty() {
                exec_items_in(
                    tx,
                    "DELETE FROM fireweed_lease_bearers WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[],
                    &t,
                    &q,
                    &finalize_ids,
                )?;
            }
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
                let mut p: Vec<RelValue> =
                    vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
                for id in chunk {
                    p.push(RelValue::Text(id.clone()));
                }
                for row in crate::rel_query(tx, &sql, &p)? {
                    let id = row.get::<String>(0)?;
                    let rc = row.get::<i64>(1)?;
                    let ma = row.get::<i64>(2)?;
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
                    RelValue::Integer(now_n),
                    RelValue::Integer(position.backend_epoch as i64),
                    &to_complete,
                ),
                (
                    state_str(ItemState::Failed),
                    false,
                    RelValue::Integer(now_n),
                    RelValue::Integer(position.backend_epoch as i64),
                    &to_failed,
                ),
                (
                    state_str(ItemState::Pending),
                    false,
                    RelValue::Null,
                    RelValue::Null,
                    &to_pending,
                ),
                (
                    state_str(ItemState::Pending),
                    true,
                    RelValue::Null,
                    RelValue::Null,
                    &to_pending_rearm,
                ),
            ];
            for (state, reset, terminal_at, terminal_epoch, ids) in buckets {
                exec_items_in(
                    tx,
                    FINALIZE_SET,
                    &[
                        RelValue::Text(state.to_string()),
                        RelValue::Integer(reset as i64),
                        terminal_at,
                        terminal_epoch,
                        RelValue::Integer(now_n),
                        RelValue::Integer(seq as i64),
                    ],
                    &t,
                    &q,
                    ids,
                )?;
            }
            for (nb_n, ids) in &backoff {
                exec_items_in(
                    tx,
                    "UPDATE fireweed_items SET not_before=?, eligible_since=? \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[RelValue::Integer(*nb_n), RelValue::Integer(*nb_n)],
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
                    &[opt_int(*not_before), RelValue::Integer(*eligible_since)],
                    &t,
                    &q,
                    ids,
                )?;
            }
            let ids: Vec<ItemId> = c.outcomes.iter().map(|o| o.item_id).collect();
            let reenters_eligibility = c.outcomes.iter().any(|o| {
                matches!(
                    o.kind,
                    FinalizeKind::Retry | FinalizeKind::Release | FinalizeKind::Rearm
                )
            });
            if reenters_eligibility {
                reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            }
            // Item Claim/Complete leave fireweed_group_summary lagged. Re-entry must relect,
            // not increment, or Release would double-count a head that was never dropped.
            if grouped_shards.contains(shard) && reenters_eligibility {
                let groups = groups_of(tx, shard, &ids)?;
                relect_group_summaries(tx, shard, &groups, now)?;
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
                let params = std::iter::once(RelValue::Text(t.to_string()))
                    .chain(std::iter::once(RelValue::Text(q.to_string())))
                    .chain(id_strings.into_iter().map(RelValue::Text))
                    .collect::<Vec<_>>();
                let exhausted = tx
                    .query(&sql, &params)?
                    .into_iter()
                    .map(|row| Ok::<_, EngineError>((row.get::<i64>(0)?, row.get::<i64>(1)?)))
                    .collect::<EngineResult<Vec<_>>>()?
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
            crate::rel_exec(
                tx,
                "UPDATE fireweed_cohorts SET state=?4, cohort_lease_token_hash=NULL, retention_until=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    c.cohort_id.as_str().into(),
                    next_state.into(),
                    retention_until.into(),
                ],
            )?;
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
            crate::rel_exec(
                tx,
                "UPDATE fireweed_items SET superseded=1, updated_at=?4, last_command_sequence=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    c.superseded_item_id.to_string().into(),
                    now_n.into(),
                    (seq as i64).into(),
                ],
            )?;
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
            relect_group_summaries(tx, shard, &groups, now)?;
            Ok(())
        }
        QueueCommand::LeaseExpired(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            // Never unlease a live Class S claim. Only rows that are still Leased *and*
            // past expiry move back to Pending. Token/version mismatch is a no-op.
            exec_items_in(
                tx,
                "UPDATE fireweed_items SET lifecycle_state='Pending', lease_token_hash=NULL, \
                 lease_expires_at=NULL, worker_id=NULL, item_version=item_version+1, updated_at=?, \
                 last_command_sequence=? WHERE lease_expires_at IS NOT NULL AND lease_expires_at<? \
                 AND tenant_id=? AND queue_id=? AND lifecycle_state='Leased' AND item_id IN",
                &[
                    RelValue::Integer(now_n),
                    RelValue::Integer(seq as i64),
                    RelValue::Integer(now_n),
                ],
                &t,
                &q,
                &ids,
            )?;
            let still_leased = leased_item_id_set(tx, shard, &ids)?;
            let released: Vec<String> = ids
                .iter()
                .filter(|id| !still_leased.contains(*id))
                .cloned()
                .collect();
            if !released.is_empty() {
                exec_items_in(
                    tx,
                    "DELETE FROM fireweed_lease_bearers WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[],
                    &t,
                    &q,
                    &released,
                )?;
            }
            for id in &c.item_ids {
                if !still_leased.contains(&id.to_string()) {
                    token_ops.push(TokenOp::Clear(shard.clone(), *id));
                }
            }
            if grouped_shards.contains(shard) {
                let groups = groups_of(tx, shard, &c.item_ids)?;
                relect_group_summaries(tx, shard, &groups, now)?;
            }
            Ok(())
        }
        QueueCommand::CohortExpired(c) => {
            // Force every non-terminal member of the cohort to Failed (cohort-incomplete).
            let ids: Vec<ItemId> = {
                let mut out = Vec::new();
                for row in crate::rel_query(
                    tx,
                    "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
                     AND group_key=?3 AND lifecycle_state NOT IN ('Complete','Failed')",
                    [
                        RelValue::Text(t.to_string()),
                        RelValue::Text(q.to_string()),
                        c.group_key.as_str().into(),
                    ],
                )? {
                    out.push(
                        ItemId::new(row.get::<String>(0)?)
                            .map_err(|e| EngineError::Storage(e.to_string()))?,
                    );
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
                    RelValue::Integer(now_n),
                    RelValue::Integer(position.backend_epoch as i64),
                    RelValue::Integer(now_n),
                    RelValue::Integer(seq as i64),
                ],
                &t,
                &q,
                &id_strs,
            )?;
            for id in &ids {
                token_ops.push(TokenOp::Clear(shard.clone(), *id));
            }
            crate::rel_exec(
                tx,
                "UPDATE fireweed_cohorts SET state='terminal', expire_command_pos=?4, \
                 cohort_lease_token_hash=NULL, retention_until=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    c.group_key.as_str().into(),
                    (seq as i64).into(),
                    cohort_retention_until(queues, shard, now_n)?.into(),
                ],
            )?;
            // The whole cohort (group) is now terminal — refresh its summary to empty.
            relect_group_summaries(tx, shard, std::slice::from_ref(&c.group_key), now)?;
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
            crate::rel_exec(
                tx,
                "UPDATE queues SET paused=1,pause_drain_intake=?3 WHERE tenant=?1 AND queue=?2",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    command.drain_intake.into(),
                ],
            )?;
            Ok(())
        }
        QueueCommand::ResumeQueue => {
            crate::rel_exec(
                tx,
                "UPDATE queues SET paused=0,pause_drain_intake=0 WHERE tenant=?1 AND queue=?2",
                [RelValue::Text(t.to_string()), RelValue::Text(q.to_string())],
            )?;
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
            // Only retained keys and group-summary repair need pre-delete
            // metadata. The conservative group memo is loaded on reopen and
            // set whenever a grouped row is inserted; it is never cleared here.
            let metadata_ids = if retention_ms > 0 || grouped_shards.contains(shard) {
                id_strs.as_slice()
            } else {
                &[]
            };
            for chunk in metadata_ids.chunks(SQLITE_BATCH) {
                let ph = vec!["?"; chunk.len()].join(",");
                let (sql, p) = if tx.prefer_point_updates() {
                    let values = vec!["(?)"; chunk.len()].join(",");
                    let sql = format!(
                        "WITH incoming(item_id) AS (VALUES {values}) \
                         SELECT i.item_id, i.group_key, i.client_item_key, i.lifecycle_state \
                         FROM incoming CROSS JOIN fireweed_items i \
                         INDEXED BY sqlite_autoindex_fireweed_items_1 \
                         ON i.tenant_id=? AND i.queue_id=? AND i.item_id=incoming.item_id"
                    );
                    let mut p = chunk
                        .iter()
                        .cloned()
                        .map(RelValue::Text)
                        .collect::<Vec<_>>();
                    p.extend([RelValue::Text(t.clone()), RelValue::Text(q.clone())]);
                    (sql, p)
                } else {
                    let sql = format!(
                        "SELECT item_id, group_key, client_item_key, lifecycle_state FROM fireweed_items \
                         WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
                    );
                    let mut p = vec![RelValue::Text(t.clone()), RelValue::Text(q.clone())];
                    p.extend(chunk.iter().cloned().map(RelValue::Text));
                    (sql, p)
                };
                for r in crate::rel_query(tx, &sql, &p)? {
                    let item_id: String = r.get(0)?;
                    let gk: Option<String> = r.get(1)?;
                    let ck: String = r.get(2)?;
                    let state: String = r.get(3)?;
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
                    let mut p: Vec<RelValue> = Vec::with_capacity(chunk.len() * 5);
                    for (ck, item_id) in chunk {
                        p.push(RelValue::Text(t.clone()));
                        p.push(RelValue::Text(q.clone()));
                        p.push(RelValue::Text(ck.clone()));
                        p.push(RelValue::Text(item_id.clone()));
                        p.push(RelValue::Integer(expires));
                    }
                    crate::rel_exec(tx, &sql, &p)?;
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
            exec_items_in(
                tx,
                "DELETE FROM fireweed_item_payloads WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
            exec_items_in(
                tx,
                "DELETE FROM fireweed_lease_bearers WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
            delete_typed_index_rows(tx, &t, &q, &id_strs)?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(shard.clone(), *id));
            }
            relect_group_summaries(tx, shard, &groups, now)?;
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
                            RelValue::Text(t.clone()),
                            RelValue::Text(q.clone()),
                            RelValue::Text(gate.as_str().to_string()),
                        ]);
                    }
                    crate::rel_exec(
                        tx,
                        &format!(
                            "INSERT INTO fireweed_gate_state (tenant_id,queue_id,gate_key) VALUES {values} \
                             ON CONFLICT(tenant_id,queue_id,gate_key) DO NOTHING"
                        ),
                        &parameters,
                    )?;
                }
            } else {
                for chunk in c.gate_keys.chunks(SQLITE_BATCH) {
                    let placeholders = vec!["?"; chunk.len()].join(",");
                    let mut parameters = Vec::with_capacity(chunk.len() + 2);
                    parameters.extend([RelValue::Text(t.clone()), RelValue::Text(q.clone())]);
                    parameters.extend(
                        chunk
                            .iter()
                            .map(|gate| RelValue::Text(gate.as_str().to_string())),
                    );
                    crate::rel_exec(
                        tx,
                        &format!(
                            "DELETE FROM fireweed_gate_state WHERE tenant_id=? AND queue_id=? \
                             AND gate_key IN ({placeholders})"
                        ),
                        &parameters,
                    )?;
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
                        RelValue::Text(t.clone()),
                        RelValue::Text(q.clone()),
                        RelValue::Blob(record.key.clone()),
                        RelValue::Blob(record.payload.to_vec()),
                    ]);
                }
                crate::rel_exec(
                    tx,
                    &format!(
                        "INSERT INTO fireweed_side_records (tenant_id,queue_id,key,payload) \
                         VALUES {values} ON CONFLICT(tenant_id,queue_id,key) DO UPDATE SET \
                         payload=excluded.payload"
                    ),
                    &parameters,
                )?;
            }
            Ok(())
        }
        // C6 (epic pqueue-2201fd37): advance a caller-supplied opaque instance/state fence. Validated
        // pre-commit (stored==expected, next>expected), so the upsert is infallible. Disjoint from
        // `fireweed_items` — a fence is never claimable/peekable work.
        QueueCommand::AdvanceInstanceFence(c) => {
            crate::rel_exec(
                tx,
                "INSERT INTO fireweed_instance_fences (tenant_id,queue_id,instance_key,fence) \
                 VALUES (?1,?2,?3,?4) \
                 ON CONFLICT(tenant_id,queue_id,instance_key) DO UPDATE SET fence=excluded.fence",
                [
                    RelValue::Text(t.to_string()),
                    RelValue::Text(q.to_string()),
                    c.instance_key.clone().into(),
                    (c.next as i64).into(),
                ],
            )?;
            Ok(())
        }
        QueueCommand::MutateItems(c) => {
            reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
            let item_ids = c.items.iter().map(|item| item.item_id).collect::<Vec<_>>();
            let groups = if grouped_shards.contains(shard) {
                groups_of(tx, shard, &item_ids)?
            } else {
                Vec::new()
            };
            let invalidated: Vec<String> = c
                .items
                .iter()
                .filter_map(|item| match &item.action {
                    ResolvedItemMutationAction::Replace(values)
                    | ResolvedItemMutationAction::ReplaceKeepingPayload(values)
                        if values.invalidate_lease =>
                    {
                        Some(item.item_id.to_string())
                    }
                    _ => None,
                })
                .collect();
            exec_items_in(
                tx,
                "DELETE FROM fireweed_lease_bearers WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &invalidated,
            )?;
            let definition = queues.get(shard).ok_or(EngineError::NotFound)?;
            let has_indexes =
                !definition.typed_indexes.is_empty() || !definition.secondary_indexes.is_empty();

            let mut distinct = HashSet::new();
            let batch_auxiliary = c.items.iter().all(|item| {
                item.action.replacement_values().is_some() && distinct.insert(item.item_id)
            });
            // Only independent lease-clearing rows can share this guarded write.
            // Typed indexes/group summaries and ordered repeated IDs retain the
            // sequential lowering below.
            let batch_replacements = batch_auxiliary
                && !has_indexes
                && !grouped_shards.contains(shard)
                && c.items.iter().all(|item| {
                    item.action
                        .replacement_values()
                        .is_some_and(|values| values.invalidate_lease)
                });
            if batch_replacements {
                apply_clearing_item_replacements(
                    tx,
                    shard,
                    queues.get(shard).ok_or(EngineError::NotFound)?,
                    position,
                    seq,
                    now,
                    &c.items,
                    fused_claims,
                )?;
                token_ops.extend(
                    c.items
                        .iter()
                        .map(|item| TokenOp::Clear(shard.clone(), item.item_id)),
                );
            }
            let sequential_items = if batch_replacements {
                &[][..]
            } else {
                c.items.as_slice()
            };
            for item in sequential_items {
                let item_id = item.item_id.to_string();
                match &item.action {
                    ResolvedItemMutationAction::Purge => {
                        let exists: bool = crate::query_row(
                            tx,
                            "SELECT EXISTS(SELECT 1 FROM fireweed_items \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3)",
                            [
                                RelValue::Text(t.to_string()),
                                RelValue::Text(q.to_string()),
                                item_id.into(),
                            ],
                            |row| row.get(0),
                        )?;
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
                    ResolvedItemMutationAction::Replace(values)
                    | ResolvedItemMutationAction::ReplaceKeepingPayload(values) => {
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
                            (
                                RelValue::Null,
                                RelValue::Null,
                                RelValue::Null,
                                RelValue::Integer(0),
                            )
                        } else {
                            let current: (Option<Vec<u8>>, Option<i64>, Option<String>, i64) =
                                crate::query_row(
                                    tx,
                                    "SELECT lease_token_hash,lease_expires_at,worker_id,fenced FROM fireweed_items \
                                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                                    [
                                        RelValue::Text(t.to_string()),
                                        RelValue::Text(q.to_string()),
                                        RelValue::from(&item_id),
                                    ],
                                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                                )?;
                            (
                                current.0.map(RelValue::Blob).unwrap_or(RelValue::Null),
                                current.1.map(RelValue::Integer).unwrap_or(RelValue::Null),
                                current.2.map(RelValue::Text).unwrap_or(RelValue::Null),
                                RelValue::Integer(current.3),
                            )
                        };
                        let claimed_here =
                            i64::from(fused_claims.is_some_and(|ids| ids.contains(&item.item_id)));
                        let changed = crate::rel_exec(
                            tx,
                            "UPDATE fireweed_items SET lifecycle_state=?4,priority=?5,priority_sort=?6,not_before=?7,\
                             eligible_since=?8,payload=CASE WHEN ?25 THEN payload ELSE ?9 END,fields=?10,metadata=?11,entity_document=?12,index_fields=?13,\
                             lease_token_hash=?14,lease_expires_at=?15,worker_id=?16,fenced=?17,item_version=?18,\
                             terminal_at=?19,terminal_command_epoch=?20,updated_at=?21,last_command_sequence=?22, \
                             retry_count=retry_count+?24 \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 AND item_version=?23 \
                             AND (?24=0 OR (lifecycle_state='Pending' AND superseded=0))",
                            [
                                RelValue::Text(t.to_string()),
                                RelValue::Text(q.to_string()),
                                RelValue::from(&item_id),
                                state_str(values.state).into(),
                                priority_json.into(),
                                priority_sort.clone().into(),
                                values.not_before.map(ts_nanos).into(),
                                ts_nanos(values.eligible_since).into(),
                                RelValue::Null,
                                fields_to_json(&values.fields)?.into(),
                                metadata_to_json(&values.metadata)?.into(),
                                values
                                    .entity_document
                                    .as_ref()
                                    .map(to_json)
                                    .transpose()?
                                    .into(),
                                fireweed_engine::index_fields::encode_index_fields_blob(
                                    &values.index_fields,
                                )?
                                .into(),
                                lease_hash_sql,
                                lease_expiry_sql,
                                worker_sql,
                                fenced_sql,
                                (values.item_version as i64).into(),
                                terminal.then_some(now_n).into(),
                                terminal.then_some(position.backend_epoch as i64).into(),
                                now_n.into(),
                                (seq as i64).into(),
                                (values.item_version.saturating_sub(1 + claimed_here as u64)
                                    as i64)
                                    .into(),
                                claimed_here.into(),
                                item.action.keeps_payload().into(),
                            ],
                        )?;
                        if changed != 1 {
                            return Err(EngineError::Conflict);
                        }
                        if !batch_auxiliary {
                            if !item.action.keeps_payload() {
                                upsert_item_payloads(
                                    tx,
                                    &t,
                                    &q,
                                    [(
                                        item_id.to_string(),
                                        values.payload.as_ref().map(|p| p.to_vec()),
                                    )],
                                )?;
                            }
                            crate::rel_exec(
                                tx,
                                "DELETE FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                                [
                                    RelValue::Text(t.to_string()),
                                    RelValue::Text(q.to_string()),
                                    RelValue::from(&item_id),
                                ],
                            )?;
                            for gate_key in &values.gate_keys {
                                crate::rel_exec(
                                    tx,
                                    "INSERT INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) VALUES(?1,?2,?3,?4)",
                                    [
                                        RelValue::Text(t.to_string()),
                                        RelValue::Text(q.to_string()),
                                        RelValue::from(&item_id),
                                        RelValue::from(gate_key),
                                    ],
                                )?;
                            }
                        }
                        if has_indexes {
                            delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&item_id))?;
                            let keys = secondary_index_keys(
                                definition,
                                &values.fields,
                                &values.index_fields,
                                values.entity_document.as_ref(),
                            )?;
                            check_secondary_unique_conflicts(tx, &t, &q, definition, &keys, None)?;
                            insert_typed_index_rows(tx, &t, &q, &item_id, &keys)?;
                        }
                        if values.invalidate_lease {
                            token_ops.push(TokenOp::Clear(shard.clone(), item.item_id));
                        }
                    }
                }
            }

            if batch_auxiliary {
                // Independent replacements can share payload/gate statements after
                // every version guard has passed. Purges and repeated IDs retain
                // the sequential path above.
                for chunk in c.items.chunks(bind_chunk_size(4, 0)) {
                    upsert_item_payloads(
                        tx,
                        &t,
                        &q,
                        chunk
                            .iter()
                            .filter(|item| !item.action.keeps_payload())
                            .map(|item| {
                                let Some(values) = item.action.replacement_values() else {
                                    unreachable!()
                                };
                                (
                                    item.item_id.to_string(),
                                    values.payload.as_ref().map(|p| p.to_vec()),
                                )
                            }),
                    )?;
                }
                let ids: Vec<_> = c
                    .items
                    .iter()
                    .map(|item| item.item_id.to_string())
                    .collect();
                exec_items_in(
                    tx,
                    "DELETE FROM fireweed_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[],
                    &t,
                    &q,
                    &ids,
                )?;
                let gates: Vec<_> = c
                    .items
                    .iter()
                    .flat_map(|item| {
                        let Some(values) = item.action.replacement_values() else {
                            unreachable!()
                        };
                        values
                            .gate_keys
                            .iter()
                            .map(move |gate| (item.item_id, gate))
                    })
                    .collect();
                for chunk in gates.chunks(bind_chunk_size(4, 0)) {
                    let placeholders = vec!["(?,?,?,?)"; chunk.len()].join(",");
                    let mut params = Vec::with_capacity(chunk.len() * 4);
                    for (id, gate) in chunk {
                        params.extend([
                            RelValue::Text(t.clone()),
                            RelValue::Text(q.clone()),
                            RelValue::Text(id.to_string()),
                            RelValue::Text((*gate).clone()),
                        ]);
                    }
                    crate::rel_exec(
                        tx,
                        &format!(
                            "INSERT INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) VALUES {placeholders}"
                        ),
                        params,
                    )?;
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
            relect_group_summaries(tx, shard, &groups, now)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod claim_mutation_window_tests {
    use super::*;
    use fireweed_core::{LeaseToken, QueueId, TenantId};
    use fireweed_engine::{CommandChecksum, CommandId, MutateItemsCommand, ResolvedItemValues};

    fn id(n: u32) -> ItemId {
        ItemId::mint(1, 0, n)
    }
    fn claim(ids: &[u32]) -> QueueCommand {
        QueueCommand::Claim(
            ClaimCommand::new(
                ids.iter().copied().map(id).collect(),
                LeaseToken::new("lease").unwrap(),
                UtcTimestamp::new(30, 0).unwrap(),
                None,
            )
            .with_authority_first(),
        )
    }
    fn mutation(ids: &[u32]) -> QueueCommand {
        QueueCommand::MutateItems(MutateItemsCommand {
            items: ids
                .iter()
                .map(|n| fireweed_engine::ResolvedItemMutation {
                    item_id: id(*n),
                    action: ResolvedItemMutationAction::ReplaceKeepingPayload(Box::new(
                        ResolvedItemValues {
                            state: ItemState::Pending,
                            item_version: 3,
                            priority: None,
                            not_before: None,
                            eligible_since: UtcTimestamp::new(1, 0).unwrap(),
                            payload: None,
                            fields: Default::default(),
                            metadata: Default::default(),
                            gate_keys: vec![],
                            index_fields: Default::default(),
                            entity_document: None,
                            invalidate_lease: true,
                        },
                    )),
                })
                .collect(),
            gate_changes: vec![],
        })
    }
    fn inputs(commands: Vec<QueueCommand>) -> (Vec<CommandPosition>, Vec<CommandEnvelope>) {
        let shard = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
        let positions = (0..commands.len())
            .map(|i| CommandPosition::new(shard.clone(), 0, i as u64))
            .collect();
        let envelopes = commands
            .into_iter()
            .enumerate()
            .map(|(i, command)| CommandEnvelope {
                command_id: CommandId::new(format!("window-{i}")),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: vec![],
                command,
                checksum: CommandChecksum(0),
                created_at: UtcTimestamp::new(1, 0).unwrap(),
            })
            .collect();
        (positions, envelopes)
    }

    #[test]
    fn interleaved_handlers_pair_only_their_preceding_claims() {
        let (positions, envelopes) = inputs(vec![
            claim(&[1, 2]),
            claim(&[3]),
            mutation(&[1]),
            claim(&[4]),
            mutation(&[3, 4]),
        ]);
        let (end, fused) = claim_mutation_run(&positions, &envelopes, 0).unwrap();
        assert_eq!(end, 5);
        assert_eq!(fused, HashSet::from([id(1), id(3), id(4)]));
    }

    #[test]
    fn ambiguous_commands_end_the_window_before_any_part_is_consumed() {
        let mut legacy = claim(&[2]);
        let QueueCommand::Claim(c) = &mut legacy else {
            unreachable!()
        };
        c.authority_first = false;
        for barrier in [
            claim(&[1]),
            claim(&[2, 2]),
            mutation(&[1]),
            mutation(&[2, 2]),
            QueueCommand::ResumeQueue,
            legacy,
        ] {
            let (positions, envelopes) = inputs(vec![
                claim(&[1]),
                mutation(&[1]),
                barrier,
                claim(&[3]),
                mutation(&[3]),
            ]);
            let (end, fused) = claim_mutation_run(&positions, &envelopes, 0).unwrap();
            assert_eq!(end, 2);
            assert_eq!(fused, HashSet::from([id(1)]));
        }
        // A mutation preceding a claim must not be mistaken for its follow-up.
        let (positions, envelopes) = inputs(vec![
            claim(&[1]),
            mutation(&[1, 2]),
            claim(&[2]),
            mutation(&[2]),
        ]);
        assert_eq!(
            claim_mutation_run(&positions, &envelopes, 0).unwrap(),
            (2, HashSet::from([id(1)]))
        );
        for commands in [
            vec![claim(&[1, 1]), mutation(&[1])],
            vec![claim(&[1]), mutation(&[1, 1])],
            vec![mutation(&[1]), claim(&[1])],
        ] {
            let (positions, envelopes) = inputs(commands);
            assert!(claim_mutation_run(&positions, &envelopes, 0).is_none());
        }
    }

    #[test]
    fn interleaved_window_stops_at_queue_epoch_and_sequence_boundaries() {
        for boundary in 0..3 {
            let (mut positions, envelopes) = inputs(vec![
                claim(&[1]),
                mutation(&[1]),
                claim(&[2]),
                mutation(&[2]),
            ]);
            match boundary {
                0 => positions[2].queue.queue_id = QueueId::new("other").unwrap(),
                1 => positions[2].backend_epoch = 1,
                _ => positions[2].sequence += 1,
            }
            assert_eq!(
                claim_mutation_run(&positions, &envelopes, 0).unwrap(),
                (2, HashSet::from([id(1)]))
            );
        }
    }

    #[test]
    fn replacements_must_clear_the_lease_and_include_its_version_increment() {
        for unsupported in 0..3 {
            let mut replacement = mutation(&[2]);
            let QueueCommand::MutateItems(m) = &mut replacement else {
                unreachable!()
            };
            let ResolvedItemMutationAction::ReplaceKeepingPayload(values) = &mut m.items[0].action
            else {
                unreachable!()
            };
            match unsupported {
                0 => values.invalidate_lease = false,
                1 => values.state = ItemState::Leased,
                _ => values.item_version = 1,
            }
            let (positions, envelopes) =
                inputs(vec![claim(&[1]), mutation(&[1]), claim(&[2]), replacement]);
            assert_eq!(
                claim_mutation_run(&positions, &envelopes, 0).unwrap(),
                (4, HashSet::from([id(1)]))
            );
        }
    }
}
