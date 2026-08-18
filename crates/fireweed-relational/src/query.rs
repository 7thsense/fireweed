//! Shared eligibility / claim-scan reads for SQLite-family projections.

use std::collections::HashMap;

use fireweed_core::{GroupKey, ItemId, Metadata, UtcTimestamp};
use fireweed_engine::{ClaimCompatibility, EngineError, EngineResult, PushItem, QueueKey};

use crate::sql::async_projection as sql;
use crate::{RelTx, RelValue, metadata_to_json, ts_nanos};

/// Max rows per dynamically-built `IN (...)` statement. Matches the historical sqlite bind budget.
pub const RELATIONAL_BATCH: usize = 1_500;

pub fn parts(shard: &QueueKey) -> (String, String) {
    (
        shard.tenant_id.as_str().to_string(),
        shard.queue_id.as_str().to_string(),
    )
}

pub fn is_fifo_claim_scan_item(item: &PushItem) -> bool {
    item.priority.is_none()
        && item.not_before.is_none()
        && item.group_key.is_none()
        && item.cohort_size.is_none()
        && item.gate_keys.is_empty()
}

pub fn reset_claim_scan_hint(
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
) {
    claim_scan_hints.remove(shard);
    claim_scan_default_fifo.insert(shard.clone(), false);
}

pub fn observe_push_for_claim_scan(
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    items: &[&PushItem],
) {
    if items.iter().copied().all(is_fifo_claim_scan_item) {
        claim_scan_default_fifo.entry(shard.clone()).or_insert(true);
    } else {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
    }
}

pub fn queue_paused(tx: &impl RelTx, shard: &QueueKey) -> EngineResult<bool> {
    let (tenant, queue) = parts(shard);
    let paused: Option<i64> = crate::query_optional(
        tx,
        sql::SELECT_QUEUE_PAUSED,
        [RelValue::from(tenant), RelValue::from(queue)],
        |row| row.get(0),
    )?;
    Ok(paused.unwrap_or(0) != 0)
}

pub fn has_blocked_gates(tx: &impl RelTx, shard: &QueueKey) -> EngineResult<bool> {
    let (tenant, queue) = parts(shard);
    let found: Option<i64> = crate::query_optional(
        tx,
        sql::SELECT_HAS_BLOCKED_GATES,
        [RelValue::from(tenant), RelValue::from(queue)],
        |row| row.get(0),
    )?;
    Ok(found.is_some())
}

pub fn select_eligible_sql_after(
    tx: &impl RelTx,
    shard: &QueueKey,
    now: UtcTimestamp,
    limit: usize,
    rowid_floor: Option<i64>,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(tx, shard)? {
        return Ok(Vec::new());
    }
    let (tenant, queue) = parts(shard);
    if !has_blocked_gates(tx, shard)? {
        let (sql_text, params) = if let Some(floor) = rowid_floor {
            (
                sql::SELECT_ELIGIBLE_FIFO_ROWID,
                vec![
                    RelValue::from(tenant),
                    RelValue::from(queue),
                    RelValue::from(ts_nanos(now)),
                    RelValue::from(limit as i64),
                    RelValue::from(floor),
                ],
            )
        } else {
            (
                sql::SELECT_ELIGIBLE_NO_GATES,
                vec![
                    RelValue::from(tenant),
                    RelValue::from(queue),
                    RelValue::from(ts_nanos(now)),
                    RelValue::from(limit as i64),
                ],
            )
        };
        return decode_item_ids(crate::rel_query(tx, sql_text, &params)?);
    }
    decode_item_ids(crate::rel_query(
        tx,
        sql::SELECT_ELIGIBLE,
        &[
            RelValue::from(tenant),
            RelValue::from(queue),
            RelValue::from(ts_nanos(now)),
            RelValue::from(limit as i64),
        ],
    )?)
}

pub fn select_eligible_sql_with_scan_hint(
    tx: &impl RelTx,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &HashMap<QueueKey, bool>,
    shard: &QueueKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    if claim_scan_default_fifo.get(shard).copied().unwrap_or(false) {
        let hint = claim_scan_hints.get(shard).copied().unwrap_or(1).max(1);
        let hinted = select_eligible_sql_after(tx, shard, now, limit, Some(hint))?;
        if hinted.len() == limit {
            return Ok(hinted);
        }
        claim_scan_hints.remove(shard);
    }
    select_eligible_sql_after(tx, shard, now, limit, None)
}

pub fn filter_item_claim_candidates(
    tx: &impl RelTx,
    shard: &QueueKey,
    compatibility: &ClaimCompatibility,
    now: UtcTimestamp,
    max: usize,
) -> EngineResult<Vec<ItemId>> {
    if max == 0 {
        return Ok(Vec::new());
    }
    if compatibility.group_key.is_none() && compatibility.metadata_equals.is_empty() {
        return select_eligible_sql_after(tx, shard, now, max, None);
    }
    if queue_paused(tx, shard)? {
        return Ok(Vec::new());
    }
    let (tenant, queue) = parts(shard);
    let required_group = compatibility.group_key.as_ref().map(GroupKey::as_str);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    decode_item_ids(crate::rel_query(
        tx,
        sql::SELECT_ITEM_CLAIM_FILTERABLE,
        &[
            RelValue::from(tenant),
            RelValue::from(queue),
            RelValue::from(ts_nanos(now)),
            RelValue::from(max as i64),
            RelValue::from(required_group),
            RelValue::from(metadata_filter),
        ],
    )?)
}

fn decode_item_ids(rows: Vec<crate::RelRow>) -> EngineResult<Vec<ItemId>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let raw: String = row.get(0)?;
        out.push(ItemId::new(raw).map_err(|error| EngineError::Storage(error.to_string()))?);
    }
    Ok(out)
}
