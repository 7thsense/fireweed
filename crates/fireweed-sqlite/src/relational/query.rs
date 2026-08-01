use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, CohortId, GroupKey, ItemId, ItemState, LeaseToken, Metadata, QueueId, TenantId,
    UtcTimestamp,
};
use fireweed_engine::{
    ActiveScope, BatchUpdateItemRef, BatchUpdateSnapshotItem, ClaimCompatibility, ClaimedItem,
    CohortLeaseTarget, DiscoveryGranularity, EngineError, EngineResult, ItemView, LeaseView,
    LiveItemView, PendingPage, PendingSummary, QueueKey, QueueMetrics, project_scopes,
};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

use super::*;

// ---------------------------------------------------------------------------
// read queries (SQL over fireweed_items)
// ---------------------------------------------------------------------------

pub(crate) fn batch_update_snapshot_sql(
    conn: &Connection,
    shard: &QueueKey,
    refs: &[BatchUpdateItemRef],
) -> EngineResult<Vec<BatchUpdateSnapshotItem>> {
    let (tenant, queue) = parts(shard);
    let mut ids = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for item_ref in refs {
        match item_ref {
            BatchUpdateItemRef::ItemId(item_id) => {
                ids.insert(item_id.to_string());
            }
            BatchUpdateItemRef::ClientItemKey(key) => {
                keys.insert(key.as_str().to_owned());
            }
            BatchUpdateItemRef::Both {
                item_id,
                client_item_key,
            } => {
                ids.insert(item_id.to_string());
                keys.insert(client_item_key.as_str().to_owned());
            }
        }
    }
    let ids_json =
        serde_json::to_string(&ids).map_err(|error| EngineError::Storage(error.to_string()))?;
    let keys_json =
        serde_json::to_string(&keys).map_err(|error| EngineError::Storage(error.to_string()))?;
    let mut stmt = st(conn.prepare(
        "SELECT item_id,client_item_key,lifecycle_state,item_version,fenced,superseded \
         FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND (\
         item_id IN (SELECT value FROM json_each(?3)) OR \
         client_item_key IN (SELECT value FROM json_each(?4)))",
    ))?;
    let rows = st(
        stmt.query_map(params![tenant, queue, ids_json, keys_json], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        }),
    )?;
    let mut snapshot = Vec::new();
    for row in rows {
        let (item_id, client_item_key, state, item_version, fenced, superseded) = st(row)?;
        snapshot.push(BatchUpdateSnapshotItem {
            item_id: ItemId::new(item_id)
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            client_item_key: ClientItemKey::new(client_item_key)
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            state: parse_state(&state)?,
            item_version: u64::try_from(item_version)
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            fenced: fenced != 0,
            superseded: superseded != 0,
        });
    }
    Ok(snapshot)
}

pub(crate) fn queue_paused(conn: &Connection, shard: &QueueKey) -> EngineResult<bool> {
    let (t, q) = parts(shard);
    let paused: i64 = st(conn
        .query_row(
            "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .ok_or(EngineError::NotFound)?;
    Ok(paused != 0)
}

pub(crate) fn has_blocked_gates(conn: &Connection, shard: &QueueKey) -> EngineResult<bool> {
    let (t, q) = parts(shard);
    let found: Option<i64> = st(conn
        .query_row(
            "SELECT 1 FROM fireweed_gate_state WHERE tenant_id=?1 AND queue_id=?2 LIMIT 1",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?;
    Ok(found.is_some())
}

/// Priority-ordered eligible candidates (pending, not superseded, due at `now`), capped at `limit`. Empty
/// while paused. `created_seq` is the stable FIFO tiebreaker (the relational analogue of the in-memory
/// `created_seq`; BQ-11b adds Eligibility-Precedence progress-guard ordering).
pub(crate) fn select_eligible_sql_with_scan_hint(
    conn: &Connection,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &HashMap<QueueKey, bool>,
    shard: &QueueKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    if claim_scan_default_fifo.get(shard).copied().unwrap_or(false) {
        let hint = claim_scan_hints.get(shard).copied().unwrap_or(1).max(1);
        let hinted = select_eligible_sql_after(conn, shard, now, limit, Some(hint))?;
        if hinted.len() == limit {
            return Ok(hinted);
        }
        claim_scan_hints.remove(shard);
    }
    select_eligible_sql_after(conn, shard, now, limit, None)
}

pub(crate) fn filter_item_claim_candidates(
    conn: &Connection,
    shard: &QueueKey,
    compatibility: &ClaimCompatibility,
    now: UtcTimestamp,
    max: usize,
) -> EngineResult<Vec<ItemId>> {
    if max == 0 {
        return Ok(Vec::new());
    }
    if compatibility.group_key.is_none() && compatibility.metadata_equals.is_empty() {
        return select_eligible_sql_after(conn, shard, now, max, None);
    }
    if queue_paused(conn, shard)? {
        return Ok(Vec::new());
    }
    let (tenant, queue) = parts(shard);
    let required_group = compatibility.group_key.as_ref().map(GroupKey::as_str);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let mut statement = st(conn.prepare(
        "SELECT item_id FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
             AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
             AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
             JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
             AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id) \
             AND (?5 IS NULL OR group_key=?5) \
             AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
               WHERE NOT EXISTS (SELECT 1 FROM json_each(fireweed_items.metadata) actual \
                 WHERE actual.key=wanted.key AND actual.value=wanted.value \
                   AND actual.type=wanted.type)) \
             ORDER BY priority_sort,created_seq LIMIT ?4",
    ))?;
    let rows = st(statement.query_map(
        params![
            tenant,
            queue,
            ts_nanos(now),
            max as i64,
            required_group,
            metadata_filter
        ],
        |row| row.get::<_, String>(0),
    ))?;
    let mut selected = Vec::new();
    for row in rows {
        selected
            .push(ItemId::new(st(row)?).map_err(|error| EngineError::Storage(error.to_string()))?);
    }
    Ok(selected)
}

pub(crate) fn select_eligible_sql_after(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    limit: usize,
    rowid_floor: Option<i64>,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(conn, shard)? {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    if !has_blocked_gates(conn, shard)? {
        let (sql, floor) = if rowid_floor.is_some() {
            (
                "SELECT item_id FROM fireweed_items NOT INDEXED WHERE tenant_id=?1 AND queue_id=?2 \
                 AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
                 AND (not_before IS NULL OR not_before<=?3) \
                 AND eligible_since IS NOT NULL AND rowid>=?5 \
                 ORDER BY rowid LIMIT ?4",
                rowid_floor,
            )
        } else {
            (
                "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
                 AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
                 AND (not_before IS NULL OR not_before<=?3) \
                 AND eligible_since IS NOT NULL \
                 ORDER BY priority_sort, created_seq LIMIT ?4",
                None,
            )
        };
        let mut out = Vec::new();
        let mut stmt = st(conn.prepare(sql))?;
        if let Some(floor) = floor {
            let mapped = st(stmt
                .query_map(params![t, q, ts_nanos(now), limit as i64, floor], |row| {
                    row.get::<_, String>(0)
                }))?;
            for r in mapped {
                out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
            }
        } else {
            let mapped = st(
                stmt.query_map(params![t, q, ts_nanos(now), limit as i64], |row| {
                    row.get::<_, String>(0)
                }),
            )?;
            for r in mapped {
                out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
            }
        }
        return Ok(out);
    }
    // The TD-002 `BatchClaim` candidate predicate (owner-local, no shard filter): pending, due, eligible,
    // ordered by the strict-claim key. `eligible_since IS NOT NULL` matches the CTE; `progress_guard_sort`
    // is omitted — under `ordering_mode=strict` (TD-002:649 sanctions strict ordering as the valid first
    // implementation) it reduces to this strict order, which is also exact parity with the in-memory
    // reference (`eligible_candidates` has no at-risk promotion). `created_seq` is the stable analogue of
    // the CTE's `created_at, item_id` FIFO tiebreak.
    let mut stmt = st(conn.prepare(
        "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
         AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
         AND (not_before IS NULL OR not_before<=?3) \
         AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=fireweed_items.tenant_id AND ig.queue_id=fireweed_items.queue_id \
             AND ig.item_id=fireweed_items.item_id) \
         ORDER BY priority_sort, created_seq LIMIT ?4",
    ))?;
    let mapped = st(
        stmt.query_map(params![t, q, ts_nanos(now), limit as i64], |row| {
            row.get::<_, String>(0)
        }),
    )?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// BQ-14b: group-aware claim selection (group_batching / same_group_key), owner-local, consuming
// `fireweed_group_summary`. The queue has one owner, so every group is owner-local (ADR-008); the sqlite
// relational backend serializes the whole claim under `Mutex<Inner>`, so two claims cannot split a group
// (the postgres backend takes a real `FOR UPDATE SKIP LOCKED` group-summary lock for the same guarantee).
// ---------------------------------------------------------------------------

/// Candidate groups for the queue, ordered by each group's representative claim key (TD-002 g1:
/// `rep_progress_guard_sort` NULL today → `rep_priority_sort, rep_created_at, rep_item_id`). Only groups
/// with a current representative (`oldest_eligible_at IS NOT NULL`) are candidates; the live eligibility is
/// re-read per group at claim time (the summary is the ordering hint; the items are the authority). Before
/// group-aware claims call this, they refresh a bounded set of groups that became due by time alone.
/// Refresh a bounded set of groups that became eligible by time alone (`not_before <= now`) since their
/// last mutation-time summary refresh. Runs only inside mutating group-aware claims; discovery stays
/// read-only and may still under-report until a mutation/tick refreshes the row.
pub(crate) fn refresh_due_group_summaries(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut stmt = st(tx.prepare(
        "SELECT DISTINCT i.group_key \
         FROM fireweed_items i \
         LEFT JOIN fireweed_group_summary gs \
           ON gs.tenant_id=i.tenant_id AND gs.queue_id=i.queue_id AND gs.group_key=i.group_key \
         WHERE i.tenant_id=?1 AND i.queue_id=?2 \
           AND i.lifecycle_state='Pending' AND i.superseded=0 AND i.group_key IS NOT NULL \
           AND i.eligible_since IS NOT NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
           AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gstate \
             ON gstate.tenant_id=ig.tenant_id AND gstate.queue_id=ig.queue_id AND gstate.gate_key=ig.gate_key \
             WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
           AND (gs.group_key IS NULL OR gs.oldest_eligible_at IS NULL OR gs.rep_item_id IS NULL) \
         ORDER BY i.group_key LIMIT ?4",
    ))?;
    let mapped = st(
        stmt.query_map(params![t, q, now_n, GROUP_DUE_REFRESH_LIMIT], |row| {
            row.get::<_, String>(0)
        }),
    )?;
    let mut groups = Vec::new();
    for r in mapped {
        groups.push(GroupKey::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    drop(stmt);
    refresh_group_summaries(tx, shard, &groups, now)?;
    Ok(())
}

/// The live currently-eligible items of one group (pending, not superseded, due at `now`), in claim order,
/// capped at `limit`.
pub(crate) struct GroupEligibility {
    pub(crate) item_ids: Vec<ItemId>,
}

/// `group_batching` selection (API-001 whole-eligible-group, `max_groups=N`): accumulate the oldest-N
/// candidate groups' WHOLE eligible sets, in rep order, stopping when adding the next group would exceed
/// `max_items`. A group is fetched with one extra item (`max_items+1`) so an oversized group is detected:
/// a single group that alone exceeds `max_items` cannot be delivered whole → `BatchTooLarge` (TD-002:711;
/// `max_eligible_group_size` is only a config knob, NOT a hard cap on actual group size, so this guard is
/// load-bearing). Empty groups (no live-eligible item) are skipped. Paused → empty.
pub(crate) fn select_group_batching(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
    max_groups: u32,
    compatibility: &ClaimCompatibility,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(conn, shard)? {
        return Ok(Vec::new()); // a paused queue claims nothing (parity with item-level select_eligible)
    }
    let (tenant, queue) = parts(shard);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let mut statement = st(conn.prepare(
        "WITH candidate_raw AS MATERIALIZED (SELECT s.group_key,e.priority_sort rep_priority_sort,\
           e.created_at rep_created_at,e.item_id rep_item_id,e.created_seq,ROW_NUMBER() OVER \
           (PARTITION BY s.group_key ORDER BY e.priority_sort,e.created_seq,e.item_id) rn \
           FROM fireweed_group_summary s JOIN fireweed_items e ON e.tenant_id=?1 AND e.queue_id=?2 \
             AND e.group_key=s.group_key WHERE s.tenant_id=?1 AND s.queue_id=?2 \
           AND s.oldest_eligible_at IS NOT NULL AND e.lifecycle_state='Pending' AND e.superseded=0 \
           AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
           AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
             JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id \
             AND ig.queue_id=e.queue_id AND ig.item_id=e.item_id) \
           AND NOT EXISTS (SELECT 1 FROM json_each(?5) wanted WHERE NOT EXISTS \
             (SELECT 1 FROM json_each(e.metadata) actual WHERE actual.key=wanted.key \
              AND actual.value=wanted.value AND actual.type=wanted.type)) \
           AND NOT EXISTS (SELECT 1 FROM fireweed_items leased WHERE leased.tenant_id=?1 \
             AND leased.queue_id=?2 AND leased.group_key=s.group_key AND leased.superseded=0 \
             AND leased.cohort_size IS NULL AND leased.lifecycle_state='Leased')), \
         candidate AS MATERIALIZED (SELECT group_key,rep_priority_sort,rep_created_at,rep_item_id \
           FROM candidate_raw WHERE rn=1 ORDER BY rep_priority_sort,created_seq,rep_item_id,group_key LIMIT ?4), \
         eligible AS MATERIALIZED (SELECT c.group_key,c.rep_priority_sort,c.rep_created_at,\
           c.rep_item_id,i.item_id,i.priority_sort,i.created_seq FROM candidate c \
           JOIN fireweed_items i ON i.tenant_id=?1 AND i.queue_id=?2 AND i.group_key=c.group_key \
           WHERE i.lifecycle_state='Pending' AND i.superseded=0 AND i.cohort_size IS NULL \
             AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
               ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
               WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
             AND NOT EXISTS (SELECT 1 FROM json_each(?5) wanted WHERE NOT EXISTS \
               (SELECT 1 FROM json_each(i.metadata) actual WHERE actual.key=wanted.key \
                AND actual.value=wanted.value AND actual.type=wanted.type)) \
           ORDER BY c.rep_priority_sort,c.rep_created_at,c.rep_item_id,c.group_key,\
             i.priority_sort,i.created_seq,i.item_id LIMIT ?6), \
         grouped AS (SELECT group_key,rep_priority_sort,rep_created_at,rep_item_id,COUNT(*) item_count,\
           json_group_array(item_id) item_ids FROM eligible GROUP BY group_key,rep_priority_sort,\
           rep_created_at,rep_item_id) SELECT item_count,item_ids,SUM(item_count) OVER \
           (ORDER BY rep_priority_sort,rep_created_at,rep_item_id,group_key) running_count \
           FROM grouped ORDER BY rep_priority_sort,rep_created_at,rep_item_id,group_key",
    ))?;
    let rows = st(statement.query_map(
        params![
            tenant,
            queue,
            ts_nanos(now),
            i64::from(max_groups),
            metadata_filter,
            max_items.saturating_add(1) as i64
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    ))?;
    let mut selected = Vec::new();
    for row in rows {
        let (count, ids, running) = st(row)?;
        if count as usize > max_items {
            return Err(EngineError::BatchTooLarge);
        }
        if running as usize > max_items {
            break;
        }
        let ids: Vec<String> =
            serde_json::from_str(&ids).map_err(|error| EngineError::Storage(error.to_string()))?;
        for id in ids {
            selected
                .push(ItemId::new(id).map_err(|error| EngineError::Storage(error.to_string()))?);
        }
    }
    Ok(selected)
}

/// `same_group_key` selection (API-001): the server picks the single oldest eligible group and leases its
/// eligible items capped at `max_items` (a partial group is allowed — no batch-too-large). Paused → empty.
pub(crate) fn select_same_group(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
    compatibility: &ClaimCompatibility,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(conn, shard)? {
        return Ok(Vec::new());
    }
    let (tenant, queue) = parts(shard);
    let required_group = compatibility.group_key.as_ref().map(GroupKey::as_str);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let mut statement = st(conn.prepare(
        "WITH candidate AS (SELECT s.group_key FROM fireweed_group_summary s WHERE s.tenant_id=?1 \
         AND s.queue_id=?2 AND s.oldest_eligible_at IS NOT NULL AND (?5 IS NULL OR s.group_key=?5) \
         AND EXISTS (SELECT 1 FROM fireweed_items e WHERE e.tenant_id=?1 AND e.queue_id=?2 \
           AND e.group_key=s.group_key AND e.lifecycle_state='Pending' AND e.superseded=0 \
           AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
           AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
             JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id AND ig.queue_id=e.queue_id \
             AND ig.item_id=e.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
             WHERE NOT EXISTS (SELECT 1 FROM json_each(e.metadata) actual \
               WHERE actual.key=wanted.key AND actual.value=wanted.value AND actual.type=wanted.type))) \
         ORDER BY s.rep_priority_sort,s.rep_created_at,s.rep_item_id,s.group_key LIMIT 1) \
         SELECT i.item_id FROM candidate c JOIN fireweed_items i ON i.tenant_id=?1 AND i.queue_id=?2 \
         AND i.group_key=c.group_key WHERE i.lifecycle_state='Pending' AND i.superseded=0 \
         AND i.cohort_size IS NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
         AND i.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
           JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
           AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id \
           AND ig.item_id=i.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
           WHERE NOT EXISTS (SELECT 1 FROM json_each(i.metadata) actual WHERE actual.key=wanted.key \
             AND actual.value=wanted.value AND actual.type=wanted.type)) \
         ORDER BY i.priority_sort,i.created_seq,i.item_id LIMIT ?4",
    ))?;
    let rows = st(statement.query_map(
        params![
            tenant,
            queue,
            ts_nanos(now),
            max_items as i64,
            required_group,
            metadata_filter
        ],
        |row| row.get::<_, String>(0),
    ))?;
    let mut selected = Vec::new();
    for row in rows {
        selected
            .push(ItemId::new(st(row)?).map_err(|error| EngineError::Storage(error.to_string()))?);
    }
    Ok(selected)
}

/// `whole_cohort` selection (API-001 G6, all-or-nothing): the oldest COMPLETE cohort whose members are ALL
/// currently eligible. A cohort (group_key with a declared `cohort_size`) is complete when its live
/// non-superseded member count equals `cohort_size`; it is claimable only when every member is also
/// pending+due (no member leased/terminal). The whole cohort leases together, or the cohort is skipped.
/// `BatchTooLarge` if the selected cohort exceeds `max_items`. Paused → empty.
#[derive(Debug, Clone)]
pub(crate) struct SelectedCohort {
    pub(crate) cohort_id: CohortId,
    pub(crate) item_ids: Vec<ItemId>,
}

pub(crate) fn select_whole_cohort(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
    compatibility: &ClaimCompatibility,
) -> EngineResult<Option<SelectedCohort>> {
    if queue_paused(conn, shard)? {
        return Ok(None);
    }
    let (t, q) = parts(shard);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let candidate: Option<(String, String, i64)> = st(conn.query_row(
        "SELECT c.group_key,c.cohort_id,c.cohort_size FROM fireweed_cohorts c \
         WHERE c.tenant_id=?1 AND c.queue_id=?2 AND c.state='complete' \
         AND (SELECT COUNT(*) FROM fireweed_items a WHERE a.tenant_id=?1 AND a.queue_id=?2 \
           AND a.group_key=c.group_key AND a.superseded=0 AND a.cohort_size IS NOT NULL \
           AND a.lifecycle_state NOT IN ('Complete','Failed'))=c.cohort_size \
         AND NOT EXISTS (SELECT 1 FROM fireweed_items i WHERE i.tenant_id=?1 AND i.queue_id=?2 \
           AND i.group_key=c.group_key AND i.superseded=0 AND i.cohort_size IS NOT NULL \
           AND i.lifecycle_state NOT IN ('Complete','Failed') AND NOT (i.lifecycle_state='Pending' \
             AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
               ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
               WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
             AND NOT EXISTS (SELECT 1 FROM json_each(?4) wanted WHERE NOT EXISTS \
               (SELECT 1 FROM json_each(i.metadata) actual WHERE actual.key=wanted.key \
                AND actual.value=wanted.value AND actual.type=wanted.type)))) \
         ORDER BY c.cohort_created_at,c.group_key LIMIT 1",
        params![t,q,ts_nanos(now),metadata_filter],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    ).optional())?;
    let Some((group, cohort_id, size)) = candidate else {
        return Ok(None);
    };
    let size = usize::try_from(size).map_err(|error| EngineError::Storage(error.to_string()))?;
    if size > max_items {
        return Err(EngineError::BatchTooLarge);
    }
    let group = GroupKey::new(group).map_err(|error| EngineError::Storage(error.to_string()))?;
    let eligible = cohort_eligible_items(conn, shard, &group, now, size, compatibility)?;
    Ok(Some(SelectedCohort {
        cohort_id: CohortId::new(cohort_id)
            .map_err(|error| EngineError::Storage(error.to_string()))?,
        item_ids: eligible.item_ids,
    }))
}

/// The live currently-eligible COHORT members of one group (`cohort_size IS NOT NULL`), in claim order,
/// capped at `limit`. Like [`group_eligible_items`] but restricted to cohort-declared members (F1).
pub(crate) fn cohort_eligible_items(
    conn: &Connection,
    shard: &QueueKey,
    group: &GroupKey,
    now: UtcTimestamp,
    limit: usize,
    compatibility: &ClaimCompatibility,
) -> EngineResult<GroupEligibility> {
    let (t, q) = parts(shard);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let mut stmt = st(conn.prepare(
            "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NOT NULL \
         AND (not_before IS NULL OR not_before<=?4) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=fireweed_items.tenant_id AND ig.queue_id=fireweed_items.queue_id \
             AND ig.item_id=fireweed_items.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
             WHERE NOT EXISTS (SELECT 1 FROM json_each(fireweed_items.metadata) actual \
               WHERE actual.key=wanted.key AND actual.value=wanted.value AND actual.type=wanted.type)) \
         ORDER BY priority_sort, created_seq LIMIT ?5",
        ))?;
    let mapped = st(stmt.query_map(
        params![
            t,
            q,
            group.as_str(),
            ts_nanos(now),
            limit as i64,
            metadata_filter
        ],
        |row| row.get::<_, String>(0),
    ))?;
    let mut out = Vec::new();
    for row in mapped {
        out.push(ItemId::new(st(row)?).map_err(|error| EngineError::Storage(error.to_string()))?);
    }
    Ok(GroupEligibility { item_ids: out })
}

/// Non-destructive eligible view (every pending non-superseded item in priority order; ignores
/// `not_before`/pause exactly like the in-memory `peek`).
pub(crate) fn peek_sql(
    conn: &Connection,
    shard: &QueueKey,
    limit: usize,
) -> EngineResult<Vec<ItemView>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT item_id, client_item_key, priority, item_version FROM fireweed_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
         AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
           ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
           WHERE ig.tenant_id=fireweed_items.tenant_id AND ig.queue_id=fireweed_items.queue_id \
           AND ig.item_id=fireweed_items.item_id) \
         ORDER BY priority_sort, created_seq LIMIT ?3",
    ))?;
    let rows = st(stmt.query_map(params![t, q, limit as i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)?,
        ))
    }))?;
    let mut out = Vec::new();
    for r in rows {
        let (id, key, priority, version) = st(r)?;
        out.push(ItemView {
            item_id: ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
            client_item_key: ClientItemKey::new(key)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            priority: parse_priority(priority)?,
            item_version: version as u64,
        });
    }
    Ok(out)
}

const PEEK_PAGE_AFTER_SQL: &str = "SELECT item_id, client_item_key, priority, item_version FROM fireweed_items \
     WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
       AND (priority_sort, created_seq, item_id) > (SELECT priority_sort, created_seq, item_id \
         FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3) \
       AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
         ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
         WHERE ig.tenant_id=fireweed_items.tenant_id AND ig.queue_id=fireweed_items.queue_id \
         AND ig.item_id=fireweed_items.item_id) \
     ORDER BY priority_sort, created_seq, item_id LIMIT ?4";

const PEEK_PAGE_FIRST_SQL: &str = "SELECT item_id, client_item_key, priority, item_version FROM fireweed_items \
     WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
     AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
       ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
       WHERE ig.tenant_id=fireweed_items.tenant_id AND ig.queue_id=fireweed_items.queue_id \
       AND ig.item_id=fireweed_items.item_id) \
     ORDER BY priority_sort, created_seq, item_id LIMIT ?4";

pub(crate) fn peek_page_sql(
    conn: &Connection,
    shard: &QueueKey,
    after: Option<ItemId>,
    limit: usize,
) -> EngineResult<Vec<ItemView>> {
    let (tenant, queue) = parts(shard);
    let sql = if after.is_some() {
        PEEK_PAGE_AFTER_SQL
    } else {
        PEEK_PAGE_FIRST_SQL
    };
    let mut stmt = st(conn.prepare_cached(sql))?;
    let after = after.map(|item| item.to_string());
    let rows = st(
        stmt.query_map(params![tenant, queue, after, limit as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        }),
    )?;
    let mut out = Vec::with_capacity(limit);
    for row in rows {
        let (id, key, priority, version) = st(row)?;
        out.push(ItemView {
            item_id: ItemId::new(id).map_err(|error| EngineError::Storage(error.to_string()))?,
            client_item_key: ClientItemKey::new(key)
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            priority: parse_priority(priority)?,
            item_version: version as u64,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod peek_page_tests {
    use super::*;

    fn setup() -> (Connection, QueueKey, Vec<ItemId>) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(fireweed_relational::RELATIONAL_SCHEMA)
            .unwrap();
        let shard = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        );
        let (tenant, queue) = parts(&shard);
        let mut expected = Vec::new();
        for index in 0..9_u64 {
            let item_id = ItemId::from_u64(index + 1);
            conn.execute(
                "INSERT INTO fireweed_items (tenant_id,queue_id,item_id,client_item_key,\
                 lifecycle_state,priority_sort,item_version,last_command_sequence,created_at,\
                 updated_at,max_attempts,created_seq) \
                 VALUES (?1,?2,?3,?4,'Pending',?5,1,?6,0,0,1,?6)",
                params![
                    tenant,
                    queue,
                    item_id.to_string(),
                    format!("key-{index}"),
                    vec![(index / 3) as u8],
                    index as i64,
                ],
            )
            .unwrap();
            expected.push(item_id);
        }
        (conn, shard, expected)
    }

    fn plan(conn: &Connection, sql: &str, shard: &QueueKey, after: Option<ItemId>) -> Vec<String> {
        let (tenant, queue) = parts(shard);
        conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map(
                params![tenant, queue, after.map(|item| item.to_string()), 3_i64,],
                |row| row.get(3),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn assert_pending_order_plan(details: &[String]) {
        let plan = details.join("\n");
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("USING INDEX fireweed_items_pending_order_idx")),
            "pending-order index absent from plan:\n{plan}"
        );
        assert!(
            details
                .iter()
                .all(|detail| !detail.contains("USE TEMP B-TREE FOR ORDER BY")),
            "ORDER BY spilled to a temporary B-tree:\n{plan}"
        );
    }

    #[test]
    fn peek_pages_use_pending_order_index_without_temp_sort() {
        let (conn, shard, expected) = setup();
        let first = plan(&conn, PEEK_PAGE_FIRST_SQL, &shard, None);
        let after = plan(&conn, PEEK_PAGE_AFTER_SQL, &shard, Some(expected[2]));
        eprintln!("first-page plan:\n{}", first.join("\n"));
        eprintln!("after-cursor plan:\n{}", after.join("\n"));
        assert_pending_order_plan(&first);
        assert_pending_order_plan(&after);
    }

    #[test]
    fn peek_keyset_pagination_returns_every_pending_item_once() {
        let (conn, shard, expected) = setup();
        let mut cursor = None;
        let mut actual = Vec::new();
        loop {
            let page = peek_page_sql(&conn, &shard, cursor, 3).unwrap();
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|item| item.item_id);
            actual.extend(page.into_iter().map(|item| item.item_id));
        }
        assert_eq!(actual, expected);
    }
}

/// B-011 exact active-scope discovery. Keyed and ungrouped scopes are aggregated from the same live
/// `fireweed_items` source with the claim eligibility predicate (pending, not superseded, due at `now`, and
/// not blocked by a current gate). This makes a time-only `not_before` crossing visible without a write and
/// represents all ungrouped items as one `group_key=None` scope. The partial
/// `fireweed_items_active_scope_idx` bounds the source scan to one queue's pending, non-superseded rows;
/// gate anti-joins use the membership/state primary keys. The cost is O(live pending rows in the addressed
/// queue), rather than the old O(stored keyed summaries), in exchange for exact read-time eligibility.
///
/// `progress_bound_risk_count` is reported as `None` ("no signal"), NOT `Some(0)`: the summary's
/// `at_risk_count` is a hardcoded `0` placeholder while the progress-guard/at-risk derivation is deferred
/// (see `refresh_group_summaries`), and the [`ActiveScope`] contract reserves `None` for an uncomputed
/// signal vs `Some(0)` for a measured zero. When at-risk becomes live, map it to `Some` here.
///
/// PAUSE (intentional divergence from the claim path): discovery reports a scope's INTRINSIC eligibility
/// and does NOT short-circuit on `queue_paused` (unlike `select_eligible_sql`/group selection). An operator
/// hunting starvation wants to see work that has built up *because* a queue is paused; the summary itself
/// is pause-agnostic, so discovery mirrors it. (A read of a queue that does not exist yields an empty list,
/// not `NotFound` — a discovery read of an unknown queue simply has no active scopes.)
///
pub(crate) fn discover_active_scopes_sql(
    conn: &Connection,
    shard: &QueueKey,
    granularity: DiscoveryGranularity,
    now: UtcTimestamp,
) -> EngineResult<Vec<ActiveScope>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut stmt = st(conn.prepare(
        "SELECT i.group_key, MIN(i.eligible_since) AS oldest_eligible_at, COUNT(*) \
         FROM fireweed_items i \
         WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.lifecycle_state='Pending' \
         AND i.superseded=0 AND i.eligible_since IS NOT NULL \
         AND (i.not_before IS NULL OR i.not_before<=?3) \
         AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
           ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
           WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
         GROUP BY i.group_key \
         ORDER BY oldest_eligible_at ASC, i.group_key IS NOT NULL ASC, i.group_key ASC",
    ))?;
    let rows = st(stmt.query_map(params![t, q, now_n], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    }))?;
    let mut source = Vec::new();
    for r in rows {
        let (group_key, oldest_eligible_at, eligible) = st(r)?;
        // Age from `now`; a summary timestamp in the future (clock skew) clamps to 0, never underflows.
        let age_ms = now_n.saturating_sub(oldest_eligible_at).max(0) as u64 / 1_000_000;
        source.push(ActiveScope {
            queue_id: q.clone(),
            group_key,
            oldest_eligible_age_ms: age_ms,
            eligible_count: Some(eligible as u64),
            // Deferred at-risk derivation → no signal (not a measured zero). See the doc above.
            progress_bound_risk_count: None,
        });
    }
    Ok(project_scopes(source, granularity))
}

/// In-flight (leased) items. The lease token comes from the ephemeral live-token map (the durable table
/// keeps only the hash); a leased item whose token was lost to a reopen is omitted.
pub(crate) fn pending_sql(
    conn: &Connection,
    live_tokens: &HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    shard: &QueueKey,
) -> EngineResult<Vec<LeaseView>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT item_id, lease_expires_at, retry_count FROM fireweed_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased'",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<i64>>(1)?,
            row.get::<_, i64>(2)?,
        ))
    }))?;
    let mut out = Vec::new();
    for r in rows {
        let (id, exp, retry) = st(r)?;
        let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
        let (Some(token), Some(exp)) = (
            live_tokens
                .get(shard)
                .and_then(|tokens| tokens.get(&item_id)),
            exp,
        ) else {
            continue;
        };
        out.push(LeaseView {
            item_id,
            lease_token: token.clone(),
            lease_expires_at: nanos_ts(exp),
            attempt_count: retry as u32,
        });
    }
    Ok(out)
}

pub(crate) fn pending_summary_sql(
    live_tokens: &HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    by_consumer: &HashMap<QueueKey, HashMap<LeaseToken, BTreeSet<ItemId>>>,
    shard: &QueueKey,
) -> PendingSummary {
    let tokens = live_tokens.get(shard);
    let mut consumers = by_consumer
        .get(shard)
        .into_iter()
        .flat_map(|consumers| consumers.iter())
        .map(|(token, ids)| (token.clone(), ids.len() as u64))
        .collect::<Vec<_>>();
    consumers.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
    PendingSummary {
        count: tokens.map_or(0, |tokens| tokens.len() as u64),
        min_id: tokens.and_then(|tokens| tokens.first_key_value().map(|(id, _)| *id)),
        max_id: tokens.and_then(|tokens| tokens.last_key_value().map(|(id, _)| *id)),
        consumers,
    }
}

/// One indexed, batched lookup for the request's ids (chunked only at SQLite's
/// bind-variable ceiling). The result preserves request order and never scans
/// or materializes unrelated PEL rows.
pub(crate) fn pending_by_ids_sql(
    conn: &Connection,
    live_tokens: &HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Vec<LeaseView>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let id_strings: Vec<_> = ids.iter().map(ToString::to_string).collect();
    let mut rows = HashMap::<String, (i64, i64)>::with_capacity(ids.len());
    for chunk in id_strings.chunks(SQLITE_BATCH) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id,lease_expires_at,retry_count FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND lifecycle_state='Leased' \
             AND lease_expires_at IS NOT NULL AND item_id IN ({placeholders})"
        );
        let mut values = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        values.extend(chunk.iter().cloned().map(Value::Text));
        let mut statement = st(conn.prepare(&sql))?;
        let mapped = st(statement.query_map(params_from_iter(values.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, i64>(1)?, row.get::<_, i64>(2)?),
            ))
        }))?;
        for row in mapped {
            let (id, data) = st(row)?;
            rows.insert(id, data);
        }
    }
    let tokens = live_tokens.get(shard);
    let mut out = Vec::with_capacity(ids.len());
    for (id, id_string) in ids.iter().zip(id_strings) {
        let (Some(token), Some((expires, attempts))) = (
            tokens.and_then(|tokens| tokens.get(id)),
            rows.get(&id_string),
        ) else {
            continue;
        };
        out.push(LeaseView {
            item_id: *id,
            lease_token: token.clone(),
            lease_expires_at: nanos_ts(*expires),
            attempt_count: *attempts as u32,
        });
    }
    Ok(out)
}

pub(crate) fn pending_page_sql(
    conn: &Connection,
    live_tokens: &HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    shard: &QueueKey,
    start: Option<ItemId>,
    limit: usize,
) -> EngineResult<PendingPage> {
    use std::ops::Bound::{Included, Unbounded};
    let Some(tokens) = live_tokens.get(shard) else {
        return Ok(PendingPage::default());
    };
    let ids: Vec<_> = tokens
        .range((start.map_or(Unbounded, Included), Unbounded))
        .map(|(id, _)| *id)
        .take(limit.saturating_add(1))
        .collect();
    let next = ids.get(limit).copied();
    let entries = pending_by_ids_sql(conn, live_tokens, shard, &ids[..ids.len().min(limit)])?;
    Ok(PendingPage { entries, next })
}

pub(crate) struct PendingRange<'a> {
    pub(crate) start: Option<ItemId>,
    pub(crate) end: Option<ItemId>,
    pub(crate) consumer: Option<&'a LeaseToken>,
    pub(crate) limit: usize,
}

pub(crate) fn pending_range_sql(
    conn: &Connection,
    live_tokens: &HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    by_consumer: &HashMap<QueueKey, HashMap<LeaseToken, BTreeSet<ItemId>>>,
    shard: &QueueKey,
    query: PendingRange<'_>,
) -> EngineResult<Vec<LeaseView>> {
    use std::ops::Bound::{Included, Unbounded};
    let bounds = (
        query.start.map_or(Unbounded, Included),
        query.end.map_or(Unbounded, Included),
    );
    let ids: Vec<_> = if let Some(consumer) = query.consumer {
        by_consumer
            .get(shard)
            .and_then(|consumers| consumers.get(consumer))
            .into_iter()
            .flat_map(|ids| ids.range(bounds))
            .copied()
            .take(query.limit)
            .collect()
    } else {
        live_tokens
            .get(shard)
            .into_iter()
            .flat_map(|tokens| tokens.range(bounds))
            .map(|(id, _)| *id)
            .take(query.limit)
            .collect()
    };
    pending_by_ids_sql(conn, live_tokens, shard, &ids)
}

/// Render the rich claimed-item shape for specific leased `ids` (the claim/XCLAIM reply). The lease token
/// for each id is supplied by `resolve` — the just-claimed token when rendering inside the claim txn, or
/// the live-token map for the `claimed_view` read port. Ids absent / not leased / with no resolvable token
/// are omitted (the caller knows the set it just acted on).
pub(crate) fn render_claimed(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
    resolve: impl Fn(&ItemId) -> Option<LeaseToken>,
) -> EngineResult<Vec<ClaimedItem>> {
    let (t, q) = parts(shard);
    type ClaimedRow = (
        String,
        i64,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        i64,
        Option<Vec<u8>>,
        String,
        String,
    );
    let mut requested = Vec::new();
    let mut id_strs = Vec::new();
    for id in ids {
        if let Some(token) = resolve(id) {
            requested.push((*id, token));
            id_strs.push(id.to_string());
        }
    }
    if requested.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows: HashMap<String, ClaimedRow> = HashMap::with_capacity(requested.len());
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id, client_item_key, item_version, priority, group_key, not_before, \
             lease_expires_at, retry_count, payload, fields, metadata FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND lifecycle_state='Leased' AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let mut stmt = st(conn.prepare(&sql))?;
        let mapped = st(stmt.query_map(params_from_iter(p.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                (
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<Vec<u8>>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                ),
            ))
        }))?;
        for row in mapped {
            let (item_id, data) = st(row)?;
            rows.insert(item_id, data);
        }
    }
    let gate_keys = item_gate_keys_for_ids(conn, shard, &id_strs)?;
    let mut out = Vec::new();
    for (id, token) in requested {
        let id_str = id.to_string();
        let Some((
            key,
            version,
            priority,
            group,
            not_before,
            exp,
            retry,
            payload,
            fields,
            metadata,
        )) = rows.get(&id_str).cloned()
        else {
            continue;
        };
        let Some(exp) = exp else { continue };
        let gate_keys = gate_keys.get(&id_str).cloned().unwrap_or_default();
        out.push(ClaimedItem {
            item_id: id,
            client_item_key: ClientItemKey::new(key)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            item_version: version as u64,
            priority: parse_priority(priority)?,
            group_key: group
                .map(GroupKey::new)
                .transpose()
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            not_before: not_before.map(nanos_ts),
            lease_token: Some(token),
            lease_expires_at: nanos_ts(exp),
            attempt_count: retry as u32,
            payload: payload.map(Bytes::from),
            fields: fields_from_json(fields)?,
            metadata: metadata_from_json(metadata)?,
            gate_keys,
        });
    }
    Ok(out)
}

pub(crate) fn item_gate_keys_for_ids(
    conn: &Connection,
    shard: &QueueKey,
    id_strs: &[String],
) -> EngineResult<HashMap<String, Vec<String>>> {
    let (t, q) = parts(shard);
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id, gate_key FROM fireweed_item_gates \
             WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph}) \
             ORDER BY item_id, gate_key"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let mut stmt = st(conn.prepare(&sql))?;
        let rows = st(stmt.query_map(params_from_iter(p.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }))?;
        for row in rows {
            let (item_id, gate_key) = st(row)?;
            out.entry(item_id).or_default().push(gate_key);
        }
    }
    Ok(out)
}

pub(crate) fn item_gate_key_map(
    conn: &Connection,
    shard: &QueueKey,
) -> EngineResult<HashMap<ItemId, Vec<String>>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT item_id,gate_key FROM fireweed_item_gates \
         WHERE tenant_id=?1 AND queue_id=?2 \
         ORDER BY item_id,gate_key",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    }))?;
    let mut out: HashMap<ItemId, Vec<String>> = HashMap::new();
    for row in rows {
        let (item_id, gate_key) = st(row)?;
        let item_id = ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?;
        out.entry(item_id).or_default().push(gate_key);
    }
    Ok(out)
}

pub(crate) fn apply_whole_cohort_response_shape(items: &mut [ClaimedItem]) -> Option<GroupKey> {
    let cohort_id = items.first().and_then(|item| item.group_key.clone());
    for item in items {
        item.lease_token = None;
    }
    cohort_id
}

pub(crate) fn live_items_sql(
    conn: &Connection,
    shard: &QueueKey,
    keys: &[ClientItemKey],
) -> EngineResult<Vec<Option<LiveItemView>>> {
    let (t, q) = parts(shard);
    let mut found = HashMap::<String, LiveItemView>::with_capacity(keys.len());
    for chunk in keys.chunks(SQLITE_BATCH) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        // prepare_cached: chunk lengths are stable under SQLITE_BATCH, so 10M-item
        // recovery verification reuses one statement plan instead of recompiling
        // tens of thousands of identical IN-list queries.
        let sql = format!(
            "SELECT client_item_key, item_id, item_version, lifecycle_state, priority, group_key, \
             not_before, retry_count, payload, fields FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND client_item_key IN ({placeholders}) \
               AND superseded=0 AND lifecycle_state IN ('Pending','Leased')"
        );
        let mut parameters = Vec::with_capacity(chunk.len() + 2);
        parameters.extend([Value::Text(t.to_string()), Value::Text(q.to_string())]);
        parameters.extend(
            chunk
                .iter()
                .map(|key| Value::Text(key.as_str().to_string())),
        );
        let mut statement = st(conn.prepare_cached(&sql))?;
        let rows = st(
            statement.query_map(params_from_iter(parameters.iter()), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<Vec<u8>>>(8)?,
                    row.get::<_, String>(9)?,
                ))
            }),
        )?;
        for row in rows {
            let (key, id, version, state, priority, group, not_before, retry, payload, fields) =
                st(row)?;
            found.insert(
                key.clone(),
                LiveItemView {
                    item_id: ItemId::new(id)
                        .map_err(|error| EngineError::Storage(error.to_string()))?,
                    client_item_key: ClientItemKey::new(key.clone())
                        .map_err(|error| EngineError::Storage(error.to_string()))?,
                    item_version: version as u64,
                    lifecycle_state: parse_state(&state)?,
                    priority: parse_priority(priority)?,
                    group_key: group
                        .map(GroupKey::new)
                        .transpose()
                        .map_err(|error| EngineError::Storage(error.to_string()))?,
                    not_before: not_before.map(nanos_ts),
                    attempt_count: retry as u32,
                    payload: payload.map(Bytes::from),
                    fields: fields_from_json(fields)?,
                },
            );
        }
    }
    Ok(keys
        .iter()
        .map(|key| found.get(key.as_str()).cloned())
        .collect())
}

pub(crate) fn metrics_sql(conn: &Connection, shard: &QueueKey) -> EngineResult<QueueMetrics> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT lifecycle_state, COUNT(*) FROM fireweed_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND superseded=0 GROUP BY lifecycle_state",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }))?;
    let mut m = QueueMetrics::default();
    for r in rows {
        let (state, count) = st(r)?;
        let count = count as u64;
        match parse_state(&state)? {
            ItemState::Pending => m.pending = count,
            ItemState::Leased => m.leased = count,
            ItemState::Complete => m.complete = count,
            ItemState::Failed => m.failed = count,
        }
    }
    m.resident_terminal_count = m.complete + m.failed;
    Ok(m)
}

/// Lifecycle state + flags for a BATCH of items in ONE round-trip per ≤256-id chunk (was one SELECT per
/// id), keyed by `item_id` string. Absent ids are simply missing from the map (the per-id classifier
/// treats a miss as `NotFound`). Replaces the former per-item `item_flags` helper.
pub(crate) fn item_flags_map(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<HashMap<String, (ItemState, bool, bool, bool)>> {
    let (t, q) = parts(shard);
    let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
    let mut map = HashMap::with_capacity(ids.len());
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id, lifecycle_state, fenced, superseded, cohort_size IS NOT NULL FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let mut stmt = st(conn.prepare(&sql))?;
        let mapped = st(stmt.query_map(params_from_iter(p.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        }))?;
        for r in mapped {
            let (id, s, fenced, superseded, cohort_member) = st(r)?;
            map.insert(
                id,
                (
                    parse_state(&s)?,
                    fenced != 0,
                    superseded != 0,
                    cohort_member != 0,
                ),
            );
        }
    }
    Ok(map)
}

/// Shared "present + Leased + not fenced + not superseded + not terminal" check — identical error
/// precedence to `ProjectionData::validate_leased` (finalize/renew/reassign pre-commit). Classifies every
/// id from ONE batched read; precedence is still evaluated per id in request order (first failing id wins),
/// byte-for-byte as the former per-id SELECT loop did.
pub(crate) fn validate_leased(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<()> {
    let flags = item_flags_map(conn, shard, ids)?;
    for id in ids {
        match flags.get(&id.to_string()) {
            None => return Err(EngineError::NotFound),
            Some((_, true, _, _)) => return Err(EngineError::StaleLease),
            Some((s, _, _, _)) if s.is_terminal() => return Err(EngineError::Terminal),
            Some((_, _, true, _)) => return Err(EngineError::Superseded),
            Some((_, _, _, true)) => {
                return Err(EngineError::Invalid("cohort member requires cohort lease"));
            }
            Some((s, _, _, _)) if *s != ItemState::Leased => {
                return Err(EngineError::Invalid("item is not leased"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

pub(crate) fn validate_cohort_lease(
    conn: &Connection,
    shard: &QueueKey,
    target: &CohortLeaseTarget,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let row: Option<(String, Option<Vec<u8>>)> = st(conn
        .query_row(
            "SELECT state, cohort_lease_token_hash FROM fireweed_cohorts \
             WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
            params![t, q, target.cohort_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional())?;
    let Some((state, hash)) = row else {
        return Err(EngineError::NotFound);
    };
    if state == "terminal" {
        return Err(EngineError::Terminal);
    }
    if state != "leased" {
        return Err(EngineError::Invalid("cohort is not leased"));
    }
    if hash.as_deref() != Some(lease_hash(&target.cohort_lease_token).as_slice()) {
        return Err(EngineError::StaleLease);
    }
    Ok(())
}

/// Active (non-superseded) item id under `client_item_key`, or `None`. The generic upsert's look-then-replace
/// read; the partial unique index keeps this single-valued.
pub(crate) fn lookup_active_by_key(
    conn: &Connection,
    shard: &QueueKey,
    client_item_key: &ClientItemKey,
) -> EngineResult<Option<ItemId>> {
    let (t, q) = parts(shard);
    let id: Option<String> = st(conn
        .query_row(
            "SELECT item_id FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 AND superseded=0",
            params![t, q, client_item_key.as_str()],
            |row| row.get(0),
        )
        .optional())?;
    id.map(|s| ItemId::new(s).map_err(|e| EngineError::Storage(e.to_string())))
        .transpose()
}

/// Batch form of [`lookup_active_by_key`]: one (or few chunked) SQL round-trips for a whole keyed
/// XADD pipeline. Returns `client_item_key → (item_id, lifecycle_state)` for every live hit.
/// Keys with no live row are omitted. Chunks at 400 to stay under SQLite's variable limit.
pub(crate) fn lookup_active_by_keys(
    conn: &Connection,
    shard: &QueueKey,
    keys: &[ClientItemKey],
) -> EngineResult<HashMap<String, (ItemId, ItemState)>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let (t, q) = parts(shard);
    let mut out = HashMap::with_capacity(keys.len().min(1024));
    // Leave headroom under SQLITE_MAX_VARIABLE_NUMBER (default 999): 2 fixed + N keys.
    const CHUNK: usize = 400;
    for chunk in keys.chunks(CHUNK) {
        let mut sql = String::from(
            "SELECT client_item_key, item_id, lifecycle_state FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND superseded=0 AND client_item_key IN (",
        );
        for i in 0..chunk.len() {
            if i > 0 {
                sql.push(',');
            }
            // ?3, ?4, ...
            sql.push('?');
            sql.push_str(&(i + 3).to_string());
        }
        sql.push(')');
        let mut stmt = st(conn.prepare(&sql))?;
        let mut bind: Vec<Value> = Vec::with_capacity(2 + chunk.len());
        bind.push(Value::Text(t.clone()));
        bind.push(Value::Text(q.clone()));
        for key in chunk {
            bind.push(Value::Text(key.as_str().to_owned()));
        }
        let rows = st(stmt.query_map(params_from_iter(bind), |row| {
            let key: String = row.get(0)?;
            let id: String = row.get(1)?;
            let state: String = row.get(2)?;
            Ok((key, id, state))
        }))?;
        for row in rows {
            let (key, id, state) = st(row)?;
            let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
            let item_state = parse_state(&state)?;
            out.insert(key, (item_id, item_state));
        }
    }
    Ok(out)
}

/// Lifecycle state of `id` (any superseded/terminal flavor), or `None` if absent.
pub(crate) fn item_state_sql(
    conn: &Connection,
    shard: &QueueKey,
    id: &ItemId,
) -> EngineResult<Option<ItemState>> {
    Ok(item_flags_map(conn, shard, std::slice::from_ref(id))?
        .get(&id.to_string())
        .map(|(s, _, _, _)| *s))
}

pub(crate) fn leased_id_count_sql(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<usize> {
    let (t, q) = parts(shard);
    let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    let mut total = 0usize;
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT COUNT(*) FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND lifecycle_state='Leased' AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let count: i64 = st(conn.query_row(&sql, params_from_iter(p.iter()), |row| row.get(0)))?;
        total += count as usize;
    }
    Ok(total)
}

/// Committed `item_version` of `id`, or `None` if absent.
pub(crate) fn item_version_sql(
    conn: &Connection,
    shard: &QueueKey,
    id: &ItemId,
) -> EngineResult<Option<u64>> {
    let (t, q) = parts(shard);
    let v: Option<i64> = st(conn
        .query_row(
            "SELECT item_version FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            params![t, q, id.to_string()],
            |row| row.get(0),
        )
        .optional())?;
    Ok(v.map(|v| v as u64))
}

/// This queue's leases expired strictly before `now` (half-open), ordered by item id (the generic
/// `reclaim_expired` truncates to its `limit`). Mirrors the monolith's per-queue reclaim selection.
pub(crate) fn expired_leases_sql(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut stmt = st(conn.prepare(
        "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
         AND lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
         AND lease_expires_at<?3 ORDER BY item_id",
    ))?;
    let rows = st(stmt.query_map(params![t, q, now_n], |row| row.get::<_, String>(0)))?;
    let mut ids = Vec::new();
    for r in rows {
        ids.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(ids)
}

/// Every queue's expired leases at `now` (the global tick sweep), grouped per queue. Mirrors the monolith's
/// `ReclaimDriver::tick` selection (queues with none are omitted).
pub(crate) fn all_expired_leases_sql(
    conn: &Connection,
    now: UtcTimestamp,
) -> EngineResult<Vec<(QueueKey, Vec<ItemId>)>> {
    let now_n = ts_nanos(now);
    let mut stmt = st(conn.prepare(
        "SELECT tenant_id, queue_id, item_id FROM fireweed_items \
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
    Ok(by_queue)
}

/// One raw-row-bounded keyset page for the composed background reclaim driver. Partition filtering occurs
/// after the bounded SQL page, while `next` follows the last raw row, so foreign-only pages make honest
/// bounded progress without skipping rows owned by another fixed worker.
pub(crate) fn expired_leases_page_sql(
    conn: &Connection,
    now: UtcTimestamp,
    cursor: Option<&fireweed_engine::ExpiredLeaseCursor>,
    limit: usize,
    worker_partition: Option<(usize, usize)>,
) -> EngineResult<fireweed_engine::ExpiredLeasePage> {
    if limit == 0 {
        return Err(EngineError::Invalid(
            "expired lease page limit must be nonzero",
        ));
    }
    let (has_cursor, after_expiry, after_tenant, after_queue, after_item) = match cursor {
        Some(cursor) => {
            let (expiry, tenant, queue, item) = cursor.row_parts()?;
            (1_i64, expiry, tenant, queue, item)
        }
        None => (0_i64, 0_i64, String::new(), String::new(), String::new()),
    };
    let mut statement = st(conn.prepare(
        "SELECT lease_expires_at,tenant_id,queue_id,item_id FROM fireweed_items \
         WHERE lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
         AND lease_expires_at<?1 AND (?2=0 OR \
           lease_expires_at>?3 OR \
           (lease_expires_at=?3 AND tenant_id>?4) OR \
           (lease_expires_at=?3 AND tenant_id=?4 AND queue_id>?5) OR \
           (lease_expires_at=?3 AND tenant_id=?4 AND queue_id=?5 AND item_id>?6)) \
         ORDER BY lease_expires_at,tenant_id,queue_id,item_id LIMIT ?7",
    ))?;
    let row_limit = i64::try_from(limit.saturating_add(1))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let mapped = st(statement.query_map(
        params![
            ts_nanos(now),
            has_cursor,
            after_expiry,
            after_tenant,
            after_queue,
            after_item,
            row_limit
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        },
    ))?;
    let mut rows = Vec::with_capacity(limit.saturating_add(1));
    for row in mapped {
        rows.push(st(row)?);
    }
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next = if has_more {
        let (expiry, tenant, queue, item) = rows.last().expect("nonzero bounded page");
        let queue = QueueKey::new(
            TenantId::new(tenant.clone())
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            QueueId::new(queue.clone()).map_err(|error| EngineError::Storage(error.to_string()))?,
        );
        let item =
            ItemId::new(item.clone()).map_err(|error| EngineError::Storage(error.to_string()))?;
        Some(fireweed_engine::ExpiredLeaseCursor::from_row(
            *expiry, &queue, &item,
        ))
    } else {
        None
    };
    let mut leases = Vec::<(QueueKey, Vec<ItemId>)>::new();
    for (_, tenant, queue, item) in rows {
        let queue = QueueKey::new(
            TenantId::new(tenant).map_err(|error| EngineError::Storage(error.to_string()))?,
            QueueId::new(queue).map_err(|error| EngineError::Storage(error.to_string()))?,
        );
        if worker_partition.is_some_and(|(index, partitions)| {
            fireweed_engine::queue_worker_partition(&queue, partitions) != index
        }) {
            continue;
        }
        let item = ItemId::new(item).map_err(|error| EngineError::Storage(error.to_string()))?;
        match leases.last_mut() {
            Some((last, ids)) if *last == queue => ids.push(item),
            _ => leases.push((queue, vec![item])),
        }
    }
    Ok(fireweed_engine::ExpiredLeasePage { leases, next })
}

/// In-place field/payload update pre-commit validation, with the exact error precedence the monolith's
/// `UpdateFieldsPort` enforces: absent → `NotFound`, fenced → `StaleLease`, terminal → `Terminal`,
/// superseded → `Superseded`, version mismatch → `Conflict`. Mutates nothing.
pub(crate) fn update_fields_validate_sql(
    conn: &Connection,
    shard: &QueueKey,
    id: &ItemId,
    expected_item_version: Option<u64>,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let row: Option<(String, i64, i64, i64)> = st(conn
        .query_row(
            "SELECT lifecycle_state, superseded, fenced, item_version FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            params![t, q, id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
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
    Ok(())
}

/// Durable instance/state fence for `key` (absent → `None`, read by the caller as the unset value `0`).
pub(crate) fn instance_fence_sql(
    conn: &Connection,
    shard: &QueueKey,
    key: &[u8],
) -> EngineResult<Option<u64>> {
    let (t, q) = parts(shard);
    let v: Option<i64> = st(conn
        .query_row(
            "SELECT fence FROM fireweed_instance_fences \
             WHERE tenant_id=?1 AND queue_id=?2 AND instance_key=?3",
            params![t, q, key],
            |row| row.get(0),
        )
        .optional())?;
    Ok(v.map(|v| v as u64))
}

/// Opaque non-work side record by key, or `None`.
pub(crate) fn side_record_sql(
    conn: &Connection,
    shard: &QueueKey,
    key: &[u8],
) -> EngineResult<Option<Bytes>> {
    let (t, q) = parts(shard);
    let payload: Option<Vec<u8>> = st(conn
        .query_row(
            "SELECT payload FROM fireweed_side_records \
             WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
            params![t, q, key],
            |row| row.get(0),
        )
        .optional())?;
    Ok(payload.map(Bytes::from))
}

#[cfg(test)]
mod reclaim_page_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static RECLAIM_SELECTS: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn batch_update_snapshot_resolves_ids_and_keys_in_one_query() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE fireweed_items (
                tenant_id TEXT NOT NULL,
                queue_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                client_item_key TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                item_version INTEGER NOT NULL,
                fenced INTEGER NOT NULL,
                superseded INTEGER NOT NULL
             );",
        )
        .unwrap();
        let shard = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        );
        let first = ItemId::from_u64(1);
        let second = ItemId::from_u64(2);
        conn.execute(
            "INSERT INTO fireweed_items VALUES (?1,?2,?3,'key-1','Pending',4,0,0)",
            params![
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                first.to_string()
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fireweed_items VALUES (?1,?2,?3,'key-2','Leased',7,1,0)",
            params![
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                second.to_string()
            ],
        )
        .unwrap();

        let mut rows = batch_update_snapshot_sql(
            &conn,
            &shard,
            &[
                BatchUpdateItemRef::ItemId(first),
                BatchUpdateItemRef::ClientItemKey(ClientItemKey::new("key-2").unwrap()),
            ],
        )
        .unwrap();
        rows.sort_by_key(|row| row.item_id);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].item_version, 4);
        assert_eq!(rows[0].state, ItemState::Pending);
        assert_eq!(rows[1].item_version, 7);
        assert_eq!(rows[1].state, ItemState::Leased);
        assert!(rows[1].fenced);
    }

    fn count_reclaim_select(statement: &str) {
        if statement
            .starts_with("SELECT lease_expires_at,tenant_id,queue_id,item_id FROM fireweed_items")
        {
            RECLAIM_SELECTS.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn expired_lease_pages_are_bounded_and_progress_past_foreign_first_pages() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE fireweed_items (
                lease_expires_at INTEGER,
                tenant_id TEXT NOT NULL,
                queue_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL
             );",
        )
        .unwrap();
        let tenant = TenantId::new("tenant").unwrap();
        let target_worker = 1;
        let width = 2;
        let mut foreign = 0usize;
        let mut owned = 0usize;
        let mut suffix = 0usize;
        while foreign < 300 || owned < 300 {
            let queue_id = QueueId::new(format!("queue-{suffix:06}")).unwrap();
            suffix += 1;
            let queue = QueueKey::new(tenant.clone(), queue_id.clone());
            let is_owned = fireweed_engine::queue_worker_partition(&queue, width) == target_worker;
            if (is_owned && owned == 300) || (!is_owned && foreign == 300) {
                continue;
            }
            let (expiry, item) = if is_owned {
                owned += 1;
                (2_i64, ItemId::from_u64(10_000 + owned as u64))
            } else {
                foreign += 1;
                (1_i64, ItemId::from_u64(foreign as u64))
            };
            conn.execute(
                "INSERT INTO fireweed_items VALUES (?1,?2,?3,?4,'Leased')",
                params![expiry, tenant.as_str(), queue_id.as_str(), item.to_string()],
            )
            .unwrap();
        }

        let mut cursor = None;
        let mut observed = Vec::new();
        let mut pages = 0usize;
        RECLAIM_SELECTS.store(0, Ordering::Relaxed);
        conn.trace(Some(count_reclaim_select));
        loop {
            let page = expired_leases_page_sql(
                &conn,
                UtcTimestamp::new(3, 0).unwrap(),
                cursor.as_ref(),
                128,
                Some((target_worker, width)),
            )
            .unwrap();
            pages += 1;
            if pages <= 2 {
                assert!(page.leases.is_empty(), "the first 256 raw rows are foreign");
            }
            observed.extend(page.leases.into_iter().flat_map(|(_, ids)| ids));
            let Some(next) = page.next else { break };
            cursor = Some(next);
        }
        conn.trace(None);
        observed.sort_unstable();
        let expected = (1..=300)
            .map(|index| ItemId::from_u64(10_000 + index))
            .collect::<Vec<_>>();
        assert_eq!(observed, expected);
        assert_eq!(pages, 5, "600 raw rows at 128 rows/page");
        assert_eq!(
            RECLAIM_SELECTS.load(Ordering::Relaxed),
            pages,
            "one bounded SELECT per worker page"
        );
        assert!(
            fireweed_relational::RELATIONAL_SCHEMA
                .contains("fireweed_items_global_expired_lease_idx")
        );
    }
}
