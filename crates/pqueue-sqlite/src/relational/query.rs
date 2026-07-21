use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, CohortId, GroupKey, ItemId, ItemState, LeaseToken, Metadata, QueueId, TenantId,
    UtcTimestamp,
};
use pqueue_engine::{
    ActiveScope, ClaimCompatibility, ClaimedItem, CohortLeaseTarget, DiscoveryGranularity,
    EngineError, EngineResult, ItemView, LeaseView, LiveItemView, PendingPage, PendingSummary,
    QueueKey, QueueMetrics, project_scopes,
};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

use super::*;

// ---------------------------------------------------------------------------
// read queries (SQL over pqueue_items)
// ---------------------------------------------------------------------------

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
            "SELECT 1 FROM pqueue_gate_state WHERE tenant_id=?1 AND queue_id=?2 LIMIT 1",
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
        "SELECT item_id FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
             AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
             AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
             JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=pqueue_items.tenant_id \
             AND ig.queue_id=pqueue_items.queue_id AND ig.item_id=pqueue_items.item_id) \
             AND (?5 IS NULL OR group_key=?5) \
             AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
               WHERE NOT EXISTS (SELECT 1 FROM json_each(pqueue_items.metadata) actual \
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
                "SELECT item_id FROM pqueue_items NOT INDEXED WHERE tenant_id=?1 AND queue_id=?2 \
                 AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
                 AND (not_before IS NULL OR not_before<=?3) \
                 AND eligible_since IS NOT NULL AND rowid>=?5 \
                 ORDER BY rowid LIMIT ?4",
                rowid_floor,
            )
        } else {
            (
                "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
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
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
         AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
         AND (not_before IS NULL OR not_before<=?3) \
         AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
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
// `pqueue_group_summary`. The queue has one owner, so every group is owner-local (ADR-008); the sqlite
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
         FROM pqueue_items i \
         LEFT JOIN pqueue_group_summary gs \
           ON gs.tenant_id=i.tenant_id AND gs.queue_id=i.queue_id AND gs.group_key=i.group_key \
         WHERE i.tenant_id=?1 AND i.queue_id=?2 \
           AND i.lifecycle_state='Pending' AND i.superseded=0 AND i.group_key IS NOT NULL \
           AND i.eligible_since IS NOT NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
           AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gstate \
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
           FROM pqueue_group_summary s JOIN pqueue_items e ON e.tenant_id=?1 AND e.queue_id=?2 \
             AND e.group_key=s.group_key WHERE s.tenant_id=?1 AND s.queue_id=?2 \
           AND s.oldest_eligible_at IS NOT NULL AND e.lifecycle_state='Pending' AND e.superseded=0 \
           AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
           AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
             JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id \
             AND ig.queue_id=e.queue_id AND ig.item_id=e.item_id) \
           AND NOT EXISTS (SELECT 1 FROM json_each(?5) wanted WHERE NOT EXISTS \
             (SELECT 1 FROM json_each(e.metadata) actual WHERE actual.key=wanted.key \
              AND actual.value=wanted.value AND actual.type=wanted.type)) \
           AND NOT EXISTS (SELECT 1 FROM pqueue_items leased WHERE leased.tenant_id=?1 \
             AND leased.queue_id=?2 AND leased.group_key=s.group_key AND leased.superseded=0 \
             AND leased.cohort_size IS NULL AND leased.lifecycle_state='Leased')), \
         candidate AS MATERIALIZED (SELECT group_key,rep_priority_sort,rep_created_at,rep_item_id \
           FROM candidate_raw WHERE rn=1 ORDER BY rep_priority_sort,created_seq,rep_item_id,group_key LIMIT ?4), \
         eligible AS MATERIALIZED (SELECT c.group_key,c.rep_priority_sort,c.rep_created_at,\
           c.rep_item_id,i.item_id,i.priority_sort,i.created_seq FROM candidate c \
           JOIN pqueue_items i ON i.tenant_id=?1 AND i.queue_id=?2 AND i.group_key=c.group_key \
           WHERE i.lifecycle_state='Pending' AND i.superseded=0 AND i.cohort_size IS NULL \
             AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
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
        "WITH candidate AS (SELECT s.group_key FROM pqueue_group_summary s WHERE s.tenant_id=?1 \
         AND s.queue_id=?2 AND s.oldest_eligible_at IS NOT NULL AND (?5 IS NULL OR s.group_key=?5) \
         AND EXISTS (SELECT 1 FROM pqueue_items e WHERE e.tenant_id=?1 AND e.queue_id=?2 \
           AND e.group_key=s.group_key AND e.lifecycle_state='Pending' AND e.superseded=0 \
           AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
           AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
             JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id AND ig.queue_id=e.queue_id \
             AND ig.item_id=e.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
             WHERE NOT EXISTS (SELECT 1 FROM json_each(e.metadata) actual \
               WHERE actual.key=wanted.key AND actual.value=wanted.value AND actual.type=wanted.type))) \
         ORDER BY s.rep_priority_sort,s.rep_created_at,s.rep_item_id,s.group_key LIMIT 1) \
         SELECT i.item_id FROM candidate c JOIN pqueue_items i ON i.tenant_id=?1 AND i.queue_id=?2 \
         AND i.group_key=c.group_key WHERE i.lifecycle_state='Pending' AND i.superseded=0 \
         AND i.cohort_size IS NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
         AND i.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
           JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
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
        "SELECT c.group_key,c.cohort_id,c.cohort_size FROM pqueue_cohorts c \
         WHERE c.tenant_id=?1 AND c.queue_id=?2 AND c.state='complete' \
         AND (SELECT COUNT(*) FROM pqueue_items a WHERE a.tenant_id=?1 AND a.queue_id=?2 \
           AND a.group_key=c.group_key AND a.superseded=0 AND a.cohort_size IS NOT NULL \
           AND a.lifecycle_state NOT IN ('Complete','Failed'))=c.cohort_size \
         AND NOT EXISTS (SELECT 1 FROM pqueue_items i WHERE i.tenant_id=?1 AND i.queue_id=?2 \
           AND i.group_key=c.group_key AND i.superseded=0 AND i.cohort_size IS NOT NULL \
           AND i.lifecycle_state NOT IN ('Complete','Failed') AND NOT (i.lifecycle_state='Pending' \
             AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
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
            "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NOT NULL \
         AND (not_before IS NULL OR not_before<=?4) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
             WHERE NOT EXISTS (SELECT 1 FROM json_each(pqueue_items.metadata) actual \
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
        "SELECT item_id, client_item_key, priority, item_version FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
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

pub(crate) fn peek_page_sql(
    conn: &Connection,
    shard: &QueueKey,
    after: Option<ItemId>,
    limit: usize,
) -> EngineResult<Vec<ItemView>> {
    let (tenant, queue) = parts(shard);
    let sql = if after.is_some() {
        "SELECT item_id, client_item_key, priority, item_version FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
           AND (priority_sort, created_seq, item_id) > (SELECT priority_sort, created_seq, item_id \
             FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3) \
         ORDER BY priority_sort, created_seq, item_id LIMIT ?4"
    } else {
        "SELECT item_id, client_item_key, priority, item_version FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
         ORDER BY priority_sort, created_seq, item_id LIMIT ?4"
    };
    let mut stmt = st(conn.prepare(sql))?;
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

/// BQ-14e active-scope discovery: roll up `pqueue_group_summary` into ranked [`ActiveScope`]s. Each group
/// that currently holds eligible work (`oldest_eligible_at IS NOT NULL`) becomes one source scope, ordered
/// owner-local oldest-first (smallest `oldest_eligible_at` = most-aged group, group-key tiebreak for
/// determinism); `eligible_item_count` carries through as the eligible signal. [`project_scopes`] then
/// collapses to the requested granularity (Group = per-group detail in the oldest-first order; Queue = a
/// single rollup row for the queue — see [`project_scopes`] arithmetic).
///
/// `progress_bound_risk_count` is reported as `None` ("no signal"), NOT `Some(0)`: the summary's
/// `at_risk_count` is a hardcoded `0` placeholder while the progress-guard/at-risk derivation is deferred
/// (see `refresh_group_summaries`), and the [`ActiveScope`] contract reserves `None` for an uncomputed
/// signal vs `Some(0)` for a measured zero. When at-risk becomes live, map it to `Some` here.
///
/// PAUSE (intentional divergence from the claim path): discovery reports a group's INTRINSIC eligibility
/// and does NOT short-circuit on `queue_paused` (unlike `select_eligible_sql`/group selection). An operator
/// hunting starvation wants to see work that has built up *because* a queue is paused; the summary itself
/// is pause-agnostic, so discovery mirrors it. (A read of a queue that does not exist yields an empty list,
/// not `NotFound` — a discovery read of an unknown queue simply has no active scopes.)
///
/// KNOWN LIMITATION: read-only discovery does not run the mutating due-refresh used by group-aware claims.
/// A group made eligible ONLY by time passing can keep `oldest_eligible_at = NULL` until its next mutation
/// or a background due-sweep refresh, so discovery can UNDER-report time-triggered starvation.
pub(crate) fn discover_active_scopes_sql(
    conn: &Connection,
    shard: &QueueKey,
    granularity: DiscoveryGranularity,
    now: UtcTimestamp,
) -> EngineResult<Vec<ActiveScope>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut stmt = st(conn.prepare(
        "SELECT group_key, oldest_eligible_at, eligible_item_count \
         FROM pqueue_group_summary \
         WHERE tenant_id=?1 AND queue_id=?2 AND oldest_eligible_at IS NOT NULL \
         ORDER BY oldest_eligible_at ASC, group_key ASC",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((
            row.get::<_, String>(0)?,
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
            group_key: Some(group_key),
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
        "SELECT item_id, lease_expires_at, retry_count FROM pqueue_items \
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
            "SELECT item_id,lease_expires_at,retry_count FROM pqueue_items \
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
             lease_expires_at, retry_count, payload, fields, metadata FROM pqueue_items \
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
            "SELECT item_id, gate_key FROM pqueue_item_gates \
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
        "SELECT item_id,gate_key FROM pqueue_item_gates \
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
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let row = st(conn
            .query_row(
                "SELECT item_id, item_version, lifecycle_state, priority, group_key, not_before, \
                 retry_count, payload, fields FROM pqueue_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 \
                   AND superseded=0 AND lifecycle_state IN ('Pending','Leased')",
                params![t, q, key.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<Vec<u8>>>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional())?;
        out.push(match row {
            Some((id, version, state, priority, group, not_before, retry, payload, fields)) => {
                Some(LiveItemView {
                    item_id: ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
                    client_item_key: key.clone(),
                    item_version: version as u64,
                    lifecycle_state: parse_state(&state)?,
                    priority: parse_priority(priority)?,
                    group_key: group
                        .map(GroupKey::new)
                        .transpose()
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    not_before: not_before.map(nanos_ts),
                    attempt_count: retry as u32,
                    payload: payload.map(Bytes::from),
                    fields: fields_from_json(fields)?,
                })
            }
            None => None,
        });
    }
    Ok(out)
}

pub(crate) fn metrics_sql(conn: &Connection, shard: &QueueKey) -> EngineResult<QueueMetrics> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT lifecycle_state, COUNT(*) FROM pqueue_items \
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
            "SELECT item_id, lifecycle_state, fenced, superseded, cohort_size IS NOT NULL FROM pqueue_items \
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
            "SELECT state, cohort_lease_token_hash FROM pqueue_cohorts \
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
            "SELECT item_id FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 AND superseded=0",
            params![t, q, client_item_key.as_str()],
            |row| row.get(0),
        )
        .optional())?;
    id.map(|s| ItemId::new(s).map_err(|e| EngineError::Storage(e.to_string())))
        .transpose()
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
            "SELECT COUNT(*) FROM pqueue_items \
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
            "SELECT item_version FROM pqueue_items \
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
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
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
    Ok(by_queue)
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
            "SELECT lifecycle_state, superseded, fenced, item_version FROM pqueue_items \
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
            "SELECT fence FROM pqueue_instance_fences \
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
            "SELECT payload FROM pqueue_side_records \
             WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
            params![t, q, key],
            |row| row.get(0),
        )
        .optional())?;
    Ok(payload.map(Bytes::from))
}
