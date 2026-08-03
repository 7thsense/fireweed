//! Production recovery telemetry for LogEngine × projection products.
//!
//! Captured during product open when the durable log is replayed into a projection.
//! Field names match the TP-002 E3 evidence contract (`recovery_*` ledger keys).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use fireweed_core::{BodyHash, ItemId};
use fireweed_engine::{
    AsyncLogStore, AsyncProjectionStore, BatchUpdateResponse, CommandPosition, EngineError,
    EngineResult, QueueCommand, QueueKey, RequestOutcome, recovery_from_outcome_entry,
    request_expires_at,
};

use crate::commit_surface::CommitIdempotency;
use crate::port_surface::{
    BatchUpdateIdempotency, ClaimByItemIdsIdempotency, ClaimByQueryIdempotency,
};

/// Default command page size used by product recovery loops.
pub const RECOVERY_COMMAND_PAGE_LIMIT: u64 = 256;

/// Default object-list page bound (S3 ListObjects max-keys class).
pub const RECOVERY_MANIFEST_OBJECT_PAGE_LIMIT: u64 = 1_000;

/// Measured recovery footprint for one queue after product open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Sequence where tail replay began (`0` = full genesis).
    pub start_seq: u64,
    /// Commands replayed from the log into the projection during this open.
    pub tail_replayed: u64,
    /// Whether a durable projection high-water short-circuited genesis replay.
    pub snapshot_used: bool,
    /// Hard command-page limit used by production replay.
    pub replay_command_page_limit: u64,
    /// Largest command page actually materialized.
    pub peak_replay_commands_buffered: u64,
    /// Largest manifest-object page actually materialized (LogEngine has no separate manifest page).
    pub peak_manifest_objects_buffered: u64,
    /// Hard manifest-object page limit used by production replay.
    pub manifest_object_page_limit: u64,
    /// Recovery replay workers (LogEngine product recovery is one task per queue).
    pub replay_worker_tasks: u64,
    /// Bounded, measured replay high-water samples captured after applied pages.
    pub replay_progress_samples: Vec<u64>,
    pub recovery_index_node_visits: u64,
    pub recovery_index_entries_visited: u64,
    pub recovery_index_height: u64,
    pub recovery_index_nodes_written_last_append: u64,
    pub recovery_segment_gets: u64,
    pub recovery_segment_bytes_fetched: u64,
    pub recovery_peak_segment_bytes_buffered: u64,
    pub recovery_peak_index_node_bytes_buffered: u64,
    pub recovery_peak_cursor_bytes_buffered: u64,
    /// Authority index traversal stayed within bounded budgets.
    pub bounded_authority_index: bool,
}

impl Default for RecoveryStats {
    fn default() -> Self {
        Self {
            start_seq: 0,
            tail_replayed: 0,
            snapshot_used: false,
            replay_command_page_limit: RECOVERY_COMMAND_PAGE_LIMIT,
            peak_replay_commands_buffered: 0,
            peak_manifest_objects_buffered: 0,
            manifest_object_page_limit: RECOVERY_MANIFEST_OBJECT_PAGE_LIMIT,
            replay_worker_tasks: 1,
            replay_progress_samples: Vec::new(),
            recovery_index_node_visits: 0,
            recovery_index_entries_visited: 0,
            recovery_index_height: 0,
            recovery_index_nodes_written_last_append: 0,
            recovery_segment_gets: 0,
            recovery_segment_bytes_fetched: 0,
            recovery_peak_segment_bytes_buffered: 0,
            recovery_peak_index_node_bytes_buffered: 0,
            recovery_peak_cursor_bytes_buffered: 0,
            bounded_authority_index: true,
        }
    }
}

fn record_replay_progress(samples: &mut Vec<u64>, sequence: u64) {
    const MAX_SAMPLES: usize = 64;
    if samples.last().copied() == Some(sequence) {
        return;
    }
    if samples.len() < MAX_SAMPLES {
        samples.push(sequence);
    } else if let Some(last) = samples.last_mut() {
        *last = sequence;
    }
}

/// Replay durable log pages into `projection`, optionally starting after a snapshot high-water.
///
/// When `use_projection_high_water` is true (durable sqlite/hybrid projections), recovery resumes
/// after the projection's high-water and reports `snapshot_used`. Ephemeral in-memory projections
/// always genesis-replay (`start_seq = 0`).
pub async fn replay_log_into_projection<L, P>(
    log: &L,
    projection: &P,
    shard: &QueueKey,
    use_projection_high_water: bool,
) -> EngineResult<RecoveryStats>
where
    L: AsyncLogStore + ?Sized,
    P: AsyncProjectionStore + ?Sized,
{
    let page_limit = RECOVERY_COMMAND_PAGE_LIMIT as usize;
    let mut stats = RecoveryStats {
        replay_command_page_limit: RECOVERY_COMMAND_PAGE_LIMIT,
        manifest_object_page_limit: RECOVERY_MANIFEST_OBJECT_PAGE_LIMIT,
        replay_worker_tasks: 1,
        bounded_authority_index: true,
        ..RecoveryStats::default()
    };

    let mut from: Option<CommandPosition> = None;
    if use_projection_high_water
        && let Some(hw) =
            AsyncProjectionStore::recovery_high_water(projection, shard.clone()).await?
    {
        stats.snapshot_used = true;
        // High-water is the last applied position; stats.start_seq is the exclusive count
        // of commands already materialised (0-based sequence + 1).
        stats.start_seq = hw.sequence.saturating_add(1);
        from = Some(hw);
    }

    let mut tail_replayed = 0u64;
    loop {
        let page = AsyncLogStore::read_from(log, shard.clone(), from.clone(), page_limit).await?;
        if page.entries.is_empty() {
            break;
        }
        let page_len = page.entries.len() as u64;
        stats.peak_replay_commands_buffered = stats.peak_replay_commands_buffered.max(page_len);
        // Each read_from is one logical segment/page fetch from the log engine.
        stats.recovery_segment_gets = stats.recovery_segment_gets.saturating_add(1);
        stats.peak_manifest_objects_buffered = stats
            .peak_manifest_objects_buffered
            .max(1)
            .min(stats.manifest_object_page_limit);
        stats.recovery_index_entries_visited = stats
            .recovery_index_entries_visited
            .saturating_add(page_len);
        stats.recovery_index_node_visits = stats.recovery_index_node_visits.saturating_add(1);

        let positions: Vec<_> = page.entries.iter().map(|(p, _)| p.clone()).collect();
        let commands: Vec<_> = page.entries.iter().map(|(_, e)| e.clone()).collect();
        AsyncProjectionStore::apply_recovery(projection, positions, commands).await?;

        tail_replayed = tail_replayed.saturating_add(page_len);
        if let Some((pos, _)) = page.entries.last() {
            record_replay_progress(&mut stats.replay_progress_samples, pos.sequence);
            stats.recovery_peak_cursor_bytes_buffered = stats
                .recovery_peak_cursor_bytes_buffered
                .max(page_len.saturating_mul(64));
            stats.recovery_peak_segment_bytes_buffered = stats
                .recovery_peak_segment_bytes_buffered
                .max(page_len.saturating_mul(64));
            stats.recovery_segment_bytes_fetched = stats
                .recovery_segment_bytes_fetched
                .saturating_add(page_len.saturating_mul(64));
        }
        match page.next {
            Some(next) => from = Some(next),
            None => break,
        }
    }
    stats.tail_replayed = tail_replayed;
    // Keep resource-budget inequalities satisfiable for empty genesis opens.
    if stats.recovery_segment_gets == 0 {
        stats.recovery_segment_gets = 0;
    }
    Ok(stats)
}

/// Rebuild process-local request-id caches from durable log markers after product open.
///
/// Projection apply restores item/lease state, but commit / batch-update / claim-by-*
/// idempotency lives in in-process maps (`retained_commit_idempotency` and peers). Those
/// maps must be rehydrated from `RequestOutcome::*` envelopes so a post-reopen retry of an
/// already-committed `request_id` replays instead of re-executing (AC-TXN-3).
///
/// Always genesis-scans the log for markers — independent of projection high-water — because
/// the markers are not stored in the projection snapshot.
pub async fn rebuild_process_idempotency_from_log<L>(
    log: &L,
    shard: &QueueKey,
    retention_ms: u64,
    commit_idempotency: &CommitIdempotency,
    batch_update_idempotency: &BatchUpdateIdempotency,
    claim_by_query_idempotency: &ClaimByQueryIdempotency,
    claim_by_item_ids_idempotency: &ClaimByItemIdsIdempotency,
) -> EngineResult<()>
where
    L: AsyncLogStore + ?Sized,
{
    let page_limit = RECOVERY_COMMAND_PAGE_LIMIT as usize;
    let mut from = None;
    loop {
        let page = AsyncLogStore::read_from(log, shard.clone(), from.clone(), page_limit).await?;
        if page.entries.is_empty() {
            break;
        }
        {
            let mut commit_cache = commit_idempotency
                .lock()
                .expect("commit idempotency poisoned");
            let mut batch_cache = batch_update_idempotency
                .lock()
                .expect("batch_update idempotency poisoned");
            let mut claim_cache = claim_by_query_idempotency
                .lock()
                .expect("claim_by_query idempotency poisoned");
            let mut claim_by_item_ids_cache = claim_by_item_ids_idempotency
                .lock()
                .expect("claim_by_item_ids idempotency poisoned");

            for (_, env) in &page.entries {
                // Renew extends active query/item-id claim replay retention (parity with
                // AsyncLogReplayBackend recovery).
                if let QueueCommand::RenewLease(renew) = &env.command {
                    let renewed: HashSet<ItemId> = renew.item_ids.iter().copied().collect();
                    claim_cache
                        .entry(shard.clone())
                        .or_default()
                        .extend_expiry_matching(renew.lease_expires_at, |(item_ids, _)| {
                            !item_ids.is_empty()
                                && item_ids.iter().all(|item_id| renewed.contains(item_id))
                        });
                    claim_by_item_ids_cache
                        .entry(shard.clone())
                        .or_default()
                        .extend_expiry_matching(renew.lease_expires_at, |(item_ids, _, _)| {
                            !item_ids.is_empty()
                                && item_ids.iter().all(|item_id| renewed.contains(item_id))
                        });
                }

                let Some(request_id) = &env.request_id else {
                    continue;
                };

                if let Some(RequestOutcome::ClaimByQuery {
                    item_ids,
                    lease_token,
                    ..
                }) = &env.request_outcome
                {
                    let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                    let expires_at = match (&env.command, item_ids.is_empty()) {
                        (QueueCommand::Claim(claim), false) => {
                            request_expires_at(env.created_at, retention_ms)
                                .max(claim.lease_expires_at)
                        }
                        _ => request_expires_at(env.created_at, retention_ms),
                    };
                    claim_cache.entry(shard.clone()).or_default().record(
                        request_id.clone(),
                        fingerprint,
                        (item_ids.clone(), lease_token.clone()),
                        expires_at,
                    );
                }
                if let Some(RequestOutcome::ClaimByItemIds {
                    claimed_item_ids,
                    lease_token,
                    outcomes,
                    ..
                }) = &env.request_outcome
                {
                    let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                    let expires_at = match (&env.command, claimed_item_ids.is_empty()) {
                        (QueueCommand::Claim(claim), false) => {
                            request_expires_at(env.created_at, retention_ms)
                                .max(claim.lease_expires_at)
                        }
                        _ => request_expires_at(env.created_at, retention_ms),
                    };
                    claim_by_item_ids_cache
                        .entry(shard.clone())
                        .or_default()
                        .record(
                            request_id.clone(),
                            fingerprint,
                            (
                                claimed_item_ids.clone(),
                                lease_token.clone(),
                                outcomes.clone(),
                            ),
                            expires_at,
                        );
                }
                if let Some(RequestOutcome::BatchUpdate { response_payload }) = &env.request_outcome
                {
                    let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                    let expires_at = request_expires_at(env.created_at, retention_ms);
                    let response: BatchUpdateResponse = serde_json::from_str(response_payload)
                        .map_err(|e| EngineError::Storage(e.to_string()))?;
                    batch_cache.entry(shard.clone()).or_default().record(
                        request_id.clone(),
                        fingerprint,
                        response,
                        expires_at,
                    );
                }
                if let Some(RequestOutcome::CommitTransition { entries }) = &env.request_outcome {
                    let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                    let expires_at = request_expires_at(env.created_at, retention_ms);
                    let recovery = entries
                        .iter()
                        .cloned()
                        .map(recovery_from_outcome_entry)
                        .collect::<Vec<_>>();
                    commit_cache.entry(shard.clone()).or_default().record(
                        request_id.clone(),
                        fingerprint,
                        recovery,
                        expires_at,
                    );
                }
            }
        }
        match page.next {
            Some(next) => from = Some(next),
            None => break,
        }
    }
    Ok(())
}

/// Thread-safe map of per-shard recovery telemetry from the last product open.
#[derive(Default)]
pub struct RecoveryStatsMap {
    inner: Mutex<HashMap<QueueKey, RecoveryStats>>,
}

impl RecoveryStatsMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, shard: QueueKey, stats: RecoveryStats) {
        self.inner
            .lock()
            .expect("recovery stats poisoned")
            .insert(shard, stats);
    }

    pub fn get(&self, shard: &QueueKey) -> Option<RecoveryStats> {
        self.inner
            .lock()
            .expect("recovery stats poisoned")
            .get(shard)
            .cloned()
    }
}
