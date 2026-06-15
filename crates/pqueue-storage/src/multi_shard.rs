use pqueue_core::ItemId;

use crate::types::ShardKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardFanoutPlan {
    pub shard_key: ShardKey,
    pub max_items: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClaimSortKey {
    pub progress_guard_rank: i64,
    pub priority_rank: i64,
    pub created_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimCandidate {
    pub shard_key: ShardKey,
    pub item_id: ItemId,
    pub sort_key: ClaimSortKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardProgress {
    pub shard_key: ShardKey,
    pub oldest_eligible_age_ms: Option<u64>,
    pub progress_bound_risk_count: u64,
    pub observed_at_ms: u64,
    pub owned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossShardProgress {
    pub oldest_eligible_age_ms: Option<u64>,
    pub progress_bound_risk_count: u64,
    pub as_of_ms: u64,
    pub stalled_shards: Vec<ShardKey>,
}

pub fn plan_fanout_claim(shards: &[ShardKey], max_items: usize) -> Vec<ShardFanoutPlan> {
    if shards.is_empty() || max_items == 0 {
        return Vec::new();
    }

    let mut ordered = shards.to_vec();
    ordered.sort();
    let base = max_items / ordered.len();
    let remainder = max_items % ordered.len();

    ordered
        .into_iter()
        .enumerate()
        .filter_map(|(idx, shard_key)| {
            let max_items = base + usize::from(idx < remainder);
            (max_items > 0).then_some(ShardFanoutPlan {
                shard_key,
                max_items,
            })
        })
        .collect()
}

pub fn deterministic_k_way_merge(
    mut candidates: Vec<ClaimCandidate>,
    max_items: usize,
) -> Vec<ClaimCandidate> {
    candidates.sort_by(|left, right| {
        left.sort_key
            .cmp(&right.sort_key)
            .then_with(|| left.item_id.as_str().cmp(right.item_id.as_str()))
            .then_with(|| left.shard_key.cmp(&right.shard_key))
    });
    candidates.truncate(max_items);
    candidates
}

pub fn aggregate_cross_shard_progress(
    shards: &[ShardProgress],
    progress_bound_ms: u64,
    stale_after_ms: u64,
    now_ms: u64,
) -> CrossShardProgress {
    let oldest_eligible_age_ms = shards
        .iter()
        .filter_map(|shard| shard.oldest_eligible_age_ms)
        .max();
    let progress_bound_risk_count = shards
        .iter()
        .map(|shard| {
            let age_risk = shard
                .oldest_eligible_age_ms
                .is_some_and(|age| age >= progress_bound_ms);
            shard.progress_bound_risk_count + u64::from(age_risk)
        })
        .sum();
    let as_of_ms = shards
        .iter()
        .map(|shard| shard.observed_at_ms)
        .min()
        .unwrap_or(0);
    let mut stalled_shards = shards
        .iter()
        .filter(|shard| {
            !shard.owned || now_ms.saturating_sub(shard.observed_at_ms) > stale_after_ms
        })
        .map(|shard| shard.shard_key.clone())
        .collect::<Vec<_>>();
    stalled_shards.sort();

    CrossShardProgress {
        oldest_eligible_age_ms,
        progress_bound_risk_count,
        as_of_ms,
        stalled_shards,
    }
}
