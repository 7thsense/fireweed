use std::collections::BTreeMap;

use crate::types::ShardKey;
use pqueue_core::{GroupKey, ItemId, QueueId, TenantId};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardActiveScopeSummary {
    pub group_key: Option<GroupKey>,
    pub oldest_eligible_age_ms: Option<u64>,
    pub eligible_count: Option<u64>,
    pub progress_bound_risk_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardActiveScopeRead {
    pub shard_key: ShardKey,
    pub observed_at_ms: u64,
    pub active_scopes: Vec<ShardActiveScopeSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveScopeDescriptor {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub group_key: Option<GroupKey>,
    pub oldest_eligible_age_ms: u64,
    pub eligible_count: Option<u64>,
    pub progress_bound_risk_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossShardActiveScopes {
    pub as_of_ms: u64,
    pub active_scopes: Vec<ActiveScopeDescriptor>,
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

pub fn aggregate_cross_shard_active_scopes(
    shard_reads: &[ShardActiveScopeRead],
    max_results: usize,
) -> CrossShardActiveScopes {
    let as_of_ms = shard_reads
        .iter()
        .map(|read| read.observed_at_ms)
        .min()
        .unwrap_or(0);
    let mut by_scope: BTreeMap<(TenantId, QueueId, Option<GroupKey>), ActiveScopeDescriptor> =
        BTreeMap::new();

    for read in shard_reads {
        for scope in &read.active_scopes {
            let Some(oldest_eligible_age_ms) = scope.oldest_eligible_age_ms else {
                continue;
            };
            let key = (
                read.shard_key.tenant_id.clone(),
                read.shard_key.queue_id.clone(),
                scope.group_key.clone(),
            );
            by_scope
                .entry(key)
                .and_modify(|existing| {
                    existing.oldest_eligible_age_ms =
                        existing.oldest_eligible_age_ms.max(oldest_eligible_age_ms);
                    existing.eligible_count =
                        sum_optional(existing.eligible_count, scope.eligible_count);
                    existing.progress_bound_risk_count = sum_optional(
                        existing.progress_bound_risk_count,
                        scope.progress_bound_risk_count,
                    );
                })
                .or_insert_with(|| ActiveScopeDescriptor {
                    tenant_id: read.shard_key.tenant_id.clone(),
                    queue_id: read.shard_key.queue_id.clone(),
                    group_key: scope.group_key.clone(),
                    oldest_eligible_age_ms,
                    eligible_count: scope.eligible_count,
                    progress_bound_risk_count: scope.progress_bound_risk_count,
                });
        }
    }

    let mut active_scopes = by_scope.into_values().collect::<Vec<_>>();
    active_scopes.sort_by(|left, right| {
        right
            .oldest_eligible_age_ms
            .cmp(&left.oldest_eligible_age_ms)
            .then_with(|| left.tenant_id.as_str().cmp(right.tenant_id.as_str()))
            .then_with(|| left.queue_id.as_str().cmp(right.queue_id.as_str()))
            .then_with(|| left.group_key.cmp(&right.group_key))
    });
    active_scopes.truncate(max_results);

    CrossShardActiveScopes {
        as_of_ms,
        active_scopes,
    }
}

fn sum_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
