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
