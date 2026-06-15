#![forbid(unsafe_code)]

use pqueue_core::{ClientItemKey, ItemId, QueueId, TenantId, UtcTimestamp};
use pqueue_storage::commands::{BatchPushCommand, PushItem};
use pqueue_storage::multi_shard::{
    ClaimCandidate, ClaimIntentRecord, ClaimIntentReplayDecision, ClaimSortKey,
    MultiShardCommandKind, ShardClaimReplayState, ShardCommandCommit, deterministic_k_way_merge,
    evaluate_multi_shard_command_convergence, plan_fanout_claim, replay_claim_intent,
};
use pqueue_storage::{
    CommandEnvelope, CommandId, QueueCommand,
    memory::MemoryProjectionStore,
    traits::{ClaimRequest, ProjectionStore},
    types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey},
};

fn tenant() -> TenantId {
    TenantId::new("test-tenant").unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn iid(s: &str) -> ItemId {
    ItemId::new(s).unwrap()
}

fn cik(s: &str) -> ClientItemKey {
    ClientItemKey::new(s).unwrap()
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn shard(tenant: TenantId, queue: QueueId, shard_id: u32) -> ShardKey {
    ShardKey {
        tenant_id: tenant,
        queue_id: queue,
        shard_id: ShardId::new(shard_id),
    }
}

fn make_push_item(id: &str, key: &str) -> PushItem {
    PushItem {
        item_id: iid(id),
        client_item_key: cik(key),
        priority: None,
        not_before: None,
        max_attempts: 3,
        payload: None,
    }
}

fn push_cmd(
    tenant: TenantId,
    queue: QueueId,
    shard_id: u32,
    items: Vec<PushItem>,
    cmd_id: &str,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: items.iter().map(|item| item.item_id.clone()).collect(),
        command: QueueCommand::BatchPush(BatchPushCommand { items }),
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

#[tokio::test]
async fn storage_conformance_multi_shard_tests_fanout_plan_is_bounded_and_stable() {
    let t = tenant();
    let q = qid("multi-plan");
    let shards = vec![
        shard(t.clone(), q.clone(), 2),
        shard(t.clone(), q.clone(), 0),
        shard(t.clone(), q.clone(), 1),
    ];

    let plan = plan_fanout_claim(&shards, 5);
    assert_eq!(
        plan.iter().map(|entry| entry.max_items).collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
    assert_eq!(plan.iter().map(|entry| entry.max_items).sum::<usize>(), 5);
    assert_eq!(
        plan.iter()
            .map(|entry| entry.shard_key.shard_id.as_u32())
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn storage_conformance_multi_shard_tests_k_way_merge_orders_global_rank() {
    let t = tenant();
    let q = qid("multi-merge");
    let candidates = vec![
        claim_candidate(&t, &q, 1, "item-b", 10, 10, 2),
        claim_candidate(&t, &q, 0, "item-a", 5, 20, 1),
        claim_candidate(&t, &q, 2, "item-c", 5, 20, 1),
        claim_candidate(&t, &q, 0, "item-d", 20, 1, 3),
    ];

    let merged = deterministic_k_way_merge(candidates, 3);
    assert_eq!(
        merged
            .iter()
            .map(|candidate| candidate.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["item-a", "item-c", "item-b"]
    );
    assert_eq!(
        merged
            .iter()
            .map(|candidate| candidate.shard_key.shard_id.as_u32())
            .collect::<Vec<_>>(),
        vec![0, 2, 1]
    );
}

#[tokio::test]
async fn multi_shard_claim_order_replay_tests_replay_is_stable_under_input_order() {
    let t = tenant();
    let q = qid("multi-replay");
    let original = vec![
        claim_candidate(&t, &q, 1, "item-c", 30, 1, 3),
        claim_candidate(&t, &q, 0, "item-a", 10, 1, 1),
        claim_candidate(&t, &q, 2, "item-b", 20, 1, 2),
    ];
    let mut replayed = original.clone();
    replayed.reverse();

    let first = deterministic_k_way_merge(original, 10);
    let second = deterministic_k_way_merge(replayed, 10);
    assert_eq!(first, second);
    assert_eq!(
        first
            .iter()
            .map(|candidate| candidate.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["item-a", "item-b", "item-c"]
    );
}

#[tokio::test]
async fn storage_conformance_multi_shard_tests_fanout_claims_planned_shards() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("multi-memory");
    let shards = vec![
        shard(t.clone(), q.clone(), 0),
        shard(t.clone(), q.clone(), 1),
    ];

    for (idx, shard_key) in shards.iter().enumerate() {
        let pos = CommandPosition {
            shard_key: shard_key.clone(),
            sequence: 0,
            backend_epoch: 0,
        };
        store
            .apply_committed(
                pos,
                &[push_cmd(
                    t.clone(),
                    q.clone(),
                    shard_key.shard_id.as_u32(),
                    vec![
                        make_push_item(&format!("s{idx}-a"), &format!("s{idx}-ka")),
                        make_push_item(&format!("s{idx}-b"), &format!("s{idx}-kb")),
                    ],
                    &format!("cmd-s{idx}"),
                )],
            )
            .await
            .unwrap();
    }

    let plan = plan_fanout_claim(&shards, 4);
    let mut candidates = Vec::new();
    for entry in plan {
        let result = store
            .batch_claim(ClaimRequest {
                shard_key: entry.shard_key.clone(),
                max_items: entry.max_items,
                now: ts(1_000),
                lease_token: format!("lease-s{}", entry.shard_key.shard_id.as_u32()),
                lease_expires_at: ts(2_000),
            })
            .await
            .unwrap();
        for (offset, item_id) in result.claimed_item_ids.into_iter().enumerate() {
            candidates.push(ClaimCandidate {
                shard_key: entry.shard_key.clone(),
                item_id,
                sort_key: ClaimSortKey {
                    progress_guard_rank: offset as i64,
                    priority_rank: 0,
                    created_sequence: offset as u64,
                },
            });
        }
    }

    let merged = deterministic_k_way_merge(candidates, 4);
    assert_eq!(merged.len(), 4);
    let metrics = store
        .metrics(&QueueKey {
            tenant_id: t,
            queue_id: q,
        })
        .await
        .unwrap();
    assert_eq!(metrics.pending_count, 0);
    assert_eq!(metrics.leased_count, 4);
}

#[tokio::test]
async fn multi_shard_claim_order_replay_tests_claim_intent_reuses_plan_after_partial_failure() {
    let t = tenant();
    let q = qid("intent-partial");
    let shards = vec![
        shard(t.clone(), q.clone(), 0),
        shard(t.clone(), q.clone(), 1),
        shard(t.clone(), q.clone(), 2),
    ];
    let intent = ClaimIntentRecord {
        shard_plans: plan_fanout_claim(&shards, 5),
    };
    let decision = replay_claim_intent(
        &intent,
        &[
            ShardClaimReplayState {
                shard_key: shard(t.clone(), q.clone(), 0),
                committed_item_ids: vec![iid("item-a"), iid("item-b")],
                leases_active: true,
                retryable_failure: false,
            },
            ShardClaimReplayState {
                shard_key: shard(t.clone(), q.clone(), 1),
                committed_item_ids: Vec::new(),
                leases_active: false,
                retryable_failure: true,
            },
        ],
    );

    let ClaimIntentReplayDecision::Replay(replay) = decision else {
        panic!("active partial lease set should replay, not expire");
    };
    assert_eq!(
        replay
            .committed_item_ids
            .iter()
            .map(ItemId::as_str)
            .collect::<Vec<_>>(),
        vec!["item-a", "item-b"]
    );
    assert!(replay.partial);
    assert_eq!(
        replay
            .retry_plan
            .iter()
            .map(|plan| (plan.shard_key.shard_id.as_u32(), plan.max_items))
            .collect::<Vec<_>>(),
        vec![(1, 2), (2, 1)]
    );
}

#[tokio::test]
async fn multi_shard_claim_order_replay_tests_request_expired_is_envelope_scoped() {
    let t = tenant();
    let q = qid("intent-expired");
    let intent = ClaimIntentRecord {
        shard_plans: plan_fanout_claim(
            &[
                shard(t.clone(), q.clone(), 0),
                shard(t.clone(), q.clone(), 1),
            ],
            4,
        ),
    };
    let decision = replay_claim_intent(
        &intent,
        &[
            ShardClaimReplayState {
                shard_key: shard(t.clone(), q.clone(), 0),
                committed_item_ids: vec![iid("old-a")],
                leases_active: false,
                retryable_failure: false,
            },
            ShardClaimReplayState {
                shard_key: shard(t.clone(), q.clone(), 1),
                committed_item_ids: vec![iid("old-b")],
                leases_active: false,
                retryable_failure: false,
            },
        ],
    );

    assert_eq!(decision, ClaimIntentReplayDecision::RequestExpired);
}

#[tokio::test]
async fn storage_conformance_multi_shard_tests_set_gates_converges_only_after_all_shards_commit() {
    let t = tenant();
    let q = qid("set-gates-convergence");
    let shards = vec![
        shard(t.clone(), q.clone(), 0),
        shard(t.clone(), q.clone(), 1),
        shard(t.clone(), q.clone(), 2),
    ];
    let partial = evaluate_multi_shard_command_convergence(
        MultiShardCommandKind::SetGates,
        &shards,
        &[
            ShardCommandCommit {
                shard_key: shard(t.clone(), q.clone(), 2),
                committed: true,
            },
            ShardCommandCommit {
                shard_key: shard(t.clone(), q.clone(), 0),
                committed: true,
            },
        ],
    );

    assert!(!partial.converged);
    assert!(!partial.ack_allowed);
    assert!(!partial.visible);
    assert_eq!(
        partial
            .committed_shards
            .iter()
            .map(|shard| shard.shard_id.as_u32())
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(
        partial
            .retry_shards
            .iter()
            .map(|shard| shard.shard_id.as_u32())
            .collect::<Vec<_>>(),
        vec![1]
    );

    let converged = evaluate_multi_shard_command_convergence(
        MultiShardCommandKind::SetGates,
        &shards,
        &[
            ShardCommandCommit {
                shard_key: shard(t.clone(), q.clone(), 2),
                committed: true,
            },
            ShardCommandCommit {
                shard_key: shard(t.clone(), q.clone(), 0),
                committed: true,
            },
            ShardCommandCommit {
                shard_key: shard(t.clone(), q.clone(), 1),
                committed: true,
            },
        ],
    );
    assert!(converged.converged);
    assert!(converged.ack_allowed);
    assert!(converged.visible);
    assert!(converged.retry_shards.is_empty());
}

#[tokio::test]
async fn storage_conformance_multi_shard_tests_purge_items_converges_by_retrying_uncommitted_shards()
 {
    let t = tenant();
    let q = qid("purge-convergence");
    let shards = vec![
        shard(t.clone(), q.clone(), 0),
        shard(t.clone(), q.clone(), 1),
    ];
    let partial = evaluate_multi_shard_command_convergence(
        MultiShardCommandKind::PurgeItems,
        &shards,
        &[ShardCommandCommit {
            shard_key: shard(t.clone(), q.clone(), 1),
            committed: true,
        }],
    );

    assert_eq!(partial.kind, MultiShardCommandKind::PurgeItems);
    assert!(!partial.ack_allowed);
    assert!(!partial.visible);
    assert_eq!(
        partial
            .retry_shards
            .iter()
            .map(|shard| shard.shard_id.as_u32())
            .collect::<Vec<_>>(),
        vec![0]
    );

    let converged = evaluate_multi_shard_command_convergence(
        MultiShardCommandKind::PurgeItems,
        &shards,
        &[
            ShardCommandCommit {
                shard_key: shard(t.clone(), q.clone(), 1),
                committed: true,
            },
            ShardCommandCommit {
                shard_key: shard(t.clone(), q.clone(), 0),
                committed: true,
            },
        ],
    );
    assert!(converged.converged);
    assert!(converged.ack_allowed);
    assert!(converged.visible);
}

fn claim_candidate(
    tenant_id: &TenantId,
    queue_id: &QueueId,
    shard_id: u32,
    item_id: &str,
    progress_guard_rank: i64,
    priority_rank: i64,
    created_sequence: u64,
) -> ClaimCandidate {
    ClaimCandidate {
        shard_key: shard(tenant_id.clone(), queue_id.clone(), shard_id),
        item_id: iid(item_id),
        sort_key: ClaimSortKey {
            progress_guard_rank,
            priority_rank,
            created_sequence,
        },
    }
}
