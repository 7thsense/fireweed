
//! fireweed-6e38e2b4: reopen recovery must reseed item-id counters and keep the
//! eligibility index free of duplicate item ids after commit_transition lifecycle
//! work and post-reopen upserts (the snorri offline-MVP claim path).

use std::collections::{BTreeMap, HashSet};

use fireweed_core::{
    ClientItemKey, LeaseToken, Metadata, MetadataValue, PriorityModel, PriorityValue, UtcTimestamp,
    WorkerId,
};
use fireweed_engine::{
    ClaimCompatibility, ClaimPort, ClaimRef, ClaimRequest, CommitEntryOutcome, CommitTransition,
    CommitTransitionEntry, CommitTransitionPort, ControlPlaneStore, FinalizeKind, ProjectionStore,
    PushSpec, UpsertPort, UpsertOutcome,
};
use fireweed_sqlite::composed_sqlite_backend;
use fireweed_core::RequestId;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn qdef() -> fireweed_core::QueueDefinition {
    let mut d = fireweed_conformance::qdef();
    d.priority_model = PriorityModel::timestamp_ascending();
    d.max_claim_batch_size = 10_000;
    d.max_push_batch_size = 10_000;
    d
}

fn assert_unique_eligible(
    backend: &fireweed_engine::AsyncLogReplayBackend<
        fireweed_sqlite::SqliteLog,
        fireweed_projection::InMemoryProjection,
    >,
    now: UtcTimestamp,
    expected: usize,
) {
    let shard = fireweed_conformance::shard();
    let candidates = backend
        .with_projection(|p| ProjectionStore::eligible_candidates(p, &shard, now, 10_000))
        .unwrap();
    let unique: HashSet<_> = candidates.iter().copied().collect();
    assert_eq!(
        unique.len(),
        candidates.len(),
        "eligible_candidates has duplicate item ids: {candidates:?}"
    );
    assert_eq!(
        candidates.len(),
        expected,
        "unexpected eligible set: {candidates:?}"
    );
}

#[tokio::test]
async fn reopen_after_commit_transition_lifecycle_keeps_unique_eligible_and_fresh_ids() {
    let path = std::env::temp_dir().join(format!(
        "fw024-elig-recovery-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let path_str = path.to_str().unwrap();
    let shard = fireweed_conformance::shard();

    let lifecycle_ids = {
        let b = composed_sqlite_backend(path_str).unwrap();
        b.create_queue(qdef()).await.unwrap();
        let key = ClientItemKey::new("stage-0").unwrap();
        let mut metadata = Metadata::new();
        metadata.insert("native_transition_item", MetadataValue::String("1".into()));
        b.replace_if_pending(
            &shard,
            &key,
            Some(PriorityValue::Timestamp(ts(20_000))),
            None,
            None,
            None,
            BTreeMap::new(),
            metadata,
            None,
            ts(1),
            None,
        )
        .await
        .unwrap();
        let claimed = b
            .claim(ClaimRequest {
                eligibility_time: None,
                shard: shard.clone(),
                worker_id: WorkerId::new("w").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("L0").unwrap(),
                lease_expires_at: ts(200),
                now: ts(10),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        let c = &claimed.items[0];
        let outcomes = b
            .commit_transition(
                &shard,
                CommitTransition {
                    request_id: Some(RequestId::new("txn-elig-recovery").unwrap()),
                    entries: vec![CommitTransitionEntry {
                        claim_ref: ClaimRef {
                            item_id: c.item_id,
                            lease_token: c.lease_token.clone().unwrap(),
                            lease_expires_at: c.lease_expires_at,
                            item_version: c.item_version,
                        },
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: Vec::new(),
                        lifecycle_items: vec![
                            PushSpec {
                                priority: Some(PriorityValue::Timestamp(ts(5))),
                                not_before: Some(ts(5)),
                                ..Default::default()
                            },
                            PushSpec {
                                priority: Some(PriorityValue::Timestamp(ts(6))),
                                not_before: None,
                                ..Default::default()
                            },
                        ],
                        instance_fence: None,
                    }],
                },
                ts(20),
                None,
            )
            .await
            .unwrap();
        match &outcomes[0] {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids.clone(),
            other => panic!("expected commit, got {other:?}"),
        }
    };

    // Reopen: recover() rebuilds the projection AND must reseed mint counters.
    let b = composed_sqlite_backend(path_str).unwrap();
    b.create_queue(qdef()).await.unwrap(); // idempotent snorri-style re-create
    assert_unique_eligible(&b, ts(100), 2);

    let key = ClientItemKey::new("stage-1").unwrap();
    let mut metadata = Metadata::new();
    metadata.insert("native_transition_item", MetadataValue::String("1".into()));
    let upserted = b
        .replace_if_pending(
            &shard,
            &key,
            Some(PriorityValue::Timestamp(ts(20_001))),
            None,
            None,
            None,
            BTreeMap::new(),
            metadata,
            None,
            ts(30),
            None,
        )
        .await
        .unwrap();
    let new_id = match upserted {
        UpsertOutcome::Inserted { item_id } => item_id,
        UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
    };
    assert!(
        !lifecycle_ids.contains(&new_id),
        "post-reopen mint re-used a recovered item id {new_id:?}; counters were not reseeded"
    );

    assert_unique_eligible(&b, ts(100), 3);
    let claimed = b
        .claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("w2").unwrap(),
            max_items: 10_000,
            lease_token: LeaseToken::new("L1").unwrap(),
            lease_expires_at: ts(300),
            now: ts(100),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        })
        .await
        .expect("claim after reopen must accept a unique plan");
    assert_eq!(claimed.items.len(), 3);
    let claimed_ids: HashSet<_> = claimed.items.iter().map(|i| i.item_id).collect();
    assert_eq!(claimed_ids.len(), 3);

    let _ = std::fs::remove_file(&path);
}
