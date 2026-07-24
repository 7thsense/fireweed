use fireweed_conformance::{envelope, item, qdef, ts};
use fireweed_core::{CohortId, CohortPolicy, GroupKey, ItemId, ItemState, LeaseToken};
use fireweed_engine::{
    AsyncProjectionStore, ClaimCommand, CohortClaimCommand, CohortFinalizeCommand,
    CohortLeaseTarget, CohortRenewLeaseCommand, CommandPosition, EngineError, FenceLeaseCommand,
    FinalizeKind, LeaseExpiredCommand, PushCommand, QueueCommand, QueueKey,
};
use fireweed_sqlite::AsyncSqliteProjectionStore;
use fireweed_turso::TursoRelational;

#[tokio::test]
async fn expired_lease_selection_and_transition_match_sqlite() {
    let mut definition = qdef();
    definition.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(60_000),
        on_incomplete: None,
        max_cohort_size: Some(10),
    });
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let sqlite = AsyncSqliteProjectionStore::open(":memory:").await.unwrap();
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&sqlite, definition.clone())
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let ids = [
        ItemId::new("2").unwrap(),
        ItemId::new("10").unwrap(),
        ItemId::new("1").unwrap(),
    ];
    let cohort_ids = [ItemId::new("20").unwrap(), ItemId::new("21").unwrap()];
    let cohort_group = GroupKey::new("cohort").unwrap();
    let mut pushed = ids
        .iter()
        .map(|id| item(&id.to_string(), &format!("key-{id}"), 3))
        .collect::<Vec<_>>();
    pushed.extend(cohort_ids.iter().map(|id| {
        let mut member = item(&id.to_string(), &format!("key-{id}"), 3);
        member.group_key = Some(cohort_group.clone());
        member.cohort_size = Some(2);
        member
    }));
    let all_ids = ids
        .iter()
        .chain(cohort_ids.iter())
        .copied()
        .collect::<Vec<_>>();
    let push = envelope(
        QueueCommand::Push(PushCommand { items: pushed }),
        all_ids.clone(),
    );
    let claim = envelope(
        QueueCommand::Claim(ClaimCommand {
            item_ids: all_ids.clone(),
            lease_token: LeaseToken::new("lease").unwrap(),
            lease_expires_at: ts(10),
            worker_id: None,
        }),
        all_ids,
    );
    let fence = envelope(
        QueueCommand::FenceLease(FenceLeaseCommand {
            item_ids: vec![ids[0]],
        }),
        vec![ids[0]],
    );
    let positions = vec![
        CommandPosition::new(shard.clone(), 0, 0),
        CommandPosition::new(shard.clone(), 0, 1),
        CommandPosition::new(shard.clone(), 0, 2),
    ];
    AsyncProjectionStore::apply_live(
        &sqlite,
        positions.clone(),
        vec![push.clone(), claim.clone(), fence.clone()],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(&turso, positions, vec![push, claim, fence])
        .await
        .unwrap();

    assert!(
        AsyncProjectionStore::expired_leases(&sqlite, shard.clone(), ts(10), 10)
            .await
            .unwrap()
            .is_empty(),
        "expiry is strict: equal-to-now is not expired"
    );
    let sqlite_ids = AsyncProjectionStore::expired_leases(&sqlite, shard.clone(), ts(11), 2)
        .await
        .unwrap();
    let turso_ids = AsyncProjectionStore::expired_leases(&turso, shard.clone(), ts(11), 2)
        .await
        .unwrap();
    assert_eq!(turso_ids, sqlite_ids);
    assert_eq!(turso_ids, vec![ids[2], ids[1]]);

    let expired = envelope(
        QueueCommand::LeaseExpired(LeaseExpiredCommand {
            item_ids: turso_ids.clone(),
        }),
        turso_ids.clone(),
    );
    AsyncProjectionStore::apply_live(
        &sqlite,
        vec![CommandPosition::new(shard.clone(), 0, 3)],
        vec![expired.clone()],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 0, 3)],
        vec![expired],
    )
    .await
    .unwrap();
    for id in turso_ids {
        assert_eq!(
            AsyncProjectionStore::item_state(&sqlite, shard.clone(), id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&turso, shard.clone(), id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
    }
    assert_eq!(
        AsyncProjectionStore::expired_leases(&turso, shard, ts(11), 10)
            .await
            .unwrap(),
        Vec::<ItemId>::new(),
        "fenced and cohort leases stay excluded after the ordinary leases are reclaimed"
    );
    sqlite.close_and_drain().await.unwrap();
}

#[tokio::test]
async fn cohort_lease_validation_renew_and_retry_match_sqlite() {
    let mut definition = qdef();
    definition.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(60_000),
        on_incomplete: None,
        max_cohort_size: Some(10),
    });
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let sqlite = AsyncSqliteProjectionStore::open(":memory:").await.unwrap();
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&sqlite, definition.clone())
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();

    let group = GroupKey::new("cohort-group").unwrap();
    let cohort_id = CohortId::new(format!("coh:{}:0", group.as_str())).unwrap();
    let ids = [ItemId::new("31").unwrap(), ItemId::new("32").unwrap()];
    let items = ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let mut member = item(&id.to_string(), &format!("key-{id}"), 3);
            member.group_key = Some(group.clone());
            member.cohort_size = Some(2);
            member.max_attempts = if index == 0 { 1 } else { 3 };
            member
        })
        .collect();
    let token = LeaseToken::new("shared-cohort-token").unwrap();
    let push = envelope(QueueCommand::Push(PushCommand { items }), ids.to_vec());
    let claim = envelope(
        QueueCommand::CohortClaim(CohortClaimCommand {
            cohort_id: cohort_id.clone(),
            item_ids: ids.to_vec(),
            lease_token: token.clone(),
            lease_expires_at: ts(20),
        }),
        ids.to_vec(),
    );
    let positions = vec![
        CommandPosition::new(shard.clone(), 0, 0),
        CommandPosition::new(shard.clone(), 0, 1),
    ];
    AsyncProjectionStore::apply_live(
        &sqlite,
        positions.clone(),
        vec![push.clone(), claim.clone()],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(&turso, positions, vec![push, claim])
        .await
        .unwrap();

    let target = CohortLeaseTarget {
        cohort_id: cohort_id.clone(),
        cohort_lease_token: token,
    };
    let sqlite_members =
        AsyncProjectionStore::cohort_lease_validate(&sqlite, shard.clone(), target.clone(), ts(20))
            .await
            .unwrap();
    let turso_members =
        AsyncProjectionStore::cohort_lease_validate(&turso, shard.clone(), target.clone(), ts(20))
            .await
            .unwrap();
    assert_eq!(turso_members, sqlite_members);
    assert_eq!(
        turso_members
            .iter()
            .map(|member| member.item_id)
            .collect::<Vec<_>>(),
        ids
    );

    let wrong_target = CohortLeaseTarget {
        cohort_id: cohort_id.clone(),
        cohort_lease_token: LeaseToken::new("wrong-token").unwrap(),
    };
    assert!(matches!(
        AsyncProjectionStore::cohort_lease_validate(&turso, shard.clone(), wrong_target, ts(20))
            .await,
        Err(EngineError::StaleLease)
    ));

    let renew = envelope(
        QueueCommand::CohortRenewLease(CohortRenewLeaseCommand {
            cohort_id: cohort_id.clone(),
            lease_expires_at: ts(40),
        }),
        ids.to_vec(),
    );
    AsyncProjectionStore::apply_live(
        &sqlite,
        vec![CommandPosition::new(shard.clone(), 0, 2)],
        vec![renew.clone()],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 0, 2)],
        vec![renew],
    )
    .await
    .unwrap();
    let renewed_members =
        AsyncProjectionStore::cohort_lease_validate(&turso, shard.clone(), target, ts(30))
            .await
            .unwrap();
    assert_eq!(
        renewed_members
            .iter()
            .map(|member| member.item_id)
            .collect::<Vec<_>>(),
        ids
    );

    let retry = envelope(
        QueueCommand::CohortFinalize(CohortFinalizeCommand {
            cohort_id,
            kind: FinalizeKind::Retry,
            not_before: Some(ts(50)),
        }),
        ids.to_vec(),
    );
    AsyncProjectionStore::apply_live(
        &sqlite,
        vec![CommandPosition::new(shard.clone(), 0, 3)],
        vec![retry.clone()],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 0, 3)],
        vec![retry],
    )
    .await
    .unwrap();
    for (id, expected) in ids.into_iter().zip([ItemState::Failed, ItemState::Failed]) {
        assert_eq!(
            AsyncProjectionStore::item_state(&sqlite, shard.clone(), id)
                .await
                .unwrap(),
            Some(expected)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&turso, shard.clone(), id)
                .await
                .unwrap(),
            Some(expected)
        );
    }
    sqlite.close_and_drain().await.unwrap();
}
