//! CONTRACT-006 cross-boundary witnesses over the real log/Turso adapter.
//! Local publication exercises the shared log protocol, not public S3 qualification.
use super::*;
use fireweed_core::{
    EligibilityPolicy, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, RecurrencePolicy, RetryPolicy, WorkerId,
};
use fireweed_engine::{
    ControlPlaneConfig, ControlPlaneStore, InMemoryControlPlane, OwnershipOutcome,
    QueueControlPlane, acquire_and_fence,
};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(1);

fn definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("formal").unwrap(),
        queue_id: QueueId::new("claims").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    }
}

async fn backend() -> (DerivedObjectLogTursoBackend, std::path::PathBuf, QueueKey) {
    let root = std::env::temp_dir().join(format!(
        "fireweed-formal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let log = ObjectLogEngineStore::open_local(
        &root.join("log"),
        flush_config_from_segment(256 * 1024, 50),
    )
    .await
    .unwrap();
    let path = root.join("projection.db");
    let projection = open_turso_projection_async(&path).await.unwrap();
    let backend = DerivedObjectLogTursoBackend::from_log_and_projection(
        log,
        projection,
        path,
        0,
        Some(AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 3).unwrap()),
    )
    .await
    .unwrap();
    let definition = definition();
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    backend.create_queue(definition).await.unwrap();
    backend
        .push(
            &queue,
            vec![PushSpec::default(), PushSpec::default()],
            UtcTimestamp::new(1, 0).unwrap(),
            None,
        )
        .await
        .unwrap();
    backend.peek(&queue, 2).await.unwrap();
    (backend, root, queue)
}

fn request(queue: &QueueKey, id: &str, token: &str, epoch: u64) -> ClaimRequest {
    ClaimRequest {
        shard: queue.clone(),
        worker_id: WorkerId::new("worker").unwrap(),
        max_items: 1,
        lease_token: LeaseToken::new(token).unwrap(),
        lease_expires_at: UtcTimestamp::new(30, 0).unwrap(),
        now: UtcTimestamp::new(2, 0).unwrap(),
        eligibility_time: None,
        compatibility: ClaimCompatibility::default(),
        expected_epoch: Some(epoch),
        request_id: Some(RequestId::new(id).unwrap()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_claim_response_deferred_apply_handoff_replays_original_lease() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (backend, root, queue) = backend().await;
        let cp = InMemoryControlPlane::new(ControlPlaneConfig::default());
        let a = OwnerId::new("a").unwrap();
        let b = OwnerId::new("b").unwrap();
        let now = UtcTimestamp::new(2, 0).unwrap();
        cp.register_owner(&a, now).unwrap();
        cp.register_owner(&b, now).unwrap();
        let OwnershipOutcome::Owned(first_owner) =
            acquire_and_fence(&cp, &backend, &queue, &a, now)
                .await
                .unwrap()
        else {
            panic!("first acquire rejected");
        };
        let coordinator = backend.async_apply.as_ref().unwrap();
        coordinator.pause();
        let applied = coordinator.snapshot(&queue).await.applied_high_water;
        let original = request(
            &queue,
            "lost-response",
            "first-token",
            first_owner.fence_epoch,
        );
        let claimed = backend.claim(original.clone()).await.unwrap();
        // Capture the server's result as an oracle, then simulate its loss to the
        // client by retrying only the request ID after ownership changes.
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(
            coordinator.snapshot(&queue).await.applied_high_water,
            applied
        );
        cp.release_queue_lease(&queue, &a, first_owner.lease_epoch, now)
            .unwrap();
        let OwnershipOutcome::Owned(second_owner) =
            acquire_and_fence(&cp, &backend, &queue, &b, now)
                .await
                .unwrap()
        else {
            panic!("handoff acquire rejected");
        };
        assert!(second_owner.fence_epoch > first_owner.fence_epoch);
        coordinator.resume();
        backend.peek(&queue, 2).await.unwrap();
        let before = AsyncLogStore::high_water(backend.log.as_ref(), queue.clone())
            .await
            .unwrap();
        let replay = backend
            .claim(ClaimRequest {
                expected_epoch: Some(second_owner.fence_epoch),
                lease_token: LeaseToken::new("retry-token").unwrap(),
                ..original
            })
            .await
            .unwrap();
        assert_eq!(replay.items.len(), 1);
        assert_eq!(replay.items[0].item_id, claimed.items[0].item_id);
        assert_eq!(replay.items[0].lease_token, claimed.items[0].lease_token);
        assert_eq!(
            AsyncLogStore::high_water(backend.log.as_ref(), queue.clone())
                .await
                .unwrap(),
            before
        );
        // A stale owner cannot create another lease after the handoff, even with
        // a fresh request ID. The original item lease itself remains valid.
        assert!(matches!(
            backend
                .claim(request(
                    &queue,
                    "stale",
                    "stale-token",
                    first_owner.fence_epoch
                ))
                .await,
            Err(EngineError::EpochFenced)
        ));
        backend.close_and_drain().await.unwrap();
        drop(backend);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("handoff/replay witness timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_projection_competing_claims_keep_disjoint_tokens_and_items() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (backend, root, queue) = backend().await;
        let epoch = backend.current_epoch(&queue).await.unwrap();
        let coordinator = backend.async_apply.as_ref().unwrap();
        coordinator.pause();
        let applied = coordinator.snapshot(&queue).await.applied_high_water;
        let (a, b) = tokio::join!(
            backend.claim(request(&queue, "a", "token-a", epoch)),
            backend.claim(request(&queue, "b", "token-b", epoch)),
        );
        assert_eq!(
            coordinator.snapshot(&queue).await.applied_high_water,
            applied
        );
        assert!(coordinator.snapshot(&queue).await.apply_lag_commands >= 2);
        assert_ne!(
            AsyncLogStore::high_water(backend.log.as_ref(), queue.clone())
                .await
                .unwrap(),
            applied
        );
        coordinator.resume();
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!((a.items.len(), b.items.len()), (1, 1));
        assert_ne!(a.items[0].item_id, b.items[0].item_id);
        assert_ne!(a.items[0].lease_token, b.items[0].lease_token);
        // The capture predates both claims; their responses were obtainable with
        // apply paused, so reservations had to protect the same stale snapshot.
        assert!(applied.is_some());
        backend.peek(&queue, 2).await.unwrap();
        backend.close_and_drain().await.unwrap();
        drop(backend);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("competing claim witness timed out");
}
