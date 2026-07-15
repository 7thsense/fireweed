//! Restart-reconciliation test (bead pqueue-b29435b2): a durable SQLite-backed backend with an
//! advanced storage epoch is paired with a fresh `InMemoryControlPlane` (simulating process restart).
//! `acquire_and_fence` reconciles the gap, the queue serves push/claim, and stale pre-restart writes
//! are `EpochFenced`.

use pqueue_conformance::{claim_req, qdef, shard, ts};
use pqueue_core::OwnerId;
use pqueue_engine::{
    ClaimPort, ControlPlaneConfig, ControlPlaneStore, EngineError, InMemoryControlPlane,
    OwnershipOutcome, ProjectionRead, PushPort, PushSpec, QueueControlPlane, acquire_and_fence,
};
use pqueue_sqlite::SqliteRelationalBackend;

/// After a simulated restart (fresh CP + durable backend with elevated storage epoch),
/// `acquire_and_fence` reconciles the gap, the queue serves a real push/claim, and stale
/// pre-restart operations are `EpochFenced`.
#[tokio::test]
async fn ownership_restart_reacquire_serves_push_claim() {
    let storage = SqliteRelationalBackend::in_memory().unwrap();
    storage.create_queue(qdef()).await.unwrap();

    // Advance the storage epoch above genesis, simulating a pre-restart ownership history.
    let pre_epoch = storage.acquire_epoch(&shard()).await.unwrap();
    assert!(pre_epoch >= 1, "pre-restart epoch advances");
    // Double-advance to simulate at least two ownership changes before restart.
    let pre_epoch2 = storage.acquire_epoch(&shard()).await.unwrap();
    assert!(pre_epoch2 > pre_epoch);
    assert_eq!(
        storage.current_epoch(&shard()).await.unwrap(),
        pre_epoch2,
        "storage retains the advanced epoch"
    );

    // Simulated restart: fresh InMemoryControlPlane (no state), same durable backend.
    let cp = InMemoryControlPlane::new(ControlPlaneConfig::default());
    let owner = OwnerId::new("node-a").unwrap();
    cp.register_owner(&owner, ts(0)).unwrap();

    // The fresh CP assigns epoch 1, but the durable backend is at pre_epoch2 (> 1).
    // acquire_and_fence must reconcile without EpochFenced.
    let OwnershipOutcome::Owned(session) =
        acquire_and_fence(&cp, &storage, &shard(), &owner, ts(0))
            .await
            .unwrap()
    else {
        panic!("expected Owned after restart reconciliation");
    };
    assert!(
        session.fence_epoch > pre_epoch2,
        "fence_epoch exceeds pre-restart epoch"
    );
    assert_eq!(
        session.lease_epoch, 1,
        "lease epoch is the fresh CP assignment (1)"
    );
    assert_eq!(
        storage.current_epoch(&shard()).await.unwrap(),
        session.fence_epoch,
        "durable storage epoch matches the new fence"
    );

    // Push at the reconciled epoch succeeds.
    storage
        .push(
            &shard(),
            vec![PushSpec::default()],
            ts(1),
            Some(session.fence_epoch),
        )
        .await
        .unwrap();
    assert_eq!(storage.metrics(&shard()).await.unwrap().pending, 1);

    // Claim at the reconciled epoch succeeds.
    let claimed = storage
        .claim(claim_req(10, 60_000, ts(1).seconds))
        .await
        .unwrap();
    assert_eq!(claimed.items.len(), 1, "claimed the pushed item");

    // Stale pre-restart push (at pre_epoch2) is EpochFenced.
    assert!(
        matches!(
            storage
                .push(&shard(), vec![PushSpec::default()], ts(2), Some(pre_epoch2),)
                .await,
            Err(EngineError::EpochFenced)
        ),
        "stale pre-restart push must be EpochFenced"
    );

    // Stale pre-restart claim is EpochFenced.
    let stale_req = pqueue_engine::ClaimRequest {
        expected_epoch: Some(pre_epoch2),
        ..claim_req(10, 60_000, ts(2).seconds)
    };
    assert!(
        matches!(
            storage.claim(stale_req).await,
            Err(EngineError::EpochFenced)
        ),
        "stale pre-restart claim must be EpochFenced"
    );

    // Current-epoch operations still succeed.
    assert_eq!(
        storage.metrics(&shard()).await.unwrap().pending,
        0,
        "the stale claim fenced nothing"
    );
}
