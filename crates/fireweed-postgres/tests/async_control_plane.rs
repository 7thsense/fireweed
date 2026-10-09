use fireweed_core::{OwnerId, QueueId, TenantId, UtcTimestamp};
use fireweed_engine::{
    AcquireOutcome, AsyncQueueControlPlane, ControlPlaneConfig, LeaseRenewal, LeaseRenewalOutcome,
    LeaseState, QueueKey,
};
use fireweed_postgres::AsyncPostgresControlPlane;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires an explicit limited-role PostgreSQL fixture URL"]
async fn native_async_authority_serializes_acquire_and_reconnects() {
    let url = std::env::var("FIREWEED_CONTROL_PLANE_TEST_URL").expect("explicit PG fixture URL");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("async_control_{nonce}");
    let a =
        AsyncPostgresControlPlane::connect_in_schema(&url, &schema, ControlPlaneConfig::default())
            .await
            .unwrap();
    let b =
        AsyncPostgresControlPlane::connect_in_schema(&url, &schema, ControlPlaneConfig::default())
            .await
            .unwrap();
    let owner_a = OwnerId::new("a").unwrap();
    let owner_b = OwnerId::new("b").unwrap();
    let now = UtcTimestamp::new(10, 0).unwrap();
    a.register_owner(&owner_a, now).await.unwrap();
    b.register_owner(&owner_b, now).await.unwrap();
    let key = QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("race").unwrap(),
    );
    let (left, right) = tokio::join!(
        a.acquire_queue_lease(&key, &owner_a, now),
        b.acquire_queue_lease(&key, &owner_b, now)
    );
    let (owner, lease) = match (left.unwrap(), right.unwrap()) {
        (AcquireOutcome::Acquired(lease), AcquireOutcome::Rejected(_)) => (owner_a, lease),
        (AcquireOutcome::Rejected(_), AcquireOutcome::Acquired(lease)) => (owner_b, lease),
        other => panic!("exactly one independent connection must acquire: {other:?}"),
    };
    assert_eq!(lease.state, LeaseState::PendingFence);
    a.confirm_queue_lease_fence(&key, &owner, lease.assignment_epoch, now)
        .await
        .unwrap();
    let missing = QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("missing").unwrap(),
    );
    let resolved = a
        .resolve_queue_owners(&[key.clone(), missing.clone()], now)
        .await
        .unwrap();
    assert_eq!(resolved.len(), 2);
    assert_eq!(a.diagnostics().batch_resolution_statements, 1);
    let valid = LeaseRenewal {
        queue: key.clone(),
        owner: owner.clone(),
        expected_epoch: lease.assignment_epoch,
    };
    let invalid = LeaseRenewal {
        expected_epoch: lease.assignment_epoch + 1,
        ..valid.clone()
    };
    let absent = LeaseRenewal {
        queue: missing,
        ..valid.clone()
    };
    let renewals = a
        .renew_queue_leases(&[valid, invalid, absent], UtcTimestamp::new(11, 0).unwrap())
        .await
        .unwrap();
    assert!(matches!(renewals[0], LeaseRenewalOutcome::Renewed(_)));
    assert!(matches!(renewals[1], LeaseRenewalOutcome::Fenced));
    assert!(matches!(renewals[2], LeaseRenewalOutcome::Missing));
    assert_eq!(a.diagnostics().batch_renewal_transactions, 1);
    assert_eq!(a.diagnostics().batch_renewal_statements, 1);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    let reopened =
        AsyncPostgresControlPlane::connect_in_schema(&url, &schema, ControlPlaneConfig::default())
            .await
            .unwrap();
    let restored = reopened.lease(&key).await.unwrap();
    assert_eq!(restored.active_owner_id, Some(owner));
    assert_eq!(restored.assignment_epoch, lease.assignment_epoch);
    assert_eq!(restored.state, LeaseState::Assigned);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires an explicit limited-role PostgreSQL fixture URL"]
async fn row_lock_wait_is_async_and_cancelled_transaction_rolls_back() {
    use std::sync::Arc;
    use std::time::Duration;
    let url = std::env::var("FIREWEED_CONTROL_PLANE_TEST_URL").unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("async_cancel_{nonce}");
    let plane = Arc::new(
        AsyncPostgresControlPlane::connect_in_schema(&url, &schema, ControlPlaneConfig::default())
            .await
            .unwrap(),
    );
    let owner = OwnerId::new("cancel-owner").unwrap();
    let now = UtcTimestamp::new(10, 0).unwrap();
    let queue = QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("locked").unwrap(),
    );
    plane.register_owner(&owner, now).await.unwrap();
    let AcquireOutcome::Acquired(lease) = plane
        .acquire_queue_lease(&queue, &owner, now)
        .await
        .unwrap()
    else {
        panic!("initial ownership");
    };
    plane
        .confirm_queue_lease_fence(&queue, &owner, lease.assignment_epoch, now)
        .await
        .unwrap();
    let before = plane.lease(&queue).await.unwrap();
    let (mut locker, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let tx = locker.transaction().await.unwrap();
    tx.query(&format!("SELECT state FROM \"{schema}\".fireweed_queue_owner WHERE tenant='tenant' AND queue='locked' FOR UPDATE"), &[]).await.unwrap();
    let handle = Arc::clone(&plane);
    let mut pending = tokio::spawn(async move {
        handle
            .renew_queue_lease(
                &queue,
                &owner,
                lease.assignment_epoch,
                UtcTimestamp::new(11, 0).unwrap(),
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err(),
        "locked renewal must await without blocking the single runtime thread"
    );
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    tx.rollback().await.unwrap();
    let key = QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("locked").unwrap(),
    );
    let after = tokio::time::timeout(Duration::from_secs(3), plane.lease(&key))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after, before,
        "cancelled renewal must leave the durable lease unchanged"
    );
    drop(locker);
    driver.await.unwrap().unwrap();
    let owned = match Arc::try_unwrap(plane) {
        Ok(owned) => owned,
        Err(_) => panic!("cancelled task retained authority"),
    };
    owned.shutdown().await.unwrap();
}
