// Provenance: crates/fireweed/tests/facade.rs::renew_extends_lease_without_charging_a_delivery
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn renew_extends_lease_without_charging_a_delivery() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap(); // lease_expires_at = 30s, attempt 1
    let id = claimed[0].item_id;
    assert_eq!(claimed[0].attempt_count, 1);

    // Renew to 60s from now: the lease deadline extends, the delivery count does NOT change.
    fireweed.renew(&q, [id], 60_000).await.unwrap();
    let view = fireweed
        .claimed(&q, std::slice::from_ref(&id))
        .await
        .unwrap();
    assert_eq!(view.len(), 1);
    assert_eq!(view[0].attempt_count, 1, "renew does not charge a delivery");
    assert_eq!(
        view[0].lease_expires_at,
        fireweed_core::UtcTimestamp::new(60, 0).unwrap(),
        "renew extended the lease deadline"
    );
}

// Provenance: crates/fireweed/tests/facade.rs::fail_dead_letters_a_claimed_item
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn fail_dead_letters_a_claimed_item() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    fireweed
        .fail(&q, claimed.iter().map(|c| c.item_id))
        .await
        .unwrap();
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (m.failed, m.leased),
        (1, 0),
        "fail moves the item to terminal failed"
    );
}

// Provenance: crates/fireweed/tests/facade.rs::reclaim_expired_convenience_uses_handle_clock
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn reclaim_expired_convenience_uses_handle_clock() {
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(Arc::new(composed_memory_backend()), clock.clone());
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let id = fireweed.push(&q, at(5)).await.unwrap();
    fireweed.claim(&q, 1, 30_000).await.unwrap(); // lease for 30s
    assert_eq!(fireweed.metrics(&q).await.unwrap().leased, 1);

    // Before the lease expires: nothing to reclaim (half-open — still valid at the boundary).
    clock.set(10);
    assert!(fireweed.reclaim_expired(&q, None).await.unwrap().is_empty());

    // Past the 30s lease: the sweep returns the id and the item is Pending again.
    clock.set(40);
    let reclaimed = fireweed.reclaim_expired(&q, None).await.unwrap();
    assert_eq!(reclaimed, vec![id]);
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!((m.pending, m.leased), (1, 0));
    // Idempotent: a second sweep finds nothing.
    assert!(fireweed.reclaim_expired(&q, None).await.unwrap().is_empty());
    // And the item is claimable again.
    assert_eq!(fireweed.claim(&q, 1, 30_000).await.unwrap().len(), 1);
}
