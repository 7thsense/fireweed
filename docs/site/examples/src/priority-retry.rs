// Provenance: crates/fireweed/tests/whitebox/facade.rs::push_claim_ack_nack_lifecycle_over_memory
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn push_claim_ack_nack_lifecycle_over_memory() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    // push out of priority order.
    for p in [30, 10, 20] {
        fireweed.push(&q, at(p)).await.unwrap();
    }

    // peek is priority-ordered (ascending Int64): 10, 20, 30.
    let peeked: Vec<i64> = fireweed
        .peek(&q, 10)
        .await
        .unwrap()
        .iter()
        .map(|v| match v.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(peeked, vec![10, 20, 30]);

    // claim 2 highest-priority → 10, 20; both leased.
    let claimed = fireweed.claim(&q, 2, 30_000).await.unwrap();
    let claimed_pri: Vec<i64> = claimed
        .iter()
        .map(|c| match c.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(claimed_pri, vec![10, 20]);
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!((m.pending, m.leased), (1, 2));

    // ack them → complete.
    fireweed
        .ack(&q, claimed.iter().map(|c| c.item_id))
        .await
        .unwrap();
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!((m.complete, m.leased), (2, 0));

    // claim the last (30), nack Retry → back to pending, claimable again with a bumped attempt.
    let last = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].attempt_count, 1);
    fireweed
        .nack(
            &q,
            last.iter().map(|c| c.item_id),
            Nack::Retry { not_before: None },
        )
        .await
        .unwrap();
    let again = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(again.len(), 1, "retried item is claimable again");
    assert!(again[0].attempt_count > 1, "redelivery bumps attempt_count");
}
