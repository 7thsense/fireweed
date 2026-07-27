// Provenance: crates/fireweed/tests/multi_queue_claim.rs::memory_claims_share_time_and_preserve_input_order
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn memory_claims_share_time_and_preserve_input_order() {
    let clock = Arc::new(ManualClock::at(17));
    let fireweed = fireweed::open_memory(clock);
    let a = queue("a");
    let b = queue("b");
    for (key, id) in [(&a, "a"), (&b, "b")] {
        fireweed.create_queue(definition(id)).await.unwrap();
        fireweed.push(key, NewItem::default()).await.unwrap();
    }

    let results = fireweed
        .claim_across_queues(
            vec![target(b.clone(), 1), target(a.clone(), 1)],
            MultiQueueClaimLimits::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        results.iter().map(|entry| &entry.queue).collect::<Vec<_>>(),
        vec![&b, &a]
    );
    for entry in results {
        let claimed = entry.result.unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(claimed.items[0].lease_expires_at.seconds, 47);
    }
}
