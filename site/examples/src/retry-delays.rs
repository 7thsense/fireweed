// Provenance: crates/fireweed/tests/facade.rs::retry_aliases_match_absolute_relative_and_exhaustion_behavior
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn retry_aliases_match_absolute_relative_and_exhaustion_behavior() {
    let clock = Arc::new(ManualClock::at(100));
    let fireweed = RuntimeCore::new(Arc::new(composed_memory_backend()), clock.clone());
    let q = qkey();
    let mut definition = qdef();
    definition.retry_policy.max_attempts = 2;
    fireweed.create_queue(definition).await.unwrap();

    for priority in [10, 20, 30, 40] {
        fireweed.push(&q, at(priority)).await.unwrap();
    }
    let claimed = fireweed.claim(&q, 4, 30_000).await.unwrap();
    fireweed
        .retry(&q, [claimed[0].item_id], Some(ts(110)))
        .await
        .unwrap();
    fireweed
        .nack(
            &q,
            [claimed[1].item_id],
            Nack::Retry {
                not_before: Some(ts(110)),
            },
        )
        .await
        .unwrap();
    fireweed
        .retry_after(&q, [claimed[2].item_id], 20_000)
        .await
        .unwrap();
    fireweed
        .nack_retry_after(&q, [claimed[3].item_id], 20_000)
        .await
        .unwrap();

    clock.set(109);
    assert!(fireweed.claim(&q, 4, 30_000).await.unwrap().is_empty());
    clock.set(110);
    let absolute = fireweed.claim(&q, 4, 30_000).await.unwrap();
    assert_eq!(absolute.len(), 2);
    assert_eq!(
        absolute.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        vec![claimed[0].item_id, claimed[1].item_id,]
    );
    fireweed
        .retry(&q, absolute.iter().map(|item| item.item_id), None)
        .await
        .unwrap();
    let exhausted = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        exhausted.failed, 2,
        "retry exhaustion matches nack retry semantics"
    );

    clock.set(119);
    assert!(fireweed.claim(&q, 4, 30_000).await.unwrap().is_empty());
    clock.set(120);
    let relative = fireweed.claim(&q, 4, 30_000).await.unwrap();
    assert_eq!(relative.len(), 2);
    assert_eq!(
        relative.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        vec![claimed[2].item_id, claimed[3].item_id,]
    );
}
