// Provenance: crates/fireweed/tests/product_validation_tests.rs::scheduled_batch_delivery_profile
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn scheduled_batch_delivery_profile<B: LibBackend>(
    fireweed: &RuntimeCore<B>,
    clock: Arc<ManualClock>,
    tenant: &str,
) -> ScheduledProfileEvidence {
    let q = qk(tenant, "campaign");
    fireweed
        .create_queue(qdef(
            tenant,
            "campaign",
            PriorityDirection::Ascending,
            OrderingMode::Strict,
        ))
        .await
        .unwrap();

    let actions = [
        ("complete", 10i64),
        ("fail", 20),
        ("retry", 30),
        ("release", 40),
        ("rearm", 50),
    ];
    for &(outcome, due) in &actions {
        let key = ClientItemKey::new(format!("{tenant}-{outcome}")).unwrap();
        let item = NewItem {
            client_item_key: Some(key),
            priority: Some(PriorityValue::Int64(due)),
            not_before: Some(ts(due)),
            payload: Some(Bytes::from(outcome.as_bytes().to_vec())),
            ..Default::default()
        };
        fireweed.push(&q, item).await.unwrap();
    }
    assert!(
        fireweed.claim(&q, 10, 60_000).await.unwrap().is_empty(),
        "not_before prevents early delivery"
    );

    clock.set(100);
    let mut delivered_order = Vec::new();
    let mut delivered_ids = Vec::new();
    let mut max_items_pacing_observed = true;
    let mut stable_client_keys_observed = true;

    let complete = claim_one(fireweed, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&complete), "complete");
    assert_eq!(
        complete.client_item_key.as_str(),
        format!("{tenant}-complete")
    );
    fireweed.ack(&q, [complete.item_id]).await.unwrap();

    let failed = claim_one(fireweed, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&failed), "fail");
    stable_client_keys_observed &= failed.client_item_key.as_str() == format!("{tenant}-fail");
    fireweed.fail(&q, [failed.item_id]).await.unwrap();

    let retry = claim_one(fireweed, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&retry), "retry");
    stable_client_keys_observed &= retry.client_item_key.as_str() == format!("{tenant}-retry");
    fireweed
        .nack(
            &q,
            [retry.item_id],
            Nack::Retry {
                not_before: Some(ts(130)),
            },
        )
        .await
        .unwrap();

    let release = claim_one(fireweed, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&release), "release");
    stable_client_keys_observed &= release.client_item_key.as_str() == format!("{tenant}-release");
    fireweed
        .nack(&q, [release.item_id], Nack::Release)
        .await
        .unwrap();
    let release_again = fireweed.claim(&q, 1, 60_000).await.unwrap();
    max_items_pacing_observed &= release_again.len() == 1;
    assert_eq!(release_again[0].item_id, release.item_id);
    fireweed.ack(&q, [release_again[0].item_id]).await.unwrap();

    let rearm = claim_one(fireweed, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&rearm), "rearm");
    stable_client_keys_observed &= rearm.client_item_key.as_str() == format!("{tenant}-rearm");
    fireweed.rearm(&q, [rearm.item_id]).await.unwrap();
    let rearm_again = fireweed.claim(&q, 1, 60_000).await.unwrap();
    max_items_pacing_observed &= rearm_again.len() == 1;
    assert_eq!(rearm_again[0].item_id, rearm.item_id);
    fireweed.ack(&q, [rearm_again[0].item_id]).await.unwrap();

    clock.set(120);
    assert!(
        fireweed.claim(&q, 1, 60_000).await.unwrap().is_empty(),
        "retry backoff is caller-chosen not_before, not fireweed rate admission"
    );
    clock.set(130);
    let retry_again = fireweed.claim(&q, 1, 60_000).await.unwrap();
    max_items_pacing_observed &= retry_again.len() == 1;
    assert_eq!(retry_again[0].item_id, retry.item_id);
    fireweed.ack(&q, [retry_again[0].item_id]).await.unwrap();

    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (m.complete, m.failed, m.pending, m.leased),
        (4, 1, 0, 0),
        "all scheduled actions reached terminal state after the five outcome mappings"
    );

    delivered_ids.sort();
    delivered_ids.dedup();
    ScheduledProfileEvidence {
        scheduled_actions: actions.len(),
        delivered_in_schedule_order: delivered_order == [10, 20, 30, 40, 50],
        unique_deliveries: delivered_ids.len(),
        max_items_pacing_observed,
        stable_client_keys_observed,
    }
}
