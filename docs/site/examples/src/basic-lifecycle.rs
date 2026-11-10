// Provenance: crates/fireweed/examples/queue_lifecycle.rs::schema
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn schema() -> QueueSchema {
    QueueSchema {
        work: CollectionDefinition {
            fields: BTreeMap::from([("kind".into(), FieldType::Text)]),
            indexes: BTreeMap::new(),
            unique_indexes: BTreeSet::new(),
            additional_field_prefixes: BTreeSet::new(),
        },
        state: BTreeMap::new(),
        progress_bound_nanos: 1_000_000_000,
        max_attempts: 3,
        retention: QueueRetention {
            request_id_nanos: 86_400_000_000_000,
            deleted_keys: DeletedKeyPolicy::Reusable,
        },
    }
}

// Provenance: crates/fireweed/examples/queue_lifecycle.rs::push_claim_complete
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn push_claim_complete(queue: &Queue, address: &QueueAddress) -> QueueResult<()> {
    queue
        .provision(ProvisionRequest {
            queue: address.clone(),
            schema: schema(),
        })
        .await?;
    queue
        .insert(InsertRequest {
            queue: address.clone(),
            request_id: "load-1".into(),
            mode: InsertMode::InsertOnly,
            items: vec![job("welcome", 5, 0), job("digest", 1, 0)],
            issued_at: now(),
        })
        .await?;

    // Lower priority values are claimed first under `PriorityAscending`.
    let claimed = queue
        .claim(claim_request(address, "claim-1", now()))
        .await?;
    assert_eq!(claimed[0].record.work.key, "digest");

    for (index, work) in claimed.iter().enumerate() {
        let receipts = queue
            .commit(settle(
                address,
                &format!("done-{index}"),
                work,
                Disposition::Complete,
            ))
            .await?;
        assert!(matches!(
            receipts[0].outcome,
            GroupOutcome::Committed { .. }
        ));
    }
    let stats = queue
        .stats(StatsRequest {
            buckets: None,
            queue: address.clone(),
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: now(),
        })
        .await?;
    assert_eq!(stats.totals.completed, 2);
    Ok(())
}
