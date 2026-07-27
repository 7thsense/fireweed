// Provenance: crates/fireweed/examples/scheduler_boundary.rs::run_workflow
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn run_workflow(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let fireweed =
        fireweed::open_sqlite_relational(path.to_str().expect("UTF-8 temp path"), clock)?;
    let deliveries = queue("deliveries");
    let maintenance = queue("maintenance");
    let template = queue_template();

    // Queue creation is an explicit control-plane choice. The reusable template injects each target's
    // identity and exact-match checks the durable definition on every ensure.
    let deliveries_policy = fireweed
        .ensure_queue(&deliveries, &template)
        .await?
        .definition;
    fireweed.ensure_queue(&maintenance, &template).await?;

    // Discovery must expose both grouped and ungrouped eligible work. `None` is real ungrouped work, not
    // an absent descriptor, and the selector reports that it cannot become an exact group claim filter.
    for (key, group, priority) in [
        ("delivery-ungrouped", None, 10),
        ("delivery-campaign", Some("campaign-a"), 20),
    ] {
        fireweed
            .push(
                &deliveries,
                NewItem {
                    client_item_key: Some(ClientItemKey::new(key)?),
                    group_key: group.map(GroupKey::new).transpose()?,
                    priority: Some(PriorityValue::Int64(priority)),
                    payload: Some(Bytes::from_static(b"deliver message")),
                    ..Default::default()
                },
            )
            .await?;
    }
    for n in 0..2 {
        fireweed
            .push(
                &maintenance,
                NewItem {
                    client_item_key: Some(ClientItemKey::new(format!("maintenance-{n}"))?),
                    priority: Some(PriorityValue::Int64(n)),
                    payload: Some(Bytes::from_static(b"perform maintenance")),
                    ..Default::default()
                },
            )
            .await?;
    }

    let primary_claim = match fireweed
        .discover_active_scopes_stamped(&deliveries, DiscoveryGranularity::Group)
        .await
    {
        Ok(discovery) => {
            if !discovery
                .scopes
                .iter()
                .any(|scope| scope.group_key.is_none())
                || !discovery
                    .scopes
                    .iter()
                    .any(|scope| scope.group_key.is_some())
            {
                return Err("expected grouped and ungrouped discovery descriptors".into());
            }
            let prefix = OldestFirstScopePrefix::attest(discovery)?;
            let selected = select_active_scope_from_prefix(
                &prefix,
                &deliveries,
                b"worker-17",
                8,
                deliveries_policy.progress_bound_ms,
                250,
                1_000,
            )?;
            println!(
                "advisory scope index={} group={:?} exact_group_filter={}",
                selected.index, selected.scope.group_key, selected.group_filter_available
            );
            let compatibility = if selected.group_filter_available {
                ClaimCompatibility {
                    group_key: selected
                        .scope
                        .group_key
                        .as_deref()
                        .map(GroupKey::new)
                        .transpose()?,
                    ..Default::default()
                }
            } else {
                ClaimCompatibility::default()
            };
            ClaimAt::new(1, 30_000).compatibility(compatibility)
        }
        // Discovery is an optional relational capability. A caller must retain an ordinary claim path
        // rather than treating an unavailable advisory read as a queue failure.
        Err(EngineError::Unavailable) => ClaimAt::new(1, 30_000),
        Err(error) => return Err(error.into()),
    };

    // Fan-in is bounded caller-side orchestration, not a cross-queue transaction. Results stay correlated
    // with input order and each successful target owns an independent queue-local lease.
    let mut results = fireweed
        .claim_across_queues(
            vec![
                MultiQueueClaimTarget {
                    queue: deliveries.clone(),
                    claim: primary_claim,
                },
                MultiQueueClaimTarget {
                    queue: maintenance.clone(),
                    claim: ClaimAt::new(2, 30_000),
                },
            ],
            MultiQueueClaimLimits {
                max_targets: 2,
                max_total_items: 3,
            },
        )
        .await?
        .into_iter();
    let delivery_claim = results.next().expect("delivery result").result?;
    let maintenance_claim = results.next().expect("maintenance result").result?;
    assert!(results.next().is_none());
    assert_eq!(delivery_claim.items.len(), 1);
    assert_eq!(maintenance_claim.items.len(), 2);

    fireweed
        .complete(
            &deliveries,
            delivery_claim.items.iter().map(|item| item.item_id),
        )
        .await?;
    fireweed
        .retry(&maintenance, [maintenance_claim.items[0].item_id], None)
        .await?;
    fireweed
        .release(&maintenance, [maintenance_claim.items[1].item_id])
        .await?;

    // Immediate retry and release both become ordinarily claimable again; finish every lease so the
    // runnable example leaves no in-flight work behind.
    let retried = fireweed.claim(&maintenance, 2, 30_000).await?;
    fireweed
        .complete(&maintenance, retried.iter().map(|item| item.item_id))
        .await?;
    Ok(())
}
