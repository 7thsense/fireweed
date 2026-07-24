use std::path::{Path, PathBuf};
use std::sync::Arc;

use fireweed::{
    Bytes, ClaimAt, ClaimCompatibility, ClientItemKey, Clock, CohortPolicy, CreateQueue,
    DiscoveryGranularity, EngineError, GroupKey, MultiQueueClaimLimits, MultiQueueClaimTarget,
    NewItem, OldestFirstScopePrefix, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId, QueueKey,
    QueueTemplate, RecurrencePolicy, RetryPolicy, SystemClock, TenantId,
    select_active_scope_from_prefix,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = temporary_database_path("run");
    remove_sqlite_files(&path);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run_workflow(&path));
    remove_sqlite_files(&path);
    result
}

async fn run_workflow(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let pq = fireweed::open_sqlite_relational(path.to_str().expect("UTF-8 temp path"), clock)?;
    let deliveries = queue("deliveries");
    let maintenance = queue("maintenance");
    let template = queue_template();

    // Queue creation is an explicit control-plane choice. The reusable template injects each target's
    // identity and exact-match checks the durable definition on every ensure.
    let deliveries_policy = pq.ensure_queue(&deliveries, &template).await?.definition;
    pq.ensure_queue(&maintenance, &template).await?;

    // Discovery must expose both grouped and ungrouped eligible work. `None` is real ungrouped work, not
    // an absent descriptor, and the selector reports that it cannot become an exact group claim filter.
    for (key, group, priority) in [
        ("delivery-ungrouped", None, 10),
        ("delivery-campaign", Some("campaign-a"), 20),
    ] {
        pq.push(
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
        pq.push(
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

    let primary_claim = match pq
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
    let mut results = pq
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

    pq.complete(
        &deliveries,
        delivery_claim.items.iter().map(|item| item.item_id),
    )
    .await?;
    pq.retry(&maintenance, [maintenance_claim.items[0].item_id], None)
        .await?;
    pq.release(&maintenance, [maintenance_claim.items[1].item_id])
        .await?;

    // Immediate retry and release both become ordinarily claimable again; finish every lease so the
    // runnable example leaves no in-flight work behind.
    let retried = pq.claim(&maintenance, 2, 30_000).await?;
    pq.complete(&maintenance, retried.iter().map(|item| item.item_id))
        .await?;
    Ok(())
}

fn queue(queue_id: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("tenant-a").expect("valid tenant"),
        QueueId::new(queue_id).expect("valid queue"),
    )
}

fn queue_template() -> QueueTemplate {
    QueueTemplate::new(
        CreateQueue {
            // Template identity is intentionally discarded when it resolves a concrete QueueKey.
            tenant_id: TenantId::new("template").expect("valid template tenant"),
            queue_id: QueueId::new("template").expect("valid template queue"),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: Default::default(),
            cohort_policy: CohortPolicy::disabled(),
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 600_000,
            client_item_key_retention_ms: 600_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records: true,
        },
        QueueCreationPolicy::default(),
    )
    .with_name("worker-queues")
    .with_revision("v1")
}

fn temporary_database_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "fireweed-scheduler-boundary-{label}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn remove_sqlite_files(path: &Path) {
    for suffix in ["", "-shm", "-wal"] {
        let candidate = PathBuf::from(format!("{}{}", path.display(), suffix));
        if let Err(error) = std::fs::remove_file(&candidate)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("could not remove {}: {error}", candidate.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_workflow_runs_end_to_end() {
        let path = temporary_database_path("test");
        remove_sqlite_files(&path);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(run_workflow(&path)).unwrap();
        remove_sqlite_files(&path);
    }
}
