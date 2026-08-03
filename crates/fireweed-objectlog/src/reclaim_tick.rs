//! Shared bounded lease-reclaim driver for native object-log products.

use fireweed_core::UtcTimestamp;
use fireweed_engine::{
    ControlPlane, EngineResult, InProcessControlPlane, InProcessProjectionStore, ProjectionStore,
    ReclaimPort, TickReport,
};

/// Maximum expired projection rows inspected by one native object-log tick.
///
/// The projection page may span up to this many queues. Durable projection families implement the
/// page as a storage-level keyset query; the memory family uses the bounded in-process fallback.
/// A subsequent tick scans the oldest rows still expired, so the bound guarantees finite work and
/// oldest-first progress instead of draining an arbitrarily large backlog in one call.
pub(crate) const EXPIRED_LEASE_SCAN_LIMIT: usize = 128;

/// Reclaim one bounded projection page through the product's queue-scoped reclaim path.
///
/// Each queue operation is capped by both the rows observed in the global page and that queue's
/// `max_claim_batch_size`. `ReclaimPort` retains the product's existing per-queue permit, so reclaim
/// still serializes with claims. Only ids returned after a committed reclaim contribute to the report.
pub(crate) async fn tick_expired_leases<S, B>(
    projection: &InProcessProjectionStore<S>,
    control: &InProcessControlPlane,
    backend: &B,
    now: UtcTimestamp,
) -> EngineResult<TickReport>
where
    S: ProjectionStore + Send + 'static,
    B: ReclaimPort + Sync,
{
    let page = projection
        .run_with_store(move |projection| {
            ProjectionStore::expired_leases_page(
                projection,
                now,
                None,
                EXPIRED_LEASE_SCAN_LIMIT,
                None,
            )
        })
        .await?;

    let mut leases_reclaimed = 0u64;
    for (shard, expired_ids) in page.leases {
        let definition = ControlPlane::queue_definition(control, &shard)?;
        let queue_limit = usize::try_from(definition.max_claim_batch_size).unwrap_or(usize::MAX);
        let limit = expired_ids.len().min(queue_limit);
        if limit == 0 {
            continue;
        }
        let reclaimed =
            ReclaimPort::reclaim_expired(backend, &shard, Some(limit), now, None).await?;
        leases_reclaimed = leases_reclaimed.saturating_add(reclaimed.len() as u64);
    }

    Ok(TickReport {
        leases_reclaimed,
        ..TickReport::default()
    })
}
