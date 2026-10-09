//! Async ownership authority for application control planes.
//!
//! Storage and network calls are awaited directly. Pure lease decisions remain
//! synchronous; implementations reuse the decisions in `control_plane`.
use crate::{
    AcquireOutcome, ControlPlaneStore, EngineError, EngineResult, LeaseRenewal,
    LeaseRenewalOutcome, LeaseState, OwnedSession, OwnerEndpointAdvertisement, OwnerResolution,
    OwnershipOutcome, QueueKey, QueueLease,
};
use async_trait::async_trait;
use fireweed_core::{OwnerId, UtcTimestamp};

/// Object-safe asynchronous queue ownership and worker registry.
#[async_trait]
pub trait AsyncQueueControlPlane: Send + Sync {
    /// Register or refresh a worker heartbeat.
    async fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()>;
    /// Register a worker and its reachable endpoint atomically.
    async fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()>;
    /// List live advertised workers.
    async fn live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>>;
    /// Refresh a worker heartbeat.
    async fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()>;
    /// Resolve current and target ownership without fabricating missing reads.
    async fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution>;
    /// Resolve the node inventory in input order.
    async fn resolve_queue_owners(
        &self,
        queues: &[QueueKey],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerResolution>>;
    /// Acquire a live worker's lease before advancing storage fencing.
    async fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome>;
    /// Publish serving ownership only after durable storage fencing/hydration.
    async fn confirm_queue_lease_fence(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease>;
    /// Renew without changing the assignment epoch.
    async fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease>;
    /// Renew a node inventory with independent outcomes in input order.
    async fn renew_queue_leases(
        &self,
        renewals: &[LeaseRenewal],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<LeaseRenewalOutcome>>;
    /// Begin a graceful handoff at the expected epoch.
    async fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease>;
    /// Release ownership while retaining the durable epoch.
    async fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()>;
    /// Read the durable lease or genesis lease for an unknown queue.
    async fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease>;
    /// Whether authority resets on restart; this is pure capability metadata.
    fn is_ephemeral(&self) -> bool {
        false
    }
}

/// Acquire, fence and hydrate through async ports before publishing ownership.
///
/// An authority failure never advances storage. Storage failures release the
/// pending lease; a durable authority behind storage fails closed.
pub async fn acquire_and_fence_async<CP, S>(
    control: &CP,
    storage: &S,
    queue: &QueueKey,
    owner: &OwnerId,
    now: UtcTimestamp,
) -> EngineResult<OwnershipOutcome>
where
    CP: AsyncQueueControlPlane + ?Sized,
    S: ControlPlaneStore + ?Sized,
{
    let prior_owner = if control.is_ephemeral() {
        control.lease(queue).await?.active_owner_id
    } else {
        None
    };
    let lease = match control.acquire_queue_lease(queue, owner, now).await? {
        AcquireOutcome::Rejected(held) => return Ok(OwnershipOutcome::Rejected(held)),
        AcquireOutcome::Acquired(lease) => lease,
    };
    let fenced = async {
        let current = storage.current_epoch(queue).await?;
        let epoch = if current <= lease.assignment_epoch {
            storage.fence_epoch(queue, lease.assignment_epoch).await?
        } else if prior_owner.as_ref() == Some(owner) {
            current
        } else if control.is_ephemeral() {
            storage.acquire_epoch(queue).await?
        } else {
            return Err(EngineError::EpochFenced);
        };
        storage.hydrate_projection_for_ownership(queue).await?;
        if lease.state == LeaseState::PendingFence {
            control
                .confirm_queue_lease_fence(queue, owner, lease.assignment_epoch, now)
                .await?;
        }
        Ok(epoch)
    }
    .await;
    match fenced {
        Ok(fence_epoch) => Ok(OwnershipOutcome::Owned(OwnedSession {
            owner: owner.clone(),
            queue: queue.clone(),
            lease_epoch: lease.assignment_epoch,
            fence_epoch,
        })),
        Err(error) => {
            let _ = control
                .release_queue_lease(queue, owner, lease.assignment_epoch, now)
                .await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CreateQueueOutcome;
    use crate::{ControlPlaneConfig, InMemoryControlPlane, QueueControlPlane};
    use fireweed_core::{QueueDefinition, QueueId, TenantId};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    };

    // Pure in-memory decisions need no I/O offload; only the test port is async.
    struct Plane(Arc<InMemoryControlPlane>);
    #[async_trait]
    impl AsyncQueueControlPlane for Plane {
        async fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
            self.0.register_owner(owner, now)
        }
        async fn advertise_owner_endpoint(
            &self,
            owner: &OwnerId,
            endpoint: &str,
            now: UtcTimestamp,
        ) -> EngineResult<()> {
            self.0.advertise_owner_endpoint(owner, endpoint, now)
        }
        async fn live_owner_endpoints(
            &self,
            now: UtcTimestamp,
        ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
            self.0.live_owner_endpoints(now)
        }
        async fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
            self.0.heartbeat(owner, now)
        }
        async fn resolve_queue_owner(
            &self,
            queue: &QueueKey,
            now: UtcTimestamp,
        ) -> EngineResult<OwnerResolution> {
            self.0.resolve_queue_owner(queue, now)
        }
        async fn resolve_queue_owners(
            &self,
            queues: &[QueueKey],
            now: UtcTimestamp,
        ) -> EngineResult<Vec<OwnerResolution>> {
            self.0.resolve_queue_owners(queues, now)
        }
        async fn acquire_queue_lease(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            now: UtcTimestamp,
        ) -> EngineResult<AcquireOutcome> {
            self.0.acquire_queue_lease(queue, owner, now)
        }
        async fn confirm_queue_lease_fence(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            expected_epoch: u64,
            now: UtcTimestamp,
        ) -> EngineResult<QueueLease> {
            self.0
                .confirm_queue_lease_fence(queue, owner, expected_epoch, now)
        }
        async fn renew_queue_lease(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            expected_epoch: u64,
            now: UtcTimestamp,
        ) -> EngineResult<QueueLease> {
            self.0.renew_queue_lease(queue, owner, expected_epoch, now)
        }
        async fn renew_queue_leases(
            &self,
            renewals: &[LeaseRenewal],
            now: UtcTimestamp,
        ) -> EngineResult<Vec<LeaseRenewalOutcome>> {
            self.0.renew_queue_leases(renewals, now)
        }
        async fn begin_drain(
            &self,
            queue: &QueueKey,
            expected_epoch: u64,
            target_owner: &OwnerId,
            now: UtcTimestamp,
        ) -> EngineResult<QueueLease> {
            self.0.begin_drain(queue, expected_epoch, target_owner, now)
        }
        async fn release_queue_lease(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            expected_epoch: u64,
            now: UtcTimestamp,
        ) -> EngineResult<()> {
            self.0
                .release_queue_lease(queue, owner, expected_epoch, now)
        }
        async fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
            self.0.lease(queue)
        }
        fn is_ephemeral(&self) -> bool {
            true
        }
    }
    struct Storage {
        plane: Arc<InMemoryControlPlane>,
        epoch: AtomicU64,
        phases: Mutex<Vec<&'static str>>,
        fail_hydration: bool,
    }
    impl ControlPlaneStore for Storage {
        async fn create_queue(&self, _: QueueDefinition) -> EngineResult<CreateQueueOutcome> {
            Err(EngineError::Unavailable)
        }
        async fn queue_definition(&self, _: &QueueKey) -> EngineResult<QueueDefinition> {
            Err(EngineError::Unavailable)
        }
        async fn list_queues(&self, _: &TenantId) -> EngineResult<Vec<QueueId>> {
            Err(EngineError::Unavailable)
        }
        async fn current_epoch(&self, _: &QueueKey) -> EngineResult<u64> {
            self.phases.lock().unwrap().push("read");
            Ok(self.epoch.load(Ordering::SeqCst))
        }
        async fn acquire_epoch(&self, _: &QueueKey) -> EngineResult<u64> {
            Ok(self.epoch.fetch_add(1, Ordering::SeqCst) + 1)
        }
        async fn fence_epoch(&self, _: &QueueKey, target: u64) -> EngineResult<u64> {
            self.phases.lock().unwrap().push("fence");
            self.epoch.store(target, Ordering::SeqCst);
            Ok(target)
        }
        async fn hydrate_projection_for_ownership(&self, queue: &QueueKey) -> EngineResult<()> {
            self.phases.lock().unwrap().push("hydrate");
            assert_eq!(self.plane.lease(queue)?.state, LeaseState::PendingFence);
            if self.fail_hydration {
                Err(EngineError::Storage("hydration failed".into()))
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn serving_waits_for_hydration_and_failed_hydration_releases_lease() {
        for fail_hydration in [false, true] {
            futures::executor::block_on(async {
                let plane = Arc::new(InMemoryControlPlane::new(ControlPlaneConfig::default()));
                let port = Plane(Arc::clone(&plane));
                let owner = OwnerId::new("async-owner").unwrap();
                let queue = QueueKey::new(
                    TenantId::new("tenant").unwrap(),
                    QueueId::new("queue").unwrap(),
                );
                let now = UtcTimestamp::new(1, 0).unwrap();
                port.register_owner(&owner, now).await.unwrap();
                let storage = Storage {
                    plane,
                    epoch: AtomicU64::new(0),
                    phases: Mutex::new(Vec::new()),
                    fail_hydration,
                };
                let outcome = acquire_and_fence_async(&port, &storage, &queue, &owner, now).await;
                assert_eq!(
                    *storage.phases.lock().unwrap(),
                    vec!["read", "fence", "hydrate"]
                );
                let lease = port.lease(&queue).await.unwrap();
                if fail_hydration {
                    assert!(
                        matches!(outcome,Err(EngineError::Storage(ref message)) if message=="hydration failed")
                    );
                    assert_eq!(lease.state, LeaseState::Unassigned);
                } else {
                    assert!(matches!(outcome, Ok(OwnershipOutcome::Owned(_))));
                    assert_eq!(lease.state, LeaseState::Assigned);
                }
            });
        }
    }
}
