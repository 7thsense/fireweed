//! Shared control plane, manual clocks and fault injection for owner tests.
use super::*;
use async_trait::async_trait;
use fireweed_core::{OwnerId, UtcTimestamp};
use fireweed_engine::{
    AcquireOutcome, AsyncQueueControlPlane, ControlPlaneConfig, EngineError, EngineResult,
    InMemoryControlPlane, LeaseRenewal, LeaseRenewalOutcome, OwnerEndpointAdvertisement,
    OwnerResolution, QueueControlPlane, QueueKey, QueueLease,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

pub(super) const TTL: Duration = Duration::from_secs(15);

/// The lease clock tests drive by hand.
#[derive(Default)]
pub(super) struct ManualLeaseClock(AtomicU64);
impl ManualLeaseClock {
    pub(super) fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_nanos() as u64, Ordering::SeqCst);
    }
}
impl LeaseClock for ManualLeaseClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::SeqCst))
    }
}
/// Wall clock stamped on control-plane calls; moves with the lease clock.
#[derive(Default)]
pub(super) struct ManualWall(AtomicU64);
impl ManualWall {
    pub(super) fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_millis() as u64, Ordering::SeqCst);
    }
}
impl fireweed_engine::Clock for ManualWall {
    fn now(&self) -> UtcTimestamp {
        let millis = self.0.load(Ordering::SeqCst);
        UtcTimestamp::new(
            1_000 + (millis / 1000) as i64,
            ((millis % 1000) * 1_000_000) as u32,
        )
        .unwrap()
    }
}

/// The in-memory control plane behind the async port, with faults.
pub(super) struct TestPlane {
    inner: InMemoryControlPlane,
    /// Every call fails as unavailable.
    pub(super) down: AtomicBool,
    /// The next renewal lands but its reply is lost.
    pub(super) lose_renew_reply: AtomicBool,
    /// Confirmation fails: an entry that acquired cannot finish.
    pub(super) fail_confirm: AtomicBool,
    pub(super) renewals: AtomicU64,
    /// Runs inside a renewal call, before it returns: a slow control plane.
    pub(super) during_renew: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}
impl TestPlane {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryControlPlane::new(ControlPlaneConfig::default()),
            down: AtomicBool::new(false),
            lose_renew_reply: AtomicBool::new(false),
            fail_confirm: AtomicBool::new(false),
            renewals: AtomicU64::new(0),
            during_renew: std::sync::Mutex::new(None),
        })
    }
    fn check(&self) -> EngineResult<()> {
        if self.down.load(Ordering::SeqCst) {
            Err(EngineError::Unavailable)
        } else {
            Ok(())
        }
    }
}
#[async_trait]
impl AsyncQueueControlPlane for TestPlane {
    async fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        self.check()?;
        self.inner.register_owner(owner, now)
    }
    async fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.check()?;
        self.inner.advertise_owner_endpoint(owner, endpoint, now)
    }
    async fn live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
        self.check()?;
        self.inner.live_owner_endpoints(now)
    }
    async fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        self.check()?;
        self.inner.heartbeat(owner, now)
    }
    async fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution> {
        self.check()?;
        self.inner.resolve_queue_owner(queue, now)
    }
    async fn resolve_queue_owners(
        &self,
        queues: &[QueueKey],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerResolution>> {
        self.check()?;
        self.inner.resolve_queue_owners(queues, now)
    }
    async fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        self.check()?;
        self.inner.acquire_queue_lease(queue, owner, now)
    }
    async fn confirm_queue_lease_fence(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.check()?;
        if self.fail_confirm.load(Ordering::SeqCst) {
            return Err(EngineError::Unavailable);
        }
        self.inner
            .confirm_queue_lease_fence(queue, owner, expected_epoch, now)
    }
    async fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.check()?;
        self.renewals.fetch_add(1, Ordering::SeqCst);
        if let Some(hook) = self.during_renew.lock().unwrap().as_ref() {
            hook();
        }
        let renewed = self
            .inner
            .renew_queue_lease(queue, owner, expected_epoch, now)?;
        if self.lose_renew_reply.swap(false, Ordering::SeqCst) {
            return Err(EngineError::Unavailable);
        }
        Ok(renewed)
    }
    async fn renew_queue_leases(
        &self,
        renewals: &[LeaseRenewal],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<LeaseRenewalOutcome>> {
        self.check()?;
        self.inner.renew_queue_leases(renewals, now)
    }
    async fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.check()?;
        self.inner
            .begin_drain(queue, expected_epoch, target_owner, now)
    }
    async fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.check()?;
        self.inner
            .release_queue_lease(queue, owner, expected_epoch, now)
    }
    async fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
        self.check()?;
        self.inner.lease(queue)
    }
    fn is_ephemeral(&self) -> bool {
        true
    }
}

/// One control plane and one time shared by every handle in a test.
#[derive(Clone)]
pub(in crate::queue_recovery_pipeline) struct TestOwnership {
    pub(super) plane: Arc<TestPlane>,
    pub(super) lease_clock: Arc<ManualLeaseClock>,
    pub(super) wall: Arc<ManualWall>,
}
impl TestOwnership {
    pub(in crate::queue_recovery_pipeline) fn new() -> Self {
        Self {
            plane: TestPlane::new(),
            lease_clock: Arc::new(ManualLeaseClock::default()),
            wall: Arc::new(ManualWall::default()),
        }
    }
    /// Move both clocks together.
    pub(super) fn advance(&self, by: Duration) {
        self.lease_clock.advance(by);
        self.wall.advance(by);
    }
    /// A handle's ownership with no skew wait, so tests need no real sleeping.
    pub(in crate::queue_recovery_pipeline) fn handle(&self, name: &str) -> QueueOwnership {
        self.handle_with_skew(name, Duration::ZERO)
    }
    pub(super) fn handle_with_skew(&self, name: &str, skew: Duration) -> QueueOwnership {
        QueueOwnership::new(
            self.plane.clone(),
            OwnerId::new(name).unwrap(),
            self.wall.clone(),
            self.lease_clock.clone(),
            TTL,
        )
        .unwrap()
        .with_skew(skew)
    }
}
