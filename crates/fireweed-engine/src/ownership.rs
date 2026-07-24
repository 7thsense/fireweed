//! Queue-ownership orchestration PRIMITIVES (TD-003 §Recovery + §Per-Queue Progress Bound owner-liveness).
//!
//! This module is the engine-level CORE that a server owner-runtime will build on (the full fireweed-server
//! wiring is tracked as a follow-up — see SCOPE below). It is NOT itself the data-plane fence.
//!
//! 1. [`acquire_and_fence`] — the lease↔fence binding primitive. TD-003 Recovery step 1 / the Single
//!    Authoritative Fencing Rule: a new owner acquires the lease AND durably advances the storage epoch
//!    before serving. On backends whose control-plane acquire transaction already advanced the storage
//!    fence, this reuses that value; otherwise it advances the storage epoch after a successful
//!    `acquire_queue_lease`. It returns the [`OwnedSession`] whose `fence_epoch` is the value the owner is
//!    meant to stamp on every data-plane write.
//!
//!    FENCE SCOPE (pqueue-7bac12ce closes the BQ-20/21/22 deferral): Every data-plane port
//!    (`ClaimPort`/`PushPort`/`FinalizePort`/`RenewLeasePort`/`ReassignLeasePort`/`PurgePort`/`UpsertPort`
//!    /`CommitTransitionPort`/`ReclaimPort`) accepts `expected_epoch: Option<u64>` from the caller. Both
//!    the library facade (`Fireweed::push`/`claim`/`ack`/etc.) and the RESP server wiring
//!    (`OwnershipRuntime::expected_epoch_for_write`) supply the owner's cached `fence_epoch` from the
//!    [`OwnedSession`] — so a SUPERSEDED owner's claim/push/finalize is `EpochFenced` at commit time, not
//!    just the typed raw-commit seam. Backend implementations (compose.rs, sqlite/relational/apply.rs,
//!    segmented writer, etc.) check `expected_epoch.is_some_and(|e| e != current_epoch)` inside the atomic
//!    unit of work before applying anything. `None` is the degenerate sole-owner path (never self-fence).
//!    Tests `claim_fences_superseded_owner_epoch`, `push_fences_superseded_owner_epoch`, and
//!    `finalize_fences_superseded_owner_epoch` (fireweed-memory::tests, fireweed-sqlite::conformance) prove this.
//!
//!    TWO-COUNTER RECONCILIATION (bead pqueue-b29435b2): for backends whose control-plane acquire does
//!    NOT bind the storage fence in the same transaction, `acquire_and_fence` may observe
//!    `current_epoch > lease.assignment_epoch` after the acquire succeeds. This happens when an ephemeral
//!    control plane (e.g., `InMemoryControlPlane`) is reset on process restart while the durable backend
//!    retains a higher storage epoch. This gap is reconciled here by distinguishing the restart scenario
//!    from a genuine inconsistency:
//!
//!    - **Ephemeral reset, cold start**: the CP's prior `active_owner_id` is `None` (the queue was
//!      genuinely unassigned after restart). `acquire_and_fence` advances the storage epoch to fence stale
//!      pre-restart writers and sets `fence_epoch` to the new higher value.
//!    - **Ephemeral reset, same-owner re-affirm**: the CP's prior `active_owner_id` is `Some(owner)` and
//!      the epoch was preserved (no ownership change). The storage was already advanced by a prior
//!      restart-reconciliation; re-advancing would self-fence the owner's in-flight writes. `acquire_and_fence`
//!      reuses `current_epoch` as `fence_epoch` without advancing.
//!    - **Durable CP**: `current_epoch > lease.assignment_epoch` is a genuine inconsistency that still
//!      fails closed `EpochFenced`. See [`QueueControlPlane::is_ephemeral`].
//!
//!    Tests `ephemeral_restart_reacquire_advances_storage_and_serves` (engine-level, crates/fireweed-engine)
//!    and `ownership_restart_reacquire_serves_push_claim` (crates/fireweed-sqlite/tests/conformance.rs) prove
//!    the restart-reconciliation invariant. Stale-epoch writes from the pre-restart epoch are still
//!    `EpochFenced` (proven by the `*_fences_superseded_owner_epoch` suite against the post-restart fence).
//!
//! 2. [`owner_liveness_violation`] — the PREDICATE KERNEL of the TD-003 owner-liveness / stalled-queue guard
//!    (FR-41): a queue with eligible work aged at/past `progress_bound_ms` while it has no live SERVING
//!    owner (unowned, or draining) is a progress-bound violation. FR-41 also requires this to be OBSERVABLE
//!    (metrics + `DiscoverActiveScopes`); wiring the predicate to those surfaces is the follow-up — this is
//!    the pure decision only.
//!
//! SCOPE (honest): BQ-23's bead area is `fireweed-server`, but the full server runtime (the per-node
//! acquire/renew/heartbeat loop, the per-connection serve-gate, drain's "stop serving BatchClaim", and the
//! observable stalled-queue surface) is a follow-up. This module + its tests deliver the reusable,
//! unit-testable core those build on.

use fireweed_core::{OwnerId, UtcTimestamp};

use crate::control_plane::{
    AcquireOutcome, LeaseState, OwnerResolution, QueueControlPlane, QueueLease,
};
use crate::error::EngineResult;
use crate::port::ControlPlaneStore;
use crate::types::QueueKey;

/// A live ownership session: the queue an owner won + the epochs it operates under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedSession {
    pub owner: OwnerId,
    pub queue: QueueKey,
    /// The control-plane lease epoch — the renew/release credential (lease liveness authority).
    pub lease_epoch: u64,
    /// The durable STORAGE fence epoch the owner stamps as `expected_epoch` on every data-plane write
    /// (claim/push/finalize/renew/reassign/purge/upsert/commit). Every port backend checks this against the
    /// current durable epoch inside the atomic unit of work — a stale value is rejected `EpochFenced`
    /// (BQ-20, threaded through the real ports by bead pqueue-7bac12ce). The owner MUST
    /// re-`acquire_and_fence` rather than write at a stale epoch.
    pub fence_epoch: u64,
}

/// The outcome of [`acquire_and_fence`]: either an owned session, or a rejection carrying the current
/// authority record (a DIFFERENT owner holds a live lease — the caller re-resolves / waits).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipOutcome {
    Owned(OwnedSession),
    Rejected(QueueLease),
}

/// Acquire the queue lease and ensure the storage fence epoch is advanced (TD-003 Recovery step 1). The
/// order matters: the control-plane acquire (single-active-lease + liveness) happens FIRST; only on success
/// do we observe or advance the durable storage fence, so a rejected acquire never touches the fence. See
/// the module-doc SCOPE for what this does and does NOT fence.
///
/// RESET-RESTART RECONCILIATION (bead pqueue-b29435b2): for ephemeral control planes (`is_ephemeral()`)
/// whose state resets on process restart, the acquire succeeds but the durable backend's `current_epoch`
/// may be greater than `lease.assignment_epoch`. This function reads the CP's prior `active_owner_id`
/// before the acquire to distinguish a cold restart (advance storage) from a same-owner re-affirm (reuse
/// current storage epoch), avoiding both permanent `EpochFenced` and self-fencing on re-acquire.
pub async fn acquire_and_fence<CP, S>(
    control_plane: &CP,
    storage: &S,
    queue: &QueueKey,
    owner: &OwnerId,
    now: UtcTimestamp,
) -> EngineResult<OwnershipOutcome>
where
    CP: QueueControlPlane + ?Sized,
    S: ControlPlaneStore + ?Sized,
{
    // Read prior active owner before acquire so we can distinguish cold restart from same-owner re-affirm.
    let prior_owner = if control_plane.is_ephemeral() {
        control_plane
            .lease(queue)
            .ok()
            .and_then(|l| l.active_owner_id)
    } else {
        None
    };

    match control_plane.acquire_queue_lease(queue, owner, now)? {
        AcquireOutcome::Rejected(held) => Ok(OwnershipOutcome::Rejected(held)),
        AcquireOutcome::Acquired(lease) => {
            // Durable fence BEFORE the owner serves. Postgres-native control planes bind the storage fence
            // inside the acquire transaction, so the current storage epoch may already equal the lease
            // epoch. Reference/in-memory control planes still need the explicit storage advance.
            let current_epoch = storage.current_epoch(queue).await?;
            let fence_result = if current_epoch <= lease.assignment_epoch {
                storage.fence_epoch(queue, lease.assignment_epoch).await
            } else {
                // current_epoch > lease.assignment_epoch
                if prior_owner.as_ref() == Some(owner) {
                    // Same-owner re-affirm after a prior restart-reconciliation advanced the storage.
                    // CP preserved the epoch; re-advancing would self-fence in-flight writes.
                    Ok(current_epoch)
                } else if control_plane.is_ephemeral() {
                    // Ephemeral CP was reset on restart: storage epoch is ahead of the fresh CP.
                    // Advance storage to fence stale pre-restart writers.
                    storage.acquire_epoch(queue).await
                } else {
                    Err(crate::error::EngineError::EpochFenced)
                }
            };
            let fence_epoch = match fence_result {
                Ok(epoch) => epoch,
                Err(error) => {
                    let _ = control_plane.release_queue_lease(
                        queue,
                        owner,
                        lease.assignment_epoch,
                        now,
                    );
                    return Err(error);
                }
            };
            if lease.state == LeaseState::PendingFence
                && let Err(error) = control_plane.confirm_queue_lease_fence(
                    queue,
                    owner,
                    lease.assignment_epoch,
                    now,
                )
            {
                let _ =
                    control_plane.release_queue_lease(queue, owner, lease.assignment_epoch, now);
                return Err(error);
            }
            Ok(OwnershipOutcome::Owned(OwnedSession {
                owner: owner.clone(),
                queue: queue.clone(),
                lease_epoch: lease.assignment_epoch,
                fence_epoch,
            }))
        }
    }
}

/// The TD-003 owner-liveness / stalled-queue guard (FR-41). Returns `true` iff the queue is a progress-bound
/// violation *because of ownership*: it has eligible work whose oldest item has aged at or past
/// `progress_bound_ms`, while the queue has NO live owner SERVING new claims —
/// [`Unassigned`](LeaseState::Unassigned) (no live owner) or [`Draining`](LeaseState::Draining) (the owner
/// stopped accepting new claims for handoff). An [`Assigned`](LeaseState::Assigned) lease is serving, so the
/// queue-global progress bound is the claim planner's responsibility there, not an ownership violation.
///
/// Pure: it reasons over the current [`OwnerResolution`] + the queue's authoritative oldest-eligible age
/// (from the per-group summary, TD-003 §Per-Queue Progress Bound). `oldest_eligible_age_ms = None` means no
/// eligible work — never a violation.
pub fn owner_liveness_violation(
    resolution: &OwnerResolution,
    oldest_eligible_age_ms: Option<u64>,
    progress_bound_ms: u64,
) -> bool {
    let Some(age) = oldest_eligible_age_ms else {
        return false; // no eligible work to starve
    };
    let unserved = match resolution.state {
        LeaseState::Assigned => false,
        LeaseState::Unassigned | LeaseState::PendingFence | LeaseState::Draining => true,
    };
    unserved && age >= progress_bound_ms
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::OwnerResolution;

    fn resolution(state: LeaseState) -> OwnerResolution {
        OwnerResolution {
            target_owner: None,
            active_owner: None,
            assignment_epoch: None,
            lease_expires_at: None,
            state,
        }
    }

    #[test]
    fn no_eligible_work_is_never_a_violation() {
        for state in [
            LeaseState::Unassigned,
            LeaseState::PendingFence,
            LeaseState::Assigned,
            LeaseState::Draining,
        ] {
            assert!(!owner_liveness_violation(&resolution(state), None, 1_000));
        }
    }

    #[test]
    fn an_assigned_owner_serving_is_not_an_ownership_violation() {
        assert!(!owner_liveness_violation(
            &resolution(LeaseState::Assigned),
            Some(10_000),
            1_000
        ));
    }

    #[test]
    fn unowned_or_draining_past_the_bound_is_a_violation() {
        assert!(owner_liveness_violation(
            &resolution(LeaseState::Unassigned),
            Some(1_000),
            1_000
        ));
        assert!(owner_liveness_violation(
            &resolution(LeaseState::Draining),
            Some(1_500),
            1_000
        ));
    }

    #[test]
    fn unowned_within_the_bound_is_not_yet_a_violation() {
        assert!(!owner_liveness_violation(
            &resolution(LeaseState::Unassigned),
            Some(999),
            1_000
        ));
    }

    // -----------------------------------------------------------------------
    // Restart-reconciliation test (bead pqueue-b29435b2)
    // -----------------------------------------------------------------------

    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::control_plane::{ControlPlaneConfig, InMemoryControlPlane};
    use crate::error::EngineError;
    use crate::port::{ControlPlaneStore, CreateQueueOutcome};
    use fireweed_core::{OwnerId, QueueDefinition, QueueId, TenantId, UtcTimestamp};

    /// A minimal in-memory `ControlPlaneStore` for the restart-reconciliation test. Tracks a single
    /// per-queue epoch counter (the durable storage fence). Supports pre-advancing the epoch to simulate
    /// a durable backend that retained a high epoch across restart.
    struct EpochStore {
        epochs: Mutex<HashMap<QueueKey, u64>>,
    }

    impl EpochStore {
        fn new() -> Self {
            EpochStore {
                epochs: Mutex::new(HashMap::new()),
            }
        }
        fn set_epoch(&self, queue: &QueueKey, epoch: u64) {
            self.epochs
                .lock()
                .expect("poisoned")
                .insert(queue.clone(), epoch);
        }
    }

    impl ControlPlaneStore for EpochStore {
        fn create_queue(
            &self,
            _definition: QueueDefinition,
        ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
            std::future::ready(Ok(CreateQueueOutcome {
                created: true,
                definition: _definition,
            }))
        }
        fn queue_definition(
            &self,
            _key: &QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
            std::future::ready(Err(EngineError::NotFound))
        }
        fn list_queues(
            &self,
            _tenant: &TenantId,
        ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
            std::future::ready(Ok(vec![]))
        }
        fn current_epoch(
            &self,
            shard: &QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
            let epoch = self
                .epochs
                .lock()
                .expect("poisoned")
                .get(shard)
                .copied()
                .unwrap_or(0);
            std::future::ready(Ok(epoch))
        }
        fn acquire_epoch(
            &self,
            shard: &QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
            let mut g = self.epochs.lock().expect("poisoned");
            let next = g.get(shard).copied().unwrap_or(0) + 1;
            g.insert(shard.clone(), next);
            std::future::ready(Ok(next))
        }
    }

    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }

    fn qk() -> QueueKey {
        QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap())
    }

    /// After an ephemeral CP reset + durable backend with a higher epoch, `acquire_and_fence` reconciles
    /// the gap: it advances the storage epoch and returns a session whose `fence_epoch >= current_epoch`.
    #[test]
    fn ephemeral_restart_reacquire_advances_storage_and_serves() {
        use futures::executor::block_on;

        let storage = EpochStore::new();
        let cp = InMemoryControlPlane::new(ControlPlaneConfig::default());
        let owner = OwnerId::new("node-a").unwrap();
        let q = qk();

        // Simulate pre-restart: storage has epoch 3 from prior operations.
        storage.set_epoch(&q, 3);
        // Fresh CP after restart (no state).
        cp.register_owner(&owner, ts(0)).unwrap();

        // Re-acquire: should succeed, advancing storage from 3 to 4.
        let OwnershipOutcome::Owned(session) =
            block_on(acquire_and_fence(&cp, &storage, &q, &owner, ts(0))).unwrap()
        else {
            panic!("expected Owned after restart reconciliation");
        };
        assert!(
            session.fence_epoch > 3,
            "fence_epoch must exceed pre-restart storage epoch"
        );
        assert_eq!(session.fence_epoch, 4, "storage advanced exactly once");
        assert_eq!(
            session.lease_epoch, 1,
            "lease epoch is the fresh CP assignment (1)"
        );
        assert_eq!(
            block_on(storage.current_epoch(&q)).unwrap(),
            4,
            "durable storage epoch is the new fence"
        );

        // Same-owner re-affirm (lease lapse + re-acquire) preserves storage epoch.
        cp.register_owner(&owner, ts(100)).unwrap();
        let OwnershipOutcome::Owned(session2) =
            block_on(acquire_and_fence(&cp, &storage, &q, &owner, ts(100))).unwrap()
        else {
            panic!("expected Owned on same-owner re-affirm");
        };
        assert_eq!(
            session2.fence_epoch, 4,
            "same-owner re-affirm must NOT re-advance storage"
        );
        assert_eq!(
            session2.lease_epoch, 1,
            "same-owner re-affirm preserves CP epoch"
        );
    }

    /// A durable CP (non-ephemeral) with `current_epoch > lease.assignment_epoch` still fails closed.
    #[test]
    fn durable_cp_mismatch_still_fails_closed() {
        use futures::executor::block_on;

        // Use InMemoryControlPlane but wrap it to report is_ephemeral=false.
        struct DurableCp(InMemoryControlPlane);
        impl QueueControlPlane for DurableCp {
            fn is_ephemeral(&self) -> bool {
                false
            }
            fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
                self.0.register_owner(owner, now)
            }
            fn advertise_owner_endpoint(
                &self,
                owner: &OwnerId,
                endpoint: &str,
                now: UtcTimestamp,
            ) -> EngineResult<()> {
                self.0.advertise_owner_endpoint(owner, endpoint, now)
            }
            fn live_owner_endpoints(
                &self,
                now: UtcTimestamp,
            ) -> EngineResult<Vec<crate::control_plane::OwnerEndpointAdvertisement>> {
                self.0.live_owner_endpoints(now)
            }
            fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
                self.0.heartbeat(owner, now)
            }
            fn resolve_queue_owner(
                &self,
                queue: &QueueKey,
                now: UtcTimestamp,
            ) -> EngineResult<OwnerResolution> {
                self.0.resolve_queue_owner(queue, now)
            }
            fn acquire_queue_lease(
                &self,
                queue: &QueueKey,
                owner: &OwnerId,
                now: UtcTimestamp,
            ) -> EngineResult<AcquireOutcome> {
                self.0.acquire_queue_lease(queue, owner, now)
            }
            fn renew_queue_lease(
                &self,
                queue: &QueueKey,
                owner: &OwnerId,
                expected_epoch: u64,
                now: UtcTimestamp,
            ) -> EngineResult<QueueLease> {
                self.0.renew_queue_lease(queue, owner, expected_epoch, now)
            }
            fn begin_drain(
                &self,
                queue: &QueueKey,
                expected_epoch: u64,
                target_owner: &OwnerId,
                now: UtcTimestamp,
            ) -> EngineResult<QueueLease> {
                self.0.begin_drain(queue, expected_epoch, target_owner, now)
            }
            fn release_queue_lease(
                &self,
                queue: &QueueKey,
                owner: &OwnerId,
                expected_epoch: u64,
                now: UtcTimestamp,
            ) -> EngineResult<()> {
                self.0
                    .release_queue_lease(queue, owner, expected_epoch, now)
            }
            fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
                self.0.lease(queue)
            }
        }

        let storage = EpochStore::new();
        let cp = DurableCp(InMemoryControlPlane::new(ControlPlaneConfig::default()));
        let owner = OwnerId::new("node-a").unwrap();
        let q = qk();

        storage.set_epoch(&q, 5);
        cp.register_owner(&owner, ts(0)).unwrap();

        let result = block_on(acquire_and_fence(&cp, &storage, &q, &owner, ts(0)));
        assert!(
            matches!(result, Err(EngineError::EpochFenced)),
            "durable CP with storage > CP epoch must fail closed"
        );
    }
}
