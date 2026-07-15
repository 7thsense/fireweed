//! Queue-ownership orchestration PRIMITIVES (TD-003 §Recovery + §Per-Queue Progress Bound owner-liveness).
//!
//! This module is the engine-level CORE that a server owner-runtime will build on (the full pqueue-server
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
//!    the library facade (`Pqueue::push`/`claim`/`ack`/etc.) and the RESP server wiring
//!    (`OwnershipRuntime::expected_epoch_for_write`) supply the owner's cached `fence_epoch` from the
//!    [`OwnedSession`] — so a SUPERSEDED owner's claim/push/finalize is `EpochFenced` at commit time, not
//!    just the raw `LogWriter::append` seam. Backend implementations (compose.rs, sqlite/relational/apply.rs,
//!    segmented writer, etc.) check `expected_epoch.is_some_and(|e| e != current_epoch)` inside the atomic
//!    unit of work before applying anything. `None` is the degenerate sole-owner path (never self-fence).
//!    Tests `claim_fences_superseded_owner_epoch`, `push_fences_superseded_owner_epoch`, and
//!    `finalize_fences_superseded_owner_epoch` (pqueue-memory::tests, pqueue-sqlite::conformance) prove this.
//!
//!    TWO-COUNTER NON-ATOMICITY (proven benign for every current deployment): for backends whose
//!    control-plane acquire does not bind the storage fence in the same transaction, `acquire_and_fence`
//!    still performs two mutations (control-plane lease epoch, then storage fence epoch). A crash BETWEEN
//!    them can delay fencing or drift counters. This is BENIGN for every current deployment:
//!    - In-memory control planes (`InMemoryControlPlane`): a process crash resets all state to genesis, so
//!      the gap is irrelevant — the next acquire starts fresh at epoch 0.
//!    - Postgres-native control plane (`PostgresControlPlane`): advances the storage fence inside the same
//!      acquire transaction — no gap exists.
//!    - SQLite compositions use `InProcessControlPlane` (in-memory), which loses lease state on restart;
//!      the queue is unowned after crash and re-acquired at the current (or genesis) epoch.
//!
//!    Any future durable control plane that does NOT bind the storage fence in the acquire transaction
//!    MUST address this gap.
//!
//! 2. [`owner_liveness_violation`] — the PREDICATE KERNEL of the TD-003 owner-liveness / stalled-queue guard
//!    (FR-41): a queue with eligible work aged at/past `progress_bound_ms` while it has no live SERVING
//!    owner (unowned, or draining) is a progress-bound violation. FR-41 also requires this to be OBSERVABLE
//!    (metrics + `DiscoverActiveScopes`); wiring the predicate to those surfaces is the follow-up — this is
//!    the pure decision only.
//!
//! SCOPE (honest): BQ-23's bead area is `pqueue-server`, but the full server runtime (the per-node
//! acquire/renew/heartbeat loop, the per-connection serve-gate, drain's "stop serving BatchClaim", and the
//! observable stalled-queue surface) is a follow-up. This module + its tests deliver the reusable,
//! unit-testable core those build on.

use pqueue_core::{OwnerId, UtcTimestamp};

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
    match control_plane.acquire_queue_lease(queue, owner, now)? {
        AcquireOutcome::Rejected(held) => Ok(OwnershipOutcome::Rejected(held)),
        AcquireOutcome::Acquired(lease) => {
            // Durable fence BEFORE the owner serves. Postgres-native control planes bind the storage fence
            // inside the acquire transaction, so the current storage epoch may already equal the lease
            // epoch. Reference/in-memory control planes still need the explicit storage advance.
            let current_epoch = storage.current_epoch(queue).await?;
            let fence_epoch = if current_epoch == lease.assignment_epoch {
                current_epoch
            } else if current_epoch < lease.assignment_epoch {
                storage.acquire_epoch(queue).await?
            } else {
                return Err(crate::error::EngineError::EpochFenced);
            };
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
        LeaseState::Unassigned | LeaseState::Draining => true,
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
            LeaseState::Assigned,
            LeaseState::Draining,
        ] {
            assert!(!owner_liveness_violation(&resolution(state), None, 1_000));
        }
    }

    #[test]
    fn an_assigned_owner_serving_is_not_an_ownership_violation() {
        // Even far past the bound, an assigned (serving) owner is the claim planner's concern, not the
        // owner-liveness guard.
        assert!(!owner_liveness_violation(
            &resolution(LeaseState::Assigned),
            Some(10_000),
            1_000
        ));
    }

    #[test]
    fn unowned_or_draining_past_the_bound_is_a_violation() {
        // Unowned with aged eligible work past the bound → violation.
        assert!(owner_liveness_violation(
            &resolution(LeaseState::Unassigned),
            Some(1_000),
            1_000
        ));
        // Draining (not accepting new claims) past the bound → violation.
        assert!(owner_liveness_violation(
            &resolution(LeaseState::Draining),
            Some(1_500),
            1_000
        ));
    }

    #[test]
    fn unowned_within_the_bound_is_not_yet_a_violation() {
        // Eligible work exists and the queue is unowned, but the oldest item is still within budget.
        assert!(!owner_liveness_violation(
            &resolution(LeaseState::Unassigned),
            Some(999),
            1_000
        ));
    }
}
