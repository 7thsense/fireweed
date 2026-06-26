//! Queue-ownership orchestration PRIMITIVES (TD-003 §Recovery + §Per-Queue Progress Bound owner-liveness).
//!
//! This module is the engine-level CORE that a server owner-runtime will build on (the full pqueue-server
//! wiring is tracked as a follow-up — see SCOPE below). It is NOT itself the data-plane fence.
//!
//! 1. [`acquire_and_fence`] — the lease↔fence binding primitive. TD-003 Recovery step 1 / the Single
//!    Authoritative Fencing Rule: a new owner acquires the lease AND durably advances the storage epoch
//!    before serving. This drives both — control-plane `acquire_queue_lease` (liveness + single-active-lease)
//!    then storage `acquire_epoch` — and returns the [`OwnedSession`] whose `fence_epoch` is the value the
//!    owner is meant to stamp on `LogWriter::append(..., expected_epoch)`.
//!
//!    SCOPE / WHAT IS AND IS NOT FENCED (do not overstate this): the storage `LogWriter::append` SEAM does
//!    reject a stale `expected_epoch` (BQ-20), and the end-to-end test drives exactly that seam. But the
//!    REAL data-plane ports — `ClaimPort`/`PushPort`/`FinalizePort` and the backends' `commit_command` /
//!    `append_durable` / projection `commit` fast paths — currently read the queue's CURRENT epoch
//!    internally and pass it as `expected_epoch` (always-current, NEVER self-fences; see
//!    `pqueue-projection::commit`, `pqueue-sqlite::commit_command`). They do NOT yet take an owner's cached
//!    `fence_epoch`. So a SUPERSEDED owner's actual CLAIM is NOT fenced today — only a write made through the
//!    raw append seam is. Threading `fence_epoch` through the real ports (the work that genuinely closes the
//!    BQ-20/21/22 deferral) is the server-wiring follow-up (pqueue-c33c367e); the `port.rs::acquire_epoch`
//!    note that "the two epochs are separate" remains accurate until then.
//!
//!    KNOWN HAZARD (two-counter non-atomicity): `acquire_and_fence` performs two NON-transactional
//!    mutations (the control-plane lease epoch, then the storage fence epoch). A crash BETWEEN them, or a
//!    partial failure, can (a) leave the storage epoch un-advanced so an old owner's writes still pass while
//!    the control plane reports a new owner (delayed fencing), or (b) drift the two counters permanently
//!    (a later owner whose lease renews but whose appends are `EpochFenced`, or the mirror). TD-003's
//!    "atomic acquire→fence" is satisfied by the postgres_native SINGLE-ROW binding (the acquire txn IS the
//!    durable fence); this in-memory two-counter reference does not yet collapse them. The single-row
//!    unification + the hot-path threading are the same follow-up.
//!
//! 2. [`owner_liveness_violation`] — the PREDICATE KERNEL of the TD-003 owner-liveness / stalled-queue guard
//!    (FR-41): a queue with eligible work aged at/past `progress_bound_ms` while it has no live SERVING
//!    owner (unowned, or draining) is a progress-bound violation. FR-41 also requires this to be OBSERVABLE
//!    (metrics + `DiscoverActiveScopes`); wiring the predicate to those surfaces is the follow-up — this is
//!    the pure decision only.
//!
//! SCOPE (honest): BQ-23's bead area is `pqueue-server`, but the full server runtime (the per-node
//! acquire/renew/heartbeat loop, the per-connection serve-gate, stamping `fence_epoch` on the data plane,
//! drain's "stop serving BatchClaim", and the observable stalled-queue surface) is deferred to
//! pqueue-c33c367e. This module + its tests deliver the reusable, unit-testable core those build on.

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
    /// The durable STORAGE fence epoch the owner is meant to stamp as `expected_epoch` on
    /// `LogWriter::append`. At the raw append SEAM a stale value is rejected `EpochFenced` (BQ-20); the real
    /// claim/push ports do NOT yet consume this (they self-stamp the current epoch — see the module-doc
    /// SCOPE; threading it in is pqueue-c33c367e). The owner MUST re-`acquire_and_fence` rather than write
    /// at a stale epoch.
    pub fence_epoch: u64,
}

/// The outcome of [`acquire_and_fence`]: either an owned session, or a rejection carrying the current
/// authority record (a DIFFERENT owner holds a live lease — the caller re-resolves / waits).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipOutcome {
    Owned(OwnedSession),
    Rejected(QueueLease),
}

/// Acquire the queue lease and advance the storage fence epoch (TD-003 Recovery step 1). The order matters:
/// the control-plane acquire (single-active-lease + liveness) happens FIRST; only on success do we advance
/// the durable storage fence — so a rejected acquire never touches the fence. See the module-doc SCOPE for
/// what this does and does NOT fence, and the two-counter non-atomicity HAZARD.
pub async fn acquire_and_fence<CP, S>(
    control_plane: &CP,
    storage: &S,
    queue: &QueueKey,
    owner: &OwnerId,
    now: UtcTimestamp,
) -> EngineResult<OwnershipOutcome>
where
    CP: QueueControlPlane,
    S: ControlPlaneStore,
{
    match control_plane.acquire_queue_lease(queue, owner, now)? {
        AcquireOutcome::Rejected(held) => Ok(OwnershipOutcome::Rejected(held)),
        AcquireOutcome::Acquired(lease) => {
            // Durable fence BEFORE the owner serves: advance the storage append-fence epoch. After this
            // commits, any prior owner's cached `fence_epoch` is stale and its next append is `EpochFenced`.
            let fence_epoch = storage.acquire_epoch(queue).await?;
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
