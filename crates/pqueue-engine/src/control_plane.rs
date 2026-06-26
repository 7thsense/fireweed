//! Queue ownership control plane (TD-003 §Queue Ownership and Placement + §Queue Lease Lifecycle).
//!
//! The control plane is the **single fencing + assignment authority** for queue ownership, kept PLUGGABLE
//! and separate from the storage backends (ADR-008: "pluggable control plane"). It owns three things:
//!
//! 1. **The live owner set** — registered owner workers with a heartbeat; an owner is live while
//!    `heartbeat_at + heartbeat_ttl_ms > now`. pqueue never discovers owners peer-to-peer.
//! 2. **Deterministic assignment** — [`resolve_queue_owner`](QueueControlPlane::resolve_queue_owner)
//!    computes the `target_owner` as a pure function of `((tenant, queue), live_owner_set)` via rendezvous
//!    (highest-random-weight) hashing, so adding/removing one owner moves only `O(queues/owners)` queues.
//! 3. **The per-queue authority record** — `(active_owner_id, assignment_epoch, lease_expires_at, state,
//!    target_owner_id)`, mutated only through the transactional lease ops (acquire / renew / begin_drain /
//!    release), which enforce the C4b seam invariants:
//!    - **single active lease**: acquire rejects a different owner's live lease;
//!    - **monotonic epoch**: `assignment_epoch` increases strictly on every ownership change, never repeats
//!      or decreases;
//!    - **atomic acquire→fence**: acquire allocates the new epoch AND records it before returning (the
//!      durable fence; step 1 of the Single Authoritative Fencing Rule, TD-003 / BQ-20);
//!    - **fail-closed**: an unregistered/dead owner cannot acquire; a stale-epoch or wrong-owner renew is
//!      rejected `queue-epoch-stale` ([`EngineError::EpochFenced`]); no live owner ⇒ no `target_owner`.
//!
//! This module is the **reference, in-memory** control plane ([`InMemoryControlPlane`]) + the trait seam
//! ([`QueueControlPlane`]). The transactional postgres-backed implementation (the production default) is
//! BQ-22; binding the lease epoch to each storage backend's durable `assignment_epoch` (BQ-20) and stamping
//! the owner's epoch on the data-plane write path is the server wiring (BQ-23). Here the authority record's
//! `assignment_epoch` IS the reference fence value, advanced atomically at acquire.
//!
//! REFERENCE-IMPL SIMPLIFICATIONS (the postgres impl, BQ-22, lifts these — recorded so they are not
//! mistaken for the contract):
//! - **Durability**: state is an in-process `Mutex<HashMap>`; a restart resets every queue to genesis
//!   epoch 0, so the "never repeats" half of epoch monotonicity holds only WITHIN a process. The DURABLE
//!   authority (where the epoch survives restart and truly never repeats) is the postgres row (BQ-22).
//! - **Per-owner heartbeat TTL**: one global [`ControlPlaneConfig::heartbeat_ttl_ms`] for all owners;
//!   TD-003 puts the TTL per `OwnerRegistration`. Heterogeneous owner TTLs are a BQ-22 concern.
//! - **Cooperative placement**: acquire admits ANY live registered owner of an unowned/expired queue (the
//!   epoch still fences for safety); it does NOT enforce `acquirer == target_owner`. TD-003's "the target
//!   owner SHOULD acquire" is advisory — placement convergence is cooperative, safety is the epoch.

use std::collections::HashMap;
use std::sync::Mutex;

use pqueue_core::{OwnerId, UtcTimestamp};

use crate::error::{EngineError, EngineResult};
use crate::types::QueueKey;

/// The lifecycle state of a queue's ownership lease (TD-003 §Queue Lease Lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// No live active owner; the `target_owner` SHOULD acquire.
    Unassigned,
    /// An active owner holds a non-expired lease for the current epoch.
    Assigned,
    /// The active owner is finishing in-flight work (handing off to a recorded `target_owner`); it MUST
    /// stop serving new claims. Reclaimed to `Unassigned` when drain completes or the deadline passes.
    Draining,
}

/// The per-queue authority record — the single source of truth for who owns the queue and at what epoch
/// (TD-003: "at most one active owner lease"). `assignment_epoch` is the fence authority (BQ-20).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueLease {
    pub state: LeaseState,
    /// The lease holder (whoever holds the non-expired lease). `None` only while `Unassigned`.
    pub active_owner_id: Option<OwnerId>,
    /// The deterministic assignment-function output recorded at the last acquire/drain. May differ from
    /// `active_owner_id` transiently during reassignment.
    pub target_owner_id: Option<OwnerId>,
    /// Strictly-monotonic per-queue epoch; the append fence rejects any non-current value (BQ-20).
    pub assignment_epoch: u64,
    /// When the lease expires (lease liveness; governs safety, unlike the heartbeat). `None` while
    /// `Unassigned`.
    pub lease_expires_at: Option<UtcTimestamp>,
}

impl QueueLease {
    /// A queue with no owner yet (genesis): unassigned, epoch 0.
    fn unassigned() -> Self {
        QueueLease {
            state: LeaseState::Unassigned,
            active_owner_id: None,
            target_owner_id: None,
            assignment_epoch: 0,
            lease_expires_at: None,
        }
    }

    /// Whether this lease is currently held by a live (non-expired) owner at `now`. An expired
    /// `lease_expires_at` makes the queue reclaimable regardless of recorded `state`.
    fn is_live(&self, now: UtcTimestamp) -> bool {
        matches!(self.state, LeaseState::Assigned | LeaseState::Draining)
            && self.lease_expires_at.is_some_and(|exp| now < exp)
    }
}

/// What `resolve_queue_owner` reports: the deterministic `target_owner` plus the current authority record
/// fields a caller needs to decide whether to acquire, renew, drain, or wait (TD-003 §"What
/// `resolve_queue_owner` returns by state").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerResolution {
    /// The deterministic assignment-function output for the current live owner set. `None` iff there are
    /// no live owners (fail-closed: nobody serves).
    pub target_owner: Option<OwnerId>,
    pub active_owner: Option<OwnerId>,
    /// The current assignment epoch, or `None` when no lease has ever been granted for this queue (genesis)
    /// — distinguished from `Some(0)` per TD-003 (a granted lease is always epoch `>= 1`).
    pub assignment_epoch: Option<u64>,
    /// The active lease's expiry (when live), else `None`.
    pub lease_expires_at: Option<UtcTimestamp>,
    pub state: LeaseState,
}

/// The outcome of [`acquire_queue_lease`](QueueControlPlane::acquire_queue_lease): either the caller is now
/// the active owner (`Acquired`), or a DIFFERENT owner holds a live lease (`Rejected`) and the caller must
/// re-resolve / wait. The rejected case carries the current authority record (TD-003 acquire step 1:
/// "return current active_owner + epoch + state").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    Acquired(QueueLease),
    Rejected(QueueLease),
}

/// Owner-liveness + lease TTLs (milliseconds). `lease_ttl_ms` SHOULD be `>= heartbeat_ttl_ms` so a healthy
/// owner is never spuriously reclaimed (TD-003 lease-TTL guidance).
#[derive(Debug, Clone, Copy)]
pub struct ControlPlaneConfig {
    pub heartbeat_ttl_ms: u64,
    pub lease_ttl_ms: u64,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        ControlPlaneConfig {
            heartbeat_ttl_ms: 5_000,
            lease_ttl_ms: 15_000,
        }
    }
}

/// The pluggable control-plane seam (TD-003 §Queue Lease Lifecycle). All lease ops are transactional: each
/// is one atomic mutation of the authority record. The production impl is transactional-postgres (BQ-22);
/// [`InMemoryControlPlane`] is the reference + the default for single-node / tests.
pub trait QueueControlPlane: Send + Sync {
    /// Register (or refresh) an owner worker in the candidate set with a heartbeat at `now`.
    fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp);

    /// Refresh an owner's heartbeat. An owner whose heartbeat has expired leaves the live set (changing
    /// future `target_owner` computations) but its lease is reclaimed only via `lease_expires_at`.
    fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp);

    /// The deterministic `target_owner` (rendezvous/HRW over the live owner set) plus the current authority
    /// record. `target_owner` is `None` iff no owner is live (fail-closed).
    fn resolve_queue_owner(&self, queue: &QueueKey, now: UtcTimestamp) -> OwnerResolution;

    /// Acquire the queue at a strictly-greater, durably-recorded epoch (TD-003 acquire). Rejects if a
    /// DIFFERENT owner holds a live (`assigned`/`draining`, non-expired) lease. The caller MUST be a live
    /// registered owner (fail-closed) — else `EngineError::Forbidden`.
    ///
    /// CONTRACT — acquire is NOT idempotent: EVERY successful acquire allocates a NEW strictly-greater
    /// epoch, INCLUDING a same-owner re-acquire of a still-live lease. This is intentional — a restarted
    /// owner re-acquiring its own queue MUST fence its pre-crash in-flight appends (TD-003 Recovery: a new
    /// epoch fences who may extend the log). The consequence a caller MUST respect: do NOT blindly retry
    /// `acquire` after a timeout (a retry whose first attempt actually succeeded would double-bump and fence
    /// the caller's own epoch-N writes); instead re-`resolve_queue_owner` and use the returned epoch as
    /// authoritative. To merely extend an existing lease, call [`renew_queue_lease`](Self::renew_queue_lease)
    /// (same epoch), not acquire.
    fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome>;

    /// Renew the active owner's lease (TD-003 renewal). MUST NOT change the epoch. A renewal whose
    /// `expected_epoch` mismatches, or whose `owner` is not the `active_owner`, fails `queue-epoch-stale`
    /// ([`EngineError::EpochFenced`]) — the worker must stop appending and re-resolve.
    fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease>;

    /// Begin a graceful drain toward `target_owner` (TD-003 §Graceful Drain). Records `state=draining` +
    /// `target_owner_id` for the CURRENT epoch; the active owner observes it on its next renew and stops
    /// serving new claims. Optimistically concurrency-checked: fails `queue-epoch-stale`
    /// ([`EngineError::EpochFenced`]) unless the live lease is at `expected_epoch` (so a drain computed
    /// against a superseded lease never flips a newer owner to draining). Only valid on an `assigned` lease
    /// whose target differs from the active owner.
    fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease>;

    /// Release the lease (handoff / shutdown). Fails `queue-epoch-stale` unless `owner` is the active owner
    /// at `expected_epoch`. Sets `state=unassigned` (the epoch is retained; the NEXT acquire allocates a
    /// strictly-greater one, fencing this owner's stragglers).
    fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()>;

    /// Read the current authority record (defaulting to `unassigned`/epoch 0 for a never-owned queue).
    fn lease(&self, queue: &QueueKey) -> QueueLease;
}

/// `now + ms` as a normalized [`UtcTimestamp`] (seconds saturate; nanos carry). Used for lease deadlines.
fn add_millis(t: UtcTimestamp, ms: u64) -> UtcTimestamp {
    let add_secs = (ms / 1000) as i64;
    let add_nanos = ((ms % 1000) * 1_000_000) as u32;
    let mut secs = t.seconds.saturating_add(add_secs);
    let mut nanos = t.nanoseconds + add_nanos;
    if nanos >= 1_000_000_000 {
        nanos -= 1_000_000_000;
        secs = secs.saturating_add(1);
    }
    UtcTimestamp::new(secs, nanos).expect("nanoseconds normalized below 1e9")
}

/// Stable rendezvous (highest-random-weight) hash of `(queue, owner)`. A fixed-seed FNV-1a keeps the
/// assignment a deterministic pure function of its inputs (no `RandomState`, no run-to-run variation), so
/// every node computes the same `target_owner` for the same live set (TD-003 assignment-function rule).
fn rendezvous_weight(queue: &QueueKey, owner: &OwnerId) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a offset basis
    let mut mix = |bytes: &[u8]| {
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
        }
        hash ^= 0xff; // domain separator between fields
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    };
    mix(queue.tenant_id.as_str().as_bytes());
    mix(queue.queue_id.as_str().as_bytes());
    mix(owner.as_str().as_bytes());
    hash
}

/// Pick the deterministic target owner from a live set: the owner with the greatest rendezvous weight
/// (owner-id breaks an astronomically-unlikely weight tie, keeping it a pure total function). Empty set →
/// `None` (fail-closed).
fn resolve_target<'a>(
    queue: &QueueKey,
    live: impl Iterator<Item = &'a OwnerId>,
) -> Option<OwnerId> {
    live.map(|o| (rendezvous_weight(queue, o), o))
        .max_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.as_str().cmp(b.1.as_str())))
        .map(|(_, o)| o.clone())
}

/// The reference in-memory control plane (single-node / test default). One `Mutex` makes each lease op an
/// atomic transaction — the in-memory analogue of the postgres single-row transaction (BQ-22).
pub struct InMemoryControlPlane {
    config: ControlPlaneConfig,
    state: Mutex<CpState>,
}

#[derive(Default)]
struct CpState {
    /// owner → last heartbeat time.
    owners: HashMap<OwnerId, UtcTimestamp>,
    /// queue → authority record.
    leases: HashMap<QueueKey, QueueLease>,
}

impl InMemoryControlPlane {
    pub fn new(config: ControlPlaneConfig) -> Self {
        InMemoryControlPlane {
            config,
            state: Mutex::new(CpState::default()),
        }
    }

    /// Milliseconds elapsed from `a` to `b` (0 if `b <= a`), for TTL comparisons.
    fn elapsed_ms(a: UtcTimestamp, b: UtcTimestamp) -> u64 {
        if b <= a {
            return 0;
        }
        let secs = (b.seconds - a.seconds) as i128;
        let nanos = b.nanoseconds as i128 - a.nanoseconds as i128;
        let total_ms = secs * 1000 + nanos.div_euclid(1_000_000);
        total_ms.max(0) as u64
    }

    fn is_owner_live(
        &self,
        owners: &HashMap<OwnerId, UtcTimestamp>,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> bool {
        owners
            .get(owner)
            .is_some_and(|hb| Self::elapsed_ms(*hb, now) < self.config.heartbeat_ttl_ms)
    }

    fn live_owners<'a>(
        &self,
        owners: &'a HashMap<OwnerId, UtcTimestamp>,
        now: UtcTimestamp,
    ) -> Vec<&'a OwnerId> {
        owners
            .iter()
            .filter(|(_, hb)| Self::elapsed_ms(**hb, now) < self.config.heartbeat_ttl_ms)
            .map(|(o, _)| o)
            .collect()
    }
}

impl Default for InMemoryControlPlane {
    fn default() -> Self {
        Self::new(ControlPlaneConfig::default())
    }
}

impl QueueControlPlane for InMemoryControlPlane {
    fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) {
        self.state
            .lock()
            .expect("poisoned")
            .owners
            .insert(owner.clone(), now);
    }

    fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) {
        // Register-on-heartbeat is intentional: a heartbeat from an unknown owner re-admits it (a node that
        // briefly fell out of the live set rejoins), matching the postgres upsert.
        self.state
            .lock()
            .expect("poisoned")
            .owners
            .insert(owner.clone(), now);
    }

    fn resolve_queue_owner(&self, queue: &QueueKey, now: UtcTimestamp) -> OwnerResolution {
        let g = self.state.lock().expect("poisoned");
        let target = resolve_target(queue, self.live_owners(&g.owners, now).into_iter());
        let lease = g
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned);
        // A lease whose `lease_expires_at` has passed is reported as `unassigned` (reclaimable), even if the
        // stored state still reads assigned/draining — lease liveness, not the stale stored flag, governs.
        let (state, active, expires) = if lease.is_live(now) {
            (
                lease.state,
                lease.active_owner_id.clone(),
                lease.lease_expires_at,
            )
        } else {
            (LeaseState::Unassigned, None, None)
        };
        OwnerResolution {
            target_owner: target,
            active_owner: active,
            // epoch 0 == genesis (never granted) → None; a granted lease is always >= 1.
            assignment_epoch: (lease.assignment_epoch > 0).then_some(lease.assignment_epoch),
            lease_expires_at: expires,
            state,
        }
    }

    fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let mut g = self.state.lock().expect("poisoned");
        // Fail-closed: only a live registered owner may acquire.
        if !self.is_owner_live(&g.owners, owner, now) {
            return Err(EngineError::Forbidden(
                "owner is not live (register + heartbeat first)",
            ));
        }
        let current = g
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned);
        // Single active lease: a DIFFERENT owner's live lease blocks the acquire.
        if current.is_live(now) && current.active_owner_id.as_ref() != Some(owner) {
            return Ok(AcquireOutcome::Rejected(current));
        }
        // Atomic acquire→fence: strictly-greater epoch, recorded with the new owner before returning.
        let new_epoch = current.assignment_epoch + 1;
        let lease_expires_at = add_millis(now, self.config.lease_ttl_ms);
        let acquired = QueueLease {
            state: LeaseState::Assigned,
            active_owner_id: Some(owner.clone()),
            target_owner_id: Some(owner.clone()),
            assignment_epoch: new_epoch,
            lease_expires_at: Some(lease_expires_at),
        };
        g.leases.insert(queue.clone(), acquired.clone());
        Ok(AcquireOutcome::Acquired(acquired))
    }

    fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let mut g = self.state.lock().expect("poisoned");
        let current = g
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned);
        // queue-epoch-stale: wrong owner, wrong epoch, or an already-reclaimed (expired) lease.
        if current.active_owner_id.as_ref() != Some(owner)
            || current.assignment_epoch != expected_epoch
            || !current.is_live(now)
        {
            return Err(EngineError::EpochFenced);
        }
        // Renewal extends the deadline at the SAME epoch (never reallocates).
        let renewed = QueueLease {
            lease_expires_at: Some(add_millis(now, self.config.lease_ttl_ms)),
            ..current
        };
        g.leases.insert(queue.clone(), renewed.clone());
        Ok(renewed)
    }

    fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let mut g = self.state.lock().expect("poisoned");
        let current = g
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned);
        // Optimistic concurrency: a drain computed against a superseded lease is queue-epoch-stale (so it
        // can never flip a newer owner to draining).
        if current.assignment_epoch != expected_epoch || !current.is_live(now) {
            return Err(EngineError::EpochFenced);
        }
        // Drain only applies to a currently-assigned lease being handed to a DIFFERENT target.
        if current.state != LeaseState::Assigned {
            return Err(EngineError::Conflict);
        }
        if current.active_owner_id.as_ref() == Some(target_owner) {
            return Err(EngineError::Invalid("drain target is the active owner"));
        }
        let draining = QueueLease {
            state: LeaseState::Draining,
            target_owner_id: Some(target_owner.clone()),
            ..current
        };
        g.leases.insert(queue.clone(), draining.clone());
        Ok(draining)
    }

    fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let mut g = self.state.lock().expect("poisoned");
        let current = g
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned);
        // Only the active owner at the current epoch may release; an expired lease is already reclaimable.
        if current.active_owner_id.as_ref() != Some(owner)
            || current.assignment_epoch != expected_epoch
            || !matches!(current.state, LeaseState::Assigned | LeaseState::Draining)
        {
            return Err(EngineError::EpochFenced);
        }
        // Unassign but RETAIN the epoch — the next acquire allocates a strictly-greater one (fences
        // stragglers from this owner). target_owner_id is cleared.
        let released = QueueLease {
            state: LeaseState::Unassigned,
            active_owner_id: None,
            target_owner_id: None,
            lease_expires_at: None,
            assignment_epoch: current.assignment_epoch,
        };
        g.leases.insert(queue.clone(), released);
        let _ = now;
        Ok(())
    }

    fn lease(&self, queue: &QueueKey) -> QueueLease {
        self.state
            .lock()
            .expect("poisoned")
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqueue_core::{QueueId, TenantId};

    fn cp() -> InMemoryControlPlane {
        // Explicit TTLs: owner heartbeat 5s, lease 15s.
        InMemoryControlPlane::new(ControlPlaneConfig {
            heartbeat_ttl_ms: 5_000,
            lease_ttl_ms: 15_000,
        })
    }
    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }
    fn owner(s: &str) -> OwnerId {
        OwnerId::new(s).unwrap()
    }
    fn qk(q: &str) -> QueueKey {
        QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new(q).unwrap())
    }

    // ----- lifecycle -----

    #[test]
    fn full_lifecycle_acquire_renew_drain_release_reacquire() {
        let cp = cp();
        let a = owner("a");
        let b = owner("b");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));

        // Acquire: epoch 1, assigned, active=target=a, deadline now+15s.
        let AcquireOutcome::Acquired(l1) = cp.acquire_queue_lease(&q, &a, ts(0)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(l1.assignment_epoch, 1);
        assert_eq!(l1.state, LeaseState::Assigned);
        assert_eq!(l1.active_owner_id.as_ref(), Some(&a));
        assert_eq!(l1.lease_expires_at, Some(ts(15)));

        // Renew before expiry: same epoch, extended deadline.
        let l2 = cp.renew_queue_lease(&q, &a, 1, ts(10)).unwrap();
        assert_eq!(l2.assignment_epoch, 1, "renew never changes the epoch");
        assert_eq!(l2.lease_expires_at, Some(ts(25)));

        // begin_drain toward b: draining, target=b, still epoch 1.
        cp.register_owner(&b, ts(10));
        let l3 = cp.begin_drain(&q, 1, &b, ts(11)).unwrap();
        assert_eq!(l3.state, LeaseState::Draining);
        assert_eq!(l3.target_owner_id.as_ref(), Some(&b));
        assert_eq!(
            l3.active_owner_id.as_ref(),
            Some(&a),
            "active owner unchanged during drain"
        );

        // a releases: unassigned, epoch RETAINED (next acquire goes strictly higher).
        cp.release_queue_lease(&q, &a, 1, ts(12)).unwrap();
        let rel = cp.lease(&q);
        assert_eq!(rel.state, LeaseState::Unassigned);
        assert_eq!(rel.active_owner_id, None);
        assert_eq!(rel.assignment_epoch, 1, "epoch retained across release");

        // b acquires: strictly-greater epoch 2.
        let AcquireOutcome::Acquired(l4) = cp.acquire_queue_lease(&q, &b, ts(13)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(l4.assignment_epoch, 2);
        assert_eq!(l4.active_owner_id.as_ref(), Some(&b));
    }

    // ----- seam invariant: single active lease -----

    #[test]
    fn a_different_owners_live_lease_blocks_acquire() {
        let cp = cp();
        let a = owner("a");
        let b = owner("b");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));
        cp.register_owner(&b, ts(0));
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap();

        // b acquires while a's lease is live → Rejected, carrying a's authority record (b heartbeats so it
        // is a live owner; a's LEASE — not a's heartbeat — is what blocks the acquire).
        cp.heartbeat(&b, ts(4));
        let AcquireOutcome::Rejected(held) = cp.acquire_queue_lease(&q, &b, ts(4)).unwrap() else {
            panic!("expected Rejected");
        };
        assert_eq!(held.active_owner_id.as_ref(), Some(&a));
        assert_eq!(held.assignment_epoch, 1);
        // a's lease is untouched (no epoch bump from a rejected acquire).
        assert_eq!(cp.lease(&q).assignment_epoch, 1);
    }

    #[test]
    fn the_same_owner_reacquiring_its_live_lease_bumps_epoch() {
        // A re-acquire by the SAME owner is allowed (e.g. a restart that re-resolves to itself) and still
        // allocates a strictly-greater epoch — fencing any of its own in-flight stragglers from the old epoch.
        let cp = cp();
        let a = owner("a");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap();
        let AcquireOutcome::Acquired(l2) = cp.acquire_queue_lease(&q, &a, ts(1)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(l2.assignment_epoch, 2);
    }

    // ----- seam invariant: monotonic epoch + expired-lease reclaim -----

    #[test]
    fn expired_lease_is_reclaimable_at_a_strictly_greater_epoch() {
        let cp = cp();
        let a = owner("a");
        let b = owner("b");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap(); // epoch 1, expires ts(15)

        // After expiry (and a's heartbeat gone), b is live and acquires → epoch 2 (no rejection).
        cp.register_owner(&b, ts(20));
        let AcquireOutcome::Acquired(l2) = cp.acquire_queue_lease(&q, &b, ts(20)).unwrap() else {
            panic!("expected Acquired (a's lease expired)");
        };
        assert_eq!(l2.assignment_epoch, 2);
        assert_eq!(l2.active_owner_id.as_ref(), Some(&b));
    }

    #[test]
    fn epoch_strictly_increases_across_every_ownership_change() {
        let cp = cp();
        let a = owner("a");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));
        let mut prev = 0;
        // Acquire → release → acquire → release ... epoch must climb strictly each acquire.
        for i in 0..5 {
            let t = ts(i * 100);
            cp.heartbeat(&a, t); // keep the owner live across the time jumps
            let AcquireOutcome::Acquired(l) = cp.acquire_queue_lease(&q, &a, t).unwrap() else {
                panic!("acquire");
            };
            assert!(l.assignment_epoch > prev, "epoch must strictly increase");
            prev = l.assignment_epoch;
            cp.release_queue_lease(&q, &a, prev, ts(i * 100 + 1))
                .unwrap();
        }
        assert_eq!(prev, 5);
    }

    // ----- seam invariant: atomic acquire→fence -----

    #[test]
    fn acquire_records_the_new_epoch_before_returning() {
        let cp = cp();
        let a = owner("a");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));
        let AcquireOutcome::Acquired(l) = cp.acquire_queue_lease(&q, &a, ts(0)).unwrap() else {
            panic!("acquire");
        };
        // The durable record already reflects the new epoch the instant acquire returns (the fence is in
        // place before the owner serves) — resolve + lease both see epoch 1.
        assert_eq!(cp.lease(&q).assignment_epoch, l.assignment_epoch);
        assert_eq!(cp.resolve_queue_owner(&q, ts(0)).assignment_epoch, Some(1));
    }

    // ----- seam invariant: fail-closed -----

    #[test]
    fn unregistered_or_dead_owner_cannot_acquire() {
        let cp = cp();
        let a = owner("a");
        let q = qk("q1");
        // Never registered.
        assert!(matches!(
            cp.acquire_queue_lease(&q, &a, ts(0)),
            Err(EngineError::Forbidden(_))
        ));
        // Registered, but heartbeat expired by acquire time (5s TTL, acquire at 10s).
        cp.register_owner(&a, ts(0));
        assert!(matches!(
            cp.acquire_queue_lease(&q, &a, ts(10)),
            Err(EngineError::Forbidden(_))
        ));
    }

    #[test]
    fn renew_fails_closed_on_stale_epoch_wrong_owner_or_expiry() {
        let cp = cp();
        let a = owner("a");
        let b = owner("b");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap(); // epoch 1

        // Wrong expected epoch.
        assert_eq!(
            cp.renew_queue_lease(&q, &a, 99, ts(1)),
            Err(EngineError::EpochFenced)
        );
        // Wrong owner.
        assert_eq!(
            cp.renew_queue_lease(&q, &b, 1, ts(1)),
            Err(EngineError::EpochFenced)
        );
        // After expiry (lease gone), even the right owner+epoch is queue-epoch-stale.
        assert_eq!(
            cp.renew_queue_lease(&q, &a, 1, ts(100)),
            Err(EngineError::EpochFenced)
        );
    }

    #[test]
    fn a_superseded_owner_is_fenced_on_renew_after_handoff() {
        // The end-to-end stale-owner story: a holds epoch 1, its lease lapses, b acquires epoch 2; a's
        // renew at its cached epoch 1 is now queue-epoch-stale (a must stop appending and re-resolve).
        let cp = cp();
        let a = owner("a");
        let b = owner("b");
        let q = qk("q1");
        cp.register_owner(&a, ts(0));
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap();
        cp.register_owner(&b, ts(20));
        cp.acquire_queue_lease(&q, &b, ts(20)).unwrap(); // epoch 2
        assert_eq!(
            cp.renew_queue_lease(&q, &a, 1, ts(21)),
            Err(EngineError::EpochFenced),
            "the superseded owner is fenced"
        );
    }

    #[test]
    fn no_live_owner_resolves_to_no_target() {
        let cp = cp();
        let q = qk("q1");
        // No owners registered → fail-closed: nobody serves.
        assert_eq!(cp.resolve_queue_owner(&q, ts(0)).target_owner, None);
        // Registered then expired → still no target.
        cp.register_owner(&owner("a"), ts(0));
        assert_eq!(cp.resolve_queue_owner(&q, ts(100)).target_owner, None);
    }

    // ----- seam invariant: concurrent acquire linearizes (TD-003 "at most one succeeds vs a prior epoch") -----

    #[test]
    fn concurrent_acquires_linearize_to_distinct_strictly_increasing_epochs() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        let cp = Arc::new(cp());
        let q = qk("q1");
        // 8 contending owners, all live, all racing to acquire the SAME queue at the SAME instant.
        let owners: Vec<OwnerId> = (0..8).map(|i| owner(&format!("o{i}"))).collect();
        for o in &owners {
            cp.register_owner(o, ts(0));
        }
        let acquired_count = Arc::new(AtomicU64::new(0));
        let max_epoch = Arc::new(AtomicU64::new(0));
        let handles: Vec<_> = owners
            .into_iter()
            .map(|o| {
                let cp = Arc::clone(&cp);
                let q = q.clone();
                let acquired_count = Arc::clone(&acquired_count);
                let max_epoch = Arc::clone(&max_epoch);
                std::thread::spawn(move || {
                    if let Ok(AcquireOutcome::Acquired(l)) = cp.acquire_queue_lease(&q, &o, ts(0)) {
                        acquired_count.fetch_add(1, Ordering::SeqCst);
                        max_epoch.fetch_max(l.assignment_epoch, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // All acquires serialize under the mutex; the FIRST takes the lease, the rest see a live different
        // owner and are Rejected (single active lease). So exactly ONE wins and the epoch advances exactly
        // once — a thundering herd cannot produce two winners or a torn/double-bumped epoch (no lost update).
        let acquired = acquired_count.load(Ordering::SeqCst);
        assert_eq!(acquired, 1, "exactly one acquire wins the contended queue");
        assert_eq!(
            cp.lease(&q).assignment_epoch,
            1,
            "the epoch advanced exactly once under contention — linearized, no double-bump"
        );
        assert_eq!(max_epoch.load(Ordering::SeqCst), 1);
    }

    // ----- assignment function: deterministic HRW -----

    #[test]
    fn resolve_is_a_deterministic_pure_function_of_queue_and_live_set() {
        let cp1 = cp();
        let cp2 = cp();
        for o in ["a", "b", "c"] {
            cp1.register_owner(&owner(o), ts(0));
            cp2.register_owner(&owner(o), ts(0));
        }
        // Same live set + same queue → same target, on two independent control planes (no run-to-run
        // randomness), and stable across repeated calls.
        for q in ["q1", "q2", "q3", "q4", "q5"] {
            let t1 = cp1.resolve_queue_owner(&qk(q), ts(0)).target_owner;
            let t2 = cp2.resolve_queue_owner(&qk(q), ts(1)).target_owner;
            assert!(t1.is_some());
            assert_eq!(t1, t2, "HRW must be a pure function");
        }
    }

    #[test]
    fn hrw_spreads_queues_and_moves_only_a_fraction_when_an_owner_leaves() {
        let cp = cp();
        for o in ["a", "b", "c"] {
            cp.register_owner(&owner(o), ts(0));
        }
        let queues: Vec<QueueKey> = (0..60).map(|i| qk(&format!("q{i}"))).collect();
        let before: Vec<Option<OwnerId>> = queues
            .iter()
            .map(|q| cp.resolve_queue_owner(q, ts(0)).target_owner)
            .collect();
        // All three owners get some queues (the assignment is spread, not all-to-one).
        for o in ["a", "b", "c"] {
            assert!(
                before
                    .iter()
                    .any(|t| t.as_ref().map(|x| x.as_str()) == Some(o)),
                "owner {o} should own some queues under HRW"
            );
        }
        // Drop owner "c" (heartbeat expires); only c's queues move — a/b assignments are stable.
        cp.register_owner(&owner("a"), ts(10));
        cp.register_owner(&owner("b"), ts(10));
        let mut moved = 0;
        for (i, q) in queues.iter().enumerate() {
            let after = cp.resolve_queue_owner(q, ts(10)).target_owner;
            if after != before[i] {
                moved += 1;
                // A moved queue was previously c's (rendezvous only reshuffles the departed owner's share).
                assert_eq!(before[i].as_ref().map(|x| x.as_str()), Some("c"));
            }
        }
        assert!(
            moved > 0 && moved < queues.len(),
            "only c's fraction moves, not all"
        );
    }
}
