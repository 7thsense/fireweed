//! Queue ownership control plane (TD-003 §Queue Ownership and Placement + §Queue Lease Lifecycle).
//!
//! The control plane is the **single fencing + assignment authority** for queue ownership, kept PLUGGABLE
//! and separate from the storage backends (ADR-008: "pluggable control plane"). It owns three things:
//!
//! 1. **The live owner set** — registered owner workers with a heartbeat; an owner is live while
//!    `heartbeat_at + heartbeat_ttl_ms > now`. fireweed never discovers owners peer-to-peer.
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

use fireweed_core::{OwnerId, UtcTimestamp};

use crate::error::{EngineError, EngineResult};
use crate::types::QueueKey;

/// The lifecycle state of a queue's ownership lease (TD-003 §Queue Lease Lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// No live active owner; the `target_owner` SHOULD acquire.
    Unassigned,
    /// An owner has durably allocated this epoch, but storage has not yet confirmed the exact fence. This
    /// lease excludes competing acquisition but is non-serving across process restarts.
    PendingFence,
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
    /// A queue with no owner yet (genesis): unassigned, epoch 0. The durable stores (in-memory map /
    /// postgres row) materialize a missing record as this.
    pub fn unassigned() -> Self {
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
    pub fn is_live(&self, now: UtcTimestamp) -> bool {
        matches!(
            self.state,
            LeaseState::PendingFence | LeaseState::Assigned | LeaseState::Draining
        ) && self.lease_expires_at.is_some_and(|exp| now < exp)
    }
}

// ---------------------------------------------------------------------------
// Pure lease decisions (TD-003 §Queue Lease Lifecycle). The state machine + the C4b seam invariants live
// HERE so EVERY `QueueControlPlane` impl (in-memory, postgres BQ-22) shares one authority — a store only
// reads the current record, applies the decision, and persists the next record. None of these consult the
// owner set (owner-liveness is the caller's store-specific check); they reason purely about one record.
// ---------------------------------------------------------------------------

/// Whether an owner's heartbeat is still live at `now` (TD-003: `heartbeat_at + ttl > now`, strict).
pub fn owner_heartbeat_live(heartbeat_at: UtcTimestamp, now: UtcTimestamp, ttl_ms: u64) -> bool {
    elapsed_ms(heartbeat_at, now) < ttl_ms
}

/// Acquire decision over the `current` record (the caller has already confirmed the owner is LIVE). Rejects
/// (carrying `current`) if a DIFFERENT owner holds a live lease; otherwise returns the acquired record. See
/// [`QueueControlPlane::acquire_queue_lease`] for the epoch contract.
///
/// EPOCH POLICY (TD-003 fence authority / BQ-20 — the fix for the self-fencing collapse, bead
/// pqueue-79178303): the `assignment_epoch` fence exists ONLY to supersede a DIFFERENT owner's writes. It is
/// advanced (`+1`) on a genuine ownership CHANGE — a different `active_owner`, or `None` (cold-start first
/// acquire / a post-`release` re-grant) — and PRESERVED on a same-owner re-affirmation where the authority
/// record still names US as `active_owner`, EVEN when our lease has lapsed (`now > lease_expires_at`).
///
/// Safety: if `current.active_owner_id` still names US at re-acquire time, then no OTHER owner acquired in
/// the lapse gap — a takeover would have set `active_owner` to them AND bumped the epoch. So our in-flight
/// writes at this epoch are legitimately ours and MUST NOT be fenced. Bumping here would fence the node's
/// OWN in-flight pushes/claims (`EpochFenced`), which under CPU starvation (late lease renewal → self-driven
/// `Unassigned`→re-acquire) collapses throughput instead of degrading gracefully. Preserving the epoch lets
/// a slow-but-alive sole owner keep serving at its existing fence — just slower. Takeover by a different
/// owner still bumps (that owner's `acquire` records itself as `active_owner` first), so real fencing is
/// untouched.
pub fn lease_decide_acquire(
    current: &QueueLease,
    owner: &OwnerId,
    now: UtcTimestamp,
    lease_ttl_ms: u64,
) -> AcquireOutcome {
    if current.is_live(now)
        && (current.active_owner_id.as_ref() != Some(owner)
            || current.state == LeaseState::PendingFence)
    {
        return AcquireOutcome::Rejected(current.clone());
    }
    // Re-affirming OUR OWN (still-recorded) lease preserves the epoch; a takeover/cold-start advances it.
    let same_owner = current.active_owner_id.as_ref() == Some(owner)
        && current.state != LeaseState::PendingFence;
    let assignment_epoch = if same_owner {
        current.assignment_epoch
    } else {
        current.assignment_epoch + 1
    };
    AcquireOutcome::Acquired(QueueLease {
        state: if same_owner {
            LeaseState::Assigned
        } else {
            LeaseState::PendingFence
        },
        active_owner_id: Some(owner.clone()),
        target_owner_id: Some(owner.clone()),
        assignment_epoch,
        lease_expires_at: Some(add_millis(now, lease_ttl_ms)),
    })
}

/// Mark an acquired lease serving only after its exact storage fence is durable. Idempotent for the same
/// owner/epoch, and rejected after expiry or takeover.
pub fn lease_decide_confirm_fence(
    current: &QueueLease,
    owner: &OwnerId,
    expected_epoch: u64,
    now: UtcTimestamp,
) -> EngineResult<QueueLease> {
    if current.active_owner_id.as_ref() != Some(owner)
        || current.assignment_epoch != expected_epoch
        || !current.is_live(now)
        || !matches!(
            current.state,
            LeaseState::PendingFence | LeaseState::Assigned
        )
    {
        return Err(EngineError::EpochFenced);
    }
    Ok(QueueLease {
        state: LeaseState::Assigned,
        ..current.clone()
    })
}

/// Renew decision: extend the deadline at the SAME epoch. `queue-epoch-stale` ([`EngineError::EpochFenced`])
/// on wrong owner, wrong epoch, or an already-reclaimed (expired) lease.
pub fn lease_decide_renew(
    current: &QueueLease,
    owner: &OwnerId,
    expected_epoch: u64,
    now: UtcTimestamp,
    lease_ttl_ms: u64,
) -> EngineResult<QueueLease> {
    if current.active_owner_id.as_ref() != Some(owner)
        || current.assignment_epoch != expected_epoch
        || !matches!(current.state, LeaseState::Assigned | LeaseState::Draining)
        || !current.is_live(now)
    {
        return Err(EngineError::EpochFenced);
    }
    let requested_expiry = add_millis(now, lease_ttl_ms);
    Ok(QueueLease {
        // Concurrent node-level batches may reach the authority in a different order from the
        // clocks they sampled. A valid renewal must never shorten an already-extended lease.
        lease_expires_at: Some(
            current
                .lease_expires_at
                .map_or(requested_expiry, |expiry| expiry.max(requested_expiry)),
        ),
        ..current.clone()
    })
}

/// Begin-drain decision: optimistic-concurrency-checked against `expected_epoch` (stale → `EpochFenced`),
/// valid only on a live `assigned` lease handed to a DIFFERENT target.
pub fn lease_decide_begin_drain(
    current: &QueueLease,
    expected_epoch: u64,
    target_owner: &OwnerId,
    now: UtcTimestamp,
) -> EngineResult<QueueLease> {
    if current.assignment_epoch != expected_epoch || !current.is_live(now) {
        return Err(EngineError::EpochFenced);
    }
    if current.state != LeaseState::Assigned {
        return Err(EngineError::Conflict);
    }
    if current.active_owner_id.as_ref() == Some(target_owner) {
        return Err(EngineError::Invalid("drain target is the active owner"));
    }
    Ok(QueueLease {
        state: LeaseState::Draining,
        target_owner_id: Some(target_owner.clone()),
        ..current.clone()
    })
}

/// Release decision: only the active owner at `expected_epoch` may release. Returns the unassigned record
/// with the epoch RETAINED (the next acquire allocates a strictly-greater one, fencing this owner's
/// stragglers). `queue-epoch-stale` otherwise.
pub fn lease_decide_release(
    current: &QueueLease,
    owner: &OwnerId,
    expected_epoch: u64,
) -> EngineResult<QueueLease> {
    if current.active_owner_id.as_ref() != Some(owner)
        || current.assignment_epoch != expected_epoch
        || !matches!(
            current.state,
            LeaseState::PendingFence | LeaseState::Assigned | LeaseState::Draining
        )
    {
        return Err(EngineError::EpochFenced);
    }
    Ok(QueueLease {
        state: LeaseState::Unassigned,
        active_owner_id: None,
        target_owner_id: None,
        lease_expires_at: None,
        assignment_epoch: current.assignment_epoch,
    })
}

/// Build the [`OwnerResolution`] a caller acts on: the deterministic `target` plus the `current` record,
/// reported as `unassigned` when its lease has expired (lease liveness governs, not the stale stored flag).
pub fn lease_resolution(
    current: &QueueLease,
    target: Option<OwnerId>,
    now: UtcTimestamp,
) -> OwnerResolution {
    let (state, active, expires) = if current.is_live(now) {
        (
            current.state,
            current.active_owner_id.clone(),
            current.lease_expires_at,
        )
    } else {
        (LeaseState::Unassigned, None, None)
    };
    OwnerResolution {
        target_owner: target,
        active_owner: active,
        // epoch 0 == genesis (never granted) → None; a granted lease is always >= 1.
        assignment_epoch: (current.assignment_epoch > 0).then_some(current.assignment_epoch),
        lease_expires_at: expires,
        state,
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

/// A control-plane advertisement mapping one live owner to its client-reachable RESP endpoint.
/// The control plane treats the endpoint as opaque data; the server validates it before publishing and
/// again before routing so malformed durable rows can never become redirects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerEndpointAdvertisement {
    pub owner: OwnerId,
    pub endpoint: String,
    /// Heartbeat-derived liveness deadline. Consumers must not route at or beyond this instant even if
    /// their node-level refresh loop has not run yet.
    pub expires_at: UtcTimestamp,
}

/// One queue lease renewal submitted as part of a node-level batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRenewal {
    pub queue: QueueKey,
    pub owner: OwnerId,
    pub expected_epoch: u64,
}

/// One ordered result from [`QueueControlPlane::renew_queue_leases`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseRenewalOutcome {
    Renewed(QueueLease),
    Fenced,
    Missing,
    Error(EngineError),
}

/// The pluggable control-plane seam (TD-003 §Queue Lease Lifecycle). All lease ops are transactional: each
/// is one atomic mutation of the authority record. The production impl is transactional-postgres (BQ-22);
/// [`InMemoryControlPlane`] is the reference + the default for single-node / tests.
pub trait QueueControlPlane: Send + Sync {
    /// Register (or refresh) an owner worker in the candidate set with a heartbeat at `now`. Fallible: a
    /// durable store (postgres) can fail the write — surfaced rather than silently swallowed (a swallowed
    /// failure would leave the owner looking dead, which is fail-safe but must not be hidden).
    fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()>;

    /// Atomically register/heartbeat an owner and publish its client-reachable endpoint. A server calls
    /// this once at startup and once per node-level ownership tick, never once per managed queue.
    fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        _endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        // Compatibility default for third-party control planes: preserve membership but publish no
        // redirect. The corresponding empty-list default below therefore fails closed.
        self.register_owner(owner, now)
    }

    /// Return endpoint advertisements for the live owner set at `now`. Expired and unadvertised owners
    /// are omitted. Callers must still validate the opaque endpoint before using it for a redirect.
    fn live_owner_endpoints(
        &self,
        _now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
        Ok(Vec::new())
    }

    /// Refresh an owner's heartbeat. An owner whose heartbeat has expired leaves the live set (changing
    /// future `target_owner` computations) but its lease is reclaimed only via `lease_expires_at`.
    fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()>;

    /// The deterministic `target_owner` (rendezvous/HRW over the live owner set) plus the current authority
    /// record. `target_owner` is `None` iff no owner is live (fail-closed). FALLIBLE: a durable store
    /// (postgres) MUST surface a read failure rather than fabricate an `unassigned` record — a fabricated
    /// "unowned" would invite a spurious acquire and hide a control-plane outage (TD-003 fail-closed).
    fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution>;

    /// Resolve a node's queue inventory in input order. Durable implementations override this so one
    /// assignment poll uses a fixed number of statements rather than one round trip per queue.
    fn resolve_queue_owners(
        &self,
        queues: &[QueueKey],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerResolution>> {
        queues
            .iter()
            .map(|queue| self.resolve_queue_owner(queue, now))
            .collect()
    }

    /// Acquire the queue at a strictly-greater, durably-recorded epoch (TD-003 acquire). Rejects if a
    /// DIFFERENT owner holds a live (`assigned`/`draining`, non-expired) lease. The caller MUST be a live
    /// registered owner (fail-closed) — else `EngineError::Forbidden`.
    ///
    /// CONTRACT — the epoch advances ONLY on a genuine ownership CHANGE (a DIFFERENT owner takes over an
    /// expired/free lease, or the first acquire of an unowned/released queue). A same-owner re-acquire — the
    /// authority record still names the acquirer as `active_owner`, even with a LAPSED lease — PRESERVES the
    /// epoch and only refreshes `lease_expires_at` (bead pqueue-79178303). Rationale: the epoch fence exists
    /// to supersede a DIFFERENT owner; if the record still names us, no other owner acquired in the gap, so
    /// our in-flight epoch-N writes are legitimately ours and must NOT be self-fenced. A consequence: a
    /// same-owner retry after a timeout is now epoch-idempotent (it will not double-bump and fence the
    /// caller's own writes). To merely extend a still-live lease without re-resolving, prefer
    /// [`renew_queue_lease`](Self::renew_queue_lease) (same epoch); a takeover by a different owner still
    /// allocates a strictly-greater epoch (fencing the superseded owner).
    fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome>;

    /// Durably mark a newly acquired epoch serving after its exact storage fence succeeds. Implementations
    /// that cannot persist this transition fail closed.
    fn confirm_queue_lease_fence(
        &self,
        _queue: &QueueKey,
        _owner: &OwnerId,
        _expected_epoch: u64,
        _now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        Err(EngineError::Unavailable)
    }

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

    /// Renew a node's owned queues as one logical batch while preserving input order and independent
    /// per-queue outcomes. Durable implementations override this to use a fixed number of statements and
    /// one transaction; the compatibility default preserves behavior for in-process and third-party stores.
    fn renew_queue_leases(
        &self,
        renewals: &[LeaseRenewal],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<LeaseRenewalOutcome>> {
        Ok(renewals
            .iter()
            .map(|renewal| {
                match self.renew_queue_lease(
                    &renewal.queue,
                    &renewal.owner,
                    renewal.expected_epoch,
                    now,
                ) {
                    Ok(lease) => LeaseRenewalOutcome::Renewed(lease),
                    Err(EngineError::EpochFenced) => LeaseRenewalOutcome::Fenced,
                    Err(error) => LeaseRenewalOutcome::Error(error),
                }
            })
            .collect())
    }

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

    /// Read the current authority record (genesis `unassigned`/epoch 0 for a never-owned queue). FALLIBLE
    /// for the same fail-closed reason as [`resolve_queue_owner`](Self::resolve_queue_owner): a fabricated
    /// epoch-0 record on a DB error is the worst possible value to feed the append fence (BQ-23).
    fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease>;

    /// Whether this control plane loses its epoch state on restart (e.g., `InMemoryControlPlane`).
    /// A `false` return means the control plane durably binds the assignment epoch and survives restart
    /// with its epoch state intact. Used by [`acquire_and_fence`](crate::acquire_and_fence) to decide
    /// whether `backend.current_epoch > lease.assignment_epoch` after a successful acquire is a safe
    /// restart-reconciliation scenario or a genuine inconsistency that must fail closed.
    fn is_ephemeral(&self) -> bool {
        false
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

/// `now + ms` as a normalized [`UtcTimestamp`] (seconds saturate; nanos carry). Used for lease deadlines.
pub fn add_millis(t: UtcTimestamp, ms: u64) -> UtcTimestamp {
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
pub fn resolve_target<'a>(
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
    /// owner → opaque advertised endpoint; liveness is always determined by `owners` above.
    endpoints: HashMap<OwnerId, String>,
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

    fn is_owner_live(
        &self,
        owners: &HashMap<OwnerId, UtcTimestamp>,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> bool {
        owners
            .get(owner)
            .is_some_and(|hb| owner_heartbeat_live(*hb, now, self.config.heartbeat_ttl_ms))
    }

    fn live_owners<'a>(
        &self,
        owners: &'a HashMap<OwnerId, UtcTimestamp>,
        now: UtcTimestamp,
    ) -> Vec<&'a OwnerId> {
        owners
            .iter()
            .filter(|(_, hb)| owner_heartbeat_live(**hb, now, self.config.heartbeat_ttl_ms))
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
    fn is_ephemeral(&self) -> bool {
        true
    }

    fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        self.state
            .lock()
            .expect("poisoned")
            .owners
            .insert(owner.clone(), now);
        Ok(())
    }

    fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let mut state = self.state.lock().expect("poisoned");
        state.owners.insert(owner.clone(), now);
        state.endpoints.insert(owner.clone(), endpoint.to_string());
        Ok(())
    }

    fn live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
        let state = self.state.lock().expect("poisoned");
        Ok(state
            .endpoints
            .iter()
            .filter(|(owner, _)| self.is_owner_live(&state.owners, owner, now))
            .map(|(owner, endpoint)| OwnerEndpointAdvertisement {
                owner: owner.clone(),
                endpoint: endpoint.clone(),
                expires_at: add_millis(state.owners[owner], self.config.heartbeat_ttl_ms),
            })
            .collect())
    }

    fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        // Register-on-heartbeat is intentional: a heartbeat from an unknown owner re-admits it (a node that
        // briefly fell out of the live set rejoins), matching the postgres upsert.
        self.state
            .lock()
            .expect("poisoned")
            .owners
            .insert(owner.clone(), now);
        Ok(())
    }

    fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution> {
        let g = self.state.lock().expect("poisoned");
        let target = resolve_target(queue, self.live_owners(&g.owners, now).into_iter());
        let current = g
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned);
        Ok(lease_resolution(&current, target, now))
    }

    fn resolve_queue_owners(
        &self,
        queues: &[QueueKey],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerResolution>> {
        let state = self.state.lock().expect("poisoned");
        let live = self.live_owners(&state.owners, now);
        Ok(queues
            .iter()
            .map(|queue| {
                let target = resolve_target(queue, live.iter().copied());
                let current = state
                    .leases
                    .get(queue)
                    .cloned()
                    .unwrap_or_else(QueueLease::unassigned);
                lease_resolution(&current, target, now)
            })
            .collect())
    }

    fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let mut g = self.state.lock().expect("poisoned");
        // Fail-closed: only a live registered owner may acquire (the store-specific liveness check).
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
        let outcome = lease_decide_acquire(&current, owner, now, self.config.lease_ttl_ms);
        if let AcquireOutcome::Acquired(ref acquired) = outcome {
            g.leases.insert(queue.clone(), acquired.clone());
        }
        Ok(outcome)
    }

    fn confirm_queue_lease_fence(
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
        let confirmed = lease_decide_confirm_fence(&current, owner, expected_epoch, now)?;
        g.leases.insert(queue.clone(), confirmed.clone());
        Ok(confirmed)
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
        let renewed = lease_decide_renew(
            &current,
            owner,
            expected_epoch,
            now,
            self.config.lease_ttl_ms,
        )?;
        g.leases.insert(queue.clone(), renewed.clone());
        Ok(renewed)
    }

    fn renew_queue_leases(
        &self,
        renewals: &[LeaseRenewal],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<LeaseRenewalOutcome>> {
        let mut state = self.state.lock().expect("poisoned");
        Ok(renewals
            .iter()
            .map(|renewal| {
                let Some(current) = state.leases.get(&renewal.queue).cloned() else {
                    return LeaseRenewalOutcome::Missing;
                };
                match lease_decide_renew(
                    &current,
                    &renewal.owner,
                    renewal.expected_epoch,
                    now,
                    self.config.lease_ttl_ms,
                ) {
                    Ok(renewed) => {
                        state.leases.insert(renewal.queue.clone(), renewed.clone());
                        LeaseRenewalOutcome::Renewed(renewed)
                    }
                    Err(EngineError::EpochFenced) => LeaseRenewalOutcome::Fenced,
                    Err(error) => LeaseRenewalOutcome::Error(error),
                }
            })
            .collect())
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
        let draining = lease_decide_begin_drain(&current, expected_epoch, target_owner, now)?;
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
        let released = lease_decide_release(&current, owner, expected_epoch)?;
        let _ = now; // release validates by epoch, not time (an expired lease is already reclaimable)
        g.leases.insert(queue.clone(), released);
        Ok(())
    }

    fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
        Ok(self
            .state
            .lock()
            .expect("poisoned")
            .leases
            .get(queue)
            .cloned()
            .unwrap_or_else(QueueLease::unassigned))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::{QueueId, TenantId};

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
    fn confirm(cp: &InMemoryControlPlane, queue: &QueueKey, owner: &OwnerId, epoch: u64, at: i64) {
        cp.confirm_queue_lease_fence(queue, owner, epoch, ts(at))
            .unwrap();
    }

    // ----- lifecycle -----

    #[test]
    fn full_lifecycle_acquire_renew_drain_release_reacquire() {
        let cp = cp();
        let a = owner("a");
        let b = owner("b");
        let q = qk("q1");
        cp.register_owner(&a, ts(0)).unwrap();

        // Acquire allocates epoch 1 in a durable non-serving state; storage confirmation promotes it.
        let AcquireOutcome::Acquired(l1) = cp.acquire_queue_lease(&q, &a, ts(0)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(l1.assignment_epoch, 1);
        assert_eq!(l1.state, LeaseState::PendingFence);
        assert_eq!(l1.active_owner_id.as_ref(), Some(&a));
        assert_eq!(l1.lease_expires_at, Some(ts(15)));
        let l1 = cp.confirm_queue_lease_fence(&q, &a, 1, ts(0)).unwrap();
        assert_eq!(l1.state, LeaseState::Assigned);

        // Renew before expiry: same epoch, extended deadline.
        let l2 = cp.renew_queue_lease(&q, &a, 1, ts(10)).unwrap();
        assert_eq!(l2.assignment_epoch, 1, "renew never changes the epoch");
        assert_eq!(l2.lease_expires_at, Some(ts(25)));

        // begin_drain toward b: draining, target=b, still epoch 1.
        cp.register_owner(&b, ts(10)).unwrap();
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
        let rel = cp.lease(&q).unwrap();
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
        cp.register_owner(&a, ts(0)).unwrap();
        cp.register_owner(&b, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap();

        // b acquires while a's lease is live → Rejected, carrying a's authority record (b heartbeats so it
        // is a live owner; a's LEASE — not a's heartbeat — is what blocks the acquire).
        cp.heartbeat(&b, ts(4)).unwrap();
        let AcquireOutcome::Rejected(held) = cp.acquire_queue_lease(&q, &b, ts(4)).unwrap() else {
            panic!("expected Rejected");
        };
        assert_eq!(held.active_owner_id.as_ref(), Some(&a));
        assert_eq!(held.assignment_epoch, 1);
        // a's lease is untouched (no epoch bump from a rejected acquire).
        assert_eq!(cp.lease(&q).unwrap().assignment_epoch, 1);
    }

    #[test]
    fn the_same_owner_reaffirming_its_live_lease_preserves_the_epoch() {
        // A re-acquire by the SAME owner (still the recorded active_owner) re-affirms an uncontested lease:
        // it PRESERVES the epoch and only refreshes the deadline — it must NOT self-fence the owner's own
        // in-flight epoch-N writes (bead pqueue-79178303). Epoch advances only on a takeover by a DIFFERENT
        // owner.
        let cp = cp();
        let a = owner("a");
        let q = qk("q1");
        cp.register_owner(&a, ts(0)).unwrap();
        let AcquireOutcome::Acquired(l1) = cp.acquire_queue_lease(&q, &a, ts(0)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(l1.assignment_epoch, 1);
        confirm(&cp, &q, &a, 1, 0);
        // Re-acquire while still live: same epoch, deadline pushed out from ts(1).
        let AcquireOutcome::Acquired(l2) = cp.acquire_queue_lease(&q, &a, ts(1)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(
            l2.assignment_epoch, 1,
            "same-owner re-affirm preserves epoch"
        );
        assert_eq!(l2.lease_expires_at, Some(ts(16)), "deadline refreshed");
    }

    #[test]
    fn the_same_owner_reacquiring_its_lapsed_lease_preserves_the_epoch() {
        // THE self-fencing-collapse fix (bead pqueue-79178303): a sole owner whose lease TTL lapsed (the
        // renew task ran late under CPU starvation) re-acquires its OWN queue. The authority record still
        // names it active_owner, so no other owner took over in the gap → preserve the epoch (no self-fence),
        // just refresh liveness. This is graceful degradation, not collapse.
        let cp = cp();
        let a = owner("a");
        let q = qk("q1");
        cp.register_owner(&a, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap(); // epoch 1, expires ts(15)
        confirm(&cp, &q, &a, 1, 0);
        // Keep the owner heartbeat-live, but re-acquire only AFTER the lease has lapsed (ts(100) > ts(15)).
        cp.heartbeat(&a, ts(100)).unwrap();
        let lease_before = cp.lease(&q).unwrap();
        assert!(
            !lease_before.is_live(ts(100)),
            "precondition: the lease has actually lapsed"
        );
        assert_eq!(lease_before.active_owner_id.as_ref(), Some(&a));
        let AcquireOutcome::Acquired(l2) = cp.acquire_queue_lease(&q, &a, ts(100)).unwrap() else {
            panic!("expected Acquired (same owner re-affirms its lapsed lease)");
        };
        assert_eq!(
            l2.assignment_epoch, 1,
            "re-affirming a LAPSED own lease must NOT bump the epoch (no self-fence)"
        );
        assert_eq!(l2.lease_expires_at, Some(ts(115)), "deadline refreshed");
        assert_eq!(l2.active_owner_id.as_ref(), Some(&a));
    }

    // ----- seam invariant: monotonic epoch + expired-lease reclaim -----

    #[test]
    fn expired_lease_is_reclaimable_at_a_strictly_greater_epoch() {
        let cp = cp();
        let a = owner("a");
        let b = owner("b");
        let q = qk("q1");
        cp.register_owner(&a, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap(); // epoch 1, expires ts(15)

        // After expiry (and a's heartbeat gone), b is live and acquires → epoch 2 (no rejection).
        cp.register_owner(&b, ts(20)).unwrap();
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
        cp.register_owner(&a, ts(0)).unwrap();
        let mut prev = 0;
        // Acquire → release → acquire → release ... epoch must climb strictly each acquire.
        for i in 0..5 {
            let t = ts(i * 100);
            cp.heartbeat(&a, t).unwrap(); // keep the owner live across the time jumps
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
        cp.register_owner(&a, ts(0)).unwrap();
        let AcquireOutcome::Acquired(l) = cp.acquire_queue_lease(&q, &a, ts(0)).unwrap() else {
            panic!("acquire");
        };
        // The durable record already reflects the new epoch the instant acquire returns (the fence is in
        // place before the owner serves) — resolve + lease both see epoch 1.
        assert_eq!(cp.lease(&q).unwrap().assignment_epoch, l.assignment_epoch);
        assert_eq!(
            cp.resolve_queue_owner(&q, ts(0)).unwrap().assignment_epoch,
            Some(1)
        );
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
        cp.register_owner(&a, ts(0)).unwrap();
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
        cp.register_owner(&a, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap(); // epoch 1
        confirm(&cp, &q, &a, 1, 0);

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
        cp.register_owner(&a, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap();
        cp.register_owner(&b, ts(20)).unwrap();
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
        assert_eq!(
            cp.resolve_queue_owner(&q, ts(0)).unwrap().target_owner,
            None
        );
        // Registered then expired → still no target.
        cp.register_owner(&owner("a"), ts(0)).unwrap();
        assert_eq!(
            cp.resolve_queue_owner(&q, ts(100)).unwrap().target_owner,
            None
        );
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
            cp.register_owner(o, ts(0)).unwrap();
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
            cp.lease(&q).unwrap().assignment_epoch,
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
            cp1.register_owner(&owner(o), ts(0)).unwrap();
            cp2.register_owner(&owner(o), ts(0)).unwrap();
        }
        // Same live set + same queue → same target, on two independent control planes (no run-to-run
        // randomness), and stable across repeated calls.
        for q in ["q1", "q2", "q3", "q4", "q5"] {
            let t1 = cp1.resolve_queue_owner(&qk(q), ts(0)).unwrap().target_owner;
            let t2 = cp2.resolve_queue_owner(&qk(q), ts(1)).unwrap().target_owner;
            assert!(t1.is_some());
            assert_eq!(t1, t2, "HRW must be a pure function");
        }
    }

    #[test]
    fn hrw_spreads_queues_and_moves_only_a_fraction_when_an_owner_leaves() {
        let cp = cp();
        for o in ["a", "b", "c"] {
            cp.register_owner(&owner(o), ts(0)).unwrap();
        }
        let queues: Vec<QueueKey> = (0..60).map(|i| qk(&format!("q{i}"))).collect();
        let before: Vec<Option<OwnerId>> = queues
            .iter()
            .map(|q| cp.resolve_queue_owner(q, ts(0)).unwrap().target_owner)
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
        cp.register_owner(&owner("a"), ts(10)).unwrap();
        cp.register_owner(&owner("b"), ts(10)).unwrap();
        let mut moved = 0;
        for (i, q) in queues.iter().enumerate() {
            let after = cp.resolve_queue_owner(q, ts(10)).unwrap().target_owner;
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
