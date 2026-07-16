//! Per-node density primitives (TD-003 §per-node density / scale — a single node holds MANY queues
//! cheaply). The engine-level, unit-testable CORE; this delivers the LRU-bounding + the renewal sweep. The
//! pieces the bead also names — the per-node assignment POLL and the shared SWEEPER TASK, plus the server
//! loop that drives all of this on a cadence — are NOT built here; they are the server-runtime follow-up
//! (pqueue-7bac12ce threaded the data-plane fence epoch; the sweeper/runtime loop is a separate follow-up).
//! Two primitives:
//!
//! 1. [`ResidentQueues`] — an LRU-BOUNDED set of the queues a node keeps HOT (a resident per-queue handle:
//!    the in-memory projection for the log-replay family, or the lease/session for the relational family).
//!    A node may be ASSIGNED far more queues than it keeps resident: admitting past the cap evicts the
//!    least-recently-used queue and HANDS its handle BACK to the caller. This bounds the resident-set
//!    CARDINALITY (the hot working set) by the cap, not by the assigned-queue count.
//!
//!    IMPORTANT — bounding cardinality is NECESSARY but not SUFFICIENT for bounded resources: the evicted
//!    handle is RETURNED, not released. The caller MUST then release that queue's lease
//!    ([`QueueControlPlane::release_queue_lease`]) and drop the handle; until it does, the evicted queue
//!    keeps its lease (the new owner can only reclaim it at TTL expiry). Wiring that release on the server
//!    loop is the follow-up; this primitive only surfaces the eviction.
//! 2. [`renew_all_resident`] — a per-resident renewal SWEEP: one node call that renews every resident
//!    queue's lease, partitioning into renewed / fenced / errored. NOTE: this is N independent control-plane
//!    `renew_queue_lease` calls in a loop (one round-trip per queue), NOT a single batched statement — a
//!    true multi-lease batch is a postgres optimization left to the follow-up. A FENCED lease (the node lost
//!    the queue to a newer owner) is evicted + shed; a TRANSIENT control-plane error leaves the queue
//!    resident to retry next sweep (never sheds on a non-fence error).

use std::collections::HashMap;

use pqueue_core::UtcTimestamp;

use crate::control_plane::QueueControlPlane;
use crate::error::EngineError;
use crate::ownership::OwnedSession;
use crate::types::QueueKey;

/// An LRU-bounded set of resident per-queue handles. `cap == 0` is rejected at construction (a node must
/// keep at least one queue hot to make progress). Recency is a monotonic per-instance counter (not wall
/// clock), so eviction order is deterministic and test-stable.
pub struct ResidentQueues<H> {
    cap: usize,
    tick: u64,
    /// queue → (handle, last-used recency).
    entries: HashMap<QueueKey, (H, u64)>,
}

impl<H> ResidentQueues<H> {
    /// Create a resident set bounded to `cap` hot queues. Panics if `cap == 0` (a programming error — a
    /// node with a zero working set can never serve).
    pub fn new(cap: usize) -> Self {
        assert!(cap > 0, "resident-queue cap must be > 0");
        ResidentQueues {
            cap,
            tick: 0,
            entries: HashMap::new(),
        }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, queue: &QueueKey) -> bool {
        self.entries.contains_key(queue)
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    /// The current least-recently-used resident queue (the next eviction victim), if any.
    fn lru_victim(&self) -> Option<QueueKey> {
        self.entries
            .iter()
            .min_by_key(|(_, (_, recency))| *recency)
            .map(|(q, _)| q.clone())
    }

    /// Make `queue` resident with `handle`, returning any queue EVICTED to stay within the cap (its handle,
    /// for the caller to release the lease + drop). Re-admitting an already-resident queue refreshes its
    /// recency + replaces its handle and evicts NOTHING (it was already counted). Admitting a NEW queue at
    /// capacity evicts the LRU resident (never the queue being admitted).
    pub fn admit(&mut self, queue: QueueKey, handle: H) -> Option<(QueueKey, H)> {
        let recency = self.next_tick();
        if let Some(slot) = self.entries.get_mut(&queue) {
            // Already resident: refresh recency + handle, no eviction.
            slot.0 = handle;
            slot.1 = recency;
            return None;
        }
        // New queue: evict the LRU first if we are at capacity (the new queue is NOT yet inserted, so it can
        // never be its own victim).
        let evicted = if self.entries.len() >= self.cap {
            let victim = self.lru_victim().expect("non-empty at capacity");
            self.entries.remove(&victim).map(|(h, _)| (victim, h))
        } else {
            None
        };
        self.entries.insert(queue, (handle, recency));
        evicted
    }

    /// Promote `queue` to most-recently-used (call on access/claim so a hot queue is not evicted). Returns
    /// whether it was resident.
    pub fn touch(&mut self, queue: &QueueKey) -> bool {
        let recency = self.next_tick();
        match self.entries.get_mut(queue) {
            Some(slot) => {
                slot.1 = recency;
                true
            }
            None => false,
        }
    }

    pub fn get(&self, queue: &QueueKey) -> Option<&H> {
        self.entries.get(queue).map(|(h, _)| h)
    }

    /// Mutable access to a resident handle, PROMOTING it to most-recently-used (a mutable access is a use,
    /// so an actively-mutated queue is never evicted as "LRU").
    pub fn get_mut(&mut self, queue: &QueueKey) -> Option<&mut H> {
        let recency = self.next_tick();
        let slot = self.entries.get_mut(queue)?;
        slot.1 = recency;
        Some(&mut slot.0)
    }

    /// Explicitly evict `queue` (e.g. on a fenced renewal or a deliberate release), returning its handle.
    pub fn evict(&mut self, queue: &QueueKey) -> Option<H> {
        self.entries.remove(queue).map(|(h, _)| h)
    }

    /// The resident queues (unordered) — for a shared per-node sweep / batched renewal pass.
    pub fn queues(&self) -> impl Iterator<Item = &QueueKey> {
        self.entries.keys()
    }
}

/// The outcome of a [`renew_all_resident`] sweep, partitioning every resident queue:
/// - `renewed` — lease extended at the same epoch;
/// - `fenced` — `queue-epoch-stale`: a newer owner took over, so the queue was EVICTED from the resident
///   set (the node lost it, no release owed — the lease is already the new owner's);
/// - `errored` — a TRANSIENT control-plane failure: the queue is LEFT RESIDENT (not shed) to retry next
///   sweep, and the error is surfaced here so a swallowed failure is countable, not silent.
///
/// The sweep ALWAYS completes the full pass and returns the full partition — it never aborts mid-pass and
/// loses the `fenced`/`renewed` already accumulated (the server loop needs the fenced list to stop serving
/// shed queues even when some other queue's renewal hit a transient error).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RenewSweep {
    pub renewed: Vec<QueueKey>,
    pub fenced: Vec<QueueKey>,
    pub errored: Vec<(QueueKey, EngineError)>,
}

/// A per-resident renewal sweep: renew every resident queue's lease (N control-plane round-trips, one per
/// queue — see the module doc on why this is not a single batched statement). A FENCED renewal evicts +
/// sheds the queue; a TRANSIENT error leaves it resident to retry; the full [`RenewSweep`] partition is
/// always returned (never a mid-pass abort).
pub fn renew_all_resident<CP>(
    control_plane: &CP,
    residents: &mut ResidentQueues<OwnedSession>,
    now: UtcTimestamp,
) -> RenewSweep
where
    CP: QueueControlPlane,
{
    let mut sweep = RenewSweep::default();
    // Snapshot the (queue, session) set first so we can mutate `residents` while iterating (single-threaded
    // &mut — no concurrent admit is possible).
    let targets: Vec<(QueueKey, OwnedSession)> = residents
        .entries
        .iter()
        .map(|(q, (s, _))| (q.clone(), s.clone()))
        .collect();
    for (queue, session) in targets {
        match control_plane.renew_queue_lease(&queue, &session.owner, session.lease_epoch, now) {
            Ok(_) => sweep.renewed.push(queue),
            Err(EngineError::EpochFenced) => {
                // Lost to a newer owner: shed it. No release owed (the lease is already theirs).
                residents.evict(&queue);
                sweep.fenced.push(queue);
            }
            // Transient (e.g. control-plane unreachable): keep the queue resident, retry next sweep.
            Err(other) => sweep.errored.push((queue, other)),
        }
    }
    sweep
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqueue_core::{OwnerId, QueueId, TenantId};

    fn qk(q: &str) -> QueueKey {
        QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new(q).unwrap())
    }

    #[test]
    fn resident_set_stays_bounded_as_queue_count_grows_to_1000() {
        // The density acceptance: a node ASSIGNED 1000 queues keeps only `cap` hot — resources are bounded
        // by the cap, not the assigned count. Every admit past the cap evicts exactly one LRU queue.
        let cap = 64;
        let mut residents: ResidentQueues<u32> = ResidentQueues::new(cap);
        let mut evictions = 0;
        for i in 0..1000 {
            let evicted = residents.admit(qk(&format!("q{i}")), i);
            if let Some((_q, _h)) = evicted {
                evictions += 1;
            }
            // The resident set NEVER exceeds the cap, at any point along the way.
            assert!(residents.len() <= cap, "resident set exceeded its cap");
        }
        assert_eq!(residents.len(), cap, "the hot set saturates at the cap");
        assert_eq!(
            evictions,
            1000 - cap,
            "every queue admitted past the cap evicted exactly one LRU queue"
        );
    }

    #[test]
    fn admit_evicts_the_least_recently_used() {
        let mut residents: ResidentQueues<&str> = ResidentQueues::new(2);
        assert!(residents.admit(qk("a"), "a").is_none());
        assert!(residents.admit(qk("b"), "b").is_none());
        // Touch "a" so "b" is now the LRU; admitting "c" evicts "b", not "a".
        assert!(residents.touch(&qk("a")));
        let evicted = residents.admit(qk("c"), "c").expect("eviction at cap");
        assert_eq!(
            evicted.0,
            qk("b"),
            "the least-recently-used queue is evicted"
        );
        assert!(residents.contains(&qk("a")) && residents.contains(&qk("c")));
        assert!(!residents.contains(&qk("b")));
    }

    #[test]
    fn re_admitting_a_resident_queue_evicts_nothing() {
        let mut residents: ResidentQueues<i32> = ResidentQueues::new(2);
        residents.admit(qk("a"), 1);
        residents.admit(qk("b"), 2);
        // Re-admit "a" with a new handle — already counted, so no eviction; the handle is replaced.
        assert!(residents.admit(qk("a"), 99).is_none());
        assert_eq!(residents.get(&qk("a")), Some(&99));
        assert_eq!(residents.len(), 2);
    }

    // ----- batched renewal -----

    use crate::control_plane::{AcquireOutcome, InMemoryControlPlane, QueueControlPlane};

    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }
    fn owner(s: &str) -> OwnerId {
        OwnerId::new(s).unwrap()
    }

    /// Acquire `q` for `o` through the control plane and wrap it in an `OwnedSession` (the `fence_epoch` is
    /// irrelevant to renewal — renew keys off the lease epoch — so it is left 0 here).
    fn own(
        cp: &InMemoryControlPlane,
        q: &QueueKey,
        o: &OwnerId,
        now: UtcTimestamp,
    ) -> OwnedSession {
        cp.register_owner(o, now).unwrap();
        let AcquireOutcome::Acquired(lease) = cp.acquire_queue_lease(q, o, now).unwrap() else {
            panic!("acquire {}", q.queue_id.as_str());
        };
        cp.confirm_queue_lease_fence(q, o, lease.assignment_epoch, now)
            .unwrap();
        OwnedSession {
            owner: o.clone(),
            queue: q.clone(),
            lease_epoch: lease.assignment_epoch,
            fence_epoch: 0,
        }
    }

    #[test]
    fn batched_renewal_renews_all_resident_leases_in_one_pass() {
        let cp = InMemoryControlPlane::default();
        let a = owner("a");
        let mut residents: ResidentQueues<OwnedSession> = ResidentQueues::new(8);
        for i in 0..5 {
            let q = qk(&format!("q{i}"));
            let session = own(&cp, &q, &a, ts(0));
            residents.admit(q, session);
        }
        // One sweep renews all 5 before any expires.
        let sweep = renew_all_resident(&cp, &mut residents, ts(5));
        assert_eq!(sweep.renewed.len(), 5);
        assert!(sweep.fenced.is_empty() && sweep.errored.is_empty());
        assert_eq!(residents.len(), 5, "all leases held; nothing shed");
    }

    #[test]
    fn batched_renewal_sheds_a_fenced_queue() {
        let cp = InMemoryControlPlane::default();
        let (a, b) = (owner("a"), owner("b"));
        let q = qk("q1");
        let sa = own(&cp, &q, &a, ts(0));
        let mut residents: ResidentQueues<OwnedSession> = ResidentQueues::new(8);
        residents.admit(q.clone(), sa);

        // A's lease lapses; B takes over (a strictly-greater lease epoch).
        let _sb = own(&cp, &q, &b, ts(100_000));

        // A's renewal sweep now finds its lease fenced → the queue is shed from the resident set.
        let sweep = renew_all_resident(&cp, &mut residents, ts(100_001));
        assert!(sweep.renewed.is_empty());
        assert_eq!(sweep.fenced, vec![q.clone()]);
        assert!(!residents.contains(&q), "a fenced (lost) queue is evicted");
    }

    /// I1: cardinality is bounded by the cap, AND eviction surfaces the still-valid lease so the caller can
    /// RELEASE it (proving the bound is a real resource bound, not just a count): the evicted owner releases
    /// its lease, after which a DIFFERENT owner can acquire the evicted queue.
    #[test]
    fn eviction_surfaces_the_lease_for_release_so_resources_are_actually_bounded() {
        let cp = InMemoryControlPlane::default();
        let (a, b) = (owner("a"), owner("b"));
        let mut residents: ResidentQueues<OwnedSession> = ResidentQueues::new(1);

        let q1 = qk("q1");
        residents.admit(q1.clone(), own(&cp, &q1, &a, ts(0)));
        // Admitting q2 past the cap=1 evicts q1, handing back its (still-valid) session.
        let q2 = qk("q2");
        let (evicted_q, evicted_session) = residents
            .admit(q2.clone(), own(&cp, &q2, &a, ts(0)))
            .expect("q1 evicted at cap");
        assert_eq!(evicted_q, q1);

        // Before release, q1's lease is still A's — B cannot take it.
        cp.register_owner(&b, ts(0)).unwrap();
        assert!(matches!(
            cp.acquire_queue_lease(&q1, &b, ts(0)).unwrap(),
            crate::control_plane::AcquireOutcome::Rejected(_)
        ));
        // The caller releases the evicted lease (the contract for an evicted handle).
        cp.release_queue_lease(
            &q1,
            &evicted_session.owner,
            evicted_session.lease_epoch,
            ts(0),
        )
        .unwrap();
        // Now the released queue is acquirable by another owner — the resource was genuinely freed.
        assert!(matches!(
            cp.acquire_queue_lease(&q1, &b, ts(0)).unwrap(),
            crate::control_plane::AcquireOutcome::Acquired(_)
        ));
        assert_eq!(residents.len(), 1, "the hot set stays at the cap");
    }
}
