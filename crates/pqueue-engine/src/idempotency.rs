//! Durable request-id idempotency cache (TD-007 section 4; Phase 2 section 4a, first durable unit).
//!
//! A mutating request carries a `request_id` and a body fingerprint (`BodyHash`). The cache records
//! `request_id -> {fingerprint, outcome, expires_at}` so a retried request *replays* the prior
//! outcome, a different body under the same `request_id` is a conflict (API-001
//! `request-id-conflict`), and a retry after the retention window is `request-expired`. Entries
//! expire after the queue's `request_id_retention_ms` and are compacted; the cache is reconstructable
//! by replaying the retained recorded entries (TD-007 section 4 replay row).
//!
//! Scope invariant: the idempotency key is `(tenant, queue, request_id)`; the `(tenant, queue)`
//! components are external to this map, which is keyed by bare `request_id`. There MUST be one
//! instance per `(tenant, queue, shard)`. Sharing one instance across queues/tenants is a correctness
//! bug (cross-replay / false-conflict on a colliding client-supplied `request_id`).
//!
//! Caller mapping (decision -> behavior; structured, never stringly-typed; review B2):
//! `Conflict -> EngineError::RequestIdConflict`; `Expired ->` for push/gates treat as `Proceed`
//! (a genuinely new logical request), for claim return `EngineError::RequestExpired` (a claim replay
//! whose leases are gone; API-001). `pqueue_core::check_idempotency` is the single-record decision
//! primitive; this type is its durable, retention-bounded, multi-record home.
//!
//! NOT YET WIRED: built ahead of integration. Wiring into push/claim/finalize and `compact()` from
//! the ReclaimDriver lands with those units (Phase 2 / TD-007 section 3). See build-progress.md.

use std::collections::HashMap;

use pqueue_core::{BodyHash, RequestId, UtcTimestamp};

/// The outcome of consulting the cache for `(request_id, fingerprint)` at time `now`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyDecision<O> {
    /// No record for this `request_id`. Proceed, then `record(...)` the outcome.
    Proceed,
    /// A live record with the same fingerprint. Replay this cached outcome (no re-execution).
    Replay(O),
    /// A live record with a different fingerprint. `request-id-conflict`.
    Conflict,
    /// A record existed but its retention window has elapsed. The caller decides per operation
    /// (push: treat as new/Proceed; claim: `request-expired`).
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry<O> {
    fingerprint: BodyHash,
    outcome: O,
    expires_at: UtcTimestamp,
}

/// A retention-bounded idempotency cache for ONE `(tenant, queue, shard)` (see module scope
/// invariant), parameterized by the cached outcome type `O`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueIdempotencyCache<O> {
    entries: HashMap<RequestId, Entry<O>>,
}

impl<O> Default for QueueIdempotencyCache<O> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<O: Clone> QueueIdempotencyCache<O> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide what to do with `request_id` carrying `fingerprint` at time `now`.
    pub fn check(
        &self,
        request_id: &RequestId,
        fingerprint: BodyHash,
        now: UtcTimestamp,
    ) -> IdempotencyDecision<O> {
        match self.entries.get(request_id) {
            None => IdempotencyDecision::Proceed,
            Some(e) if e.expires_at <= now => IdempotencyDecision::Expired,
            Some(e) if e.fingerprint == fingerprint => {
                IdempotencyDecision::Replay(e.outcome.clone())
            }
            Some(_) => IdempotencyDecision::Conflict,
        }
    }

    /// Record the outcome of a freshly-executed request. Only call after `check` returned `Proceed`
    /// or `Expired` (never overwrite a live `Replay`/`Conflict` record; that would corrupt the
    /// idempotency guarantee).
    pub fn record(
        &mut self,
        request_id: RequestId,
        fingerprint: BodyHash,
        outcome: O,
        expires_at: UtcTimestamp,
    ) {
        self.entries.insert(
            request_id,
            Entry {
                fingerprint,
                outcome,
                expires_at,
            },
        );
    }

    /// Read the retained outcome for `request_id` IGNORING the body fingerprint (a recovery/explain read has
    /// only the id, not the original body). Returns the retained outcome while the record exists; the caller
    /// treats `None` (never recorded, or compacted away) as "no retained record". Unlike [`check`], this does
    /// not classify expiry — recovery surfaces the record for as long as it is retained.
    pub fn peek(&self, request_id: &RequestId) -> Option<O> {
        self.entries.get(request_id).map(|e| e.outcome.clone())
    }

    /// Drop entries whose retention has elapsed at `now` (bounded growth, TD-007 section 4).
    /// Called from the apply path / ReclaimDriver.
    pub fn compact(&mut self, now: UtcTimestamp) {
        self.entries.retain(|_, e| e.expires_at > now);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(s: &str) -> RequestId {
        RequestId::new(s).unwrap()
    }
    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }

    #[test]
    fn new_request_proceeds() {
        let c: QueueIdempotencyCache<&str> = QueueIdempotencyCache::new();
        assert_eq!(
            c.check(&rid("r1"), BodyHash(1), ts(0)),
            IdempotencyDecision::Proceed
        );
    }

    #[test]
    fn same_request_same_body_replays_cached_outcome() {
        let mut c = QueueIdempotencyCache::new();
        c.record(rid("r1"), BodyHash(7), "OK", ts(100));
        assert_eq!(
            c.check(&rid("r1"), BodyHash(7), ts(50)),
            IdempotencyDecision::Replay("OK")
        );
    }

    #[test]
    fn same_request_different_body_conflicts() {
        let mut c = QueueIdempotencyCache::new();
        c.record(rid("r1"), BodyHash(7), "OK", ts(100));
        assert_eq!(
            c.check(&rid("r1"), BodyHash(8), ts(50)),
            IdempotencyDecision::<&str>::Conflict
        );
    }

    #[test]
    fn expired_entry_reports_expired_then_compacts() {
        let mut c = QueueIdempotencyCache::new();
        c.record(rid("r1"), BodyHash(7), "OK", ts(100));
        // At or after expiry the record is `Expired` (caller maps per op), not silently Proceed.
        assert_eq!(
            c.check(&rid("r1"), BodyHash(7), ts(100)),
            IdempotencyDecision::Expired
        );
        assert_eq!(c.len(), 1);
        c.compact(ts(100));
        assert!(c.is_empty(), "expired entry compacted");
    }

    #[test]
    fn check_then_record_flow_never_overwrites_a_live_outcome() {
        // Drive the real check->record flow over a stream of requests; prove a live request_id is
        // never re-recorded with a different outcome (the idempotency guarantee).
        let stream: [(&str, u64, &str, i64); 4] = [
            ("r1", 1, "a", 0),  // new
            ("r2", 2, "b", 10), // new
            ("r1", 1, "a", 20), // retry, same body -> Replay (no record)
            ("r1", 2, "x", 30), // same id, different body -> Conflict (no record)
        ];
        let mut cache = QueueIdempotencyCache::new();
        let mut decisions = Vec::new();
        for (r, fp, outcome, now) in stream {
            let d = cache.check(&rid(r), BodyHash(fp), ts(now));
            if matches!(
                d,
                IdempotencyDecision::Proceed | IdempotencyDecision::Expired
            ) {
                cache.record(rid(r), BodyHash(fp), outcome, ts(now + 1000));
            }
            decisions.push(d);
        }
        assert_eq!(
            decisions,
            vec![
                IdempotencyDecision::Proceed,
                IdempotencyDecision::Proceed,
                IdempotencyDecision::Replay("a"),
                IdempotencyDecision::Conflict,
            ]
        );
        // Only the first outcome for r1 survives; it is never overwritten by "x".
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.check(&rid("r1"), BodyHash(1), ts(40)),
            IdempotencyDecision::Replay("a")
        );
    }

    #[test]
    fn replay_rebuilds_from_the_retained_window() {
        // Durability claim: after compaction, the cache equals a fresh replay of ONLY the entries
        // still inside the retention window (TD-007 section 4 "re-derives from the retained window").
        let log = [
            (rid("r1"), BodyHash(1), "a", ts(100)), // expires at 100
            (rid("r2"), BodyHash(2), "b", ts(300)), // expires at 300
        ];
        let mut live = QueueIdempotencyCache::new();
        for (r, h, o, e) in &log {
            live.record(r.clone(), *h, *o, *e);
        }
        // Time advances to 200: r1 expired, r2 retained.
        live.compact(ts(200));
        assert_eq!(live.len(), 1);

        // Rebuild from the log, replaying only entries whose retention has NOT elapsed at now=200.
        let mut rebuilt = QueueIdempotencyCache::new();
        for (r, h, o, e) in &log {
            if *e > ts(200) {
                rebuilt.record(r.clone(), *h, *o, *e);
            }
        }
        assert_eq!(
            live, rebuilt,
            "compacted cache == replay of the retained window"
        );
        assert_eq!(
            rebuilt.check(&rid("r2"), BodyHash(2), ts(250)),
            IdempotencyDecision::Replay("b")
        );
        assert_eq!(
            rebuilt.check(&rid("r1"), BodyHash(1), ts(250)),
            IdempotencyDecision::Proceed
        );
    }
}
