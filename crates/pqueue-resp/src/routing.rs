//! Per-command routing decision (TD-006 §1A: owner-of-record redirect, serve-only-under-lease,
//! authz-before-redirect). This is the engine-pure DECISION a server owner-runtime applies to every
//! queue-addressed command; the live wiring (per-connection, holding this node's `OwnedSession`, calling
//! `resolve_queue_owner`, and emitting the RESP reply) is the server-runtime follow-up (pqueue-7bac12ce
//! threaded the data-plane fence epoch; the full routing runtime is a separate follow-up).
//!
//! The TD-006 ordering this enforces, exactly:
//! 1. **AUTHZ FIRST** — an unauthorized principal gets `-NOPERM` and NEVER a `-MOVED`/`Serve`/`Unavailable`,
//!    so a redirect cannot REVEAL a queue's existence or placement across a tenant boundary. Note: `route`
//!    takes the `resolution` as a parameter, so the caller has already CONSULTED ownership
//!    (`resolve_queue_owner`) — `route` only guarantees nothing is RETURNED to an unauthorized principal. To
//!    also avoid an unauthorized principal driving a control-plane resolve (a timing/load side-channel), the
//!    server wiring MUST call `authorize_tenant` BEFORE `resolve_queue_owner`.
//! 2. **Serve only under a live current-epoch lease** — a node serves only while it is the recorded
//!    `active_owner` with a live lease (state `assigned`, or `draining` for in-flight commands).
//! 3. **`-MOVED` on miss** — a node that does not own the queue redirects to the recorded `active_owner`
//!    (one source of truth → one-hop convergence, no two-node loop). The redirect slot is computed from the
//!    LITERAL key the client sent ([`crate::hash_slot`]), so the client (which keyed its routing table by
//!    that same key) updates the correct slot and does not loop. (NOT [`crate::queue_slot`], which hashes
//!    the canonical `{tenant/queue}` form — a different slot under the current `tenant:queue` wire key.)
//! 4. **Drain split** — a `draining` owner serves in-flight commands but refuses a NEW claim with a
//!    retryable `unavailable`; an unassigned queue (no live owner) is likewise `unavailable`.
//!
//! Bounded-stale reads (a deposed-but-not-yet-renewed owner serving a read from its frozen projection) are a
//! server-runtime CACHING behavior — this decision, given a FRESH `resolve_queue_owner`, is authoritative
//! (a non-owner redirects). "Misrouted write fenced" is the BQ-20 epoch fence: even if stale routing slips a
//! write to a deposed owner, `LogWriter::append` rejects the non-current epoch. Routing + fence together are
//! AC-ROUTE-1.

use pqueue_core::OwnerId;
use pqueue_engine::{AuthContext, LeaseState, OwnerResolution, QueueKey};

use crate::hash_slot;

/// The routing outcome for one queue-addressed command (TD-006 §1A). The server maps these to the wire:
/// `Serve` → run the command; `Moved` → `-MOVED <slot> <host:port>`; `NoPerm` → `-NOPERM`; `Unavailable` →
/// `-ERR pqueue unavailable` (retryable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// This node owns the queue at the current epoch — serve locally.
    Serve,
    /// Redirect a stock cluster client to the recorded active owner.
    Moved { slot: u16, endpoint: String },
    /// Authorization denied — emitted BEFORE any placement is consulted, so it never reveals ownership.
    NoPerm,
    /// No servable owner right now (queue unassigned, a draining owner refusing a new claim, or the active
    /// owner's endpoint is unknown) — a retryable `unavailable`.
    Unavailable,
}

/// Decide how to route a queue-addressed command (TD-006 §1A). `this_owner` is this node's control-plane
/// owner id; `routing_key` is the LITERAL key bytes the client sent (the `-MOVED` slot source);
/// `resolution` is a FRESH [`OwnerResolution`] from `resolve_queue_owner`; `owner_endpoint` maps an owner id
/// to its `host:port` (None when unknown); `is_new_claim` is true for a new delivery (`XREADGROUP >` /
/// cross-consumer `XCLAIM`) for the drain split.
pub fn route(
    this_owner: &OwnerId,
    shard: &QueueKey,
    routing_key: &[u8],
    auth: &AuthContext,
    resolution: &OwnerResolution,
    owner_endpoint: impl Fn(&OwnerId) -> Option<String>,
    is_new_claim: bool,
) -> RouteDecision {
    // 1. AUTHZ FIRST — return NoPerm before REVEALING any placement (the caller already resolved ownership
    // to pass `resolution`; the no-side-channel ordering of authz before that resolve is the server's, see
    // the module doc).
    if auth.authorize_tenant(shard.tenant_id.as_str()).is_err() {
        return RouteDecision::NoPerm;
    }
    // 2/3/4. PRECONDITION: `resolution` must be FRESH. `route` keys off `active_owner == this_owner` + state,
    // NOT `assignment_epoch`; it is sound only because `OwnerResolution` downgrades an EXPIRED lease to
    // unassigned/None, so in a fresh resolution `active_owner == this_owner` ⟹ live current-epoch owner. A
    // STALE cached resolution would let a deposed node wrongly Serve — the server MUST pass
    // a fresh `resolve_queue_owner` per write command (writes are also backstopped by the BQ-20 epoch fence;
    // reads are bounded-stale by design, TD-006 §Staleness).
    match (resolution.state, resolution.active_owner.as_ref()) {
        // Epoch allocated but storage fence not durably confirmed: nobody may serve or redirect to it.
        (LeaseState::PendingFence, _) => RouteDecision::Unavailable,
        // This node is the live assigned owner → serve.
        (LeaseState::Assigned, Some(owner)) if owner == this_owner => RouteDecision::Serve,
        // This node is the draining owner → serve in-flight, refuse a NEW claim (drain split).
        (LeaseState::Draining, Some(owner)) if owner == this_owner => {
            if is_new_claim {
                RouteDecision::Unavailable
            } else {
                RouteDecision::Serve
            }
        }
        // A DIFFERENT live owner holds the queue → redirect to it (slot from the literal key, no loop).
        (_, Some(owner)) => match owner_endpoint(owner) {
            Some(endpoint) => RouteDecision::Moved {
                slot: hash_slot(routing_key),
                endpoint,
            },
            None => RouteDecision::Unavailable, // owner of record, but no endpoint to redirect to
        },
        // No live owner (unassigned) → nothing to serve or redirect to.
        (_, None) => RouteDecision::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqueue_core::{QueueId, TenantId, UtcTimestamp};

    fn shard(t: &str, q: &str) -> QueueKey {
        QueueKey::new(TenantId::new(t).unwrap(), QueueId::new(q).unwrap())
    }
    fn owner(s: &str) -> OwnerId {
        OwnerId::new(s).unwrap()
    }
    fn auth_for(tenant: &str) -> AuthContext {
        AuthContext::new("p1", [tenant])
    }
    /// An `OwnerResolution` with the given state + active owner (epoch/expiry irrelevant to routing).
    fn resolution(state: LeaseState, active: Option<&str>) -> OwnerResolution {
        OwnerResolution {
            target_owner: None,
            active_owner: active.map(owner),
            assignment_epoch: active.map(|_| 1),
            lease_expires_at: active.map(|_| UtcTimestamp::new(100, 0).unwrap()),
            state,
        }
    }
    /// A fixed endpoint resolver: every owner is at `10.0.0.<n>:7000` derived from its id's last char.
    fn endpoints(o: &OwnerId) -> Option<String> {
        Some(format!("10.0.0.{}:7000", o.as_str()))
    }

    // ----- AUTHZ FIRST (no placement leak) -----

    #[test]
    fn unauthorized_principal_gets_noperm_and_never_a_redirect() {
        // The queue is owned by ANOTHER node — a misrouted command would normally -MOVED. But an
        // unauthorized principal must get -NOPERM with NO placement revealed (authz is checked first).
        let res = resolution(LeaseState::Assigned, Some("nodeB"));
        let d = route(
            &owner("nodeA"),
            &shard("t1", "q1"),
            b"t1:q1",
            &auth_for("OTHER_TENANT"), // not authorized for t1
            &res,
            endpoints,
            false,
        );
        assert_eq!(
            d,
            RouteDecision::NoPerm,
            "authz denial must not reveal placement"
        );
    }

    // ----- serve only under a live current-epoch lease -----

    #[test]
    fn the_live_assigned_owner_serves() {
        let res = resolution(LeaseState::Assigned, Some("nodeA"));
        assert_eq!(
            route(
                &owner("nodeA"),
                &shard("t1", "q1"),
                b"t1:q1",
                &auth_for("t1"),
                &res,
                endpoints,
                false
            ),
            RouteDecision::Serve
        );
    }

    #[test]
    fn pending_fence_owner_never_serves_or_redirects_to_itself() {
        let res = resolution(LeaseState::PendingFence, Some("nodeA"));
        assert_eq!(
            route(
                &owner("nodeA"),
                &shard("t1", "q1"),
                b"t1:q1",
                &auth_for("t1"),
                &res,
                endpoints,
                false
            ),
            RouteDecision::Unavailable
        );
    }

    #[test]
    fn a_non_owner_redirects_to_the_recorded_active_owner_with_the_literal_key_slot() {
        let res = resolution(LeaseState::Assigned, Some("nodeB"));
        let d = route(
            &owner("nodeA"),
            &shard("t1", "q1"),
            b"t1:q1",
            &auth_for("t1"),
            &res,
            endpoints,
            false,
        );
        // One-hop convergence: -MOVED names the active owner's endpoint, slot = hash_slot of the LITERAL key
        // the client sent (so the client updates the right slot and does not loop).
        assert_eq!(
            d,
            RouteDecision::Moved {
                slot: hash_slot(b"t1:q1"),
                endpoint: "10.0.0.nodeB:7000".to_string(),
            }
        );
        // The slot is NOT the canonical queue_slot (which hashes {t1/q1}) — proving we matched the client.
        if let RouteDecision::Moved { slot, .. } = d {
            assert_eq!(slot, hash_slot(b"t1:q1"));
            assert_ne!(slot, crate::queue_slot(&shard("t1", "q1")));
        }
    }

    #[test]
    fn a_redirect_to_an_owner_with_no_known_endpoint_is_unavailable() {
        let res = resolution(LeaseState::Assigned, Some("nodeB"));
        let d = route(
            &owner("nodeA"),
            &shard("t1", "q1"),
            b"t1:q1",
            &auth_for("t1"),
            &res,
            |_| None, // endpoint unknown
            false,
        );
        assert_eq!(d, RouteDecision::Unavailable);
    }

    // ----- drain split -----

    #[test]
    fn the_draining_owner_serves_in_flight_but_refuses_a_new_claim() {
        let res = resolution(LeaseState::Draining, Some("nodeA"));
        // In-flight (XACK / same-consumer XCLAIM / renew) continues on the draining owner.
        assert_eq!(
            route(
                &owner("nodeA"),
                &shard("t1", "q1"),
                b"t1:q1",
                &auth_for("t1"),
                &res,
                endpoints,
                false
            ),
            RouteDecision::Serve
        );
        // A NEW claim (XREADGROUP > / cross-consumer XCLAIM) is refused — retryable until handoff completes.
        assert_eq!(
            route(
                &owner("nodeA"),
                &shard("t1", "q1"),
                b"t1:q1",
                &auth_for("t1"),
                &res,
                endpoints,
                true
            ),
            RouteDecision::Unavailable
        );
    }

    #[test]
    fn an_unassigned_queue_is_unavailable() {
        let res = resolution(LeaseState::Unassigned, None);
        assert_eq!(
            route(
                &owner("nodeA"),
                &shard("t1", "q1"),
                b"t1:q1",
                &auth_for("t1"),
                &res,
                endpoints,
                false
            ),
            RouteDecision::Unavailable
        );
    }

    #[test]
    fn authz_is_checked_before_serve_too() {
        // Even when THIS node would serve, an unauthorized principal is denied first (no information about
        // whether the node owns the queue leaks).
        let res = resolution(LeaseState::Assigned, Some("nodeA"));
        assert_eq!(
            route(
                &owner("nodeA"),
                &shard("t1", "q1"),
                b"t1:q1",
                &auth_for("nope"),
                &res,
                endpoints,
                false
            ),
            RouteDecision::NoPerm
        );
    }
}
