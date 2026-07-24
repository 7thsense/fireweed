//! Drain command-split classification (TD-006 §1A "Reassignment (drain) on the wire"). During a TD-003
//! `draining` handoff the queue still has ONE lease-holding owner (the draining owner); the `target_owner`
//! MUST NOT acquire until `unassigned`, so there is no `-ASK` (the target cannot serve yet). Instead the
//! command set SPLITS:
//!
//! - **In-flight commands STAY on the draining owner** so in-flight leases finalize and a worker is NEVER
//!   redirected mid-lease: `XACK` and a SAME-consumer `XCLAIM` (lease renew). Pushes/updates/renewals/
//!   finalizations MAY also continue (TD-003 §Graceful Drain step 2), so `XADD` (push) is served; `XDEL`
//!   (a mutation) is likewise not a NEW CLAIM and so is not refused by the drain split.
//! - **NEW claims are NOT started**: `XREADGROUP >`, a CROSS-consumer `XCLAIM` (a new delivery), and
//!   `XAUTOCLAIM` (see below) get a retryable `-ERR pqueue unavailable` until handoff completes (then the
//!   BQ-31 `MOVED`-on-miss redirects new claims to the new owner).
//!
//! This classifier feeds [`crate::route`]'s `is_new_claim`. Two pqueue-specific subtleties (in pqueue "the
//! consumer name IS the lease token", TD-006 §3):
//! - `XAUTOCLAIM` here reclaims only IDLE (lease-EXPIRED) entries and ALWAYS REASSIGNS them (a re-delivery,
//!   never a renew — see the `xautoclaim` handler) — so TD-006's "XAUTOCLAIM of the caller's own (live) PEL"
//!   in-flight case is unreachable in pqueue. It is therefore classified [`DrainClass::NewClaim`] (refused
//!   during drain); no worker holding a LIVE lease is affected (an expired lease is not "mid-lease").
//! - `XCLAIM` is RUNTIME-dependent AND can be MIXED: the `xclaim` handler splits one command into renews
//!   (same lease token → in-flight) and reassigns (different token → new delivery). A single `is_new_claim`
//!   bool cannot route a mixed batch correctly — the dispatch MUST apply the drain split PER ENTRY (serve the
//!   renews, refuse the reassigns with `unavailable`). [`DrainClass::RuntimeConsumerDependent`] +
//!   [`is_new_claim_on_drain`]'s default (`cross_consumer = false` → in-flight) is the SAFE coarse fallback
//!   that never refuses a renew; the faithful per-entry split is a separate wiring requirement.

/// The drain-split class of a queue-addressed command (TD-006 §1A).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainClass {
    /// A NEW delivery — a draining owner refuses it (`-ERR pqueue unavailable`). `XREADGROUP >`, and
    /// `XAUTOCLAIM` (which in pqueue always re-delivers idle/expired entries).
    NewClaim,
    /// In-flight or a push/mutation — a draining owner SERVES it (leases finalize; pushes/updates continue
    /// per TD-003 §Graceful Drain). `XACK`, `XADD`, an explicit-id `XREADGROUP` (own-PEL read), `XDEL`.
    InFlight,
    /// A pure read — bounded-stale, no drain restriction (served by the owner; redirected once deposed).
    Read,
    /// Not queue-addressed (`PING`, `CLUSTER`, `COMMAND`, handshake, `XGROUP`) — no drain semantics.
    Control,
    /// `XCLAIM` — RUNTIME-dependent AND possibly MIXED: per entry, a SAME-consumer claim is a renew
    /// (in-flight) and a CROSS-consumer claim is a reassign (new delivery). The dispatch MUST split the batch
    /// per entry (serve renews, refuse reassigns); the single-bool [`is_new_claim_on_drain`] is only the safe
    /// coarse fallback (default in-flight, never refuses a renew). See the module doc.
    RuntimeConsumerDependent,
}

/// Statically classify a command for the drain split. `name` is the UPPER-CASED command name; `args` is the
/// full argument vector (`args[0]` is the command name). Runtime-dependent commands return
/// [`DrainClass::RuntimeConsumerDependent`] — resolve with [`is_new_claim_on_drain`].
pub fn drain_class(name: &str, args: &[Vec<u8>]) -> DrainClass {
    match name {
        // A new delivery only when reading `>`; an explicit id reads the consumer's own PEL (in-flight).
        "XREADGROUP" => {
            if xreadgroup_requests_new_delivery(args) {
                DrainClass::NewClaim
            } else {
                DrainClass::InFlight
            }
        }
        // Acks finalize in-flight leases; pushes/updates MAY continue during drain (TD-003 §Graceful Drain
        // step 2). XADD is a push; XDEL is a mutation, not a NEW CLAIM, so the drain split does not refuse it.
        "XACK" | "XADD" | "XDEL" => DrainClass::InFlight,
        // XAUTOCLAIM always re-delivers idle/expired entries (a new delivery) in pqueue → refused on drain.
        "XAUTOCLAIM" => DrainClass::NewClaim,
        // XCLAIM is per-entry renew(in-flight)/reassign(new-delivery) — runtime + mixed; dispatch splits it.
        "XCLAIM" => DrainClass::RuntimeConsumerDependent,
        // Pure reads — bounded-stale.
        "XLEN" | "XPENDING" | "XINFO" | "PQ.MGET" | "PQ.HGETALL" | "PQ.HMGET" => DrainClass::Read,
        // PING, CLUSTER, COMMAND, CLIENT, HELLO, XGROUP, and anything unknown — not queue-addressed.
        _ => DrainClass::Control,
    }
}

/// Whether this command is a NEW claim under drain — the input to [`crate::route`]'s `is_new_claim`.
/// `cross_consumer` is the dispatch's per-entry verdict for the runtime-dependent commands (true only when
/// the `XCLAIM`/`XAUTOCLAIM` reassigns to a DIFFERENT lease token than the entry's current holder); it is
/// ignored for the statically-classified commands. Defaulting `cross_consumer = false` keeps a renew
/// in-flight, so a worker renewing its own lease is never refused.
pub fn is_new_claim_on_drain(class: DrainClass, cross_consumer: bool) -> bool {
    match class {
        DrainClass::NewClaim => true,
        DrainClass::RuntimeConsumerDependent => cross_consumer,
        DrainClass::InFlight | DrainClass::Read | DrainClass::Control => false,
    }
}

/// Does an `XREADGROUP ... STREAMS <key...> <id...>` request NEW deliveries (`>`)? The ids follow the
/// `STREAMS` keyword (N keys then N ids); a `>` in the id half means new delivery, any explicit id means an
/// own-PEL read. Robust to `COUNT`/`BLOCK`/`NOACK` options before `STREAMS`.
fn xreadgroup_requests_new_delivery(args: &[Vec<u8>]) -> bool {
    let Some(streams_at) = args.iter().position(|a| a.eq_ignore_ascii_case(b"STREAMS")) else {
        return false; // malformed — not a new delivery (the handler will reject it)
    };
    let rest = &args[streams_at + 1..];
    // `rest` is [keys..., ids...] with EQUAL halves (N keys then N ids). An odd/empty count is malformed
    // (the handler rejects it) — treat as not-a-new-delivery so we never inspect a key as an id.
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return false;
    }
    let ids = &rest[rest.len() / 2..];
    ids.iter().any(|id| id.as_slice() == b">")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    // ----- static classification -----

    #[test]
    fn xreadgroup_new_delivery_is_a_new_claim() {
        let a = argv(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "t1:q1", ">"]);
        assert_eq!(drain_class("XREADGROUP", &a), DrainClass::NewClaim);
        // With COUNT/BLOCK options before STREAMS.
        let a = argv(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "3",
            "BLOCK",
            "0",
            "STREAMS",
            "t1:q1",
            ">",
        ]);
        assert_eq!(drain_class("XREADGROUP", &a), DrainClass::NewClaim);
    }

    #[test]
    fn xreadgroup_explicit_id_reads_own_pel_in_flight() {
        // An explicit id (`0`) reads the consumer's OWN pending — in-flight, served during drain.
        let a = argv(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "t1:q1", "0"]);
        assert_eq!(drain_class("XREADGROUP", &a), DrainClass::InFlight);
    }

    #[test]
    fn acks_pushes_deletes_are_in_flight() {
        for name in ["XACK", "XADD", "XDEL"] {
            assert_eq!(
                drain_class(name, &argv(&[name, "t1:q1"])),
                DrainClass::InFlight
            );
        }
    }

    #[test]
    fn xclaim_is_runtime_consumer_dependent_xautoclaim_is_a_new_claim() {
        // XCLAIM is per-entry renew/reassign — runtime + mixed.
        assert_eq!(
            drain_class(
                "XCLAIM",
                &argv(&["XCLAIM", "t1:q1", "g", "tok", "0", "1-0"])
            ),
            DrainClass::RuntimeConsumerDependent
        );
        // XAUTOCLAIM in pqueue only re-delivers idle/expired entries → a new delivery, refused on drain.
        assert_eq!(
            drain_class(
                "XAUTOCLAIM",
                &argv(&["XAUTOCLAIM", "t1:q1", "g", "tok", "0", "0-0"])
            ),
            DrainClass::NewClaim
        );
    }

    #[test]
    fn reads_and_control_are_classified() {
        for name in [
            "XLEN",
            "XPENDING",
            "XINFO",
            "PQ.MGET",
            "PQ.HGETALL",
            "PQ.HMGET",
        ] {
            assert_eq!(drain_class(name, &argv(&[name, "t1:q1"])), DrainClass::Read);
        }
        for name in ["PING", "CLUSTER", "COMMAND", "HELLO", "XGROUP", "WHATEVER"] {
            assert_eq!(drain_class(name, &argv(&[name])), DrainClass::Control);
        }
    }

    // ----- is_new_claim_on_drain (the route() input) -----

    #[test]
    fn new_claim_resolution_honors_the_drain_split() {
        // A new-delivery XREADGROUP is a new claim regardless of the consumer verdict.
        assert!(is_new_claim_on_drain(DrainClass::NewClaim, false));
        // In-flight / read / control are NEVER new claims (served during drain).
        assert!(!is_new_claim_on_drain(DrainClass::InFlight, true));
        assert!(!is_new_claim_on_drain(DrainClass::Read, true));
        assert!(!is_new_claim_on_drain(DrainClass::Control, true));
    }

    #[test]
    fn a_same_consumer_xclaim_renew_is_never_refused_a_cross_consumer_reassign_is() {
        // SAME-consumer (renew): cross_consumer=false → in-flight, SERVED during drain (no worker redirected
        // mid-lease — the acceptance).
        assert!(!is_new_claim_on_drain(
            DrainClass::RuntimeConsumerDependent,
            false
        ));
        // CROSS-consumer (reassign to a different lease token): a NEW delivery → refused during drain.
        assert!(is_new_claim_on_drain(
            DrainClass::RuntimeConsumerDependent,
            true
        ));
    }
}
