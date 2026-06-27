//! Conformance for the memory backend.
//!
//! The full **port-level** behavioral no-stub suite is shared (`pqueue-conformance`) and run against
//! `MemoryBackend` by the `conformance_suite!` invocation below. The projection-internals white-box
//! tests (item_version monotonicity, high-water survives compaction) now live in `pqueue-projection`,
//! where that state lives. What remains here is the test-only `ManualClock`/`SeqIdGen` helpers, which
//! are memory-specific and not expressible through the ports.

use super::*;
use pqueue_conformance::ts;

// The full shared backend-conformance suite (16 port-level scenarios) against MemoryBackend.
pqueue_conformance::conformance_suite!(MemoryBackend::new);

#[tokio::test]
async fn manual_clock_and_idgen_are_real() {
    let clock = ManualClock::at(10);
    assert_eq!(clock.now(), ts(10));
    clock.set(20);
    assert_eq!(clock.now(), ts(20));

    let ids = SeqIdGen::default();
    let a = ids.next_item_id();
    let b = ids.next_item_id();
    assert_ne!(
        a.as_str(),
        b.as_str(),
        "ids must be unique, not a no-op constant"
    );
}

/// B1a (ADR-009 / TD-003 In-Process Library Owner-Runtime): a claim stamped with the owner's *cached*
/// acquire-time epoch is fenced at commit once a newer epoch is acquired (the owner was superseded), and
/// leases nothing; the current-epoch owner claims normally; `None` (sole-owner) is unaffected.
#[tokio::test]
async fn claim_fences_superseded_owner_epoch() {
    use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
    use pqueue_engine::{
        ClaimPort, ClaimRequest, ControlPlaneStore, EngineError, ProjectionRead, PushCommand,
        QueueCommand,
    };

    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    // Push one item at the current (genesis) epoch via the shared commit helper (degenerate owner).
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("a", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Ownership handoff: acquire a strictly-greater epoch (0 -> 1), durably superseding the epoch-0 owner.
    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(e1 >= 1, "acquire advances the durable epoch");

    // A claim carrying the STALE cached epoch (0) is fenced at commit and leases nothing.
    let stale = ClaimRequest {
        expected_epoch: Some(0),
        ..claim_req(10, 500, 100)
    };
    assert!(
        matches!(b.claim(stale).await, Err(EngineError::EpochFenced)),
        "a superseded owner's claim must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().leased,
        0,
        "a fenced claim must lease nothing (atomic reject before apply)"
    );

    // The current-epoch owner claims normally.
    let ok = ClaimRequest {
        expected_epoch: Some(e1),
        ..claim_req(10, 500, 100)
    };
    let claimed = b.claim(ok).await.unwrap();
    assert_eq!(claimed.items.len(), 1, "current-epoch owner claims the item");
}
