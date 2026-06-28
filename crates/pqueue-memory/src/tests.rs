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
    assert_ne!(a, b, "ids must be unique, not a no-op constant");
}

/// ADR-009 collision fix: two instances with distinct `node_id`s minting into the same queue at the same
/// epoch+counter produce DISTINCT ids (the node byte disambiguates). The pre-fix per-connection counter
/// gave both writers identical ids — this is the regression guard.
#[tokio::test]
async fn distinct_node_ids_never_collide_on_concurrent_push() {
    use pqueue_conformance::{qdef, shard};
    use pqueue_engine::{PushPort, PushSpec};

    let a = MemoryBackend::new().with_node_id(1);
    let b = MemoryBackend::new().with_node_id(7);
    a.create_queue(qdef()).await.unwrap();
    b.create_queue(qdef()).await.unwrap();

    // Both writers push the FIRST item into the same queue at the genesis epoch (counter base 0 on each).
    let ida = a.push(&shard(), vec![PushSpec::default()], ts(0), None).await.unwrap()[0];
    let idb = b.push(&shard(), vec![PushSpec::default()], ts(0), None).await.unwrap()[0];

    assert_ne!(ida, idb, "same epoch+counter on two nodes must not collide");
    assert_eq!((ida.node(), ida.counter()), (1, 0));
    assert_eq!((idb.node(), idb.counter()), (7, 0));
    // The dedup client_item_key (defaulting to the id) is likewise distinct.
    assert_ne!(ida.to_string(), idb.to_string());
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
                items: vec![item("1", "ka", 5)],
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

/// B1b (ADR-009 / TD-003): the same cached-epoch fence applies to `PushPort::push` — a superseded owner's
/// push is `EpochFenced` and appends nothing; the current-epoch owner appends normally.
#[tokio::test]
async fn push_fences_superseded_owner_epoch() {
    use pqueue_conformance::{qdef, qkey, shard};
    use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushPort, PushSpec};

    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    let e1 = b.acquire_epoch(&shard()).await.unwrap(); // advance genesis 0 -> 1
    assert!(e1 >= 1);

    // Stale-epoch push is fenced and appends nothing.
    assert!(
        matches!(
            b.push(&shard(), vec![PushSpec::default()], ts(0), Some(0)).await,
            Err(EngineError::EpochFenced)
        ),
        "a superseded owner's push must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        0,
        "a fenced push must append nothing"
    );

    // Current-epoch push succeeds.
    let ids = b
        .push(&shard(), vec![PushSpec::default()], ts(1), Some(e1))
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
}

/// B1b (ADR-009 / TD-003): the cached-epoch fence also covers `FinalizePort::finalize` — completing the
/// TD-003 explicit Push/Claim/Finalize fence MUST. A superseded owner's finalize is `EpochFenced` and
/// makes no lifecycle transition; the current-epoch owner finalizes normally.
#[tokio::test]
async fn finalize_fences_superseded_owner_epoch() {
    use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
    use pqueue_engine::{
        ClaimPort, ClaimRequest, ControlPlaneStore, EngineError, FinalizeKind, FinalizeOutcome,
        FinalizePort, ProjectionRead, PushCommand, QueueCommand,
    };

    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    // Lease the item under the degenerate (sole-owner) path.
    let claimed = b
        .claim(ClaimRequest {
            expected_epoch: None,
            ..claim_req(10, 500, 10)
        })
        .await
        .unwrap();
    let id = claimed.items[0].item_id;

    let e1 = b.acquire_epoch(&shard()).await.unwrap(); // ownership handoff 0 -> 1
    let outcomes = vec![FinalizeOutcome {
        item_id: id,
        kind: FinalizeKind::Complete,
    }];

    assert!(
        matches!(
            b.finalize(&shard(), outcomes.clone(), ts(20), Some(0)).await,
            Err(EngineError::EpochFenced)
        ),
        "a superseded owner's finalize must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().complete,
        0,
        "a fenced finalize must make no transition"
    );

    b.finalize(&shard(), outcomes, ts(20), Some(e1)).await.unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().complete, 1);
}
