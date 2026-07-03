//! BQ-23 — the ownership binding primitive over a REAL backend (`MemoryBackend`) + the in-memory control
//! plane. These prove `acquire_and_fence` advances the durable storage fence on acquire, so a write made
//! through the raw `LogWriter::append` SEAM at a superseded epoch is rejected `EpochFenced`.
//!
//! SCOPE (do not overstate): this fences the raw append SEAM only. The REAL claim/push ports self-stamp the
//! current epoch and are NOT yet owner-fenced — threading `fence_epoch` through them (the work that closes
//! the BQ-20/21/22 deferral end-to-end) is the server-wiring follow-up (pqueue-c33c367e). The
//! lease-lifecycle + C4b seam invariants are unit-tested in `pqueue-engine` (control_plane + ownership).

use pqueue_conformance::{envelope, qdef, shard};
use pqueue_core::{OwnerId, UtcTimestamp};
use pqueue_engine::{
    Backend, ControlPlaneStore, EngineError, EngineResult, InMemoryControlPlane, LogWriter,
    OwnershipOutcome, PauseQueueCommand, ProjectionWriter, QueueCommand, QueueControlPlane,
    acquire_and_fence,
};
use pqueue_memory::{ComposedMemoryBackend, composed_memory_backend};

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}
fn owner(s: &str) -> OwnerId {
    OwnerId::new(s).unwrap()
}

/// Append `PauseQueue` under `expected_epoch` through the atomic write UoW; returns the fence outcome.
async fn append_at(b: &ComposedMemoryBackend, epoch: u64) -> EngineResult<()> {
    let env = envelope(QueueCommand::PauseQueue(PauseQueueCommand::default()), vec![]);
    b.write(
        move |lw: &mut dyn LogWriter, pw: &mut dyn ProjectionWriter| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env), epoch)?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        },
    )
    .await
}

/// `acquire_and_fence` advances the durable storage fence on acquire, so once a NEW owner takes over (after
/// the prior lease expired) the prior owner's cached `fence_epoch` is stale and a SEAM write at it is
/// rejected. (Seam-level only — the real claim path is not yet owner-fenced; see the module SCOPE.)
#[tokio::test]
async fn a_superseded_owner_is_fenced_at_the_append_seam() {
    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();
    let cp = InMemoryControlPlane::default(); // heartbeat 5s, lease 15s
    let (a, c) = (owner("a"), owner("c"));

    // Owner A acquires + durably fences; it writes at its fence epoch.
    cp.register_owner(&a, ts(0)).unwrap();
    let OwnershipOutcome::Owned(sa) = acquire_and_fence(&cp, &b, &shard(), &a, ts(0))
        .await
        .unwrap()
    else {
        panic!("A should win the unowned queue");
    };
    append_at(&b, sa.fence_epoch).await.unwrap();

    // A's lease lapses (15s); much later owner C acquires + fences at a STRICTLY-GREATER storage epoch.
    cp.register_owner(&c, ts(100_000)).unwrap();
    let OwnershipOutcome::Owned(sc) = acquire_and_fence(&cp, &b, &shard(), &c, ts(100_000))
        .await
        .unwrap()
    else {
        panic!("C should win (A's lease expired)");
    };
    assert!(
        sc.fence_epoch > sa.fence_epoch,
        "the durable storage fence epoch strictly advanced at handoff"
    );

    // The SUPERSEDED owner A's write at its now-stale fence epoch is rejected end-to-end.
    assert_eq!(
        append_at(&b, sa.fence_epoch).await,
        Err(EngineError::EpochFenced),
        "a superseded owner is fenced on the data-plane write path"
    );
    // The current owner C writes fine at its fence epoch.
    append_at(&b, sc.fence_epoch).await.unwrap();
}

/// A different owner's LIVE lease blocks `acquire_and_fence` (single active lease) — and a rejected acquire
/// MUST NOT advance the storage fence (no spurious epoch bump that would fence the live owner).
#[tokio::test]
async fn a_live_lease_rejects_a_second_acquire_without_touching_the_fence() {
    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();
    let cp = InMemoryControlPlane::default();
    let (a, c) = (owner("a"), owner("c"));
    cp.register_owner(&a, ts(0)).unwrap();
    cp.register_owner(&c, ts(0)).unwrap();

    acquire_and_fence(&cp, &b, &shard(), &a, ts(0))
        .await
        .unwrap();
    let fence_before = b.current_epoch(&shard()).await.unwrap();

    // C acquires while A's lease is live → Rejected (carrying A's record).
    let OwnershipOutcome::Rejected(held) = acquire_and_fence(&cp, &b, &shard(), &c, ts(1))
        .await
        .unwrap()
    else {
        panic!("C should be rejected while A holds a live lease");
    };
    assert_eq!(held.active_owner_id.as_ref(), Some(&a));
    assert_eq!(
        b.current_epoch(&shard()).await.unwrap(),
        fence_before,
        "a rejected acquire must NOT advance the storage fence"
    );
    // A's writes still work at its (unchanged) fence epoch.
    append_at(&b, fence_before).await.unwrap();
}

/// A re-acquire by the SAME owner (the sole owner whose lease lapsed under load and re-resolves to ITSELF)
/// PRESERVES the fence epoch — it does NOT advance the storage fence and does NOT self-fence the owner's own
/// in-flight writes (bead pqueue-79178303). The authority record still names this node `active_owner`, so no
/// DIFFERENT owner took over in the gap; the fence exists only to supersede a different owner, so there is
/// nothing legitimate to fence here. Bumping would fence the node's own epoch-N writes and collapse
/// throughput instead of degrading gracefully. (Contrast: a DIFFERENT owner taking over DOES bump and fence
/// — `a_superseded_owner_is_fenced_at_the_append_seam` above.)
///
/// NOTE: a real process restart on the in-memory control plane resets the record to genesis (epoch 0,
/// unowned), so a restarted node takes the COLD-START path (epoch advances 0→1) — it does not reach this
/// same-owner-preserve branch. This branch is the in-process lease-lapse case (the self-fencing-collapse bug).
#[tokio::test]
async fn a_same_owner_reaffirm_preserves_its_fence_and_does_not_self_fence() {
    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();
    let cp = InMemoryControlPlane::default();
    let a = owner("a");
    cp.register_owner(&a, ts(0)).unwrap();

    // First owner does some work (a pause), then its lease lapses.
    let OwnershipOutcome::Owned(s1) = acquire_and_fence(&cp, &b, &shard(), &a, ts(0))
        .await
        .unwrap()
    else {
        panic!("acquire");
    };
    append_at(&b, s1.fence_epoch).await.unwrap();

    // The SAME node re-resolves to itself and re-acquires its OWN (lapsed) lease: the fence epoch is
    // PRESERVED (no storage advance), on the same durable queue (no rewind).
    cp.register_owner(&a, ts(100_000)).unwrap();
    let OwnershipOutcome::Owned(s2) = acquire_and_fence(&cp, &b, &shard(), &a, ts(100_000))
        .await
        .unwrap()
    else {
        panic!("re-acquire");
    };
    assert_eq!(
        s2.fence_epoch, s1.fence_epoch,
        "same-owner re-affirm keeps the fence epoch (no self-advance)"
    );
    assert_eq!(
        b.current_epoch(&shard()).await.unwrap(),
        s1.fence_epoch,
        "the durable storage fence does not advance on a same-owner re-affirm"
    );
    // The in-flight write at the (still-current) fence epoch is NOT fenced — graceful degradation, no storm.
    append_at(&b, s1.fence_epoch).await.unwrap();
    append_at(&b, s2.fence_epoch).await.unwrap();
}
