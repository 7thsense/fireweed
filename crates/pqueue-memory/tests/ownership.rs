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
    OwnershipOutcome, ProjectionWriter, QueueCommand, QueueControlPlane, acquire_and_fence,
};
use pqueue_memory::MemoryBackend;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}
fn owner(s: &str) -> OwnerId {
    OwnerId::new(s).unwrap()
}

/// Append `PauseQueue` under `expected_epoch` through the atomic write UoW; returns the fence outcome.
async fn append_at(b: &MemoryBackend, epoch: u64) -> EngineResult<()> {
    let env = envelope(QueueCommand::PauseQueue, vec![]);
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
    let b = MemoryBackend::new();
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
    let b = MemoryBackend::new();
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

/// A re-acquire by the SAME owner (the cold-restart-re-resolves-to-itself case) allocates a strictly-greater
/// fence epoch that fences its OWN pre-restart stragglers at the append seam. This is NOT a full TD-003
/// §Recovery test — there is no crash, snapshot load, or log-tail replay here (those are the
/// relational-reconnect + log-replay suites, run WITHOUT an ownership handoff); the recovery-with-replay-
/// under-handoff scenario is deferred (pqueue-c33c367e). It asserts only the re-acquire→fence-stragglers
/// property on a still-live in-process backend.
#[tokio::test]
async fn a_re_acquire_fences_the_owners_own_stragglers_at_the_seam() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    let cp = InMemoryControlPlane::default();
    let a = owner("a");
    cp.register_owner(&a, ts(0)).unwrap();

    // First owner does some work (a pause), then its lease expires.
    let OwnershipOutcome::Owned(s1) = acquire_and_fence(&cp, &b, &shard(), &a, ts(0))
        .await
        .unwrap()
    else {
        panic!("acquire");
    };
    append_at(&b, s1.fence_epoch).await.unwrap();

    // The SAME node re-resolves to itself and re-acquires (recovery): a strictly-greater fence epoch that
    // fences its own pre-recovery stragglers, on the same durable queue (no rewind).
    cp.register_owner(&a, ts(100_000)).unwrap();
    let OwnershipOutcome::Owned(s2) = acquire_and_fence(&cp, &b, &shard(), &a, ts(100_000))
        .await
        .unwrap()
    else {
        panic!("re-acquire");
    };
    assert!(
        s2.fence_epoch > s1.fence_epoch,
        "recovery fences the prior epoch"
    );
    // A straggler write at the pre-recovery epoch is fenced; the recovered owner resumes at the new epoch.
    assert_eq!(
        append_at(&b, s1.fence_epoch).await,
        Err(EngineError::EpochFenced)
    );
    append_at(&b, s2.fence_epoch).await.unwrap();
}
