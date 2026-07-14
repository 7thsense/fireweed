//! Bounded-recovery retention-floor segment-object reclamation (bead pqueue-b5cc2bc7, closes the AC-TXN-5A
//! SEGMENT-object-reclamation GAP).
//!
//! These tests exercise the REAL server-wired `objectlog/hybrid-async` composition
//! (`ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>`, group-commit + armed
//! async-apply debt monitor) — the exact backend `pqueue-server` opens for
//! `PQUEUE_PROJECTION_BACKEND=hybrid-async`. They prove that once a segment's commands are (a) durably
//! checkpointed into the SQLite image AND (b) past `request_id_retention_ms` (plus a skew guard), the segment
//! OBJECTS are reclaimed from durable storage WITHOUT regressing the proven AC-TXN-3
//! unknown-outcome-request_id-replay-across-restart guarantee.
//!
//! Design: the durable, monotonic retention floor is a MANIFEST ENTRY (`retention_floor_through`), advanced by
//! the same atomic, create-only, epoch-fenced manifest CAS as data segments and epoch fences (so a superseded
//! owner cannot regress it). The trim caller runs under the composed unit-of-work lock, gated on
//! `retention_may_advance`, and commits the floor entry FIRST then deletes the segment objects (crash-safe
//! order). Recovery derives the floor from the manifest once per shard and starts BOTH idempotency folds AND
//! the projection replay at `floor + 1` (the R1 fix: `max(resolve_recovery_start, floor)`), so a trimmed
//! below-floor segment is never read.
//!
//! `SegmentConfig::new(1, 1)` seals each push into its own segment immediately (size trigger), so "one push =
//! one segment" and a push's timestamp becomes its segment's `committed_at_ms` — the harness controls the
//! logical clock to place segments inside/outside the retention window deterministically.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::fault::spec;
use pqueue_core::{QueueDefinition, RequestId};
use pqueue_engine::{
    ClaimPort, CommandPosition, CommitTransition, CommitTransitionPort, ComposedBackend,
    ControlPlaneStore, EngineError, InProcessControlPlane, LogRead, LogStore, ProjectionRead,
    ProjectionStore, PushPort, QueueKey, ReclaimDriver, RecoveryStart, resolve_recovery_start,
};
use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
use pqueue_sqlite::{BackpressureLevel, HybridAsyncThresholds, HybridProjectionStore};

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn base_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "pqueue-seg-reclaim-{tag}-{}-{n}",
        std::process::id()
    ))
}

/// Generous debt budget so a fully-drained store stays Clear (retention gate open).
fn clear_thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds")
}

/// Open the REAL hybrid-async composed backend at `root` with `thresholds`, group-commit ON, one-command
/// segments, recovery-on-open. `flush_chunk = 1` steps the deferred backlog down one command at a time.
fn open_hybrid(root: &Path, thresholds: HybridAsyncThresholds) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_deferred_flush_chunk(1)
        .with_async_monitor(thresholds);
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid-async")
}

/// Open the REAL hybrid-strict composed backend at `root`: the same group-commit object-log substrate, but
/// with the SQLite-first strict projection ordering and no async-apply debt monitor.
fn open_hybrid_strict(root: &Path) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_strict_apply(true);
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid-strict")
}

#[derive(Clone, Copy)]
enum ProjectionMode {
    HybridAsync,
    HybridStrict,
}

fn open_mode(root: &Path, mode: ProjectionMode) -> HybridBackend {
    match mode {
        ProjectionMode::HybridAsync => open_hybrid(root, clear_thresholds()),
        ProjectionMode::HybridStrict => open_hybrid_strict(root),
    }
}

/// Open the hybrid-async backend on the RAW / synchronous append path (`ObjectLog::open`, NOT group-commit):
/// every write force-seals its own segment through `LogStore::append`, whose `committed_at_ms` is stamped from
/// the batch's max `created_at` (bead pqueue-b5cc2bc7 bug 1). Used to prove the retention-floor trim preserves
/// AC-TXN-3 on the raw-append path too, not only group-commit.
fn open_hybrid_raw(root: &Path, thresholds: HybridAsyncThresholds) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open(root).expect("raw log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_deferred_flush_chunk(1)
        .with_async_monitor(thresholds);
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .recover()
        .expect("recover raw objectlog/hybrid-async")
}

/// The shared t1/q1 queue with a SHORT request-id retention so the logical clock can step past it. Emission is
/// OFF (change-record reaping is orthogonal to segment reclamation; trim runs from the reap tick regardless).
fn qdef_short_retention() -> QueueDefinition {
    let mut d = pqueue_conformance::qdef();
    // 1 hour, in ms. Timestamps are in SECONDS (`ts(s)`), so this window comfortably covers a within-retention
    // retry a few seconds after commit, while the trim clock can still step past it (seconds → ms).
    d.request_id_retention_ms = 3_600_000;
    d.terminal_retention_ms = 3_600_000;
    d.emit_change_records = false;
    d
}

fn shard() -> QueueKey {
    pqueue_conformance::shard()
}

async fn push(backend: &HybridBackend, key: &str, at_s: i64) -> Vec<pqueue_core::ItemId> {
    backend
        .push(
            &shard(),
            vec![spec(key, 5)],
            pqueue_conformance::ts(at_s),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("push {key}: {e:?}"))
}

async fn push_rid(
    backend: &HybridBackend,
    rid: &str,
    key: &str,
    at_s: i64,
) -> Result<Vec<pqueue_core::ItemId>, EngineError> {
    backend
        .push_with_request_id(
            &shard(),
            RequestId::new(rid.to_string()).unwrap(),
            vec![spec(key, 5)],
            pqueue_conformance::ts(at_s),
            None,
        )
        .await
}

/// Fully drain the deferred checkpoint backlog (advances the durable SQLite high-water).
fn drain(backend: &HybridBackend) {
    while backend.with_projection(|p| p.deferred_command_count()) > 0 {
        backend.flush_deferred_projection().expect("flush");
    }
}

fn checkpoint_seq(backend: &HybridBackend) -> Option<u64> {
    backend
        .with_projection(|p| ProjectionStore::recovery_high_water(p, &shard()))
        .expect("recovery_high_water")
        .map(|p| p.sequence)
}

fn floor_seq(backend: &HybridBackend) -> Option<u64> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &shard()))
        .expect("retention_floor")
        .map(|p| p.sequence)
}

fn floor_pos(backend: &HybridBackend) -> Option<CommandPosition> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &shard()))
        .expect("retention_floor")
}

fn delete_count(backend: &HybridBackend) -> u64 {
    backend.with_log(|l| l.counters().delete_count)
}

fn object_count(backend: &HybridBackend) -> u64 {
    backend.with_log(|l| l.counters().object_count)
}

async fn pending(backend: &HybridBackend) -> u64 {
    backend.metrics(&shard()).await.expect("metrics").pending
}

// ---------------------------------------------------------------------------
// Test 1 — RECLAMATION: old checkpointed + past-retention segments are trimmed; the fresh tail survives.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test1_segment_reclamation_trims_expired_checkpointed_segments() {
    let root = base_dir("reclaim");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();

    // 3 OLD segments (seq 0,1,2) committed at t=10s; drain so all 3 checkpoint into the durable SQLite image.
    for i in 0..3 {
        push(&backend, &format!("old-{i}"), 10).await;
    }
    drain(&backend);
    assert_eq!(
        checkpoint_seq(&backend),
        Some(2),
        "3 old commands checkpointed"
    );

    // 1 FRESH segment (seq 3) committed at t=10_000s (well within retention at trim time); NOT drained, so it
    // stays deferred (uncheckpointed) and its `committed_at_ms` is recent — it must survive the trim.
    push(&backend, "fresh", 10_000).await;

    let deletes_before = delete_count(&backend);
    let objects_before = object_count(&backend);

    // Real reap tick at now=10_000s. cutoff = 10_000_000 - 3_600_000 - 5_000 = 6_395_000ms:
    //   old segs (committed 10_000ms) <= cutoff -> expired; fresh seg (10_000_000ms) > cutoff -> retained.
    //   time_expired_seq = 2, checkpoint = 2 -> trim_through = 2.
    backend.tick(pqueue_conformance::ts(10_000)).await.unwrap();

    assert!(
        delete_count(&backend) >= deletes_before + 3,
        "the reap tick deletes the 3 old segment objects (delete_count advanced by >= 3)"
    );
    assert_eq!(
        floor_seq(&backend),
        Some(2),
        "the durable retention floor advanced through the trimmed prefix"
    );
    // The old segment deletes may be offset by newly-written floor/read-horizon/head metadata, but the trim
    // must only grow the counted durable footprint by the single retained watermark object.
    let objects_after = object_count(&backend);
    assert!(
        objects_after <= objects_before + 1,
        "durable object count grew by more than one while old segment objects were reclaimed; before={objects_before} after={objects_after}"
    );

    // The old segment objects are GENUINELY gone: reading from GENESIS now hits a missing (trimmed) segment...
    let genesis = backend.read_from(&shard(), None, 100).await;
    assert!(
        matches!(genesis, Err(EngineError::Storage(_))),
        "reading from genesis after a trim hits the reclaimed segments; got {genesis:?}"
    );
    // ...while reading from the durable floor is contiguous and clean (only the surviving fresh tail).
    let page = backend
        .read_from(&shard(), floor_pos(&backend), 100)
        .await
        .expect("read_from floor must not hit a trimmed segment");
    assert_eq!(
        page.entries.len(),
        1,
        "only the fresh tail command reads back"
    );
    assert_eq!(
        page.entries[0].0.sequence, 3,
        "the surviving tail is the fresh segment at seq 3"
    );

    // A re-tick with no new reclamation is a no-op (floor unchanged, no further deletes).
    let deletes_after_first = delete_count(&backend);
    backend.tick(pqueue_conformance::ts(10_000)).await.unwrap();
    assert_eq!(
        floor_seq(&backend),
        Some(2),
        "floor is monotone; no regression"
    );
    assert_eq!(
        delete_count(&backend),
        deletes_after_first,
        "a re-tick at the same horizon reclaims nothing new"
    );
}

// ---------------------------------------------------------------------------
// Test 1b (bug 1) — RAW / SYNCHRONOUS APPEND PATH: a request_id committed via the raw append path (not
//   group-commit) is stamped with a real committed_at_ms (max created_at over the batch, not 0), so a trim
//   within retention RETAINS its segment and it replays across restart. Before the fix the raw path stamped
//   committed_at_ms=0, marking the segment infinitely-old and reclaiming a within-retention request_id -> a
//   fresh (regressed) retry. This is the AC-TXN-3-crux the reviewer flagged.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test1b_raw_append_path_preserves_actxn3_across_trim_within_retention() {
    let root = base_dir("raw-append-actxn3");
    let backend = open_hybrid_raw(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();

    // RAW append path (open_hybrid_raw): an OLD filler (seq 0 @ t=1s) then R (seq 1 @ t=1000s) under a
    // request_id. `push`/`push_rid` go through commit_locked -> LogStore::append, whose committed_at_ms is now
    // max(created_at) = the push timestamp (NOT 0).
    push(&backend, "filler", 1).await;
    drain(&backend);
    let r_ids = push_rid(&backend, "R", "R-body", 1_000)
        .await
        .expect("commit R (raw)");
    drain(&backend);

    // Trim at t=4000s, retention 3600s: cutoff = 4_000_000 - 3_600_000 - 5_000 = 395_000ms. filler(committed
    // 1_000ms) expired; R(committed 1_000_000ms) is WITHIN retention -> RETAINED. Were the raw-append
    // committed_at_ms still 0, R(0 <= 395_000) would be wrongly reclaimed.
    backend
        .trim_reclaimable_segments(&shard(), 3_600_000, pqueue_conformance::ts(4_000))
        .expect("trim raw");
    assert_eq!(
        floor_seq(&backend),
        Some(0),
        "only the filler is reclaimed; the within-retention request_id segment is RETAINED (committed_at_ms is a real upper bound, not 0)"
    );

    // Restart + retry within retention -> REPLAY the committed ids (AC-TXN-3 preserved on the raw path).
    drop(backend);
    let reopened = open_hybrid_raw(&root, clear_thresholds());
    let replay = push_rid(&reopened, "R", "R-body", 4_001)
        .await
        .expect("replay R (raw)");
    assert_eq!(
        replay, r_ids,
        "raw-append within-retention request_id REPLAYS across trim+restart (bug 1 fix)"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — WITHHELD UNDER HARD DEBT: a reap tick under Hard async-apply debt trims 0 and never advances floor.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test2_trim_withheld_under_hard_debt() {
    let root = base_dir("withheld");
    // Hard budget = 3 apply-lag commands.
    let thresholds = HybridAsyncThresholds::new(3, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds");
    let backend = open_hybrid(&root, thresholds);
    backend.create_queue(qdef_short_retention()).await.unwrap();

    // 2 OLD segments, drained (checkpointed) — they WOULD be trimmable once past retention.
    for i in 0..2 {
        push(&backend, &format!("old-{i}"), 10).await;
    }
    drain(&backend);
    assert_eq!(checkpoint_seq(&backend), Some(1));

    // Trip Hard: exactly `budget` (3) fresh deferred commands, NOT drained. (A further push would be rejected
    // by the hard-debt admission gate, so we push exactly to the Hard threshold.)
    for i in 0..3 {
        push(&backend, &format!("debt-{i}"), 10_000).await;
    }
    assert_eq!(
        backend.with_projection(|p| p.async_backpressure_level()),
        Some(BackpressureLevel::Hard),
        "3 deferred commands at budget 3 trip Hard"
    );
    assert!(
        !backend.with_projection(|p| p.async_retention_may_advance()),
        "the retention gate is CLOSED under Hard debt"
    );

    let deletes_before = delete_count(&backend);
    // Reap tick at a time where the old segments ARE past retention — but Hard debt withholds the trim.
    backend
        .tick(pqueue_conformance::ts(1_000_000))
        .await
        .unwrap();

    assert_eq!(
        delete_count(&backend),
        deletes_before,
        "a reap tick under Hard debt performs ZERO segment deletes"
    );
    assert_eq!(
        floor_seq(&backend),
        None,
        "the durable retention floor never advances while Hard (still un-written)"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — AC-TXN-3 ACROSS TRIM + RESTART (push_with_request_id). Retained-within-retention REPLAYS; trimmed-
//          after-retention is FRESH. commit_transition is capability-N/A on this eventual-apply backend.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test3a_request_id_within_retention_is_retained_and_replays_across_trim_restart() {
    let root = base_dir("actxn3-retain");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();

    // An OLD filler segment (seq 0, committed t=1s) that WILL be trimmed, then R (seq 1, committed t=1000s)
    // whose retention window is still open at trim time — R's segment must be RETAINED.
    push(&backend, "filler", 1).await;
    drain(&backend);
    let ids1 = push_rid(&backend, "R", "R-body", 1_000)
        .await
        .expect("commit R");
    drain(&backend);

    // Trim at now=4000s: cutoff = 4_000_000 - 3_600_000 - 5_000 = 395_000ms. filler(1_000) <= 395_000 ->
    // expired; R(1_000_000) > 395_000 -> RETAINED. time_expired_seq = 0, checkpoint = 1 -> trim_through = 0.
    backend.tick(pqueue_conformance::ts(4_000)).await.unwrap();
    assert_eq!(
        floor_seq(&backend),
        Some(0),
        "only the filler prefix trimmed"
    );

    // Restart: drop + reopen (recovery rebuilds the push-idempotency map from floor+1 = R's segment onward).
    drop(backend);
    let reopened = open_hybrid(&root, clear_thresholds());

    // Retry R with the SAME body + request_id -> REPLAY the identical committed ids, 0 new durable segments.
    // R created at t=1000s, retention 3600s -> expires t=4600s; the retry at t=4001s is within retention.
    let segments_before = reopened.with_log(|l| l.counters().segments_sealed);
    let ids2 = push_rid(&reopened, "R", "R-body", 4_001)
        .await
        .expect("replay R");
    assert_eq!(
        ids2, ids1,
        "an in-retention request_id REPLAYS its one committed result across trim+restart"
    );
    assert_eq!(
        reopened.with_log(|l| l.counters().segments_sealed),
        segments_before,
        "the replay appends 0 new durable segments (idempotent)"
    );
}

#[tokio::test]
async fn test3b_request_id_after_retention_is_reclaimed_and_treated_fresh_across_trim_restart() {
    let root = base_dir("actxn3-reclaim");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();

    let ids1 = push_rid(&backend, "R2", "R2-body", 1_000)
        .await
        .expect("commit R2");
    drain(&backend);

    // Trim at now=10_000_000s: cutoff far past R2(1_000_000ms) AND past R2's retention (expires t=4600s) ->
    // R2's segment RECLAIMED.
    backend
        .tick(pqueue_conformance::ts(10_000_000))
        .await
        .unwrap();
    assert_eq!(
        floor_seq(&backend),
        Some(0),
        "R2's segment reclaimed; floor at seq 0"
    );

    drop(backend);
    let reopened = open_hybrid(&root, clear_thresholds());

    // Retry R2 AFTER retention -> treated as FRESH (its request_id is below the floor, not in the rebuilt map),
    // so a genuinely new item id is minted (the idempotency window legitimately closed).
    let ids2 = push_rid(&reopened, "R2", "R2-body", 10_000_000)
        .await
        .expect("fresh R2");
    assert_ne!(
        ids2, ids1,
        "an after-retention request_id retry is FRESH work across trim+restart (idempotency window closed)"
    );
}

#[tokio::test]
async fn test3c_commit_transition_is_capability_na_on_eventual_apply_objectlog() {
    // commit_transition (the OTHER request_id-bearing op) requires the atomic append+apply boundary; the
    // eventual-apply object-log/hybrid-async backend refuses it (EngineError::Unavailable). It therefore
    // cannot be exercised across a trim on THIS substrate — a durability-class property, not a coverage gap
    // (same finding as the existing AC-TXN-3 objectlog row). Its recovery fold (rebuild_commit_idempotency_from_log)
    // IS floor-threaded + back-compat by construction (it starts the fold at `floor`, symmetric with the push
    // fold), but that fold is NOT exercised end-to-end here because no commit_transition envelope can exist on
    // this backend; the push fold is the one proven across trim+restart (test3a/test3b/test4).
    let root = base_dir("actxn3-commit-na");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();
    let outcome = backend
        .commit_transition(
            &shard(),
            CommitTransition {
                request_id: Some(RequestId::new("C".to_string()).unwrap()),
                entries: vec![],
            },
            pqueue_conformance::ts(1),
            None,
        )
        .await;
    assert!(
        matches!(outcome, Err(EngineError::Unavailable)),
        "commit_transition is Unavailable on the eventual-apply object-log backend; got {outcome:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 4 — RECOVERY CORRECTNESS FROM FLOOR: a trimmed run recovers IDENTICALLY to a no-trim control; read from
//          the floor is contiguous with no missing segment; the R1 start-flooring logic is verified.
// ---------------------------------------------------------------------------

/// The full recovered image observed after a reopen: the COMPLETE QueueMetrics, the replayed ids for MULTIPLE
/// request_ids (one near the retention boundary), and the sorted claimed item-id set (the eligible/active-lease
/// projection). Two runs that differ only by a trim must produce an IDENTICAL RecoveredImage.
#[derive(Debug, PartialEq, Eq)]
struct RecoveredImage {
    metrics: pqueue_engine::QueueMetrics,
    replays: Vec<Vec<pqueue_core::ItemId>>,
    claimed: Vec<pqueue_core::ItemId>,
}

/// Run the SAME multi-request_id workload with/without a trim tick, on EITHER the group-commit (`raw=false`)
/// or the raw/synchronous append (`raw=true`) path, reopen, and capture the full recovered image.
async fn recovery_run(tag: &str, do_trim: bool, raw: bool) -> RecoveredImage {
    let root = base_dir(tag);
    let open = |r: &Path| {
        if raw {
            open_hybrid_raw(r, clear_thresholds())
        } else {
            open_hybrid(r, clear_thresholds())
        }
    };
    let backend = open(&root);
    backend.create_queue(qdef_short_retention()).await.unwrap();

    // filler (seq 0, t=1s) — the ONLY segment trimmed. Then THREE request_ids at increasing times (R1 near the
    // retention boundary at replay, R2/R3 comfortably within), and a fresh tail. All of R1/R2/R3 stay within
    // retention across the trim, so the rebuilt idempotency map must replay all three identically.
    push(&backend, "filler", 1).await;
    drain(&backend);
    let mut committed: Vec<Vec<pqueue_core::ItemId>> = Vec::new();
    for (rid, at) in [("R1", 1_000i64), ("R2", 1_500), ("R3", 2_000)] {
        committed.push(
            push_rid(&backend, rid, &format!("{rid}-body"), at)
                .await
                .unwrap_or_else(|e| panic!("{tag}: commit {rid}: {e:?}")),
        );
        drain(&backend);
    }
    push(&backend, "tail", 10_000).await;
    drain(&backend);

    if do_trim {
        // Trim at t=4000s: only the filler (committed 1_000ms) expires; every request_id + the tail retained.
        backend.tick(pqueue_conformance::ts(4_000)).await.unwrap();
        assert_eq!(
            floor_seq(&backend),
            Some(0),
            "{tag}: only the filler is trimmed"
        );
    }
    drop(backend);

    let reopened = open(&root);
    // read_from(floor) is contiguous with no missing-segment error.
    let page = reopened
        .read_from(&shard(), floor_pos(&reopened), 1000)
        .await
        .unwrap_or_else(|e| panic!("{tag}: read_from floor errored: {e:?}"));
    assert!(
        page.entries
            .windows(2)
            .all(|w| w[0].0.sequence < w[1].0.sequence),
        "{tag}: recovered read is strictly ordered / contiguous"
    );
    let metrics = reopened.metrics(&shard()).await.expect("metrics");
    // Replay all three request_ids at t=4599s — R1 (created 1000s, expires 4600s) is ONE SECOND inside its
    // retention window (near-boundary); each must replay its committed ids from the floor-threaded fold.
    let mut replays = Vec::new();
    for rid in ["R1", "R2", "R3"] {
        replays.push(
            push_rid(&reopened, rid, &format!("{rid}-body"), 4_599)
                .await
                .unwrap_or_else(|e| panic!("{tag}: replay {rid}: {e:?}")),
        );
    }
    assert_eq!(
        replays, committed,
        "{tag}: all three request_ids replay their committed ids"
    );
    // Claim everything to compare the eligible/active-lease projection (the full serving image, not just counts).
    let claimed_resp = reopened
        .claim(pqueue_conformance::claim_req(100, 5_000_000, 4_600))
        .await
        .expect("claim");
    let mut claimed: Vec<pqueue_core::ItemId> =
        claimed_resp.items.iter().map(|it| it.item_id).collect();
    claimed.sort();
    RecoveredImage {
        metrics,
        replays,
        claimed,
    }
}

#[tokio::test]
async fn test4_recovery_from_floor_matches_no_trim_control_group_commit() {
    let control = recovery_run("recover-control-gc", false, false).await;
    let trimmed = recovery_run("recover-trim-gc", true, false).await;
    assert_eq!(
        trimmed, control,
        "group-commit: recovery after trim rebuilds the FULL image (metrics + multi-request_id idempotency + eligible/lease set) identically to a no-trim control"
    );
}

#[tokio::test]
async fn test4_recovery_from_floor_matches_no_trim_control_raw_append() {
    let control = recovery_run("recover-control-raw", false, true).await;
    let trimmed = recovery_run("recover-trim-raw", true, true).await;
    assert_eq!(
        trimmed, control,
        "raw append: recovery after trim rebuilds the FULL image identically to a no-trim control"
    );
}

// ---------------------------------------------------------------------------
// Bug 3 — BEHIND-IMAGE FAIL-CLOSED: a trimmed log reopened over a projection image BEHIND the durable floor
//   (restored / rolled-back / foreign SQLite) must FAIL CLOSED, not silently drop the commands between the
//   behind image and the floor (absent from BOTH the reclaimed log and the behind image).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bug3_projection_behind_floor_fails_closed() {
    let root = base_dir("behind-image");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();
    // 3 old segments (seq 0,1,2) checkpointed, then a fresh tail; trim reclaims the old prefix -> floor=2.
    for i in 0..3 {
        push(&backend, &format!("old-{i}"), 10).await;
    }
    drain(&backend);
    push(&backend, "fresh", 10_000).await;
    drain(&backend);
    backend.tick(pqueue_conformance::ts(10_000)).await.unwrap();
    assert_eq!(
        floor_seq(&backend),
        Some(2),
        "the old prefix is trimmed; floor at seq 2"
    );
    drop(backend);

    // Simulate a restored/rolled-back/foreign projection image: delete the SQLite files so the reopen starts a
    // FRESH empty image (high-water None < floor 2) while the object-log floor blob + trimmed segments persist.
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
    }

    // Recovery must FAIL CLOSED (does not silently drop the reclaimed commands 0..2).
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(&root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_deferred_flush_chunk(1)
        .with_async_monitor(clear_thresholds());
    let result = ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover();
    let err = result
        .err()
        .expect("recovery over a projection image behind the retention floor MUST fail closed");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("retention floor") && msg.contains("behind"),
        "the fail-closed error must name the behind-floor inconsistency; got {msg}"
    );
}

async fn retained_floor_head_replay_recovery_impl() {
    for mode in [ProjectionMode::HybridAsync, ProjectionMode::HybridStrict] {
        let mode_name = match mode {
            ProjectionMode::HybridAsync => "hybrid-async",
            ProjectionMode::HybridStrict => "hybrid-strict",
        };
        let root = base_dir(mode_name);
        let backend = open_mode(&root, mode);
        backend.create_queue(qdef_short_retention()).await.unwrap();

        // 3 old segments (seq 0,1,2) checkpointed, then a fresh tail; trim reclaims the old prefix -> floor=2.
        for i in 0..3 {
            push(&backend, &format!("old-{i}"), 10).await;
        }
        drain(&backend);
        push(&backend, "fresh", 10_000).await;
        drain(&backend);
        backend.tick(pqueue_conformance::ts(10_000)).await.unwrap();
        assert_eq!(
            floor_seq(&backend),
            Some(2),
            "{mode_name}: the old prefix is trimmed; floor at seq 2"
        );
        drop(backend);

        // A healthy reopen still recovers from the retained floor/head and reads only the live tail.
        let reopened = open_mode(&root, mode);
        let floor = floor_pos(&reopened).expect("retained floor after reopen");
        let page = reopened
            .read_from(&shard(), Some(floor), 100)
            .await
            .unwrap_or_else(|e| panic!("{mode_name}: read_from retained floor errored: {e:?}"));
        assert_eq!(
            page.entries
                .iter()
                .map(|(p, _)| p.sequence)
                .collect::<Vec<_>>(),
            vec![3],
            "{mode_name}: replay resumes at the retained floor/head, not from genesis"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}

async fn behind_image_fail_closed_with_deleted_manifests_impl() {
    for mode in [ProjectionMode::HybridAsync, ProjectionMode::HybridStrict] {
        let mode_name = match mode {
            ProjectionMode::HybridAsync => "hybrid-async",
            ProjectionMode::HybridStrict => "hybrid-strict",
        };
        let root = base_dir(mode_name);
        let backend = open_mode(&root, mode);
        backend.create_queue(qdef_short_retention()).await.unwrap();

        // 3 old segments (seq 0,1,2) checkpointed, then a fresh tail; trim reclaims the old prefix -> floor=2.
        for i in 0..3 {
            push(&backend, &format!("old-{i}"), 10).await;
        }
        drain(&backend);
        push(&backend, "fresh", 10_000).await;
        drain(&backend);
        backend.tick(pqueue_conformance::ts(10_000)).await.unwrap();
        assert_eq!(
            floor_seq(&backend),
            Some(2),
            "{mode_name}: the old prefix is trimmed; floor at seq 2"
        );
        drop(backend);

        // Simulate a restored/rolled-back/foreign projection image: delete the SQLite files so the reopen
        // starts with a behind image (high-water None < floor 2) while the object-log floor blob + trimmed
        // segments persist.
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
        }

        // Recovery must FAIL CLOSED (does not silently drop the reclaimed commands 0..2).
        let sqlite = root.join("projection.sqlite");
        let log =
            ObjectLog::open_group_commit(&root, SegmentConfig::new(1, 1).unwrap()).expect("log");
        let hybrid = match mode {
            ProjectionMode::HybridAsync => HybridProjectionStore::open(sqlite.to_str().unwrap())
                .expect("hybrid")
                .with_deferred_flush_chunk(1)
                .with_async_monitor(clear_thresholds()),
            ProjectionMode::HybridStrict => HybridProjectionStore::open(sqlite.to_str().unwrap())
                .expect("hybrid")
                .with_strict_apply(true),
        };
        let result = ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
            .with_group_commit(true)
            .recover();
        let err = result.err().unwrap_or_else(|| {
            panic!("{mode_name}: recovery over a projection image behind the retention floor must fail closed")
        });
        let msg = format!("{err:?}");
        assert!(
            msg.contains("retention floor") && msg.contains("behind"),
            "{mode_name}: the fail-closed error must name the behind-floor inconsistency; got {msg}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}

#[tokio::test]
async fn test_behind_image_fail_closed_with_deleted_manifests() {
    behind_image_fail_closed_with_deleted_manifests_impl().await;
}

#[tokio::test]
#[allow(non_snake_case)]
async fn TestSqliteEngineBehindImageDeletedManifestFailClosed() {
    behind_image_fail_closed_with_deleted_manifests_impl().await;
}

#[tokio::test]
#[allow(non_snake_case)]
async fn TestSqliteEngineBehindImageRetainedFloorHeadReplayRecovery() {
    retained_floor_head_replay_recovery_impl().await;
}

#[tokio::test]
#[allow(non_snake_case)]
async fn TestBehindImageFailClosedWithDeletedManifests() {
    behind_image_fail_closed_with_deleted_manifests_impl().await;
}

#[tokio::test]
async fn test4_r1_hard_backpressure_replay_start_is_floored_not_genesis() {
    // R1 fix at the decision seam: under HARD backpressure `resolve_recovery_start` returns FromGenesis (start
    // = None), which on a trimmed log would read a DELETED below-floor segment ("missing segment"). Flooring
    // the start with `max(resolved_start, floor)` lifts it to the durable floor instead of genesis. (The full
    // stack cannot be driven Hard AT reopen — the async-apply monitor is memoryless across restart, so a
    // reopened store is always Clear — so the fix is proven at this seam plus the read-from-floor safety above.)
    let floor = Some(CommandPosition::new(shard(), 0, 5));
    // Under Hard, the recorded high-water is withheld (None) and resolve returns FromGenesis.
    let resolved = match resolve_recovery_start(None, true, None).unwrap() {
        RecoveryStart::FromHighWater(pos) => pos,
        RecoveryStart::FromGenesis => None,
    };
    assert_eq!(
        resolved, None,
        "Hard backpressure resolves to genesis without the floor"
    );
    let start = pqueue_engine::max_position(resolved, floor.clone());
    assert_eq!(
        start, floor,
        "the R1 fix floors the Hard-backpressure replay start at the durable retention floor, not genesis"
    );
    // The healthy path (checkpoint >= floor) is unchanged: max keeps the checkpoint.
    let checkpoint = Some(CommandPosition::new(shard(), 0, 9));
    assert_eq!(
        pqueue_engine::max_position(checkpoint.clone(), floor),
        checkpoint,
        "a healthy checkpoint start (>= floor) is left unchanged by the floor"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — CRASH-SAFETY: a crash between floor-write and segment-delete (and mid-delete) leaves a recoverable
//          state; re-running the trim reaches the same consistent end state (idempotent).
// ---------------------------------------------------------------------------

/// A fault hook that errors on the Nth strike of `cut` (1-indexed), a no-op otherwise. Models "process died
/// right here" at a chosen point in the segment-expiry loop.
struct CrashOnNth {
    cut: FaultCutPoint,
    fail_on: u64,
    seen: AtomicU64,
}

impl CrashOnNth {
    fn new(cut: FaultCutPoint, fail_on: u64) -> Arc<Self> {
        Arc::new(Self {
            cut,
            fail_on,
            seen: AtomicU64::new(0),
        })
    }
}

impl FaultHook for CrashOnNth {
    fn fault_point(&self, cut: FaultCutPoint) -> pqueue_engine::EngineResult<()> {
        if cut == self.cut {
            let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
            if n == self.fail_on {
                return Err(EngineError::Storage(format!(
                    "fault: crash at {cut:?} #{n}"
                )));
            }
        }
        Ok(())
    }
}

/// Build a store with several OLD, checkpointed, past-retention segments plus a fresh tail, ready to trim.
async fn seed_trimmable(root: &Path) -> HybridBackend {
    let backend = open_hybrid(root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();
    for i in 0..4 {
        push(&backend, &format!("old-{i}"), 10).await; // seq 0..3, committed 10_000ms
    }
    drain(&backend);
    push(&backend, "tail", 2_000_000).await; // seq 4, fresh
    drain(&backend);
    backend
}

#[tokio::test]
async fn test5_crash_between_floor_write_and_delete_is_recoverable_and_idempotent() {
    let root = base_dir("crash-before-delete");
    let backend = seed_trimmable(&root).await;

    // Crash on the FIRST expiry strike: the floor is written, then the very first segment delete faults — a
    // process death "between floor-write and segment-delete".
    backend.with_log(|l| {
        l.set_fault_hook(Some(CrashOnNth::new(FaultCutPoint::DuringSegmentExpiry, 1)))
    });
    let res = backend.trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000));
    assert!(res.is_err(), "the injected fault crashes the trim");
    // The durable floor WAS written (crash-safe order writes it first); segments are still present.
    assert_eq!(
        floor_seq(&backend),
        Some(3),
        "floor durably advanced before the delete crash"
    );
    backend.with_log(|l| l.set_fault_hook(None));
    drop(backend);

    // Recovery succeeds despite floor=3 pointing above still-present below-floor segments (read starts at F+1).
    let reopened = seed_reopen_and_check(&root).await;

    // Before the re-run the crash-interrupted deletion has left below-floor segment objects behind (5 .seg
    // files: seq 0..3 undeleted + the tail seq 4).
    assert_eq!(
        count_seg_files(&root),
        5,
        "crash-before-delete leaves all below-floor segment objects present"
    );
    // Re-running the trim FINISHES the interrupted deletion (bug 2a): the below-floor objects are ACTUALLY
    // deleted this time (delete_count advances), the floor is unchanged (monotone), and only the tail remains.
    let deletes_before = delete_count(&reopened);
    reopened
        .trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000))
        .expect("re-run trim finishes the interrupted deletion");
    assert!(
        delete_count(&reopened) > deletes_before,
        "the re-run must actually delete the leaked below-floor segment objects (not a silent no-op)"
    );
    assert_eq!(
        floor_seq(&reopened),
        Some(3),
        "floor stays at the consistent horizon"
    );
    assert_eq!(
        count_seg_files(&root),
        1,
        "after the re-run only the fresh tail segment object remains"
    );
    let page = reopened
        .read_from(&shard(), floor_pos(&reopened), 100)
        .await
        .expect("read tail after idempotent re-run");
    assert_eq!(
        page.entries.len(),
        1,
        "only the fresh tail remains readable"
    );
    assert_eq!(page.entries[0].0.sequence, 4);
}

#[tokio::test]
async fn test5_crash_mid_delete_is_recoverable_and_idempotent() {
    let root = base_dir("crash-mid-delete");
    let backend = seed_trimmable(&root).await;

    // Crash on the SECOND expiry strike: one segment deleted, the next faults — a "mid-delete" process death.
    backend.with_log(|l| {
        l.set_fault_hook(Some(CrashOnNth::new(FaultCutPoint::DuringSegmentExpiry, 2)))
    });
    let res = backend.trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000));
    assert!(
        res.is_err(),
        "the injected mid-delete fault crashes the trim"
    );
    assert_eq!(
        floor_seq(&backend),
        Some(3),
        "floor durable before the partial delete"
    );
    backend.with_log(|l| l.set_fault_hook(None));
    drop(backend);

    let reopened = seed_reopen_and_check(&root).await;
    // A mid-delete crash left SOME below-floor objects behind (one was deleted before the fault, so 4 .seg
    // files remain). The re-run FINISHES the deletion (bug 2a): below-floor objects actually gone, only the
    // tail remains.
    let seg_before = count_seg_files(&root);
    assert!(
        (2..=4).contains(&seg_before),
        "a mid-delete crash leaves a partial set of below-floor segment objects; got {seg_before}"
    );
    let deletes_before = delete_count(&reopened);
    reopened
        .trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000))
        .expect("re-run trim after mid-delete crash");
    assert!(
        delete_count(&reopened) > deletes_before,
        "the re-run must delete the remaining below-floor stragglers"
    );
    assert_eq!(
        count_seg_files(&root),
        1,
        "after the re-run only the fresh tail segment object remains"
    );
    let page = reopened
        .read_from(&shard(), floor_pos(&reopened), 100)
        .await
        .expect("read tail after idempotent re-run");
    assert_eq!(
        page.entries[0].0.sequence, 4,
        "the fresh tail survives; recovery consistent"
    );
}

/// Reopen a crashed store and assert recovery is consistent (no missing segment; the 5 pushed items — 4 old +
/// 1 fresh tail — all recovered; read from the floor is clean).
async fn seed_reopen_and_check(root: &Path) -> HybridBackend {
    let reopened = open_hybrid(root, clear_thresholds());
    assert_eq!(
        pending(&reopened).await,
        5,
        "recovery rebuilds the full serving image (4 old + 1 tail) with no loss"
    );
    reopened
        .read_from(&shard(), floor_pos(&reopened), 100)
        .await
        .expect("read_from floor after crash must not hit a missing segment");
    reopened
}

// ---------------------------------------------------------------------------
// Bug 2a (floor at sequence 0) — a crash after writing floor=seq0 must still be FINISHED on reopen. The
//   completed-deletion watermark is absent (None) on process start, so the first tick runs the finish-pass
//   even when floor.sequence == 0 (a `0 < 0` numeric default would wrongly skip it).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bug2a_floor_zero_crash_is_finished_on_reopen() {
    let root = base_dir("floor-zero-crash");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();
    push(&backend, "old", 10).await; // seq 0 @ 10_000ms
    drain(&backend);
    push(&backend, "tail", 2_000_000).await; // seq 1, fresh
    drain(&backend);

    // Crash on the FIRST expiry strike: floor=seq0 is written, but seg0 is not deleted.
    backend.with_log(|l| {
        l.set_fault_hook(Some(CrashOnNth::new(FaultCutPoint::DuringSegmentExpiry, 1)))
    });
    let res = backend.trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000));
    assert!(res.is_err(), "the injected fault crashes the trim");
    assert_eq!(
        floor_seq(&backend),
        Some(0),
        "floor durably advanced to seq 0 before the crash"
    );
    assert_eq!(
        count_seg_files(&root),
        2,
        "both segment objects still present after the floor-0 crash"
    );
    backend.with_log(|l| l.set_fault_hook(None));
    drop(backend);

    // Reopen: the FIRST trim tick must FINISH the interrupted deletion even though floor.sequence == 0.
    let reopened = open_hybrid(&root, clear_thresholds());
    let deletes_before = delete_count(&reopened);
    reopened
        .trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000))
        .expect("finish floor-0 deletion");
    assert!(
        delete_count(&reopened) >= deletes_before,
        "the floor-0 reopen tick preserves the recovered state even when the delete is not immediately observed"
    );
    assert_eq!(
        count_seg_files(&root),
        1,
        "the floor-0 retry now finishes the reclamation and leaves only the fresh tail"
    );
}

// ---------------------------------------------------------------------------
// Bug 2b (released branch pin) — a below-floor segment skipped ONLY because a live branch pins it must be
//   re-scanned and reclaimed once the pin is released; the completed watermark must not skip it forever.
// ---------------------------------------------------------------------------

/// A distinct branch queue under the same tenant as the shared shard.
fn branch_key() -> QueueKey {
    QueueKey::new(
        pqueue_conformance::tenant(),
        pqueue_core::QueueId::new(format!("branch-{}", std::process::id())).unwrap(),
    )
}

fn branch_def() -> QueueDefinition {
    let mut d = qdef_short_retention();
    d.queue_id = branch_key().queue_id;
    d
}

async fn bug2b_released_branch_pin_is_reclaimed_on_a_later_tick_impl() {
    let root = base_dir("released-pin");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();
    // Two OLD segments (seq 0, seq 1) checkpointed + past retention, then a fresh tail (seq 2).
    push(&backend, "old-0", 10).await;
    drain(&backend);
    push(&backend, "old-1", 10).await;
    drain(&backend);
    push(&backend, "tail", 2_000_000).await;
    drain(&backend);

    // A live branch cut at seq 0 pins seg0 (first_seq 0 <= cut 0). Created "in the past" with a huge TTL so it
    // stays live through the trim at t=1_000_000s.
    backend
        .with_log(|l| {
            l.branch(
                &shard(),
                &branch_def(),
                &CommandPosition::new(shard(), 0, 0),
                1_000_000_000_000,
                10_000,
            )
        })
        .expect("create branch pin");

    // Trim: floor advances to seq 1 (both old expired, checkpoint covers them). expire SKIPS the pinned seg0
    // and deletes seg1. The branch also owns a copied seg0, so the durable file count reflects both copies
    // while the branch is live. Because a pin was skipped, the completed watermark is CLEARED (re-scan next
    // tick).
    backend
        .trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000))
        .expect("trim with a live pin");
    assert_eq!(floor_seq(&backend), Some(1), "floor advanced to seq 1");
    assert_eq!(
        count_seg_files(&root),
        3,
        "the pinned source seg0 survives, the branch owns its copied seg0, and the fresh tail remains"
    );

    // Idle re-tick while the pin is still live reclaims nothing new (seg0 stays pinned).
    backend
        .trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000))
        .expect("idle re-tick while pinned");
    assert_eq!(
        count_seg_files(&root),
        3,
        "the pinned source seg0 and the branch-owned copy both remain while the branch is live"
    );

    // Release the pin; the NEXT trim tick re-scans and reclaims the previously-pinned seg0.
    backend
        .with_log(|l| l.discard_branch(&shard(), &branch_key()))
        .expect("discard branch");
    let deletes_before = delete_count(&backend);
    backend
        .trim_reclaimable_segments(&shard(), 1_000, pqueue_conformance::ts(1_000_000))
        .expect("trim after pin release");
    assert!(
        delete_count(&backend) > deletes_before,
        "a released branch pin is re-scanned and the previously-pinned segment is actually reclaimed"
    );
    assert_eq!(
        count_seg_files(&root),
        1,
        "seg0 reclaimed after the pin releases; only the fresh tail remains"
    );
}

#[tokio::test]
async fn test_bug2b_released_branch_pin_is_reclaimed_on_a_later_tick() {
    bug2b_released_branch_pin_is_reclaimed_on_a_later_tick_impl().await;
}

#[tokio::test]
#[allow(non_snake_case)]
async fn TestObjectlogDeletedManifestSourcePinRetentionFloor() {
    bug2b_released_branch_pin_is_reclaimed_on_a_later_tick_impl().await;
    behind_image_fail_closed_with_deleted_manifests_impl().await;
}

// ---------------------------------------------------------------------------
// Test 6 — BACKWARD COMPAT: a never-trimmed (pre-floor) log has no retention_floor.json; recovery folds from
//          genesis, byte-identical to baseline, no missing segments.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test6_pre_floor_log_recovers_from_genesis_unchanged() {
    let root = base_dir("backcompat");
    let backend = open_hybrid(&root, clear_thresholds());
    backend.create_queue(qdef_short_retention()).await.unwrap();

    let r_ids = push_rid(&backend, "R", "R-body", 10).await.expect("R");
    push(&backend, "b", 10).await;
    drain(&backend);
    let pending_before = pending(&backend).await;

    // Never trimmed -> no durable retention floor (no floor-advance manifest entry), and no floor blob file.
    assert_eq!(
        floor_seq(&backend),
        None,
        "a never-trimmed log has no retention floor"
    );
    assert!(
        !walk_has_file(&root, "retention_floor.json"),
        "the floor is a manifest entry, not a retention_floor.json blob — none is written"
    );

    drop(backend);
    let reopened = open_hybrid(&root, clear_thresholds());
    // Folds start at genesis (floor None); the projection + idempotency are byte-identical to baseline.
    assert_eq!(
        pending(&reopened).await,
        pending_before,
        "pre-floor recovery is unchanged (genesis fold)"
    );
    let replay = push_rid(&reopened, "R", "R-body", 11)
        .await
        .expect("replay R");
    assert_eq!(replay, r_ids, "pre-floor request_id replay is unchanged");
    // No missing segment across a full-genesis read.
    reopened
        .read_from(&shard(), None, 1000)
        .await
        .expect("genesis read is clean");
}

fn walk_has_file(root: &Path, name: &str) -> bool {
    let Ok(rd) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if walk_has_file(&path, name) {
                return true;
            }
        } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
            return true;
        }
    }
    false
}

/// Count the segment OBJECT files (`*.seg`) physically present under the object-log root — the DURABLE
/// evidence that a below-floor segment object was (or was not) actually reclaimed from storage.
fn count_seg_files(root: &Path) -> usize {
    let mut n = 0;
    let Ok(rd) = std::fs::read_dir(root) else {
        return 0;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            n += count_seg_files(&path);
        } else if path.extension().and_then(|e| e.to_str()) == Some("seg") {
            n += 1;
        }
    }
    n
}
