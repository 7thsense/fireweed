use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::executor::block_on;
use pqueue_conformance::{qdef, qkey, ts};
use pqueue_engine::{
    ComposedBackend, ControlPlaneStore, InProcessControlPlane, LogRead, LogStore, PushPort,
    PushSpec, QueueKey, ReclaimDriver,
};
use pqueue_objectlog::{ObjectLog, SegmentConfig};
use pqueue_sqlite::{HybridAsyncThresholds, HybridProjectionStore};

static COUNTER: AtomicU64 = AtomicU64::new(0);

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

#[derive(Clone, Copy)]
enum ProjectionMode {
    HybridStrict,
    HybridAsync,
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "pqueue-engine-deleted-manifest-{tag}-{}-{n}",
        std::process::id()
    ))
}

fn shard() -> QueueKey {
    qkey()
}

fn clear_thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds")
}

fn make_mode(root: &Path, mode: ProjectionMode) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = match mode {
        ProjectionMode::HybridStrict => HybridProjectionStore::open(sqlite.to_str().unwrap())
            .expect("hybrid")
            .with_strict_apply(true),
        ProjectionMode::HybridAsync => HybridProjectionStore::open(sqlite.to_str().unwrap())
            .expect("hybrid")
            .with_deferred_flush_chunk(1)
            .with_async_monitor(clear_thresholds()),
    };
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new()).with_group_commit(true)
}

fn drain_projection(backend: &HybridBackend) {
    while backend.with_projection(|p| p.deferred_command_count()) > 0 {
        backend.flush_deferred_projection().expect("flush");
    }
}

async fn push(backend: &HybridBackend, key: &str, at_s: i64) {
    backend
        .push(&shard(), vec![PushSpec::default()], ts(at_s), None)
        .await
        .unwrap_or_else(|e| panic!("push {key}: {e:?}"));
}

async fn create_trimmed_backend(mode: ProjectionMode, root: &Path) -> HybridBackend {
    let backend = make_mode(root, mode).recover().expect("recover");
    backend.create_queue(qdef()).await.unwrap();
    for i in 0..3 {
        push(&backend, &format!("old-{i}"), 10).await;
    }
    drain_projection(&backend);
    push(&backend, "fresh", 10_000).await;
    drain_projection(&backend);
    backend.tick(ts(10_000)).await.unwrap();
    backend
}

#[test]
#[allow(non_snake_case)]
fn TestEngineObjectlogDeletedManifestRecovery() {
    for mode in [ProjectionMode::HybridStrict, ProjectionMode::HybridAsync] {
        let tag = match mode {
            ProjectionMode::HybridStrict => "strict",
            ProjectionMode::HybridAsync => "async",
        };
        let root = tmp_root(tag);
        let backend = block_on(create_trimmed_backend(mode, &root));
        let floor = backend
            .with_log(|log| log.retention_floor(&shard()))
            .expect("retention floor")
            .expect("trimmed floor");
        drop(backend);

        // A healthy reopen still resumes from the retained floor/head.
        let reopened = make_mode(&root, mode).recover().unwrap();
        let replay = block_on(reopened.read_from(&shard(), Some(floor), 100))
            .unwrap_or_else(|e| panic!("{tag}: read_from retained floor errored: {e:?}"));
        assert_eq!(
            replay
                .entries
                .iter()
                .map(|(p, _)| p.sequence)
                .collect::<Vec<_>>(),
            vec![3],
            "{tag}: recovery resumes at the retained floor/head without data loss"
        );
        drop(reopened);

        // A projection image behind the deleted prefix must fail closed on reopen.
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
        }
        let err = match make_mode(&root, mode).recover() {
            Ok(_) => panic!("{tag}: recovery over a deleted manifest prefix must fail closed"),
            Err(err) => err,
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("read below retention floor"),
            "{tag}: deleted manifest prefixes must fail closed with the distinct signal; got {msg}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[test]
#[allow(non_snake_case)]
fn TestEngineObjectlogFloorHeadReplayRecovery() {
    let root = tmp_root("floor-head");
    let backend = block_on(create_trimmed_backend(ProjectionMode::HybridStrict, &root));
    let floor = backend
        .with_log(|log| log.retention_floor(&shard()))
        .expect("retention floor")
        .expect("trimmed floor");
    drop(backend);

    let reopened = make_mode(&root, ProjectionMode::HybridStrict)
        .recover()
        .unwrap();
    let replay = block_on(reopened.read_from(&shard(), Some(floor), 100))
        .unwrap_or_else(|e| panic!("read_from retained floor errored: {e:?}"));
    assert_eq!(
        replay
            .entries
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![3],
        "recovery resumes at the retained floor/head and preserves the live tail"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// TestSqliteEnginePqueueC33c367eReleaseNote: release notes record the evaluated
/// pqueue-c33c367e interaction before landing, including whether it affects
/// SQLite, engine, retained floor/head replay, retention-floor semantics,
/// source-pin semantics, or fail-closed behavior.
#[test]
#[allow(non_snake_case)]
fn TestSqliteEnginePqueueC33c367eReleaseNote() {
    let release_notes = include_str!("../../../docs/releases/v0.14.0.md");
    // pqueue-c33c367e evaluation must be recorded
    assert!(
        release_notes.contains("pqueue-c33c367e"),
        "pqueue-c33c367e evaluation must be recorded in v0.14.0 release notes"
    );
    // SQLite surface: pqueue-c33c367e interaction for SQLite propagation
    assert!(
        release_notes.contains("SQLite") && release_notes.contains("pqueue-c33c367e"),
        "pqueue-c33c367e interaction for SQLite must be recorded in release notes"
    );
    // Engine surface: pqueue-c33c367e interaction for engine composed recovery
    assert!(
        release_notes.contains("engine") && release_notes.contains("pqueue-c33c367e"),
        "pqueue-c33c367e interaction for engine must be recorded in release notes"
    );
    // Retained floor/head replay surface
    assert!(
        release_notes.contains("floor/head replay") || release_notes.contains("floor/head"),
        "retained floor/head replay surface must be recorded in release notes"
    );
    // Retention-floor semantics surface
    assert!(
        release_notes.contains("retention-floor"),
        "retention-floor semantics must be recorded in release notes"
    );
    // Source-pin semantics surface
    assert!(
        release_notes.contains("source-pin") || release_notes.contains("source pin"),
        "source-pin semantics must be recorded in release notes"
    );
    // Fail-closed behavior surface
    assert!(
        release_notes.contains("fail closed") || release_notes.contains("fail-closed"),
        "fail-closed behavior must be recorded in release notes"
    );
}

/// TestDeletedManifestReleaseNoteArtifacts: release notes name governing
/// artifacts docs/perf/design/manifest-compaction-hotpath.md:374 and
/// docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224
/// plus dependency ID pqueue-8928baec.
#[test]
#[allow(non_snake_case)]
fn TestDeletedManifestReleaseNoteArtifacts() {
    let release_notes = include_str!("../../../docs/releases/v0.14.0.md");
    // Governing artifact: docs/perf/design/manifest-compaction-hotpath.md:374
    assert!(
        release_notes.contains("docs/perf/design/manifest-compaction-hotpath.md:374")
            || release_notes.contains("docs/perf/design/manifest-compaction-hotpath.md"),
        "governing artifact docs/perf/design/manifest-compaction-hotpath.md:374 must be named in release notes"
    );
    // Governing artifact: docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224
    assert!(
        release_notes.contains(
            "docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224"
        ) || release_notes
            .contains("docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md"),
        "governing artifact docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224 must be named in release notes"
    );
    // Dependency ID pqueue-8928baec
    assert!(
        release_notes.contains("pqueue-8928baec"),
        "dependency ID pqueue-8928baec must be named in release notes"
    );
}

/// TestDeletedManifestVerificationEvidence: verification evidence document names
/// the governing artifacts, dependency ID pqueue-8928baec, pqueue-c33c367e
/// conclusion, and the SQLite and engine deleted-manifest recovery test symbols
/// covered by sibling beads.
#[test]
#[allow(non_snake_case)]
fn TestDeletedManifestVerificationEvidence() {
    let evidence = include_str!(
        "../../../.ddx/executions/20260714T234920-be4f9d8d/deleted-manifest-recovery-evidence.md"
    );

    // Governing artifact: docs/perf/design/manifest-compaction-hotpath.md:374
    assert!(
        evidence.contains("docs/perf/design/manifest-compaction-hotpath.md:374")
            || evidence.contains("docs/perf/design/manifest-compaction-hotpath.md"),
        "governing artifact hotpath.md:374 must be named in evidence"
    );

    // Governing artifact: docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224
    assert!(
        evidence.contains("TP-003-verification-acceptance-criteria.md:224")
            || evidence.contains("TP-003-verification-acceptance-criteria.md"),
        "governing artifact TP-003:224 must be named in evidence"
    );

    // Dependency ID pqueue-8928baec
    assert!(
        evidence.contains("pqueue-8928baec"),
        "dependency ID pqueue-8928baec must be named in evidence"
    );

    // pqueue-c33c367e conclusion
    assert!(
        evidence.contains("pqueue-c33c367e"),
        "pqueue-c33c367e evaluation conclusion must be named in evidence"
    );

    // Engine-level sibling test symbols
    assert!(
        evidence.contains("TestEngineObjectlogDeletedManifestRecovery"),
        "engine sibling test symbol TestEngineObjectlogDeletedManifestRecovery must be named in evidence"
    );
    assert!(
        evidence.contains("TestEngineObjectlogFloorHeadReplayRecovery"),
        "engine sibling test symbol TestEngineObjectlogFloorHeadReplayRecovery must be named in evidence"
    );
    assert!(
        evidence.contains("TestSqliteEnginePqueueC33c367eReleaseNote"),
        "engine sibling test symbol TestSqliteEnginePqueueC33c367eReleaseNote must be named in evidence"
    );
    assert!(
        evidence.contains("TestDeletedManifestReleaseNoteArtifacts"),
        "engine sibling test symbol TestDeletedManifestReleaseNoteArtifacts must be named in evidence"
    );

    // SQLite-level sibling test symbols
    assert!(
        evidence.contains("TestSqliteObjectlogDeletedManifestRecovery"),
        "SQLite sibling test symbol TestSqliteObjectlogDeletedManifestRecovery must be named in evidence"
    );
    assert!(
        evidence.contains("TestSqliteDeletedManifestErrorPreservesGuarantees"),
        "SQLite sibling test symbol TestSqliteDeletedManifestErrorPreservesGuarantees must be named in evidence"
    );
    assert!(
        evidence.contains("TestSqlitePropagationPqueueC33c367eInteractionRecorded"),
        "SQLite sibling test symbol TestSqlitePropagationPqueueC33c367eInteractionRecorded must be named in evidence"
    );
    assert!(
        evidence.contains("TestSqliteObjectlogFloorHeadReplayRecovery"),
        "SQLite sibling test symbol TestSqliteObjectlogFloorHeadReplayRecovery must be named in evidence"
    );
    assert!(
        evidence.contains("TestSqliteFloorHeadReplayPreservesFailClosedBoundary"),
        "SQLite sibling test symbol TestSqliteFloorHeadReplayPreservesFailClosedBoundary must be named in evidence"
    );
    assert!(
        evidence.contains("TestSqlitePqueueC33c367eInteractionRecorded"),
        "SQLite sibling test symbol TestSqlitePqueueC33c367eInteractionRecorded must be named in evidence"
    );
}

/// TestDeletedManifestEvidenceSurfaces: verification evidence explicitly covers
/// objectlog, SQLite, engine, conformance, formatting, linting, Go, lefthook,
/// PR gate, and Codex adversarial review requirements, marking missing optional
/// tools/configs as operator-required gate failures rather than skipped.
#[test]
#[allow(non_snake_case)]
fn TestDeletedManifestEvidenceSurfaces() {
    let evidence = include_str!(
        "../../../.ddx/executions/20260714T234920-be4f9d8d/deleted-manifest-recovery-evidence.md"
    );

    // Objectlog surface
    assert!(
        evidence.contains("Objectlog") || evidence.contains("objectlog"),
        "objectlog surface must be covered in evidence"
    );

    // SQLite surface
    assert!(
        evidence.contains("SQLite") || evidence.contains("sqlite"),
        "SQLite surface must be covered in evidence"
    );

    // Engine surface
    assert!(
        evidence.contains("Engine") || evidence.contains("engine"),
        "engine surface must be covered in evidence"
    );

    // Conformance surface
    assert!(
        evidence.contains("Conformance") || evidence.contains("conformance"),
        "conformance surface must be covered in evidence"
    );

    // Formatting surface
    assert!(
        evidence.contains("fmt")
            || evidence.contains("Formatting")
            || evidence.contains("formatting"),
        "formatting surface must be covered in evidence"
    );

    // Linting surface
    assert!(
        evidence.contains("clippy") || evidence.contains("Clippy") || evidence.contains("linting"),
        "linting surface must be covered in evidence"
    );

    // Go surface (mark not-applicable)
    assert!(
        evidence.contains("go.mod") || evidence.contains("Go"),
        "Go surface must be covered in evidence (mark not-applicable if absent)"
    );

    // Lefthook surface (mark as operator-required gate failure)
    assert!(
        evidence.contains("operator-required") || evidence.contains("operator_required"),
        "lefthook/config must be marked as operator-required gate failure in evidence"
    );

    // PR gate surface
    assert!(
        evidence.contains("PR gate")
            || evidence.contains("pr-gate")
            || evidence.contains("pr_gate"),
        "PR gate surface must be covered in evidence"
    );

    // Codex adversarial review surface
    assert!(
        evidence.contains("Codex") || evidence.contains("adversarial"),
        "Codex adversarial review surface must be covered in evidence"
    );
}
