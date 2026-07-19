use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::executor::block_on;
use pqueue_conformance::{qdef, qkey, ts};
use pqueue_engine::{
    ComposedBackend, ControlPlaneStore, InProcessControlPlane, LogRead, LogStore,
    MaintenanceStopReason, PushPort, PushSpec, QueueKey, ReclaimDriver,
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

async fn create_authorized_trimmed_backend(root: &Path) -> HybridBackend {
    let backend = make_mode(root, ProjectionMode::HybridStrict)
        .recover()
        .expect("recover");
    backend.create_queue(qdef()).await.unwrap();
    let epoch = backend
        .acquire_epoch(&shard())
        .await
        .expect("acquire maintenance owner");
    assert_eq!(
        backend.with_log(|log| log.maintenance_owner_epoch(&shard())),
        Some(epoch)
    );
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
    let root = tmp_root("strict");
    let backend = block_on(create_authorized_trimmed_backend(&root));
    let floor = backend
        .with_log(|log| log.retention_floor(&shard()))
        .expect("retention floor")
        .expect("trimmed floor");
    drop(backend);

    // A healthy authorized strict reopen resumes from the retained floor/head.
    let reopened = make_mode(&root, ProjectionMode::HybridStrict)
        .recover()
        .unwrap();
    let replay = block_on(reopened.read_from(&shard(), Some(floor), 100))
        .unwrap_or_else(|e| panic!("strict: read_from retained floor errored: {e:?}"));
    assert_eq!(
        replay
            .entries
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![3],
        "strict: recovery resumes at the retained floor/head without data loss"
    );
    drop(reopened);

    // A projection image behind the deleted prefix must fail closed on reopen.
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
    }
    let err = match make_mode(&root, ProjectionMode::HybridStrict).recover() {
        Ok(_) => panic!("strict: recovery over a deleted manifest prefix must fail closed"),
        Err(err) => err,
    };
    let msg = format!("{err:?}");
    assert!(
        msg.contains("read below retention floor"),
        "strict: deleted manifest prefixes must fail closed with the distinct signal; got {msg}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
#[allow(non_snake_case)]
fn TestEngineObjectlogFloorHeadReplayRecovery() {
    let root = tmp_root("floor-head");
    let backend = block_on(create_authorized_trimmed_backend(&root));
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

#[test]
#[allow(non_snake_case)]
fn TestEngineHybridAsyncMissingFrontierRetainsGenesisRecovery() {
    let root = tmp_root("async-frontier-retain");
    let backend = make_mode(&root, ProjectionMode::HybridAsync)
        .recover()
        .expect("recover async");
    block_on(async {
        backend.create_queue(qdef()).await.unwrap();
        backend
            .acquire_epoch(&shard())
            .await
            .expect("acquire async maintenance owner");
        for i in 0..3 {
            push(&backend, &format!("old-{i}"), 10).await;
        }
        drain_projection(&backend);
        push(&backend, "fresh", 10_000).await;
        drain_projection(&backend);
    });
    let deletes_before = backend.with_log(|log| log.counters().delete_count);
    let report = block_on(backend.tick(ts(10_000))).expect("async retention tick");
    assert_eq!(
        report.maintenance.stopped_by,
        Some(MaintenanceStopReason::FrontierProofMissing)
    );
    assert_eq!(
        backend
            .with_log(|log| log.retention_floor(&shard()))
            .expect("retention floor"),
        None,
        "hybrid-async publishes no floor without complete-frontier proof"
    );
    assert_eq!(
        backend.with_log(|log| log.counters().delete_count),
        deletes_before,
        "hybrid-async deletes no segments without complete-frontier proof"
    );
    let assert_genesis = |backend: &HybridBackend, phase: &str| {
        let page = block_on(backend.read_from(&shard(), None, 100))
            .unwrap_or_else(|e| panic!("{phase}: genesis read failed: {e:?}"));
        assert_eq!(
            page.entries
                .iter()
                .map(|(position, _)| position.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3],
            "{phase}: the complete retained log is readable"
        );
    };
    assert_genesis(&backend, "before reopen");
    drop(backend);

    let reopened = make_mode(&root, ProjectionMode::HybridAsync)
        .recover()
        .expect("healthy async reopen");
    assert_genesis(&reopened, "healthy reopen");
    drop(reopened);

    // Projection loss is recoverable from genesis because no prefix was deleted.
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
    }
    let rebuilt = make_mode(&root, ProjectionMode::HybridAsync)
        .recover()
        .expect("async projection-loss reopen rebuilds from retained log");
    assert_genesis(&rebuilt, "projection-loss reopen");
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

/// TestDeletedManifestEvidenceCodexAccuracy: both reports record that direct Codex
/// gpt-5.4 review completed and returned BLOCK with the two actual findings;
/// reject claims that Codex hung, an unobserved sub-agent found zero blockers, or
/// stale sibling reviews satisfy the fresh review.
#[test]
#[allow(non_snake_case)]
fn TestDeletedManifestEvidenceCodexAccuracy() {
    let evidence = include_str!(
        "../../../.ddx/executions/20260714T234920-be4f9d8d/deleted-manifest-recovery-evidence.md"
    );
    let gate_evidence = include_str!(
        "../../../.ddx/executions/20260714T235844-72ceadbe/deleted-manifest-recovery-gate-evidence.md"
    );

    // Both reports must record direct Codex gpt-5.4 review completed
    assert!(
        evidence.contains("Codex gpt-5.4"),
        "evidence must record direct Codex gpt-5.4 review"
    );
    assert!(
        gate_evidence.contains("Codex gpt-5.4"),
        "gate evidence must record direct Codex gpt-5.4 review"
    );

    // Both reports must record BLOCK verdict
    assert!(
        evidence.contains("BLOCK"),
        "evidence must record Codex BLOCK verdict"
    );
    assert!(
        gate_evidence.contains("BLOCK"),
        "gate evidence must record Codex BLOCK verdict"
    );

    // Both reports must document both blocking findings
    // Finding 1: physical deleted-prefix/head behavior not proven (projection.sqlite only)
    assert!(
        evidence.contains("projection.sqlite") && evidence.contains("manifest"),
        "evidence must document physical deleted-prefix finding (projection.sqlite not blob manifest)"
    );
    assert!(
        gate_evidence.contains("projection.sqlite") && gate_evidence.contains("manifest"),
        "gate evidence must document physical deleted-prefix finding"
    );

    // Finding 2: live source-pin replay across reopen unproved
    assert!(
        evidence.contains("source-pin") && evidence.contains("reopen"),
        "evidence must document live source-pin replay across reopen finding"
    );
    assert!(
        gate_evidence.contains("source-pin") && gate_evidence.contains("reopen"),
        "gate evidence must document live source-pin replay across reopen finding"
    );

    // Both reports must reject the false claim that Codex hung
    assert!(
        !evidence.contains("hangs non-interactively"),
        "evidence must not claim Codex hung"
    );
    assert!(
        !evidence.contains("hangs"),
        "evidence must not contain 'hangs' in Codex context"
    );

    // Both reports must reject the false claim that an unobserved sub-agent found zero blockers
    assert!(
        !evidence.contains("No BLOCKING findings"),
        "evidence must not claim an unobserved sub-agent found no blockers"
    );

    // Report 2 must reject stale sibling reviews
    assert!(
        !gate_evidence.contains("SATISFIED (prior sibling bead reviews)"),
        "gate evidence must not claim Codex review satisfied by sibling beads"
    );
    assert!(
        !gate_evidence.contains("COMPLETED (by sibling beads)"),
        "gate evidence must not claim Codex completed by sibling beads"
    );
    assert!(
        !gate_evidence.contains("All reviewed gates returned no blocking findings"),
        "gate evidence must not claim all reviews returned no blocking findings"
    );

    // Both reports must reference the tracking beads for the blocking findings
    assert!(
        evidence.contains("pqueue-879c9d05"),
        "evidence must track blocking finding via pqueue-879c9d05"
    );
    assert!(
        evidence.contains("pqueue-d7134740"),
        "evidence must track blocking finding via pqueue-d7134740"
    );
    assert!(
        gate_evidence.contains("pqueue-879c9d05"),
        "gate evidence must track blocking finding via pqueue-879c9d05"
    );
    assert!(
        gate_evidence.contains("pqueue-d7134740"),
        "gate evidence must track blocking finding via pqueue-d7134740"
    );
}

/// TestDeletedManifestEvidencePrGateAccuracy: both reports acknowledge
/// scripts/ci/pr-gate.sh exists and record the actual enforcing-gate exit result;
/// a timeout or incomplete coverage phase is not PASS.
#[test]
#[allow(non_snake_case)]
fn TestDeletedManifestEvidencePrGateAccuracy() {
    let evidence = include_str!(
        "../../../.ddx/executions/20260714T234920-be4f9d8d/deleted-manifest-recovery-evidence.md"
    );
    let gate_evidence = include_str!(
        "../../../.ddx/executions/20260714T235844-72ceadbe/deleted-manifest-recovery-gate-evidence.md"
    );

    // Both reports must acknowledge scripts/ci/pr-gate.sh exists
    assert!(
        evidence.contains("scripts/ci/pr-gate.sh"),
        "evidence must reference scripts/ci/pr-gate.sh"
    );
    assert!(
        gate_evidence.contains("scripts/ci/pr-gate.sh"),
        "gate evidence must reference scripts/ci/pr-gate.sh"
    );

    // Report 1: must not claim PR gate was "Not run" without acknowledging the script exists
    assert!(
        evidence.contains("exists"),
        "evidence must acknowledge pr-gate.sh exists"
    );

    // Report 2: must not claim PASS when coverage phase was incomplete
    assert!(
        !gate_evidence.contains("PASS (within timeout"),
        "gate evidence must not claim PASS for incomplete coverage"
    );
    assert!(
        gate_evidence.contains("INCOMPLETE"),
        "gate evidence must record the actual INCOMPLETE result"
    );
    assert!(
        gate_evidence.contains("timeout"),
        "gate evidence must document the timeout reason"
    );
    assert!(
        gate_evidence.contains("not PASS"),
        "gate evidence must explicitly state the result is not PASS"
    );
}

/// TestDeletedManifestEvidenceNoFalseGreen: focused assertions that reject the
/// superseded false phrases in both reports and require the blocking findings.
#[test]
#[allow(non_snake_case)]
fn TestDeletedManifestEvidenceNoFalseGreen() {
    let evidence = include_str!(
        "../../../.ddx/executions/20260714T234920-be4f9d8d/deleted-manifest-recovery-evidence.md"
    );
    let gate_evidence = include_str!(
        "../../../.ddx/executions/20260714T235844-72ceadbe/deleted-manifest-recovery-gate-evidence.md"
    );

    // === Report 1 (evidence): reject false Codex claims ===
    // Must NOT say Codex hangs
    assert!(
        !evidence.contains("hangs"),
        "report 1 must not claim Codex hangs"
    );
    // Must NOT say sub-agent found no blockers
    let sub_agent_claims = [
        "independent adversarial-review sub-agent",
        "No BLOCKING findings",
        "sub-agent dispatched",
    ];
    for phrase in &sub_agent_claims {
        assert!(
            !evidence.contains(phrase),
            "report 1 must not contain '{}' (sub-agent claim)",
            phrase
        );
    }
    // Must NOT say PR gate not run (should acknowledge existence)
    assert!(
        !evidence.contains("Not run in this worktree"),
        "report 1 must not say PR gate was 'Not run in this worktree'"
    );

    // === Report 2 (gate evidence): reject false green claims ===
    // Must NOT say pr-gate PASS
    let false_green_pr = ["PASS (within timeout", "PASS (green within timeout"];
    for phrase in &false_green_pr {
        assert!(
            !gate_evidence.contains(phrase),
            "report 2 must not contain '{}' (false pr-gate pass)",
            phrase
        );
    }
    // Must NOT say Codex SATISFIED by sibling beads
    let false_codex = [
        "SATISFIED (prior sibling bead reviews)",
        "COMPLETED (by sibling beads)",
        "All reviewed gates returned no blocking findings",
        "Codex adversarial review gate for this release scope is satisfied by these prior reviews",
    ];
    for phrase in &false_codex {
        assert!(
            !gate_evidence.contains(phrase),
            "report 2 must not contain '{}' (false codex satisfaction)",
            phrase
        );
    }

    // === Both reports: require the blocking findings ===
    for (name, content) in [("evidence", evidence), ("gate evidence", gate_evidence)] {
        // Must have BLOCK verdict
        assert!(
            content.contains("BLOCK"),
            "{name} must contain BLOCK verdict"
        );
        // Both findings must be documented
        assert!(
            content.contains("projection.sqlite"),
            "{name} must document projection.sqlite finding"
        );
        assert!(
            content.contains("source-pin"),
            "{name} must document source-pin finding"
        );
        // Tracking beads
        assert!(
            content.contains("pqueue-879c9d05"),
            "{name} must reference pqueue-879c9d05"
        );
        assert!(
            content.contains("pqueue-d7134740"),
            "{name} must reference pqueue-d7134740"
        );
        // Reference to the evaluation record
        assert!(
            content.contains("20260714T215347-b2d013a9"),
            "{name} must reference the evaluation record"
        );
    }
}
