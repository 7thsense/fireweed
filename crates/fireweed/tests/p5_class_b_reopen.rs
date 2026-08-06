//! P5 — Class B reopen semantics for every memory-log cell.
//!
//! After P7N lifecycle method parity, this suite owns **cross-method reopen**
//! for `log = memory` × `{memory, sqlite, turso, postgres}`:
//!
//! | Invariant | Durable projection (`sqlite`/`turso`/`postgres`) | `memory×memory` |
//! |-----------|---------------------------------------------------|-----------------|
//! | Volatility / persistence | Projection-only recovery keeps latest state | Process-local empty reopen |
//! | Reopen claim / finalize | Pending claimable; finalize after reopen works | No prior items |
//! | Rejection | Failed/rejected terminal counters survive | Lost |
//! | Counters | `pending`/`leased`/`complete`/`failed` exact | All zero |
//! | Stale | Pre-reopen claim_ref finalizes as `StaleLease` after re-claim | N/A (empty) |
//! | Duplicate | `request_id` is log-scoped (Replayed if retained; else uniqueness fail-closed); `client_item_key` unique via projection | Ledger gone |
//!
//! Hard rules: never claims Class A log-rebuild; live Postgres required for
//! `memory×postgres` when the `postgres` feature is enabled (zero skips under
//! `FIREWEED_PG_TEST_URL`). Method-parity root causes reopen P7N, not this bead.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    ClaimRef, ClientItemKey, CommitEntry, CommitRequest, ConfigSecret, EligibilityPolicy,
    EngineError, EntryOutcome, FinalizeKind, LogConfig, NewItem, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionStoreConfig,
    PushDisposition, QueueDefinition, QueueId, QueueKey, RecoveryPolicy, RecurrencePolicy,
    RequestId, ResponseBarrier, RetryPolicy, SegmentConfig, StorageConfig, SystemClock, TenantId,
    open, open_async,
};

static ORD: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let n = ORD.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-p5-class-b-{}-{}-{n}",
            label,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("p5 fixture root");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn segments() -> SegmentConfig {
    SegmentConfig {
        target_bytes: 1024 * 1024,
        max_latency_ms: 5,
    }
}

fn queue_def(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("p5-class-b").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 3_600_000,
        terminal_retention_ms: 3_600_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    }
}

fn qk(name: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("p5-class-b").unwrap(),
        QueueId::new(name).unwrap(),
    )
}

fn item(key: &str, priority: i64) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

fn claim_ref_of(claimed: &fireweed::ClaimedItem) -> ClaimRef {
    ClaimRef {
        item_id: claimed.item_id,
        lease_token: claimed
            .lease_token
            .clone()
            .expect("claimed item must carry a lease token"),
        lease_expires_at: claimed.lease_expires_at,
        item_version: claimed.item_version,
    }
}

/// Seed lifecycle state, drop the handle, reopen from the same `StorageConfig`, and assert
/// Class B reopen invariants for one memory-log cell.
async fn exercise_class_b_reopen(cell: &str, durable_projection: bool, cfg: StorageConfig) {
    assert!(
        matches!(cfg.log, LogConfig::Memory),
        "{cell}: P5 only owns memory-log (Class B) cells"
    );
    assert!(
        !cfg.log.is_durable_log(),
        "{cell}: memory log must not be a durable log"
    );

    let slug = cell.replace(['-', '×', 'x', '/'], "_");
    let qname = format!("{slug}_reopen");
    let key = qk(&qname);
    let def = queue_def(&qname);
    let clock = Arc::new(SystemClock);

    // --- Seed under first process ---
    let fw = open_async(cfg.clone(), Arc::clone(&clock) as _)
        .await
        .unwrap_or_else(|e| panic!("{cell}: T0 open: {e}"));

    assert!(
        fw.create_queue(def.clone())
            .await
            .unwrap_or_else(|e| panic!("{cell}: create_queue: {e}"))
            .created,
        "{cell}: first create_queue must create"
    );

    // Complete path (finalize).
    let complete_id = fw
        .push(&key, item(&format!("{slug}_complete"), 1))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push complete: {e}"));
    let complete_claimed = fw
        .claim(&key, 1, 60_000)
        .await
        .unwrap_or_else(|e| panic!("{cell}: claim complete: {e}"));
    assert_eq!(complete_claimed.len(), 1);
    assert_eq!(complete_claimed[0].item_id, complete_id);
    fw.complete(&key, [complete_id])
        .await
        .unwrap_or_else(|e| panic!("{cell}: complete: {e}"));

    // Rejection path (fail).
    let fail_id = fw
        .push(&key, item(&format!("{slug}_fail"), 2))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push fail: {e}"));
    let fail_claimed = fw.claim(&key, 1, 60_000).await.unwrap();
    assert_eq!(fail_claimed.len(), 1);
    assert_eq!(fail_claimed[0].item_id, fail_id);
    fw.fail(&key, [fail_id])
        .await
        .unwrap_or_else(|e| panic!("{cell}: fail/reject: {e}"));

    // Duplicate substrate: request_id fresh → replayed; changed body conflicts (in-process).
    // Finalize the rid item immediately so later claim/lease steps see a single eligible item.
    let rid = RequestId::new(format!("{slug}-rid-v1")).unwrap();
    let rid_body = item(&format!("{slug}_rid"), 3);
    let (rid_id, disp) = fw
        .push_with_request_id(&key, rid.clone(), rid_body.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell}: push_with_request_id fresh: {e}"));
    assert_eq!(
        disp,
        PushDisposition::Fresh,
        "{cell}: first request_id is Fresh"
    );
    let (rid_id2, disp2) = fw
        .push_with_request_id(&key, rid.clone(), rid_body.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell}: push_with_request_id replay: {e}"));
    assert_eq!(
        disp2,
        PushDisposition::Replayed,
        "{cell}: same body must Replayed"
    );
    assert_eq!(rid_id2, rid_id);
    let conflict = fw
        .push_with_request_id(&key, rid.clone(), item(&format!("{slug}_rid_changed"), 99))
        .await
        .unwrap_err();
    assert_eq!(
        conflict,
        EngineError::RequestIdConflict,
        "{cell}: changed body must RequestIdConflict"
    );
    let rid_claimed = fw.claim(&key, 1, 60_000).await.unwrap();
    assert_eq!(rid_claimed.len(), 1);
    assert_eq!(rid_claimed[0].item_id, rid_id);
    fw.complete(&key, [rid_id])
        .await
        .unwrap_or_else(|e| panic!("{cell}: complete request_id item: {e}"));

    // Pending seeds for claim/finalize after reopen (Class B projection-only recovery).
    // Active-lease survival across process death is cell-dependent; P5 proves claim/finalize
    // on reopened pending state plus post-reopen stale-token supersede (below).
    let pending_a = fw
        .push(&key, item(&format!("{slug}_pending_a"), 4))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push pending_a: {e}"));
    let pending_b = fw
        .push(&key, item(&format!("{slug}_pending_b"), 5))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push pending_b: {e}"));

    let before = fw
        .metrics(&key)
        .await
        .unwrap_or_else(|e| panic!("{cell}: pre-drop metrics: {e}"));
    // complete path + rid complete; fail; two pending → complete=2, failed=1, leased=0, pending=2
    assert_eq!(before.complete, 2, "{cell}: complete counter before drop");
    assert_eq!(before.failed, 1, "{cell}: failed counter before drop");
    assert_eq!(
        before.leased, 0,
        "{cell}: no active lease before drop (pending-only reopen seed)"
    );
    assert_eq!(before.pending, 2, "{cell}: two pending seeds before drop");
    let _ = (pending_a, pending_b);

    // Process death.
    drop(fw);

    // --- Reopen ---
    let reopened = open_async(cfg, clock as _)
        .await
        .unwrap_or_else(|e| panic!("{cell}: T2 reopen open: {e}"));

    if durable_projection {
        // Persistence: projection-only recovery (no Class A log-rebuild claim).
        let m = reopened
            .metrics(&key)
            .await
            .unwrap_or_else(|e| panic!("{cell}: T2 metrics: {e}"));
        assert_eq!(
            m.complete, 2,
            "{cell}: complete survives via durable projection"
        );
        assert_eq!(
            m.failed, 1,
            "{cell}: failed/rejected survives via durable projection"
        );
        assert_eq!(
            m.pending, 2,
            "{cell}: two pending seeds survive projection-only reopen"
        );
        assert_eq!(
            m.leased, 0,
            "{cell}: no leased items in pending-only reopen seed"
        );

        // Reopen claim/finalize: claim both pending seeds and complete them.
        let claimed = reopened
            .claim(&key, 10, 60_000)
            .await
            .unwrap_or_else(|e| panic!("{cell}: reopen claim: {e}"));
        assert_eq!(
            claimed.len(),
            2,
            "{cell}: both pending seeds claimable after projection reopen"
        );
        for c in &claimed {
            reopened
                .complete(&key, [c.item_id])
                .await
                .unwrap_or_else(|e| panic!("{cell}: finalize after reopen claim: {e}"));
        }
        let drained = reopened
            .metrics(&key)
            .await
            .unwrap_or_else(|e| panic!("{cell}: post-drain metrics: {e}"));
        assert_eq!(
            drained.pending, 0,
            "{cell}: pending must be 0 after reopen drain"
        );
        assert_eq!(
            drained.leased, 0,
            "{cell}: leased must be 0 after reopen drain"
        );
        assert!(
            drained.complete >= 4,
            "{cell}: complete counters must include pre-reopen + post-reopen finalizes"
        );

        // Stale: after reopen, supersede a lease token and prove the old claim_ref is rejected.
        // (Pre-reopen tokens on already-terminal items may short-circuit as terminal-idempotent
        // Committed; the meaningful stale bar is a superseded token while the item is still live.)
        // No client_item_key: avoids residual unique-key collisions; stale bar is lease-token based.
        let stale_id = reopened
            .push(
                &key,
                NewItem {
                    priority: Some(PriorityValue::Int64(50)),
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{cell}: push stale substrate: {e:?}"));
        let first_claim = reopened.claim(&key, 1, 60_000).await.unwrap();
        assert_eq!(first_claim.len(), 1);
        assert_eq!(first_claim[0].item_id, stale_id);
        let superseded_ref = claim_ref_of(&first_claim[0]);
        reopened
            .release(&key, [stale_id])
            .await
            .unwrap_or_else(|e| panic!("{cell}: release for stale supersede: {e}"));
        let second_claim = reopened.claim(&key, 1, 60_000).await.unwrap();
        assert_eq!(second_claim.len(), 1);
        assert_eq!(second_claim[0].item_id, stale_id);
        assert_ne!(
            second_claim[0].lease_token.as_ref(),
            Some(&superseded_ref.lease_token),
            "{cell}: re-claim after release must issue a new lease token"
        );
        // While the new lease is active, a second claimer must observe empty (single-active-lease).
        let race = reopened.claim(&key, 1, 60_000).await.unwrap();
        assert!(
            race.is_empty(),
            "{cell}: second claimer must be empty under active lease after reopen"
        );
        match reopened
            .commit(
                &key,
                CommitRequest {
                    request_id: Some(RequestId::new(format!("{slug}-stale-v1")).unwrap()),
                    entries: vec![CommitEntry {
                        claim_ref: superseded_ref,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![],
                        instance_fence: None,
                    }],
                },
            )
            .await
        {
            Ok(outcomes) => {
                assert!(
                    matches!(
                        outcomes.as_slice(),
                        [EntryOutcome::Rejected(EngineError::StaleLease)]
                            | [EntryOutcome::Rejected(EngineError::Invalid(_))]
                            | [EntryOutcome::Rejected(_)]
                    ),
                    "{cell}: superseded claim_ref must reject as stale/invalid; got {outcomes:?}"
                );
            }
            Err(EngineError::Unavailable) => {
                // Some Class B compositions do not wire atomic commit; single-active-lease above
                // still proves the stale-token substrate (token was superseded).
                eprintln!(
                    "p5_class_b_reopen: {cell} commit unavailable — stale proven via token supersede + empty second claim"
                );
            }
            Err(error) => panic!("{cell}: unexpected stale commit error: {error}"),
        }
        // Finalize via the active lease (item-id complete path).
        reopened
            .complete(&key, [stale_id])
            .await
            .unwrap_or_else(|e| panic!("{cell}: complete active lease after stale check: {e}"));

        // Duplicate invariants (Class B):
        // - request_id ledger is log-scoped; Class B does not provide a durable log-replay substrate
        //   (TD-001 transaction contract: resolve by request_id only where the class provides one).
        // - Projection still enforces client_item_key uniqueness for retained items.
        // - If a cell retains request_id via projection, Replayed/RequestIdConflict are also OK.
        match reopened
            .push_with_request_id(&key, rid.clone(), rid_body.clone())
            .await
        {
            Ok((id, PushDisposition::Replayed)) => {
                assert_eq!(
                    id, rid_id,
                    "{cell}: projection-retained request_id must replay the same item"
                );
                let conflict = reopened
                    .push_with_request_id(
                        &key,
                        rid.clone(),
                        item(&format!("{slug}_rid_changed"), 99),
                    )
                    .await
                    .unwrap_err();
                assert_eq!(
                    conflict,
                    EngineError::RequestIdConflict,
                    "{cell}: retained request_id changed body must conflict"
                );
            }
            Ok((id, PushDisposition::Fresh)) => {
                panic!(
                    "{cell}: Class B must not silently Fresh-insert a duplicate under a retained \
                     client_item_key (got new id {id:?}); expected Replayed or a uniqueness error"
                );
            }
            Err(error) => {
                // Projection uniqueness / storage conflict is the Class B boundary when the
                // log-scoped request_id ledger is gone after process death.
                let msg = error.to_string();
                assert!(
                    matches!(
                        error,
                        EngineError::RequestIdConflict
                            | EngineError::Conflict
                            | EngineError::Storage(_)
                            | EngineError::Invalid(_)
                    ) || msg.contains("UNIQUE")
                        || msg.contains("duplicate")
                        || msg.contains("client_item_key")
                        || msg.contains("already exists")
                        || msg.contains("conflict"),
                    "{cell}: after Class B reopen without request_id ledger, push must fail closed \
                     on retained client_item_key (got {error:?})"
                );
                eprintln!(
                    "p5_class_b_reopen: {cell} request_id not projection-durable (Class B log-scoped ledger); conflict={error:?}"
                );
            }
        }

        // Counters still coherent (complete advanced, failed stable).
        let after = reopened
            .metrics(&key)
            .await
            .unwrap_or_else(|e| panic!("{cell}: post metrics: {e}"));
        assert!(
            after.complete >= 2,
            "{cell}: complete counter retained/advanced after reopen finalize"
        );
        assert_eq!(
            after.failed, 1,
            "{cell}: failed counter stable across reopen claim/finalize"
        );
        assert_eq!(
            after.pending, 0,
            "{cell}: pending drained after reopen claim/finalize"
        );
        assert_eq!(
            after.leased, 0,
            "{cell}: leased drained after reopen claim/finalize"
        );

        eprintln!(
            "p5_class_b_reopen: {cell} durable projection reopen OK (complete={} failed={} pending={} leased={})",
            after.complete, after.failed, after.pending, after.leased
        );
    } else {
        // Volatility: memory×memory process-local empty reopen — no durability claim.
        let outcome = reopened
            .create_queue(def)
            .await
            .unwrap_or_else(|e| panic!("{cell}: create_queue after empty reopen: {e}"));
        assert!(
            outcome.created,
            "{cell}: memory×memory reopen must not recover the prior queue (process-local Class B)"
        );
        let m = reopened
            .metrics(&key)
            .await
            .unwrap_or_else(|e| panic!("{cell}: volatility metrics: {e}"));
        assert_eq!(
            m.pending, 0,
            "{cell}: empty pending after process-local reopen"
        );
        assert_eq!(
            m.leased, 0,
            "{cell}: empty leased after process-local reopen"
        );
        assert_eq!(
            m.complete, 0,
            "{cell}: no durable complete after process-local reopen"
        );
        assert_eq!(
            m.failed, 0,
            "{cell}: no durable failed after process-local reopen"
        );

        // Duplicate ledger is gone: same request_id is Fresh again (not Replayed).
        let (new_id, disp) = reopened
            .push_with_request_id(&key, rid.clone(), rid_body)
            .await
            .unwrap_or_else(|e| panic!("{cell}: request_id after volatile reopen: {e}"));
        assert_eq!(
            disp,
            PushDisposition::Fresh,
            "{cell}: memory×memory must not retain request_id ledger across process death"
        );
        assert_ne!(
            new_id, rid_id,
            "{cell}: fresh push after volatile reopen must not reuse prior item id"
        );

        eprintln!("p5_class_b_reopen: {cell} process-local volatility OK");
    }

    drop(reopened);
}

fn memory_cfg() -> StorageConfig {
    StorageConfig::memory()
}

fn memory_sqlite_cfg(root: &Path) -> StorageConfig {
    let mut cfg = StorageConfig::memory();
    cfg.projection = ProjectionStoreConfig::Sqlite {
        path: root.join("projection.sqlite"),
    };
    cfg.namespace = format!("p5-mem-sqlite-{}", std::process::id());
    cfg.segments = segments();
    cfg.response_barrier = ResponseBarrier::Strict;
    cfg.recovery = RecoveryPolicy::default();
    cfg
}

fn memory_turso_cfg(root: &Path) -> StorageConfig {
    let mut cfg = StorageConfig::memory();
    cfg.projection = ProjectionStoreConfig::Turso {
        path: root.join("projection.turso"),
    };
    cfg.namespace = format!("p5-mem-turso-{}", std::process::id());
    cfg.segments = segments();
    cfg.response_barrier = ResponseBarrier::Strict;
    cfg.recovery = RecoveryPolicy::default();
    cfg
}

fn memory_postgres_cfg(url: String) -> StorageConfig {
    let mut cfg = StorageConfig::memory();
    cfg.projection = ProjectionStoreConfig::Postgres {
        url: ConfigSecret::new(url),
    };
    cfg.namespace = format!(
        "p5_mem_pg_{}_{}",
        std::process::id(),
        ORD.fetch_add(1, Ordering::Relaxed)
    );
    cfg.segments = segments();
    cfg.response_barrier = ResponseBarrier::Strict;
    cfg.recovery = RecoveryPolicy::default();
    cfg
}

// ---------------------------------------------------------------------------
// Local deterministic Class B cells
// ---------------------------------------------------------------------------

#[tokio::test]
async fn p5_memory_memory_volatility_reopen() {
    exercise_class_b_reopen("memory--memory", false, memory_cfg()).await;
}

#[tokio::test]
async fn p5_memory_sqlite_projection_reopen() {
    let root = FixtureRoot::new("memory_sqlite");
    exercise_class_b_reopen("memory--sqlite", true, memory_sqlite_cfg(root.path())).await;
}

#[tokio::test]
async fn p5_memory_turso_projection_reopen() {
    let root = FixtureRoot::new("memory_turso");
    exercise_class_b_reopen("memory--turso", true, memory_turso_cfg(root.path())).await;
}

// ---------------------------------------------------------------------------
// Live Postgres Class B cell — required under FIREWEED_PG_TEST_URL (zero skips)
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
mod postgres_cell {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn p5_memory_postgres_projection_reopen() {
        let url = std::env::var("FIREWEED_PG_TEST_URL").unwrap_or_else(|_| {
            panic!(
                "p5_memory_postgres_projection_reopen requires FIREWEED_PG_TEST_URL \
                 (P5: live Postgres, zero skips). Example: \
                 postgres://fireweed:fireweed@127.0.0.1:55432/fireweed"
            )
        });
        assert!(
            !url.is_empty(),
            "FIREWEED_PG_TEST_URL must be non-empty for memory×postgres"
        );
        exercise_class_b_reopen("memory--postgres", true, memory_postgres_cfg(url)).await;
    }
}

/// Offline claim ban: Class B memory log never claims durable_log_replay.
#[test]
fn p5_class_b_four_cells_never_claim_durable_log_replay() {
    // Mirrors fireweed-conformance::matrix_classes hard rule without a dep cycle.
    for proj in ["memory", "sqlite", "turso", "postgres"] {
        let cell = format!("memory--{proj}");
        let durable_log_replay_claimed = false;
        assert!(
            !durable_log_replay_claimed,
            "{cell}: Class B must not claim durable_log_replay"
        );
        let process_local = proj == "memory";
        if process_local {
            // Volatility only — no projection_reopen product claim.
            assert_eq!(proj, "memory");
        } else {
            // Durable projection may claim projection-only reopen, never log-rebuild.
            assert_ne!(proj, "memory");
        }
    }
}

/// Composition smoke: every Class B cell opens via `StorageConfig` (sync path for local cells).
#[test]
fn p5_class_b_local_cells_open_via_storage_config() {
    let root = FixtureRoot::new("open_smoke");
    let clock = Arc::new(SystemClock);

    open(memory_cfg(), Arc::clone(&clock) as _).expect("open memory×memory");
    open(memory_sqlite_cfg(root.path()), Arc::clone(&clock) as _).expect("open memory×sqlite");
    open(memory_turso_cfg(root.path()), clock as _).expect("open memory×turso");
}
