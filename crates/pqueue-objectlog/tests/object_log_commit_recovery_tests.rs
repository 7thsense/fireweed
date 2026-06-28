//! TP-002 **E3 — object-log cost/ack + recovery** evidence (TD-004; backend `object_log_sqlite_projection`).
//! This is the spec-named E3 suite (TP-002 §"Required suites"); it replaces the pqueue-service-era suite of
//! the same name that was removed in the hexagonal migration.
//!
//! WHAT THIS MEASURES (real, in-process, on the file-backed object-log reference backend):
//!   - THROUGHPUT: object-log ingest (push) and claim+ack sustained items/s, asserted at/above the E0
//!     per-queue floor (10M items/hr == 2777.78 items/s) — TP-002 §E3 "throughput". (NOTE: ~30-50x headroom
//!     on this backend — this is a floor/correctness check, not a tight performance gate; the load-bearing
//!     assertions are the resident-set reconstruction and full-drain counts, which catch a lossy backend.)
//!   - ACK LATENCY: the per-commit finalize (ack) latency distribution (p50/p95/p99), REPORTED alongside
//!     throughput — a per-command-append sanity figure, NOT the §E3 group-commit bar (see the deferral note).
//!   - RECOVERY (correctness + local rebuild rate): the object log is the source of truth — drop the backend,
//!     reopen, and the projection is rebuilt purely by replaying the durable log. We assert the resident set
//!     is fully reconstructed from disk and MEASURE the rebuild time/rate.
//!
//! WHAT THIS DOES NOT MEASURE (honestly deferred — NOT claimed here; do NOT cite this as full §E3 coverage):
//!   - The recovery here is FULL-FROM-GENESIS log replay: `ObjectLogBackend::open` → `rebuild_all` replays
//!     EVERY object from seq 0 and does NOT consult snapshots/high-water (the `SnapshotStore` ports exist but
//!     are unused by recovery). TP-002 §E3's bar is "rebuild from SNAPSHOT + LOG TAIL" — the snapshot+bounded-
//!     tail mechanism is NOT implemented in this reference, so the measured rate is genesis-replay, not the
//!     production snapshot-bounded path, and MUST NOT be extrapolated to a 10M snapshot+tail budget.
//!   - The rebuilt projection is the shared IN-MEMORY log-replay `ProjectionData` (HashMap/BTreeSet), NOT a
//!     SQLite-materialized projection. Despite the `object_log_sqlite_projection` profile NAME, the SQLite
//!     projection family is not what this reference rebuilds; the SQLite-materialized recovery is the
//!     production form.
//!   - GROUP-COMMIT ACK LATENCY ACROSS >=2 SEGMENT SIZES within a `segment_max_latency_ms` window: the
//!     in-process reference appends ONE object per command (no group-commit batching, no configurable segment
//!     size) — the production S3 profile, deferred to the live object-log run (bead pqueue-2f9ebac3).
//!   - COST ($/billion-commands beats `postgres_native` at high sustained volume): an ADR-001 analytical
//!     cost-table claim, not a runtime measurement — deferred (pqueue-2f9ebac3 / ADR-001 analysis).
//!   - MANIFEST-CAS FENCING (stale-epoch writer's manifest CAS rejected; Postgres-pointer fallback): its own
//!     bead (pqueue-e5c6d6fc); the in-process reference stamps the current durable epoch but has no CAS fence.
//!   - The true 10M-item-in-S3 snapshot+tail rebuild within a stated recovery-window budget is the live run
//!     (pqueue-2f9ebac3); here the local genesis-replay rate is REPORTED only.

use std::time::Instant;

use pqueue_conformance::{envelope, item};
use pqueue_core::{
    EligibilityPolicy, ItemId, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRequest, CommandEnvelope, ControlPlaneStore,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, ProjectionRead, PushCommand, QueueCommand,
};
use pqueue_objectlog::ObjectLogBackend;

/// The E0 per-queue throughput floor (TP-002): 10,000,000 accepted items/hr == 2,777.78 items/s.
const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0;

fn tmp_root(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("pqueue-objlog-e3-{tag}-{}", std::process::id()))
}

fn sk(tenant: &str, queue: &str) -> pqueue_engine::QueueKey {
    pqueue_engine::QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn big_qdef(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 600_000,
        client_item_key_retention_ms: 600_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: 10_000_000,
        max_claim_batch_size: 10_000_000,
        max_eligible_group_size: None,
    }
}

/// Apply one command through the atomic unit of work (append + apply) on `shard`, stamping the queue's
/// current durable epoch (the in-process owner is always current). Mirrors the conformance `commit` helper
/// but parameterized by shard so we can address our own large-capacity queue.
async fn commit_to<B: Backend + ControlPlaneStore>(
    backend: &B,
    shard: &pqueue_engine::QueueKey,
    env: CommandEnvelope,
) {
    let epoch = backend.current_epoch(shard).await.expect("current epoch");
    let shard = shard.clone();
    backend
        .write(move |lw, pw| {
            let pos = lw.append(&shard, std::slice::from_ref(&env), epoch)?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .expect("commit");
}

/// Push `items` items into `shard` in batches of `batch`, returning the measured ingest rate (items/s).
async fn push_all(
    b: &ObjectLogBackend,
    shard: &pqueue_engine::QueueKey,
    items: u64,
    batch: u64,
) -> f64 {
    let t = Instant::now();
    let mut pushed = 0u64;
    while pushed < items {
        let n = (items - pushed).min(batch);
        let push_items = (0..n)
            .map(|k| {
                let id = pushed + k;
                item(&format!("{id}"), &format!("k{id}"), (id % 1000) as i64)
            })
            .collect();
        commit_to(
            b,
            shard,
            envelope(
                QueueCommand::Push(PushCommand { items: push_items }),
                vec![],
            ),
        )
        .await;
        pushed += n;
    }
    items as f64 / t.elapsed().as_secs_f64()
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (((sorted.len() as f64) * p).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

#[tokio::test]
async fn object_log_e3_throughput_recovery_and_ack_latency() {
    let root = tmp_root("e3");
    let _ = std::fs::remove_dir_all(&root);
    let shard = sk("e3", "hot");
    let items = 120_000u64;
    let push_batch = 10_000u64;
    let ack_batch = 1_000usize;

    // ----- INGEST throughput -----
    let ingest_rate = {
        let b = ObjectLogBackend::open(&root).expect("open");
        b.create_queue(big_qdef("e3", "hot")).await.unwrap();
        let r = push_all(&b, &shard, items, push_batch).await;
        assert_eq!(
            b.metrics(&shard).await.unwrap().pending,
            items,
            "all pushed items resident before recovery"
        );
        r
    }; // drop the backend → only the durable object log remains on disk

    // ----- RECOVERY: rebuild the projection purely by replaying the durable log on reopen -----
    let t_rec = Instant::now();
    let b = ObjectLogBackend::open(&root).expect("reopen rebuilds from the object log");
    let recovery = t_rec.elapsed();
    assert_eq!(
        b.metrics(&shard).await.unwrap().pending,
        items,
        "recovery must rebuild the full resident set from the object log alone"
    );
    let recovery_rate = items as f64 / recovery.as_secs_f64();

    // ----- CLAIM + ACK throughput and per-commit ack latency -----
    let mut ack_latencies: Vec<f64> = Vec::new();
    let t_claim = Instant::now();
    let mut drained = 0u64;
    while drained < items {
        let claimed = b
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w1").unwrap(),
                max_items: ack_batch,
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: UtcTimestamp::new(3_600_000, 0).unwrap(),
                now: UtcTimestamp::new(1, 0).unwrap(),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        if claimed.items.is_empty() {
            break;
        }
        let ids: Vec<ItemId> = claimed.items.iter().map(|c| c.item_id.clone()).collect();
        let outcomes = ids
            .iter()
            .map(|id| FinalizeOutcome {
                item_id: id.clone(),
                kind: FinalizeKind::Complete,
            })
            .collect();
        let t_ack = Instant::now();
        commit_to(
            &b,
            &shard,
            envelope(
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                ids.clone(),
            ),
        )
        .await;
        ack_latencies.push(t_ack.elapsed().as_secs_f64() * 1000.0); // ms
        drained += ids.len() as u64;
    }
    assert_eq!(drained, items, "claim+ack must drain every item");
    let claim_rate = items as f64 / t_claim.elapsed().as_secs_f64();
    assert_eq!(
        b.metrics(&shard).await.unwrap().pending,
        0,
        "all items finalized"
    );
    ack_latencies.sort_by(|a, c| a.partial_cmp(c).unwrap());

    println!(
        "\nTP-002 E3 object-log cost/ack + recovery (file-backed object log + in-memory replay projection; full-genesis recovery, NOT snapshot+tail / SQLite-materialized production form):"
    );
    println!("  ingest throughput   : {ingest_rate:.0} items/s");
    println!("  claim+ack throughput: {claim_rate:.0} items/s");
    println!(
        "  ack latency (per-commit, NOT production group-commit): p50={:.3}ms p95={:.3}ms p99={:.3}ms",
        pct(&ack_latencies, 0.50),
        pct(&ack_latencies, 0.95),
        pct(&ack_latencies, 0.99)
    );
    println!(
        "  recovery: rebuilt {items} resident items from the log in {:.2}ms ({recovery_rate:.0} items/s replay)",
        recovery.as_secs_f64() * 1000.0
    );

    // ----- E3 bars (in-process) -----
    assert!(
        ingest_rate >= FLOOR_ITEMS_PER_SEC,
        "object-log ingest must hold the E0 floor (>= {FLOOR_ITEMS_PER_SEC:.0}/s): {ingest_rate:.0}/s"
    );
    assert!(
        claim_rate >= FLOOR_ITEMS_PER_SEC,
        "object-log claim+ack must hold the E0 floor (>= {FLOOR_ITEMS_PER_SEC:.0}/s): {claim_rate:.0}/s"
    );
    // Recovery's teeth are the `pending == items` reconstruction assertion above (a lossy rebuild fails it);
    // the rate is reported, not gated. Sanity-bound the genesis replay so a pathological rebuild is caught.
    assert!(
        recovery_rate > FLOOR_ITEMS_PER_SEC,
        "log replay rebuild rate must clear the E0 floor: {recovery_rate:.0}/s"
    );

    // Emit a TP-002 E3 verification-ledger row from the REAL measured values. `backend_profile` is the
    // FILE-BACKED reference (honest: not the SQLite-materialized production form); `environment`/`scale`
    // carry the BQ-42 deferrals (full-genesis replay not snapshot+tail; group-commit ack / cost / SQLite
    // projection / 10M-in-S3 → pqueue-2f9ebac3).
    let row = pqueue_release::LedgerRow {
        suite: "object_log_commit_recovery_tests".into(),
        command: "cargo test -p pqueue-objectlog --test object_log_commit_recovery_tests".into(),
        backend_profile: "object_log_file_reference".into(),
        scale: "in-process-smoke".into(),
        seed: 0,
        environment:
            "in-process file-backed object log + in-memory replay projection; full-genesis recovery (not snapshot+tail); group-commit ack / cost / SQLite-materialized projection / 10M-in-S3 deferred to pqueue-2f9ebac3"
                .into(),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "ingest & claim+ack >= E0 floor; recovery rebuilds full resident set from the durable log".into(),
        evidence_tier: "smoke".into(),
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["E3".into()],
            values: std::collections::BTreeMap::from([
                ("ingest_per_s".into(), serde_json::json!(ingest_rate.round())),
                ("claim_ack_per_s".into(), serde_json::json!(claim_rate.round())),
                ("ack_p50_ms".into(), serde_json::json!((pct(&ack_latencies, 0.50) * 1000.0).round() / 1000.0)),
                ("ack_p95_ms".into(), serde_json::json!((pct(&ack_latencies, 0.95) * 1000.0).round() / 1000.0)),
                ("ack_p99_ms".into(), serde_json::json!((pct(&ack_latencies, 0.99) * 1000.0).round() / 1000.0)),
                ("recovery_replay_per_s".into(), serde_json::json!(recovery_rate.round())),
                ("recovered_items".into(), serde_json::json!(items)),
                ("e0_floor_per_s".into(), serde_json::json!(FLOOR_ITEMS_PER_SEC.round())),
            ]),
        },
    };
    let path = pqueue_release::ledger_path(
        env!("CARGO_MANIFEST_DIR"),
        "object_log_commit_recovery_tests",
    );
    let _ = std::fs::remove_file(&path);
    pqueue_release::append_row(&path, &row).expect("emit E3 ledger row");
    let summary =
        pqueue_release::verify_ledger(&path, true).expect("emitted E3 row validates strict");
    // SMOKE-tier row: recorded under smoke_evidence_ids; a release gate must NOT count it toward headline E3.
    assert!(
        summary.smoke_evidence_ids.contains("E3"),
        "row carries the E3 evidence id"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// Heavier FULL-GENESIS rebuild measurement (NOT the production snapshot+tail path — `rebuild_all` replays
/// every object from seq 0). `#[ignore]` by default — run with
/// `cargo test -p pqueue-objectlog object_log_e3_recovery_at_scale -- --ignored --nocapture`. Scale via
/// `PQUEUE_E3_RECOVERY_ITEMS` (default 1,000,000). The true 10M-item-in-S3 SNAPSHOT+TAIL rebuild within a
/// stated recovery-window budget is the live object-log run (pqueue-2f9ebac3); here the local genesis-replay
/// rate is REPORTED only and must not be extrapolated to the snapshot-bounded budget.
#[tokio::test]
#[ignore = "heavy recovery-at-scale measurement; run explicitly with --ignored"]
async fn object_log_e3_recovery_at_scale() {
    let items: u64 = std::env::var("PQUEUE_E3_RECOVERY_ITEMS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let root = tmp_root("e3-scale");
    let _ = std::fs::remove_dir_all(&root);
    let shard = sk("e3", "scale");

    {
        let b = ObjectLogBackend::open(&root).expect("open");
        b.create_queue(big_qdef("e3", "scale")).await.unwrap();
        let ingest_rate = push_all(&b, &shard, items, 10_000).await;
        println!("\nE3 recovery-at-scale: ingested {items} items at {ingest_rate:.0}/s");
    }

    let t = Instant::now();
    let b = ObjectLogBackend::open(&root).expect("reopen");
    let recovery = t.elapsed();
    assert_eq!(
        b.metrics(&shard).await.unwrap().pending,
        items,
        "recovery rebuilt the full {items}-item resident set from the log"
    );
    println!(
        "E3 recovery-at-scale: rebuilt {items} resident items by FULL-GENESIS replay in {:.2}s ({:.0} items/s) [file-backed in-memory-projection reference; the production snapshot+tail SQLite-projection rebuild within a recovery-window budget is the live run pqueue-2f9ebac3]",
        recovery.as_secs_f64(),
        items as f64 / recovery.as_secs_f64()
    );
    let _ = std::fs::remove_dir_all(&root);
}
