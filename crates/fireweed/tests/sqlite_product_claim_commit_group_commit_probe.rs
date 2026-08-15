//! fireweed-3469cf97: does shortening the queue admit permit let product-path multi-worker
//! claim+commit on ONE queue produce concurrent log appends (and therefore group-commit seal
//! coalescing), the way `sqlite_log_group_commit_stress` proves for direct concurrent
//! `AsyncLogStore::append` calls?
//!
//! `commit_transition`'s pure CPU prep (entity validate, push-item build, claim-ref shape) now
//! runs off the queue admit permit (fireweed-3469cf97, landed on `commit_transition`). This probe
//! measures whether that is enough to also free concurrent *durable* appends for claim+commit on
//! a single queue, or whether the per-queue admit permit (`AsyncComposedBackend::submit_operation`)
//! still serializes the durable section end to end — see
//! `same_queue_operation_planning_starts_only_after_predecessor_releases` in
//! `fireweed-engine::async_composed` for the exclusivity guarantee this measures against.
//!
//! ```text
//! cargo test -p fireweed --test sqlite_product_claim_commit_group_commit_probe --release \
//!   --features sqlite -- --nocapture
//! ```
//!
//! Evidence: docs/perf/evidence/tp005/multi-worker-tps-latest.md

#![cfg(feature = "sqlite")]

use std::sync::Arc;

use fireweed::*;
use fireweed_memory::ManualClock;

const WORKERS: usize = 8;
const CLAIM_BATCH: usize = 10;
const ITERATIONS_PER_WORKER: usize = 8;

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t-product-gc").unwrap(),
        queue_id: QueueId::new("q-product-gc").unwrap(),
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
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 10_000,
        max_claim_batch_size: 10_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    }
}

fn tmp_sqlite(tag: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "fireweed-product-gc-probe-{tag}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    path.to_string_lossy().into_owned()
}

async fn worker_loop(fw: Arc<Fireweed>, queue: QueueKey, key_base: u64) -> usize {
    let mut committed = 0usize;
    let mut next_key = key_base;
    for _ in 0..ITERATIONS_PER_WORKER {
        let claimed = fw.claim(&queue, CLAIM_BATCH, 30_000).await.expect("claim");
        assert_eq!(claimed.len(), CLAIM_BATCH, "claim batch size");
        let entries: Vec<CommitEntry> = claimed
            .into_iter()
            .map(|item| {
                let k = next_key;
                next_key += 1;
                CommitEntry {
                    claim_ref: ClaimRef {
                        item_id: item.item_id,
                        lease_token: item.lease_token.expect("lease"),
                        lease_expires_at: item.lease_expires_at,
                        item_version: item.item_version,
                    },
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![NewItem {
                        payload: Some(bytes::Bytes::from(format!("payload-{k}"))),
                        ..Default::default()
                    }],
                    instance_fence: None,
                }
            })
            .collect();
        let outcomes = fw
            .commit(
                &queue,
                CommitRequest {
                    request_id: None,
                    entries,
                },
            )
            .await
            .expect("commit");
        for o in outcomes {
            if let EntryOutcome::Rejected(e) = o {
                panic!("rejected: {e}");
            }
        }
        committed += CLAIM_BATCH;
    }
    committed
}

/// Measures `(seals, appends)` for product-path claim+commit on ONE queue under concurrent
/// workers. Informational probe (see module docs) — the pass/fail bar is that every worker's
/// claim+commit completes correctly; whether seals < appends is the fact under investigation,
/// not a gate, since the per-queue admit permit's exclusivity is itself load-bearing correctness
/// (double-claim / fence-race prevention) and is not expected to relax within this probe.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn product_claim_commit_group_commit_stats_on_one_queue() {
    let path = tmp_sqlite("run");
    let clock = Arc::new(ManualClock::at(0));
    let (fw, backend) = open_sqlite_with_lock_stats_handle(&path, clock).expect("open sqlite");
    let fw = Arc::new(fw);
    let def = qdef();
    let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.expect("create queue");

    let seed_total = CLAIM_BATCH * WORKERS;
    let seed: Vec<NewItem> = (0..seed_total)
        .map(|i| NewItem {
            payload: Some(bytes::Bytes::from(format!("seed-{i}"))),
            ..Default::default()
        })
        .collect();
    fw.push_batch(&queue, seed).await.expect("seed push");

    backend.reset_log_group_commit_stats();

    let mut tasks = Vec::with_capacity(WORKERS);
    for w in 0..WORKERS {
        let fw = Arc::clone(&fw);
        let queue = queue.clone();
        let key_base = (w as u64) * 1_000_000;
        tasks.push(tokio::spawn(worker_loop(fw, queue, key_base)));
    }
    let mut total_committed = 0usize;
    for t in tasks {
        total_committed += t.await.expect("worker task panicked");
    }
    assert_eq!(
        total_committed,
        CLAIM_BATCH * WORKERS * ITERATIONS_PER_WORKER
    );

    let (seals, appends) = backend
        .log_group_commit_stats()
        .expect("group-commit enabled on open_sqlite");
    eprintln!(
        "product claim+commit (one queue, {WORKERS} workers): seals={seals} appends={appends} \
         coalesced={}",
        seals < appends
    );

    // Every durable claim or commit_transition call performs exactly one logical append; with
    // the per-queue admit permit serializing entry into the durable section (claim validate +
    // log append + projection apply), no two of those appends are ever in flight together, so
    // seals == appends is the expected steady state here — unlike the direct-append stress test,
    // which bypasses the permit entirely.
    assert!(appends > 0, "expected at least one durable append");
    assert!(seals > 0, "expected at least one seal");
    assert!(
        seals <= appends,
        "seals must never exceed logical appends: seals={seals} appends={appends}"
    );

    let _ = std::fs::remove_file(&path);
}
