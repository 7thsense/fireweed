//! fireweed-a355d82b: SQLite `queue.commit` per-entry cost must be flat across batch size.
//!
//! Snorri observed superlinear cost: 64 entries/commit ≈ 3.6 ms/entry, 512 ≈ 22.9 ms/entry
//! (~6.3× worse per entry for 8× larger batches). Cause: commit_transition re-cloned and
//! re-validated the entire staged lifecycle-push set on every entry.
//!
//! This regression gate:
//! 1. Reproduces the measurement shape (fixed total work, vary entries/commit).
//! 2. Asserts per-entry cost at 512 is within a stated tolerance of cost at 64.
//! 3. Documents the defect as sqlite log-replay specific (postgres is not superlinear).

#![cfg(feature = "sqlite")]
#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use fireweed::*;
use fireweed_memory::ManualClock;

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t-commit-linear").unwrap(),
        queue_id: QueueId::new("q-commit-linear").unwrap(),
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
        "fireweed-commit-linear-{tag}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    path.to_string_lossy().into_owned()
}

/// Measure mean wall-clock per entry for `Fireweed::commit` at a given entries-per-call size.
///
/// Fixed total of `total_entries` finalize+lifecycle transitions, matching the snorri shape where
/// every commit entry stages a lifecycle continuation.
async fn measure_ms_per_entry(entries_per_commit: usize, total_entries: usize) -> (f64, Duration) {
    assert!(total_entries.is_multiple_of(entries_per_commit));
    let path = tmp_sqlite(&format!("b{entries_per_commit}"));
    let fw = open_sqlite(&path, Arc::new(ManualClock::at(0))).expect("open sqlite");
    let def = qdef();
    let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.expect("create queue");

    let mut inputs = Vec::with_capacity(total_entries);
    for _ in 0..total_entries {
        inputs.push(NewItem::default());
    }
    for chunk in inputs.chunks(500) {
        fw.push_batch(&queue, chunk.to_vec())
            .await
            .expect("push inputs");
    }

    let commits = total_entries / entries_per_commit;
    let mut commit_wall = Duration::ZERO;
    let mut committed_entries = 0usize;

    for _ in 0..commits {
        let claimed = fw
            .claim(&queue, entries_per_commit, 30_000)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), entries_per_commit, "claim batch size");
        let entries: Vec<CommitEntry> = claimed
            .into_iter()
            .map(|item| CommitEntry {
                claim_ref: ClaimRef {
                    item_id: item.item_id,
                    lease_token: item.lease_token.expect("lease token"),
                    lease_expires_at: item.lease_expires_at,
                    item_version: item.item_version,
                },
                finalize: FinalizeKind::Complete,
                side_records: vec![],
                lifecycle_items: vec![NewItem::default()],
                instance_fence: None,
            })
            .collect();
        let t0 = Instant::now();
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
        commit_wall += t0.elapsed();
        for outcome in outcomes {
            match outcome {
                EntryOutcome::Committed { .. } => committed_entries += 1,
                EntryOutcome::Rejected(e) => panic!("entry rejected: {e}"),
            }
        }
    }
    assert_eq!(committed_entries, total_entries);
    let _ = std::fs::remove_file(&path);
    let ms_per_entry = commit_wall.as_secs_f64() * 1000.0 / total_entries as f64;
    (ms_per_entry, commit_wall)
}

/// Per-entry cost at 512 entries/commit must stay within 2.5× of cost at 64 entries/commit.
///
/// Pre-fix superlinearity was ~6.3×. Tolerance is loose enough for noisy CI hosts but tight
/// enough to catch a return of the O(n) staged-set revalidation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_per_entry_cost_is_flat_from_64_to_512() {
    const TOTAL: usize = 1024;
    let _ = measure_ms_per_entry(64, TOTAL).await;

    let (ms_64, wall_64) = measure_ms_per_entry(64, TOTAL).await;
    let (ms_512, wall_512) = measure_ms_per_entry(512, TOTAL).await;

    let ratio = ms_512 / ms_64.max(1e-9);
    eprintln!(
        "sqlite commit linearity: 64 → {ms_64:.3} ms/entry (wall {wall_64:?}); \
         512 → {ms_512:.3} ms/entry (wall {wall_512:?}); ratio={ratio:.2}"
    );

    const MAX_RATIO: f64 = 2.5;
    assert!(
        ratio <= MAX_RATIO,
        "per-entry commit cost must be flat from 64 to 512 entries/call: \
         64={ms_64:.3} ms/entry, 512={ms_512:.3} ms/entry, ratio={ratio:.2} (max {MAX_RATIO}). \
         Superlinearity indicates staged push validation regressed (fireweed-a355d82b)."
    );
}

/// Named repro harness: print the measurement table shape snorri reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_batch_size_sweep_repro_table() {
    const TOTAL: usize = 512;
    eprintln!("entries/commit\tms/entry\twall");
    for batch in [16usize, 32, 64, 128, 256, 512] {
        if !TOTAL.is_multiple_of(batch) {
            continue;
        }
        let (ms, wall) = measure_ms_per_entry(batch, TOTAL).await;
        eprintln!("{batch}\t{ms:.3}\t{wall:?}");
    }
}
