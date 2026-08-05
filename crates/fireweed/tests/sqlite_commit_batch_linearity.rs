//! fireweed-a355d82b / fireweed-60ca4bfd: SQLite `queue.commit` per-entry cost must be flat
//! across batch size — including queues that declare unique secondary/typed indexes.
//!
//! Snorri observed superlinear cost: 64 entries/commit ≈ 3.6 ms/entry, 512 ≈ 22.9 ms/entry
//! (~6.3× worse per entry for 8× larger batches). Cause: commit_transition re-cloned and
//! re-validated the entire staged lifecycle-push set on every entry. a355d82b fixed queues
//! without unique indexes; 60ca4bfd extends the same linearity to unique-index queues via
//! incremental staged-key tracking.
//!
//! This regression gate:
//! 1. Reproduces the measurement shape (fixed total work, vary entries/commit).
//! 2. Asserts per-entry cost at 512 is within a stated tolerance of cost at 64.
//! 3. Covers both plain queues and unique typed-index queues (the a355d82b gap).
//! 4. Documents the defect as sqlite log-replay specific (postgres is not superlinear).

#![cfg(feature = "sqlite")]
#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use axon_esf::IndexDef;
use fireweed::*;
use fireweed_core::{IndexDeclaration, IndexType, QueueIndex};
use fireweed_memory::ManualClock;
use serde_json::json;

fn qdef_plain() -> QueueDefinition {
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

/// Queue shape matching snorri's unique typed index (fireweed-60ca4bfd residual).
fn qdef_unique_typed() -> QueueDefinition {
    let mut def = qdef_plain();
    def.queue_id = QueueId::new("q-commit-linear-unique").unwrap();
    def.typed_indexes = vec![QueueIndex {
        name: "by_run_target_key".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "target_key".to_string(),
            index_type: IndexType::String,
            unique: true,
        }),
    }];
    def
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
/// every commit entry stages a lifecycle continuation. When `unique_lifecycle` is true, each
/// lifecycle item carries a distinct unique-indexed entity key (no intentional conflicts).
async fn measure_ms_per_entry(
    def: QueueDefinition,
    tag: &str,
    entries_per_commit: usize,
    total_entries: usize,
    unique_lifecycle: bool,
) -> (f64, Duration) {
    assert!(total_entries.is_multiple_of(entries_per_commit));
    let path = tmp_sqlite(&format!("{tag}-b{entries_per_commit}"));
    let fw = open_sqlite(&path, Arc::new(ManualClock::at(0))).expect("open sqlite");
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
    let mut next_key = 0u64;

    for _ in 0..commits {
        let claimed = fw
            .claim(&queue, entries_per_commit, 30_000)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), entries_per_commit, "claim batch size");
        let entries: Vec<CommitEntry> = claimed
            .into_iter()
            .map(|item| {
                let lifecycle = if unique_lifecycle {
                    let k = next_key;
                    next_key += 1;
                    NewItem {
                        entity: Some(json!({ "target_key": format!("k-{k}") })),
                        ..Default::default()
                    }
                } else {
                    NewItem::default()
                };
                CommitEntry {
                    claim_ref: ClaimRef {
                        item_id: item.item_id,
                        lease_token: item.lease_token.expect("lease token"),
                        lease_expires_at: item.lease_expires_at,
                        item_version: item.item_version,
                    },
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![lifecycle],
                    instance_fence: None,
                }
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

async fn assert_linearity(label: &str, def: QueueDefinition, tag: &str, unique_lifecycle: bool) {
    const TOTAL: usize = 1024;
    // Warm-up so first-open / page-cache noise does not dominate the small batch.
    let _ = measure_ms_per_entry(
        def.clone(),
        &format!("{tag}-warm"),
        64,
        TOTAL,
        unique_lifecycle,
    )
    .await;

    let (ms_64, wall_64) = measure_ms_per_entry(
        def.clone(),
        &format!("{tag}-64"),
        64,
        TOTAL,
        unique_lifecycle,
    )
    .await;
    let (ms_512, wall_512) =
        measure_ms_per_entry(def, &format!("{tag}-512"), 512, TOTAL, unique_lifecycle).await;

    let ratio = ms_512 / ms_64.max(1e-9);
    eprintln!(
        "sqlite commit linearity ({label}): 64 → {ms_64:.3} ms/entry (wall {wall_64:?}); \
         512 → {ms_512:.3} ms/entry (wall {wall_512:?}); ratio={ratio:.2}"
    );

    const MAX_RATIO: f64 = 2.5;
    assert!(
        ratio <= MAX_RATIO,
        "per-entry commit cost must be flat from 64 to 512 entries/call ({label}): \
         64={ms_64:.3} ms/entry, 512={ms_512:.3} ms/entry, ratio={ratio:.2} (max {MAX_RATIO}). \
         Superlinearity indicates staged push validation regressed \
         (fireweed-a355d82b / fireweed-60ca4bfd)."
    );
}

/// Per-entry cost at 512 entries/commit must stay within 2.5× of cost at 64 entries/commit.
///
/// Pre-fix superlinearity was ~6.3×. Tolerance is loose enough for noisy CI hosts but tight
/// enough to catch a return of the O(n) staged-set revalidation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_per_entry_cost_is_flat_from_64_to_512() {
    assert_linearity("plain", qdef_plain(), "plain", false).await;
}

/// fireweed-60ca4bfd: unique-index queues must get the same flat per-entry commit cost.
///
/// Pre-fix (post-a355d82b) unique-index queues still re-validated the full staged set each
/// entry (~6.3× worse at 512 vs 64). Incremental staged-key tracking makes them linear.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_per_entry_cost_is_flat_with_unique_typed_index() {
    assert_linearity("unique-typed", qdef_unique_typed(), "unique", true).await;
}

/// Within-commit duplicate unique keys must still be rejected after the linear validation path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_rejects_in_commit_duplicate_unique_typed_key() {
    let path = tmp_sqlite("dup-unique");
    let fw = open_sqlite(&path, Arc::new(ManualClock::at(0))).expect("open sqlite");
    let def = qdef_unique_typed();
    let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.expect("create queue");

    fw.push_batch(&queue, vec![NewItem::default(), NewItem::default()])
        .await
        .expect("push");
    let claimed = fw.claim(&queue, 2, 30_000).await.expect("claim");
    assert_eq!(claimed.len(), 2);
    let entry = |item: ClaimedItem, key: &str| CommitEntry {
        claim_ref: ClaimRef {
            item_id: item.item_id,
            lease_token: item.lease_token.expect("lease"),
            lease_expires_at: item.lease_expires_at,
            item_version: item.item_version,
        },
        finalize: FinalizeKind::Complete,
        side_records: vec![],
        lifecycle_items: vec![NewItem {
            entity: Some(json!({ "target_key": key })),
            ..Default::default()
        }],
        instance_fence: None,
    };
    let outcomes = fw
        .commit(
            &queue,
            CommitRequest {
                request_id: None,
                entries: vec![
                    entry(claimed[0].clone(), "same-key"),
                    entry(claimed[1].clone(), "same-key"),
                ],
            },
        )
        .await
        .expect("commit");
    assert!(
        matches!(outcomes[0], EntryOutcome::Committed { .. }),
        "first entry commits: {:?}",
        outcomes[0]
    );
    assert!(
        matches!(outcomes[1], EntryOutcome::Rejected(_)),
        "duplicate unique key must be rejected: {:?}",
        outcomes[1]
    );
    let _ = std::fs::remove_file(&path);
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
        let (ms, wall) =
            measure_ms_per_entry(qdef_plain(), &format!("sweep-{batch}"), batch, TOTAL, false)
                .await;
        eprintln!("{batch}\t{ms:.3}\t{wall:?}");
    }
}
