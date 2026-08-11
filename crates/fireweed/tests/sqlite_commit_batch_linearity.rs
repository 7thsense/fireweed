//! fireweed-a355d82b / fireweed-60ca4bfd / fireweed-110c25bc: SQLite `queue.commit` per-entry
//! cost must amortize with batch size — including unique secondary/typed indexes and
//! finalize+side+fence shapes (snorri).
//!
//! Snorri observed superlinear cost historically (64 ≈ 3.6 ms/entry, 512 ≈ 22.9 ms/entry) and
//! residual inverted batching at v0.31.2 (0.93 ms/entry@500 → 1.44 ms/entry@1000). Prior
//! O(N²) staged validation fixes required only ratio ≤2.5×; product bar is amortization:
//! ms/entry must be monotone non-increasing as entries/commit grows until IO geometry saturates.
//!
//! This regression gate:
//! 1. Reproduces the measurement shape (fixed total work, vary entries/commit).
//! 2. Asserts amortization (ratio ms_large/ms_small ≤ 1.05 in the amortizing range).
//! 3. Covers plain, unique typed-index, finalize+side+fence, multi-index, and the full
//!    snorri shape (19 typed indexes, ~2.3 KB payload, entity docs, 500-entry batches) on
//!    both `open_sqlite` and `open_sqlite_relational` (fireweed-d8ceee81).
//! 4. Prints a ladder table for evidence (`docs/perf/evidence/tp005/commit-amortization-latest.md`).

#![cfg(feature = "sqlite")]
#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use axon_esf::IndexDef;
use fireweed::*;
use fireweed_core::{IndexDeclaration, IndexType, QueueIndex};
use fireweed_memory::ManualClock;
use serde_json::{json, Value as JsonValue};

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

/// fireweed-346a8d9b: snorri-like multi typed-index queue (≥8 single-field indexes).
fn qdef_multi_typed(n: usize) -> QueueDefinition {
    let mut def = qdef_plain();
    def.queue_id = QueueId::new("q-commit-linear-multi").unwrap();
    def.typed_indexes = (0..n)
        .map(|i| QueueIndex {
            name: format!("by_f{i}"),
            declaration: IndexDeclaration::Single(IndexDef {
                field: format!("f{i}"),
                index_type: IndexType::String,
                unique: i == 0, // one unique index among many
            }),
        })
        .collect();
    def
}

/// Measure finalize+lifecycle with multi-field entity documents for multi-index queues.
async fn measure_ms_per_entry_multi_index(
    n_indexes: usize,
    tag: &str,
    entries_per_commit: usize,
    total_entries: usize,
) -> (f64, Duration) {
    assert!(total_entries.is_multiple_of(entries_per_commit));
    let path = tmp_sqlite(&format!("{tag}-b{entries_per_commit}"));
    let fw = open_fw(OpenKind::LogReplay, &path);
    let def = qdef_multi_typed(n_indexes);
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
                let k = next_key;
                next_key += 1;
                let mut entity = serde_json::Map::new();
                entity.insert("f0".into(), json!(format!("k-{k}")));
                for i in 1..n_indexes {
                    entity.insert(format!("f{i}"), json!(format!("v{i}-{k}")));
                }
                CommitEntry {
                    claim_ref: ClaimRef {
                        item_id: item.item_id,
                        lease_token: item.lease_token.expect("lease token"),
                        lease_expires_at: item.lease_expires_at,
                        item_version: item.item_version,
                    },
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![NewItem {
                        entity: Some(JsonValue::Object(entity)),
                        ..Default::default()
                    }],
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

#[derive(Clone, Copy, Debug)]
enum OpenKind {
    /// log=sqlite × projection=memory (Class A log-replay).
    LogReplay,
    /// Unified relational sqlite (snorri production-shaped sole-owner cell).
    Relational,
}

fn open_fw(kind: OpenKind, path: &str) -> Fireweed {
    match kind {
        OpenKind::LogReplay => open_sqlite(path, Arc::new(ManualClock::at(0))).expect("open sqlite"),
        OpenKind::Relational => {
            open_sqlite_relational(path, Arc::new(ManualClock::at(0))).expect("open sqlite relational")
        }
    }
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
    measure_ms_per_entry_kind(
        OpenKind::LogReplay,
        def,
        tag,
        entries_per_commit,
        total_entries,
        unique_lifecycle,
    )
    .await
}

async fn measure_ms_per_entry_kind(
    kind: OpenKind,
    def: QueueDefinition,
    tag: &str,
    entries_per_commit: usize,
    total_entries: usize,
    unique_lifecycle: bool,
) -> (f64, Duration) {
    assert!(total_entries.is_multiple_of(entries_per_commit));
    let path = tmp_sqlite(&format!("{tag}-b{entries_per_commit}"));
    let fw = open_fw(kind, &path);
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
    let (ms_512, wall_512) = measure_ms_per_entry(
        def.clone(),
        &format!("{tag}-512"),
        512,
        TOTAL,
        unique_lifecycle,
    )
    .await;

    let ratio = ms_512 / ms_64.max(1e-9);
    eprintln!(
        "sqlite commit linearity ({label}): 64 → {ms_64:.3} ms/entry (wall {wall_64:?}); \
         512 → {ms_512:.3} ms/entry (wall {wall_512:?}); ratio={ratio:.2}"
    );

    // fireweed-110c25bc: amortization (ratio ≤1.0) with 5% host noise.
    const MAX_RATIO: f64 = 1.05;
    assert!(
        ratio <= MAX_RATIO,
        "per-entry commit cost must amortize from 64 to 512 entries/call ({label}): \
         64={ms_64:.3} ms/entry, 512={ms_512:.3} ms/entry, ratio={ratio:.2} (max {MAX_RATIO}). \
         Rising or flat-linear cost is a product defect (fireweed-110c25bc)."
    );

    // 500→1000 ladder (snorri inverted-batching window).
    const TOTAL_5: usize = 2000;
    let (ms_500, _) = measure_ms_per_entry(
        def.clone(),
        &format!("{tag}-500"),
        500,
        TOTAL_5,
        unique_lifecycle,
    )
    .await;
    let (ms_1000, _) =
        measure_ms_per_entry(def, &format!("{tag}-1000"), 1000, TOTAL_5, unique_lifecycle).await;
    let ratio_5 = ms_1000 / ms_500.max(1e-9);
    eprintln!(
        "sqlite commit 500→1000 ({label}): {ms_500:.3} → {ms_1000:.3} ms/entry; ratio={ratio_5:.2}"
    );
    assert!(
        ratio_5 <= MAX_RATIO,
        "per-entry commit must amortize 500→1000 ({label}): \
         500={ms_500:.3}, 1000={ms_1000:.3}, ratio={ratio_5:.2}"
    );
}

/// Per-entry cost at 512 entries/commit must not exceed cost at 64 (amortization; ≤1.05 noise).
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

/// fireweed-ca9c45a0: print amortization ladder for snorri-shaped entries.
///
/// Fixed total work per ladder segment; prints ms/entry for batches in the snorri set.
/// Interpretation (amortizing / inverted / flat) is recorded in
/// `docs/perf/evidence/tp005/commit-amortization-latest.md`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_batch_size_sweep_repro_table() {
    // Power-of-two ladder (total 1024).
    const TOTAL_POW2: usize = 1024;
    // 500/1000 ladder (total 2000) — snorri's inverted-batching observation window.
    const TOTAL_5: usize = 2000;

    for kind in [OpenKind::LogReplay, OpenKind::Relational] {
        let kind_label = match kind {
            OpenKind::LogReplay => "open_sqlite",
            OpenKind::Relational => "open_sqlite_relational",
        };

        eprintln!("=== shape: finalize+lifecycle (plain) {kind_label} ===");
        eprintln!("entries/commit\tms/entry\twall\ttotal");
        for batch in [64usize, 128, 256, 512] {
            let (ms, wall) = measure_ms_per_entry_kind(
                kind,
                qdef_plain(),
                &format!("sweep-plain-{kind_label}-{batch}"),
                batch,
                TOTAL_POW2,
                false,
            )
            .await;
            eprintln!("{batch}\t{ms:.4}\t{wall:?}\t{TOTAL_POW2}");
        }
        for batch in [500usize, 1000] {
            let (ms, wall) = measure_ms_per_entry_kind(
                kind,
                qdef_plain(),
                &format!("sweep-plain-{kind_label}-{batch}"),
                batch,
                TOTAL_5,
                false,
            )
            .await;
            eprintln!("{batch}\t{ms:.4}\t{wall:?}\t{TOTAL_5}");
        }

        eprintln!("=== shape: finalize+lifecycle (unique typed index) {kind_label} ===");
        eprintln!("entries/commit\tms/entry\twall\ttotal");
        for batch in [64usize, 128, 256, 512] {
            let (ms, wall) = measure_ms_per_entry_kind(
                kind,
                qdef_unique_typed(),
                &format!("sweep-unique-{kind_label}-{batch}"),
                batch,
                TOTAL_POW2,
                true,
            )
            .await;
            eprintln!("{batch}\t{ms:.4}\t{wall:?}\t{TOTAL_POW2}");
        }
        for batch in [500usize, 1000] {
            let (ms, wall) = measure_ms_per_entry_kind(
                kind,
                qdef_unique_typed(),
                &format!("sweep-unique-{kind_label}-{batch}"),
                batch,
                TOTAL_5,
                true,
            )
            .await;
            eprintln!("{batch}\t{ms:.4}\t{wall:?}\t{TOTAL_5}");
        }

        eprintln!("=== shape: finalize+side+fence {kind_label} ===");
        eprintln!("entries/commit\tms/entry\twall\ttotal");
        for batch in [64usize, 128, 256, 512] {
            let (ms, wall) = measure_ms_per_entry_finalize_only_kind(
                kind,
                &format!("sweep-fin-{kind_label}-{batch}"),
                batch,
                TOTAL_POW2,
            )
            .await;
            eprintln!("{batch}\t{ms:.4}\t{wall:?}\t{TOTAL_POW2}");
        }
        for batch in [500usize, 1000] {
            let (ms, wall) = measure_ms_per_entry_finalize_only_kind(
                kind,
                &format!("sweep-fin-{kind_label}-{batch}"),
                batch,
                TOTAL_5,
            )
            .await;
            eprintln!("{batch}\t{ms:.4}\t{wall:?}\t{TOTAL_5}");
        }
    }
}

/// fireweed-2045eac0: finalize + side_record + instance_fence entries (no lifecycle pushes).
///
/// Snorri dispatch commits use this shape; a355d82b/60ca4bfd only fixed push validation.
async fn measure_ms_per_entry_finalize_only(
    tag: &str,
    entries_per_commit: usize,
    total_entries: usize,
) -> (f64, Duration) {
    measure_ms_per_entry_finalize_only_kind(OpenKind::LogReplay, tag, entries_per_commit, total_entries)
        .await
}

async fn measure_ms_per_entry_finalize_only_kind(
    kind: OpenKind,
    tag: &str,
    entries_per_commit: usize,
    total_entries: usize,
) -> (f64, Duration) {
    let path = tmp_sqlite(tag);
    let fw = open_fw(kind, &path);
    let def = qdef_plain();
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
    let mut fence_i = 0u64;

    for _ in 0..commits {
        let claimed = fw
            .claim(&queue, entries_per_commit, 30_000)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), entries_per_commit, "claim batch size");
        let entries: Vec<CommitEntry> = claimed
            .into_iter()
            .map(|item| {
                fence_i += 1;
                let key = format!("inst-{fence_i}").into_bytes();
                CommitEntry {
                    claim_ref: ClaimRef {
                        item_id: item.item_id,
                        lease_token: item.lease_token.expect("lease token"),
                        lease_expires_at: item.lease_expires_at,
                        item_version: item.item_version,
                    },
                    finalize: FinalizeKind::Complete,
                    side_records: vec![SideRecord {
                        key: format!("side-{fence_i}").into_bytes(),
                        payload: bytes::Bytes::from_static(b"payload"),
                    }],
                    lifecycle_items: vec![],
                    instance_fence: Some(InstanceFence {
                        instance_key: key,
                        expected: 0,
                        next: 1,
                    }),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_finalize_side_record_fence_commit_per_entry_cost_is_flat() {
    const TOTAL: usize = 1024;
    let _ = measure_ms_per_entry_finalize_only("fin-warm", 64, TOTAL).await;
    let (ms_64, wall_64) = measure_ms_per_entry_finalize_only("fin-64", 64, TOTAL).await;
    let (ms_512, wall_512) = measure_ms_per_entry_finalize_only("fin-512", 512, TOTAL).await;
    let ratio = ms_512 / ms_64.max(1e-9);
    eprintln!(
        "sqlite finalize+side+fence linearity: 64 → {ms_64:.3} ms/entry (wall {wall_64:?}); \
         512 → {ms_512:.3} ms/entry (wall {wall_512:?}); ratio={ratio:.2}"
    );
    const MAX_RATIO: f64 = 1.05;
    assert!(
        ratio <= MAX_RATIO,
        "per-entry finalize+side+fence commit cost must amortize 64→512: \
         64={ms_64:.3}, 512={ms_512:.3}, ratio={ratio:.2} (max {MAX_RATIO}). \
         fireweed-110c25bc / fireweed-6e651ac5."
    );
    // Absolute software floor on open_sqlite + ManualClock (B2 AC #3).
    assert!(
        ms_512 <= 0.25,
        "finalize+side+fence @512 must be ≤0.25 ms/entry on open_sqlite (got {ms_512:.3})"
    );

    // 500→1000 ladder (snorri inverted-batching window).
    const TOTAL_5: usize = 2000;
    let (ms_500, _) = measure_ms_per_entry_finalize_only("fin-500", 500, TOTAL_5).await;
    let (ms_1000, _) = measure_ms_per_entry_finalize_only("fin-1000", 1000, TOTAL_5).await;
    let ratio_5 = ms_1000 / ms_500.max(1e-9);
    eprintln!(
        "sqlite finalize+side+fence 500→1000: {ms_500:.3} → {ms_1000:.3} ms/entry; ratio={ratio_5:.2}"
    );
    assert!(
        ratio_5 <= MAX_RATIO,
        "finalize+side+fence must amortize 500→1000: 500={ms_500:.3}, 1000={ms_1000:.3}, ratio={ratio_5:.2}"
    );
}

/// fireweed-346a8d9b: amortization holds with ≥8 typed indexes (snorri-like index count).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_amortizes_with_multi_typed_indexes() {
    const N_INDEXES: usize = 8;
    const TOTAL: usize = 1024;
    let _ = measure_ms_per_entry_multi_index(N_INDEXES, "multi-warm", 64, TOTAL).await;
    let (ms_64, wall_64) =
        measure_ms_per_entry_multi_index(N_INDEXES, "multi-64", 64, TOTAL).await;
    let (ms_512, wall_512) =
        measure_ms_per_entry_multi_index(N_INDEXES, "multi-512", 512, TOTAL).await;
    let ratio = ms_512 / ms_64.max(1e-9);
    eprintln!(
        "sqlite multi-index ({N_INDEXES}) commit: 64 → {ms_64:.3} ms/entry (wall {wall_64:?}); \
         512 → {ms_512:.3} ms/entry (wall {wall_512:?}); ratio={ratio:.2}"
    );
    const MAX_RATIO: f64 = 1.05;
    assert!(
        ratio <= MAX_RATIO,
        "multi-index commit must amortize 64→512: 64={ms_64:.3}, 512={ms_512:.3}, ratio={ratio:.2}"
    );
}

/// fireweed-d8ceee81: assert per-entry amortization for the snorri-shaped probe.
///
/// Ratio-only (host-independent), never an absolute floor: ms/entry at 500 and at 512
/// entries/commit must each be <=1.05x ms/entry at 64. This is the assertion that failed to
/// exist when relational bulk-apply coalescing regressed snorri's real w=8 ladder while the
/// unasserted probe kept printing worse numbers and exiting 0 (docs/perf/evidence/tp005/
/// commit-amortization-latest.md, HOLD fireweed-6bfe48ca).
fn assert_snorri_amortizes(kind_label: &str, ms_64: f64, ms_500: f64, ms_512: f64) {
    const MAX_RATIO: f64 = 1.05;
    let ratio_500 = ms_500 / ms_64.max(1e-9);
    let ratio_512 = ms_512 / ms_64.max(1e-9);
    assert!(
        ratio_500 <= MAX_RATIO,
        "snorri-shaped commit must amortize 64->500 entries/commit ({kind_label}): \
         64={ms_64:.3} ms/entry, 500={ms_500:.3} ms/entry, 512={ms_512:.3} ms/entry, \
         ratio(500/64)={ratio_500:.2} (max {MAX_RATIO}). Rising per-entry cost at 500-entry \
         batches is the durable_queue_commit inflation signature (fireweed-6bfe48ca)."
    );
    assert!(
        ratio_512 <= MAX_RATIO,
        "snorri-shaped commit must amortize 64->512 entries/commit ({kind_label}): \
         64={ms_64:.3} ms/entry, 500={ms_500:.3} ms/entry, 512={ms_512:.3} ms/entry, \
         ratio(512/64)={ratio_512:.2} (max {MAX_RATIO})."
    );
}

/// Proves `assert_snorri_amortizes` can actually fail: feeds it a synthetic ladder shaped like
/// the reverted relational bulk-apply-coalescing regression (500/512-entry batches costing MORE
/// per entry than 64-entry batches — durable_queue_commit inflation, HOLD fireweed-6bfe48ca) and
/// asserts it panics. Without this, the gate above could silently stop asserting (e.g. a future
/// edit turns the `assert!` back into an `eprintln!`) and no test would catch it.
#[test]
fn sqlite_commit_snorri_shape_rejects_batch_inversion() {
    let result = std::panic::catch_unwind(|| {
        // Synthetic, not measured: mirrors the observed v0.31.2->landing regression where
        // 500-entry batches got slower per entry than 64-entry batches on the relational path.
        assert_snorri_amortizes("open_sqlite_relational (synthetic-inverted)", 0.30, 0.48, 0.46);
    });
    assert!(
        result.is_err(),
        "assert_snorri_amortizes must reject an inverted batch-size ladder (500/512-entry \
         batches costing more per entry than 64-entry batches); it did not panic on synthetic \
         inverted data, so the regression gate cannot actually fail."
    );
}

/// fireweed-6bfe48ca / fireweed-d8ceee81: snorri-shaped regression gate — 19 typed indexes,
/// ~2.3 KB payload, entity docs.
///
/// Asserts per-entry amortization (ratio <=1.05x cost-at-64) for both open_sqlite and
/// open_sqlite_relational at 500 and 512 entries/commit; still prints the full ladder for
/// evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_commit_snorri_shaped_ladder_probe() {
    const N_INDEXES: usize = 19;
    const PAYLOAD_BYTES: usize = 2300;
    const TOTAL: usize = 1000; // divisible by 64? 1000/64 no — use 1024 and 1000 separately

    let payload = bytes::Bytes::from(vec![b'x'; PAYLOAD_BYTES]);

    async fn measure(
        kind: OpenKind,
        n_indexes: usize,
        payload: bytes::Bytes,
        batch: usize,
        total: usize,
        tag: &str,
    ) -> f64 {
        assert!(total.is_multiple_of(batch));
        let path = tmp_sqlite(tag);
        let fw = open_fw(kind, &path);
        let def = qdef_multi_typed(n_indexes);
        let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        fw.create_queue(def).await.expect("create");
        let inputs: Vec<NewItem> = (0..total)
            .map(|_| NewItem {
                payload: Some(payload.clone()),
                ..Default::default()
            })
            .collect();
        for chunk in inputs.chunks(500) {
            fw.push_batch(&queue, chunk.to_vec()).await.expect("push");
        }
        let mut wall = Duration::ZERO;
        let mut next = 0u64;
        for _ in 0..(total / batch) {
            let claimed = fw.claim(&queue, batch, 30_000).await.expect("claim");
            let entries: Vec<CommitEntry> = claimed
                .into_iter()
                .map(|item| {
                    let k = next;
                    next += 1;
                    let mut entity = serde_json::Map::new();
                    entity.insert("f0".into(), json!(format!("k-{k}")));
                    for i in 1..n_indexes {
                        entity.insert(format!("f{i}"), json!(format!("v{i}-{k}")));
                    }
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
                            entity: Some(JsonValue::Object(entity)),
                            payload: Some(payload.clone()),
                            ..Default::default()
                        }],
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
            wall += t0.elapsed();
            for o in outcomes {
                if let EntryOutcome::Rejected(e) = o {
                    panic!("rejected: {e}");
                }
            }
        }
        let _ = std::fs::remove_file(&path);
        wall.as_secs_f64() * 1000.0 / total as f64
    }

    eprintln!("=== snorri-shaped (19 indexes, ~2.3KB payload) ===");
    for kind in [OpenKind::LogReplay, OpenKind::Relational] {
        let label = match kind {
            OpenKind::LogReplay => "open_sqlite",
            OpenKind::Relational => "open_sqlite_relational",
        };
        eprintln!("--- {label} ---");
        let (mut ms_64, mut ms_500, mut ms_512) = (0.0f64, 0.0f64, 0.0f64);
        for (batch, total) in [(64usize, 1024), (500, 1000), (512, 1024)] {
            let ms = measure(
                kind,
                N_INDEXES,
                payload.clone(),
                batch,
                total,
                &format!("snorri-{label}-{batch}"),
            )
            .await;
            eprintln!("entries/commit={batch}\tms/entry={ms:.4}\ttotal={total}");
            match batch {
                64 => ms_64 = ms,
                500 => ms_500 = ms,
                512 => ms_512 = ms,
                _ => unreachable!(),
            }
        }
        assert_snorri_amortizes(label, ms_64, ms_500, ms_512);
    }
}
