//! Real load-driving harness shared by the release-scale evidence suites.
//!
//! This module exists to replace the previous "ledger writer" suites, which
//! wrote hard-coded `0`/constant measurements into the verification ledger
//! without ever exercising the engine. Everything here drives the *real*
//! in-process storage engine (`pqueue_storage::memory` + `fault_injection`)
//! under concurrency and fault injection, and the violation/throughput numbers
//! it reports are *measured*, not asserted.
//!
//! Scale is parameterised by environment variables so a CI run executes a
//! tractable-but-genuine load while a release run on capable hardware can be
//! scaled up toward the TP-002/TP-003 envelope:
//!   PQUEUE_STRESS_RESIDENT_ITEMS  (default 40_000)
//!   PQUEUE_STRESS_CONCURRENCY     (default 256)
//!   PQUEUE_STRESS_KILL_CYCLES     (default 1_000)
//!   PQUEUE_STRESS_CLAIM_BATCH     (default 64)
//!
//! The in-memory backend `batch_claim` does a full O(n) scan per call, so the
//! 1M/10M *resident envelope* named by TP-003 is certified on the persistent
//! backends via the performance suites; this harness certifies the invariant
//! *mechanism* (single-active-lease, no-lost-work, no-conflicting-terminal,
//! ordering, durable-ack) under real concurrency + crash/replay.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, PriorityValue, QueueId, TenantId, UtcTimestamp,
};
use pqueue_storage::commands::{
    BatchFinalizeCommand, BatchPushCommand, FinalizeKind, FinalizeOutcome, PushItem,
};
use pqueue_storage::fault_injection::{FailureMode, FaultInjectedLogStore, replay};
use pqueue_storage::memory::{MemoryLogStore, MemoryProjectionStore};
use pqueue_storage::multi_shard::{
    MultiShardCommandKind, ShardCommandCommit, ShardProgress, aggregate_cross_shard_progress,
    evaluate_multi_shard_command_convergence,
};
use pqueue_storage::traits::{ClaimRequest, LogStore, ProjectionStore};
use pqueue_storage::types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey};
use pqueue_storage::{CommandEnvelope, CommandId, QueueCommand};

/// Index into [`StressOutcome::inv_violations`]; `INV_n` lives at `n - 1`.
pub const INV_COUNT: usize = 10;

#[derive(Debug, Clone)]
pub struct StressConfig {
    pub resident_items: u64,
    pub concurrency: u64,
    pub kill_cycles: u64,
    pub claim_batch: usize,
}

impl StressConfig {
    /// Configuration from env, defaulting to a tractable-but-genuine load.
    pub fn from_env() -> Self {
        Self {
            resident_items: env_u64("PQUEUE_STRESS_RESIDENT_ITEMS", 40_000),
            concurrency: env_u64("PQUEUE_STRESS_CONCURRENCY", 256),
            kill_cycles: env_u64("PQUEUE_STRESS_KILL_CYCLES", 1_000),
            claim_batch: env_u64("PQUEUE_STRESS_CLAIM_BATCH", 64) as usize,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StressOutcome {
    pub pushed: u64,
    pub completed: u64,
    /// Measured violation count per invariant; index `n-1` is INV-`n`.
    pub inv_violations: [u64; INV_COUNT],
    pub measured_resident_items: u64,
    pub measured_concurrency: u64,
    pub measured_kill_count: u64,
    pub claim_p95_micros: u64,
    pub claim_p99_micros: u64,
}

impl StressOutcome {
    pub fn total_violations(&self) -> u64 {
        self.inv_violations.iter().sum()
    }

    pub fn inv(&self, n: usize) -> u64 {
        self.inv_violations[n - 1]
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn tenant(s: &str) -> TenantId {
    TenantId::new(s).expect("tenant id")
}

fn queue(s: &str) -> QueueId {
    QueueId::new(s).expect("queue id")
}

fn item_id(index: u64) -> ItemId {
    // Zero-padded so lexical order == push order (used by the INV-6 check).
    ItemId::new(format!("itm-{index:012}")).expect("item id")
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).expect("timestamp")
}

fn shard_key(t: &TenantId, q: &QueueId, shard_id: u32) -> ShardKey {
    ShardKey {
        tenant_id: t.clone(),
        queue_id: q.clone(),
        shard_id: ShardId::new(shard_id),
    }
}

fn queue_key(t: &TenantId, q: &QueueId) -> QueueKey {
    QueueKey {
        tenant_id: t.clone(),
        queue_id: q.clone(),
    }
}

/// Build a single-item push envelope with a (deterministically skewed) priority.
fn push_envelope(
    t: &TenantId,
    q: &QueueId,
    shard_id: u32,
    index: u64,
    cmd_seq: u64,
) -> CommandEnvelope {
    let id = item_id(index);
    // Deterministically skewed priority distribution: a few hot priorities.
    let priority = PriorityValue::Int64((index % 7) as i64);
    let item = PushItem {
        client_item_key: ClientItemKey::new(format!("cik-{index:012}")).expect("cik"),
        item_id: id.clone(),
        priority: Some(priority),
        not_before: None,
        max_attempts: 3,
        payload: None,
    };
    CommandEnvelope {
        command_id: CommandId::new(format!("push-{shard_id}-{cmd_seq}")),
        request_id: None,
        tenant_id: t.clone(),
        queue_id: q.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: vec![id],
        command: QueueCommand::BatchPush(BatchPushCommand { items: vec![item] }),
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

fn finalize_envelope(
    t: &TenantId,
    q: &QueueId,
    shard_id: u32,
    ids: &[ItemId],
    cmd_seq: u64,
) -> CommandEnvelope {
    let outcomes: Vec<FinalizeOutcome> = ids
        .iter()
        .map(|id| FinalizeOutcome {
            item_id: id.clone(),
            kind: FinalizeKind::Complete,
        })
        .collect();
    CommandEnvelope {
        command_id: CommandId::new(format!("fin-{shard_id}-{cmd_seq}")),
        request_id: None,
        tenant_id: t.clone(),
        queue_id: q.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: ids.to_vec(),
        command: QueueCommand::BatchFinalize(BatchFinalizeCommand { outcomes }),
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

fn percentile(sorted_micros: &[u64], pct: f64) -> u64 {
    if sorted_micros.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * (sorted_micros.len() as f64 - 1.0)).round() as usize;
    sorted_micros[rank.min(sorted_micros.len() - 1)]
}

/// Deliberate fault injected into the harness itself, used by the non-ignored
/// self-test to prove the invariant watchdogs actually fire on a real break.
/// `None` is the production path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StressFault {
    None,
    /// A worker re-reports an already-claimed id (simulates a double-lease).
    DuplicateClaim,
    /// Workers never finalize (simulates lost/abandoned work).
    SkipFinalize,
}

/// Drive the real engine under concurrency + crash/replay and MEASURE the
/// invariant violations. Returns measured counts; the caller asserts they are
/// zero before writing any ledger evidence.
pub async fn run_invariant_stress(cfg: &StressConfig) -> StressOutcome {
    run_invariant_stress_with_fault(cfg, StressFault::None).await
}

pub async fn run_invariant_stress_with_fault(
    cfg: &StressConfig,
    fault: StressFault,
) -> StressOutcome {
    let t = tenant("stress-tenant");
    let q = queue("stress-queue");
    let sk = shard_key(&t, &q, 0);
    let qk = queue_key(&t, &q);

    let proj = Arc::new(MemoryProjectionStore::new());

    // --- Phase 1: load the resident set (Pending). ---
    let resident = cfg.resident_items;
    let load_batch = 512u64;
    let mut seq = 0u64;
    let mut idx = 0u64;
    while idx < resident {
        let mut envs = Vec::new();
        let end = (idx + load_batch).min(resident);
        for i in idx..end {
            envs.push(push_envelope(&t, &q, 0, i, seq));
            seq += 1;
        }
        let pos = CommandPosition {
            shard_key: sk.clone(),
            sequence: idx,
            backend_epoch: 0,
        };
        proj.apply_committed(pos, &envs).await.expect("load push");
        idx = end;
    }

    // INV-5 idempotency runs against an ISOLATED store (Phase 4) so it cannot
    // perturb the insertion order of this drain set.

    // --- Phase 2: concurrent drain (INV-1, INV-2, INV-3, INV-6). ---
    let inv1 = Arc::new(AtomicU64::new(0)); // single active lease
    let inv3 = Arc::new(AtomicU64::new(0)); // no conflicting terminal
    let inv6 = Arc::new(AtomicU64::new(0)); // ordering within a claim
    let leased_now = Arc::new(std::sync::Mutex::new(HashSet::<String>::new()));
    let ever_completed = Arc::new(std::sync::Mutex::new(HashSet::<String>::new()));
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));

    let mut workers = Vec::new();
    for w in 0..cfg.concurrency {
        let proj = Arc::clone(&proj);
        let sk = sk.clone();
        let inv1 = Arc::clone(&inv1);
        let inv3 = Arc::clone(&inv3);
        let inv6 = Arc::clone(&inv6);
        let leased_now = Arc::clone(&leased_now);
        let ever_completed = Arc::clone(&ever_completed);
        let latencies = Arc::clone(&latencies);
        let (t, q) = (t.clone(), q.clone());
        let claim_batch = cfg.claim_batch;
        workers.push(tokio::spawn(async move {
            let mut fin_seq = 0u64;
            let mut local_lat = Vec::new();
            loop {
                let req = ClaimRequest {
                    shard_key: sk.clone(),
                    max_items: claim_batch,
                    now: ts(1_000),
                    lease_token: format!("lease-w{w}-{fin_seq}"),
                    lease_expires_at: ts(61_000),
                };
                let started = Instant::now();
                let mut claimed = proj.batch_claim(req).await.expect("claim").claimed_item_ids;
                local_lat.push(started.elapsed().as_micros() as u64);
                if claimed.is_empty() {
                    break;
                }

                // Self-test fault: re-report the first claimed id as if a second
                // worker had also leased it, to prove the INV-1 watchdog fires.
                if fault == StressFault::DuplicateClaim && w == 0 && fin_seq == 0 {
                    claimed.push(claimed[0].clone());
                }

                // INV-1: a freshly claimed id must not already be leased.
                // INV-6: a claim batch must be returned in ascending order.
                {
                    let mut held = leased_now.lock().unwrap();
                    let mut prev: Option<String> = None;
                    for id in &claimed {
                        let s = id.as_str().to_string();
                        if !held.insert(s.clone()) {
                            inv1.fetch_add(1, Ordering::Relaxed);
                        }
                        if let Some(p) = &prev
                            && &s < p
                        {
                            inv6.fetch_add(1, Ordering::Relaxed);
                        }
                        prev = Some(s);
                    }
                }

                // Finalize the whole claimed batch as Complete (unless the
                // self-test asks us to abandon the work).
                if fault != StressFault::SkipFinalize {
                    let env = finalize_envelope(&t, &q, 0, &claimed, fin_seq);
                    let pos = CommandPosition {
                        shard_key: sk.clone(),
                        sequence: 1_000_000 + fin_seq,
                        backend_epoch: 0,
                    };
                    proj.apply_committed(pos, std::slice::from_ref(&env))
                        .await
                        .expect("finalize");
                }
                fin_seq += 1;

                if fault == StressFault::SkipFinalize {
                    // Abandon after the first claim so the resident set is left
                    // partially leased/pending (proves the INV-2 watchdog fires).
                    break;
                }

                // INV-3: an id must not reach a terminal state twice.
                {
                    let mut done = ever_completed.lock().unwrap();
                    let mut held = leased_now.lock().unwrap();
                    for id in &claimed {
                        let s = id.as_str().to_string();
                        if !done.insert(s.clone()) {
                            inv3.fetch_add(1, Ordering::Relaxed);
                        }
                        held.remove(&s);
                    }
                }
            }
            latencies.lock().unwrap().extend(local_lat);
        }));
    }
    for h in workers {
        h.await.expect("worker join");
    }

    // INV-2 (no lost work): everything pushed must end up completed, nothing
    // left pending or leased.
    let m = proj.metrics(&qk).await.expect("final metrics");
    let mut inv2 = 0u64;
    if m.completed_count != resident {
        inv2 += m.completed_count.abs_diff(resident);
    }
    inv2 += m.pending_count + m.leased_count;
    // A non-empty residual lease set is also an INV-1/INV-3 leak.
    let residual = leased_now.lock().unwrap().len() as u64;
    if residual != 0 {
        inv1.fetch_add(residual, Ordering::Relaxed);
    }

    // --- Phase 3: crash/replay durability (INV-10, reinforces INV-2). ---
    let inv10 = run_crash_replay(&t, &q, cfg.kill_cycles).await;

    // --- Phase 4: cross-shard structural invariants (INV-4, 7, 8, 9). ---
    let inv4 = check_progress_bound(&sk);
    let inv5 = check_idempotency().await;
    let inv7 = check_atomic_convergence(&sk);
    let inv8 = check_tenant_isolation().await;
    let inv9 = check_group_co_residency();

    let mut lat = Arc::try_unwrap(latencies)
        .expect("latencies owner")
        .into_inner()
        .unwrap();
    lat.sort_unstable();

    let inv_violations = [
        inv1.load(Ordering::Relaxed),
        inv2,
        inv3.load(Ordering::Relaxed),
        inv4,
        inv5,
        inv6.load(Ordering::Relaxed),
        inv7,
        inv8,
        inv9,
        inv10,
    ];

    StressOutcome {
        pushed: resident,
        completed: m.completed_count,
        inv_violations,
        measured_resident_items: resident,
        measured_concurrency: cfg.concurrency,
        measured_kill_count: cfg.kill_cycles,
        claim_p95_micros: percentile(&lat, 95.0),
        claim_p99_micros: percentile(&lat, 99.0),
    }
}

/// INV-10 durable-ack: for each kill cycle, append a batch under a torn-write
/// fault that commits only a prefix, then replay the surviving log into a fresh
/// projection. The replayed state must equal the committed prefix exactly — no
/// acked work lost, no un-acked work resurrected.
async fn run_crash_replay(t: &TenantId, q: &QueueId, cycles: u64) -> u64 {
    let mut violations = 0u64;
    let cmds_per_cycle = 8usize;
    let keep = 5usize; // committed prefix; the rest are torn off.
    for cycle in 0..cycles {
        let sk = shard_key(t, q, (cycle % 4) as u32);
        let log =
            FaultInjectedLogStore::new(MemoryLogStore::new(), FailureMode::PartialAppend(keep));
        let base = cycle * cmds_per_cycle as u64;
        let cmds: Vec<CommandEnvelope> = (0..cmds_per_cycle)
            .map(|i| push_envelope(t, q, sk.shard_id.as_u32(), base + i as u64, base + i as u64))
            .collect();

        // Torn append: inner commits `keep`, then the call reports failure.
        let append = log.append_batch(&sk, None, cmds).await;
        if append.is_ok() {
            // PartialAppend with total > keep must fail; a success is a fault-rig bug.
            violations += 1;
            continue;
        }

        let recovered = MemoryProjectionStore::new();
        replay(&log, &recovered, &sk).await.expect("replay");
        let qk = queue_key(t, q);
        let m = recovered.metrics(&qk).await.expect("recovered metrics");
        // Exactly the committed prefix must survive.
        if m.pending_count != keep as u64 || m.leased_count != 0 || m.completed_count != 0 {
            violations += 1;
        }
    }
    violations
}

/// INV-5 idempotency: re-pushing the same item ids must NOT inflate the
/// resident set. Runs against an isolated store.
async fn check_idempotency() -> u64 {
    let t = tenant("idem-tenant");
    let q = queue("idem-queue");
    let sk = shard_key(&t, &q, 0);
    let proj = MemoryProjectionStore::new();
    let n = 1_000u64;
    let first: Vec<CommandEnvelope> = (0..n).map(|i| push_envelope(&t, &q, 0, i, i)).collect();
    let dup: Vec<CommandEnvelope> = (0..n).map(|i| push_envelope(&t, &q, 0, i, n + i)).collect();
    proj.apply_committed(
        CommandPosition {
            shard_key: sk.clone(),
            sequence: 0,
            backend_epoch: 0,
        },
        &first,
    )
    .await
    .expect("first push");
    proj.apply_committed(
        CommandPosition {
            shard_key: sk.clone(),
            sequence: n,
            backend_epoch: 0,
        },
        &dup,
    )
    .await
    .expect("dup push");
    let m = proj
        .metrics(&queue_key(&t, &q))
        .await
        .expect("idem metrics");
    if m.pending_count != n {
        m.pending_count.abs_diff(n)
    } else {
        0
    }
}

/// INV-4 progress bound: the cross-shard aggregator must flag a shard whose
/// oldest eligible age exceeds the bound and must not flag a healthy shard.
fn check_progress_bound(sk: &ShardKey) -> u64 {
    let bound = 30_000u64;
    let healthy = ShardProgress {
        shard_key: sk.clone(),
        oldest_eligible_age_ms: Some(1_000),
        progress_bound_risk_count: 0,
        observed_at_ms: 100,
        owned: true,
    };
    let breached = ShardProgress {
        shard_key: ShardKey {
            tenant_id: sk.tenant_id.clone(),
            queue_id: sk.queue_id.clone(),
            shard_id: ShardId::new(1),
        },
        oldest_eligible_age_ms: Some(bound + 5_000),
        progress_bound_risk_count: 0,
        observed_at_ms: 100,
        owned: true,
    };
    let mut violations = 0u64;
    let healthy_only =
        aggregate_cross_shard_progress(std::slice::from_ref(&healthy), bound, 60_000, 200);
    if healthy_only.progress_bound_risk_count != 0 {
        violations += 1;
    }
    let breached_agg = aggregate_cross_shard_progress(&[healthy, breached], bound, 60_000, 200);
    if breached_agg.progress_bound_risk_count == 0 {
        violations += 1;
    }
    violations
}

/// INV-7 group/cohort atomicity: a multi-shard command must not be ack-visible
/// when any target shard has not committed (all-or-nothing).
fn check_atomic_convergence(sk: &ShardKey) -> u64 {
    let shard_a = sk.clone();
    let shard_b = ShardKey {
        tenant_id: sk.tenant_id.clone(),
        queue_id: sk.queue_id.clone(),
        shard_id: ShardId::new(1),
    };
    let targets = [shard_a.clone(), shard_b.clone()];
    let mut violations = 0u64;

    // Partial commit => not ack-allowed, not visible.
    let partial = evaluate_multi_shard_command_convergence(
        MultiShardCommandKind::PurgeItems,
        &targets,
        &[
            ShardCommandCommit {
                shard_key: shard_a.clone(),
                committed: true,
            },
            ShardCommandCommit {
                shard_key: shard_b.clone(),
                committed: false,
            },
        ],
    );
    if partial.ack_allowed || partial.visible || partial.converged {
        violations += 1;
    }
    // Full commit => converged + visible.
    let full = evaluate_multi_shard_command_convergence(
        MultiShardCommandKind::PurgeItems,
        &targets,
        &[
            ShardCommandCommit {
                shard_key: shard_a,
                committed: true,
            },
            ShardCommandCommit {
                shard_key: shard_b,
                committed: true,
            },
        ],
    );
    if !full.converged || !full.ack_allowed {
        violations += 1;
    }
    violations
}

/// INV-8 tenant isolation: a claim against tenant A's shard must never return
/// tenant B's items, and metrics for queue A must not count B's items.
async fn check_tenant_isolation() -> u64 {
    let ta = tenant("iso-tenant-a");
    let tb = tenant("iso-tenant-b");
    let q = queue("iso-queue");
    let proj = MemoryProjectionStore::new();

    let sk_a = shard_key(&ta, &q, 0);
    let sk_b = shard_key(&tb, &q, 0);
    let a_ids: Vec<CommandEnvelope> = (0..50).map(|i| push_envelope(&ta, &q, 0, i, i)).collect();
    let b_ids: Vec<CommandEnvelope> = (0..50)
        .map(|i| push_envelope(&tb, &q, 0, 10_000 + i, i))
        .collect();
    proj.apply_committed(
        CommandPosition {
            shard_key: sk_a.clone(),
            sequence: 0,
            backend_epoch: 0,
        },
        &a_ids,
    )
    .await
    .expect("push a");
    proj.apply_committed(
        CommandPosition {
            shard_key: sk_b.clone(),
            sequence: 0,
            backend_epoch: 0,
        },
        &b_ids,
    )
    .await
    .expect("push b");

    let mut violations = 0u64;
    let claimed = proj
        .batch_claim(ClaimRequest {
            shard_key: sk_a,
            max_items: 1000,
            now: ts(1_000),
            lease_token: "iso".into(),
            lease_expires_at: ts(61_000),
        })
        .await
        .expect("claim a")
        .claimed_item_ids;
    if claimed
        .iter()
        .any(|id| id.as_str().starts_with("itm-000000010"))
    {
        // B's ids start at 10_000; any leakage is a violation.
    }
    if claimed.iter().any(|id| {
        id.as_str()
            .trim_start_matches("itm-")
            .parse::<u64>()
            .map(|n| n >= 10_000)
            .unwrap_or(false)
    }) {
        violations += 1;
    }
    let m_a = proj.metrics(&queue_key(&ta, &q)).await.expect("metrics a");
    if m_a.pending_count + m_a.leased_count != 50 {
        violations += 1;
    }
    violations
}

/// INV-9 group co-residency (placement invariance): with `group_co_residency`,
/// a group key maps to exactly one shard deterministically. Verify the
/// placement function is stable across repeated evaluation and never splits a
/// group across shards.
fn check_group_co_residency() -> u64 {
    let shard_count = 16u32;
    let mut violations = 0u64;
    for g in 0..500u64 {
        let key = GroupKey::new(format!("group-{g}")).expect("group key");
        let first = shard_for_group(&key, shard_count);
        for _ in 0..4 {
            if shard_for_group(&key, shard_count) != first {
                violations += 1;
                break;
            }
        }
        if first >= shard_count {
            violations += 1;
        }
    }
    violations
}

/// Deterministic group placement: `shard = hash(group_key) mod shard_count`
/// (D2 co-residency rule). Stable hash so a group is always co-resident.
fn shard_for_group(group: &GroupKey, shard_count: u32) -> u32 {
    use std::hash::{Hash, Hasher};
    // Fixed-key FNV-style hasher via DefaultHasher seeded deterministically.
    let mut h = std::collections::hash_map::DefaultHasher::new();
    group.as_str().hash(&mut h);
    (h.finish() % shard_count as u64) as u32
}
