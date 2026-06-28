//! TP-003 **product validation** (AC-E2E-*) — the P0/core product workflows driven through the current
//! library facade ([`pqueue::Pqueue`]) over the in-memory backend, at SMOKE scale. This rebuilds the
//! `product_validation_tests` suite that lived in the removed `pqueue-service` crate.
//!
//! Each workflow exercises the real acceptance-criterion invariants (not trivial asserts) and emits a
//! SMOKE-tier verification-ledger row (recorded + gate-visible, but never satisfies a release gate — the
//! release shape is the heavier provisioned run). Workflows are added one per BQ-43d.N sub-bead.
//!
//! Implemented here:
//!   - AC-E2E-9 downstream-pacing non-goal (`downstream_pacing_non_goal_e2e`).
//!   - AC-E2E-8 generic priority + bounded-relaxed (`generic_priority_bounded_relaxed_e2e`).
//!   - AC-E2E-1 scheduled-action delivery (`scheduled_action_delivery_e2e`).
//!   - AC-E2E-4 jobs/connectors recurring singleton (`jobs_connectors_recurring_e2e`).
//!   - AC-E2E-2 Marketo group-cardinality batching (`marketo_group_batching_e2e`).
//!   - AC-E2E-3 callback cohort execution (`callback_cohort_e2e`).
//!   - AC-E2E-6 noisy-neighbor + active-scope routing (`noisy_neighbor_scale_e2e`).
//!   - AC-E2E-5 worker crash recovery (`worker_crash_recovery_e2e`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use pqueue::{
    ClaimCompatibility, ClientItemKey, EngineError, GroupBatching, LibBackend, Nack, NewItem,
    Pqueue, UpsertOutcome,
};
use pqueue_core::{
    CohortPolicy, EligibilityPolicy, GroupKey, ItemId, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_engine::QueueKey;
use pqueue_memory::{ManualClock, MemoryBackend};
use pqueue_objectlog::ObjectLogBackend;
use pqueue_sqlite::SqliteBackend;

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

/// A fresh in-memory single-node deployment + a manual clock (so a workflow can advance wall-clock time
/// deterministically). Returns the handle and the clock.
fn deployment() -> (Pqueue<MemoryBackend>, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), clock.clone());
    (pq, clock)
}

/// A large-capacity queue definition (so a workflow can push/claim realistic batch sizes).
fn qdef(
    tenant: &str,
    queue: &str,
    direction: PriorityDirection,
    ordering: OrderingMode,
) -> QueueDefinition {
    qdef_attempts(tenant, queue, direction, ordering, 1_000_000)
}

/// Like [`qdef`] but with an explicit `max_attempts` (for retry-exhaustion workflows).
fn qdef_attempts(
    tenant: &str,
    queue: &str,
    direction: PriorityDirection,
    ordering: OrderingMode,
    max_attempts: u32,
) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: ordering,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 600_000,
        client_item_key_retention_ms: 600_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy { max_attempts },
        max_push_batch_size: 1_000_000,
        max_claim_batch_size: 1_000_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
    }
}

fn qk(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn unique_temp_path(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "pqueue-product-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ))
}

/// Emit a SMOKE-tier AC-E2E ledger row from real measured/observed values, and assert it round-trips strict
/// validation under its acceptance id. (Structure check; the workflow's own asserts verify the behavior.)
fn emit_ac(
    ac_id: &str,
    inv_ids: &[&str],
    pass_bar: &str,
    values: BTreeMap<String, serde_json::Value>,
) {
    emit_ac_with_context(
        ac_id,
        inv_ids,
        pass_bar,
        "memory",
        "in-process lib facade (Pqueue + MemoryBackend); release shape is the provisioned run",
        values,
    );
}

fn emit_ac_with_context(
    ac_id: &str,
    inv_ids: &[&str],
    pass_bar: &str,
    backend_profile: &str,
    environment: &str,
    values: BTreeMap<String, serde_json::Value>,
) {
    let suite = format!(
        "product_validation_tests_{}",
        ac_id.to_lowercase().replace('-', "_")
    );
    let row = pqueue_release::LedgerRow {
        suite: "product_validation_tests".into(),
        command: "cargo test -p pqueue --test product_validation_tests".into(),
        backend_profile: backend_profile.into(),
        scale: "smoke".into(),
        seed: 0,
        environment: environment.into(),
        exit_status: 0,
        ac_ids: vec![ac_id.into()],
        inv_ids: inv_ids.iter().map(|s| s.to_string()).collect(),
        pass_bar: pass_bar.into(),
        evidence_tier: "smoke".into(),
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec![],
            values,
        },
    };
    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), &suite);
    let _ = std::fs::remove_file(&path);
    pqueue_release::append_row(&path, &row).expect("emit AC ledger row");
    let summary =
        pqueue_release::verify_ledger(&path, true).expect("emitted AC row validates strict");
    // ac_ids make the row traceable even with no tp002 evidence id.
    assert_eq!(summary.rows, 1, "one AC row emitted");
}

// ---------------------------------------------------------------------------
// AC-E2E-9 — downstream pacing is a NON-GOAL
// ---------------------------------------------------------------------------

/// AC-E2E-9 (TP-003): prove pqueue does NOT enforce downstream API rate/quota admission. Load many eligible
/// items for one compatibility group, claim with caller-selected `max_items` and deliberate pauses, and
/// compare results to eligibility/`max_items` ONLY — pqueue returns up to `max_items` subject only to normal
/// eligibility/active-leases/batch-limits, a short/empty batch is valid, and it never withholds otherwise-
/// eligible work for a downstream-rate reason. (FR-45, Non-Goals.)
#[tokio::test]
async fn downstream_pacing_non_goal_e2e() {
    let (pq, clock) = deployment();
    let q = qk("paced", "calls");
    pq.create_queue(qdef(
        "paced",
        "calls",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();

    // Load many eligible items. They all carry the same `group_key` (modeling one downstream API target),
    // though item-level claim pools by eligibility, not by group — the grouping is incidental to AC-E2E-9.
    let total = 500u64;
    let group = GroupKey::new("downstream-api").unwrap();
    let items: Vec<NewItem> = (0..total)
        .map(|i| NewItem {
            priority: Some(PriorityValue::Int64(i as i64)),
            group_key: Some(group.clone()),
            payload: Some(Bytes::from(format!("call-{i}").into_bytes())),
            ..Default::default()
        })
        .collect();
    pq.push_batch(&q, items).await.unwrap();
    // NON-GOAL proof part 1 — there is no "rate-deferred"/"admission" parking state: every accepted item is
    // immediately eligible (pending). The metrics surface is purely lifecycle {pending,leased,complete,failed}
    // — it exposes NO downstream-rate/admission state for an item to hide in.
    let m0 = pq.metrics(&q).await.unwrap();
    assert_eq!(
        (m0.pending, m0.leased, m0.complete, m0.failed),
        (total, 0, 0, 0),
        "all accepted items are immediately eligible — no rate/admission parking state"
    );

    // Claim with a SEQUENCE of caller-selected max_items, advancing the wall clock between calls. The engine
    // has NO time-based admission seam (claim consults `now` only for not_before — None here — and lease
    // expiry), so advancing the clock provably changes NOTHING about what claim returns: each claim returns
    // EXACTLY min(max_items, remaining eligible) — capped only by the caller's max, never withheld for a
    // downstream-rate reason. Strict ascending priority means the k-th claimed item has priority == its order.
    let mut remaining = total as i64;
    let mut claimed_total = 0u64;
    let mut next_priority = 0i64; // strict ascending → claimed in priority order 0,1,2,...
    let mut batches = 0u64;
    let mut max_batch_seen = 0usize;
    let mut pause_s = 1i64;
    for &max in &[1usize, 25, 100, 100, 100, 100, 100, 100] {
        clock.set(pause_s); // wall-clock advances between calls; no rate path consults it
        pause_s += 5;
        let got = pq.claim(&q, max, 3_600_000).await.unwrap();
        let expected = (max as i64).min(remaining).max(0) as usize;
        assert_eq!(
            got.len(),
            expected,
            "claim(max={max}) must return min(max, remaining={remaining}) — never withheld for a downstream-rate reason, even after a wall-clock pause"
        );
        // Opaque payload round-trips byte-for-byte (delivered whole, not transformed by any pacing logic).
        for c in &got {
            let pri = match c.priority {
                Some(PriorityValue::Int64(n)) => n,
                _ => panic!("int64 priority"),
            };
            assert_eq!(pri, next_priority, "strict ascending claim order");
            assert_eq!(
                c.payload.as_deref(),
                Some(format!("call-{pri}").as_bytes()),
                "opaque payload delivered intact"
            );
            next_priority += 1;
        }
        max_batch_seen = max_batch_seen.max(got.len());
        pq.ack(&q, got.iter().map(|c| c.item_id)).await.unwrap();
        claimed_total += got.len() as u64;
        remaining -= got.len() as i64;
        batches += 1;
    }
    // Drain whatever remains so we can prove the totals.
    while pq.metrics(&q).await.unwrap().pending > 0 {
        let got = pq.claim(&q, 100, 3_600_000).await.unwrap();
        pq.ack(&q, got.iter().map(|c| c.item_id)).await.unwrap();
        claimed_total += got.len() as u64;
        batches += 1;
    }

    // A claim on a now-empty queue is a VALID empty batch (no error, no downstream-rate "throttled" state).
    let empty = pq.claim(&q, 100, 3_600_000).await.unwrap();
    assert!(
        empty.is_empty(),
        "claim on a drained queue is a valid empty batch"
    );

    // NON-GOAL proof part 2 — full lifecycle accounting: every accepted item ended terminal-complete, with
    // none failed and none left withheld/parked. claimed_total == total proves nothing was withheld.
    assert_eq!(
        claimed_total, total,
        "all eligible work was claimable; none withheld for a downstream-rate reason"
    );
    let m = pq.metrics(&q).await.unwrap();
    assert_eq!(
        (m.complete, m.pending, m.leased, m.failed),
        (total, 0, 0, 0),
        "every item accounted as complete — no rate-withheld/admission residue"
    );
    // The largest batch returned was capped by the caller's max_items (100), never by a downstream rate.
    assert!(
        max_batch_seen <= 100,
        "no claim exceeded the caller's max_items"
    );

    emit_ac(
        "AC-E2E-9",
        &[],
        "BatchClaim returns min(max_items, eligible) even across wall-clock pauses; short/empty valid; metrics surface has no rate/admission state; full lifecycle accounting, none withheld",
        BTreeMap::from([
            ("items".into(), serde_json::json!(total)),
            ("claimed_total".into(), serde_json::json!(claimed_total)),
            ("claim_batches".into(), serde_json::json!(batches)),
            (
                "max_batch_returned".into(),
                serde_json::json!(max_batch_seen),
            ),
        ]),
    );
}

// ---------------------------------------------------------------------------
// AC-E2E-8 — generic priority + bounded-relaxed (pqueue is not timestamp-/Seventh-Sense-only)
// ---------------------------------------------------------------------------

/// Drain `q` fully in `batch`-sized claims (ack each), returning the claimed priorities in delivery order.
async fn drain_priorities(pq: &Pqueue<MemoryBackend>, q: &QueueKey, batch: usize) -> Vec<i64> {
    let mut order = Vec::new();
    loop {
        let got = pq.claim(q, batch, 3_600_000).await.unwrap();
        if got.is_empty() {
            break;
        }
        for c in &got {
            // Opaque payload + metadata round-trip byte-for-byte (no Seventh Sense shape is consulted).
            let pri = match c.priority {
                Some(PriorityValue::Int64(n)) => n,
                _ => panic!("int64 priority"),
            };
            assert_eq!(
                c.payload.as_deref(),
                Some(format!("payload@{pri}").as_bytes()),
                "opaque payload delivered intact"
            );
            assert_eq!(
                c.fields.get("opaque").map(|b| b.as_ref()),
                Some(b"meta".as_ref()),
                "opaque metadata field round-trips"
            );
            order.push(pri);
        }
        pq.ack(q, got.iter().map(|c| c.item_id)).await.unwrap();
    }
    order
}

/// Build skewed-priority generic items (heavy low, few high) with opaque payload + metadata, NO timestamp /
/// not_before / Seventh Sense field. `n` items over priorities derived from a deterministic skew.
fn skewed_items(n: u64) -> Vec<NewItem> {
    (0..n)
        .map(|i| {
            // Deterministic skew: mostly 10, some 50, few 90 → ties exercise the CreatedSequence tie-break.
            let pri = match i % 10 {
                0..=6 => 10,
                7..=8 => 50,
                _ => 90,
            };
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("opaque".to_string(), Bytes::from_static(b"meta"));
            NewItem {
                priority: Some(PriorityValue::Int64(pri)),
                payload: Some(Bytes::from(format!("payload@{pri}").into_bytes())),
                fields,
                ..Default::default()
            }
        })
        .collect()
}

/// AC-E2E-8 (TP-003): prove pqueue is NOT timestamp-only or Seventh-Sense-only. (a) A strict `int64`
/// DESCENDING queue delivers in strict priority order with 0 inversions; (b) a bounded-relaxed queue is
/// accepted and makes progress (INV-4) with opaque payload/metadata round-tripping — using only generic
/// int64 priorities + opaque bytes, no Seventh Sense metadata shape. (FR-1,2,4,5-9,12-16,18-21, Non-Goals.)
#[tokio::test]
async fn generic_priority_bounded_relaxed_e2e() {
    let (pq, _clock) = deployment();
    let n = 300u64;

    // ----- (a) STRICT int64 DESCENDING: 0 inversions vs the spec ordering tuple -----
    let strict = qk("generic", "strict-desc");
    pq.create_queue(qdef(
        "generic",
        "strict-desc",
        PriorityDirection::Descending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    pq.push_batch(&strict, skewed_items(n)).await.unwrap();
    let strict_order = drain_priorities(&pq, &strict, 32).await;
    assert_eq!(strict_order.len() as u64, n, "all strict items delivered");
    // 0 inversions: descending strict ⇒ priorities are NON-INCREASING across the whole delivery order.
    let strict_inversions = strict_order.windows(2).filter(|w| w[0] < w[1]).count();
    assert_eq!(
        strict_inversions, 0,
        "strict int64-descending claim order must have 0 inversions vs the priority tuple: {strict_order:?}"
    );
    // And it genuinely reordered the skewed input (90s before 10s), not a passthrough of push order.
    assert_eq!(
        strict_order.first(),
        Some(&90),
        "highest priority delivered first"
    );
    assert_eq!(
        strict_order.last(),
        Some(&10),
        "lowest priority delivered last"
    );

    // ----- (b) BOUNDED-RELAXED: accepted + progress (INV-4); opaque round-trip -----
    // HONEST SCOPE: OrderingMode::BoundedRelaxed is accepted but currently selects identically to Strict —
    // the projection ignores ordering_mode and there is no rank-error-bound config. So the observed rank
    // error is 0 (trivially within any bound). The genuine bounded-relaxed proof (a NON-ZERO rank error
    // within a declared bound + relaxed selection) is DEFERRED to pqueue-b725d3ee.
    let relaxed = qk("generic", "bounded-relaxed");
    pq.create_queue(qdef(
        "generic",
        "bounded-relaxed",
        PriorityDirection::Ascending,
        OrderingMode::BoundedRelaxed,
    ))
    .await
    .expect("a bounded-relaxed queue is accepted");
    pq.push_batch(&relaxed, skewed_items(n)).await.unwrap();
    let relaxed_order = drain_priorities(&pq, &relaxed, 32).await;
    // INV-4 progress: every eligible item was eventually claimed (the queue fully drained).
    assert_eq!(
        relaxed_order.len() as u64,
        n,
        "INV-4: all bounded-relaxed items make progress"
    );
    // Current behavior == strict ascending ⇒ rank error 0 (within any bound). Measured, not assumed.
    let relaxed_inversions = relaxed_order.windows(2).filter(|w| w[0] > w[1]).count();
    assert_eq!(
        relaxed_inversions, 0,
        "bounded-relaxed currently selects strict-ascending (rank error 0; relaxed selection deferred to pqueue-b725d3ee)"
    );

    emit_ac(
        "AC-E2E-8",
        // INV-6 here substantiates ONLY its STRICT clause (0 inversions); INV-6's bounded-relaxed
        // rank-error-bound clause is unimplemented + deferred (see the measurements). INV-4 is a full-drain
        // progress proxy at smoke scale.
        &["INV-6", "INV-4"],
        "strict int64-descending claim order has 0 inversions; opaque payload/metadata round-trips; bounded-relaxed accepted + makes progress (INV-4) [non-zero rank-error-within-bound deferred to pqueue-b725d3ee]; no Seventh Sense field required",
        BTreeMap::from([
            ("items_per_queue".into(), serde_json::json!(n)),
            (
                "strict_inversions".into(),
                serde_json::json!(strict_inversions),
            ),
            (
                "inv6_strict_clause".into(),
                serde_json::json!("met (0 inversions)"),
            ),
            (
                "inv6_bounded_relaxed_clause".into(),
                serde_json::json!("DEFERRED (rank-error bound unimplemented) -> pqueue-b725d3ee"),
            ),
            (
                "relaxed_progress_inversions".into(),
                serde_json::json!(relaxed_inversions),
            ),
            (
                "bounded_relaxed_selection".into(),
                serde_json::json!(
                    "strict-equivalent (relaxed selection unimplemented) -> pqueue-b725d3ee"
                ),
            ),
            ("seventh_sense_fields_used".into(), serde_json::json!(0)),
        ]),
    );
}

// ---------------------------------------------------------------------------
// AC-E2E-1 — scheduled action delivery
// ---------------------------------------------------------------------------

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

/// AC-E2E-1 (TP-003): model `scheduled_actions` — a timestamp-ascending queue where items are pushed EARLY
/// with a future send time (`not_before`), become eligible exactly when the clock reaches that time, are
/// delivered in schedule order, and finalize through each API-003 outcome mapping; with cross-tenant
/// isolation and metrics matching the terminal state. (FR-1..3, FR-7, FR-18..28, FR-40..46.)
///
/// COVERED via the lib facade: not_before scheduling + eligibility gating by the clock; strict
/// timestamp-ascending delivery order (INV: schedule order == timestamp); stable client keys; caller
/// max_items/cadence pacing; complete/fail/retry/release/rearm finalize mappings; progress to terminal
/// (INV-4); tenant NAMESPACING (same queue_id under two tenants are independent queues with no cross-tenant
/// leakage); metrics match the terminal state.
/// DEFERRED (tracked on pqueue-7a96f929 — facade lacks the seam): BatchUpdate reschedule (change
/// priority/not_before after push), SetGates close+reopen gating (no gated item claimed while blocked),
/// and claim-by-group_key. Cross-tenant AUTHZ denial lives in the auth layer (ADR-002), not this trusted
/// library facade. NOT claimed in the row.
#[tokio::test]
async fn scheduled_action_delivery_e2e() {
    let (pq, clock) = deployment();
    let memory = scheduled_batch_delivery_profile(&pq, clock.clone(), "sched-mem").await;
    let memory_idempotent = assert_keyed_upsert_converges(&pq, "sched-mem-idempotent").await;

    let sqlite_path = unique_temp_path("scheduled-sqlite");
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_clock = Arc::new(ManualClock::at(0));
    let sqlite = Pqueue::new(
        Arc::new(SqliteBackend::open(sqlite_path.to_str().unwrap()).expect("open sqlite")),
        sqlite_clock.clone(),
    );
    let sqlite_evidence =
        scheduled_batch_delivery_profile(&sqlite, sqlite_clock, "sched-sqlite").await;
    let sqlite_idempotent = assert_keyed_upsert_converges(&sqlite, "sched-sqlite-idempotent").await;
    let _ = std::fs::remove_file(&sqlite_path);

    let dir = unique_temp_path("scheduled-objectlog");
    let _ = std::fs::remove_dir_all(&dir);
    let object_clock = Arc::new(ManualClock::at(0));
    let objectlog = Pqueue::new(
        Arc::new(ObjectLogBackend::open(&dir).expect("open object log")),
        object_clock.clone(),
    );
    let object = scheduled_batch_delivery_profile(&objectlog, object_clock, "sched-obj").await;
    let objectlog_upsert_unavailable =
        assert_upsert_unavailable(&objectlog, "sched-obj-idempotent").await;
    let _ = std::fs::remove_dir_all(&dir);

    // Tenant NAMESPACING: the SAME queue_id under two different tenants are independent queues with NO
    // cross-tenant leakage. Push a distinct marker into each tenant's same-named queue and prove each claim
    // sees ONLY its own tenant's item (bidirectional). (Cross-tenant AUTHZ denial — a principal of tenant A
    // being refused tenant B's data plane — lives in the auth layer / RESP front per ADR-002, NOT in this
    // trusted library facade; it is not exercised here.)
    let qa = qk("iso-a", "shared");
    let qb = qk("iso-b", "shared");
    pq.create_queue(qdef(
        "iso-a",
        "shared",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    pq.create_queue(qdef(
        "iso-b",
        "shared",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    clock.set(0);
    pq.push(
        &qa,
        NewItem {
            payload: Some(Bytes::from_static(b"tenant-a")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    pq.push(
        &qb,
        NewItem {
            payload: Some(Bytes::from_static(b"tenant-b")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let from_a = pq.claim(&qa, 10, 60_000).await.unwrap();
    let from_b = pq.claim(&qb, 10, 60_000).await.unwrap();
    assert_eq!(
        from_a.len(),
        1,
        "tenant A's queue delivers exactly its own item"
    );
    assert_eq!(
        from_b.len(),
        1,
        "tenant B's queue delivers exactly its own item"
    );
    assert_eq!(
        from_a[0].payload.as_deref(),
        Some(b"tenant-a".as_ref()),
        "no cross-tenant leakage into A"
    );
    assert_eq!(
        from_b[0].payload.as_deref(),
        Some(b"tenant-b".as_ref()),
        "no cross-tenant leakage into B"
    );

    emit_ac_with_context(
        "AC-E2E-1",
        &["INV-4"],
        "scheduled actions use stable client_item_key, become eligible at not_before, obey caller max_items/cadence pacing, map application results onto complete/fail/retry/release/rearm, preserve the no-rate-admission boundary, remain tenant-namespaced, and reach terminal metrics on memory/sqlite/object-log smoke profiles [DEFERRED -> pqueue-7a96f929: BatchUpdate-reschedule, SetGates-gating, claim-by-group_key; cross-tenant AUTHZ denial is the auth layer]",
        "memory+sqlite+object_log_sqlite_projection",
        "in-process lib facade over MemoryBackend, SqliteBackend, and ObjectLogBackend; release shape is the provisioned run",
        BTreeMap::from([
            (
                "scheduled_actions".into(),
                serde_json::json!(memory.scheduled_actions),
            ),
            (
                "delivered_in_schedule_order".into(),
                serde_json::json!(
                    memory.delivered_in_schedule_order
                        && sqlite_evidence.delivered_in_schedule_order
                        && object.delivered_in_schedule_order
                ),
            ),
            (
                "unique_deliveries".into(),
                serde_json::json!(memory.unique_deliveries),
            ),
            (
                "finalize_outcomes".into(),
                serde_json::json!(["complete", "fail", "retry", "release", "rearm"]),
            ),
            (
                "max_items_pacing_observed".into(),
                serde_json::json!(
                    memory.max_items_pacing_observed
                        && sqlite_evidence.max_items_pacing_observed
                        && object.max_items_pacing_observed
                ),
            ),
            (
                "stable_client_keys_observed".into(),
                serde_json::json!(
                    memory.stable_client_keys_observed
                        && sqlite_evidence.stable_client_keys_observed
                        && object.stable_client_keys_observed
                ),
            ),
            (
                "idempotent_client_key_convergence_profiles".into(),
                serde_json::json!({
                    "memory": memory_idempotent,
                    "sqlite": sqlite_idempotent,
                    "object_log_upsert_unavailable": objectlog_upsert_unavailable
                }),
            ),
            (
                "backend_profiles".into(),
                serde_json::json!(["memory", "sqlite", "object_log_sqlite_projection"]),
            ),
            (
                "deferred_subparts".into(),
                serde_json::json!(
                    "BatchUpdate-reschedule, SetGates-gating, claim-by-group_key -> pqueue-7a96f929"
                ),
            ),
        ]),
    );
}

struct ScheduledProfileEvidence {
    scheduled_actions: usize,
    delivered_in_schedule_order: bool,
    unique_deliveries: usize,
    max_items_pacing_observed: bool,
    stable_client_keys_observed: bool,
}

async fn scheduled_batch_delivery_profile<B: LibBackend>(
    pq: &Pqueue<B>,
    clock: Arc<ManualClock>,
    tenant: &str,
) -> ScheduledProfileEvidence {
    let q = qk(tenant, "campaign");
    pq.create_queue(qdef(
        tenant,
        "campaign",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();

    let actions = [
        ("complete", 10i64),
        ("fail", 20),
        ("retry", 30),
        ("release", 40),
        ("rearm", 50),
    ];
    for &(outcome, due) in &actions {
        let key = ClientItemKey::new(format!("{tenant}-{outcome}")).unwrap();
        let item = NewItem {
            client_item_key: Some(key),
            priority: Some(PriorityValue::Int64(due)),
            not_before: Some(ts(due)),
            payload: Some(Bytes::from(outcome.as_bytes().to_vec())),
            ..Default::default()
        };
        pq.push(&q, item).await.unwrap();
    }
    assert!(
        pq.claim(&q, 10, 60_000).await.unwrap().is_empty(),
        "not_before prevents early delivery"
    );

    clock.set(100);
    let mut delivered_order = Vec::new();
    let mut delivered_ids = Vec::new();
    let mut max_items_pacing_observed = true;
    let mut stable_client_keys_observed = true;

    let complete = claim_one(&pq, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&complete), "complete");
    assert_eq!(
        complete.client_item_key.as_str(),
        format!("{tenant}-complete")
    );
    pq.ack(&q, [complete.item_id]).await.unwrap();

    let failed = claim_one(&pq, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&failed), "fail");
    stable_client_keys_observed &= failed.client_item_key.as_str() == format!("{tenant}-fail");
    pq.fail(&q, [failed.item_id]).await.unwrap();

    let retry = claim_one(&pq, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&retry), "retry");
    stable_client_keys_observed &= retry.client_item_key.as_str() == format!("{tenant}-retry");
    pq.nack(
        &q,
        [retry.item_id],
        Nack::Retry {
            not_before: Some(ts(130)),
        },
    )
    .await
    .unwrap();

    let release = claim_one(&pq, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&release), "release");
    stable_client_keys_observed &= release.client_item_key.as_str() == format!("{tenant}-release");
    pq.nack(&q, [release.item_id], Nack::Release).await.unwrap();
    let release_again = pq.claim(&q, 1, 60_000).await.unwrap();
    max_items_pacing_observed &= release_again.len() == 1;
    assert_eq!(release_again[0].item_id, release.item_id);
    pq.ack(&q, [release_again[0].item_id]).await.unwrap();

    let rearm = claim_one(&pq, &q, &mut delivered_order, &mut delivered_ids).await;
    assert_eq!(payload_label(&rearm), "rearm");
    stable_client_keys_observed &= rearm.client_item_key.as_str() == format!("{tenant}-rearm");
    pq.rearm(&q, [rearm.item_id]).await.unwrap();
    let rearm_again = pq.claim(&q, 1, 60_000).await.unwrap();
    max_items_pacing_observed &= rearm_again.len() == 1;
    assert_eq!(rearm_again[0].item_id, rearm.item_id);
    pq.ack(&q, [rearm_again[0].item_id]).await.unwrap();

    clock.set(120);
    assert!(
        pq.claim(&q, 1, 60_000).await.unwrap().is_empty(),
        "retry backoff is caller-chosen not_before, not pqueue rate admission"
    );
    clock.set(130);
    let retry_again = pq.claim(&q, 1, 60_000).await.unwrap();
    max_items_pacing_observed &= retry_again.len() == 1;
    assert_eq!(retry_again[0].item_id, retry.item_id);
    pq.ack(&q, [retry_again[0].item_id]).await.unwrap();

    let m = pq.metrics(&q).await.unwrap();
    assert_eq!(
        (m.complete, m.failed, m.pending, m.leased),
        (4, 1, 0, 0),
        "all scheduled actions reached terminal state after the five outcome mappings"
    );

    delivered_ids.sort();
    delivered_ids.dedup();
    ScheduledProfileEvidence {
        scheduled_actions: actions.len(),
        delivered_in_schedule_order: delivered_order == [10, 20, 30, 40, 50],
        unique_deliveries: delivered_ids.len(),
        max_items_pacing_observed,
        stable_client_keys_observed,
    }
}

async fn claim_one<B: LibBackend>(
    pq: &Pqueue<B>,
    q: &QueueKey,
    order: &mut Vec<i64>,
    ids: &mut Vec<ItemId>,
) -> pqueue::ClaimedItem {
    let got = pq.claim(q, 1, 60_000).await.unwrap();
    assert_eq!(
        got.len(),
        1,
        "caller-selected max_items=1 paces delivery; pqueue returns the one eligible item instead of applying downstream admission"
    );
    let item = got.into_iter().next().unwrap();
    if let Some(PriorityValue::Int64(n)) = item.priority {
        order.push(n);
    }
    ids.push(item.item_id);
    item
}

fn payload_label(item: &pqueue::ClaimedItem) -> String {
    String::from_utf8(item.payload.clone().expect("payload").to_vec()).expect("utf8 payload")
}

async fn assert_keyed_upsert_converges<B: LibBackend>(pq: &Pqueue<B>, tenant: &str) -> bool {
    let q = qk(tenant, "campaign");
    pq.create_queue(qdef(
        tenant,
        "campaign",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();

    let key = ClientItemKey::new(format!("{tenant}-stable")).unwrap();
    let first = pq
        .upsert(
            &q,
            key.clone(),
            NewItem {
                priority: Some(PriorityValue::Int64(10)),
                payload: Some(Bytes::from_static(b"first")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let first_id = match first {
        UpsertOutcome::Inserted { item_id } => item_id,
        UpsertOutcome::Replaced { .. } => panic!("first upsert inserts"),
    };
    let second = pq
        .upsert(
            &q,
            key.clone(),
            NewItem {
                priority: Some(PriorityValue::Int64(20)),
                payload: Some(Bytes::from_static(b"second")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let second_id = match second {
        UpsertOutcome::Replaced {
            new_item_id,
            superseded_item_id,
        } => {
            assert_eq!(superseded_item_id, first_id);
            new_item_id
        }
        UpsertOutcome::Inserted { .. } => panic!("second upsert replaces the pending item"),
    };

    let got = pq.claim(&q, 10, 60_000).await.unwrap();
    assert_eq!(
        got.len(),
        1,
        "stable client_item_key duplicate submission converges to one live item"
    );
    assert_eq!(got[0].item_id, second_id);
    assert_eq!(got[0].client_item_key, key);
    assert_eq!(got[0].payload.as_deref(), Some(b"second".as_ref()));
    true
}

async fn assert_upsert_unavailable<B: LibBackend>(pq: &Pqueue<B>, tenant: &str) -> bool {
    let q = qk(tenant, "campaign");
    pq.create_queue(qdef(
        tenant,
        "campaign",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();

    let err = pq
        .upsert(
            &q,
            ClientItemKey::new(format!("{tenant}-stable")).unwrap(),
            NewItem {
                priority: Some(PriorityValue::Int64(10)),
                payload: Some(Bytes::from_static(b"object-log-upsert")),
                ..Default::default()
            },
        )
        .await
        .expect_err("object-log profile keeps replace-if-pending unavailable");
    assert_eq!(err, EngineError::Unavailable);
    true
}

// ---------------------------------------------------------------------------
// AC-E2E-4 — jobs/connectors recurring singleton
// ---------------------------------------------------------------------------

/// AC-E2E-4 (TP-003): model `jobs_queue`/`connectors_queue` poll-cursor rows — ONE logical item per
/// job/connector key, repeated claim→work→rearm cycles, per-cycle retry exhaustion, and PurgeItems teardown.
///
/// COVERED via the lib facade (each assertion's counterfactual bites):
///   - recurring SINGLETON: one item cycles via claim→rearm; it is always the SAME id (no duplicate row),
///     `item_version` increases monotonically across re-arms, and it survives MANY cycles (> max_attempts);
///   - rearm does NOT consume the retry budget: each cycle is `attempt_count == 1` (rearm resets the delivery
///     count). COUNTERFACTUAL with the SAME `max_attempts`: a `Retry`-nacked item terminalizes at the bound;
///   - PurgeItems is idempotent (a second purge of the same id is a no-op) and a late finalize after purge
///     returns `not_found`.
/// DEFERRED (tracked on pqueue-8cbae731 — FinalizeRearm sets no new not_before, RecurrencePolicy.until is not
/// enforced): rearm idle-period (new not_before each cycle), recurrence.until terminal cutoff, and the
/// idle-recurring-doesn't-inflate-oldest-eligible-age check. NOT asserted, NOT claimed in the row.
#[tokio::test]
async fn jobs_connectors_recurring_e2e() {
    let (pq, _clock) = deployment();

    // max_attempts = 2 so the retry-exhaustion counterfactual bites in two cycles.
    let rec_q = qk("jobs", "connectors");
    pq.create_queue(qdef_attempts(
        "jobs",
        "connectors",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
        2,
    ))
    .await
    .unwrap();

    // --- recurring singleton: one logical poll-cursor item, repeated claim→rearm cycles ---
    let job = pq
        .push(
            &rec_q,
            NewItem {
                payload: Some(Bytes::from_static(b"poll-cursor")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut versions = Vec::new();
    let mut attempts_per_cycle = Vec::new();
    let cycles = 5;
    for _ in 0..cycles {
        let got = pq.claim(&rec_q, 10, 60_000).await.unwrap();
        assert_eq!(
            got.len(),
            1,
            "exactly one logical item cycles (no duplicate singleton rows)"
        );
        assert_eq!(
            got[0].item_id, job,
            "the SAME item recurs — rearm does not create a new row"
        );
        assert_eq!(
            got[0].attempt_count, 1,
            "rearm reset the delivery count: every cycle is attempt 1 (retry budget not consumed)"
        );
        attempts_per_cycle.push(got[0].attempt_count);
        versions.push(got[0].item_version);
        pq.rearm(&rec_q, [got[0].item_id]).await.unwrap();
    }
    assert!(
        versions.windows(2).all(|w| w[1] > w[0]),
        "item_version increases monotonically across re-arms: {versions:?}"
    );
    // Survived 5 cycles (> max_attempts=2): rearm never exhausted the budget; still exactly one item, pending.
    let m = pq.metrics(&rec_q).await.unwrap();
    assert_eq!(
        (m.pending, m.failed),
        (1, 0),
        "the recurring singleton survives many cycles, never terminal"
    );

    // --- retry COUNTERFACTUAL (same max_attempts=2): nack(Retry) DOES consume the budget → terminal ---
    let retry_q = qk("jobs", "retrying");
    pq.create_queue(qdef_attempts(
        "jobs",
        "retrying",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
        2,
    ))
    .await
    .unwrap();
    let _ = pq.push(&retry_q, NewItem::default()).await.unwrap();
    for _ in 0..2 {
        let got = pq.claim(&retry_q, 1, 60_000).await.unwrap();
        assert_eq!(got.len(), 1);
        pq.nack(
            &retry_q,
            got.iter().map(|c| c.item_id),
            Nack::Retry { not_before: None },
        )
        .await
        .unwrap();
    }
    let retry_terminal = pq.metrics(&retry_q).await.unwrap();
    assert_eq!(
        (retry_terminal.failed, retry_terminal.pending),
        (1, 0),
        "a Retry-nacked item terminalizes at max_attempts=2 — proving the recurring item's survival was rearm RESETTING the budget, not an absent bound"
    );

    // --- PurgeItems teardown: idempotent + late finalize → not_found ---
    let purge_q = qk("jobs", "teardown");
    pq.create_queue(qdef(
        "jobs",
        "teardown",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    let pid = pq.push(&purge_q, NewItem::default()).await.unwrap();
    let claimed = pq.claim(&purge_q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "item leased before operator teardown");
    let n1 = pq.purge(&purge_q, [pid], true).await.unwrap(); // force: the item is leased
    assert_eq!(n1, 1, "purge removes the leased item (force)");
    let n2 = pq.purge(&purge_q, [pid], true).await.unwrap();
    assert_eq!(
        n2, 0,
        "purge is IDEMPOTENT: a second purge of the same id is a no-op (0 removed)"
    );
    let late = pq.ack(&purge_q, [pid]).await;
    let late_not_found = matches!(late, Err(EngineError::NotFound));
    assert!(
        late_not_found,
        "a late finalize after purge returns not_found: {late:?}"
    );

    // Measured values (not literals): each field is the observed result of the asserted behavior above.
    // NOTE: "approximate counter convergence under eventual consistency" (TP-003 AC-E2E-4) is a DURABLE-backend
    // concern — the in-memory backend's metrics() counts are EXACT, so it is not separately exercised here.
    emit_ac(
        "AC-E2E-4",
        &[],
        "recurring singleton cycles as one row with monotonic item_version; rearm resets the delivery count (does NOT consume retry budget — counterfactual: a Retry-nack terminalizes at max_attempts); PurgeItems idempotent + late finalize -> not_found [DEFERRED -> pqueue-8cbae731: rearm idle-period, recurrence.until; approx-counter convergence is a durable-backend concern (exact here)]",
        BTreeMap::from([
            ("rearm_cycles".into(), serde_json::json!(cycles)),
            ("item_versions".into(), serde_json::json!(versions)),
            (
                "attempt_count_per_cycle".into(),
                serde_json::json!(attempts_per_cycle),
            ),
            (
                "retry_counterfactual_failed_pending".into(),
                serde_json::json!([retry_terminal.failed, retry_terminal.pending]),
            ),
            (
                "purge_first_then_second".into(),
                serde_json::json!([n1, n2]),
            ),
            (
                "late_finalize_not_found".into(),
                serde_json::json!(late_not_found),
            ),
        ]),
    );
}

// ---------------------------------------------------------------------------
// AC-E2E-2 — Marketo group-cardinality batching
// ---------------------------------------------------------------------------

/// A group-batching queue definition: `max_eligible_group_size` set (so `group_batching` claims validate).
fn group_qdef(tenant: &str, queue: &str, max_eligible_group_size: u64) -> QueueDefinition {
    let mut d = qdef(
        tenant,
        queue,
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    );
    d.max_eligible_group_size = Some(max_eligible_group_size);
    d
}

/// AC-E2E-2 (TP-003): model a downstream API that accepts up to N distinct lead groups — a group-batching
/// queue. The whole-group SELECTION (claim/finalize whole eligible groups, under contention) is NOT yet
/// implemented; this validates the group-batching claim-compatibility CONTRACT + item-level parity, and
/// defers the selection. The cited FRs (FR-29..32, FR-35, FR-47, FR-48) are the full AC-E2E-2 scope — only
/// the validation-contract subset is exercised here (the row's `inv_ids` is empty; nothing is overclaimed).
///
/// COVERED via the lib facade (each assertion bites):
///   - the queue is loaded with >=1000 groups x multiple tasks, and item-level claim still works on it
///     (parity — the group config does not break ordinary delivery);
///   - the group-batching claim-compatibility CONTRACT (validate_claim_compatibility, which IS implemented):
///       * `group_batching` on a queue WITHOUT max_eligible_group_size -> Invalid;
///       * `group_batching.max_groups == 0` -> Invalid;
///       * `max_eligible_group_size > max_items` -> BatchTooLarge (the "next whole group cannot fit" guard);
///       * a well-formed WHOLE-GROUP claim unit is RECOGNIZED and refused with the structured `Unavailable`
///         (the group selection is not yet implemented — NOT silently item-claimed or mis-rejected).
/// DEFERRED (whole-group SELECTION not implemented -> BQ-14b / pqueue-7a96f929): atomic whole-group claim,
/// INV-7 (0 partial groups), <=max_groups groups per claim, group-representative ordering, concurrent
/// claimers do not duplicate groups, and active-group discovery. NOT asserted, NOT claimed in the row.
#[tokio::test]
async fn marketo_group_batching_e2e() {
    let (pq, _clock) = deployment();
    let max_group_size = 5u64;
    let q = qk("marketo", "leads");
    pq.create_queue(group_qdef("marketo", "leads", max_group_size))
        .await
        .unwrap();

    // Load >=1000 lead groups, multiple tasks per lead (establishing a realistic group population).
    let groups = 1000u64;
    let tasks_per_group = 3u64;
    let mut items = Vec::new();
    for g in 0..groups {
        for t in 0..tasks_per_group {
            items.push(NewItem {
                priority: Some(PriorityValue::Int64(((g * 7 + t) % 100) as i64)),
                group_key: Some(GroupKey::new(format!("lead-{g}")).unwrap()),
                ..Default::default()
            });
        }
    }
    let loaded = items.len() as u64;
    pq.push_batch(&q, items).await.unwrap();
    assert_eq!(
        pq.metrics(&q).await.unwrap().pending,
        loaded,
        "all group tasks resident"
    );

    // Parity: ITEM-level claim (the default unit) still works on a group-batching queue — the group config
    // does not disable ordinary delivery. (Counterfactual that the queue itself is healthy.)
    let item_claim = pq.claim(&q, 10, 60_000).await.unwrap();
    assert_eq!(
        item_claim.len(),
        10,
        "item-level claim works on a group-batching queue"
    );
    pq.nack(&q, item_claim.iter().map(|c| c.item_id), Nack::Release)
        .await
        .unwrap();

    // --- group-batching claim-compatibility CONTRACT (implemented validation; each error is distinct) ---
    let whole_group = |max_groups: u32| ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups }),
        ..Default::default()
    };

    // (a) max_groups == 0 -> Invalid (pin the message so this is distinct from the missing-config Invalid (c)).
    let zero = pq.claim_with(&q, 100, 60_000, whole_group(0)).await;
    assert!(
        matches!(&zero, Err(EngineError::Invalid(m)) if m.contains("max_groups")),
        "max_groups=0 is Invalid(max_groups...): {zero:?}"
    );

    // (b) max_eligible_group_size (5) > max_items (3) -> BatchTooLarge (the whole group can't fit the batch).
    let too_large = pq.claim_with(&q, 3, 60_000, whole_group(300)).await;
    assert!(
        matches!(too_large, Err(EngineError::BatchTooLarge)),
        "a max_items below the group size fires BatchTooLarge (next whole group cannot fit): {too_large:?}"
    );

    // (c) group_batching on a queue WITHOUT max_eligible_group_size -> Invalid.
    let plain_q = qk("marketo", "plain");
    pq.create_queue(qdef(
        "marketo",
        "plain",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    let _ = pq.push(&plain_q, NewItem::default()).await.unwrap();
    let unconfigured = pq.claim_with(&plain_q, 100, 60_000, whole_group(300)).await;
    assert!(
        matches!(&unconfigured, Err(EngineError::Invalid(m)) if m.contains("max_eligible_group_size")),
        "group_batching requires max_eligible_group_size (distinct from the max_groups Invalid): {unconfigured:?}"
    );

    // (d) a WELL-FORMED whole-group claim (max_items >= group size) is RECOGNIZED and refused as Unavailable —
    // the WholeGroup selection is not yet implemented (BQ-14b), NOT silently item-claimed. This is the biting
    // contract: the unit is validated to WholeGroup, then the unimplemented selection returns the structured
    // Unavailable (not Invalid, not a partial/item claim).
    let well_formed = pq.claim_with(&q, 100, 60_000, whole_group(300)).await;
    assert!(
        matches!(well_formed, Err(EngineError::Unavailable)),
        "a well-formed whole-group claim is refused with Unavailable (selection unimplemented -> BQ-14b): {well_formed:?}"
    );

    emit_ac(
        "AC-E2E-2",
        &[],
        "group-batching queue loaded with >=1000 groups; item-level claim parity holds; the group-batching claim-compatibility contract is enforced (max_groups=0 -> Invalid; group-size>max_items -> BatchTooLarge; missing max_eligible_group_size -> Invalid; well-formed whole-group -> Unavailable, selection unimplemented). [DEFERRED -> BQ-14b/pqueue-7a96f929: atomic whole-group claim, INV-7 0-partial-groups, <=max_groups/claim, group-rep ordering, concurrent no-dup, discovery]",
        BTreeMap::from([
            ("groups_loaded".into(), serde_json::json!(groups)),
            ("items_loaded".into(), serde_json::json!(loaded)),
            (
                "item_level_claim_len".into(),
                serde_json::json!(item_claim.len()),
            ),
            (
                "max_groups_zero".into(),
                serde_json::json!(format!("{zero:?}")),
            ),
            (
                "batch_too_large".into(),
                serde_json::json!(format!("{too_large:?}")),
            ),
            (
                "missing_group_size".into(),
                serde_json::json!(format!("{unconfigured:?}")),
            ),
            (
                "well_formed_whole_group".into(),
                serde_json::json!(format!("{well_formed:?}")),
            ),
        ]),
    );
}

// ---------------------------------------------------------------------------
// AC-E2E-3 — callback cohort execution
// ---------------------------------------------------------------------------

/// A cohort-enabled queue definition (cohort_policy with the given enable flag + completion_bound_ms). NOTE:
/// this is a CLAIM-VALIDATION fixture — it sets only the fields `validate_claim_compatibility` inspects
/// (`enabled`/`completion_bound_ms`); the fuller create-path cohort validator (which also requires
/// on_incomplete + max_cohort_size) is not exercised here (whole-cohort selection is deferred to BQ-14c).
fn cohort_qdef(
    tenant: &str,
    queue: &str,
    enabled: bool,
    completion_bound_ms: Option<u64>,
) -> QueueDefinition {
    let mut d = qdef(
        tenant,
        queue,
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    );
    d.cohort_policy = Some(CohortPolicy {
        enabled,
        completion_bound_ms,
        on_incomplete: None,
        max_cohort_size: None,
    });
    d
}

/// AC-E2E-3 (TP-003): model `actions_scheduled` callback batches on a cohort-enabled queue. The whole-cohort
/// SELECTION (claim/finalize complete cohorts atomically; incomplete cohorts hidden from claim/discovery;
/// expiry-to-failed) is NOT yet implemented; this validates the whole_cohort claim-compatibility CONTRACT +
/// item-level parity and defers the selection. (FR-32a..32c, FR-47a, FR-47c, FR-48.)
///
/// COVERED via the lib facade (each assertion bites a DISTINCT cause):
///   - item-level claim still works on a cohort-enabled queue (parity);
///   - whole_cohort on a NON-cohort queue -> Invalid("...enabled=true");
///   - whole_cohort on a cohort queue with completion_bound_ms = None -> Invalid("requires cohort completion...");
///   - whole_cohort with completion_bound_ms (90s) > progress_bound_ms (60s) -> Invalid("...<= progress_bound_ms");
///   - whole_cohort COMBINED with group_key -> Invalid("cannot be combined...");
///   - a well-formed whole_cohort claim is RECOGNIZED (ClaimUnit::WholeCohort) and refused with Unavailable
///     (selection unimplemented -> BQ-14c), NOT silently item-claimed.
/// DEFERRED (-> BQ-14c): atomic whole-cohort claim under one shared lease, incomplete-cohort hiding from
/// claim/discovery (eligible_candidates ignores cohorts in the in-memory family), INV-7 (0 cohort leaks),
/// and expired-incomplete -> terminal failed with reason. NOT asserted, NOT claimed in the row.
#[tokio::test]
async fn callback_cohort_e2e() {
    let (pq, _clock) = deployment();

    // A VALID cohort queue: enabled + completion_bound_ms (30s) <= progress_bound_ms (60s).
    let cohort_q = qk("cohort", "callbacks");
    pq.create_queue(cohort_qdef("cohort", "callbacks", true, Some(30_000)))
        .await
        .unwrap();

    // Item-level parity: ordinary delivery still works on a cohort-enabled queue.
    let _ = pq.push(&cohort_q, NewItem::default()).await.unwrap();
    let item_claim = pq.claim(&cohort_q, 10, 60_000).await.unwrap();
    assert_eq!(
        item_claim.len(),
        1,
        "item-level claim works on a cohort-enabled queue"
    );
    pq.nack(
        &cohort_q,
        item_claim.iter().map(|c| c.item_id),
        Nack::Release,
    )
    .await
    .unwrap();

    let whole_cohort = || ClaimCompatibility {
        whole_cohort: true,
        ..Default::default()
    };

    // (a) whole_cohort on a NON-cohort queue -> Invalid(enabled).
    let plain_q = qk("cohort", "plain");
    pq.create_queue(qdef(
        "cohort",
        "plain",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    let _ = pq.push(&plain_q, NewItem::default()).await.unwrap();
    let not_cohort = pq.claim_with(&plain_q, 10, 60_000, whole_cohort()).await;
    assert!(
        matches!(&not_cohort, Err(EngineError::Invalid(m)) if m.contains("enabled=true")),
        "whole_cohort on a non-cohort queue is Invalid(enabled=true): {not_cohort:?}"
    );

    // (b) whole_cohort on a cohort queue with completion_bound_ms = None -> Invalid(requires completion).
    let no_bound_q = qk("cohort", "nobound");
    pq.create_queue(cohort_qdef("cohort", "nobound", true, None))
        .await
        .unwrap();
    let _ = pq.push(&no_bound_q, NewItem::default()).await.unwrap();
    let no_bound = pq.claim_with(&no_bound_q, 10, 60_000, whole_cohort()).await;
    assert!(
        matches!(&no_bound, Err(EngineError::Invalid(m)) if m.contains("requires cohort completion")),
        "whole_cohort requires completion_bound_ms: {no_bound:?}"
    );

    // (c) completion_bound_ms (90s) > progress_bound_ms (60s) -> Invalid(<= progress_bound_ms).
    let bad_bound_q = qk("cohort", "badbound");
    pq.create_queue(cohort_qdef("cohort", "badbound", true, Some(90_000)))
        .await
        .unwrap();
    let _ = pq.push(&bad_bound_q, NewItem::default()).await.unwrap();
    let bad_bound = pq
        .claim_with(&bad_bound_q, 10, 60_000, whole_cohort())
        .await;
    assert!(
        matches!(&bad_bound, Err(EngineError::Invalid(m)) if m.contains("<= progress_bound_ms")),
        "completion_bound_ms must be <= progress_bound_ms: {bad_bound:?}"
    );

    // (d) whole_cohort COMBINED with group_key -> Invalid(cannot be combined).
    let combined = pq
        .claim_with(
            &cohort_q,
            10,
            60_000,
            ClaimCompatibility {
                whole_cohort: true,
                group_key: Some(GroupKey::new("g").unwrap()),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(&combined, Err(EngineError::Invalid(m)) if m.contains("cannot be combined")),
        "whole_cohort cannot be combined with group_key: {combined:?}"
    );

    // (e) a WELL-FORMED whole_cohort claim is recognized (ClaimUnit::WholeCohort) and refused with Unavailable
    // (selection unimplemented -> BQ-14c), NOT silently item-claimed.
    let well_formed = pq.claim_with(&cohort_q, 10, 60_000, whole_cohort()).await;
    assert!(
        matches!(well_formed, Err(EngineError::Unavailable)),
        "a well-formed whole_cohort claim is refused with Unavailable (selection unimplemented -> BQ-14c): {well_formed:?}"
    );

    emit_ac(
        "AC-E2E-3",
        &[],
        "item-level claim parity on a cohort-enabled queue; the whole_cohort claim-compatibility contract is enforced with distinct errors (non-cohort -> Invalid(enabled); no completion_bound -> Invalid(requires); completion>progress -> Invalid(<=progress); combined with group_key -> Invalid(combined); well-formed -> Unavailable, selection unimplemented). [DEFERRED -> BQ-14c: atomic whole-cohort claim, incomplete-cohort hiding, INV-7 0-cohort-leaks, expiry->failed]",
        BTreeMap::from([
            (
                "item_level_claim_len".into(),
                serde_json::json!(item_claim.len()),
            ),
            (
                "non_cohort".into(),
                serde_json::json!(format!("{not_cohort:?}")),
            ),
            (
                "no_completion_bound".into(),
                serde_json::json!(format!("{no_bound:?}")),
            ),
            (
                "completion_gt_progress".into(),
                serde_json::json!(format!("{bad_bound:?}")),
            ),
            (
                "combined_with_group_key".into(),
                serde_json::json!(format!("{combined:?}")),
            ),
            (
                "well_formed_whole_cohort".into(),
                serde_json::json!(format!("{well_formed:?}")),
            ),
        ]),
    );
}

// ---------------------------------------------------------------------------
// AC-E2E-6 — noisy-neighbor + active-scope routing
// ---------------------------------------------------------------------------

const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0; // E0 per-queue floor: 2777.78/s.

/// AC-E2E-6 (TP-003): one hot queue with a large resident backlog, one small eligible queue, and K active
/// queues on ONE node. (FR-1, FR-12, FR-40..43, FR-48.)
///
/// SCOPE (honest): this is the SINGLE-THREADED, in-process CORRECTNESS-isolation slice of AC-E2E-6 — it shows
/// that per-queue ownership keeps each queue's delivery + drain correct and independent with a large hot
/// backlog + K queues co-resident. These are restatements of per-queue keying, NOT a contention proof; the
/// real noisy-neighbor CONTENTION measurement (concurrent workers contending the shared node, throughput-
/// flatness across a residency ladder) is BQ-41 (queue_density_single_node_tests), already committed.
///
/// COVERED via the lib facade:
///   - a claim from the small queue returns ONLY the small queue's items (and the hot/active claims return
///     only their own) — per-queue keying, zero cross-queue leakage, each claim verified NON-EMPTY;
///   - K queues are INDEPENDENTLY claimable (each returns exactly its own 10 items);
///   - the small queue drains FULLY (completeness) and, at this single in-memory operating point, its
///     claim+ack rate is far above the E0 floor (high headroom — this is a sanity floor, NOT the algorithmic-
///     cost ladder; that, and concurrent contention, are BQ-41);
///   - the hot backlog is uncorrupted by the small queue's drain (no cross-queue mutation).
/// DEFERRED (-> pqueue-289c8d5a / pqueue-c33c367e): DiscoverActiveScopes ranking authorized active scopes by
/// oldest-eligible age, unauthorized-scope exclusion (auth layer per ADR-002), the AC-LAT-1 p95<250ms/
/// p99<1000ms latency bars at release scale (provisioned perf env), bounded-per-node worker pools, and the
/// CONCURRENT noisy-neighbor measurement (BQ-41). NOT claimed in the row.
#[tokio::test]
async fn noisy_neighbor_scale_e2e() {
    let (pq, _clock) = deployment();

    // Hot queue: a large resident backlog.
    let hot = qk("nn", "hot");
    pq.create_queue(qdef(
        "nn",
        "hot",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    let hot_backlog = 50_000u64;
    let hot_items: Vec<NewItem> = (0..hot_backlog)
        .map(|_| NewItem {
            payload: Some(Bytes::from_static(b"hot")),
            ..Default::default()
        })
        .collect();
    pq.push_batch(&hot, hot_items).await.unwrap();

    // K active queues, each with a small resident set.
    let k = 50u64;
    for i in 0..k {
        let q = qk("nn", &format!("active{i}"));
        pq.create_queue(qdef(
            "nn",
            &format!("active{i}"),
            PriorityDirection::Ascending,
            OrderingMode::Strict,
        ))
        .await
        .unwrap();
        let marker = format!("active{i}");
        let items: Vec<NewItem> = (0..10)
            .map(|_| NewItem {
                payload: Some(Bytes::from(marker.clone().into_bytes())),
                ..Default::default()
            })
            .collect();
        pq.push_batch(&q, items).await.unwrap();
    }

    // Small eligible queue.
    let small = qk("nn", "small");
    pq.create_queue(qdef(
        "nn",
        "small",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();
    let small_n = 200u64;
    let small_items: Vec<NewItem> = (0..small_n)
        .map(|_| NewItem {
            payload: Some(Bytes::from_static(b"small")),
            ..Default::default()
        })
        .collect();
    pq.push_batch(&small, small_items).await.unwrap();

    // CORRECTNESS isolation: with the hot backlog + K queues all resident, a claim from the small queue
    // returns ONLY the small queue's items (no hot/active leakage), and a claim from the hot queue returns
    // only hot items. Per-queue keying ⇒ zero cross-queue leakage.
    let from_small = pq.claim(&small, 10, 60_000).await.unwrap();
    assert_eq!(
        from_small.len(),
        10,
        "small queue claim returns items (not vacuously empty)"
    );
    assert!(
        from_small
            .iter()
            .all(|c| c.payload.as_deref() == Some(b"small".as_ref())),
        "the small queue delivers only its own items (no hot/active leakage)"
    );
    pq.nack(&small, from_small.iter().map(|c| c.item_id), Nack::Release)
        .await
        .unwrap();
    let from_hot = pq.claim(&hot, 10, 60_000).await.unwrap();
    assert_eq!(
        from_hot.len(),
        10,
        "hot queue claim returns items (not vacuously empty)"
    );
    assert!(
        from_hot
            .iter()
            .all(|c| c.payload.as_deref() == Some(b"hot".as_ref())),
        "the hot queue delivers only its own items"
    );
    pq.nack(&hot, from_hot.iter().map(|c| c.item_id), Nack::Release)
        .await
        .unwrap();

    // K queues independently claimable: each returns only its own marker.
    for i in 0..k {
        let q = qk("nn", &format!("active{i}"));
        let got = pq.claim(&q, 100, 60_000).await.unwrap();
        let marker = format!("active{i}");
        assert_eq!(
            got.len(),
            10,
            "active queue {i} returns exactly its own 10 items (not empty/leaked)"
        );
        assert!(
            got.iter()
                .all(|c| c.payload.as_deref() == Some(marker.as_bytes())),
            "active queue {i} delivers only its own items"
        );
        pq.nack(&q, got.iter().map(|c| c.item_id), Nack::Release)
            .await
            .unwrap();
    }

    // COMPLETENESS + sanity throughput (measured): drain the small queue with the hot backlog + K queues
    // resident; it fully drains and its claim+ack rate is far above the E0 floor at this single in-memory
    // operating point. (High headroom — a SANITY floor, not the algorithmic-cost ladder or concurrent
    // contention; those are BQ-41.)
    let t = Instant::now();
    let mut drained = 0u64;
    loop {
        let got = pq.claim(&small, 100, 60_000).await.unwrap();
        if got.is_empty() {
            break;
        }
        drained += got.len() as u64;
        pq.ack(&small, got.iter().map(|c| c.item_id)).await.unwrap();
    }
    let small_rate = drained as f64 / t.elapsed().as_secs_f64();
    assert_eq!(
        drained, small_n,
        "the small queue fully drains (completeness) with the hot backlog resident"
    );
    assert!(
        small_rate >= FLOOR_ITEMS_PER_SEC,
        "the small queue clears the E0 floor (>= {FLOOR_ITEMS_PER_SEC:.0}/s) with a {hot_backlog}-item hot backlog + {k} queues resident: {small_rate:.0}/s"
    );
    // The hot backlog is untouched by the small queue's drain (isolation): still fully resident.
    let hot_pending_after = pq.metrics(&hot).await.unwrap().pending;
    assert_eq!(
        hot_pending_after, hot_backlog,
        "hot backlog undisturbed by the small queue"
    );

    emit_ac(
        "AC-E2E-6",
        &[],
        "SINGLE-THREADED correctness-isolation (per-queue ownership): small/hot/K-queue claims each return only their own items (zero cross-queue leakage, all non-empty); K queues independently claimable; small queue fully drains (completeness) and clears the E0 floor at this in-memory operating point (sanity, high headroom); hot backlog uncorrupted [DEFERRED -> pqueue-289c8d5a: DiscoverActiveScopes ranking + authz exclusion + AC-LAT-1 latency-at-release-scale; CONCURRENT noisy-neighbor contention + algorithmic-cost ladder is BQ-41; bounded-per-node-pools pqueue-c33c367e]",
        BTreeMap::from([
            ("hot_backlog".into(), serde_json::json!(hot_backlog)),
            ("active_queues".into(), serde_json::json!(k)),
            ("small_items".into(), serde_json::json!(small_n)),
            (
                "small_drain_rate_per_s".into(),
                serde_json::json!(small_rate.round()),
            ),
            (
                "e0_floor_per_s".into(),
                serde_json::json!(FLOOR_ITEMS_PER_SEC.round()),
            ),
            (
                "hot_pending_after_small_drain".into(),
                serde_json::json!(hot_pending_after),
            ),
        ]),
    );
}

// ---------------------------------------------------------------------------
// AC-E2E-5 — worker crash recovery
// ---------------------------------------------------------------------------

/// AC-E2E-5 (TP-003): durable recovery — acknowledged commands survive a restart and no accepted item is
/// lost. Driven via the lib facade over a FILE-BACKED durable backend (ObjectLogBackend): build durable
/// state, DROP the handle (the process "crashes"), reopen a fresh handle on the same on-disk log, and verify
/// the state was rebuilt from disk. (FR-23..28, FR-33..39; durability/recovery.)
///
/// COVERED via the lib facade (biting):
///   - DURABLE REOPEN: push N + ack some + leave some pending/leased, drop the handle (only the on-disk
///     object log remains), reopen → the projection is rebuilt from disk: acknowledged items stay complete
///     and EVERY accepted item is accounted (pending+leased+complete+failed == N, zero loss). The
///     COUNTERFACTUAL bites: a FRESH (empty) dir reopened sees 0 — recovery is genuinely from disk, not a
///     surviving in-memory projection;
///   - lease REASSIGN: a leased item's lease is reassigned to a new token and the item stays leased (no loss).
/// DEFERRED (honest — the facade/in-memory family lacks the seam):
///   - worker-crash lease-expiry REDELIVERY (expired leases redeliver without resetting eligible age) — needs
///     a reclaim tick not on the facade (-> pqueue-7a96f929);
///   - duplicate request replay convergence (replayed request_ids converge) — needs a request_id-carrying
///     data-plane port; all envelopes are request_id:None today (-> BQ-11e / pqueue-e1b21208);
///   - live multi-PROCESS service injection + owner reassignment/epoch-advance under load (TD-003 control
///     plane) (-> pqueue-c33c367e server runtime). NOT asserted, NOT claimed in the row.
#[tokio::test]
async fn worker_crash_recovery_e2e() {
    let dir = std::env::temp_dir().join(format!("pqueue-pv-e2e5-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let q = qk("recovery", "jobs");
    let n = 100u64;
    let acked = 30u64;
    let leased = 20u64;

    // ----- build durable state, then "crash" (drop the handle) -----
    let (complete_before, accounted_before) = {
        let pq = Pqueue::new(
            Arc::new(ObjectLogBackend::open(&dir).expect("open object log")),
            Arc::new(ManualClock::at(0)),
        );
        pq.create_queue(qdef(
            "recovery",
            "jobs",
            PriorityDirection::Ascending,
            OrderingMode::Strict,
        ))
        .await
        .unwrap();
        let items: Vec<NewItem> = (0..n).map(|_| NewItem::default()).collect();
        pq.push_batch(&q, items).await.unwrap();
        // Ack `acked` (acknowledged commands), leave `leased` leased, the rest pending.
        let to_ack = pq.claim(&q, acked as usize, 3_600_000).await.unwrap();
        pq.ack(&q, to_ack.iter().map(|c| c.item_id)).await.unwrap();
        let _still_leased = pq.claim(&q, leased as usize, 3_600_000).await.unwrap(); // left leased
        let m = pq.metrics(&q).await.unwrap();
        assert_eq!(m.complete, acked, "acked items complete before crash");
        let accounted = m.pending + m.leased + m.complete + m.failed;
        assert_eq!(accounted, n, "every accepted item accounted before crash");
        (m.complete, accounted)
    }; // <- the Pqueue + ObjectLogBackend drop here; only the on-disk object log survives.

    // ----- COUNTERFACTUAL: a FRESH dir recovers nothing (recovery is from disk, not a surviving projection) -----
    let fresh_dir =
        std::env::temp_dir().join(format!("pqueue-pv-e2e5-fresh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&fresh_dir);
    {
        let fresh = Pqueue::new(
            Arc::new(ObjectLogBackend::open(&fresh_dir).expect("open fresh")),
            Arc::new(ManualClock::at(0)),
        );
        // The queue itself isn't known to a fresh backend (no create_queue command in its empty log).
        assert!(
            fresh.metrics(&q).await.is_err(),
            "a fresh empty backend has no record of the crashed node's queue"
        );
        let _ = std::fs::remove_dir_all(&fresh_dir);
    }

    // ----- RECOVERY: reopen the SAME on-disk log; the projection is rebuilt from disk -----
    let pq = Pqueue::new(
        Arc::new(ObjectLogBackend::open(&dir).expect("reopen object log")),
        Arc::new(ManualClock::at(0)),
    );
    let m = pq
        .metrics(&q)
        .await
        .expect("the crashed node's durable state is recovered");
    assert_eq!(
        m.complete, complete_before,
        "acknowledged commands survived the restart"
    );
    let accounted_after = m.pending + m.leased + m.complete + m.failed;
    assert_eq!(
        accounted_after, n,
        "no accepted item lost across the restart"
    );
    assert_eq!(
        accounted_after, accounted_before,
        "the full resident set was recovered"
    );

    // ----- lease REASSIGN (item-level, ReassignLeasePort): reassign a leased item's lease; it stays leased,
    // not lost to pending/complete. (Only the leased COUNT is observed via metrics — the facade does not
    // surface the post-reassign token, so token transfer itself is not asserted here.)
    let leased_before = pq.metrics(&q).await.unwrap().leased;
    // Claim a fresh pending item to reassign (it is now leased). Non-conditional so the proof can't be skipped.
    let claimed = pq.claim(&q, 1, 3_600_000).await.unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "a pending item is claimable on the recovered queue"
    );
    pq.reassign(&q, [claimed[0].item_id], 3_600_000)
        .await
        .unwrap();
    assert_eq!(
        pq.metrics(&q).await.unwrap().leased,
        leased_before + 1,
        "the reassigned item remains leased (not lost to pending/complete)"
    );

    emit_ac(
        "AC-E2E-5",
        &[],
        "[crash modeled as handle-drop + reopen of the file-backed object log] durable reopen recovers the crashed node's state from disk: acknowledged commands survive the restart and every accepted item is accounted (zero loss); a fresh empty backend recovers nothing (recovery is from disk); an item-level lease reassign keeps the item leased (not lost) [DEFERRED: lease-expiry REDELIVERY -> pqueue-7a96f929; duplicate-request-replay convergence -> BQ-11e/pqueue-e1b21208; live multi-process + owner/epoch reassignment -> pqueue-c33c367e]",
        BTreeMap::from([
            ("accepted_items".into(), serde_json::json!(n)),
            ("acked_before_crash".into(), serde_json::json!(acked)),
            (
                "complete_after_recovery".into(),
                serde_json::json!(m.complete),
            ),
            (
                "accounted_after_recovery".into(),
                serde_json::json!(accounted_after),
            ),
            ("lease_reassigned".into(), serde_json::json!(true)),
        ]),
    );
    let _ = std::fs::remove_dir_all(&dir);
}
