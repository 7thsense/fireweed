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

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use pqueue::{ClaimCompatibility, EngineError, GroupBatching, Nack, NewItem, Pqueue};
use pqueue_core::{
    EligibilityPolicy, GroupKey, ItemId, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_engine::QueueKey;
use pqueue_memory::{ManualClock, MemoryBackend};

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
    }
}

fn qk(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

/// Emit a SMOKE-tier AC-E2E ledger row from real measured/observed values, and assert it round-trips strict
/// validation under its acceptance id. (Structure check; the workflow's own asserts verify the behavior.)
fn emit_ac(
    ac_id: &str,
    inv_ids: &[&str],
    pass_bar: &str,
    values: BTreeMap<String, serde_json::Value>,
) {
    let suite = format!(
        "product_validation_tests_{}",
        ac_id.to_lowercase().replace('-', "_")
    );
    let row = pqueue_release::LedgerRow {
        suite: "product_validation_tests".into(),
        command: "cargo test -p pqueue --test product_validation_tests".into(),
        backend_profile: "memory".into(),
        scale: "smoke".into(),
        seed: 0,
        environment:
            "in-process lib facade (Pqueue + MemoryBackend); release shape is the provisioned run"
                .into(),
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
        pq.ack(&q, got.iter().map(|c| c.item_id.clone()))
            .await
            .unwrap();
        claimed_total += got.len() as u64;
        remaining -= got.len() as i64;
        batches += 1;
    }
    // Drain whatever remains so we can prove the totals.
    while pq.metrics(&q).await.unwrap().pending > 0 {
        let got = pq.claim(&q, 100, 3_600_000).await.unwrap();
        pq.ack(&q, got.iter().map(|c| c.item_id.clone()))
            .await
            .unwrap();
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
        pq.ack(q, got.iter().map(|c| c.item_id.clone()))
            .await
            .unwrap();
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
/// delivered in schedule order, and renew/finalize cleanly; with cross-tenant isolation and metrics matching
/// the terminal state. (FR-1..3, FR-7, FR-18..28, FR-40..46.)
///
/// COVERED via the lib facade: not_before scheduling + eligibility gating by the clock; strict
/// timestamp-ascending delivery order (INV: schedule order == timestamp); single delivery per item (INV-1);
/// renew commits + preserves the lease; progress to terminal (INV-4); tenant NAMESPACING (same queue_id under
/// two tenants are independent queues with no cross-tenant leakage); metrics match the terminal state.
/// DEFERRED (tracked on pqueue-7a96f929 — facade lacks the seam): BatchUpdate reschedule (change
/// priority/not_before after push), SetGates close+reopen gating (no gated item claimed while blocked),
/// claim-by-group_key, and the expiry-REDELIVERY-vs-renew proof (needs a facade reclaim tick). Cross-tenant
/// AUTHZ denial lives in the auth layer (ADR-002), not this trusted library facade. NOT claimed in the row.
#[tokio::test]
async fn scheduled_action_delivery_e2e() {
    let (pq, clock) = deployment();
    // Timestamp-ascending queue: priority == scheduled send time, ascending → earliest scheduled first.
    let q = qk("sched", "campaign");
    pq.create_queue(qdef(
        "sched",
        "campaign",
        PriorityDirection::Ascending,
        OrderingMode::Strict,
    ))
    .await
    .unwrap();

    // Push 3 actions EARLY (clock is at 0), each scheduled for a future send time via not_before, priority ==
    // the send time so schedule order == timestamp order.
    let schedule = [10i64, 20, 30];
    for &t in &schedule {
        pq.push(
            &q,
            NewItem {
                priority: Some(PriorityValue::Int64(t)),
                not_before: Some(ts(t)),
                payload: Some(Bytes::from(format!("send@{t}").into_bytes())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(
        pq.metrics(&q).await.unwrap().pending,
        3,
        "all scheduled, pending"
    );

    // BEFORE any send time (clock=0): nothing is eligible — a claim returns an empty batch (the actions are
    // scheduled, not withheld; their not_before simply hasn't arrived).
    assert!(
        pq.claim(&q, 10, 60_000).await.unwrap().is_empty(),
        "no action is deliverable before its scheduled time"
    );

    // Delivery in schedule order as the clock advances; track ids to prove single delivery (INV-1).
    let mut delivered_order = Vec::new();
    let mut delivered_ids: Vec<ItemId> = Vec::new();

    // clock=15 → only the action scheduled at 10 is eligible.
    clock.set(15);
    let batch = pq.claim(&q, 10, 60_000).await.unwrap();
    assert_eq!(
        batch.len(),
        1,
        "exactly the one due action is deliverable at t=15"
    );
    record(&batch, &mut delivered_order, &mut delivered_ids);
    pq.ack(&q, batch.iter().map(|c| c.item_id.clone()))
        .await
        .unwrap();

    // clock=100 → the remaining actions (20, 30) are eligible; delivered in ascending schedule order.
    clock.set(100);
    let batch = pq.claim(&q, 10, 50_000).await.unwrap(); // lease 50s → expiry at 150
    assert_eq!(
        batch.len(),
        2,
        "both remaining due actions are deliverable at t=100"
    );
    record(&batch, &mut delivered_order, &mut delivered_ids);

    // RENEW the leases: the renew verb commits on the leased items and they remain leased + claimable for
    // finalize. NOTE (honest scope): expiry-REDELIVERY suppression (an un-renewed lease would redeliver after
    // its deadline, a renewed one would not) is NOT exercised here — the memory backend only returns an
    // expired lease to Pending via a reclaim tick (ReclaimDriver), which the lib facade does not expose, so a
    // claim never reclaims expired leases regardless of renew. The redelivery-vs-renew proof needs a facade
    // reclaim-tick seam (deferred to pqueue-7a96f929). Here we prove only that renew succeeds and preserves
    // the lease.
    clock.set(140);
    pq.renew(&q, batch.iter().map(|c| c.item_id.clone()), 50_000)
        .await
        .unwrap();
    assert_eq!(
        pq.metrics(&q).await.unwrap().leased,
        2,
        "renew preserves the lease (items still leased, claimable for finalize)"
    );
    pq.ack(&q, batch.iter().map(|c| c.item_id.clone()))
        .await
        .unwrap();

    // Schedule order == timestamp order (INV: ordering), and each action delivered exactly once (INV-1).
    assert_eq!(
        delivered_order, schedule,
        "actions delivered in scheduled (timestamp) order"
    );
    delivered_ids.sort();
    delivered_ids.dedup();
    assert_eq!(
        delivered_ids.len(),
        3,
        "each action delivered exactly once (INV-1: no duplicate delivery)"
    );

    // Metrics match the terminal state (INV-4 progress: everything reached complete).
    let m = pq.metrics(&q).await.unwrap();
    assert_eq!(
        (m.complete, m.pending, m.leased),
        (3, 0, 0),
        "all actions terminal-complete"
    );

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

    emit_ac(
        "AC-E2E-1",
        &["INV-1", "INV-4"],
        "scheduled actions become eligible at not_before, delivered in timestamp order, single delivery (INV-1), renew commits + preserves the lease, tenant-namespaced (no cross-tenant leakage), progress to terminal (INV-4), metrics match terminal [DEFERRED -> pqueue-7a96f929: BatchUpdate-reschedule, SetGates-gating, claim-by-group_key, expiry-redelivery-vs-renew (needs facade reclaim tick); cross-tenant AUTHZ denial is the auth layer]",
        BTreeMap::from([
            (
                "scheduled_actions".into(),
                serde_json::json!(schedule.len()),
            ),
            (
                "delivered_in_schedule_order".into(),
                serde_json::json!(delivered_order == schedule),
            ),
            (
                "unique_deliveries".into(),
                serde_json::json!(delivered_ids.len()),
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

/// Record a claimed batch's priorities (delivery order) + ids (for single-delivery checks).
fn record(batch: &[pqueue::ClaimedItem], order: &mut Vec<i64>, ids: &mut Vec<ItemId>) {
    for c in batch {
        if let Some(PriorityValue::Int64(n)) = c.priority {
            order.push(n);
        }
        ids.push(c.item_id.clone());
    }
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
        pq.rearm(&rec_q, [got[0].item_id.clone()]).await.unwrap();
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
        pq.nack(&retry_q, got.iter().map(|c| c.item_id.clone()), Nack::Retry)
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
    let n1 = pq.purge(&purge_q, [pid.clone()], true).await.unwrap(); // force: the item is leased
    assert_eq!(n1, 1, "purge removes the leased item (force)");
    let n2 = pq.purge(&purge_q, [pid.clone()], true).await.unwrap();
    assert_eq!(
        n2, 0,
        "purge is IDEMPOTENT: a second purge of the same id is a no-op (0 removed)"
    );
    let late = pq.ack(&purge_q, [pid.clone()]).await;
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
    pq.nack(
        &q,
        item_claim.iter().map(|c| c.item_id.clone()),
        Nack::Release,
    )
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
