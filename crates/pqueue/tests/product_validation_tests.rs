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

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use pqueue::{NewItem, Pqueue};
use pqueue_core::{
    EligibilityPolicy, GroupKey, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId,
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
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
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
