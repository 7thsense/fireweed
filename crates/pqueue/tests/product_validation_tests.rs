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
