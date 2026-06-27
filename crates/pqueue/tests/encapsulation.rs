//! B3 (ADR-009 §4a / L6): the published library is the only blessed surface to the engine.
//!
//! Two guarantees:
//! 1. `open_*` builds a usable `Pqueue` WITHOUT the client ever naming a concrete backend or a port.
//! 2. A source-scan guard fails the build if `pqueue` ever re-exports a port trait or a backend accessor —
//!    so a client of the published crate cannot reach `PushPort`/`ClaimPort`/`FinalizePort` via `pqueue`.

use std::sync::Arc;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId,
};
use pqueue_engine::QueueKey;
use pqueue_memory::ManualClock;

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
    }
}

/// The blessed construction path: `pqueue::open_memory` yields a usable handle with no concrete backend
/// type named by the caller (the returned type is `Pqueue<impl LibBackend>`).
#[tokio::test]
async fn open_memory_builds_a_usable_pqueue() {
    let pq = pqueue::open_memory(Arc::new(ManualClock::at(0)));
    pq.create_queue(qdef()).await.unwrap();
    pq.push(&qkey(), pqueue::NewItem {
        priority: Some(PriorityValue::Int64(5)),
        ..Default::default()
    })
    .await
    .unwrap();
    let claimed = pq.claim(&qkey(), 10, 1_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "open_memory handle claims normally");
}

/// The blessed sqlite path builds and round-trips too.
#[tokio::test]
async fn open_sqlite_builds_a_usable_pqueue() {
    let pq = pqueue::open_sqlite(":memory:", Arc::new(ManualClock::at(0))).unwrap();
    pq.create_queue(qdef()).await.unwrap();
    pq.push(&qkey(), pqueue::NewItem::default()).await.unwrap();
    assert_eq!(pq.metrics(&qkey()).await.unwrap().pending, 1);
}

/// GUARD (ADR-009 L6): `pqueue` must not re-export a port trait on its public surface, and must not expose
/// a backend accessor — either would let a client reach a raw port and bypass coordination/fencing. This
/// scans the library source so a regression (an accidental `pub use ...Port`) fails the build.
#[test]
fn public_surface_exposes_no_port_or_backend() {
    let src = include_str!("../src/lib.rs");

    // No `pub use` may re-export a port trait (a name ending in `Port`). Private `use ... Port` (how the
    // facade consumes the ports internally) is fine — only the PUBLIC re-export surface is constrained.
    let mut in_pub_use = false;
    for line in src.lines() {
        let t = line.trim_start();
        if t.starts_with("pub use") {
            in_pub_use = true;
        }
        if in_pub_use {
            for tok in line.split(|c: char| !c.is_alphanumeric() && c != '_') {
                assert!(
                    !tok.ends_with("Port"),
                    "pqueue must not re-export the port trait `{tok}` on its public surface — clients must \
                     use `Pqueue`, never the raw ports (ADR-009 L6 / B3)"
                );
            }
            if line.contains(';') {
                in_pub_use = false;
            }
        }
    }

    // No public backend accessor — `Pqueue` must never hand back the port-bearing backend it wraps.
    assert!(
        !src.contains("pub fn backend"),
        "Pqueue must not expose a backend accessor (it would leak a port-bearing handle)"
    );

    // The backend-injection constructor `Pqueue::new(Arc<B>, …)` must stay `#[doc(hidden)]` so the
    // documented construction surface is only the `open_*` builders (which never expose a backend type).
    let lines: Vec<&str> = src.lines().collect();
    let new_is_hidden = lines.windows(2).any(|w| {
        w[0].trim() == "#[doc(hidden)]" && w[1].trim_start().starts_with("pub fn new(")
    });
    assert!(
        new_is_hidden,
        "Pqueue::new must be #[doc(hidden)] — open_* is the only documented construction path (ADR-009 L6)"
    );
}
