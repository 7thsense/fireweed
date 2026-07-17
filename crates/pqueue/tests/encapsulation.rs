//! B3 (ADR-009 §4a / L6): the published library is the only blessed surface to the engine.
//!
//! Two guarantees:
//! 1. `open_*` builds a usable `Pqueue` WITHOUT the client ever naming a concrete backend or a port.
//! 2. A source-scan guard fails the build if `pqueue` ever re-exports a port trait or a backend accessor —
//!    so a client of the published crate cannot reach `PushPort`/`ClaimPort`/`FinalizePort` via `pqueue`.

use std::sync::Arc;

use pqueue::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueDefinition,
    QueueId, QueueKey, RecurrencePolicy, RetryPolicy, TenantId,
};
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
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn sqlite_test_path(test_name: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "pqueue-{test_name}-{}-{nonce}.sqlite",
            std::process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

fn retain_static<T: 'static>(value: T) -> Arc<T> {
    Arc::new(value)
}

#[test]
fn facade_exports_queue_definition_construction_surface() {
    let key = qkey();
    assert_eq!(key.tenant_id.as_str(), "t1");
    assert_eq!(key.queue_id.as_str(), "q1");

    let definition = qdef();
    assert_eq!(definition.tenant_id.as_str(), "t1");
    assert_eq!(definition.queue_id.as_str(), "q1");

    let create = CreateQueue {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q2").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    };

    let validated = create.validate(&QueueCreationPolicy::default()).unwrap();
    assert_eq!(validated.queue_id.as_str(), "q2");
}

/// The blessed construction path: `pqueue::open_memory` yields a usable handle with no concrete backend
/// type named by the caller (the returned type is `Pqueue<impl LibBackend>`).
#[tokio::test]
async fn open_memory_builds_a_usable_pqueue() {
    let pq = pqueue::open_memory(Arc::new(ManualClock::at(0)));
    pq.create_queue(qdef()).await.unwrap();
    pq.push(
        &qkey(),
        pqueue::NewItem {
            priority: Some(PriorityValue::Int64(5)),
            ..Default::default()
        },
    )
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

/// Rust 2024 return-position `impl Trait` must not capture the borrowed path: the backend owns all
/// state needed by the returned handle.
#[tokio::test]
async fn open_sqlite_retained_handle_owns_path() {
    let path = sqlite_test_path("retained-handle-owns-path");
    let cleanup_path = std::path::PathBuf::from(&path);
    let pq =
        retain_static(pqueue::open_sqlite(path.as_str(), Arc::new(ManualClock::at(0))).unwrap());
    drop(path);

    pq.create_queue(qdef()).await.unwrap();
    pq.push(&qkey(), pqueue::NewItem::default()).await.unwrap();
    assert_eq!(pq.metrics(&qkey()).await.unwrap().pending, 1);
    drop(pq);
    std::fs::remove_file(cleanup_path).unwrap();
}

/// Dropping every handle closes SQLite cleanly; the same caller-owned path can then reopen the durable
/// queue without leaking the path to manufacture a `'static` borrow.
#[tokio::test]
async fn open_sqlite_owned_path_reopens_after_all_handles_drop() {
    let path = sqlite_test_path("owned-path-reopens");
    {
        let pq = retain_static(
            pqueue::open_sqlite(path.as_str(), Arc::new(ManualClock::at(0))).unwrap(),
        );
        pq.create_queue(qdef()).await.unwrap();
        pq.push(&qkey(), pqueue::NewItem::default()).await.unwrap();
        let second_handle = Arc::clone(&pq);
        drop(pq);
        assert_eq!(second_handle.metrics(&qkey()).await.unwrap().pending, 1);
    }

    let reopened =
        retain_static(pqueue::open_sqlite(path.as_str(), Arc::new(ManualClock::at(1))).unwrap());
    assert_eq!(reopened.metrics(&qkey()).await.unwrap().pending, 1);
    drop(reopened);
    std::fs::remove_file(path).unwrap();
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
    let new_is_hidden = lines
        .windows(2)
        .any(|w| w[0].trim() == "#[doc(hidden)]" && w[1].trim_start().starts_with("pub fn new("));
    assert!(
        new_is_hidden,
        "Pqueue::new must be #[doc(hidden)] — open_* is the only documented construction path (ADR-009 L6)"
    );
}
