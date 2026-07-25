//! OWED-6: the blessed postgres construction paths (require `--features postgres` + a live DB).
//!
//! `open_postgres_coordinated` builds the postgres backend AND the binding control plane internally and
//! returns a coordinated `Fireweed` — the client names neither a backend nor a control plane. Env-gated on
//! `FIREWEED_PG_TEST_URL`; driven by a non-tokio executor (the sync postgres client).
#![cfg(feature = "postgres")]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use fireweed::{ConfigSecret, NewItem, PostgresMode, PostgresRuntimeConfig};
use fireweed_core::{
    EligibilityPolicy, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use fireweed_engine::{Clock, ControlPlaneConfig, QueueKey};

fn bo<F: Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

struct ManualClock(AtomicI64);
impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::new(self.0.load(Ordering::SeqCst), 0).unwrap()
    }
}

fn qkey(queue_id: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("t1").unwrap(),
        QueueId::new(queue_id).unwrap(),
    )
}
fn qdef(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new(queue_id).unwrap(),
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

fn schema_url(url: &str, schema: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    // The convenience constructor deliberately takes one connection URL. Supplying search_path in that
    // URL keeps its storage and control-plane connections in the same isolated schema.
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

/// The coordinated postgres constructor builds backend + binding control plane internally and yields a
/// working multi-instance owner (acquires + fences) without the client naming either.
#[test]
fn open_postgres_coordinated_builds_a_working_owner() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!("OWED-6 SKIPPED — set FIREWEED_PG_TEST_URL to a live DB");
        return;
    };
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the test clock is after the Unix epoch")
        .as_nanos();
    let queue_id = format!("coordinated-{unique}");
    let schema = format!("fireweed_coordinated_{unique}");
    let mut admin = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .unwrap();
    let isolated_url = schema_url(&url, &schema);
    let queue = qkey(&queue_id);
    let clock = Arc::new(ManualClock(AtomicI64::new(0)));
    let fireweed = fireweed::open_postgres_coordinated(
        &isolated_url,
        clock,
        OwnerId::new("inst-1").unwrap(),
        ControlPlaneConfig::default(),
    )
    .expect("postgres binds the storage epoch, so the coordinated constructor succeeds");

    bo(fireweed.create_queue(qdef(&queue_id))).unwrap();
    bo(fireweed.push(&queue, NewItem::default())).unwrap();
    assert_eq!(bo(fireweed.metrics(&queue)).unwrap().pending, 1);
    // It is a real coordinated owner: ownership resolves to Mine at a granted epoch.
    assert!(matches!(
        bo(fireweed.ownership(&queue)).unwrap(),
        fireweed::Ownership::Mine { epoch: Some(e) } if e >= 1
    ));
    drop(fireweed);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn open_postgres_runtime_async_is_safe_inside_tokio() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!("SKIPPED — set FIREWEED_PG_TEST_URL to a live DB");
        return;
    };
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the test clock is after the Unix epoch")
        .as_nanos();
    let queue_id = format!("async-postgres-{unique}");
    let schema = format!("fireweed_async_{unique}");
    let queue = qkey(&queue_id);
    let fireweed = fireweed::open_postgres_runtime_async(
        PostgresRuntimeConfig {
            url: ConfigSecret::new(url.clone()),
            schema: Some(schema.clone()),
            mode: PostgresMode::Relational,
            node_id: None,
            coordination: None,
        },
        Arc::new(ManualClock(AtomicI64::new(0))),
    )
    .await
    .expect("async PostgreSQL construction runs off the Tokio runtime thread");
    fireweed.create_queue(qdef(&queue_id)).await.unwrap();
    fireweed.push(&queue, NewItem::default()).await.unwrap();
    assert_eq!(fireweed.metrics(&queue).await.unwrap().pending, 1);
    drop(fireweed);
    tokio::task::spawn_blocking(move || {
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
    })
    .await
    .unwrap();
}
