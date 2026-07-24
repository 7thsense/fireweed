use std::sync::{Arc, Barrier};

use fireweed_conformance::{claim_req, qdef, shard, ts};
use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, GateKeyPolicy, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition, RecurrenceMode,
    RecurrencePolicy, RetryPolicy,
};
use fireweed_engine::{
    ClaimPort, ControlPlaneStore, CreateQueueOutcome, EngineError, EngineResult, PushPort, PushSpec,
};
use fireweed_sqlite::SqliteRelationalBackend;
use rusqlite::Connection;

fn db_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "fireweed-relational-create-or-read-{tag}-{}.db",
            std::process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

fn remove_db(path: &str) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

fn rich_definition() -> QueueDefinition {
    let mut definition = qdef();
    definition.priority_model = PriorityModel {
        kind: PriorityModelKind::Text,
        direction: PriorityDirection::Descending,
        tie_breaker: PriorityTieBreaker::ClientItemKey,
    };
    definition.ordering_mode = OrderingMode::BoundedRelaxed;
    definition.max_rank_error = 7;
    definition.progress_bound_ms = 12_345;
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(4);
    definition.eligibility_policy.max_gates_per_request = Some(9);
    definition.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(30_000),
        on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
        max_cohort_size: Some(12),
    });
    definition.recurrence = RecurrencePolicy {
        mode: RecurrenceMode::Recurring,
        until: Some(ts(8_000)),
    };
    definition.request_id_retention_ms = 71_000;
    definition.client_item_key_retention_ms = 72_000;
    definition.terminal_retention_ms = 73_000;
    definition.max_lease_duration_ms = 74_000;
    definition.retry_policy = RetryPolicy { max_attempts: 11 };
    definition.max_push_batch_size = 17;
    definition.max_claim_batch_size = 13;
    definition.max_eligible_group_size = Some(8);
    definition.emit_change_records = false;
    definition
}

fn durable_definition(path: &str) -> QueueDefinition {
    let conn = Connection::open(path).unwrap();
    let encoded: String = conn
        .query_row(
            "SELECT definition FROM queues WHERE tenant=?1 AND queue=?2",
            [shard().tenant_id.as_str(), shard().queue_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&encoded).unwrap()
}

fn concurrent_creates(
    path: &str,
    definitions: Vec<QueueDefinition>,
) -> Vec<(SqliteRelationalBackend, EngineResult<CreateQueueOutcome>)> {
    let barrier = Arc::new(Barrier::new(definitions.len()));
    let handles: Vec<_> = definitions
        .into_iter()
        .map(|definition| {
            let backend = SqliteRelationalBackend::open(path).unwrap();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let outcome = runtime.block_on(backend.create_queue(definition));
                (backend, outcome)
            })
        })
        .collect();
    handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect()
}

#[tokio::test]
async fn relational_sqlite_create_returns_authoritative_rich_definition() {
    let path = db_path("rich");
    remove_db(&path);
    let definition = rich_definition();
    let backend = SqliteRelationalBackend::open(&path).unwrap();

    let outcome = backend.create_queue(definition.clone()).await.unwrap();

    assert!(outcome.created);
    assert_eq!(outcome.definition, definition);
    assert_eq!(durable_definition(&path), definition);
    drop(backend);
    remove_db(&path);
}

#[test]
fn relational_sqlite_concurrent_compatible_create_has_one_winner_no_overwrite() {
    let path = db_path("compatible-race");
    remove_db(&path);
    let definition = rich_definition();
    let attempts = concurrent_creates(&path, vec![definition.clone(); 8]);

    let mut winners = 0;
    for (_, outcome) in attempts {
        let outcome = outcome.unwrap();
        winners += usize::from(outcome.created);
        assert_eq!(outcome.definition, definition);
    }
    assert_eq!(winners, 1, "exactly one insert may win");
    assert_eq!(durable_definition(&path), definition);
    remove_db(&path);
}

#[test]
fn relational_sqlite_concurrent_incompatible_create_conflicts() {
    let path = db_path("incompatible-race");
    remove_db(&path);
    let first = rich_definition();
    let mut second = first.clone();
    second.progress_bound_ms += 1;
    second.max_claim_batch_size += 1;

    let attempts = concurrent_creates(&path, vec![first.clone(), second.clone()]);
    let mut winner = None;
    let mut conflicts = 0;
    for (_, outcome) in attempts {
        match outcome {
            Ok(outcome) => {
                assert!(outcome.created);
                winner = Some(outcome.definition);
            }
            Err(EngineError::QueueDefinitionConflict) => conflicts += 1,
            Err(error) => panic!("unexpected create error: {error:?}"),
        }
    }
    assert_eq!(conflicts, 1);
    let winner = winner.expect("one durable winner");
    assert!(winner == first || winner == second);
    assert_eq!(durable_definition(&path), winner);
    remove_db(&path);
}

#[tokio::test]
async fn relational_sqlite_create_loser_can_push_claim_and_reopen() {
    let path = db_path("loser-immediate-use");
    remove_db(&path);
    let definition = rich_definition();
    let winner = SqliteRelationalBackend::open(&path).unwrap();
    let loser = SqliteRelationalBackend::open(&path).unwrap();

    assert!(
        winner
            .create_queue(definition.clone())
            .await
            .unwrap()
            .created
    );
    let loser_outcome = loser.create_queue(definition.clone()).await.unwrap();
    assert!(!loser_outcome.created);
    assert_eq!(loser_outcome.definition, definition);

    let pushed = loser
        .push(&shard(), vec![PushSpec::default()], ts(1), None)
        .await
        .unwrap();
    let claimed = loser.claim(claim_req(1, 100, 2)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].item_id, pushed[0]);
    drop(winner);
    drop(loser);

    let reopened = SqliteRelationalBackend::open(&path).unwrap();
    assert_eq!(
        reopened.queue_definition(&shard()).await.unwrap(),
        definition
    );
    drop(reopened);
    remove_db(&path);
}
