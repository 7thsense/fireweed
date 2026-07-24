//! Postgres-specific durability: the projection is a derived view; the LOG (in postgres tables) is the
//! source of truth. These tests reconnect to the SAME schema and assert the committed state is
//! reconstructed by replaying the durable log — the property the shared conformance suite (a fresh schema
//! per scenario) cannot exercise. Env-gated on `PQUEUE_PG_TEST_URL`; LOUD skip if absent.

use std::collections::{BTreeMap, HashSet};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axon_esf::IndexDef;
use fireweed_conformance::{claim_req, commit, envelope, item, qdef, qkey};
use fireweed_core::{
    ClientItemKey, CohortOnIncomplete, CohortPolicy, EligibilityPolicy, EntitySchemaDocument,
    GateKeyPolicy, IndexDeclaration, IndexSpec, IndexType, MetadataValue, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition,
    QueueIndex, RecurrenceMode, RecurrencePolicy, RetryPolicy,
};
use fireweed_engine::{
    ClaimPort, ControlPlaneStore, EngineError, ProjectionRead, PushCommand, PushPort, PushSpec,
    QueueCommand,
};
use fireweed_postgres::{PostgresBackend, PostgresRelationalBackend};

fn pg_url() -> Option<String> {
    std::env::var("PQUEUE_PG_TEST_URL").ok()
}

fn fresh_schema(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_dura_{}_{}_{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

#[test]
fn projection_rebuilds_from_durable_log_on_reconnect() {
    let Some(url) = pg_url() else {
        eprintln!("POSTGRES DURABILITY SKIPPED (rebuild) — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    futures::executor::block_on(projection_rebuilds_from_durable_log_on_reconnect_inner(url));
}

async fn projection_rebuilds_from_durable_log_on_reconnect_inner(url: String) {
    let schema = fresh_schema("reopen");

    // Session 1: create the queue, push three items, claim the highest-priority one.
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("connect");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![
                        item("1", "ka", 30),
                        item("2", "kb", 10),
                        item("3", "kc", 20),
                    ],
                }),
                vec![],
            ),
        )
        .await;
        // Claims "b" (priority 10, lowest = highest priority under ascending).
        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(claimed.items.len(), 1);
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!((m.pending, m.leased), (2, 1));
    } // backend dropped — only the durable postgres rows remain.

    // Session 2: RECONNECT to the same schema. The in-memory projection is gone; it must be rebuilt from
    // the log.
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("reconnect");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (2, 1),
            "reconnected projection must reflect the 3 pushes + 1 claim replayed from the durable log"
        );
        // The still-eligible items are the two unclaimed ones, in priority order (c=20 before a=30).
        let elig = b
            .select_eligible(
                &fireweed_conformance::shard(),
                fireweed_conformance::ts(200),
                10,
            )
            .await
            .unwrap();
        let ids: Vec<u64> = elig.iter().map(|i| i.as_u64()).collect();
        assert_eq!(
            ids,
            vec![3, 1],
            "eligibility order survives the rebuild (c=prio20 before a=prio30)"
        );
    }
}

#[test]
fn orchestration_writes_after_reconnect_do_not_collide() {
    let Some(url) = pg_url() else {
        eprintln!("POSTGRES DURABILITY SKIPPED (recollide) — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    futures::executor::block_on(orchestration_writes_after_reconnect_do_not_collide_inner(
        url,
    ));
}

async fn orchestration_writes_after_reconnect_do_not_collide_inner(url: String) {
    // `cmd_seq` is restored past the highest replayed `pg-N`, so a claim AFTER a reconnect mints a fresh
    // command id and commits durably (a colliding id would fail the PK / corrupt the log).
    let schema = fresh_schema("recollide");
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("connect");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("1", "ka", 5), item("2", "kb", 9)],
                }),
                vec![],
            ),
        )
        .await;
        // A claim goes through make_envelope -> "pg-0" durably.
        b.claim(claim_req(1, 500, 100)).await.unwrap();
    }
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("reconnect");
        // Claim again post-reconnect: must succeed (fresh id, no collision) and lease the remaining item.
        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(
            claimed.items.len(),
            1,
            "second item claimable after reconnect"
        );
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (0, 2),
            "both items leased across the two sessions"
        );
    }
}

#[test]
fn atomic_queue_create_child_process() {
    if std::env::var("PQUEUE_PG_ATOMIC_CREATE_CHILD")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let url = std::env::var("PQUEUE_PG_TEST_URL").expect("child url");
    let schema = std::env::var("PQUEUE_PG_ATOMIC_SCHEMA").expect("child schema");
    let backend = std::env::var("PQUEUE_PG_ATOMIC_BACKEND").expect("child backend");
    let child_id = std::env::var("PQUEUE_PG_ATOMIC_CHILD_ID").expect("child id");
    let incompatible = std::env::var("PQUEUE_PG_ATOMIC_INCOMPATIBLE")
        .ok()
        .as_deref()
        == Some("1");
    let exercise_loser = std::env::var("PQUEUE_PG_ATOMIC_EXERCISE_LOSER")
        .ok()
        .as_deref()
        == Some("1");

    let outcome = futures::executor::block_on(child_create_attempt(
        &url,
        &schema,
        &backend,
        &child_id,
        incompatible,
        exercise_loser,
    ));
    record_child_result(&url, &schema, &child_id, outcome);
}

#[test]
fn postgres_queue_create_is_atomic_across_processes() {
    let url = pg_url().expect(
        "mandatory PostgreSQL atomic-create gate requires PQUEUE_PG_TEST_URL to point at a live DB",
    );

    run_atomic_create_process_scenario(&url, "native");
    run_atomic_create_process_scenario(&url, "relational");
    run_native_failed_hydration_retry_scenario(&url);
}

struct ChildAttempt {
    outcome: &'static str,
    created: bool,
    definition: QueueDefinition,
    push_claim_ok: bool,
}

async fn child_create_attempt(
    url: &str,
    schema: &str,
    backend: &str,
    child_id: &str,
    incompatible: bool,
    exercise_loser: bool,
) -> Result<ChildAttempt, EngineError> {
    let definition = if incompatible {
        incompatible_qdef()
    } else {
        rich_qdef()
    };
    match backend {
        "native" => {
            let backend = PostgresBackend::connect_in_schema(url, schema).expect("connect native");
            wait_for_parent_release(url, schema, child_id);
            match backend.create_queue(definition).await {
                Ok(outcome) => {
                    let push_claim_ok = if outcome.created || !exercise_loser {
                        false
                    } else {
                        verify_seeded_work_native(&backend).await?
                    };
                    Ok(ChildAttempt {
                        outcome: "ok",
                        created: outcome.created,
                        definition: outcome.definition,
                        push_claim_ok,
                    })
                }
                Err(EngineError::QueueDefinitionConflict) => Ok(ChildAttempt {
                    outcome: "conflict",
                    created: false,
                    definition: backend.queue_definition(&qkey()).await?,
                    push_claim_ok: false,
                }),
                Err(error) => Err(error),
            }
        }
        "relational" => {
            let backend = PostgresRelationalBackend::connect_in_schema(url, schema)
                .expect("connect relational");
            wait_for_parent_release(url, schema, child_id);
            match backend.create_queue(definition).await {
                Ok(outcome) => {
                    let push_claim_ok = if outcome.created || !exercise_loser {
                        false
                    } else {
                        verify_seeded_work_relational(&backend).await?
                    };
                    Ok(ChildAttempt {
                        outcome: "ok",
                        created: outcome.created,
                        definition: outcome.definition,
                        push_claim_ok,
                    })
                }
                Err(EngineError::QueueDefinitionConflict) => Ok(ChildAttempt {
                    outcome: "conflict",
                    created: false,
                    definition: backend.queue_definition(&qkey()).await?,
                    push_claim_ok: false,
                }),
                Err(error) => Err(error),
            }
        }
        other => panic!("unknown backend {other}"),
    }
}

fn rich_push(key: &str, status: &str) -> PushSpec {
    let mut fields = BTreeMap::new();
    fields.insert("customer".to_string(), format!("customer-{key}").into());
    fields.insert("region".to_string(), b"north".to_vec().into());
    PushSpec {
        client_item_key: Some(ClientItemKey::new(key).expect("valid key")),
        entity: Some(serde_json::json!({"status": status, "attempt": 1})),
        fields,
        ..PushSpec::default()
    }
}

async fn verify_seeded_work_native(backend: &PostgresBackend) -> Result<bool, EngineError> {
    verify_seeded_reads_native(backend).await?;
    let new_ids = backend
        .push(
            &qkey(),
            vec![rich_push("new", "new")],
            fireweed_conformance::ts(1),
            None,
        )
        .await?;
    let all = backend
        .live_items(
            &qkey(),
            &["seed-a", "seed-b", "new"]
                .into_iter()
                .map(|key| ClientItemKey::new(key).expect("valid key"))
                .collect::<Vec<_>>(),
        )
        .await?;
    let all_ids = all
        .iter()
        .map(|item| item.as_ref().expect("live item").item_id)
        .collect::<HashSet<_>>();
    let claimed = backend.claim(claim_req(3, 500, 100)).await?;
    Ok(new_ids.len() == 1
        && all_ids.len() == 3
        && all_ids.contains(&new_ids[0])
        && claimed.items.len() == 3
        && claimed
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<HashSet<_>>()
            == all_ids)
}

async fn verify_seeded_reads_native(backend: &PostgresBackend) -> Result<(), EngineError> {
    let seeded = backend
        .live_items(
            &qkey(),
            &["seed-a", "seed-b"]
                .into_iter()
                .map(|key| ClientItemKey::new(key).expect("valid key"))
                .collect::<Vec<_>>(),
        )
        .await?;
    if seeded.iter().any(Option::is_none) {
        return Err(EngineError::Storage(
            "loser did not hydrate both durable seed items".to_string(),
        ));
    }
    Ok(())
}

async fn verify_seeded_work_relational(
    backend: &PostgresRelationalBackend,
) -> Result<bool, EngineError> {
    let new_ids = backend
        .push(
            &qkey(),
            vec![rich_push("new", "new")],
            fireweed_conformance::ts(1),
            None,
        )
        .await?;
    let all = backend
        .live_items(
            &qkey(),
            &["seed-a", "seed-b", "new"]
                .into_iter()
                .map(|key| ClientItemKey::new(key).expect("valid key"))
                .collect::<Vec<_>>(),
        )
        .await?;
    let all_ids = all
        .iter()
        .map(|item| item.as_ref().expect("live item").item_id)
        .collect::<HashSet<_>>();
    let claimed = backend.claim(claim_req(3, 500, 100)).await?;
    Ok(new_ids.len() == 1
        && all_ids.len() == 3
        && all_ids.contains(&new_ids[0])
        && claimed.items.len() == 3
        && claimed
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<HashSet<_>>()
            == all_ids)
}

fn rich_qdef() -> QueueDefinition {
    let mut blockers = BTreeMap::new();
    blockers.insert(
        "blocked".to_string(),
        vec![MetadataValue::String("yes".to_string())],
    );
    QueueDefinition {
        priority_model: PriorityModel {
            kind: PriorityModelKind::Text,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::ClientItemKey,
        },
        ordering_mode: OrderingMode::BoundedRelaxed,
        max_rank_error: 7,
        progress_bound_ms: 12_345,
        eligibility_policy: EligibilityPolicy {
            metadata_blockers: blockers,
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(3),
            max_gates_per_request: Some(5),
        },
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(9_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(8),
        }),
        recurrence: RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: Some(fireweed_conformance::ts(4_242)),
        },
        request_id_retention_ms: 11_000,
        client_item_key_retention_ms: 12_000,
        terminal_retention_ms: 13_000,
        max_lease_duration_ms: 14_000,
        retry_policy: RetryPolicy { max_attempts: 9 },
        max_push_batch_size: 17,
        max_claim_batch_size: 19,
        max_eligible_group_size: Some(23),
        secondary_indexes: vec![IndexSpec {
            name: "by_customer".to_string(),
            fields: vec!["customer".to_string(), "region".to_string()],
            unique: true,
        }],
        entity_schema: Some(
            serde_json::from_value::<EntitySchemaDocument>(serde_json::json!({
                "entity_schema": {
                    "type": "object",
                    "required": ["status"],
                    "properties": {
                        "status": {"type": "string"},
                        "attempt": {"type": "integer"}
                    }
                }
            }))
            .expect("valid entity schema"),
        ),
        typed_indexes: vec![QueueIndex {
            name: "by_status".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "status".to_string(),
                index_type: IndexType::String,
                unique: false,
            }),
        }],
        emit_change_records: false,
        ..qdef()
    }
}

fn incompatible_qdef() -> QueueDefinition {
    QueueDefinition {
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        ..rich_qdef()
    }
}

fn run_atomic_create_process_scenario(url: &str, backend: &str) {
    let race_schema = fresh_schema(&format!("atomic_race_{backend}"));
    init_atomic_schema(url, &race_schema);
    bootstrap_atomic_backend(url, &race_schema, backend);

    let child_count = 6;
    let mut children = (0..child_count)
        .map(|index| spawn_atomic_child(url, &race_schema, backend, index, false, false))
        .collect::<Vec<_>>();
    wait_for_children(url, &race_schema, child_count);
    release_children(url, &race_schema);
    for child in &mut children {
        let status = child.wait().expect("wait child");
        assert!(status.success(), "compatible child failed with {status}");
    }

    let durable_definition = read_durable_definition(url, &race_schema);
    let rows = read_atomic_results(url, &race_schema);
    assert_eq!(rows.len(), child_count);
    assert_eq!(
        rows.iter().filter(|row| row.created).count(),
        1,
        "{backend}: exactly one process must win create"
    );
    assert!(
        rows.iter()
            .all(|row| row.outcome == "ok" && row.definition == durable_definition),
        "{backend}: child outcomes must carry the durable stored definition"
    );
    assert_eq!(durable_definition, rich_qdef());
    drop_schema(url, &race_schema);

    let handoff_schema = fresh_schema(&format!("atomic_handoff_{backend}"));
    init_atomic_schema(url, &handoff_schema);
    bootstrap_atomic_backend(url, &handoff_schema, backend);
    let mut exercise_loser =
        spawn_atomic_child(url, &handoff_schema, backend, child_count, false, true);
    wait_for_children(url, &handoff_schema, 1);
    seed_winner(url, &handoff_schema, backend);
    release_children(url, &handoff_schema);
    let status = exercise_loser.wait().expect("wait exercise child");
    assert!(
        status.success(),
        "losing-handle exercise child failed with {status}"
    );
    let rows = read_atomic_results(url, &handoff_schema);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "ok");
    assert!(
        !rows[0].created && rows[0].definition == rich_qdef() && rows[0].push_claim_ok,
        "{backend}: a compatible uncached loser must decode the winner, read seed work, mint a unique id, and claim all work"
    );
    drop_schema(url, &handoff_schema);

    let conflict_schema = fresh_schema(&format!("atomic_conflict_{backend}"));
    init_atomic_schema(url, &conflict_schema);
    bootstrap_atomic_backend(url, &conflict_schema, backend);
    let mut incompatible =
        spawn_atomic_child(url, &conflict_schema, backend, child_count + 1, true, false);
    wait_for_children(url, &conflict_schema, 1);
    seed_winner(url, &conflict_schema, backend);
    release_children(url, &conflict_schema);
    let status = incompatible.wait().expect("wait incompatible child");
    assert!(status.success(), "incompatible child failed with {status}");
    let rows = read_atomic_results(url, &conflict_schema);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "conflict");
    assert_eq!(
        rows[0].definition,
        rich_qdef(),
        "{backend}: queue_definition must expose the decoded durable winner after conflict"
    );
    drop_schema(url, &conflict_schema);
}

fn seed_winner(url: &str, schema: &str, backend: &str) {
    futures::executor::block_on(async {
        match backend {
            "native" => {
                let winner =
                    PostgresBackend::connect_in_schema(url, schema).expect("connect native winner");
                assert!(winner.create_queue(rich_qdef()).await.unwrap().created);
                let ids = winner
                    .push(
                        &qkey(),
                        vec![rich_push("seed-a", "seed"), rich_push("seed-b", "seed")],
                        fireweed_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(ids.len(), 2);
                assert_ne!(ids[0], ids[1]);
            }
            "relational" => {
                let winner = PostgresRelationalBackend::connect_in_schema(url, schema)
                    .expect("connect relational winner");
                assert!(winner.create_queue(rich_qdef()).await.unwrap().created);
                let ids = winner
                    .push(
                        &qkey(),
                        vec![rich_push("seed-a", "seed"), rich_push("seed-b", "seed")],
                        fireweed_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(ids.len(), 2);
                assert_ne!(ids[0], ids[1]);
            }
            other => panic!("unknown backend {other}"),
        }
    });
}

fn run_native_failed_hydration_retry_scenario(url: &str) {
    let schema = fresh_schema("atomic_native_retry");
    let loser = PostgresBackend::connect_in_schema(url, &schema).expect("connect empty loser");
    seed_winner(url, &schema, "native");

    let original = corrupt_first_log_envelope(url, &schema);
    let first = futures::executor::block_on(loser.create_queue(rich_qdef()));
    assert!(
        matches!(first, Err(EngineError::Storage(_))),
        "corrupt durable replay must fail hydration"
    );
    assert!(matches!(
        futures::executor::block_on(loser.queue_definition(&qkey())),
        Err(EngineError::NotFound)
    ));

    restore_first_log_envelope(url, &schema, &original);
    let retry = futures::executor::block_on(loser.create_queue(rich_qdef())).unwrap();
    assert!(!retry.created);
    assert_eq!(retry.definition, rich_qdef());
    assert!(futures::executor::block_on(verify_seeded_work_native(&loser)).unwrap());
    drop_schema(url, &schema);
}

fn corrupt_first_log_envelope(url: &str, schema: &str) -> String {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect corruption");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set corruption search_path");
    let original: String = client
        .query_one(
            "SELECT envelope FROM log_entries WHERE tenant='t1' AND queue='q1' ORDER BY epoch,seq LIMIT 1",
            &[],
        )
        .expect("read original envelope")
        .get(0);
    client
        .execute(
            "UPDATE log_entries SET envelope='{not-json' WHERE tenant='t1' AND queue='q1' AND (epoch,seq)=(SELECT epoch,seq FROM log_entries WHERE tenant='t1' AND queue='q1' ORDER BY epoch,seq LIMIT 1)",
            &[],
        )
        .expect("corrupt envelope");
    original
}

fn restore_first_log_envelope(url: &str, schema: &str, original: &str) {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect restore");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set restore search_path");
    client
        .execute(
            "UPDATE log_entries SET envelope=$1 WHERE tenant='t1' AND queue='q1' AND (epoch,seq)=(SELECT epoch,seq FROM log_entries WHERE tenant='t1' AND queue='q1' ORDER BY epoch,seq LIMIT 1)",
            &[&original],
        )
        .expect("restore envelope");
}

fn bootstrap_atomic_backend(url: &str, schema: &str, backend: &str) {
    match backend {
        "native" => {
            let _ = PostgresBackend::connect_in_schema(url, schema).expect("bootstrap native");
        }
        "relational" => {
            let _ = PostgresRelationalBackend::connect_in_schema(url, schema)
                .expect("bootstrap relational");
        }
        other => panic!("unknown backend {other}"),
    }
}

struct AtomicResult {
    outcome: String,
    created: bool,
    definition: QueueDefinition,
    push_claim_ok: bool,
}

fn init_atomic_schema(url: &str, schema: &str) {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect init");
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; \
             CREATE SCHEMA {schema}; \
             SET search_path TO {schema}; \
             CREATE TABLE atomic_create_barrier(child_id TEXT PRIMARY KEY, released BOOLEAN NOT NULL DEFAULT false); \
             CREATE TABLE atomic_create_results( \
               child_id TEXT PRIMARY KEY, outcome TEXT NOT NULL, created BOOLEAN NOT NULL, \
               definition TEXT NOT NULL, push_claim_ok BOOLEAN NOT NULL \
             );"
        ))
        .expect("init atomic schema");
}

fn spawn_atomic_child(
    url: &str,
    schema: &str,
    backend: &str,
    index: usize,
    incompatible: bool,
    exercise_loser: bool,
) -> std::process::Child {
    Command::new(std::env::current_exe().expect("current exe"))
        .arg("--exact")
        .arg("atomic_queue_create_child_process")
        .arg("--nocapture")
        .env("PQUEUE_PG_TEST_URL", url)
        .env("PQUEUE_PG_ATOMIC_CREATE_CHILD", "1")
        .env("PQUEUE_PG_ATOMIC_SCHEMA", schema)
        .env("PQUEUE_PG_ATOMIC_BACKEND", backend)
        .env("PQUEUE_PG_ATOMIC_CHILD_ID", format!("{backend}-{index}"))
        .env(
            "PQUEUE_PG_ATOMIC_INCOMPATIBLE",
            if incompatible { "1" } else { "0" },
        )
        .env(
            "PQUEUE_PG_ATOMIC_EXERCISE_LOSER",
            if exercise_loser { "1" } else { "0" },
        )
        .spawn()
        .expect("spawn child")
}

fn wait_for_parent_release(url: &str, schema: &str, child_id: &str) {
    let mut client =
        postgres::Client::connect(url, postgres::NoTls).expect("connect child barrier");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set child search_path");
    client
        .execute(
            "INSERT INTO atomic_create_barrier(child_id,released) VALUES($1,false)",
            &[&child_id],
        )
        .expect("mark child ready");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let released: bool = client
            .query_one(
                "SELECT released FROM atomic_create_barrier WHERE child_id=$1",
                &[&child_id],
            )
            .expect("read release")
            .get(0);
        if released {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for release");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_children(url: &str, schema: &str, child_count: usize) {
    let mut client =
        postgres::Client::connect(url, postgres::NoTls).expect("connect parent barrier");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set parent search_path");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let ready: i64 = client
            .query_one("SELECT COUNT(*) FROM atomic_create_barrier", &[])
            .expect("count ready")
            .get(0);
        if ready as usize == child_count {
            break;
        }
        assert!(Instant::now() < deadline, "timed out waiting for children");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn release_children(url: &str, schema: &str) {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect release");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set release search_path");
    client
        .execute("UPDATE atomic_create_barrier SET released=true", &[])
        .expect("release children");
}

fn record_child_result(
    url: &str,
    schema: &str,
    child_id: &str,
    result: Result<ChildAttempt, EngineError>,
) {
    let (outcome, created, definition, push_claim_ok) = match result {
        Ok(result) => (
            result.outcome.to_string(),
            result.created,
            serde_json::to_string(&result.definition).expect("serialize definition"),
            result.push_claim_ok,
        ),
        Err(error) => panic!("unexpected child error: {error:?}"),
    };
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect child result");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set result search_path");
    client
        .execute(
            "INSERT INTO atomic_create_results(child_id,outcome,created,definition,push_claim_ok) \
             VALUES($1,$2,$3,$4,$5)",
            &[&child_id, &outcome, &created, &definition, &push_claim_ok],
        )
        .expect("record child result");
}

fn read_durable_definition(url: &str, schema: &str) -> QueueDefinition {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect durable read");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set durable search_path");
    let definition: String = client
        .query_one(
            "SELECT definition FROM queues WHERE tenant='t1' AND queue='q1'",
            &[],
        )
        .expect("read durable definition")
        .get(0);
    serde_json::from_str(&definition).expect("decode durable definition")
}

fn read_atomic_results(url: &str, schema: &str) -> Vec<AtomicResult> {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect results");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set result search_path");
    client
        .query(
            "SELECT outcome, created, definition, push_claim_ok FROM atomic_create_results ORDER BY child_id",
            &[],
        )
        .expect("read results")
        .into_iter()
        .map(|row| {
            let definition: String = row.get(2);
            AtomicResult {
                outcome: row.get(0),
                created: row.get(1),
                definition: serde_json::from_str(&definition).expect("decode result definition"),
                push_claim_ok: row.get(3),
            }
        })
        .collect()
}

fn drop_schema(url: &str, schema: &str) {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect cleanup");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop test schema");
}
