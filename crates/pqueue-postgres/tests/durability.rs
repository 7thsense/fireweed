//! Postgres-specific durability: the projection is a derived view; the LOG (in postgres tables) is the
//! source of truth. These tests reconnect to the SAME schema and assert the committed state is
//! reconstructed by replaying the durable log — the property the shared conformance suite (a fresh schema
//! per scenario) cannot exercise. Env-gated on `PQUEUE_PG_TEST_URL`; LOUD skip if absent.

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey};
use pqueue_core::{OrderingMode, QueueDefinition};
use pqueue_engine::{
    ClaimPort, ControlPlaneStore, EngineError, ProjectionRead, PushCommand, PushPort, PushSpec,
    QueueCommand,
};
use pqueue_postgres::{PostgresBackend, PostgresRelationalBackend};

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
                &pqueue_conformance::shard(),
                pqueue_conformance::ts(200),
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

    wait_for_parent_release(&url, &schema, &child_id);
    let outcome = futures::executor::block_on(child_create_attempt(
        &url,
        &schema,
        &backend,
        incompatible,
        exercise_loser,
    ));
    record_child_result(&url, &schema, &child_id, outcome);
}

#[test]
fn postgres_queue_create_is_atomic_across_processes() {
    let Some(url) = pg_url() else {
        eprintln!("POSTGRES ATOMIC CREATE SKIPPED — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };

    run_atomic_create_process_scenario(&url, "native");
    run_atomic_create_process_scenario(&url, "relational");
}

async fn child_create_attempt(
    url: &str,
    schema: &str,
    backend: &str,
    incompatible: bool,
    exercise_loser: bool,
) -> Result<(bool, QueueDefinition, bool), EngineError> {
    let definition = if incompatible {
        incompatible_qdef()
    } else {
        qdef()
    };
    match backend {
        "native" => {
            let backend = PostgresBackend::connect_in_schema(url, schema).expect("connect native");
            let outcome = backend.create_queue(definition).await?;
            let push_claim_ok = if outcome.created || !exercise_loser {
                false
            } else {
                push_and_claim_native(&backend).await?
            };
            Ok((outcome.created, outcome.definition, push_claim_ok))
        }
        "relational" => {
            let backend = PostgresRelationalBackend::connect_in_schema(url, schema)
                .expect("connect relational");
            let outcome = backend.create_queue(definition).await?;
            let push_claim_ok = if outcome.created || !exercise_loser {
                false
            } else {
                push_and_claim_relational(&backend).await?
            };
            Ok((outcome.created, outcome.definition, push_claim_ok))
        }
        other => panic!("unknown backend {other}"),
    }
}

async fn push_and_claim_native(backend: &PostgresBackend) -> Result<bool, EngineError> {
    backend
        .push(
            &qkey(),
            vec![PushSpec::default()],
            pqueue_conformance::ts(1),
            None,
        )
        .await?;
    let claimed = backend.claim(claim_req(1, 500, 100)).await?;
    Ok(claimed.items.len() == 1)
}

async fn push_and_claim_relational(
    backend: &PostgresRelationalBackend,
) -> Result<bool, EngineError> {
    backend
        .push(
            &qkey(),
            vec![PushSpec::default()],
            pqueue_conformance::ts(1),
            None,
        )
        .await?;
    let claimed = backend.claim(claim_req(1, 500, 100)).await?;
    Ok(claimed.items.len() == 1)
}

fn incompatible_qdef() -> QueueDefinition {
    let mut definition = qdef();
    definition.ordering_mode = OrderingMode::BoundedRelaxed;
    definition
}

fn run_atomic_create_process_scenario(url: &str, backend: &str) {
    let schema = fresh_schema(&format!("atomic_{backend}"));
    init_atomic_schema(url, &schema);
    bootstrap_atomic_backend(url, &schema, backend);

    let child_count = 6;
    let mut children = (0..child_count)
        .map(|index| spawn_atomic_child(url, &schema, backend, index, false, false))
        .collect::<Vec<_>>();
    release_children_when_ready(url, &schema, child_count);
    for child in &mut children {
        let status = child.wait().expect("wait child");
        assert!(status.success(), "compatible child failed with {status}");
    }

    let durable_definition = read_durable_definition(url, &schema);
    let rows = read_atomic_results(url, &schema);
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

    truncate_atomic_coordination(url, &schema);
    let mut exercise_loser = spawn_atomic_child(url, &schema, backend, child_count, false, true);
    release_children_when_ready(url, &schema, 1);
    let status = exercise_loser.wait().expect("wait exercise child");
    assert!(
        status.success(),
        "losing-handle exercise child failed with {status}"
    );
    let rows = read_atomic_results(url, &schema);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "ok");
    assert!(
        !rows[0].created && rows[0].push_claim_ok,
        "{backend}: a compatible losing handle must push and claim immediately"
    );

    truncate_atomic_coordination(url, &schema);
    let mut incompatible = spawn_atomic_child(url, &schema, backend, child_count + 1, true, false);
    release_children_when_ready(url, &schema, 1);
    let status = incompatible.wait().expect("wait incompatible child");
    assert!(status.success(), "incompatible child failed with {status}");
    let rows = read_atomic_results(url, &schema);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "conflict");
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
    definition: String,
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

fn truncate_atomic_coordination(url: &str, schema: &str) {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect truncate");
    client
        .batch_execute(&format!(
            "SET search_path TO {schema}; TRUNCATE atomic_create_barrier, atomic_create_results;"
        ))
        .expect("truncate atomic coordination");
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

fn release_children_when_ready(url: &str, schema: &str, child_count: usize) {
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
    client
        .execute("UPDATE atomic_create_barrier SET released=true", &[])
        .expect("release children");
}

fn record_child_result(
    url: &str,
    schema: &str,
    child_id: &str,
    result: Result<(bool, QueueDefinition, bool), EngineError>,
) {
    let (outcome, created, definition, push_claim_ok) = match result {
        Ok((created, definition, push_claim_ok)) => (
            "ok".to_string(),
            created,
            serde_json::to_string(&definition).expect("serialize definition"),
            push_claim_ok,
        ),
        Err(EngineError::QueueDefinitionConflict) => {
            ("conflict".to_string(), false, String::new(), false)
        }
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

fn read_durable_definition(url: &str, schema: &str) -> String {
    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect durable read");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set durable search_path");
    client
        .query_one(
            "SELECT definition FROM queues WHERE tenant='t1' AND queue='q1'",
            &[],
        )
        .expect("read durable definition")
        .get(0)
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
        .map(|row| AtomicResult {
            outcome: row.get(0),
            created: row.get(1),
            definition: row.get(2),
            push_claim_ok: row.get(3),
        })
        .collect()
}
