use futures::executor::block_on;
use postgres::NoTls;
use pqueue_conformance::{qdef, shard};
use pqueue_core::{LeaseToken, UtcTimestamp, WorkerId};
use pqueue_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, CommandPosition, ControlPlaneStore, FinalizeKind,
    FinalizeOutcome, FinalizePort, LogStore, ProjectionRead, PushPort, PushSpec,
};
use pqueue_postgres::composed_postgres_relational_in_schema;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn queue_definition(emit_change_records: bool) -> pqueue_core::QueueDefinition {
    let mut definition = qdef();
    definition.emit_change_records = emit_change_records;
    definition.terminal_retention_ms = 1;
    definition
}

fn claim_req() -> ClaimRequest {
    ClaimRequest {
        shard: shard(),
        worker_id: WorkerId::new("worker-1").unwrap(),
        max_items: 1,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(500),
        now: ts(10),
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    }
}

fn fresh_schema(tag: &str) -> String {
    format!("pq_rel_term_{}_{}", std::process::id(), tag)
}

fn open(url: &str, schema: &str) -> pqueue_postgres::ComposedPostgresRelationalBackend {
    composed_postgres_relational_in_schema(url, schema)
        .expect("open composed postgres-relational db")
}

#[test]
fn reap_waits_for_emission_cursor_on_opted_in_queue() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES UNIFIED COMPOSITION SKIPPED (reap_waits_for_emission_cursor_on_opted_in_queue) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = fresh_schema("in");
    let mut c = postgres::Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop schema");
    drop(c);

    let backend = open(&url, &schema);
    block_on(backend.create_queue(queue_definition(true))).unwrap();

    let ids = block_on(backend.push(&shard(), vec![PushSpec::default()], ts(0), None)).unwrap();
    let item_id = ids[0];

    block_on(backend.claim(claim_req())).unwrap();
    block_on(backend.finalize(
        &shard(),
        vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
        ts(2),
        None,
    ))
    .unwrap();

    let mut log = backend.with_log(|log| log.clone());
    log.set_emission_cursor(&shard(), CommandPosition::new(shard(), 0, 1))
        .unwrap();

    assert_eq!(
        backend
            .reap_terminal_items(&shard(), ts(3), 1, true)
            .unwrap(),
        0,
        "retention-elapsed but cursor-behind must not reap"
    );
    assert_eq!(block_on(backend.metrics(&shard())).unwrap().complete, 1);

    log.set_emission_cursor(&shard(), CommandPosition::new(shard(), 0, 2))
        .unwrap();
    assert_eq!(
        backend
            .reap_terminal_items(&shard(), ts(5), 1, true)
            .unwrap(),
        1,
        "retention-elapsed and cursor-passed must reap"
    );
    assert_eq!(block_on(backend.metrics(&shard())).unwrap().complete, 0);
}

#[test]
fn reap_ignores_emission_cursor_for_opted_out_queue() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES UNIFIED COMPOSITION SKIPPED (reap_ignores_emission_cursor_for_opted_out_queue) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = fresh_schema("out");
    let mut c = postgres::Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop schema");
    drop(c);

    let backend = open(&url, &schema);
    block_on(backend.create_queue(queue_definition(false))).unwrap();

    let ids = block_on(backend.push(&shard(), vec![PushSpec::default()], ts(0), None)).unwrap();
    let item_id = ids[0];

    block_on(backend.claim(claim_req())).unwrap();
    block_on(backend.finalize(
        &shard(),
        vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
        ts(2),
        None,
    ))
    .unwrap();

    let mut log = backend.with_log(|log| log.clone());
    log.set_emission_cursor(&shard(), CommandPosition::new(shard(), 0, 1))
        .unwrap();

    assert_eq!(
        backend
            .reap_terminal_items(&shard(), ts(3), 1, false)
            .unwrap(),
        1,
        "opted-out queues reap on retention alone"
    );
    assert_eq!(block_on(backend.metrics(&shard())).unwrap().complete, 0);
}
