use futures::executor::block_on;
use postgres::NoTls;
use pqueue_conformance::{qdef, shard};
use pqueue_core::{LeaseToken, UtcTimestamp, WorkerId};
use pqueue_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, FinalizeKind, FinalizeOutcome,
    FinalizePort, ProjectionRead, PushPort, PushSpec, ReclaimDriver,
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
        eligibility_time: None,
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

    c = postgres::Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("SET search_path TO {schema}"))
        .expect("set schema");
    let terminal_sequence: i64 = c
        .query_one(
            "SELECT last_command_sequence FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
            &[&"t1", &"q1", &item_id.to_string()],
        )
        .unwrap()
        .get(0);
    c.execute(
        "INSERT INTO relational_emission_cursor(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
         ON CONFLICT (tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch,seq=EXCLUDED.seq",
        &[&"t1", &"q1", &0_i64, &(terminal_sequence - 1)],
    )
    .unwrap();

    block_on(backend.tick(ts(3))).unwrap();
    assert_eq!(block_on(backend.metrics(&shard())).unwrap().complete, 1);

    c.execute(
        "UPDATE relational_emission_cursor SET epoch=$3,seq=$4 WHERE tenant=$1 AND queue=$2",
        &[&"t1", &"q1", &0_i64, &terminal_sequence],
    )
    .unwrap();
    block_on(backend.tick(ts(5))).unwrap();
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

    block_on(backend.tick(ts(3))).unwrap();
    assert_eq!(block_on(backend.metrics(&shard())).unwrap().complete, 0);
}
