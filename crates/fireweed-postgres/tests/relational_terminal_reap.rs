use fireweed_conformance::{qdef, shard};
use fireweed_core::{LeaseToken, UtcTimestamp, WorkerId};
use fireweed_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, CommandPosition, ControlPlaneStore, FinalizeKind,
    FinalizeOutcome, FinalizePort, ProjectionRead, PushPort, PushSpec, ReclaimDriver,
};
use fireweed_postgres::PostgresRelationalBackend;
use futures::executor::block_on;
use postgres::NoTls;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn queue_definition(emit_change_records: bool) -> fireweed_core::QueueDefinition {
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
    format!("fireweed_rel_term_{}_{}", std::process::id(), tag)
}

fn open(url: &str, schema: &str) -> PostgresRelationalBackend {
    PostgresRelationalBackend::connect_in_schema(url, schema)
        .expect("open postgres relational backend")
}

fn set_emission_cursor(url: &str, schema: &str, position: CommandPosition) {
    let mut client = postgres::Client::connect(url, NoTls).expect("connect");
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .expect("set schema");
    let (tenant, queue) = {
        let shard = position.queue.clone();
        (
            shard.tenant_id.as_str().to_string(),
            shard.queue_id.as_str().to_string(),
        )
    };
    client
        .execute(
            "INSERT INTO relational_emission_cursor(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
             ON CONFLICT (tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq",
            &[
                &tenant,
                &queue,
                &(position.backend_epoch as i64),
                &(position.sequence as i64),
            ],
        )
        .expect("write cursor");
}

fn terminal_position(url: &str, schema: &str, item_id: &str) -> CommandPosition {
    let mut client = postgres::Client::connect(url, NoTls).expect("connect");
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .expect("set schema");
    let row = client
        .query_one(
            "SELECT c.assignment_epoch, c.seq FROM fireweed_items i \
             JOIN fireweed_commands c ON c.tenant=i.tenant_id AND c.queue=i.queue_id \
                 AND c.seq=i.last_command_sequence \
             WHERE i.tenant_id=$1 AND i.queue_id=$2 AND i.item_id=$3",
            &[&"t1", &"q1", &item_id],
        )
        .expect("read terminal command position");
    let epoch: i64 = row.get(0);
    let sequence: i64 = row.get(1);
    CommandPosition::new(shard(), epoch as u64, sequence as u64)
}

#[test]
fn postgres_terminal_reap_sweeps_with_cursor_conjunction() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");
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

    let terminal_position = terminal_position(&url, &schema, &item_id.to_string());
    let behind_sequence = terminal_position
        .sequence
        .checked_sub(1)
        .expect("a terminal command follows at least one queue command");
    set_emission_cursor(
        &url,
        &schema,
        CommandPosition::new(shard(), terminal_position.backend_epoch, behind_sequence),
    );

    block_on(backend.tick(ts(3))).unwrap();
    assert_eq!(
        block_on(backend.metrics(&shard())).unwrap().complete,
        1,
        "retention-elapsed but cursor-behind must not reap"
    );

    set_emission_cursor(&url, &schema, terminal_position);
    block_on(backend.tick(ts(5))).unwrap();
    assert_eq!(
        block_on(backend.metrics(&shard())).unwrap().complete,
        0,
        "retention-elapsed and cursor-passed must reap"
    );
}

#[test]
fn terminal_reap_opt_out_ignores_cursor() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");
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

    set_emission_cursor(&url, &schema, CommandPosition::new(shard(), 0, 1));

    block_on(backend.tick(ts(3))).unwrap();
    assert_eq!(
        block_on(backend.metrics(&shard())).unwrap().complete,
        0,
        "opted-out queues reap on retention alone"
    );
}
