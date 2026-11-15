//! PostgreSQL-specific checks the shared suite cannot express: committed
//! state survives into a new, independent instance, and two instances on
//! the same storage never lease one item twice. Needs `FIREWEED_PG_TEST_URL`.

use std::collections::BTreeSet;
use std::sync::Arc;

use fireweed_interface::*;
use fireweed_interface_postgres::PostgresDataPlane;
use futures::executor::block_on;

fn url() -> Option<String> {
    std::env::var("FIREWEED_PG_TEST_URL").ok()
}

fn schema() -> SpaceSchema {
    SpaceSchema {
        version: 1,
        request_id_retention_ms: 600_000,
        queues: vec![QueueDecl {
            name: ObjectName::new("work"),
            spec: QueueSpec::basic(PriorityKind::Int64),
            body: BodyKind::Json,
            fields: vec![],
            history: None,
        }],
        tables: vec![],
        event_logs: vec![],
        blobs: None,
        reads: vec![],
    }
}

fn fresh_schema(url: &str, tag: &str) -> String {
    let name = format!("fw_inst_{}_{tag}", std::process::id());
    let mut c = postgres::Client::connect(url, postgres::NoTls).unwrap();
    c.batch_execute(&format!(
        "DROP SCHEMA IF EXISTS {name} CASCADE; CREATE SCHEMA {name}"
    ))
    .unwrap();
    name
}

fn drop_schema(url: &str, name: &str) {
    let mut c = postgres::Client::connect(url, postgres::NoTls).unwrap();
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {name} CASCADE"))
        .unwrap();
}

fn open(url: &str, schema: &str, clock: &Arc<ManualClock>) -> PostgresDataPlane {
    PostgresDataPlane::connect_in_schema(
        url,
        schema,
        clock.clone(),
        fireweed_interface_memory_limits(),
    )
    .unwrap()
}

fn fireweed_interface_memory_limits() -> ImplementationLimits {
    ImplementationLimits {
        max_ops_per_commit: 64,
        max_entries_per_commit: 1000,
        max_request_bytes: 8 << 20,
        max_in_list: 1000,
        max_writer_scan_rows: 10_000,
        max_read_scan_rows: 10_000,
        max_cohort_size: 0,
        max_selector_affected: 10_000,
        max_body_bytes: 1 << 20,
        max_value_bytes: 256 << 10,
        max_set_values: 128,
        max_blob_bytes: 0,
        max_dry_run_count: 100_000,
        max_read_rows: 1000,
        max_claim_batch_size: 1000,
        max_lease_duration_ms: 3_600_000,
        max_request_id_retention_ms: 7 * 86_400_000,
        max_objects_per_space: 256,
    }
}

fn space() -> SpaceKey {
    SpaceKey::new("t", "s")
}

#[test]
fn committed_state_survives_a_new_instance() {
    let Some(url) = url() else { return };
    let name = fresh_schema(&url, "reopen");
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    {
        let a = open(&url, &name, &clock);
        block_on(a.apply_schema(ApplySchemaRequest {
            space: space(),
            schema: schema(),
            expected_version: None,
            min_durability: None,
        }))
        .unwrap();
        let r = block_on(a.enqueue(EnqueueRequest {
            header: WriteHeader::new(space(), "e1"),
            queue: ObjectName::new("work"),
            items: vec![NewItem::new(Some("k1"), PriorityValue::Int(1))],
            on_key_conflict: KeyConflict::Duplicate,
        }))
        .unwrap();
        assert_eq!(r.durability, DurabilityLevel::Local);
    }
    let b = open(&url, &name, &clock);
    let c = block_on(b.claim(ClaimRequest::new(
        WriteHeader::new(space(), "c1"),
        "work",
        "w",
        10,
        60_000,
    )))
    .unwrap();
    assert_eq!(c.items.len(), 1);
    assert_eq!(c.items[0].client_item_key, ClientItemKey::new("k1"));
    // The enqueue's request record survived too: replaying it is a replay.
    let again = block_on(b.enqueue(EnqueueRequest {
        header: WriteHeader::new(space(), "e1"),
        queue: ObjectName::new("work"),
        items: vec![NewItem::new(Some("k1"), PriorityValue::Int(1))],
        on_key_conflict: KeyConflict::Duplicate,
    }))
    .unwrap();
    assert_eq!(again.entries[0].status, EntryStatus::Accepted);
    drop(b);
    drop_schema(&url, &name);
}

#[test]
fn two_instances_never_lease_one_item_twice() {
    let Some(url) = url() else { return };
    let name = fresh_schema(&url, "race");
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    let a = Arc::new(open(&url, &name, &clock));
    let b = Arc::new(open(&url, &name, &clock));
    block_on(a.apply_schema(ApplySchemaRequest {
        space: space(),
        schema: schema(),
        expected_version: None,
        min_durability: None,
    }))
    .unwrap();
    let items = (0..40)
        .map(|i| NewItem::new(Some(&format!("k{i}")), PriorityValue::Int(i)))
        .collect();
    block_on(a.enqueue(EnqueueRequest {
        header: WriteHeader::new(space(), "fill"),
        queue: ObjectName::new("work"),
        items,
        on_key_conflict: KeyConflict::Duplicate,
    }))
    .unwrap();
    let workers: Vec<_> = [a.clone(), b.clone()]
        .into_iter()
        .enumerate()
        .map(|(w, dp)| {
            std::thread::spawn(move || {
                let mut got = vec![];
                for n in 0..20 {
                    let r = block_on(dp.claim(ClaimRequest::new(
                        WriteHeader::new(space(), format!("w{w}-{n}")),
                        "work",
                        &format!("w{w}"),
                        2,
                        60_000,
                    )))
                    .unwrap();
                    got.extend(r.items.into_iter().map(|i| i.item_id));
                }
                got
            })
        })
        .collect();
    let all: Vec<ItemId> = workers
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let distinct: BTreeSet<_> = all.iter().collect();
    assert_eq!(all.len(), 40, "every item claimed once");
    assert_eq!(distinct.len(), all.len(), "an item was leased twice");
    drop((a, b));
    drop_schema(&url, &name);
}
