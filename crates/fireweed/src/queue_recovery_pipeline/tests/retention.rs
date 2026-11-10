//! The cleanup pass the owner's loop offers: when it runs, what it costs and
//! what a failed one does to the requests around it. Counts and outcomes, never
//! time: the store clock is moved by hand.
use super::record_counts::Env;
use super::*;
use fireweed_conformance::queue_contract as suite;
use fireweed_engine::queue_contract::QueueStore;
use std::sync::atomic::Ordering;

const MINUTE: i64 = 60_000_000_000;
const T0: i64 = 100 * 60 * MINUTE;

fn schema() -> QueueSchema {
    suite::schema_with(QueueRetention {
        request_id_nanos: u64::try_from(30 * MINUTE).unwrap(),
        deleted_keys: DeletedKeyPolicy::Reusable,
    })
}
async fn read(
    store: &NativeQueueStore,
    queue: &QueueAddress,
    ids: &[(&str, i64)],
) -> Vec<ReceiptLookup> {
    store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: ids
                .iter()
                .map(|(id, issued_at)| ReceiptReadId {
                    request_id: (*id).into(),
                    issued_at: *issued_at,
                })
                .collect(),
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
}
/// A pass runs after the operation that starts it is answered, and an operation
/// sent after it waits behind it. This reads once to start one and once to wait.
async fn tick(store: &NativeQueueStore, queue: &QueueAddress) {
    read(store, queue, &[]).await;
    read(store, queue, &[]).await;
}
fn insert(queue: &QueueAddress, id: &str, key: &str, issued_at: i64) -> InsertRequest {
    InsertRequest {
        queue: queue.clone(),
        request_id: id.into(),
        mode: InsertMode::InsertOnly,
        items: vec![suite::item(key, "retention", 0, 0)],
        issued_at,
    }
}
fn published(store: &NativeQueueStore) -> u64 {
    store.pipeline_for_test().published.load(Ordering::SeqCst)
}

/// A queue offers one pass per schedule interval of store time, whatever the
/// traffic, and none while the clock stands still.
#[tokio::test]
async fn a_pass_is_offered_once_per_interval_of_store_time_local() {
    let env = Env::new("retention-schedule", false);
    let store = env.open("a.db").await;
    let queue = env.address("schedule");
    env.clock.set(T0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    store
        .insert(insert(&queue, "first", "k1", T0))
        .await
        .unwrap();
    let baseline = published(&store);
    for _ in 0..5 {
        tick(&store, &queue).await;
    }
    assert_eq!(published(&store), baseline, "no time has passed");
    // The interval is a minute of store time.
    env.clock.set(T0 + MINUTE - 1);
    tick(&store, &queue).await;
    assert_eq!(published(&store), baseline);
    env.clock.set(T0 + MINUTE);
    tick(&store, &queue).await;
    assert_eq!(published(&store), baseline + 1, "one record for the pass");
    for _ in 0..5 {
        tick(&store, &queue).await;
    }
    assert_eq!(
        published(&store),
        baseline + 1,
        "the next one is a minute away"
    );
    env.clock.set(T0 + 2 * MINUTE);
    tick(&store, &queue).await;
    assert_eq!(published(&store), baseline + 2);
    store.shutdown().await.unwrap();
}

/// A pass whose reply is lost after the record is durable ends the tenure, and
/// costs none of the requests around it: the answered one stays answered, the
/// next one enters again and rebuilds the horizon from the log.
#[tokio::test]
async fn a_failed_pass_costs_no_foreground_request_local() {
    let env = Env::new("retention-failed-pass", false);
    let store = env.open("a.db").await;
    let queue = env.address("failed");
    env.clock.set(T0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    let old = insert(&queue, "old", "k1", T0);
    store.insert(old.clone()).await.unwrap();
    env.clock.set(T0 + MINUTE);
    // The read that starts the pass is answered, though the pass then fails.
    store
        .pipeline_for_test()
        .lose_append_reply
        .store(true, Ordering::SeqCst);
    assert_eq!(
        read(&store, &queue, &[("old", T0)]).await,
        vec![ReceiptLookup::Absent]
    );
    // This one waits behind the pass, so the pass has failed by the time it runs.
    read(&store, &queue, &[]).await;
    assert!(
        !store
            .pipeline_for_test()
            .lose_append_reply
            .load(Ordering::SeqCst),
        "the pass appended and lost its reply"
    );
    // The next requests enter again and carry on.
    let fresh = insert(&queue, "fresh", "k2", T0 + MINUTE);
    assert_eq!(store.insert(fresh).await.unwrap().len(), 1);
    // Applying the record on rebuild moved the horizon to `T0 + MINUTE - 30 minutes`,
    // which is behind `T0`: nothing is expired yet, and the id still replays.
    assert_eq!(store.insert(old.clone()).await.unwrap().len(), 1);
    // A later pass moves it past `old`, and rebuilding from the log keeps it there.
    env.clock.set(T0 + 31 * MINUTE);
    tick(&store, &queue).await;
    assert_eq!(store.insert(old).await, Err(QueueError::RequestExpired));
    assert_eq!(
        read(&store, &queue, &[("old", T0), ("fresh", T0 + MINUTE)]).await[0],
        ReceiptLookup::Expired
    );
    store.shutdown().await.unwrap();
}
