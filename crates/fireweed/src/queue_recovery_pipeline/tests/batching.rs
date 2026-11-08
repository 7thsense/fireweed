//! One log record per loop iteration. Operations are queued behind a closed gate so
//! that exactly one iteration takes them; the assertions count publishes, staged
//! entries and records, never time.
use super::owner_safety::{pending_items, stats};
use super::record_counts::{Env, delta, grow, now};
use super::*;
use fireweed_conformance::queue_contract as suite;
use fireweed_core::queue_contract::{FieldValue, Rejection};
use fireweed_engine::queue_contract::QueueStore;
use futures::future::BoxFuture;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

const OPERATIONS: usize = 128;

/// Holds the queue's loop inside one iteration while operations queue behind it.
struct Gate {
    entered: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}
fn close_gate(store: &NativeQueueStore) -> Gate {
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel::<()>();
    let entered_tx = Mutex::new(entered_tx);
    let release_rx = Mutex::new(release_rx);
    let fired = AtomicBool::new(false);
    *store.pipeline_for_test().on_stage.lock().unwrap() = Some(Arc::new(move |stage| {
        if stage == owner::Stage::BatchTaken && !fired.swap(true, Ordering::SeqCst) {
            entered_tx.lock().unwrap().send(()).unwrap();
            // The loop's task waits here; the test runtime has other threads.
            release_rx.lock().unwrap().recv().unwrap();
        }
    }));
    Gate { entered, release }
}
fn in_flight(store: &NativeQueueStore) -> usize {
    OPERATIONS
        - store
            .pipeline_for_test()
            .operation_slots
            .available_permits()
}
/// Run `operations` so that one loop iteration takes all of them, in the order
/// given. `trigger` is a read that opens the iteration the gate holds.
async fn together<T: Send + 'static>(
    store: &NativeQueueStore,
    trigger: BoxFuture<'static, ()>,
    operations: Vec<BoxFuture<'static, T>>,
) -> Vec<T> {
    let gate = close_gate(store);
    let trigger = tokio::spawn(trigger);
    let gate = tokio::task::spawn_blocking(move || {
        gate.entered.recv().unwrap();
        gate
    })
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for operation in operations {
        let queued = in_flight(store) + 1;
        tasks.push(tokio::spawn(operation));
        // Submission is synchronous up to the enqueue, so the count shows arrival.
        while in_flight(store) < queued {
            tokio::task::yield_now().await;
        }
    }
    gate.release.send(()).unwrap();
    trigger.await.unwrap();
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    *store.pipeline_for_test().on_stage.lock().unwrap() = None;
    results
}
/// A read of `queue` that opens an iteration.
fn read_of(store: &NativeQueueStore, queue: &QueueAddress) -> BoxFuture<'static, ()> {
    let (store, queue) = (store.clone(), queue.clone());
    Box::pin(async move {
        let _ = stats(&store, &queue).await;
    })
}
fn counts(store: &NativeQueueStore) -> (u64, u64) {
    let pipeline = store.pipeline_for_test();
    (
        pipeline.published.load(Ordering::SeqCst),
        pipeline.staged.load(Ordering::SeqCst),
    )
}
fn keyed(key: &str, target: &str, score: i64) -> NewWork {
    let mut work = suite::item(key, "counts", 0, score);
    work.fields
        .insert("target".into(), FieldValue::Text(target.into()));
    work
}
fn insert_of(
    store: &NativeQueueStore,
    queue: &QueueAddress,
    id: &str,
    mode: InsertMode,
    items: Vec<NewWork>,
) -> BoxFuture<'static, QueueResult<Vec<InsertOutcome>>> {
    let (store, request) = (
        store.clone(),
        InsertRequest {
            queue: queue.clone(),
            request_id: id.into(),
            mode,
            items,
        },
    );
    Box::pin(async move { store.insert(request).await })
}
fn commit_of(
    store: &NativeQueueStore,
    queue: &QueueAddress,
    id: &str,
    key: &str,
) -> BoxFuture<'static, QueueResult<Vec<CommitReceipt>>> {
    let mut group = suite::group(id);
    group.work_inserts.push(suite::item(key, "counts", 0, 0));
    let (store, request) = (
        store.clone(),
        CommitBatch {
            queue: queue.clone(),
            groups: vec![group],
        },
    );
    Box::pin(async move { store.commit(request).await })
}
fn inserted(outcomes: &QueueResult<Vec<InsertOutcome>>) -> bool {
    matches!(outcomes.as_deref(), Ok([InsertOutcome::Inserted { .. }]))
}

async fn concurrent_mutations_share_one_record(s3: bool, n: usize) {
    let env = Env::new("one-record", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let (published, staged) = counts(&store);
    let mut operations: Vec<BoxFuture<'static, QueueResult<()>>> = Vec::new();
    for n in 0..n {
        let operation = if n % 2 == 0 {
            let insert = insert_of(
                &store,
                &queue,
                &format!("batch-insert-{n}"),
                InsertMode::InsertOnly,
                vec![keyed(&format!("k-{n}"), &format!("k-{n}"), n as i64)],
            );
            Box::pin(async move {
                insert
                    .await
                    .and_then(|o| inserted(&Ok(o)).then_some(()).ok_or(QueueError::Busy))
            }) as BoxFuture<'static, _>
        } else {
            let commit = commit_of(
                &store,
                &queue,
                &format!("batch-commit-{n}"),
                &format!("cc-{n}"),
            );
            Box::pin(async move {
                commit.await.and_then(|r| {
                    matches!(r[0].outcome, GroupOutcome::Committed { .. })
                        .then_some(())
                        .ok_or(QueueError::Busy)
                })
            })
        };
        operations.push(operation);
    }
    let results = together(&store, read_of(&store, &queue), operations).await;
    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    let (published_after, staged_after) = counts(&store);
    assert_eq!(published_after - published, 1, "one publish for the batch");
    assert_eq!(staged_after - staged, n as u64, "one entry per request");
    // Each request kept its own outcome, found by its own identity.
    let found = store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: (0..n)
                .filter(|n| n % 2 == 1)
                .map(|n| format!("batch-commit-{n}"))
                .collect(),
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert!(
        found
            .iter()
            .all(|r| matches!(r, ReceiptLookup::Found(r) if matches!(r.outcome, GroupOutcome::Committed { .. })))
    );
    assert_eq!(pending_items(&store, &queue).await, 1 + n as u64);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_mutations_share_one_record_local() {
    concurrent_mutations_share_one_record(false, 20).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_mutations_share_one_record_real_s3() {
    concurrent_mutations_share_one_record(true, 6).await;
}

async fn a_later_entry_sees_an_earlier_staged_one(s3: bool) {
    let env = Env::new("sees-staged", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let (published, _) = counts(&store);
    let one = |id: &str, mode, score| {
        insert_of(&store, &queue, id, mode, vec![keyed("same", "same", score)])
    };
    let results = together(
        &store,
        read_of(&store, &queue),
        vec![
            one("first", InsertMode::InsertOnly, 1),
            one("second", InsertMode::InsertOnly, 2),
            one("third", InsertMode::Upsert, 3),
            // The same request twice: the second finds the first's staged receipt.
            one("first", InsertMode::InsertOnly, 1),
        ],
    )
    .await;
    let outcomes: Vec<_> = results.into_iter().map(|r| r.unwrap()).collect();
    assert!(matches!(
        outcomes[0][..],
        [InsertOutcome::Inserted { version: 1, .. }]
    ));
    assert!(matches!(
        outcomes[1][..],
        [InsertOutcome::Rejected {
            reason: Rejection::AlreadyExists,
            ..
        }]
    ));
    assert!(matches!(
        outcomes[2][..],
        [InsertOutcome::Replaced { version: 2, .. }]
    ));
    assert_eq!(outcomes[3], outcomes[0]);
    assert_eq!(counts(&store).0 - published, 1);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_entry_sees_an_earlier_staged_one_local() {
    a_later_entry_sees_an_earlier_staged_one(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_entry_sees_an_earlier_staged_one_real_s3() {
    a_later_entry_sees_an_earlier_staged_one(true).await;
}

async fn unique_swaps_pass_and_real_duplicates_are_refused(s3: bool) {
    let env = Env::new("unique", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    for (id, key, target) in [("seed-a", "a", "x"), ("seed-b", "b", "y")] {
        let seeded = insert_of(
            &store,
            &queue,
            id,
            InsertMode::InsertOnly,
            vec![keyed(key, target, 0)],
        )
        .await;
        assert!(inserted(&seeded));
    }
    let (published, _) = counts(&store);
    let upsert = |id: &str, key: &str, target: &str| {
        insert_of(
            &store,
            &queue,
            id,
            InsertMode::Upsert,
            vec![keyed(key, target, 0)],
        )
    };
    let only = |id: &str, key: &str, target: &str| {
        insert_of(
            &store,
            &queue,
            id,
            InsertMode::InsertOnly,
            vec![keyed(key, target, 0)],
        )
    };
    let results = together(
        &store,
        read_of(&store, &queue),
        vec![
            // b leaves y, and only then may a take it: a swap across two requests.
            upsert("move-b", "b", "z"),
            upsert("move-a", "a", "y"),
            only("take-w", "c", "w"),
            // A real duplicate of the target the request before it took.
            only("duplicate-w", "d", "w"),
        ],
    )
    .await;
    let outcomes: Vec<_> = results.into_iter().map(|r| r.unwrap()).collect();
    assert!(
        matches!(outcomes[0][..], [InsertOutcome::Replaced { .. }]),
        "{outcomes:?}"
    );
    assert!(
        matches!(outcomes[1][..], [InsertOutcome::Replaced { .. }]),
        "{outcomes:?}"
    );
    assert!(
        matches!(outcomes[2][..], [InsertOutcome::Inserted { .. }]),
        "{outcomes:?}"
    );
    assert!(
        matches!(
            outcomes[3][..],
            [InsertOutcome::Rejected {
                reason: Rejection::UniqueConflict,
                ..
            }]
        ),
        "{outcomes:?}"
    );
    assert_eq!(
        counts(&store).0 - published,
        1,
        "one record carried all four"
    );
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_swaps_pass_and_real_duplicates_are_refused_local() {
    unique_swaps_pass_and_real_duplicates_are_refused(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_swaps_pass_and_real_duplicates_are_refused_real_s3() {
    unique_swaps_pass_and_real_duplicates_are_refused(true).await;
}

/// A fence taken by someone else fails the batch's publish for certain.
async fn failed_publish_leaves_the_projection_unchanged(s3: bool, n: usize) {
    let env = Env::new("failed-publish", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    let log = grow(&store, &queue, 4).await;
    let pipeline = store.pipeline_for_test();
    let job = Arc::new(
        QueueAdmission::new(1, 64 << 20)
            .unwrap()
            .reserve(0)
            .unwrap(),
    );
    let namespace = pipeline.log.open(&queue, &job).await.unwrap().unwrap();
    pipeline.log.acquire_bundle(namespace, &job).await.unwrap();
    let (published, _) = counts(&store);
    let before = now(&store);
    let operations = (0..n)
        .map(|n| {
            insert_of(
                &store,
                &queue,
                &format!("after-fault-{n}"),
                InsertMode::InsertOnly,
                vec![keyed(&format!("f-{n}"), &format!("f-{n}"), 0)],
            )
        })
        .collect();
    let results = together(&store, read_of(&store, &queue), operations).await;
    assert!(results.iter().all(inserted), "{results:?}");
    // The first publish was refused, the batch left owner state, and the
    // re-entered owner read exactly the log as it was: nothing staged survived.
    // It then published the whole batch in one record.
    assert_eq!(counts(&store).0 - published, 2);
    let (records, _, fences) = delta(&store, before);
    assert_eq!((records, fences), (log, 1));
    assert_eq!(pending_items(&store, &queue).await, 3 + n as u64);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_publish_leaves_the_projection_unchanged_local() {
    failed_publish_leaves_the_projection_unchanged(false, 12).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_publish_leaves_the_projection_unchanged_real_s3() {
    failed_publish_leaves_the_projection_unchanged(true, 4).await;
}

async fn a_reply_lost_after_a_durable_publish_finds_every_request(s3: bool, n: usize) {
    let env = Env::new("lost-reply", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    let log = grow(&store, &queue, 4).await;
    let (published, _) = counts(&store);
    let before = now(&store);
    store
        .pipeline_for_test()
        .lose_append_reply
        .store(true, Ordering::SeqCst);
    let operations = (0..n)
        .map(|n| {
            insert_of(
                &store,
                &queue,
                &format!("lost-{n}"),
                InsertMode::InsertOnly,
                vec![keyed(&format!("l-{n}"), &format!("l-{n}"), 0)],
            )
        })
        .collect();
    let results = together(&store, read_of(&store, &queue), operations).await;
    // Every request is answered with its own original outcome, not a duplicate's.
    assert!(results.iter().all(inserted), "{results:?}");
    // One publish, durable though its reply was lost; re-entry read it back and
    // found every request in it by id, so nothing was published again.
    assert_eq!(counts(&store).0 - published, 1);
    let (records, _, fences) = delta(&store, before);
    assert_eq!((records, fences), (log + 1, 1));
    assert_eq!(pending_items(&store, &queue).await, 3 + n as u64);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_lost_after_a_durable_publish_finds_every_request_local() {
    a_reply_lost_after_a_durable_publish_finds_every_request(false, 12).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_lost_after_a_durable_publish_finds_every_request_real_s3() {
    a_reply_lost_after_a_durable_publish_finds_every_request(true, 4).await;
}

/// What a reader of the queue can see, for comparing two owners of one log.
async fn observed(
    store: &NativeQueueStore,
    queue: &QueueAddress,
    request_ids: Vec<String>,
) -> String {
    let page = store
        .query(QueryRequest {
            queue: queue.clone(),
            source: RecordSource::Work,
            index: "by_score".into(),
            filter: suite::filter("counts"),
            descending: false,
            cursor: None,
            max_records: 1000,
            max_bytes: 8 << 20,
        })
        .await
        .unwrap();
    let totals = stats(store, queue).await.unwrap();
    let receipts = store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids,
            max_bytes: 8 << 20,
        })
        .await
        .unwrap();
    format!("{page:?}\n{totals:?}\n{receipts:?}")
}
async fn rebuilding_batch_records_gives_the_live_state(s3: bool, n: usize) {
    let env = Env::new("rebuild", s3);
    let live = env.open("a.db").await;
    let queue = env.address("q");
    grow(&live, &queue, 2).await;
    let mut operations: Vec<BoxFuture<'static, QueueResult<()>>> = Vec::new();
    let mut commit_ids = Vec::new();
    for n in 0..n {
        let key = format!("r-{n}");
        let operation: BoxFuture<'static, QueueResult<()>> = match n % 3 {
            0 => {
                let insert = insert_of(
                    &live,
                    &queue,
                    &format!("ri-{n}"),
                    InsertMode::Upsert,
                    vec![keyed(&key, &key, n as i64)],
                );
                Box::pin(async move { insert.await.map(|_| ()) })
            }
            1 => {
                commit_ids.push(format!("rc-{n}"));
                let commit = commit_of(&live, &queue, &format!("rc-{n}"), &key);
                Box::pin(async move { commit.await.map(|_| ()) })
            }
            // A duplicate key and a unique conflict, both rejected inside the batch.
            _ => {
                let insert = insert_of(
                    &live,
                    &queue,
                    &format!("rd-{n}"),
                    InsertMode::InsertOnly,
                    vec![keyed("r-0", "r-0", 0)],
                );
                Box::pin(async move { insert.await.map(|_| ()) })
            }
        };
        operations.push(operation);
    }
    let published = counts(&live).0;
    let results = together(&live, read_of(&live, &queue), operations).await;
    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    assert_eq!(counts(&live).0 - published, 1);
    let mut ids = commit_ids;
    ids.push("seed-never-written".into());
    let expected = observed(&live, &queue, ids.clone()).await;
    // Another owner takes the queue over and rebuilds it from the log.
    env.ownership
        .advance(super::ownership::TTL + std::time::Duration::from_secs(1));
    let other = env.open("b.db").await;
    let before = now(&other);
    assert_eq!(observed(&other, &queue, ids).await, expected);
    assert!(delta(&other, before).0 >= 2, "it rebuilt from the log");
    live.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuilding_batch_records_gives_the_live_state_local() {
    rebuilding_batch_records_gives_the_live_state(false, 30).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuilding_batch_records_gives_the_live_state_real_s3() {
    rebuilding_batch_records_gives_the_live_state(true, 6).await;
}

async fn the_record_bound_splits_a_batch_without_loss_or_reorder(s3: bool, n: usize, bound: u64) {
    let env = Env::new("split-batch", s3);
    let store = env.open_bounded("a.db", bound).await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let (published, staged) = counts(&store);
    let operations = (0..n)
        .map(|n| {
            insert_of(
                &store,
                &queue,
                &format!("split-{n:03}"),
                InsertMode::InsertOnly,
                vec![keyed(&format!("s-{n:03}"), &format!("s-{n:03}"), 0)],
            )
        })
        .collect();
    let results = together(&store, read_of(&store, &queue), operations).await;
    assert!(results.iter().all(inserted), "{results:?}");
    let (published_after, staged_after) = counts(&store);
    assert_eq!(staged_after - staged, n as u64, "every request staged once");
    let records = published_after - published;
    assert!(records > 1, "the bound split the batch");
    assert!(records < n as u64, "and it still shared records");
    // Arrival order is insertion order: claims return the keys as submitted. A
    // claim is itself an entry, so take them a few at a time.
    let mut keys = Vec::new();
    for page in 0..=(n + 1) / 4 {
        let claimed = store
            .claim(ClaimRequest {
                max_records: 4,
                ..suite::claim(&queue, &format!("order-{page}"), "counts")
            })
            .await
            .unwrap();
        keys.extend(claimed.iter().map(|w| w.record.work.key.clone()));
    }
    let expected: Vec<_> = (0..n).map(|n| format!("s-{n:03}")).collect();
    // The grow() seed items come first in insertion order and have other keys.
    let ours: Vec<_> = keys.into_iter().filter(|k| k.starts_with("s-")).collect();
    assert_eq!(ours, expected);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_record_bound_splits_a_batch_without_loss_or_reorder_local() {
    the_record_bound_splits_a_batch_without_loss_or_reorder(false, 40, 6000).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_record_bound_splits_a_batch_without_loss_or_reorder_real_s3() {
    the_record_bound_splits_a_batch_without_loss_or_reorder(true, 12, 3000).await;
}

async fn provision_runs_in_an_iteration_of_its_own(s3: bool) {
    let env = Env::new("provision-alone", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    let provision = {
        let (store, request) = (
            store.clone(),
            ProvisionRequest {
                queue: queue.clone(),
                schema: suite::schema(),
            },
        );
        Box::pin(async move { store.provision(request).await.map(|_| vec![]) })
            as BoxFuture<'static, QueueResult<Vec<InsertOutcome>>>
    };
    let published = counts(&store).0;
    let results = together(
        &store,
        read_of(&store, &queue),
        vec![
            provision,
            insert_of(
                &store,
                &queue,
                "p-1",
                InsertMode::InsertOnly,
                vec![keyed("p-1", "p-1", 0)],
            ),
            insert_of(
                &store,
                &queue,
                "p-2",
                InsertMode::InsertOnly,
                vec![keyed("p-2", "p-2", 0)],
            ),
        ],
    )
    .await;
    assert!(results[0].is_ok());
    assert!(
        inserted(&results[1]) && inserted(&results[2]),
        "{results:?}"
    );
    // The genesis is alone in its record; the two inserts share the next one.
    assert_eq!(counts(&store).0 - published, 2);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provision_runs_in_an_iteration_of_its_own_local() {
    provision_runs_in_an_iteration_of_its_own(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provision_runs_in_an_iteration_of_its_own_real_s3() {
    provision_runs_in_an_iteration_of_its_own(true).await;
}

async fn a_commit_batch_that_fills_the_record_continues_in_the_next(s3: bool, groups: usize) {
    let env = Env::new("split-commit", s3);
    let store = env.open_bounded("a.db", 6000).await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let published = counts(&store).0;
    let request = || CommitBatch {
        queue: queue.clone(),
        groups: (0..groups)
            .map(|n| {
                let mut group = suite::group(&format!("big-{n:03}"));
                group
                    .work_inserts
                    .push(suite::item(&format!("big-{n:03}"), "counts", 0, 0));
                group
            })
            .collect(),
    };
    let receipts = store.commit(request()).await.unwrap();
    assert_eq!(receipts.len(), groups);
    assert!(
        receipts
            .iter()
            .all(|r| matches!(r.outcome, GroupOutcome::Committed { .. })),
        "{receipts:?}"
    );
    let records = counts(&store).0 - published;
    assert!(records > 1, "the bound split the batch's groups");
    // Resubmitting finds every group by its identity and publishes nothing.
    assert_eq!(store.commit(request()).await.unwrap(), receipts);
    assert_eq!(counts(&store).0 - published, records);
    assert_eq!(pending_items(&store, &queue).await, 1 + groups as u64);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_batch_that_fills_the_record_continues_in_the_next_local() {
    a_commit_batch_that_fills_the_record_continues_in_the_next(false, 60).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_batch_that_fills_the_record_continues_in_the_next_real_s3() {
    a_commit_batch_that_fills_the_record_continues_in_the_next(true, 30).await;
}

/// Files under `root`, the local object store's objects.
fn objects_under(root: &std::path::Path) -> u64 {
    let mut count = 0;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                count += 1;
            }
        }
    }
    count
}
/// After entry, a batch of any size is one conditional publish. On the local
/// store the objects it writes are counted too. On S3 the requests are counted
/// at the wire by a proxy between the SDK client and the endpoint (the SDK builds
/// its store inside `NativeLog` and offers no hook): no GET, HEAD or LIST, and
/// the same PUTs and round trips for every batch size.
async fn publishes_per_batch_do_not_grow_with_batch_size(s3: bool, sizes: &[usize]) {
    let env = if s3 {
        Env::counted("batch-sizes").await
    } else {
        Env::new("batch-sizes", false)
    };
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    // Entry and the first append of a tenure are done; measure from here.
    insert_of(
        &store,
        &queue,
        "warm",
        InsertMode::InsertOnly,
        vec![keyed("warm", "warm", 0)],
    )
    .await
    .unwrap();
    let mut written = Vec::new();
    let mut wire = Vec::new();
    for &size in sizes {
        let (published, staged) = counts(&store);
        if let Some(proxy) = &env.proxy {
            proxy.reset();
        }
        let objects = objects_under(&env.root.join("log"));
        let operations = (0..size)
            .map(|n| {
                insert_of(
                    &store,
                    &queue,
                    &format!("size-{size}-{n}"),
                    InsertMode::InsertOnly,
                    vec![keyed(&format!("z-{size}-{n}"), &format!("z-{size}-{n}"), 0)],
                )
            })
            .collect();
        let results = together(&store, read_of(&store, &queue), operations).await;
        assert!(results.iter().all(inserted));
        let window = env.proxy.as_ref().map(|proxy| proxy.snapshot());
        let (published_after, staged_after) = counts(&store);
        assert_eq!(
            published_after - published,
            1,
            "batch of {size}: one publish"
        );
        if let Some(window) = window {
            let (puts, gets, heads, lists) = (
                window.count("PUT"),
                window.count("GET"),
                window.count("HEAD"),
                window.count("LIST"),
            );
            eprintln!(
                "batch={size} entries={} PUT={puts} (conditional={}) GET={gets} HEAD={heads} LIST={lists} other={} round_trips={}",
                staged_after - staged,
                window.cas_puts(),
                window.other(),
                window.round_trips
            );
            let reads: Vec<_> = window
                .requests
                .iter()
                .filter(|r| r.method != "PUT")
                .map(|r| (r.method.clone(), r.uri.clone(), r.status))
                .collect();
            assert_eq!(
                (gets, heads, lists),
                (0, 0, 0),
                "batch of {size} read: {reads:?}"
            );
            assert_eq!(
                window.cas_puts(),
                1,
                "batch of {size}: one conditional publish"
            );
            assert_eq!(
                window.cas_puts() + window.create_only_puts(),
                puts,
                "batch of {size}: every PUT is conditional"
            );
            wire.push((puts, window.round_trips));
        }
        assert_eq!(staged_after - staged, size as u64);
        let objects = objects_under(&env.root.join("log")) - objects;
        eprintln!(
            "batch of {size}: publishes {}, entries {}, new local objects {objects}",
            published_after - published,
            staged_after - staged
        );
        written.push(objects);
    }
    assert!(
        wire.windows(2).all(|pair| pair[0] == pair[1]),
        "PUTs and round trips per batch must not depend on its size: {wire:?}"
    );
    if !s3 {
        assert!(
            written.windows(2).all(|pair| pair[0] == pair[1]),
            "objects written per batch must not depend on its size: {written:?}"
        );
    }
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publishes_per_batch_do_not_grow_with_batch_size_local() {
    publishes_per_batch_do_not_grow_with_batch_size(false, &[1, 10, 100]).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publishes_per_batch_do_not_grow_with_batch_size_real_s3() {
    publishes_per_batch_do_not_grow_with_batch_size(true, &[1, 10, 100]).await;
}

/// While a batch holds its queue's writer across its publish, more operations of
/// other queues than the pool has scratch workers are answered without holding up
/// the batch: none of them waits on a worker for the batch's writer.
async fn queues_waiting_for_the_writer_do_not_starve_a_batch_of_workers(s3: bool) {
    let env = Env::new("starve", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel::<()>();
    let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
    let fired = AtomicBool::new(false);
    *store.pipeline_for_test().on_stage.lock().unwrap() = Some(Arc::new(move |stage| {
        // The batch is open and holds the writer when it is about to publish.
        if stage == owner::Stage::BeforeAppend && !fired.swap(true, Ordering::SeqCst) {
            entered_tx.lock().unwrap().send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    }));
    let writer = tokio::spawn(insert_of(
        &store,
        &queue,
        "starve-1",
        InsertMode::InsertOnly,
        vec![keyed("starve-1", "starve-1", 0)],
    ));
    tokio::task::spawn_blocking(move || entered.recv().unwrap())
        .await
        .unwrap();
    // More other-queue operations than the pool has workers.
    let others: Vec<_> = (0..6)
        .map(|n| {
            let (store, queue) = (store.clone(), env.address(&format!("other-{n}")));
            tokio::spawn(async move {
                store
                    .read_commit(ReceiptReadRequest {
                        queue,
                        request_ids: vec!["none".into()],
                        max_bytes: 1 << 20,
                    })
                    .await
            })
        })
        .collect();
    while in_flight(&store) < 1 + others.len() {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    release.send(()).unwrap();
    let limit = std::time::Duration::from_secs(60);
    let written = tokio::time::timeout(limit, writer)
        .await
        .expect("the batch finishes")
        .unwrap();
    assert!(inserted(&written), "{written:?}");
    for other in others {
        let result = tokio::time::timeout(limit, other)
            .await
            .expect("the other queue is answered")
            .unwrap();
        assert_eq!(result.err(), Some(QueueError::QueueNotFound));
    }
    *store.pipeline_for_test().on_stage.lock().unwrap() = None;
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queues_waiting_for_the_writer_do_not_starve_a_batch_of_workers_local() {
    queues_waiting_for_the_writer_do_not_starve_a_batch_of_workers(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queues_waiting_for_the_writer_do_not_starve_a_batch_of_workers_real_s3() {
    queues_waiting_for_the_writer_do_not_starve_a_batch_of_workers(true).await;
}

fn lone_too_large(store: &NativeQueueStore) -> u64 {
    store
        .pipeline_for_test()
        .lone_too_large
        .load(Ordering::SeqCst)
}
/// A record the planner accepts must fit alone once it is wrapped as the only
/// entry of an entry-list record. Sweep insert sizes across the bound: the
/// planner refuses what is too large, and `Batch::stage` never has to.
async fn a_planned_insert_always_fits_its_wrapping_record(s3: bool, bound: u64) {
    let env = Env::new("planned-fits-insert", s3);
    let store = env.open_bounded("a.db", bound).await;
    let queue = env.address("q");
    grow(&store, &queue, 1).await;
    let attempt = |len: usize| {
        insert_of(
            &store,
            &queue,
            &format!("fit-{len:05}"),
            InsertMode::InsertOnly,
            vec![keyed(&format!("f-{len:05}"), &"x".repeat(len), 0)],
        )
    };
    // A record that is too large is refused as `TooLarge`. A frame already over
    // the bound, further up, is refused otherwise: the search treats any error as
    // over, and the sweep below stays in the `TooLarge` band.
    let refused = |outcome: &QueueResult<Vec<InsertOutcome>>| !inserted(outcome);
    // The smallest size the store refuses.
    let (mut fits, mut over) = (0usize, bound as usize);
    assert!(!refused(&attempt(fits).await));
    assert!(refused(&attempt(over).await));
    while over - fits > 1 {
        let mid = (fits + over) / 2;
        if refused(&attempt(mid).await) {
            over = mid;
        } else {
            fits = mid;
        }
    }
    let mut refusals = 0;
    for len in over.saturating_sub(3)..over + 40 {
        let outcome = attempt(len).await;
        if refused(&outcome) {
            refusals += 1;
            assert!(matches!(outcome, Err(QueueError::TooLarge)), "{outcome:?}");
        }
        assert_eq!(!inserted(&outcome), len >= over, "len {len}");
    }
    assert!(refusals > 0);
    assert_eq!(lone_too_large(&store), 0);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_insert_always_fits_its_wrapping_record_local() {
    a_planned_insert_always_fits_its_wrapping_record(false, 3000).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_insert_always_fits_its_wrapping_record_real_s3() {
    a_planned_insert_always_fits_its_wrapping_record(true, 3000).await;
}

/// Commit packs that fill the bound are staged, never declined for size and
/// replaced by single groups. The group size is swept so some pack lands in the
/// bytes between the bound less the wrapping overhead and the bound.
async fn a_commit_pack_that_fills_the_bound_is_staged(s3: bool, sizes: std::ops::Range<usize>) {
    let env = Env::new("planned-fits-pack", s3);
    let store = env.open_bounded("a.db", 3000).await;
    let queue = env.address("q");
    grow(&store, &queue, 1).await;
    let groups = 24;
    for pad in sizes {
        let receipts = store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: (0..groups)
                    .map(|n| {
                        let mut group = suite::group(&format!("p{pad:03}-{n:03}"));
                        group.work_inserts.push(suite::item(
                            &format!("p{pad:03}-{n:03}-{}", "x".repeat(pad)),
                            "counts",
                            0,
                            0,
                        ));
                        group
                    })
                    .collect(),
            })
            .await
            .unwrap();
        assert!(
            receipts
                .iter()
                .all(|r| matches!(r.outcome, GroupOutcome::Committed { .. })),
            "{receipts:?}"
        );
    }
    assert_eq!(lone_too_large(&store), 0);
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_pack_that_fills_the_bound_is_staged_local() {
    a_commit_pack_that_fills_the_bound_is_staged(false, 0..40).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_pack_that_fills_the_bound_is_staged_real_s3() {
    a_commit_pack_that_fills_the_bound_is_staged(true, 0..30).await;
}
