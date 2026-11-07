//! The log transport under the owner loop: rebuilds that cross several leaves of
//! the log's index, lease renewal while rebuilding, and runs of records read
//! through `QueueNativeLog` directly.
use super::owner_safety::{pending_items, stats};
use super::ownership::TTL;
use super::record_counts::{Env, delta, grow, insert, now};
use super::*;
use fireweed_conformance::queue_contract as suite;
use fireweed_engine::queue_contract::QueueStore;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const SECOND: Duration = Duration::from_secs(1);

/// Provision, then insert one at a time so log order is key order.
async fn sequential(store: &NativeQueueStore, queue: &QueueAddress, len: usize) {
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: suite::schema(),
        })
        .await
        .unwrap();
    for n in 1..len {
        insert(store, queue, n).await;
    }
}
/// Move both clocks forward by `by` each time a rebuild finishes a run, `times` times.
fn tick_during_rebuild(
    store: &NativeQueueStore,
    ownership: &TestOwnership,
    by: Duration,
    times: usize,
) -> Arc<AtomicUsize> {
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = runs.clone();
    let ownership = ownership.clone();
    *store.pipeline_for_test().on_stage.lock().unwrap() = Some(Arc::new(move |stage| {
        if stage == owner::Stage::RebuildRun && counted.fetch_add(1, Ordering::SeqCst) < times {
            ownership.advance(by);
        }
    }));
    runs
}

async fn rebuild_crosses_leaves(s3: bool, len: usize) {
    let env = Env::new("leaves", s3);
    let queue = env.address("q");
    let first = env.open("a.db").await;
    sequential(&first, &queue, len).await;
    first.shutdown().await.unwrap();
    drop(first);
    let second = env.open("b.db").await;
    // Each of the first two runs takes more than half a lease.
    let runs = tick_during_rebuild(&second, &env.ownership, 8 * SECOND, 2);
    let before = now(&second);
    assert_eq!(pending_items(&second, &queue).await, len as u64 - 1);
    let (records, heads, fences) = delta(&second, before);
    assert_eq!((records, heads, fences), (len as u64, 1, 1));
    assert!(
        runs.load(Ordering::SeqCst) >= 3,
        "{len} records fit in fewer than three runs"
    );
    assert!(
        env.ownership.plane.renewals.load(Ordering::SeqCst) >= 2,
        "the rebuild renewed between runs"
    );
    // Every record was applied, in log order.
    let page = second
        .query(QueryRequest {
            queue: queue.clone(),
            source: RecordSource::Work,
            index: "by_score".into(),
            filter: suite::filter("counts"),
            descending: false,
            cursor: None,
            max_records: len as u32,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    let found: Vec<_> = page
        .records
        .iter()
        .map(|record| match record {
            QueryRecord::Work(work) => (work.work.key.clone(), work.insertion_sequence),
            QueryRecord::State(_) => panic!("work expected"),
        })
        .collect();
    let expected: Vec<_> = (1..len).map(|n| (format!("k-{n}"), n as u64)).collect();
    assert_eq!(found, expected);
    // The rebuilt owner appends where the log ended.
    let before = now(&second);
    insert(&second, &queue, 9999).await;
    assert_eq!(delta(&second, before), (0, 0, 0));
    assert_eq!(pending_items(&second, &queue).await, len as u64);
    second.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuild_crosses_several_leaves_local() {
    rebuild_crosses_leaves(false, 70).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuild_crosses_several_leaves_real_s3() {
    rebuild_crosses_leaves(true, 70).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_renewal_during_rebuild_is_busy() {
    let env = Env::new("rejected-renewal", false);
    let queue = env.address("q");
    let first = env.open("a.db").await;
    grow(&first, &queue, 70).await;
    first.shutdown().await.unwrap();
    drop(first);
    let second = env.open("b.db").await;
    // The first run takes more than half a lease, and the renewal is refused.
    let runs = tick_during_rebuild(&second, &env.ownership, 8 * SECOND, 1);
    env.ownership
        .plane
        .reject_renew
        .store(true, Ordering::SeqCst);
    let before = now(&second);
    assert_eq!(stats(&second, &queue).await.err(), Some(QueueError::Busy));
    let (records, _, _) = delta(&second, before);
    assert!(records > 0 && records < 70, "stopped part way: {records}");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "no run after the refusal");
    assert!(env.ownership.plane.renewals.load(Ordering::SeqCst) >= 1);
    // Nothing was served under that lease, and the queue is not stuck.
    env.ownership
        .plane
        .reject_renew
        .store(false, Ordering::SeqCst);
    *second.pipeline_for_test().on_stage.lock().unwrap() = None;
    env.ownership.advance(TTL + SECOND);
    assert_eq!(pending_items(&second, &queue).await, 69);
    second.shutdown().await.unwrap();
}

async fn runs_of_records_match_single_reads(s3: bool) {
    let env = Env::new("runs", s3);
    let queue = env.address("q");
    let store = env.open("a.db").await;
    sequential(&store, &queue, 50).await;
    let pipeline = store.pipeline_for_test();
    let log = &pipeline.log;
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let namespace = log.open(&queue, &job).await.unwrap().unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    assert_eq!(cut.head().bounds(), (0, 50));
    let hard = 256 << 20;
    // One run stops where a leaf does, and the next run continues from it.
    let mut offset = 0;
    let mut runs = 0;
    while offset < 50 {
        let run = log
            .read_run(&namespace, &cut, offset, 256, u64::MAX, hard, &job)
            .await
            .unwrap();
        assert!(!run.is_empty() && run.len() <= 50 - offset as usize);
        for (at, record) in (offset..).zip(&run) {
            let single = log
                .read_admitted(&namespace, &cut, at, hard, &job)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(record.bytes(), single.bytes(), "record {at}");
        }
        offset += run.len() as u64;
        runs += 1;
    }
    assert!(runs >= 3, "50 records span several leaves: {runs} runs");
    // Fewer records, fewer bytes, but never fewer than one.
    for (max_records, max_bytes, taken) in [(3, u64::MAX, 3), (256, 1, 1), (1, u64::MAX, 1)] {
        let run = log
            .read_run(&namespace, &cut, 0, max_records, max_bytes, hard, &job)
            .await
            .unwrap();
        assert_eq!(run.len(), taken, "{max_records} records, {max_bytes} bytes");
    }
    let one = log
        .read_run(&namespace, &cut, 1, 256, u64::MAX, hard, &job)
        .await
        .unwrap();
    let two = log
        .read_run(
            &namespace,
            &cut,
            1,
            256,
            one[0].bytes().len() as u64 * 2,
            hard,
            &job,
        )
        .await
        .unwrap();
    assert!(two.len() >= 2 && two.len() <= one.len());
    // A record over the hard bound fails the run, as it fails a single read.
    assert!(matches!(
        log.read_run(&namespace, &cut, 0, 256, u64::MAX, 8, &job)
            .await,
        Err(QueueError::TooLarge)
    ));
    assert!(matches!(
        log.read_admitted(&namespace, &cut, 0, 8, &job).await,
        Err(QueueError::TooLarge)
    ));
    // Nothing at or past the head.
    assert!(
        log.read_run(&namespace, &cut, 50, 256, u64::MAX, hard, &job)
            .await
            .unwrap()
            .is_empty()
    );
    drop((one, two, namespace, cut, pipeline));
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_of_records_match_single_reads_local() {
    runs_of_records_match_single_reads(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_of_records_match_single_reads_real_s3() {
    runs_of_records_match_single_reads(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_elected_with_an_empty_head_is_absent_until_provisioned() {
    // A crash between electing the namespace and publishing the genesis.
    let env = Env::new("empty-head", false);
    let queue = env.address("q");
    let store = env.open("a.db").await;
    {
        let pipeline = store.pipeline_for_test();
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        pipeline
            .log
            .provision_namespace(&queue, &job)
            .await
            .unwrap();
    }
    let read = || {
        store.read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: vec!["none".into()],
            max_bytes: 1 << 20,
        })
    };
    assert_eq!(read().await, Err(QueueError::QueueNotFound));
    // Reading again re-enters and still finds nothing.
    assert_eq!(read().await, Err(QueueError::QueueNotFound));
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: suite::schema(),
        })
        .await
        .unwrap();
    insert(&store, &queue, 1).await;
    assert_eq!(pending_items(&store, &queue).await, 1);
    // Provisioning again is a schema comparison.
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: suite::schema(),
        })
        .await
        .unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_genesis_is_a_definite_error_and_leaves_the_queue_absent() {
    let env = Env::new("big-genesis", false);
    let queue = env.address("q");
    // The genesis record cannot fit a 64-byte bound.
    let small = super::native_port_fixture_sized(
        &env.root,
        "small.db",
        env.prefix.as_deref(),
        env.clock.clone(),
        env.ownership.handle("small"),
        64,
    )
    .await;
    let request = || ProvisionRequest {
        queue: queue.clone(),
        schema: suite::schema(),
    };
    assert_eq!(small.provision(request()).await, Err(QueueError::TooLarge));
    // The owner did not loop on the error: the lease was given back.
    let ordinary = env.open("ordinary.db").await;
    assert_eq!(
        ordinary
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec!["none".into()],
                max_bytes: 1 << 20,
            })
            .await,
        Err(QueueError::QueueNotFound)
    );
    ordinary.provision(request()).await.unwrap();
    insert(&ordinary, &queue, 1).await;
    assert_eq!(pending_items(&ordinary, &queue).await, 1);
    small.shutdown().await.unwrap();
    ordinary.shutdown().await.unwrap();
}
