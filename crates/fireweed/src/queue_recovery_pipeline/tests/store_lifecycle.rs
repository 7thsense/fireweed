//! Store-interface behaviour the owner loop still has: caller drop, batch packing
//! and splitting, interruption after a durable prefix, and the administrative
//! cancel path. Nothing here reaches into the loop except the test stage hook.
use super::owner_safety::{pending_items, wait_for};
use super::ownership::TestOwnership;
use super::record_counts::{Env, grow, now};
use super::*;
use fireweed_conformance::queue_contract as suite;
use fireweed_core::queue_administration::*;
use fireweed_engine::queue_contract::QueueStore;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn work_group(id: &str, key: &str) -> CommitGroup {
    let mut group = suite::group(id);
    group.work_inserts.push(suite::item(key, "counts", 0, 0));
    group
}
/// Addressed update of `key`'s priority.
fn admin_group(id: &str, key: &str, version: u64, priority: i64) -> CommitGroup {
    let mut group = suite::group(id);
    group.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 0,
            before_fields: Some(Default::default()),
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: key.into(),
                expected_version: Some(version),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(priority),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: Default::default(),
                },
            }]),
        },
    });
    group
}
fn committed(receipt: &CommitReceipt) -> bool {
    matches!(receipt.outcome, GroupOutcome::Committed { .. })
}
fn administered(receipt: &CommitReceipt) -> bool {
    matches!(
        receipt.outcome,
        GroupOutcome::AdministrativeCommitted { .. }
    )
}
/// Close `store` and count the log records a fresh owner reads to rebuild.
async fn log_length(env: &Env, store: NativeQueueStore, queue: &QueueAddress) -> u64 {
    store.shutdown().await.unwrap();
    let probe = env
        .open_with("probe.db", TestOwnership::new().handle("probe"))
        .await;
    let before = now(&probe);
    pending_items(&probe, queue).await;
    let records = now(&probe).admitted_raw_records - before.admitted_raw_records;
    probe.shutdown().await.unwrap();
    records
}
fn batch(queue: &QueueAddress, groups: Vec<CommitGroup>) -> CommitBatch {
    CommitBatch {
        queue: queue.clone(),
        groups,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_drop_still_completes_the_operation_durably() {
    let env = Env::new("caller-drop", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 2).await;
    let request = || InsertRequest {
        queue: queue.clone(),
        request_id: "dropped-caller".into(),
        mode: InsertMode::InsertOnly,
        items: vec![suite::item("dropped", "counts", 0, 0)],
        issued_at: 0,
    };
    let caller = tokio::spawn({
        let a = a.clone();
        let request = request();
        async move { a.insert(request).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    caller.abort();
    let probe = a.clone();
    let probe_queue = queue.clone();
    wait_for("the dropped caller's insert", async || {
        pending_items(&probe, &probe_queue).await == 2
    })
    .await;
    // The retry returns the stored outcome and publishes nothing new.
    let before = now(&a);
    let outcome = a.insert(request()).await.unwrap();
    assert!(matches!(outcome[0], InsertOutcome::Inserted { .. }));
    assert_eq!(pending_items(&a, &queue).await, 2);
    assert_eq!(now(&a).admitted_raw_records, before.admitted_raw_records);
    a.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_groups_pack_into_one_record() {
    let env = Env::new("pack", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 2).await;
    let receipts = a
        .commit(batch(
            &queue,
            vec![
                work_group("p1", "pk1"),
                work_group("p2", "pk2"),
                work_group("p3", "pk3"),
            ],
        ))
        .await
        .unwrap();
    assert!(receipts.iter().all(committed), "{receipts:?}");
    // genesis, the seed commit and one pack.
    assert_eq!(log_length(&env, a, &queue).await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn administration_splits_the_pack() {
    let env = Env::new("split", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 2).await;
    let staged = a.pipeline_for_test().staged.load(Ordering::SeqCst);
    let receipts = a
        .commit(batch(
            &queue,
            vec![
                work_group("s1", "sk1"),
                admin_group("s-admin", "c-1", 1, 17),
                work_group("s3", "sk3"),
                work_group("s4", "sk4"),
            ],
        ))
        .await
        .unwrap();
    assert!(
        committed(&receipts[0]) && administered(&receipts[1]),
        "{receipts:?}"
    );
    assert!(committed(&receipts[2]) && committed(&receipts[3]));
    // The administrative group splits the packs into three entries: [s1], admin,
    // [s3, s4]. They share one record: genesis, seed, and that record.
    let staged = a.pipeline_for_test().staged.load(Ordering::SeqCst) - staged;
    assert_eq!(staged, 3);
    assert_eq!(log_length(&env, a, &queue).await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_interrupted_after_a_durable_prefix_resolves_by_identity() {
    let env = Env::new("interrupted", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 2).await;
    let request = || {
        batch(
            &queue,
            vec![
                work_group("i1", "ik1"),
                admin_group("i-admin", "c-1", 1, 23),
                work_group("i3", "ik3"),
            ],
        )
    };
    // The append lands, its reply is lost, and the control plane goes away. The
    // three groups are three entries of this one record.
    let appends = Arc::new(AtomicUsize::new(0));
    let pipeline = a.pipeline_for_test();
    let plane = env.ownership.plane.clone();
    let flag = pipeline.clone();
    *pipeline.on_stage.lock().unwrap() = Some(Arc::new(move |stage| {
        if stage == owner::Stage::BeforeAppend && appends.fetch_add(1, Ordering::SeqCst) == 0 {
            flag.lose_append_reply.store(true, Ordering::SeqCst);
            plane.down.store(true, Ordering::SeqCst);
        }
    }));
    assert!(matches!(
        a.commit(request()).await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    *pipeline.on_stage.lock().unwrap() = None;
    drop(pipeline);
    env.ownership.plane.down.store(false, Ordering::SeqCst);
    // The retry finds the prefix by identity and finishes the rest once.
    let receipts = a.commit(request()).await.unwrap();
    assert!(committed(&receipts[0]) && administered(&receipts[1]) && committed(&receipts[2]));
    let again = a.commit(request()).await.unwrap();
    assert_eq!(again, receipts);
    assert_eq!(pending_items(&a, &queue).await, 3, "c-1, ik1, ik3");
    // genesis, seed, and the one record: nothing was written twice.
    assert_eq!(log_length(&env, a, &queue).await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_administrative_commit_is_resolved_by_retry() {
    let env = Env::new("admin-cancel", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 2).await;
    let request = || batch(&queue, vec![admin_group("cancelled-admin", "c-1", 1, 31)]);
    let caller = tokio::spawn({
        let a = a.clone();
        let request = request();
        async move { a.commit(request).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    caller.abort();
    let receipts = a.commit(request()).await.unwrap();
    assert!(administered(&receipts[0]), "{receipts:?}");
    // One administrative record, however many times it was submitted.
    assert_eq!(log_length(&env, a, &queue).await, 3);
}
